"""MehrNews worker implementation."""

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp
import feedparser
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import setup_logging
from app.core.category_normalizer import normalize_category
from app.db.base import AsyncSessionLocal
from app.db.models import News
from app.storage.s3 import get_s3_session, init_s3
from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter

logger = setup_logging(source="mehrnews")

# RSS feed URL
MEHRNEWS_RSS_URL = "https://www.mehrnews.com/rss"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class MehrNewsWorker(BaseWorker):
    """Worker for MehrNews RSS feed."""

    def __init__(self):
        """Initialize MehrNews worker."""
        super().__init__("mehrnews")
        self.rss_url = MEHRNEWS_RSS_URL
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._s3_initialized = False
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                timeout=HTTP_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
        return self.http_session

    async def _fetch_with_retry(
        self,
        url: str,
        max_retries: int = HTTP_RETRIES,
        request_type: str = "article",
    ) -> Optional[bytes]:
        """
        Fetch URL with retries and rate limiting.

        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            request_type: Type of request (rss, article) for logging

        Returns:
            Response content as bytes, or None if all retries failed
        """
        session = await self._get_http_session()
        for attempt in range(max_retries):
            try:
                # Apply rate limiting before making request
                await self.rate_limiter.acquire(
                    source=self.source_name,
                    request_type=request_type
                )
                
                stats = self.rate_limiter.get_stats(self.source_name)
                self.logger.debug(
                    f"Rate limit: {stats['requests_last_minute']}/{stats['max_requests_per_minute']} requests/min, "
                    f"delay: {stats['delay_between_requests']}s",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                        "delay_applied": stats['delay_between_requests'],
                    }
                )
                
                async with session.get(url) as response:
                    # Handle HTTP 429 (Too Many Requests)
                    if response.status == 429:
                        # Get Retry-After header if available
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                wait_time = float(retry_after)
                            except ValueError:
                                wait_time = 60  # Default to 60 seconds
                        else:
                            # Exponential backoff for 429
                            wait_time = min(2 ** attempt * 10, 300)  # Max 5 minutes
                        
                        self.logger.warning(
                            f"HTTP 429 (Rate Limited) for {url}, waiting {wait_time}s before retry",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                                "article_url": url,
                                "retry_after": wait_time,
                            }
                        )
                        
                        if attempt < max_retries - 1:
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            self.logger.error(
                                f"Rate limit exceeded after {max_retries} attempts: {url}",
                                extra={
                                    "source": self.source_name,
                                    "request_type": request_type,
                                    "article_url": url,
                                }
                            )
                            return None
                    
                    if response.status == 200:
                        return await response.read()
                    else:
                        self.logger.warning(
                            f"HTTP {response.status} for {url}, attempt {attempt + 1}/{max_retries}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                                "article_url": url,
                            }
                        )
            except Exception as e:
                self.logger.warning(
                    f"Error fetching {url}, attempt {attempt + 1}/{max_retries}: {e}",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                        "article_url": url,
                    }
                )
                if attempt < max_retries - 1:
                    # Exponential backoff for temporary failures
                    wait_time = min(2 ** attempt, 60)  # Max 60 seconds
                    await asyncio.sleep(wait_time)
        return None

    async def _parse_rss_feed(self) -> list[dict]:
        """
        Fetch and parse RSS feed.

        Returns:
            List of RSS items as dictionaries
        """
        self.logger.info(f"Fetching RSS feed: {self.rss_url}")
        content = await self._fetch_with_retry(self.rss_url, request_type="rss")

        if content is None:
            self.logger.error(f"Failed to fetch RSS feed: {self.rss_url}")
            return []

        try:
            # Try parsing as string first (feedparser can handle both)
            if isinstance(content, bytes):
                feed = feedparser.parse(content.decode('utf-8', errors='ignore'))
            else:
                feed = feedparser.parse(content)
        except Exception as e:
            self.logger.error(f"Error parsing RSS feed: {e}", exc_info=True)
            return []

        if feed.bozo and feed.bozo_exception:
            self.logger.warning(f"RSS feed parsing warning: {feed.bozo_exception}")

        items = []

        if not feed.entries:
            self.logger.warning("RSS feed has no entries")
            return []

        for entry in feed.entries:
            item = {
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "description": entry.get("description", ""),
                "pubDate": entry.get("published", ""),
                "category": entry.get("tags", [{}])[0].get("term", "") if entry.get("tags") else "",
            }
            if item["link"]:  # Only add items with valid links
                items.append(item)
                self.logger.debug(
                    f"Parsed RSS item: {item['title'][:50]}...",
                    extra={"article_url": item["link"]}
                )

        self.logger.info(f"Parsed {len(items)} items from RSS feed")
        return items

    async def _check_url_exists(self, url: str, db: AsyncSession) -> bool:
        """
        Check if article URL already exists in database.

        Args:
            url: Article URL
            db: Database session

        Returns:
            True if URL exists, False otherwise
        """
        result = await db.execute(select(News).where(News.url == url))
        return result.scalar_one_or_none() is not None

    async def _extract_article_content(self, url: str) -> Optional[dict]:
        """
        Fetch and extract article content from HTML page.

        Args:
            url: Article URL

        Returns:
            Dictionary with extracted content, or None if extraction failed
        """
        content = await self._fetch_with_retry(url, request_type="article")
        if content is None:
            self.logger.error(f"Failed to fetch article page", extra={"article_url": url})
            return None

        try:
            # Try different encodings if needed
            if isinstance(content, bytes):
                try:
                    html_content = content.decode('utf-8')
                except UnicodeDecodeError:
                    html_content = content.decode('utf-8', errors='ignore')
            else:
                html_content = content

            soup = BeautifulSoup(html_content, "html.parser")

            # Extract title - try multiple selectors
            title = ""
            title_selectors = [
                "h1.article-title",
                "h1",
                ".title",
                "title"
            ]
            for selector in title_selectors:
                title_tag = soup.select_one(selector) if "." in selector or "#" in selector else soup.find(selector)
                if title_tag:
                    title = title_tag.get_text(strip=True)
                    if title:
                        break

            # Extract article body - prefer <article> tag
            body_html = ""
            article_selectors = [
                "article",
                ".article-body",
                ".content",
                ".post-content",
                "#content",
                ".news-content",
                ".article-content"
            ]
            
            article_tag = None
            for selector in article_selectors:
                if "." in selector or "#" in selector:
                    article_tag = soup.select_one(selector)
                else:
                    article_tag = soup.find(selector)
                if article_tag:
                    break
            
            if article_tag:
                # Remove script and style tags
                for tag in article_tag.find_all(["script", "style", "iframe"]):
                    tag.decompose()
                # Remove ads and social media widgets
                for tag in article_tag.find_all(class_=lambda x: x and any(skip in x.lower() for skip in ["ad", "advertisement", "social", "share"])):
                    tag.decompose()
                # Remove content after "کد خبر"
                code_marker = article_tag.find(string=lambda text: text and "کد خبر" in text)
                if code_marker:
                    marker_parent = code_marker.parent
                    for node in list(marker_parent.find_all_next()):
                        node.decompose()
                    marker_parent.decompose()
                body_html = str(article_tag)
            else:
                self.logger.warning(f"Could not find article body content", extra={"article_url": url})

            # Extract summary from meta description or first paragraph
            summary = ""
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content"):
                summary = meta_desc["content"]
            elif body_html:
                # Fallback: first paragraph
                first_p = soup.find("p")
                if first_p:
                    summary = first_p.get_text(strip=True)[:500]

            # Extract category
            category = ""
            category_tag = soup.find("meta", attrs={"property": "article:section"})
            if category_tag and category_tag.get("content"):
                category = category_tag["content"]
            else:
                # Try to find category in breadcrumbs or navigation
                breadcrumb = soup.find("nav", class_=re.compile("breadcrumb", re.I))
                if breadcrumb:
                    links = breadcrumb.find_all("a")
                    if len(links) > 1:
                        category = links[-1].get_text(strip=True)

            # Extract main image - prefer og:image
            image_url = ""
            og_image = soup.find("meta", attrs={"property": "og:image"})
            if og_image and og_image.get("content"):
                image_url = og_image["content"]
            else:
                # Fallback: first large image in article
                if article_tag:
                    images = article_tag.find_all("img")
                    for img in images:
                        src = img.get("src") or img.get("data-src", "")
                        if src:
                            # Skip small images, logos, icons
                            if any(skip in src.lower() for skip in ["logo", "icon", "avatar", "ad"]):
                                continue
                            # Make absolute URL
                            image_url = urljoin(url, src)
                            break

            return {
                "title": title,
                "body_html": body_html,
                "summary": summary,
                "category": category,
                "image_url": image_url,
            }
        except Exception as e:
            self.logger.error(
                f"Error extracting article content: {e}",
                extra={"article_url": url},
                exc_info=True
            )
            return None

    async def _download_image(self, image_url: str) -> Optional[bytes]:
        """
        Download image from URL with rate limiting.

        Args:
            image_url: Image URL

        Returns:
            Image content as bytes, or None if download failed
        """
        if not image_url:
            return None

        self.logger.debug(f"Downloading image: {image_url}")
        
        # Use rate-limited fetch method
        content = await self._fetch_with_retry(image_url, request_type="image")
        if content is None:
            return None
        
        # Validate image content
        if len(content) < 4:
            return None
        
        # Check magic bytes for image formats
        if len(content) >= 12:
            # WebP: RIFF...WEBP
            if content[:4] == b'RIFF' and content[8:12] == b'WEBP':
                return content
        
        magic_bytes = content[:4]
        if (
            magic_bytes.startswith(b'\xff\xd8') or  # JPEG
            magic_bytes.startswith(b'\x89PNG') or   # PNG
            magic_bytes.startswith(b'GIF8') or      # GIF
            magic_bytes.startswith(b'GIF9')         # GIF
        ):
            return content
        
        return None

    async def _upload_image_to_s3(
        self, image_data: bytes, source: str, url: str
    ) -> Optional[str]:
        """
        Upload image to S3.

        Args:
            image_data: Image data as bytes
            source: News source name
            url: Article URL (to determine path)

        Returns:
            S3 path if successful, None otherwise
        """
        try:
            # Generate S3 path: news-images/{source}/{yyyy}/{mm}/{dd}/{filename}
            now = datetime.utcnow()
            
            # Generate safe filename using hash of URL to avoid special characters
            url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
            
            # Try to get extension from image data or URL
            extension = ".jpg"  # default
            if image_data[:2] == b"\xff\xd8":
                extension = ".jpg"
            elif image_data[:4] == b"\x89PNG":
                extension = ".png"
            elif image_data[:4] in [b"GIF8", b"GIF9"]:
                extension = ".gif"
            elif image_data[:4] == b"RIFF" and b"WEBP" in image_data[:12]:
                extension = ".webp"
            else:
                # Try to get extension from URL
                parsed_url = urlparse(url)
                url_path = parsed_url.path.lower()
                if url_path.endswith((".jpg", ".jpeg")):
                    extension = ".jpg"
                elif url_path.endswith(".png"):
                    extension = ".png"
                elif url_path.endswith(".gif"):
                    extension = ".gif"
                elif url_path.endswith(".webp"):
                    extension = ".webp"
            
            filename = f"{url_hash}{extension}"
            s3_path = (
                f"news-images/{source}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{filename}"
            )

            s3_session = get_s3_session()
            endpoint_uses_https = settings.s3_endpoint.startswith("https://")
            
            from botocore.config import Config
            boto_config = Config(
                connect_timeout=60,
                read_timeout=60,
                retries={'max_attempts': 3}
            )
            
            client_kwargs = {
                "endpoint_url": settings.s3_endpoint,
                "aws_access_key_id": settings.s3_access_key,
                "aws_secret_access_key": settings.s3_secret_key,
                "region_name": settings.s3_region,
                "use_ssl": settings.s3_use_ssl,
                "config": boto_config,
            }
            if endpoint_uses_https:
                client_kwargs["verify"] = settings.s3_verify_ssl

            async with s3_session.client("s3", **client_kwargs) as s3_client:
                await s3_client.upload_fileobj(
                    BytesIO(image_data),
                    settings.s3_bucket,
                    s3_path,
                    ExtraArgs={"ContentType": "image/jpeg"}
                )

            # Return full S3 URL or path
            s3_url = f"{settings.s3_endpoint}/{settings.s3_bucket}/{s3_path}"
            self.logger.info(f"Uploaded image to S3: {s3_url}")
            return s3_url

        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

    async def _save_article(self, rss_item: dict, article_content: dict, s3_image_url: str) -> None:
        """
        Save article to database.

        Args:
            rss_item: RSS item data
            article_content: Extracted article content
            s3_image_url: S3 URL for the image
        """
        async with AsyncSessionLocal() as db:
            try:
                # Check if URL already exists
                if await self._check_url_exists(rss_item["link"], db):
                    self.logger.debug(
                        f"Article already exists, skipping: {rss_item['link']}",
                        extra={"article_url": rss_item["link"]}
                    )
                    return

                # Get raw category
                raw_category = article_content.get("category") or rss_item.get("category", "")
                
                # Normalize category
                normalized_category, preserved_raw_category = normalize_category("mehrnews", raw_category)
                
                # Create news article
                news = News(
                    source="mehrnews",
                    title=article_content.get("title") or rss_item["title"],
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary") or rss_item.get("description", ""),
                    url=rss_item["link"],
                    published_at=rss_item.get("pubDate", ""),
                    image_url=s3_image_url,
                    category=normalized_category,  # Store normalized category
                    raw_category=preserved_raw_category,  # Store original category
                    language="fa",  # Persian language
                )

                db.add(news)
                await db.commit()

                self.logger.info(
                    f"Saved article: {news.title[:50]}...",
                    extra={"article_url": rss_item["link"]}
                )

            except Exception as e:
                await db.rollback()
                self.logger.error(
                    f"Error saving article to database: {e}",
                    extra={"article_url": rss_item["link"]},
                    exc_info=True
                )

    async def _ensure_s3_initialized(self) -> None:
        """Ensure S3 is initialized."""
        if not self._s3_initialized:
            try:
                await init_s3()
                self._s3_initialized = True
                self.logger.info("S3 storage initialized for worker")
            except Exception as e:
                self.logger.error(f"Failed to initialize S3: {e}", exc_info=True)
                raise

    async def fetch_news(self) -> None:
        """Fetch and process news from MehrNews RSS feed."""
        self.logger.info("Starting MehrNews fetch cycle")

        # Ensure S3 is initialized
        await self._ensure_s3_initialized()

        try:
            # Parse RSS feed
            rss_items = await self._parse_rss_feed()
            if not rss_items:
                self.logger.warning("No items found in RSS feed")
                return

            self.logger.info(f"Found {len(rss_items)} RSS items to process")

            # Process each item
            processed = 0
            skipped_existing = 0
            failed = 0

            for idx, rss_item in enumerate(rss_items, 1):
                # Check for cancellation
                if not self.running:
                    self.logger.info("Shutdown requested, stopping article processing")
                    break
                
                # Check if task was cancelled
                try:
                    await asyncio.sleep(0)  # Yield to allow cancellation
                except asyncio.CancelledError:
                    self.logger.info("Task cancelled, stopping article processing")
                    raise

                article_url = rss_item["link"]
                if not article_url:
                    self.logger.warning(f"RSS item {idx} has no link, skipping")
                    continue

                self.logger.info(
                    f"Processing article {idx}/{len(rss_items)}: {rss_item.get('title', 'No title')[:50]}...",
                    extra={"article_url": article_url}
                )

                try:
                    # Check if article already exists
                    async with AsyncSessionLocal() as db:
                        if await self._check_url_exists(article_url, db):
                            self.logger.debug(
                                f"Article already exists, skipping: {article_url}",
                                extra={"article_url": article_url}
                            )
                            skipped_existing += 1
                            continue

                    # Check again before long-running operations
                    if not self.running:
                        break
                    
                    # Extract article content
                    self.logger.debug(f"Extracting content from: {article_url}")
                    article_content = await self._extract_article_content(article_url)
                    if not article_content:
                        self.logger.warning(
                            f"Failed to extract content from article: {article_url}",
                            extra={"article_url": article_url}
                        )
                        failed += 1
                        continue

                    if not article_content.get("title") and not rss_item.get("title"):
                        self.logger.warning(
                            f"Article has no title, skipping: {article_url}",
                            extra={"article_url": article_url}
                        )
                        failed += 1
                        continue

                    # Download and upload image
                    s3_image_url = ""
                    image_url = article_content.get("image_url", "")
                    if image_url:
                        self.logger.debug(f"Downloading image: {image_url}")
                        image_data = await self._download_image(image_url)
                        if image_data:
                            s3_image_url = await self._upload_image_to_s3(
                                image_data, "mehrnews", article_url
                            ) or ""
                        else:
                            self.logger.debug(f"Failed to download image: {image_url}")

                    # Save article to database
                    await self._save_article(rss_item, article_content, s3_image_url)
                    processed += 1
                    self.logger.info(
                        f"Successfully processed article: {article_content.get('title', rss_item.get('title', 'Unknown'))[:50]}...",
                        extra={"article_url": article_url}
                    )

                except Exception as e:
                    self.logger.error(
                        f"Error processing article {article_url}: {e}",
                        extra={"article_url": article_url},
                        exc_info=True
                    )
                    failed += 1
                    # Continue with next article
                    continue

            self.logger.info(
                f"Completed fetch cycle: processed {processed} new articles, "
                f"skipped {skipped_existing} existing, failed {failed}"
            )

        except Exception as e:
            self.logger.error(f"Error in fetch cycle: {e}", exc_info=True)

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self.http_session and not self.http_session.closed:
            try:
                # Cancel any pending requests first
                if hasattr(self.http_session, '_connector'):
                    connector = self.http_session._connector
                    if connector:
                        # Close all connections
                        connector._close()
                
                # Close with timeout to prevent hanging
                await asyncio.wait_for(self.http_session.close(), timeout=2.0)
                self.logger.debug("HTTP session closed successfully")
            except asyncio.TimeoutError:
                self.logger.warning("HTTP session close timed out, forcing close")
            except Exception as e:
                self.logger.warning(f"Error closing HTTP session: {e}")
            finally:
                self.http_session = None
                self.logger.info("MehrNews worker cleanup complete")

