"""IRNA worker implementation."""

import asyncio
import hashlib
import json
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

logger = setup_logging(source="irna")

# RSS feed URL
IRNA_RSS_URL = "https://www.irna.ir/rss"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class IRNAWorker(BaseWorker):
    """Worker for IRNA RSS feed."""

    def __init__(self):
        """Initialize IRNA worker."""
        super().__init__("irna")
        self.rss_url = IRNA_RSS_URL
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._s3_initialized = False
        self._playwright = None
        self._browser = None
        self._browser_context = None
        
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
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Cache-Control": "max-age=0",
                }
            )
        return self.http_session

    async def _get_browser_context(self):
        """Get or create a Playwright browser context for JavaScript rendering."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            self.logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
            return None, None
        
        try:
            if self._playwright is None:
                self.logger.info("Initializing Playwright browser for IRNA...")
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                )
                self._browser_context = await self._browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080},
                    locale='fa-IR',
                    timezone_id='Asia/Tehran'
                )
                self.logger.info("Playwright browser initialized successfully for IRNA")
            
            return self._browser_context, self._playwright
        except Exception as e:
            self.logger.error(f"Failed to initialize Playwright browser: {e}. Make sure Playwright is installed: pip install playwright && playwright install chromium")
            return None, None

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
            request_type: Type of request (rss, article, image) for logging

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
            image_url = ""
            # Try common RSS media fields
            try:
                media_content = entry.get("media_content") or entry.get("media:content")
                if media_content and isinstance(media_content, list) and media_content[0].get("url"):
                    image_url = media_content[0]["url"]
            except Exception:
                pass
            if not image_url:
                try:
                    media_thumbnail = entry.get("media_thumbnail") or entry.get("media:thumbnail")
                    if media_thumbnail and isinstance(media_thumbnail, list) and media_thumbnail[0].get("url"):
                        image_url = media_thumbnail[0]["url"]
                except Exception:
                    pass
            if not image_url:
                try:
                    enclosures = entry.get("enclosures") or []
                    if enclosures and isinstance(enclosures, list):
                        href = enclosures[0].get("href") or enclosures[0].get("url")
                        if href:
                            image_url = href
                except Exception:
                    pass

            item = {
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "description": entry.get("description", ""),
                "pubDate": None,
                "category": entry.get("category", "") if entry.get("category") else "",
                "image_url": image_url,
            }
            
            # Parse publication date
            if "published_parsed" in entry and entry.published_parsed:
                try:
                    item["pubDate"] = datetime(*entry.published_parsed[:6])
                except Exception as e:
                    self.logger.debug(f"Could not parse published_parsed: {e}")
            
            # Fallback to published or updated fields
            if not item["pubDate"]:
                for date_field in ["published", "updated", "pubDate"]:
                    if entry.get(date_field):
                        try:
                            item["pubDate"] = datetime.strptime(
                                entry[date_field], "%a, %d %b %Y %H:%M:%S %Z"
                            )
                            break
                        except Exception:
                            # Try alternative formats
                            try:
                                item["pubDate"] = datetime.fromisoformat(entry[date_field])
                                break
                            except Exception:
                                continue
            
            items.append(item)
        
        return items

    async def _check_url_exists(self, url: str, db: AsyncSession) -> bool:
        """
        Check if a news article with the given URL already exists.

        Args:
            url: Article URL
            db: Database session

        Returns:
            True if article exists, False otherwise
        """
        result = await db.execute(select(News.id).where(News.url == url))
        return result.scalar_one_or_none() is not None

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

    async def _extract_article_content(self, url: str) -> Optional[dict]:
        """
        Extract article content from HTML page using headless browser for CDN redirects.

        Args:
            url: Article URL

        Returns:
            Dictionary with article content, or None if extraction failed
        """
        page = None
        html_content = None
        
        # Try using headless browser first (for CDN challenges)
        browser_context, playwright = await self._get_browser_context()
        if browser_context:
            try:
                page = await browser_context.new_page()
                self.logger.info(f"Using headless browser to fetch IRNA article", extra={"article_url": url})
                await page.goto(url, wait_until='networkidle', timeout=30000)
                # Wait for content to load (JavaScript rendering and CDN redirects)
                await page.wait_for_timeout(3000)  # Wait 3 seconds for JS to render
                # Try to wait for article content to appear
                try:
                    await page.wait_for_selector('article, .content, .news-body, [class*="content"], p', timeout=5000)
                except:
                    pass  # Continue even if selector not found
                html_content = await page.content()
                await page.close()
                page = None
                self.logger.info(f"Successfully fetched IRNA page with headless browser, HTML length: {len(html_content)}", extra={"article_url": url})
            except Exception as browser_error:
                if page:
                    try:
                        await page.close()
                    except:
                        pass
                    page = None
                self.logger.warning(f"Headless browser failed: {browser_error}", extra={"article_url": url})
                # Fall back to HTTP request
                content = await self._fetch_with_retry(url, request_type="article")
                if content:
                    html_content = content.decode("utf-8", errors="ignore") if isinstance(content, bytes) else content
        else:
            # Fallback to regular HTTP request if browser not available
            self.logger.warning(f"Browser not available, using HTTP request", extra={"article_url": url})
            content = await self._fetch_with_retry(url, request_type="article")
            if content:
                html_content = content.decode("utf-8", errors="ignore") if isinstance(content, bytes) else content

        if not html_content:
            self.logger.error("Failed to fetch article page", extra={"article_url": url})
            return None

        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Detect IRNA interstitial / not-found pages
            page_text = soup.get_text(" ", strip=True)
            if (
                "در ﺣﺎل اﻧﺘﻘﺎل" in page_text
                or "Transferring to the website" in page_text
                or "صفحهٔ درخواستی شما یافت نشد" in page_text
                or "چنین صفحه‌ای موجود نیست" in page_text
            ):
                self.logger.warning(
                    "IRNA returned an interstitial/not-found HTML page; skipping extraction",
                    extra={"article_url": url},
                )
                return None

            # Extract title
            title = ""
            title_tag = soup.find("h1") or soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)

            # Extract article body
            body_html = ""
            article_selectors = [
                "article",
                ".item-text",
                ".news-text",
                ".content",
                ".article-body",
                ".news-body",
                "[itemprop='articleBody']",
            ]
            
            article_tag = None
            for selector in article_selectors:
                article_tag = soup.select_one(selector)
                if article_tag:
                    self.logger.debug(f"Found article body with selector: {selector}", extra={"article_url": url})
                    break
            
            if article_tag:
                # Remove unwanted elements
                for tag in article_tag.find_all(["script", "style", "iframe"]):
                    tag.decompose()
                for tag in article_tag.find_all(class_=lambda x: x and any(skip in x.lower() for skip in ["ad", "advertisement", "social", "share", "item-nav"])):
                    tag.decompose()
                
                # Remove share buttons, short links and navigation sections
                for div in list(article_tag.find_all("div")):
                    if div is None or not div.name:  # Skip if already decomposed
                        continue
                    div_class = " ".join(div.get("class", [])).lower() if div.get("class") else ""
                    # Remove divs with specific share/link/nav related classes
                    if any(keyword in div_class for keyword in ["short-link", "share", "social", "item-nav", "news-nav"]):
                        div.decompose()
                        continue
                
                # Remove all <i> tags with share icon classes
                for i_tag in list(article_tag.find_all("i")):
                    if i_tag is None or not i_tag.name:
                        continue
                    i_class = " ".join(i_tag.get("class", [])).lower() if i_tag.get("class") else ""
                    if any(icon in i_class for icon in ["icon-twitter", "icon-telegram", "icon-facebook", "icon-linkedin", "icon-link"]):
                        # Remove parent li and ul if they become empty
                        parent = i_tag.parent
                        i_tag.decompose()
                        if parent and parent.name == "a":
                            parent_li = parent.parent
                            parent.decompose()
                            if parent_li and parent_li.name == "li":
                                parent_ul = parent_li.parent
                                parent_li.decompose()
                                # Check if ul is now empty
                                if parent_ul and parent_ul.name == "ul" and not parent_ul.get_text(strip=True):
                                    parent_ul.decompose()
                
                # Remove "related news" sections (usually at the end)
                # IRNA specific: look for sections with "اخبار مرتبط", "خبرهای مرتبط", "مطالب مرتبط"
                for related_section in article_tag.find_all(class_=lambda x: x and any(word in str(x).lower() for word in ["related", "مرتبط", "پیشنهادی"])):
                    related_section.decompose()
                
                # Also remove sections with headers containing "اخبار مرتبط" or similar
                for heading in article_tag.find_all(["h2", "h3", "h4", "div"]):
                    heading_text = heading.get_text(strip=True)
                    if heading_text and any(keyword in heading_text for keyword in ["اخبار مرتبط", "خبرهای مرتبط", "مطالب مرتبط", "بیشتر بخوانید"]):
                        # Remove this heading and all following siblings
                        for sibling in list(heading.find_next_siblings()):
                            sibling.decompose()
                        heading.decompose()
                        break
                
                # Keep the full article content including images
                body_html = str(article_tag)

            # Extract summary
            summary = ""
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content"):
                summary = meta_desc["content"]
            elif body_html:
                first_p = soup.find("p")
                if first_p:
                    summary = first_p.get_text(strip=True)[:500]

            # Extract category
            category = ""
            category_tag = soup.find("meta", attrs={"property": "article:section"})
            if category_tag and category_tag.get("content"):
                category = category_tag["content"]

            # Extract main image
            image_url = ""
            og_image = soup.find("meta", attrs={"property": "og:image"})
            if og_image and og_image.get("content"):
                image_url = og_image["content"]
            
            if not image_url:
                header_image_selectors = [
                    "img.news-image",
                    "img.lead-image",
                    "figure img",
                    ".article-header img",
                ]
                for selector in header_image_selectors:
                    header_image = soup.select_one(selector)
                    if header_image:
                        src = header_image.get("src") or header_image.get("data-src")
                        if src and not any(skip in src.lower() for skip in ["logo", "icon", "avatar", "ad"]):
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
                normalized_category, preserved_raw_category = normalize_category("irna", raw_category)

                # Ensure published_at is stored as string
                published_at_val = rss_item.get("pubDate", "")
                if isinstance(published_at_val, datetime):
                    published_at_val = published_at_val.isoformat()
                
                # Create news article
                news = News(
                    source="irna",
                    title=article_content.get("title") or rss_item["title"],
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary") or rss_item.get("description", ""),
                    url=rss_item["link"],
                    published_at=published_at_val,
                    image_url=s3_image_url,
                    category=normalized_category,  # Store normalized category
                    raw_category=preserved_raw_category,  # Store original category
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
        """Fetch and process news from IRNA RSS feed."""
        self.logger.info("Starting IRNA fetch cycle")

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
                if not self.running:
                    break

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

                    # Extract article content
                    self.logger.debug(f"Extracting content from: {article_url}")
                    article_content = await self._extract_article_content(article_url)
                    
                    # Fallback to RSS data if HTML extraction failed
                    if not article_content:
                        self.logger.info(
                            f"HTML extraction failed, using RSS data for: {article_url}",
                            extra={"article_url": article_url}
                        )
                        # Build article_content from RSS
                        article_content = {
                            "title": rss_item.get("title", ""),
                            "body_html": f"<p>{rss_item.get('description', '')}</p>",
                            "summary": rss_item.get("description", "")[:500],
                            "category": rss_item.get("category", ""),
                            "image_url": rss_item.get("image_url", ""),
                        }

                    if not article_content.get("title") and not rss_item.get("title"):
                        self.logger.warning(
                            f"Article has no title, skipping: {article_url}",
                            extra={"article_url": article_url}
                        )
                        failed += 1
                        continue

                    # Download and upload image
                    s3_image_url = ""
                    image_url = article_content.get("image_url", "") or rss_item.get("image_url", "")
                    if image_url:
                        self.logger.debug(f"Downloading image: {image_url}")
                        image_data = await self._download_image(image_url)
                        if image_data:
                            s3_image_url = await self._upload_image_to_s3(
                                image_data, "irna", article_url
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
        """Cleanup resources on shutdown."""
        # Close Playwright browser
        if self._browser_context:
            try:
                await self._browser_context.close()
            except Exception as e:
                self.logger.warning(f"Error closing browser context: {e}")
            self._browser_context = None
        if self._browser:
            try:
                await self._browser.close()
            except Exception as e:
                self.logger.warning(f"Error closing browser: {e}")
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping Playwright: {e}")
            self._playwright = None
        
        # Close HTTP session
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        self.logger.info("IRNA worker shutdown complete")

