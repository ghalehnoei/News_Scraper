"""IRIB News worker implementation."""

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp
import feedparser
from selectolax.parser import HTMLParser
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

logger = setup_logging(source="iribnews")

# RSS feed URL
IRIB_RSS_URL = "https://www.iribnews.ir/fa/rss/allnews"
IRIB_BASE_URL = "https://www.iribnews.ir"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class IRIBNewsWorker(BaseWorker):
    """Worker for IRIB News RSS feed."""

    def __init__(self):
        """Initialize IRIB News worker."""
        super().__init__("iribnews")
        self.rss_url = IRIB_RSS_URL
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
                
                async with session.get(url) as response:
                    # Handle HTTP 429 (Too Many Requests)
                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                wait_time = float(retry_after)
                            except ValueError:
                                wait_time = 60.0
                            self.logger.warning(
                                f"Rate limited (429), waiting {wait_time}s before retry",
                                extra={"url": url, "retry_after": wait_time}
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            await asyncio.sleep(60)
                            continue
                    
                    if response.status == 200:
                        content = await response.read()
                        self.logger.debug(
                            f"Successfully fetched {request_type}",
                            extra={"url": url, "status": response.status, "size": len(content)}
                        )
                        return content
                    else:
                        self.logger.warning(
                            f"HTTP {response.status} for {request_type}",
                            extra={"url": url, "status": response.status, "attempt": attempt + 1}
                        )
                        
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Timeout fetching {request_type} (attempt {attempt + 1}/{max_retries})",
                    extra={"url": url, "attempt": attempt + 1}
                )
            except Exception as e:
                self.logger.warning(
                    f"Error fetching {request_type} (attempt {attempt + 1}/{max_retries}): {e}",
                    extra={"url": url, "attempt": attempt + 1, "error": str(e)}
                )
            
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                await asyncio.sleep(wait_time)
        
        self.logger.error(
            f"Failed to fetch {request_type} after {max_retries} attempts",
            extra={"url": url, "max_retries": max_retries}
        )
        return None

    async def _ensure_s3_initialized(self) -> None:
        """Ensure S3 storage is initialized."""
        if not self._s3_initialized:
            await init_s3()
            self._s3_initialized = True

    async def _parse_rss_feed(self) -> list[dict]:
        """
        Parse RSS feed and extract article links.

        Returns:
            List of article dictionaries with url, title, published_at, image_url
        """
        self.logger.info(f"Fetching RSS feed from {self.rss_url}")
        content = await self._fetch_with_retry(self.rss_url, request_type="rss")
        
        if content is None:
            self.logger.error("Failed to fetch RSS feed")
            return []
        
        try:
            feed = feedparser.parse(content)
            articles = []
            
            for entry in feed.entries:
                article_url = entry.get("link", "").strip()
                if not article_url:
                    continue
                
                # Extract published date
                published_at = ""
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        dt = datetime(*entry.published_parsed[:6])
                        published_at = dt.isoformat()
                    except Exception as e:
                        self.logger.debug(f"Error parsing date: {e}", extra={"article_url": article_url})
                
                # Fallback to pubDate string
                if not published_at and hasattr(entry, "published"):
                    published_at = entry.published
                
                # Extract image from enclosure
                image_url = ""
                if hasattr(entry, "enclosures") and entry.enclosures:
                    enclosures_count = len(entry.enclosures)
                    self.logger.debug(
                        f"Found {enclosures_count} enclosures in RSS entry",
                        extra={"article_url": article_url}
                    )
                    for idx, enclosure in enumerate(entry.enclosures):
                        if enclosure.get("type", "").startswith("image/"):
                            image_url = enclosure.get("url", "")
                            # Make absolute URL if relative
                            if image_url and not image_url.startswith(("http://", "https://")):
                                image_url = urljoin(IRIB_BASE_URL, image_url)
                            self.logger.debug(
                                f"Found image in RSS enclosure {idx+1}: {image_url[:100]}",
                                extra={"article_url": article_url, "enclosure_index": idx}
                            )
                            break
                
                articles.append({
                    "url": article_url,
                    "title": entry.get("title", "").strip(),
                    "published_at": published_at,
                    "description": entry.get("description", "").strip(),
                    "image_url": image_url,
                })
            
            self.logger.info(f"Found {len(articles)} articles in RSS feed")
            return articles
            
        except Exception as e:
            self.logger.error(f"Error parsing RSS feed: {e}", exc_info=True)
            return []

    async def _extract_article_content(self, url: str) -> Optional[dict]:
        """
        Extract article content from URL.

        Args:
            url: Article URL

        Returns:
            Dictionary with title, body_html, summary, category, published_at, image_url
        """
        content = await self._fetch_with_retry(url, request_type="article")
        if content is None:
            return None

        try:
            html_content = content.decode('utf-8')
        except UnicodeDecodeError:
            html_content = content.decode('utf-8', errors='ignore')

        try:
            tree = HTMLParser(html_content)

            # Extract title
            title = ""
            title_tag = tree.css_first('h1')
            if title_tag and title_tag.text():
                title = title_tag.text(strip=True)
            if not title:
                title_elem = tree.css_first('title')
                if title_elem and title_elem.text():
                    title = title_elem.text(strip=True)

            # Extract article body
            body_html = ""
            article_elem = tree.css_first('article')
            if not article_elem:
                article_elem = tree.css_first('div[class*="article"], div[class*="content"], div[class*="news-body"], div[class*="body"]')
            
            if article_elem:
                for tag in article_elem.css('script, style, iframe'):
                    tag.decompose()
                for tag in article_elem.css('[class*="ad"], [class*="advertisement"], [class*="social"], [class*="share"]'):
                    tag.decompose()
                
                # Remove all images from body HTML (only first image is kept as main image)
                images_in_body = article_elem.css('img')
                images_count = len(images_in_body)
                self.logger.info(
                    f"Found {images_count} images in article body before removal",
                    extra={"article_url": url, "images_count": images_count}
                )
                
                # Log image URLs before removal
                if images_count > 0:
                    image_urls = []
                    for img in images_in_body:
                        src = img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy-src") or img.attributes.get("data-original", "")
                        if src:
                            image_urls.append(src[:100])  # First 100 chars
                    self.logger.debug(
                        f"Image URLs in body: {image_urls[:5]}",  # First 5 URLs
                        extra={"article_url": url, "total_images": images_count}
                    )
                
                for img in images_in_body:
                    img.decompose()
                
                # Verify images are removed
                remaining_images = article_elem.css('img')
                remaining_count = len(remaining_images)
                if remaining_count > 0:
                    self.logger.warning(
                        f"WARNING: {remaining_count} images still remain in body after removal attempt",
                        extra={"article_url": url}
                    )
                else:
                    self.logger.debug(
                        f"Successfully removed all {images_count} images from body",
                        extra={"article_url": url}
                    )
                
                body_html = article_elem.html
            else:
                self.logger.warning(f"Could not find article body content", extra={"article_url": url})

            # Extract summary from meta description
            summary = ""
            meta_desc = tree.css_first('meta[name="description"]')
            if meta_desc and meta_desc.attributes.get("content"):
                summary = meta_desc.attributes.get("content", "")

            # Extract category - try multiple methods
            category = ""
            categories = []
            
            # Method 1: breadcrumb navigation (most reliable for full category path)
            # Try different breadcrumb selectors
            breadcrumb_selectors = [
                'nav[class*="breadcrumb"]',
                'div[class*="breadcrumb"]',
                'ul[class*="breadcrumb"]',
                'ol[class*="breadcrumb"]',
                '[class*="breadcrumb"]',
                'nav[aria-label*="breadcrumb"]',
                '[role="navigation"]',
            ]
            
            for selector in breadcrumb_selectors:
                breadcrumb = tree.css_first(selector)
                if breadcrumb:
                    # Get all links in breadcrumb
                    cat_links = breadcrumb.css('a')
                    if cat_links:
                        for link in cat_links:
                            cat_text = link.text(strip=True) if link.text() else ""
                            # Skip home/main page links
                            if cat_text and cat_text not in ["خانه", "صفحه اصلی", "خانه >", "Home", "صفحه نخست", "اخبار"]:
                                # Also check href to skip home links
                                href = link.attributes.get("href", "")
                                if href and not any(skip in href.lower() for skip in ["/fa", "/", "index", "home"]):
                                    if cat_text not in categories:
                                        categories.append(cat_text)
                        if categories:
                            break
            
            # Method 2: Look for category links in navigation or header
            if not categories:
                # Try to find category links in various locations
                nav_elements = tree.css('nav a, header a, [class*="nav"] a, [class*="menu"] a')
                for nav_link in nav_elements:
                    href = nav_link.attributes.get("href", "")
                    cat_text = nav_link.text(strip=True) if nav_link.text() else ""
                    # Check if it's a category link (contains /category/ or /news/ with category pattern)
                    if href and cat_text:
                        if any(pattern in href.lower() for pattern in ["/category/", "/cat/", "/news/", "/fa/news/"]):
                            # Skip if it's the current article or home
                            if cat_text not in ["خانه", "صفحه اصلی", "اخبار"] and cat_text not in categories:
                                categories.append(cat_text)
                                # Limit to avoid too many categories
                                if len(categories) >= 3:
                                    break
            
            # Method 3: meta tags (fallback, usually only has first category)
            if not categories:
                meta_cat = tree.css_first('meta[property="article:section"]')
                if meta_cat and meta_cat.attributes.get("content"):
                    cat_text = meta_cat.attributes.get("content", "").strip()
                    if cat_text:
                        categories.append(cat_text)
            
            # Method 4: Look for category in article header or metadata
            if not categories:
                # Try to find category in article header area
                header_elem = tree.css_first('header, [class*="header"], [class*="article-header"], [class*="news-header"]')
                if header_elem:
                    cat_links = header_elem.css('a[href*="/category/"], a[href*="/cat/"], a[href*="/news/"]')
                    for link in cat_links:
                        cat_text = link.text(strip=True) if link.text() else ""
                        if cat_text and cat_text not in ["خانه", "صفحه اصلی", "اخبار"]:
                            if cat_text not in categories:
                                categories.append(cat_text)
                                if len(categories) >= 3:
                                    break
            
            # Join categories with >
            if categories:
                category = " > ".join(categories)
                self.logger.debug(f"Found category: {category}", extra={"article_url": url})
            else:
                self.logger.warning(f"Could not extract category", extra={"article_url": url})

            # Extract published date
            published_at = ""
            # Method 1: meta tags
            meta_date = tree.css_first('meta[property="article:published_time"]')
            if meta_date and meta_date.attributes.get("content"):
                published_at = meta_date.attributes.get("content", "")
            
            # Method 2: time tag
            if not published_at:
                time_tag = tree.css_first('time[datetime]')
                if time_tag and time_tag.attributes.get("datetime"):
                    published_at = time_tag.attributes.get("datetime", "")

            # Extract main image - ONLY the first image
            image_url = ""
            # Method 1: og:image meta tag
            og_image = tree.css_first('meta[property="og:image"]')
            if og_image and og_image.attributes.get("content"):
                image_url = og_image.attributes.get("content", "")
                if image_url:
                    image_url = urljoin(url, image_url)
                    self.logger.info(
                        f"Found main image from og:image meta tag: {image_url[:100]}",
                        extra={"article_url": url, "source": "og:image"}
                    )
            
            # Method 2: First image in article body (only first one)
            if not image_url and article_elem:
                images = article_elem.css('img')
                images_count = len(images)
                self.logger.debug(
                    f"Found {images_count} images in article_elem for main image selection",
                    extra={"article_url": url}
                )
                # Only take the first image as main image
                if images:
                    first_img = images[0]
                    src = first_img.attributes.get("src") or first_img.attributes.get("data-src") or first_img.attributes.get("data-lazy-src") or first_img.attributes.get("data-original", "")
                    if src:
                        if not any(skip in src.lower() for skip in ["logo", "icon", "avatar", "ad", "placeholder"]):
                            image_url = urljoin(url, src)
                            self.logger.info(
                                f"Found main image from first image in body: {image_url[:100]}",
                                extra={"article_url": url, "source": "body_first_image"}
                            )
                        else:
                            self.logger.debug(
                                f"Skipped first image (looks like logo/icon/ad): {src[:100]}",
                                extra={"article_url": url}
                            )
            
            if not image_url:
                self.logger.warning(
                    f"No main image found for article",
                    extra={"article_url": url}
                )

            return {
                "title": title,
                "body_html": body_html,
                "summary": summary,
                "category": category,
                "published_at": published_at,
                "image_url": image_url,
            }
        except Exception as e:
            self.logger.error(f"Error extracting article content: {e}", extra={"article_url": url}, exc_info=True)
            return None

    async def _upload_image_to_s3(self, image_data: bytes, source: str, url: str) -> Optional[str]:
        """
        Upload image to S3 and return S3 key (path only, not s3://bucket/path).
        
        Args:
            image_data: Image data as bytes
            source: News source name
            url: Original image URL for context
            
        Returns:
            S3 key (path) if successful, None otherwise
            Example: "news-images/iribnews/2025/12/30/abc123.jpg"
        """
        self.logger.info(
            f"_upload_image_to_s3 called for URL: {url[:100]}",
            extra={"image_url": url, "image_size": len(image_data), "source": source}
        )
        await self._ensure_s3_initialized()
        try:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            timestamp = datetime.now().strftime("%Y/%m/%d")
            filename = f"{url_hash}.jpg"
            s3_path = f"news-images/{source}/{timestamp}/{filename}"

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
                "use_ssl": endpoint_uses_https,
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

            self.logger.info(f"Uploaded image to S3: {s3_path}")
            # Return only the S3 key (path), not s3://bucket/path
            return s3_path
        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

    async def _article_exists(self, url: str) -> bool:
        """
        Check if article already exists in database.
        
        Args:
            url: Article URL
            
        Returns:
            True if article exists, False otherwise
        """
        async with AsyncSessionLocal() as db:
            try:
                result = await db.execute(
                    select(News).where(News.url == url)
                )
                existing = result.scalar_one_or_none()
                return existing is not None
            except Exception as e:
                self.logger.error(f"Error checking if article exists: {e}", extra={"article_url": url}, exc_info=True)
                return False

    async def _save_news(self, article_data: dict) -> bool:
        """
        Save news article to database.

        Args:
            article_data: Dictionary with article data

        Returns:
            True if saved, False if already exists or error
        """
        async with AsyncSessionLocal() as db:
            try:
                # Check if article already exists
                result = await db.execute(
                    select(News).where(News.url == article_data["url"])
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    self.logger.info(
                        f"Article already exists in database (double-check): {article_data['url']}",
                        extra={"article_url": article_data["url"]}
                    )
                    return False
                
                # Normalize category
                normalized_category, raw_category = normalize_category(
                    self.source_name,
                    article_data.get("category")
                )
                
                # Create news record
                news = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=article_data.get("body_html", ""),
                    summary=article_data.get("summary", ""),
                    url=article_data["url"],
                    published_at=article_data.get("published_at", ""),
                    image_url=article_data.get("image_url", ""),  # This should be S3 key only
                    category=normalized_category,
                    raw_category=raw_category,
                )
                
                db.add(news)
                await db.commit()
                
                self.logger.info(
                    f"Saved article: {article_data['title'][:50]}...",
                    extra={
                        "article_url": article_data["url"],
                        "source": self.source_name,
                        "category": normalized_category,
                    }
                )
                return True
                
            except Exception as e:
                await db.rollback()
                self.logger.error(
                    f"Error saving article: {e}",
                    extra={"article_url": article_data.get("url"), "error": str(e)},
                    exc_info=True
                )
                return False

    async def fetch_news(self) -> None:
        """Fetch news from IRIB News RSS feed."""
        self.logger.info(f"Starting to fetch news from {self.source_name}")
        
        # Parse RSS feed
        articles = await self._parse_rss_feed()
        
        if not articles:
            self.logger.warning("No articles found in RSS feed")
            return
        
        self.logger.info(
            f"Found {len(articles)} articles in RSS feed, processing each one...",
            extra={"total_articles": len(articles)}
        )
        
        # Process each article
        saved_count = 0
        skipped_count = 0
        for idx, article_info in enumerate(articles, 1):
            article_url = article_info.get("url", "")
            self.logger.info(
                f"Processing article {idx}/{len(articles)}: {article_info.get('title', '')[:50]}...",
                extra={
                    "article_index": idx,
                    "total_articles": len(articles),
                    "article_url": article_url
                }
            )
            
            # Check if article already exists BEFORE extracting content and downloading images
            if await self._article_exists(article_url):
                self.logger.info(
                    f"Article already exists, skipping: {article_url}",
                    extra={"article_url": article_url}
                )
                skipped_count += 1
                continue
            
            try:
                # Extract full article content (only if article doesn't exist)
                article_data = await self._extract_article_content(article_url)
                if not article_data:
                    self.logger.warning(f"Failed to extract content from {article_url}")
                    continue
                
                # Merge RSS data with extracted data
                article_data["url"] = article_info["url"]
                if not article_data.get("title"):
                    article_data["title"] = article_info.get("title", "")
                if not article_data.get("published_at"):
                    article_data["published_at"] = article_info.get("published_at", "")
                if not article_data.get("summary"):
                    article_data["summary"] = article_info.get("description", "")
                
                # Use RSS image if available and extracted image is not
                if article_info.get("image_url") and not article_data.get("image_url"):
                    article_data["image_url"] = article_info.get("image_url", "")
                
                # Note: Images are already removed from body_html in _extract_article_content
                # No need to process body images separately
                
                # Download and upload main image if exists
                if article_data.get("image_url"):
                    main_image_url = article_data["image_url"]
                    self.logger.info(
                        f"Processing main image for article: {main_image_url[:100]}",
                        extra={"article_url": article_data["url"], "main_image_url": main_image_url}
                    )
                    
                    # Make sure image_url is absolute
                    if main_image_url and not main_image_url.startswith(("http://", "https://")):
                        main_image_url = urljoin(IRIB_BASE_URL, main_image_url)
                        article_data["image_url"] = main_image_url
                    
                    self.logger.info(
                        f"Downloading main image: {main_image_url[:100]}",
                        extra={"article_url": article_data["url"]}
                    )
                    
                    image_data = await self._fetch_with_retry(
                        main_image_url,
                        request_type="image"
                    )
                    if image_data:
                        self.logger.info(
                            f"Downloaded main image ({len(image_data)} bytes), uploading to S3...",
                            extra={"article_url": article_data["url"], "image_size": len(image_data)}
                        )
                        s3_path = await self._upload_image_to_s3(
                            image_data,
                            self.source_name,
                            main_image_url
                        )
                        if s3_path:
                            # Store only the S3 key (path), not s3://bucket/path
                            # API routes will use this to generate presigned URLs
                            article_data["image_url"] = s3_path
                            self.logger.info(
                                f"Successfully uploaded main image to S3: {s3_path}",
                                extra={"article_url": article_data["url"], "s3_path": s3_path}
                            )
                        else:
                            self.logger.warning(
                                f"Failed to upload main image to S3",
                                extra={"article_url": article_data["url"]}
                            )
                    else:
                        self.logger.warning(
                            f"Failed to download main image",
                            extra={"article_url": article_data["url"], "image_url": main_image_url}
                        )
                else:
                    self.logger.debug(
                        f"No main image URL for article",
                        extra={"article_url": article_data["url"]}
                    )
                
                # Save to database
                if await self._save_news(article_data):
                    saved_count += 1
                
                # Small delay between articles
                await asyncio.sleep(0.5)
                
            except Exception as e:
                self.logger.error(
                    f"Error processing article {article_info.get('url', 'unknown')}: {e}",
                    exc_info=True
                )
                continue
        
        self.logger.info(
            f"Finished fetching news from {self.source_name}: {saved_count} new articles saved, {skipped_count} articles skipped (already exist)"
        )

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            self.logger.info("HTTP session closed")
