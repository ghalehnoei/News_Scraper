"""Eghtesad Online worker implementation."""

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

logger = setup_logging(source="eghtesadonline")

# RSS feed URL
EGHTESAD_RSS_URL = "https://www.eghtesadonline.com/fa/updates/allnews"
EGHTESAD_BASE_URL = "https://www.eghtesadonline.com"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class EghtesadOnlineWorker(BaseWorker):
    """Worker for Eghtesad Online RSS feed."""

    def __init__(self):
        """Initialize Eghtesad Online worker."""
        super().__init__("eghtesadonline")
        self.rss_url = EGHTESAD_RSS_URL
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
        """Fetch URL with retries and rate limiting."""
        session = await self._get_http_session()
        for attempt in range(max_retries):
            try:
                await self.rate_limiter.acquire(
                    source=self.source_name,
                    request_type=request_type
                )
                
                async with session.get(url) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning(
                            f"Rate limited (429), waiting {retry_after}s",
                            extra={"url": url, "attempt": attempt + 1}
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    
                    response.raise_for_status()
                    return await response.read()
                    
            except asyncio.TimeoutError:
                logger.warning(
                    f"Timeout on attempt {attempt + 1}/{max_retries}",
                    extra={"url": url}
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
            except aiohttp.ClientError as e:
                logger.warning(
                    f"HTTP error on attempt {attempt + 1}/{max_retries}: {e}",
                    extra={"url": url}
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching URL: {e}",
                    extra={"url": url},
                    exc_info=True
                )
                break
                
        return None

    async def _upload_image_to_s3(self, image_url: str) -> Optional[str]:
        """Download image and upload to S3."""
        if not self._s3_initialized:
            await init_s3()
            self._s3_initialized = True

        try:
            image_data = await self._fetch_with_retry(image_url, request_type="image")
            if not image_data:
                logger.warning(f"Failed to download image: {image_url}")
                return None

            image_hash = hashlib.md5(image_data).hexdigest()[:8]
            parsed_url = urlparse(image_url)
            extension = parsed_url.path.split(".")[-1].lower()
            if extension not in ["jpg", "jpeg", "png", "gif", "webp"]:
                extension = "jpg"
            
            timestamp = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"news-images/{self.source_name}/{timestamp}/{image_hash}.{extension}"

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
                    s3_key,
                    ExtraArgs={"ContentType": f"image/{extension}"}
                )

            logger.debug(f"Uploaded image to S3: {s3_key}")
            return s3_key

        except Exception as e:
            logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

    async def _article_exists(self, url: str) -> bool:
        """Check if article already exists in database."""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(News).where(News.url == url)
                )
                exists = result.scalar_one_or_none() is not None
                return exists
        except Exception as e:
            logger.error(f"Error checking article existence: {e}", exc_info=True)
            return False

    async def _extract_article_content(self, url: str) -> Optional[dict]:
        """Extract article content from Eghtesad Online article page."""
        try:
            html_content = await self._fetch_with_retry(url, request_type="article")
            if not html_content:
                logger.warning(f"Failed to fetch article content from {url}")
                return None

            tree = HTMLParser(html_content.decode("utf-8", errors="ignore"))

            # Extract title
            title = ""
            title_elem = tree.css_first('h1.title, h1, .news-title')
            if title_elem:
                title = title_elem.text(strip=True)
            
            if not title:
                og_title = tree.css_first('meta[property="og:title"]')
                if og_title:
                    title = og_title.attributes.get("content", "")

            # Extract published date and time
            published_at = ""
            
            # First try .dateInfo which has both date and time in clean format
            dateinfo_elem = tree.css_first('.dateInfo')
            if dateinfo_elem:
                published_at = dateinfo_elem.text(strip=True)
            else:
                # Try to get date and clock separately
                date_elem = tree.css_first('.date')
                clock_elem = tree.css_first('.time .clock, .clock')
                
                if date_elem and clock_elem:
                    # Combine date and clock time
                    date_text = date_elem.text(strip=True)
                    clock_text = clock_elem.text(strip=True)
                    published_at = f"{date_text} {clock_text}"
                elif date_elem:
                    published_at = date_elem.text(strip=True)
                elif clock_elem:
                    published_at = clock_elem.text(strip=True)
            
            # Fallback to other methods if not found
            if not published_at:
                datetime_elem = tree.css_first('time[datetime]')
                if datetime_elem:
                    published_at = datetime_elem.attributes.get("datetime", "")
            
            if not published_at:
                meta_time = tree.css_first('meta[property="article:published_time"]')
                if meta_time:
                    published_at = meta_time.attributes.get("content", "")

            # Extract category
            category = ""
            breadcrumb = tree.css('.breadcrumb a, .category a')
            if breadcrumb and len(breadcrumb) > 1:
                category = breadcrumb[-1].text(strip=True)
            
            if not category:
                category_meta = tree.css_first('meta[property="article:section"]')
                if category_meta:
                    category = category_meta.attributes.get("content", "")

            # Extract summary
            summary = ""
            summary_elem = tree.css_first('.lead, .summary, .news-summary, meta[property="og:description"], meta[name="description"]')
            if summary_elem:
                if summary_elem.tag == "meta":
                    summary = summary_elem.attributes.get("content", "")
                else:
                    summary = summary_elem.text(strip=True)

            # Extract article body
            body_html = ""
            article_elem = tree.css_first('.body, .news-body, .content, article')
            if article_elem:
                for unwanted in article_elem.css('script, style, iframe, .ad, .advertisement, .related-news, .tags, .share'):
                    unwanted.decompose()
                
                # Remove images from body (only first image will be kept as main image)
                images_in_body = article_elem.css('img')
                logger.debug(f"Found {len(images_in_body)} images in body before removal", extra={"article_url": url})
                for img in images_in_body:
                    img.decompose()
                
                body_html = article_elem.html
            else:
                logger.warning(f"Could not find article body element", extra={"article_url": url})

            # Extract main image
            image_url = ""
            og_image = tree.css_first('meta[property="og:image"]')
            if og_image and og_image.attributes.get("content"):
                image_url = og_image.attributes.get("content", "")
                if image_url:
                    image_url = urljoin(url, image_url)
            
            if not image_url:
                images = tree.css('img')
                skip_patterns = ["/logo", "/icon", "/avatar", "/placeholder"]
                for img in images:
                    src = img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy-src", "")
                    if src:
                        src_lower = src.lower()
                        if any(pattern in src_lower for pattern in skip_patterns):
                            continue
                        image_url = urljoin(url, src)
                        break

            return {
                "title": title,
                "body_html": body_html,
                "summary": summary,
                "category": category,
                "published_at": published_at,
                "image_url": image_url,
            }

        except Exception as e:
            logger.error(f"Error extracting article content from {url}: {e}", exc_info=True)
            return None

    async def _save_article(self, article_data: dict) -> bool:
        """Save article to database."""
        async with AsyncSessionLocal() as db:
            try:
                result = await db.execute(
                    select(News).where(News.url == article_data["url"])
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    self.logger.info(
                        f"Article already exists in database: {article_data['url']}",
                        extra={"article_url": article_data["url"]}
                    )
                    return False
                
                normalized_category, raw_category = normalize_category(
                    self.source_name,
                    article_data.get("category")
                )
                
                news = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=article_data.get("body_html", ""),
                    summary=article_data.get("summary", ""),
                    url=article_data["url"],
                    published_at=article_data.get("published_at", ""),
                    image_url=article_data.get("image_url", ""),
                    category=normalized_category,
                    raw_category=raw_category,
                    language="fa",  # Persian language
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
        """Fetch news from Eghtesad Online RSS feed."""
        self.logger.info(f"Starting to fetch news from {self.source_name}")
        
        try:
            rss_content = await self._fetch_with_retry(self.rss_url, request_type="rss")
            if not rss_content:
                self.logger.error("Failed to fetch RSS feed")
                return

            feed = feedparser.parse(rss_content)
            if not feed.entries:
                self.logger.warning("No entries found in RSS feed")
                return

            self.logger.info(f"Found {len(feed.entries)} articles in RSS feed")

            saved_count = 0
            skipped_count = 0

            for entry in feed.entries:
                try:
                    article_url = entry.get("link", "").strip()
                    if not article_url:
                        continue

                    if await self._article_exists(article_url):
                        self.logger.debug(f"Article already exists, skipping: {article_url}")
                        skipped_count += 1
                        continue

                    article_data = await self._extract_article_content(article_url)
                    if not article_data:
                        continue

                    article_data["url"] = article_url
                    
                    if not article_data.get("published_at") and hasattr(entry, "published"):
                        article_data["published_at"] = entry.published

                    # Use enclosure image from RSS if available
                    if not article_data.get("image_url") and hasattr(entry, "enclosures") and entry.enclosures:
                        for enclosure in entry.enclosures:
                            if enclosure.get("type", "").startswith("image/"):
                                article_data["image_url"] = enclosure.get("url", "")
                                break

                    if article_data.get("image_url"):
                        original_image_url = article_data["image_url"]
                        s3_key = await self._upload_image_to_s3(original_image_url)
                        if s3_key:
                            article_data["image_url"] = s3_key
                        else:
                            self.logger.warning(f"Failed to upload image, keeping original URL: {original_image_url}")

                    if await self._save_article(article_data):
                        saved_count += 1

                except Exception as e:
                    self.logger.error(f"Error processing article: {e}", exc_info=True)
                    continue

            self.logger.info(
                f"Finished fetching news from {self.source_name}: {saved_count} new articles saved, {skipped_count} articles skipped"
            )

        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

