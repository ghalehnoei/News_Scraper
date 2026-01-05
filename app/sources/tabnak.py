"""Tabnak News worker implementation."""

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

logger = setup_logging(source="tabnak")

# RSS feed URL
TABNAK_RSS_URL = "https://www.tabnak.ir/fa/rss/allnews"
TABNAK_BASE_URL = "https://www.tabnak.ir"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class TabnakWorker(BaseWorker):
    """Worker for Tabnak RSS feed."""

    def __init__(self):
        """Initialize Tabnak worker."""
        super().__init__("tabnak")
        self.rss_url = TABNAK_RSS_URL
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
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
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
        """
        Download image and upload to S3.

        Args:
            image_url: URL of the image to download

        Returns:
            S3 key (path) of uploaded image, or None if upload failed
        """
        if not self._s3_initialized:
            await init_s3()
            self._s3_initialized = True

        try:
            # Download image
            image_data = await self._fetch_with_retry(image_url, request_type="image")
            if not image_data:
                logger.warning(f"Failed to download image: {image_url}")
                return None

            # Generate unique S3 key
            image_hash = hashlib.md5(image_data).hexdigest()[:8]
            parsed_url = urlparse(image_url)
            extension = parsed_url.path.split(".")[-1].lower()
            if extension not in ["jpg", "jpeg", "png", "gif", "webp"]:
                extension = "jpg"
            
            timestamp = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"news-images/{self.source_name}/{timestamp}/{image_hash}.{extension}"

            # Upload to S3 using async client
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
        """
        Check if article already exists in database.

        Args:
            url: Article URL to check

        Returns:
            True if article exists, False otherwise
        """
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
        """
        Extract article content from Tabnak article page.

        Args:
            url: Article URL

        Returns:
            Dictionary with article data or None if extraction failed
        """
        try:
            html_content = await self._fetch_with_retry(url, request_type="article")
            if not html_content:
                logger.warning(f"Failed to fetch article content from {url}")
                return None

            tree = HTMLParser(html_content.decode("utf-8", errors="ignore"))

            # Extract title
            title = ""
            title_elem = tree.css_first('h1.title, h1')
            if title_elem:
                title = title_elem.text(strip=True)
            
            # Fallback to og:title
            if not title:
                og_title = tree.css_first('meta[property="og:title"]')
                if og_title:
                    title = og_title.attributes.get("content", "")

            # Extract published date
            published_at = ""
            # Method 1: Try fa_date class (Tabnak specific)
            fa_date_elem = tree.css_first('.fa_date')
            if fa_date_elem:
                published_at = fa_date_elem.text(strip=True)
                logger.debug(f"Published date from .fa_date: {published_at}", extra={"article_url": url})
            
            # Method 2: Try time element with datetime attribute
            if not published_at:
                time_elem = tree.css_first('time[datetime]')
                if time_elem:
                    published_at = time_elem.attributes.get("datetime", "")
                    logger.debug(f"Published date from time[datetime]: {published_at}", extra={"article_url": url})
            
            # Method 3: Try meta tag
            if not published_at:
                meta_time = tree.css_first('meta[property="article:published_time"]')
                if meta_time:
                    published_at = meta_time.attributes.get("content", "")
                    logger.debug(f"Published date from meta: {published_at}", extra={"article_url": url})

            # Extract category
            category = ""
            # Try breadcrumb
            breadcrumb = tree.css('.breadcrumb a, .category a')
            if breadcrumb and len(breadcrumb) > 1:
                category = breadcrumb[-1].text(strip=True)
            
            # Try meta tag
            if not category:
                category_meta = tree.css_first('meta[property="article:section"]')
                if category_meta:
                    category = category_meta.attributes.get("content", "")

            # Extract summary
            summary = ""
            summary_elem = tree.css_first('.lead, .article-lead, meta[property="og:description"], meta[name="description"]')
            if summary_elem:
                if summary_elem.tag == "meta":
                    summary = summary_elem.attributes.get("content", "")
                else:
                    summary = summary_elem.text(strip=True)

            # Extract video content if present (for video news)
            video_html = ""
            video_elem = tree.css_first('#mainNewsVideo')
            if video_elem:
                logger.debug(f"Found video content in article", extra={"article_url": url})
                
                # Extract direct video URL from script
                video_url = ""
                video_poster = ""
                scripts = video_elem.css('script')
                for script in scripts:
                    script_content = script.text()
                    if '"file":' in script_content:
                        # Extract video URL
                        import re
                        matches = re.findall(r'"file":\s*"([^"]+\.mp4)"', script_content)
                        if matches:
                            video_url = matches[0]
                        # Extract poster image
                        poster_matches = re.findall(r"'player_\d+_\d+',\s*'([^']+\.jpg)'", script_content)
                        if poster_matches:
                            video_poster = poster_matches[0]
                            if not video_poster.startswith('http'):
                                video_poster = urljoin(url, video_poster)
                
                # Create simple HTML5 video player
                if video_url:
                    poster_attr = f' poster="{video_poster}"' if video_poster else ''
                    video_html = f'''<div class="video-container" style="max-width: 700px; margin: 20px 0;">
    <video controls{poster_attr} style="width: 100%; height: auto;">
        <source src="{video_url}" type="video/mp4">
        <p>مرورگر شما از پخش ویدیو پشتیبانی نمی‌کند. <a href="{video_url}">دانلود ویدیو</a></p>
    </video>
    <p style="margin-top: 10px;"><a href="{video_url}" download>دانلود ویدیو</a></p>
</div>'''
                    logger.debug(f"Created HTML5 video player: {video_url}", extra={"article_url": url})
                else:
                    # Fallback to full video element if URL extraction failed
                    video_html = video_elem.html
                    logger.warning(f"Could not extract video URL, keeping full video element", extra={"article_url": url})
            
            # Extract article body
            body_html = ""
            article_elem = tree.css_first('.body, .item_text, article .content')
            if article_elem:
                # Remove unwanted elements (but keep scripts for video news)
                for unwanted in article_elem.css('style, .ad, .advertisement, .related-news, .tags, .share'):
                    unwanted.decompose()
                
                # Check if this is a photo gallery (category is "عکس" or has news_album class)
                is_gallery = False
                if category == "عکس" or article_elem.css_first('.news_album_main_part, .image_set'):
                    is_gallery = True
                    logger.debug(f"Detected photo gallery article", extra={"article_url": url})
                
                # Check if this is a video article
                is_video = bool(video_html) or category == "فیلم"
                
                # For video articles, remove scripts from body (they're in video_html)
                # For galleries, keep images. For regular articles, remove images
                if is_video:
                    # Remove scripts and iframes from body since video is separate
                    for script in article_elem.css('script, iframe'):
                        script.decompose()
                    logger.debug(f"Video article detected, keeping text content only in body", extra={"article_url": url})
                
                if not is_gallery and not is_video:
                    images_in_body = article_elem.css('img')
                    logger.debug(f"Found {len(images_in_body)} images in body before removal", extra={"article_url": url})
                    for img in images_in_body:
                        img.decompose()
                else:
                    logger.debug(f"Keeping images in body for {'gallery' if is_gallery else 'video'} article", extra={"article_url": url})
                
                body_html = article_elem.html
            else:
                logger.warning(f"Could not find article body element", extra={"article_url": url})
            
            # Prepend video content to body if exists
            if video_html:
                body_html = video_html + "\n" + body_html
                logger.debug(f"Added video content to body", extra={"article_url": url})

            # Extract main image - ONLY the first image
            image_url = ""
            # Method 1: og:image meta tag
            og_image = tree.css_first('meta[property="og:image"]')
            if og_image and og_image.attributes.get("content"):
                image_url = og_image.attributes.get("content", "")
                if image_url:
                    image_url = urljoin(url, image_url)
                    logger.debug(f"Main image from og:image: {image_url}", extra={"article_url": url})
            
            # Method 2: First image on page (fallback)
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
                        logger.debug(f"Main image from first page image: {image_url}", extra={"article_url": url})
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
        """
        Save article to database.

        Args:
            article_data: Dictionary containing article information

        Returns:
            True if saved successfully, False otherwise
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
                    language="fa",  # Persian language
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=article_data.get("body_html", ""),
                    summary=article_data.get("summary", ""),
                    url=article_data["url"],
                    published_at=article_data.get("published_at", ""),
                    image_url=article_data.get("image_url", ""),
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
        """Fetch news from Tabnak RSS feed."""
        self.logger.info(f"Starting to fetch news from {self.source_name}")
        
        try:
            # Fetch RSS feed
            rss_content = await self._fetch_with_retry(self.rss_url, request_type="rss")
            if not rss_content:
                self.logger.error("Failed to fetch RSS feed")
                return

            # Parse RSS feed
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

                    # Check if article already exists (early check to avoid unnecessary processing)
                    if await self._article_exists(article_url):
                        self.logger.debug(f"Article already exists, skipping: {article_url}")
                        skipped_count += 1
                        continue

                    # Extract full article content
                    article_data = await self._extract_article_content(article_url)
                    if not article_data:
                        continue

                    # Add URL and RSS metadata
                    article_data["url"] = article_url
                    
                    # Use RSS pubDate if article extraction didn't find one
                    if not article_data.get("published_at") and hasattr(entry, "published"):
                        article_data["published_at"] = entry.published

                    # Upload main image to S3 if available
                    if article_data.get("image_url"):
                        original_image_url = article_data["image_url"]
                        s3_key = await self._upload_image_to_s3(original_image_url)
                        if s3_key:
                            article_data["image_url"] = s3_key
                            self.logger.debug(f"Image uploaded to S3: {s3_key}")
                        else:
                            self.logger.warning(f"Failed to upload image, keeping original URL: {original_image_url}")

                    # Save article
                    if await self._save_article(article_data):
                        saved_count += 1

                except Exception as e:
                    self.logger.error(f"Error processing article: {e}", exc_info=True)
                    continue

            self.logger.info(
                f"Finished fetching news from {self.source_name}: {saved_count} new articles saved, {skipped_count} articles skipped (already exist)"
            )

        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

