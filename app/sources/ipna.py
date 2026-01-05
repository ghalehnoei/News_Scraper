"""IPNA (Iran Press News Agency) worker implementation."""

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse

import aiohttp
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

logger = setup_logging(source="ipna")

# IPNA base URL and sections
# Note: IPNA is a sports news agency, so all sections are sports-related
IPNA_BASE_URL = "https://www.ipna.ir"
# Using main page to get all sports news (all categorized as sports)
IPNA_SECTIONS = {
    "sports": f"{IPNA_BASE_URL}",  # Main page contains all sports news
}

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class IPNAWorker(BaseWorker):
    """Worker for IPNA (Iran Press News Agency) website scraping."""

    def __init__(self):
        """Initialize IPNA worker."""
        super().__init__("ipna")
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
            request_type: Type of request (page, article, image) for logging

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
                        retry_after = int(response.headers.get("Retry-After", "60"))
                        self.logger.warning(
                            f"Rate limited (429), waiting {retry_after}s before retry",
                            extra={"url": url, "retry_after": retry_after}
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    
                    response.raise_for_status()
                    return await response.read()
                    
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Timeout fetching {request_type} (attempt {attempt + 1}/{max_retries})",
                    extra={"url": url}
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except aiohttp.ClientError as e:
                self.logger.warning(
                    f"Client error fetching {request_type} (attempt {attempt + 1}/{max_retries}): {e}",
                    extra={"url": url, "error": str(e)}
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                self.logger.error(
                    f"Unexpected error fetching {request_type}: {e}",
                    extra={"url": url, "error": str(e)},
                    exc_info=True
                )
                break
        
        return None

    async def _ensure_s3_initialized(self) -> None:
        """Ensure S3 is initialized."""
        if not self._s3_initialized:
            await init_s3()
            self._s3_initialized = True

    async def _scrape_articles_from_page(self, section_url: str, category: str) -> List[Dict[str, str]]:
        """
        Scrape article links from a section page using class="all-post".

        Args:
            section_url: URL of the section page
            category: Category name for the articles

        Returns:
            List of article dictionaries with url and category
        """
        html_content = await self._fetch_with_retry(section_url, request_type="page")
        if not html_content:
            self.logger.warning(f"Failed to fetch section page: {section_url}")
            return []

        try:
            tree = HTMLParser(html_content.decode("utf-8"))
            articles = []

            # Look for article links inside the "all-post" class
            all_post_container = tree.css_first('.all-post')
            
            if not all_post_container:
                self.logger.warning(f"Could not find .all-post container on {section_url}")
                return []
            
            # Find all links inside the all-post container
            article_links = all_post_container.css('a[href]')
            
            self.logger.debug(
                f"Found {len(article_links)} total links in .all-post container",
                extra={"section_url": section_url}
            )

            seen_urls = set()
            for link in article_links:
                href = link.attributes.get("href", "")
                if not href or href in seen_urls:
                    continue
                
                # Make URL absolute
                article_url = urljoin(IPNA_BASE_URL, href)
                
                # Filter valid news article URLs
                # IPNA uses direct URLs without /news/ or /fa/ patterns
                # Just ensure it starts with base URL and exclude unwanted patterns
                if (article_url.startswith(IPNA_BASE_URL)
                    and article_url != IPNA_BASE_URL
                    and article_url != f"{IPNA_BASE_URL}/"
                    and not any(skip in article_url.lower() for skip in [
                        "/tag/", "/category/", "/author/", "#", 
                        "javascript:", "mailto:", ".jpg", ".png", ".pdf"
                    ])):
                    seen_urls.add(href)
                    articles.append({
                        "url": article_url,
                        "category": category,
                    })
                    self.logger.debug(f"Found article: {article_url}", extra={"article_url": article_url})

            self.logger.info(
                f"Found {len(articles)} article links in {category} section",
                extra={"section_url": section_url, "category": category, "count": len(articles)}
            )
            return articles

        except Exception as e:
            self.logger.error(f"Error scraping section page: {e}", extra={"section_url": section_url}, exc_info=True)
            return []

    async def _extract_article_content(self, url: str) -> Optional[Dict]:
        """
        Extract full article content from article page.

        Args:
            url: Article URL

        Returns:
            Dictionary with article data (title, body_html, summary, category, published_at, image_url)
        """
        html_content = await self._fetch_with_retry(url, request_type="article")
        if not html_content:
            return None

        try:
            tree = HTMLParser(html_content.decode("utf-8"))

            # Extract title
            title = ""
            # Try multiple selectors for title
            title_elem = tree.css_first('h1, .title h1, meta[property="og:title"]')
            if title_elem:
                if title_elem.tag == "meta":
                    title = title_elem.attributes.get("content", "")
                else:
                    title = title_elem.text(strip=True)
            
            if not title:
                # Fallback to any h1
                h1_elem = tree.css_first('h1')
                if h1_elem:
                    title = h1_elem.text(strip=True)

            # Extract published date
            published_at = ""
            # Try multiple selectors for date/time
            time_elem = tree.css_first('time[datetime], .publish-date, .article-date, meta[property="article:published_time"]')
            if time_elem:
                if time_elem.tag == "time":
                    published_at = time_elem.attributes.get("datetime", "")
                elif time_elem.tag == "meta":
                    published_at = time_elem.attributes.get("content", "")
                else:
                    date_text = time_elem.text(strip=True)
                    published_at = self._parse_persian_date(date_text)

            # Extract category
            category = ""
            # Try breadcrumbs
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
            summary_elem = tree.css_first('.article-summary, .lead, meta[property="og:description"], meta[name="description"]')
            if summary_elem:
                if summary_elem.tag == "meta":
                    summary = summary_elem.attributes.get("content", "")
                else:
                    summary = summary_elem.text(strip=True)

            # Extract article body from class="con"
            body_html = ""
            article_elem = tree.css_first('.con')
            if article_elem:
                # Remove unwanted elements
                for unwanted in article_elem.css('script, style, iframe, .ad, .advertisement, .related-news'):
                    unwanted.decompose()
                
                # Remove all images from body (only first image will be kept as main image)
                images_in_body = article_elem.css('img')
                self.logger.debug(f"Found {len(images_in_body)} images in body before removal", extra={"article_url": url})
                for img in images_in_body:
                    img.decompose()
                
                body_html = article_elem.html
            else:
                self.logger.warning(f"Could not find .con element for body content", extra={"article_url": url})

            # Extract main image - ONLY the first image
            image_url = ""
            # Method 1: First large/full-size image on page (excluding thumbnails, logos, icons)
            images = tree.css('img')
            # Use word boundaries to avoid false matches (e.g., "ad" in "uploads")
            skip_patterns = ["/logo", "/icon", "/avatar", "/placeholder", ".gif"]
            thumbnail_patterns = ["-145x95", "-150x150", "-100x100", "-75x75", "-50x50"]
            
            for img in images:
                src = img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy-src", "")
                if src:
                    src_lower = src.lower()
                    # Skip if contains skip patterns
                    if any(pattern in src_lower for pattern in skip_patterns):
                        continue
                    # Skip if it's a thumbnail (small size)
                    if any(pattern in src for pattern in thumbnail_patterns):
                        continue
                    # This is likely the main article image
                    image_url = urljoin(url, src)
                    self.logger.debug(f"Main image (first large image): {image_url}", extra={"article_url": url})
                    break
            
            # Method 2: og:image meta tag (fallback)
            if not image_url:
                og_image = tree.css_first('meta[property="og:image"]')
                if og_image and og_image.attributes.get("content"):
                    image_url = og_image.attributes.get("content", "")
                    if image_url:
                        image_url = urljoin(url, image_url)
                        self.logger.debug(f"Main image from og:image: {image_url}", extra={"article_url": url})

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

    def _parse_persian_date(self, date_text: str) -> str:
        """
        Parse Persian date string to ISO format.

        Args:
            date_text: Persian date string

        Returns:
            ISO format date string
        """
        # This is a simplified parser - you may need to enhance it
        # For now, return current time if parsing fails
        try:
            # Try to extract time pattern HH:MM
            time_match = re.search(r'(\d{1,2}):(\d{2})', date_text)
            if time_match:
                hour, minute = time_match.groups()
                now = datetime.now()
                return datetime(now.year, now.month, now.day, int(hour), int(minute)).isoformat(timespec='seconds')
            
            # Return current time as fallback
            return datetime.now().isoformat(timespec='seconds')
        except Exception:
            return datetime.now().isoformat(timespec='seconds')

    async def _upload_image_to_s3(self, image_data: bytes, source: str, url: str) -> Optional[str]:
        """
        Upload image to S3 and return S3 key (path only, not s3://bucket/path).
        
        Args:
            image_data: Image data as bytes
            source: News source name
            url: Original image URL for context
            
        Returns:
            S3 key (path) if successful, None otherwise
        """
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
        """Fetch news from IPNA website."""
        self.logger.info(f"Starting to fetch news from {self.source_name}")
        
        all_articles = []
        
        # Scrape articles from each section
        for category, section_url in IPNA_SECTIONS.items():
            if not self.running:
                self.logger.info("Worker stopped, cancelling fetch operation")
                break
            
            self.logger.info(f"Scraping {category} section: {section_url}")
            articles = await self._scrape_articles_from_page(section_url, category)
            all_articles.extend(articles)
            
            # Small delay between sections
            await asyncio.sleep(1)
        
        if not all_articles:
            self.logger.warning("No articles found on website")
            return
        
        self.logger.info(
            f"Found {len(all_articles)} total article links, processing each one...",
            extra={"total_articles": len(all_articles)}
        )
        
        # Process each article
        saved_count = 0
        skipped_count = 0
        for idx, article_info in enumerate(all_articles, 1):
            if not self.running:
                self.logger.info("Worker stopped, cancelling fetch operation")
                break
            
            article_url = article_info["url"]
            self.logger.info(
                f"Processing article {idx}/{len(all_articles)}: {article_url}",
                extra={
                    "article_index": idx,
                    "total_articles": len(all_articles),
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
                
                # Set URL and category from scraping
                article_data["url"] = article_url
                if not article_data.get("category"):
                    article_data["category"] = article_info.get("category", "")
                
                # Download and upload main image if exists
                if article_data.get("image_url"):
                    main_image_url = article_data["image_url"]
                    self.logger.info(
                        f"Downloading main image: {main_image_url[:100]}",
                        extra={"article_url": article_url}
                    )
                    
                    image_data = await self._fetch_with_retry(
                        main_image_url,
                        request_type="image"
                    )
                    if image_data:
                        s3_path = await self._upload_image_to_s3(
                            image_data,
                            self.source_name,
                            main_image_url
                        )
                        if s3_path:
                            article_data["image_url"] = s3_path
                            self.logger.info(
                                f"Successfully uploaded main image to S3: {s3_path}",
                                extra={"article_url": article_url}
                            )
                        else:
                            self.logger.warning(
                                f"Failed to upload main image to S3",
                                extra={"article_url": article_url}
                            )
                            article_data["image_url"] = ""
                    else:
                        self.logger.warning(
                            f"Failed to download main image",
                            extra={"article_url": article_url}
                        )
                        article_data["image_url"] = ""
                
                # Save article to database
                if await self._save_news(article_data):
                    saved_count += 1
                
            except Exception as e:
                self.logger.error(
                    f"Error processing article {article_url}: {e}",
                    extra={"article_url": article_url},
                    exc_info=True
                )
            
            # Small delay between articles
            await asyncio.sleep(0.5)
        
        self.logger.info(
            f"Finished fetching news from {self.source_name}: {saved_count} new articles saved, {skipped_count} articles skipped (already exist)"
        )

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

