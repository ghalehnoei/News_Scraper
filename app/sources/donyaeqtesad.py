"""Donya-e-Eqtesad (World Economy) worker implementation."""

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse

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

logger = setup_logging(source="donyaeqtesad")

# RSS feed URL
DONYA_EQTESAD_RSS_URL = "https://donya-e-eqtesad.com/feeds/"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class DonyaEqtesadWorker(BaseWorker):
    """Worker for Donya-e-Eqtesad RSS feed."""

    def __init__(self):
        """Initialize Donya-e-Eqtesad worker."""
        super().__init__("donyaeqtesad")
        self.rss_url = DONYA_EQTESAD_RSS_URL
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
                    if response.status == 200:
                        content = await response.read()
                        self.logger.debug(
                            f"Successfully fetched {request_type}: {url}",
                            extra={"source": self.source_name, "request_type": request_type, "article_url": url}
                        )
                        return content
                    elif response.status == 404:
                        self.logger.warning(
                            f"404 Not Found for {request_type}: {url}",
                            extra={"source": self.source_name, "request_type": request_type, "article_url": url}
                        )
                        return None
                    else:
                        self.logger.warning(
                            f"HTTP {response.status} for {request_type}: {url}",
                            extra={"source": self.source_name, "request_type": request_type, "article_url": url}
                        )
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Timeout fetching {request_type} (attempt {attempt + 1}/{max_retries}): {url}",
                    extra={"source": self.source_name, "request_type": request_type, "article_url": url}
                )
            except Exception as e:
                self.logger.warning(
                    f"Error fetching {request_type} (attempt {attempt + 1}/{max_retries}): {e}",
                    extra={"source": self.source_name, "request_type": request_type, "article_url": url}
                )
            
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        self.logger.error(
            f"Failed to fetch {request_type} after {max_retries} attempts: {url}",
            extra={"source": self.source_name, "request_type": request_type, "article_url": url}
        )
        return None

    async def _parse_rss_feed(self) -> list[dict]:
        """
        Parse RSS feed and extract article information.

        Returns:
            List of dictionaries containing article data
        """
        content = await self._fetch_with_retry(self.rss_url, request_type="rss")
        if content is None:
            self.logger.error("Failed to fetch RSS feed")
            return []

        try:
            feed = feedparser.parse(content)
            items = []
            
            for entry in feed.entries:
                item = {
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "description": "",
                    "pubDate": "",
                    "category": "",
                    "image_url": "",
                }
                
                # Extract description (may contain HTML)
                description = entry.get("description", "")
                if description:
                    # Parse HTML to extract text and image
                    desc_soup = BeautifulSoup(description, 'html.parser')
                    # Extract image from description
                    img_tag = desc_soup.find("img")
                    if img_tag and img_tag.get("src"):
                        item["image_url"] = img_tag["src"]
                    # Extract text description
                    div_tag = desc_soup.find("div")
                    if div_tag:
                        item["description"] = div_tag.get_text(strip=True)
                    else:
                        item["description"] = desc_soup.get_text(strip=True)
                
                # Extract published date
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        pub_date = datetime(*entry.published_parsed[:6])
                        item["pubDate"] = pub_date.isoformat()
                    except Exception as e:
                        self.logger.debug(f"Could not parse published_parsed: {e}")
                
                # Fallback to published field
                if not item["pubDate"]:
                    for date_field in ["published", "updated", "pubDate"]:
                        if entry.get(date_field):
                            try:
                                item["pubDate"] = datetime.strptime(
                                    entry[date_field], "%a, %d %b %Y %H:%M:%S %z"
                                ).isoformat()
                                break
                            except Exception:
                                try:
                                    item["pubDate"] = datetime.strptime(
                                        entry[date_field], "%a, %d %b %Y %H:%M:%S %Z"
                                    ).isoformat()
                                    break
                                except Exception:
                                    pass
                
                # Extract category from tags, category, or dc:subject
                category = ""
                if hasattr(entry, "tags") and entry.tags:
                    category = entry.tags[0].get("term", "")
                elif hasattr(entry, "category"):
                    category = entry.category
                elif hasattr(entry, "dc_subject"):
                    category = entry.dc_subject
                
                item["category"] = category
                
                items.append(item)
            
            self.logger.info(f"Parsed {len(items)} items from RSS feed")
            return items
        except Exception as e:
            self.logger.error(f"Error parsing RSS feed: {e}", exc_info=True)
            return []

    async def _check_url_exists(self, url: str, db: AsyncSession) -> bool:
        """
        Check if article URL already exists in database for this source.

        Args:
            url: Article URL
            db: Database session

        Returns:
            True if URL exists, False otherwise
        """
        # Normalize URL: remove trailing slash and query parameters for comparison
        normalized_url = url.rstrip('/').split('?')[0].split('#')[0]
        
        # Check with source filter
        result = await db.execute(
            select(News).where(
                (News.source == "donyaeqtesad") & 
                (News.url == normalized_url)
            )
        )
        existing = result.scalar_one_or_none()
        
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "donyaeqtesad") & 
                    (News.url == url)
                )
            )
            existing = result.scalar_one_or_none()
        
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "donyaeqtesad") & 
                    (News.url.like(f"{normalized_url}%"))
                )
            )
            existing = result.scalar_one_or_none()
        
        if existing:
            self.logger.info(
                f"Article already exists in database (skipping): {url}",
                extra={"article_url": url}
            )
        else:
            self.logger.debug(
                f"Article not found in database, will save: {url}",
                extra={"article_url": url}
            )
        return existing is not None

    async def _extract_article_content(self, url: str) -> Optional[dict]:
        """
        Extract article content from HTML page.

        Args:
            url: Article URL

        Returns:
            Dictionary with article content or None if extraction failed
        """
        try:
            content = await self._fetch_with_retry(url, request_type="article")
            if content is None:
                return None

            # Try different encodings
            html_content = None
            for encoding in ['utf-8', 'utf-8-sig', 'windows-1256', 'iso-8859-1']:
                try:
                    html_content = content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if html_content is None:
                html_content = content.decode('utf-8', errors='ignore')

            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract title
            title = ""
            # Priority 1: h1 within article tag
            article_tag_for_title = soup.find("article")
            if article_tag_for_title:
                h1_tag = article_tag_for_title.find("h1")
                if h1_tag:
                    title = h1_tag.get_text(strip=True)
            
            # Priority 2: Any h1 on the page
            if not title:
                h1_tag = soup.find("h1")
                if h1_tag:
                    title = h1_tag.get_text(strip=True)
            
            # Priority 3: og:title meta tag
            if not title:
                meta_title = soup.find("meta", attrs={"property": "og:title"})
                if meta_title and meta_title.get("content"):
                    title = meta_title["content"]
            
            # Priority 4: title tag (remove site name suffix)
            if not title:
                title_tag = soup.find("title")
                if title_tag:
                    title = title_tag.get_text(strip=True)
                    # Remove " - دنیای اقتصاد" or similar suffixes
                    if " - " in title:
                        title = title.split(" - ")[0].strip()

            # Extract article body using XPath (Priority 1)
            body_html = ""
            article_tag = None
            
            # Priority 1: XPath /html/body/div/div[2]/main/div[1]/div[1]/div[1]/div[1]/article/div[2]
            try:
                from lxml import etree
                parser = etree.HTMLParser(encoding='utf-8')
                tree = etree.fromstring(content, parser=parser)
                xpath_result = tree.xpath('/html/body/div/div[2]/main/div[1]/div[1]/div[1]/div[1]/article/div[2]')
                if xpath_result and len(xpath_result) > 0:
                    # Convert lxml element to BeautifulSoup
                    xpath_elem = xpath_result[0]
                    # Get HTML string from lxml element
                    xpath_html = etree.tostring(xpath_elem, encoding='unicode', method='html')
                    # Parse with BeautifulSoup for further processing
                    xpath_soup = BeautifulSoup(xpath_html, 'html.parser')
                    # Check if it has meaningful content
                    text_content = xpath_soup.get_text(strip=True)
                    if len(text_content) > 100:
                        article_tag = xpath_soup
                        self.logger.debug(f'Found article body using XPath /html/body/div/div[2]/main/div[1]/div[1]/div[1]/div[1]/article/div[2]', extra={"article_url": url})
            except Exception as e:
                self.logger.debug(f"Error extracting article body using XPath: {e}", extra={"article_url": url})
            
            # Fallback: Try CSS selectors (only if XPath fails)
            if not article_tag:
                article_selectors = [
                    "article div",
                    "article",
                    ".article-body",
                    ".content",
                    ".post-content",
                    "#content",
                    ".news-content",
                    ".article-content",
                    "main",
                    "[role='main']",
                    ".main-content",
                    ".news-body",
                    ".news-text",
                    "div.news",
                    "div[class*='news']",
                    "div[class*='content']",
                    "div[class*='article']",
                ]
                
                for selector in article_selectors:
                    try:
                        article_tag = soup.select_one(selector)
                        if article_tag:
                            # Check if it has meaningful content (at least 100 characters)
                            text_content = article_tag.get_text(strip=True)
                            if len(text_content) > 100:
                                self.logger.debug(f"Found article body with selector: {selector}", extra={"article_url": url})
                                break
                            else:
                                article_tag = None
                    except Exception as e:
                        self.logger.debug(f"Error with selector {selector}: {e}", extra={"article_url": url})
                        continue

            if article_tag:
                # Remove script and style tags
                for tag in article_tag.find_all(["script", "style", "iframe"]):
                    tag.decompose()
                
                # Remove specific unwanted classes
                unwanted_classes = [
                    "ad", "advertisement", "social", "share",
                    "comment", "comments", "related", "sidebar"
                ]
                
                # Remove elements with unwanted classes
                for tag in article_tag.find_all(class_=lambda x: x and any(skip in x.lower() for skip in unwanted_classes)):
                    tag.decompose()
                
                # Remove "inline-box-theme1 inline-box-id" elements (related news boxes)
                for tag in article_tag.find_all(class_=lambda x: x and ("inline-box-theme1" in str(x) or "inline-box-id" in str(x))):
                    tag.decompose()
                    self.logger.debug("Removed inline-box element (related news)", extra={"article_url": url})
                
                # Remove advertisement links
                for ad_link in article_tag.find_all("a", href=lambda x: x and ("/redirect/ads/" in x or "/ads/" in x or "advertisement" in x.lower())):
                    parent = ad_link.find_parent()
                    if parent:
                        parent.decompose()
                
                # Remove content after "انتهای پیام" (end of message marker)
                end_marker = article_tag.find(string=lambda text: text and "انتهای پیام" in text)
                if end_marker:
                    marker_parent = end_marker.find_parent()
                    if marker_parent:
                        for sibling in list(marker_parent.next_siblings):
                            if hasattr(sibling, 'decompose'):
                                sibling.decompose()
                
                # Remove "خبر مرتبط" (related news) sections
                related_news_markers = [
                    "خبر مرتبط",
                    "اخبار مرتبط",
                    "مطالب مرتبط",
                    "خبرهای مرتبط",
                ]
                for marker_text in related_news_markers:
                    # Find elements containing the marker text
                    marker_elements = article_tag.find_all(string=lambda text: text and marker_text in text)
                    for marker in marker_elements:
                        marker_parent = marker.find_parent()
                        if marker_parent:
                            # Remove the parent element and all its siblings after it
                            for sibling in list(marker_parent.next_siblings):
                                if hasattr(sibling, 'decompose'):
                                    sibling.decompose()
                            # Also remove the parent itself
                            marker_parent.decompose()
                            self.logger.debug(f"Removed related news section: {marker_text}", extra={"article_url": url})
                
                # Convert relative image URLs to absolute URLs
                for img_tag in article_tag.find_all("img"):
                    for attr in ["src", "data-src", "data-lazy-src", "data-original"]:
                        img_url = img_tag.get(attr)
                        if img_url:
                            # Convert relative URL to absolute
                            if not img_url.startswith("http"):
                                absolute_url = urljoin(url, img_url)
                                img_tag[attr] = absolute_url
                                self.logger.debug(f"Converted relative image URL to absolute: {img_url} -> {absolute_url}", extra={"article_url": url})
                            break
                
                # Remove unwanted links (internal site links, social media links, etc.)
                for link_tag in article_tag.find_all("a"):
                    href = link_tag.get("href", "")
                    if href:
                        # Remove links to main site page
                        if href == "/" or href == "https://donya-e-eqtesad.com/" or href == "https://donya-e-eqtesad.com":
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed link to main site: {href}", extra={"article_url": url})
                        # Remove internal news links
                        elif href.startswith("/") and ("/بخش-" in href or "/news/" in href):
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed internal news link: {href}", extra={"article_url": url})
                        # Remove social media links
                        elif any(social in href.lower() for social in ["facebook.com", "twitter.com", "instagram.com", "telegram.org", "t.me", "plus.google.com"]):
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed social media link: {href}", extra={"article_url": url})
                        # Remove tag/category links
                        elif href.startswith("/tag/") or href.startswith("/category/"):
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed tag/category link: {href}", extra={"article_url": url})
                
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

            # Extract category and published date
            category = ""
            published_at = ""

            # Extract image
            image_url = ""
            
            # Priority 1: og:image
            og_image = soup.find("meta", attrs={"property": "og:image"})
            if og_image and og_image.get("content"):
                image_url = og_image["content"]
                self.logger.debug(f"Found image from og:image: {image_url}", extra={"article_url": url})
            
            # Priority 2: Image within article > figure > img
            if not image_url:
                article_elem = soup.find("article")
                if article_elem:
                    figures = article_elem.find_all("figure")
                    for figure_tag in figures:
                        ad_link = figure_tag.find("a", href=lambda x: x and ("/redirect/ads/" in x or "/ads/" in x))
                        if ad_link:
                            continue
                        
                        img_tag = figure_tag.find("img")
                        if img_tag:
                            for attr in ["src", "data-src", "data-lazy-src", "data-original"]:
                                if img_tag.get(attr):
                                    image_url = img_tag[attr]
                                    self.logger.debug(f"Found image from article figure: {image_url}", extra={"article_url": url})
                                    break
                        if image_url:
                            break
            
            # Priority 3: First image in article content
            if not image_url and article_tag:
                all_imgs = article_tag.find_all("img")
                for img_tag in all_imgs:
                    parent_link = img_tag.find_parent("a", href=lambda x: x and ("/redirect/ads/" in x or "/ads/" in x))
                    if parent_link:
                        continue
                    
                    for attr in ["src", "data-src", "data-lazy-src", "data-original"]:
                        if img_tag.get(attr):
                            src = img_tag[attr]
                            if "logo" in src.lower() or "barcode" in src.lower():
                                continue
                            image_url = src
                            self.logger.debug(f"Found image from article content: {image_url}", extra={"article_url": url})
                            break
                    if image_url:
                        break
            
            # Priority 4: Any large image on the page from CDN
            if not image_url:
                all_images = soup.find_all("img")
                for img in all_images:
                    parent_link = img.find_parent("a", href=lambda x: x and ("/redirect/ads/" in x or "/ads/" in x))
                    if parent_link:
                        continue
                    
                    src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or img.get("data-original")
                    if src and ("donya-e-eqtesad.com" in src or "cdn.donya-e-eqtesad.com" in src):
                        if "logo" in src.lower() or "barcode" in src.lower():
                            continue
                        width = img.get("width")
                        height = img.get("height")
                        if width and height:
                            try:
                                if int(width) > 200 and int(height) > 200:
                                    image_url = src
                                    self.logger.debug(f"Found large image from CDN: {image_url}", extra={"article_url": url})
                                    break
                            except ValueError:
                                pass
                        else:
                            image_url = src
                            self.logger.debug(f"Found image from CDN (no dimensions): {image_url}", extra={"article_url": url})
                            break

            # Make image URL absolute if relative
            if image_url and not image_url.startswith("http"):
                image_url = urljoin(url, image_url)
            
            # Remove query string from image URL
            if image_url:
                parsed_img = urlparse(image_url)
                image_url = urlunparse((parsed_img.scheme, parsed_img.netloc, parsed_img.path, '', '', ''))

            return {
                "title": title,
                "body_html": body_html,
                "summary": summary,
                "image_url": image_url,
                "category": category,
                "published_at": published_at,
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
        Download image from URL.

        Args:
            image_url: Image URL

        Returns:
            Image data as bytes, or None if download failed
        """
        if not image_url:
            return None
        
        try:
            content = await self._fetch_with_retry(image_url, request_type="image")
            if content:
                # Validate image content
                if (content.startswith(b'\xff\xd8') or  # JPEG
                    content.startswith(b'\x89PNG') or  # PNG
                    content.startswith(b'GIF8') or     # GIF
                    content.startswith(b'GIF9') or     # GIF
                    content.startswith(b'RIFF') and len(content) > 12 and content[8:12] == b'WEBP'):  # WebP
                    return content
                else:
                    self.logger.warning(f"Invalid image format for {image_url}", extra={"article_url": image_url})
            return None
        except Exception as e:
            self.logger.warning(f"Error downloading image {image_url}: {e}", extra={"article_url": image_url})
            return None

    async def _upload_image_to_s3(
        self, image_data: bytes, source: str, url: str
    ) -> Optional[str]:
        """
        Upload image to S3.

        Args:
            image_data: Image data as bytes
            source: News source name
            url: Article URL (for generating unique filename)

        Returns:
            S3 key (path) if successful, None otherwise
        """
        try:
            # Generate S3 path: news-images/{source}/{yyyy}/{mm}/{dd}/{filename}
            now = datetime.now()
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            
            # Determine file extension from image data
            if image_data.startswith(b'\xff\xd8'):
                ext = '.jpg'
            elif image_data.startswith(b'\x89PNG'):
                ext = '.png'
            elif image_data.startswith(b'GIF8') or image_data.startswith(b'GIF9'):
                ext = '.gif'
            elif image_data.startswith(b'RIFF') and len(image_data) > 12 and image_data[8:12] == b'WEBP':
                ext = '.webp'
            else:
                ext = '.jpg'  # Default to jpg
            
            s3_key = f"news-images/{source}/{now.year}/{now.month:02d}/{now.day:02d}/{url_hash}{ext}"
            
            # Initialize S3 if not already done
            if not self._s3_initialized:
                await init_s3()
                self._s3_initialized = True
            
            # Upload to S3
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
            
            async with s3_session.client('s3', **client_kwargs) as s3_client:
                await s3_client.upload_fileobj(
                    BytesIO(image_data),
                    settings.s3_bucket,
                    s3_key,
                    ExtraArgs={"ContentType": f"image/{ext[1:]}" if ext != '.webp' else "image/webp"}
                )
            
            # Generate presigned URL
            from app.storage.s3 import generate_presigned_url
            presigned_url = await generate_presigned_url(s3_key)
            
            if presigned_url:
                self.logger.info(f"Successfully uploaded image to S3: {s3_key}", extra={"article_url": url})
                return presigned_url
            else:
                s3_url = f"{settings.s3_endpoint}/{settings.s3_bucket}/{s3_key}"
                self.logger.info(f"Uploaded image to S3 (no presigned URL): {s3_url}", extra={"article_url": url})
                return s3_url
        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", extra={"article_url": url}, exc_info=True)
            return None

    async def _save_article(
        self, rss_item: dict, article_content: dict, s3_image_url: str
    ) -> None:
        """
        Save article to database.

        Args:
            rss_item: RSS feed item data
            article_content: Extracted article content
            s3_image_url: S3 URL for the image
        """
        async with AsyncSessionLocal() as db:
            try:
                # Check if URL already exists (with source filter)
                if await self._check_url_exists(rss_item["link"], db):
                    return

                # Get raw category
                raw_category = article_content.get("category") or rss_item.get("category", "")
                
                # Clean and truncate raw_category to fit database limit (200 chars)
                if raw_category:
                    # Remove extra whitespace and normalize spaces
                    raw_category = re.sub(r'\s+', ' ', raw_category).strip()
                    # Truncate to 200 characters if too long
                    if len(raw_category) > 200:
                        raw_category = raw_category[:200].rstrip()
                        self.logger.debug(f"Truncated raw_category to 200 chars", extra={"article_url": rss_item["link"]})
                
                # Normalize category
                normalized_category, preserved_raw_category = normalize_category("donyaeqtesad", raw_category)
                
                # Also truncate preserved_raw_category if needed
                if preserved_raw_category and len(preserved_raw_category) > 200:
                    preserved_raw_category = preserved_raw_category[:200].rstrip()
                
                # Create news article
                published_at = article_content.get("published_at") or rss_item.get("pubDate", "")
                
                # Normalize URL for storage
                normalized_url = rss_item["link"].rstrip('/').split('?')[0].split('#')[0]
                
                news = News(
                    source="donyaeqtesad",
                    title=article_content.get("title") or rss_item["title"],
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary") or rss_item.get("description", ""),
                    url=normalized_url,
                    published_at=published_at,
                    image_url=s3_image_url,
                    category=normalized_category,
                    raw_category=preserved_raw_category,
                )

                db.add(news)
                await db.commit()
                
                self.logger.info(
                    f"Successfully saved article: {news.title[:50]}...",
                    extra={"article_url": rss_item["link"]}
                )
            except Exception as e:
                self.logger.error(
                    f"Error saving article to database: {e}",
                    extra={"article_url": rss_item["link"]},
                    exc_info=True
                )
                await db.rollback()
                raise

    async def fetch_news(self) -> None:
        """
        Fetch news from Donya-e-Eqtesad RSS feed.
        """
        if not self.running:
            return

        try:
            # Parse RSS feed
            rss_items = await self._parse_rss_feed()
            
            if not rss_items:
                self.logger.warning("No items found in RSS feed")
                return

            self.logger.info(f"Processing {len(rss_items)} articles from RSS feed")

            # Process each article
            for idx, rss_item in enumerate(rss_items, 1):
                if not self.running:
                    self.logger.info("Worker stopped, cancelling fetch operation")
                    break

                self.logger.info(
                    f"Processing article {idx}/{len(rss_items)}: {rss_item['title'][:50]}...",
                    extra={"article_url": rss_item["link"]}
                )

                # Quick check if URL already exists before extracting content
                async with AsyncSessionLocal() as quick_db:
                    if await self._check_url_exists(rss_item["link"], quick_db):
                        self.logger.debug(
                            f"Skipping article (already exists): {rss_item['title'][:50]}...",
                            extra={"article_url": rss_item["link"]}
                        )
                        continue

                # Extract article content
                article_content = await self._extract_article_content(rss_item["link"])
                if not article_content:
                    self.logger.warning(
                        f"Failed to extract content for article: {rss_item['link']}",
                        extra={"article_url": rss_item["link"]}
                    )
                    continue

                # Download and upload image if available
                s3_image_url = ""
                image_url = article_content.get("image_url") or rss_item.get("image_url", "")
                if not image_url and rss_item.get("image_url"):
                    image_url = rss_item.get("image_url")
                if image_url:
                    self.logger.debug(
                        f"Processing main image for article: {image_url}",
                        extra={"article_url": rss_item["link"]}
                    )
                    image_data = await self._download_image(image_url)
                    if image_data:
                        self.logger.debug(
                            f"Downloaded main image ({len(image_data)} bytes), uploading to S3...",
                            extra={"article_url": rss_item["link"]}
                        )
                        s3_image_url = await self._upload_image_to_s3(
                            image_data, self.source_name, rss_item["link"]
                        )
                        if s3_image_url:
                            self.logger.info(
                                f"Successfully uploaded main image to S3: {s3_image_url}",
                                extra={"article_url": rss_item["link"]}
                            )

                # Save article to database
                try:
                    await self._save_article(rss_item, article_content, s3_image_url)
                    self.logger.info(
                        f"Successfully processed article: {rss_item['title'][:50]}...",
                        extra={"article_url": rss_item["link"]}
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error saving article: {e}",
                        extra={"article_url": rss_item["link"]},
                        exc_info=True
                    )

                # Small delay between articles
                await asyncio.sleep(0.5)
                await asyncio.sleep(0)

        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

