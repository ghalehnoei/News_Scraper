"""SNN (Student News Network) worker implementation."""

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

logger = setup_logging(source="snn")

# RSS feed URL
SNN_RSS_URL = "https://snn.ir/fa/rss/allnews"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class SNNWorker(BaseWorker):
    """Worker for SNN (Student News Network) RSS feed."""

    def __init__(self):
        """Initialize SNN worker."""
        super().__init__("snn")
        self.rss_url = SNN_RSS_URL
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
                    "description": entry.get("description", ""),
                    "pubDate": "",
                    "category": "",
                    "image_url": "",
                }
                
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
                                    entry[date_field], "%d %b %Y %H:%M:%S %z"
                                ).isoformat()
                                break
                            except Exception:
                                try:
                                    item["pubDate"] = datetime.strptime(
                                        entry[date_field], "%a, %d %b %Y %H:%M:%S %z"
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
                
                # Extract image from enclosure
                if hasattr(entry, "enclosures") and entry.enclosures:
                    for enclosure in entry.enclosures:
                        if enclosure.get("type", "").startswith("image/"):
                            item["image_url"] = enclosure.get("url", "")
                            break
                
                # Also check for media:content or media:thumbnail
                if not item["image_url"]:
                    if hasattr(entry, "media_content") and entry.media_content:
                        for media in entry.media_content:
                            if media.get("type", "").startswith("image/"):
                                item["image_url"] = media.get("url", "")
                                break
                    elif hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
                        item["image_url"] = entry.media_thumbnail[0].get("url", "")
                
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
                (News.source == "snn") & 
                (News.url == normalized_url)
            )
        )
        existing = result.scalar_one_or_none()
        
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "snn") & 
                    (News.url == url)
                )
            )
            existing = result.scalar_one_or_none()
        
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "snn") & 
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
                    # Remove " - خبرگزاری دانشجو" or similar suffixes
                    if " - " in title:
                        title = title.split(" - ")[0].strip()

            # Extract article body using class="body"
            body_html = ""
            article_tag = None
            
            # Priority 1: Use class="body"
            try:
                article_tag = soup.select_one(".body")
                if article_tag:
                    # Check if it has meaningful content (at least 100 characters)
                    text_content = article_tag.get_text(strip=True)
                    if len(text_content) > 100:
                        self.logger.debug(f"Found article body with class='body'", extra={"article_url": url})
                    else:
                        article_tag = None
            except Exception as e:
                self.logger.debug(f"Error extracting article body with class='body': {e}", extra={"article_url": url})
            
            # Fallback: Try other CSS selectors (only if class="body" fails)
            if not article_tag:
                article_selectors = [
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
                
                # Convert relative video URLs to absolute URLs
                for video_tag in article_tag.find_all("video"):
                    for attr in ["src", "poster"]:
                        video_url = video_tag.get(attr)
                        if video_url:
                            # Convert relative URL to absolute
                            if not video_url.startswith("http"):
                                absolute_url = urljoin(url, video_url)
                                video_tag[attr] = absolute_url
                                self.logger.debug(f"Converted relative video URL to absolute: {video_url} -> {absolute_url}", extra={"article_url": url})
                
                # Convert relative video URLs in source tags
                for source_tag in article_tag.find_all("source"):
                    src_url = source_tag.get("src")
                    if src_url:
                        # Convert relative URL to absolute
                        if not src_url.startswith("http"):
                            absolute_url = urljoin(url, src_url)
                            source_tag["src"] = absolute_url
                            self.logger.debug(f"Converted relative source URL to absolute: {src_url} -> {absolute_url}", extra={"article_url": url})
                
                # Convert relative video URLs in anchor tags that link to video files
                for link_tag in article_tag.find_all("a"):
                    href = link_tag.get("href", "")
                    if href:
                        # Check if it's a video file link
                        video_extensions = [".mp4", ".webm", ".ogg", ".avi", ".mov", ".mkv"]
                        if any(href.lower().endswith(ext) for ext in video_extensions):
                            # Convert relative URL to absolute
                            if not href.startswith("http"):
                                absolute_url = urljoin(url, href)
                                link_tag["href"] = absolute_url
                                self.logger.debug(f"Converted relative video link URL to absolute: {href} -> {absolute_url}", extra={"article_url": url})
                
                # Convert relative video URLs in iframe tags
                for iframe_tag in article_tag.find_all("iframe"):
                    iframe_src = iframe_tag.get("src")
                    if iframe_src:
                        # Convert relative URL to absolute
                        if not iframe_src.startswith("http"):
                            absolute_url = urljoin(url, iframe_src)
                            iframe_tag["src"] = absolute_url
                            self.logger.debug(f"Converted relative iframe URL to absolute: {iframe_src} -> {absolute_url}", extra={"article_url": url})
                
                # Convert relative video URLs in embed tags
                for embed_tag in article_tag.find_all("embed"):
                    embed_src = embed_tag.get("src")
                    if embed_src:
                        # Convert relative URL to absolute
                        if not embed_src.startswith("http"):
                            absolute_url = urljoin(url, embed_src)
                            embed_tag["src"] = absolute_url
                            self.logger.debug(f"Converted relative embed URL to absolute: {embed_src} -> {absolute_url}", extra={"article_url": url})
                
                # Remove unwanted links (internal site links, social media links, etc.)
                for link_tag in article_tag.find_all("a"):
                    href = link_tag.get("href", "")
                    if href:
                        # Remove links to main site page
                        if href == "/" or href == "https://snn.ir/" or href == "https://snn.ir":
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed link to main site: {href}", extra={"article_url": url})
                        # Remove internal news links
                        elif href.startswith("/fa/news/") or href.startswith("https://snn.ir/fa/news/"):
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed internal news link: {href}", extra={"article_url": url})
                        # Remove social media links
                        elif any(social in href.lower() for social in ["facebook.com", "twitter.com", "instagram.com", "telegram.org", "t.me", "plus.google.com"]):
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed social media link: {href}", extra={"article_url": url})
                        # Remove tag/category links
                        elif href.startswith("/tag/") or href.startswith("/category/") or "/tags/" in href:
                            # Keep text, remove link
                            link_tag.unwrap()
                            self.logger.debug(f"Removed tag/category link: {href}", extra={"article_url": url})
                
                body_html = str(article_tag)
            
            # Extract image gallery/album if it exists (outside of article_tag to avoid conflicts)
            gallery_html = ""
            try:
                # Look for album container with class "album-box" or "image_set"
                gallery_container = soup.select_one(".album-box, .image_set, [class*='album-box']")
                if gallery_container:
                    # Find all image links in the gallery
                    gallery_links = gallery_container.find_all("a", href=lambda x: x and "/files/fa/news/" in str(x) and any(ext in str(x).lower() for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]))
                    
                    if gallery_links and len(gallery_links) >= 3:  # At least 3 images to be considered a gallery
                        # Create a gallery section
                        gallery_div = soup.new_tag("div", attrs={"class": "snn-image-gallery"})
                        
                        for link in gallery_links:
                            img_href = link.get("href", "")
                            if img_href:
                                # Convert relative URL to absolute
                                if not img_href.startswith("http"):
                                    img_href = urljoin(url, img_href)
                                
                                # Get the image tag inside the link (for alt text)
                                img_tag = link.find("img")
                                alt_text = link.get("title", "") or (img_tag.get("alt", "") if img_tag else "")
                                
                                # Use full image URL, not thumbnail
                                img_src = img_href
                                
                                # Create a gallery item
                                gallery_item = soup.new_tag("div", attrs={"class": "gallery-item"})
                                gallery_img = soup.new_tag("img", attrs={"src": img_src, "alt": alt_text, "loading": "lazy"})
                                gallery_item.append(gallery_img)
                                gallery_div.append(gallery_item)
                        
                        if len(gallery_div.find_all("img")) > 0:
                            gallery_html = str(gallery_div)
                            self.logger.debug(f"Found image gallery with {len(gallery_div.find_all('img'))} images", extra={"article_url": url})
            except Exception as e:
                self.logger.debug(f"Error extracting image gallery: {e}", extra={"article_url": url})
            
            # Append gallery HTML to body if it exists
            if gallery_html:
                body_html = body_html + gallery_html
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

            # Extract category from elements with class="hasBullet bullet4" within class="news_path"
            category = ""
            try:
                # First find the element with class="news_path"
                news_path_elem = soup.select_one(".news_path")
                if news_path_elem:
                    # Find all elements with class="hasBullet bullet4" within news_path
                    category_elements = news_path_elem.select(".hasBullet.bullet4")
                    if category_elements:
                        # Extract text from each element and join with " > "
                        parts = [elem.get_text(strip=True) for elem in category_elements if elem.get_text(strip=True)]
                        category = " > ".join(parts)
                        self.logger.debug(f"Found category from .news_path .hasBullet.bullet4: {category}", extra={"article_url": url})
            except Exception as e:
                self.logger.debug(f"Error extracting category from .news_path .hasBullet.bullet4: {e}", extra={"article_url": url})
            
            # Extract published date
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
                    if src and ("snn.ir" in src or "files.snn.ir" in src):
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
                normalized_category, preserved_raw_category = normalize_category("snn", raw_category)
                
                # Also truncate preserved_raw_category if needed
                if preserved_raw_category and len(preserved_raw_category) > 200:
                    preserved_raw_category = preserved_raw_category[:200].rstrip()
                
                # Create news article
                published_at = article_content.get("published_at") or rss_item.get("pubDate", "")
                
                # Normalize URL for storage
                normalized_url = rss_item["link"].rstrip('/').split('?')[0].split('#')[0]
                
                news = News(
                    source="snn",
                    title=article_content.get("title") or rss_item["title"],
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary") or rss_item.get("description", ""),
                    url=normalized_url,
                    published_at=published_at,
                    image_url=s3_image_url,
                    category=normalized_category,
                    raw_category=preserved_raw_category,
                    language="fa",  # Persian language
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
        Fetch news from SNN RSS feed.
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

