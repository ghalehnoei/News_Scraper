"""Mizan worker implementation."""

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

logger = setup_logging(source="mizan")

# RSS feed URL
MIZAN_RSS_URL = "https://www.mizanonline.ir/fa/rss/allnews"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class MizanWorker(BaseWorker):
    """Worker for Mizan News Agency RSS feed."""

    def __init__(self):
        """Initialize Mizan worker."""
        super().__init__("mizan")
        self.rss_url = MIZAN_RSS_URL
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

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self.http_session and not self.http_session.closed:
            try:
                # Close connector connections
                if self.http_session.connector:
                    await self.http_session.connector.close()
                # Close session with timeout
                await asyncio.wait_for(self.http_session.close(), timeout=2.0)
            except Exception as e:
                self.logger.warning(f"Error closing HTTP session: {e}")
            finally:
                self.http_session = None

    async def close_db(self) -> None:
        """Close database connections."""
        # AsyncSessionLocal handles connection pooling, no explicit close needed
        pass

    async def close_s3(self) -> None:
        """Close S3 connections."""
        # S3 session is managed by get_s3_session, no explicit close needed
        pass

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
                                    entry[date_field], "%a, %d %b %Y %H:%M:%S %z"
                                ).isoformat()
                                break
                            except Exception:
                                try:
                                    item["pubDate"] = datetime.strptime(
                                        entry[date_field], "%d %b %Y %H:%M:%S %z"
                                    ).isoformat()
                                    break
                                except Exception:
                                    continue
                
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
        # This must match the normalization in _save_article
        normalized_url = url.rstrip('/').split('?')[0].split('#')[0]
        
        # Check with source filter to avoid conflicts with other sources
        # Since we always store normalized URLs, check normalized URL first
        result = await db.execute(
            select(News).where(
                (News.source == "mizan") & 
                (News.url == normalized_url)
            )
        )
        existing = result.scalar_one_or_none()
        
        # If not found with normalized URL, check original URL (for backward compatibility)
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "mizan") & 
                    (News.url == url)
                )
            )
            existing = result.scalar_one_or_none()
        
        # If still not found, check if any stored URL starts with normalized URL
        # (for cases where URL was stored with query params)
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "mizan") & 
                    (News.url.like(f"{normalized_url}%"))
                )
            )
            existing = result.scalar_one_or_none()
        
        if existing:
            self.logger.info(
                f"Article already exists in database (skipping): {url} (normalized: {normalized_url}, matched: {existing.url})",
                extra={"article_url": url}
            )
        else:
            self.logger.debug(
                f"Article not found in database, will save: {url} (normalized: {normalized_url})",
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
            h1_tag = soup.find("h1")
            if h1_tag:
                title = h1_tag.get_text(strip=True)
            else:
                # Fallback to meta title
                meta_title = soup.find("meta", attrs={"property": "og:title"})
                if meta_title and meta_title.get("content"):
                    title = meta_title["content"]
                else:
                    title_tag = soup.find("title")
                    if title_tag:
                        title = title_tag.get_text(strip=True)

            # Extract article body - Priority 1: Manual navigation based on XPath structure
            # XPath: //*[@id="maintbl"]/div[1]/div[2]
            body_html = ""
            article_tag = None
            
            # Priority 1: Manual navigation based on XPath (highest priority)
            self.logger.debug("Trying manual navigation based on XPath structure (priority 1)", extra={"article_url": url})
            maintbl_elem = soup.find(id="maintbl")
            if maintbl_elem:
                self.logger.debug("Found #maintbl element", extra={"article_url": url})
                # Navigate: maintbl > div[1] > div[2]
                # Use find_all with recursive=False to get direct children only
                try:
                    # Level 1: maintbl > div[1] (first div, index 0)
                    divs_level1 = maintbl_elem.find_all("div", recursive=False)
                    if len(divs_level1) >= 1:
                        level1 = divs_level1[0]  # div[1] (index 0)
                        self.logger.debug(f"Found level1 div[1] (total: {len(divs_level1)})", extra={"article_url": url})
                        
                        # Level 2: div[1] > div[2] (second div, index 1)
                        divs_level2 = level1.find_all("div", recursive=False)
                        if len(divs_level2) >= 2:
                            level2 = divs_level2[1]  # div[2] (index 1)
                            text_content = level2.get_text(strip=True)
                            self.logger.debug(f"Found level2 div[2] (total: {len(divs_level2)}), text length: {len(text_content)}", extra={"article_url": url})
                            if len(text_content) > 50:  # Lower threshold for XPath
                                article_tag = level2
                                self.logger.info(f"✓ Found article body using XPath navigation, text length: {len(text_content)}", extra={"article_url": url})
                            else:
                                self.logger.warning(f"Level2 div found but text too short: {len(text_content)} chars", extra={"article_url": url})
                        else:
                            self.logger.debug(f"Level1 has {len(divs_level2)} direct div children, need at least 2", extra={"article_url": url})
                    else:
                        self.logger.debug(f"#maintbl has {len(divs_level1)} direct div children, need at least 1", extra={"article_url": url})
                except Exception as e:
                    self.logger.warning(f"Error in XPath navigation: {e}", exc_info=True, extra={"article_url": url})
            else:
                self.logger.debug("No #maintbl element found", extra={"article_url": url})
            
            # Priority 2: Try CSS selectors
            if not article_tag:
                self.logger.debug("Trying CSS selectors (priority 2)", extra={"article_url": url})
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
            
            # Priority 3: Try to find the main content area by looking for divs with substantial text
            if not article_tag:
                # Find h1 first
                h1_tag = soup.find("h1")
                if h1_tag:
                    # Look for divs after h1 that contain substantial text
                    current = h1_tag.next_sibling
                    while current:
                        if hasattr(current, 'name') and current.name == 'div':
                            text_content = current.get_text(strip=True)
                            # Check if this div has substantial content and doesn't look like navigation/menu
                            if len(text_content) > 200 and not any(skip in str(current.get('class', [])).lower() for skip in ['nav', 'menu', 'header', 'footer', 'sidebar']):
                                article_tag = current
                                self.logger.debug("Found article body by searching after h1", extra={"article_url": url})
                                break
                        current = current.next_sibling if hasattr(current, 'next_sibling') else None
                
                # If still not found, try finding parent of h1 and look for content divs
                if not article_tag and h1_tag:
                    parent = h1_tag.parent
                    if parent:
                        # Look for divs with substantial text in the parent
                        content_divs = parent.find_all('div', recursive=False)
                        for div in content_divs:
                            text_content = div.get_text(strip=True)
                            if len(text_content) > 200:
                                article_tag = div
                                self.logger.debug("Found article body in parent container", extra={"article_url": url})
                                break

            if article_tag:
                # Remove script and style tags
                for tag in article_tag.find_all(["script", "style", "iframe"]):
                    tag.decompose()
                
                # Remove specific unwanted classes for Mizan
                unwanted_classes = [
                    "ad", "advertisement", "social", "share",
                    "short_link_box",
                    "noprint",
                    "zxc_mb",
                    "comment",
                    "comments"
                ]
                
                # Remove elements with unwanted classes
                for tag in article_tag.find_all(class_=lambda x: x and any(skip in x.lower() for skip in unwanted_classes)):
                    tag.decompose()
                
                # Remove content after "انتهای پیام" (end of message marker)
                end_marker = article_tag.find(string=lambda text: text and "انتهای پیام" in text)
                if end_marker:
                    marker_parent = end_marker.find_parent()
                    if marker_parent:
                        # Remove all siblings after the marker parent
                        for sibling in list(marker_parent.next_siblings):
                            if hasattr(sibling, 'decompose'):
                                sibling.decompose()
                        # Keep the marker parent itself but remove its content after the marker
                        marker_text = marker_parent.get_text()
                        if "انتهای پیام" in marker_text:
                            # Find the position of "انتهای پیام" in the text
                            marker_index = marker_text.find("انتهای پیام")
                            if marker_index > 0:
                                # Remove text after "انتهای پیام" from the marker parent
                                for text_node in marker_parent.find_all(string=True):
                                    if "انتهای پیام" in text_node:
                                        # Split the text and keep only up to "انتهای پیام"
                                        parts = text_node.split("انتهای پیام", 1)
                                        if len(parts) > 1:
                                            text_node.replace_with(parts[0] + "انتهای پیام")
                
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

            # Extract category and published date from breadcrumb
            category = ""
            published_at = ""
            
            # Try to find breadcrumb for category
            breadcrumb = soup.find(class_=lambda x: x and ("breadcrumb" in str(x).lower() or "bread" in str(x).lower()))
            if breadcrumb:
                # Extract category from breadcrumb links (usually the last link before the article title)
                breadcrumb_links = breadcrumb.find_all("a")
                if breadcrumb_links:
                    # Skip first link (usually home) and last link (usually current page)
                    for link in breadcrumb_links[1:-1] if len(breadcrumb_links) > 2 else breadcrumb_links[1:]:
                        link_text = link.get_text(strip=True)
                        skip_texts = ["خانه", "صفحه اصلی", "Home", "اخبار", "خبر"]
                        if link_text and link_text not in skip_texts:
                            category = link_text
                            self.logger.debug(f"Found category from breadcrumb: {category}", extra={"article_url": url})
                            break

            # Extract published date from meta or breadcrumb
            meta_date = soup.find("meta", attrs={"property": "article:published_time"})
            if meta_date and meta_date.get("content"):
                try:
                    published_at = datetime.fromisoformat(meta_date["content"].replace('Z', '+00:00')).isoformat()
                except Exception:
                    pass
            
            # Fallback: try to find date in breadcrumb or article header
            if not published_at:
                date_elem = soup.find(class_=lambda x: x and ("date" in str(x).lower() or "time" in str(x).lower()))
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    # Try to parse Persian date format
                    # This is a simplified parser - you may need to enhance it
                    self.logger.debug(f"Found date text: {date_text}", extra={"article_url": url})

            # Extract main image - Priority 1: og:image meta tag
            image_url = ""
            og_image = soup.find("meta", attrs={"property": "og:image"})
            if og_image and og_image.get("content"):
                img_src = og_image["content"]
                # Make absolute URL if relative
                if img_src.startswith("/"):
                    parsed_url = urlparse(url)
                    image_url = f"{parsed_url.scheme}://{parsed_url.netloc}{img_src}"
                elif not img_src.startswith("http"):
                    image_url = urljoin(url, img_src)
                else:
                    image_url = img_src
                self.logger.debug(f"Found image from og:image: {image_url}", extra={"article_url": url})
            
            # Helper function to make absolute URL
            def make_absolute_url(src: str) -> str:
                """Convert relative URL to absolute URL."""
                if not src:
                    return ""
                if src.startswith("http://") or src.startswith("https://"):
                    return src
                elif src.startswith("//"):
                    parsed_url = urlparse(url)
                    return f"{parsed_url.scheme}:{src}"
                elif src.startswith("/"):
                    parsed_url = urlparse(url)
                    return f"{parsed_url.scheme}://{parsed_url.netloc}{src}"
                else:
                    return urljoin(url, src)
            
            # Priority 2: Look for image in article content (#maintbl > div[1] > div[2])
            if not image_url and article_tag:
                # Try to find first image in article_tag
                images = article_tag.find_all("img")
                for img in images:
                    src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or img.get("data-original", "")
                    if src:
                        # Skip small images, logos, icons, ads
                        src_lower = src.lower()
                        if any(skip in src_lower for skip in ["logo", "icon", "avatar", "ad", "banner", "sponsor"]):
                            continue
                        # Make absolute URL
                        image_url = make_absolute_url(src)
                        self.logger.debug(f"Found image in article content: {image_url}", extra={"article_url": url})
                        break
            
            # Priority 3: Look for image in #maintbl structure
            if not image_url:
                maintbl_elem = soup.find(id="maintbl")
                if maintbl_elem:
                    divs_level1 = maintbl_elem.find_all("div", recursive=False)
                    if len(divs_level1) >= 1:
                        level1 = divs_level1[0]
                        divs_level2 = level1.find_all("div", recursive=False)
                        if len(divs_level2) >= 2:
                            level2 = divs_level2[1]
                            # Find first image in level2
                            img_tag = level2.find("img")
                            if img_tag:
                                src = img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-lazy-src") or img_tag.get("data-original", "")
                                if src:
                                    src_lower = src.lower()
                                    if not any(skip in src_lower for skip in ["logo", "icon", "avatar", "ad", "banner", "sponsor"]):
                                        image_url = make_absolute_url(src)
                                        self.logger.debug(f"Found image in #maintbl structure: {image_url}", extra={"article_url": url})
            
            # Priority 4: Look for any large image on the page
            if not image_url:
                all_images = soup.find_all("img")
                for img in all_images:
                    src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or img.get("data-original", "")
                    if src:
                        src_lower = src.lower()
                        # Skip small images, logos, icons, ads
                        if any(skip in src_lower for skip in ["logo", "icon", "avatar", "ad", "banner", "sponsor", "social"]):
                            continue
                        # Check image dimensions if available
                        width = img.get("width")
                        height = img.get("height")
                        if width and height:
                            try:
                                w = int(width)
                                h = int(height)
                                if w > 200 and h > 200:  # Large enough to be main image
                                    image_url = make_absolute_url(src)
                                    self.logger.debug(f"Found large image: {image_url} ({w}x{h})", extra={"article_url": url})
                                    break
                            except (ValueError, TypeError):
                                pass
                        else:
                            # If no dimensions, check if URL looks like main image
                            if "/news/" in src or "/files/" in src:
                                image_url = make_absolute_url(src)
                                self.logger.debug(f"Found potential main image: {image_url}", extra={"article_url": url})
                                break

            return {
                "title": title,
                "body_html": body_html,
                "summary": summary,
                "category": category,
                "image_url": image_url,
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
                    ExtraArgs={"ContentType": f"image/{ext[1:]}"}
                )
            
            # Return full S3 URL or path
            s3_url = f"{settings.s3_endpoint}/{settings.s3_bucket}/{s3_key}"
            self.logger.debug(f"Uploaded image to S3: {s3_url}", extra={"article_url": url})
            return s3_key  # Return just the key, not full URL
        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", exc_info=True, extra={"article_url": url})
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
                # Check if URL already exists (with source filter)
                # Logging is handled inside _check_url_exists
                if await self._check_url_exists(rss_item["link"], db):
                    return

                # Get raw category
                raw_category = article_content.get("category") or rss_item.get("category", "")
                
                # Normalize category
                normalized_category, preserved_raw_category = normalize_category("mizan", raw_category)
                
                # Create news article
                # Use published_at from article_content (extracted from page) if available,
                # otherwise fall back to RSS pubDate
                published_at = article_content.get("published_at") or rss_item.get("pubDate", "")
                
                # Normalize URL for storage (remove trailing slash and query parameters)
                normalized_url = rss_item["link"].rstrip('/').split('?')[0].split('#')[0]
                
                news = News(
                    source="mizan",
                    title=article_content.get("title") or rss_item["title"],
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary") or rss_item.get("description", ""),
                    url=normalized_url,  # Store normalized URL
                    published_at=published_at,
                    image_url=s3_image_url,
                    category=normalized_category,  # Store normalized category
                    raw_category=preserved_raw_category,  # Store original category
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
        Fetch news from Mizan RSS feed.
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
                # Priority: article_content (from HTML) > RSS enclosure > RSS image_url
                s3_image_url = ""
                image_url = article_content.get("image_url") or rss_item.get("image_url", "")
                if not image_url and rss_item.get("image_url"):
                    # Try RSS image_url as fallback
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

                # Small delay between articles to avoid overwhelming the server
                await asyncio.sleep(0.5)
                
                # Yield control to allow cancellation
                await asyncio.sleep(0)

        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

