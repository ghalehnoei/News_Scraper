"""Varzesh3 worker implementation."""

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp
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

logger = setup_logging(source="varzesh3")

# News page URL (HTML scraping instead of RSS)
VARZESH3_NEWS_URL = "https://www.varzesh3.com/news"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class Varzesh3Worker(BaseWorker):
    """Worker for Varzesh3 RSS feed."""

    def __init__(self):
        """Initialize Varzesh3 worker."""
        super().__init__("varzesh3")
        self.news_url = VARZESH3_NEWS_URL
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

    async def _parse_news_page(self) -> list[dict]:
        """
        Parse news page HTML and extract article links.

        Returns:
            List of dictionaries containing article data
        """
        content = await self._fetch_with_retry(self.news_url, request_type="article")
        if content is None:
            self.logger.error("Failed to fetch news page")
            return []

        try:
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
            items = []
            
            # Find all news article links - they are in links with href starting with /news/
            # Looking for links like: /news/2235628/قلی-زاده-درباره-شادی-گل-بهتر-است-چیزی-نگویم
            news_links = soup.find_all('a', href=re.compile(r'/news/\d+/'))

            self.logger.info(f"Found {len(news_links)} potential news links on page")
            # Debug: show first few links
            for i, link in enumerate(news_links[:3]):
                self.logger.debug(f"Link {i+1}: {link.get('href')}")

            # Use a set to avoid duplicates
            seen_urls = set()
            
            for link in news_links:
                href = link.get('href', '')
                if not href or href in seen_urls:
                    continue
                
                # Make absolute URL
                if href.startswith('/'):
                    article_url = f"https://www.varzesh3.com{href}"
                elif href.startswith('http'):
                    article_url = href
                else:
                    article_url = urljoin(self.news_url, href)
                
                # Normalize URL (remove query params and fragments)
                article_url = article_url.split('?')[0].split('#')[0]
                
                if article_url in seen_urls:
                    continue
                
                seen_urls.add(article_url)
                
                # Extract title from link text or nearby elements
                title = link.get_text(strip=True)
                if not title:
                    # Try to find title in parent or sibling elements
                    parent = link.parent
                    if parent:
                        title_elem = parent.find(['h1', 'h2', 'h3', 'h4', 'strong', 'span'])
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                
                # Extract summary from paragraph near the link
                summary = ""
                # Look for paragraph in the same container
                parent = link.parent
                if parent:
                    p_tag = parent.find('p')
                    if p_tag:
                        summary = p_tag.get_text(strip=True)
                
                # Extract image URL if available
                image_url = ""
                # Look for img tag in the same container or nearby
                img_tag = link.find('img')
                if not img_tag and parent:
                    img_tag = parent.find('img')
                if img_tag:
                    img_src = img_tag.get('src') or img_tag.get('data-src') or img_tag.get('data-lazy-src', '')
                    if img_src:
                        if img_src.startswith('/'):
                            image_url = f"https://www.varzesh3.com{img_src}"
                        elif not img_src.startswith('http'):
                            image_url = urljoin(self.news_url, img_src)
                        else:
                            image_url = img_src
                
                # Extract time from nearby elements (look for time indicators)
                time_text = ""
                time_elem = parent.find(string=re.compile(r'\d+\s*(دقیقه|ساعت|روز)\s*پیش'))
                if time_elem:
                    time_text = time_elem.strip()
                
                item = {
                    "title": title,
                    "link": article_url,
                    "description": summary,
                    "pubDate": "",  # Will be extracted from article page
                    "category": "",  # Will be extracted from article page
                    "image_url": image_url,
                }
                
                items.append(item)
            
            self.logger.info(f"Found {len(items)} articles from news page")
            return items
        except Exception as e:
            self.logger.error(f"Error parsing news page: {e}", exc_info=True)
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
                (News.source == "varzesh3") & 
                (News.url == normalized_url)
            )
        )
        existing = result.scalar_one_or_none()
        
        # If not found with normalized URL, check original URL (for backward compatibility)
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "varzesh3") & 
                    (News.url == url)
                )
            )
            existing = result.scalar_one_or_none()
        
        # If still not found, check if any stored URL starts with normalized URL
        # (for cases where URL was stored with query params)
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "varzesh3") & 
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

            # Extract article body
            body_html = ""
            article_tag = None
            
            # Try multiple selectors for Varzesh3
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
            
            # If still not found, try to find the main content area by looking for divs with substantial text
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
                
                # Remove specific unwanted classes for Varzesh3
                unwanted_classes = [
                    "ad", "advertisement", "social", "share",
                    "short_link_box",
                    "noprint",
                    "zxc_mb",
                    "comment",
                    "comments",
                    "related",
                    "sidebar"
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

            # Extract category and published date
            category = "sports"  # Varzesh3 is a sports news website, so all articles are sports
            published_at = ""

            # Extract published date from the info section
            # Look for pattern like "کد: 2235628 | 12 دی 1404 ساعت 00:10 | 7.2K بازدید"
            info_text = ""

            # Try multiple approaches to find the date info
            # Approach 1: Look for the "کد:" text
            info_elem = soup.find(string=lambda text: text and "کد:" in text and "|" in text)
            if info_elem:
                # Get the parent element containing the info
                parent = info_elem.find_parent()
                if parent:
                    info_text = parent.get_text(strip=True)
                    self.logger.debug("Found info text from کد: pattern: " + info_text, extra={"article_url": url})

            # Approach 2: If not found, look for any element containing the date pattern
            if not info_text:
                # Find elements that contain date-like text
                import re
                date_elements = soup.find_all(string=re.compile(r'\d{1,2}\s*دی\s*\d{4}\s*ساعت'))
                for elem in date_elements:
                    parent = elem.find_parent()
                    if parent:
                        info_text = parent.get_text(strip=True)
                        self.logger.debug("Found info text from date pattern: " + info_text, extra={"article_url": url})
                        break

            # Approach 3: Search the entire page text
            if not info_text:
                all_text = soup.get_text()
                # Look for the pattern in the entire text
                lines = all_text.split('\n')
                for line in lines:
                    if 'کد:' in line and 'ساعت' in line:
                        info_text = line.strip()
                        self.logger.debug("Found info text from page text: " + info_text, extra={"article_url": url})
                        break

            if info_text:
                # Parse the Persian date from the info text
                # Format: "کد: 2235628 | 12 دی 1404 ساعت 00:10 | 29 بازدید"
                import re
                date_match = re.search(r'(\d{1,2})\s*(دی|بهمن|اسفند|فروردین|اردیبهشت|خرداد|تیر|مرداد|شهریور|مهر|آبان|آذر)\s*(\d{4})\s*ساعت\s*(\d{1,2}):(\d{2})', info_text)
                if date_match:
                    day, month_name, year, hour, minute = date_match.groups()
                    self.logger.debug(f"Date match groups: {date_match.groups()}", extra={"article_url": url})

                    # Convert Persian month to number
                    persian_months = {
                        'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4, 'مرداد': 5, 'شهریور': 6,
                        'مهر': 7, 'آبان': 8, 'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12
                    }

                    month = persian_months.get(month_name, 1)
                    self.logger.debug(f"Month conversion: {month_name} -> {month}", extra={"article_url": url})

                    # Convert to Gregorian date (simplified conversion)
                    # For production, you might want to use a proper Persian calendar library
                    try:
                        # Simple approximation: Persian year is about 621 years before Gregorian
                        gregorian_year = int(year) + 621

                        # Adjust for month differences (approximate)
                        if month <= 6:  # First 6 months
                            gregorian_month = month + 3
                        else:  # Last 6 months
                            gregorian_month = month - 9
                            gregorian_year += 1

                        published_at = f"{gregorian_year}-{gregorian_month:02d}-{int(day):02d}T{hour}:{minute}:00"
                        self.logger.debug("Parsed Persian date: " + published_at, extra={"article_url": url})
                    except Exception as e:
                        self.logger.debug(f"Error parsing Persian date: {e}", extra={"article_url": url})
                else:
                    self.logger.debug("No date match found in: " + info_text, extra={"article_url": url})
                    # Try alternative patterns
                    alt_match = re.search(r'(\d{1,2})\s*(\w+)\s*(\d{4})\s*ساعت\s*(\d{1,2}):(\d{2})', info_text)
                    if alt_match:
                        self.logger.debug(f"Alternative date match: {alt_match.groups()}", extra={"article_url": url})
            else:
                self.logger.debug("No info element found with any method", extra={"article_url": url})

            # Fallback: try meta tags
            if not published_at:
                meta_date = soup.find("meta", attrs={"property": "article:published_time"})
                if meta_date and meta_date.get("content"):
                    try:
                        published_at = datetime.fromisoformat(meta_date["content"].replace('Z', '+00:00')).isoformat()
                    except Exception:
                        pass

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
            
            # Priority 2: Look for main article image (usually the first large image after title)
            if not image_url:
                # Find the main image which is usually right after the title and summary
                # Look for img tags in the content area
                content_area = soup.find("div", class_=lambda x: x and ("content" in str(x).lower() or "news" in str(x).lower()))
                if content_area:
                    images = content_area.find_all("img")
                    for img in images:
                        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or img.get("data-original", "")
                        if src:
                            # Skip small images, logos, icons, ads
                            src_lower = src.lower()
                            if any(skip in src_lower for skip in ["logo", "icon", "avatar", "ad", "banner", "sponsor", "social", "thumb"]):
                                continue
                            # Check if this looks like a main article image
                            if "/news/" in src or "/files/" in src or "/images/" in src:
                                image_url = make_absolute_url(src)
                                self.logger.debug(f"Found main article image: {image_url}", extra={"article_url": url})
                                break

            # Priority 3: Look for image in article content
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
            
            # Priority 3: Look for any large image on the page
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
                            if "/news/" in src or "/image/" in src:
                                image_url = make_absolute_url(src)
                                self.logger.debug(f"Found potential main image: {image_url}", extra={"article_url": url})
                                break

            self.logger.debug("Final extracted data - published_at: " + str(published_at) + ", category: " + category, extra={"article_url": url})

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

    async def _save_article(self, news_item: dict, article_content: dict, s3_image_url: str) -> None:
        """
        Save article to database.

        Args:
            news_item: News item data from HTML page
            article_content: Extracted article content
            s3_image_url: S3 URL for the image
        """
        async with AsyncSessionLocal() as db:
            try:
                # Check if URL already exists (with source filter)
                # Logging is handled inside _check_url_exists
                if await self._check_url_exists(news_item["link"], db):
                    return

                # Get raw category
                raw_category = article_content.get("category") or news_item.get("category", "")
                
                # Normalize category
                normalized_category, preserved_raw_category = normalize_category("varzesh3", raw_category)
                
                # Create news article
                # Use published_at from article_content (extracted from page) if available,
                # otherwise fall back to news_item pubDate
                published_at = article_content.get("published_at") or news_item.get("pubDate", "")
                
                # Normalize URL for storage (remove trailing slash and query parameters)
                normalized_url = news_item["link"].rstrip('/').split('?')[0].split('#')[0]
                
                news = News(
                    source="varzesh3",
                    title=article_content.get("title") or news_item["title"],
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary") or news_item.get("description", ""),
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
                    extra={"article_url": news_item["link"]}
                )
            except Exception as e:
                self.logger.error(
                    f"Error saving article to database: {e}",
                    extra={"article_url": news_item["link"]},
                    exc_info=True
                )
                await db.rollback()
                raise

    async def fetch_news(self) -> None:
        """
        Fetch news from Varzesh3 RSS feed.
        """
        if not self.running:
            return

        try:
            # Parse news page HTML
            news_items = await self._parse_news_page()
            
            if not news_items:
                self.logger.warning("No items found on news page")
                return

            self.logger.info(f"Processing {len(news_items)} articles from news page")

            # Process each article
            for idx, news_item in enumerate(news_items, 1):
                if not self.running:
                    self.logger.info("Worker stopped, cancelling fetch operation")
                    break

                self.logger.info(
                    f"Processing article {idx}/{len(news_items)}: {news_item['title'][:50]}...",
                    extra={"article_url": news_item["link"]}
                )

                # Quick check if URL already exists before extracting content
                async with AsyncSessionLocal() as quick_db:
                    if await self._check_url_exists(news_item["link"], quick_db):
                        self.logger.debug(
                            f"Skipping article (already exists): {news_item['title'][:50]}...",
                            extra={"article_url": news_item["link"]}
                        )
                        continue

                # Extract article content
                article_content = await self._extract_article_content(news_item["link"])
                if not article_content:
                    self.logger.warning(
                        f"Failed to extract content for article: {news_item['link']}",
                        extra={"article_url": news_item["link"]}
                    )
                    continue

                # Download and upload image if available
                # Priority: article_content (from HTML) > news_item image_url
                s3_image_url = ""
                image_url = article_content.get("image_url") or news_item.get("image_url", "")
                if image_url:
                    self.logger.debug(
                        f"Processing main image for article: {image_url}",
                        extra={"article_url": news_item["link"]}
                    )
                    image_data = await self._download_image(image_url)
                    if image_data:
                        self.logger.debug(
                            f"Downloaded main image ({len(image_data)} bytes), uploading to S3...",
                            extra={"article_url": news_item["link"]}
                        )
                        s3_image_url = await self._upload_image_to_s3(
                            image_data, self.source_name, news_item["link"]
                        )
                        if s3_image_url:
                            self.logger.info(
                                f"Successfully uploaded main image to S3: {s3_image_url}",
                                extra={"article_url": news_item["link"]}
                            )

                # Save article to database
                try:
                    await self._save_article(news_item, article_content, s3_image_url)
                    self.logger.info(
                        f"Successfully processed article: {news_item['title'][:50]}...",
                        extra={"article_url": news_item["link"]}
                    )
                except Exception as e:
                    self.logger.error(
                        f"Error saving article: {e}",
                        extra={"article_url": news_item["link"]},
                        exc_info=True
                    )

                # Small delay between articles to avoid overwhelming the server
                await asyncio.sleep(0.5)
                
                # Yield control to allow cancellation
                await asyncio.sleep(0)

        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

