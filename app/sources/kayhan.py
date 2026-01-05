"""Kayhan worker implementation."""

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

logger = setup_logging(source="kayhan")

# RSS feed URL
KAYHAN_RSS_URL = "https://kayhan.ir/fa/rss/allnews"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class KayhanWorker(BaseWorker):
    """Worker for Kayhan RSS feed."""

    def __init__(self):
        """Initialize Kayhan worker."""
        super().__init__("kayhan")
        self.rss_url = KAYHAN_RSS_URL
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
                    f"Timeout fetching {request_type}, attempt {attempt + 1}/{max_retries}: {url}",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                        "article_url": url,
                    }
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
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
            # Extract category from various RSS fields
            category = ""
            # Try tags first
            if entry.get("tags"):
                category = entry.get("tags", [{}])[0].get("term", "")
            # Try category field
            if not category and entry.get("category"):
                category = entry.get("category")
            # Try dc:subject (Dublin Core)
            if not category and hasattr(entry, 'dc_subject'):
                if isinstance(entry.dc_subject, list) and entry.dc_subject:
                    category = entry.dc_subject[0]
                elif isinstance(entry.dc_subject, str):
                    category = entry.dc_subject
            
            item = {
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "description": entry.get("description", ""),
                "pubDate": entry.get("published", ""),
                "category": category,
                "image_url": "",
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
                (News.source == "kayhan") & 
                (News.url == normalized_url)
            )
        )
        existing = result.scalar_one_or_none()
        
        # If not found with normalized URL, check original URL (for backward compatibility)
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "kayhan") & 
                    (News.url == url)
                )
            )
            existing = result.scalar_one_or_none()
        
        # If still not found, check if any stored URL starts with normalized URL
        # (for cases where URL was stored with query params)
        if not existing:
            result = await db.execute(
                select(News).where(
                    (News.source == "kayhan") & 
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

            html = content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html, 'html.parser')

            # Extract title - prefer og:title or h1
            title = ""
            og_title = soup.find("meta", attrs={"property": "og:title"})
            if og_title and og_title.get("content"):
                title = og_title["content"]
            else:
                h1_tag = soup.find("h1")
                if h1_tag:
                    title = h1_tag.get_text(strip=True)
            
            if not title:
                title_tag = soup.find("title")
                if title_tag:
                    title = title_tag.get_text(strip=True)

            # Extract article body - Priority 1: Manual navigation based on XPath structure
            # XPath: //*[@id="news"]/main/div/div/div/div[1]/div[2]/div[2]
            body_html = ""
            article_tag = None
            
            # Priority 1: Manual navigation based on XPath (highest priority)
            # XPath: //*[@id="news"]/main/div/div/div/div[1]/div[2]/div[2]
            self.logger.debug("Trying manual navigation based on XPath structure (priority 1)", extra={"article_url": url})
            news_elem = soup.find(id="news")
            if news_elem:
                self.logger.debug("Found #news element", extra={"article_url": url})
                main_elem = news_elem.find("main")
                if main_elem:
                    self.logger.debug("Found main element", extra={"article_url": url})
                    # Navigate: main > div > div > div > div[1] > div[2] > div[2]
                    # Use find_all with recursive=False to get direct children only
                    try:
                        # Level 1: main > div (first div)
                        divs_level1 = main_elem.find_all("div", recursive=False, limit=1)
                        if divs_level1:
                            level1 = divs_level1[0]
                            self.logger.debug(f"Found level1 div", extra={"article_url": url})
                            
                            # Level 2: div > div (first div)
                            divs_level2 = level1.find_all("div", recursive=False, limit=1)
                            if divs_level2:
                                level2 = divs_level2[0]
                                self.logger.debug(f"Found level2 div", extra={"article_url": url})
                                
                                # Level 3: div > div (first div)
                                divs_level3 = level2.find_all("div", recursive=False, limit=1)
                                if divs_level3:
                                    level3 = divs_level3[0]
                                    self.logger.debug(f"Found level3 div", extra={"article_url": url})
                                    
                                    # Level 4: div > div[1] (first div, index 0)
                                    divs_level4 = level3.find_all("div", recursive=False)
                                    if len(divs_level4) >= 1:
                                        level4 = divs_level4[0]  # div[1] (index 0)
                                        self.logger.debug(f"Found level4 div[1] (total: {len(divs_level4)})", extra={"article_url": url})
                                        
                                        # Level 5: div[1] > div[2] (second div, index 1)
                                        divs_level5 = level4.find_all("div", recursive=False)
                                        if len(divs_level5) >= 2:
                                            level5 = divs_level5[1]  # div[2] (index 1)
                                            self.logger.debug(f"Found level5 div[2] (total: {len(divs_level5)})", extra={"article_url": url})
                                            
                                            # Level 6: div[2] > div[2] (second div, index 1)
                                            divs_level6 = level5.find_all("div", recursive=False)
                                            if len(divs_level6) >= 2:
                                                level6 = divs_level6[1]  # div[2] (index 1)
                                                text_content = level6.get_text(strip=True)
                                                self.logger.debug(f"Found level6 div[2] (total: {len(divs_level6)}), text length: {len(text_content)}", extra={"article_url": url})
                                                if len(text_content) > 50:  # Lower threshold for XPath
                                                    article_tag = level6
                                                    self.logger.info(f"✓ Found article body using XPath navigation, text length: {len(text_content)}", extra={"article_url": url})
                                                else:
                                                    self.logger.warning(f"Level6 div found but text too short: {len(text_content)} chars", extra={"article_url": url})
                                            else:
                                                self.logger.debug(f"Level5 has {len(divs_level6)} direct div children, need at least 2", extra={"article_url": url})
                                        else:
                                            self.logger.debug(f"Level4 has {len(divs_level5)} direct div children, need at least 2", extra={"article_url": url})
                                    else:
                                        self.logger.debug(f"Level3 has {len(divs_level4)} direct div children, need at least 1", extra={"article_url": url})
                                else:
                                    self.logger.debug("Level2 has no direct div children", extra={"article_url": url})
                            else:
                                self.logger.debug("Level1 has no direct div children", extra={"article_url": url})
                        else:
                            self.logger.debug("Main element has no direct div children", extra={"article_url": url})
                    except Exception as e:
                        self.logger.warning(f"Error in XPath navigation: {e}", exc_info=True, extra={"article_url": url})
                else:
                    self.logger.debug("No main element found inside #news", extra={"article_url": url})
            else:
                self.logger.debug("No #news element found", extra={"article_url": url})
            
            # Priority 2: Try CSS selectors
            if not article_tag:
                self.logger.debug("Trying CSS selectors (priority 2)", extra={"article_url": url})
                article_selectors = [
                    "#news main",
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
                    "h1 + div",
                    "h1 ~ div",
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
                self.logger.debug("Trying to find content by text length", extra={"article_url": url})
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
                
                # Remove specific unwanted classes for Kayhan
                unwanted_classes = [
                    "ad", "advertisement", "social", "share",
                    "short_link_box",
                    "noprint",
                    "zxc_mb"
                ]
                
                # Remove elements with unwanted classes
                for tag in article_tag.find_all(class_=lambda x: x and any(skip in x.lower() for skip in unwanted_classes)):
                    tag.decompose()
                
                # Remove content after "انتهای پیام" (end of message marker)
                end_marker = article_tag.find(string=lambda text: text and "انتهای پیام" in text)
                if end_marker:
                    marker_parent = end_marker.parent
                    if marker_parent and marker_parent.parent:
                        # Get the parent container
                        container = marker_parent.parent
                        
                        # Get all children of the container
                        all_children = list(container.children)
                        
                        # Find the index of marker_parent
                        try:
                            marker_index = None
                            for i, child in enumerate(all_children):
                                # Check if this child is marker_parent or contains marker_parent
                                if child == marker_parent:
                                    marker_index = i
                                    break
                                # Also check if marker_parent is a descendant
                                if hasattr(child, 'find') and child.find(lambda tag: tag == marker_parent):
                                    marker_index = i
                                    break
                            
                            # Remove all children after marker_parent
                            if marker_index is not None:
                                removed_count = 0
                                # Work backwards to avoid index issues
                                for i in range(len(all_children) - 1, marker_index, -1):
                                    child = all_children[i]
                                    if hasattr(child, 'decompose'):
                                        try:
                                            child.decompose()
                                            removed_count += 1
                                        except:
                                            pass
                                
                                self.logger.debug(
                                    f"Removed {removed_count} elements after 'انتهای پیام' marker (kept marker itself)",
                                    extra={"article_url": url}
                                )
                        except Exception as e:
                            self.logger.warning(f"Error removing content after 'انتهای پیام': {e}", extra={"article_url": url})
                
                body_html = str(article_tag)
            else:
                # Last resort: try to find divs with multiple paragraphs
                self.logger.debug("Trying last resort: finding divs with multiple paragraphs", extra={"article_url": url})
                all_divs = soup.find_all('div')
                best_div = None
                max_paragraphs = 0
                
                for div in all_divs:
                    # Skip navigation, header, footer, sidebar
                    div_classes = ' '.join(div.get('class', [])).lower() if div.get('class') else ''
                    if any(skip in div_classes for skip in ['nav', 'menu', 'header', 'footer', 'sidebar', 'ad', 'social', 'share']):
                        continue
                    
                    paragraphs = div.find_all('p')
                    if len(paragraphs) >= 2:  # At least 2 paragraphs
                        text_length = div.get_text(strip=True)
                        if len(text_length) > 300:  # Substantial content
                            if len(paragraphs) > max_paragraphs:
                                max_paragraphs = len(paragraphs)
                                best_div = div
                
                if best_div:
                    article_tag = best_div
                    # Remove script and style tags
                    for tag in article_tag.find_all(["script", "style", "iframe"]):
                        tag.decompose()
                    
                    # Remove specific unwanted classes for Kayhan
                    unwanted_classes = [
                        "ad", "advertisement", "social", "share",
                        "short_link_box",
                        "noprint",
                        "zxc_mb"
                    ]
                    
                    # Remove elements with unwanted classes
                    for tag in article_tag.find_all(class_=lambda x: x and any(skip in x.lower() for skip in unwanted_classes)):
                        tag.decompose()
                    
                    body_html = str(article_tag)
                    self.logger.debug("Found article body using paragraph count method", extra={"article_url": url})
                else:
                    self.logger.warning(f"Could not find article body content", extra={"article_url": url})

            # For Kayhan, don't extract summary (leave it empty)
            summary = ""

            # Extract category and published date
            category = ""
            published_at = ""
            
            # Priority 1: Extract category from breadcrumb_list class (highest priority)
            breadcrumb_list = soup.find(class_="breadcrumb_list")
            if breadcrumb_list:
                self.logger.debug(f"Found breadcrumb_list element (priority 1)", extra={"article_url": url})
                # Extract category from links in breadcrumb_list
                category_links = breadcrumb_list.find_all("a")
                if category_links:
                    self.logger.debug(f"Found {len(category_links)} links in breadcrumb_list", extra={"article_url": url})
                    # Try last link first (category is usually the last item in breadcrumb)
                    for link in reversed(category_links):
                        link_text = link.get_text(strip=True)
                        # Skip common non-category text
                        skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", "", "صفحه نخست", "ایستگاه خبر"]
                        if link_text and link_text not in skip_texts:
                            # Also check href to skip home links
                            href = link.get("href", "")
                            if href and not any(skip in href.lower() for skip in ["/fa", "/", "index", "home", "#"]):
                                category = link_text
                                self.logger.debug(f"Found category from breadcrumb_list link (last): {category}", extra={"article_url": url})
                                break
                    
                    # If not found from last link, try all links from beginning
                    if not category:
                        for link in category_links:
                            link_text = link.get_text(strip=True)
                            skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", "", "صفحه نخست", "ایستگاه خبر"]
                            if link_text and link_text not in skip_texts:
                                href = link.get("href", "")
                                if href and not any(skip in href.lower() for skip in ["/fa", "/", "index", "home", "#"]):
                                    category = link_text
                                    self.logger.debug(f"Found category from breadcrumb_list link: {category}", extra={"article_url": url})
                                    break
                
                # If no category from links, try spans, divs, or li elements
                if not category:
                    category_elements = breadcrumb_list.find_all(["span", "div", "li", "p"])
                    self.logger.debug(f"Found {len(category_elements)} elements in breadcrumb_list", extra={"article_url": url})
                    for elem in category_elements:
                        elem_text = elem.get_text(strip=True)
                        # Skip if it looks like a date or time
                        if elem_text and not re.search(r'\d{4}.*\d{1,2}.*\d{1,2}', elem_text):
                            skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", "", "صفحه نخست", "ایستگاه خبر"]
                            if elem_text not in skip_texts and len(elem_text) > 2:
                                category = elem_text
                                self.logger.debug(f"Found category from breadcrumb_list element: {category}", extra={"article_url": url})
                                break
                
                # If still no category, try extracting from breadcrumb_list text directly
                if not category:
                    breadcrumb_text = breadcrumb_list.get_text(separator=" > ", strip=True)
                    self.logger.debug(f"breadcrumb_list full text: {breadcrumb_text}", extra={"article_url": url})
                    # Split by common separators
                    parts = re.split(r'[>|/\\\s]+', breadcrumb_text)
                    for part in reversed(parts):  # Start from end
                        part = part.strip()
                        # Skip if it looks like a date, time, or common words
                        if part and not re.search(r'\d{4}.*\d{1,2}.*\d{1,2}', part):
                            skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", "", "صفحه نخست", "ایستگاه خبر"]
                            if part not in skip_texts and len(part) > 2:
                                category = part
                                self.logger.debug(f"Found category from breadcrumb_list text: {category}", extra={"article_url": url})
                                break
            
            # Fallback 1: try breadcrumb noprint class if breadcrumb_list didn't provide category
            if not category:
                breadcrumb_noprint = soup.find(class_=lambda x: x and "breadcrumb" in x.lower() and "noprint" in x.lower())
                if breadcrumb_noprint:
                    self.logger.debug(f"Found breadcrumb noprint element (fallback 1)", extra={"article_url": url})
                    # Extract category from links in breadcrumb
                    category_links = breadcrumb_noprint.find_all("a")
                    if category_links:
                        self.logger.debug(f"Found {len(category_links)} links in breadcrumb noprint", extra={"article_url": url})
                        # Category is usually in one of the links (skip first link which is usually home)
                        for link in category_links:
                            link_text = link.get_text(strip=True)
                            # Skip common non-category text
                            skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", ""]
                            if link_text and link_text not in skip_texts:
                                category = link_text
                                self.logger.debug(f"Found category from breadcrumb noprint link: {category}", extra={"article_url": url})
                                break
                    
                    # If no category from links, try spans or divs
                    if not category:
                        category_elements = breadcrumb_noprint.find_all(["span", "div", "li"])
                        for elem in category_elements:
                            elem_text = elem.get_text(strip=True)
                            # Skip if it looks like a date or time
                            if elem_text and not re.search(r'\d{4}.*\d{1,2}.*\d{1,2}', elem_text):
                                skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", ""]
                                if elem_text not in skip_texts and len(elem_text) > 2:
                                    category = elem_text
                                    self.logger.debug(f"Found category from breadcrumb noprint element: {category}", extra={"article_url": url})
                                    break
            
            # Then try bread_time class for published date (and category if not found yet)
            bread_time = soup.find(class_="bread_time")
            if bread_time:
                self.logger.debug(f"Found bread_time element", extra={"article_url": url})
                
                # Only extract category from bread_time if not already found
                if not category:
                    # Method 1: Extract category from links in bread_time
                    category_links = bread_time.find_all("a")
                    if category_links:
                        self.logger.debug(f"Found {len(category_links)} links in bread_time", extra={"article_url": url})
                        # Category is usually in one of the links (skip first link which is usually home)
                        for link in category_links:
                            link_text = link.get_text(strip=True)
                            # Skip common non-category text
                            skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", ""]
                            if link_text and link_text not in skip_texts:
                                category = link_text
                                self.logger.debug(f"Found category from bread_time link: {category}", extra={"article_url": url})
                                break
                    
                    # Method 2: If no category from links, try spans or divs
                    if not category:
                        category_elements = bread_time.find_all(["span", "div", "li"])
                        for elem in category_elements:
                            elem_text = elem.get_text(strip=True)
                            # Skip if it looks like a date or time
                            if elem_text and not re.search(r'\d{4}.*\d{1,2}.*\d{1,2}', elem_text):
                                skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", ""]
                                if elem_text not in skip_texts and len(elem_text) > 2:
                                    category = elem_text
                                    self.logger.debug(f"Found category from bread_time element: {category}", extra={"article_url": url})
                                    break
                    
                    # Method 3: Extract from bread_time text directly (split by common separators)
                    if not category:
                        bread_text = bread_time.get_text(separator="|", strip=True)
                        self.logger.debug(f"bread_time text: {bread_text}", extra={"article_url": url})
                        # Split by common separators
                        parts = re.split(r'[|>\/\s]+', bread_text)
                        for part in parts:
                            part = part.strip()
                            # Skip if it looks like a date, time, or common words
                            if part and not re.search(r'\d{4}.*\d{1,2}.*\d{1,2}', part):
                                skip_texts = ["خانه", "صفحه اصلی", "Home", "خانه", "اخبار", "خبر", "", "بازگشت"]
                                if part not in skip_texts and len(part) > 2:
                                    category = part
                                    self.logger.debug(f"Found category from bread_time text split: {category}", extra={"article_url": url})
                                    break
                
                # Extract published date from bread_time
                # Date might be in text or in a specific element
                bread_text = bread_time.get_text(strip=True)
                # Look for date patterns in the text
                date_patterns = [
                    r'(\d{4}/\d{1,2}/\d{1,2})',  # 1403/10/12
                    r'(\d{1,2}/\d{1,2}/\d{4})',  # 12/10/1403
                    r'(\d{4}-\d{1,2}-\d{1,2})',  # 1403-10-12
                ]
                for pattern in date_patterns:
                    match = re.search(pattern, bread_text)
                    if match:
                        published_at = match.group(1)
                        break
                
                # Also check for time elements or spans within bread_time
                time_elements = bread_time.find_all(["time", "span", "div"])
                for elem in time_elements:
                    elem_text = elem.get_text(strip=True)
                    # Check if it looks like a date
                    if re.search(r'\d{4}.*\d{1,2}.*\d{1,2}', elem_text):
                        published_at = elem_text
                        break
                    # Check datetime attribute
                    if elem.get("datetime"):
                        published_at = elem.get("datetime")
                        break
            
            # Fallback: try meta tags if bread_time didn't provide category
            if not category:
                self.logger.debug("Category not found in bread_time, trying fallback methods", extra={"article_url": url})
                category_tag = soup.find("meta", attrs={"property": "article:section"})
                if category_tag and category_tag.get("content"):
                    category = category_tag["content"]
                    self.logger.debug(f"Found category from meta tag: {category}", extra={"article_url": url})
                else:
                    # Try to find category in breadcrumbs or navigation
                    breadcrumb = soup.find("nav", class_=re.compile("breadcrumb", re.I))
                    if breadcrumb:
                        links = breadcrumb.find_all("a")
                        if len(links) > 1:
                            category = links[-1].get_text(strip=True)
                            self.logger.debug(f"Found category from breadcrumb nav: {category}", extra={"article_url": url})
            
            if not category:
                self.logger.warning(f"Could not extract category from article page", extra={"article_url": url})

            # For Kayhan, use site logo instead of extracting images
            # Use the logo from kayhan.ir homepage
            image_url = "https://kayhan.ir/client/themes/fa/main/img/bolet_tele.gif"

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
            if content is None:
                return None

            # Validate image content
            magic_bytes = content[:4]
            if (
                magic_bytes.startswith(b'\xff\xd8') or  # JPEG
                magic_bytes.startswith(b'\x89PNG') or   # PNG
                magic_bytes.startswith(b'GIF8') or      # GIF
                magic_bytes.startswith(b'GIF9')         # GIF
            ):
                return content
            
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
            url: Article URL (to determine path)

        Returns:
            S3 path if successful, None otherwise
        """
        try:
            # Generate S3 path: news-images/{source}/{yyyy}/{mm}/{dd}/{filename}
            now = datetime.utcnow()
            
            # Generate safe filename using hash of URL to avoid special characters
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            
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
                normalized_category, preserved_raw_category = normalize_category("kayhan", raw_category)
                
                # Create news article
                # Use published_at from article_content (extracted from bread_time) if available,
                # otherwise fall back to RSS pubDate
                published_at = article_content.get("published_at") or rss_item.get("pubDate", "")
                
                # Normalize URL for storage (remove trailing slash and query parameters)
                normalized_url = rss_item["link"].rstrip('/').split('?')[0].split('#')[0]
                
                news = News(
                    source="kayhan",
                    title=article_content.get("title") or rss_item["title"],
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary") or rss_item.get("description", ""),
                    url=normalized_url,  # Store normalized URL
                    published_at=published_at,
                    image_url=s3_image_url,
                    category=normalized_category,  # Store normalized category
                    raw_category=preserved_raw_category,  # Store original category
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
        Fetch news from Kayhan RSS feed.
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

                # For Kayhan, use site logo URL directly (don't download/upload)
                # Use the logo URL from kayhan.ir
                s3_image_url = "https://kayhan.ir/client/themes/fa/main/img/bolet_tele.gif"

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

        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def cleanup(self) -> None:
        """Clean up resources."""
        if self.http_session and not self.http_session.closed:
            try:
                # Close connector explicitly with timeout
                if self.http_session.connector:
                    await asyncio.wait_for(
                        self.http_session.connector.close(),
                        timeout=2.0
                    )
                # Close session with timeout
                await asyncio.wait_for(
                    self.http_session.close(),
                    timeout=2.0
                )
            except (asyncio.TimeoutError, Exception) as e:
                self.logger.warning(f"Error closing HTTP session: {e}")
        
        self.logger.info("Kayhan worker cleanup complete")

