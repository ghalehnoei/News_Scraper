"""Tasnim News Agency worker implementation."""

import asyncio
import hashlib
import re
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
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

logger = setup_logging(source="tasnim")

# Archive page URL
TASNIM_ARCHIVE_URL = "https://www.tasnimnews.ir/fa/archive"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class TasnimWorker(BaseWorker):
    """Worker for Tasnim News Agency archive page."""

    def __init__(self):
        """Initialize Tasnim worker."""
        super().__init__("tasnim")
        self.archive_url = TASNIM_ARCHIVE_URL
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
            request_type: Type of request (listing, article, image) for logging

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
                            f"Successfully fetched {url}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                                "article_url": url if request_type == "article" else None,
                            }
                        )
                        return content
                    elif response.status == 404:
                        self.logger.warning(
                            f"404 Not Found: {url}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                            }
                        )
                        return None
                    else:
                        self.logger.warning(
                            f"HTTP {response.status} for {url}, attempt {attempt + 1}/{max_retries}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                            }
                        )
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Timeout fetching {url}, attempt {attempt + 1}/{max_retries}",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                    }
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                self.logger.warning(
                    f"Error fetching {url}: {e}, attempt {attempt + 1}/{max_retries}",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                    }
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    self.logger.error(
                        f"Failed to fetch {url} after {max_retries} attempts",
                        extra={
                            "source": self.source_name,
                            "request_type": request_type,
                            "article_url": url if request_type == "article" else None,
                        }
                    )
                    return None
        
        return None

    async def _parse_archive_page(self) -> list[dict]:
        """
        Parse the archive page to extract article links.

        Returns:
            List of dictionaries with article info (url, title if available)
        """
        content = await self._fetch_with_retry(self.archive_url, request_type="listing")
        if content is None:
            self.logger.error("Failed to fetch archive page")
            return []

        try:
            html_content = content.decode('utf-8')
        except UnicodeDecodeError:
            html_content = content.decode('utf-8', errors='ignore')

        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            articles = []
            found_links = set()
            
            # Find article links - Tasnim uses various patterns
            # Look for links in article containers
            article_containers = soup.find_all(['article', 'div'], class_=re.compile(r'article|news|item', re.I))
            
            for container in article_containers:
                links = container.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if not href:
                        continue
                    
                    # Normalize URL
                    if href.startswith('/'):
                        url = urljoin('https://www.tasnimnews.ir', href)
                    elif href.startswith('http'):
                        url = href
                    else:
                        continue
                    
                    # Only process Tasnim news URLs
                    if 'tasnimnews.ir' not in url:
                        continue
                    
                    # Check if it's a news article URL
                    # Tasnim URLs typically: /fa/news/YYYY/MM/DD/ID/title
                    is_news_url = (
                        '/news/' in url or
                        re.search(r'/fa/news/\d{4}/\d{2}/\d{2}/\d+', url)
                    )
                    
                    if not is_news_url:
                        continue
                    
                    # Skip non-news items
                    excluded_patterns = [
                        '/category/', '/tag/', '/author/', '/archive',
                        '/rss', '/feed', '/api', '/ajax'
                    ]
                    if any(skip in url.lower() for skip in excluded_patterns):
                        continue
                    
                    # Normalize URL (remove fragments, query params)
                    parsed = urlparse(url)
                    normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    
                    if normalized_url in found_links:
                        continue
                    
                    found_links.add(normalized_url)
                    
                    # Extract title
                    title = link.get_text(strip=True)
                    if not title or len(title) < 10:
                        # Try to find title in parent elements
                        parent = link.parent
                        for _ in range(5):
                            if parent:
                                title_elem = parent.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                                if title_elem:
                                    title = title_elem.get_text(strip=True)
                                    if title:
                                        break
                                parent = parent.parent
                    
                    articles.append({
                        "url": normalized_url,
                        "title": title,
                    })
                    self.logger.info(f"Found article URL: {normalized_url}")
            
            # Also try to find links directly
            if len(articles) == 0:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href', '')
                    if not href:
                        continue
                    
                    if href.startswith('/'):
                        url = urljoin('https://www.tasnimnews.ir', href)
                    elif href.startswith('http'):
                        url = href
                    else:
                        continue
                    
                    if 'tasnimnews.ir' not in url:
                        continue
                    
                    is_news_url = (
                        '/news/' in url or
                        re.search(r'/fa/news/\d{4}/\d{2}/\d{2}/\d+', url)
                    )
                    
                    if not is_news_url:
                        continue
                    
                    excluded_patterns = [
                        '/category/', '/tag/', '/author/', '/archive',
                        '/rss', '/feed', '/api', '/ajax'
                    ]
                    if any(skip in url.lower() for skip in excluded_patterns):
                        continue
                    
                    parsed = urlparse(url)
                    normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    
                    if normalized_url in found_links:
                        continue
                    
                    found_links.add(normalized_url)
                    
                    title = link.get_text(strip=True)
                    articles.append({
                        "url": normalized_url,
                        "title": title,
                    })
            
            # Remove duplicates
            seen_urls = set()
            unique_articles = []
            for article in articles:
                if article["url"] not in seen_urls:
                    seen_urls.add(article["url"])
                    unique_articles.append(article)

            self.logger.info(f"Parsed {len(unique_articles)} articles from archive page")
            return unique_articles

        except Exception as e:
            self.logger.error(f"Error parsing archive page: {e}", exc_info=True)
            return []

    def _parse_tasnim_date(self, date_str: str) -> Optional[str]:
        """
        Parse Tasnim date string and convert to ISO format.
        Adjusts time by subtracting 3.5 hours to convert to Tehran timezone.
        
        Args:
            date_str: Date string from Tasnim (e.g., "1404/10/08 14:30" or Persian date)
            
        Returns:
            ISO format date string (YYYY-MM-DDTHH:MM:SS) or None if parsing fails
        """
        if not date_str:
            return None
        
        try:
            # Try to parse ISO format dates
            iso_match = re.search(r'(\d{4})[/-](\d{2})[/-](\d{2})[T\s]?(\d{2}):(\d{2}):?(\d{2})?', date_str)
            if iso_match:
                year, month, day, hour, minute, second = iso_match.groups()
                second = second or "00"
                # Create datetime object and subtract 3.5 hours (3 hours 30 minutes)
                dt = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
                dt = dt - timedelta(hours=3, minutes=30)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            
            # Try to parse date without time
            date_match = re.search(r'(\d{4})[/-](\d{2})[/-](\d{2})', date_str)
            if date_match:
                year, month, day = date_match.groups()
                # No time adjustment needed if no time is present
                return f"{year}-{month}-{day}T00:00:00"
            
            # Try to parse time only (assume today's date)
            time_match = re.search(r'(\d{2}):(\d{2}):?(\d{2})?', date_str)
            if time_match:
                hour, minute, second = time_match.groups()
                second = second or "00"
                # Use current date
                now = datetime.now()
                dt = datetime(now.year, now.month, now.day, int(hour), int(minute), int(second))
                # Subtract 3.5 hours (3 hours 30 minutes)
                dt = dt - timedelta(hours=3, minutes=30)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            
            # If it's a Persian year (1404), try to extract and convert
            persian_year_match = re.search(r'(\d{4})[/-](\d{2})[/-](\d{2})', date_str)
            if persian_year_match:
                year, month, day = persian_year_match.groups()
                # Convert Persian year to Gregorian (approximate: add 621 or 622)
                try:
                    persian_year = int(year)
                    if 1300 <= persian_year <= 1500:  # Likely Persian year
                        gregorian_year = persian_year + 621
                        return f"{gregorian_year}-{month}-{day}T00:00:00"
                except ValueError:
                    pass
            
            return None
        except Exception as e:
            self.logger.debug(f"Error parsing date '{date_str}': {e}", extra={"date_str": date_str})
            return None

    async def _check_url_exists(self, url: str, db: AsyncSession) -> bool:
        """
        Check if article URL already exists in database.

        Args:
            url: Article URL
            db: Database session

        Returns:
            True if URL exists, False otherwise
        """
        result = await db.execute(select(News).where(News.url == url))
        return result.scalar_one_or_none() is not None

    async def _extract_article_content(self, url: str) -> Optional[dict]:
        """
        Fetch and extract article content from HTML page.

        Args:
            url: Article URL

        Returns:
            Dictionary with extracted content, or None if extraction failed
        """
        content = await self._fetch_with_retry(url, request_type="article")
        if content is None:
            return None

        try:
            html_content = content.decode('utf-8')
        except UnicodeDecodeError:
            html_content = content.decode('utf-8', errors='ignore')

        try:
            # Use selectolax for faster parsing
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

            # Extract article body using selectolax
            body_html = ""
            article_elem = tree.css_first('article')
            if not article_elem:
                # Try other common article containers
                article_elem = tree.css_first('div.article, div.content, div.news-body, div[class*="article"], div[class*="content"], div[class*="news-body"]')
            
            if article_elem:
                # Get HTML content
                body_html = article_elem.html
                
                # Parse again to clean unwanted elements
                body_tree = HTMLParser(body_html)
                
                # Remove script, style, and iframe tags
                for tag in body_tree.css('script, style, iframe'):
                    tag.decompose()
                
                # Remove ads and social media widgets
                unwanted_classes = ["ad", "advertisement", "social", "share", "related", "recommend"]
                for tag in body_tree.css('*'):
                    if tag.attributes:
                        class_attr = tag.attributes.get("class", "")
                        if class_attr:
                            class_lower = class_attr.lower()
                            if any(skip in class_lower for skip in unwanted_classes):
                                tag.decompose()
                
                body_html = body_tree.html
            else:
                self.logger.warning(f"Could not find article body content", extra={"article_url": url})

            # Extract summary from meta description
            summary = ""
            meta_desc = tree.css_first('meta[name="description"]')
            if meta_desc and meta_desc.attributes.get("content"):
                summary = meta_desc.attributes.get("content", "")

            # Extract category - use class "service" with selectolax
            category = ""
            # Method 1: Look for element with class "service"
            service_elems = tree.css('.service')
            if service_elems:
                # Get all service elements and join with >
                categories = []
                for service_elem in service_elems:
                    cat_text = service_elem.text(strip=True) if service_elem.text() else ""
                    if cat_text:
                        # Remove "اخبار" from the beginning of category
                        if cat_text.startswith("اخبار"):
                            cat_text = cat_text[5:].strip()  # Remove "اخبار" (5 characters) and strip
                        if cat_text and cat_text not in categories:
                            categories.append(cat_text)
                if categories:
                    category = " > ".join(categories)
                    self.logger.debug(f"Found category from service class: {category}", extra={"article_url": url})
            
            # Method 2: meta tag (fallback)
            if not category:
                category_tag = tree.css_first('meta[property="article:section"]')
                if category_tag and category_tag.attributes.get("content"):
                    category = category_tag.attributes.get("content", "")
                    if category:
                        # Remove "اخبار" from the beginning
                        if category.startswith("اخبار"):
                            category = category[5:].strip()
                        self.logger.debug(f"Found category from meta tag: {category}", extra={"article_url": url})
            
            # Method 3: breadcrumb navigation (fallback)
            if not category:
                breadcrumb = tree.css_first('nav.breadcrumb, nav[class*="breadcrumb"]')
                if breadcrumb:
                    links = breadcrumb.css('a')
                    if links:
                        # Join all breadcrumb links with >
                        categories = []
                        for link in links:
                            link_text = link.text(strip=True) if link.text() else ""
                            if link_text:
                                # Remove "اخبار" from the beginning
                                if link_text.startswith("اخبار"):
                                    link_text = link_text[5:].strip()
                                if link_text and link_text not in categories:
                                    categories.append(link_text)
                        if categories:
                            category = " > ".join(categories)
                            self.logger.debug(f"Found category from breadcrumb: {category}", extra={"article_url": url})
            
            if not category:
                self.logger.warning(f"Could not extract category", extra={"article_url": url})

            # Extract published date - use selectolax with XPath-like navigation
            # XPath: /html/body/main/main/section/div/div/div[1]/aside/section/article/div/ul/li[1]
            # Note: Category is also in the same ul, need to filter it out
            published_at = ""
            
            # Method 1: Use selectolax to find ul/li elements in article area
            try:
                # Try to find article > div > ul > li
                article_elem = tree.css_first('article')
                if article_elem:
                    article_div = article_elem.css_first('div')
                    if article_div:
                        ul = article_div.css_first('ul')
                        if ul:
                            lis = ul.css('li')
                            for li in lis:
                                li_text = li.text(strip=True) if li.text() else ""
                                if li_text:
                                    # Check if it looks like a date/time (not category)
                                    is_date = (
                                        re.search(r'\d{4}[/-]\d{2}[/-]\d{2}', li_text) or  # Date pattern
                                        re.search(r'\d{2}:\d{2}', li_text) or  # Time pattern
                                        (":" in li_text and re.search(r'\d', li_text)) or  # Time with numbers
                                        re.search(r'\d{4}', li_text)  # Year pattern
                                    )
                                    # Exclude if it's clearly a category
                                    is_category = any(cat in li_text for cat in [
                                        "سیاسی", "ورزشی", "بین الملل", "اقتصادی", "اجتماعی",
                                        "فرهنگی", "استانها", "فضا", "حوزه"
                                    ])
                                    if is_date and not is_category:
                                        # Try to parse and format the date
                                        published_at = self._parse_tasnim_date(li_text)
                                        if not published_at:
                                            published_at = li_text  # Fallback to raw text
                                        self.logger.debug(f"Found published_at from ul/li: {published_at}", extra={"article_url": url})
                                        break
            except Exception as xpath_error:
                self.logger.debug(f"Error using selectolax for date: {xpath_error}", extra={"article_url": url})
            
            # Method 2: Try to find ul/li anywhere (simpler approach)
            if not published_at:
                uls = tree.css('ul')
                for ul in uls:
                    lis = ul.css('li')
                    for li in lis:
                        li_text = li.text(strip=True) if li.text() else ""
                        if li_text:
                            # Check if it looks like a date/time (not category)
                            is_date = (
                                re.search(r'\d{4}[/-]\d{2}[/-]\d{2}', li_text) or
                                re.search(r'\d{2}:\d{2}', li_text) or
                                (":" in li_text and re.search(r'\d', li_text))
                            )
                            is_category = any(cat in li_text for cat in [
                                "سیاسی", "ورزشی", "بین الملل", "اقتصادی", "اجتماعی",
                                "فرهنگی", "استانها", "فضا", "حوزه"
                            ])
                            if is_date and not is_category:
                                # Try to parse and format the date
                                published_at = self._parse_tasnim_date(li_text)
                                if not published_at:
                                    published_at = li_text  # Fallback to raw text
                                self.logger.debug(f"Found published_at from ul/li (simplified): {published_at}", extra={"article_url": url})
                                break
                    if published_at:
                        break
            
            # Method 3: article:published_time meta tag (fallback)
            if not published_at:
                pub_date_meta = tree.css_first('meta[property="article:published_time"]')
                if pub_date_meta and pub_date_meta.attributes.get("content"):
                    published_at = pub_date_meta.attributes.get("content", "")
                    if published_at:
                        self.logger.debug(f"Found published_at from article:published_time: {published_at}", extra={"article_url": url})
            
            # Method 4: Extract from URL (Tasnim URLs contain date: /fa/news/YYYY/MM/DD/ID)
            if not published_at:
                url_parts = urlparse(url)
                path_parts = url_parts.path.split('/')
                # Tasnim format: /fa/news/1404/10/08/3483037/...
                if len(path_parts) >= 6 and path_parts[1] == "fa" and path_parts[2] == "news":
                    try:
                        year = path_parts[3]  # Persian year (e.g., 1404)
                        month = path_parts[4]
                        day = path_parts[5]
                        published_at = f"{year}-{month}-{day}"
                        self.logger.debug(f"Found published_at from URL: {published_at}", extra={"article_url": url})
                    except (IndexError, ValueError):
                        pass
            
            if not published_at:
                self.logger.warning(f"Could not extract published_at", extra={"article_url": url})

            # Extract main image - use selectolax
            # Priority: First image in article body > og:image > other methods
            image_url = ""
            
            # Method 1: First image in article body (highest priority)
            article_elem = tree.css_first('article')
            if article_elem:
                images = article_elem.css('img')
                for img in images:
                    src = img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy-src") or img.attributes.get("data-original", "")
                    if src:
                        # Skip small images, logos, icons
                        if any(skip in src.lower() for skip in ["logo", "icon", "avatar", "ad", "placeholder"]):
                            continue
                        # Make absolute URL
                        image_url = urljoin(url, src)
                        self.logger.debug(f"Found image from article body (first image): {image_url}", extra={"article_url": url})
                        break
            
            # Method 2: og:image meta tag (fallback)
            if not image_url:
                og_image = tree.css_first('meta[property="og:image"]')
                if og_image and og_image.attributes.get("content"):
                    image_url = og_image.attributes.get("content", "")
                    if image_url:
                        # Make absolute URL if relative
                        image_url = urljoin(url, image_url)
                        self.logger.debug(f"Found image from og:image: {image_url}", extra={"article_url": url})
            
            # Method 3: Use selectolax to find figure > a > img in article
            # Try to find article > div > div[2] > figure > a > img
            if not image_url:
                try:
                    if article_elem:
                        article_div = article_elem.css_first('div')
                        if article_div:
                            # Get all direct child divs and take the second one (div[2])
                            divs = article_div.css('div')
                            if len(divs) >= 2:
                                div2_elem = divs[1]  # Second div (index 1)
                                figure = div2_elem.css_first('figure')
                                if figure:
                                    a_tag = figure.css_first('a')
                                    if a_tag:
                                        img = a_tag.css_first('img')
                                        if img:
                                            src = img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy-src") or img.attributes.get("data-original", "")
                                            if src:
                                                image_url = urljoin(url, src)
                                                self.logger.debug(f"Found image from div[2] figure/a/img: {image_url}", extra={"article_url": url})
                            else:
                                # If div[2] not found, try to find figure directly in article_div or its children
                                all_divs = article_div.css('div')
                                for div_elem in all_divs:
                                    figure = div_elem.css_first('figure')
                                    if figure:
                                        a_tag = figure.css_first('a')
                                        if a_tag:
                                            img = a_tag.css_first('img')
                                            if img:
                                                src = img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy-src") or img.attributes.get("data-original", "")
                                                if src:
                                                    image_url = urljoin(url, src)
                                                    self.logger.debug(f"Found image from article_div child figure/a/img: {image_url}", extra={"article_url": url})
                                                    break
                                    if image_url:
                                        break
                except Exception as xpath_error:
                    self.logger.debug(f"Error using selectolax for image: {xpath_error}", extra={"article_url": url})
            
            # Method 4: Use CSS selector to find figure > a > img (simpler approach)
            if not image_url:
                figure_img = tree.css_first('figure a img')
                if figure_img:
                    src = figure_img.attributes.get("src") or figure_img.attributes.get("data-src") or figure_img.attributes.get("data-lazy-src") or figure_img.attributes.get("data-original", "")
                    if src:
                        # Skip small images, logos, icons
                        if not any(skip in src.lower() for skip in ["logo", "icon", "avatar", "ad", "placeholder"]):
                            image_url = urljoin(url, src)
                            self.logger.debug(f"Found image from CSS selector figure a img: {image_url}", extra={"article_url": url})
            
            # Method 5: Try to find figure > a > img in article area
            if not image_url:
                if article_elem:
                    figures = article_elem.css('figure')
                    for figure in figures:
                        a_tag = figure.css_first('a')
                        if a_tag:
                            img = a_tag.css_first('img')
                            if img:
                                src = img.attributes.get("src") or img.attributes.get("data-src") or img.attributes.get("data-lazy-src") or img.attributes.get("data-original", "")
                                if src:
                                    # Skip small images, logos, icons
                                    if not any(skip in src.lower() for skip in ["logo", "icon", "avatar", "ad", "placeholder"]):
                                        image_url = urljoin(url, src)
                                        self.logger.debug(f"Found image from article figure/a/img: {image_url}", extra={"article_url": url})
                                        break
            
            if not image_url:
                self.logger.warning(f"Could not extract article image", extra={"article_url": url})

            return {
                "title": title,
                "body_html": body_html,
                "summary": summary,
                "category": category,
                "published_at": published_at,
                "image_url": image_url,
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
        Download image from URL with rate limiting.

        Args:
            image_url: Image URL

        Returns:
            Image content as bytes, or None if download failed
        """
        if not image_url:
            return None

        self.logger.debug(f"Downloading image: {image_url}")
        content = await self._fetch_with_retry(image_url, request_type="image")
        if content is None:
            return None
        
        # Validate image content
        if len(content) < 4:
            return None
        
        # Check magic bytes for image formats
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
        Upload image to S3 storage.

        Args:
            image_data: Image content as bytes
            source: News source name
            url: Original image URL for context

        Returns:
            S3 key (path) if successful, None otherwise
        """
        await self._ensure_s3_initialized()
        
        try:
            # Generate unique filename
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

            s3_url = f"{settings.s3_endpoint}/{settings.s3_bucket}/{s3_path}"
            self.logger.info(f"Uploaded image to S3: {s3_url}")
            return s3_path  # Return only the S3 key

        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

    async def _save_article(self, listing_item: dict, article_content: dict, s3_image_url: str) -> None:
        """
        Save article to database.

        Args:
            listing_item: Listing page item data (url, title if available)
            article_content: Extracted article content
            s3_image_url: S3 URL for the image
        """
        async with AsyncSessionLocal() as db:
            try:
                # Check if URL already exists
                if await self._check_url_exists(listing_item["url"], db):
                    self.logger.debug(
                        f"Article already exists, skipping: {listing_item['url']}",
                        extra={"article_url": listing_item["url"]}
                    )
                    return

                # Get raw category
                raw_category = article_content.get("category", "")
                
                # Normalize category
                normalized_category, preserved_raw_category = normalize_category("tasnim", raw_category)
                
                # Create news article
                news = News(
                    source="tasnim",
                    title=article_content.get("title") or listing_item.get("title", ""),
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary", ""),
                    url=listing_item["url"],
                    published_at=article_content.get("published_at", ""),
                    image_url=s3_image_url,
                    category=normalized_category,
                    raw_category=preserved_raw_category,
                    language="fa",  # Persian language
                )

                db.add(news)
                await db.commit()

                self.logger.info(
                    f"Saved article: {news.title[:50]}...",
                    extra={"article_url": listing_item["url"]}
                )

            except Exception as e:
                await db.rollback()
                self.logger.error(
                    f"Error saving article to database: {e}",
                    extra={"article_url": listing_item["url"]},
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
        """Fetch and process news from Tasnim archive page."""
        self.logger.info("Starting Tasnim fetch cycle")

        # Ensure S3 is initialized
        await self._ensure_s3_initialized()

        try:
            # Parse archive page
            listing_items = await self._parse_archive_page()
            if not listing_items:
                self.logger.warning("No articles found on archive page")
                return

            self.logger.info(f"Found {len(listing_items)} articles to process")

            # Process each item
            processed = 0
            skipped_existing = 0
            failed = 0

            for idx, listing_item in enumerate(listing_items, 1):
                if not self.running:
                    break

                article_url = listing_item["url"]
                if not article_url:
                    self.logger.warning(f"Listing item {idx} has no URL, skipping")
                    continue

                self.logger.info(
                    f"Processing article {idx}/{len(listing_items)}: {listing_item.get('title', 'No title')[:50]}...",
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
                    if not article_content:
                        self.logger.warning(
                            f"Failed to extract content from article: {article_url}",
                            extra={"article_url": article_url}
                        )
                        failed += 1
                        continue

                    if not article_content.get("title") and not listing_item.get("title"):
                        self.logger.warning(
                            f"Article has no title, skipping: {article_url}",
                            extra={"article_url": article_url}
                        )
                        failed += 1
                        continue

                    # Download and upload main image
                    s3_image_url = ""
                    image_url = article_content.get("image_url", "")
                    if image_url:
                        self.logger.debug(f"Downloading main image: {image_url}")
                        image_data = await self._download_image(image_url)
                        if image_data:
                            s3_path = await self._upload_image_to_s3(
                                image_data, "tasnim", image_url
                            )
                            if s3_path:
                                s3_image_url = f"{settings.s3_endpoint}/{settings.s3_bucket}/{s3_path}"
                        else:
                            self.logger.debug(f"Failed to download main image: {image_url}")

                    # Save article to database
                    await self._save_article(listing_item, article_content, s3_image_url)
                    processed += 1
                    self.logger.info(
                        f"Successfully processed article: {article_content.get('title', listing_item.get('title', 'Unknown'))[:50]}...",
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
        """Cleanup resources."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        self.logger.info("Tasnim worker shutdown complete")

