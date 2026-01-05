"""Fars News Agency worker implementation."""

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import Optional
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

logger = setup_logging(source="fars")

# Listing page URL
FARS_SEARCH_URL = "https://farsnews.ir/search?topicID=0"

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
HTTP_RETRIES = 3


class FarsWorker(BaseWorker):
    """Worker for Fars News Agency search/listing page."""

    def __init__(self):
        """Initialize Fars worker."""
        super().__init__("fars")
        self.search_url = FARS_SEARCH_URL
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._s3_initialized = False
        self._playwright = None
        self._browser = None
        self._browser_context = None
        
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

    async def _get_browser_context(self):
        """Get or create a Playwright browser context for JavaScript rendering."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            self.logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
            return None, None
        
        try:
            if self._playwright is None:
                self.logger.info("Initializing Playwright browser for Fars...")
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                )
                self._browser_context = await self._browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080},
                    locale='fa-IR',
                    timezone_id='Asia/Tehran'
                )
                self.logger.info("Playwright browser initialized successfully for Fars")
            
            return self._browser_context, self._playwright
        except Exception as e:
            self.logger.error(f"Failed to initialize Playwright browser: {e}. Make sure Playwright is installed: pip install playwright && playwright install chromium")
            return None, None

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
                
                stats = self.rate_limiter.get_stats(self.source_name)
                self.logger.debug(
                    f"Rate limit: {stats['requests_last_minute']}/{stats['max_requests_per_minute']} requests/min, "
                    f"delay: {stats['delay_between_requests']}s",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                        "delay_applied": stats['delay_between_requests'],
                    }
                )
                
                async with session.get(url) as response:
                    # Handle HTTP 429 (Too Many Requests)
                    if response.status == 429:
                        # Get Retry-After header if available
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                wait_time = float(retry_after)
                            except ValueError:
                                wait_time = 60  # Default to 60 seconds
                        else:
                            # Exponential backoff for 429
                            wait_time = min(2 ** attempt * 10, 300)  # Max 5 minutes
                        
                        self.logger.warning(
                            f"HTTP 429 (Rate Limited) for {url}, waiting {wait_time}s before retry",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                                "article_url": url if request_type == "article" else None,
                                "retry_after": wait_time,
                            }
                        )
                        
                        if attempt < max_retries - 1:
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            self.logger.error(
                                f"Rate limit exceeded after {max_retries} attempts: {url}",
                                extra={
                                    "source": self.source_name,
                                    "request_type": request_type,
                                    "article_url": url if request_type == "article" else None,
                                }
                            )
                            return None
                    
                    if response.status == 200:
                        return await response.read()
                    else:
                        self.logger.warning(
                            f"HTTP {response.status} for {url}, attempt {attempt + 1}/{max_retries}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                                "article_url": url if request_type == "article" else None,
                            }
                        )
            except Exception as e:
                self.logger.warning(
                    f"Error fetching {url} (attempt {attempt + 1}/{max_retries}): {e}",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                        "article_url": url if request_type == "article" else None,
                    }
                )
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = min(2 ** attempt, 10)
                    await asyncio.sleep(wait_time)
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

    async def _parse_listing_page(self) -> list[dict]:
        """
        Parse the listing/search page to extract article links.
        Uses Playwright for JavaScript rendering if needed.

        Returns:
            List of dictionaries with article info (url, title if available)
        """
        html_content = None
        page = None
        
        # Try using headless browser first (for JavaScript-rendered content)
        browser_context, playwright = await self._get_browser_context()
        if browser_context:
            try:
                page = await browser_context.new_page()
                self.logger.info(f"Using headless browser to fetch Fars listing page")
                
                # Intercept network requests to find API calls
                api_responses = []
                
                async def handle_response(response):
                    url = response.url
                    # Log all responses to see what's being called
                    content_type = response.headers.get('content-type', '')
                    self.logger.debug(f"Network response: {url}, content-type: {content_type}")
                    
                    # Look for API endpoints that might return article data
                    if any(pattern in url.lower() for pattern in ['/api/', '/search', '/news', '/article', 'json', 'data', 'feed', 'rss']):
                        try:
                            if 'json' in content_type or 'application' in content_type or 'text' in content_type:
                                try:
                                    # Try JSON first
                                    try:
                                        json_data = await response.json()
                                        api_responses.append({
                                            'url': url,
                                            'data': json_data
                                        })
                                        self.logger.info(f"Found JSON API response: {url}, data keys: {list(json_data.keys()) if isinstance(json_data, dict) else type(json_data)}")
                                    except:
                                        # Try text
                                        text_data = await response.text()
                                        if len(text_data) < 10000:  # Only log small responses
                                            self.logger.info(f"Found text API response: {url}, length: {len(text_data)}, preview: {text_data[:200]}")
                                except Exception as e:
                                    self.logger.debug(f"Error reading response {url}: {e}")
                        except:
                            pass
                
                page.on("response", handle_response)
                
                await page.goto(self.search_url, wait_until='networkidle', timeout=30000)
                
                # Wait for initial content to load
                await page.wait_for_timeout(2000)
                
                # Scroll down a bit to trigger lazy loading, then scroll back to top
                # This loads more content but we'll only extract from the top (recent news)
                self.logger.info("Scrolling down to load more content...")
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.3)")  # Scroll to 30% of page
                await page.wait_for_timeout(1500)  # Wait for content to load
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6)")  # Scroll to 60% of page
                await page.wait_for_timeout(1500)  # Wait for content to load
                await page.evaluate("window.scrollTo(0, 0)")  # Scroll back to top
                await page.wait_for_timeout(1000)  # Wait a bit after scrolling back
                
                # Wait for article content to appear - try multiple selectors
                self.logger.info("Waiting for article content to load...")
                selectors_to_wait = [
                    'a[href*="/news/"]',
                    'article',
                    '[class*="news"]',
                    '[class*="item"]',
                    '[data-testid*="news"]',
                    '[data-testid*="article"]'
                ]
                
                for selector in selectors_to_wait:
                    try:
                        await page.wait_for_selector(selector, timeout=3000)
                        self.logger.info(f"Found content with selector: {selector}")
                        break
                    except:
                        continue
                
                # Additional wait for any AJAX/API calls to complete
                await page.wait_for_timeout(3000)
                
                # Check if we found any API responses with article data
                if api_responses:
                    self.logger.info(f"Found {len(api_responses)} API responses, checking for article URLs...")
                    article_urls_from_api = []
                    for api_resp in api_responses:
                        self.logger.info(f"Processing API URL: {api_resp['url']}")
                        # Try to extract article URLs from API response
                        data = api_resp['data']
                        if isinstance(data, dict):
                            # Common patterns in API responses
                            for key in ['items', 'articles', 'news', 'data', 'results', 'list', 'posts', 'content']:
                                if key in data and isinstance(data[key], list):
                                    self.logger.info(f"Found list in key '{key}' with {len(data[key])} items")
                                    for item in data[key]:
                                        if isinstance(item, dict):
                                            # Look for URL fields
                                            for url_key in ['url', 'link', 'href', 'id', 'slug', 'path', 'uri']:
                                                if url_key in item:
                                                    url_value = item[url_key]
                                                    if isinstance(url_value, str):
                                                        # Construct full URL if needed
                                                        if url_value.startswith('/'):
                                                            url_value = f"https://farsnews.ir{url_value}"
                                                        if 'farsnews.ir' in url_value and '/news/' in url_value:
                                                            article_urls_from_api.append(url_value)
                                                            self.logger.info(f"Found article URL in API: {url_value}")
                        elif isinstance(data, list):
                            # Data is directly a list
                            self.logger.info(f"API response is a list with {len(data)} items")
                            for item in data:
                                if isinstance(item, dict):
                                    for url_key in ['url', 'link', 'href', 'id', 'slug']:
                                        if url_key in item:
                                            url_value = item[url_key]
                                            if isinstance(url_value, str) and 'farsnews.ir' in url_value and '/news/' in url_value:
                                                article_urls_from_api.append(url_value)
                                                self.logger.info(f"Found article URL in API list: {url_value}")
                    
                    # Store URLs from API for later use
                    if article_urls_from_api:
                        self.logger.info(f"Total article URLs found in API: {len(article_urls_from_api)}")
                        # We'll add these to articles list in the parsing section
                
                # Use XPath to find the news list container
                # XPath: //*[@id="app"]/div/div[1]/div/div/div[2]/div/div/div[2]/div/div[1]
                listing_xpath = '//*[@id="app"]/div/div[1]/div/div/div[2]/div/div/div[2]/div/div[1]'
                try:
                    listing_element = await page.query_selector(f'xpath={listing_xpath}')
                    if listing_element:
                        self.logger.info("Found news list container using XPath")
                        # Extract HTML from this container
                        listing_html = await listing_element.inner_html()
                        await listing_element.dispose()
                        # Parse this container for article links first
                        listing_tree = HTMLParser(listing_html)
                        listing_links = listing_tree.css("a[href]")
                        self.logger.info(f"Found {len(listing_links)} links in news list container")
                        # Store for later processing
                        self._listing_links = listing_links
                    else:
                        self.logger.warning("Could not find news list container using XPath")
                except Exception as xpath_error:
                    self.logger.warning(f"Error using XPath for listing: {xpath_error}")
                
                html_content = await page.content()
                
                # Try to extract article data from JavaScript variables
                try:
                    js_article_data = await page.evaluate("""
                        () => {
                            // Look for article data in window object
                            let articles = [];
                            if (window.__INITIAL_STATE__) {
                                articles.push(JSON.stringify(window.__INITIAL_STATE__));
                            }
                            if (window.__NEXT_DATA__) {
                                articles.push(JSON.stringify(window.__NEXT_DATA__));
                            }
                            if (window.articles) {
                                articles.push(JSON.stringify(window.articles));
                            }
                            if (window.news) {
                                articles.push(JSON.stringify(window.news));
                            }
                            // Look for data in script tags
                            let scripts = Array.from(document.querySelectorAll('script'));
                            for (let script of scripts) {
                                if (script.textContent && (script.textContent.includes('/news/') || script.textContent.includes('article'))) {
                                    articles.push(script.textContent.substring(0, 500));
                                }
                            }
                            return articles;
                        }
                    """)
                    if js_article_data:
                        self.logger.info(f"Found JavaScript data, length: {len(str(js_article_data))}")
                        # Try to extract URLs from JS data
                        js_str = str(js_article_data)
                        news_urls = re.findall(r'https?://[^"\'<>\\s]+farsnews\.ir[^"\'<>\\s]*/news/[^"\'<>\\s]+', js_str)
                        if news_urls:
                            self.logger.info(f"Found {len(news_urls)} article URLs in JavaScript: {news_urls[:5]}")
                except Exception as js_error:
                    self.logger.debug(f"Error extracting JS data: {js_error}")
                
                await page.close()
                page = None
                self.logger.info(f"Successfully fetched Fars listing page with headless browser, HTML length: {len(html_content)}")
            except Exception as browser_error:
                self.logger.warning(f"Error using headless browser: {browser_error}, falling back to HTTP")
                if page:
                    try:
                        await page.close()
                    except:
                        pass
                page = None
        
        # Fallback to HTTP if browser failed or not available
        if html_content is None:
            content = await self._fetch_with_retry(self.search_url, request_type="listing")
            if content is None:
                self.logger.error("Failed to fetch listing page")
                return []

            try:
                # Try different encodings if needed
                if isinstance(content, bytes):
                    try:
                        html_content = content.decode('utf-8')
                    except UnicodeDecodeError:
                        html_content = content.decode('utf-8', errors='ignore')
                else:
                    html_content = content
            except Exception as e:
                self.logger.error(f"Error decoding HTML content: {e}")
                return []

        try:
            self.logger.debug(f"Fetched HTML content, length: {len(html_content)}")

            tree = HTMLParser(html_content)
            articles = []
            found_links = set()  # Initialize early for JSON-LD parsing
            
            # First, try to extract articles from the XPath container if we found it
            if hasattr(self, '_listing_links') and self._listing_links:
                self.logger.info(f"Processing {len(self._listing_links)} links from XPath container")
                for link in self._listing_links:
                    href = link.attributes.get("href", "") if link.attributes else ""
                    if not href:
                        continue
                    
                    # Normalize URL
                    if href.startswith("/"):
                        url = urljoin("https://farsnews.ir", href)
                    elif href.startswith("http"):
                        url = href
                    else:
                        continue
                    
                    # Only process Fars news URLs
                    if "farsnews.ir" not in url:
                        continue
                    
                    # Check for news article patterns
                    is_news_url = (
                        "/news/" in url or 
                        url.startswith("https://farsnews.ir/news") or
                        re.search(r'/news/\d+', url) or
                        re.search(r'farsnews\.ir/[^/]+/\d+/[^/]+', url)
                    )
                    
                    if not is_news_url:
                        continue
                    
                    # Skip non-news items
                    excluded_patterns = [
                        "/category/", "/search/", "/tag/", "/author/", 
                        "/rss", "/feed", "/api", "/ajax", "/archive",
                        "/showcase", "/campaigns", "/tv", "/privacy", "/trends"
                    ]
                    if any(skip in url.lower() for skip in excluded_patterns):
                        continue
                    
                    # Normalize URL
                    parsed = urlparse(url)
                    normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    
                    if normalized_url in found_links:
                        continue
                    
                    found_links.add(normalized_url)
                    
                    # Extract title
                    title = link.text(strip=True) if link.text() else ""
                    if not title or len(title) < 10:
                        # Try parent elements
                        parent = link.parent
                        for _ in range(5):
                            if parent:
                                title_elem = parent.css_first("h1, h2, h3, h4, h5, .title, .headline")
                                if title_elem:
                                    title = title_elem.text(strip=True) if title_elem.text() else ""
                                    if title:
                                        break
                                parent = parent.parent if parent.parent else None
                    
                    articles.append({
                        "url": normalized_url,
                        "title": title,
                    })
                    self.logger.info(f"Found article from XPath container: {normalized_url}")
                
                # Clear the stored links
                delattr(self, '_listing_links')
                
                if articles:
                    self.logger.info(f"Found {len(articles)} articles from XPath container")
                    # Remove duplicates and return
                    seen_urls = set()
                    unique_articles = []
                    for article in articles:
                        if article["url"] not in seen_urls:
                            seen_urls.add(article["url"])
                            unique_articles.append(article)
                    return unique_articles
            
            # Extract article URLs from JavaScript data (if we captured any)
            # This will be populated by the browser context if API responses were found
            # For now, we'll search the HTML for any /news/ patterns that might be in script tags
            script_tags = tree.css("script")
            for script in script_tags:
                script_text = script.text() if script.text() else ""
                if script_text:
                    # Look for URLs in script content
                    news_urls_in_script = re.findall(r'https?://[^"\'<>\\s]+farsnews\.ir[^"\'<>\\s]*/news/[^"\'<>\\s]+', script_text)
                    for url in news_urls_in_script:
                        parsed = urlparse(url)
                        normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                        if normalized_url not in found_links and "farsnews.ir" in normalized_url:
                            found_links.add(normalized_url)
                            articles.append({
                                "url": normalized_url,
                                "title": "",
                            })
                            self.logger.info(f"Found article URL in script tag: {normalized_url}")
            
            # Try to find article data in JSON-LD or script tags (common for JS-rendered sites)
            json_ld_articles = []
            try:
                json_ld_scripts = tree.css('script[type="application/ld+json"]')
                for script in json_ld_scripts:
                    try:
                        import json
                        script_text = script.text() if script.text() else ""
                        if script_text:
                            data = json.loads(script_text)
                            if isinstance(data, dict):
                                if data.get("@type") == "NewsArticle" and data.get("url"):
                                    json_ld_articles.append(data.get("url"))
                                elif isinstance(data.get("@graph"), list):
                                    for item in data.get("@graph", []):
                                        if item.get("@type") == "NewsArticle" and item.get("url"):
                                            json_ld_articles.append(item.get("url"))
                    except:
                        pass
                
                if json_ld_articles:
                    self.logger.info(f"Found {len(json_ld_articles)} articles in JSON-LD: {json_ld_articles[:5]}")
                    for url in json_ld_articles:
                        if "farsnews.ir" in url and "/news/" in url:
                            parsed = urlparse(url)
                            normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                            if normalized_url not in found_links:
                                found_links.add(normalized_url)
                                articles.append({
                                    "url": normalized_url,
                                    "title": "",
                                })
            except Exception as e:
                self.logger.debug(f"Error parsing JSON-LD: {e}")

            # Find all article links - try multiple selectors
            # Common patterns: article links, news item links
            link_selectors = [
                "a[href*='/news/']",  # Links containing /news/
                "a[href^='/news']",  # Links starting with /news
                "a[href*='farsnews.ir/news']",  # Links with full domain
                "article a",  # Links inside article tags
                "article[class*='news'] a",  # Links in news article tags
                ".news-item a",  # Links in news-item containers
                ".item a",  # Links in item containers
                ".news a",  # Links in news containers
                "a.news-link",  # Links with news-link class
                "[class*='news'] a",  # Links in elements with 'news' in class
                "[class*='item'] a",  # Links in elements with 'item' in class
                "[data-testid*='news'] a",  # Links in data-testid with news
                "[data-testid*='article'] a",  # Links in data-testid with article
                "h2 a",  # Links in h2 tags (common for news titles)
                "h3 a",  # Links in h3 tags
                "h4 a",  # Links in h4 tags
                ".title a",  # Links in title containers
                ".headline a",  # Links in headline containers
                "[role='article'] a",  # Links in article role elements
            ]

            # found_links already initialized above for JSON-LD
            all_links_found = 0
            rejected_urls = {
                "not_fars": 0,
                "no_news_path": 0,
                "excluded_path": 0,
                "duplicate": 0
            }
            sample_rejected = []  # Store sample rejected URLs for debugging

            for selector in link_selectors:
                try:
                    links = tree.css(selector)
                    all_links_found += len(links)
                    if len(links) > 0:
                        self.logger.info(f"Selector '{selector}' found {len(links)} links")
                    
                    for link in links:
                        href = link.get("href", "")
                        if not href:
                            continue
                        
                        # Normalize URL to canonical form
                        if href.startswith("/"):
                            # Relative URL
                            url = urljoin("https://farsnews.ir", href)
                        elif href.startswith("http"):
                            # Absolute URL
                            url = href
                        else:
                            # Skip invalid URLs
                            continue
                        
                        # Only process Fars news URLs
                        if "farsnews.ir" not in url:
                            rejected_urls["not_fars"] += 1
                            if len(sample_rejected) < 5:
                                sample_rejected.append(("not_fars", url))
                            continue
                        
                        # Check for news article patterns - Fars uses different URL structures
                        # Patterns:
                        # - /news/123456 (standard)
                        # - /{author}/{id}/{slug} (Fars specific, e.g., /mahnaz_nazeri/1766990636798060380/...)
                        # - /{category}/{id}/{slug} (e.g., /Live_TV/1766992453398844849/...)
                        is_news_url = (
                            "/news/" in url or 
                            url.startswith("https://farsnews.ir/news") or
                            re.search(r'/news/\d+', url) or  # Pattern like /news/123456
                            # Fars specific pattern: /author_or_category/numeric_id/slug
                            re.search(r'farsnews\.ir/[^/]+/\d+/[^/]+', url)  # Pattern like /mahnaz_nazeri/1766990636798060380/title
                        )
                        
                        # Exclude known non-article pages
                        if not is_news_url:
                            # Check if it's a known non-article page
                            excluded_patterns = [
                                '/showcase', '/campaigns', '/tv', '/search', '/privacy', 
                                '/trends', '/hashtag/', '/topic/', '/tag/', '/author/',
                                '/category/', '/rss', '/feed', '/api', '/ajax', '/archive'
                            ]
                            if any(pattern in url.lower() for pattern in excluded_patterns):
                                rejected_urls["excluded_path"] += 1
                                if len(sample_rejected) < 10:
                                    sample_rejected.append(("excluded_path", url))
                            else:
                                rejected_urls["no_news_path"] += 1
                                if len(sample_rejected) < 10:
                                    sample_rejected.append(("no_news_path", url))
                            continue
                        
                        # Skip non-news items (e.g., category pages, search pages, RSS feeds)
                        if any(skip in url.lower() for skip in [
                            "/category/", "/search/", "/tag/", "/author/", 
                            "/rss", "/feed", "/api", "/ajax", "/archive"
                        ]):
                            rejected_urls["excluded_path"] += 1
                            if len(sample_rejected) < 10:
                                sample_rejected.append(("excluded_path", url))
                            continue
                        
                        # Normalize URL (remove fragments, query params if needed)
                        parsed = urlparse(url)
                        normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                        
                        # Skip if already found
                        if normalized_url in found_links:
                            rejected_urls["duplicate"] += 1
                            continue
                        
                        found_links.add(normalized_url)
                        self.logger.info(f"Found article URL: {normalized_url}")
                        
                        # Extract title if available
                        title = ""
                        # Try to get title from link text
                        link_text = link.get_text(strip=True)
                        if link_text and len(link_text) > 10:  # Reasonable title length
                            title = link_text
                        else:
                            # Try to find title in parent elements
                            parent = link.parent
                            for _ in range(5):  # Check up to 5 levels up
                                if parent:
                                    # Try various title elements
                                    title_elem = parent.find(["h1", "h2", "h3", "h4", "h5"])
                                    if not title_elem:
                                        title_elem = parent.find(class_=re.compile("title|headline", re.I))
                                    if title_elem:
                                        title = title_elem.get_text(strip=True)
                                        if title:
                                            break
                                    parent = parent.parent
                        
                        articles.append({
                            "url": normalized_url,
                            "title": title,
                        })
                except Exception as selector_error:
                    self.logger.debug(f"Error with selector '{selector}': {selector_error}")
                    continue

            self.logger.info(f"Total links found across all selectors: {all_links_found}")
            self.logger.info(f"Unique article URLs found: {len(found_links)}")
            self.logger.info(f"Rejected URLs breakdown: {rejected_urls}")
            if sample_rejected:
                self.logger.info(f"Sample rejected URLs: {sample_rejected[:10]}")

            # Fallback: if no articles found, try finding ALL links and filter
            if len(found_links) == 0:
                self.logger.debug("No articles found with selectors, trying fallback: all links")
                all_links = tree.css("a[href]")
                self.logger.debug(f"Found {len(all_links)} total links on page")
                
                # Log first 20 links for debugging
                sample_links = []
                for i, link in enumerate(all_links[:20]):
                    href = link.attributes.get("href", "") if link.attributes else ""
                    if href:
                        if href.startswith("/"):
                            full_url = urljoin("https://farsnews.ir", href)
                        elif href.startswith("http"):
                            full_url = href
                        else:
                            full_url = href
                        sample_links.append(full_url)
                self.logger.info(f"Sample links found (first 20): {sample_links}")
                
                for link in all_links:
                    href = link.attributes.get("href", "") if link.attributes else ""
                    if not href:
                        continue
                    
                    # Normalize URL
                    if href.startswith("/"):
                        url = urljoin("https://farsnews.ir", href)
                    elif href.startswith("http"):
                        url = href
                    else:
                        continue
                    
                    # Must be Fars news URL
                    if "farsnews.ir" not in url:
                        continue
                    
                    # Check for news article patterns - Fars uses different URL structures
                    is_news_url = (
                        "/news/" in url or 
                        url.startswith("https://farsnews.ir/news") or
                        re.search(r'/news/\d+', url) or  # Pattern like /news/123456
                        # Fars specific pattern: /author_or_category/numeric_id/slug
                        re.search(r'farsnews\.ir/[^/]+/\d+/[^/]+', url)  # Pattern like /mahnaz_nazeri/1766990636798060380/title
                    )
                    
                    if not is_news_url:
                        continue
                    
                    # Skip non-news items
                    excluded_patterns = [
                        "/category/", "/search/", "/tag/", "/author/", 
                        "/rss", "/feed", "/api", "/ajax", "/archive",
                        "/showcase", "/campaigns", "/tv", "/privacy", "/trends"
                    ]
                    if any(skip in url.lower() for skip in excluded_patterns):
                        continue
                    
                    # Normalize URL
                    parsed = urlparse(url)
                    normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    
                    if normalized_url in found_links:
                        continue
                    
                    found_links.add(normalized_url)
                    
                    # Extract title
                    title = link.text(strip=True) if link.text() else ""
                    if not title or len(title) < 10:
                        # Try parent elements
                        parent = link.parent
                        for _ in range(5):
                            if parent:
                                title_elem = parent.css_first("h1, h2, h3, h4, h5")
                                if not title_elem:
                                    title_elem = parent.css_first(".title, .headline")
                                if title_elem:
                                    title = title_elem.text(strip=True) if title_elem.text() else ""
                                    if title:
                                        break
                                parent = parent.parent if parent.parent else None
                    
                    articles.append({
                        "url": normalized_url,
                        "title": title,
                    })
                
                self.logger.debug(f"Fallback found {len(articles)} additional articles")

            # Remove duplicates by URL (in case same URL appears with different titles)
            seen_urls = set()
            unique_articles = []
            for article in articles:
                if article["url"] not in seen_urls:
                    seen_urls.add(article["url"])
                    unique_articles.append(article)

            self.logger.info(f"Parsed {len(unique_articles)} articles from listing page")
            
            
            # If no articles found, log some debug info and try to find any /news/ pattern
            if len(unique_articles) == 0:
                # Search for /news/ pattern in HTML to see if articles exist but aren't linked
                news_pattern_matches = len(re.findall(r'/news/\d+', html_content))
                self.logger.warning(
                    f"No articles found. HTML length: {len(html_content)}, "
                    f"Total links found: {all_links_found}, "
                    f"/news/ pattern matches in HTML: {news_pattern_matches}. "
                    f"First 2000 chars of HTML: {html_content[:2000]}"
                )
                
                # Try to find article IDs or slugs in the HTML
                article_id_patterns = re.findall(r'/news/(\d+[^"\'<>]*?)', html_content)
                if article_id_patterns:
                    self.logger.info(f"Found potential article IDs in HTML: {article_id_patterns[:10]}")
            
            return unique_articles

        except Exception as e:
            self.logger.error(f"Error parsing listing page: {e}", exc_info=True)
            return []

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
        Fetch and extract article content from HTML page using selectolax.
        Uses Playwright for JavaScript rendering and XPath to locate content.

        Args:
            url: Article URL

        Returns:
            Dictionary with extracted content, or None if extraction failed
        """
        page = None
        
        # Use headless browser for JavaScript-rendered content
        browser_context, playwright = await self._get_browser_context()
        if not browser_context:
            self.logger.error("Playwright browser not available, cannot extract Fars article", extra={"article_url": url})
            return None
        
        try:
            page = await browser_context.new_page()
            self.logger.info(f"Using headless browser to fetch Fars article", extra={"article_url": url})
            await page.goto(url, wait_until='networkidle', timeout=30000)
            # Wait for content to load (JavaScript rendering)
            await page.wait_for_timeout(3000)
            # Wait for app container
            try:
                await page.wait_for_selector('#app', timeout=10000)
            except:
                self.logger.warning(f"#app not found after wait", extra={"article_url": url})
            
            # Get HTML content
            html_content = await page.content()
            tree = HTMLParser(html_content)
            
            # Store html_content for later use in date extraction
            _html_content_for_date = html_content

            # Extract title
            title = ""
            title_tag = tree.css_first("h1")
            if title_tag:
                title = title_tag.text(strip=True) if title_tag.text() else ""
            if not title:
                title_elem = tree.css_first("title")
                if title_elem:
                    title = title_elem.text(strip=True) if title_elem.text() else ""
            
            if not title:
                self.logger.warning(f"Could not extract title", extra={"article_url": url})

            # Extract article body using XPath to get the main content container
            body_html = ""
            xpath = '//*[@id="app"]/div/div[1]/div/div/div[2]/div/div/div/div/div/div[1]'
            
            try:
                element_handle = await page.query_selector(f'xpath={xpath}')
                if element_handle:
                    content_html = await element_handle.inner_html()
                    await element_handle.dispose()
                    
                    if content_html:
                        # Parse the content with selectolax
                        content_tree = HTMLParser(content_html)
                        
                        # Find div.px-post-padding-x.pb-2 inside the XPath content
                        content_div = content_tree.css_first("div.px-post-padding-x.pb-2")
                        
                        if content_div:
                            self.logger.debug(f"Found div.px-post-padding-x.pb-2 in XPath content", extra={"article_url": url})
                            
                            # Get all direct child divs - iterate through children to preserve structure
                            inner_divs = []
                            for child in content_div.iter():
                                if child.tag == "div" and child.parent == content_div:
                                    inner_divs.append(child)
                            
                            if inner_divs:
                                self.logger.debug(f"Found {len(inner_divs)} direct child divs", extra={"article_url": url})
                                
                                # Combine all inner divs while preserving HTML structure
                                body_parts = []
                                for div in inner_divs:
                                    div_html = div.html
                                    if not div_html or not div_html.strip():
                                        continue
                                    
                                    # Parse and clean each div while preserving structure
                                    div_tree = HTMLParser(div_html)
                                    
                                    # Remove unwanted elements (script, style, iframe)
                                    for tag in div_tree.css("script, style, iframe"):
                                        tag.decompose()
                                    
                                    # Remove h1 if exists (we already extracted it)
                                    for h1_tag in div_tree.css("h1"):
                                        h1_tag.decompose()
                                    
                                    # Remove unwanted classes (ads, social, etc.) but preserve content structure
                                    unwanted_classes = [
                                        "ad", "advertisement", "social", "share", "related", "recommend",
                                        "nav", "navigation", "footer", "header", "breadcrumb", "menu",
                                        "sidebar", "widget", "comment", "tag", "category-link"
                                    ]
                                    
                                    tags_to_remove = []
                                    for tag in div_tree.css("*"):
                                        if tag.attributes:
                                            class_attr = tag.attributes.get("class", "")
                                            if class_attr:
                                                class_lower = class_attr.lower()
                                                if any(skip in class_lower for skip in unwanted_classes):
                                                    tags_to_remove.append(tag)
                                    
                                    # Remove unwanted tags
                                    for tag in tags_to_remove:
                                        tag.decompose()
                                    
                                    # Get cleaned HTML - this preserves structure (p, img, figure, div, etc.)
                                    cleaned_html = div_tree.html
                                    if cleaned_html and cleaned_html.strip():
                                        body_parts.append(cleaned_html)
                                
                                if body_parts:
                                    # Join all divs - this preserves the structure and order
                                    body_html = "".join(body_parts)
                                    self.logger.info(f"Combined {len(body_parts)} divs, body length: {len(body_html)} chars", extra={"article_url": url})
                                else:
                                    self.logger.warning(f"No content found in inner divs", extra={"article_url": url})
                            else:
                                # Fallback: use content_div itself
                                self.logger.debug(f"No direct child divs, using content_div directly", extra={"article_url": url})
                                content_html_raw = content_div.html
                                if content_html_raw:
                                    content_tree_clean = HTMLParser(content_html_raw)
                                    # Remove unwanted elements
                                    for tag in content_tree_clean.css("script, style, iframe"):
                                        tag.decompose()
                                    for h1_tag in content_tree_clean.css("h1"):
                                        h1_tag.decompose()
                                    body_html = content_tree_clean.html
                                    self.logger.info(f"Using content_div directly, body length: {len(body_html)} chars", extra={"article_url": url})
                        else:
                            # Fallback: use the entire XPath content
                            self.logger.debug(f"div.px-post-padding-x.pb-2 not found, using XPath content directly", extra={"article_url": url})
                            content_tree_clean = HTMLParser(content_html)
                            for tag in content_tree_clean.css("script, style, iframe"):
                                tag.decompose()
                            body_html = content_tree_clean.html
                            self.logger.info(f"Using XPath content directly, body length: {len(body_html)} chars", extra={"article_url": url})
                    else:
                        self.logger.warning(f"XPath element has no inner HTML", extra={"article_url": url})
                else:
                    self.logger.warning(f"Could not find element using XPath: {xpath}", extra={"article_url": url})
            except Exception as xpath_error:
                self.logger.warning(f"Error using XPath: {xpath_error}, trying fallback", extra={"article_url": url}, exc_info=True)
            
            if not body_html:
                self.logger.error(f"Could not extract article body", extra={"article_url": url})
            
            # Extract summary from meta description only (NOT from first paragraph)
            summary = ""
            # Try multiple meta description selectors
            meta_desc = tree.css_first('meta[name="description"]')
            if not meta_desc or not meta_desc.attributes:
                meta_desc = tree.css_first('meta[property="og:description"]')
            if meta_desc and meta_desc.attributes:
                summary = meta_desc.attributes.get("content", "")
                if summary:
                    self.logger.debug(f"Found summary from meta description: {summary[:100]}...", extra={"article_url": url})
            if not summary:
                self.logger.warning(f"Could not extract summary from meta tags", extra={"article_url": url})

            # Extract category - try multiple methods
            category = ""
            # Method 1: meta tag
            category_tag = tree.css_first('meta[property="article:section"]')
            if category_tag and category_tag.attributes:
                category = category_tag.attributes.get("content", "")
                if category:
                    self.logger.debug(f"Found category from meta tag: {category}", extra={"article_url": url})
            
            # Method 2: breadcrumb navigation
            if not category:
                breadcrumb = tree.css_first("nav[class*='breadcrumb']")
                if breadcrumb:
                    links = breadcrumb.css("a")
                    if len(links) > 1:
                        category = links[-1].text(strip=True) if links[-1].text() else ""
                        if category:
                            self.logger.debug(f"Found category from breadcrumb: {category}", extra={"article_url": url})
            
            # Method 3: Extract from XPath content area (after date/time)
            if not category and page:
                try:
                    element_handle = await page.query_selector(f'xpath={xpath}')
                    if element_handle:
                        # Look for category elements in the content area
                        # Category usually appears after date/time
                        category_selectors = [
                            'a[href*="/category/"]',
                            'a[href*="/topic/"]',
                            '[class*="category"]',
                            '[class*="tag"]',
                            'span[class*="category"]',
                            'div[class*="category"]',
                            'a[class*="category"]',
                        ]
                        
                        for selector in category_selectors:
                            try:
                                category_elems = await element_handle.query_selector_all(selector)
                                for cat_elem in category_elems:
                                    cat_text = await cat_elem.inner_text()
                                    if cat_text and cat_text.strip():
                                        cat_text_clean = cat_text.strip()
                                        # Skip if it looks like a date or time
                                        if not re.search(r'\d{4}[/-]\d{2}[/-]\d{2}|\d{2}:\d{2}', cat_text_clean):
                                            category = cat_text_clean
                                            self.logger.debug(f"Found category from {selector}: {category}", extra={"article_url": url})
                                            await cat_elem.dispose()
                                            break
                                    await cat_elem.dispose()
                                if category:
                                    break
                            except Exception as selector_error:
                                self.logger.debug(f"Error with category selector {selector}: {selector_error}", extra={"article_url": url})
                                continue
                        
                        await element_handle.dispose()
                except Exception as category_error:
                    self.logger.debug(f"Error finding category in XPath content: {category_error}", extra={"article_url": url})
            
            # Method 4: Try to find category links in the HTML
            if not category:
                category_links = tree.css('a[href*="/category/"], a[href*="/topic/"]')
                for link in category_links:
                    if link.attributes:
                        href = link.attributes.get("href", "")
                        link_text = link.text(strip=True) if link.text() else ""
                        # Check if it's a category link (not a news article link)
                        if href and ("/category/" in href or "/topic/" in href) and link_text:
                            # Skip if it's in the article body (likely not the main category)
                            # We want the category from navigation or meta area
                            category = link_text
                            self.logger.debug(f"Found category from link: {category}", extra={"article_url": url})
                            break
            
            if not category:
                self.logger.warning(f"Could not extract category", extra={"article_url": url})

            # Extract published date - try multiple methods
            published_at = ""
            # Method 1: article:published_time meta tag
            pub_date_meta = tree.css_first('meta[property="article:published_time"]')
            if pub_date_meta and pub_date_meta.attributes:
                published_at = pub_date_meta.attributes.get("content", "")
                if published_at:
                    self.logger.debug(f"Found published_at from article:published_time: {published_at}", extra={"article_url": url})
            
            # Method 2: Try other date meta tags
            if not published_at:
                date_meta_tags = [
                    'meta[name="publish-date"]',
                    'meta[name="date"]',
                    'meta[property="article:published"]',
                    'meta[property="og:published_time"]',
                ]
                for selector in date_meta_tags:
                    date_meta = tree.css_first(selector)
                    if date_meta and date_meta.attributes:
                        published_at = date_meta.attributes.get("content", "")
                        if published_at:
                            self.logger.debug(f"Found published_at from {selector}: {published_at}", extra={"article_url": url})
                            break
            
            # Method 3: Try time element with datetime attribute
            if not published_at:
                time_elements = tree.css("time[datetime]")
                for time_elem in time_elements:
                    if time_elem.attributes:
                        published_at = time_elem.attributes.get("datetime", "")
                        if published_at:
                            self.logger.debug(f"Found published_at from time[datetime]: {published_at}", extra={"article_url": url})
                            break
            
            # Method 4: Try time element without datetime (text content)
            if not published_at:
                date_elem = tree.css_first("time")
                if date_elem:
                    if date_elem.attributes:
                        published_at = date_elem.attributes.get("datetime", "")
                    if not published_at:
                        published_at = date_elem.text(strip=True) if date_elem.text() else ""
                        if published_at:
                            self.logger.debug(f"Found published_at from time text: {published_at}", extra={"article_url": url})
            
            # Method 5: Try to extract date from XPath content area using Playwright
            if not published_at and page:
                try:
                    # Use the same XPath for content area
                    element_handle = await page.query_selector(f'xpath={xpath}')
                    if element_handle:
                        # Look for date elements in the content area
                        # Try to find date/time elements
                        date_selectors = [
                            'time',
                            '[class*="date"]',
                            '[class*="time"]',
                            '[class*="publish"]',
                            'span[class*="date"]',
                            'div[class*="date"]',
                        ]
                        
                        for selector in date_selectors:
                            try:
                                date_elem = await element_handle.query_selector(selector)
                                if date_elem:
                                    # Try datetime attribute first
                                    datetime_attr = await date_elem.get_attribute("datetime")
                                    if datetime_attr:
                                        published_at = datetime_attr
                                        self.logger.debug(f"Found published_at from {selector} datetime: {published_at}", extra={"article_url": url})
                                        await date_elem.dispose()
                                        break
                                    
                                    # Try text content
                                    date_text = await date_elem.inner_text()
                                    if date_text and date_text.strip():
                                        # Check if it looks like a date
                                        date_text_clean = date_text.strip()
                                        # Look for ISO date pattern
                                        iso_match = re.search(r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}', date_text_clean)
                                        if iso_match:
                                            published_at = iso_match.group(0).replace(' ', 'T')
                                            self.logger.debug(f"Found published_at from {selector} text (ISO): {published_at}", extra={"article_url": url})
                                            await date_elem.dispose()
                                            break
                                        # Look for date pattern
                                        date_match = re.search(r'\d{4}[/-]\d{2}[/-]\d{2}', date_text_clean)
                                        if date_match:
                                            published_at = date_match.group(0)
                                            self.logger.debug(f"Found published_at from {selector} text: {published_at}", extra={"article_url": url})
                                            await date_elem.dispose()
                                            break
                                    await date_elem.dispose()
                            except Exception as selector_error:
                                self.logger.debug(f"Error with selector {selector}: {selector_error}", extra={"article_url": url})
                                continue
                        
                        await element_handle.dispose()
                        if published_at:
                            # If we found a date, we're done
                            pass
                except Exception as date_error:
                    self.logger.debug(f"Error finding date in XPath content: {date_error}", extra={"article_url": url})
            
            # Method 6: Try to find date in HTML content using date-like patterns
            if not published_at:
                try:
                    # Look for date patterns in the HTML content
                    html_text = _html_content_for_date if '_html_content_for_date' in locals() else ""
                    if not html_text and tree:
                        html_text = tree.html if hasattr(tree, 'html') else ""
                    
                    if html_text:
                        # Look for Persian date patterns and ISO dates
                        date_patterns = [
                            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format (prioritize)
                            r'\d{4}[/-]\d{2}[/-]\d{2}',  # YYYY-MM-DD or YYYY/MM/DD
                            r'\d{2}[/-]\d{2}[/-]\d{4}',  # DD-MM-YYYY or DD/MM/YYYY
                        ]
                        for pattern in date_patterns:
                            matches = re.findall(pattern, html_text)
                            if matches:
                                # Take the first match that looks like a date
                                published_at = matches[0]
                                self.logger.debug(f"Found published_at from content pattern: {published_at}", extra={"article_url": url})
                                break
                except Exception as date_error:
                    self.logger.debug(f"Error finding date in content: {date_error}", extra={"article_url": url})
            
            if not published_at:
                self.logger.warning(f"Could not extract published_at", extra={"article_url": url})

            # Extract main image - prioritize: og:image > images in XPath content > any large image
            image_url = ""
            
            # First try og:image
            og_image = tree.css_first('meta[property="og:image"]')
            if og_image and og_image.attributes:
                og_img_url = og_image.attributes.get("content", "")
                # Skip default icons
                if og_img_url and "icon" not in og_img_url.lower() and "logo" not in og_img_url.lower():
                    image_url = urljoin(url, og_img_url) if not og_img_url.startswith("http") else og_img_url
                    if image_url:
                        self.logger.debug(f"Found image from og:image: {image_url}", extra={"article_url": url})
            
            # If no og:image, look in XPath content
            if not image_url:
                try:
                    element_handle = await page.query_selector(f'xpath={xpath}')
                    if element_handle:
                        content_html = await element_handle.inner_html()
                        await element_handle.dispose()
                        
                        if content_html:
                            content_tree = HTMLParser(content_html)
                            images = content_tree.css("img")
                            
                            for img in images:
                                if not img.attributes:
                                    continue
                                
                                # Try multiple src attributes (for lazy loading)
                                src = (
                                    img.attributes.get("src") or 
                                    img.attributes.get("data-src") or 
                                    img.attributes.get("data-lazy-src") or 
                                    img.attributes.get("data-original") or ""
                                )
                                
                                if not src:
                                    continue
                                
                                # Skip unwanted images
                                src_lower = src.lower()
                                skip_patterns = ["logo", "icon", "avatar", "ad", "placeholder", "banner", "header", "footer"]
                                if any(skip in src_lower for skip in skip_patterns):
                                    continue
                                
                                # Make absolute URL
                                image_url = urljoin(url, src) if not src.startswith("http") else src
                                self.logger.debug(f"Found image in XPath content: {image_url}", extra={"article_url": url})
                                break
                except Exception as img_error:
                    self.logger.debug(f"Error finding image in XPath content: {img_error}", extra={"article_url": url})
            
            # Last resort: find any large image on the page
            if not image_url:
                all_images = tree.css("img")
                for img in all_images:
                    if not img.attributes:
                        continue
                    
                    src = (
                        img.attributes.get("src") or 
                        img.attributes.get("data-src") or 
                        img.attributes.get("data-lazy-src") or 
                        img.attributes.get("data-original") or ""
                    )
                    
                    if not src:
                        continue
                    
                    # Skip unwanted images
                    src_lower = src.lower()
                    skip_patterns = ["logo", "icon", "avatar", "ad", "placeholder", "banner", "header", "footer"]
                    if any(skip in src_lower for skip in skip_patterns):
                        continue
                    
                    # Check image size if available
                    width = img.attributes.get("width", "")
                    height = img.attributes.get("height", "")
                    if width and height:
                        try:
                            w = int(width)
                            h = int(height)
                            # Skip very small images
                            if w < 200 or h < 200:
                                continue
                        except:
                            pass
                    
                    # Make absolute URL
                    image_url = urljoin(url, src) if not src.startswith("http") else src
                    self.logger.debug(f"Found image as last resort: {image_url}", extra={"article_url": url})
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
        finally:
            # Ensure page is closed
            if page:
                try:
                    await page.close()
                except:
                    pass

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
        
        # Use rate-limited fetch method
        content = await self._fetch_with_retry(image_url, request_type="image")
        if content is None:
            return None
        
        # Validate image content
        if len(content) < 4:
            return None
        
        # Check magic bytes for image formats
        if len(content) >= 12:
            # WebP: RIFF...WEBP
            if content[:4] == b'RIFF' and content[8:12] == b'WEBP':
                return content
        
        magic_bytes = content[:4]
        if (
            magic_bytes.startswith(b'\xff\xd8') or  # JPEG
            magic_bytes.startswith(b'\x89PNG') or   # PNG
            magic_bytes.startswith(b'GIF8') or      # GIF
            magic_bytes.startswith(b'GIF9')         # GIF
        ):
            return content
        
        return None

    async def _process_body_images(
        self, body_html: str, article_url: str
    ) -> str:
        """
        Process images in body_html: download, upload to S3, and replace URLs.
        
        Args:
            body_html: HTML content with image URLs
            article_url: Article URL for context
            
        Returns:
            body_html with image URLs replaced with S3 URLs
        """
        if not body_html:
            return body_html
        
        try:
            # Parse body_html to find all images
            body_tree = HTMLParser(body_html)
            images = body_tree.css("img")
            
            # Also check for images in style attributes (background-image)
            style_images = []
            for tag in body_tree.css("*"):
                if tag.attributes:
                    style_attr = tag.attributes.get("style", "")
                    if style_attr and "url(" in style_attr:
                        # Extract URLs from style attribute
                        url_matches = re.findall(r'url\(["\']?([^"\')]+)["\']?\)', style_attr)
                        for url in url_matches:
                            if "cdn.farsnews.ir" in url or "farsnews.ir" in url:
                                style_images.append((tag, url))
            
            if not images and not style_images:
                self.logger.debug(f"No images found in body_html", extra={"article_url": article_url})
                return body_html
            
            self.logger.debug(f"Found {len(images)} <img> tags and {len(style_images)} style images in body_html", extra={"article_url": article_url})
            
            # Process each image
            replacements = {}  # Map: original_src -> s3_url
            
            # Process <img> tags
            for img in images:
                if not img.attributes:
                    continue
                
                # Get image source (try multiple attributes for lazy loading)
                src = (
                    img.attributes.get("src") or 
                    img.attributes.get("data-src") or 
                    img.attributes.get("data-lazy-src") or 
                    img.attributes.get("data-original") or ""
                )
                
                if not src:
                    continue
                
                # Skip data URIs and already processed images
                if src.startswith("data:") or src.startswith("s3://"):
                    continue
                
                # If src is already an S3 endpoint URL, extract the S3 key and skip processing
                if src.startswith(settings.s3_endpoint):
                    # This is already an S3 URL, extract the key for later use
                    prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
                    if src.startswith(prefix):
                        s3_key = src[len(prefix):]
                        # Store as s3:// format for consistency
                        s3_url = f"s3://{settings.s3_bucket}/{s3_key}"
                        replacements[src] = s3_url
                        self.logger.debug(f"Already S3 URL, converting to s3:// format: {src[:60]}... -> {s3_url[:60]}...", extra={"article_url": article_url})
                    continue
                
                # Skip unwanted images
                src_lower = src.lower()
                skip_patterns = ["logo", "icon", "avatar", "ad", "placeholder", "banner", "header", "footer"]
                if any(skip in src_lower for skip in skip_patterns):
                    continue
                
                # Make absolute URL (remove query string for matching, but keep it for download)
                img_url_parsed = urlparse(src)
                if not img_url_parsed.netloc:
                    # Relative URL, make absolute
                    img_url = urljoin(article_url, src)
                else:
                    # Absolute URL
                    img_url = src
                
                # For matching, use the original src as it appears in HTML
                # For download, use the absolute URL
                original_src = src
                
                # Download image
                self.logger.debug(f"Downloading body image: {img_url}", extra={"article_url": article_url})
                image_data = await self._download_image(img_url)
                
                if image_data:
                    # Upload to S3
                    s3_full_url = await self._upload_image_to_s3(image_data, "fars", img_url)
                    if s3_full_url:
                        # Extract S3 key from full URL (format: {endpoint}/{bucket}/{key})
                        # We need just the key part for s3:// format
                        prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
                        if s3_full_url.startswith(prefix):
                            s3_key = s3_full_url[len(prefix):]
                        else:
                            # Try without trailing slash
                            prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}"
                            if s3_full_url.startswith(prefix):
                                s3_key = s3_full_url[len(prefix):].lstrip("/")
                            else:
                                # Assume it's already just the key
                                s3_key = s3_full_url
                        
                        # Store as s3:// format - API will generate presigned URLs
                        s3_url = f"s3://{settings.s3_bucket}/{s3_key}"
                        replacements[original_src] = s3_url
                        self.logger.debug(f"Will replace body image URL: {original_src[:60]}... -> s3://.../{s3_key[:40]}...", extra={"article_url": article_url})
                    else:
                        self.logger.warning(f"Failed to upload body image to S3: {img_url}", extra={"article_url": article_url})
                else:
                    self.logger.warning(f"Failed to download body image: {img_url}", extra={"article_url": article_url})
            
            # Process images in style attributes
            for tag, style_url in style_images:
                # Make absolute URL
                if not style_url.startswith("http"):
                    style_url_abs = urljoin(article_url, style_url)
                else:
                    style_url_abs = style_url
                
                # Skip unwanted images
                style_url_lower = style_url_abs.lower()
                skip_patterns = ["logo", "icon", "avatar", "ad", "placeholder", "banner", "header", "footer"]
                if any(skip in style_url_lower for skip in skip_patterns):
                    continue
                
                # Download image
                self.logger.debug(f"Downloading style image: {style_url_abs}", extra={"article_url": article_url})
                image_data = await self._download_image(style_url_abs)
                
                if image_data:
                    # Upload to S3
                    s3_full_url = await self._upload_image_to_s3(image_data, "fars", style_url_abs)
                    if s3_full_url:
                        # Extract S3 key from full URL (format: {endpoint}/{bucket}/{key})
                        prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
                        if s3_full_url.startswith(prefix):
                            s3_key = s3_full_url[len(prefix):]
                        else:
                            # Try without trailing slash
                            prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}"
                            if s3_full_url.startswith(prefix):
                                s3_key = s3_full_url[len(prefix):].lstrip("/")
                            else:
                                # Assume it's already just the key
                                s3_key = s3_full_url
                        
                        # Store as s3:// format - API will generate presigned URLs
                        s3_url = f"s3://{settings.s3_bucket}/{s3_key}"
                        replacements[style_url] = s3_url
                        self.logger.debug(f"Will replace style image URL: {style_url[:60]}... -> s3://.../{s3_key[:40]}...", extra={"article_url": article_url})
                    else:
                        self.logger.warning(f"Failed to upload style image to S3: {style_url_abs}", extra={"article_url": article_url})
                else:
                    self.logger.warning(f"Failed to download style image: {style_url_abs}", extra={"article_url": article_url})
            
            # Replace URLs in body_html
            if replacements:
                for old_url, new_url in replacements.items():
                    # Escape special regex characters in old_url
                    escaped_old_url = re.escape(old_url)
                    
                    # Replace in all possible attribute formats
                    # Handle both quoted and unquoted attributes
                    patterns = [
                        # src attribute
                        (rf'src\s*=\s*"{escaped_old_url}"', f'src="{new_url}"'),
                        (rf"src\s*=\s*'{escaped_old_url}'", f"src='{new_url}'"),
                        (rf'src\s*=\s*({escaped_old_url})(?:\s|>)', rf'src="{new_url}"\1'),
                        # data-src attribute
                        (rf'data-src\s*=\s*"{escaped_old_url}"', f'data-src="{new_url}"'),
                        (rf"data-src\s*=\s*'{escaped_old_url}'", f"data-src='{new_url}'"),
                        (rf'data-src\s*=\s*({escaped_old_url})(?:\s|>)', rf'data-src="{new_url}"\1'),
                        # data-lazy-src attribute
                        (rf'data-lazy-src\s*=\s*"{escaped_old_url}"', f'data-lazy-src="{new_url}"'),
                        (rf"data-lazy-src\s*=\s*'{escaped_old_url}'", f"data-lazy-src='{new_url}'"),
                        (rf'data-lazy-src\s*=\s*({escaped_old_url})(?:\s|>)', rf'data-lazy-src="{new_url}"\1'),
                        # data-original attribute
                        (rf'data-original\s*=\s*"{escaped_old_url}"', f'data-original="{new_url}"'),
                        (rf"data-original\s*=\s*'{escaped_old_url}'", f"data-original='{new_url}'"),
                        (rf'data-original\s*=\s*({escaped_old_url})(?:\s|>)', rf'data-original="{new_url}"\1'),
                    ]
                    
                    for pattern, replacement in patterns:
                        body_html = re.sub(pattern, replacement, body_html)
                
                # Also do simple string replacement as fallback (for any remaining occurrences)
                for old_url, new_url in replacements.items():
                    body_html = body_html.replace(old_url, new_url)
                    # Also replace URL-encoded versions
                    from urllib.parse import quote
                    old_url_encoded = quote(old_url, safe=':/?#[]@!$&\'()*+,;=')
                    if old_url_encoded != old_url:
                        body_html = body_html.replace(old_url_encoded, new_url)
                
                # Clean up any malformed S3 URLs (if old URL was already an S3 URL or had endpoint in it)
                # Pattern: s3://bucket/https://... or s3://bucket/s3://...
                malformed_pattern = r's3://([^/]+)/(https?://[^"\'>\s\)]+)'
                def fix_malformed_s3(match):
                    bucket = match.group(1)
                    old_full_url = match.group(2)
                    # Extract just the path from the old URL if it contains our S3 endpoint
                    if settings.s3_endpoint in old_full_url:
                        # Extract S3 key from full URL
                        # Try different patterns
                        prefix1 = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
                        prefix2 = f"{settings.s3_endpoint.rstrip('/')}/{settings.s3_bucket}/"
                        if old_full_url.startswith(prefix1):
                            s3_key = old_full_url[len(prefix1):]
                            return f"s3://{bucket}/{s3_key}"
                        elif old_full_url.startswith(prefix2):
                            s3_key = old_full_url[len(prefix2):]
                            return f"s3://{bucket}/{s3_key}"
                        else:
                            # Try to extract path after endpoint
                            endpoint_pos = old_full_url.find(settings.s3_endpoint)
                            if endpoint_pos != -1:
                                after_endpoint = old_full_url[endpoint_pos + len(settings.s3_endpoint):].lstrip('/')
                                # Remove bucket name if present
                                if after_endpoint.startswith(f"{settings.s3_bucket}/"):
                                    s3_key = after_endpoint[len(f"{settings.s3_bucket}/"):]
                                else:
                                    s3_key = after_endpoint
                                return f"s3://{bucket}/{s3_key}"
                    # If not our S3 URL, return original (shouldn't happen)
                    return match.group(0)
                
                body_html = re.sub(malformed_pattern, fix_malformed_s3, body_html)
                
                # Also clean up any S3 URLs that already have the endpoint in them
                # Pattern: s3://bucket/s3://bucket/path (nested)
                nested_s3_pattern = r's3://([^/]+)/s3://([^/]+)/([^"\'>\s\)]+)'
                def fix_nested_s3(match):
                    outer_bucket = match.group(1)
                    inner_bucket = match.group(2)
                    path = match.group(3)
                    # Use the inner bucket and path
                    return f"s3://{inner_bucket}/{path}"
                
                body_html = re.sub(nested_s3_pattern, fix_nested_s3, body_html)
                
                # Replace in style attributes (background-image)
                for old_url, new_url in replacements.items():
                    # Replace in style="background-image: url('old_url')"
                    escaped_old_url = re.escape(old_url)
                    style_patterns = [
                        (rf'url\s*\(\s*["\']?{escaped_old_url}["\']?\s*\)', f"url('{new_url}')"),
                        (rf'url\s*\(\s*{escaped_old_url}\s*\)', f"url('{new_url}')"),
                    ]
                    for pattern, replacement in style_patterns:
                        body_html = re.sub(pattern, replacement, body_html, flags=re.IGNORECASE)
                
                # Additional fallback: find and replace any remaining CDN URLs
                # This handles cases where URL might have been modified or query params added
                cdn_pattern = r'(https?://cdn\.farsnews\.ir/[^"\'>\s\)]+)'
                cdn_matches = re.findall(cdn_pattern, body_html)
                if cdn_matches:
                    unique_cdn_urls = list(set(cdn_matches))
                    self.logger.warning(
                        f"Found {len(unique_cdn_urls)} unmatched CDN URLs in body_html after replacement. "
                        f"Sample: {unique_cdn_urls[0][:80] if unique_cdn_urls else 'N/A'}...",
                        extra={"article_url": article_url}
                    )
                    # Try to match and replace any remaining CDN URLs
                    for cdn_url in unique_cdn_urls:
                        # Check if we have a similar URL in replacements (same base URL, different query params)
                        cdn_url_base = cdn_url.split('?')[0]  # Remove query string
                        for old_url, new_url in replacements.items():
                            old_url_base = old_url.split('?')[0]
                            if cdn_url_base == old_url_base:
                                # Same base URL, use the replacement
                                body_html = body_html.replace(cdn_url, new_url)
                                self.logger.debug(f"Replaced remaining CDN URL: {cdn_url[:60]}... -> {new_url[:60]}...", extra={"article_url": article_url})
                                break
                
                self.logger.info(f"Replaced {len(replacements)} image URLs in body_html", extra={"article_url": article_url})
            
            # After replacement, remove any remaining images with Fars CDN URLs
            # These are original source images that should be removed since we have S3 versions
            body_tree_final = HTMLParser(body_html)
            fars_cdn_patterns = ["cdn.farsnews.ir", "farsnews.ir"]
            
            images_to_remove = []
            for img in body_tree_final.css("img"):
                if not img.attributes:
                    continue
                
                # Check all possible src attributes
                src = (
                    img.attributes.get("src") or 
                    img.attributes.get("data-src") or 
                    img.attributes.get("data-lazy-src") or 
                    img.attributes.get("data-original") or ""
                )
                
                if src and any(pattern in src for pattern in fars_cdn_patterns):
                    # Skip if it's already an S3 URL (shouldn't happen, but safety check)
                    if not src.startswith("s3://") and not src.startswith(settings.s3_endpoint):
                        images_to_remove.append(img)
                        self.logger.debug(f"Removing original Fars CDN image: {src[:60]}...", extra={"article_url": article_url})
            
            # Remove the images
            for img in images_to_remove:
                # Try to remove the parent element if it's a link or container
                parent = img.parent
                if parent:
                    # If parent is an <a> tag, remove the entire link
                    if parent.tag == "a":
                        parent.decompose()
                    else:
                        # Otherwise, just remove the img tag
                        img.decompose()
                else:
                    img.decompose()
            
            if images_to_remove:
                body_html = body_tree_final.html
                self.logger.info(f"Removed {len(images_to_remove)} original Fars CDN images from body_html", extra={"article_url": article_url})
            
            # Also remove duplicate images with the same S3 URL (keep only the first occurrence)
            body_tree_dedup = HTMLParser(body_html)
            seen_s3_urls = set()
            duplicate_images = []
            
            for img in body_tree_dedup.css("img"):
                if not img.attributes:
                    continue
                
                src = (
                    img.attributes.get("src") or 
                    img.attributes.get("data-src") or 
                    img.attributes.get("data-lazy-src") or 
                    img.attributes.get("data-original") or ""
                )
                
                # Only check S3 URLs (s3:// or presigned URLs)
                if src and (src.startswith("s3://") or settings.s3_endpoint in src):
                    # Normalize URL (remove query params for comparison)
                    src_base = src.split('?')[0]
                    if src_base in seen_s3_urls:
                        # This is a duplicate
                        duplicate_images.append(img)
                        self.logger.debug(f"Found duplicate S3 image: {src[:60]}...", extra={"article_url": article_url})
                    else:
                        seen_s3_urls.add(src_base)
            
            # Remove duplicate images
            for img in duplicate_images:
                parent = img.parent
                if parent and parent.tag == "a":
                    parent.decompose()
                else:
                    img.decompose()
            
            if duplicate_images:
                body_html = body_tree_dedup.html
                self.logger.info(f"Removed {len(duplicate_images)} duplicate S3 images from body_html", extra={"article_url": article_url})
            
            return body_html
            
        except Exception as e:
            self.logger.error(
                f"Error processing body images: {e}",
                extra={"article_url": article_url},
                exc_info=True
            )
            # Return original body_html on error
            return body_html

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
            url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
            
            # Try to get extension from image data or URL
            extension = ".jpg"  # default
            if image_data[:2] == b"\xff\xd8":
                extension = ".jpg"
            elif image_data[:4] == b"\x89PNG":
                extension = ".png"
            elif image_data[:4] in [b"GIF8", b"GIF9"]:
                extension = ".gif"
            elif image_data[:4] == b"RIFF" and b"WEBP" in image_data[:12]:
                extension = ".webp"
            else:
                # Try to get extension from URL
                parsed_url = urlparse(url)
                url_path = parsed_url.path.lower()
                if url_path.endswith((".jpg", ".jpeg")):
                    extension = ".jpg"
                elif url_path.endswith(".png"):
                    extension = ".png"
                elif url_path.endswith(".gif"):
                    extension = ".gif"
                elif url_path.endswith(".webp"):
                    extension = ".webp"
            
            filename = f"{url_hash}{extension}"
            s3_path = (
                f"news-images/{source}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{filename}"
            )

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

            # Return full S3 URL or path
            s3_url = f"{settings.s3_endpoint}/{settings.s3_bucket}/{s3_path}"
            self.logger.info(f"Uploaded image to S3: {s3_url}")
            return s3_url

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
                normalized_category, preserved_raw_category = normalize_category("fars", raw_category)
                
                # Create news article
                news = News(
                    source="fars",
                    title=article_content.get("title") or listing_item.get("title", ""),
                    body_html=article_content.get("body_html", ""),
                    summary=article_content.get("summary", ""),
                    url=listing_item["url"],
                    published_at=article_content.get("published_at", ""),
                    image_url=s3_image_url,
                    category=normalized_category,  # Store normalized category
                    raw_category=preserved_raw_category,  # Store original category
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
        """Fetch and process news from Fars listing/search page."""
        self.logger.info("Starting Fars fetch cycle")

        # Ensure S3 is initialized
        await self._ensure_s3_initialized()

        try:
            # Parse listing page
            listing_items = await self._parse_listing_page()
            if not listing_items:
                self.logger.warning("No articles found on listing page")
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
                            s3_image_url = await self._upload_image_to_s3(
                                image_data, "fars", article_url
                            ) or ""
                        else:
                            self.logger.debug(f"Failed to download main image: {image_url}")

                    # Process images in body_html (download, upload to S3, replace URLs)
                    body_html = article_content.get("body_html", "")
                    if body_html:
                        self.logger.debug(f"Processing body images for article, body_html length: {len(body_html)}", extra={"article_url": article_url})
                        body_html = await self._process_body_images(body_html, article_url)
                        article_content["body_html"] = body_html
                        self.logger.debug(f"After processing body images, body_html length: {len(body_html)}", extra={"article_url": article_url})
                    else:
                        self.logger.warning(f"No body_html to process images for", extra={"article_url": article_url})

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
        # Close browser context and browser
        if self._browser_context:
            try:
                await self._browser_context.close()
            except Exception as e:
                self.logger.warning(f"Error closing browser context: {e}")
            self._browser_context = None
        
        if self._browser:
            try:
                await self._browser.close()
            except Exception as e:
                self.logger.warning(f"Error closing browser: {e}")
            self._browser = None
        
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping Playwright: {e}")
            self._playwright = None
        
        # Close HTTP session
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        self.logger.info("Fars worker shutdown complete")

