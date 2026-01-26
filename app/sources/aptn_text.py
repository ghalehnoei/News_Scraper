"""AP Text News Worker - Fetches text articles from AP API."""

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging
import xml.etree.ElementTree as ET
import os

import aiohttp
from sqlalchemy import select
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

from app.core.config import settings
from app.core.logging import setup_logging
from app.db.models import News
from app.db.session import AsyncSessionLocal
from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter
from app.core.category_normalizer import normalize_category

logger = setup_logging()


class APTextWorker(BaseWorker):
    """Worker for fetching text articles from AP API."""

    def __init__(self):
        """Initialize AP text worker."""
        super().__init__(source_name="aptn_text")
        
        # Load API key from environment
        def _clean_env(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            cleaned = value.strip()
            if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in ("'", '"'):
                cleaned = cleaned[1:-1].strip()
            return cleaned or None

        self.ap_api_key = _clean_env(os.getenv("AP_API_KEY"))
        
        # Log presence and masked values only
        def _mask(value: Optional[str]) -> str:
            if not value:
                return "missing"
            if len(value) <= 6:
                return "***"
            return f"{value[:2]}***{value[-2:]}"

        self.logger.info(
            "AP env loaded (masked): AP_API_KEY=%s",
            _mask(self.ap_api_key),
        )
        
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )
        
        self.logger = logger
        # Store next page URL for pagination (AP provides `next_page` in feed data)
        self.next_page_url: Optional[str] = None

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json",
                    "x-api-key": self.ap_api_key or "",
                }
            )
        return self.http_session

    async def _fetch_feed(self, url: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Fetch feed from AP API."""
        if not self.ap_api_key:
            self.logger.error("AP_API_KEY not set in environment")
            return None
        
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            
            # Use provided URL or default feed URL
            if not url:
                url = "https://api.ap.org/media/v/content/feed?q=type:text&page_size=20"
            
            headers = {
                "Accept": "application/json",
                "x-api-key": self.ap_api_key,
            }
            
            self.logger.debug(f"Fetching feed from {url[:100]}...")
            
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    self.logger.error(f"Feed fetch failed with status {response.status}")
                    response_text = await response.text()
                    self.logger.error(f"Response: {response_text[:500]}")
                    return None
                
                json_response = await response.json()
                return json_response
                
        except Exception as e:
            self.logger.error(f"Error fetching AP feed: {e}", exc_info=True)
            return None

    async def _fetch_item_detail(self, uri: str) -> Optional[Dict[str, Any]]:
        """Fetch item detail from URI to get urgency, language, and other metadata."""
        if not self.ap_api_key:
            return None
        
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            headers = {
                "Accept": "application/json",
                "x-api-key": self.ap_api_key,
            }
            
            self.logger.debug(f"Fetching item detail from {uri[:100]}...")
            
            async with session.get(uri, headers=headers) as response:
                if response.status != 200:
                    self.logger.warning(f"Item detail fetch failed with status {response.status} for {uri[:100]}")
                    return None
                
                json_response = await response.json()
                return json_response
                
        except Exception as e:
            self.logger.error(f"Error fetching item detail: {e}", exc_info=True)
            return None

    async def _download_xml_body(self, xml_url: str) -> Optional[str]:
        """Download XML body from renditions.nitf.href."""
        if not self.ap_api_key:
            return None
        
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            headers = {
                "Accept": "text/xml, application/xml, */*",
                "x-api-key": self.ap_api_key,
            }
            
            self.logger.debug(f"Downloading XML body from {xml_url[:100]}...")
            
            async with session.get(xml_url, headers=headers) as response:
                if response.status != 200:
                    self.logger.warning(f"XML download failed with status {response.status} for {xml_url[:100]}")
                    return None
                
                xml_content = await response.text()
                return xml_content
                
        except Exception as e:
            self.logger.error(f"Error downloading XML body: {e}", exc_info=True)
            return None

    def _parse_nitf_xml(self, xml_content: str) -> Optional[str]:
        """Parse NITF XML and extract body text."""
        try:
            root = ET.fromstring(xml_content)
            
            # NITF namespace (try common variants)
            ns_variants = [
                {'nitf': 'http://iptc.org/std/NITF/2006-10-18/'},
                {'nitf': 'http://iptc.org/std/NITF/2003-1-0/'},
                {},  # No namespace
            ]
            
            body_text_parts = []
            
            for ns in ns_variants:
                # Try body.content first (standard NITF structure)
                body_content = root.find('.//nitf:body.content', ns) if ns else root.find('.//body.content')
                if body_content is not None:
                    # Extract all paragraphs
                    for p in body_content.findall('.//nitf:p', ns) if ns else body_content.findall('.//p'):
                        text = p.text.strip() if p.text else ""
                        if text:
                            body_text_parts.append(text)
                    if body_text_parts:
                        break
                
                # Fallback: try body/body.content
                if not body_text_parts:
                    body_body_content = root.find('.//nitf:body/nitf:body.content', ns) if ns else root.find('.//body/body.content')
                    if body_body_content is not None:
                        for p in body_body_content.findall('.//nitf:p', ns) if ns else body_body_content.findall('.//p'):
                            text = p.text.strip() if p.text else ""
                            if text:
                                body_text_parts.append(text)
                        if body_text_parts:
                            break
                
                # Fallback: try to get all text from body.content or body
                if not body_text_parts:
                    body_elem = root.find('.//nitf:body.content', ns) if ns else root.find('.//body.content')
                    if body_elem is None:
                        body_elem = root.find('.//nitf:body', ns) if ns else root.find('.//body')
                    
                    if body_elem is not None:
                        body_text = ''.join(body_elem.itertext()).strip()
                        if body_text and len(body_text) > 50:  # Minimum reasonable length
                            # Split by common paragraph markers
                            paragraphs = [p.strip() for p in body_text.split('\n\n') if p.strip()]
                            if paragraphs:
                                body_text_parts.extend(paragraphs)
                                break
            
            # Join paragraphs with newlines
            body_text = '\n'.join(body_text_parts) if body_text_parts else ""
            
            return body_text if body_text else None
            
        except Exception as e:
            self.logger.error(f"Error parsing NITF XML: {e}", exc_info=True)
            return None

    async def _parse_feed_items(self, feed_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse feed JSON and extract article data."""
        articles = []
        
        try:
            data = feed_data.get("data", {})
            items = data.get("items", [])
            
            # Store next_page for next cycle (if provided by AP feed)
            self.next_page_url = data.get("next_page")

            self.logger.info(f"Found {len(items)} items in feed (next_page: {self.next_page_url})")
            
            for item_data in items:
                try:
                    item = item_data.get("item", {})
                    if not item:
                        continue
                    
                    article_data = {}
                    
                    # Extract GUID from altids.itemid
                    altids = item.get("altids", {})
                    itemid = altids.get("itemid", "")
                    if not itemid:
                        continue
                    
                    article_data["guid"] = itemid
                    article_data["title"] = item.get("headline", "")
                    article_data["published_at"] = item.get("versioncreated", "")
                    
                    # Get URI for fetching detail
                    uri = item.get("uri", "")
                    if not uri:
                        self.logger.debug(f"No URI found for item {itemid}")
                        continue
                    
                    # Fetch item detail to get urgency, language, and other metadata
                    detail_data = await self._fetch_item_detail(uri)
                    detail_item = None
                    if detail_data:
                        # Extract item from detail response (structure: data.item)
                        detail_item = detail_data.get("data", {}).get("item", {})
                        if detail_item:
                            # Extract urgency/priority
                            urgency = detail_item.get("urgency")
                            if urgency:
                                article_data["priority"] = urgency
                            else:
                                article_data["priority"] = 3  # Default priority
                            
                            # Extract language
                            language = detail_item.get("language")
                            if language:
                                article_data["language"] = language
                            else:
                                article_data["language"] = "en"  # Default to English
                            
                            # Extract category if available (use editorial_subject only)
                            subjects = detail_item.get("subject", [])
                            if subjects:
                                if isinstance(subjects, list):
                                    # Extract editorial_subject from subject objects
                                    subject_names = []
                                    for s in subjects:
                                        if isinstance(s, dict):
                                            editorial_subject = s.get("editorial_subject")
                                            if editorial_subject:
                                                subject_names.append(str(editorial_subject))
                                        else:
                                            subject_names.append(str(s))
                                    article_data["category"] = ", ".join(subject_names)
                                else:
                                    article_data["category"] = str(subjects)
                            else:
                                article_data["category"] = ""
                        else:
                            # Fallback if detail structure is different
                            article_data["language"] = "en"
                            article_data["priority"] = 3
                            article_data["category"] = ""
                    else:
                        # Fallback if detail fetch fails
                        article_data["language"] = "en"
                        article_data["priority"] = 3
                        article_data["category"] = ""
                    
                    # Extract XML URL from renditions (prefer detail item, fallback to feed item)
                    source_item = detail_item if detail_item else item
                    renditions = source_item.get("renditions", {})
                    nitf_rendition = renditions.get("nitf", {})
                    xml_url = nitf_rendition.get("href", "")
                    
                    if not xml_url:
                        self.logger.debug(f"No NITF XML URL found for item {itemid}")
                        continue
                    
                    article_data["xml_url"] = xml_url
                    
                    # Download and parse XML body
                    xml_content = await self._download_xml_body(xml_url)
                    if xml_content:
                        body_text = self._parse_nitf_xml(xml_content)
                        if body_text:
                            article_data["body"] = body_text
                        else:
                            self.logger.warning(f"Could not parse body from XML for item {itemid}")
                            continue
                    else:
                        self.logger.warning(f"Could not download XML for item {itemid}")
                        continue
                    
                    # Skip if essential fields are missing
                    if not article_data.get("title") or not article_data.get("body"):
                        continue
                    
                    articles.append(article_data)
                    
                except Exception as e:
                    self.logger.error(f"Error parsing item: {e}", exc_info=True)
                    continue
            
            return articles
            
        except Exception as e:
            self.logger.error(f"Error parsing feed items: {e}", exc_info=True)
            return []

    async def _get_existing_guids(self) -> set[str]:
        """Get all existing GUIDs from database in one query."""
        try:
            async with AsyncSessionLocal() as db:
                # Fetch all URLs that start with "aptn:" for this source
                result = await db.execute(
                    select(News.url).where(
                        News.source == self.source_name,
                        News.url.like("aptn:%")
                    )
                )
                urls = result.scalars().all()
                # Extract GUIDs from URLs (format: "aptn:GUID")
                guids = {url.split(":", 1)[1] for url in urls if ":" in url}
                self.logger.debug(f"Found {len(guids)} existing articles in database")
                return guids
        except Exception as e:
            self.logger.error(f"Error fetching existing GUIDs: {e}", exc_info=True)
            return set()

    async def _save_article(self, article_data: dict) -> bool:
        """Save article to database."""
        async with AsyncSessionLocal() as db:
            try:
                # Use GUID as unique URL identifier
                url = f"aptn:{article_data['guid']}"
                
                result = await db.execute(
                    select(News).where(News.url == url)
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    return False
                
                normalized_category, raw_category = normalize_category(
                    self.source_name,
                    article_data.get("category")
                )
                
                # Truncate raw_category to fit database limit (200 chars)
                if raw_category and len(raw_category) > 200:
                    raw_category = raw_category[:197] + "..."
                
                # Parse published date
                published_at = None
                if article_data.get("published_at"):
                    try:
                        published_at_str = article_data["published_at"]
                        # ISO 8601 format
                        published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                    except Exception as e:
                        self.logger.warning(f"Could not parse date: {article_data.get('published_at')}, error: {e}")
                        published_at = datetime.utcnow()
                
                if not published_at:
                    published_at = datetime.utcnow()
                
                # Build body HTML with paragraphs
                body_html = ""
                priority = article_data.get("priority", 3)
                title = article_data.get("title", "")
                
                if title:
                    if priority in [1, 2]:
                        body_html += f'<h2 style="color: red;">{title}</h2>'
                    else:
                        body_html += f'<h2>{title}</h2>'
                
                # Split body text into paragraphs and create HTML
                body_text = article_data.get("body", "")
                if body_text:
                    # Split by newlines
                    paragraphs = [p.strip() for p in body_text.split('\n') if p.strip()]
                    for paragraph in paragraphs:
                        body_html += f'<p style="color: black;">{paragraph}</p>'
                
                news = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=body_html,
                    summary="",  # AP doesn't provide summary in feed
                    url=url,
                    published_at=published_at.isoformat() if hasattr(published_at, 'isoformat') else str(published_at),
                    image_url="",
                    category=normalized_category,
                    raw_category=raw_category,
                    language=article_data.get("language", "en"),
                    priority=article_data.get("priority", 3),
                    is_international=True,  # AP is an international source
                    source_type='external',
                )
                
                db.add(news)
                await db.commit()
                
                self.logger.info(
                    f"Saved AP article: {article_data['title'][:50]}...",
                    extra={
                        "guid": article_data["guid"],
                        "source": self.source_name,
                        "category": normalized_category,
                    }
                )
                return True
                
            except Exception as e:
                await db.rollback()
                self.logger.error(
                    f"Error saving article: {e}",
                    extra={"guid": article_data.get("guid"), "error": str(e)},
                    exc_info=True
                )
                return False

    async def fetch_news(self) -> None:
        """Fetch text news from AP API."""
        try:
            saved_count = 0
            skipped_count = 0
            
            # Base URL (used when next_page_url is not available)
            base_url = "https://api.ap.org/media/v/content/feed?q=type:text"

            # If we have a next_page_url from previous run, continue from there
            feed_url = self.next_page_url if getattr(self, "next_page_url", None) else base_url
            self.logger.debug(f"Fetching feed URL: {feed_url}")

            # Fetch feed
            feed_data = await self._fetch_feed(feed_url)
            if not feed_data:
                self.logger.warning("No feed data received")
                return
            
            # Parse articles
            articles = await self._parse_feed_items(feed_data)
            if not articles:
                self.logger.info("No articles parsed from feed")
                return
            
            total = len(articles)
            self.logger.info(f"Processing {total} articles...")
            
            # Batch check for existing articles (much faster than individual queries)
            existing_guids = await self._get_existing_guids()
            
            for i, article in enumerate(articles, 1):
                try:
                    guid = article.get("guid")
                    if not guid:
                        continue
                    
                    # Check against in-memory set (O(1) lookup)
                    if guid in existing_guids:
                        skipped_count += 1
                        continue
                    
                    # Log title for first received article
                    title = article.get('title', 'N/A')
                    self.logger.info(f"First received - Title: {title}")
                    
                    if await self._save_article(article):
                        saved_count += 1
                        # Add to set to avoid duplicate saves in same batch
                        existing_guids.add(guid)
                        self.logger.info(f"Saved {saved_count}/{total}: {title[:50]}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing article: {e}", exc_info=True)
                    continue
            
            self.logger.info(f"Processed {total} articles, saved {saved_count}, skipped {skipped_count}")
            
        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

