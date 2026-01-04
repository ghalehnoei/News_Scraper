"""Reuters Text News Worker - Fetches text articles from Reuters API."""

import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging

import aiohttp
import os
from sqlalchemy import select
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from app.core.config import settings
from app.core.logging import setup_logging
from app.db.models import News
from sqlalchemy import update
from app.db.session import AsyncSessionLocal
from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter
from app.core.category_normalizer import normalize_category

logger = setup_logging()


class ReutersTextWorker(BaseWorker):
    """Worker for fetching text articles from Reuters API."""

    def __init__(self):
        """Initialize Reuters text worker."""
        super().__init__(source_name="reuters_text")
        self.reuters_username = os.getenv("REUTERS_USERNAME")
        self.reuters_password = os.getenv("REUTERS_PASSWORD")
        self.auth_token = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )
        
        self.logger = logger

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/xml, text/xml, */*",
                }
            )
        return self.http_session

    async def _authenticate(self) -> bool:
        """Authenticate with Reuters API and get token."""
        try:
            # Check if credentials are available
            if not self.reuters_username or not self.reuters_password:
                self.logger.error("Reuters username or password not configured. Please set REUTERS_USERNAME and REUTERS_PASSWORD in .env file")
                return False
            
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            self.logger.info(
                "Authenticating with Reuters API...",
                extra={"source": self.source_name}
            )
            
            session = await self._get_http_session()
            auth_url = "https://commerce.reuters.com/rmd/rest/xml/login"
            
            # Reuters API uses GET with query parameters
            params = {
                "username": self.reuters_username,
                "password": self.reuters_password,
            }
            
            async with session.get(auth_url, params=params) as response:
                if response.status != 200:
                    self.logger.error(f"Authentication failed with status {response.status}")
                    return False
                
                xml_content = await response.text()
                
                # Log XML response for debugging
                self.logger.debug(f"Auth response XML: {xml_content[:500]}")
                
                # Parse XML to extract token
                # Response format: <?xml version="1.0" encoding="UTF-8" standalone="yes"?><authToken>TOKEN</authToken>
                root = ET.fromstring(xml_content)
                
                # The root element itself is <authToken>
                if root.tag == "authToken" and root.text:
                    self.auth_token = root.text.strip()
                    self.logger.info(
                        "Successfully authenticated with Reuters API",
                        extra={"source": self.source_name}
                    )
                    return True
                else:
                    self.logger.error(f"Auth token not found in response. Root tag: {root.tag}, XML: {xml_content[:200]}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error during authentication: {e}", exc_info=True)
            return False

    async def _fetch_text_channels(self) -> Optional[str]:
        """Fetch list of text channels from Reuters API."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            channels_url = f"http://rmb.reuters.com/rmd/rest/xml/channels?channelCategory=TXT&token={self.auth_token}"
            
            self.logger.info(
                f"Fetching text channels from: {channels_url}",
                extra={"source": self.source_name}
            )
            
            async with session.get(channels_url) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to fetch channels, status: {response.status}")
                    return None
                
                xml_content = await response.text()
                return xml_content
                
        except Exception as e:
            self.logger.error(f"Error fetching text channels: {e}", exc_info=True)
            return None

    def _parse_channels_list(self, xml_content: str) -> List[Dict[str, str]]:
        """Parse channels XML and extract channel information."""
        try:
            root = ET.fromstring(xml_content)
            channels = []
            
            # Find all channelInformation elements (from the XML structure provided)
            for channel_elem in root.findall(".//channelInformation"):
                channel_info = {}

                # Extract channel alias (used as channel ID)
                alias_elem = channel_elem.find("alias")
                if alias_elem is not None and alias_elem.text:
                    channel_info["alias"] = alias_elem.text.strip()
                else:
                    continue  # Skip if no alias

                # Extract channel description
                desc_elem = channel_elem.find("description")
                if desc_elem is not None and desc_elem.text:
                    channel_info["description"] = desc_elem.text.strip()
                else:
                    channel_info["description"] = ""

                self.logger.debug(f"Found channel: {channel_info['alias']} - {channel_info['description']}")
                channels.append(channel_info)
            
            self.logger.info(f"Found {len(channels)} text channels")
            return channels
            
        except Exception as e:
            self.logger.error(f"Error parsing channels list: {e}", exc_info=True)
            return []

    async def _fetch_items_list(self, channel: str, limit: int = 20) -> Optional[str]:
        """Fetch list of items from a specific text channel."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            items_url = f"http://rmb.reuters.com/rmd/rest/xml/items?token={self.auth_token}&channel={channel}&limit={limit}"
            
            async with session.get(items_url) as response:
                if response.status != 200:
                    self.logger.warning(f"Failed to fetch items from channel {channel}, status: {response.status}")
                    return None
                
                xml_content = await response.text()
                return xml_content
                
        except Exception as e:
            self.logger.error(f"Error fetching items list from channel {channel}: {e}", exc_info=True)
            return None

    def _parse_items_list(self, xml_content: str) -> List[Dict[str, str]]:
        """Parse items list XML and extract item IDs and GUIDs."""
        try:
            root = ET.fromstring(xml_content)
            items = []
            
            # Find all result elements
            for result_elem in root.findall(".//result"):
                item_info = {}
                
                # Extract item ID
                id_elem = result_elem.find("id")
                if id_elem is not None and id_elem.text:
                    item_info["id"] = id_elem.text
                else:
                    continue
                
                # Extract GUID
                guid_elem = result_elem.find("guid")
                if guid_elem is not None and guid_elem.text:
                    item_info["guid"] = guid_elem.text
                else:
                    item_info["guid"] = item_info["id"]
                
                items.append(item_info)
            
            return items
            
        except Exception as e:
            self.logger.error(f"Error parsing items list: {e}", exc_info=True)
            return []

    async def _fetch_item_detail(self, item_id: str, channel: str) -> Optional[str]:
        """Fetch detailed XML for a specific item."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            detail_url = f"http://rmb.reuters.com/rmd/rest/xml/item?token={self.auth_token}&channel={channel}&id={item_id}"
            
            async with session.get(detail_url) as response:
                if response.status != 200:
                    self.logger.warning(f"Failed to fetch item detail for {item_id}, status: {response.status}")
                    return None
                
                xml_content = await response.text()
                return xml_content
                
        except Exception as e:
            self.logger.error(f"Error fetching item detail for {item_id}: {e}", exc_info=True)
            return None

    def _parse_item_detail(self, xml_content: str) -> Optional[Dict[str, Any]]:
        """Parse item detail XML and extract article metadata and content."""
        try:
            root = ET.fromstring(xml_content)
            
            data = {}
            
            # GUID from newsItem attribute
            news_item = root.find('.//{http://iptc.org/std/nar/2006-10-01/}newsItem')
            if news_item is not None:
                data["guid"] = news_item.get("guid", "")
            else:
                data["guid"] = ""
            
            # Headline
            headline_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}headline')
            data["headline"] = headline_elem.text if headline_elem is not None else ""
            
            # Body content from multiple possible locations
            # Try inlineXML first (full article body)
            inline_xml_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}inlineXML')
            if inline_xml_elem is not None:
                # Extract all text content from inlineXML
                body_text = ''.join(inline_xml_elem.itertext()).strip()
                data["body"] = body_text
            else:
                # Fallback to description
                desc_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}description')
                if desc_elem is not None:
                    desc_text = ''.join(desc_elem.itertext()).strip()
                    data["body"] = desc_text
                else:
                    data["body"] = ""
            
            # Language from newsItem xml:lang attribute
            if news_item is not None:
                data["language"] = news_item.get("{http://www.w3.org/XML/1998/namespace}lang", "en")
            else:
                data["language"] = "en"

            # Determine if this is breaking/urgent news based on priority
            # Priority mapping:
            # 1 (Flash): بسیار حیاتی - breaking
            # 2 (Alert): مهم اقتصادی و سیاسی - breaking
            # 3 (Urgent): اخبار تکمیلی و فوری - breaking
            # 4 (Standard): گزارش‌های خبری روتین - not breaking
            # 5 (Background): تحلیل‌ها و مقالات بلند - not breaking
            priority = int(data.get("priority", "5"))
            data["is_breaking"] = priority <= 3  # Priority 1-3 are breaking news

            # Also check for breaking keywords as fallback
            headline = data.get("headline", "").upper()
            body = data.get("body", "").upper()
            has_breaking_keywords = any(keyword in headline or keyword in body for keyword in [
                "BREAKING", "URGENT", "FLASH", "BULLETIN", "ALERT", "EMERGENCY"
            ])

            # If keywords found, override priority-based decision
            if has_breaking_keywords:
                data["is_breaking"] = True

            # Detect text direction based on language
            data["text_direction"] = "rtl" if data["language"].startswith("ar") else "ltr"
            
            # Priority from header
            priority_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}header/{http://iptc.org/std/nar/2006-10-01/}priority')
            data["priority"] = priority_elem.text if priority_elem is not None else "5"
            
            # Sent date from header
            sent_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}header/{http://iptc.org/std/nar/2006-10-01/}sent')
            data["sent"] = sent_elem.text if sent_elem is not None else ""
            
            # Category/Subject
            subjects = root.findall('.//{http://iptc.org/std/nar/2006-10-01/}contentMeta/{http://iptc.org/std/nar/2006-10-01/}subject')
            if subjects:
                # Use first subject with a name element
                for subj in subjects:
                    name_elem = subj.find("{http://iptc.org/std/nar/2006-10-01/}name")
                    if name_elem is not None and name_elem.text:
                        data["category"] = name_elem.text
                        break
            
            if "category" not in data:
                data["category"] = "اخبار"  # Default to "News"
            
            logger.debug(f"Parsed text article: {data.get('headline', 'N/A')[:50]}, GUID: {data.get('guid', 'N/A')[:30]}")
            return data
            
        except Exception as e:
            logger.error(f"Error parsing item detail: {e}", exc_info=True)
            return None

    async def _article_exists(self, guid: str) -> bool:
        """Check if article already exists in database by GUID in URL."""
        try:
            async with AsyncSessionLocal() as db:
                # Store GUID as URL for Reuters text articles
                result = await db.execute(
                    select(News).where(News.url == f"reuters_text:{guid}")
                )
                exists = result.scalar_one_or_none() is not None
                return exists
        except Exception as e:
            logger.error(f"Error checking article existence: {e}", exc_info=True)
            return False

    async def _save_article(self, article_data: dict) -> bool:
        """Save article to database."""
        async with AsyncSessionLocal() as db:
            try:
                # Use GUID as unique URL identifier
                url = f"reuters_text:{article_data['guid']}"
                
                result = await db.execute(
                    select(News).where(News.url == url)
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    self.logger.info(
                        f"Article already exists in database: {article_data['guid']}",
                        extra={"guid": article_data["guid"]}
                    )
                    return False
                
                normalized_category, raw_category = normalize_category(
                    self.source_name,
                    article_data.get("category")
                )
                
                news = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=article_data.get("body_html", ""),
                    summary=article_data.get("summary", ""),
                    url=url,
                    published_at=article_data.get("published_at", ""),
                    image_url=article_data.get("image_url", ""),
                    category=normalized_category,
                    raw_category=raw_category,
                    language=article_data.get("language", "en"),
                    is_breaking=article_data.get("is_breaking", False),
                    priority=article_data.get("priority", 5),
                )
                
                db.add(news)
                await db.commit()
                
                self.logger.info(
                    f"Saved Reuters text article: {article_data['title'][:50]}...",
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
        """Fetch text articles from Reuters API."""
        self.logger.info(f"Starting to fetch text articles from {self.source_name}")
        
        try:
            # Authenticate first
            if not await self._authenticate():
                self.logger.error("Failed to authenticate with Reuters API")
                return
            
            # Fetch text channels
            channels_xml = await self._fetch_text_channels()
            if not channels_xml:
                self.logger.error("Failed to fetch text channels")
                return
            
            # Parse channels list
            channels = self._parse_channels_list(channels_xml)
            if not channels:
                self.logger.warning("No text channels found")
                return
            
            self.logger.info(f"Found {len(channels)} text channels to process")
            
            saved_count = 0
            skipped_count = 0
            
            # Process each channel
            for channel_info in channels[:5]:  # Limit to first 5 channels for now
                channel_alias = channel_info["alias"]
                channel_desc = channel_info["description"]
                
                self.logger.info(f"Processing channel: {channel_alias} - {channel_desc}")
                
                # Fetch items list from this channel
                items_xml = await self._fetch_items_list(channel_alias, limit=10)
                if not items_xml:
                    continue
                
                # Parse items list
                items = self._parse_items_list(items_xml)
                if not items:
                    self.logger.debug(f"No items found in channel {channel_alias}")
                    continue
                
                self.logger.info(f"Found {len(items)} articles in channel {channel_alias}")

                # Ensure we process up to 10 items per channel
                items_to_process = items[:10]  # Take first 10 items (or all if less than 10)
                self.logger.debug(f"Processing {len(items_to_process)} items from channel {channel_alias} (available: {len(items)})")

                # Process each item
                channel_saved = 0
                channel_skipped = 0
                for item in items_to_process:
                    try:
                        item_id = item["id"]
                        guid = item["guid"]
                        
                        # Check if already exists
                        if await self._article_exists(guid):
                            self.logger.debug(f"Article already exists, skipping: {guid}")
                            skipped_count += 1
                            continue
                        
                        # Fetch item detail
                        detail_xml = await self._fetch_item_detail(item_id, channel_alias)
                        if not detail_xml:
                            continue
                        
                        # Parse detail
                        detail_data = self._parse_item_detail(detail_xml)
                        if not detail_data:
                            logger.warning(f"Failed to parse detail for item {item_id}")
                            continue
                        
                        # Determine text direction and styling based on language and breaking news status
                        text_direction = detail_data.get("text_direction", "ltr")
                        is_breaking = detail_data.get("is_breaking", False)

                        # Split paragraphs only at periods (.) as per XML structure
                        body_text = detail_data.get("body", "").strip()

                        # Process the body text to separate paragraphs only at periods
                        if body_text:
                            import re
                            # Split by periods followed by space and capital letter (sentence endings)
                            paragraphs = re.split(r'(?<=\.)\s+(?=[A-Z])', body_text)
                            paragraphs = [p.strip() for p in paragraphs if p.strip()]

                            # If no periods found or only one paragraph, keep as single paragraph
                            if len(paragraphs) == 1:
                                paragraphs = [body_text]

                            # Create HTML with separate paragraphs
                            paragraph_html = ''.join(f'<p>{p}</p>' for p in paragraphs if p)
                        else:
                            paragraph_html = '<p>No content available.</p>'

                        # Apply styling based on language and breaking news status
                        dir_style = f"direction: {text_direction}; text-align: {'right' if text_direction == 'rtl' else 'left'};"
                        if is_breaking:
                            dir_style += " color: #e74c3c; font-weight: bold;"

                        body_html = f'<div dir="{text_direction}" style="{dir_style}">{paragraph_html}</div>'
                        
                        # Prepare article data
                        article_data = {
                            "guid": guid,
                            "title": detail_data.get("headline", "Reuters Article"),
                            "body_html": body_html,
                            "summary": "",  # Empty summary for Reuters text
                            "published_at": detail_data.get("sent", ""),
                            "image_url": "",  # No image for text articles
                            "category": detail_data.get("category", "اخبار"),
                            "language": detail_data.get("language", "en"),  # Language code
                            "is_breaking": is_breaking,  # Breaking news flag
                            "text_direction": text_direction,  # rtl or ltr
                            "priority": int(detail_data.get("priority", "5")),  # Priority level 1-5
                        }
                        
                        # Save to database
                        if await self._save_article(article_data):
                            saved_count += 1
                            channel_saved += 1
                        else:
                            channel_skipped += 1

                    except Exception as e:
                        self.logger.error(f"Error processing item {item_id} from channel {channel_alias}: {e}", exc_info=True)
                        continue

                # Log channel processing summary
                self.logger.info(f"Channel {channel_alias} processed: {channel_saved} saved, {channel_skipped} skipped (total processed: {channel_saved + channel_skipped}/10)")
            
            self.logger.info(
                f"Finished fetching text articles from {self.source_name}: {saved_count} new articles saved, {skipped_count} articles skipped"
            )
            
        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

