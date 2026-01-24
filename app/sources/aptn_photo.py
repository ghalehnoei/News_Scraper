"""AP Photo News Worker - Fetches photo articles from AP API."""

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging
import os
import hashlib
from io import BytesIO
import xml.etree.ElementTree as ET

import aiohttp
from sqlalchemy import select
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

from app.core.config import settings
from app.core.logging import setup_logging
from app.core.category_normalizer import normalize_category
from app.db.models import News
from app.db.session import AsyncSessionLocal
from app.storage.s3 import get_s3_session, init_s3
from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter


logger = setup_logging()


class APPhotoWorker(BaseWorker):
    """Worker for fetching photo articles from AP API."""

    def __init__(self):
        """Initialize AP photo worker."""
        super().__init__(source_name="aptn_photo")
        
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
        self._s3_initialized = False
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )
        
        self.logger = logger
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
                url = "https://api.ap.org/media/v/content/feed?q=type:picture&page_size=20"
            
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
            
            async with session.get(uri, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    self.logger.error(f"Item detail fetch failed with status {response.status}, URI: {uri[:100]}")
                    response_text = await response.text()
                    self.logger.error(f"Response: {response_text[:500]}")
                    return None
                
                json_response = await response.json()
                return json_response
                
        except Exception as e:
            self.logger.error(f"Error fetching item detail from {uri[:100]}: {e}", exc_info=True)
            return None

    async def _download_xml_body(self, xml_url: str) -> Optional[str]:
        """Download XML body from renditions.caption_nitf.href (or script_nitf/nitf as fallback)."""
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
            
            async with session.get(xml_url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
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

    async def _download_image(self, url: str) -> Optional[bytes]:
        """Download image from URL."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="image"
            )
            
            session = await self._get_http_session()
            headers = {
                "Accept": "image/*",
                "x-api-key": self.ap_api_key,
            }
            
            self.logger.debug(f"Downloading image from {url[:80]}...")
            
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
                if response.status != 200:
                    self.logger.warning(f"Image download failed with status {response.status} for {url[:80]}")
                    return None
                
                image_data = await response.read()
                self.logger.debug(f"Downloaded image: {len(image_data)} bytes")
                return image_data
                
        except Exception as e:
            self.logger.error(f"Error downloading image: {e}", exc_info=True)
            return None

    async def _upload_to_s3(self, data: bytes, filename: str, content_type: str) -> Optional[str]:
        """Upload data (image) to S3."""
        if not self._s3_initialized:
            await init_s3()
            self._s3_initialized = True

        try:
            # Generate S3 key
            timestamp = datetime.now().strftime("%Y/%m/%d")

            # Extract extension from filename
            extension = filename.split(".")[-1].lower() if "." in filename else "jpg"
            if extension not in ["jpg", "jpeg", "png", "gif", "webp"]:
                extension = "jpg"

            # Generate hash for uniqueness
            data_hash = hashlib.md5(data).hexdigest()[:8]
            s3_key = f"news-images/{self.source_name}/{timestamp}/{data_hash}.{extension}"

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
                    BytesIO(data),
                    settings.s3_bucket,
                    s3_key,
                    ExtraArgs={"ContentType": content_type}
                )

            self.logger.debug(f"Uploaded to S3: {s3_key}")
            return s3_key

        except Exception as e:
            self.logger.error(f"Error uploading to S3: {e}", exc_info=True)
            return None

    def _extract_preview_image_url(self, renditions: Dict[str, Any]) -> str:
        """Extract preview image URL from renditions based on title 'Preview (JPG)'."""
        try:
            # Iterate through all renditions
            for key, rendition in renditions.items():
                if isinstance(rendition, dict):
                    title = rendition.get("title", "")
                    href = rendition.get("href", "")
                    
                    if not href:
                        continue
                    
                    # Look for Preview (JPG)
                    if "Preview" in title and "JPG" in title:
                        self.logger.debug(f"Found preview image: {title}")
                        return href
            
        except Exception as e:
            self.logger.error(f"Error extracting preview image URL: {e}", exc_info=True)

        return ""

    async def _parse_feed_items(self, feed_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse feed JSON and extract article data."""
        articles = []
        
        try:
            data = feed_data.get("data", {})
            items = data.get("items", [])
            
            # Store next_page for next cycle
            self.next_page_url = data.get("next_page")
            
            self.logger.info(f"Found {len(items)} items in feed")
            
            for item_data in items:
                try:
                    item = item_data.get("item", {})
                    if not item:
                        self.logger.debug(f"Item data missing 'item' key, skipping")
                        continue
                    
                    article_data = {}
                    
                    # Extract GUID from altids.itemid
                    altids = item.get("altids", {})
                    itemid = altids.get("itemid", "")
                    if not itemid:
                        self.logger.debug(f"No itemid found in altids, skipping item")
                        continue
                    
                    article_data["guid"] = itemid
                    article_data["title"] = item.get("headline", "")
                    article_data["published_at"] = item.get("versioncreated", "")
                    
                    # Get URI for fetching detail
                    uri = item.get("uri", "")
                    if not uri:
                        self.logger.warning(f"No URI found for item {itemid}, skipping")
                        continue
                    
                    # Fetch item detail to get urgency, language, renditions, and other metadata
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
                    
                    # Extract XML URL and preview image URL from renditions (prefer detail item, fallback to feed item)
                    source_item = detail_item if detail_item else item
                    renditions = source_item.get("renditions", {})
                    
                    if not renditions:
                        self.logger.warning(f"No renditions found for item {itemid} (title: {article_data.get('title', 'N/A')[:50]})")
                        continue
                    
                    # Extract preview image URL
                    preview_image_url = self._extract_preview_image_url(renditions)
                    article_data["preview_image_url"] = preview_image_url
                    if preview_image_url:
                        self.logger.debug(f"Found preview image URL for item {itemid}")
                    else:
                        self.logger.debug(f"No preview image URL found for item {itemid}")
                    
                    # Extract XML URL from caption_nitf rendition
                    caption_nitf_rendition = renditions.get("caption_nitf", {})
                    xml_url = caption_nitf_rendition.get("href", "")
                    
                    if not xml_url:
                        # Fallback to script_nitf rendition
                        script_nitf_rendition = renditions.get("script_nitf", {})
                        xml_url = script_nitf_rendition.get("href", "")
                    
                    if not xml_url:
                        # Fallback to nitf rendition
                        nitf_rendition = renditions.get("nitf", {})
                        xml_url = nitf_rendition.get("href", "")
                    
                    if not xml_url:
                        self.logger.warning(f"No NITF XML URL found for item {itemid} (title: {article_data.get('title', 'N/A')[:50]}). Renditions keys: {list(renditions.keys())[:10]}")
                        continue
                    
                    article_data["xml_url"] = xml_url
                    
                    # Download and parse XML body
                    xml_content = await self._download_xml_body(xml_url)
                    if xml_content:
                        body_text = self._parse_nitf_xml(xml_content)
                        if body_text:
                            article_data["body"] = body_text
                            self.logger.debug(f"Successfully parsed body from XML for item {itemid} (body length: {len(body_text)})")
                        else:
                            self.logger.warning(f"Could not parse body from XML for item {itemid} (title: {article_data.get('title', 'N/A')[:50]})")
                    else:
                        self.logger.warning(f"Could not download XML for item {itemid} (title: {article_data.get('title', 'N/A')[:50]}, XML URL: {xml_url[:100]})")
                    
                    # Skip if essential fields are missing
                    if not article_data.get("title"):
                        self.logger.warning(f"No title found for item {itemid}, skipping")
                        continue
                    
                    self.logger.debug(f"Successfully parsed article: {article_data.get('title', 'N/A')[:50]} (guid: {itemid})")
                    articles.append(article_data)
                    
                except Exception as e:
                    itemid = item_data.get("item", {}).get("altids", {}).get("itemid", "unknown")
                    self.logger.error(f"Error parsing item (itemid: {itemid}): {e}", exc_info=True)
                    continue
            
            self.logger.info(f"Parsed {len(articles)} articles from {len(items)} items")
            return articles
            
        except Exception as e:
            self.logger.error(f"Error parsing feed items: {e}", exc_info=True)
            return []

    async def _article_exists(self, guid: str) -> bool:
        """Check if article already exists in database by GUID."""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(News).where(News.url == f"aptn:{guid}")
                )
                exists = result.scalar_one_or_none() is not None
                return exists
        except Exception as e:
            self.logger.error(f"Error checking article existence: {e}", exc_info=True)
            return False

    def _build_body_html(self, article_data: dict) -> str:
        """Build HTML body with embedded image and paragraphs."""
        body_parts = []

        try:
            body_parts.append('<div dir="ltr" style="direction: ltr; text-align: left;">')

            # Add image if available
            if article_data.get("image_url"):
                image_style = (
                    "max-width: 100%; "
                    "max-height: 80vh; "
                    "width: auto; "
                    "height: auto; "
                    "border-radius: 5px; "
                    "box-shadow: 0 2px 8px rgba(0,0,0,0.1);"
                )
                body_parts.append(f'''
                    <div style="margin: 20px 0; text-align: center;">
                        <img src="{article_data['image_url']}" alt="{article_data.get('title', 'Image')}" style="{image_style}">
                    </div>
                ''')

            # Add body text paragraphs
            if article_data.get("body"):
                body_text = article_data["body"]
                # Split by newlines to create paragraphs
                paragraphs = body_text.split('\n')
                for paragraph in paragraphs:
                    paragraph = paragraph.strip()
                    if paragraph:  # Only add non-empty paragraphs
                        body_parts.append(f"<p>{paragraph}</p>")

            body_parts.append('</div>')

        except Exception as e:
            self.logger.error(f"Error building body HTML: {e}", exc_info=True)
            return f"<p>Error building content: {e}</p>"

        return ''.join(body_parts)

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
                
                body_html = self._build_body_html(article_data)
                
                news = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=body_html,
                    summary="",  # AP doesn't provide summary in feed
                    url=url,
                    published_at=published_at.isoformat() if hasattr(published_at, 'isoformat') else str(published_at),
                    image_url=article_data.get("image_url", ""),
                    video_url="",
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
                    f"Saved AP photo article: {article_data['title'][:50]}...",
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
        """Fetch photo news from AP API."""
        try:
            saved_count = 0
            skipped_count = 0
            
            # Use next_page from previous cycle, or start with base URL
            feed_url = self.next_page_url if self.next_page_url else None
            
            # Fetch feed
            feed_data = await self._fetch_feed(feed_url)
            if not feed_data:
                self.logger.warning("No feed data received")
                return
            
            # Parse articles (stores next_page_url in self.next_page_url for next cycle)
            articles = await self._parse_feed_items(feed_data)
            if not articles:
                self.logger.info("No articles parsed from feed")
                return
            
            total = len(articles)
            self.logger.info(f"Processing {total} articles...")
            
            for i, article in enumerate(articles, 1):
                try:
                    guid = article.get("guid")
                    if not guid:
                        continue
                    
                    if await self._article_exists(guid):
                        skipped_count += 1
                        continue
                    
                    # Download preview image
                    image_url = None
                    if article.get("preview_image_url"):
                        self.logger.info(f"Downloading preview image for {guid}")
                        image_content = await self._download_image(article["preview_image_url"])
                        if image_content:
                            filename = f"preview_{guid}.jpg"
                            image_url = await self._upload_to_s3(image_content, filename, "image/jpeg")
                            if image_url:
                                self.logger.info(f"Preview image uploaded: {filename}")
                        else:
                            self.logger.warning(f"Failed to download preview image for {guid}")
                    
                    # Update article data with image URL
                    article["image_url"] = image_url
                    
                    if await self._save_article(article):
                        saved_count += 1
                        self.logger.info(f"Saved {saved_count}/{total}: {article.get('title', 'N/A')[:50]}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing article: {e}", exc_info=True)
                    continue
            
            self.logger.info(f"Processed {total} articles, saved {saved_count}, skipped {skipped_count}")
            if self.next_page_url:
                self.logger.info(f"Next page URL stored for next cycle: {self.next_page_url[:100]}...")
            
        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

