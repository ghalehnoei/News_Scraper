"""Reuters Video News Worker - Fetches video articles from Reuters API."""

import asyncio
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional
from io import BytesIO
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
from app.storage.s3 import get_s3_session, init_s3

logger = setup_logging()

# Video channels to fetch from
VIDEO_CHANNELS = [
    "Bzd359",
    "Dir447",
    "LVj378",
    "TdX812",
    "Wbz248",
    "cRK028",
    "dcn998",
    "fbg014",
    "hig865",
    "jts558"
]


class ReutersVideoWorker(BaseWorker):
    """Worker for fetching video articles from Reuters API."""

    def __init__(self):
        """Initialize Reuters video worker."""
        super().__init__(source_name="reuters_video")
        self.reuters_username = os.getenv("REUTERS_USERNAME")
        self.reuters_password = os.getenv("REUTERS_PASSWORD")
        self.auth_token = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._s3_initialized = False
        
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

    async def _fetch_video_channels(self) -> Optional[str]:
        """Fetch list of video channels from Reuters API."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            channels_url = f"http://rmb.reuters.com/rmd/rest/xml/channels?channelCategory=BRV&token={self.auth_token}"
            
            self.logger.info(
                f"Fetching video channels from: {channels_url}",
                extra={"source": self.source_name}
            )
            
            async with session.get(channels_url) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to fetch channels, status: {response.status}")
                    return None
                
                xml_content = await response.text()
                return xml_content
                
        except Exception as e:
            self.logger.error(f"Error fetching video channels: {e}", exc_info=True)
            return None

    def _parse_channels_list(self, xml_content: str) -> List[Dict[str, str]]:
        """Parse channels XML and extract channel information."""
        try:
            root = ET.fromstring(xml_content)
            channels = []
            
            # Find all channelInformation elements
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
            
            self.logger.info(f"Found {len(channels)} video channels")
            return channels
            
        except Exception as e:
            self.logger.error(f"Error parsing channels list: {e}", exc_info=True)
            return []

    async def _fetch_items_list(self, channel: str, limit: int = 20) -> Optional[str]:
        """Fetch list of items from a specific video channel."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            # Use mediaType=V for video and remoteContentComplete=True
            items_url = f"http://rmb.reuters.com/rmd/rest/xml/items?token={self.auth_token}&channel={channel}&limit={limit}&mediaType=V&remoteContentComplete=True"
            
            self.logger.info(
                f"Fetching video items from channel: {channel}",
                extra={"source": self.source_name, "channel": channel}
            )
            
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

    async def _fetch_item_detail(self, item_id: str) -> Optional[str]:
        """Fetch detailed XML for a specific item."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )
            
            session = await self._get_http_session()
            # Use the URL format: http://rmb.reuters.com/rmd/rest/xml/item? with id and token
            detail_url = f"http://rmb.reuters.com/rmd/rest/xml/item?token={self.auth_token}&id={item_id}"
            
            self.logger.debug(f"Fetching item detail for {item_id}")
            
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
        """Parse item detail XML and extract video metadata and content."""
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
                data["category"] = "ویدئو"  # Default to "Video"
            
            # Extract image URL from remoteContent
            # Priority: BASEIMAGE -> VIEWIMAGE -> THUMBNAIL (BASEIMAGE has highest quality)
            # The image is in the picture newsItem (itemClass qcode="icls:picture")
            data["image_url"] = None
            data["image_filename"] = None
            
            # Find all newsItems and check for picture items
            all_news_items = root.findall('.//{http://iptc.org/std/nar/2006-10-01/}newsItem')
            picture_items = []
            
            logger.debug(f"Found {len(all_news_items)} total newsItems in XML")
            
            for news_item in all_news_items:
                # Check if this is a picture item
                item_class_elem = news_item.find('.//{http://iptc.org/std/nar/2006-10-01/}itemMeta/{http://iptc.org/std/nar/2006-10-01/}itemClass')
                if item_class_elem is not None:
                    qcode = item_class_elem.get("qcode", "")
                    logger.debug(f"Found newsItem with itemClass qcode: {qcode}")
                    if qcode == "icls:picture":
                        picture_items.append(news_item)
                        logger.debug(f"Added picture item to list")
            
            logger.debug(f"Found {len(picture_items)} picture items for {data.get('guid', 'N/A')[:30]}")
            
            # Helper function to extract image URL from remoteContent
            def extract_image_url(remote_content):
                """Extract image URL from remoteContent, prefer href over altLoc."""
                image_url = remote_content.get("href")
                if not image_url:
                    # Try altLoc as fallback
                    alt_loc_elem = remote_content.find("{http://www.reuters.com/ns/2003/08/content}altLoc")
                    if alt_loc_elem is not None and alt_loc_elem.text:
                        image_url = alt_loc_elem.text
                
                if image_url:
                    # Get filename from altId if available
                    alt_id_elems = remote_content.findall("{http://www.reuters.com/ns/2003/08/content}altId")
                    filename = None
                    for alt_id_elem in alt_id_elems:
                        if alt_id_elem.text:
                            filename = alt_id_elem.text
                            break
                    return image_url, filename
                return None, None
            
            # Priority 1: Search for BASEIMAGE (highest quality)
            # Search all remoteContent in the document for BASEIMAGE
            logger.info(f"Searching for BASEIMAGE in document for {data.get('guid', 'N/A')[:30]}")
            all_remote_contents = root.findall('.//{http://iptc.org/std/nar/2006-10-01/}contentSet/{http://iptc.org/std/nar/2006-10-01/}remoteContent')
            logger.info(f"Found {len(all_remote_contents)} total remoteContent elements in document")
            
            # Log all renditions found for debugging
            all_renditions = []
            for remote_content in all_remote_contents:
                rendition = remote_content.get("rendition", "")
                content_type = remote_content.get("contenttype", "")
                if rendition:
                    all_renditions.append(f"{rendition} ({content_type})")
            
            if all_renditions:
                logger.info(f"All renditions found: {', '.join(all_renditions[:10])}")  # Show first 10
            
            for remote_content in all_remote_contents:
                rendition = remote_content.get("rendition", "")
                content_type = remote_content.get("contenttype", "")
                # Check for baseImage in rendition (case insensitive) and ensure it's an image
                if "baseimage" in rendition.lower():
                    # Double check it's actually an image (not video/audio)
                    if not content_type or "image" in content_type.lower():
                        logger.info(f"Found BASEIMAGE with rendition: {rendition}, contenttype: {content_type}")
                        image_url, filename = extract_image_url(remote_content)
                        if image_url:
                            data["image_url"] = image_url
                            data["image_filename"] = filename
                            logger.info(f"Successfully extracted BASEIMAGE URL: {image_url[:80]}...")
                            break
            
            if not data["image_url"]:
                logger.warning("BASEIMAGE not found anywhere, will try VIEWIMAGE next")
            
            # Extract video URL from remoteContent
            # Look for video with format="fmt:H264/mpeg" and height="432"
            # The video is in the video newsItem (itemClass qcode="icls:video")
            data["video_url"] = None
            data["video_filename"] = None
            
            # Find all newsItems and check for video items
            video_items = []
            for news_item in all_news_items:
                # Check if this is a video item
                item_class_elem = news_item.find('.//{http://iptc.org/std/nar/2006-10-01/}itemMeta/{http://iptc.org/std/nar/2006-10-01/}itemClass')
                if item_class_elem is not None and item_class_elem.get("qcode") == "icls:video":
                    video_items.append(news_item)
            
            logger.info(f"Found {len(video_items)} video items, searching for video with format='fmt:H264/mpeg' and height='432' for {data.get('guid', 'N/A')[:30]}")
            
            # Search in video items first
            for video_item in video_items:
                video_remote_contents = video_item.findall('.//{http://iptc.org/std/nar/2006-10-01/}contentSet/{http://iptc.org/std/nar/2006-10-01/}remoteContent')
                for remote_content in video_remote_contents:
                    format_attr = remote_content.get("format", "")
                    height_attr = remote_content.get("height", "")
                    content_type = remote_content.get("contenttype", "")
                    
                    # Check for H264/mpeg format and height 432
                    if "H264/mpeg" in format_attr and height_attr == "432" and "video" in content_type.lower():
                        logger.info(f"Found video with format={format_attr}, height={height_attr}, contenttype={content_type}")
                        # Try altLoc first (it's the authenticated URL), then href as fallback
                        video_url = None
                        alt_loc_elem = remote_content.find("{http://www.reuters.com/ns/2003/08/content}altLoc")
                        if alt_loc_elem is not None and alt_loc_elem.text:
                            video_url = alt_loc_elem.text
                            logger.debug(f"Using altLoc for video URL: {video_url[:80]}...")
                        
                        if not video_url:
                            video_url = remote_content.get("href")
                            if video_url:
                                logger.debug(f"Using href for video URL: {video_url[:80]}...")
                        
                        if video_url:
                            data["video_url"] = video_url
                            # Get filename from altId if available
                            alt_id_elems = remote_content.findall("{http://www.reuters.com/ns/2003/08/content}altId")
                            for alt_id_elem in alt_id_elems:
                                if alt_id_elem.text:
                                    data["video_filename"] = alt_id_elem.text
                                    break
                            logger.info(f"Successfully extracted video URL: {video_url[:80]}...")
                            break
                
                if data["video_url"]:
                    break
            
            # If not found in video items, search all remoteContent as fallback
            if not data["video_url"]:
                logger.warning("Video not found in video items, searching all remoteContent")
                for remote_content in all_remote_contents:
                    format_attr = remote_content.get("format", "")
                    height_attr = remote_content.get("height", "")
                    content_type = remote_content.get("contenttype", "")
                    
                    # Check for H264/mpeg format and height 432
                    if "H264/mpeg" in format_attr and height_attr == "432" and "video" in content_type.lower():
                        logger.info(f"Found video in all remoteContent with format={format_attr}, height={height_attr}, contenttype={content_type}")
                        # Try altLoc first (it's the authenticated URL), then href as fallback
                        video_url = None
                        alt_loc_elem = remote_content.find("{http://www.reuters.com/ns/2003/08/content}altLoc")
                        if alt_loc_elem is not None and alt_loc_elem.text:
                            video_url = alt_loc_elem.text
                            logger.debug(f"Using altLoc for video URL: {video_url[:80]}...")
                        
                        if not video_url:
                            video_url = remote_content.get("href")
                            if video_url:
                                logger.debug(f"Using href for video URL: {video_url[:80]}...")
                        
                        if video_url:
                            data["video_url"] = video_url
                            # Get filename from altId if available
                            alt_id_elems = remote_content.findall("{http://www.reuters.com/ns/2003/08/content}altId")
                            for alt_id_elem in alt_id_elems:
                                if alt_id_elem.text:
                                    data["video_filename"] = alt_id_elem.text
                                    break
                            logger.info(f"Successfully extracted video URL: {video_url[:80]}...")
                            break
            
            if not data["video_url"]:
                logger.warning("Video with format='fmt:H264/mpeg' and height='432' not found")
            
            # Priority 2: If no BASEIMAGE found, try VIEWIMAGE
            if not data["image_url"]:
                logger.debug("Searching for VIEWIMAGE")
                for picture_item in picture_items:
                    remote_contents = picture_item.findall('.//{http://iptc.org/std/nar/2006-10-01/}contentSet/{http://iptc.org/std/nar/2006-10-01/}remoteContent')
                    for remote_content in remote_contents:
                        rendition = remote_content.get("rendition", "")
                        if "viewImage" in rendition.lower():
                            logger.debug(f"Found VIEWIMAGE with rendition: {rendition}")
                            image_url, filename = extract_image_url(remote_content)
                            if image_url:
                                data["image_url"] = image_url
                                data["image_filename"] = filename
                                logger.info(f"Using VIEWIMAGE URL: {image_url[:80]}...")
                                break
                    
                    if data["image_url"]:
                        break
            
            # Priority 3: If no VIEWIMAGE found, try THUMBNAIL as last resort
            if not data["image_url"]:
                logger.debug("Searching for THUMBNAIL as last resort")
                for picture_item in picture_items:
                    remote_contents = picture_item.findall('.//{http://iptc.org/std/nar/2006-10-01/}contentSet/{http://iptc.org/std/nar/2006-10-01/}remoteContent')
                    for remote_content in remote_contents:
                        rendition = remote_content.get("rendition", "")
                        if "thumbnail" in rendition.lower() and "thumbnailgrid" not in rendition.lower():
                            logger.debug(f"Found THUMBNAIL with rendition: {rendition}")
                            image_url, filename = extract_image_url(remote_content)
                            if image_url:
                                data["image_url"] = image_url
                                data["image_filename"] = filename
                                logger.warning(f"Using THUMBNAIL (fallback) URL: {image_url[:80]}...")
                                break
                    
                    if data["image_url"]:
                        break
            
            # Log image extraction result
            if data["image_url"]:
                logger.debug(
                    f"Extracted image URL for {data.get('guid', 'N/A')[:30]}: {data['image_url'][:80]}...",
                    extra={"guid": data.get('guid', ''), "image_url": data['image_url']}
                )
            else:
                logger.debug(f"No image URL found for {data.get('guid', 'N/A')[:30]}")
            
            logger.debug(f"Parsed video article: {data.get('headline', 'N/A')[:50]}, GUID: {data.get('guid', 'N/A')[:30]}")
            return data
            
        except Exception as e:
            logger.error(f"Error parsing item detail: {e}", exc_info=True)
            return None

    async def _download_image(self, url: str) -> Optional[bytes]:
        """Download image from URL using aiohttp."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="image"
            )
            
            # Add token to URL (check if URL already has query parameters)
            if "?" in url:
                url_with_token = f"{url}&token={self.auth_token}"
            else:
                url_with_token = f"{url}?token={self.auth_token}"
            
            self.logger.debug(f"Downloading image from {url[:80]}...")
            
            session = await self._get_http_session()
            async with session.get(
                url_with_token,
                headers={
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                },
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to download image, status: {response.status}, URL: {url_with_token[:100]}")
                    return None
                
                content = await response.read()
                
                self.logger.info(
                    f"Successfully downloaded image: {len(content)} bytes",
                    extra={
                        "url": url[:60],
                        "size_mb": f"{len(content)/1024/1024:.2f}"
                    }
                )
                
                return content
                
        except Exception as e:
            self.logger.error(
                f"Error downloading image: {e}",
                extra={"url": url[:80]},
                exc_info=True
            )
            return None

    async def _upload_image_to_s3(self, image_data: bytes, filename: str) -> Optional[str]:
        """Upload image to S3."""
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
            image_hash = hashlib.md5(image_data).hexdigest()[:8]
            s3_key = f"news-images/{self.source_name}/{timestamp}/{image_hash}.{extension}"

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
                    BytesIO(image_data),
                    settings.s3_bucket,
                    s3_key,
                    ExtraArgs={"ContentType": f"image/{extension}"}
                )

            self.logger.debug(f"Uploaded image to S3: {s3_key}")
            return s3_key

        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

    async def _download_video(self, url: str) -> Optional[bytes]:
        """Download video directly from URL using aiohttp."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="video"
            )
            
            # Add token to URL (check if URL already has query parameters)
            if "?" in url:
                url_with_token = f"{url}&token={self.auth_token}"
            else:
                url_with_token = f"{url}?token={self.auth_token}"
            
            self.logger.info(f"Downloading video from {url[:80]}...")
            self.logger.debug(f"Full video URL with token: {url_with_token[:200]}")
            
            # Download video directly using aiohttp
            session = await self._get_http_session()
            async with session.get(
                url_with_token,
                headers={
                    'Accept': 'video/mp4,video/*,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                },
                timeout=aiohttp.ClientTimeout(total=300)  # 5 minutes timeout for videos
            ) as response:
                if response.status != 200:
                    self.logger.error(
                        f"Failed to download video, status: {response.status}, URL: {url[:100]}",
                        extra={"url": url[:100], "status": response.status}
                    )
                    return None
                
                # Read content in chunks to handle large files
                content = b''
                async for chunk in response.content.iter_chunked(8192):
                    content += chunk
                
                self.logger.info(
                    f"Successfully downloaded video: {len(content)} bytes ({len(content)/1024/1024:.2f} MB)",
                    extra={
                        "url": url[:60],
                        "size_mb": f"{len(content)/1024/1024:.2f}"
                    }
                )
                
                return content
                
        except Exception as e:
            self.logger.error(
                f"Error downloading video: {e}",
                extra={"url": url[:80]},
                exc_info=True
            )
            return None

    async def _upload_video_to_s3(self, video_data: bytes, filename: str) -> Optional[str]:
        """Upload video to S3."""
        if not self._s3_initialized:
            await init_s3()
            self._s3_initialized = True

        try:
            # Generate S3 key
            timestamp = datetime.now().strftime("%Y/%m/%d")
            
            # Extract extension from filename
            extension = filename.split(".")[-1].lower() if "." in filename else "mp4"
            if extension not in ["mp4", "mpg", "mpeg", "webm", "mov", "avi"]:
                extension = "mp4"
            
            # Generate hash for uniqueness
            video_hash = hashlib.md5(video_data).hexdigest()[:8]
            s3_key = f"news-videos/{self.source_name}/{timestamp}/{video_hash}.{extension}"

            s3_session = get_s3_session()
            endpoint_uses_https = settings.s3_endpoint.startswith("https://")
            
            from botocore.config import Config
            boto_config = Config(
                connect_timeout=300,  # Longer timeout for videos
                read_timeout=300,
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
                    BytesIO(video_data),
                    settings.s3_bucket,
                    s3_key,
                    ExtraArgs={"ContentType": f"video/{extension}"}
                )

            self.logger.info(f"Uploaded video to S3: {s3_key}")
            return s3_key

        except Exception as e:
            self.logger.error(f"Error uploading video to S3: {e}", exc_info=True)
            return None

    async def _article_exists(self, guid: str) -> bool:
        """Check if article already exists in database by GUID in URL."""
        try:
            async with AsyncSessionLocal() as db:
                # Store GUID as URL for Reuters video articles
                result = await db.execute(
                    select(News).where(News.url == f"reuters_video:{guid}")
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
                url = f"reuters_video:{article_data['guid']}"
                
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
                    is_international=True,  # Reuters is an international source
                    source_type='external',
                )
                
                db.add(news)
                await db.commit()
                
                self.logger.info(
                    f"Saved Reuters video article: {article_data['title'][:50]}...",
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
        """Fetch video articles from Reuters API."""
        self.logger.info(f"Starting to fetch video articles from {self.source_name}")
        
        try:
            # Authenticate first
            if not await self._authenticate():
                self.logger.error("Failed to authenticate with Reuters API")
                return
            
            # Fetch video channels from API
            self.logger.info("Fetching video channels from API...")
            channels_xml = await self._fetch_video_channels()
            if not channels_xml:
                self.logger.error("Failed to fetch video channels from API")
                return
            
            # Parse channels list
            channels = self._parse_channels_list(channels_xml)
            if not channels:
                self.logger.warning("No video channels found, using fallback list")
                # Fallback to hardcoded list if API fails
                channels = [{"alias": ch, "description": ""} for ch in VIDEO_CHANNELS]
            
            self.logger.info(f"Processing {len(channels)} video channels")
            
            saved_count = 0
            skipped_count = 0
            
            # Process each video channel
            for channel_info in channels:
                channel = channel_info["alias"]
                channel_desc = channel_info.get("description", "")
                self.logger.info(f"Processing channel: {channel} - {channel_desc}")
                self.logger.info(f"Processing channel: {channel}")
                
                # Fetch items list from this channel
                items_xml = await self._fetch_items_list(channel, limit=10)
                if not items_xml:
                    continue
                
                # Parse items list
                items = self._parse_items_list(items_xml)
                if not items:
                    self.logger.debug(f"No items found in channel {channel}")
                    continue
                
                self.logger.info(f"Found {len(items)} video articles in channel {channel}")

                # Process each item
                channel_saved = 0
                channel_skipped = 0
                for item in items:
                    try:
                        item_id = item["id"]
                        guid = item["guid"]
                        
                        # Check if already exists
                        if await self._article_exists(guid):
                            self.logger.debug(f"Article already exists, skipping: {guid}")
                            skipped_count += 1
                            channel_skipped += 1
                            continue
                        
                        # Fetch item detail
                        detail_xml = await self._fetch_item_detail(item_id)
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

                        # Download and upload video to S3 (priority over image)
                        video_s3_key = None
                        original_video_url = detail_data.get("video_url", "")
                        video_filename = detail_data.get("video_filename", f"{guid}.mp4")
                        
                        # Download and upload video if available
                        if original_video_url:
                            self.logger.info(
                                f"Found video URL for {guid}: {original_video_url[:80]}...",
                                extra={"guid": guid, "video_url": original_video_url[:100]}
                            )
                            # Download video
                            video_data = await self._download_video(original_video_url)
                            if video_data:
                                self.logger.info(f"Successfully downloaded video for {guid}: {len(video_data)} bytes ({len(video_data)/1024/1024:.2f} MB)")
                                # Upload to S3
                                video_s3_key = await self._upload_video_to_s3(
                                    video_data,
                                    video_filename
                                )
                                if video_s3_key:
                                    self.logger.info(f"Successfully uploaded video to S3 for {guid}: {video_s3_key}")
                                else:
                                    self.logger.warning(f"Failed to upload video to S3 for {guid} - skipping article (video not ready)")
                                    # If video upload failed, skip saving this article
                                    channel_skipped += 1
                                    skipped_count += 1
                                    continue
                            else:
                                self.logger.warning(f"Failed to download video for {guid} from {original_video_url[:80]} - skipping article (video not ready)")
                                # If video download failed, skip saving this article
                                channel_skipped += 1
                                skipped_count += 1
                                continue
                        else:
                            self.logger.debug(f"No video URL found for {guid}")
                        
                        # Download and upload BASEIMAGE (highest quality) to S3 as fallback/thumbnail
                        image_s3_key = None
                        original_image_url = detail_data.get("image_url", "")
                        image_filename = detail_data.get("image_filename", f"{guid}.jpg")
                        
                        if original_image_url:
                            self.logger.info(
                                f"Found image URL for {guid}: {original_image_url[:80]}...",
                                extra={"guid": guid, "image_url": original_image_url[:100]}
                            )
                            # Download image
                            image_data = await self._download_image(original_image_url)
                            if image_data:
                                self.logger.info(f"Successfully downloaded image for {guid}: {len(image_data)} bytes")
                                # Upload to S3
                                image_s3_key = await self._upload_image_to_s3(
                                    image_data,
                                    image_filename
                                )
                                if image_s3_key:
                                    self.logger.info(f"Successfully uploaded image to S3 for {guid}: {image_s3_key}")
                                else:
                                    self.logger.warning(f"Failed to upload image to S3 for {guid} - skipping article (image not ready)")
                                    # If image upload failed, skip saving this article
                                    channel_skipped += 1
                                    skipped_count += 1
                                    continue
                            else:
                                self.logger.warning(f"Failed to download image for {guid} from {original_image_url[:80]} - skipping article (image not ready)")
                                # If image download failed, skip saving this article
                                channel_skipped += 1
                                skipped_count += 1
                                continue
                        else:
                            self.logger.debug(f"No image URL found for {guid}")
                        
                        # If neither video nor image was successfully downloaded and uploaded, skip article
                        if not video_s3_key and not image_s3_key:
                            self.logger.warning(f"No video or image available for {guid} - skipping article (media not ready)")
                            channel_skipped += 1
                            skipped_count += 1
                            continue

                        # Add video or image to HTML
                        media_html = ""
                        if video_s3_key:
                            # Use video (priority) - S3 key will be converted to presigned URL by API
                            media_html = f'''<div class="reuters-video-player" style="margin: 15px 0;">
                                <video controls style="max-width: 100%; height: auto; width: 100%;" poster="{image_s3_key if image_s3_key else ''}">
                                    <source src="{video_s3_key}" type="video/mp4">
                                    Your browser does not support the video tag.
                                </video>
                            </div>'''
                        elif image_s3_key:
                            # Use image if video not available (should be rare, but only if image S3 upload succeeded)
                            media_html = f'<div class="reuters-video-image" style="margin: 15px 0;"><img src="{image_s3_key}" alt="{detail_data.get("headline", "Reuters Video")}" style="max-width: 100%; height: auto;" /></div>'

                        body_html = f'<div dir="{text_direction}" style="{dir_style}">{media_html}{paragraph_html}</div>'
                        
                        # Prepare article data
                        # image_url should contain BASEIMAGE (for list/thumbnail view), NOT video
                        # Video player is only shown in body_html (detail view)
                        final_image_url = image_s3_key if image_s3_key else (original_image_url if original_image_url else "")
                        
                        article_data = {
                            "guid": guid,
                            "title": detail_data.get("headline", "Reuters Video Article"),
                            "body_html": body_html,
                            "summary": "",  # Empty summary for Reuters video
                            "published_at": detail_data.get("sent", ""),
                            "image_url": final_image_url,
                            "category": detail_data.get("category", "ویدئو"),
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
                        self.logger.error(f"Error processing item {item_id} from channel {channel}: {e}", exc_info=True)
                        continue

                # Log channel processing summary
                self.logger.info(f"Channel {channel} processed: {channel_saved} saved, {channel_skipped} skipped")
            
            self.logger.info(
                f"Finished fetching video articles from {self.source_name}: {saved_count} new articles saved, {skipped_count} articles skipped"
            )
            
        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

