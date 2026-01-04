"""Reuters Photos API worker implementation."""

import asyncio
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from typing import Optional, Dict, Any
from urllib.parse import urljoin
import tempfile
import os

import aiohttp
import yt_dlp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from app.core.config import settings
from app.core.logging import setup_logging
from app.core.category_normalizer import normalize_category
from app.db.base import AsyncSessionLocal
from app.db.models import News
from app.storage.s3 import get_s3_session, init_s3
from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter

logger = setup_logging(source="reuters_photos")

# Reuters API settings
REUTERS_AUTH_URL = "https://commerce.reuters.com/rmd/rest/xml/login"
REUTERS_ITEMS_URL = "http://rmb.reuters.com/rmd/rest/xml/items"
REUTERS_ITEM_URL = "http://rmb.reuters.com/rmd/rest/xml/item"
REUTERS_CHANNEL = "pwu404"
REUTERS_USERNAME = os.getenv("REUTERS_USERNAME")
REUTERS_PASSWORD = os.getenv("REUTERS_PASSWORD")

# HTTP client settings
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=20)
HTTP_RETRIES = 3


class ReutersPhotosWorker(BaseWorker):
    """Worker for Reuters Photos API feed."""

    def __init__(self):
        """Initialize Reuters Photos worker."""
        super().__init__("reuters_photos")
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._s3_initialized = False
        self.auth_token: Optional[str] = None
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.http_session is None or self.http_session.closed:
            # Create a connector with cookies enabled
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
            
            # Create cookie jar to handle cookies like a browser
            cookie_jar = aiohttp.CookieJar(unsafe=True)
            
            self.http_session = aiohttp.ClientSession(
                timeout=HTTP_TIMEOUT,
                connector=connector,
                cookie_jar=cookie_jar,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8,application/xml,text/xml",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                }
            )
        return self.http_session

    async def _authenticate(self) -> Optional[str]:
        """Authenticate with Reuters API and get auth token."""
        try:
            # Check if credentials are available
            if not REUTERS_USERNAME or not REUTERS_PASSWORD:
                logger.error("Reuters username or password not configured. Please set REUTERS_USERNAME and REUTERS_PASSWORD in .env file")
                return None

            session = await self._get_http_session()

            params = {
                "username": REUTERS_USERNAME,
                "password": REUTERS_PASSWORD,
            }
            
            logger.info("Authenticating with Reuters API...")
            async with session.get(REUTERS_AUTH_URL, params=params) as response:
                response.raise_for_status()
                xml_content = await response.text()
                
                # Parse XML to extract auth token
                # Response format: <?xml version="1.0" encoding="UTF-8" standalone="yes"?><authToken>TOKEN</authToken>
                root = ET.fromstring(xml_content)
                
                # The root element itself is <authToken>
                if root.tag == "authToken" and root.text:
                    self.auth_token = root.text.strip()
                    logger.info("Successfully authenticated with Reuters API")
                    return self.auth_token
                else:
                    logger.error(f"Auth token not found in response. Root tag: {root.tag}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error authenticating with Reuters API: {e}", exc_info=True)
            return None

    async def _fetch_items_list(self, limit: int = 10) -> Optional[str]:
        """Fetch list of items from Reuters API."""
        if not self.auth_token:
            await self._authenticate()
            
        if not self.auth_token:
            return None
            
        try:
            session = await self._get_http_session()
            
            params = {
                "channel": REUTERS_CHANNEL,
                "token": self.auth_token,
                "limit": str(limit),
                "mediaType": "P",  # P for Photos
                "completeSentences": "true",
            }
            
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="list"
            )
            
            logger.debug(f"Fetching items list (limit={limit})...")
            async with session.get(REUTERS_ITEMS_URL, params=params) as response:
                response.raise_for_status()
                xml_content = await response.text()
                return xml_content
                
        except Exception as e:
            logger.error(f"Error fetching items list: {e}", exc_info=True)
            return None

    async def _fetch_item_detail(self, item_id: str) -> Optional[str]:
        """Fetch detailed item data from Reuters API."""
        if not self.auth_token:
            await self._authenticate()
            
        if not self.auth_token:
            return None
            
        try:
            session = await self._get_http_session()
            
            params = {
                "id": item_id,
                "channel": REUTERS_CHANNEL,
                "token": self.auth_token,
            }
            
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="detail"
            )
            
            logger.debug(f"Fetching item detail (id={item_id})...")
            async with session.get(REUTERS_ITEM_URL, params=params) as response:
                response.raise_for_status()
                xml_content = await response.text()
                return xml_content
                
        except Exception as e:
            logger.error(f"Error fetching item detail: {e}", exc_info=True)
            return None

    def _parse_items_list(self, xml_content: str) -> list[Dict[str, str]]:
        """Parse items list XML and extract item IDs and GUIDs."""
        try:
            root = ET.fromstring(xml_content)
            items = []
            
            # Find all result elements
            for result in root.findall(".//result"):
                item_id = result.find("id")
                guid = result.find("guid")
                
                if item_id is not None and guid is not None:
                    items.append({
                        "id": item_id.text,
                        "guid": guid.text,
                    })
            
            logger.debug(f"Parsed {len(items)} items from list")
            return items
            
        except Exception as e:
            logger.error(f"Error parsing items list: {e}", exc_info=True)
            return []

    def _parse_item_detail(self, xml_content: str) -> Optional[Dict[str, Any]]:
        """Parse item detail XML and extract photo metadata."""
        try:
            root = ET.fromstring(xml_content)
            
            # Define namespaces
            ns = {
                '': 'http://iptc.org/std/nar/2006-10-01/',
                'rtr': 'http://www.reuters.com/ns/2003/08/content',
                'xml': 'http://www.w3.org/XML/1998/namespace',
            }
            
            # Extract fields
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
            
            # Description/Body
            desc_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}description')
            if desc_elem is not None:
                # Get text content (may have sub-elements)
                desc_text = ''.join(desc_elem.itertext()).strip()
                data["body"] = desc_text
            else:
                data["body"] = ""
            
            # Language from newsItem xml:lang attribute
            if news_item is not None:
                data["language"] = news_item.get("{http://www.w3.org/XML/1998/namespace}lang", "en")
            else:
                data["language"] = "en"
            
            # Priority from header
            priority_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}header/{http://iptc.org/std/nar/2006-10-01/}priority')
            data["priority"] = priority_elem.text if priority_elem is not None else "5"
            
            # Sent date from header
            sent_elem = root.find('.//{http://iptc.org/std/nar/2006-10-01/}header/{http://iptc.org/std/nar/2006-10-01/}sent')
            data["sent"] = sent_elem.text if sent_elem is not None else ""
            
            # Photo URLs from remoteContent
            # remoteContent[0] = VIEWIMAGE (medium size - used for both list and detail)
            # remoteContent[1] = THUMBNAIL (small size - extracted but not used)
            # remoteContent[2] = BASEIMAGE (full size - URL only for download button)
            # remoteContent[3] = LIMITEDIMAGE (optional)
            remote_contents = root.findall('.//{http://iptc.org/std/nar/2006-10-01/}contentSet/{http://iptc.org/std/nar/2006-10-01/}remoteContent')
            data["thumbnail_url"] = None  # Extracted but not used (fallback only)
            data["viewimage_url"] = None  # Used for both list and detail display
            data["baseimage_url"] = None  # Used for download button (URL only)
            data["filename"] = None
            
            # Get THUMBNAIL (index 1) - extracted for fallback only, not actively used
            if len(remote_contents) > 1:
                thumb_elem = remote_contents[1]
                data["thumbnail_url"] = thumb_elem.get("href")
                
                if not data["thumbnail_url"]:
                    alt_loc_elem = thumb_elem.find("{http://www.reuters.com/ns/2003/08/content}altLoc")
                    if alt_loc_elem is not None and alt_loc_elem.text:
                        data["thumbnail_url"] = alt_loc_elem.text
            
            # Get VIEWIMAGE (index 0) - used for both list and detail page
            if len(remote_contents) > 0:
                view_elem = remote_contents[0]
                data["viewimage_url"] = view_elem.get("href")
                
                if not data["viewimage_url"]:
                    alt_loc_elem = view_elem.find("{http://www.reuters.com/ns/2003/08/content}altLoc")
                    if alt_loc_elem is not None and alt_loc_elem.text:
                        data["viewimage_url"] = alt_loc_elem.text
            
            # Get BASEIMAGE (index 2) - for download button (URL only, not downloaded)
            if len(remote_contents) > 2:
                base_elem = remote_contents[2]
                data["baseimage_url"] = base_elem.get("href")
                
                if not data["baseimage_url"]:
                    alt_loc_elem = base_elem.find("{http://www.reuters.com/ns/2003/08/content}altLoc")
                    if alt_loc_elem is not None and alt_loc_elem.text:
                        data["baseimage_url"] = alt_loc_elem.text
                
                # Get filename from BASEIMAGE
                alt_id_elem = base_elem.find("{http://www.reuters.com/ns/2003/08/content}altId")
                if alt_id_elem is not None:
                    data["filename"] = alt_id_elem.text
            
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
                data["category"] = "عکس"  # Default to "Photo"
            
            logger.debug(f"Parsed item detail: {data.get('headline', 'N/A')[:50]}, GUID: {data.get('guid', 'N/A')[:30]}")
            return data
            
        except Exception as e:
            logger.error(f"Error parsing item detail: {e}", exc_info=True)
            return None

    async def _download_image(self, url: str) -> Optional[bytes]:
        """Download image from URL using yt-dlp."""
        temp_file = None
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="image"
            )
            
            # Add token to URL
            url_with_token = f"{url}?token={self.auth_token}"
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
            temp_path = temp_file.name
            temp_file.close()
            
            # Configure yt-dlp options
            ydl_opts = {
                'outtmpl': temp_path,
                'quiet': True,
                'no_warnings': True,
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'http_headers': {
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                },
                'nocheckcertificate': True,
                'socket_timeout': 60,
            }
            
            logger.debug(f"Downloading image with yt-dlp from {url[:80]}...")
            
            # Download in thread pool to avoid blocking
            def download_sync():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    # For images, we need to download the URL directly
                    # yt-dlp will handle redirects automatically
                    import urllib.request
                    req = urllib.request.Request(
                        url_with_token,
                        headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.9',
                        }
                    )
                    with urllib.request.urlopen(req, timeout=60) as response:
                        with open(temp_path, 'wb') as f:
                            f.write(response.read())
            
            # Run download in thread pool
            await asyncio.to_thread(download_sync)
            
            # Read the downloaded file
            with open(temp_path, 'rb') as f:
                content = f.read()
            
            logger.info(
                f"Successfully downloaded image with yt-dlp: {len(content)} bytes",
                extra={
                    "url": url[:60],
                    "size_mb": f"{len(content)/1024/1024:.2f}"
                }
            )
            
            return content
                
        except Exception as e:
            logger.error(
                f"Error downloading image with yt-dlp: {e}",
                extra={"url": url[:80]},
                exc_info=True
            )
            return None
        finally:
            # Clean up temporary file
            if temp_file and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass

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

            logger.debug(f"Uploaded image to S3: {s3_key}")
            return s3_key

        except Exception as e:
            logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

    async def _article_exists(self, guid: str) -> bool:
        """Check if article already exists in database by GUID in URL."""
        try:
            async with AsyncSessionLocal() as db:
                # Store GUID as URL for Reuters photos
                result = await db.execute(
                    select(News).where(News.url == f"reuters:{guid}")
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
                url = f"reuters:{article_data['guid']}"
                
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
                )
                
                db.add(news)
                await db.commit()
                
                self.logger.info(
                    f"Saved Reuters photo: {article_data['title'][:50]}...",
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
        """Fetch photos from Reuters API."""
        self.logger.info(f"Starting to fetch photos from {self.source_name}")
        
        try:
            # Authenticate first
            if not await self._authenticate():
                self.logger.error("Failed to authenticate with Reuters API")
                return
            
            # Fetch items list
            items_xml = await self._fetch_items_list(limit=20)
            if not items_xml:
                self.logger.error("Failed to fetch items list")
                return
            
            # Parse items list
            items = self._parse_items_list(items_xml)
            if not items:
                self.logger.warning("No items found in list")
                return
            
            self.logger.info(f"Found {len(items)} photos in feed")
            
            saved_count = 0
            skipped_count = 0
            
            for item in items:
                try:
                    item_id = item["id"]
                    guid = item["guid"]
                    
                    # Check if already exists
                    if await self._article_exists(guid):
                        self.logger.debug(f"Photo already exists, skipping: {guid}")
                        skipped_count += 1
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
                    
                    # Download and upload VIEWIMAGE (for both list and detail page - better quality)
                    viewimage_s3_key = None
                    if detail_data.get("viewimage_url"):
                        viewimage_data = await self._download_image(detail_data["viewimage_url"])
                        if viewimage_data:
                            viewimage_s3_key = await self._upload_image_to_s3(
                                viewimage_data,
                                f"view_{detail_data.get('filename', f'{guid}.jpg')}"
                            )
                            if viewimage_s3_key:
                                logger.info(f"Successfully uploaded VIEWIMAGE for {guid}")
                            else:
                                logger.warning(f"Failed to upload VIEWIMAGE to S3 for {guid}")
                        else:
                            logger.warning(f"Failed to download VIEWIMAGE for {guid}")
                    
                    # Build body HTML with LTR direction for English content
                    body_parts = []
                    
                    # Wrap all content in LTR div
                    body_parts.append('<div dir="ltr" style="direction: ltr; text-align: left;">')
                    
                    # Add description from <description> tag
                    if detail_data.get("body"):
                        body_parts.append(f"<p>{detail_data['body']}</p>")
                    
                    # Add VIEWIMAGE if available (use S3 key directly, API will convert to presigned URL)
                    if viewimage_s3_key:
                        body_parts.append(f'<div class="reuters-image"><img src="{viewimage_s3_key}" alt="{detail_data.get("headline", "Reuters Photo")}" style="max-width: 100%; height: auto;" /></div>')
                    elif detail_data.get("viewimage_url"):
                        # Fallback to direct Reuters URL if S3 upload failed
                        viewimage_with_token = f"{detail_data['viewimage_url']}?token={self.auth_token}"
                        body_parts.append(f'<div class="reuters-image"><img src="{viewimage_with_token}" alt="{detail_data.get("headline", "Reuters Photo")}" style="max-width: 100%; height: auto;" /></div>')
                    
                    # Close LTR div
                    body_parts.append('</div>')
                    
                    # Add download button for BASEIMAGE if available (outside LTR for Persian text)
                    if detail_data.get("baseimage_url"):
                        baseimage_download_url = f"{detail_data['baseimage_url']}?token={self.auth_token}"
                        body_parts.append(f'<div class="reuters-download" dir="rtl" style="margin-top: 15px; text-align: center;"><a href="{baseimage_download_url}" target="_blank" download class="btn-download" style="display: inline-block; padding: 10px 20px; background-color: #e74c3c; color: white; text-decoration: none; border-radius: 5px; font-weight: bold;">⬇ دانلود تصویر با کیفیت اصلی</a></div>')
                    
                    body_html = "".join(body_parts)
                    
                    # Prepare article data
                    # Use headline from <headline> tag for title
                    # Use description from <description> tag for body
                    # Summary should be empty for Reuters photos
                    # Use VIEWIMAGE for both list and detail (better quality than THUMBNAIL)
                    article_data = {
                        "guid": guid,
                        "title": detail_data.get("headline", "Reuters Photo"),
                        "body_html": body_html,
                        "summary": "",  # Empty summary - not needed for Reuters photos
                        "published_at": detail_data.get("sent", ""),
                        "image_url": viewimage_s3_key if viewimage_s3_key else detail_data.get("viewimage_url", ""),
                        "category": detail_data.get("category", "عکس"),
                    }
                    
                    # Save to database
                    if await self._save_article(article_data):
                        saved_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing item: {e}", exc_info=True)
                    continue
            
            self.logger.info(
                f"Finished fetching photos from {self.source_name}: {saved_count} new photos saved, {skipped_count} photos skipped"
            )
            
        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

