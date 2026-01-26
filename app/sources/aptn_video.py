"""AP Video News Worker - Fetches video articles from AP API."""

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging
import os
import hashlib
from io import BytesIO
import xml.etree.ElementTree as ET
import tempfile

import aiohttp
import yt_dlp
from sqlalchemy import select
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

from app.core.config import settings
from app.core.logging import setup_logging
from app.db.models import News
from app.db.session import AsyncSessionLocal
from app.storage.s3 import get_s3_session, init_s3
from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter
from app.core.category_normalizer import normalize_category

logger = setup_logging()


class APVideoWorker(BaseWorker):
    """Worker for fetching video articles from AP API."""

    def __init__(self):
        """Initialize AP video worker."""
        super().__init__(source_name="aptn_video")
        
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
                url = "https://api.ap.org/media/v/content/feed?q=type:video&page_size=20"
            
            self.logger.debug(f"Fetching feed from {url[:100]}...")
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    self.logger.error(f"Feed fetch failed with status {response.status}")
                    response_text = await response.text()
                    self.logger.error(f"Response: {response_text[:500]}")
                    return None
                
                json_response = await response.json()
                return json_response
                
        except Exception as e:
            self.logger.error(f"Error fetching feed: {e}", exc_info=True)
            return None

    async def _fetch_item_detail(self, uri: str) -> Optional[Dict[str, Any]]:
        """Fetch item detail from URI to get urgency, language, and other metadata."""
        if not self.ap_api_key:
            self.logger.error("AP_API_KEY is not set, cannot fetch item detail.")
            return None
        
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api_detail"
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
        """Download XML body from renditions.script_nitf.href."""
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
            self.logger.error(f"Error downloading XML: {e}", exc_info=True)
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

    def _progress_hook(self, d: Dict[str, Any]) -> None:
        """Progress hook for yt-dlp downloads."""
        if d.get('status') == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            downloaded = d.get('downloaded_bytes', 0)
            if total:
                percent = (downloaded / total) * 100
                speed = d.get('speed', 0)
                speed_mb = speed / (1024 * 1024) if speed else 0
                self.logger.info(
                    f"Download progress: {percent:.1f}% ({downloaded // 1024 // 1024} MB / {total // 1024 // 1024} MB) - {speed_mb:.2f} MB/s"
                )
            else:
                downloaded_mb = downloaded // 1024 // 1024
                self.logger.info(f"Downloading... {downloaded_mb} MB downloaded")
        elif d.get('status') == 'finished':
            self.logger.info(f"Download finished: {d.get('filename', 'unknown')}")

    async def _download_video(self, url: str) -> Optional[bytes]:
        """Download video using yt-dlp (similar to AFP method)."""
        temp_path = None
        temp_dir = None
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="video"
            )
            
            self.logger.info(f"Downloading video from {url[:80]}...")
            self.logger.debug(f"Full video URL: {url[:200]}")
            
            # Create temporary directory for yt-dlp output
            temp_dir = tempfile.mkdtemp()
            temp_filename = os.path.join(temp_dir, 'video.%(ext)s')
            
            # Configure yt-dlp options
            ydl_opts = {
                'outtmpl': temp_filename,
                'quiet': True,
                'no_warnings': False,
                'progress_hooks': [self._progress_hook],
                'format': 'best',
            }
            
            # Add headers for authentication if API key is available
            if self.ap_api_key:
                ydl_opts['http_headers'] = {
                    'x-api-key': self.ap_api_key,
                }
            
            # Download using yt-dlp in thread pool (yt-dlp is synchronous)
            def download_sync():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
            
            await asyncio.to_thread(download_sync)
            
            # Find the downloaded file (yt-dlp may add extension)
            downloaded_files = [f for f in os.listdir(temp_dir) if f.startswith('video.')]
            if not downloaded_files:
                self.logger.error(f"No video file found in temp directory: {temp_dir}")
                return None
            
            # Use the first matching file
            downloaded_file = os.path.join(temp_dir, downloaded_files[0])
            temp_path = downloaded_file
            
            # Read the downloaded file
            with open(temp_path, 'rb') as f:
                video_data = f.read()
            
            self.logger.info(
                f"Successfully downloaded video: {len(video_data)} bytes ({len(video_data)/1024/1024:.2f} MB)",
                extra={
                    "url": url[:60],
                    "size_mb": f"{len(video_data)/1024/1024:.2f}"
                }
            )
            
            return video_data
                
        except Exception as e:
            self.logger.error(
                f"Error downloading video: {e}",
                extra={"url": url[:80]},
                exc_info=True
            )
            return None
        finally:
            # Clean up temporary files and directory
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    self.logger.warning(f"Failed to delete temp file {temp_path}: {e}")
            
            if temp_dir and os.path.exists(temp_dir):
                try:
                    # Remove any remaining files in temp directory
                    for f in os.listdir(temp_dir):
                        try:
                            os.unlink(os.path.join(temp_dir, f))
                        except:
                            pass
                    os.rmdir(temp_dir)
                except Exception as e:
                    self.logger.warning(f"Failed to delete temp directory {temp_dir}: {e}")

    async def _upload_to_s3(self, data: bytes, filename: str, content_type: str) -> Optional[str]:
        """Upload data (image or video) to S3."""
        if not self._s3_initialized:
            await init_s3()
            self._s3_initialized = True

        try:
            # Generate S3 key
            timestamp = datetime.now().strftime("%Y/%m/%d")

            # Extract extension from filename
            extension = filename.split(".")[-1].lower() if "." in filename else ("jpg" if "image" in content_type else "mp4")
            if extension not in ["jpg", "jpeg", "png", "gif", "webp", "mp4"]:
                extension = "jpg" if "image" in content_type else "mp4"

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

    def _extract_rendition_urls(self, renditions: Dict[str, Any]) -> Dict[str, str]:
        """Extract thumbnail, video, and script_nitf URLs from renditions."""
        urls = {
            "thumbnail": "",
            "video": "",
            "script_nitf": ""
        }

        try:
            # Iterate through all renditions
            for key, rendition in renditions.items():
                if isinstance(rendition, dict):
                    href = rendition.get("href", "")
                    
                    if not href:
                        continue
                    
                    # Look for main_1080_25 for video
                    if key == "main_1080_25" or "main_1080_25" in str(key).lower():
                        urls["video"] = href
                        self.logger.debug(f"Found video: {key}")
                    
                    # Look for script_nitf for body text
                    if key == "script_nitf" or "script_nitf" in str(key).lower():
                        urls["script_nitf"] = href
                        self.logger.debug(f"Found script_nitf: {key}")
                    
                    # Look for thumbnail (fallback to first image-like rendition)
                    title = rendition.get("title", "")
                    if "Thumbnail" in title or (not urls["thumbnail"] and "image" in str(key).lower()):
                        urls["thumbnail"] = href
            
        except Exception as e:
            self.logger.error(f"Error extracting rendition URLs: {e}", exc_info=True)

        return urls

    async def _parse_feed_items(self, feed_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse feed JSON and extract article data."""
        articles = []
        
        try:
            data = feed_data.get("data", {})
            items = data.get("items", [])
            
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
                    
                    # Extract rendition URLs from detail item (prefer detail item, fallback to feed item)
                    source_item = detail_item if detail_item else item
                    renditions = source_item.get("renditions", {})
                    
                    if not renditions:
                        self.logger.warning(f"No renditions found for item {itemid} (title: {article_data.get('title', 'N/A')[:50]})")
                        continue
                    
                    rendition_urls = self._extract_rendition_urls(renditions)
                    article_data["thumbnail_url"] = rendition_urls.get("thumbnail", "")
                    article_data["video_url"] = rendition_urls.get("video", "")
                    article_data["script_nitf_url"] = rendition_urls.get("script_nitf", "")
                    
                    if rendition_urls.get("video"):
                        self.logger.debug(f"Found video URL for item {itemid}")
                    else:
                        self.logger.debug(f"No video URL found for item {itemid}")
                    
                    # Skip if no video URL
                    if not article_data.get("video_url"):
                        self.logger.warning(f"No video URL found for item {itemid} (title: {article_data.get('title', 'N/A')[:50]}). Renditions keys: {list(renditions.keys())[:10]}")
                        continue
                    
                    # Download and parse script_nitf XML for body
                    if article_data.get("script_nitf_url"):
                        xml_content = await self._download_xml_body(article_data["script_nitf_url"])
                        if xml_content:
                            body_text = self._parse_nitf_xml(xml_content)
                            if body_text:
                                article_data["body"] = body_text
                                self.logger.debug(f"Successfully parsed body from XML for item {itemid} (body length: {len(body_text)})")
                            else:
                                self.logger.warning(f"Could not parse body from XML for item {itemid} (title: {article_data.get('title', 'N/A')[:50]})")
                        else:
                            self.logger.warning(f"Could not download script_nitf XML for item {itemid} (title: {article_data.get('title', 'N/A')[:50]}, XML URL: {article_data.get('script_nitf_url', 'N/A')[:100]})")
                    else:
                        self.logger.debug(f"No script_nitf URL found for item {itemid}")
                    
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
        """Build HTML body with embedded video player."""
        body_parts = []

        try:
            # Wrap all content in LTR div for proper text direction
            body_parts.append('<div dir="ltr" style="direction: ltr; text-align: left;">')

            # Add video player first
            if article_data.get("video_url"):
                video_style = (
                    "max-width: 100%; "
                    "max-height: 80vh; "
                    "width: auto; "
                    "height: auto; "
                    "border-radius: 5px; "
                    "box-shadow: 0 2px 8px rgba(0,0,0,0.1);"
                )
                body_parts.append(f'''
                    <div style="margin: 20px 0; text-align: center;">
                        <video controls style="{video_style}" preload="metadata">
                            <source src="{article_data['video_url']}" type="video/mp4">
                            Your browser does not support the video tag.
                        </video>
                    </div>
                ''')

            # Add description/body text with paragraph recognition after video
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
            self.logger.error(f"Error building video body HTML: {e}", exc_info=True)
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
                    video_url=article_data.get("video_url", ""),
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
                    f"Saved AP video article: {article_data['title'][:50]}...",
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

    async def process_articles(self, articles: List[Dict[str, Any]]) -> None:
        """Process and save video articles."""
        saved_count = 0
        skipped_count = 0
        total = len(articles)

        try:
            if total > 0:
                self.logger.info(f"Processing {total} video articles...")
            
            for i, article_data in enumerate(articles, 1):
                try:
                    guid = article_data.get("guid")
                    if not guid:
                        continue

                    if await self._article_exists(guid):
                        skipped_count += 1
                        self.logger.debug(f"Video already exists, skipping: {guid}")
                        continue

                    self.logger.info(f"[{i}/{total}] Processing: {article_data.get('title', 'N/A')[:50]}")

                    # Download thumbnail image
                    image_url = None
                    if article_data.get("thumbnail_url"):
                        self.logger.info(f"Downloading thumbnail for {guid}")
                        image_content = await self._download_image(article_data["thumbnail_url"])
                        if image_content:
                            filename = f"thumbnail_{guid}.jpg"
                            image_url = await self._upload_to_s3(image_content, filename, "image/jpeg")
                            if image_url:
                                self.logger.info(f"Thumbnail uploaded: {filename}")
                        else:
                            self.logger.warning(f"Failed to download thumbnail for {guid}")
                    else:
                        self.logger.warning(f"No thumbnail URL found for {guid}")

                    # Download video
                    video_url = None
                    if article_data.get("video_url"):
                        self.logger.info(f"Downloading video for {guid}")
                        video_content = await self._download_video(article_data["video_url"])
                        if video_content:
                            filename = f"video_{guid}.mp4"
                            video_url = await self._upload_to_s3(video_content, filename, "video/mp4")
                            if video_url:
                                self.logger.info(f"Video uploaded: {filename}")
                        else:
                            self.logger.warning(f"Failed to download video for {guid}")
                    else:
                        self.logger.warning(f"No video URL found for {guid}")

                    processed_data = {
                        **article_data,
                        "image_url": image_url,
                        "video_url": video_url,
                    }

                    if await self._save_article(processed_data):
                        saved_count += 1
                        self.logger.info(f"Saved {saved_count}/{total}: {article_data.get('title', 'N/A')[:50]}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing article: {e}", exc_info=True)
                    continue
            
            self.logger.info(f"Processed {total} video articles, saved {saved_count}, skipped {skipped_count}")
            
        except Exception as e:
            self.logger.error(f"Error processing articles: {e}", exc_info=True)

    async def fetch_news(self) -> None:
        """Fetch video news from AP API."""
        try:
            # Always use base URL (no pagination)
            base_url = "https://api.ap.org/media/v/content/feed?q=type:video&page_size=20"
            
            # Fetch feed
            feed_data = await self._fetch_feed(base_url)
            if not feed_data:
                self.logger.warning("No feed data received")
                return
            
            # Parse articles
            articles = await self._parse_feed_items(feed_data)
            if not articles:
                self.logger.info("No articles parsed from feed")
                return
            
            # Process articles (download media and save)
            await self.process_articles(articles)
            
        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

