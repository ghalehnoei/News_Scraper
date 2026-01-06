"""Service for downloading high-quality Reuters videos."""

import asyncio
import hashlib
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, Any, Callable
from io import BytesIO
import logging

import aiohttp
import yt_dlp
from dotenv import load_dotenv

load_dotenv()

from app.core.config import settings
from app.core.logging import setup_logging
from app.storage.s3 import get_s3_session, init_s3

logger = setup_logging()


class ReutersVideoDownloader:
    """Service for downloading high-quality Reuters videos."""
    
    def __init__(self):
        """Initialize the downloader."""
        self.reuters_username = os.getenv("REUTERS_USERNAME")
        self.reuters_password = os.getenv("REUTERS_PASSWORD")
        self._s3_initialized = False
        
    async def _authenticate(self) -> Optional[str]:
        """Authenticate with Reuters API and get fresh token."""
        try:
            if not self.reuters_username or not self.reuters_password:
                logger.error("Reuters credentials not configured")
                return None
            
            async with aiohttp.ClientSession() as session:
                auth_url = "https://commerce.reuters.com/rmd/rest/xml/login"
                params = {
                    "username": self.reuters_username,
                    "password": self.reuters_password,
                }
                
                async with session.get(auth_url, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Authentication failed with status {response.status}")
                        return None
                    
                    xml_content = await response.text()
                    root = ET.fromstring(xml_content)
                    
                    if root.tag == "authToken" and root.text:
                        token = root.text.strip()
                        logger.info("Successfully authenticated with Reuters API")
                        return token
                    else:
                        logger.error("Auth token not found in response")
                        return None
                        
        except Exception as e:
            logger.error(f"Error during authentication: {e}", exc_info=True)
            return None
    
    async def _fetch_item_detail(self, item_id: str, token: str) -> Optional[str]:
        """Fetch item detail XML."""
        try:
            async with aiohttp.ClientSession() as session:
                detail_url = f"http://rmb.reuters.com/rmd/rest/xml/item?token={token}&id={item_id}"
                
                async with session.get(detail_url) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch item detail for {item_id}, status: {response.status}")
                        return None
                    
                    xml_content = await response.text()
                    return xml_content
                    
        except Exception as e:
            logger.error(f"Error fetching item detail for {item_id}: {e}", exc_info=True)
            return None
    
    def _extract_high_quality_video_url(self, xml_content: str, token: str) -> Optional[str]:
        """Extract high-quality video URL (rend:stream:8256:16x9:mp4) from XML."""
        try:
            root = ET.fromstring(xml_content)
            
            # Find all remoteContent elements
            all_remote_contents = root.findall('.//{http://iptc.org/std/nar/2006-10-01/}remoteContent')
            
            # Find the target rendition
            target_rendition = 'rend:stream:8256:16x9:mp4'
            for remote_content in all_remote_contents:
                rendition = remote_content.get('rendition', '')
                
                # Look for rend:stream:8256m:16x9:mp4 (case-insensitive)
                if rendition.lower() == target_rendition.lower():
                    # Try altLoc first (authenticated URL)
                    alt_loc = remote_content.find('.//{http://www.reuters.com/ns/2006-04-01/}altLoc')
                    if alt_loc is not None and alt_loc.text:
                        url = alt_loc.text.strip()
                        # Add token if not already present
                        if 'token=' not in url:
                            separator = '&' if '?' in url else '?'
                            url = f"{url}{separator}token={token}"
                        return url
                    
                    # Fallback to href
                    href = remote_content.get('href', '')
                    if href:
                        if 'token=' not in href:
                            separator = '&' if '?' in href else '?'
                            href = f"{href}{separator}token={token}"
                        return href
            
            logger.warning(f"High-quality video URL ({target_rendition}) not found in XML")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting video URL from XML: {e}", exc_info=True)
            return None
    
    async def _download_video_with_ytdlp(
        self, 
        url: str, 
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Optional[bytes]:
        """Download video using direct HTTP download (similar to yt-dlp approach)."""
        temp_path = None
        try:
            if progress_callback:
                progress_callback({
                    'status': 'downloading',
                    'progress': 30,
                    'message': 'در حال دانلود ویدئو...'
                })
            
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
            temp_path = temp_file.name
            temp_file.close()
            
            # Download using aiohttp with proper headers
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=600),  # 10 minutes timeout
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'video/mp4,video/*,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
            ) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"Failed to download video, status: {response.status}")
                        if progress_callback:
                            progress_callback({
                                'status': 'error',
                                'progress': 30,
                                'message': f'خطا در دانلود: HTTP {response.status}'
                            })
                        return None
                    
                    # Get content length for progress tracking
                    content_length = response.headers.get('Content-Length')
                    total_size = int(content_length) if content_length else None
                    downloaded = 0
                    
                    # Download in chunks
                    chunk_size = 8192
                    with open(temp_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Update progress if we know total size
                            if total_size and progress_callback:
                                progress = 30 + int((downloaded / total_size) * 60)  # 30-90%
                                progress_callback({
                                    'status': 'downloading',
                                    'progress': progress,
                                    'message': f'در حال دانلود: {downloaded // 1024 // 1024} MB / {total_size // 1024 // 1024 if total_size else "?"} MB'
                                })
            
            # Read the downloaded file
            with open(temp_path, 'rb') as f:
                video_data = f.read()
            
            # Clean up
            os.unlink(temp_path)
            
            if progress_callback:
                progress_callback({
                    'status': 'downloading',
                    'progress': 90,
                    'message': 'دانلود کامل شد'
                })
            
            return video_data
                
        except Exception as e:
            logger.error(f"Error downloading video: {e}", exc_info=True)
            # Clean up on error
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass
            
            if progress_callback:
                progress_callback({
                    'status': 'error',
                    'progress': 30,
                    'message': f'خطا در دانلود: {str(e)}'
                })
            
            return None
    
    async def _upload_to_s3(self, video_data: bytes, s3_key: str) -> bool:
        """Upload video to S3."""
        try:
            # Verify credentials are present
            if not settings.s3_access_key or not settings.s3_secret_key:
                logger.error("S3 credentials are missing! Check environment variables.")
                raise ValueError("S3 credentials are missing")
            
            # Create a new session with credentials explicitly
            import aioboto3
            from botocore.config import Config
            
            endpoint_uses_https = settings.s3_endpoint.startswith("https://")
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
            
            # Create a new session for this upload
            s3_session = aioboto3.Session()
            async with s3_session.client("s3", **client_kwargs) as s3:
                await s3.put_object(
                    Bucket=settings.s3_bucket,
                    Key=s3_key,
                    Body=video_data,
                    ContentType='video/mp4',
                )
            
            logger.info(f"Successfully uploaded video to S3: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading video to S3: {e}", exc_info=True)
            return False
    
    async def download_and_upload(
        self,
        item_id: str,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    ) -> Optional[str]:
        """
        Download high-quality video and upload to S3.
        
        Args:
            item_id: Reuters item ID
            progress_callback: Optional callback for progress updates
            
        Returns:
            S3 key of uploaded video, or None if failed
        """
        try:
            # Step 1: Authenticate
            if progress_callback:
                progress_callback({
                    'status': 'authenticating',
                    'progress': 0,
                    'message': 'در حال احراز هویت...'
                })
            
            token = await self._authenticate()
            if not token:
                if progress_callback:
                    progress_callback({
                        'status': 'error',
                        'progress': 0,
                        'message': 'خطا در احراز هویت'
                    })
                return None
            
            # Step 2: Fetch item detail
            if progress_callback:
                progress_callback({
                    'status': 'fetching',
                    'progress': 10,
                    'message': 'در حال دریافت اطلاعات ویدئو...'
                })
            
            xml_content = await self._fetch_item_detail(item_id, token)
            if not xml_content:
                if progress_callback:
                    progress_callback({
                        'status': 'error',
                        'progress': 10,
                        'message': 'خطا در دریافت اطلاعات ویدئو'
                    })
                return None
            
            # Step 3: Extract video URL
            if progress_callback:
                progress_callback({
                    'status': 'extracting',
                    'progress': 20,
                    'message': 'در حال استخراج آدرس ویدئو...'
                })
            
            video_url = self._extract_high_quality_video_url(xml_content, token)
            if not video_url:
                if progress_callback:
                    progress_callback({
                        'status': 'error',
                        'progress': 20,
                        'message': 'آدرس ویدئو با کیفیت بالا یافت نشد'
                    })
                return None
            
            # Step 4: Download video
            if progress_callback:
                progress_callback({
                    'status': 'downloading',
                    'progress': 30,
                    'message': 'در حال دانلود ویدئو...'
                })
            
            video_data = await self._download_video_with_ytdlp(video_url, progress_callback)
            if not video_data:
                if progress_callback:
                    progress_callback({
                        'status': 'error',
                        'progress': 30,
                        'message': 'خطا در دانلود ویدئو'
                    })
                return None
            
            # Step 5: Generate S3 key
            timestamp = datetime.now().strftime("%Y/%m/%d")
            video_hash = hashlib.md5(video_data).hexdigest()[:8]
            s3_key = f"news-videos/reuters_video/{timestamp}/{video_hash}.mp4"
            
            # Step 6: Upload to S3
            if progress_callback:
                progress_callback({
                    'status': 'uploading',
                    'progress': 90,
                    'message': 'در حال آپلود به S3...'
                })
            
            success = await self._upload_to_s3(video_data, s3_key)
            if not success:
                if progress_callback:
                    progress_callback({
                        'status': 'error',
                        'progress': 90,
                        'message': 'خطا در آپلود به S3'
                    })
                return None
            
            if progress_callback:
                progress_callback({
                    'status': 'completed',
                    'progress': 100,
                    'message': 'دانلود و آپلود با موفقیت انجام شد',
                    's3_key': s3_key
                })
            
            return s3_key
            
        except Exception as e:
            logger.error(f"Error in download_and_upload: {e}", exc_info=True)
            if progress_callback:
                progress_callback({
                    'status': 'error',
                    'progress': 0,
                    'message': f'خطا: {str(e)}'
                })
            return None

