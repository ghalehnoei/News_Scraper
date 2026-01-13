"""AFP Video News Worker - Fetches video articles from AFP API."""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import logging
import base64
import xml.etree.ElementTree as ET
from io import BytesIO
import hashlib
import tempfile
import os
import urllib.request

import aiohttp
import os
from sqlalchemy import select
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

logger = setup_logging()


class AFPVideoWorker(BaseWorker):
    """Worker for fetching video articles from AFP API."""

    def __init__(self):
        """Initialize AFP video worker."""
        super().__init__(source_name="afp_video")
        self.afp_username = os.getenv("AFP_USERNAME", "ghasemzade@gmail.com")
        self.afp_password = os.getenv("AFP_PASSWORD", "1234@Qwe")
        self.afp_basic_auth = os.getenv(
            "AFP_BASIC_AUTH",
            "SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJREN1WGczREQ1OWk2YUg="
        )
        self.access_token = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._s3_initialized = False

        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session with proper headers."""
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Content-Type': 'application/json',
                },
                timeout=aiohttp.ClientTimeout(total=60)
            )
        return self.http_session

    async def _authenticate(self) -> bool:
        """Authenticate with AFP API and get access token."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )

            self.logger.info(
                "Authenticating with AFP video API...",
                extra={"source": self.source_name}
            )

            # Use exact same URL as working Postman request
            auth_url = "https://afp-apicore-prod.afp.com/oauth/token?grant_type=password&username=ghasemzade%40gmail.com&password=1234%40Qwe"

            # Exact same headers as working Postman request
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": "Basic SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJRUN1WGczREQ1OWk2YUg="
            }

            # Empty body as in working Postman request
            data = ""

            # Create a fresh session without default headers that might interfere
            async with aiohttp.ClientSession() as session:
                async with session.post(auth_url, headers=headers, data=data) as response:
                    if response.status != 200:
                        self.logger.error(f"Authentication failed with status {response.status}")
                        response_text = await response.text()
                        self.logger.error(f"Response: {response_text}")
                        return False

                    json_response = await response.json()

                    # Extract access token
                    if "access_token" in json_response:
                        self.access_token = json_response["access_token"]
                        self.logger.info("Successfully authenticated with AFP video API")
                        return True
                    else:
                        self.logger.error(f"Access token not found in response: {json_response}")
                        return False

        except Exception as e:
            self.logger.error(f"Error authenticating with AFP video API: {e}", exc_info=True)
            return False

    async def _search_news(self, lang: str = 'en', max_rows: int = 100) -> Optional[Dict[str, Any]]:
        """Search for video news from AFP API."""
        if not self.access_token:
            await self._authenticate()

        if not self.access_token:
            return None

        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )

            session = await self._get_http_session()

            # AFP search endpoint
            search_url = "https://afp-apicore-prod.afp.com/v1/api/search?wt=g2"

            # Prepare headers with Bearer token
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            }

            # Prepare request body for VIDEOS (similar to photos)
            body = {
                "dateRange": {
                    "from": "now-1d",
                    "to": "now"
                },
                "sortOrder": "desc",
                "sortField": "published",
                "maxRows": str(max_rows),
                "lang": lang,
                "query": {
                    "and": [
                        {
                            "name": "class",
                            "and": ["video"]
                        }
                    ]
                }
            }

            self.logger.info(f"Searching AFP videos for language: {lang}")

            async with session.post(search_url, headers=headers, json=body) as response:
                if response.status != 200:
                    self.logger.error(f"Search failed with status {response.status}")
                    response_text = await response.text()
                    self.logger.error(f"Response: {response_text}")
                    return None

                json_response = await response.json()
                self.logger.info(f"AFP video search successful, found videos")
                return json_response

        except Exception as e:
            self.logger.error(f"Error searching AFP videos: {e}", exc_info=True)
            return None

    async def _parse_search_results(self, search_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse search results and extract video data."""
        try:
            videos = []

            # AFP API response structure: response.docs
            response_obj = search_response.get("response", {})
            documents = response_obj.get("docs", [])

            # Fallback to other possible structures
            if not documents:
                documents = search_response.get("documents", [])
            if not documents:
                documents = search_response.get("docs", [])
            if not documents:
                documents = search_response.get("results", [])

            self.logger.info(f"Processing {len(documents)} AFP video search results")

            for doc in documents:

                # Skip if no medias - check different possible locations
                medias = doc.get('medias', [])

                # Check bagItem structure like in afp_photo.py
                if not medias:
                    bag_item = doc.get('bagItem', [])
                    if bag_item and len(bag_item) > 0:
                        bag_item_0 = bag_item[0] if isinstance(bag_item, list) else bag_item
                        medias = bag_item_0.get("medias", []) if isinstance(bag_item_0, dict) else []

                if not medias:
                    continue

                video_data = {}

                # Extract basic info
                video_data["guid"] = doc.get("guid", "")
                video_data["title"] = doc.get("headline", "")
                video_data["language"] = doc.get("language", "en")
                video_data["published_at"] = doc.get("versioncreated", "")

                # Extract video and image URLs
                media_urls = self._extract_image_urls(medias)
                video_data["image_url"] = media_urls.get("preview", "")
                video_data["video_url"] = media_urls.get("video", "")

                # Skip if no video URL
                if not video_data.get("video_url"):
                    self.logger.info(f"Skipping AFP video without video URL: {video_data.get('title', 'N/A')[:50]}...")
                    continue

                # Handle category
                category = doc.get("category") or "ویدیو"
                if isinstance(category, list):
                    video_data["category"] = " ".join(str(item) for item in category)
                else:
                    video_data["category"] = str(category)

                # Urgency filter
                urgency = doc.get("urgency")
                video_data["priority"] = urgency or 3
                if urgency and urgency > 5:
                    self.logger.info(f"Skipping AFP video with high urgency {urgency}: {video_data.get('title', 'N/A')[:50]}...")
                    continue

                # DON'T extract summary for AFP video (unlike AFP photo)
                # The user specifically requested no summary extraction for videos

                # Extract body/description/caption
                body = doc.get("news", "") 
                if isinstance(body, list):
                    video_data["body"] = " ".join(str(item) for item in body)
                else:
                    video_data["body"] = str(body)

                # Process body to recognize paragraphs (split by \n)
                if video_data.get("body"):
                    # Replace literal \n with actual line breaks for paragraphs
                    video_data["body"] = video_data["body"].replace('\\n', '\n')

                # Determine if preview image is vertical
                video_data["is_vertical"] = self._is_vertical_image(medias)

                videos.append(video_data)

        except Exception as e:
            self.logger.error(f"Error parsing AFP video search results: {e}", exc_info=True)

        return videos

    def _extract_image_urls(self, medias: List[Dict[str, Any]]) -> Dict[str, str]:
        """Extract preview image and video URLs from medias."""
        urls = {
            "preview": "",
            "video": ""
        }

        try:
            for media in medias:
                rendition_type = media.get("rendition", "")
                role = media.get("role", "")

                # Preview image for base news image
                if rendition_type == "rnd:preview" and role == "Preview":
                    if "href" in media:
                        urls["preview"] = media["href"]
                        self.logger.info(f"Found preview image URL: {media['href']}")

                # Video file for video_url
                if rendition_type == "afpveprnd:VID_MP4_H264_1920x1080i25_T":
                    if "href" in media:
                        urls["video"] = media["href"]
                        self.logger.info(f"Found video URL: {media['href']}")

            self.logger.info(f"Extracted URLs - Preview: {'YES' if 'preview' in urls else 'NO'}, Video: {'YES' if 'video' in urls else 'NO'}")


        except Exception as e:
            self.logger.error(f"Error extracting video URLs: {e}", exc_info=True)

        return urls

    def _is_vertical_image(self, medias: List[Dict[str, Any]]) -> bool:
        """Check if preview image is vertical based on media dimensions."""
        try:
            for media in medias:
                if media.get("role") == "Preview":
                    width = media.get("width")
                    height = media.get("height")
                    if width and height and height > width:
                        return True
        except Exception as e:
            self.logger.error(f"Error checking image orientation: {e}", exc_info=True)
        return False

    async def _download_image(self, url: str) -> Optional[bytes]:
        """Download image/video from URL."""
        temp_file = None
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="image"
            )

            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
            temp_path = temp_file.name
            temp_file.close()

            # Use urllib.request for downloading (similar to reuters_photos.py)
            import urllib.request

            self.logger.debug(f"Downloading media from {url[:80]}...")

            def download_sync():
                req = urllib.request.Request(
                    url,
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

            self.logger.info(
                f"Successfully downloaded media: {len(content)} bytes",
                extra={
                    "url": url[:60],
                    "size_mb": f"{len(content)/1024/1024:.2f}"
                }
            )

            return content

        except Exception as e:
            self.logger.error(
                f"Error downloading media: {e}",
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
            if extension not in ["jpg", "jpeg", "png", "gif", "webp", "mp4"]:
                extension = "jpg"

            # Determine content type based on extension
            if extension == "mp4":
                content_type = f"video/{extension}"
            else:
                content_type = f"image/{extension}"

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
                    ExtraArgs={"ContentType": content_type}
                )

            self.logger.debug(f"Uploaded image to S3: {s3_key}")
            return s3_key

        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

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

    async def _save_article(self, article_data: Dict[str, Any]) -> bool:
        """Save article to database."""
        try:
            self.logger.info(
                f"Starting to save AFP video article: {article_data.get('title', 'N/A')[:50]}...",
                extra={"guid": article_data.get("guid", "N/A")}
            )

            async with AsyncSessionLocal() as db:
                # Use GUID as unique URL identifier
                url = f"afp:{article_data['guid']}"
                self.logger.debug(f"Generated URL: {url}")

                # Check if article already exists
                self.logger.debug("Checking for existing article...")
                result = await db.execute(
                    select(News).where(News.url == url)
                )
                existing = result.scalar_one_or_none()

                if existing:
                    self.logger.info(
                        f"Article already exists in database: {article_data['guid']}",
                        extra={"guid": article_data["guid"], "existing_id": existing.id}
                    )
                    return False

                self.logger.debug("Article does not exist, proceeding with save...")

                # Normalize category
                raw_category = article_data.get("category", "")
                self.logger.debug(f"Raw category: '{raw_category}'")
                normalized_category, preserved_raw = normalize_category(self.source_name, raw_category)
                self.logger.debug(f"Normalized category: '{normalized_category}', preserved raw: '{preserved_raw}'")

                # Truncate raw_category if too long
                if len(preserved_raw) > 197:
                    preserved_raw = preserved_raw[:197] + "..."
                    self.logger.debug(f"Truncated raw_category to fit database limit: {len(preserved_raw)} chars")

                # Parse published date
                published_at = None
                if article_data.get("published_at"):
                    try:
                        published_at_str = article_data["published_at"]
                        self.logger.debug(f"Parsing published date: '{published_at_str}'")
                        if published_at_str.endswith("Z"):
                            published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                        else:
                            published_at = datetime.fromisoformat(published_at_str)
                        self.logger.debug(f"Parsed date: {published_at}")
                    except Exception as e:
                        self.logger.warning(f"Could not parse date: {article_data.get('published_at')}, error: {e}")
                        published_at = datetime.utcnow()

                if not published_at:
                    published_at = datetime.utcnow()
                    self.logger.debug("Using current UTC time as published date")

                # Build body HTML with embedded video
                self.logger.debug("Building body HTML...")
                body_html = self._build_body_html(article_data)
                self.logger.debug(f"Body HTML length: {len(body_html)} characters")

                self.logger.debug("Creating News object...")
                news_article = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=body_html,
                    summary="",  # No summary for AFP video
                    url=url,
                    published_at=published_at.isoformat() if hasattr(published_at, 'isoformat') else str(published_at),
                    image_url=article_data.get("image_url", ""),
                    video_url=article_data.get("video_url", ""),
                    is_vertical_image=article_data.get("is_vertical", False),
                    category=normalized_category,
                    raw_category=preserved_raw,
                    language=article_data.get("language", "en"),
                    priority=article_data.get("priority", 3),
                    is_international=True,  # AFP is an international source
                    source_type='external',
                )

                self.logger.debug(f"News object created - Title: '{news_article.title[:30]}...', URL: '{news_article.url}', Image: {bool(news_article.image_url)}, Video: {bool(news_article.video_url)}")

                self.logger.debug("Adding article to database...")
                db.add(news_article)

                self.logger.debug("Committing transaction...")
                await db.commit()

                self.logger.info(
                    f"✅ Successfully saved AFP video article: {article_data['title'][:50]}...",
                    extra={
                        "guid": article_data["guid"],
                        "title": article_data["title"][:30],
                        "has_image": bool(article_data.get("image_url")),
                        "has_video": bool(article_data.get("video_url")),
                        "category": normalized_category
                    }
                )
                return True

        except Exception as e:
            self.logger.error(f"Error saving AFP video article: {e}", exc_info=True)
            return False

    async def process_articles(self, articles: List[Dict[str, Any]]) -> None:
        """Process and save video articles."""
        saved_count = 0

        try:
            self.logger.info(f"Processing {len(articles)} AFP video articles...")
            for i, article_data in enumerate(articles, 1):
                try:
                    self.logger.info(
                        f"[{i}/{len(articles)}] Processing article: {article_data.get('title', 'N/A')[:40]}...",
                        extra={"guid": article_data.get("guid", "N/A")}
                    )

                    # Download and upload preview image
                    image_url = None
                    if article_data.get("image_url"):
                        self.logger.info(f"📷 Downloading preview image...")
                        image_content = await self._download_image(article_data["image_url"])
                        if image_content:
                            self.logger.info(f"📤 Uploading image to S3 ({len(image_content)} bytes)...")
                            filename = f"preview_{article_data['guid']}.jpg"
                            image_url = await self._upload_image_to_s3(image_content, filename)
                            if image_url:
                                self.logger.info(f"✅ Image uploaded: {image_url}")
                            else:
                                self.logger.error("❌ Image upload failed")
                        else:
                            self.logger.warning("❌ Image download failed")
                    else:
                        self.logger.debug("No preview image URL found, skipping image download")

                    # Download and upload video
                    video_url = None
                    if article_data.get("video_url"):
                        self.logger.info(f"🎥 Downloading video...")
                        video_content = await self._download_image(article_data["video_url"])
                        if video_content:
                            self.logger.info(f"📤 Uploading video to S3 ({len(video_content)} bytes)...")
                            filename = f"video_{article_data['guid']}.mp4"
                            video_url = await self._upload_image_to_s3(video_content, filename)
                            if video_url:
                                self.logger.info(f"✅ Video uploaded: {video_url}")
                            else:
                                self.logger.error("❌ Video upload failed")
                        else:
                            self.logger.warning("❌ Video download failed")
                    else:
                        self.logger.warning("❌ No video URL found!")

                    # Prepare article data
                    processed_data = {
                        **article_data,
                        "image_url": image_url,
                        "video_url": video_url,
                        "is_vertical": article_data.get("is_vertical", False),
                    }

                    self.logger.info(f"💾 Saving article to database...")
                    # Save article
                    saved = await self._save_article(processed_data)
                    if saved:
                        saved_count += 1
                        self.logger.info(f"✅ Article saved successfully ({saved_count}/{i})")
                    else:
                        self.logger.warning(f"⚠️ Article save skipped (already exists or error)")

                    # Rate limiting
                    await asyncio.sleep(1)

                except Exception as e:
                    self.logger.error(f"Error processing video: {e}", exc_info=True)
                    continue

        except Exception as e:
            self.logger.error(f"Error in process_articles: {e}", exc_info=True)
        finally:
            await self.close()

    async def fetch_news(self) -> None:
        """Main method to fetch and process AFP video news."""
        try:
            self.logger.info(f"🚀 Starting to fetch videos from {self.source_name}")

            # Authenticate
            self.logger.info("🔐 Authenticating with AFP API...")
            if not await self._authenticate():
                self.logger.error("❌ Authentication failed, aborting video fetch")
                return
            self.logger.info("✅ Authentication successful")

            saved_count = 0
            languages = ['en']  # Focus on English for now

            for lang in languages:
                try:
                    self.logger.info(f"🔍 Searching for videos in language: {lang}")

                    # Search for videos
                    search_result = await self._search_news(lang=lang, max_rows=50)
                    if not search_result:
                        self.logger.warning(f"⚠️ Search failed for language {lang}")
                        continue

                    self.logger.info(f"📋 Search completed, parsing results...")

                    # Parse results
                    videos = await self._parse_search_results(search_result)
                    if not videos:
                        self.logger.warning(f"⚠️ No videos found for language {lang}")
                        continue

                    self.logger.info(f"📹 Found {len(videos)} videos, processing...")

                    # Process articles
                    await self.process_articles(videos)

                    # Small delay between languages
                    await asyncio.sleep(1)

                except Exception as e:
                    self.logger.error(f"❌ Error processing language {lang}: {e}", exc_info=True)
                    continue

            self.logger.info(f"🎉 Finished fetching videos from {self.source_name}: {saved_count} videos processed")

        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()