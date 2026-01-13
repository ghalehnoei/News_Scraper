"""AFP Photo News Worker - Fetches photo articles from AFP API."""

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


class AFPPhotoWorker(BaseWorker):
    """Worker for fetching photo articles from AFP API."""

    def __init__(self):
        """Initialize AFP photo worker."""
        super().__init__(source_name="afp_photo")
        self.afp_username = os.getenv("AFP_USERNAME", "ghasemzade@gmail.com")
        self.afp_password = os.getenv("AFP_PASSWORD", "1234@Qwe")
        self.afp_basic_auth = os.getenv(
            "AFP_BASIC_AUTH",
            "SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJREN1WGczREQ1OWk2YUg="
        )
        self.access_token = None
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
                }
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
                "Authenticating with AFP API...",
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

            self.logger.debug(f"Auth URL: {auth_url}")
            self.logger.debug(f"Basic Auth: {self.afp_basic_auth}")
            self.logger.debug(f"Username: {self.afp_username}")
            self.logger.debug(f"Password: {self.afp_password}")
            self.logger.debug(f"Headers: {headers}")

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
                        self.logger.info("Successfully authenticated with AFP API")
                        return True
                        self.logger.error(f"Access token not found in response: {json_response}")
                        return False

        except Exception as e:
            self.logger.error(f"Error authenticating with AFP API: {e}", exc_info=True)
            return False

    async def _search_news(self, lang: str = "en", max_rows: int = 50) -> Optional[Dict[str, Any]]:
        """Search for latest photo news from AFP API."""
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
            search_url = "https://afp-apicore-prod.afp.com/v1/api/search?wt=g2"

            # Prepare headers with Bearer token
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            }

            # Calculate date range (last 24 hours)
            now = datetime.utcnow()
            yesterday = now - timedelta(days=1)

            # Prepare request body for PICTURES
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
                            "and": ["picture"]
                        }
                    ]
                }
            }

            self.logger.info(f"Searching AFP photos for language: {lang}")

            async with session.post(search_url, headers=headers, json=body) as response:
                if response.status != 200:
                    self.logger.error(f"Search failed with status {response.status}")
                    response_text = await response.text()
                    self.logger.error(f"Response: {response_text[:500]}")
                    return None

                json_response = await response.json()
                return json_response

        except Exception as e:
            self.logger.error(f"Error searching AFP photos: {e}", exc_info=True)
            return None

    async def _parse_search_results(self, search_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse search results and extract photo data."""
        try:
            photos = []

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

            for doc in documents:
                photo_data = {}

                # Extract photo fields using AFP API field names
                photo_data["guid"] = doc.get("guid") or doc.get("id") or doc.get("_id", "")

                # Use title as headline
                title = doc.get("title") or doc.get("headline", "")
                photo_data["title"] = title if isinstance(title, str) else (title[0] if isinstance(title, list) and title else "")

                # Extract description from news field (paragraphs)
                news_content = doc.get("news") or ""
                if isinstance(news_content, list):
                    photo_data["body"] = " ".join(str(item) for item in news_content)
                else:
                    photo_data["body"] = str(news_content)

                # Extract summary from abstract field only
                abstract = doc.get("abstract", "")
                if isinstance(abstract, list):
                    photo_data["summary"] = " ".join(str(item) for item in abstract)
                else:
                    photo_data["summary"] = str(abstract)

                # Handle published_at - convert to string
                published_at = doc.get("published") or doc.get("sent") or doc.get("created", "")
                photo_data["published_at"] = str(published_at)
                photo_data["language"] = doc.get("lang") or "en"
                urgency = doc.get("urgency")
                photo_data["priority"] = urgency or 3

                # Skip articles with urgency higher than 4 (only accept urgency 1-4)
                if urgency and urgency > 5:
                    self.logger.info(f"Skipping AFP photo with high urgency {urgency}: {photo_data.get('title', 'N/A')[:50]}...")
                    continue

                # Handle category - may be string or list
                category = doc.get("category") or "عکس"
                if isinstance(category, list):
                    photo_data["category"] = " ".join(str(item) for item in category)
                else:
                    photo_data["category"] = str(category)

                # Extract aspect ratios
                aspect_ratios = doc.get("aspectRatios", [])
                photo_data["aspect_ratios"] = aspect_ratios

                # Extract medias from bagItem structure
                medias = []
                bag_item = doc.get("bagItem", [])
                if isinstance(bag_item, list) and len(bag_item) > 0:
                    bag_item_0 = bag_item[0]
                    medias = bag_item_0.get("medias", [])

                photo_data["medias"] = medias

                # Determine if image is vertical (portrait) using media dimensions
                photo_data["is_vertical"] = self._is_vertical_image_from_medias(medias)

                # Skip if essential fields are missing
                if not photo_data["guid"] or not photo_data["title"]:
                    continue

                photos.append(photo_data)

            self.logger.info(f"Parsed {len(photos)} photos from search results")
            return photos

        except Exception as e:
            self.logger.error(f"Error parsing search results: {e}", exc_info=True)
            return []

    async def _download_image(self, url: str) -> Optional[bytes]:
        """Download image from URL."""
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

            self.logger.debug(f"Downloading image from {url[:80]}...")

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

            self.logger.debug(f"Uploaded image to S3: {s3_key}")
            return s3_key

        except Exception as e:
            self.logger.error(f"Error uploading image to S3: {e}", exc_info=True)
            return None

    def _extract_image_urls(self, medias: List[Dict[str, Any]]) -> Dict[str, str]:
        """Extract high resolution and preview image URLs from medias."""
        urls = {
            "highres": "",
            "preview": ""
        }

        try:
            for media in medias:
                rendition_type = media.get("rendition", "")
                role = media.get("role", "")

                # Find high resolution image (rnd:highRes)
                if rendition_type == "rnd:highRes":
                    urls["highres"] = media.get("href", "")

                # Find preview image (role: Preview)
                if role == "Preview":
                    urls["preview"] = media.get("href", "")

        except Exception as e:
            self.logger.error(f"Error extracting image URLs: {e}", exc_info=True)

        return urls

    def _is_vertical_image_from_medias(self, medias: List[Dict[str, Any]]) -> bool:
        """Determine if image is vertical (portrait) based on media dimensions."""
        if not medias:
            return False

        # Find the preview or high-res media to check dimensions
        for media in medias:
            width = media.get("width")
            height = media.get("height")
            role = media.get("role", "")

            # Use preview media for dimension check (more reliable than high-res for orientation)
            if role == "Preview" and width and height:
                return height > width

        # Fallback: check any media with dimensions
        for media in medias:
            width = media.get("width")
            height = media.get("height")

            if width and height:
                return height > width

        # If no dimensions found, default to horizontal
        return False

    async def _article_exists(self, guid: str) -> bool:
        """Check if article already exists in database by GUID."""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(News).where(News.url == f"afp:{guid}")
                )
                exists = result.scalar_one_or_none() is not None
                return exists
        except Exception as e:
            self.logger.error(f"Error checking article existence: {e}", exc_info=True)
            return False

    async def _save_article(self, article_data: dict) -> bool:
        """Save article to database."""
        async with AsyncSessionLocal() as db:
            try:
                # Use GUID as unique URL identifier
                url = f"afp:{article_data['guid']}"

                result = await db.execute(
                    select(News).where(News.url == url)
                )
                existing = result.scalar_one_or_none()

                if existing:
                    self.logger.debug(
                        f"Article already exists in database: {article_data['guid']}",
                        extra={"guid": article_data["guid"]}
                    )
                    return False

                normalized_category, raw_category = normalize_category(
                    self.source_name,
                    article_data.get("category")
                )

                # Truncate raw_category to fit database limit (200 chars)
                if len(raw_category) > 200:
                    raw_category = raw_category[:197] + "..."

                # Parse published date
                published_at = None
                if article_data.get("published_at"):
                    try:
                        # Try to parse various date formats
                        published_at_str = article_data["published_at"]
                        # Common formats: ISO 8601, Unix timestamp, etc.
                        if isinstance(published_at_str, (int, float)):
                            published_at = datetime.fromtimestamp(published_at_str)
                        else:
                            # Try ISO format
                            published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                    except Exception as e:
                        self.logger.warning(f"Could not parse date: {article_data.get('published_at')}, error: {e}")
                        published_at = datetime.utcnow()

                if not published_at:
                    published_at = datetime.utcnow()

                # Build body HTML with embedded images
                body_html = self._build_body_html(article_data)

                news = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=body_html,
                    summary=article_data.get("summary", ""),
                    url=url,
                    published_at=published_at.isoformat() if hasattr(published_at, 'isoformat') else str(published_at),
                    image_url=article_data.get("image_url", ""),
                    is_vertical_image=article_data.get("is_vertical", False),
                    category=normalized_category,
                    raw_category=raw_category,
                    language=article_data.get("language", "en"),
                    priority=article_data.get("priority", 3),
                    is_international=True,  # AFP is an international source
                    source_type='external',
                )

                db.add(news)
                await db.commit()

                self.logger.info(
                    f"Saved AFP photo: {article_data['title'][:50]}...",
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

    def _build_body_html(self, article_data: dict) -> str:
        """Build HTML body with embedded images."""
        body_parts = []

        try:
            # Wrap all content in LTR div for proper text direction
            body_parts.append('<div dir="ltr" style="direction: ltr; text-align: left;">')

            # Add description/body text
            if article_data.get("body"):
                body_parts.append(f"<p>{article_data['body']}</p>")

            # Add high resolution image if available
            if article_data.get("highres_image_url"):
                # Check if image is vertical and apply appropriate styling
                is_vertical = article_data.get("is_vertical", False)

                if is_vertical:
                    # Vertical images: show full image with proper aspect ratio
                    image_style = (
                        "max-width: 100%; "
                        "max-height: 80vh; "  # Limit height to viewport
                        "width: auto; "
                        "height: auto; "
                        "object-fit: contain; "  # Show full image without cropping
                        "border-radius: 5px; "
                        "box-shadow: 0 2px 8px rgba(0,0,0,0.1);"
                    )
                    container_style = (
                        "margin: 20px 0; "
                        "text-align: center; "
                        "display: flex; "
                        "justify-content: center; "
                        "align-items: center; "
                        "min-height: 200px;"  # Ensure minimum space for vertical images
                    )
                else:
                    # Horizontal images: standard responsive display
                    image_style = (
                        "max-width: 100%; "
                        "height: auto; "
                        "border-radius: 5px; "
                        "box-shadow: 0 2px 8px rgba(0,0,0,0.1);"
                    )
                    container_style = "margin: 20px 0; text-align: center;"

                body_parts.append(
                    f'<div class="afp-image{" afp-vertical" if is_vertical else ""}" style="{container_style}">'
                    f'<img src="{article_data["highres_image_url"]}" '
                    f'alt="{article_data.get("title", "AFP Photo")}" '
                    f'style="{image_style}" />'
                    f'</div>'
                )

            # Close LTR div
            body_parts.append('</div>')

            return "".join(body_parts)

        except Exception as e:
            self.logger.error(f"Error building body HTML: {e}", exc_info=True)
            # Return simple fallback
            return f"<p>{article_data.get('body', '')}</p>"

    async def fetch_news(self) -> None:
        """Fetch photo news from AFP API."""
        self.logger.info(f"Starting to fetch photos from {self.source_name}")

        try:
            # Authenticate first
            if not await self._authenticate():
                self.logger.error("Failed to authenticate with AFP API")
                return

            # Languages to fetch (from workflow: en, ar, fr, es)
            languages = ["en"]

            saved_count = 0
            skipped_count = 0

            for lang in languages:
                try:
                    # Search for photos in this language
                    search_response = await self._search_news(lang=lang, max_rows=50)
                    if not search_response:
                        self.logger.warning(f"No search results for language: {lang}")
                        continue

                    # Parse search results
                    photos = await self._parse_search_results(search_response)
                    if not photos:
                        self.logger.warning(f"No photos parsed for language: {lang}")
                        continue

                    self.logger.info(f"Found {len(photos)} photos for language: {lang}")

                    # Process each photo
                    for photo in photos:
                        try:
                            guid = photo.get("guid")
                            if not guid:
                                continue

                            # Check if already exists
                            if await self._article_exists(guid):
                                self.logger.debug(f"Photo already exists, skipping: {guid}")
                                skipped_count += 1
                                continue

                            # Extract image URLs from medias
                            image_urls = self._extract_image_urls(photo.get("medias", []))

                            # Download and upload preview image for news image_url
                            preview_s3_key = None
                            if image_urls.get("preview"):
                                preview_data = await self._download_image(image_urls["preview"])
                                if preview_data:
                                    preview_s3_key = await self._upload_image_to_s3(
                                        preview_data,
                                        f"preview_{guid}.jpg"
                                    )
                                    if preview_s3_key:
                                        self.logger.info(f"Successfully uploaded preview image for {guid}")
                                    else:
                                        self.logger.warning(f"Failed to upload preview image to S3 for {guid}")

                            # Download and upload high resolution image for body display
                            highres_s3_key = None
                            if image_urls.get("highres"):
                                highres_data = await self._download_image(image_urls["highres"])
                                if highres_data:
                                    highres_s3_key = await self._upload_image_to_s3(
                                        highres_data,
                                        f"highres_{guid}.jpg"
                                    )
                                    if highres_s3_key:
                                        self.logger.info(f"Successfully uploaded high-res image for {guid}")
                                    else:
                                        self.logger.warning(f"Failed to upload high-res image to S3 for {guid}")

                            # Prepare article data
                            article_data = {
                                "guid": guid,
                                "title": photo.get("title", "AFP Photo"),
                                "body": photo.get("body", ""),
                                "summary": photo.get("summary", ""),
                                "published_at": photo.get("published_at", ""),
                                "image_url": preview_s3_key if preview_s3_key else image_urls.get("preview", ""),
                                "highres_image_url": highres_s3_key if highres_s3_key else image_urls.get("highres", ""),
                                "category": photo.get("category", "عکس"),
                                "language": photo.get("language", "en"),
                                "priority": photo.get("priority", 3),
                                "is_vertical": photo.get("is_vertical", False),
                            }

                            # Save to database
                            if await self._save_article(article_data):
                                saved_count += 1

                        except Exception as e:
                            self.logger.error(f"Error processing photo: {e}", exc_info=True)
                            continue

                    # Small delay between languages
                    await asyncio.sleep(1)

                except Exception as e:
                    self.logger.error(f"Error processing language {lang}: {e}", exc_info=True)
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
