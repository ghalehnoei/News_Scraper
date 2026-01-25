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
import requests
import os
from sqlalchemy import select
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables from .env file
# Ensure .env values override any existing process env vars
load_dotenv(override=True)

from app.core.config import settings
from app.core.logging import setup_logging
from app.core.category_normalizer import normalize_category
from app.db.base import AsyncSessionLocal
from app.db.models import News
from app.storage.s3 import get_s3_session, init_s3
from app.workers.api_worker import APIWorker
from app.workers.rate_limiter import RateLimiter
from app.services.http_client import HTTPClient
from app.services.news_repository import NewsRepository
from app.services.article_processor import ArticleProcessor

logger = setup_logging()


class AFPPhotoWorker(APIWorker):
    """Worker for fetching photo articles from AFP API."""

    def __init__(self):
        """Initialize AFP photo worker."""
        super().__init__(source_name="afp_photo")
        # Load sensitive credentials from environment variables (no defaults)
        def _clean_env(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            cleaned = value.strip()
            if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in ("'", '"'):
                cleaned = cleaned[1:-1].strip()
            return cleaned or None

        self.afp_username = _clean_env(os.getenv("AFP_USERNAME"))
        self.afp_password = _clean_env(os.getenv("AFP_PASSWORD"))
        self.afp_basic_auth = _clean_env(os.getenv("AFP_BASIC_AUTH"))
        if self.afp_basic_auth and self.afp_basic_auth.lower().startswith("basic "):
            self.afp_basic_auth = self.afp_basic_auth.split(" ", 1)[1].strip()
        # Log presence and masked values only (never log secrets)
        def _mask(value: Optional[str]) -> str:
            if not value:
                return "missing"
            if len(value) <= 6:
                return "***"
            return f"{value[:2]}***{value[-2:]}"

        self.logger.info(
            "AFP env loaded (masked): AFP_USERNAME=%s, AFP_PASSWORD=%s, AFP_BASIC_AUTH=%s",
            _mask(self.afp_username),
            _mask(self.afp_password),
            _mask(self.afp_basic_auth),
        )
        self.access_token = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._s3_initialized = False

        # Initialize HTTP client
        self.http_client = HTTPClient(source_name=self.source_name)
        
        # Initialize news repository
        self.news_repo = NewsRepository()
        
        # Initialize article processor
        self.article_processor = ArticleProcessor(source_name=self.source_name)

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
        """Authenticate with AFP API and get access token using requests module."""
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type="api"
            )


            if not self.afp_username or not self.afp_password:
                self.logger.error("AFP credentials not set in environment (AFP_USERNAME/AFP_PASSWORD).")
                return False

            if not self.afp_basic_auth:
                self.logger.error("AFP_BASIC_AUTH not set in environment. This is required for client authentication.")
                return False

            # Build auth URL exactly like test.py
            auth_url = (
                f"https://afp-apicore-prod.afp.com/oauth/token"
                f"?username={self.afp_username}"
                f"&password={self.afp_password}"
                f"&grant_type=password"
            )

            # Build headers exactly like test.py
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJRUN1WGczREQ1OWk2YUg='
            }

            payload = {}

            # Use requests module for authentication (exactly like test.py)
            def authenticate_sync():
                return requests.request("GET", auth_url, headers=headers, data=payload)

            # Run requests in thread pool (sync operation in async context)
            response = await asyncio.to_thread(authenticate_sync)

            if response.status_code != 200:
                self.logger.error(f"Authentication failed with status {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return False

            json_response = response.json()

            # Extract access token
            if "access_token" in json_response:
                self.access_token = json_response["access_token"]
                return True
            else:
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
        return await self.news_repo.get_by_url(f"afp:{guid}") is not None

    async def _save_article(self, article_data: dict) -> bool:
        """Save article to database."""
        try:
            # Use GUID as unique URL identifier
            url = f"afp:{article_data['guid']}"

            if await self.news_repo.get_by_url(url) is not None:
                return False

            normalized_category, raw_category = self.article_processor.normalize_category(
                article_data.get("category")
            )

            # Truncate raw_category to fit database limit (200 chars)
            if len(raw_category) > 200:
                raw_category = raw_category[:197] + "..."

            # Parse published date
            published_at = self.article_processor.parse_date(article_data.get("published_at"))
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

            await self.news_repo.save(news)
            return True

        except Exception as e:
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
                        continue

                    photos = await self._parse_search_results(search_response)
                    if not photos:
                        continue

                    total = len(photos)
                    self.logger.info(f"Processing {total} photos...")

                    for i, photo in enumerate(photos, 1):
                        try:
                            guid = photo.get("guid")
                            if not guid:
                                continue

                            if await self._article_exists(guid):
                                skipped_count += 1
                                continue

                            image_urls = self._extract_image_urls(photo.get("medias", []))

                            preview_s3_key = None
                            if image_urls.get("preview"):
                                preview_data = await self._download_image(image_urls["preview"])
                                if preview_data:
                                    preview_s3_key = await self._upload_image_to_s3(
                                        preview_data,
                                        f"preview_{guid}.jpg"
                                    )

                            highres_s3_key = None
                            if image_urls.get("highres"):
                                highres_data = await self._download_image(image_urls["highres"])
                                if highres_data:
                                    highres_s3_key = await self._upload_image_to_s3(
                                        highres_data,
                                        f"highres_{guid}.jpg"
                                    )

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

                            if await self._save_article(article_data):
                                saved_count += 1
                                self.logger.info(f"Saved {saved_count}/{total}: {article_data.get('title', 'N/A')[:50]}")

                        except Exception as e:
                            self.logger.error(f"Error processing photo: {e}", exc_info=True)
                            continue

                    self.logger.info(f"Processed {total} photos, saved {saved_count}, skipped {skipped_count}")
                    await asyncio.sleep(1)

                except Exception as e:
                    self.logger.error(f"Error processing language {lang}: {e}", exc_info=True)
                    continue


        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
