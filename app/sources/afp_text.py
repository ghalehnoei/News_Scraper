"""AFP Text News Worker - Fetches text articles from AFP API."""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import logging
import base64
import xml.etree.ElementTree as ET

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


class AFPTextWorker(BaseWorker):
    """Worker for fetching text articles from AFP API."""

    def __init__(self):
        """Initialize AFP text worker."""
        super().__init__(source_name="afp_text")
        self.afp_username = os.getenv("AFP_USERNAME", "ghasemzade@gmail.com")
        self.afp_password = os.getenv("AFP_PASSWORD", "1234@Qwe")
        self.afp_basic_auth = os.getenv(
            "AFP_BASIC_AUTH",
            "SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJREN1WGczREQ1OWk2YUg="
        )
        self.access_token = None
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
        """Search for latest text news from AFP API."""
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
            
            # Prepare request body
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
                            "and": ["text"]
                        }
                    ]
                }
            }
            
            self.logger.info(f"Searching AFP news for language: {lang}")

            async with session.post(search_url, headers=headers, json=body) as response:
                if response.status != 200:
                    self.logger.error(f"Search failed with status {response.status}")
                    response_text = await response.text()
                    self.logger.error(f"Response: {response_text[:500]}")
                    return None
                
                json_response = await response.json()
                return json_response
                
        except Exception as e:
            self.logger.error(f"Error searching AFP news: {e}", exc_info=True)
            return None

    async def _parse_search_results(self, search_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse search results and extract article data."""
        try:
            articles = []
            
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
                article_data = {}
                
                # Extract article fields using AFP API field names
                article_data["guid"] = doc.get("id") or doc.get("guid") or doc.get("_id", "")

                # Use title as headline
                title = doc.get("title") or ""
                article_data["title"] = title if isinstance(title, str) else (title[0] if isinstance(title, list) and title else "")

                # Extract body from news field (paragraphs)
                news_content = doc.get("news") or ""
                if isinstance(news_content, list):
                    article_data["body"] = " ".join(str(item) for item in news_content)
                else:
                    article_data["body"] = str(news_content)

                # Use abstract as summary
                abstract = doc.get("abstract") or ""
                if isinstance(abstract, list):
                    article_data["summary"] = " ".join(str(item) for item in abstract)
                else:
                    article_data["summary"] = str(abstract)

                # Handle published_at - convert to string
                published_at = doc.get("published") or doc.get("publishedDate") or doc.get("date", "")
                article_data["published_at"] = str(published_at)
                article_data["language"] = doc.get("lang") or "en"  # Use "lang" field from AFP API
                article_data["priority"] = doc.get("urgency") or 3  # Use "urgency" field as priority (default 3 if not present)
                article_data["category"] = doc.get("category") or doc.get("subject") or ""
                article_data["url"] = doc.get("url") or doc.get("link") or ""

                # Skip if essential fields are missing
                if not article_data["guid"] or not article_data["title"]:
                    continue
                
                articles.append(article_data)
            
            self.logger.info(f"Parsed {len(articles)} articles from search results")
            return articles

        except Exception as e:
            self.logger.error(f"Error parsing search results: {e}", exc_info=True)
            return []


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
                
                # Build body HTML
                body_html = ""
                if article_data.get("summary"):
                    body_html += f"<p><strong>{article_data['summary']}</strong></p>"
                if article_data.get("body"):
                    body_html += f"<p>{article_data['body']}</p>"
                
                news = News(
                    source=self.source_name,
                    title=article_data["title"],
                    body_html=body_html,
                    summary=article_data.get("summary", ""),
                    url=url,
                    published_at=published_at.isoformat() if hasattr(published_at, 'isoformat') else str(published_at),
                    image_url=article_data.get("image_url", ""),
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
                    f"Saved AFP article: {article_data['title'][:50]}...",
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
        """Fetch text news from AFP API."""
        self.logger.info(f"Starting to fetch text news from {self.source_name}")
        
        try:
            # Authenticate first
            if not await self._authenticate():
                self.logger.error("Failed to authenticate with AFP API")
                return
            
            # Languages to fetch (from workflow: en, ar, fr, es)
            languages = ["en", "ar", "fr", "es"]
            
            saved_count = 0
            skipped_count = 0
            
            for lang in languages:
                try:
                    # Search for news in this language
                    search_response = await self._search_news(lang=lang, max_rows=50)
                    if not search_response:
                        self.logger.warning(f"No search results for language: {lang}")
                        continue
                    
                    # Parse search results
                    articles = await self._parse_search_results(search_response)
                    if not articles:
                        self.logger.warning(f"No articles parsed for language: {lang}")
                        continue
                    
                    self.logger.info(f"Found {len(articles)} articles for language: {lang}")
                    
                    # Process each article
                    for article in articles:
                        try:
                            guid = article.get("guid")
                            if not guid:
                                continue
                            
                            # Check if already exists
                            if await self._article_exists(guid):
                                self.logger.debug(f"Article already exists, skipping: {guid}")
                                skipped_count += 1
                                continue
                            
                            # Save article
                            if await self._save_article(article):
                                saved_count += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing article: {e}", exc_info=True)
                            continue
                    
                    # Small delay between languages
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    self.logger.error(f"Error processing language {lang}: {e}", exc_info=True)
                    continue
            
            self.logger.info(
                f"Finished fetching text news from {self.source_name}: {saved_count} new articles saved, {skipped_count} articles skipped"
            )
            
        except Exception as e:
            self.logger.error(f"Error in fetch_news: {e}", exc_info=True)

    async def close(self) -> None:
        """Close HTTP session and perform cleanup."""
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

