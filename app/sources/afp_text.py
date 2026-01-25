"""AFP Text News Worker - Fetches text articles from AFP API."""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import os
from dotenv import load_dotenv

import requests

# Load environment variables from .env file
# Ensure .env values override any existing process env vars
load_dotenv(override=True)

from app.core.logging import setup_logging
from app.workers.api_worker import APIWorker
from app.services.news_repository import NewsRepository
from app.services.article_processor import ArticleProcessor
from app.db.models import News

logger = setup_logging()


class AFPTextWorker(APIWorker):
    """Worker for fetching text articles from AFP API."""

    def __init__(self):
        """Initialize AFP text worker."""
        super().__init__(source_name="afp_text")
        self.api_base_url = "https://afp-apicore-prod.afp.com"
        
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
        
        # Initialize article processor
        self.article_processor = ArticleProcessor(source_name="afp_text")
        
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

    async def _authenticate(self) -> bool:
        """Authenticate with AFP API and get access token."""
        try:
            # Check credentials
            if not self.afp_username or not self.afp_password:
                self.logger.error("AFP credentials not set in environment (AFP_USERNAME/AFP_PASSWORD).")
                return False

            if not self.afp_basic_auth:
                self.logger.error("AFP_BASIC_AUTH not set in environment. This is required for client authentication.")
                return False

            # Build auth URL
            auth_url = (
                f"{self.api_base_url}/oauth/token"
                f"?username={self.afp_username}"
                f"&password={self.afp_password}"
                f"&grant_type=password"
            )

            # Build headers
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic SVRBQTQ5X0FQSV8yMDI1OkIxTmkwdDJRNXZMOUh4R2F4STVIMS1tMVRJRUN1WGczREQ1OWk2YUg='
            }

            payload = {}

            # Use requests module for authentication
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
                    # Keep paragraphs as list for proper HTML structure
                    article_data["body_paragraphs"] = [str(item).strip() for item in news_content if str(item).strip()]
                    article_data["body"] = " ".join(article_data["body_paragraphs"])  # Keep joined version for backward compatibility
                else:
                    article_data["body_paragraphs"] = [str(news_content).strip()] if str(news_content).strip() else []
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
            
            return articles

        except Exception as e:
            self.logger.error(f"Error parsing search results: {e}", exc_info=True)
            return []


    async def _article_exists(self, guid: str) -> bool:
        """Check if article already exists in database by GUID."""
        url = f"afp:{guid}"
        existing = await NewsRepository.get_by_url(url)
        return existing is not None

    async def _save_article(self, article_data: dict) -> bool:
        """Save article to database using NewsRepository."""
        try:
            # Use GUID as unique URL identifier
            url = f"afp:{article_data['guid']}"
            
            # Check if article already exists
            if await self._article_exists(article_data['guid']):
                return False
            
            # Use ArticleProcessor to build the news object
            news_data = self.article_processor.create_news_object(
                article_data=article_data,
                source=self.source_name,
                url=url,
                is_international=True,
                source_type='external'
            )
            
            # Create News object
            news = News(
                source=news_data["source"],
                title=news_data["title"],
                body_html=news_data["body_html"],
                summary=news_data["summary"],
                url=news_data["url"],
                published_at=news_data["published_at"],
                image_url=news_data["image_url"],
                category=news_data["category"],
                raw_category=news_data["raw_category"],
                language=news_data["language"],
                priority=news_data["priority"],
                is_international=news_data["is_international"],
                source_type=news_data["source_type"],
            )
            
            # Save using NewsRepository
            if await NewsRepository.save(news):
                self.logger.info(
                    f"Saved AFP article: {article_data['title'][:50]}...",
                    extra={
                        "guid": article_data["guid"],
                        "source": self.source_name,
                        "category": news_data["category"],
                    }
                )
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(
                f"Error saving article: {e}",
                extra={"guid": article_data.get("guid"), "error": str(e)},
                exc_info=True
            )
            return False

    async def fetch_news(self) -> None:
        """Fetch text news from AFP API."""
        try:
            if not await self._authenticate():
                self.logger.error("Failed to authenticate with AFP API")
                return
            
            languages = ["en", "ar", "fr", "es"]
            saved_count = 0
            skipped_count = 0
            
            for lang in languages:
                try:
                    search_response = await self._search_news(lang=lang, max_rows=50)
                    if not search_response:
                        continue
                    
                    articles = await self._parse_search_results(search_response)
                    if not articles:
                        continue
                    
                    total = len(articles)
                    self.logger.info(f"Processing {total} articles ({lang})...")
                    
                    for i, article in enumerate(articles, 1):
                        try:
                            guid = article.get("guid")
                            if not guid:
                                continue
                            
                            if await self._article_exists(guid):
                                skipped_count += 1
                                continue
                            
                            if await self._save_article(article):
                                saved_count += 1
                                self.logger.info(f"Saved {saved_count}/{total} ({lang}): {article.get('title', 'N/A')[:50]}")
                            
                        except Exception as e:
                            self.logger.error(f"Error processing article: {e}", exc_info=True)
                            continue
                    
                    self.logger.info(f"Processed {total} articles ({lang}), saved {saved_count}, skipped {skipped_count}")
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

