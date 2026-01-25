"""Web scraping worker base class for news sources."""

import asyncio
from abc import ABC
from typing import Optional
import aiohttp

from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter
from app.core.config import settings


class WebScraperWorker(BaseWorker, ABC):
    """
    Base class for web scraping news workers.
    
    Provides common functionality for workers that scrape news from HTML pages.
    """

    def __init__(self, source_name: str):
        """
        Initialize web scraper worker.
        
        Args:
            source_name: Name of the news source
        """
        super().__init__(source_name)
        self.base_url: str = ""
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """
        Get or create HTTP session with scraping-specific headers.
        
        Returns:
            Configured aiohttp.ClientSession
        """
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30, connect=10),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                }
            )
        return self.http_session

    async def _fetch_with_retry(
        self,
        url: str,
        max_retries: int = 3,
        request_type: str = "article"
    ) -> Optional[bytes]:
        """
        Fetch URL with retries and rate limiting.
        
        Args:
            url: URL to fetch
            max_retries: Maximum number of retry attempts
            request_type: Type of request (listing, article, image) for logging
            
        Returns:
            Response content as bytes, or None if all retries failed
        """
        session = await self._get_http_session()
        
        for attempt in range(max_retries):
            try:
                # Apply rate limiting before making request
                await self.rate_limiter.acquire(
                    source=self.source_name,
                    request_type=request_type
                )
                
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        self.logger.debug(
                            f"Successfully fetched {url}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                                "article_url": url if request_type == "article" else None,
                            }
                        )
                        return content
                    elif response.status == 404:
                        self.logger.warning(
                            f"404 Not Found: {url}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                            }
                        )
                        return None
                    else:
                        self.logger.warning(
                            f"HTTP {response.status} for {url}, attempt {attempt + 1}/{max_retries}",
                            extra={
                                "source": self.source_name,
                                "request_type": request_type,
                            }
                        )
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Timeout fetching {url}, attempt {attempt + 1}/{max_retries}",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                    }
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                self.logger.warning(
                    f"Error fetching {url}: {e}, attempt {attempt + 1}/{max_retries}",
                    extra={
                        "source": self.source_name,
                        "request_type": request_type,
                    }
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    self.logger.error(
                        f"Failed to fetch {url} after {max_retries} attempts",
                        extra={
                            "source": self.source_name,
                            "request_type": request_type,
                            "article_url": url if request_type == "article" else None,
                        }
                    )
                    return None
        
        return None

    async def close(self) -> None:
        """
        Close HTTP session and perform cleanup.
        """
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
