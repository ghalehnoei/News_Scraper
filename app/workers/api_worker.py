"""API-based worker base class for news sources."""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import aiohttp

from app.workers.base_worker import BaseWorker
from app.workers.rate_limiter import RateLimiter
from app.core.config import settings


class APIWorker(BaseWorker, ABC):
    """
    Base class for API-based news workers.
    
    Provides common functionality for workers that fetch news from REST APIs.
    """

    def __init__(self, source_name: str):
        """
        Initialize API worker.
        
        Args:
            source_name: Name of the news source
        """
        super().__init__(source_name)
        self.api_base_url: str = ""
        self.auth_token: Optional[str] = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.max_requests_per_minute,
            delay_between_requests=settings.delay_between_requests,
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """
        Get or create HTTP session with common headers.
        
        Returns:
            Configured aiohttp.ClientSession
        """
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json",
                }
            )
        return self.http_session

    async def _authenticate(self) -> bool:
        """
        Authenticate with the API and obtain access token.
        
        Returns:
            True if authentication succeeded, False otherwise
        """
        # To be implemented by subclasses
        return False

    async def _handle_api_error(self, response: aiohttp.ClientResponse) -> Optional[Dict[str, Any]]:
        """
        Handle API error responses.
        
        Args:
            response: The HTTP response with error status
            
        Returns:
            Error information dictionary or None
        """
        try:
            error_data = await response.json()
            return {
                "status": response.status,
                "error": error_data
            }
        except Exception:
            return {
                "status": response.status,
                "error": await response.text()
            }

    async def _make_api_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        json: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        request_type: str = "api"
    ) -> Optional[Dict[str, Any]]:
        """
        Make an authenticated API request.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (appended to base_url)
            params: Query parameters
            data: Form data
            json: JSON body
            headers: Additional headers
            request_type: Type of request for rate limiting
            
        Returns:
            Response JSON or None if request failed
        """
        try:
            await self.rate_limiter.acquire(
                source=self.source_name,
                request_type=request_type
            )
            
            session = await self._get_http_session()
            url = f"{self.api_base_url}/{endpoint}"
            
            # Prepare headers
            request_headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            
            # Add authentication if available
            if self.auth_token:
                request_headers["Authorization"] = f"Bearer {self.auth_token}"
            
            # Merge with custom headers
            if headers:
                request_headers.update(headers)
            
            # Make the request
            async with session.request(
                method,
                url,
                params=params,
                data=data,
                json=json,
                headers=request_headers
            ) as response:
                
                if response.status == 200:
                    return await response.json()
                else:
                    error_info = await self._handle_api_error(response)
                    self.logger.error(
                        f"API request failed: {error_info}",
                        extra={
                            "source": self.source_name,
                            "url": url,
                            "status": response.status
                        }
                    )
                    return None
                    
        except Exception as e:
            self.logger.error(
                f"Error making API request: {e}",
                extra={
                    "source": self.source_name,
                    "url": f"{self.api_base_url}/{endpoint}"
                },
                exc_info=True
            )
            return None

    async def close(self) -> None:
        """
        Close HTTP session and perform cleanup.
        """
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
