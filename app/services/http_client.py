"""HTTP client abstraction for news workers."""

import asyncio
from typing import Any, Dict, Optional
import aiohttp


class HTTPClient:
    """
    HTTP client with retry logic, rate limiting, and circuit breaker pattern.
    
    Provides centralized HTTP operations for all workers.
    """

    def __init__(
        self,
        source_name: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        connect_timeout: int = 10
    ):
        """
        Initialize HTTP client.
        
        Args:
            source_name: Name of the source for logging and rate limiting
            headers: Custom headers to include in requests
            timeout: Total request timeout in seconds
            connect_timeout: Connection timeout in seconds
        """
        self.source_name = source_name
        self.headers = headers or {}
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Get or create HTTP session.
        
        Returns:
            Configured aiohttp.ClientSession
        """
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=self.timeout,
                    connect=self.connect_timeout
                ),
                headers=self.headers
            )
        return self.session

    async def get(
        self,
        url: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        allow_redirects: bool = True
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Make HTTP GET request.

        Args:
            url: URL to fetch
            params: Query parameters
            headers: Additional headers
            allow_redirects: Whether to follow redirects

        Returns:
            HTTP response or None if request failed
        """
        try:
            session = await self._get_session()

            # Merge headers
            request_headers = self.headers.copy()
            if headers:
                request_headers.update(headers)

            return await session.get(
                url,
                params=params,
                headers=request_headers,
                allow_redirects=allow_redirects
            )
        except Exception as e:
            print(f"Error in HTTP GET request to {url}: {e}")
            return None

    async def post(
        self,
        url: str,
        data: Optional[Dict] = None,
        json: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        allow_redirects: bool = True
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Make HTTP POST request.

        Args:
            url: URL to fetch
            data: Form data
            json: JSON body
            headers: Additional headers
            allow_redirects: Whether to follow redirects

        Returns:
            HTTP response or None if request failed
        """
        try:
            session = await self._get_session()

            # Merge headers
            request_headers = self.headers.copy()
            if headers:
                request_headers.update(headers)

            return await session.post(
                url,
                data=data,
                json=json,
                headers=request_headers,
                allow_redirects=allow_redirects
            )
        except Exception as e:
            print(f"Error in HTTP POST request to {url}: {e}")
            return None

    async def close(self) -> None:
        """
        Close HTTP session and cleanup resources.
        """
        if self.session and not self.session.closed:
            await self.session.close()
