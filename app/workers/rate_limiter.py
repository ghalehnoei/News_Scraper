"""Rate limiter for worker HTTP requests."""

import asyncio
import time
from collections import defaultdict
from typing import Optional


class RateLimiter:
    """
    Async-compatible rate limiter for HTTP requests.
    
    Supports per-source rate limiting with configurable limits.
    """

    def __init__(
        self,
        max_requests_per_minute: int = 60,
        delay_between_requests: float = 1.0,
    ):
        """
        Initialize rate limiter.

        Args:
            max_requests_per_minute: Maximum requests allowed per minute
            delay_between_requests: Minimum delay in seconds between requests
        """
        self.max_requests_per_minute = max_requests_per_minute
        self.delay_between_requests = delay_between_requests
        
        # Track request timestamps per source
        self._request_timestamps: dict[str, list[float]] = defaultdict(list)
        self._last_request_time: dict[str, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        source: str,
        request_type: str = "request",
    ) -> None:
        """
        Acquire permission to make a request.

        This method will block until the rate limit allows the request.

        Args:
            source: Source name for rate limiting
            request_type: Type of request (rss, article, etc.) for logging
        """
        async with self._lock:
            now = time.time()
            
            # Clean old timestamps (older than 1 minute)
            timestamps = self._request_timestamps[source]
            timestamps[:] = [ts for ts in timestamps if now - ts < 60]
            
            # Check if we've exceeded the per-minute limit
            if len(timestamps) >= self.max_requests_per_minute:
                # Calculate wait time until oldest request expires
                oldest_timestamp = min(timestamps)
                wait_time = 60 - (now - oldest_timestamp) + 0.1  # Add small buffer
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    # Clean timestamps again after waiting
                    now = time.time()
                    timestamps[:] = [ts for ts in timestamps if now - ts < 60]
            
            # Apply delay between requests
            last_request = self._last_request_time[source]
            if last_request > 0:
                time_since_last = now - last_request
                if time_since_last < self.delay_between_requests:
                    delay = self.delay_between_requests - time_since_last
                    await asyncio.sleep(delay)
                    now = time.time()
            
            # Record this request
            timestamps.append(now)
            self._last_request_time[source] = now

    def get_stats(self, source: str) -> dict:
        """
        Get rate limit statistics for a source.

        Args:
            source: Source name

        Returns:
            Dictionary with rate limit stats
        """
        now = time.time()
        timestamps = self._request_timestamps[source]
        # Clean old timestamps
        timestamps[:] = [ts for ts in timestamps if now - ts < 60]
        
        return {
            "requests_last_minute": len(timestamps),
            "max_requests_per_minute": self.max_requests_per_minute,
            "delay_between_requests": self.delay_between_requests,
        }

