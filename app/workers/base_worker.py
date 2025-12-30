"""Base worker class for news sources."""

import asyncio
import platform
import signal
from typing import Optional

from app.core.config import settings
from app.core.logging import setup_logging
from app.db.session import init_db

logger = setup_logging()

# Check if running on Windows
IS_WINDOWS = platform.system() == "Windows"


class BaseWorker:
    """
    Base class for news source workers.

    Each worker is responsible for polling a single news source.
    For Phase 1, this is a concrete implementation with placeholder logic.
    """

    def __init__(self, source_name: str):
        """
        Initialize the worker.

        Args:
            source_name: Name of the news source this worker handles
        """
        self.source_name = source_name
        self.logger = setup_logging(source=source_name)
        self.running = False
        self._shutdown_event = asyncio.Event()

    async def fetch_news(self) -> None:
        """
        Fetch news from the source.

        Phase 1: Placeholder implementation.
        In future phases, this will be overridden by source-specific workers.
        """
        self.logger.info(f"Placeholder: fetch_news() called for {self.source_name}")

    async def run(self) -> None:
        """
        Main worker loop.

        Runs continuously, polling the source at configured intervals.
        Handles graceful shutdown on SIGTERM/SIGINT.
        """
        self.logger.info(f"Starting worker for source: {self.source_name}")
        
        # Initialize database and create tables if they don't exist
        try:
            await init_db()
            self.logger.info("Database tables initialized/verified")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}", exc_info=True)
            raise
        
        self.running = True

        # Setup signal handlers for graceful shutdown (Unix only)
        if not IS_WINDOWS:
            try:
                loop = asyncio.get_event_loop()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(
                        sig,
                        lambda s=sig: asyncio.create_task(self._shutdown(s))
                    )
            except NotImplementedError:
                # Signal handlers not available on this platform
                pass
        else:
            # On Windows, we'll handle KeyboardInterrupt in the main loop
            self.logger.info("Running on Windows - signal handlers not available")

        try:
            while self.running:
                self.logger.info(f"Polling source: {self.source_name}")

                try:
                    await self.fetch_news()
                except Exception as e:
                    self.logger.error(
                        f"Error fetching news from {self.source_name}: {e}",
                        exc_info=True
                    )

                # Wait for poll interval or shutdown signal
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=settings.poll_interval
                    )
                    # Shutdown event was set
                    break
                except asyncio.TimeoutError:
                    # Timeout is expected, continue loop
                    pass

        except Exception as e:
            self.logger.error(f"Worker error: {e}", exc_info=True)
        finally:
            # Cleanup resources if cleanup method exists
            if hasattr(self, "cleanup"):
                try:
                    await self.cleanup()
                except Exception as cleanup_error:
                    self.logger.error(f"Error during cleanup: {cleanup_error}")
            self.logger.info(f"Worker stopped for source: {self.source_name}")

    async def _shutdown(self, signum: int) -> None:
        """
        Handle shutdown signal.

        Args:
            signum: Signal number
        """
        signal_name = signal.Signals(signum).name
        self.logger.info(f"Received {signal_name}, initiating graceful shutdown...")
        self.running = False
        self._shutdown_event.set()

