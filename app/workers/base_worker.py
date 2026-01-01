"""Base worker class for news sources."""

import asyncio
import platform
import signal
import time
from typing import Optional
from datetime import timedelta

from app.core.config import settings
from app.core.logging import setup_logging
from app.db.session import init_db, close_db
from app.db.base import engine
from app.storage.s3 import close_s3

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

        fetch_task = None
        cycle_start_time = None
        try:
            while self.running:
                cycle_start_time = time.time()
                self.logger.info(f"Polling source: {self.source_name}")

                try:
                    # Run fetch_news with cancellation support
                    fetch_task = asyncio.create_task(self.fetch_news())
                    await fetch_task
                    fetch_task = None  # Clear reference after successful completion
                except asyncio.CancelledError:
                    self.logger.info("Fetch operation cancelled, shutting down...")
                    self.running = False
                    break
                except KeyboardInterrupt:
                    self.logger.info("KeyboardInterrupt received during fetch, shutting down...")
                    self.running = False
                    # Cancel the fetch task if it's still running
                    if fetch_task and not fetch_task.done():
                        fetch_task.cancel()
                        try:
                            await fetch_task
                        except (asyncio.CancelledError, Exception):
                            pass
                    break
                except Exception as e:
                    self.logger.error(
                        f"Error fetching news from {self.source_name}: {e}",
                        exc_info=True
                    )
                    fetch_task = None  # Clear reference after error

                # Check if shutdown was requested before waiting
                if not self.running:
                    break

                # Calculate time taken for this cycle and remaining time
                if cycle_start_time:
                    cycle_duration = time.time() - cycle_start_time
                    time_remaining = settings.poll_interval - cycle_duration
                    
                    if time_remaining > 0:
                        # Format time remaining
                        remaining_td = timedelta(seconds=int(time_remaining))
                        hours, remainder = divmod(remaining_td.seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        
                        if hours > 0:
                            time_str = f"{hours} ساعت و {minutes} دقیقه و {seconds} ثانیه"
                        elif minutes > 0:
                            time_str = f"{minutes} دقیقه و {seconds} ثانیه"
                        else:
                            time_str = f"{seconds} ثانیه"
                        
                        self.logger.info(
                            f"Cycle completed in {cycle_duration:.1f}s. "
                            f"Waiting {time_str} until next cycle..."
                        )
                    else:
                        self.logger.warning(
                            f"Cycle took {cycle_duration:.1f}s (longer than poll interval of {settings.poll_interval}s). "
                            f"Starting next cycle immediately."
                        )
                        time_remaining = 0
                else:
                    time_remaining = settings.poll_interval

                # Wait for poll interval or shutdown signal
                try:
                    if time_remaining > 0:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=time_remaining
                        )
                        # Shutdown event was set
                        break
                    else:
                        # Check if shutdown was requested
                        if self._shutdown_event.is_set():
                            break
                except asyncio.TimeoutError:
                    # Timeout is expected, continue loop
                    pass
                except asyncio.CancelledError:
                    self.logger.info("Wait operation cancelled, shutting down...")
                    self.running = False
                    break
                except KeyboardInterrupt:
                    self.logger.info("KeyboardInterrupt received during wait, shutting down...")
                    self.running = False
                    break

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received in main loop, shutting down...")
            self.running = False
            # Cancel any running fetch task
            if fetch_task and not fetch_task.done():
                fetch_task.cancel()
                try:
                    await fetch_task
                except (asyncio.CancelledError, Exception):
                    pass
        except Exception as e:
            self.logger.error(f"Worker error: {e}", exc_info=True)
        finally:
            # Ensure shutdown event is set
            self.running = False
            self._shutdown_event.set()
            
            # Cancel any remaining fetch task
            if fetch_task and not fetch_task.done():
                fetch_task.cancel()
                try:
                    await asyncio.wait_for(fetch_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
            
            # Cleanup resources if cleanup method exists
            if hasattr(self, "cleanup"):
                try:
                    self.logger.info("Running cleanup...")
                    # Add timeout to cleanup to prevent hanging
                    await asyncio.wait_for(self.cleanup(), timeout=10.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Cleanup timed out, forcing exit...")
                except Exception as cleanup_error:
                    self.logger.error(f"Error during cleanup: {cleanup_error}", exc_info=True)
            
            # Close database connections
            try:
                self.logger.debug("Closing database connections...")
                # Dispose engine directly with timeout
                await asyncio.wait_for(engine.dispose(close=True), timeout=5.0)
                self.logger.debug("Database connections closed")
            except asyncio.TimeoutError:
                self.logger.warning("Database close timed out")
            except Exception as db_error:
                self.logger.warning(f"Error closing database: {db_error}")
            
            # Close S3 connections
            try:
                self.logger.debug("Closing S3 connections...")
                await asyncio.wait_for(close_s3(), timeout=2.0)
            except (asyncio.TimeoutError, Exception) as s3_error:
                self.logger.warning(f"Error closing S3: {s3_error}")
            
            # Cancel any remaining background tasks
            try:
                loop = asyncio.get_running_loop()
                tasks = [t for t in asyncio.all_tasks(loop) if not t.done() and t is not asyncio.current_task()]
                if tasks:
                    self.logger.debug(f"Cancelling {len(tasks)} remaining tasks...")
                    for task in tasks:
                        task.cancel()
                    # Wait briefly for cancellation
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as task_error:
                self.logger.debug(f"Error cancelling tasks: {task_error}")
            
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
    
    def shutdown(self) -> None:
        """
        Trigger graceful shutdown from external code (e.g., KeyboardInterrupt handler).
        Can be called synchronously.
        """
        self.logger.info("Shutdown requested, initiating graceful shutdown...")
        self.running = False
        self._shutdown_event.set()

