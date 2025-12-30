"""Worker runner entry point."""

import asyncio
import sys

from app.core.config import settings
from app.core.logging import setup_logging
from app.workers.base_worker import BaseWorker

logger = setup_logging()

# Import source-specific workers
from app.sources.isna import ISNAWorker
from app.sources.mehrnews import MehrNewsWorker
from app.sources.irna import IRNAWorker
from app.sources.fars import FarsWorker
from app.sources.tasnim import TasnimWorker
from app.sources.iribnews import IRIBNewsWorker


async def main() -> None:
    """Main entry point for worker process."""
    if not settings.worker_source:
        logger.error("WORKER_SOURCE environment variable is required")
        sys.exit(1)

    logger.info(f"Starting worker for source: {settings.worker_source}")

    # Instantiate appropriate worker based on source
    worker = None
    if settings.worker_source == "mehrnews":
        worker = MehrNewsWorker()
    elif settings.worker_source == "isna":
        worker = ISNAWorker()
    elif settings.worker_source == "irna":
        worker = IRNAWorker()
    elif settings.worker_source == "fars":
        worker = FarsWorker()
    elif settings.worker_source == "tasnim":
        worker = TasnimWorker()
    elif settings.worker_source == "iribnews":
        worker = IRIBNewsWorker()
    else:
        # Fallback to base worker for unknown sources
        worker = BaseWorker(settings.worker_source)

    try:
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user (Ctrl+C)")
        # Trigger graceful shutdown
        worker.running = False
        worker._shutdown_event.set()
        # Cleanup if worker has cleanup method
        if hasattr(worker, "cleanup"):
            await worker.cleanup()
    except Exception as e:
        logger.error(f"Worker failed: {e}", exc_info=True)
        if hasattr(worker, "cleanup"):
            await worker.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

