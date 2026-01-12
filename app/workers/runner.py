"""Worker runner entry point."""

import asyncio
import sys

from app.core.config import settings
from app.core.logging import setup_logging
from app.workers.base_worker import BaseWorker

# Import source-specific workers
from app.sources.isna import ISNAWorker
from app.sources.mehrnews import MehrNewsWorker
from app.sources.irna import IRNAWorker
from app.sources.fars import FarsWorker
from app.sources.tasnim import TasnimWorker
from app.sources.iribnews import IRIBNewsWorker
from app.sources.ilna import ILNAWorker
from app.sources.kayhan import KayhanWorker
from app.sources.mizan import MizanWorker
from app.sources.varzesh3 import Varzesh3Worker
from app.sources.mashreghnews import MashreghNewsWorker
from app.sources.yjc import YJCWorker
from app.sources.iqna import IQNAWorker
from app.sources.hamshahri import HamshahriWorker
from app.sources.donyaeqtesad import DonyaEqtesadWorker
from app.sources.snn import SNNWorker
from app.sources.ipna import IPNAWorker
from app.sources.tabnak import TabnakWorker
from app.sources.eghtesadonline import EghtesadOnlineWorker
from app.sources.reuters_photos import ReutersPhotosWorker
from app.sources.reuters_text import ReutersTextWorker
from app.sources.reuters_video import ReutersVideoWorker
from app.sources.afp_text import AFPTextWorker


async def main() -> None:
    """Main entry point for worker process."""
    if not settings.worker_source:
        # Create a temporary logger for error message
        temp_logger = setup_logging()
        temp_logger.error("WORKER_SOURCE environment variable is required")
        sys.exit(1)

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
    elif settings.worker_source == "ilna":
        worker = ILNAWorker()
    elif settings.worker_source == "kayhan":
        worker = KayhanWorker()
    elif settings.worker_source == "mizan":
        worker = MizanWorker()
    elif settings.worker_source == "varzesh3":
        worker = Varzesh3Worker()
    elif settings.worker_source == "mashreghnews":
        worker = MashreghNewsWorker()
    elif settings.worker_source == "yjc":
        worker = YJCWorker()
    elif settings.worker_source == "iqna":
        worker = IQNAWorker()
    elif settings.worker_source == "hamshahri":
        worker = HamshahriWorker()
    elif settings.worker_source == "donyaeqtesad":
        worker = DonyaEqtesadWorker()
    elif settings.worker_source == "snn":
        worker = SNNWorker()
    elif settings.worker_source == "ipna":
        worker = IPNAWorker()
    elif settings.worker_source == "tabnak":
        worker = TabnakWorker()
    elif settings.worker_source == "eghtesadonline":
        worker = EghtesadOnlineWorker()
    elif settings.worker_source == "reuters_photos":
        worker = ReutersPhotosWorker()
    elif settings.worker_source == "reuters_text":
        worker = ReutersTextWorker()
    elif settings.worker_source == "reuters_video":
        worker = ReutersVideoWorker()
    elif settings.worker_source == "afp_text":
        worker = AFPTextWorker()
    else:
        # Fallback to base worker for unknown sources
        worker = BaseWorker(settings.worker_source)

    worker.logger.info(f"Starting worker for source: {settings.worker_source}")

    try:
        await worker.run()
        worker.logger.info("Worker completed normally")
    except KeyboardInterrupt:
        worker.logger.info("Worker interrupted by user (Ctrl+C)")
        # Trigger graceful shutdown
        worker.shutdown()
        # Give the worker a moment to finish current operations
        try:
            await asyncio.wait_for(worker._shutdown_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            worker.logger.warning("Shutdown timeout, forcing exit...")
        # Cleanup is handled in worker.run() finally block
        worker.logger.info("Worker shutdown complete")
    except Exception as e:
        worker.logger.error(f"Worker failed: {e}", exc_info=True)
        worker.shutdown()
        # Cleanup is handled in worker.run() finally block
        sys.exit(1)
    finally:
        # Ensure we exit cleanly
        worker.logger.debug("Exiting worker process")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Create a temporary logger for this message since worker may not exist yet
        temp_logger = setup_logging()
        temp_logger.info("Received KeyboardInterrupt, exiting...")
        sys.exit(0)

