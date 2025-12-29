"""Application lifecycle management."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from app.core.logging import setup_logging
from app.db.session import init_db, close_db
from app.storage.s3 import init_s3, close_s3

logger = setup_logging()


@asynccontextmanager
async def lifespan(app) -> AsyncGenerator[None, None]:
    """
    Manage application lifecycle events.

    Handles startup and shutdown of database and S3 connections.
    """
    # Startup
    logger.info("Starting application...")
    try:
        await init_db()
        logger.info("Database connection initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        raise

    try:
        await init_s3()
        logger.info("S3 storage connection initialized")
    except Exception as e:
        logger.error(f"Failed to initialize S3 storage: {e}", exc_info=True)
        raise

    logger.info("Application started successfully")

    yield

    # Shutdown
    logger.info("Shutting down application...")
    await close_db()
    await close_s3()
    logger.info("Application shut down complete")

