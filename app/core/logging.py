"""Structured logging configuration."""

import logging
import sys
from typing import Optional

from app.core.config import settings


def setup_logging(source: Optional[str] = None) -> logging.Logger:
    """
    Set up structured logging for the application.

    Args:
        source: Optional source identifier (e.g., worker source name)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("news_platform")
    logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    # Remove existing handlers
    logger.handlers.clear()

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    # Ensure UTF-8 encoding for the handler
    if hasattr(handler.stream, 'reconfigure'):
        handler.stream.reconfigure(encoding='utf-8')

    # Create formatter with source awareness
    if source:
        format_string = (
            "[%(asctime)s] [%(levelname)s] [%(name)s] [source:%(source)s] "
            "%(message)s"
        )
        formatter = logging.Formatter(
            format_string,
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        # Add source filter to inject source into log records
        class SourceFilter(logging.Filter):
            def __init__(self, source_name: str):
                super().__init__()
                self.source_name = source_name

            def filter(self, record: logging.LogRecord) -> bool:
                record.source = self.source_name
                return True

        handler.addFilter(SourceFilter(source))
    else:
        format_string = (
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
        )
        formatter = logging.Formatter(
            format_string,
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False

    return logger

