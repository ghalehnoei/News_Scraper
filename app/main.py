"""Main entry point for the application."""

import uvicorn

from app.core.config import settings
from app.api.main import app

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )

