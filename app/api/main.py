"""FastAPI application main module."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

from app.core.lifecycle import lifespan
from app.api.routes import health, news, ui

app = FastAPI(
    title="News Ingestion Platform API",
    description="Enterprise Persian news ingestion platform",
    version="1.0.0",
    lifespan=lifespan,
)

# Setup templates and static files
templates = Environment(loader=FileSystemLoader("app/templates"))

# Add Persian date filter
from app.core.date_utils import format_persian_date

templates.filters["persian_date"] = format_persian_date

app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Set templates instance for UI routes
ui.set_templates(templates)

# Register routers
app.include_router(health.router)
app.include_router(news.router)
app.include_router(ui.router)


@app.get("/")
async def root() -> dict:
    """Root endpoint."""
    return {
        "message": "News Ingestion Platform API",
        "version": "1.0.0",
        "ui": "/ui/news"
    }

