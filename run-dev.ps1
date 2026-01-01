# Development runner script for News Scraper
# This script sets up the environment for local SQLite development

# Set environment variable to use .env file
$env:USE_ENV_FILE = "true"

# Run the FastAPI application
.\venv\Scripts\python.exe -m uvicorn app.api.main:app --host 0.0.0.0 --port 3000 --reload

