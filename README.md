# News Ingestion Platform

Enterprise Persian news ingestion platform for collecting and aggregating news from multiple Persian news sources.

## Overview

This platform collects news from 5 major Persian news sources (ISNA, MehrNews, IRNA, Fars, Tasnim), normalizes categories, stores images in S3-compatible storage, and provides a web interface and API for accessing the aggregated news.

## Architecture

- **API Service**: FastAPI-based read-only API
- **Worker Services**: Separate workers for each news source (ISNA, MehrNews, IRNA, Fars, Tasnim)
- **Database**: PostgreSQL with SQLAlchemy async ORM
- **Storage**: External S3-compatible storage (MinIO) for images
- **Configuration**: Environment-based configuration
- **Logging**: Structured logging with source awareness

## Project Structure

```
/app
 ├── api/              # FastAPI application with UI routes
 │   └── routes/        # API endpoints (news, health, UI)
 ├── workers/          # Worker framework and runner
 ├── sources/          # Source-specific implementations
 │   ├── isna.py       # ISNA RSS feed worker
 │   ├── mehrnews.py   # MehrNews RSS feed worker
 │   ├── irna.py       # IRNA RSS feed worker (with Playwright)
 │   ├── fars.py       # Fars News Agency worker (with Playwright)
 │   └── tasnim.py     # Tasnim News Agency worker
 ├── db/               # Database models and session
 ├── storage/          # S3 storage client
 ├── core/             # Configuration, logging, category normalization
 ├── templates/         # Jinja2 templates for UI
 ├── static/           # CSS and static files
 └── main.py           # API entry point

/docker
 ├── api.Dockerfile
 └── worker.Dockerfile
```

## Prerequisites

- Docker and Docker Compose
- External S3-compatible storage service (MinIO, AWS S3, etc.)
- Python 3.11+ (for local development)
- PostgreSQL (included in docker-compose or external)

## Quick Start

### Configuration

1. **Create a `.env` file** (optional, defaults are provided):
   ```env
   # Database Configuration
   DATABASE_URL=postgresql+asyncpg://postgres:postgres@postgres:5432/news_db

   # S3/MinIO Configuration (External Service)
   S3_ENDPOINT=http://your-minio-host:9000
   S3_BUCKET=news-images
   S3_ACCESS_KEY=your-access-key
   S3_SECRET_KEY=your-secret-key
   S3_REGION=us-east-1
   S3_USE_SSL=false
   S3_VERIFY_SSL=true

   # Worker Configuration
   POLL_INTERVAL=300
   MAX_REQUESTS_PER_MINUTE=60
   DELAY_BETWEEN_REQUESTS=1.0

   # API Configuration
   API_HOST=0.0.0.0
   API_PORT=8000

   # Logging
   LOG_LEVEL=INFO
   ```

2. **Start all services:**
   ```bash
   docker-compose up -d
   ```

   This will start:
   - PostgreSQL database
   - API service (port 8000)
   - 5 worker services (one for each news source: ISNA, MehrNews, IRNA, Fars, Tasnim)

3. **Access the web interface:**
   - Open your browser: http://localhost:8000
   - Browse news by source and category
   - Search for news articles

4. **Test the API:**
   ```bash
   # Check API health
   curl http://localhost:8000/health
   
   # Get latest news
   curl http://localhost:8000/news/latest
   
   # Get news by ID
   curl http://localhost:8000/news/{id}
   ```

5. **Test the application (PowerShell):**
   ```powershell
   .\test-app.ps1
   ```

4. **View logs:**
   ```bash
   docker-compose logs -f api
   docker-compose logs -f worker_isna
   docker-compose logs -f worker_mehrnews
   docker-compose logs -f worker_irna
   docker-compose logs -f worker_fars
   docker-compose logs -f worker_tasnim
   ```
   
   Or use the test script:
   ```powershell
   .\test-app.ps1 -Logs
   ```

5. **Stop services:**
   ```bash
   docker-compose down
   ```
   
   Or use the test script:
   ```powershell
   .\test-app.ps1 -Stop
   ```

## Services

### API Service
- **Port**: 8000
- **Web Interface**: http://localhost:8000
- **API Endpoints**:
  - `GET /health` - Health check
  - `GET /` - Web interface (news grid with filters)
  - `GET /news/latest` - Latest news (JSON API)
  - `GET /news/{id}` - Get news by ID (JSON API)
  - `GET /news/{id}/details` - Get news details page (HTML)

### Worker Services
- **5 separate worker services** (one for each news source):
  - `worker_isna` - ISNA news source
  - `worker_mehrnews` - MehrNews source
  - `worker_irna` - IRNA source
  - `worker_fars` - Fars News Agency
  - `worker_tasnim` - Tasnim News Agency
- Each worker runs continuously, polling at configured intervals
- Graceful shutdown on SIGTERM/SIGINT

### Database
- **PostgreSQL 15**
- **Port**: 5432
- **Database**: news_db
- **User**: postgres / postgres

### Storage
- **External S3-compatible storage** (MinIO, AWS S3, or other S3-compatible services)
- Configure via environment variables (see Configuration section)
- **Note**: MinIO is no longer included in docker-compose. Use an external service.

## Configuration

All configuration is via environment variables:

### Database
- `DATABASE_URL` - PostgreSQL connection string

### S3 Storage (External Service)
- `S3_ENDPOINT` - S3 endpoint URL (e.g., `http://your-minio-host:9000`)
- `S3_BUCKET` - Bucket name (default: `news-images`)
- `S3_ACCESS_KEY` - Access key for your external MinIO/S3 service
- `S3_SECRET_KEY` - Secret key for your external MinIO/S3 service
- `S3_REGION` - AWS region (default: `us-east-1`)
- `S3_USE_SSL` - Use SSL (true/false, default: `false`)
- `S3_VERIFY_SSL` - Verify SSL certificates (true/false, default: `true`)

### Worker
- `WORKER_SOURCE` - Source name for this worker instance (isna, mehrnews, irna, fars, tasnim)
- `POLL_INTERVAL` - Polling interval in seconds (default: 300)
- `MAX_REQUESTS_PER_MINUTE` - Rate limit per source (default: 60)
- `DELAY_BETWEEN_REQUESTS` - Minimum delay between requests in seconds (default: 1.0)

### API
- `API_HOST` - API host (default: 0.0.0.0)
- `API_PORT` - API port (default: 8000)

### Logging
- `LOG_LEVEL` - Log level (DEBUG, INFO, WARNING, ERROR)

## Database Models

### news_sources
- `name` (PK) - Source name
- `interval_minutes` - Polling interval
- `last_run_at` - Last execution timestamp
- `enabled` - Whether source is enabled

### news
- `id` (UUID, PK) - News ID
- `source` - Source name (isna, mehrnews, irna, fars, tasnim)
- `title` - News title
- `body_html` - Full HTML content with images
- `summary` - News summary
- `url` (unique) - News URL (used for deduplication)
- `published_at` - Publication date/time (ISO format)
- `created_at` - Creation timestamp
- `image_url` - Main image URL (S3 path)
- `category` - Normalized category (politics, economy, society, etc.)
- `raw_category` - Original category from source

## Running Specific Workers

You can run only specific workers instead of all of them:

```bash
# Start only ISNA worker
docker-compose up -d worker_isna

# Start only Fars and Tasnim workers
docker-compose up -d worker_fars worker_tasnim

# Stop a specific worker
docker-compose stop worker_isna

# View logs for a specific worker
docker-compose logs -f worker_fars
```

## Development

### Local Development with SQLite (Recommended for Easy Setup)

For easier development without external dependencies, the application can use SQLite instead of PostgreSQL:

1. **Setup virtual environment and install dependencies:**
   ```powershell
   # Create virtual environment
   python -m venv venv

   # Install dependencies
   .\venv\Scripts\pip install -r requirements.txt
   ```

2. **Initialize the database:**
   ```powershell
   # Set environment variable to use SQLite
   $env:USE_ENV_FILE = "true"

   # Initialize database tables
   .\venv\Scripts\python.exe init_db.py
   ```

3. **Run the development scripts:**
   ```powershell
   # Run API server
   .\run-dev.ps1

   # Or run database operations
   .\run-db.ps1 -Action init      # Initialize database
   .\run-db.ps1 -Action migrate   # Run migrations
   .\run-db.ps1 -Action cleanup   # Clean up data
   ```

4. **Access the application:**
   - Web interface: http://localhost:3000
   - API endpoints available at the same host/port

**Note:** The `.env` file is pre-configured for SQLite development. Set `USE_ENV_FILE=true` to use SQLite, or leave it unset for PostgreSQL in production.

### Local Development (without Docker)

A PowerShell script (`run-local.ps1`) is provided to simplify local development:

```powershell
# Initial setup (first time only)
.\run-local.ps1 -Setup

# Run both API and Worker
.\run-local.ps1 -Both

# Run API only
.\run-local.ps1 -API

# Run Worker only (with source name)
.\run-local.ps1 -Worker -Source isna

# Stop running services
.\run-local.ps1 -Stop
```

#### Manual Setup (Alternative)

1. **Install dependencies:**
   ```bash
   python -m venv venv
   venv\Scripts\Activate.ps1  # Windows
   # or: source venv/bin/activate  # Linux/Mac
   pip install -r requirements.txt
   ```

2. **Install Playwright (required for IRNA and Fars workers):**
   ```bash
   playwright install chromium
   ```

3. **Create .env file:**
   ```env
   DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/news_db
   S3_ENDPOINT=http://your-minio-host:9000
   S3_BUCKET=news-images
   S3_ACCESS_KEY=your-access-key
   S3_SECRET_KEY=your-secret-key
   S3_REGION=us-east-1
   S3_USE_SSL=false
   S3_VERIFY_SSL=true
   WORKER_SOURCE=isna
   POLL_INTERVAL=300
   MAX_REQUESTS_PER_MINUTE=60
   DELAY_BETWEEN_REQUESTS=1.0
   API_HOST=0.0.0.0
   API_PORT=8000
   LOG_LEVEL=INFO
   ```

4. **Run API:**
   ```bash
   python -m app.main
   ```

5. **Run Worker (in separate terminal):**
   ```bash
   python -m app.workers.runner
   ```

#### Prerequisites for Local Development

- **Python 3.11+**
- **PostgreSQL** running locally or accessible
  - Default: `localhost:5432`
  - Database: `news_db`
  - User: `postgres` / Password: `postgres`
- **External S3-compatible storage** (MinIO, AWS S3, etc.)
  - Configure via `.env` file
- **Playwright** (for IRNA and Fars workers)
  - Install: `pip install playwright && playwright install chromium`

## Features

### News Sources

- **ISNA** - RSS feed based scraping
- **MehrNews** - RSS feed based scraping
- **IRNA** - RSS feed + Playwright for JavaScript-rendered content
- **Fars** - Playwright-based scraping from listing pages
- **Tasnim** - Archive page scraping with selectolax
- **IRIB News** - RSS feed based scraping
- **IPNA (ایپنا)** - Web scraping from section pages with selectolax
- **Tabnak (تابناک)** - RSS feed based scraping with support for photo galleries and videos
- **Eghtesad Online (اقتصادآنلاین)** - RSS feed based scraping

### Category Normalization

All news sources have their categories normalized to a standard set:
- `politics` - سیاسی
- `economy` - اقتصادی
- `society` - اجتماعی
- `sports` - ورزشی
- `culture` - فرهنگی
- `international` - بین الملل
- `technology` - فناوری
- `science` - علم
- `health` - سلامت
- `provinces` - استانها
- `other` - سایر

### Image Handling

- Images are downloaded from source websites
- Uploaded to S3-compatible storage
- Presigned URLs generated for secure access
- Duplicate images removed
- CORS issues handled

### Rate Limiting

- Per-source rate limiting
- Configurable requests per minute
- Exponential backoff on errors
- Graceful error handling

## Testing

A PowerShell test script (`test-app.ps1`) is provided to automate testing:

```powershell
# Run full test suite
.\test-app.ps1

# View service logs
.\test-app.ps1 -Logs

# Stop services
.\test-app.ps1 -Stop

# Clean up everything (including volumes)
.\test-app.ps1 -Clean
```

The test script will:
- Start Docker services
- Wait for services to be ready
- Test all API endpoints
- Verify service health
- Show test results summary

## License

Proprietary - Enterprise News Platform

