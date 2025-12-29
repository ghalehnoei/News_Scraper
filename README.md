# News Ingestion Platform - Phase 1

Enterprise Persian news ingestion platform - Phase 1: Project Skeleton

## Overview

This is the foundational skeleton for a news ingestion platform that will collect news from Persian news sources. Phase 1 focuses exclusively on infrastructure, structure, and contracts - no business logic or scraping functionality.

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
 ├── api/              # FastAPI application
 ├── workers/          # Worker framework
 ├── sources/          # Source implementations (future)
 ├── db/               # Database models and session
 ├── storage/          # S3 storage client
 ├── core/             # Configuration, logging, lifecycle
 └── main.py           # API entry point

/docker
 ├── api.Dockerfile
 └── worker.Dockerfile
```

## Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)

## Quick Start

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

2. **Test the application (PowerShell):**
   ```powershell
   .\test-app.ps1
   ```

3. **Or test manually:**
   ```bash
   # Check API health
   curl http://localhost:8000/health
   
   # Test news endpoint
   curl http://localhost:8000/news/latest
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
- **Endpoints**:
  - `GET /health` - Health check
  - `GET /news/latest` - Latest news (returns empty list in Phase 1)
  - `GET /news/{id}` - Get news by ID (not implemented in Phase 1)

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
- `WORKER_SOURCE` - Source name for this worker instance
- `POLL_INTERVAL` - Polling interval in seconds (default: 300)

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
- `source` - Source name
- `title` - News title
- `body_html` - Full HTML content
- `summary` - News summary
- `url` (unique) - News URL
- `published_at` - Raw publication date string
- `created_at` - Creation timestamp
- `image_url` - Image URL
- `category` - News category

## Development

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
.\run-local.ps1 -Worker -Source placeholder

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

2. **Create .env file:**
   ```env
   DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/news_db
   S3_ENDPOINT=http://localhost:9000
   S3_BUCKET=news-images
   S3_ACCESS_KEY=minioadmin
   S3_SECRET_KEY=minioadmin
   S3_REGION=us-east-1
   S3_USE_SSL=false
   WORKER_SOURCE=placeholder
   POLL_INTERVAL=300
   API_HOST=0.0.0.0
   API_PORT=8000
   LOG_LEVEL=INFO
   ```

3. **Run API:**
   ```bash
   python -m app.main
   ```

4. **Run Worker (in separate terminal):**
   ```bash
   python -m app.workers.runner
   ```

#### Prerequisites for Local Development

- **Python 3.11+**
- **PostgreSQL** running locally or accessible
  - Default: `localhost:5432`
  - Database: `news_db`
  - User: `postgres` / Password: `postgres`
- **MinIO or S3-compatible storage** running locally or accessible
  - Default: `localhost:9000`
  - Or use AWS S3 (update `.env` accordingly)

#### Quick Start with Docker Services

You can use Docker just for PostgreSQL and MinIO while running the app locally:

```powershell
# Start only PostgreSQL and MinIO in Docker
.\start-services.ps1

# Run the app locally
.\run-local.ps1 -Both

# Stop Docker services when done
.\start-services.ps1 -Stop
```

This gives you the best of both worlds: Docker for infrastructure, local execution for development.

## Phase 1 Limitations

- No actual scraping logic
- No RSS parsing
- No HTML extraction
- No database queries in API (returns empty lists)
- Worker only runs placeholder loop
- No image upload logic

## Next Phases

Future phases will add:
- Source-specific worker implementations
- RSS feed parsing
- HTML content extraction
- Database queries and CRUD operations
- Image download and upload to S3
- Error handling and retry logic

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

