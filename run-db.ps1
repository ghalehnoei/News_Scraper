# Database operations script for News Scraper
# This script sets up the environment for local SQLite development

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("init", "migrate", "cleanup")]
    [string]$Action
)

# Set environment variable to use .env file
$env:USE_ENV_FILE = "true"

switch ($Action) {
    "init" {
        Write-Host "Initializing database..."
        .\venv\Scripts\python.exe init_db.py
    }
    "migrate" {
        Write-Host "Running migration..."
        .\venv\Scripts\python.exe run_migration.py
    }
    "cleanup" {
        Write-Host "Cleaning up data..."
        .\venv\Scripts\python.exe cleanup_irna.py
    }
}

