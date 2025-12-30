# PowerShell script to run News Ingestion Platform locally (without Docker)
# This script sets up and runs the API and Worker services locally

param(
    [switch]$Setup,
    [switch]$API,
    [switch]$Worker,
    [switch]$Both,
    [switch]$Stop,
    [string]$Source = "placeholder"
)

$ErrorActionPreference = "Stop"

function Write-ColorOutput($ForegroundColor) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    if ($args) {
        Write-Output $args
    }
    $host.UI.RawUI.ForegroundColor = $fc
}

function Write-Success { Write-ColorOutput Green $args }
function Write-Info { Write-ColorOutput Cyan $args }
function Write-Warning { Write-ColorOutput Yellow $args }
function Write-Error { Write-ColorOutput Red $args }

function Test-Command {
    param([string]$Command)
    $null = Get-Command $Command -ErrorAction SilentlyContinue
    return $?
}

function Test-Python {
    if (-not (Test-Command "python")) {
        Write-Error "Python is not installed or not in PATH"
        Write-Info "Please install Python 3.11+ from https://www.python.org/"
        return $false
    }
    
    $version = python --version 2>&1
    Write-Success "Found: $version"
    
    # Check if version is 3.11 or higher
    if ($version -match "Python (\d+)\.(\d+)") {
        $major = [int]$matches[1]
        $minor = [int]$matches[2]
        if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 11)) {
            Write-Error "Python 3.11+ is required. Found: $version"
            return $false
        }
    }
    
    return $true
}

function Setup-Environment {
    Write-Info "`n=== Setting up local development environment ==="
    
    # Check Python
    if (-not (Test-Python)) {
        exit 1
    }
    
    # Create virtual environment if it doesn't exist
    if (-not (Test-Path "venv")) {
        Write-Info "Creating virtual environment..."
        python -m venv venv
        Write-Success "Virtual environment created"
    } else {
        Write-Info "Virtual environment already exists"
    }
    
    # Activate virtual environment
    Write-Info "Activating virtual environment..."
    & "venv\Scripts\Activate.ps1"
    
    # Upgrade pip
    Write-Info "Upgrading pip..."
    python -m pip install --upgrade pip --quiet
    
    # Install requirements
    Write-Info "Installing dependencies..."
    Write-Info "This may take a few minutes, especially for packages that need compilation..."
    
    # Try installing with verbose output to see what's happening
    pip install -r requirements.txt
    
    if ($LASTEXITCODE -ne 0) {
        Write-Error "`nFailed to install dependencies"
        Write-Warning "`nSome packages (like asyncpg) may require build tools:"
        Write-Info "  Option 1: Install Microsoft C++ Build Tools"
        Write-Info "    Download from: https://visualstudio.microsoft.com/visual-cpp-build-tools/"
        Write-Info "  Option 2: Try installing with pre-built wheels:"
        Write-Info "    pip install --only-binary :all: -r requirements.txt"
        Write-Info "  Option 3: Install dependencies one by one to identify the issue:"
        Write-Info "    pip install fastapi uvicorn pydantic pydantic-settings"
        Write-Info "    pip install sqlalchemy"
        Write-Info "    pip install asyncpg"
        Write-Info "    pip install aioboto3 boto3"
        Write-Info "    pip install python-dotenv"
        Write-Info "`nYou can continue setup manually or fix the dependency issue first."
        Write-Warning "Setup incomplete - some dependencies failed to install"
        exit 1
    }
    
    Write-Success "Dependencies installed successfully"
    
    # Create .env file if it doesn't exist
    if (-not (Test-Path ".env")) {
        Write-Info "Creating .env file..."
        @"
# Database Configuration
# For local development, you can use:
# - Local PostgreSQL: postgresql+asyncpg://postgres:password@localhost:5432/news_db
# - Docker PostgreSQL: postgresql+asyncpg://postgres:postgres@localhost:5432/news_db
DATABASE_URL=postgresql+asyncpg://user:user@localhost:5432/news_db

# S3 Storage Configuration
# For local development, you can use:
# - Local MinIO: http://localhost:9000
# - Docker MinIO: http://localhost:9000
S3_ENDPOINT=http://localhost:9000
S3_BUCKET=news-images
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_REGION=us-east-1
S3_USE_SSL=false
S3_VERIFY_SSL=true

# Worker Configuration
WORKER_SOURCE=placeholder
POLL_INTERVAL=300

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Logging
LOG_LEVEL=INFO
"@ | Out-File -FilePath ".env" -Encoding utf8
        Write-Success ".env file created"
        Write-Warning "Please update .env with your local database and S3 credentials"
    } else {
        Write-Info ".env file already exists"
    }
    
    Write-Success "`nSetup complete!"
    Write-Info "`nNext steps:"
    Write-Info "1. Make sure PostgreSQL is running and accessible"
    Write-Info "2. Make sure MinIO/S3 is running and accessible"
    Write-Info "3. Update .env file with correct credentials"
    Write-Info "4. Run: .\run-local.ps1 -Both"
}

function Start-API {
    Write-Info "`n=== Starting API Service ==="
    
    if (-not (Test-Path "venv")) {
        Write-Error "Virtual environment not found. Run with -Setup first"
        exit 1
    }
    
    # Activate virtual environment
    & "venv\Scripts\Activate.ps1"
    
    # Load environment variables from .env
    if (Test-Path ".env") {
        Get-Content ".env" | ForEach-Object {
            if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($key, $value, "Process")
            }
        }
    }
    
    # Enable .env file usage for local development
    [Environment]::SetEnvironmentVariable("USE_ENV_FILE", "true", "Process")
    
    Write-Info "Starting API on http://localhost:8000"
    Write-Info "Press Ctrl+C to stop"
    
    python -m app.main
}

function Start-Worker {
    param([string]$SourceName)
    
    Write-Info "`n=== Starting Worker Service ==="
    
    if (-not (Test-Path "venv")) {
        Write-Error "Virtual environment not found. Run with -Setup first"
        exit 1
    }
    
    # Activate virtual environment
    & "venv\Scripts\Activate.ps1"
    
    # Load environment variables from .env
    if (Test-Path ".env") {
        Get-Content ".env" | ForEach-Object {
            if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($key, $value, "Process")
            }
        }
    }
    
    # Enable .env file usage for local development
    [Environment]::SetEnvironmentVariable("USE_ENV_FILE", "true", "Process")
    
    # Set worker source
    if ($SourceName) {
        [Environment]::SetEnvironmentVariable("WORKER_SOURCE", $SourceName, "Process")
    }
    
    $workerSource = [Environment]::GetEnvironmentVariable("WORKER_SOURCE", "Process")
    if (-not $workerSource) {
        Write-Error "WORKER_SOURCE environment variable is required"
        Write-Info "Set it in .env file or use -Source parameter"
        exit 1
    }
    
    Write-Info "Starting worker for source: $workerSource"
    Write-Info "Press Ctrl+C to stop"
    
    python -m app.workers.runner
}

function Start-Both {
    param([string]$SourceName)
    
    Write-Info "`n=== Starting API and Worker Services ==="
    
    if (-not (Test-Path "venv")) {
        Write-Error "Virtual environment not found. Run with -Setup first"
        exit 1
    }
    
    # Activate virtual environment
    & "venv\Scripts\Activate.ps1"
    
    # Load environment variables from .env
    if (Test-Path ".env") {
        Get-Content ".env" | ForEach-Object {
            if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
                $key = $matches[1].Trim()
                $value = $matches[2].Trim()
                [Environment]::SetEnvironmentVariable($key, $value, "Process")
            }
        }
    }
    
    # Enable .env file usage for local development
    [Environment]::SetEnvironmentVariable("USE_ENV_FILE", "true", "Process")
    
    # Set worker source if provided
    if ($SourceName -and $SourceName -ne "placeholder") {
        [Environment]::SetEnvironmentVariable("WORKER_SOURCE", $SourceName, "Process")
    }
    
    Write-Info "Starting API and Worker in separate windows..."
    Write-Info "API will run in this window"
    Write-Info "Worker will run in a new window"
    
    # Determine worker source: prioritize parameter, then environment variable
    $workerSourceEnv = $SourceName
    if (-not $workerSourceEnv -or $workerSourceEnv -eq "placeholder") {
        $workerSourceEnv = [Environment]::GetEnvironmentVariable("WORKER_SOURCE", "Process")
    }
    
    if (-not $workerSourceEnv -or $workerSourceEnv -eq "placeholder") {
        Write-Error "WORKER_SOURCE is required. Please provide -Source parameter or set it in .env file"
        Write-Info "Example: .\run-local.ps1 -Both -Source tasnim"
        exit 1
    }
    
    Write-Info "Worker source: $workerSourceEnv"
    
    $workerScript = @"
cd '$PWD'
venv\Scripts\Activate.ps1
`$env:USE_ENV_FILE='true'
if (Test-Path '.env') {
    Get-Content '.env' | ForEach-Object {
        if (`$_ -match '^\s*([^#][^=]+)=(.*)$') {
            `$key = `$matches[1].Trim()
            `$value = `$matches[2].Trim()
            [Environment]::SetEnvironmentVariable(`$key, `$value, 'Process')
        }
    }
}
# Set WORKER_SOURCE after loading .env to override any value from .env
`$env:WORKER_SOURCE='$workerSourceEnv'
[Environment]::SetEnvironmentVariable('WORKER_SOURCE', '$workerSourceEnv', 'Process')
Write-Host 'Starting Worker for source: ' -NoNewline
Write-Host '$workerSourceEnv' -ForegroundColor Green
python -m app.workers.runner
pause
"@
    
    $workerScriptPath = Join-Path $env:TEMP "start-worker.ps1"
    $workerScript | Out-File -FilePath $workerScriptPath -Encoding utf8
    
    Start-Process powershell -ArgumentList "-NoExit", "-File", $workerScriptPath
    Write-Success "Worker started in new window"
    
    # Start API in current window
    Start-Sleep -Seconds 2
    Write-Info "`nStarting API..."
    python -m app.main
}

function Stop-Services {
    Write-Info "`n=== Stopping Services ==="
    
    # Kill Python processes - this is a simple approach
    # Note: This will stop all Python processes, be careful if you have other Python apps running
    $pythonProcesses = Get-Process python -ErrorAction SilentlyContinue
    
    if ($pythonProcesses) {
        Write-Warning "Found Python processes. Stopping them..."
        $pythonProcesses | Stop-Process -Force
        Write-Success "Stopped Python processes"
    } else {
        Write-Info "No Python processes found"
    }
    
    # Also try to find and stop worker window if it exists
    $workerWindows = Get-Process powershell -ErrorAction SilentlyContinue | Where-Object {
        $_.MainWindowTitle -like "*start-worker*" -or $_.Path -like "*start-worker*"
    }
    
    if ($workerWindows) {
        $workerWindows | Stop-Process -Force
        Write-Success "Stopped worker window"
    }
}

function Show-Help {
    Write-Info "`nNews Ingestion Platform - Local Development Script"
    Write-Info "==================================================`n"
    Write-Output "Usage: .\run-local.ps1 [OPTIONS]`n"
    Write-Output "Options:"
    Write-Output "  -Setup          Set up local development environment (first time)"
    Write-Output "  -API            Run API service only"
    Write-Output "  -Worker         Run Worker service only"
    Write-Output "  -Both           Run both API and Worker (default)"
    Write-Output "  -Source <name>  Set worker source name (for Worker or Both)"
    Write-Output "  -Stop           Stop running services"
    Write-Output "`nExamples:"
    Write-Output "  .\run-local.ps1 -Setup              # Initial setup"
    Write-Output "  .\run-local.ps1 -Both               # Run both services"
    Write-Output "  .\run-local.ps1 -API                # Run API only"
    Write-Output "  .\run-local.ps1 -Worker -Source mehr  # Run worker for 'mehr' source"
    Write-Output "  .\run-local.ps1 -Stop               # Stop services"
    Write-Output "`nPrerequisites:"
    Write-Output "  - Python 3.11+"
    Write-Output "  - PostgreSQL running on localhost:5432"
    Write-Output "  - MinIO/S3 running on localhost:9000 (or update .env)"
}

# Main execution
Write-Info "`n=========================================="
Write-Info "News Ingestion Platform - Local Runner"
Write-Info "==========================================`n"

# Handle setup first (before help check)
if ($Setup) {
    Setup-Environment
    exit 0
}

# Handle stop
if ($Stop) {
    Stop-Services
    exit 0
}

# Handle service start
if ($API) {
    Start-API
} elseif ($Worker) {
    Start-Worker -SourceName $Source
} elseif ($Both) {
    # Only use placeholder if Source was not explicitly provided
    if (-not $Source -or $Source -eq "placeholder") {
        # Check if WORKER_SOURCE is set in .env
        if (Test-Path ".env") {
            $envContent = Get-Content ".env"
            foreach ($line in $envContent) {
                if ($line -match '^\s*WORKER_SOURCE\s*=\s*(.+)$') {
                    $Source = $matches[1].Trim()
                    break
                }
            }
        }
        if (-not $Source -or $Source -eq "placeholder") {
            Write-Warning "WORKER_SOURCE not specified. Please use -Source parameter or set it in .env file"
            Write-Info "Example: .\run-local.ps1 -Both -Source tasnim"
            exit 1
        }
    }
    Start-Both -SourceName $Source
} else {
    # No action specified - show help
    Show-Help
    exit 0
}

