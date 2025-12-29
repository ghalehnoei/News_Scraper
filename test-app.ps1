# PowerShell script to test News Ingestion Platform - Phase 1
# This script tests the docker-compose setup and API endpoints

param(
    [switch]$Clean,
    [switch]$Logs,
    [switch]$Stop
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

function Wait-ForService {
    param(
        [string]$Url,
        [int]$MaxRetries = 30,
        [int]$DelaySeconds = 2
    )
    
    Write-Info "Waiting for service at $Url..."
    $retries = 0
    
    while ($retries -lt $MaxRetries) {
        try {
            $response = Invoke-WebRequest -Uri $Url -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-Success "Service is ready!"
                return $true
            }
        } catch {
            # Service not ready yet
        }
        
        $retries++
        Write-Info "  Attempt $retries/$MaxRetries - waiting ${DelaySeconds}s..."
        Start-Sleep -Seconds $DelaySeconds
    }
    
    Write-Error "Service did not become ready after $($MaxRetries * $DelaySeconds) seconds"
    return $false
}

function Test-HealthEndpoint {
    Write-Info "`n=== Testing Health Endpoint ==="
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8000/health" -Method Get
        Write-Success "✓ Health endpoint responded successfully"
        Write-Output "  Response: $($response | ConvertTo-Json -Compress)"
        return $true
    } catch {
        Write-Error "✗ Health endpoint failed: $_"
        return $false
    }
}

function Test-RootEndpoint {
    Write-Info "`n=== Testing Root Endpoint ==="
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8000/" -Method Get
        Write-Success "✓ Root endpoint responded successfully"
        Write-Output "  Response: $($response | ConvertTo-Json -Compress)"
        return $true
    } catch {
        Write-Error "✗ Root endpoint failed: $_"
        return $false
    }
}

function Test-NewsLatestEndpoint {
    Write-Info "`n=== Testing News Latest Endpoint ==="
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8000/news/latest" -Method Get
        Write-Success "✓ News latest endpoint responded successfully"
        Write-Output "  Response: $($response | ConvertTo-Json -Compress)"
        
        if ($response.Count -eq 0) {
            Write-Info "  (Empty list is expected for Phase 1)"
        }
        return $true
    } catch {
        Write-Error "✗ News latest endpoint failed: $_"
        return $false
    }
}

function Test-NewsByIdEndpoint {
    Write-Info "`n=== Testing News By ID Endpoint ==="
    try {
        $testId = "00000000-0000-0000-0000-000000000000"
        $response = Invoke-RestMethod -Uri "http://localhost:8000/news/$testId" -Method Get -ErrorAction Stop
        Write-Warning "✗ Expected 404 but got response"
        return $false
    } catch {
        $statusCode = $_.Exception.Response.StatusCode.value__
        if ($statusCode -eq 404) {
            Write-Success "✓ News by ID endpoint correctly returns 404 (expected for Phase 1)"
            return $true
        } else {
            Write-Error "✗ News by ID endpoint failed with unexpected status: $statusCode"
            return $false
        }
    }
}

function Test-DockerServices {
    Write-Info "`n=== Checking Docker Services ==="
    
    $services = @("news_api", "news_worker", "news_postgres", "news_minio")
    $allRunning = $true
    
    foreach ($service in $services) {
        $container = docker ps --filter "name=$service" --format "{{.Names}}" 2>$null
        if ($container -eq $service) {
            Write-Success "✓ $service is running"
        } else {
            Write-Error "✗ $service is not running"
            $allRunning = $false
        }
    }
    
    return $allRunning
}

function Show-Logs {
    Write-Info "`n=== Showing Recent Logs ==="
    Write-Info "API Logs:"
    docker logs news_api --tail 20 2>&1
    Write-Info "`nWorker Logs:"
    docker logs news_worker --tail 20 2>&1
}

# Main execution
Write-Info "`n=========================================="
Write-Info "News Ingestion Platform - Test Script"
Write-Info "==========================================`n"

# Check prerequisites
if (-not (Test-Command "docker")) {
    Write-Error "Docker is not installed or not in PATH"
    exit 1
}

if (-not (Test-Command "docker-compose")) {
    Write-Error "Docker Compose is not installed or not in PATH"
    exit 1
}

# Handle cleanup
if ($Clean) {
    Write-Info "Cleaning up Docker resources..."
    docker-compose down -v
    Write-Success "Cleanup complete"
    exit 0
}

# Handle stop
if ($Stop) {
    Write-Info "Stopping Docker services..."
    docker-compose down
    Write-Success "Services stopped"
    exit 0
}

# Handle logs
if ($Logs) {
    Show-Logs
    exit 0
}

# Start services
Write-Info "Starting Docker services..."
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to start Docker services"
    exit 1
}

Write-Success "Docker services started"
Start-Sleep -Seconds 5

# Check if services are running
$servicesOk = Test-DockerServices
if (-not $servicesOk) {
    Write-Warning "Some services are not running. Waiting a bit longer..."
    Start-Sleep -Seconds 10
    $servicesOk = Test-DockerServices
}

# Wait for API to be ready
Write-Info "`nWaiting for API service to be ready..."
if (-not (Wait-ForService -Url "http://localhost:8000/health" -MaxRetries 30 -DelaySeconds 2)) {
    Write-Error "API service did not become ready"
    Write-Info "Showing logs for debugging:"
    Show-Logs
    exit 1
}

# Run tests
Write-Info "`n=========================================="
Write-Info "Running API Tests"
Write-Info "==========================================`n"

$testResults = @{
    Health = Test-HealthEndpoint
    Root = Test-RootEndpoint
    NewsLatest = Test-NewsLatestEndpoint
    NewsById = Test-NewsByIdEndpoint
}

# Summary
Write-Info "`n=========================================="
Write-Info "Test Summary"
Write-Info "=========================================="

$passed = ($testResults.Values | Where-Object { $_ -eq $true }).Count
$total = $testResults.Count

foreach ($test in $testResults.GetEnumerator()) {
    $status = if ($test.Value) { "✓ PASS" } else { "✗ FAIL" }
    Write-Output "  $($test.Key): $status"
}

Write-Output "`nTotal: $passed/$total tests passed"

if ($passed -eq $total) {
    Write-Success "`n✓ All tests passed!"
    Write-Info "`nServices are running. Use -Logs to view logs, -Stop to stop services, or -Clean to remove everything."
} else {
    Write-Error "`n✗ Some tests failed. Check the output above for details."
    Write-Info "`nUse -Logs to view service logs for debugging."
    exit 1
}

