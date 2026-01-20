$ErrorActionPreference = "Stop"

function Load-DotEnv {
    param([string]$Path = ".env")
    if (-not (Test-Path $Path)) {
        Write-Host ".env not found at $Path"
        return
    }
    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) { return }
        $parts = $line.Split("=", 2)
        if ($parts.Count -lt 2) { return }
        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ($value.StartsWith('"') -and $value.EndsWith('"')) {
            $value = $value.Substring(1, $value.Length - 2)
        } elseif ($value.StartsWith("'") -and $value.EndsWith("'")) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

Set-Location (Split-Path $PSScriptRoot -Parent)
Load-DotEnv ".env"

Add-Type -AssemblyName System.Net.Http

$username = $env:AFP_USERNAME
$password = $env:AFP_PASSWORD
$basic = $env:AFP_BASIC_AUTH

Write-Host "AFP_USERNAME=$username"
Write-Host "AFP_PASSWORD=$password"
Write-Host "AFP_BASIC_AUTH=$basic"

if (-not $username -or -not $password -or -not $basic) {
    Write-Host "Missing required AFP env variables. Check .env in project root."
    exit 2
}

$authUrl = "https://afp-apicore-prod.afp.com/oauth/token?grant_type=password&username=$([uri]::EscapeDataString($username))&password=$([uri]::EscapeDataString($password))"

try {
    $handler = New-Object System.Net.Http.HttpClientHandler
    $client = New-Object System.Net.Http.HttpClient($handler)
    $client.Timeout = [TimeSpan]::FromSeconds(30)
    $client.DefaultRequestHeaders.TryAddWithoutValidation("Accept", "application/json") | Out-Null
    $client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", $basic) | Out-Null

    $content = New-Object System.Net.Http.StringContent("", [Text.Encoding]::UTF8, "application/x-www-form-urlencoded")
    $response = $client.PostAsync($authUrl, $content).Result
    $body = $response.Content.ReadAsStringAsync().Result

    Write-Host ("Status: " + [int]$response.StatusCode)
    if ($body) { $body } else { "<empty body>" }
} catch {
    if ($_.Exception -and $_.Exception.Message) {
        $_.Exception.Message
    } else {
        "Unknown error"
    }
    exit 1
} finally {
    if ($client) { $client.Dispose() }
}

