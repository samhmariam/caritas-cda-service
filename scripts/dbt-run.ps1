# Convenience wrapper for running dbt commands with environment variables loaded
# Usage:
#   .\scripts\dbt-run.ps1 compile --select staging
#   .\scripts\dbt-run.ps1 run --select staging
#   .\scripts\dbt-run.ps1 test

$ErrorActionPreference = "Stop"

# Load environment variables
$envFile = Join-Path $PSScriptRoot ".." ".env"

if (-not (Test-Path $envFile)) {
    Write-Error "❌ .env file not found at: $envFile"
    Write-Host "Please create a .env file from .env.example and fill in your credentials."
    exit 1
}

Write-Host "📁 Loading environment variables from .env..." -ForegroundColor Cyan

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    
    # Skip empty lines and comments
    if ($line -and -not $line.StartsWith("#")) {
        if ($line -match '^([^=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            
            # Remove quotes if present
            $value = $value -replace '^["'']|["'']$', ''
            
            # Set environment variable
            Set-Item -Path "env:$key" -Value $value
            
            # Mask sensitive values in output
            $maskedValue = if ($key -match 'PASSWORD|SECRET|TOKEN|KEY') {
                "***"
            } else {
                $value
            }
            Write-Host "  ✓ $key = $maskedValue" -ForegroundColor Green
        }
    }
}

Write-Host ""
Write-Host "✅ Loaded environment variables" -ForegroundColor Green
Write-Host ""

# Change to dbt directory and run dbt
$dbtDir = Join-Path $PSScriptRoot ".." "dbt"
Push-Location $dbtDir

try {
    Write-Host "🚀 Executing: dbt $($args -join ' ') --profiles-dir ." -ForegroundColor Yellow
    Write-Host ""
    
    # Run dbt with all arguments
    & dbt @args --profiles-dir .
    
    $exitCode = $LASTEXITCODE
    Pop-Location
    exit $exitCode
} catch {
    Pop-Location
    throw
}
