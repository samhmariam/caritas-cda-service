# Load environment variables from .env file
# Usage: 
#   .\scripts\load-env.ps1
#   OR
#   .\scripts\load-env.ps1 "dbt compile --select staging"

param(
    [string]$Command = ""
)

$envFile = Join-Path $PSScriptRoot ".." ".env"

if (-not (Test-Path $envFile)) {
    Write-Error "❌ .env file not found at: $envFile"
    Write-Host "Please create a .env file from .env.example and fill in your credentials."
    exit 1
}

Write-Host "📁 Loading environment variables from .env..." -ForegroundColor Cyan

$envVars = @{}
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    
    # Skip empty lines and comments
    if ($line -and -not $line.StartsWith("#")) {
        if ($line -match '^([^=]+)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            
            # Remove quotes if present
            $value = $value -replace '^["'']|["'']$', ''
            
            # Set environment variable using $env: scope (inheritable by child processes)
            Set-Item -Path "env:$key" -Value $value
            $envVars[$key] = $value
            
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
Write-Host "✅ Loaded $($envVars.Count) environment variables" -ForegroundColor Green

# If a command was provided, execute it
if ($Command) {
    Write-Host ""
    Write-Host "🚀 Executing: $Command" -ForegroundColor Yellow
    Write-Host ""
    Invoke-Expression $Command
} else {
    Write-Host ""
    Write-Host "💡 Environment variables are now loaded. You can run dbt commands:" -ForegroundColor Cyan
    Write-Host "   dbt compile --select staging" -ForegroundColor White
    Write-Host "   dbt run --select staging" -ForegroundColor White
    Write-Host ""
    Write-Host "Or pass a command to this script:" -ForegroundColor Cyan
    Write-Host "   .\scripts\load-env.ps1 'dbt compile --select staging'" -ForegroundColor White
}
