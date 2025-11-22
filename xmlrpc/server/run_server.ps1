# run_xmlrpc_server.ps1
Write-Host "Starting Music Streaming XML-RPC Microservices..." -ForegroundColor Green
Write-Host ""

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent (Split-Path -Parent $scriptDir)

# Activate virtual environment
$venvActivate = Join-Path $projectRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
    Write-Host "Activating virtual environment..." -ForegroundColor Cyan
    & $venvActivate
}

# Start MapReduce Service
Write-Host "Starting MapReduce Service (Port 8001)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python mapreduce.py"
Start-Sleep -Seconds 2

# Start User Behavior Service
Write-Host "Starting User Behavior Service (Port 8003)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python user_behavior.py"
Start-Sleep -Seconds 2

# Start Genre Analysis Service
Write-Host "Starting Genre Analysis Service (Port 8005)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python genre_analysis.py"
Start-Sleep -Seconds 2

# Start Recommendation Service
Write-Host "Starting Recommendation Service (Port 8007)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python recommendation.py"
Start-Sleep -Seconds 2

Write-Host ""
Write-Host "✅ All 4 XML-RPC services started successfully!" -ForegroundColor Green
Write-Host "Service Chain: MapReduce(8001) → UserBehavior(8003) → GenreAnalysis(8005) → Recommendation(8007)" -ForegroundColor Cyan
Write-Host ""
Write-Host "Now run the XML-RPC client:" -ForegroundColor Yellow
Write-Host "  cd ..\client" -ForegroundColor White
Write-Host "  .\run_xmlrpc_client.ps1" -ForegroundColor White
