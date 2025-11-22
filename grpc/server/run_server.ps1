# run_server.ps1
Write-Host "Starting Music Streaming Microservices..." -ForegroundColor Green
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
Write-Host "Starting MapReduce Service (Port 50051)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python mapreduce_stream_service.py"
Start-Sleep -Seconds 2

# Start User Behavior Service
Write-Host "Starting User Behavior Service (Port 50053)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python user_behavior_service.py"
Start-Sleep -Seconds 2

# Start Genre Analysis Service
Write-Host "Starting Genre Analysis Service (Port 50055)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python genre_analysis_service.py"
Start-Sleep -Seconds 2

# Start Recommendation Service
Write-Host "Starting Recommendation Service (Port 50057)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$scriptDir'; & '$venvActivate'; python recommendation_service.py"
Start-Sleep -Seconds 2

Write-Host ""
Write-Host "✅ All 4 services started successfully!" -ForegroundColor Green
Write-Host "Service Chain: MapReduce(50051) → UserBehavior(50053) → GenreAnalysis(50055) → Recommendation(50057)" -ForegroundColor Cyan
Write-Host ""
Write-Host "Now run the client:" -ForegroundColor Yellow
Write-Host "  cd ..\client" -ForegroundColor White
Write-Host "  .\run_client.ps1" -ForegroundColor White
