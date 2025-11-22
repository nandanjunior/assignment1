@echo off
REM Start all 3 music streaming microservices
echo Starting Music Streaming Microservices...
echo.

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..\..

REM Activate virtual environment
if exist "%PROJECT_ROOT%\.venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call "%PROJECT_ROOT%\.venv\Scripts\activate.bat"
)

REM Start MapReduce Service
echo Starting MapReduce Service (Port 50051)...
start "MapReduce Service" cmd /k "cd /d %SCRIPT_DIR% && python mapreduce_stream_service.py"
timeout /t 2 /nobreak >nul

REM Start User Behavior Service
echo Starting User Behavior Service (Port 50053)...
start "User Behavior Service" cmd /k "cd /d %SCRIPT_DIR% && python user_behavior_service.py"
timeout /t 2 /nobreak >nul

REM Start Genre Analysis Service
echo Starting Genre Analysis Service (Port 50055)...
start "Genre Analysis Service" cmd /k "cd /d %SCRIPT_DIR% && python genre_analysis_service.py"
timeout /t 2 /nobreak >nul

REM Start Recommendation Service
echo Starting Recommendation Service (Port 50057)...
start "Recommendation Service" cmd /k "cd /d %SCRIPT_DIR% && python recommendation_service.py"
timeout /t 2 /nobreak >nul

echo.
echo ✅ All 4 services started!
echo Service Chain: MapReduce(50051) → UserBehavior(50053) → GenreAnalysis(50055) → Recommendation(50057)
echo.
echo Now run the client: cd ..\client && run_client.bat
pause
