@echo off
:: ============================================================
:: IBM MQ -> Solace PubSub+ Migration Demo
:: Run this from the MQ_Solace_Demo folder
:: Requires: Docker Desktop running, Python 3.8+
:: ============================================================

title IBM MQ -> Solace Migration Demo

echo.
echo ============================================================
echo   IBM MQ ^> Solace PubSub+ Migration Demo
echo   HCLTech Middleware Solutioning ^| IKEA Retail Systems
echo ============================================================
echo.

:: ── STEP 0: Install Python deps ──────────────────────────────
echo [STEP 0] Installing Python dependencies ...
pip install -r requirements.txt --quiet
echo.

:: ── STEP 1: Start Docker containers ──────────────────────────
echo [STEP 1] Starting IBM MQ + Solace + TomcatEE containers ...
echo   (First run pulls images - this may take 5-10 minutes)
echo.
docker-compose up -d
if errorlevel 1 (
    echo.
    echo   ERROR: docker-compose failed. Is Docker Desktop running?
    pause
    exit /b 1
)
echo.

:: ── STEP 2: Wait for services and provision Solace ───────────
echo [STEP 2] Waiting for services to be healthy ...
echo   IBM MQ needs ~60s, Solace needs ~90s, Tomcat needs ~45s
echo.
python setup.py
if errorlevel 1 (
    echo   ERROR: Setup failed. Check: docker-compose ps
    pause
    exit /b 1
)
echo.

:: ── STEP 3: Reset demo to baseline (IBM MQ) ──────────────────
echo [STEP 3] Resetting demo to IBM MQ baseline ...
echo.
python reset_demo.py
echo.

:: ── STEP 4: Start the migration portal ───────────────────────
echo [STEP 4] Starting Self-Service Migration Portal ...
echo   Portal will open at http://localhost:5001
echo   TomcatEE app    at  http://localhost:8888/order-mgmt/
echo.
echo   Close this window to stop the portal.
echo ============================================================
echo.
python self_service_portal.py
