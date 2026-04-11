@echo off
setlocal

echo ============================================================
echo  OHLC Extractor -- Environment Setup
echo ============================================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Install Python 3.9+ from https://python.org
    pause & exit /b 1
)

if not exist "venv\" (
    echo [1/4] Creating virtual environment...
    python -m venv venv
    if errorlevel 1 ( echo [ERROR] Failed. & pause & exit /b 1 )
    echo       Done.
) else (
    echo [1/4] Virtual environment already exists. Skipping.
)

echo [2/4] Activating virtual environment...
call venv\Scripts\activate.bat

echo [3/4] Upgrading pip...
python -m pip install --upgrade pip --quiet

echo [4/4] Installing dependencies...
pip install --upgrade yfinance pandas --quiet

echo.
echo ============================================================
echo  Setup complete!
echo.
echo  To run the extractor:
echo    Double-click run.bat
echo    OR: call venv\Scripts\activate ^& python OHLC_Extractor.py
echo ============================================================
echo.

if not exist "run.bat" (
    echo @echo off > run.bat
    echo call venv\Scripts\activate.bat >> run.bat
    echo python OHLC_Extractor.py >> run.bat
    echo Created run.bat
)

pause
