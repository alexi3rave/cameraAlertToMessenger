@echo off
chcp 65001 >nul
cd /d "%~dp0.."
set PYTHONPATH=%CD%\app

echo [TEST] Starting unit test run...
echo [INFO] Project root: %CD%

echo [TEST] Checking Python...
python --version
if errorlevel 1 (
    echo [ERROR] Python not found
    exit /b 1
)

echo [TEST] Checking dependencies...
python -c "import pytest" 2>nul
if errorlevel 1 (
    echo [ERROR] pytest not found, installing...
    pip install -r requirements-dev.txt
)

echo [TEST] Running all unit tests...

if not exist "reports\unit" mkdir "reports\unit"

python -m pytest tests\unit -v --tb=short ^
    --html=reports\unit\report.html --self-contained-html ^
    --junitxml=reports\unit\junit.xml ^
    --cov=app --cov-report=term-missing ^
    --cov-report=html:reports\unit\htmlcov ^
    --cov-report=xml:reports\unit\coverage.xml

set EXITCODE=%ERRORLEVEL%

echo.
echo ========================================
if %EXITCODE%==0 (
    echo [TEST] ALL TESTS PASSED!
) else (
    echo [ERROR] TESTS FAILED ^(exit code: %EXITCODE%^)
)
echo ========================================
echo.
echo [INFO] Reports generated:
echo   - HTML Report:     reports\unit\report.html
echo   - Coverage HTML:   reports\unit\htmlcov\index.html
echo   - Coverage XML:    reports\unit\coverage.xml
echo   - JUnit XML:       reports\unit\junit.xml
echo.

exit /b %EXITCODE%
