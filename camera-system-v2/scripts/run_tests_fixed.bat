@echo off
chcp 65001 >nul
set "PYTHONPATH=%~dp0..\app"
cd /d "%~dp0.."

:: Use full path to real Python (bypass WindowsApps wrapper)
set "PYTHON=C:\Users\user\AppData\Local\Programs\Python\Python314\python.exe"

echo === Running Unit Tests ===
echo Python: %PYTHON%
echo Project: %CD%
echo.

:: Create reports directory
if not exist "reports\unit" mkdir "reports\unit"

:: Run tests with coverage
"%PYTHON%" -m pytest tests\unit -v --tb=short ^
    --html=reports\unit\report.html --self-contained-html ^
    --junitxml=reports\unit\junit.xml ^
    --cov=app --cov-report=term-missing ^
    --cov-report=html:reports\unit\htmlcov ^
    --cov-report=xml:reports\unit\coverage.xml

set EXITCODE=%ERRORLEVEL%

echo.
echo ========================================
if %EXITCODE%==0 (
    echo === ALL TESTS PASSED! ===
) else (
    echo === TESTS FAILED (exit: %EXITCODE%) ===
)
echo ========================================
echo.
echo Reports generated:
echo   - HTML Report:  reports\unit\report.html
echo   - Coverage:     reports\unit\htmlcov\index.html
echo   - Coverage XML: reports\unit\coverage.xml
echo   - JUnit XML:    reports\unit\junit.xml
echo.

exit /b %EXITCODE%
