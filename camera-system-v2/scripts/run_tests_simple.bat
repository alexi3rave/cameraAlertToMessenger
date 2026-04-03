@echo off
chcp 65001 >nul
cd /d "%~dp0.."

set PYTHONPATH=%CD%\app
set TEMPFILE=%TEMP%\test_out_%RANDOM%.txt

echo === STARTING TESTS ===
echo Project: %CD%
echo.

python -m unittest discover -s tests/unit -v > %TEMPFILE% 2>&1
set EXITCODE=%ERRORLEVEL%

type %TEMPFILE%
del %TEMPFILE% 2>nul

echo.
echo === EXIT CODE: %EXITCODE% ===
exit /b %EXITCODE%
