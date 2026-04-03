#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run unit tests for camera-system-v2 with coverage and reporting.

.DESCRIPTION
    This script runs all unit tests, generates coverage reports,
    and produces HTML/XML reports for CI integration.

.USAGE
    # Run all tests with full reporting
    .\scripts\run_unit_tests.ps1

    # Run tests without coverage (fast)
    .\scripts\run_unit_tests.ps1 -Fast

    # Run specific test file
    .\scripts\run_unit_tests.ps1 -TestFile test_ftp_cleanup_worker

    # Run specific test
    .\scripts\run_unit_tests.ps1 -Test "TestPathIsUnderRoot.test_path_inside_root_allowed"

    # Run with verbose output
    .\scripts\run_unit_tests.ps1 -Verbose

.PARAMETER Fast
    Skip coverage reporting for faster execution.

.PARAMETER TestFile
    Run only a specific test file (e.g., "test_ftp_cleanup_worker").

.PARAMETER Test
    Run only a specific test (e.g., "TestClass.test_method").

.PARAMETER Verbose
    Enable verbose pytest output.

.PARAMETER NoClean
    Don't clean reports directory before running.

.EXAMPLE
    .\scripts\run_unit_tests.ps1
    Runs all tests with coverage and generates reports.

.EXAMPLE
    .\scripts\run_unit_tests.ps1 -TestFile test_processor -Fast
    Runs only test_processor.py without coverage, quickly.
#>

param(
    [switch]$Fast,
    [string]$TestFile,
    [string]$Test,
    [switch]$Verbose,
    [switch]$NoClean
)

$ErrorActionPreference = "Stop"

# Project paths
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ReportsDir = Join-Path (Join-Path $ProjectRoot "reports") "unit"

# Colors for output
$Green = "`e[32m"
$Red = "`e[31m"
$Yellow = "`e[33m"
$Blue = "`e[34m"
$Reset = "`e[0m"

function Write-Status {
    param([string]$Message, [string]$Color = $Green)
    Write-Host "$Color[TEST]$Reset $Message"
}

function Write-Error {
    param([string]$Message)
    Write-Host "$Red[ERROR]$Reset $Message"
}

function Write-Info {
    param([string]$Message)
    Write-Host "$Blue[INFO]$Reset $Message"
}

# =============================================================================
# Setup
# =============================================================================

Write-Status "Starting unit test run..."
Write-Info "Project root: $ProjectRoot"

# Check Python
$PythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $PythonCmd) {
    Write-Error "Python not found. Please install Python 3.10+"
    exit 1
}

$PythonVersion = & python --version 2>&1
Write-Info "Using: $PythonVersion"

# Check dependencies
Write-Status "Checking dependencies..."
$MissingDeps = @()

$deps = @("pytest", "pytest-cov", "pytest-html", "coverage")
foreach ($dep in $deps) {
    $result = & python -c "import $dep" 2>&1
    if ($LASTEXITCODE -ne 0) {
        $MissingDeps += $dep
    }
}

if ($MissingDeps.Count -gt 0) {
    Write-Error "Missing dependencies: $($MissingDeps -join ', ')"
    Write-Info "Install with: pip install -r requirements-dev.txt"
    exit 1
}
Write-Status "All dependencies OK"

# Clean/create reports directory
if (-not $NoClean -and (Test-Path $ReportsDir)) {
    Write-Status "Cleaning old reports..."
    Remove-Item -Path $ReportsDir -Recurse -Force
}

if (-not (Test-Path $ReportsDir)) {
    New-Item -ItemType Directory -Path $ReportsDir -Force | Out-Null
}

# =============================================================================
# Build pytest command
# =============================================================================

$PytestArgs = @(
    "-m", "pytest",
    "-c", (Join-Path $ProjectRoot "pytest.ini")
)

# Verbosity
if ($Verbose) {
    $PytestArgs += "-vv"
} else {
    $PytestArgs += "-v"
}

# Test selection
if ($Test) {
    $PytestArgs += "-k", $Test
    Write-Status "Running test: $Test"
} elseif ($TestFile) {
    $TestPath = Join-Path $ProjectRoot "tests" "unit" "$TestFile.py"
    if (-not (Test-Path $TestPath)) {
        Write-Error "Test file not found: $TestPath"
        exit 1
    }
    $PytestArgs += $TestPath
    Write-Status "Running test file: $TestFile.py"
} else {
    $PytestArgs += (Join-Path $ProjectRoot "tests" "unit")
    Write-Status "Running all unit tests"
}

# Coverage (unless Fast mode)
if (-not $Fast) {
    Write-Status "Coverage enabled (use -Fast to skip)"
    $PytestArgs += @(
        "--cov=app",
        "--cov-config=.coveragerc",
        "--cov-report=term-missing:skip-covered",
        "--cov-report=html:reports/unit/htmlcov",
        "--cov-report=xml:reports/unit/coverage.xml"
    )
} else {
    Write-Status "Fast mode: coverage disabled"
}

# HTML Report
$PytestArgs += @(
    "--html=reports/unit/report.html",
    "--self-contained-html"
)

# JUnit XML for CI
$PytestArgs += @(
    "--junitxml=reports/unit/junit.xml"
)

# =============================================================================
# Run tests
# =============================================================================

Write-Status "Running pytest..."
Write-Info "Command: python $($PytestArgs -join ' ')"

Set-Location $ProjectRoot

# Set PYTHONPATH
$env:PYTHONPATH = Join-Path $ProjectRoot "app"

# Run pytest
& python @PytestArgs
$ExitCode = $LASTEXITCODE

# =============================================================================
# Results
# =============================================================================

Write-Host ""
Write-Host "========================================"

if ($ExitCode -eq 0) {
    Write-Status "ALL TESTS PASSED!" $Green
} else {
    Write-Error "TESTS FAILED (exit code: $ExitCode)"
}

Write-Host "========================================"
Write-Host ""

# Report locations
Write-Info "Reports generated:"
Write-Host "  - HTML Report:     reports/unit/report.html"
if (-not $Fast) {
    Write-Host "  - Coverage HTML:   reports/unit/htmlcov/index.html"
    Write-Host "  - Coverage XML:    reports/unit/coverage.xml"
}
Write-Host "  - JUnit XML:       reports/unit/junit.xml"
Write-Host ""

# Show summary if coverage available
if (-not $Fast -and (Test-Path (Join-Path $ReportsDir "coverage.xml"))) {
    Write-Status "Coverage Summary:"
    $CovResult = & python -c "
import xml.etree.ElementTree as ET
tree = ET.parse('reports/unit/coverage.xml')
root = tree.getroot()
line_rate = float(root.get('line-rate', 0))
branch_rate = float(root.get('branch-rate', 0))
lines_valid = int(root.get('lines-valid', 0))
lines_covered = int(root.get('lines-covered', 0))
print(f'  Lines: {lines_covered}/{lines_valid} ({line_rate*100:.1f}%)')
print(f'  Branch: {branch_rate*100:.1f}%')
" 2>$null
    Write-Host $CovResult
}

exit $ExitCode
