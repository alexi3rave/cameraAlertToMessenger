#!/bin/bash
#
# Run unit tests for camera-system-v2 with coverage and reporting.
#
# USAGE:
#   # Run all tests with full reporting
#   ./scripts/run_unit_tests.sh
#
#   # Run tests without coverage (fast)
#   ./scripts/run_unit_tests.sh --fast
#
#   # Run specific test file
#   ./scripts/run_unit_tests.sh --file test_ftp_cleanup_worker
#
#   # Run specific test
#   ./scripts/run_unit_tests.sh --test "TestPathIsUnderRoot.test_path_inside_root_allowed"
#
#   # Run with verbose output
#   ./scripts/run_unit_tests.sh --verbose
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPORTS_DIR="$PROJECT_ROOT/reports/unit"

# Defaults
FAST=false
TEST_FILE=""
TEST=""
VERBOSE=false
NO_CLEAN=false

# =============================================================================
# Parse arguments
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --fast)
            FAST=true
            shift
            ;;
        --file)
            TEST_FILE="$2"
            shift 2
            ;;
        --test)
            TEST="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --no-clean)
            NO_CLEAN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --fast          Skip coverage for faster execution"
            echo "  --file FILE     Run only specific test file (e.g., test_processor)"
            echo "  --test TEST     Run only specific test (e.g., TestClass.test_method)"
            echo "  --verbose       Enable verbose output"
            echo "  --no-clean      Don't clean reports directory"
            echo "  -h, --help      Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# =============================================================================
# Helper functions
# =============================================================================

log_status() {
    echo -e "${GREEN}[TEST]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# =============================================================================
# Setup
# =============================================================================

log_status "Starting unit test run..."
log_info "Project root: $PROJECT_ROOT"

# Check Python
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        log_error "Python not found. Please install Python 3.10+"
        exit 1
    fi
    PYTHON=python
else
    PYTHON=python3
fi

PYTHON_VERSION=$($PYTHON --version 2>&1)
log_info "Using: $PYTHON_VERSION"

# Check dependencies
log_status "Checking dependencies..."
MISSING_DEPS=()

for dep in pytest pytest-cov pytest-html coverage; do
    if ! $PYTHON -c "import $dep" 2>/dev/null; then
        MISSING_DEPS+=("$dep")
    fi
done

if [ ${#MISSING_DEPS[@]} -gt 0 ]; then
    log_error "Missing dependencies: ${MISSING_DEPS[*]}"
    log_info "Install with: pip install -r requirements-dev.txt"
    exit 1
fi
log_status "All dependencies OK"

# Clean/create reports directory
if [ "$NO_CLEAN" = false ] && [ -d "$REPORTS_DIR" ]; then
    log_status "Cleaning old reports..."
    rm -rf "$REPORTS_DIR"
fi

mkdir -p "$REPORTS_DIR"

# =============================================================================
# Build pytest command
# =============================================================================

PYTEST_ARGS=(
    -c "$PROJECT_ROOT/pytest.ini"
)

# Verbosity
if [ "$VERBOSE" = true ]; then
    PYTEST_ARGS+=(-vv)
else
    PYTEST_ARGS+=(-v)
fi

# Test selection
if [ -n "$TEST" ]; then
    PYTEST_ARGS+=(-k "$TEST")
    log_status "Running test: $TEST"
elif [ -n "$TEST_FILE" ]; then
    TEST_PATH="$PROJECT_ROOT/tests/unit/${TEST_FILE}.py"
    if [ ! -f "$TEST_PATH" ]; then
        log_error "Test file not found: $TEST_PATH"
        exit 1
    fi
    PYTEST_ARGS+=("$TEST_PATH")
    log_status "Running test file: $TEST_FILE.py"
else
    PYTEST_ARGS+=("$PROJECT_ROOT/tests/unit")
    log_status "Running all unit tests"
fi

# Coverage (unless Fast mode)
if [ "$FAST" = false ]; then
    log_status "Coverage enabled (use --fast to skip)"
    PYTEST_ARGS+=(
        --cov=app
        --cov-config=.coveragerc
        --cov-report=term-missing:skip-covered
        --cov-report="html:reports/unit/htmlcov"
        --cov-report="xml:reports/unit/coverage.xml"
    )
else
    log_status "Fast mode: coverage disabled"
fi

# HTML Report
PYTEST_ARGS+=(
    --html="reports/unit/report.html"
    --self-contained-html
)

# JUnit XML for CI
PYTEST_ARGS+=(
    --junitxml="reports/unit/junit.xml"
)

# =============================================================================
# Run tests
# =============================================================================

log_status "Running pytest..."
log_info "Command: $PYTHON ${PYTEST_ARGS[*]}"

cd "$PROJECT_ROOT"

# Set PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT/app"

# Run pytest
set +e
$PYTHON -m pytest "${PYTEST_ARGS[@]}"
EXIT_CODE=$?
set -e

# =============================================================================
# Results
# =============================================================================

echo ""
echo "========================================"

if [ $EXIT_CODE -eq 0 ]; then
    log_status "ALL TESTS PASSED!"
else
    log_error "TESTS FAILED (exit code: $EXIT_CODE)"
fi

echo "========================================"
echo ""

# Report locations
log_info "Reports generated:"
echo "  - HTML Report:     reports/unit/report.html"
if [ "$FAST" = false ]; then
    echo "  - Coverage HTML:   reports/unit/htmlcov/index.html"
    echo "  - Coverage XML:    reports/unit/coverage.xml"
fi
echo "  - JUnit XML:       reports/unit/junit.xml"
echo ""

# Show coverage summary if available
if [ "$FAST" = false ] && [ -f "$REPORTS_DIR/coverage.xml" ]; then
    log_status "Coverage Summary:"
    $PYTHON -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('$REPORTS_DIR/coverage.xml')
    root = tree.getroot()
    line_rate = float(root.get('line-rate', 0))
    branch_rate = float(root.get('branch-rate', 0))
    lines_valid = int(root.get('lines-valid', 0))
    lines_covered = int(root.get('lines-covered', 0))
    print(f'  Lines: {lines_covered}/{lines_valid} ({line_rate*100:.1f}%)')
    print(f'  Branch: {branch_rate*100:.1f}%')
except Exception as e:
    print(f'  Error reading coverage: {e}')
" 2>/dev/null || true
fi

exit $EXIT_CODE
