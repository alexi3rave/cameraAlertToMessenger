# QA Runbook: Unit Testing for Camera System V2

This document describes how to run unit tests, interpret results, and maintain test quality.

---

## Quick Start

### Run All Tests (Recommended)

**Windows:**
```powershell
.\scripts\run_unit_tests.ps1
```

**Linux/macOS:**
```bash
./scripts/run_unit_tests.sh
```

### Run Without Coverage (Fast)

**Windows:** `\scripts\run_unit_tests.ps1 -Fast`
**Linux/macOS:** `./scripts/run_unit_tests.sh --fast`

---

## Command Reference

### Run All Tests
```bash
# Full test run with coverage and all reports
pytest tests/unit -v --cov=app --cov-report=term-missing --html=reports/unit/report.html
```

### Run Single Test File
```bash
# Windows
.\scripts\run_unit_tests.ps1 -TestFile test_ftp_cleanup_worker

# Linux/macOS
./scripts/run_unit_tests.sh --file test_ftp_cleanup_worker

# Direct pytest
pytest tests/unit/test_ftp_cleanup_worker.py -v
```

### Run Single Test
```bash
# Windows
.\scripts\run_unit_tests.ps1 -Test "TestPathIsUnderRoot.test_path_inside_root_allowed"

# Linux/macOS
./scripts/run_unit_tests.sh --test "TestPathIsUnderRoot.test_path_inside_root_allowed"

# Direct pytest
pytest tests/unit/test_ftp_cleanup_worker.py -v -k "test_path_inside_root_allowed"
```

### Run with Verbose Output
```bash
.\scripts\run_unit_tests.ps1 -Verbose
./scripts/run_unit_tests.sh --verbose
```

---

## Test Output Interpretation

### Console Output

**Success Example:**
```
========================================
[TEST] ALL TESTS PASSED!
========================================

[INFO] Reports generated:
  - HTML Report:     reports/unit/report.html
  - Coverage HTML:   reports/unit/htmlcov/index.html
  - Coverage XML:    reports/unit/coverage.xml
  - JUnit XML:       reports/unit/junit.xml

[TEST] Coverage Summary:
  Lines: 450/500 (90.0%)
  Branch: 85.0%
```

**Failure Example:**
```
========================================
[ERROR] TESTS FAILED (exit code: 1)
========================================

FAILED test_ftp_cleanup_worker.py::TestPathIsUnderRoot::test_path_inside_root_allowed
AssertionError: assert False is True
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All tests passed |
| 1 | One or more tests failed |
| 2 | Test execution error (e.g., import error) |
| 3 | Coverage threshold not met |
| 4 | Missing dependencies |

---

## Generated Reports

After running tests, the following reports are generated in `reports/unit/`:

### 1. HTML Test Report
**Location:** `reports/unit/report.html`

- Shows all test results with pass/fail status
- Includes test duration
- Contains captured stdout/stderr for each test
- **Open in browser:** Double-click or use `start reports/unit/report.html` (Windows)

### 2. Coverage HTML Report
**Location:** `reports/unit/htmlcov/index.html`

- Line-by-line coverage visualization
- Green = covered, Red = not covered, Yellow = partial branch
- **Use for:** Finding uncovered code paths
- **Open in browser:** Same as above

### 3. Coverage XML
**Location:** `reports/unit/coverage.xml`

- Machine-readable coverage data
- Used by CI/CD pipelines
- Standard Cobertura format

### 4. JUnit XML
**Location:** `reports/unit/junit.xml`

- Test results in JUnit format
- Used by Jenkins, GitLab CI, etc.
- Contains test durations and failure details

---

## Coverage Interpretation

### Understanding Coverage Metrics

**Line Coverage:**
- Percentage of executable lines that were run during tests
- Target: >80% for new code, >70% for legacy

**Branch Coverage:**
- Percentage of decision branches (if/else, etc.) that were executed
- Higher quality metric than line coverage
- Target: >75%

**Missing Lines:**
- Shown as `Missing: 45, 67-69, 120` in terminal output
- These line numbers need test coverage

### Example Coverage Output
```
Name                        Stmts   Miss Branch BrPart  Cover   Missing
-----------------------------------------------------------------------
app/ftp_cleanup_worker.py   120     12     40      8    87%   45, 67-69, 120, 145-150
```

Interpretation:
- 120 total statements, 12 missed = 90% statement coverage
- 40 branches, 8 partial = 80% branch coverage
- Overall: 87%
- Uncovered lines: 45, 67-69, 120, 145-150

---

## What "Everything OK" Looks Like

### Terminal Output
```
[TEST] Starting unit test run...
[INFO] Project root: C:\Users\user\Documents\repo-cameras\camera-system-v2
[INFO] Using: Python 3.11.4
[TEST] All dependencies OK
[TEST] Running all unit tests
[TEST] Coverage enabled (use -Fast to skip)
...
tests/unit/test_ftp_cleanup_worker.py::TestPathIsUnderRoot::test_path_inside_root_allowed PASSED
tests/unit/test_ftp_cleanup_worker.py::TestPathIsUnderRoot::test_path_outside_root_not_allowed PASSED
...

========================================
[TEST] ALL TESTS PASSED!
========================================

[INFO] Reports generated:
  - HTML Report:     reports/unit/report.html
  - Coverage HTML:   reports/unit/htmlcov/index.html
  - Coverage XML:    reports/unit/coverage.xml
  - JUnit XML:       reports/unit/junit.xml

[TEST] Coverage Summary:
  Lines: 450/500 (90.0%)
  Branch: 85.0%
```

### File Structure After Run
```
reports/
└── unit/
    ├── junit.xml          # CI test results
    ├── coverage.xml       # CI coverage data
    ├── report.html        # Human-readable test report
    └── htmlcov/           # Line-by-line coverage
        ├── index.html
        ├── style.css
        └── *.html
```

---

## Troubleshooting

### Missing Dependencies
```
[ERROR] Missing dependencies: pytest, pytest-cov
[INFO] Install with: pip install -r requirements-dev.txt
```
**Fix:** `pip install -r requirements-dev.txt`

### Import Errors
```
ModuleNotFoundError: No module named 'ftp_cleanup_worker'
```
**Fix:** Ensure `PYTHONPATH` includes `app/` directory. The run scripts set this automatically.

### Permission Denied (Linux/macOS)
```
bash: ./scripts/run_unit_tests.sh: Permission denied
```
**Fix:** `chmod +x scripts/run_unit_tests.sh`

### Coverage Not Generated
```
[TEST] Fast mode: coverage disabled
```
**Fix:** Run without `-Fast` / `--fast` flag.

---

## Adding New Tests

### 1. Create Test File
```bash
touch tests/unit/test_<module>.py
```

### 2. Basic Structure
```python
"""Tests for <module>.py"""
import unittest
from unittest.mock import Mock, patch

class TestMyFeature(unittest.TestCase):
    def test_something_works(self):
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()
```

### 3. Run New Test
```bash
pytest tests/unit/test_<module>.py -v
```

---

## CI Integration

### GitHub Actions Example
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements-dev.txt
      - run: ./scripts/run_unit_tests.sh
      - uses: actions/upload-artifact@v3
        with:
          name: test-reports
          path: reports/unit/
```

---

## Maintenance

### Regular Tasks
- **Weekly:** Review coverage report for gaps
- **Before release:** Run full test suite
- **After bug fix:** Add regression test

### Coverage Thresholds
Edit `.coveragerc` to set minimum coverage:
```ini
[report]
fail_under = 70
```

### Test Performance
- Fast tests: < 100ms each
- Slow tests: Mark with `@pytest.mark.slow`
- Skip slow tests: `pytest -m "not slow"`
