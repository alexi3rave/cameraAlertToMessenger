"""
Shared pytest fixtures and configuration for camera-system-v2 unit tests.
"""
from __future__ import annotations

import os
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import pytest

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../app"))


# =============================================================================
# Path Fixtures
# =============================================================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    temp_path = tempfile.mkdtemp(prefix="camera_test_")
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def temp_file(temp_dir):
    """Create a temporary file that is cleaned up after the test."""
    fd, path = tempfile.mkstemp(dir=temp_dir)
    os.close(fd)
    yield path


@pytest.fixture
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def app_dir(project_root):
    """Return the app directory path."""
    return project_root / "app"


# =============================================================================
# Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_db_connection():
    """Provide a mock database connection."""
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value = cursor
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    return conn


@pytest.fixture
def mock_cursor(mock_db_connection):
    """Provide the cursor from mock_db_connection."""
    return mock_db_connection.cursor.return_value


@pytest.fixture
def mock_logger():
    """Provide a mock logger."""
    logger = Mock()
    logger.info = Mock()
    logger.error = Mock()
    logger.warning = Mock()
    logger.debug = Mock()
    return logger


# =============================================================================
# Environment Fixtures
# =============================================================================

@pytest.fixture
def clean_env():
    """Provide a clean environment with test-specific variables."""
    old_env = os.environ.copy()
    
    # Set test defaults
    os.environ["FTP_CLEANUP_ROOT"] = "/tmp/test_ftp"
    os.environ["FTP_RETENTION_DAYS"] = "7"
    os.environ["FTP_QUARANTINE_RETENTION_DAYS"] = "3"
    os.environ["FTP_CLEANUP_INTERVAL_SEC"] = "1"
    os.environ["FTP_CLEANUP_BATCH"] = "10"
    os.environ["FTP_CLEANUP_LOG_DIR"] = "/tmp/test_logs"
    
    yield os.environ
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(old_env)


@pytest.fixture(autouse=True)
def reset_modules():
    """Reset imported modules after each test to allow env variable changes."""
    # Store modules before test
    modules_before = set(sys.modules.keys())
    
    yield
    
    # Remove any new modules that were imported during the test
    # (helps with modules that read env vars at import time)
    camera_modules = [k for k in sys.modules.keys() if 'camera' in k.lower()]
    for mod in camera_modules:
        if mod not in modules_before:
            del sys.modules[mod]


# =============================================================================
# Time Fixtures
# =============================================================================

@pytest.fixture
def fixed_time():
    """Provide a fixed timestamp for deterministic tests."""
    return 1704067200  # 2024-01-01 00:00:00 UTC


# =============================================================================
# Data Fixtures
# =============================================================================

@pytest.fixture
def sample_event_row():
    """Provide a sample event database row."""
    return (
        1,                          # id
        "/source/cam01/file.jpg",   # full_path
        "file.jpg",                 # file_name
        "cam01",                    # camera_code
        "2024-01-01 10:00:00",      # first_seen_at
        "sent",                     # status
    )


@pytest.fixture
def sample_terminal_statuses():
    """Return the list of terminal statuses for cleanup."""
    return ("sent", "quarantine")


@pytest.fixture
def sample_non_terminal_statuses():
    """Return the list of non-terminal statuses."""
    return ("ready", "processing")
