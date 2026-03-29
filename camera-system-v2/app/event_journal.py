"""
Shared rotating text journal for all pipeline events.
Writes to /logs/events.log (configurable via EVENT_JOURNAL_DIR).

Format per line:
    TIMESTAMP  SERVICE  ACTION  field=value  field=value ...

Actions:
    DISCOVERED  - watcher: new file detected and inserted
    SENT        - processor: notification delivered to MAX
    FAILED      - processor: delivery failed (retry or quarantine)
    QUARANTINE  - processor: event moved to quarantine
    DELETED     - ftp_cleanup: file physically removed from disk
"""
from __future__ import annotations

import logging
import logging.handlers
import os

LOG_DIR = os.getenv("EVENT_JOURNAL_DIR", "/logs")
LOG_FILE = os.path.join(LOG_DIR, "events.log")
LOG_MAX_BYTES = int(os.getenv("EVENT_JOURNAL_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
LOG_BACKUP_COUNT = int(os.getenv("EVENT_JOURNAL_BACKUP_COUNT", "10"))

_logger: logging.Logger | None = None


def get_journal() -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("event_journal")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s\t%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)
    _logger = logger
    return logger
