import importlib
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest


def _ensure_app_on_path():
    here = Path(__file__).resolve()
    app_dir = here.parents[2] / "app"
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))


@dataclass
class FakeCursor:
    row: tuple | None
    executed: list

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self, row):
        self._row = row
        self.executed = []

    def cursor(self):
        return FakeCursor(self._row, self.executed)


@pytest.fixture
def rp(monkeypatch):
    _ensure_app_on_path()

    if "retry_policy" in sys.modules:
        del sys.modules["retry_policy"]
    mod = importlib.import_module("retry_policy")

    # Ensure no accidental real env use: tests explicitly monkeypatch mod.os.getenv.
    monkeypatch.setattr(mod.os, "getenv", lambda _k, _d=None: _d)

    return mod


# -----------------
# Case list
# 1) DB policy wins
# 2) env fallback works
# 3) defaults fallback works
# 4) partial config merges correctly
# 5) invalid/empty config does not break logic
# -----------------


def test_p0_db_policy_wins_over_env(rp, monkeypatch):
    # DB has full policy
    conn = FakeConn(({"max_attempts": 9, "schedule_seconds": [1, 2, 3]},))

    # env has different values; must be ignored when DB provides that field
    def fake_getenv(key, default=None):
        if key == "RETRY_MAX_ATTEMPTS":
            return "2"
        if key == "RETRY_BACKOFF_SECONDS":
            return "10,20"
        return default

    monkeypatch.setattr(rp.os, "getenv", fake_getenv)

    pol = rp.fetch_retry_policy(conn)

    assert pol.max_attempts == 9
    assert pol.schedule_seconds == (1, 2, 3)


def test_p0_env_fallback_works_when_db_missing(rp, monkeypatch):
    conn = FakeConn(None)

    def fake_getenv(key, default=None):
        if key == "RETRY_MAX_ATTEMPTS":
            return "7"
        if key == "RETRY_BACKOFF_SECONDS":
            return "5,10"
        return default

    monkeypatch.setattr(rp.os, "getenv", fake_getenv)

    pol = rp.fetch_retry_policy(conn)

    assert pol.max_attempts == 7
    assert pol.schedule_seconds == (5, 10)


def test_p0_defaults_fallback_when_db_and_env_missing(rp, monkeypatch):
    conn = FakeConn(None)

    # return default for env keys -> safe defaults must apply
    def fake_getenv(_key, default=None):
        return default

    monkeypatch.setattr(rp.os, "getenv", fake_getenv)

    pol = rp.fetch_retry_policy(conn)

    assert pol.max_attempts == rp.DEFAULT_MAX_ATTEMPTS
    assert pol.schedule_seconds == rp.DEFAULT_BACKOFF_SECONDS


def test_p1_partial_db_config_merges_with_env(rp, monkeypatch):
    # DB only overrides max_attempts
    conn = FakeConn(({"max_attempts": 11},))

    def fake_getenv(key, default=None):
        if key == "RETRY_MAX_ATTEMPTS":
            return "4"
        if key == "RETRY_BACKOFF_SECONDS":
            return "9,18"
        return default

    monkeypatch.setattr(rp.os, "getenv", fake_getenv)

    pol = rp.fetch_retry_policy(conn)

    assert pol.max_attempts == 11
    assert pol.schedule_seconds == (9, 18)


def test_p1_invalid_or_empty_config_is_safe(rp, monkeypatch):
    # invalid JSON in DB should not break; should fall back to env/defaults
    conn = FakeConn(("not json",))

    def fake_getenv(key, default=None):
        if key == "RETRY_MAX_ATTEMPTS":
            return "not-an-int"
        if key == "RETRY_BACKOFF_SECONDS":
            return "x, ,y"
        return default

    monkeypatch.setattr(rp.os, "getenv", fake_getenv)

    pol = rp.fetch_retry_policy(conn)

    # invalid env max -> defaults
    assert pol.max_attempts == rp.DEFAULT_MAX_ATTEMPTS
    # invalid schedule -> defaults
    assert pol.schedule_seconds == rp.DEFAULT_BACKOFF_SECONDS
