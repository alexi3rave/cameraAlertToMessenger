import importlib
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest


class FakeJournal:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, msg):
        self.infos.append(msg)

    def error(self, msg):
        self.errors.append(msg)


class FakeCursor:
    def __init__(self, executions):
        self._executions = executions

    def execute(self, sql, params=None):
        self._executions.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self):
        self.closed = False
        self.executions = []
        self.rollback_called = 0

    def cursor(self):
        return FakeCursor(self.executions)

    def rollback(self):
        self.rollback_called += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeDateTime:
    ftp_written_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    detected_at = datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc)

    @staticmethod
    def fromtimestamp(ts, tz=None):
        return FakeDateTime.ftp_written_at

    @staticmethod
    def now(tz=None):
        return FakeDateTime.detected_at


@dataclass
class StatSeq:
    values: list
    idx: int = 0

    def __call__(self, _path):
        v = self.values[self.idx]
        self.idx += 1
        return v


def _ensure_app_on_path():
    here = Path(__file__).resolve()
    app_dir = here.parents[2] / "app"
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))


@pytest.fixture
def watcher_mod(monkeypatch):
    _ensure_app_on_path()

    # watcher.py reads these env vars at import time
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_HOST", "test")

    import event_journal

    journal = FakeJournal()
    monkeypatch.setattr(event_journal, "get_journal", lambda: journal)

    if "watcher" in sys.modules:
        del sys.modules["watcher"]
    watcher = importlib.import_module("watcher")

    # deterministic + no real sleeps
    watcher.WATCH_PATH = "/source"
    watcher.WATCH_LOOP_SLEEP_SECONDS = 0
    watcher.FILE_STABILIZE_SECONDS = 0
    watcher.WATCH_USE_MTIME_CURSOR = False
    watcher.WATCH_MIN_FILE_SIZE_BYTES = 1

    # Make path logic portable across OS (CI may run on Linux)
    monkeypatch.setattr(watcher.os, "sep", "/", raising=False)
    monkeypatch.setattr(watcher.os.path, "join", lambda a, b: f"{a}/{b}")
    monkeypatch.setattr(
        watcher.os.path,
        "relpath",
        lambda full_path, base: full_path[len(base) + 1 :]
        if full_path.startswith(base + "/")
        else full_path,
    )

    watcher._journal = journal
    watcher.seen_keys.clear()
    watcher.seen_names.clear()

    # hard-disable any accidental sleeps
    monkeypatch.setattr(watcher.time, "sleep", lambda _s: (_ for _ in ()).throw(AssertionError("sleep called")))

    return watcher, journal


# -----------------
# P0 test cases
# -----------------

def test_duplicate_file_skipped_by_seen_names(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    cam = watcher.detect_camera_code(f)
    watcher.seen_names.add((cam, f))

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(watcher.WATCH_PATH + "/siteA", [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([123]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([900.0]))

    watcher.main(max_loops=1)

    assert fake_conn.executions == []
    assert journal.infos == []
    assert journal.errors == []


def test_duplicate_file_skipped_by_seen_keys(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([2048, 2048]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([990.0, 990.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)
    monkeypatch.setattr(watcher, "datetime", FakeDateTime)

    watcher.WATCH_CHECKSUM_MODE = "skip"

    cam = watcher.detect_camera_code(f)
    dupe_key = watcher.make_event_key_no_checksum(cam, f, 2048, 990.0)
    watcher.seen_keys.add(dupe_key)

    watcher.main(max_loops=1)

    assert fake_conn.executions == []
    assert journal.infos == []
    assert journal.errors == []


def test_new_file_not_duplicate_inserted_and_marked_seen(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"
    full_path = root + "/" + f

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([2048, 2048]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([990.0, 990.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)
    monkeypatch.setattr(watcher, "datetime", FakeDateTime)

    watcher.WATCH_CHECKSUM_MODE = "sha256"
    monkeypatch.setattr(watcher, "file_sha256", lambda _p: "abc" * 21 + "d")

    watcher.main(max_loops=1)

    cam = watcher.detect_camera_code(f)
    assert (cam, f) in watcher.seen_names

    expected_key = watcher.make_event_key(cam, f, 2048, ("abc" * 21 + "d"))
    assert expected_key in watcher.seen_keys

    assert len(journal.infos) == 1
    msg = journal.infos[0]
    assert "watcher\tDISCOVERED" in msg
    assert f"\tfile={f}" in msg
    assert "\tsite=sitea" in msg
    assert f"\tpath={full_path}" in msg
    assert "\tstatus=ready" in msg
    assert "\treason=-" in msg


def test_old_file_goes_to_quarantine(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([2048, 2048]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([0.0, 0.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)
    monkeypatch.setattr(watcher, "datetime", FakeDateTime)

    watcher.MAX_FILE_AGE_SECONDS = 10
    watcher.WATCH_CHECKSUM_MODE = "skip"  # avoid sha stub; deterministic event_key via mtime

    watcher.main(max_loops=1)

    assert len(journal.infos) == 1
    msg = journal.infos[0]
    assert "\tstatus=quarantine" in msg
    assert "\treason=too_old" in msg


def test_unstable_file_skipped_when_size_changes(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])

    # unstable: size changed between size1 and size2
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([100, 101]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([900.0, 900.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)

    watcher.main(max_loops=1)

    assert fake_conn.executions == []
    assert journal.infos == []
    assert watcher.seen_keys == set()
    assert watcher.seen_names == set()


def test_unstable_file_skipped_when_mtime_changes(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])

    # unstable: mtime changed between mtime1 and mtime2
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([100, 100]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([900.0, 901.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)

    watcher.main(max_loops=1)

    assert fake_conn.executions == []
    assert journal.infos == []
    assert watcher.seen_keys == set()
    assert watcher.seen_names == set()


def test_parse_camera_and_site_from_name_and_path(watcher_mod):
    watcher, _journal = watcher_mod

    assert watcher.detect_camera_code("poe4_00_20260402234022.jpg") == "poe4"
    assert watcher.detect_camera_code("POE4_00_20260402234022.JPG") == "poe4"
    assert watcher.detect_camera_code("single.jpg") == "single"

    assert watcher.extract_site_from_path("/source/siteA/poe4_00.jpg") == "sitea"
    assert watcher.extract_site_from_path("/source/siteA/nested/poe4_00.jpg") == "nested"


# -----------------
# P1 test cases
# -----------------

def test_checksum_mode_skip_stores_null_checksum_and_uses_no_checksum_key(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([2048, 2048]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([990.9, 990.9]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)
    monkeypatch.setattr(watcher, "datetime", FakeDateTime)

    watcher.WATCH_CHECKSUM_MODE = "skip"

    watcher.main(max_loops=1)

    assert len(journal.infos) == 1

    cam = watcher.detect_camera_code(f)
    expected_key = watcher.make_event_key_no_checksum(cam, f, 2048, 990.9)
    assert expected_key in watcher.seen_keys

    # Outcome-level DB payload: checksum param is None
    insert_params = fake_conn.executions[0][1]
    assert expected_key in insert_params
    assert None in insert_params


def test_boundary_min_size_equal_is_accepted(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    watcher.WATCH_MIN_FILE_SIZE_BYTES = 100

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([100, 100]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([990.0, 990.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)
    monkeypatch.setattr(watcher, "datetime", FakeDateTime)

    watcher.WATCH_CHECKSUM_MODE = "skip"

    watcher.main(max_loops=1)

    assert len(journal.infos) == 1
    assert "\tstatus=ready" in journal.infos[0]


def test_boundary_age_equal_not_quarantine(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    watcher.MAX_FILE_AGE_SECONDS = 10

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    fake_conn = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: fake_conn)

    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([2048, 2048]))

    # loop_started_at - mtime2 == MAX_FILE_AGE_SECONDS => NOT too_old because condition is '>'
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([990.0, 990.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)
    monkeypatch.setattr(watcher, "datetime", FakeDateTime)

    watcher.WATCH_CHECKSUM_MODE = "skip"

    watcher.main(max_loops=1)

    assert len(journal.infos) == 1
    assert "\tstatus=ready" in journal.infos[0]


def test_protects_against_false_duplicates_when_mtime_changes_same_name(watcher_mod, monkeypatch):
    watcher, journal = watcher_mod

    f = "poe4_00_20260402234022.jpg"
    root = watcher.WATCH_PATH + "/siteA"

    watcher.WATCH_CHECKSUM_MODE = "skip"

    # First scan: accept
    conn1 = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: conn1)
    monkeypatch.setattr(watcher.os, "walk", lambda _p: [(root, [], [f])])
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([2048, 2048]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([990.0, 990.0]))
    monkeypatch.setattr(watcher.time, "time", lambda: 1000.0)
    monkeypatch.setattr(watcher, "datetime", FakeDateTime)

    watcher.main(max_loops=1)
    assert len(journal.infos) == 1

    # Second scan: same camera+file name but mtime touched -> should be skipped via seen_names
    before = len(journal.infos)
    conn2 = FakeConn()
    monkeypatch.setattr(watcher, "make_conn", lambda: conn2)
    monkeypatch.setattr(watcher.os.path, "getsize", StatSeq([2048]))
    monkeypatch.setattr(watcher.os.path, "getmtime", StatSeq([995.0]))

    watcher.main(max_loops=1)

    assert len(journal.infos) == before
    assert conn2.executions == []
