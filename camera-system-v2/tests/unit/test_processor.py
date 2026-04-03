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


@dataclass
class FakePolicy:
    max_attempts: int = 3
    schedule_seconds: list[int] = None


@dataclass
class EventState:
    status: str | None = None
    last_error: str | None = None
    quarantine_reason: str | None = None
    recipient_id: int | None = None
    attempt_count: int = 0
    delivery_status: str | None = None
    marked_sent: bool = False
    sent_chat_id: str | None = None
    sent_text: str | None = None
    uploaded: bool = False


class FakeConn:
    def __init__(self, *, state: EventState):
        self.state = state
        self.closed = False
        self.autocommit = False

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        self.closed = True
        return None


class ConnQueue:
    def __init__(self, conns):
        self._conns = list(conns)

    def __call__(self):
        if not self._conns:
            raise AssertionError("db() called more times than expected")
        return self._conns.pop(0)


def _ensure_app_on_path():
    here = Path(__file__).resolve()
    app_dir = here.parents[2] / "app"
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))


@pytest.fixture
def processor_mod(monkeypatch):
    _ensure_app_on_path()

    import event_journal

    journal = FakeJournal()
    monkeypatch.setattr(event_journal, "get_journal", lambda: journal)

    if "processor" in sys.modules:
        del sys.modules["processor"]
    processor = importlib.import_module("processor")

    # prevent any real sleeps
    monkeypatch.setattr(
        processor.time,
        "sleep",
        lambda _s: (_ for _ in ()).throw(AssertionError("sleep called")),
    )

    # deterministic time/clock
    monkeypatch.setattr(processor.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(processor.time, "time", lambda: 1_000.0)

    class FakeDateTime:
        @staticmethod
        def now(tz=None):
            return datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(processor, "datetime", FakeDateTime)

    # journal override
    processor._journal = journal

    return processor, journal


def _run_one(processor, *, monkeypatch, state: EventState, row: dict):
    """Run processor.main() exactly for one picked event and then stop on idle sleep.

    We stop the infinite loop by:
    - returning `row` on first fetch_one_ready
    - returning None on second fetch_one_ready
    - setting PROCESSOR_IDLE_SLEEP_SECONDS > 0
    - monkeypatching time.sleep to raise SystemExit
    """

    it = iter([row, None])
    monkeypatch.setattr(processor, "fetch_one_ready", lambda _c: next(it))
    processor.PROCESSOR_IDLE_SLEEP_SECONDS = 1
    monkeypatch.setattr(processor.time, "sleep", lambda _s: (_ for _ in ()).throw(SystemExit("stop")))

    main_conn = FakeConn(state=state)
    idle_conn = FakeConn(state=state)
    monkeypatch.setattr(processor, "db", ConnQueue([main_conn, idle_conn]))

    with pytest.raises(SystemExit):
        processor.main()


def _base_row(*, recipient_id=1, max_chat_id="chat", file_name="poe4_00.jpg"):
    return {
        "id": "evt-1",
        "camera_id": 10,
        "camera_code": "poe4",
        "file_name": file_name,
        "full_path": "/source/siteA/" + file_name,
        "file_size": 2048,
        "first_seen_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "ftp_written_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "recipient_id": recipient_id,
        "max_chat_id": max_chat_id,
    }


# -----------------
# Scenario list (P0/P1) is in the assistant message.
# Tests below.
# -----------------


def test_p0_route_found_direct_prod_success_sent(processor_mod, monkeypatch):
    processor, journal = processor_mod

    state = EventState(status="ready")

    row = _base_row(recipient_id=111, max_chat_id="chat-direct")

    # DB funcs become pure state transitions
    monkeypatch.setattr(
        processor,
        "set_status",
        lambda _c, _eid, status, error_text=None, recipient_id=None, quarantine_reason=None, **kw: (
            setattr(state, "status", status),
            setattr(state, "last_error", error_text),
            setattr(state, "recipient_id", recipient_id),
            setattr(state, "quarantine_reason", quarantine_reason),
            setattr(state, "attempt_count", state.attempt_count + (1 if kw.get("increment_attempt") else 0)),
        ),
    )
    monkeypatch.setattr(processor, "insert_delivery", lambda *_a, **_k: 1)
    monkeypatch.setattr(processor, "mark_delivery_started", lambda *_a, **_k: None)
    monkeypatch.setattr(
        processor,
        "finish_delivery",
        lambda _c, _eid, _attempt_no, status, err: setattr(state, "delivery_status", status),
    )
    monkeypatch.setattr(
        processor,
        "mark_sent",
        lambda _c, _eid: (setattr(state, "status", "sent"), setattr(state, "marked_sent", True)),
    )

    # routing-related guardrails
    processor.SHADOW_MODE = "prod"

    # configured + preflight OK
    monkeypatch.setattr(processor, "is_configured", lambda: True)
    monkeypatch.setattr(processor, "PROCESSOR_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(processor, "_preflight_file_ok", lambda **_k: (True, None, None))

    # MAX client success
    def fake_upload(path):
        state.uploaded = True
        return ("payload", 1, 2, datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc), datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc))

    def fake_send(chat_id, text, attachment_payload=None, **_k):
        state.sent_chat_id = chat_id
        state.sent_text = text
        return ({"ok": True}, 3, datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc))

    monkeypatch.setattr(processor, "upload_photo_timed_debug", fake_upload)
    monkeypatch.setattr(processor, "send_message_timed_debug", fake_send)
    monkeypatch.setattr(processor, "send_message", lambda *_a, **_k: None)

    monkeypatch.setattr(processor, "fetch_retry_policy", lambda _c: FakePolicy(max_attempts=3, schedule_seconds=[1, 2, 3]))

    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "sent"
    assert state.marked_sent is True
    assert state.sent_chat_id == "chat-direct"
    assert state.uploaded is True
    assert len(journal.infos) >= 1


def test_p0_fallback_route_prod_success_sent(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    state = EventState(status="ready")

    # fallback simulated by different recipient_id/chat_id
    row = _base_row(recipient_id=222, max_chat_id="chat-fallback")

    monkeypatch.setattr(processor, "set_status", lambda *_a, **_k: setattr(state, "status", _a[2]))
    monkeypatch.setattr(processor, "insert_delivery", lambda *_a, **_k: 1)
    monkeypatch.setattr(processor, "mark_delivery_started", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "finish_delivery", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "mark_sent", lambda *_a, **_k: setattr(state, "status", "sent"))

    processor.SHADOW_MODE = "prod"
    monkeypatch.setattr(processor, "is_configured", lambda: True)
    monkeypatch.setattr(processor, "PROCESSOR_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(processor, "_preflight_file_ok", lambda **_k: (True, None, None))

    monkeypatch.setattr(processor, "upload_photo_timed_debug", lambda _p: ("payload", 1, 2, None, None))

    def fake_send(chat_id, text, attachment_payload=None, **_k):
        state.sent_chat_id = chat_id
        return ({"ok": True}, 3, None)

    monkeypatch.setattr(processor, "send_message_timed_debug", fake_send)
    monkeypatch.setattr(processor, "send_message", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "fetch_retry_policy", lambda _c: FakePolicy(max_attempts=3, schedule_seconds=[1, 2, 3]))

    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "sent"
    assert state.sent_chat_id == "chat-fallback"


def test_p0_no_route_quarantine(processor_mod, monkeypatch):
    processor, journal = processor_mod

    state = EventState(status="ready")

    row = _base_row(recipient_id=None, max_chat_id=None)

    def fake_set_status(_c, _eid, status, error_text=None, recipient_id=None, quarantine_reason=None, **_k):
        state.status = status
        state.last_error = error_text
        state.quarantine_reason = quarantine_reason
        state.recipient_id = recipient_id

    monkeypatch.setattr(processor, "set_status", fake_set_status)

    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "quarantine"
    assert state.quarantine_reason == "route_not_found"
    assert any("processor\tQUARANTINE" in m and "reason=route_not_found" in m for m in journal.infos)


def test_p0_success_shadow_mode_sent(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    state = EventState(status="ready")

    row = _base_row(recipient_id=1, max_chat_id="ignored-in-shadow")

    # state transitions
    monkeypatch.setattr(processor, "set_status", lambda *_a, **_k: setattr(state, "status", _a[2]))
    monkeypatch.setattr(processor, "insert_delivery", lambda *_a, **_k: 1)
    monkeypatch.setattr(processor, "mark_delivery_started", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "finish_delivery", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "mark_sent", lambda *_a, **_k: setattr(state, "status", "sent"))

    processor.SHADOW_MODE = "shadow"
    processor.SHADOW_TEST_CHAT_ID = "shadow-chat"

    monkeypatch.setattr(processor, "is_configured", lambda: True)
    monkeypatch.setattr(processor, "PROCESSOR_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(processor, "_preflight_file_ok", lambda **_k: (True, None, None))
    monkeypatch.setattr(processor, "upload_photo_timed_debug", lambda _p: ("payload", 1, 2, None, None))

    def fake_send(chat_id, text, attachment_payload=None, **_k):
        state.sent_chat_id = chat_id
        state.sent_text = text
        return ({"ok": True}, 3, None)

    monkeypatch.setattr(processor, "send_message_timed_debug", fake_send)
    monkeypatch.setattr(processor, "send_message", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "fetch_retry_policy", lambda _c: FakePolicy(max_attempts=3, schedule_seconds=[1, 2, 3]))

    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "sent"
    assert state.sent_chat_id == "shadow-chat"


def test_p0_retryable_error_preflight_failed_goes_failed_retryable(processor_mod, monkeypatch):
    processor, journal = processor_mod

    state = EventState(status="ready")

    row = _base_row(recipient_id=1, max_chat_id="chat")

    monkeypatch.setattr(processor, "set_status", lambda *_a, **_k: setattr(state, "status", _a[2]))
    monkeypatch.setattr(processor, "insert_delivery", lambda *_a, **_k: 1)
    monkeypatch.setattr(processor, "mark_delivery_started", lambda *_a, **_k: None)

    def fake_finish(_c, _eid, _attempt, status, err):
        state.delivery_status = status
        state.last_error = err

    monkeypatch.setattr(processor, "finish_delivery", fake_finish)

    def fake_apply_failure(_c, _eid, _rid, err, policy):
        state.status = "failed_retryable"
        state.last_error = err
        return "retry"

    monkeypatch.setattr(processor, "apply_delivery_failure", fake_apply_failure)
    monkeypatch.setattr(processor, "fetch_retry_policy", lambda _c: FakePolicy(max_attempts=3, schedule_seconds=[1, 2, 3]))

    processor.SHADOW_MODE = "shadow"
    processor.SHADOW_TEST_CHAT_ID = "shadow-chat"
    monkeypatch.setattr(processor, "is_configured", lambda: True)
    monkeypatch.setattr(processor, "PROCESSOR_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(processor, "_preflight_file_ok", lambda **_k: (False, "jpeg_incomplete", {"x": 1}))

    # ensure no false sent
    monkeypatch.setattr(processor, "mark_sent", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("mark_sent called")))
    monkeypatch.setattr(processor, "upload_photo_timed_debug", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("upload called")))
    monkeypatch.setattr(processor, "send_message_timed_debug", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("send called")))

    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "failed_retryable"
    assert state.last_error.startswith("preflight:")
    assert any("processor\tFAILED" in m and "reason=preflight" in m for m in journal.errors)


def test_p0_fatal_error_prod_missing_chat_id_quarantine(processor_mod, monkeypatch):
    processor, journal = processor_mod

    state = EventState(status="ready")

    row = _base_row(recipient_id=1, max_chat_id=None)

    def fake_set_status(_c, _eid, status, error_text=None, recipient_id=None, quarantine_reason=None, **_k):
        state.status = status
        state.last_error = error_text
        state.quarantine_reason = quarantine_reason

    monkeypatch.setattr(processor, "set_status", fake_set_status)

    processor.SHADOW_MODE = "prod"
    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "quarantine"
    assert state.quarantine_reason == "recipient_missing_chat_id"
    assert any("processor\tQUARANTINE" in m and "reason=recipient_missing_chat_id" in m for m in journal.infos)


def test_p0_dry_run_marks_sent_without_max_calls(processor_mod, monkeypatch):
    processor, journal = processor_mod

    state = EventState(status="ready")

    row = _base_row(recipient_id=1, max_chat_id="chat")

    monkeypatch.setattr(processor, "set_status", lambda *_a, **_k: setattr(state, "status", _a[2]))
    monkeypatch.setattr(processor, "insert_delivery", lambda *_a, **_k: 1)
    monkeypatch.setattr(processor, "mark_delivery_started", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "finish_delivery", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "mark_sent", lambda *_a, **_k: setattr(state, "status", "sent"))

    processor.SHADOW_MODE = "dry_run"

    # ensure no MAX calls
    monkeypatch.setattr(processor, "upload_photo_timed_debug", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("upload called")))
    monkeypatch.setattr(processor, "send_message_timed_debug", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("send called")))

    monkeypatch.setattr(processor, "fetch_retry_policy", lambda _c: FakePolicy(max_attempts=3, schedule_seconds=[1, 2, 3]))

    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "sent"
    assert any("mode=dry_run" in m for m in journal.infos)


def test_p0_txt_test_file_flow_text_only_no_upload(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    state = EventState(status="ready")

    row = _base_row(recipient_id=1, max_chat_id="chat", file_name="healthcheck.txt")

    monkeypatch.setattr(processor, "set_status", lambda *_a, **_k: setattr(state, "status", _a[2]))
    monkeypatch.setattr(processor, "insert_delivery", lambda *_a, **_k: 1)
    monkeypatch.setattr(processor, "mark_delivery_started", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "finish_delivery", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "mark_sent", lambda *_a, **_k: setattr(state, "status", "sent"))

    processor.SHADOW_MODE = "prod"
    monkeypatch.setattr(processor, "is_configured", lambda: True)

    # no upload for test events
    monkeypatch.setattr(processor, "upload_photo_timed_debug", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("upload called")))

    def fake_send(chat_id, text, **_k):
        state.sent_chat_id = chat_id
        state.sent_text = text
        return ({"ok": True}, 1, None)

    monkeypatch.setattr(processor, "send_message_timed_debug", fake_send)
    monkeypatch.setattr(processor, "send_message", lambda *_a, **_k: None)
    monkeypatch.setattr(processor, "fetch_retry_policy", lambda _c: FakePolicy(max_attempts=3, schedule_seconds=[1, 2, 3]))

    _run_one(processor, monkeypatch=monkeypatch, state=state, row=row)

    assert state.status == "sent"
    assert state.sent_text is not None
    assert "Test event received" in state.sent_text


# -----------------
# P1: message construction + preflight boundary cases
# -----------------

def test_p1_build_text_branches_include_end_to_end_when_message_sent_at_present(processor_mod):
    processor, _journal = processor_mod

    detected_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ftp_mtime = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    message_sent_at = datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc)

    txt = processor.build_text(
        "poe4",
        detected_at,
        ftp_mtime,
        detected_at,
        None,
        None,
        None,
        message_sent_at,
        None,
    )

    assert "message_sent_at" in txt
    assert "end_to_end_ms" in txt


def test_p1_build_text_without_message_sent_at_is_short(processor_mod):
    processor, _journal = processor_mod

    detected_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ftp_mtime = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    txt = processor.build_text(
        "poe4",
        detected_at,
        ftp_mtime,
        detected_at,
        None,
        None,
        None,
        None,
        None,
    )

    assert "message_sent_at" not in txt
    assert "end_to_end_ms" not in txt


def test_p1_build_test_text_contains_camera_and_time(processor_mod):
    processor, _journal = processor_mod

    first_seen = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    out = processor.build_test_text("poe4", first_seen)
    assert "Test event received" in out
    assert "Камера: poe4" in out


def test_p1_preflight_too_small_boundary_and_ok(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    processor.PROCESSOR_PREFLIGHT_MIN_SIZE_BYTES = 100
    processor.PROCESSOR_PREFLIGHT_STABILIZE_SECONDS = 0
    processor.PROCESSOR_PREFLIGHT_DECODE_VERIFY = False

    monkeypatch.setattr(processor.os.path, "exists", lambda _p: True)

    # too small
    monkeypatch.setattr(processor.os.path, "getsize", lambda _p: 99)
    ok, reason, _details = processor._preflight_file_ok(
        full_path="/source/x.jpg", file_name="x.jpg", file_size_db=99
    )
    assert ok is False
    assert reason.startswith("file_too_small")

    # boundary ok (100)
    monkeypatch.setattr(processor.os.path, "getsize", lambda _p: 100)
    monkeypatch.setattr(processor, "_read_head_tail", lambda _p, _n=2: (100, b"\xff\xd8", b"\xff\xd9"))
    ok, reason, _details = processor._preflight_file_ok(
        full_path="/source/x.jpg", file_name="x.jpg", file_size_db=100
    )
    assert ok is True
    assert reason is None


def test_p1_preflight_invalid_jpeg_incomplete(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    processor.PROCESSOR_PREFLIGHT_MIN_SIZE_BYTES = 1
    processor.PROCESSOR_PREFLIGHT_STABILIZE_SECONDS = 0
    processor.PROCESSOR_PREFLIGHT_DECODE_VERIFY = False

    monkeypatch.setattr(processor.os.path, "exists", lambda _p: True)
    monkeypatch.setattr(processor.os.path, "getsize", lambda _p: 100)
    monkeypatch.setattr(processor, "_read_head_tail", lambda _p, _n=2: (100, b"\xff\xd8", b"\x00\x00"))

    ok, reason, details = processor._preflight_file_ok(
        full_path="/source/x.jpg", file_name="x.jpg", file_size_db=100
    )
    assert ok is False
    assert reason == "jpeg_incomplete"
    assert details["eoi"] is False


def test_p1_preflight_size_mismatch_fails(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    processor.PROCESSOR_PREFLIGHT_MIN_SIZE_BYTES = 1
    processor.PROCESSOR_PREFLIGHT_STABILIZE_SECONDS = 0
    processor.PROCESSOR_PREFLIGHT_DECODE_VERIFY = False

    monkeypatch.setattr(processor.os.path, "exists", lambda _p: True)
    monkeypatch.setattr(processor.os.path, "getsize", lambda _p: 101)

    ok, reason, details = processor._preflight_file_ok(
        full_path="/source/x.jpg", file_name="x.jpg", file_size_db=100
    )
    assert ok is False
    assert reason.startswith("file_size_mismatch")
    assert details["size_db"] == 100


def test_p1_preflight_decode_verify_failure(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    processor.PROCESSOR_PREFLIGHT_MIN_SIZE_BYTES = 1
    processor.PROCESSOR_PREFLIGHT_STABILIZE_SECONDS = 0
    processor.PROCESSOR_PREFLIGHT_DECODE_VERIFY = True

    monkeypatch.setattr(processor.os.path, "exists", lambda _p: True)
    monkeypatch.setattr(processor.os.path, "getsize", lambda _p: 100)
    monkeypatch.setattr(processor, "_read_head_tail", lambda _p, _n=2: (100, b"\xff\xd8", b"\xff\xd9"))

    class _FakeImage:
        def __enter__(self):
            raise OSError("bad decode")

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakePIL:
        @staticmethod
        def open(_path):
            return _FakeImage()

    # Intercept `from PIL import Image` inside _preflight_file_ok
    import builtins

    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "PIL" and "Image" in fromlist:
            return type("PILModule", (), {"Image": _FakePIL})
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    ok, reason, details = processor._preflight_file_ok(
        full_path="/source/x.jpg", file_name="x.jpg", file_size_db=100
    )
    assert ok is False
    assert reason == "jpeg_decode_verify_failed"
    assert "bad decode" in details["error"]


def test_p0_retryable_max_send_exception_records_failed_retryable_and_not_sent(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    state = EventState(status="ready")
    row = _base_row(recipient_id=1, max_chat_id="chat")

    # Normal start-of-processing DB ops
    monkeypatch.setattr(processor, "set_status", lambda *_a, **_k: setattr(state, "status", _a[2]))
    monkeypatch.setattr(processor, "insert_delivery", lambda *_a, **_k: 1)
    monkeypatch.setattr(processor, "mark_delivery_started", lambda *_a, **_k: None)

    # Preflight ok; upload ok; send fails
    processor.SHADOW_MODE = "prod"
    monkeypatch.setattr(processor, "is_configured", lambda: True)
    monkeypatch.setattr(processor, "PROCESSOR_PREFLIGHT_ENABLED", True)
    monkeypatch.setattr(processor, "_preflight_file_ok", lambda **_k: (True, None, None))
    monkeypatch.setattr(processor, "upload_photo_timed_debug", lambda _p: ("payload", 1, 2, None, None))

    def boom_send(*_a, **_k):
        raise RuntimeError("max_send_failed")

    monkeypatch.setattr(processor, "send_message_timed_debug", boom_send)
    monkeypatch.setattr(processor, "send_message", lambda *_a, **_k: None)

    # Exception path opens a second connection and calls record_exception_failure
    def fake_record_exception_failure(_c, _eid, _rid, err, _pol):
        state.status = "failed_retryable"
        state.last_error = err

    monkeypatch.setattr(processor, "record_exception_failure", fake_record_exception_failure)
    monkeypatch.setattr(processor, "fetch_retry_policy", lambda _c: FakePolicy(max_attempts=3, schedule_seconds=[1, 2, 3]))

    # db() is called: main loop conn, then exception conn, then idle conn
    main_conn = FakeConn(state=state)
    exc_conn = FakeConn(state=state)
    idle_conn = FakeConn(state=state)
    monkeypatch.setattr(processor, "db", ConnQueue([main_conn, exc_conn, idle_conn]))

    it = iter([row, None])
    monkeypatch.setattr(processor, "fetch_one_ready", lambda _c: next(it))
    processor.PROCESSOR_IDLE_SLEEP_SECONDS = 1
    monkeypatch.setattr(processor.time, "sleep", lambda _s: (_ for _ in ()).throw(SystemExit("stop")))

    with pytest.raises(SystemExit):
        processor.main()

    assert state.status == "failed_retryable"
    assert "max_send_failed" in (state.last_error or "")
    assert state.marked_sent is False


def test_p1_preflight_unstable_when_size_db_missing_and_file_growing(processor_mod, monkeypatch):
    processor, _journal = processor_mod

    processor.PROCESSOR_PREFLIGHT_MIN_SIZE_BYTES = 1
    processor.PROCESSOR_PREFLIGHT_STABILIZE_SECONDS = 0.1
    processor.PROCESSOR_PREFLIGHT_DECODE_VERIFY = False

    monkeypatch.setattr(processor.os.path, "exists", lambda _p: True)

    sizes = [100, 101]

    def fake_getsize(_p):
        return sizes.pop(0)

    monkeypatch.setattr(processor.os.path, "getsize", fake_getsize)

    # allow the stabilize sleep in preflight without actually sleeping
    monkeypatch.setattr(processor.time, "sleep", lambda _s: None)

    ok, reason, details = processor._preflight_file_ok(
        full_path="/source/x.jpg", file_name="x.jpg", file_size_db=None
    )
    assert ok is False
    assert reason.startswith("file_still_growing")
    assert details["size_db"] is None
