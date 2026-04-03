import importlib
import sys
from dataclasses import dataclass
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
class Call:
    method: str
    url: str
    kwargs: dict


class FakeResponse:
    def __init__(self, status_code: int, *, json_data=None, headers=None, text=""):
        self.status_code = int(status_code)
        self._json_data = json_data
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json_data

    def raise_for_status(self):
        import requests

        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class DummyFile:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _ensure_app_on_path():
    here = Path(__file__).resolve()
    app_dir = here.parents[2] / "app"
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))


@pytest.fixture
def max_mod(monkeypatch):
    _ensure_app_on_path()

    # max_client reads env at import time
    monkeypatch.setenv("MAX_API_BASE", "https://api")
    monkeypatch.setenv("MAX_BOT_TOKEN", "TOKEN")
    monkeypatch.setenv("MAX_UPLOAD_URL_PATH", "/upload")
    monkeypatch.setenv("MAX_SEND_MESSAGE_PATH", "/messages")
    monkeypatch.setenv("REQUEST_TIMEOUT_SECONDS", "5")
    monkeypatch.setenv("MAX_RETRY_ATTEMPTS", "1")
    monkeypatch.setenv("MAX_RETRY_BASE_DELAY_SECONDS", "1")
    monkeypatch.setenv("MAX_RETRY_MAX_DELAY_SECONDS", "8")
    monkeypatch.setenv("MAX_AUTH_HEADER_STYLE", "raw")
    monkeypatch.setenv("MAX_MESSAGE_RECIPIENT_MODE", "chat_id")
    monkeypatch.setenv("MAX_DEBUG_LOG", "0")

    import event_journal

    journal = FakeJournal()
    monkeypatch.setattr(event_journal, "get_journal", lambda: journal)

    if "max_client" in sys.modules:
        del sys.modules["max_client"]
    max_client = importlib.import_module("max_client")

    # deterministic timing
    monkeypatch.setattr(max_client.time, "monotonic", lambda: 0.0)

    class FakeDateTime:
        @staticmethod
        def now(tz=None):
            from datetime import datetime, timezone

            return datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(max_client, "datetime", FakeDateTime)

    return max_client, journal


# -----------------
# 1) Test cases list (P0/P1) is in assistant message.
# -----------------


# -----------------
# P0
# -----------------

def test_p0_success_response_send_message_builds_request_correctly(max_mod, monkeypatch):
    max_client, _journal = max_mod

    calls: list[Call] = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        return FakeResponse(200, json_data={"ok": True})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda _s: (_ for _ in ()).throw(AssertionError("sleep called")))

    out, send_ms, message_sent_at = max_client.send_message_timed_debug("123", "hello")

    assert out == {"ok": True}
    assert send_ms == 0
    assert message_sent_at is not None

    assert len(calls) == 1
    c = calls[0]
    assert c.method == "POST"
    assert c.url == "https://api/messages"
    assert c.kwargs["timeout"] == 5

    # auth + content-type
    assert c.kwargs["headers"]["Authorization"] == "TOKEN"
    assert c.kwargs["headers"]["Content-Type"] == "application/json"

    # chat_id numeric -> int
    assert c.kwargs["params"] == {"chat_id": 123}

    # payload
    assert c.kwargs["json"] == {"text": "hello"}


def test_p0_timeout_network_error_is_retryable(max_mod, monkeypatch):
    max_client, _journal = max_mod

    import requests

    calls: list[Call] = []
    sleeps = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        if len(calls) == 1:
            raise requests.Timeout("timeout")
        return FakeResponse(200, json_data={"ok": True})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda s: sleeps.append(s))

    out, send_ms = max_client.send_message_timed("123", "hello")

    assert out == {"ok": True}
    assert send_ms == 0
    assert len(calls) == 2
    assert len(sleeps) == 1


def test_p0_5xx_is_retryable_then_success(max_mod, monkeypatch):
    max_client, _journal = max_mod

    calls: list[Call] = []
    sleeps = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        if len(calls) == 1:
            return FakeResponse(503, json_data={"error": "busy"}, headers={"Retry-After": "1"})
        return FakeResponse(200, json_data={"ok": True})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda s: sleeps.append(s))

    out, send_ms = max_client.send_message_timed("c", "hello")

    assert out == {"ok": True}
    assert send_ms == 0
    assert len(calls) == 2
    assert len(sleeps) == 1


def test_p0_4xx_is_non_retryable(max_mod, monkeypatch):
    max_client, _journal = max_mod

    calls: list[Call] = []
    slept = 0

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        return FakeResponse(400, json_data={"error": "bad"})

    monkeypatch.setattr(max_client.requests, "request", fake_request)

    def fake_sleep(_s):
        nonlocal slept
        slept += 1

    monkeypatch.setattr(max_client.time, "sleep", fake_sleep)

    with pytest.raises(Exception):
        max_client.send_message_timed("c", "hello")

    assert len(calls) == 1
    assert slept == 0


def test_p0_malformed_upload_response_is_not_success(max_mod, monkeypatch):
    max_client, _journal = max_mod

    # No real FS - patch builtins.open
    monkeypatch.setattr(max_client.os.path, "isfile", lambda _p: True)
    import builtins
    monkeypatch.setattr(builtins, "open", lambda *_a, **_k: DummyFile())
    monkeypatch.setattr(max_client.mimetypes, "guess_type", lambda _p: ("image/jpeg", None))

    calls: list[Call] = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        if len(calls) == 1:
            return FakeResponse(200, json_data={"url": "https://upload-url"})
        # Malformed: missing payload/token/photos
        return FakeResponse(200, json_data={"wat": 1})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda _s: (_ for _ in ()).throw(AssertionError("sleep called")))

    with pytest.raises(RuntimeError):
        max_client.upload_photo_timed_debug("/source/x.jpg")


# -----------------
# P1
# -----------------

def test_p1_header_auth_mode_variations(max_mod):
    max_client, _journal = max_mod

    max_client.MAX_BOT_TOKEN = "T"

    max_client.MAX_AUTH_HEADER_STYLE = "raw"
    assert max_client._headers_json()["Authorization"] == "T"

    max_client.MAX_AUTH_HEADER_STYLE = "bearer"
    assert max_client._headers_json()["Authorization"] == "Bearer T"

    # upload headers: no Content-Type
    h = max_client._headers_upload()
    assert "Content-Type" not in h


def test_p1_send_message_payload_assembly_photos_and_token(max_mod, monkeypatch):
    max_client, _journal = max_mod

    calls: list[Call] = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        return FakeResponse(200, json_data={"ok": True})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda _s: (_ for _ in ()).throw(AssertionError("sleep called")))

    out, _send_ms = max_client.send_message_timed(
        "u1",
        "hello",
        photos={"1": "ref"},
        token="tok",
    )

    assert out == {"ok": True}
    assert len(calls) == 1
    body = calls[0].kwargs["json"]
    assert body["text"] == "hello"
    assert body["attachments"][0]["type"] == "image"
    assert body["attachments"][0]["payload"] == {"photos": {"1": "ref"}, "token": "tok"}


def test_p1_send_message_attachment_payload_overrides_photos(max_mod, monkeypatch):
    max_client, _journal = max_mod

    calls: list[Call] = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        return FakeResponse(200, json_data={"ok": True})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda _s: (_ for _ in ()).throw(AssertionError("sleep called")))

    out, _send_ms = max_client.send_message_timed(
        "u1",
        "hello",
        photos={"1": "ref"},
        token="tok",
        attachment_payload={"photos": {"X": "Y"}},
    )

    assert out == {"ok": True}
    body = calls[0].kwargs["json"]
    assert body["attachments"][0]["payload"] == {"photos": {"X": "Y"}}


def test_p1_recipient_param_mode_user_id_and_string_preserved(max_mod, monkeypatch):
    max_client, _journal = max_mod

    max_client.MAX_MESSAGE_RECIPIENT_MODE = "user_id"

    calls: list[Call] = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        return FakeResponse(200, json_data={"ok": True})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda _s: (_ for _ in ()).throw(AssertionError("sleep called")))

    max_client.send_message("not-numeric", "hello")

    assert calls[0].kwargs["params"] == {"user_id": "not-numeric"}


def test_p1_upload_init_binary_and_legacy_paths(max_mod, monkeypatch):
    max_client, _journal = max_mod

    # No real FS - patch builtins.open
    monkeypatch.setattr(max_client.os.path, "isfile", lambda _p: True)
    import builtins
    monkeypatch.setattr(builtins, "open", lambda *_a, **_k: DummyFile())
    monkeypatch.setattr(max_client.mimetypes, "guess_type", lambda _p: ("image/jpeg", None))

    # 1) init returns url -> binary upload uses 'data'
    calls: list[Call] = []

    def fake_request(method, url, **kwargs):
        calls.append(Call(method=method, url=url, kwargs=kwargs))
        if len(calls) == 1:
            return FakeResponse(200, json_data={"url": "https://u"})
        return FakeResponse(200, json_data={"payload": {"photos": {"1": "r"}}})

    monkeypatch.setattr(max_client.requests, "request", fake_request)
    monkeypatch.setattr(max_client.time, "sleep", lambda _s: None)

    payload, init_ms, upload_ms, _t1, _t2 = max_client.upload_photo_timed_debug("/source/x.jpg")
    assert payload == {"photos": {"1": "r"}}
    assert init_ms == 0
    assert upload_ms == 0
    assert calls[0].url == "https://api/upload"
    assert "headers" in calls[0].kwargs and "Authorization" in calls[0].kwargs["headers"]
    assert calls[1].url == "https://u"
    assert "files" in calls[1].kwargs and "data" in calls[1].kwargs["files"]

    # 2) legacy: init has no url -> second call posts to base+upload and uses key 'file'
    calls2: list[Call] = []

    def fake_request2(method, url, **kwargs):
        calls2.append(Call(method=method, url=url, kwargs=kwargs))
        return FakeResponse(200, json_data={"photos": {"1": "r"}})

    monkeypatch.setattr(max_client.requests, "request", fake_request2)

    payload2, _init_ms2, _upload_ms2, _t3, _t4 = max_client.upload_photo_timed_debug("/source/x.jpg")
    assert payload2 == {"photos": {"1": "r"}}
    assert len(calls2) == 2
    assert calls2[0].url == "https://api/upload"
    assert calls2[1].url == "https://api/upload"
    assert "file" in calls2[1].kwargs["files"]
