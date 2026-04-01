import os
import mimetypes
import time
from datetime import datetime, timezone
import requests

MAX_API_BASE = os.getenv("MAX_API_BASE", "").rstrip("/")
MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "")
MAX_UPLOAD_URL_PATH = os.getenv("MAX_UPLOAD_URL_PATH", "")
MAX_SEND_MESSAGE_PATH = os.getenv("MAX_SEND_MESSAGE_PATH", "")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
MAX_RETRY_BASE_DELAY_SECONDS = float(os.getenv("MAX_RETRY_BASE_DELAY_SECONDS", "1"))
MAX_RETRY_MAX_DELAY_SECONDS = float(os.getenv("MAX_RETRY_MAX_DELAY_SECONDS", "8"))

# Документация MAX: https://dev.max.ru/docs-api/methods/POST/messages
# Authorization: токен целиком, без префикса "Bearer " (если нужен Bearer — MAX_AUTH_HEADER_STYLE=bearer)
MAX_AUTH_HEADER_STYLE = os.getenv("MAX_AUTH_HEADER_STYLE", "raw").strip().lower()

# chat_id | user_id — в query (официальный пример), не в теле JSON
MAX_MESSAGE_RECIPIENT_MODE = os.getenv("MAX_MESSAGE_RECIPIENT_MODE", "chat_id").strip().lower()


def is_configured():
    return all([MAX_API_BASE, MAX_BOT_TOKEN, MAX_UPLOAD_URL_PATH, MAX_SEND_MESSAGE_PATH])


def _auth_value():
    if MAX_AUTH_HEADER_STYLE == "bearer":
        return f"Bearer {MAX_BOT_TOKEN}"
    return MAX_BOT_TOKEN


def _headers_json():
    return {
        "Authorization": _auth_value(),
        "Content-Type": "application/json",
    }


def _headers_upload():
    # Для multipart/form-data не задаём Content-Type вручную.
    return {"Authorization": _auth_value()}


def _is_retryable_status(status_code: int) -> bool:
    return status_code in (429, 500, 502, 503, 504)


def _retry_after_seconds(resp) -> float:
    if resp is None:
        return 0.0
    raw = resp.headers.get("Retry-After")
    if not raw:
        return 0.0
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(v, MAX_RETRY_MAX_DELAY_SECONDS))


def _backoff_seconds(attempt_no: int, resp=None) -> float:
    retry_after = _retry_after_seconds(resp)
    if retry_after > 0:
        return retry_after
    delay = MAX_RETRY_BASE_DELAY_SECONDS * (2 ** max(0, attempt_no - 1))
    return min(delay, MAX_RETRY_MAX_DELAY_SECONDS)


def _request_with_retry(method: str, url: str, **kwargs):
    attempts = max(0, MAX_RETRY_ATTEMPTS)
    for attempt in range(0, attempts + 1):
        try:
            resp = requests.request(method, url, **kwargs)
        except requests.RequestException:
            if attempt >= attempts:
                raise
            time.sleep(_backoff_seconds(attempt + 1))
            continue

        if _is_retryable_status(resp.status_code):
            if attempt >= attempts:
                resp.raise_for_status()
            time.sleep(_backoff_seconds(attempt + 1, resp))
            continue

        resp.raise_for_status()
        return resp

    raise RuntimeError("MAX request retry loop exited unexpectedly")


def _recipient_params(chat_id: str):
    if not chat_id:
        return {}
    key = "user_id" if MAX_MESSAGE_RECIPIENT_MODE == "user_id" else "chat_id"
    s = str(chat_id).strip()
    if s.isdigit():
        return {key: int(s)}
    return {key: s}


def _extract_upload_payload(data):
    payload = data.get("payload") if isinstance(data, dict) else None
    if isinstance(payload, dict):
        return payload

    if isinstance(data, dict):
        token = data.get("token")
        photos = data.get("photos")
        if token and photos is not None:
            return {"token": token, "photos": photos}

        # Current MAX image upload may return only photos map, without top-level token.
        if isinstance(photos, dict) and photos:
            return {"photos": photos}

    raise RuntimeError(f"MAX upload unexpected response: {data}")


def upload_photo(file_path: str):
    payload, _, _ = upload_photo_timed(file_path)
    return payload


def _upload_photo_timed_internal(file_path: str):
    if not file_path or not os.path.isfile(file_path):
        raise FileNotFoundError(f"photo file not found: {file_path}")

    url = f"{MAX_API_BASE}{MAX_UPLOAD_URL_PATH}"
    mime, _ = mimetypes.guess_type(file_path)
    content_type = mime or "application/octet-stream"

    init_started = time.monotonic()
    init_resp = _request_with_retry(
        "POST",
        url,
        headers=_headers_upload(),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    init_finished = time.monotonic()
    init_ms = int((init_finished - init_started) * 1000)
    upload_init_done_at = datetime.now(timezone.utc)
    init_data = init_resp.json()

    upload_url = init_data.get("url") if isinstance(init_data, dict) else None
    if upload_url:
        upload_started = time.monotonic()
        with open(file_path, "rb") as fh:
            upload_resp = _request_with_retry(
                "POST",
                upload_url,
                files={"data": (os.path.basename(file_path), fh, content_type)},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        upload_finished = time.monotonic()
        upload_ms = int((upload_finished - upload_started) * 1000)
        upload_done_at = datetime.now(timezone.utc)
        return _extract_upload_payload(upload_resp.json()), init_ms, upload_ms, upload_init_done_at, upload_done_at

    upload_started = time.monotonic()
    with open(file_path, "rb") as fh:
        legacy_resp = _request_with_retry(
            "POST",
            url,
            headers=_headers_upload(),
            files={"file": (os.path.basename(file_path), fh, content_type)},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    upload_finished = time.monotonic()
    upload_ms = int((upload_finished - upload_started) * 1000)
    upload_done_at = datetime.now(timezone.utc)
    return _extract_upload_payload(legacy_resp.json()), init_ms, upload_ms, upload_init_done_at, upload_done_at


def upload_photo_timed(file_path: str):
    payload, init_ms, upload_ms, _, _ = _upload_photo_timed_internal(file_path)
    return payload, init_ms, upload_ms


def upload_photo_timed_debug(file_path: str):
    return _upload_photo_timed_internal(file_path)


def send_message(chat_id: str, text: str, photos=None, token=None, attachment_payload=None):
    out, _ = send_message_timed(
        chat_id,
        text,
        photos=photos,
        token=token,
        attachment_payload=attachment_payload,
    )
    return out


def _send_message_timed_internal(chat_id: str, text: str, photos=None, token=None, attachment_payload=None):
    url = f"{MAX_API_BASE}{MAX_SEND_MESSAGE_PATH}"
    params = _recipient_params(chat_id)
    body = {"text": text}

    payload = attachment_payload
    if payload is None and photos is not None:
        payload = {"photos": photos}
        if token:
            payload["token"] = token

    if payload:
        body["attachments"] = [
            {
                "type": "image",
                "payload": payload,
            }
        ]
    send_started = time.monotonic()
    resp = _request_with_retry(
        "POST",
        url,
        headers=_headers_json(),
        params=params,
        json=body,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    send_finished = time.monotonic()
    send_ms = int((send_finished - send_started) * 1000)
    message_sent_at = datetime.now(timezone.utc)
    return resp.json(), send_ms, message_sent_at


def send_message_timed(chat_id: str, text: str, photos=None, token=None, attachment_payload=None):
    out, send_ms, _ = _send_message_timed_internal(
        chat_id,
        text,
        photos=photos,
        token=token,
        attachment_payload=attachment_payload,
    )
    return out, send_ms


def send_message_timed_debug(chat_id: str, text: str, photos=None, token=None, attachment_payload=None):
    out, send_ms, message_sent_at = _send_message_timed_internal(
        chat_id,
        text,
        photos=photos,
        token=token,
        attachment_payload=attachment_payload,
    )
    return out, send_ms, message_sent_at


def send_message_with_photo(chat_id: str, text: str, file_path: str):
    out, _, _, _ = send_message_with_photo_timed(chat_id, text, file_path)
    return out


def send_message_with_photo_timed(chat_id: str, text: str, file_path: str):
    uploaded_payload, upload_init_ms, binary_upload_ms = upload_photo_timed(file_path)
    out, send_message_ms = send_message_timed(
        chat_id,
        text,
        attachment_payload=uploaded_payload,
    )
    return out, upload_init_ms, binary_upload_ms, send_message_ms


def send_message_with_photo_timed_debug(chat_id: str, text: str, file_path: str):
    uploaded_payload, upload_init_ms, binary_upload_ms, upload_init_done_at, upload_done_at = (
        upload_photo_timed_debug(file_path)
    )
    out, send_message_ms, message_sent_at = send_message_timed_debug(
        chat_id,
        text,
        attachment_payload=uploaded_payload,
    )
    return (
        out,
        upload_init_ms,
        binary_upload_ms,
        send_message_ms,
        upload_init_done_at,
        upload_done_at,
        message_sent_at,
    )
