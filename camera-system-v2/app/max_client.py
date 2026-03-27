import os
import mimetypes
import requests

MAX_API_BASE = os.getenv("MAX_API_BASE", "").rstrip("/")
MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "")
MAX_UPLOAD_URL_PATH = os.getenv("MAX_UPLOAD_URL_PATH", "")
MAX_SEND_MESSAGE_PATH = os.getenv("MAX_SEND_MESSAGE_PATH", "")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))

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
    if not file_path or not os.path.isfile(file_path):
        raise FileNotFoundError(f"photo file not found: {file_path}")

    url = f"{MAX_API_BASE}{MAX_UPLOAD_URL_PATH}"
    mime, _ = mimetypes.guess_type(file_path)
    content_type = mime or "application/octet-stream"

    # New MAX flow: POST /uploads?type=image -> get upload URL -> upload file there.
    init_resp = requests.post(
        url,
        headers=_headers_upload(),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    init_resp.raise_for_status()
    init_data = init_resp.json()

    upload_url = init_data.get("url") if isinstance(init_data, dict) else None
    if upload_url:
        with open(file_path, "rb") as fh:
            upload_resp = requests.post(
                upload_url,
                files={"data": (os.path.basename(file_path), fh, content_type)},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        upload_resp.raise_for_status()
        return _extract_upload_payload(upload_resp.json())

    # Backward compatibility: legacy one-step upload endpoint.
    with open(file_path, "rb") as fh:
        legacy_resp = requests.post(
            url,
            headers=_headers_upload(),
            files={"file": (os.path.basename(file_path), fh, content_type)},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    legacy_resp.raise_for_status()
    return _extract_upload_payload(legacy_resp.json())


def send_message(chat_id: str, text: str, photos=None, token=None, attachment_payload=None):
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
    resp = requests.post(
        url,
        headers=_headers_json(),
        params=params,
        json=body,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


def send_message_with_photo(chat_id: str, text: str, file_path: str):
    uploaded_payload = upload_photo(file_path)
    return send_message(
        chat_id,
        text,
        attachment_payload=uploaded_payload,
    )
