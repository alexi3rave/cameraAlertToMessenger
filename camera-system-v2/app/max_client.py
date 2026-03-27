import os
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


def _headers():
    if MAX_AUTH_HEADER_STYLE == "bearer":
        auth = f"Bearer {MAX_BOT_TOKEN}"
    else:
        auth = MAX_BOT_TOKEN
    return {
        "Authorization": auth,
        "Content-Type": "application/json",
    }


def _recipient_params(chat_id: str):
    if not chat_id:
        return {}
    key = "user_id" if MAX_MESSAGE_RECIPIENT_MODE == "user_id" else "chat_id"
    s = str(chat_id).strip()
    if s.isdigit():
        return {key: int(s)}
    return {key: s}


def send_message(chat_id: str, text: str, photos=None, token=None):
    url = f"{MAX_API_BASE}{MAX_SEND_MESSAGE_PATH}"
    params = _recipient_params(chat_id)
    body = {"text": text}
    if photos and token:
        body["attachments"] = [
            {
                "type": "image",
                "payload": {"photos": photos, "token": token},
            }
        ]
    resp = requests.post(
        url,
        headers=_headers(),
        params=params,
        json=body,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()
