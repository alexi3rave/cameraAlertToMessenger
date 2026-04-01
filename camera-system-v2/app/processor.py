import os
import re
import time
import uuid
import socket
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import psycopg2
from psycopg2.extras import RealDictCursor
import event_journal

from max_client import (
    is_configured,
    send_message,
    send_message_timed,
    send_message_with_photo,
    send_message_with_photo_timed,
)
from retry_policy import delay_before_next_retry, fetch_retry_policy

DB_NAME = os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "camera_v2"))
DB_USER = os.getenv("POSTGRES_USER", os.getenv("DB_USER", "camera"))
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", ""))
DB_HOST = os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "postgres"))
DB_PORT = int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")))

SHADOW_MODE = os.getenv("SHADOW_MODE", "dry_run").strip().lower()
SHADOW_TEST_CHAT_ID = os.getenv("SHADOW_TEST_CHAT_ID", "").strip()
PIPELINE_SCHEMA_MODE = os.getenv("PIPELINE_SCHEMA_MODE", "legacy").strip().lower()
PROCESSOR_IDLE_SLEEP_SECONDS = float(os.getenv("PROCESSOR_IDLE_SLEEP_SECONDS", "2"))
PROCESSOR_ERROR_SLEEP_SECONDS = float(os.getenv("PROCESSOR_ERROR_SLEEP_SECONDS", "5"))
_journal = event_journal.get_journal()
EVENT_DISPLAY_TZ = ZoneInfo("Europe/Moscow")
WORKER_ID = os.getenv("PROCESSOR_WORKER_ID", f"processor-{socket.gethostname()}")
TEST_FILE_REGEX = re.compile(
    os.getenv("TEST_FILE_REGEX", r"(^test_|_test_|healthcheck|probe)"),
    re.IGNORECASE,
)


def db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def get_ftp_write_time_utc(file_path):
    try:
        mtime = os.path.getmtime(file_path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc)
    except OSError:
        return None


def _as_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_moscow(dt):
    dt = _as_utc(dt)
    if dt is None:
        return "unknown"
    return dt.astimezone(EVENT_DISPLAY_TZ).strftime("%Y-%m-%d %H:%M:%S MSK")


def build_text(camera_code, first_seen_at, ftp_write_time_utc):
    lines = ["🚨 Camera event", f"📷 Камера: {camera_code}"]

    first_seen_utc = _as_utc(first_seen_at)
    ftp_write_utc = _as_utc(ftp_write_time_utc)
    if first_seen_utc and ftp_write_utc:
        delta_sec = abs((first_seen_utc - ftp_write_utc).total_seconds())
        if delta_sec <= 1:
            lines.append(f"🕒 Время события: {_fmt_moscow(first_seen_utc)}")
        else:
            lines.append(f"🕒 Обнаружено системой: {_fmt_moscow(first_seen_utc)}")
            lines.append(f"🗂 Время записи на FTP: {_fmt_moscow(ftp_write_utc)}")
    elif first_seen_utc:
        lines.append(f"🕒 Обнаружено системой: {_fmt_moscow(first_seen_utc)}")
    else:
        lines.append(f"🗂 Время записи на FTP: {_fmt_moscow(ftp_write_utc)}")

    return "\n".join(lines)


def is_test_file(file_name):
    return bool(file_name and TEST_FILE_REGEX.search(file_name))


def build_test_text(camera_code, first_seen_at):
    lines = [
        "✅ Test event received",
        f"📷 Камера: {camera_code}",
        f"🕒 Время получения: {_fmt_moscow(first_seen_at)}",
    ]
    return "\n".join(lines)


def fetch_one_ready(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH picked AS (
                SELECT e.id
                FROM events e
                WHERE e.status = 'ready'
                ORDER BY e.first_seen_at ASC NULLS FIRST, e.id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            SELECT
                e.id,
                e.camera_id,
                e.camera_code,
                e.file_name,
                e.full_path,
                e.first_seen_at,
                COALESCE(route_rec.id, r_site.id, r_tenant.id) AS recipient_id,
                COALESCE(
                    route_rec.max_chat_id,
                    r_site.max_chat_id,
                    r_tenant.max_chat_id
                ) AS max_chat_id
            FROM picked p
            JOIN events e ON e.id = p.id
            LEFT JOIN cameras c ON c.id = e.camera_id
            LEFT JOIN sites s ON s.id = c.site_id
            LEFT JOIN tenants t ON t.id = c.tenant_id
            LEFT JOIN LATERAL (
                SELECT r.id, r.max_chat_id
                FROM camera_routes cr
                JOIN recipients r
                    ON r.id = cr.recipient_id AND r.is_active = true
                   AND r.tenant_id = t.id
                WHERE cr.camera_id = e.camera_id AND cr.is_active = true
                ORDER BY cr.priority ASC, cr.id ASC
                LIMIT 1
            ) route_rec ON true
            LEFT JOIN recipients r_site
                ON r_site.id = s.default_recipient_id AND r_site.is_active = true
               AND r_site.tenant_id = t.id
            LEFT JOIN recipients r_tenant
                ON r_tenant.id = t.default_recipient_id AND r_tenant.is_active = true
               AND r_tenant.tenant_id = t.id
            """
        )
        return cur.fetchone()


def set_status(
    conn,
    event_id,
    status,
    error_text=None,
    recipient_id=None,
    quarantine_reason=None,
    *,
    increment_attempt: bool = False,
):
    with conn.cursor() as cur:
        if increment_attempt:
            cur.execute(
                """
                UPDATE events
                SET status = %s,
                    last_error = %s,
                    recipient_id = coalesce(%s, recipient_id),
                    last_attempt_at = now(),
                    attempt_count = coalesce(attempt_count, 0) + 1,
                    quarantine_reason = coalesce(%s, quarantine_reason),
                    next_retry_at = NULL,
                    locked_at = CASE WHEN %s = 'processing' THEN now() ELSE NULL END,
                    lock_owner = CASE WHEN %s = 'processing' THEN %s ELSE NULL END
                WHERE id = %s
                """,
                (
                    status,
                    error_text,
                    recipient_id,
                    quarantine_reason,
                    status,
                    status,
                    WORKER_ID,
                    event_id,
                ),
            )
            cur.execute(
                """
                UPDATE photo_events
                SET status = %s,
                    last_error = %s,
                    last_attempt_at = now(),
                    attempt_count = coalesce(attempt_count, 0) + 1,
                    quarantine_reason = coalesce(%s, quarantine_reason),
                    next_retry_at = NULL
                WHERE id = %s
                """,
                (status, error_text, quarantine_reason, event_id),
            )
        else:
            cur.execute(
                """
                UPDATE events
                SET status = %s,
                    last_error = %s,
                    recipient_id = coalesce(%s, recipient_id),
                    last_attempt_at = now(),
                    quarantine_reason = coalesce(%s, quarantine_reason),
                    locked_at = CASE WHEN %s = 'processing' THEN now() ELSE NULL END,
                    lock_owner = CASE WHEN %s = 'processing' THEN %s ELSE NULL END
                WHERE id = %s
                """,
                (
                    status,
                    error_text,
                    recipient_id,
                    quarantine_reason,
                    status,
                    status,
                    WORKER_ID,
                    event_id,
                ),
            )
            cur.execute(
                """
                UPDATE photo_events
                SET status = %s,
                    last_error = %s,
                    last_attempt_at = now(),
                    quarantine_reason = coalesce(%s, quarantine_reason)
                WHERE id = %s
                """,
                (status, error_text, quarantine_reason, event_id),
            )


def get_attempt_count(conn, event_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT coalesce(attempt_count, 0) FROM events WHERE id = %s",
            (event_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def apply_delivery_failure(
    conn,
    event_id: str,
    recipient_id,
    error_text: str,
    policy,
):
    """После неудачной доставки: quarantine или failed_retryable + next_retry_at."""
    ac = get_attempt_count(conn, event_id)
    if ac >= policy.max_attempts:
        set_quarantine_after_retry_limit(conn, event_id, error_text, recipient_id)
        return "quarantine"
    delay = delay_before_next_retry(ac, policy.schedule_seconds)
    set_failed_retryable(conn, event_id, error_text, recipient_id, delay)
    return "retry"


def set_failed_retryable(conn, event_id, error_text, recipient_id, delay_seconds: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE events
            SET status = 'failed_retryable',
                last_error = %s,
                recipient_id = coalesce(%s, recipient_id),
                last_attempt_at = now(),
                next_retry_at = now() + (%s * interval '1 second'),
                quarantine_reason = NULL,
                locked_at = NULL,
                lock_owner = NULL
            WHERE id = %s
            """,
            (error_text, recipient_id, delay_seconds, event_id),
        )
        cur.execute(
            """
            UPDATE photo_events
            SET status = 'failed_retryable',
                last_error = %s,
                last_attempt_at = now(),
                next_retry_at = now() + (%s * interval '1 second'),
                quarantine_reason = NULL
            WHERE id = %s
            """,
            (error_text, delay_seconds, event_id),
        )


def set_quarantine_after_retry_limit(conn, event_id, error_text, recipient_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE events
            SET status = 'quarantine',
                last_error = %s,
                recipient_id = coalesce(%s, recipient_id),
                last_attempt_at = now(),
                quarantine_reason = coalesce(quarantine_reason, 'retry_limit_reached'),
                next_retry_at = NULL,
                locked_at = NULL,
                lock_owner = NULL
            WHERE id = %s
            """,
            (error_text, recipient_id, event_id),
        )
        cur.execute(
            """
            UPDATE photo_events
            SET status = 'quarantine',
                last_error = %s,
                last_attempt_at = now(),
                quarantine_reason = coalesce(quarantine_reason, 'retry_limit_reached'),
                next_retry_at = NULL
            WHERE id = %s
            """,
            (error_text, event_id),
        )


def record_exception_failure(conn, event_id, recipient_id, err: str, policy):
    """После rollback: событие снова ready, фиксируем неудачную попытку и счётчик."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT coalesce(attempt_count, 0)
            FROM events
            WHERE id = %s
            """,
            (event_id,),
        )
        row = cur.fetchone()
        ac = int(row[0]) if row else 0
    new_ac = ac + 1
    if new_ac >= policy.max_attempts:
        set_quarantine_after_retry_limit(conn, event_id, err, recipient_id)
        return
    delay = delay_before_next_retry(new_ac, policy.schedule_seconds)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE events
            SET status = 'failed_retryable',
                last_error = %s,
                attempt_count = %s,
                last_attempt_at = now(),
                next_retry_at = now() + (%s * interval '1 second'),
                recipient_id = coalesce(%s, recipient_id),
                quarantine_reason = NULL,
                locked_at = NULL,
                lock_owner = NULL
            WHERE id = %s
            """,
            (err, new_ac, delay, recipient_id, event_id),
        )
        cur.execute(
            """
            UPDATE photo_events
            SET status = 'failed_retryable',
                last_error = %s,
                attempt_count = %s,
                last_attempt_at = now(),
                next_retry_at = now() + (%s * interval '1 second'),
                quarantine_reason = NULL
            WHERE id = %s
            """,
            (err, new_ac, delay, event_id),
        )


def insert_delivery(conn, event_id, recipient_id, mode, status, trace_id, error_text=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO deliveries (
                event_id,
                recipient_id,
                attempt_no,
                delivery_mode,
                delivery_status,
                request_trace_id,
                error_text
            )
            VALUES (
                %s,
                %s,
                coalesce(
                    (SELECT max(attempt_no) + 1 FROM deliveries WHERE event_id = %s),
                    1
                ),
                %s,
                %s,
                %s,
                %s
            )
            RETURNING attempt_no
            """,
            (event_id, recipient_id, event_id, mode, status, trace_id, error_text),
        )
        attempt_no = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO photo_event_attempts (
                photo_event_id,
                attempt_no,
                recipient_id,
                delivery_mode,
                delivery_status,
                request_trace_id,
                error_text
            )
            SELECT
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            WHERE EXISTS (
                SELECT 1
                FROM photo_events pe
                WHERE pe.id = %s
            )
            """,
            (
                event_id,
                attempt_no,
                recipient_id,
                mode,
                status,
                trace_id,
                error_text,
                event_id,
            ),
        )
        return attempt_no


def finish_delivery(conn, event_id, attempt_no, delivery_status, error_text=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE deliveries
            SET delivery_status = %s,
                finished_at = now(),
                error_text = coalesce(%s, error_text)
            WHERE event_id = %s
              AND attempt_no = %s
              AND delivery_status = 'started'
            """,
            (delivery_status, error_text, event_id, attempt_no),
        )
        cur.execute(
            """
            UPDATE photo_event_attempts
            SET delivery_status = %s,
                finished_at = now(),
                error_text = coalesce(%s, error_text)
            WHERE photo_event_id = %s
              AND attempt_no = %s
              AND delivery_status = 'started'
            """,
            (delivery_status, error_text, event_id, attempt_no),
        )


def mark_delivery_started(conn, event_id, attempt_no):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE deliveries
            SET delivery_status = 'started'
            WHERE event_id = %s
              AND attempt_no = %s
              AND delivery_status = 'pending'
            """,
            (event_id, attempt_no),
        )
        cur.execute(
            """
            UPDATE photo_event_attempts
            SET delivery_status = 'started'
            WHERE photo_event_id = %s
              AND attempt_no = %s
              AND delivery_status = 'pending'
            """,
            (event_id, attempt_no),
        )


def mark_sent(conn, event_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE events
            SET status = 'sent',
                sent_at = now(),
                last_error = null,
                next_retry_at = null,
                quarantine_reason = null,
                locked_at = null,
                lock_owner = null
            WHERE id = %s
            """,
            (event_id,),
        )
        cur.execute(
            """
            UPDATE photo_events
            SET status = 'sent',
                sent_at = now(),
                last_error = null,
                next_retry_at = null,
                quarantine_reason = null
            WHERE id = %s
            """,
            (event_id,),
        )


def delivery_mode_value():
    if SHADOW_MODE in ("dry_run", "shadow", "prod"):
        return SHADOW_MODE
    return "dry_run"


def main():
    print(f"processor started mode={PIPELINE_SCHEMA_MODE}", flush=True)
    while True:
        conn = db()
        conn.autocommit = False
        row = None
        event_id = None
        trace_id = str(uuid.uuid4())
        picked_at = None
        event_started_at = None
        pick_to_processing_ms = None
        upload_init_ms = None
        binary_upload_ms = None
        send_message_ms = None
        finalize_db_ms = None
        total_event_ms = None
        try:
            row = fetch_one_ready(conn)
            if not row:
                conn.commit()
                conn.close()
                if PROCESSOR_IDLE_SLEEP_SECONDS > 0:
                    time.sleep(PROCESSOR_IDLE_SLEEP_SECONDS)
                continue

            picked_at = time.monotonic()
            event_started_at = picked_at

            event_id = row["id"]
            recipient_id = row["recipient_id"]
            mode = delivery_mode_value()

            if not recipient_id:
                finalize_started = time.monotonic()
                set_status(
                    conn,
                    event_id,
                    "quarantine",
                    "route_not_found",
                    None,
                    "route_not_found",
                )
                conn.commit()
                finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                conn.close()
                print(f"QUARANTINE: {event_id} route_not_found", flush=True)
                _journal.info(
                    f"processor\tQUARANTINE"
                    f"\tevent_id={event_id}\trecipient_id=-"
                    f"\ttrace_id={trace_id}\tattempt_no=-"
                    f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                    f"\treason=route_not_found"
                    f"\tpick_to_processing=-\tupload_init_ms=-\tbinary_upload_ms=-"
                    f"\tsend_message_ms=-\tfinalize_db_ms={finalize_db_ms}"
                    f"\ttotal_event_ms={total_event_ms}"
                )
                continue

            if SHADOW_MODE == "prod" and not row.get("max_chat_id"):
                finalize_started = time.monotonic()
                set_status(
                    conn,
                    event_id,
                    "quarantine",
                    "recipient_missing_chat_id",
                    recipient_id,
                    "recipient_missing_chat_id",
                )
                conn.commit()
                finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                conn.close()
                print(f"QUARANTINE: {event_id} recipient_missing_chat_id", flush=True)
                _journal.info(
                    f"processor\tQUARANTINE"
                    f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                    f"\ttrace_id={trace_id}\tattempt_no=-"
                    f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                    f"\treason=recipient_missing_chat_id"
                    f"\tpick_to_processing=-\tupload_init_ms=-\tbinary_upload_ms=-"
                    f"\tsend_message_ms=-\tfinalize_db_ms={finalize_db_ms}"
                    f"\ttotal_event_ms={total_event_ms}"
                )
                continue

            policy = fetch_retry_policy(conn)

            set_status(
                conn,
                event_id,
                "processing",
                None,
                recipient_id,
                None,
                increment_attempt=True,
            )
            attempt_no = insert_delivery(
                conn, event_id, recipient_id, mode, "pending", trace_id, None
            )
            mark_delivery_started(conn, event_id, attempt_no)
            pick_to_processing_ms = int((time.monotonic() - picked_at) * 1000)

            ftp_write_time_utc = get_ftp_write_time_utc(row["full_path"])
            test_event = is_test_file(row.get("file_name"))
            text = (
                build_test_text(row["camera_code"], row["first_seen_at"])
                if test_event
                else build_text(
                    row["camera_code"],
                    row["first_seen_at"],
                    ftp_write_time_utc,
                )
            )

            if SHADOW_MODE == "dry_run":
                finalize_started = time.monotonic()
                finish_delivery(conn, event_id, attempt_no, "sent", None)
                mark_sent(conn, event_id)
                conn.commit()
                finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                conn.close()
                print(f"SENT dry_run: {event_id}", flush=True)
                _journal.info(
                    f"processor\tSENT"
                    f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                    f"\ttrace_id={trace_id}\tattempt_no={attempt_no}"
                    f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                    f"\tmode=dry_run"
                    f"\tpick_to_processing={pick_to_processing_ms}"
                    f"\tupload_init_ms=-\tbinary_upload_ms=-\tsend_message_ms=-"
                    f"\tfinalize_db_ms={finalize_db_ms}\ttotal_event_ms={total_event_ms}"
                )
                continue

            if SHADOW_MODE == "shadow":
                if not SHADOW_TEST_CHAT_ID:
                    finalize_started = time.monotonic()
                    finish_delivery(
                        conn,
                        event_id,
                        attempt_no,
                        "failed",
                        "shadow_chat_not_configured",
                    )
                    out = apply_delivery_failure(
                        conn,
                        event_id,
                        recipient_id,
                        "shadow_chat_not_configured",
                        policy,
                    )
                    conn.commit()
                    finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                    total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                    conn.close()
                    print(
                        f"FAILED: {event_id} shadow_chat_not_configured ({out})",
                        flush=True,
                    )
                    _journal.error(
                        f"processor\tFAILED"
                        f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                        f"\ttrace_id={trace_id}\tattempt_no={attempt_no}"
                        f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                        f"\treason=shadow_chat_not_configured\toutcome={out}"
                        f"\tpick_to_processing={pick_to_processing_ms if pick_to_processing_ms is not None else '-'}"
                        f"\tupload_init_ms=-\tbinary_upload_ms=-\tsend_message_ms=-"
                        f"\tfinalize_db_ms={finalize_db_ms}\ttotal_event_ms={total_event_ms}"
                    )
                    continue

                if not is_configured():
                    finalize_started = time.monotonic()
                    finish_delivery(
                        conn,
                        event_id,
                        attempt_no,
                        "failed",
                        "max_api_not_configured",
                    )
                    out = apply_delivery_failure(
                        conn,
                        event_id,
                        recipient_id,
                        "max_api_not_configured",
                        policy,
                    )
                    conn.commit()
                    finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                    total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                    conn.close()
                    print(
                        f"FAILED: {event_id} max_api_not_configured ({out})",
                        flush=True,
                    )
                    _journal.error(
                        f"processor\tFAILED"
                        f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                        f"\ttrace_id={trace_id}\tattempt_no={attempt_no}"
                        f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                        f"\treason=max_api_not_configured\tmode=shadow\toutcome={out}"
                        f"\tpick_to_processing={pick_to_processing_ms if pick_to_processing_ms is not None else '-'}"
                        f"\tupload_init_ms=-\tbinary_upload_ms=-\tsend_message_ms=-"
                        f"\tfinalize_db_ms={finalize_db_ms}\ttotal_event_ms={total_event_ms}"
                    )
                    continue

                if test_event:
                    _, send_message_ms = send_message_timed(SHADOW_TEST_CHAT_ID, text)
                else:
                    _, upload_init_ms, binary_upload_ms, send_message_ms = (
                        send_message_with_photo_timed(
                            SHADOW_TEST_CHAT_ID,
                            text,
                            row["full_path"],
                        )
                    )
                finalize_started = time.monotonic()
                finish_delivery(conn, event_id, attempt_no, "sent", None)
                mark_sent(conn, event_id)
                conn.commit()
                finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                conn.close()
                print(f"SENT shadow: {event_id}", flush=True)
                _journal.info(
                    f"processor\tSENT"
                    f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                    f"\ttrace_id={trace_id}\tattempt_no={attempt_no}"
                    f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                    f"\tmode=shadow"
                    f"\tpick_to_processing={pick_to_processing_ms}"
                    f"\tupload_init_ms={upload_init_ms if upload_init_ms is not None else '-'}"
                    f"\tbinary_upload_ms={binary_upload_ms if binary_upload_ms is not None else '-'}"
                    f"\tsend_message_ms={send_message_ms if send_message_ms is not None else '-'}"
                    f"\tfinalize_db_ms={finalize_db_ms}\ttotal_event_ms={total_event_ms}"
                )
                continue

            if SHADOW_MODE == "prod":
                if not is_configured():
                    finalize_started = time.monotonic()
                    finish_delivery(
                        conn,
                        event_id,
                        attempt_no,
                        "failed",
                        "max_api_not_configured",
                    )
                    out = apply_delivery_failure(
                        conn,
                        event_id,
                        recipient_id,
                        "max_api_not_configured",
                        policy,
                    )
                    conn.commit()
                    finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                    total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                    conn.close()
                    print(
                        f"FAILED: {event_id} max_api_not_configured ({out})",
                        flush=True,
                    )
                    _journal.error(
                        f"processor\tFAILED"
                        f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                        f"\ttrace_id={trace_id}\tattempt_no={attempt_no}"
                        f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                        f"\treason=max_api_not_configured\tmode=prod\toutcome={out}"
                        f"\tpick_to_processing={pick_to_processing_ms if pick_to_processing_ms is not None else '-'}"
                        f"\tupload_init_ms=-\tbinary_upload_ms=-\tsend_message_ms=-"
                        f"\tfinalize_db_ms={finalize_db_ms}\ttotal_event_ms={total_event_ms}"
                    )
                    continue

                chat_id = row["max_chat_id"]
                if test_event:
                    _, send_message_ms = send_message_timed(chat_id, text)
                else:
                    _, upload_init_ms, binary_upload_ms, send_message_ms = (
                        send_message_with_photo_timed(chat_id, text, row["full_path"])
                    )
                finalize_started = time.monotonic()
                finish_delivery(conn, event_id, attempt_no, "sent", None)
                mark_sent(conn, event_id)
                conn.commit()
                finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
                total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                conn.close()
                print(f"SENT prod: {event_id}", flush=True)
                _journal.info(
                    f"processor\tSENT"
                    f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                    f"\ttrace_id={trace_id}\tattempt_no={attempt_no}"
                    f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                    f"\tmode=prod"
                    f"\tpick_to_processing={pick_to_processing_ms}"
                    f"\tupload_init_ms={upload_init_ms if upload_init_ms is not None else '-'}"
                    f"\tbinary_upload_ms={binary_upload_ms if binary_upload_ms is not None else '-'}"
                    f"\tsend_message_ms={send_message_ms if send_message_ms is not None else '-'}"
                    f"\tfinalize_db_ms={finalize_db_ms}\ttotal_event_ms={total_event_ms}"
                )
                continue

            finish_delivery(conn, event_id, attempt_no, "failed", "unsupported_mode")
            out = apply_delivery_failure(
                conn,
                event_id,
                recipient_id,
                f"unsupported SHADOW_MODE={SHADOW_MODE}",
                policy,
            )
            finalize_started = time.monotonic()
            conn.commit()
            finalize_db_ms = int((time.monotonic() - finalize_started) * 1000)
            total_event_ms = int((time.monotonic() - event_started_at) * 1000)
            conn.close()
            print(f"FAILED: {event_id} unsupported_mode ({out})", flush=True)
            _journal.info(
                f"processor\tFAILED"
                f"\tevent_id={event_id}\trecipient_id={recipient_id}"
                f"\ttrace_id={trace_id}\tattempt_no={attempt_no}"
                f"\tcamera={row.get('camera_code')}\tfile={row.get('file_name')}"
                f"\treason=unsupported_mode\toutcome={out}"
                f"\tpick_to_processing={pick_to_processing_ms if pick_to_processing_ms is not None else '-'}"
                f"\tupload_init_ms={upload_init_ms if upload_init_ms is not None else '-'}"
                f"\tbinary_upload_ms={binary_upload_ms if binary_upload_ms is not None else '-'}"
                f"\tsend_message_ms={send_message_ms if send_message_ms is not None else '-'}"
                f"\tfinalize_db_ms={finalize_db_ms if finalize_db_ms is not None else '-'}"
                f"\ttotal_event_ms={total_event_ms if total_event_ms is not None else '-'}"
            )

        except Exception as e:
            err = str(e)[:500]
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                if event_started_at is not None:
                    total_event_ms = int((time.monotonic() - event_started_at) * 1000)
                if event_id and row and row.get("recipient_id"):
                    c2 = db()
                    c2.autocommit = False
                    pol = fetch_retry_policy(c2)
                    record_exception_failure(
                        c2,
                        event_id,
                        row["recipient_id"],
                        err,
                        pol,
                    )
                    insert_delivery(
                        c2,
                        event_id,
                        row["recipient_id"],
                        delivery_mode_value(),
                        "failed",
                        str(uuid.uuid4()),
                        err,
                    )
                    c2.commit()
                    c2.close()
                    print(f"ERROR event={event_id}: {e}", flush=True)
                    _journal.error(
                        f"processor\tERROR"
                        f"\tevent_id={event_id}\trecipient_id={row.get('recipient_id')}"
                        f"\ttrace_id={trace_id}\tattempt_no=-"
                        f"\tcamera={row.get('camera_code') if row else '?'}"
                        f"\tfile={row.get('file_name') if row else '?'}"
                        f"\terror={err}"
                        f"\tpick_to_processing={pick_to_processing_ms if pick_to_processing_ms is not None else '-'}"
                        f"\tupload_init_ms={upload_init_ms if upload_init_ms is not None else '-'}"
                        f"\tbinary_upload_ms={binary_upload_ms if binary_upload_ms is not None else '-'}"
                        f"\tsend_message_ms={send_message_ms if send_message_ms is not None else '-'}"
                        f"\tfinalize_db_ms={finalize_db_ms if finalize_db_ms is not None else '-'}"
                        f"\ttotal_event_ms={total_event_ms if total_event_ms is not None else '-'}"
                    )
                elif event_id:
                    c2 = db()
                    c2.autocommit = False
                    pol = fetch_retry_policy(c2)
                    record_exception_failure(c2, event_id, None, err, pol)
                    c2.commit()
                    c2.close()
                    print(f"ERROR event={event_id} (no recipient row): {e}", flush=True)
                    _journal.error(
                        f"processor\tERROR"
                        f"\tevent_id={event_id}\trecipient_id=-"
                        f"\ttrace_id={trace_id}\tattempt_no=-"
                        f"\tcamera={row.get('camera_code') if row else '?'}"
                        f"\tfile={row.get('file_name') if row else '?'}"
                        f"\terror={err}"
                        f"\tpick_to_processing={pick_to_processing_ms if pick_to_processing_ms is not None else '-'}"
                        f"\tupload_init_ms={upload_init_ms if upload_init_ms is not None else '-'}"
                        f"\tbinary_upload_ms={binary_upload_ms if binary_upload_ms is not None else '-'}"
                        f"\tsend_message_ms={send_message_ms if send_message_ms is not None else '-'}"
                        f"\tfinalize_db_ms={finalize_db_ms if finalize_db_ms is not None else '-'}"
                        f"\ttotal_event_ms={total_event_ms if total_event_ms is not None else '-'}"
                    )
                else:
                    print(f"ERROR before event picked: {e}", flush=True)
                    _journal.error(
                        f"processor\tERROR"
                        f"\tevent_id=-\trecipient_id=-\ttrace_id={trace_id}\tattempt_no=-"
                        f"\tcamera=?\tfile=?\terror={err}"
                        f"\tpick_to_processing=-\tupload_init_ms=-\tbinary_upload_ms=-"
                        f"\tsend_message_ms=-\tfinalize_db_ms=-\ttotal_event_ms=-"
                    )
            except Exception as e2:
                print(f"ERROR nested: {e2}", flush=True)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            if PROCESSOR_ERROR_SLEEP_SECONDS > 0:
                time.sleep(PROCESSOR_ERROR_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
