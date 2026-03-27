import os
import time
import uuid
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor

from max_client import is_configured, send_message_with_photo
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


def _fmt_utc(dt):
    dt = _as_utc(dt)
    if dt is None:
        return "unknown"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def build_text(camera_code, first_seen_at, ftp_write_time_utc):
    lines = ["🚨 Camera event", f"📷 Камера: {camera_code}"]

    first_seen_utc = _as_utc(first_seen_at)
    ftp_write_utc = _as_utc(ftp_write_time_utc)
    if first_seen_utc and ftp_write_utc:
        delta_sec = abs((first_seen_utc - ftp_write_utc).total_seconds())
        if delta_sec <= 1:
            lines.append(f"🕒 Время события: {_fmt_utc(first_seen_utc)}")
        else:
            lines.append(f"🕒 Обнаружено системой: {_fmt_utc(first_seen_utc)}")
            lines.append(f"🗂 Время записи на FTP: {_fmt_utc(ftp_write_utc)}")
    elif first_seen_utc:
        lines.append(f"🕒 Обнаружено системой: {_fmt_utc(first_seen_utc)}")
    else:
        lines.append(f"🗂 Время записи на FTP: {_fmt_utc(ftp_write_utc)}")

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
                WHERE cr.camera_id = e.camera_id AND cr.is_active = true
                ORDER BY cr.priority ASC, cr.id ASC
                LIMIT 1
            ) route_rec ON true
            LEFT JOIN recipients r_site
                ON r_site.id = s.default_recipient_id AND r_site.is_active = true
            LEFT JOIN recipients r_tenant
                ON r_tenant.id = t.default_recipient_id AND r_tenant.is_active = true
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
                    next_retry_at = NULL
                WHERE id = %s
                """,
                (status, error_text, recipient_id, quarantine_reason, event_id),
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
                    quarantine_reason = coalesce(%s, quarantine_reason)
                WHERE id = %s
                """,
                (status, error_text, recipient_id, quarantine_reason, event_id),
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
                quarantine_reason = NULL
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
                next_retry_at = NULL
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
                quarantine_reason = NULL
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
                quarantine_reason = null
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
        try:
            row = fetch_one_ready(conn)
            if not row:
                conn.commit()
                conn.close()
                if PROCESSOR_IDLE_SLEEP_SECONDS > 0:
                    time.sleep(PROCESSOR_IDLE_SLEEP_SECONDS)
                continue

            event_id = row["id"]
            recipient_id = row["recipient_id"]
            mode = delivery_mode_value()

            if not recipient_id:
                set_status(
                    conn,
                    event_id,
                    "quarantine",
                    "route_not_found",
                    None,
                    "route_not_found",
                )
                conn.commit()
                conn.close()
                print(f"QUARANTINE: {event_id} route_not_found", flush=True)
                continue

            if SHADOW_MODE == "prod" and not row.get("max_chat_id"):
                set_status(
                    conn,
                    event_id,
                    "quarantine",
                    "recipient_missing_chat_id",
                    recipient_id,
                    "recipient_missing_chat_id",
                )
                conn.commit()
                conn.close()
                print(f"QUARANTINE: {event_id} recipient_missing_chat_id", flush=True)
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

            ftp_write_time_utc = get_ftp_write_time_utc(row["full_path"])
            text = build_text(
                row["camera_code"],
                row["first_seen_at"],
                ftp_write_time_utc,
            )

            if SHADOW_MODE == "dry_run":
                finish_delivery(conn, event_id, attempt_no, "sent", None)
                mark_sent(conn, event_id)
                conn.commit()
                conn.close()
                print(f"SENT dry_run: {event_id}", flush=True)
                continue

            if SHADOW_MODE == "shadow":
                if not SHADOW_TEST_CHAT_ID:
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
                    conn.close()
                    print(
                        f"FAILED: {event_id} shadow_chat_not_configured ({out})",
                        flush=True,
                    )
                    continue

                if not is_configured():
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
                    conn.close()
                    print(
                        f"FAILED: {event_id} max_api_not_configured ({out})",
                        flush=True,
                    )
                    continue

                send_message_with_photo(SHADOW_TEST_CHAT_ID, text, row["full_path"])
                finish_delivery(conn, event_id, attempt_no, "sent", None)
                mark_sent(conn, event_id)
                conn.commit()
                conn.close()
                print(f"SENT shadow: {event_id}", flush=True)
                continue

            if SHADOW_MODE == "prod":
                if not is_configured():
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
                    conn.close()
                    print(
                        f"FAILED: {event_id} max_api_not_configured ({out})",
                        flush=True,
                    )
                    continue

                chat_id = row["max_chat_id"]
                send_message_with_photo(chat_id, text, row["full_path"])
                finish_delivery(conn, event_id, attempt_no, "sent", None)
                mark_sent(conn, event_id)
                conn.commit()
                conn.close()
                print(f"SENT prod: {event_id}", flush=True)
                continue

            finish_delivery(conn, event_id, attempt_no, "failed", "unsupported_mode")
            out = apply_delivery_failure(
                conn,
                event_id,
                recipient_id,
                f"unsupported SHADOW_MODE={SHADOW_MODE}",
                policy,
            )
            conn.commit()
            conn.close()
            print(f"FAILED: {event_id} unsupported_mode ({out})", flush=True)

        except Exception as e:
            err = str(e)[:500]
            try:
                conn.rollback()
            except Exception:
                pass
            try:
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
                elif event_id:
                    c2 = db()
                    c2.autocommit = False
                    pol = fetch_retry_policy(c2)
                    record_exception_failure(c2, event_id, None, err, pol)
                    c2.commit()
                    c2.close()
                    print(f"ERROR event={event_id} (no recipient row): {e}", flush=True)
                else:
                    print(f"ERROR before event picked: {e}", flush=True)
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
