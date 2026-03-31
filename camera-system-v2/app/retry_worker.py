import os
import time
import psycopg2
import event_journal

from retry_policy import fetch_retry_policy

DB_NAME = os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "camera_v2"))
DB_USER = os.getenv("POSTGRES_USER", os.getenv("DB_USER", "camera"))
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", ""))
DB_HOST = os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "postgres"))
DB_PORT = int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")))
POLL_SEC = int(os.getenv("RETRY_POLL_INTERVAL", "15"))
REAPER_STALE_MINUTES = int(os.getenv("REAPER_STALE_MINUTES", "10"))


def db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def main():
    journal = event_journal.get_journal()
    print("retry worker started", flush=True)
    while True:
        conn = db()
        conn.autocommit = False
        try:
            policy = fetch_retry_policy(conn)
            max_a = policy.max_attempts

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE events
                    SET status = 'ready',
                        next_retry_at = NULL
                    WHERE status = 'failed_retryable'
                      AND (next_retry_at IS NULL OR next_retry_at <= now())
                      AND coalesce(attempt_count, 0) < %s
                    RETURNING id
                    """,
                    (max_a,),
                )
                rows_reset = cur.fetchall()

                if rows_reset:
                    cur.execute(
                        """
                        UPDATE photo_events
                        SET status = 'ready',
                            next_retry_at = NULL
                        WHERE id = ANY(%s)
                        """,
                        ([r[0] for r in rows_reset],),
                    )

                cur.execute(
                    """
                    UPDATE events
                    SET status = 'quarantine',
                        last_error = coalesce(last_error, 'retry_limit_reached'),
                        quarantine_reason = coalesce(quarantine_reason, 'retry_limit_reached'),
                        next_retry_at = NULL
                    WHERE status = 'failed_retryable'
                      AND coalesce(attempt_count, 0) >= %s
                    RETURNING id
                    """,
                    (max_a,),
                )
                rows_final = cur.fetchall()

                if rows_final:
                    cur.execute(
                        """
                        UPDATE photo_events
                        SET status = 'quarantine',
                            last_error = coalesce(last_error, 'retry_limit_reached'),
                            quarantine_reason = coalesce(quarantine_reason, 'retry_limit_reached'),
                            next_retry_at = NULL
                        WHERE id = ANY(%s)
                        """,
                        ([r[0] for r in rows_final],),
                    )

                cur.execute(
                    """
                    UPDATE events
                    SET status = 'failed_retryable',
                        last_error = coalesce(last_error, 'stale_processing'),
                        quarantine_reason = NULL,
                        next_retry_at = now(),
                        locked_at = NULL,
                        lock_owner = NULL
                    WHERE status = 'processing'
                      AND locked_at IS NOT NULL
                      AND locked_at < (now() - (%s * interval '1 minute'))
                    RETURNING id
                    """,
                    (REAPER_STALE_MINUTES,),
                )
                rows_reaped = cur.fetchall()

                if rows_reaped:
                    cur.execute(
                        """
                        UPDATE photo_events
                        SET status = 'failed_retryable',
                            last_error = coalesce(last_error, 'stale_processing'),
                            quarantine_reason = NULL,
                            next_retry_at = now()
                        WHERE id = ANY(%s)
                        """,
                        ([r[0] for r in rows_reaped],),
                    )

                cur.execute(
                    """
                    UPDATE deliveries
                    SET delivery_status = 'failed',
                        error_text = coalesce(error_text, 'stale_processing'),
                        finished_at = now()
                    WHERE delivery_status = 'started'
                      AND started_at < (now() - (%s * interval '1 minute'))
                    RETURNING event_id, attempt_no
                    """,
                    (REAPER_STALE_MINUTES,),
                )
                rows_delivery_reaped = cur.fetchall()

                if rows_delivery_reaped:
                    cur.execute(
                        """
                        UPDATE photo_event_attempts
                        SET delivery_status = 'failed',
                            error_text = coalesce(error_text, 'stale_processing'),
                            finished_at = now()
                        WHERE (photo_event_id, attempt_no) IN (
                            SELECT * FROM unnest(%s::uuid[], %s::int[])
                        )
                        """,
                        (
                            [r[0] for r in rows_delivery_reaped],
                            [r[1] for r in rows_delivery_reaped],
                        ),
                    )

            conn.commit()
            conn.close()
            if rows_reset or rows_final or rows_reaped or rows_delivery_reaped:
                print(
                    "retry requeue="
                    f"{len(rows_reset)} final={len(rows_final)} "
                    f"reaped_events={len(rows_reaped)} reaped_deliveries={len(rows_delivery_reaped)}",
                    flush=True,
                )
                journal.info(
                    "retry\tBATCH"
                    f"\trequeue={len(rows_reset)}\tfinal={len(rows_final)}"
                    f"\treaped_events={len(rows_reaped)}"
                    f"\treaped_deliveries={len(rows_delivery_reaped)}"
                )
            time.sleep(POLL_SEC)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            print(f"retry ERROR: {e}", flush=True)
            time.sleep(30)


if __name__ == "__main__":
    main()
