import os
import time
import psycopg2

from retry_policy import fetch_retry_policy

DB_NAME = os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "camera_v2"))
DB_USER = os.getenv("POSTGRES_USER", os.getenv("DB_USER", "camera"))
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", ""))
DB_HOST = os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "postgres"))
DB_PORT = int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")))
POLL_SEC = int(os.getenv("RETRY_POLL_INTERVAL", "15"))


def db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def main():
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

            conn.commit()
            conn.close()
            if rows_reset or rows_final:
                print(
                    f"retry requeue={len(rows_reset)} final={len(rows_final)}",
                    flush=True,
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
