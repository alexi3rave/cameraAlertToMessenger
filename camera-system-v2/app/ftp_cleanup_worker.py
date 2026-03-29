"""
Удаляет файлы на FTP старше FTP_RETENTION_DAYS только для событий из реестра
в терминальных статусах. Не трогает незарегистрированные файлы и активные события.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import time
import psycopg2
import event_journal

DB_NAME = os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "camera_v2"))
DB_USER = os.getenv("POSTGRES_USER", os.getenv("DB_USER", "camera"))
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", ""))
DB_HOST = os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "postgres"))
DB_PORT = int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432")))

FTP_ROOT = os.getenv("FTP_CLEANUP_ROOT", "/source").rstrip("/") or "/source"
RETENTION_DAYS = int(os.getenv("FTP_RETENTION_DAYS", "7"))
INTERVAL_SEC = int(os.getenv("FTP_CLEANUP_INTERVAL_SEC", "3600"))
BATCH = int(os.getenv("FTP_CLEANUP_BATCH", "200"))
LOG_DIR = os.getenv("FTP_CLEANUP_LOG_DIR", "/logs")
LOG_FILE = os.path.join(LOG_DIR, "ftp_cleanup.log")
LOG_MAX_BYTES = int(os.getenv("FTP_CLEANUP_LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
LOG_BACKUP_COUNT = int(os.getenv("FTP_CLEANUP_LOG_BACKUP_COUNT", "5"))

# Статусы, после которых оригинальный файл для доставки больше не нужен.
TERMINAL = ("sent", "quarantine")


def _setup_file_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("ftp_cleanup")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s\t%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)
    return logger


def db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def assert_events_has_ftp_removed_at(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'events'
              AND column_name = 'ftp_removed_at'
            """
        )
        if cur.fetchone() is None:
            cur.execute("SELECT current_database()")
            dbn = cur.fetchone()[0]
            raise RuntimeError(
                f"В БД «{dbn}» нет колонки events.ftp_removed_at. "
                "Примените миграцию: migrations/004_ftp_cleanup.sql"
            )


def path_is_under_root(candidate: str, root_real: str) -> bool:
    try:
        cr = os.path.realpath(candidate)
    except OSError:
        return False
    root_prefix = root_real + os.sep
    return cr == root_real or cr.startswith(root_prefix)


def main():
    journal = _setup_file_logger()
    ev_journal = event_journal.get_journal()
    print(
        f"ftp_cleanup started root={FTP_ROOT} retention_days={RETENTION_DAYS} "
        f"interval={INTERVAL_SEC}s log={LOG_FILE}",
        flush=True,
    )
    print(
        f"ftp_cleanup db: host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER}",
        flush=True,
    )
    journal.info(f"START\tretention_days={RETENTION_DAYS}\troot={FTP_ROOT}")
    _probe = db()
    try:
        assert_events_has_ftp_removed_at(_probe)
    finally:
        _probe.close()
    print("ftp_cleanup schema ok (events.ftp_removed_at present)", flush=True)
    root_real = os.path.realpath(FTP_ROOT)
    while True:
        cutoff = time.time() - RETENTION_DAYS * 86400
        conn = None
        try:
            conn = db()
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, full_path, file_name, camera_code, first_seen_at
                    FROM events
                    WHERE ftp_removed_at IS NULL
                      AND status = ANY(%s)
                      AND first_seen_at < (now() - %s::interval)
                    ORDER BY first_seen_at
                    LIMIT %s
                    """,
                    (list(TERMINAL), f"{RETENTION_DAYS} days", BATCH),
                )
                rows = cur.fetchall()

            removed = 0
            skipped = 0
            for event_id, full_path, file_name, camera_code, first_seen_at in rows:
                if not full_path or not isinstance(full_path, str):
                    skipped += 1
                    continue
                if not path_is_under_root(full_path, root_real):
                    print(
                        f"ftp_cleanup skip path outside root: {full_path}",
                        flush=True,
                    )
                    skipped += 1
                    continue
                try:
                    st = os.stat(full_path)
                except FileNotFoundError:
                    with conn.cursor() as u:
                        u.execute(
                            """
                            UPDATE events
                            SET ftp_removed_at = now()
                            WHERE id = %s AND ftp_removed_at IS NULL
                            """,
                            (event_id,),
                        )
                    journal.info(
                        f"ALREADY_GONE"
                        f"\tcamera={camera_code}\tfile={file_name}"
                        f"\tfirst_seen={first_seen_at}"
                    )
                    ev_journal.info(
                        f"ftp_cleanup\tDELETED"
                        f"\tcamera={camera_code}\tfile={file_name}"
                        f"\tfirst_seen={first_seen_at}\tnote=already_gone"
                    )
                    removed += 1
                    continue
                except OSError as e:
                    print(f"ftp_cleanup stat error {full_path}: {e}", flush=True)
                    skipped += 1
                    continue

                if st.st_mtime > cutoff:
                    skipped += 1
                    continue

                try:
                    os.remove(full_path)
                except OSError as e:
                    print(f"ftp_cleanup remove error {full_path}: {e}", flush=True)
                    journal.warning(f"REMOVE_ERROR\tcamera={camera_code}\tfile={file_name}\terror={e}")
                    skipped += 1
                    continue

                with conn.cursor() as u:
                    u.execute(
                        """
                        UPDATE events
                        SET ftp_removed_at = now()
                        WHERE id = %s AND ftp_removed_at IS NULL
                        """,
                        (event_id,),
                    )
                journal.info(
                    f"DELETED"
                    f"\tcamera={camera_code}\tfile={file_name}"
                    f"\tfirst_seen={first_seen_at}\tpath={full_path}"
                )
                ev_journal.info(
                    f"ftp_cleanup\tDELETED"
                    f"\tcamera={camera_code}\tfile={file_name}"
                    f"\tfirst_seen={first_seen_at}\tpath={full_path}"
                )
                removed += 1

            conn.commit()
            conn.close()
            if removed or skipped:
                msg = f"ftp_cleanup batch removed_or_marked={removed} skipped={skipped}"
                print(msg, flush=True)
                journal.info(f"BATCH\tremoved={removed}\tskipped={skipped}")
        except Exception as e:
            print(f"ftp_cleanup ERROR: {e}", flush=True)
            journal.error(f"ERROR\t{e}")
            ev_journal.error(f"ftp_cleanup\tERROR\tcamera=?\tfile=?\terror={str(e)[:300]}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
