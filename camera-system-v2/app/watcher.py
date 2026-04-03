import os
import re
import time
import hashlib
from datetime import datetime, timezone
import psycopg2
import event_journal

WATCH_PATH = "/source"

ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png"}
TEST_FILE_ALLOWED_EXTENSIONS = {".txt"}
TEST_FILE_REGEX = re.compile(
    os.environ.get("TEST_FILE_REGEX", r"(^test_|_test_|healthcheck|probe)"),
    re.IGNORECASE,
)

MAX_FILE_AGE_SECONDS = float(os.environ.get("WATCH_MAX_FILE_AGE_SECONDS", "3600"))
FILE_STABILIZE_SECONDS = float(os.environ.get("WATCH_FILE_STABILIZE_SECONDS", "0.2"))
WATCH_LOOP_SLEEP_SECONDS = float(os.environ.get("WATCH_LOOP_SLEEP_SECONDS", "1"))
WATCH_INITIAL_LOOKBACK_SECONDS = float(
    os.environ.get("WATCH_INITIAL_LOOKBACK_SECONDS", "120")
)
WATCH_MTIME_GUARD_SECONDS = float(os.environ.get("WATCH_MTIME_GUARD_SECONDS", "2"))
WATCH_MIN_FILE_SIZE_BYTES = int(os.environ.get("WATCH_MIN_FILE_SIZE_BYTES", "1"))
WATCH_USE_MTIME_CURSOR = (
    os.environ.get("WATCH_USE_MTIME_CURSOR", "1").strip().lower() in ("1", "true", "yes")
)
PIPELINE_SCHEMA_MODE = os.environ.get("PIPELINE_SCHEMA_MODE", "legacy").strip().lower()
WATCH_CHECKSUM_MODE = os.environ.get("WATCH_CHECKSUM_MODE", "sha256").strip().lower()

DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]
DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")

print(f"watcher started mode={PIPELINE_SCHEMA_MODE}", flush=True)
_journal = event_journal.get_journal()

# in-process dedup by event_key (full fingerprint) and by (camera_code, filename)
seen_keys: set = set()
seen_names: set = set()   # (camera_code, file_name) — survives mtime/size changes
scan_from_ts = max(0.0, time.time() - WATCH_INITIAL_LOOKBACK_SECONDS)


def make_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT,
    )


def detect_camera_code(file_name):

    name=file_name.lower()

    if "_" in name:

        return name.split("_",1)[0]

    return os.path.splitext(name)[0]


def extract_site_from_path(full_path):

    rel=os.path.relpath(full_path,WATCH_PATH)

    parts=rel.split(os.sep)

    if len(parts)>=2:

        return parts[-2].lower()

    return "default"


def make_event_key(camera_code, file_name, size, checksum):

    raw=f"{camera_code}|{file_name}|{size}|{checksum}"

    return hashlib.sha256(raw.encode()).hexdigest()


def make_event_key_no_checksum(camera_code, file_name, size, mtime_seconds):

    raw = f"{camera_code}|{file_name}|{size}|{int(mtime_seconds)}"

    return hashlib.sha256(raw.encode()).hexdigest()


def file_sha256(path):

    h=hashlib.sha256()

    with open(path,"rb") as bf:

        for chunk in iter(lambda: bf.read(1024*1024),b""):

            h.update(chunk)

    return h.hexdigest()


def is_test_file(file_name):

    return bool(file_name and TEST_FILE_REGEX.search(file_name))


_INSERT_SQL = """
WITH src AS (
    SELECT tenant_id FROM ftp_sources
    WHERE %s LIKE root_path||'%%'
    LIMIT 1
),
site_ins AS (
    INSERT INTO sites(tenant_id, code, name)
    SELECT src.tenant_id, %s, %s FROM src
    WHERE NOT EXISTS(SELECT 1 FROM sites WHERE tenant_id=src.tenant_id AND code=%s)
    RETURNING id
),
site_final AS (
    SELECT id FROM site_ins
    UNION ALL
    SELECT s.id FROM sites s
    JOIN src ON s.tenant_id = src.tenant_id
    WHERE s.code=%s
    LIMIT 1
),
cam_ins AS (
    INSERT INTO cameras(tenant_id, site_id, camera_code, file_prefix)
    SELECT src.tenant_id, site_final.id, %s, %s FROM src, site_final
    WHERE NOT EXISTS(SELECT 1 FROM cameras WHERE camera_code=%s AND site_id=site_final.id)
    RETURNING id
),
cam_final AS (
    SELECT c.id FROM cameras c
    JOIN src ON c.tenant_id = src.tenant_id
    JOIN site_final sf ON sf.id = c.site_id
    WHERE c.camera_code=%s
    UNION ALL
    SELECT id FROM cam_ins
    LIMIT 1
)
INSERT INTO events(
    id, event_key, file_name, full_path, file_size,
    status, camera_code, camera_id, checksum_sha256, quarantine_reason,
    ftp_written_at,
    first_seen_at
)
SELECT
    uuid_generate_v4(), %s, %s, %s, %s, %s, %s,
    (SELECT id FROM cam_final), %s, %s, %s, %s
WHERE NOT EXISTS (
    SELECT 1
    FROM events
    WHERE camera_id = (SELECT id FROM cam_final)
      AND file_name = %s
      AND file_size = %s
      AND checksum_sha256 IS NOT DISTINCT FROM %s
)
ON CONFLICT(event_key) DO NOTHING
"""

conn = None


def main(*, max_loops=None):
    global scan_from_ts

    conn = None
    loops_done = 0
    while True:

        try:
            if conn is None or conn.closed:
                conn = make_conn()

            loop_started_at = time.time()

            for root, _, files in os.walk(WATCH_PATH):

                for f in files:

                    path = os.path.join(root, f)
                    ext = os.path.splitext(f)[1].lower()
                    test_file = is_test_file(f)

                    if ext not in ALLOWED_EXTENSIONS:
                        if not (test_file and ext in TEST_FILE_ALLOWED_EXTENSIONS):
                            continue

                    try:
                        size1 = os.path.getsize(path)
                        mtime1 = os.path.getmtime(path)
                    except OSError:
                        continue

                    camera_code = detect_camera_code(f)
                    name_key = (camera_code, f)
                    if name_key in seen_names:
                        continue
                    if FILE_STABILIZE_SECONDS > 0:
                        time.sleep(FILE_STABILIZE_SECONDS)

                    try:
                        size2 = os.path.getsize(path)
                        mtime2 = os.path.getmtime(path)
                    except OSError:
                        continue

                    if size1 != size2 or mtime1 != mtime2:
                        continue

                    if size2 < WATCH_MIN_FILE_SIZE_BYTES:
                        continue

                    ftp_written_at_utc = datetime.fromtimestamp(mtime2, tz=timezone.utc)
                    too_old = (loop_started_at - mtime2) > MAX_FILE_AGE_SECONDS

                    detected_at_utc = datetime.now(timezone.utc)

                    site_code = extract_site_from_path(path)

                    checksum = None
                    if WATCH_CHECKSUM_MODE != "skip":
                        try:
                            checksum = file_sha256(path)
                        except OSError:
                            continue

                    if checksum is None:
                        event_key = make_event_key_no_checksum(camera_code, f, size2, mtime2)
                    else:
                        event_key = make_event_key(camera_code, f, size2, checksum)
                    if event_key in seen_keys:
                        continue

                    status = "quarantine" if too_old else "ready"
                    quarantine_reason = "too_old" if too_old else None

                    try:
                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(_INSERT_SQL, (
                                    path,
                                    site_code, site_code, site_code, site_code,
                                    camera_code, camera_code, camera_code,
                                    camera_code,
                                    event_key, f, path, size2, status, camera_code,
                                    checksum, quarantine_reason, ftp_written_at_utc, detected_at_utc,
                                    f, size2, checksum,
                                ))

                                cur.execute(
                                    """
                                    INSERT INTO photo_sources (
                                      source_key, tenant_code, site_code, camera_code,
                                      ftp_root, ftp_subpath, file_prefix,
                                      recipient_id, fallback_recipient_id, is_active
                                    )
                                    SELECT
                                      'cam:' || c.id::text,
                                      COALESCE(t.code, 'default'),
                                      COALESCE(s.code, 'default'),
                                      c.camera_code, '/source', '', c.file_prefix,
                                      (SELECT cr1.recipient_id FROM camera_routes cr1
                                       WHERE cr1.camera_id=c.id AND cr1.is_active=true
                                       ORDER BY cr1.priority ASC, cr1.id ASC LIMIT 1),
                                      COALESCE(s.default_recipient_id, t.default_recipient_id),
                                      true
                                    FROM events e
                                    JOIN cameras c ON c.id=e.camera_id
                                    LEFT JOIN sites s ON s.id=c.site_id
                                    LEFT JOIN tenants t ON t.id=c.tenant_id
                                    WHERE e.event_key=%s AND e.camera_id IS NOT NULL
                                    ON CONFLICT (source_key) DO NOTHING
                                    """,
                                    (event_key,),
                                )
                                cur.execute(
                                    """
                                    INSERT INTO photo_events (
                                      id, event_key, source_id, file_name, full_path,
                                      file_size, checksum_sha256, status, attempt_count,
                                      next_retry_at, last_attempt_at, quarantine_reason,
                                      last_error, provider_message_ref, sent_at,
                                      delivered_at, ftp_removed_at, ftp_written_at, first_seen_at, updated_at
                                    )
                                    SELECT
                                      e.id, e.event_key, ps.id, e.file_name, e.full_path,
                                      e.file_size, e.checksum_sha256, e.status,
                                      COALESCE(e.attempt_count, 0), e.next_retry_at,
                                      e.last_attempt_at, e.quarantine_reason, e.last_error,
                                      e.provider_message_ref, e.sent_at,
                                      NULL::timestamptz, NULL::timestamptz, e.ftp_written_at,
                                      COALESCE(e.first_seen_at, now()), now()
                                    FROM events e
                                    JOIN photo_sources ps
                                      ON ps.source_key='cam:'||e.camera_id::text
                                    WHERE e.event_key=%s
                                    ON CONFLICT (event_key) DO NOTHING
                                    """,
                                    (event_key,),
                                )

                        seen_keys.add(event_key)
                        seen_names.add(name_key)
                        print(
                            f"event ok site={site_code} cam={camera_code} "
                            f"status={status} key={event_key}",
                            flush=True,
                        )
                        _journal.info(
                            f"watcher\tDISCOVERED"
                            f"\tcamera={camera_code}\tfile={f}"
                            f"\tsite={site_code}\tpath={path}"
                            f"\tstatus={status}\treason={quarantine_reason or '-'}"
                            f"\tftp_file_mtime={ftp_written_at_utc.isoformat()}"
                            f"\tfile_size={size2}"
                            f"\tkey={event_key}"
                        )

                    except Exception as db_err:
                        print(f"db error {camera_code}: {db_err}", flush=True)
                        _journal.error(
                            f"watcher\tERROR"
                            f"\tcamera={camera_code}\tfile={f}"
                            f"\tkey={event_key or '?'}"
                            f"\terror={str(db_err)[:300]}"
                        )
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        conn = None
                        break

            if WATCH_USE_MTIME_CURSOR:
                scan_from_ts = max(0.0, loop_started_at - WATCH_MTIME_GUARD_SECONDS)

            loops_done += 1
            if max_loops is not None and loops_done >= int(max_loops):
                return

            if WATCH_LOOP_SLEEP_SECONDS > 0:
                time.sleep(WATCH_LOOP_SLEEP_SECONDS)

        except Exception as e:
            print(f"watcher error: {e}", flush=True)
            _journal.error(f"watcher\tERROR\tcamera=?\tfile=?\terror={str(e)[:300]}")
            conn = None
            if WATCH_LOOP_SLEEP_SECONDS > 0:
                time.sleep(WATCH_LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    main()

