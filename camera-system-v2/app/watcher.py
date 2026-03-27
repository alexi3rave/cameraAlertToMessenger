import os
import time
import hashlib
import psycopg2

WATCH_PATH = "/source"

ALLOWED_EXTENSIONS = {".jpg",".jpeg",".png"}

MAX_FILE_AGE_SECONDS = 172800
PIPELINE_SCHEMA_MODE = os.environ.get("PIPELINE_SCHEMA_MODE", "legacy").strip().lower()


DB_NAME=os.environ["POSTGRES_DB"]
DB_USER=os.environ["POSTGRES_USER"]
DB_PASSWORD=os.environ["POSTGRES_PASSWORD"]

DB_HOST=os.environ.get("POSTGRES_HOST","postgres")
DB_PORT=os.environ.get("POSTGRES_PORT","5432")


print(f"watcher started mode={PIPELINE_SCHEMA_MODE}",flush=True)


def get_conn():

    return psycopg2.connect(

        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT

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


def make_event_key(camera_code,file_name,size,mtime):

    raw=f"{camera_code}|{file_name}|{size}|{mtime}"

    return hashlib.sha256(raw.encode()).hexdigest()


def file_sha256(path):

    h=hashlib.sha256()

    with open(path,"rb") as bf:

        for chunk in iter(lambda: bf.read(1024*1024),b""):

            h.update(chunk)

    return h.hexdigest()


while True:

    try:

        for root,_,files in os.walk(WATCH_PATH):

            for f in files:

                path=os.path.join(root,f)

                ext=os.path.splitext(f)[1].lower()

                if ext not in ALLOWED_EXTENSIONS:

                    continue


                size1=os.path.getsize(path)

                mtime1=os.path.getmtime(path)


                if (time.time()-mtime1)>MAX_FILE_AGE_SECONDS:

                    continue


                time.sleep(1)


                size2=os.path.getsize(path)

                mtime2=os.path.getmtime(path)


                if size1!=size2 or mtime1!=mtime2:

                    continue


                camera_code=detect_camera_code(f)

                site_code=extract_site_from_path(path)


                event_key=make_event_key(

                    camera_code,
                    f,
                    size2,
                    int(mtime2)

                )

                try:

                    checksum=file_sha256(path)

                except OSError:

                    continue


                conn=get_conn()


                try:

                    with conn:

                        with conn.cursor() as cur:

                            cur.execute(

"""
WITH src AS (

    SELECT tenant_id

    FROM ftp_sources

    WHERE %s LIKE root_path||'%%'

    LIMIT 1

),

site_ins AS (

    INSERT INTO sites(

        tenant_id,
        code,
        name

    )

    SELECT

        src.tenant_id,
        %s,
        %s

    FROM src

    WHERE NOT EXISTS(

        SELECT 1 FROM sites

        WHERE tenant_id=src.tenant_id
        AND code=%s

    )

    RETURNING id

),

site_final AS (

    SELECT id FROM site_ins

    UNION ALL

    SELECT id FROM sites

    WHERE code=%s

    LIMIT 1

),

cam_ins AS (

    INSERT INTO cameras(

        tenant_id,
        site_id,
        camera_code,
        file_prefix

    )

    SELECT

        src.tenant_id,
        site_final.id,
        %s,
        %s

    FROM src,site_final

    WHERE NOT EXISTS(

        SELECT 1 FROM cameras
        WHERE camera_code=%s
        AND site_id=site_final.id

    )

    RETURNING id

),

cam_final AS (

    SELECT id FROM cameras

    WHERE camera_code=%s

    UNION ALL

    SELECT id FROM cam_ins

    LIMIT 1

)

INSERT INTO events(

    id,
    event_key,
    file_name,
    full_path,
    file_size,
    status,
    camera_code,
    camera_id,
    checksum_sha256

)

SELECT

    uuid_generate_v4(),

    %s,

    %s,

    %s,

    %s,

    'ready',

    %s,

    (SELECT id FROM cam_final),

    %s

ON CONFLICT(event_key) DO NOTHING

""",

(

path,

site_code,
site_code,
site_code,
site_code,

camera_code,
camera_code,
camera_code,

camera_code,

event_key,
f,
path,
size2,
camera_code,

checksum

)

)

                            print(

                                f"event ok site={site_code} cam={camera_code}",

                                flush=True

                            )



                            # Dual-write в упрощённые слои (Phase A): source -> event.
                            # Legacy pipeline остаётся основным источником правды для processor.
                            cur.execute(
                                """
                                INSERT INTO photo_sources (
                                  source_key,
                                  tenant_code,
                                  site_code,
                                  camera_code,
                                  ftp_root,
                                  ftp_subpath,
                                  file_prefix,
                                  recipient_id,
                                  fallback_recipient_id,
                                  is_active
                                )
                                SELECT
                                  'cam:' || c.id::text AS source_key,
                                  COALESCE(t.code, 'default') AS tenant_code,
                                  COALESCE(s.code, 'default') AS site_code,
                                  c.camera_code,
                                  '/source' AS ftp_root,
                                  '' AS ftp_subpath,
                                  c.file_prefix,
                                  (
                                    SELECT cr1.recipient_id
                                    FROM camera_routes cr1
                                    WHERE cr1.camera_id = c.id
                                      AND cr1.is_active = true
                                    ORDER BY cr1.priority ASC, cr1.id ASC
                                    LIMIT 1
                                  ) AS recipient_id,
                                  COALESCE(s.default_recipient_id, t.default_recipient_id) AS fallback_recipient_id,
                                  true AS is_active
                                FROM events e
                                JOIN cameras c
                                  ON c.id = e.camera_id
                                LEFT JOIN sites s
                                  ON s.id = c.site_id
                                LEFT JOIN tenants t
                                  ON t.id = c.tenant_id
                                WHERE e.event_key = %s
                                  AND e.camera_id IS NOT NULL
                                ON CONFLICT (source_key) DO NOTHING
                                """,
                                (event_key,),
                            )
                            cur.execute(
                                """
                                INSERT INTO photo_events (
                                  id,
                                  event_key,
                                  source_id,
                                  file_name,
                                  full_path,
                                  file_size,
                                  checksum_sha256,
                                  status,
                                  attempt_count,
                                  next_retry_at,
                                  last_attempt_at,
                                  quarantine_reason,
                                  last_error,
                                  provider_message_ref,
                                  sent_at,
                                  delivered_at,
                                  ftp_removed_at,
                                  first_seen_at,
                                  updated_at
                                )
                                SELECT
                                  e.id,
                                  e.event_key,
                                  ps.id AS source_id,
                                  e.file_name,
                                  e.full_path,
                                  e.file_size,
                                  e.checksum_sha256,
                                  e.status,
                                  COALESCE(e.attempt_count, 0) AS attempt_count,
                                  e.next_retry_at,
                                  e.last_attempt_at,
                                  e.quarantine_reason,
                                  e.last_error,
                                  e.provider_message_ref,
                                  e.sent_at,
                                  NULL::timestamptz AS delivered_at,
                                  e.ftp_removed_at,
                                  COALESCE(e.first_seen_at, now()) AS first_seen_at,
                                  now() AS updated_at
                                FROM events e
                                JOIN photo_sources ps
                                  ON ps.source_key = 'cam:' || e.camera_id::text
                                WHERE e.event_key = %s
                                ON CONFLICT (event_key) DO NOTHING
                                """,
                                (event_key,),
                            )
                finally:

                    conn.close()


        time.sleep(5)


    except Exception as e:

        print("watcher error:",e,flush=True)

        time.sleep(5)

