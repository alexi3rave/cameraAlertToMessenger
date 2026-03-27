-- FTP lifecycle: учёт удаления файла после TTL (воркер удаляет на диске и проставляет ftp_removed_at)

alter table events
  add column if not exists ftp_removed_at timestamptz;

create index if not exists idx_events_ftp_cleanup
  on events (first_seen_at)
  where ftp_removed_at is null;

comment on column events.ftp_removed_at is 'Когда файл по full_path удалён job-ом очистки FTP';
