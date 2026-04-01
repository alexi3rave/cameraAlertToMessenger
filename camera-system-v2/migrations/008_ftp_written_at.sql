begin;

alter table events
  add column if not exists ftp_written_at timestamptz;

alter table photo_events
  add column if not exists ftp_written_at timestamptz;

update events
set ftp_written_at = coalesce(ftp_written_at, first_seen_at)
where ftp_written_at is null;

update photo_events
set ftp_written_at = coalesce(ftp_written_at, first_seen_at)
where ftp_written_at is null;

create index if not exists idx_events_ftp_written_at on events(ftp_written_at);

commit;
