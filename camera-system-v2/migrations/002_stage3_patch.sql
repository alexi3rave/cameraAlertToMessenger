alter table events
  add column if not exists locked_at timestamptz,
  add column if not exists lock_owner text,
  add column if not exists quarantine_reason text,
  add column if not exists next_retry_at timestamptz,
  add column if not exists provider_upload_ref text,
  add column if not exists provider_message_ref text,
  add column if not exists source_path text,
  add column if not exists checksum_sha256 text;

create index if not exists idx_events_status on events(status);
create index if not exists idx_events_next_retry_at on events(next_retry_at);
create index if not exists idx_events_locked_at on events(locked_at);
create index if not exists idx_events_camera_status on events(camera_id, status);

create table if not exists service_config (
  key text primary key,
  value_json jsonb not null,
  updated_at timestamptz not null default now()
);

insert into service_config(key, value_json)
values
  ('retry_policy', '{
    "max_attempts": 6,
    "schedule_seconds": [0,30,120,600,3600,21600]
  }'::jsonb),
  ('shadow_mode', '{"enabled": true}'::jsonb)
on conflict (key) do nothing;
