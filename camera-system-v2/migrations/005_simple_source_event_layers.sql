-- Phase A: add simplified 2-layer model (source -> event), no runtime switch yet

create table if not exists photo_sources (
  id uuid primary key default gen_random_uuid(),
  source_key text unique not null,
  tenant_code text not null,
  site_code text not null,
  camera_code text not null,
  ftp_root text not null,
  ftp_subpath text not null default '',
  file_prefix text,
  recipient_id uuid references recipients(id),
  fallback_recipient_id uuid references recipients(id),
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_photo_sources_active on photo_sources(is_active);
create index if not exists idx_photo_sources_lookup
  on photo_sources(ftp_root, ftp_subpath, camera_code);

create table if not exists photo_events (
  id uuid primary key default gen_random_uuid(),
  event_key text unique not null,
  source_id uuid not null references photo_sources(id),
  file_name text not null,
  full_path text not null,
  file_size bigint,
  checksum_sha256 text,
  status text not null check (
    status in ('discovered','ready','processing','sent','failed_retryable','quarantine')
  ),
  attempt_count int not null default 0,
  next_retry_at timestamptz,
  last_attempt_at timestamptz,
  quarantine_reason text,
  last_error text,
  provider_message_ref text,
  sent_at timestamptz,
  delivered_at timestamptz,
  ftp_removed_at timestamptz,
  first_seen_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_photo_events_status on photo_events(status);
create index if not exists idx_photo_events_retry on photo_events(status, next_retry_at);
create index if not exists idx_photo_events_source on photo_events(source_id, first_seen_at);

create table if not exists photo_event_attempts (
  id uuid primary key default gen_random_uuid(),
  photo_event_id uuid not null references photo_events(id) on delete cascade,
  attempt_no int not null,
  recipient_id uuid references recipients(id),
  delivery_mode text not null check (delivery_mode in ('dry_run','shadow','prod')),
  delivery_status text not null check (delivery_status in ('started','sent','failed')),
  request_trace_id text,
  provider_response_code text,
  error_text text,
  started_at timestamptz not null default now(),
  finished_at timestamptz
);

create index if not exists idx_photo_attempts_event
  on photo_event_attempts(photo_event_id, attempt_no desc);
