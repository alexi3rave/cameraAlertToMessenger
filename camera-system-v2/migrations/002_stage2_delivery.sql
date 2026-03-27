create table if not exists recipients (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  recipient_type text not null check (recipient_type in ('chat', 'user')),
  max_chat_id text,
  max_user_id text,
  display_name text,
  is_primary boolean not null default false,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists camera_routes (
  id uuid primary key default gen_random_uuid(),
  camera_id uuid not null references cameras(id) on delete cascade,
  recipient_id uuid not null references recipients(id) on delete cascade,
  is_active boolean not null default true,
  priority int not null default 100,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (camera_id, recipient_id)
);

create table if not exists deliveries (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null references events(id) on delete cascade,
  recipient_id uuid not null references recipients(id) on delete cascade,
  attempt_no int not null,
  delivery_mode text not null check (delivery_mode in ('dry_run','shadow','prod')),
  delivery_status text not null check (delivery_status in ('pending','started','sent','failed')),
  request_trace_id text,
  provider_upload_ref text,
  provider_message_ref text,
  provider_response_code text,
  error_text text,
  started_at timestamptz not null default now(),
  finished_at timestamptz
);

alter table events
  add column if not exists camera_id uuid references cameras(id),
  add column if not exists camera_code text,
  add column if not exists recipient_id uuid references recipients(id),
  add column if not exists attempt_count int not null default 0,
  add column if not exists last_attempt_at timestamptz,
  add column if not exists sent_at timestamptz,
  add column if not exists last_error text,
  add column if not exists provider_upload_ref text,
  add column if not exists provider_message_ref text;

create index if not exists idx_events_status on events(status);
create index if not exists idx_events_camera_id on events(camera_id);
create index if not exists idx_events_recipient_id on events(recipient_id);
create index if not exists idx_deliveries_event_id on deliveries(event_id);
create index if not exists idx_routes_camera_active on camera_routes(camera_id, is_active);
create index if not exists idx_recipients_tenant_active on recipients(tenant_id, is_active);
