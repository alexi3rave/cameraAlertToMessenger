create extension if not exists "uuid-ossp";

create table if not exists tenants (
  id uuid primary key,
  code text unique not null,
  name text not null,
  status text not null default 'active',
  created_at timestamptz default now()
);

create table if not exists sites (
  id uuid primary key,
  tenant_id uuid references tenants(id),
  code text not null,
  name text not null,
  created_at timestamptz default now()
);

create table if not exists cameras (
  id uuid primary key,
  tenant_id uuid references tenants(id),
  site_id uuid references sites(id),
  camera_code text not null,
  file_prefix text,
  created_at timestamptz default now()
);

create table if not exists events (
  id uuid primary key,
  event_key text unique not null,
  file_name text not null,
  full_path text not null,
  file_size bigint,
  status text not null,
  first_seen_at timestamptz default now()
);

create index idx_events_status on events(status);

