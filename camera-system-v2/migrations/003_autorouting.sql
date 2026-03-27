create table if not exists ftp_sources (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  site_id uuid references sites(id) on delete set null,
  root_path text not null unique,
  auto_register_cameras boolean not null default true,
  created_at timestamptz not null default now()
);

alter table tenants
  add column if not exists default_recipient_id uuid references recipients(id);

alter table sites
  add column if not exists default_recipient_id uuid references recipients(id);

create index if not exists idx_ftp_sources_tenant on ftp_sources(tenant_id);
create index if not exists idx_cameras_tenant_code on cameras(tenant_id, camera_code);
