begin;

create or replace function _norm_path(p text)
returns text
language sql
immutable
as $$
  select case
    when p is null or btrim(p) = '' then ''
    else regexp_replace(btrim(p), '/+$', '')
  end;
$$;

create or replace function trg_ftp_sources_no_overlap()
returns trigger
language plpgsql
as $$
declare
  new_root text;
begin
  new_root := _norm_path(new.root_path);
  if new_root = '' then
    raise exception 'ftp_sources.root_path must not be empty';
  end if;

  if exists (
    select 1
    from ftp_sources fs
    where fs.id <> coalesce(new.id, '00000000-0000-0000-0000-000000000000'::uuid)
      and (
        _norm_path(fs.root_path) = new_root
        or _norm_path(fs.root_path) like new_root || '/%'
        or new_root like _norm_path(fs.root_path) || '/%'
      )
  ) then
    raise exception 'ftp_sources.root_path overlaps existing root: %', new.root_path;
  end if;

  return new;
end;
$$;

drop trigger if exists ftp_sources_no_overlap on ftp_sources;
create trigger ftp_sources_no_overlap
before insert or update of root_path
on ftp_sources
for each row
execute function trg_ftp_sources_no_overlap();

create or replace function trg_camera_routes_tenant_guard()
returns trigger
language plpgsql
as $$
declare
  cam_tenant uuid;
  rec_tenant uuid;
begin
  select c.tenant_id into cam_tenant
  from cameras c
  where c.id = new.camera_id;

  if cam_tenant is null then
    raise exception 'camera_routes camera_id has no tenant: %', new.camera_id;
  end if;

  select r.tenant_id into rec_tenant
  from recipients r
  where r.id = new.recipient_id;

  if rec_tenant is null then
    raise exception 'camera_routes recipient_id has no tenant: %', new.recipient_id;
  end if;

  if cam_tenant <> rec_tenant then
    raise exception 'cross-tenant camera_routes is not allowed (camera tenant %, recipient tenant %)', cam_tenant, rec_tenant;
  end if;

  return new;
end;
$$;

drop trigger if exists camera_routes_tenant_guard on camera_routes;
create trigger camera_routes_tenant_guard
before insert or update of camera_id, recipient_id
on camera_routes
for each row
execute function trg_camera_routes_tenant_guard();

create or replace function trg_sites_default_recipient_tenant_guard()
returns trigger
language plpgsql
as $$
declare
  rec_tenant uuid;
begin
  if new.default_recipient_id is null then
    return new;
  end if;

  select r.tenant_id into rec_tenant
  from recipients r
  where r.id = new.default_recipient_id;

  if rec_tenant is null then
    raise exception 'sites.default_recipient_id not found: %', new.default_recipient_id;
  end if;

  if rec_tenant <> new.tenant_id then
    raise exception 'cross-tenant sites.default_recipient_id is not allowed (site tenant %, recipient tenant %)', new.tenant_id, rec_tenant;
  end if;

  return new;
end;
$$;

drop trigger if exists sites_default_recipient_tenant_guard on sites;
create trigger sites_default_recipient_tenant_guard
before insert or update of tenant_id, default_recipient_id
on sites
for each row
execute function trg_sites_default_recipient_tenant_guard();

create or replace function trg_tenants_default_recipient_tenant_guard()
returns trigger
language plpgsql
as $$
declare
  rec_tenant uuid;
begin
  if new.default_recipient_id is null then
    return new;
  end if;

  select r.tenant_id into rec_tenant
  from recipients r
  where r.id = new.default_recipient_id;

  if rec_tenant is null then
    raise exception 'tenants.default_recipient_id not found: %', new.default_recipient_id;
  end if;

  if rec_tenant <> new.id then
    raise exception 'cross-tenant tenants.default_recipient_id is not allowed (tenant %, recipient tenant %)', new.id, rec_tenant;
  end if;

  return new;
end;
$$;

drop trigger if exists tenants_default_recipient_tenant_guard on tenants;
create trigger tenants_default_recipient_tenant_guard
before insert or update of default_recipient_id
on tenants
for each row
execute function trg_tenants_default_recipient_tenant_guard();

commit;
