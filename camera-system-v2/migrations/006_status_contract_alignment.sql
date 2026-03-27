-- Align runtime statuses with approved contracts.
-- events: discovered/ready/processing/sent/failed_retryable/quarantine
-- deliveries: pending/started/sent/failed

begin;

update deliveries
set delivery_status = 'pending'
where delivery_status = 'uploaded';

update deliveries
set delivery_status = 'failed'
where delivery_status in ('failed_retryable', 'failed_terminal');

update events
set status = 'quarantine',
    quarantine_reason = coalesce(quarantine_reason, 'legacy_failed_final')
where status = 'failed_final';

update photo_events
set status = 'quarantine',
    quarantine_reason = coalesce(quarantine_reason, 'legacy_failed_final')
where status = 'failed_final';

-- Rebuild deliveries status check to canonical set.
do $$
declare
  c_name text;
begin
  for c_name in
    select conname
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    where t.relname = 'deliveries'
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%delivery_status%'
  loop
    execute format('alter table deliveries drop constraint %I', c_name);
  end loop;

  alter table deliveries
    add constraint deliveries_delivery_status_check
    check (delivery_status in ('pending','started','sent','failed'));
end $$;

-- Rebuild photo_events status check to canonical set.
do $$
declare
  c_name text;
begin
  for c_name in
    select conname
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    where t.relname = 'photo_events'
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%status in%'
  loop
    execute format('alter table photo_events drop constraint %I', c_name);
  end loop;

  alter table photo_events
    add constraint photo_events_status_check
    check (status in ('discovered','ready','processing','sent','failed_retryable','quarantine'));
end $$;

commit;
