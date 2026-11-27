begin;
alter table public.users add column if not exists auto_filters jsonb;
commit;
