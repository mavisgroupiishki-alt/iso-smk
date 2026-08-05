-- MAVIS Task Tracker v5.0
-- Фильтры, архив проектов, шаблоны и безопасное редактирование команды.
-- Миграция НЕ удаляет существующие проекты, этапы, задачи, комментарии, ссылки и историю переносов.
-- Выполнить один раз: Supabase → SQL Editor → New query → вставить весь файл → Run.

create extension if not exists "pgcrypto";

-- Контроль количества данных до обновления.
drop table if exists pg_temp._mavis_v5_before;
create temporary table _mavis_v5_before as
select
  (select count(*) from public.projects) as projects_before,
  (select count(*) from public.project_stages) as stages_before,
  (select count(*) from public.tasks) as tasks_before,
  (select count(*) from public.employees) as employees_before;

-- 1. Архив проектов. По умолчанию все существующие проекты остаются активными.
alter table public.projects
  add column if not exists archived boolean not null default false;

alter table public.projects
  add column if not exists archived_at timestamptz;

update public.projects
set archived = false
where archived is null;

create index if not exists projects_archived_idx on public.projects(archived);
create index if not exists projects_section_archived_idx on public.projects(section_id, archived);
create index if not exists projects_customer_v5_idx on public.projects(customer);

-- 2. Шаблоны проектов.
-- template_data хранит структуру этапов и типовых задач в формате JSON.
create table if not exists public.project_templates (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  description text not null default '',
  owner text not null default 'Саша',
  customer text not null default '',
  section_id uuid references public.task_sections(id) on delete set null,
  color text not null default '#c4b5fd',
  template_data jsonb not null default '{"project_deadline_offset":14,"stages":[],"tasks":[]}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create or replace function public.set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists project_templates_set_updated_at on public.project_templates;
create trigger project_templates_set_updated_at
before update on public.project_templates
for each row execute function public.set_updated_at();

create index if not exists project_templates_owner_idx on public.project_templates(owner);
create index if not exists project_templates_section_idx on public.project_templates(section_id);

alter table public.project_templates enable row level security;

drop policy if exists "allow_read_project_templates" on public.project_templates;
create policy "allow_read_project_templates" on public.project_templates
for select using (true);

drop policy if exists "allow_insert_project_templates" on public.project_templates;
create policy "allow_insert_project_templates" on public.project_templates
for insert with check (true);

drop policy if exists "allow_update_project_templates" on public.project_templates;
create policy "allow_update_project_templates" on public.project_templates
for update using (true) with check (true);

drop policy if exists "allow_delete_project_templates" on public.project_templates;
create policy "allow_delete_project_templates" on public.project_templates
for delete using (true);

-- 3. Индексы для новых глобальных фильтров.
create index if not exists tasks_priority_v5_idx on public.tasks(priority);
create index if not exists tasks_status_v5_idx on public.tasks(status);
create index if not exists tasks_owner_v5_idx on public.tasks(owner);
create index if not exists tasks_deadline_v5_idx on public.tasks(deadline);

-- REST API Supabase перечитывает новую схему.
notify pgrst, 'reload schema';

-- Контрольный отчёт: before и after должны совпасть.
select
  before_data.projects_before,
  (select count(*) from public.projects) as projects_after,
  before_data.stages_before,
  (select count(*) from public.project_stages) as stages_after,
  before_data.tasks_before,
  (select count(*) from public.tasks) as tasks_after,
  before_data.employees_before,
  (select count(*) from public.employees) as employees_after,
  exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'projects' and column_name = 'archived'
  ) as archive_ready,
  exists (
    select 1 from information_schema.tables
    where table_schema = 'public' and table_name = 'project_templates'
  ) as templates_ready
from _mavis_v5_before as before_data;
