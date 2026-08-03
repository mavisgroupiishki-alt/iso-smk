create extension if not exists "pgcrypto";

create table if not exists public.tasks (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  owner text not null,
  deadline date not null,
  period text not null default 'day',
  status text not null default 'Новая',
  priority text not null default 'Средний',
  hours numeric not null default 1,
  start_time time,
  end_time time,
  block text default '',
  result text default '',
  comment text not null default '',
  resource_url text not null default '',
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

drop trigger if exists tasks_set_updated_at on public.tasks;
create trigger tasks_set_updated_at
before update on public.tasks
for each row execute function public.set_updated_at();

alter table public.tasks enable row level security;

-- MVP-режим: доступ всем, у кого есть ссылка на приложение.
-- После запуска можно заменить на авторизацию и роли.
drop policy if exists "allow_read_tasks" on public.tasks;
create policy "allow_read_tasks" on public.tasks for select using (true);

drop policy if exists "allow_insert_tasks" on public.tasks;
create policy "allow_insert_tasks" on public.tasks for insert with check (true);

drop policy if exists "allow_update_tasks" on public.tasks;
create policy "allow_update_tasks" on public.tasks for update using (true) with check (true);

drop policy if exists "allow_delete_tasks" on public.tasks;
create policy "allow_delete_tasks" on public.tasks for delete using (true);

-- Сотрудники дашборда
create table if not exists public.employees (
  id uuid primary key default gen_random_uuid(),
  name text not null unique,
  role text not null default 'Сотрудник',
  color text not null default '#7c3aed',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists employees_set_updated_at on public.employees;
create trigger employees_set_updated_at
before update on public.employees
for each row execute function public.set_updated_at();

insert into public.employees (name, role, color)
values
  ('Саша', 'Руководитель отдела продаж', '#7c3aed'),
  ('Таня', 'Руководитель экспертного отдела', '#0284c7'),
  ('Аня', 'Проекты и автоматизация', '#ea580c'),
  ('Виктория', 'Директор', '#059669')
on conflict (name) do nothing;

alter table public.employees enable row level security;

drop policy if exists "allow_read_employees" on public.employees;
create policy "allow_read_employees" on public.employees for select using (true);

drop policy if exists "allow_insert_employees" on public.employees;
create policy "allow_insert_employees" on public.employees for insert with check (true);

drop policy if exists "allow_update_employees" on public.employees;
create policy "allow_update_employees" on public.employees for update using (true) with check (true);

drop policy if exists "allow_delete_employees" on public.employees;
create policy "allow_delete_employees" on public.employees for delete using (true);
-- MAVIS Task Tracker: проекты, этапы, комментарии и история переносов
-- Запустите этот файл один раз в Supabase → SQL Editor → Run.

create extension if not exists "pgcrypto";

create or replace function public.set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

-- 1. Проекты
create table if not exists public.projects (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  description text not null default '',
  owner text not null default 'Саша',
  deadline date,
  status text not null default 'В работе',
  color text not null default '#7c3aed',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists projects_set_updated_at on public.projects;
create trigger projects_set_updated_at
before update on public.projects
for each row execute function public.set_updated_at();

-- 2. Этапы внутри проекта
create table if not exists public.project_stages (
  id uuid primary key default gen_random_uuid(),
  project_id uuid not null references public.projects(id) on delete cascade,
  title text not null,
  description text not null default '',
  owner text not null default 'Саша',
  deadline date,
  sort_order integer not null default 1,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists project_stages_set_updated_at on public.project_stages;
create trigger project_stages_set_updated_at
before update on public.project_stages
for each row execute function public.set_updated_at();

-- 3. Привязка существующих задач к проектам/этапам и комментарий
alter table public.tasks add column if not exists project_id uuid references public.projects(id) on delete set null;
alter table public.tasks add column if not exists stage_id uuid references public.project_stages(id) on delete set null;
alter table public.tasks add column if not exists comment text not null default '';

-- Алиса в старых данных становится Сашей
update public.tasks set owner = 'Саша' where owner = 'Алиса';

-- 4. История каждого изменения дедлайна
create table if not exists public.task_reschedules (
  id uuid primary key default gen_random_uuid(),
  task_id uuid references public.tasks(id) on delete set null,
  task_title text not null,
  project_id uuid references public.projects(id) on delete set null,
  stage_id uuid references public.project_stages(id) on delete set null,
  old_deadline date not null,
  new_deadline date not null,
  changed_at timestamptz not null default now(),
  changed_by text not null default 'Саша',
  reason text not null default ''
);

create index if not exists projects_owner_idx on public.projects(owner);
create index if not exists projects_deadline_idx on public.projects(deadline);
create index if not exists project_stages_project_idx on public.project_stages(project_id, sort_order);
create index if not exists tasks_project_idx on public.tasks(project_id);
create index if not exists tasks_stage_idx on public.tasks(stage_id);
create index if not exists task_reschedules_old_deadline_idx on public.task_reschedules(old_deadline);
create index if not exists task_reschedules_new_deadline_idx on public.task_reschedules(new_deadline);
create index if not exists task_reschedules_changed_at_idx on public.task_reschedules(changed_at desc);

-- 5. MVP-политики доступа: любой пользователь приложения может работать с данными.
-- Позже можно заменить на авторизацию и роли.
alter table public.projects enable row level security;
alter table public.project_stages enable row level security;
alter table public.task_reschedules enable row level security;


drop policy if exists "allow_read_projects" on public.projects;
create policy "allow_read_projects" on public.projects for select using (true);
drop policy if exists "allow_insert_projects" on public.projects;
create policy "allow_insert_projects" on public.projects for insert with check (true);
drop policy if exists "allow_update_projects" on public.projects;
create policy "allow_update_projects" on public.projects for update using (true) with check (true);
drop policy if exists "allow_delete_projects" on public.projects;
create policy "allow_delete_projects" on public.projects for delete using (true);

drop policy if exists "allow_read_project_stages" on public.project_stages;
create policy "allow_read_project_stages" on public.project_stages for select using (true);
drop policy if exists "allow_insert_project_stages" on public.project_stages;
create policy "allow_insert_project_stages" on public.project_stages for insert with check (true);
drop policy if exists "allow_update_project_stages" on public.project_stages;
create policy "allow_update_project_stages" on public.project_stages for update using (true) with check (true);
drop policy if exists "allow_delete_project_stages" on public.project_stages;
create policy "allow_delete_project_stages" on public.project_stages for delete using (true);

drop policy if exists "allow_read_task_reschedules" on public.task_reschedules;
create policy "allow_read_task_reschedules" on public.task_reschedules for select using (true);
drop policy if exists "allow_insert_task_reschedules" on public.task_reschedules;
create policy "allow_insert_task_reschedules" on public.task_reschedules for insert with check (true);
drop policy if exists "allow_update_task_reschedules" on public.task_reschedules;
create policy "allow_update_task_reschedules" on public.task_reschedules for update using (true) with check (true);
drop policy if exists "allow_delete_task_reschedules" on public.task_reschedules;
create policy "allow_delete_task_reschedules" on public.task_reschedules for delete using (true);

