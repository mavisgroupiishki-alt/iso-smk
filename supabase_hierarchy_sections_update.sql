-- MAVIS Task Tracker: иерархия «раздел → проект → этап → задача»
-- БЕЗОПАСНАЯ МИГРАЦИЯ: не удаляет и не перезаписывает существующие задачи,
-- проекты, этапы, комментарии, ссылки, сроки и историю переносов.
-- Выполнить один раз: Supabase → SQL Editor → New query → Run.

create extension if not exists "pgcrypto";

-- Запоминаем количество данных до миграции только для контрольного отчёта.
drop table if exists pg_temp._mavis_hierarchy_before;
create temporary table _mavis_hierarchy_before as
select
  (select count(*) from public.projects) as projects_before,
  (select count(*) from public.project_stages) as stages_before,
  (select count(*) from public.tasks) as tasks_before;

-- 1. Заказчик и ответственный проекта остаются отдельными полями.
alter table public.projects
  add column if not exists owner text not null default 'Саша';

alter table public.projects
  add column if not exists customer text not null default '';

-- 2. Таблица разделов. Раздел теперь объединяет ПРОЕКТЫ одного направления.
create table if not exists public.task_sections (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  description text not null default '',
  owner text not null default 'Саша',
  color text not null default '#0f766e',
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

drop trigger if exists task_sections_set_updated_at on public.task_sections;
create trigger task_sections_set_updated_at
before update on public.task_sections
for each row execute function public.set_updated_at();

-- 3. Главная новая связь: каждый проект можно поместить в раздел.
-- NULL означает «Проект без раздела» и не мешает существующим данным.
alter table public.projects
  add column if not exists section_id uuid references public.task_sections(id) on delete set null;

create index if not exists projects_section_idx on public.projects(section_id);
create index if not exists projects_customer_idx on public.projects(customer);
create index if not exists task_sections_owner_idx on public.task_sections(owner);

-- 4. Сохраняем совместимость со старыми отдельными задачами разделов.
-- Приложение использует эту связь только для задачи без проекта.
alter table public.tasks
  add column if not exists section_id uuid references public.task_sections(id) on delete set null;

create index if not exists tasks_section_idx on public.tasks(section_id);

-- 5. Безопасный автоперенос старой структуры.
-- Если все задачи конкретного проекта были привязаны к одному и тому же разделу,
-- этот раздел назначается самому проекту. Задачи при этом НЕ изменяются.
with project_section_candidates as (
  select
    project_id,
    min(section_id::text)::uuid as section_id
  from public.tasks
  where project_id is not null
    and section_id is not null
  group by project_id
  having count(distinct section_id) = 1
)
update public.projects as p
set section_id = candidate.section_id
from project_section_candidates as candidate
where p.id = candidate.project_id
  and p.section_id is null;

-- 6. Политики текущей модели доступа приложения.
alter table public.task_sections enable row level security;

drop policy if exists "allow_read_task_sections" on public.task_sections;
create policy "allow_read_task_sections" on public.task_sections
for select using (true);

drop policy if exists "allow_insert_task_sections" on public.task_sections;
create policy "allow_insert_task_sections" on public.task_sections
for insert with check (true);

drop policy if exists "allow_update_task_sections" on public.task_sections;
create policy "allow_update_task_sections" on public.task_sections
for update using (true) with check (true);

drop policy if exists "allow_delete_task_sections" on public.task_sections;
create policy "allow_delete_task_sections" on public.task_sections
for delete using (true);

-- Просим REST API Supabase сразу перечитать схему.
notify pgrst, 'reload schema';

-- Контрольный отчёт. Числа before и after должны совпасть.
select
  before_data.projects_before,
  (select count(*) from public.projects) as projects_after,
  before_data.stages_before,
  (select count(*) from public.project_stages) as stages_after,
  before_data.tasks_before,
  (select count(*) from public.tasks) as tasks_after,
  (select count(*) from public.task_sections) as sections_total,
  (select count(*) from public.projects where section_id is not null) as projects_in_sections,
  exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'projects' and column_name = 'section_id'
  ) as project_section_ready,
  exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'projects' and column_name = 'customer'
  ) as customer_ready
from _mavis_hierarchy_before as before_data;
