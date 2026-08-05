-- MAVIS Task Tracker 6.0
-- Авторизация сотрудников, роль администратора Ани и контроль нормы активных задач.
-- НИЧЕГО не удаляет из проектов, этапов, задач, разделов и истории.
--
-- ПЕРЕД ЗАПУСКОМ:
-- 1. Supabase → Authentication → Users → Add user.
-- 2. Email: anya@mavis.local
-- 3. Задайте пароль Ани и включите Auto Confirm User.
-- 4. Затем выполните этот SQL целиком.

create extension if not exists "pgcrypto";

-- 1. Дополняем сотрудников данными доступа и нормой загрузки.
alter table public.employees
  add column if not exists login text;

alter table public.employees
  add column if not exists auth_user_id uuid references auth.users(id) on delete set null;

alter table public.employees
  add column if not exists task_capacity integer not null default 10;

alter table public.employees
  add column if not exists is_active boolean not null default true;

update public.employees
set task_capacity = 10
where task_capacity is null or task_capacity < 1;

create unique index if not exists employees_login_unique_idx
  on public.employees (lower(trim(login)))
  where login is not null and trim(login) <> '';

create unique index if not exists employees_auth_user_unique_idx
  on public.employees (auth_user_id)
  where auth_user_id is not null;

-- 2. Профили авторизации приложения.
create table if not exists public.app_users (
  auth_user_id uuid primary key references auth.users(id) on delete cascade,
  employee_id uuid not null unique references public.employees(id) on delete cascade,
  login text not null,
  internal_email text not null unique,
  is_admin boolean not null default false,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists app_users_login_unique_idx
  on public.app_users (lower(trim(login)));

create or replace function public.touch_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists app_users_touch_updated_at on public.app_users;
create trigger app_users_touch_updated_at
before update on public.app_users
for each row execute function public.touch_updated_at();

-- 3. Служебные функции прав.
create or replace function public.is_active_app_user()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.app_users u
    where u.auth_user_id = auth.uid()
      and u.is_active = true
  );
$$;

create or replace function public.is_app_admin()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from public.app_users u
    where u.auth_user_id = auth.uid()
      and u.is_active = true
      and u.is_admin = true
  );
$$;

-- Вызывается до входа. Возвращает только внутренний технический email.
create or replace function public.resolve_login_email(p_login text)
returns text
language sql
stable
security definer
set search_path = public
as $$
  select u.internal_email
  from public.app_users u
  where lower(trim(u.login)) = lower(trim(coalesce(p_login, '')))
    and u.is_active = true
  limit 1;
$$;

create or replace function public.current_app_user()
returns table (
  auth_user_id uuid,
  employee_id uuid,
  employee_name text,
  employee_role text,
  employee_color text,
  login text,
  task_capacity integer,
  is_admin boolean,
  is_active boolean
)
language sql
stable
security definer
set search_path = public
as $$
  select
    u.auth_user_id,
    e.id,
    e.name,
    e.role,
    e.color,
    u.login,
    e.task_capacity,
    u.is_admin,
    u.is_active
  from public.app_users u
  join public.employees e on e.id = u.employee_id
  where u.auth_user_id = auth.uid()
    and u.is_active = true
  limit 1;
$$;

-- 4. Создаём/связываем администратора Аню.
do $$
declare
  v_auth_user_id uuid;
  v_employee_id uuid;
begin
  select id into v_auth_user_id
  from auth.users
  where lower(email) = lower('anya@mavis.local')
  limit 1;

  if v_auth_user_id is null then
    raise exception 'Сначала создайте пользователя anya@mavis.local в Authentication → Users';
  end if;

  select id into v_employee_id
  from public.employees
  where lower(trim(name)) = lower('Аня')
  order by created_at asc
  limit 1;

  if v_employee_id is null then
    insert into public.employees (name, role, color, login, auth_user_id, task_capacity, is_active)
    values ('Аня', 'Администратор проектов и автоматизации', '#E4C1F9', 'Аня', v_auth_user_id, 12, true)
    returning id into v_employee_id;
  else
    update public.employees
    set
      login = 'Аня',
      auth_user_id = v_auth_user_id,
      task_capacity = greatest(coalesce(task_capacity, 12), 1),
      is_active = true
    where id = v_employee_id;
  end if;

  insert into public.app_users (
    auth_user_id, employee_id, login, internal_email, is_admin, is_active
  )
  values (
    v_auth_user_id, v_employee_id, 'Аня', 'anya@mavis.local', true, true
  )
  on conflict (auth_user_id) do update
  set
    employee_id = excluded.employee_id,
    login = excluded.login,
    internal_email = excluded.internal_email,
    is_admin = true,
    is_active = true;
end $$;

-- 5. Политики доступа. Без входа данные приложения недоступны.
alter table public.app_users enable row level security;
alter table public.employees enable row level security;
alter table public.task_sections enable row level security;
alter table public.projects enable row level security;
alter table public.project_stages enable row level security;
alter table public.tasks enable row level security;
alter table public.task_reschedules enable row level security;
alter table public.project_templates enable row level security;

-- Удаляем прежние открытые политики только на таблицах приложения.
do $$
declare
  r record;
begin
  for r in
    select schemaname, tablename, policyname
    from pg_policies
    where schemaname = 'public'
      and tablename in (
        'app_users', 'employees', 'task_sections', 'projects',
        'project_stages', 'tasks', 'task_reschedules', 'project_templates'
      )
  loop
    execute format('drop policy if exists %I on %I.%I', r.policyname, r.schemaname, r.tablename);
  end loop;
end $$;

-- Профиль: сотрудник видит себя, Аня — всех.
create policy "app_users_select_self_or_admin"
on public.app_users for select to authenticated
using (auth_user_id = auth.uid() or public.is_app_admin());

create policy "app_users_admin_write"
on public.app_users for all to authenticated
using (public.is_app_admin())
with check (public.is_app_admin());

-- Команда: читать могут все вошедшие, менять только Аня.
create policy "employees_authenticated_read"
on public.employees for select to authenticated
using (public.is_active_app_user());

create policy "employees_admin_insert"
on public.employees for insert to authenticated
with check (public.is_app_admin());

create policy "employees_admin_update"
on public.employees for update to authenticated
using (public.is_app_admin())
with check (public.is_app_admin());

create policy "employees_admin_delete"
on public.employees for delete to authenticated
using (public.is_app_admin());

-- Разделы: читать может вся команда, менять только Аня.
create policy "sections_authenticated_read"
on public.task_sections for select to authenticated
using (public.is_active_app_user());

create policy "sections_admin_insert"
on public.task_sections for insert to authenticated
with check (public.is_app_admin());

create policy "sections_admin_update"
on public.task_sections for update to authenticated
using (public.is_app_admin())
with check (public.is_app_admin());

create policy "sections_admin_delete"
on public.task_sections for delete to authenticated
using (public.is_app_admin());

-- Рабочие данные: вся активная команда может читать и изменять.
do $$
declare
  table_name text;
begin
  foreach table_name in array array[
    'projects', 'project_stages', 'tasks', 'task_reschedules', 'project_templates'
  ]
  loop
    execute format(
      'create policy %I on public.%I for select to authenticated using (public.is_active_app_user())',
      table_name || '_authenticated_select', table_name
    );
    execute format(
      'create policy %I on public.%I for insert to authenticated with check (public.is_active_app_user())',
      table_name || '_authenticated_insert', table_name
    );
    execute format(
      'create policy %I on public.%I for update to authenticated using (public.is_active_app_user()) with check (public.is_active_app_user())',
      table_name || '_authenticated_update', table_name
    );
    execute format(
      'create policy %I on public.%I for delete to authenticated using (public.is_active_app_user())',
      table_name || '_authenticated_delete', table_name
    );
  end loop;
end $$;

-- Права API.
revoke all on public.app_users from anon;
revoke all on public.employees from anon;
revoke all on public.task_sections from anon;
revoke all on public.projects from anon;
revoke all on public.project_stages from anon;
revoke all on public.tasks from anon;
revoke all on public.task_reschedules from anon;
revoke all on public.project_templates from anon;

grant select, insert, update, delete on public.app_users to authenticated;
grant select, insert, update, delete on public.employees to authenticated;
grant select, insert, update, delete on public.task_sections to authenticated;
grant select, insert, update, delete on public.projects to authenticated;
grant select, insert, update, delete on public.project_stages to authenticated;
grant select, insert, update, delete on public.tasks to authenticated;
grant select, insert, update, delete on public.task_reschedules to authenticated;
grant select, insert, update, delete on public.project_templates to authenticated;

grant execute on function public.resolve_login_email(text) to anon, authenticated;
grant execute on function public.current_app_user() to authenticated;
grant execute on function public.is_active_app_user() to authenticated;
grant execute on function public.is_app_admin() to authenticated;

notify pgrst, 'reload schema';

select
  exists (select 1 from public.app_users where login = 'Аня' and is_admin = true) as anya_admin_ready,
  exists (select 1 from information_schema.columns where table_schema='public' and table_name='employees' and column_name='task_capacity') as capacity_ready,
  (select count(*) from public.projects) as projects_kept,
  (select count(*) from public.tasks) as tasks_kept;
