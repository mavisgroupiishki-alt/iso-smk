-- Обновление базы для календарного отображения и импорта Excel-расписания
alter table public.tasks add column if not exists start_time time;
alter table public.tasks add column if not exists end_time time;
alter table public.tasks add column if not exists block text default '';

-- Переименование старого участника. Новые сотрудники теперь управляются из дашборда.
update public.tasks
set owner = 'Саша'
where owner = 'Алиса';
