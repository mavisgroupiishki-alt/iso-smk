-- MAVIS Task Tracker: отдельная рабочая ссылка в каждой задаче
-- Выполните один раз в Supabase → SQL Editor → New query → Run.

alter table public.tasks
  add column if not exists resource_url text not null default '';

-- Если ссылка раньше была вставлена прямо в комментарий,
-- первая найденная HTTP/HTTPS-ссылка автоматически появится в поле материалов.
update public.tasks
set resource_url = (regexp_match(comment, '(https?://[^\s]+)'))[1]
where coalesce(resource_url, '') = ''
  and coalesce(comment, '') ~ '(https?://[^\s]+)';

comment on column public.tasks.resource_url is
  'Рабочая ссылка задачи: Google Sheets, Google Docs, Drive, Miro или другой HTTP/HTTPS-ресурс';

select
  count(*) as всего_задач,
  count(*) filter (where coalesce(resource_url, '') <> '') as задач_со_ссылками
from public.tasks;
