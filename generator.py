"""
Умный генератор документов ИСО/СУОТ/СПК.
Все документы генерируются через ИИ с правильными данными компании,
датами от даты выезда эксперта, ФИО из штатного расписания.
"""
import os, json, re, zipfile, io
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests as req_lib

BASE_DIR = Path(__file__).parent.resolve()
VIBE_URL = "https://vibecode.bitrix24.tech/v1/ai/chat/completions"
VIBE_MODEL = "bitrix/bitrixgpt-5.5"

LIBS_PATH = BASE_DIR / 'libs.json'
LIBS = json.loads(LIBS_PATH.read_text('utf-8')) if LIBS_PATH.exists() else {'di': {}, 'ri': {}}


# ── Вспомогательные функции ───────────────────────────────────

def _fio(person):
    if not person: return ''
    return person.get('fio', '')

def _pos(person):
    if not person: return ''
    return person.get('position', '')

def _initials(fio: str) -> str:
    """Глушинский Олег Иванович → О.И. Глушинский"""
    parts = fio.strip().split()
    if len(parts) >= 2:
        surname = parts[0]
        inits = '.'.join(p[0] for p in parts[1:] if p) + '.'
        return f"{inits} {surname}"
    return fio

def _fmt_date(d: datetime) -> str:
    return d.strftime('%d.%m.%Y')

def calculate_dates(audit_date_str: str) -> dict:
    """Рассчитывает все даты от даты выезда эксперта"""
    audit = None
    for fmt in ('%d.%m.%Y', '%d/%m/%Y', '%Y-%m-%d'):
        try:
            audit = datetime.strptime(audit_date_str.strip(), fmt)
            break
        except: pass
    if not audit:
        audit = datetime.now() + timedelta(days=30)

    policy  = audit - timedelta(days=34)
    goals   = policy + timedelta(days=5)
    risks   = policy + timedelta(days=2)
    reports = audit - timedelta(days=7)

    return {
        'audit':    _fmt_date(audit),
        'policy':   _fmt_date(policy),
        'goals':    _fmt_date(goals),
        'risks':    _fmt_date(risks),
        'reports':  _fmt_date(reports),
        'year':     str(audit.year),
        'audit_obj': f"{_fmt_date(policy)} по {_fmt_date(reports)}",
    }


def select_responsible(itr: list) -> dict:
    """
    Выбирает ответственных из ИТР по правилам:
    - Директор = кто в должности содержит 'директор'
    - Аудиторы (3 чел) = директор + ИТР с ОТ-удостоверением
    - За процесс = гл. инженер / прораб / директор
    - За ДИ = кадровик / бухгалтер / директор
    """
    def has_kw(p, *kws):
        pos = p.get('position', '').lower()
        return any(k in pos for k in kws)

    director = next((p for p in itr if has_kw(p, 'директор')), itr[0] if itr else None)

    with_ot = sorted(
        [p for p in itr if p.get('ot_certificate')],
        key=lambda p: p.get('ot_certificate_date', ''),
        reverse=True
    )

    process_resp = next(
        (p for p in itr if has_kw(p, 'главный инженер', 'гл. инженер', 'прораб', 'производитель работ')),
        director
    )

    # Аудиторы: директор + 2 ОТ-шника (или просто первые ИТР)
    auditor_pool = [director] if director else []
    for p in with_ot:
        if p not in auditor_pool:
            auditor_pool.append(p)
        if len(auditor_pool) >= 3:
            break
    if len(auditor_pool) < 3:
        for p in itr:
            if p not in auditor_pool:
                auditor_pool.append(p)
            if len(auditor_pool) >= 3:
                break
    auditors = auditor_pool[:3]

    di_resp = next(
        (p for p in itr if has_kw(p, 'кадр', 'персонал')),
        next((p for p in itr if has_kw(p, 'бухгалтер')), director)
    )

    fnpa_resp = next(
        (p for p in itr if has_kw(p, 'главный инженер', 'гл. инженер', 'зам')),
        director
    )

    return {
        'director':     director,
        'process_resp': process_resp,
        'auditors':     auditors,
        'di_resp':      di_resp,
        'fnpa_resp':    fnpa_resp,
        'risk_group':   auditors,
        'coord_council': auditors,
    }


def vibe_call(messages, api_key, max_tokens=3000, retries=3):
    """Вызов BitrixGPT с retry при таймауте"""
    import time
    last_err = None
    for attempt in range(retries):
        try:
            resp = req_lib.post(
                VIBE_URL,
                headers={"Content-Type": "application/json", "X-Api-Key": api_key},
                json={"model": VIBE_MODEL, "max_tokens": max_tokens, "messages": messages},
                timeout=180  # 3 минуты
            )
            resp.raise_for_status()
            data = resp.json()
            text = "".join(c.get("message", {}).get("content", "") for c in data.get("choices", []))
            if text:
                return text
            # Пустой ответ — пробуем ещё раз
            last_err = "Empty response"
            time.sleep(2)
        except req_lib.exceptions.Timeout:
            last_err = f"Timeout (attempt {attempt+1}/{retries})"
            print(f"  ⚠️  {last_err}, retrying...")
            time.sleep(3 * (attempt + 1))
        except req_lib.exceptions.RequestException as e:
            last_err = str(e)
            print(f"  ⚠️  Request error: {e}, retrying...")
            time.sleep(3 * (attempt + 1))
    raise Exception(f"BitrixGPT failed after {retries} attempts: {last_err}")


def find_di_in_library(position: str):
    pos_up = position.upper().strip()
    if pos_up in LIBS['di']:
        return LIBS['di'][pos_up]
    for key, val in LIBS['di'].items():
        if pos_up in key or key in pos_up:
            return val
        words = [w for w in pos_up.split() if len(w) > 4]
        if any(w in key for w in words):
            return val
    return None


def find_ri_in_library(profession: str):
    prof_up = profession.upper().strip()
    if prof_up in LIBS['ri']:
        return LIBS['ri'][prof_up]
    for key, val in LIBS['ri'].items():
        words = [w for w in prof_up.split() if len(w) > 4]
        if any(w in key for w in words):
            return val
    return None


# ── Контекст для всех промптов ────────────────────────────────

def build_ctx(company, dates, resp, itr=None, workers=None, objects=None, suppliers=None):
    """Строит общий контекстный блок для промптов"""
    dir_fio  = _fio(resp.get('director'))
    dir_pos  = _pos(resp.get('director')) or company.get('director_position', 'Директор')
    dir_init = _initials(dir_fio)
    # Убираем форму из названия если она туда попала
    import re as _re2
    _raw_name = company.get('name','')
    _clean_name = _re2.sub(r'^(ООО|ОДО|ЧУП|ЗАО|РУП|ИП|ЧТУП|ЧТУ|ОАО)\\s*[«"\']?\\s*', '', _raw_name).strip().strip('»"\'')
    if not _clean_name: _clean_name = _raw_name
    full_name = f"{company.get('form','ООО')} «{_clean_name}»"

    aud_lines = '\n'.join(
        f"  - {_fio(a)} ({_pos(a)})" for a in resp.get('auditors', [])
    )
    itr_lines = ''
    if itr:
        itr_lines = '\n'.join(
            f"  {i+1}. {p.get('fio','')} — {p.get('position','')} (принят: {p.get('hire_date','')}, ОТ: {'есть '+p.get('ot_certificate_date','') if p.get('ot_certificate') else 'нет'})"
            for i, p in enumerate(itr)
        )
    workers_lines = ''
    if workers:
        profs = {}
        for w in workers:
            pos = w.get('position','')
            profs[pos] = profs.get(pos, 0) + 1
        workers_lines = '\n'.join(f"  - {pos}: {cnt} чел." for pos, cnt in profs.items())

    obj_lines = ''
    if objects:
        obj_lines = '\n'.join(f"  - {o.get('name','')} ({o.get('year','')}), заказчик: {o.get('customer','')}" for o in objects)

    sup_lines = ''
    if suppliers:
        sup_lines = '\n'.join(f"  - {s.get('name','')} ({s.get('type','')})" for s in suppliers)

    return f"""
=== ДАННЫЕ КОМПАНИИ ===
Полное название: {full_name}
Форма: {company.get('form','ООО')}
Название: {_clean(company)}
УНП: {company.get('unp','')}
Адрес: {company.get('address','')}
Город: {company.get('city','Минск')}
Область деятельности: {company.get('scope','')}

=== РУКОВОДСТВО ===
Директор ФИО (полностью): {dir_fio}
Должность директора: {dir_pos}
Инициалы директора: {dir_init}

=== ДАТЫ ===
Политика утверждена: {dates['policy']}
Цели/Приказы: {dates['goals']}
Реестр рисков: {dates['risks']}
Отчёты: {dates['reports']}
Выезд эксперта: {dates['audit']}
Отчётный период: {dates['audit_obj']}
Год: {dates['year']}

=== ОТВЕТСТВЕННЫЕ ===
За СМК/СУОТ: {dir_fio} ({dir_pos})
За процесс: {_fio(resp.get('process_resp'))} ({_pos(resp.get('process_resp'))})
Внутренние аудиторы (3 чел.):
{aud_lines}
За должностные инструкции: {_fio(resp.get('di_resp'))} ({_pos(resp.get('di_resp'))})
За ФНПА/ТНПА: {_fio(resp.get('fnpa_resp'))} ({_pos(resp.get('fnpa_resp'))})

=== ИТР (не рабочие) ===
{itr_lines if itr_lines else 'нет данных'}

=== РАБОЧИЕ (уникальные профессии) ===
{workers_lines if workers_lines else 'нет данных'}

=== ОБЪЕКТЫ ===
{obj_lines if obj_lines else 'нет данных'}

=== ПОСТАВЩИКИ ===
{sup_lines if sup_lines else 'нет данных'}
"""


def build_header(doc_title, company, dates, resp, date_key='goals'):
    """Строит стандартную шапку УТВЕРЖДАЮ для документа"""
    dir_fio  = _fio(resp.get('director'))
    dir_pos  = _pos(resp.get('director')) or company.get('director_position', 'Директор')
    dir_init = _initials(dir_fio)
    full = f"{company.get('form','ООО')} \"{_clean(company)}\""
    date = dates.get(date_key, dates['goals'])
    return f"""{full}

УТВЕРЖДАЮ
{dir_pos} {full}
_____________ {dir_init}
{date} г.

{doc_title}
"""


# ── Генераторы конкретных документов ─────────────────────────

def gen_policy_iso(company, dates, resp, itr, objects, api_key):
    ctx = build_ctx(company, dates, resp, itr=itr, objects=objects)
    header = build_header("ПОЛИТИКА В ОБЛАСТИ КАЧЕСТВА", company, dates, resp, 'policy')
    prompt = f"""Ты — опытный оформитель документов ИСО 9001 (Беларусь, строительная отрасль).

{ctx}

Создай ПОЛИТИКУ В ОБЛАСТИ КАЧЕСТВА. Используй строго те данные что выше.

ОБЯЗАТЕЛЬНАЯ ШАПКА (не меняй):
{header}

СОДЕРЖАНИЕ ПОЛИТИКИ:
- Область применения: {company.get('scope','')}
- Основные направления деятельности в области качества
- Цели организации
- Обязательства руководства
- Дата: {dates['policy']}
- Подпись: {dir_p(resp)} _________________ {_initials(_fio(resp.get('director')))}

ПРАВИЛО: в тексте используй название {company.get('form','ООО')} "{_clean(company)}" без искажений.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_awareness_list(doc_name, company, dates, resp, itr, date_key, api_key):
    """Лист ознакомления — все ИТР с правильными датами"""
    ctx = build_ctx(company, dates, resp, itr=itr)
    date = dates.get(date_key, dates['goals'])

    # Строим таблицу ИТР
    rows = []
    for i, p in enumerate(itr, 1):
        hire = p.get('hire_date', '')
        # Если принят позже даты документа — ставим дату приёма
        person_date = hire if hire and hire > date else date
        rows.append(f"{i}. {p.get('fio','')} — {p.get('position','')} — {person_date}")
    itr_table = '\n'.join(rows)

    prompt = f"""Ты — оформитель документов ИСО (Беларусь).
{ctx}

Создай ЛИСТ ОЗНАКОМЛЕНИЯ: {doc_name}

Шапка:
{company.get('form','ООО')} "{_clean(company)}"

{doc_name.upper()}

Таблица сотрудников (все ИТР):
ФИО | Должность | Подпись | Дата

СПИСОК ИТР (все строки обязательны):
{itr_table}

ПРАВИЛО: включи ВСЕХ сотрудников из списка выше. Дата ознакомления = {date} если сотрудник принят раньше, иначе дата приёма.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_order(num, name, company, dates, resp, itr, api_key, extra_text='', date_key='goals'):
    """Генерирует приказ с правильными данными"""
    date = dates.get(date_key, dates['goals'])
    dir_fio  = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    dir_pos  = dir_p(resp)
    full = f"{company.get('form','ООО')} \"{_clean(company)}\""
    city = company.get('city', 'Минск')
    aud_list = '\n'.join(f"- {_fio(a)}, {_pos(a)}" for a in resp.get('auditors',[]))
    ctx = build_ctx(company, dates, resp, itr=itr)

    prompt = f"""Ты — оформитель документов ИСО (Беларусь).
{ctx}

Создай ПРИКАЗ № {num}-СМК: {name}

ШАПКА (строго так):
{full}
ПРИКАЗ
{date} г.    № {num}-СМК    г. {city}

{name}

Аудиторы для приказов о назначении:
{aud_list}

За процесс: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}
За ДИ: {_fio(resp.get('di_resp'))}, {_pos(resp.get('di_resp'))}
За ФНПА: {_fio(resp.get('fnpa_resp'))}, {_pos(resp.get('fnpa_resp'))}

{extra_text}

ПОДПИСЬ В КОНЦЕ:
{dir_pos} _____________________ {dir_init}

ПРАВИЛА:
- Область: {company.get('scope','')}
- Дата введения СМК: {dates['goals']}
- Дата идентификации рисков: до {dates['reports']}
- Не используй "А.А." или заглушки — только реальные ФИО из данных выше
Отвечай только текстом приказа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_di(position, fio, company, dates, resp, api_key):
    """Должностная инструкция: из библиотеки или новая"""
    dir_fio  = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    dir_pos  = dir_p(resp)
    full = f"{company.get('form','ООО')} \"{_clean(company)}\""

    di = find_di_in_library(position)
    if di:
        prompt = f"""Ты — оформитель должностных инструкций (Беларусь, строительная отрасль).

ШАБЛОН ДИ из библиотеки:
---
{di['text'][:2500]}
---

Адаптируй эту ДИ для:
Компания: {full}
Область деятельности: {company.get('scope','')}
Сотрудник: {fio}
Должность: {position}
Директор: {dir_fio} ({dir_pos})
Дата: {dates['goals']}

ОБЯЗАТЕЛЬНАЯ ШАПКА:
{full}
УТВЕРЖДАЮ
{dir_pos} {full}
_____________ {dir_init}
{dates['goals']} г.

ДОЛЖНОСТНАЯ ИНСТРУКЦИЯ {position.upper()}

Измени: шапку, название компании, область деятельности, специфику обязанностей под компанию.
Сохрани структуру (общие положения, обязанности, права, ответственность).
Отвечай только текстом ДИ."""
    else:
        prompt = f"""Ты — юрист-оформитель должностных инструкций (Беларусь, строительная отрасль).

Разработай ДОЛЖНОСТНУЮ ИНСТРУКЦИЮ для должности: {position}

Компания: {full}
Область: {company.get('scope','')}
Сотрудник: {fio}
Директор: {dir_fio}

ОБЯЗАТЕЛЬНАЯ ШАПКА:
{full}
УТВЕРЖДАЮ
{dir_pos} {full}
_____________ {dir_init}
{dates['goals']} г.

ДОЛЖНОСТНАЯ ИНСТРУКЦИЯ {position.upper()}

Структура (обязательна):
1. Общие положения (категория, образование, стаж, кому подчиняется)
2. Должностные обязанности (конкретные для этой должности и области)
3. Права
4. Ответственность

Отвечай только текстом ДИ."""

    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_ot_instruction(profession, company, dates, resp, api_key):
    """Инструкция по охране труда для профессии"""
    dir_fio  = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    dir_pos  = dir_p(resp)
    full = f"{company.get('form','ООО')} \"{_clean(company)}\""

    ri = find_ri_in_library(profession)
    if ri:
        prompt = f"""Ты — оформитель инструкций по охране труда (Беларусь).

ШАБЛОН инструкции для {profession}:
---
{ri['text'][:2500]}
---

Адаптируй под:
Компания: {full}
Область: {company.get('scope','')}
Профессия: {profession}
Директор: {dir_fio}
Дата: {dates['goals']}

ШАПКА:
{full}
УТВЕРЖДАЮ {dir_pos} {full} _____________ {dir_init} {dates['goals']} г.

Обнови шапку и адаптируй содержание под вид работ компании.
Отвечай только текстом инструкции."""
    else:
        prompt = f"""Ты — специалист по охране труда (Беларусь, строительная отрасль).

Разработай ИНСТРУКЦИЮ ПО ОХРАНЕ ТРУДА ДЛЯ {profession.upper()}

Компания: {full}
Область: {company.get('scope','')}
Директор: {dir_fio}
Дата: {dates['goals']}

ШАПКА:
{full}
УТВЕРЖДАЮ {dir_pos} {full} _____________ {dir_init} {dates['goals']} г.
ИНСТРУКЦИЯ ПО ОХРАНЕ ТРУДА ДЛЯ {profession.upper()}

Структура (5 глав обязательно):
1. Общие требования по охране труда
2. Требования перед началом работы
3. Требования при выполнении работы
4. Требования по окончании работы
5. Требования в аварийных ситуациях

Адаптируй под специфику профессии и вид работ: {company.get('scope','')}.
Отвечай только текстом инструкции."""

    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_risk_card(role_type, positions_list, company, dates, resp, api_key):
    """Карта рисков для группы должностей"""
    dir_fio  = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    dir_pos  = dir_p(resp)
    full = f"{company.get('form','ООО')} \"{_clean(company)}\""
    positions_str = ', '.join(positions_list)
    is_office = role_type == 'office'

    prompt = f"""Ты — специалист по охране труда (Беларусь, ISO 45001).

Создай КАРТУ РИСКОВ для должностей: {positions_str}

Компания: {full}
Область: {company.get('scope','')}
Тип работников: {'офисные (ИТР)' if is_office else 'производственные/рабочие'}
Дата: {dates['goals']}

ШАПКА:
{full}
УТВЕРЖДАЮ {dir_pos} {full} _____________ {dir_init} {dates['goals']} г.
КАРТА РИСКОВ
Должности: {positions_str}

ТАБЛИЦА (минимум 6-8 рисков):
| № | Опасность/вредный фактор | Возможные последствия | Вероятность (1-3) | Тяжесть (1-3) | Уровень риска (В×Т) | Меры управления |

{'Риски офисных сотрудников: нагрузка на зрение (ПК), неудобная поза, нервное напряжение, падение на скользком полу, пожар.' if is_office else f'Риски производственных работников: падение с высоты, падение грузов, травмы от инструмента, поражение электротоком, вредные вещества. Учитывай специфику: {company.get("scope","")}'}

Уровень риска = Вероятность × Тяжесть. Неприемлемый > 9.
Подписи ответственных внизу.
Отвечай только текстом документа."""

    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_risk_register(company, dates, resp, itr, api_key):
    """Реестр рисков СМК"""
    ctx = build_ctx(company, dates, resp, itr=itr)
    header = build_header(f"РЕЕСТР РИСКОВ\nна {dates['year']} г.\n{company.get('form','ООО')} \"{_clean(company)}\"", 
                          company, dates, resp, 'risks')
    prompt = f"""Ты — оформитель документов ИСО 9001 (Беларусь).
{ctx}

Создай РЕЕСТР РИСКОВ И ВОЗМОЖНОСТЕЙ для строительной организации.

ШАПКА:
{header}

ТАБЛИЦА РИСКОВ (8-10 рисков):
| № | Наименование риска | Причина | P (1-3) | S (1-3) | OR (P×S) | Категория |

ТИПОВЫЕ РИСКИ ДЛЯ СТРОИТЕЛЬСТВА:
- Финансовый риск (недостаток оборотных средств)
- Риск потери заказчиков
- Риск срыва сроков строительства
- Риск квалификации персонала
- Риск нарушения требований ТНПА
- Риск поставки некачественных материалов

Категория: OR 1-4 — допустимый, 5-8 — значительный, 9+ — критический
Подписи ответственных за риски ({', '.join(_fio(a) for a in resp.get('risk_group',[]))}):

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_report(doc_name, company, dates, resp, objects, api_key, date_key='reports'):
    """Отчёт по процессу / сводный отчёт"""
    ctx = build_ctx(company, dates, resp, objects=objects)
    has_obj = bool(objects)
    obj_count = len(objects) if objects else 0
    objects_text = ''
    if objects:
        objects_text = '\n'.join(f"- {o.get('name','')} ({o.get('year','')})" for o in objects)

    prompt = f"""Ты — оформитель документов ИСО 9001 (Беларусь).
{ctx}

Создай: {doc_name}

ШАПКА:
{build_header(doc_name.upper(), company, dates, resp, date_key)}

Отчётный период: {dates['audit_obj']}
Ответственный: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}

{'Объектов выполнено: ' + str(obj_count) + chr(10) + objects_text if has_obj else 'Объектов за отчётный период не было.'}

ПРАВИЛО ДЛЯ РЕЗУЛЬТАТИВНОСТИ:
{'Все критерии 100%, система результативна.' if has_obj else 'Результативность оценить не представляется возможным в связи с отсутствием реализованных объектов.'}

{'Жалоб и рекламаций не поступало. Оценка удовлетворённости заказчиков: 5 баллов.' if has_obj else ''}

Дата отчёта: {dates[date_key]}
Подпись: {_fio(resp.get('process_resp'))} _______________

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_supplier_card(supplier, company, dates, resp, api_key):
    """Карточка оценки поставщика"""
    dir_fio  = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    full = f"{company.get('form','ООО')} \"{_clean(company)}\""
    sup_name = supplier.get('name', '')
    sup_type = supplier.get('type', '')

    prompt = f"""Ты — оформитель документов ИСО 9001 (Беларусь).

Создай КАРТОЧКУ ОЦЕНКИ ПОСТАВЩИКА.

Компания-покупатель: {full}
Поставщик: {sup_name}
Вид продукции: {sup_type}
Директор: {dir_fio}
Дата: {dates['reports']}

ШАПКА:
Утверждаю
{dir_p(resp)} {full} _____________ {dir_init} {dates['reports']} г.
Карточка оценки поставщиков
{sup_type}

ТАБЛИЦА (основной поставщик + 1 конкурент):
| № | Наименование | Пстп | Пк | Поп | Пуп | Пуо | Пи | П | Сумма | Статус |

Оценка по 7 критериям (каждый 0/2.5/5 баллов):
- Поставщик {sup_name}: 5 5 5 5 5 2.5 2.5 = 30 — основной
- Конкурент (аналогичный): 5 5 2.5 5 2.5 2.5 2.5 = 25 — приемлемый

Примечания: расшифровка показателей.
Подпись: {dir_fio} _______________

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_audit_program(company, dates, resp, itr, api_key):
    """Программа внутренних аудитов"""
    ctx = build_ctx(company, dates, resp, itr=itr)
    header = build_header("ПРОГРАММА проведения внутренних аудитов СМК", company, dates, resp, 'goals')

    # Критерии по должностям
    prod_criteria = "4.1-4.4, 5.1-5.3, 6.1-6.3, 7.1-7.5, 8.1, 8.2, 8.4-8.7, 9.1-9.3, 10.1-10.3"
    office_criteria = "4.1, 4.2, 5.3, 6.1-6.3, 7.2-7.5, 10.3"

    rows = []
    for i, p in enumerate(itr, 1):
        pos = p.get('position','').lower()
        is_office = any(k in pos for k in ['бухгалтер', 'кадр', 'юрис', 'делопроиз'])
        criteria = office_criteria if is_office else prod_criteria
        rows.append(f"{i}. {p.get('fio','')} ({p.get('position','')}) — {criteria}")
    rows_text = '\n'.join(rows)

    prompt = f"""Ты — оформитель документов ИСО 9001 (Беларусь).
{ctx}

Создай ПРОГРАММУ ВНУТРЕННИХ АУДИТОВ на {dates['year']} год.

{header}

Аудиторы: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}
Год: {dates['year']}

ТАБЛИЦА АУДИТОВ (все ИТР):
Должностное лицо | Критерии аудита | Месяц

{rows_text}

ПРАВИЛО: распредели аудиты равномерно по году. Первый аудит — {_fio(resp.get('director'))}.
В конце: Журнал регистрации внутренних аудитов (1 запись — директор, дата {dates['reports']}).
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_satisfaction_report(company, dates, resp, objects, api_key):
    """Отчёт по оценке удовлетворённости заказчиков"""
    has_obj = bool(objects)
    dir_fio  = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    full = f"{company.get('form','ООО')} \"{_clean(company)}\""
    obj_rows = '\n'.join(f"| {o.get('name','')} | — | 5 |" for o in objects) if objects else ''

    prompt = f"""Ты — оформитель документов ИСО 9001 (Беларусь).

Создай ОТЧЁТ ПО ОЦЕНКЕ УДОВЛЕТВОРЁННОСТИ ЗАКАЗЧИКОВ.

Компания: {full}
Директор: {dir_fio}
Отчётный период: {dates['audit_obj']}
Дата отчёта: {dates['reports']}
Ответственный: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}

ШАПКА:
Утверждаю
{dir_p(resp)} {full} _____________ {dir_init} {dates['reports']} г.
ОТЧЁТ по оценке удовлетворённости заказчиков за период {dates['audit_obj']}

{'ТАБЛИЦА ЗАКАЗЧИКОВ:'+chr(10)+'| Заказчик | Кол-во рекламаций | Балл |'+chr(10)+obj_rows+chr(10)+'Общая оценка: 5 баллов — высокая удовлетворённость.' if has_obj else 'Оценить уровень удовлетворённости не представляется возможным в связи с отсутствием реализованных объектов за отчётный период.'}

Подпись: {_fio(resp.get('process_resp'))} _______________

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def dir_p(resp):
    """Должность директора"""
    d = resp.get('director')
    if d: return d.get('position', 'Директор')
    return 'Директор'


# ── Создание DOCX из текста ───────────────────────────────────

def create_docx_from_text(text: str) -> bytes:
    content_types = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>'''
    rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>'''
    word_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"/>'''

    def escape(s):
        return s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')

    def make_para(line):
        line = escape(line)
        is_heading = (line.isupper() or line.startswith('УТВЕРЖДАЮ') or line.startswith('ПРИКАЗ') or line.startswith('ДОЛЖНОСТНАЯ') or line.startswith('ИНСТРУКЦИЯ') or line.startswith('ПОЛИТИКА') or line.startswith('КАРТА') or line.startswith('РЕЕСТР') or line.startswith('ОТЧЁТ') or line.startswith('ПРОГРАММА')) and len(line) < 120
        align = 'center' if (line.startswith('УТВЕРЖДАЮ') or is_heading) else 'both'
        bold = 'true' if is_heading else 'false'
        sz = '28' if is_heading else '24'
        return f'''<w:p><w:pPr><w:jc w:val="{align}"/><w:spacing w:line="360" w:lineRule="auto"/></w:pPr><w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/><w:b w:val="{bold}"/><w:sz w:val="{sz}"/><w:szCs w:val="{sz}"/></w:rPr><w:t xml:space="preserve">{line if line.strip() else ' '}</w:t></w:r></w:p>'''

    lines = text.replace('\r\n','\n').replace('\r','\n').split('\n')
    paras = '\n'.join(make_para(l) for l in lines)

    doc_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
<w:body>
{paras}
<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1134" w:right="850" w:bottom="1134" w:left="1701" w:header="709" w:footer="709" w:gutter="0"/></w:sectPr>
</w:body></w:document>'''

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types)
        zf.writestr('_rels/.rels', rels)
        zf.writestr('word/document.xml', doc_xml)
        zf.writestr('word/_rels/document.xml.rels', word_rels)
    return buf.getvalue()


# ── Главная функция генерации пакета ─────────────────────────

def generate_package(company_data: dict, api_key: str, product: str, progress_cb=None) -> dict:
    company  = company_data.get('company', {})
    staff    = company_data.get('staff', [])
    dates_in = company_data.get('dates', {}) or company_data.get('certification', {})
    objects    = company_data.get('objects', []) or []
    suppliers  = company_data.get('suppliers', []) or []
    work_types = company_data.get('work_types', []) or []
    # Если work_types не переданы — пробуем извлечь из области деятельности
    if not work_types and company.get('scope'):
        scope = company['scope'].lower()
        SCOPE_MAP = {
            'земляны': 'Земляные работы',
            'фундамент': 'Устройство оснований и фундаментов зданий и сооружений',
            'кирпич': 'Возведение каменных и армокаменных конструкций',
            'кладк': 'Возведение каменных и армокаменных конструкций',
            'монолит': 'Возведение монолитных бетонных и железобетонных конструкций',
            'металлоконструк': 'Монтаж стальных конструкций',
            'кровел': 'Устройство кровель',
            'крыш': 'Устройство кровель',
            'утеплен': 'Устройство тепловой изоляции наружных ограждающих конструкций',
            'фасад': 'Устройство тепловой изоляции наружных ограждающих конструкций',
            'штукатур': 'Штукатурные, малярные, обойные и стекольные работы',
            'малярн': 'Штукатурные, малярные, обойные и стекольные работы',
            'покраск': 'Штукатурные, малярные, обойные и стекольные работы',
            'облицов': 'Устройство покрытий из плиточных материалов',
            'плиточн': 'Устройство покрытий из плиточных материалов',
            'окон': 'Заполнение оконных и дверных проёмов',
            'дверн': 'Заполнение оконных и дверных проёмов',
            'полов': 'Устройство полов',
            'стяжк': 'Устройство полов',
            'благоустройств': 'Благоустройство территорий',
            'сантехник': 'Монтаж систем внутреннего и наружного водоснабжения и канализации',
            'водопровод': 'Монтаж систем внутреннего и наружного водоснабжения и канализации',
            'отоплен': 'Монтаж систем отопления, вентиляции и кондиционирования',
            'вентиляц': 'Монтаж систем отопления, вентиляции и кондиционирования',
            'электромонтаж': 'Электромонтажные работы',
            'электрик': 'Электромонтажные работы',
            'автоматик': 'Монтаж систем автоматизации',
            'слаботочн': 'Монтаж систем связи и диспетчеризации',
            'дорожн': 'Устройство дорог и улиц',
            'сварк': 'Сварочные работы',
            'ремонт': 'Штукатурные, малярные, обойные и стекольные работы',
            'строительно-монтажн': 'Общестроительные работы',
            'общестрой': 'Общестроительные работы',
        }
        for kw, wt in SCOPE_MAP.items():
            if kw in scope and wt not in work_types:
                work_types.append(wt)
    # Если всё ещё пусто — ставим базовый
    if not work_types:
        work_types = ['Общестроительные работы']
    # Добавляем work_types в company для передачи в генераторы
    company['work_types'] = work_types

    audit_date = dates_in.get('audit_date', '') or company_data.get('certification', {}).get('audit_date', '')
    dates = calculate_dates(audit_date)

    # Ключевые слова должностей рабочих
    WORKER_KEYWORDS = [
        'штукатур','маляр','сварщик','электрогаз','облицовщик','плиточник',
        'кровельщик','монтажник','электромонтажник','плотник','бетонщик',
        'каменщик','арматурщик','разнорабочий','подсобный','стропальщик',
        'водитель','машинист','оператор','слесарь','токарь','фрезеровщик',
        'сантехник','электрик','тракторист','экскаваторщик','крановщик',
    ]
    def _is_worker(p):
        if p.get('is_worker'): return True
        pos = p.get('position','').lower()
        return any(kw in pos for kw in WORKER_KEYWORDS)

    itr     = [s for s in staff if not _is_worker(s)]
    workers = [s for s in staff if _is_worker(s)]

    # Если рабочих нет в staff — берём из поля workers (список профессий)
    if not workers and company_data.get('workers'):
        for w in company_data['workers']:
            pos = w if isinstance(w, str) else w.get('position','')
            if pos:
                workers.append({'fio': '', 'position': pos, 'is_worker': True})

    # Также проверяем company.scope на наличие профессий рабочих
    if not workers:
        scope = company.get('scope','').lower()
        SCOPE_PROFS = {
            'штукатур': 'Штукатур', 'малярн': 'Маляр',
            'сварк': 'Электрогазосварщик', 'облицовк': 'Облицовщик-плиточник',
            'кровельн': 'Кровельщик', 'электромонтаж': 'Электромонтажник по электрооборудованию',
        }
        for kw, prof in SCOPE_PROFS.items():
            if kw in scope:
                workers.append({'fio':'','position':prof,'is_worker':True})

    # Директор может быть задан в company напрямую
    if not any(p.get('role','') == 'director' or 'директор' in p.get('position','').lower() for p in itr):
        if company.get('director_fio'):
            itr.insert(0, {
                'fio': company['director_fio'],
                'position': company.get('director_position', 'Директор'),
                'role': 'director',
                'is_worker': False,
                'ot_certificate': True,
                'ot_certificate_date': '',
                'hire_date': ''
            })

    resp = select_responsible(itr)

    worker_professions = list({s.get('position','') for s in workers if s.get('position')})

    docs = []

    def add(name, text):
        try:
            docs.append({'name': name, 'bytes': create_docx_from_text(text)})
        except Exception as e:
            print(f"Ошибка создания {name}: {e}")

    step = [0]
    def p(msg):
        step[0] += 1
        if progress_cb: progress_cb(step[0], 100, msg)
        print(f"  [{step[0]}] {msg}")

    org = company.get('name', 'org')

    if product in ('iso', 'iso_suot'):
        _gen_iso(org, company, dates, resp, itr, objects, suppliers, api_key, add, p)

    if product in ('suot', 'iso_suot'):
        _gen_suot(org, company, dates, resp, itr, workers, worker_professions, api_key, add, p)

    if product in ('spk_stroy', 'spk_bisp'):
        _gen_spk(org, company, dates, resp, itr, api_key, add, p, variant=product)

    return {
        'docs': docs,
        'dates': dates,
        'responsible': resp,
        'itr_count': len(itr),
        'workers_count': len(workers),
        'professions': worker_professions
    }


def _parallel(tasks, max_workers=4):
    """Выполняет задачи параллельно. tasks = [(fn, args), ...]"""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fn, *args): name for name, fn, args in tasks}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as e:
                print(f"  ⚠️ {name}: {e}")
                results[name] = f"[Ошибка генерации: {e}]"
    return results



def _gen_iso(org, company, dates, resp, itr, objects, suppliers, api_key, add, p):
    """Полный пакет ИСО 9001 — 40+ документов"""
    year = dates['year']
    has_welding = company.get('has_welding', False)
    machinery = company.get('machinery', [])

    p("Политика + лист ознакомления...")
    add(f"{org} - 1 Политика в области качества.docx",
        gen_policy_iso(company, dates, resp, itr, objects, api_key))
    add(f"{org} - 1.2 Лист ознакомления с политикой.docx",
        gen_awareness_list("Лист ознакомления с Политикой в области качества",
                           company, dates, resp, itr, "policy", api_key))

    p("Цели + лист ознакомления...")
    ctx = build_ctx(company, dates, resp, itr=itr, objects=objects)
    hdr = build_header(f"Цели в области качества {company.get('form','ООО')} «{_clean(company)}» на {year} г.",
                       company, dates, resp, "goals")
    text = vibe_call([{"role":"user","content":
        f"Создай ЦЕЛИ В ОБЛАСТИ КАЧЕСТВА.\n{ctx}\n{hdr}\n"
        f"Таблица 5 целей с показателями, ответственными, сроками:\n"
        f"1. Удовлетворённость потребителей — 0 претензий — {_fio(resp.get('director'))}\n"
        f"2. Повысить квалификацию — обучение аудиторов — {_fio(resp.get('process_resp'))}\n"
        f"3. Улучшить качество СМР — снизить несоответствия — {_fio(resp.get('process_resp'))}\n"
        f"4. Новые объекты — тендеры — {_fio(resp.get('director'))}\n"
        f"5. Внедрить СМК — соответствие ISO 9001 — {_fio(resp.get('director'))}\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} - 2.1 Цели в области качества.docx", text)
    add(f"{org} - 2.2 Лист ознакомления с целями.docx",
        gen_awareness_list("Лист ознакомления с Целями в области качества",
                           company, dates, resp, itr, "goals", api_key))

    p("Номенклатура дел...")
    hdr2 = build_header(f"НОМЕНКЛАТУРА ДЕЛ N 1\nг. {company.get('city','Минск')} на {year} год",
                        company, dates, resp, "policy")
    text = vibe_call([{"role":"user","content":
        f"Создай НОМЕНКЛАТУРУ ДЕЛ для строительной компании.\n{hdr2}\n"
        "Разделы (таблица Индекс|Наименование|Кол-во|Срок хранения|Примечание):\n"
        "01 — Руководство (приказы, устав, лицензии)\n"
        "02 — Персонал (ДИ, приказы о приёме)\n"
        "03 — Качество (РК СМК, СТП, политика, цели)\n"
        "04 — Производство (договоры, исп.документация)\n"
        "05 — Поставщики (договоры, карточки)\n"
        "06 — Журналы СМК\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} - 3 Номенклатура дел.docx", text)

    p("Приказы 1-9 (параллельно)...")
    orders = [
        (1, "О разработке системы менеджмента качества", "policy",
         f"Разработать СМК. Ответственный: {_fio(resp.get('director'))}."),
        (2, "О введении в действие документов СМК и внедрении СМК", "goals",
         f"Ввести в действие с {dates['goals']}: РК СМК 01-{year}, СТП СМК 02-{year}, 03-{year}, 04-{year}."),
        (3, "О назначении аудиторов для проведения внутреннего аудита", "goals",
         f"Аудиторы: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}"),
        (4, "О назначении ответственного за ведение и учёт должностных инструкций", "goals",
         f"Ответственный: {_fio(resp.get('di_resp'))}, {_pos(resp.get('di_resp'))}."),
        (5, "О проведении оценки и анализа рисков", "policy",
         f"Группа: {', '.join(_fio(a) for a in resp.get('risk_group',[]))}. Срок: до {dates['reports']}."),
        (6, "О назначении владельцев процессов", "goals",
         f"Владелец: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}."),
        (7, "О назначении ответственного за управление фондом ТНПА и документов СМК", "goals",
         f"За ТНПА: {_fio(resp.get('fnpa_resp'))}, {_pos(resp.get('fnpa_resp'))}."),
        (8, "О создании Координационного совета", "goals",
         f"Председатель: {_fio(resp.get('director'))}. Члены: {', '.join(_fio(a) for a in resp.get('coord_council',[]) if a != resp.get('director'))}."),
        (9, "О проведении внутреннего обучения специалистов", "goals",
         f"Обучить: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
    ]
    order_tasks = [
        (f"{org} - 3.{num} Приказ {num} {name[:50]}.docx",
         gen_order, (num, name, company, dates, resp, itr, api_key, extra, date_key))
        for num, name, date_key, extra in orders
    ]
    for fname, txt in _parallel(order_tasks, max_workers=4).items():
        add(fname, txt)

    p("Протокол КС...")
    aud_str = ", ".join(_fio(a)+" ("+_pos(a)+")" for a in resp.get("coord_council",[]))
    text = vibe_call([{"role":"user","content":
        f"Создай ПРОТОКОЛ заседания Координационного совета N 1.\n"
        f"{build_ctx(company,dates,resp,itr=itr,objects=objects)}\n"
        f"Дата: {dates['reports']}. Присутствуют: {aud_str}.\n"
        "Повестка: 1.Анализ СМК. 2.Рекомендации по улучшению.\n"
        "Решения: СМК признана результативной. Несоответствий нет.\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} - 3.8.2 Протокол КС.docx", text)

    p("Программа и протокол обучения...")
    aud_str2 = ", ".join(_fio(a)+" ("+_pos(a)+")" for a in resp.get("auditors",[]))
    text = vibe_call([{"role":"user","content":
        f"Создай ПРОГРАММУ ВНУТРЕННЕГО ОБУЧЕНИЯ по теме СМК.\n"
        f"{build_header('Программа семинара Документальное оформление и порядок разработки СМК. Внутренний аудит', company, dates, resp, 'goals')}\n"
        f"Продолжительность: 1 день (8 часов). Темы: ISO 9001, аудит, документация.\n"
        f"Список обучаемых: {aud_str2}.\nОтвечай только текстом."}], api_key)
    add(f"{org} - 3.9.3 Программа внутреннего обучения.docx", text)

    text = vibe_call([{"role":"user","content":
        f"Создай ПРОТОКОЛ внутреннего обучения N 1.\n"
        f"{build_ctx(company,dates,resp,itr=itr)}\n"
        f"Дата: {dates['goals']}. Тема: СМК и внутренний аудит ISO 9001.\n"
        f"Обучены: {aud_str2}.\nРезультат: пройдено.\nОтвечай только текстом."}], api_key)
    add(f"{org} - 3.9.2 Протокол обучения.docx", text)

    p("Программа аудитов...")
    add(f"{org} - 4.1 Программа внутренних аудитов СМК.docx",
        gen_audit_program(company, dates, resp, itr, api_key))

    p("Реестр рисков + план мероприятий...")
    add(f"{org} - 10 Реестр рисков.docx",
        gen_risk_register(company, dates, resp, itr, api_key))
    add(f"{org} - 11 План мероприятий по рискам.docx",
        gen_plan_vozmozhnostei(company, dates, resp, objects, api_key))

    p("Отчёты (параллельно)...")
    report_tasks = [
        (f"{org} - Отчёт по процессу строительства.docx",
         gen_report, ("Отчёт по качеству владельца процесса СМР", company, dates, resp, objects, api_key)),
        (f"{org} - Отчёт по оценке удовлетворённости заказчиков.docx",
         gen_satisfaction_report, (company, dates, resp, objects, api_key)),
        (f"{org} - Сводный отчёт по СМК.docx",
         gen_report, ("Отчёт по анализу функционирования СМК", company, dates, resp, objects, api_key)),
    ]
    for fname, txt in _parallel(report_tasks, max_workers=3).items():
        add(fname, txt)

    p("Анкета СМК + исходная информация...")
    add(f"{org} - Анкета-вопросник СМК.docx",
        gen_anket_smk(company, dates, resp, api_key))
    add(f"{org} - Исходная информация СМК.docx",
        gen_ishodnaya_smk(company, dates, resp, itr, [], objects, suppliers, api_key))

    p("РК СМК 01...")
    add(f"{org} - РК СМК 01-{year}.docx",
        gen_rk_smk(company, dates, resp, itr, objects, suppliers, api_key))

    p("СТП СМК 02,03,04 (параллельно)...")
    stp_tasks = [
        (f"{org} - СТП СМК 02-{year}.docx", gen_stp_smk,
         (2, "Управление рисками", company, dates, resp, itr, api_key)),
        (f"{org} - СТП СМК 03-{year}.docx", gen_stp_smk,
         (3, "Управление документированной информацией", company, dates, resp, itr, api_key)),
        (f"{org} - СТП СМК 04-{year}.docx", gen_stp_smk,
         (4, "Процесс производства СМР", company, dates, resp, itr, api_key)),
    ]
    for fname, txt in _parallel(stp_tasks, max_workers=3).items():
        add(fname, txt)

    p(f"ДИ ({len(itr)} чел., параллельно)...")
    di_tasks = []
    for person in itr:
        pos = person.get("position","")
        fio = person.get("fio","")
        safe = re.sub(r"[^\w\s-]","",pos)[:40]
        di_tasks.append((f"{org} - ДИ {safe}.docx", gen_di, (pos, fio, company, dates, resp, api_key)))
    for fname, txt in _parallel(di_tasks, max_workers=4).items():
        add(fname, txt)

    p(f"Карточки поставщиков ({len(suppliers[:6])} шт., параллельно)...")
    sup_tasks = []
    for i, sup in enumerate(suppliers[:6], 1):
        safe = re.sub(r"[^\w\s-]","",sup.get("name",f"поставщик_{i}"))[:30]
        sup_tasks.append((f"{org} - Карточка поставщика {i} {safe}.docx",
                          gen_supplier_card, (sup, company, dates, resp, api_key)))
    for fname, txt in _parallel(sup_tasks, max_workers=4).items():
        add(fname, txt)

    p("Перечень продукции входного контроля + приказ...")
    hdr3 = build_header("ПЕРЕЧЕНЬ продукции подлежащей входному контролю", company, dates, resp, "goals")
    text = vibe_call([{"role":"user","content":
        f"Создай ПЕРЕЧЕНЬ ПРОДУКЦИИ подлежащей входному контролю.\n{hdr3}\n"
        f"Таблица: Наименование|ТНПА|Контролируемые показатели|Периодичность\n"
        f"Типовые материалы для {company.get('scope','')}: цемент, кирпич, смеси, арматура, краски.\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} - Перечень продукции входного контроля.docx", text)
    add(f"{org} - Приказ о входном контроле.docx",
        gen_order(0, "О входном контроле продукции", company, dates, resp, itr, api_key,
                  f"Ввести входной контроль. Ответственный: {_fio(resp.get('process_resp'))}.", "goals"))

    p("Журналы ИСО...")
    journals_iso = [
        (1, "Журнал регистрации входящей и исходящей корреспонденции"),
        (2, "Журнал регистрации приказов"),
        (3, "Журнал регистрации договоров с заказчиками"),
        (4, "Журнал учёта технических нормативных правовых актов"),
        (5, "Журнал регистрации внутренних аудитов СМК"),
        (6, "Журнал учёта претензий и рекламаций"),
        (7, "Журнал учёта выдачи инструкций"),
        (8, "Журнал учёта должностных инструкций"),
        (9, "Журнал входного контроля"),
    ]
    for jnum, jname in journals_iso:
        add(f"{org} - Журнал {jnum} {jname[:50]}.docx",
            gen_zhurnal_iso(jnum, jname, company))

    if has_welding:
        p("График валидации (сварка)...")
        add(f"{org} - График валидации процесса сварки.docx",
            gen_grafik_validacii(company, dates, resp, api_key))

    p("График ППР...")
    mach = machinery if machinery else ["Автомобиль транспортное средство"]
    add(f"{org} - График ППР.docx",
        gen_plan_ppr(company, dates, resp, mach, api_key))

    p("План повышения квалификации...")
    itr_str = chr(10).join(
        _fio(pp)+" | "+_pos(pp)+" | повышение квалификации | в течение "+year
        for pp in itr[:5])
    hdr4 = build_header(f"План-график обучения персонала на {year} год", company, dates, resp, "goals")
    text = vibe_call([{"role":"user","content":
        f"Создай ПЛАН-ГРАФИК ОБУЧЕНИЯ ПЕРСОНАЛА на {year} год.\n{hdr4}\n"
        f"Таблица: ФИО|Тема обучения|Дата|Примечание\n{itr_str}\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} - Plan повышения квалификации.docx", text)



def _gen_suot(org, company, dates, resp, itr, workers, professions, api_key, add, p):
    """Полный пакет СУОТ ISO 45001 — 77 документов"""
    year = dates['year']

    p("Политика ОТ + лист ознакомления...")
    ctx = build_ctx(company, dates, resp, itr=itr, workers=workers)
    hdr = build_header("ПОЛИТИКА В ОБЛАСТИ ОХРАНЫ ТРУДА", company, dates, resp, "goals")
    text = vibe_call([{"role":"user","content":
        f"Создай ПОЛИТИКУ В ОБЛАСТИ ОХРАНЫ ТРУДА ISO 45001.\n{ctx}\n{hdr}\n"
        f"Область: {company.get('scope','')}.\n"
        "Обязательства: безопасные условия труда, устранение рисков, вовлечение работников.\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} СУОТ - 2.1 Политика ОТ.docx", text)
    add(f"{org} СУОТ - 2.2 Лист ознакомления с политикой ОТ.docx",
        gen_awareness_list("Лист ознакомления с Политикой в области охраны труда",
                           company, dates, resp, itr, "goals", api_key))

    p("Цели OH&S + лист + план мероприятий...")
    hdr2 = build_header(f"Цели организации в области охраны труда и мероприятия на {year} г.",
                        company, dates, resp, "goals")
    text = vibe_call([{"role":"user","content":
        f"Создай ЦЕЛИ организации в области охраны труда.\n{ctx}\n{hdr2}\n"
        f"Таблица (5 целей с мероприятиями, ответственными, сроками, ресурсами):\n"
        f"1. Результативность СУОТ — аудиты — {_fio(resp.get('director'))} — в течение {year}\n"
        f"2. Снижение рисков — мероприятия по картам рисков — {_fio(resp.get('process_resp'))} — постоянно\n"
        f"3. Обучение персонала — инструктажи, проверка знаний — {_fio(resp.get('process_resp'))} — по графику\n"
        f"4. Выполнение плана ОТ — контроль мероприятий — {_fio(resp.get('director'))} — ежеквартально\n"
        f"5. Внедрение СУОТ — соответствие ISO 45001 — {_fio(resp.get('director'))} — {year}\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} СУОТ - 2.3 Цели OH&S и мероприятия.docx", text)
    add(f"{org} СУОТ - 2.4 Лист ознакомления с целями OH&S.docx",
        gen_awareness_list("Лист ознакомления с целями OH&S",
                           company, dates, resp, itr, "goals", api_key))
    add(f"{org} СУОТ - 2.5 План мероприятий по ОТ.docx",
        gen_plan_vozmozhnostei(company, dates, resp, [], api_key))

    p("Приказы СУОТ 1-10 (параллельно)...")
    suot_orders = [
        (1, "О разработке системы управления охраной труда", "policy",
         f"Разработать СУОТ. Ответственный: {_fio(resp.get('director'))}."),
        (2, "О внедрении системы управления охраной труда", "goals",
         f"Считать СУОТ внедрённой с {dates['goals']}."),
        (3, "О введении в действие документов системы управления охраной труда", "goals",
         f"Ввести: Р OH&S 01-{year}, СТП OH&S 8.1-02-{year}, Политику OH&S."),
        (4, "О назначении ответственных лиц в области охраны труда", "goals",
         f"Ответственный: {_fio(resp.get('director'))}. За инструктажи: {_fio(resp.get('process_resp'))}."),
        (5, "О назначении внутренних аудиторов OH&S", "goals",
         f"Аудиторы: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}"),
        (6, "О назначении ответственных за идентификацию рисков OH&S", "goals",
         f"Группа: {', '.join(_fio(a) for a in resp.get('risk_group',[]))}. Срок: до {dates['reports']}."),
        (7, "О разработке инструкций по охране труда", "goals",
         f"Ответственный: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}."),
        (8, "Об обучении пожарно-техническому минимуму", "goals",
         f"Обучить: {', '.join(_fio(a) for a in resp.get('auditors',[]))}."),
        (9, "О дне охраны труда", "goals",
         f"Комиссия: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
        (10, "О проведении внутреннего обучения по СУОТ", "goals",
         f"Обучить: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
    ]
    suot_order_tasks = [
        (f"{org} СУОТ - {num} Приказ {num} {name[:40]}.docx",
         gen_order, (num, name, company, dates, resp, itr, api_key, extra, dk))
        for num, name, dk, extra in suot_orders
    ]
    for fname, txt in _parallel(suot_order_tasks, max_workers=4).items():
        add(fname, txt)

    p("Перечни 1-12 (отдельными файлами, параллельно)...")
    perech_titles = {
        1: "Перечень аварийных ситуаций и мероприятия по предупреждению",
        2: "Перечень должностей руководителей и специалистов проверки знаний ОТ",
        3: "Перечень профессий рабочих — проверка знаний ОТ",
        4: "Перечень лиц выдачи наряда-допуска",
        5: "Перечень профессий медицинских осмотров",
        6: "Перечень рабочих мест идентификации опасностей",
        7: "Перечень мест с повышенной опасностью",
        8: "Перечень неэлектротехнического персонала",
        9: "Перечень профессий рабочих — стажировка",
        10: "Перечень работ субподрядчиков",
        11: "Перечень рабочих мест — электробезопасность",
        12: "Перечень СИЗ",
    }
    perech_tasks = [
        (f"{org} СУОТ - {num} Перечень {title[:40]}.docx",
         gen_suot_perech, (num, title, company, dates, resp, itr, workers, api_key))
        for num, title in perech_titles.items()
    ]
    for fname, txt in _parallel(perech_tasks, max_workers=4).items():
        add(fname, txt)

    p("Перечень инструкций по ОТ...")
    text = vibe_call([{"role":"user","content":
        f"Создай ПЕРЕЧЕНЬ ИНСТРУКЦИЙ ПО ОХРАНЕ ТРУДА.\n"
        f"{build_header('Перечень инструкций по охране труда', company, dates, resp, 'goals')}\n"
        "Таблица: N инструкции|Наименование|Срок пересмотра\n"
        "ОТ-1 Инструкция о проведении контроля за соблюдением законодательства по ОТ — 3 года\n"
        "ОТ-2 Общеобъектовая инструкция о мерах пожарной безопасности — 3 года\n"
        "ОТ-3 Инструкция по ОТ при работе с ПК — 3 года\n"
        "ОТ-4 Инструкция по оказанию первой медицинской помощи — 3 года\n"
        "ОТ-5 Инструкция действий работников при пожаре — 3 года\n"
        "ОТ-6 Инструкция по безопасной эвакуации — 3 года\n"
        "ОТ-7 Инструкция по ОТ при работе с ручным электроинструментом — 3 года\n"
        "ОТ-8 Инструкция по выполнению работ на высоте — 3 года\n"
        "ОТ-9 Инструкция по погрузочно-разгрузочным работам — 3 года\n"
        f"{''.join(chr(10)+'ОТ-'+str(10+i)+' Инструкция по ОТ '+prof+' — 3 года' for i,prof in enumerate(professions[:7]))}\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} СУОТ - Перечень инструкций по ОТ.docx", text)

    p("Общие инструкции ОТ 1-9 (параллельно)...")
    general_instr = [
        "1 Инструкция о проведении контроля за соблюдением законодательства по охране труда",
        "2 Общеобъектовая инструкция о мерах пожарной безопасности",
        "3 Инструкция по охране труда при работе с персональным компьютером",
        "4 Инструкция по оказанию первой медицинской помощи пострадавшим",
        "5 Инструкция действий работников в случае возникновения пожара",
        "6 Инструкция по безопасной эвакуации работников при пожаре",
        "7 Инструкция по охране труда при работе с ручным электроинструментом",
        "8 Инструкция по охране труда при выполнении работ на высоте",
        "9 Инструкция по охране труда при выполнении погрузочно-разгрузочных работ",
    ]
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get("director")))
    instr_tasks = []
    for instr_name in general_instr:
        safe = re.sub(r"[^\w\s-]","",instr_name)[:60]
        instr_tasks.append(
            (f"{org} СУОТ - {safe}.docx", vibe_call,
             ([{"role":"user","content":
                f"Создай: {instr_name}\n"
                f"{build_header(instr_name.upper(), company, dates, resp, 'goals')}\n"
                f"Компания: {full}, область: {company.get('scope','')}.\n"
                "Структура: Глава 1 Общие требования, Глава 2 До начала работы, "
                "Глава 3 При выполнении, Глава 4 По окончании, Глава 5 Аварийные ситуации.\n"
                "Отвечай только текстом."}], api_key))
    )
    # Параллельно через ThreadPool
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(vibe_call, msgs, key): fname
                for fname, _, (msgs, key) in instr_tasks}
        for fut in as_completed(futs):
            fname = futs[fut]
            try:
                add(fname, fut.result())
            except Exception as e:
                print(f"  Ошибка {fname}: {e}")

    p(f"Инструкции ОТ по профессиям ({len(professions)} шт., параллельно)...")
    ot_tasks = []
    for prof in professions:
        safe = re.sub(r"[^\w\s-]","",prof)[:40]
        ot_tasks.append((f"{org} СУОТ - Инструкция ОТ {safe}.docx",
                         gen_ot_instruction, (prof, company, dates, resp, api_key)))
    for fname, txt in _parallel(ot_tasks, max_workers=4).items():
        add(fname, txt)

    p("Карты рисков (параллельно)...")
    office_pos = [pp.get("position","") for pp in itr
                  if any(k in pp.get("position","").lower()
                         for k in ["директор","бухгалтер","кадр","юрис","техник","зав"])]
    if not office_pos:
        office_pos = ["Директор", "Главный бухгалтер"]
    prod_pos = [pp.get("position","") for pp in itr
                if any(k in pp.get("position","").lower()
                       for k in ["инженер","прораб","мастер","производитель"])]
    if not prod_pos:
        prod_pos = ["Главный инженер", "Производитель работ"]

    risk_tasks = [
        (f"{org} СУОТ - 3 Директор, гл бух.docx",
         gen_risk_card, ("office", office_pos, company, dates, resp, api_key)),
        (f"{org} СУОТ - 4 ГИ, прораб, мастер.docx",
         gen_risk_card, ("production", prod_pos, company, dates, resp, api_key)),
        (f"{org} СУОТ - Карта рисков Посетители.docx",
         gen_risk_card, ("office", ["Посетители"], company, dates, resp, api_key)),
    ]
    for prof in professions:
        safe = re.sub(r"[^\w\s-]","",prof)[:40]
        risk_tasks.append((f"{org} СУОТ - Карта рисков {safe}.docx",
                           gen_risk_card, ("worker", [prof], company, dates, resp, api_key)))
    for fname, txt in _parallel(risk_tasks, max_workers=4).items():
        add(fname, txt)

    p("Реестр неприемлемых рисков + программа управления ОТ...")
    profs_str = ", ".join(professions)
    aud_str = ", ".join(_fio(a) for a in resp.get("auditors",[]))
    text = vibe_call([{"role":"user","content":
        f"Создай РЕЕСТР НЕПРИЕМЛЕМЫХ РИСКОВ (OR > 9).\n"
        f"{build_header('РЕЕСТР НЕПРИЕМЛЕМЫХ РИСКОВ', company, dates, resp, 'goals')}\n"
        f"Профессии: {profs_str}\n"
        "Неприемлемые риски (OR > 9): падение с высоты (3x4=12), обрушение (3x4=12), поражение током (2x5=10), хим.воздействие при сварке (3x4=12).\n"
        f"Подписи: {aud_str}\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} СУОТ - 2 Реестр неприемлемых рисков.docx", text)

    text = vibe_call([{"role":"user","content":
        f"Создай ПРОГРАММУ УПРАВЛЕНИЯ ОХРАНОЙ ТРУДА и снижения неприемлемых рисков.\n"
        f"{build_header(f'Программа управления охраной труда на {year} г.', company, dates, resp, 'goals')}\n"
        "Таблица: N|Должность|Риск|Цель|Мероприятие|Срок|Ресурсы|Ответственный|Выполнение\n"
        f"Профессии: {profs_str}\n"
        "Мероприятия: обеспечение СИЗ, наряды-допуски, обучение, медосмотры.\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} СУОТ - 1 Программа управления ОТ.docx", text)

    p("Реестр НПА...")
    add(f"{org} СУОТ - 5 Реестр НПА.docx",
        gen_reestr_npa(company, dates, resp, api_key))

    p("SWOT-анализ OH&S + оценка возможностей...")
    add(f"{org} СУОТ - SWOT анализ OH&S.docx",
        gen_swot_suot(company, dates, resp, api_key))
    add(f"{org} СУОТ - 11 Оценка возможностей OH&S.docx",
        gen_vozmozhnosti_suot(company, dates, resp, api_key))

    p("Протоколы проверки знаний (ИТР и рабочие)...")
    add(f"{org} СУОТ - Протокол проверки знаний ИТР.docx",
        gen_protokol_proverki(1, dates["goals"], False, company, dates, resp, itr, workers, api_key))
    # Протокол 2 — рабочие, дата на 14 дней позже
    from datetime import datetime as _dt, timedelta as _td
    try:
        d2 = _dt.strptime(dates["goals"], "%d.%m.%Y") + _td(days=14)
        date2 = d2.strftime("%d.%m.%Y")
    except:
        date2 = dates["goals"]
    add(f"{org} СУОТ - Протокол проверки знаний рабочих.docx",
        gen_protokol_proverki(2, date2, True, company, dates, resp, itr, workers, api_key))

    p("Билеты для проверки знаний...")
    add(f"{org} СУОТ - Билеты рабочих.docx",
        gen_bilety(True, company, dates, resp, api_key))
    add(f"{org} СУОТ - Билеты ИТР.docx",
        gen_bilety(False, company, dates, resp, api_key))
    add(f"{org} СУОТ - Перечень вопросов для проверки знаний.docx",
        gen_voprosy_proverki(company, dates, resp, api_key))

    p("Программы аудитов OH&S + контрольные листы + отчёты...")
    text = vibe_call([{"role":"user","content":
        f"Создай ПРОГРАММУ ПРОВЕДЕНИЯ ВНУТРЕННИХ АУДИТОВ OH&S на {year} год.\n"
        f"{build_header(f'Программа проведения внутренних аудитов OH&S на {year} год', company, dates, resp, 'goals')}\n"
        "Таблица: N|Проверяемые лица|Критерии (СТБ ISO 45001)|Сроки (месяцы года)|Отметка\n"
        f"Аудиторы: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}\n"
        "Отвечай только текстом."}], api_key)
    add(f"{org} СУОТ - Программа внутренних аудитов OH&S.docx", text)

    fio_dir = _fio(resp.get("director")) + " (" + _pos(resp.get("director","{}")) + ")"
    add(f"{org} СУОТ - Контрольный лист аудита 1.docx",
        gen_kontrol_list_audit(1, dates["reports"], fio_dir, company, resp, api_key))

    from datetime import datetime as _dt2, timedelta as _td2
    try:
        d3 = _dt2.strptime(dates["reports"], "%d.%m.%Y") + _td2(days=11)
        date3 = d3.strftime("%d.%m.%Y")
    except:
        date3 = dates["reports"]
    add(f"{org} СУОТ - Контрольный лист аудита 2.docx",
        gen_kontrol_list_audit(2, date3, fio_dir, company, resp, api_key))
    add(f"{org} СУОТ - Отчёт по внутреннему аудиту 1.docx",
        gen_otchet_audit_suot(1, date3, company, resp, api_key))
    add(f"{org} СУОТ - Отчёт по внутреннему аудиту 2.docx",
        gen_otchet_audit_suot(2, dates["reports"], company, resp, api_key))
    add(f"{org} СУОТ - Отчёт для анализа OH&S.docx",
        gen_otchet_analiz_suot(company, dates, resp, itr, workers, [], api_key))

    p("Акты и графики УТЗ...")
    temy_utz = [
        "Порядок эвакуации пострадавших из опасной зоны при пожаре",
        "Порядок действий при временном прекращении подачи электроэнергии",
        "Прорыв трубопроводов горячей воды",
    ]
    try:
        base_d = _dt.strptime(dates["goals"], "%d.%m.%Y")
        utz_dates = [
            (base_d + _td(days=18)).strftime("%d.%m.%Y"),
            (base_d + _td(days=127)).strftime("%d.%m.%Y"),
            (base_d + _td(days=229)).strftime("%d.%m.%Y"),
        ]
    except:
        utz_dates = [dates["goals"]] * 3

    for i, (tema, dt) in enumerate(zip(temy_utz, utz_dates), 1):
        add(f"{org} СУОТ - Акт УТЗ {i}.docx",
            gen_akt_utz(i, dt, tema, company, dates, resp, itr, api_key))
    add(f"{org} СУОТ - 3.2 График УТЗ.docx",
        gen_grafik_utz(company, dates, resp, api_key))

    p("Программы вводного инструктажа и пожарной безопасности...")
    add(f"{org} СУОТ - 3.3 Программа вводного инструктажа.docx",
        gen_programa_vvodnogo(company, dates, resp, api_key))
    add(f"{org} СУОТ - Программа по пожарной безопасности.docx",
        gen_programa_pozhar(company, dates, resp, api_key))

    p("Руководство OH&S + СТП 8.1-02...")
    add(f"{org} СУОТ - Руководство OH&S 45001.docx",
        gen_rukovodstvo_suot(company, dates, resp, itr, workers, [], api_key))
    add(f"{org} СУОТ - СТП OH&S 8.1-02 Управление операциями.docx",
        gen_stp_suot(company, dates, resp, itr, workers, api_key))

    p("Положение о службе ОТ + схема аварий...")
    add(f"{org} СУОТ - 3.1 Положение о службе ОТ.docx",
        gen_polozhenie_ot(company, dates, resp, itr, api_key))
    add(f"{org} СУОТ - 3.4 Схема прохождения информации при авариях.docx",
        gen_schema_avarii(company, dates, resp, api_key))

    p("Исходная информация СУОТ + анкета-вопросник...")
    add(f"{org} СУОТ - Исходная информация СУОТ.docx",
        gen_ishodnaya_smk(company, dates, resp, itr, workers, [], [], api_key))
    text = vibe_call([{"role":"user","content":
        f"Создай АНКЕТУ-ВОПРОСНИК для оценки соответствия СУОТ требованиям СТБ ISO 45001-2020.\n"
        f"Компания: {company.get('form','ООО')} «{_clean(company)}»\n"
        f"Область: {company.get('scope','')}\n"
        "Таблица (пункт СТБ ISO 45001|Вопрос|Да/Нет|Комментарий):\n"
        "4.1 Определены ли факторы? 4.2 Заинтересованные стороны?\n"
        "5.1 Лидерство? 6.1 Риски? 7.2 Компетентность?\n"
        "8.1 Операции? 9.1 Мониторинг? 9.2 Аудиты? 10.2 Инциденты?\n"
        "(30-35 вопросов)\nОтвечай только текстом."}], api_key)
    add(f"{org} СУОТ - Анкета-вопросник OH&S.docx", text)

    p("Журналы СУОТ (12 шт.)...")
    suot_journals = [
        (1, "Журнал регистрации вводного инструктажа по охране труда"),
        (2, "Журнал регистрации нарядов-допусков"),
        (3, "Журнал регистрации несчастных случаев"),
        (4, "Журнал регистрации профессиональных заболеваний"),
        (5, "Журнал контроля за соблюдением требований по охране труда"),
        (6, "Журнал проверок органов надзора и контроля"),
        (7, "Журнал учёта и испытаний лестниц"),
        (8, "Журнал приёмки и осмотра лесов и подмостей"),
        (9, "Журнал осмотра электроинструмента"),
        (10, "Журнал учёта выдачи инструкций по охране труда"),
        (11, "Журнал регистрации инструктажей по ОТ на рабочем месте"),
        (12, "Журнал учёта выдачи документации OH&S"),
    ]
    for jnum, jname in suot_journals:
        add(f"{org} СУОТ - {jnum} {jname[:60]}.docx",
            gen_zhurnal_suot(jnum, jname, company))

    p("5 Приложений к Руководству OH&S...")
    prilozhenia = [
        (1, "Организационная структура"),
        (2, "Перечень оборудования и устройств повышенной опасности (инвертор сварочный)"),
        (3, "Перечень разрешительных документов (свидетельство о тех.компетентности, сертификаты)"),
        (4, "Организационная структура службы ОТ"),
        (5, "Перечень документов и записей СУОТ"),
    ]
    for pnum, pname in prilozhenia:
        full2 = f"{company.get('form','ООО')} «{_clean(company)}»"
        dir2 = _initials(_fio(resp.get("director")))
        text = vibe_call([{"role":"user","content":
            f"Создай ПРИЛОЖЕНИЕ {pnum} к Руководству OH&S: {pname}\n"
            f"{full2}\n"
            f"ПРИЛОЖЕНИЕ {pnum}\n{pname}\n"
            f"Директор _____________ {dir2}\n"
            "Оформи официально с реальным содержанием.\nОтвечай только текстом."}], api_key)
        add(f"{org} СУОТ - Приложение {pnum} {pname[:40]}.docx", text)



def _gen_spk(org, company, dates, resp, itr, api_key, add, p, variant='spk_stroy'):
    """Полный пакет СПК Строй (12 докум.) или БИСП (+8 докум.)"""
    bisp = (variant == 'spk_bisp')
    work_types = company.get('work_types', [company.get('scope', 'Строительно-монтажные работы')])

    # Документ 1: Условия в помещениях
    p("СПК: Условия в производственных помещениях...")
    add(f"{org} СПК - 1 Условия в производственных помещениях.docx",
        gen_spk_usloviya(company, dates, resp, api_key))

    # Документ 2: Справка ИТР
    p("СПК: Справка ИТР...")
    add(f"{org} СПК - 2 Справка ИТР.docx",
        gen_spk_spravka_itr(company, dates, resp, itr, api_key))

    # Документ 3: Оргструктура СПК
    p("СПК: Оргструктура...")
    add(f"{org} СПК - 3 Организационная структура СПК.docx",
        gen_spk_orgstruktura(company, dates, resp, itr, api_key))

    # Документы 4.1-4.4: Приказы
    p("СПК: Приказы 1-4 (параллельно)...")
    prikaz_names = [
        (1, "О внесении изменений в систему производственного контроля"),
        (2, "О проведении внутреннего обучения специалистов"),
        (3, "О технических осмотрах средств измерений"),
        (4, "О назначении ответственного за средства малой механизации"),
    ]
    spk_prikaz_tasks = [
        (f"{org} СПК - 4.{num} Приказ {num} {name[:40]}.docx",
         gen_spk_prikaz, (num, name, company, dates, resp, itr, api_key))
        for num, name in prikaz_names
    ]
    for fname, txt in _parallel(spk_prikaz_tasks, max_workers=4).items():
        add(fname, txt)

    # Протокол внутреннего обучения
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get("director")))
    text = vibe_call([{"role":"user","content":
        f"Создай ПРОТОКОЛ N 1 внутреннего обучения специалистов СПК.\n"
        f"{full}\nПРОТОКОЛ N 1 {dates['goals']} г.\n"
        f"В соответствии с приказом от {dates['goals']} N 2/СПК проведено обучение.\n"
        f"Тема: контроль качества строительно-монтажных работ.\n"
        f"Обучены: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[])[:3])}.\n"
        "Результат: пройдено. Отвечай только текстом."}], api_key)
    add(f"{org} СПК - 4.2.2 Протокол обучения.docx", text)

    # Документ 5: Положение о СПК
    p("СПК: Положение о СПК...")
    add(f"{org} СПК - 5 Положение о системе производственного контроля.docx",
        gen_spk_polozhenie(company, dates, resp, itr, api_key))

    # Документ 6: Паспорт СПК
    p("СПК: Паспорт СПК...")
    add(f"{org} СПК - 6 Паспорт СПК.docx",
        gen_spk_pasport(company, dates, resp, itr, api_key))

    # Документ 7: Справка ТТК
    p("СПК: Справка ТТК...")
    add(f"{org} СПК - 7 Справка ТТК.docx",
        gen_spk_spravka_ttk(company, dates, resp, work_types, api_key))

    # Документ 8: Справка СИ
    p("СПК: Справка СИ...")
    add(f"{org} СПК - 8 Справка СИ.docx",
        gen_spk_spravka_si(company, dates, resp, api_key))

    # Гарантийное письмо 9.1 (ТТК) — для всех
    p("СПК: Гарантийное письмо на ТТК...")
    add(f"{org} СПК - 9.1 Гарантийное письмо на ТТК.docx",
        gen_spk_garantiynoe(1, "гарантийное письмо на ТТК", company, dates, resp, api_key))

    # План внутреннего аудита
    p("СПК: План внутреннего аудита...")
    add(f"{org} СПК - План внутреннего аудита.docx",
        gen_spk_plan_audita(company, dates, resp, api_key))

    # График поверки СИ
    p("СПК: График поверки СИ...")
    add(f"{org} СПК - График периодической поверки СИ.docx",
        gen_spk_grafik_poverki(company, dates, resp, api_key))

    # Технические требования — отдельный файл на каждый вид работ
    p(f"СПК: Технические требования ({len(work_types)} видов, параллельно)...")
    tt_tasks = []
    for wt in work_types[:6]:
        safe = re.sub(r"[^\w\s-]","",wt)[:50]
        tt_tasks.append((f"{org} СПК - Тех.требования {safe}.docx",
                         gen_tech_trebovaniya, (wt, company, dates, resp, api_key)))
    for fname, txt in _parallel(tt_tasks, max_workers=3).items():
        add(fname, txt)

    # БИСП — дополнительные документы
    if bisp:
        p("СПК БИСП: дополнительные документы (параллельно)...")

        # 5.2 Положение о входном контроле
        add(f"{org} СПК БИСП - 5.2 Положение о входном контроле.docx",
            gen_spk_polozhenie_vhod_kontrol(company, dates, resp, api_key))

        # Гарантийные письма 9.3 и 9.6
        bisp_tasks = [
            (f"{org} СПК БИСП - 9.3 Гарантийное письмо по лаборатории.docx",
             gen_spk_garantiynoe, (3, "гарантийное письмо по лаборатории", company, dates, resp, api_key)),
            (f"{org} СПК БИСП - 9.6 Гарантийное письмо об отсутствии рекламаций.docx",
             gen_spk_garantiynoe, (6, "гарантийное письмо об отсутствии рекламаций", company, dates, resp, api_key)),
            (f"{org} СПК БИСП - Перечень продукции входного контроля.docx",
             gen_spk_perech_produkcii, (company, dates, resp, work_types, api_key)),
        ]
        for fname, txt in _parallel(bisp_tasks, max_workers=3).items():
            add(fname, txt)

        # Справка о предприятии
        full2 = f"{company.get('form','ООО')} «{_clean(company)}»"
        dir2 = _initials(_fio(resp.get("director")))
        text = vibe_call([{"role":"user","content":
            f"Создай СПРАВКУ О ПРЕДПРИЯТИИ для СПК БИСП.\n"
            f"Исх. N от {dates['goals']} г.\nРУП «СтройМедиаПроект»\n"
            f"СПРАВКА\n{full2}\n"
            f"УНП: {company.get('unp','')}\n"
            f"Адрес: {company.get('address','')}\n"
            f"Директор: {_fio(resp.get('director'))}\n"
            f"Область деятельности: {company.get('scope','')}\n"
            f"Директор _____________ {dir2} М.П.\n"
            "Отвечай только текстом."}], api_key)
        add(f"{org} СПК БИСП - Справка о предприятии.docx", text)


# ═══════════════════════════════════════════════════════════════
# ДОПОЛНИТЕЛЬНЫЕ ГЕНЕРАТОРЫ СУОТ — недостающие 38 документов
# ═══════════════════════════════════════════════════════════════

def gen_suot_perech(num, title, company, dates, resp, itr, workers, api_key):
    """Перечень СУОТ (отдельный файл на каждый из 12)"""
    dir_fio = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    dir_pos = dir_p(resp)
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    itr_list = '\n'.join(f"{p.get('position','')} — {p.get('fio','')}" for p in itr)
    worker_list = '\n'.join(set(w.get('position','') for w in workers if w.get('position')))

    # Специфический контент для каждого перечня
    extra = {
        1: f"Аварийные ситуации: пожар, взрыв, обрушение, несчастный случай.\nМероприятия по предупреждению и реагированию.\nИсполнители: {dir_fio}, {_fio(resp.get('process_resp'))}",
        2: f"ИТР, проходящие проверку знаний по ОТ раз в 3 года:\n{itr_list}",
        3: f"Рабочие, проходящие проверку знаний по ОТ раз в год:\n{worker_list}",
        4: f"Лица, имеющие право выдачи наряда-допуска:\nДиректор — {dir_fio}\nЗаместитель директора — {_fio(next((p for p in itr if 'зам' in p.get('position','').lower()), resp.get('process_resp')))}",
        5: f"Профессии, для которых обязателен медосмотр:\n{worker_list}\nПериодичность: 1 раз в год",
        6: f"Рабочие места, на которых идентифицируются опасности и оцениваются риски:\n{itr_list}\n{worker_list}",
        7: f"Места с повышенной опасностью (требуется наряд-допуск):\n1. Работы на высоте более 1,8 м\n2. Работы в электроустановках\n3. Строительно-монтажные работы\n4. Работы в охранных зонах ЛЭП",
        8: f"Неэлектротехнический персонал:\n{itr_list}\n{worker_list}",
        9: f"Рабочие, проходящие стажировку перед допуском (2-5 дней):\n{worker_list}",
        10: "Субподрядчики, работающие на объектах: Отсутствуют",
        11: f"Рабочие места электробезопасности (1 группа):\n{worker_list}",
        12: f"СИЗ по нормам для профессий:\n{worker_list}\nКостюм, ботинки, рукавицы, каска, страховочная привязь (при работе на высоте)",
    }.get(num, f"Перечень № {num}")

    prompt = f"""Создай ПЕРЕЧЕНЬ № {num} для СУОТ ISO 45001: {title}

Компания: {full}
УТВЕРЖДАЮ {dir_pos} {full} _____________ {dir_init} {dates['goals']} г.

{title.upper()}

{extra}

Оформи как официальный документ с подписью директора.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_rukovodstvo_suot(company, dates, resp, itr, workers, objects, api_key):
    """Руководство OH&S (Р OH&S 01) — большой документ"""
    dir_fio = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    dir_pos = dir_p(resp)
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    year = dates['year']
    itr_list = '\n'.join(f"— {p.get('fio','')} ({p.get('position','')})" for p in itr)
    auditors = ', '.join(_fio(a) + ' (' + _pos(a) + ')' for a in resp.get('auditors',[]))
    obj_list = '\n'.join(f"— {o.get('name','')} ({o.get('year','')}, заказчик: {o.get('customer','')})" for o in objects) if objects else "Объекты в процессе привлечения"

    prompt = f"""Создай РУКОВОДСТВО ПО СИСТЕМЕ МЕНЕДЖМЕНТА ЗДОРОВЬЯ И БЕЗОПАСНОСТИ ПРИ ПРОФЕССИОНАЛЬНОЙ ДЕЯТЕЛЬНОСТИ ISO 45001.

{full}
УТВЕРЖДАЮ {dir_pos} {full} _____________ {dir_init} {dates['goals']} г.
Р OH&S 01-{year}

СОДЕРЖАНИЕ (все разделы):
Введение
1. Область применения: {company.get('scope','')}
2. Нормативные ссылки (СТБ ISO 45001-2020)
3. Термины и определения
4. Контекст организации:
   4.1 Внешние и внутренние факторы
   4.2 Заинтересованные стороны
   4.3 Область применения OH&S
   4.4 Система менеджмента OH&S
5. Лидерство:
   5.1 Лидерство и приверженность — Директор {dir_fio}
   5.2 Политика OH&S
   5.3 Роли, ответственность и полномочия:
      Директор {dir_fio} — высшее руководство
      ИТР: {itr_list}
6. Планирование:
   6.1 Риски и возможности
   6.2 Цели OH&S
7. Поддержка (компетентность, осведомлённость, документация)
8. Операционная деятельность:
   8.1 Планирование и управление операциями
   8.1.2 Устранение опасностей и снижение рисков
   Объекты: {obj_list}
9. Оценивание результативности:
   9.1 Мониторинг
   9.2 Внутренний аудит — аудиторы: {auditors}
   9.3 Анализ со стороны руководства
10. Улучшение

Приложения:
1. Организационная структура
2. Перечень оборудования с повышенной опасностью
3. Перечень разрешительных документов
4. Организационная структура службы ОТ
5. Перечень документов и записей

Напиши полный текст руководства. Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key, max_tokens=4000)


def gen_stp_suot(company, dates, resp, itr, workers, api_key):
    """СТП OH&S 8.1-02 Управление операциями"""
    dir_fio = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    year = dates['year']
    worker_profs = list(set(w.get('position','') for w in workers if w.get('position')))

    prompt = f"""Создай СТП OH&S 8.1-02-{year} УПРАВЛЕНИЕ ОПЕРАЦИЯМИ для СУОТ ISO 45001.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
СТП OH&S 8.1-02-{year}

СОДЕРЖАНИЕ:
1. Область применения: {company.get('scope','')}
2. Обозначения и сокращения
3. Общие положения
4. Организация работ — ответственный: {dir_fio}
5. Порядок выполнения работ:
   5.1 Отстранение работающих от работы
   5.2 Монтажные и ремонтные работы
   5.3 Погрузочно-разгрузочные работы
   5.4 Работа с ручным электроинструментом
   5.5 Работа на высоте и с лестниц
   5.6 Работы с повышенной опасностью (наряд-допуск)
   5.7 Огневые работы
   5.8 Санитарно-технические работы
   5.9 Вентиляция и кондиционирование
   5.10 Работа с персональными компьютерами
   5.11 Взаимодействие с заинтересованными сторонами

Профессии рабочих: {', '.join(worker_profs)}

Лист регистрации изменений.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key, max_tokens=3000)


def gen_swot_suot(company, dates, resp, api_key):
    """SWOT-анализ OH&S"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай SWOT-анализ системы менеджмента здоровья и безопасности при профессиональной деятельности OH&S.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
SWOT-анализ OH&S

Область: {company.get('scope','')}

Таблица SWOT с анализом для строительной компании:

СИЛЬНЫЕ СТОРОНЫ (S):
— Наличие внедрённой системы OH&S
— Квалифицированный ИТР
— Выполнение требований законодательства РБ по ОТ
— Проведение регулярных инструктажей

СЛАБЫЕ СТОРОНЫ (W):
— Высокая текучесть кадров в строительстве
— Работа на сторонних объектах (ограниченный контроль условий)
— Зависимость от субподрядчиков

ВОЗМОЖНОСТИ (O):
— Повышение квалификации персонала
— Внедрение новых СИЗ
— Расширение рынка за счёт сертифицированной системы ОТ

УГРОЗЫ (T):
— Изменения в законодательстве по ОТ
— Несчастные случаи на производстве
— Риски при работе с новыми подрядчиками

Оценка возможностей: таблица с баллами.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_vozmozhnosti_suot(company, dates, resp, api_key):
    """Оценка возможностей организации в области OH&S"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ОЦЕНКУ ВОЗМОЖНОСТЕЙ организации в области OH&S.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Оценка возможностей организации в области ОН&S

Таблица оценки возможностей:
| Возможность | Степень устранения причин риска (1-3) | Способность реализовать (1-3) | Уровень новых рисков (1-3) | Оценка (произведение) |

Возможности для {company.get('scope','')}:
1. Обеспечение безопасных условий труда на рабочих местах
2. Закупка современного оборудования и СИЗ
3. Обучение сотрудников безопасным условиям труда
4. Внедрение системы контроля подрядчиков
5. Автоматизация опасных операций

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_reestr_npa(company, dates, resp, api_key):
    """Реестр законодательных и других применяемых требований (НПА)"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай РЕЕСТР ЗАКОНОДАТЕЛЬНЫХ И ДРУГИХ ПРИМЕНЯЕМЫХ ТРЕБОВАНИЙ в области охраны труда.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Реестр законодательных и других применяемых требований в области охраны труда

Таблица: № п/п | Наименование документа | Вид хранения

Включи актуальные НПА Республики Беларусь по охране труда:
1. Закон РБ «Об охране труда» от 23.06.2008 № 356-З
2. Трудовой кодекс Республики Беларусь
3. СТБ ISO 45001-2020
4. Постановление Минтруда № 175 от 28.06.2008
5. Постановление Минтруда № 176 от 28.11.2008
6. Инструкция о порядке проведения обязательных медосмотров
7. ГОСТ 12.0.004-2015 Обучение безопасности труда
8. Постановление МЧС по пожарной безопасности
9. ТКП по строительству и монтажу
(всего 20-25 актуальных документов)

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_protokol_proverki(num, date_str, is_workers, company, dates, resp, itr, workers, api_key):
    """Протокол проверки знаний по ОТ"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    aud_list = resp.get('auditors', [])

    if is_workers:
        staff = workers
        title = "рабочих"
    else:
        staff = itr
        title = "руководителей и специалистов"

    rows = '\n'.join(
        f"{i+1}. {p.get('fio','')} — {p.get('position','')} — ПЕРИОДИЧЕСКАЯ — удовлетворительно"
        for i, p in enumerate(staff)
    )

    prompt = f"""Создай ПРОТОКОЛ проверки знаний по вопросам охраны труда.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {date_str} г.

ПРОТОКОЛ № {num}
проверки знаний по вопросам охраны труда {title}
от {date_str} г.

Комиссия (на основании приказа № 4-OH&S от {dates['goals']}):
Председатель: Директор {_fio(resp.get('director'))}
Члены комиссии: {', '.join(_fio(a) + ' (' + _pos(a) + ')' for a in aud_list[1:] if a)}

Проверены знания:
{rows}

Вид проверки: ПЕРИОДИЧЕСКАЯ
Результат: Удовлетворительно у всех проверяемых.

Подписи председателя и членов комиссии.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_bilety(is_workers, company, dates, resp, api_key):
    """Билеты для проверки знаний по ОТ"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    kind = "рабочих" if is_workers else "руководителей и специалистов"

    if is_workers:
        topics = """Билет №1: 1.Требования безопасности при работе на высоте. 2.Погрузочно-разгрузочные работы. 3.Опасные факторы при СМР. 4.Действия при пожаре.
Билет №2: 1.Правила работы с электроинструментом. 2.Средства защиты при работе на высоте. 3.Действия в аварийных ситуациях. 4.Работа с лестниц и стремянок.
Билет №3: 1.Требования к СИЗ. 2.Первая помощь при травмах. 3.Виды инструктажей по ОТ. 4.Порядок допуска к работам с повышенной опасностью.
Билет №4: 1.Электробезопасность при СМР. 2.Правила работы с ручным инструментом. 3.Меры пожарной безопасности. 4.Порядок расследования несчастных случаев."""
    else:
        topics = """Билет №1: 1.Виды и сроки инструктажей по ОТ. 2.Обучение и проверка знаний. 3.Требования безопасности при работе на высоте. 4.Сроки расследования НС.
Билет №2: 1.Первичный инструктаж на рабочем месте. 2.Внеочередная проверка знаний. 3.Тушение пожара в электроустановках. 4.Действия при авариях.
Билет №3: 1.Вводный инструктаж. 2.Первичные средства пожаротушения. 3.Порядок обеспечения СИЗ. 4.Специальное расследование НС.
Билет №4: 1.Обязанности нанимателя по ОТ. 2.Права работника в области ОТ. 3.Наряд-допуск на работы повышенной опасности. 4.Порядок хранения и выдачи инструкций по ОТ."""

    prompt = f"""Создай БИЛЕТЫ для проверки знаний по охране труда {kind}.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
БИЛЕТЫ для проверки знаний по охране труда {kind}

{topics}

(всего 6-8 билетов по 4 вопроса)
Оформи официально с подписью директора.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_voprosy_proverki(company, dates, resp, api_key):
    """Перечень вопросов для проверки знаний по ОТ"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ПЕРЕЧЕНЬ ВОПРОСОВ ДЛЯ ОБУЧЕНИЯ И ПРОВЕРКИ ЗНАНИЙ ПО ВОПРОСАМ ОХРАНЫ ТРУДА.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
ПЕРЕЧЕНЬ ВОПРОСОВ ДЛЯ ОБУЧЕНИЯ И ПРОВЕРКИ ЗНАНИЙ ПО ВОПРОСАМ ОХРАНЫ ТРУДА

1. Трудовое законодательство: правила внутреннего распорядка, рабочее время
2. Законодательство об охране труда: понятие ОТ, основные НПА
3. Производственный травматизм: виды, причины, расследование
4. Обязанности нанимателя и работника по ОТ
5. Инструктажи по ОТ (виды, сроки, порядок проведения)
6. Обучение и проверка знаний по ОТ
7. Средства индивидуальной защиты
8. Пожарная безопасность
9. Электробезопасность
10. Первая медицинская помощь
11. Охрана труда при строительно-монтажных работах: {company.get('scope','')}
12. Работы на высоте
13. Погрузочно-разгрузочные работы
14. Работа с электроинструментом

(20-25 вопросов с подпунктами)
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_akt_utz(num, date_str, tema, company, dates, resp, itr, api_key):
    """Акт о проведении учебно-тренировочных занятий"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    aud_list = resp.get('auditors', [])
    participants = '; '.join(p.get('fio','') + ' (' + p.get('position','') + ')' for p in itr)

    prompt = f"""Создай АКТ о проведении учебно-тренировочных занятий.

{dir_p(resp)} {full} _____________ {dir_init} {date_str} г.

Акт о проведении учебно-тренировочных занятий работающих при возникновении аварийных ситуаций или аварий

1. Тема: {tema}
2. Дата и время: {date_str} г. 13:00
3. Комиссия:
   Председатель: Директор {_fio(resp.get('director'))}
   Члены: {', '.join(_fio(a) + ' (' + _pos(a) + ')' for a in aud_list[1:] if a)}
4. Участники: {participants if participants else 'все сотрудники'}
5. Несоответствия: нет

Директор _____________ {dir_init}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_grafik_utz(company, dates, resp, api_key):
    """График учебно-тренировочных занятий"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    year = dates['year']
    prompt = f"""Создай ГРАФИК учебно-тренировочных занятий работающих при возникновении аварийных ситуаций.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
График учебно-тренировочных занятий на {year} г.

Таблица:
| Имитируемая ситуация | Документ (порядок действий) | Срок | Руководитель | Отметка |

1. Порядок эвакуации при пожаре — План эвакуации — II квартал {year} — Директор
2. Прекращение подачи электроэнергии — Инструкции по ОТ — IV квартал {year} — Директор
3. Прорыв трубопроводов — Перечень аварийных ситуаций — IV квартал {year} — Директор

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_kontrol_list_audit(num, date_str, fio_checked, company, resp, api_key):
    """Контрольный лист для проведения внутреннего аудита OH&S"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    prompt = f"""Создай КОНТРОЛЬНЫЙ ЛИСТ для проведения внутреннего аудита OH&S № {num}.

Организация: {full}
Проверяемое должностное лицо: {fio_checked}
Дата: {date_str}

ДОКУМЕНТАЦИЯ OH&S:
— Руководство по OH&S
— Политика OH&S
— Карты идентификации опасностей и оценки рисков
— Программа достижения целей в области ОТ
— Другие записи по элементам OH&S

Перечень вопросов (по разделам СТБ ISO 45001-2020):
4.1 Определены ли внешние и внутренние факторы OH&S? — Да
4.2 Определены ли заинтересованные стороны? — Да
5.1 Демонстрирует ли руководство лидерство? — Да
6.1 Проведена ли оценка рисков? — Да
7.2 Обеспечена ли компетентность персонала? — Да
8.1 Управляются ли операционные процессы? — Да
9.1 Осуществляется ли мониторинг? — Да
10.2 Расследуются ли инциденты? — Да

Результат: несоответствий не выявлено.
Аудитор: {_fio(resp.get('director'))}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_otchet_audit_suot(num, date_str, company, resp, api_key):
    """Отчёт по внутреннему аудиту OH&S"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ОТЧЁТ по внутреннему аудиту OH&S № {num}.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {date_str} г.

Отчёт по внутреннему аудиту
Директор от {date_str} г. № {num}

Элемент OH&S | НД | Кол-во несоответствий | Корректирующих действий | Рекомендаций

Выявленных нарушений нет. Протокол несоответствий не составлялся.

Заключение: СУОТ функционирует результативно.
Система соответствует требованиям СТБ ISO 45001-2020.

Аудитор: {_fio(resp.get('director'))} _____________
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_otchet_analiz_suot(company, dates, resp, itr, workers, objects, api_key):
    """Отчёт для анализа OH&S со стороны руководства"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    obj_str = '; '.join(o.get('name','') for o in objects) if objects else "Объекты в процессе привлечения"
    prompt = f"""Создай ОТЧЁТ о результативности функционирования системы менеджмента здоровья и безопасности при профессиональной деятельности.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['reports']} г.

ОТЧЁТ о результативности функционирования OH&S
за период {dates['audit_obj']}

1. Выполнение Плана мероприятий по ОТ: выполнено
2. Идентификация опасностей и оценка рисков: проведена для всех рабочих мест
3. Изменения в документах OH&S: внесены в соответствии с актуальным штатом
4. Достижение Целей в области ОТ: выполнено
5. Соблюдение Графика УТЗ: выполнено
6. Аварии, несчастные случаи, профзаболевания: не зарегистрированы
7. Результаты аудитов: несоответствий не выявлено
8. Объекты: {obj_str}
9. Изменения в законодательстве: отслежены

Заключение: СУОТ функционирует результативно.

Директор _____________ {dir_init}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_programa_vvodnogo(company, dates, resp, api_key):
    """Программа вводного инструктажа по ОТ"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ПРОГРАММУ вводного инструктажа по охране труда и пожарной безопасности.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Программа вводного инструктажа по охране труда, пожарной безопасности

Полное наименование: {full}
Адрес: {company.get('address','')}
Область деятельности: {company.get('scope','')}

Темы (таблица: № | Тема | Часы):
1. Сведения об организации и характере деятельности — 0.5 ч
2. Основные положения законодательства по ОТ — 0.5 ч
3. Правила внутреннего трудового распорядка — 0.5 ч
4. Производственный травматизм и профзаболевания — 0.5 ч
5. Требования пожарной безопасности — 0.5 ч
6. Первая медицинская помощь — 0.5 ч
7. Средства индивидуальной защиты — 0.5 ч
8. Электробезопасность — 0.5 ч
Итого: 4 часа

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_programa_pozhar(company, dates, resp, api_key):
    """Программа обучения пожарной безопасности"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ПРОГРАММУ для обучения работников по обеспечению пожарной безопасности.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Программа для обучения работников по обеспечению пожарной безопасности

Тема 1. Введение (2 ч): статистика пожаров в РБ, законодательство
Тема 2. Организационные мероприятия (2 ч): обязанности, инструктажи
Тема 3. Пожарная профилактика (2 ч): причины, меры предупреждения
Тема 4. Первичные средства пожаротушения (2 ч): огнетушители, применение
Тема 5. Действия при пожаре (2 ч): эвакуация, оповещение, вызов МЧС
Итого: 10 часов

Область деятельности: {company.get('scope','')}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_polozhenie_ot(company, dates, resp, itr, api_key):
    """Положение о службе охраны труда"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ПОЛОЖЕНИЕ О СЛУЖБЕ ОХРАНЫ ТРУДА организации.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Положение о службе охраны труда организации

1. Общие положения (на основании Закона РБ «Об охране труда» от 23.06.2008)
2. Структура службы ОТ: служба ОТ подчиняется директору {_fio(resp.get('director'))}
3. Основные задачи службы ОТ
4. Функции службы ОТ
5. Права службы ОТ
6. Ответственность

Численность персонала: {len(itr)} ИТР
Область: {company.get('scope','')}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_schema_avarii(company, dates, resp, api_key):
    """Схема прохождения информации при авариях"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай СХЕМУ прохождения информации при возникновении аварий и аварийных ситуаций.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Схема прохождения информации при возникновении аварий, аварийных ситуаций

Иерархия оповещения:

Любой работник → Директор {_fio(resp.get('director'))} → Органы МЧС / Милиция / Скорая помощь / МЧС
                                ↓
                  Заместитель директора / Главный инженер
                                ↓
               Центр гигиены и эпидемиологии (при профзаболевании)
               Прокуратура (при тяжёлом/смертельном НС)
               Страховая организация

Контактные телефоны:
— Пожарная служба: 101
— Скорая помощь: 103
— Милиция: 102
— МЧС: 112

Отвечай только текстом документа (4 схемы для разных ситуаций: пожар, НС, авария, ЧС)."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_zhurnal_suot(num, name, company, api_key=None):
    """Журнал СУОТ (пустой шаблон)"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"

    journal_headers = {
        1: "Дата | Вид инструктажа | ФИО инструктируемого | Должность | ФИО инструктирующего | Подпись",
        2: "№ наряда-допуска | Дата выдачи | Адрес объекта | Вид работ | Выдал | Получил | Выполнено",
        3: "Дата | ФИО | Профессия | Место | Обстоятельства | Диагноз | Акт Н-1",
        4: "№ | Акт ПЗ-1 | ФИО | Возраст | Профессия | Диагноз | Дата",
        5: "Дата | Структурное подразделение | Нарушения | ФИО руководителя | Подпись",
        6: "Дата | Тип нарушения | Описание | Принятые меры | Подпись",
        7: "№ | Тип лестницы | Дата испытания | Результат | Дата следующего | Подпись",
        8: "Место установки | Тип лесов | Дата приёмки | Заключение | ФИО | Подпись",
        9: "Дата | Наименование оборудования | Дефекты | Лицо проводившее осмотр | Устранение",
        10: "Дата | Обозначение инструкции | Наименование | Получатель | Кол-во | Подпись",
        11: "Дата | Причина | Отметка (прошёл/не прошёл) | Дата следующей | Подпись | Номер",
        12: "№ | Дата | № записи | Наименование | Подразделение | Кол-во | Получатель | Подпись",
    }

    header = journal_headers.get(num, "Дата | Описание | Ответственный | Подпись")

    text = f"""{full}

{name.upper()}

Начат: _________________ Окончен: _________________

{header}

_____________________________________________
В настоящем журнале пронумеровано, прошнуровано и скреплено печатью _____ листов

{dir_p({'director': {'position': company.get('director_position','Директор')}}) if False else 'Директор'} _____________ ({company.get('director_fio','')})"""
    return text


def _clean(company):
    """Убирает форму из названия"""
    import re as _re
    raw = company.get('name', '')
    clean = _re.sub(r'^(ООО|ОДО|ЧУП|ЗАО|РУП|ИП|ЧТУП|ЧТУ|ОАО|ЧП)\s*[«"\']?\s*', '', raw).strip().strip('»"\'')
    return clean if clean else raw


# ═══════════════════════════════════════════════════════════════
# ДОПОЛНИТЕЛЬНЫЕ ГЕНЕРАТОРЫ ИСО — недостающие документы
# ═══════════════════════════════════════════════════════════════

def gen_rk_smk(company, dates, resp, itr, objects, suppliers, api_key):
    """РК СМК 01 — Руководство по качеству"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    year = dates['year']
    itr_list = '\n'.join(f"— {p.get('fio','')} ({p.get('position','')})" for p in itr)
    obj_list = '\n'.join(f"— {o.get('name','')}" for o in objects) if objects else "Объекты в процессе привлечения"
    sup_list = '\n'.join(f"— {s.get('name','')} ({s.get('type','')})" for s in suppliers[:4]) if suppliers else "Поставщики по потребности"
    auditors = ', '.join(_fio(a) for a in resp.get('auditors',[]))

    prompt = f"""Создай РУКОВОДСТВО ПО КАЧЕСТВУ (РК СМК 01-{year}) для системы менеджмента качества ISO 9001.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Введён в действие: Приказ от {dates['goals']} № 2-СМК
РК СМК 01-{year}

СОДЕРЖАНИЕ (все разделы ISO 9001:2015):

Введение
1. Область применения СМК: {company.get('scope','')}
2. Нормативные ссылки (СТБ ISO 9001-2015)
3. Термины и определения
4. Контекст организации:
   4.1 Внешние и внутренние факторы
   4.2 Заинтересованные стороны
   4.3 Область применения СМК
   4.4 Процессы СМК (карта процессов)
5. Лидерство:
   5.1 Директор {_fio(resp.get('director'))} — высшее руководство
   5.2 Политика в области качества
   5.3 Роли и ответственность ИТР:
      {itr_list}
6. Планирование:
   6.1 Риски и возможности
   6.2 Цели в области качества
   6.3 Планирование изменений
7. Поддержка:
   7.1 Ресурсы, инфраструктура
   7.2 Компетентность
   7.3 Осведомлённость
   7.4 Обмен информацией
   7.5 Документированная информация
8. Операционная деятельность:
   8.1 Планирование и управление
   8.2 Требования к продукции/услугам
   8.4 Управление поставщиками:
      {sup_list}
   8.5 Производство: {company.get('scope','')}
      Объекты: {obj_list}
   8.6 Выпуск продукции/услуг
   8.7 Управление несоответствующими выходами
9. Оценивание результативности:
   9.1 Мониторинг
   9.2 Внутренний аудит — аудиторы: {auditors}
   9.3 Анализ со стороны руководства
10. Улучшение

Лист согласований. Лист регистрации изменений.
Напиши полный текст руководства. Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key, max_tokens=4000)


def gen_stp_smk(num, name, company, dates, resp, itr, api_key):
    """СТП СМК 02/03/04"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    year = dates['year']

    contents = {
        2: f"""УПРАВЛЕНИЕ РИСКАМИ
1. Область применения
2. Обозначения
3. Порядок идентификации рисков и возможностей
4. Оценка рисков (P × S = OR, категории: допустимый/значительный/критический)
5. Меры по устранению рисков
6. Мониторинг рисков
Ответственные: {_fio(resp.get('director'))}, {', '.join(_fio(a) for a in resp.get('auditors',[]))}\n Лист регистрации изменений.""",
        3: f"""УПРАВЛЕНИЕ ДОКУМЕНТИРОВАННОЙ ИНФОРМАЦИЕЙ
1. Область применения
2. Порядок разработки документов СМК
3. Идентификация и хранение документов
4. Управление изменениями
5. Документы внешнего происхождения (ТНПА)
6. Записи СМК (журналы, протоколы)
Ответственный за ДИ: {_fio(resp.get('di_resp'))} ({_pos(resp.get('di_resp'))})\n Лист регистрации изменений.""",
        4: f"""ПРОЦЕСС ПРОИЗВОДСТВА СТРОИТЕЛЬНО-МОНТАЖНЫХ РАБОТ
1. Область применения: {company.get('scope','')}
2. Входные данные процесса (договор, ПСД, ТТК)
3. Выходные данные (выполненные работы, исполнительная документация)
4. Управление процессом (входной/операционный/приёмочный контроль)
5. Ответственный за процесс: {_fio(resp.get('process_resp'))} ({_pos(resp.get('process_resp'))})
6. Критерии и методы контроля
7. Ресурсы процесса\n Лист регистрации изменений.""",
    }

    prompt = f"""Создай СТП СМК 0{num}-{year}: {name}

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Введён в действие: Приказ от {dates['goals']} № 2-СМК
СТП СМК 0{num}-{year}

СТАНДАРТ ОРГАНИЗАЦИИ
Система менеджмента качества
{name.upper()}

{contents.get(num, '')}

Напиши полный текст стандарта организации. Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key, max_tokens=3000)


def gen_anket_smk(company, dates, resp, api_key):
    """Анкета-вопросник СМК"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    prompt = f"""Создай АНКЕТУ-ВОПРОСНИК для оценки соответствия СМК требованиям СТБ ISO 9001-2015.

АНКЕТА-ВОПРОСНИК
для оценки соответствия системы менеджмента качества требованиям СТБ ISO 9001-2015

{full}
Область применения: {company.get('scope','')}
Ответственный за СМК: {_fio(resp.get('director'))}
Дата заполнения: _____________

Таблица (пункт ISO | Вопрос | Да/Нет | Комментарий):
4.1 Определены ли внешние и внутренние факторы?
4.2 Определены ли заинтересованные стороны?
5.1 Демонстрирует ли руководство лидерство?
5.2 Разработана ли Политика в области качества?
6.1 Идентифицированы ли риски и возможности?
6.2 Установлены ли цели в области качества?
7.1 Обеспечены ли необходимые ресурсы?
8.1 Осуществляется ли планирование деятельности?
8.4 Управляются ли внешние поставщики?
9.1 Проводится ли мониторинг?
9.2 Проводятся ли внутренние аудиты?
10.1 Проводится ли анализ несоответствий?
(всего 30-35 вопросов по всем разделам)
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_ishodnaya_smk(company, dates, resp, itr, workers, objects, suppliers, api_key):
    """Исходная информация СМК"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    prompt = f"""Создай СОСТАВ ОБЯЗАТЕЛЬНОЙ ИСХОДНОЙ ИНФОРМАЦИИ для разработки СМК.

Состав обязательной исходной информации для оценки системы менеджмента качества
{full}
Область применения СМК: {company.get('scope','')}
Ответственный: {_fio(resp.get('director'))}

1. Сведения о производстве:
   — Оргструктура: Директор → ИТР → Рабочие
   — Численность персонала: {len(itr)} ИТР + {len(workers)} рабочих = {len(itr)+len(workers)} чел.
   — Режим работы: 1 смена
   — Субподрядчики: нет/по потребности

2. Сведения о продукции/услугах:
   — Область: {company.get('scope','')}
   — Заказчики: {'; '.join(o.get('customer','') for o in objects[:3]) if objects else 'физические и юридические лица'}

3. Процессы СМК:
   — Основной: строительно-монтажные работы
   — Вспомогательные: закупки, управление документами

4. Поставщики: {'; '.join(s.get('name','') + ' (' + s.get('type','') + ')' for s in suppliers[:3]) if suppliers else 'по потребности'}

5. Нормативная база: СТБ ISO 9001-2015, ТНПА по строительству

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_plan_vozmozhnostei(company, dates, resp, objects, api_key):
    """План мероприятий по реализации возможностей"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    year = dates['year']
    prompt = f"""Создай ПЛАН МЕРОПРИЯТИЙ ПО РЕАЛИЗАЦИИ ВОЗМОЖНОСТЕЙ на {year} год.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
План мероприятий по реализации возможностей на {year} г.

Таблица: № | Возможность | Мероприятие | Срок | Ответственный

1. Поиск новых объектов — участие в тендерах — в течение {year} г. — {_fio(resp.get('director'))}
2. Расширение видов работ — обучение персонала — I полугодие {year} — {_fio(resp.get('process_resp'))}
3. Повышение качества — внедрение входного контроля — I квартал {year} — {_fio(resp.get('process_resp'))}
4. Снижение рекламаций — обучение рабочих — постоянно — {_fio(resp.get('process_resp'))}
5. Новые поставщики — мониторинг рынка — постоянно — {_fio(resp.get('di_resp'))}

Область: {company.get('scope','')}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_plan_ppr(company, dates, resp, machinery, api_key):
    """График планово-предупредительных ремонтов"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    year = dates['year']

    if not machinery:
        machinery = ["Автомобиль/транспортное средство"]

    mach_rows = '\n'.join(
        f"{i+1}. {m} | ТО | ПП |" + " |"*10
        for i, m in enumerate(machinery)
    )

    prompt = f"""Создай ГРАФИК ПЛАНОВО-ПРЕДУПРЕДИТЕЛЬНЫХ РЕМОНТОВ И ТЕХНИЧЕСКОГО ОБСЛУЖИВАНИЯ на {year} год.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
График планово-предупредительных ремонтов и технического обслуживания
на {year} год

Таблица: Наименование | янв | фев | мар | апр | май | июн | июл | авг | сен | окт | ноя | дек

{mach_rows}

ТО — техническое обслуживание, ПП — плановый ремонт

Ответственный: {_fio(resp.get('process_resp'))} ({_pos(resp.get('process_resp'))})
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_grafik_validacii(company, dates, resp, api_key):
    """График валидации процесса сварки"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    year = dates['year']
    prompt = f"""Создай ГРАФИК ВАЛИДАЦИИ ПРОЦЕССА СВАРКИ на {year} год.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
График валидации процесса сварки на {year} год

Таблица: Наименование | янв | фев | ... | дек
Валидация процесса сварки {year} | | | | | Х | | | | | | | |

Ответственный: {_fio(resp.get('process_resp'))} ({_pos(resp.get('process_resp'))})
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_zhurnal_iso(num, name, company, api_key=None):
    """Журнал ИСО (пустой шаблон)"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"

    headers = {
        1: "№ | Дата | Наименование | Исходящий №/Входящий № | Кому/От кого | Подпись",
        2: "№ | Дата рег. | № приказа | Краткое содержание | Кем подписан | Примечание",
        3: "№ | Дата | № договора | Стороны | Предмет | Сумма | Срок",
        4: "№ | Дата | Наименование документа | Откуда получен | Примечание",
        5: "№ | Дата | Должностное лицо | Тема аудита | Дата | Результат",
        6: "№ | Дата | Заказчик | Содержание претензии | Меры | Подпись",
        7: "№ | Дата выдачи | № инструкции | Наименование | ФИО | Кол-во | Подпись",
        8: "№ | Дата | Наименование ДИ | ФИО | Должность | Дата ознакомления | Подпись",
        9: "№ | Дата | Наименование продукции | Поставщик | Документы | Результат контроля | Подпись",
    }

    header = headers.get(num, "№ | Дата | Описание | Ответственный | Подпись")

    return f"""{full}

{name.upper()}

Начат: _________________ Окончен: _________________

{header}


_____________________________________________
В настоящем журнале пронумеровано, прошнуровано и скреплено печатью _____ листов

Директор _____________ ({company.get('director_fio','')})"""


# ═══════════════════════════════════════════════════════════════
# ГЕНЕРАТОРЫ СПК
# ═══════════════════════════════════════════════════════════════

def gen_spk_usloviya(company, dates, resp, api_key):
    """СПК Документ 1: Условия в производственных помещениях"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    prompt = f"""Создай таблицу УСЛОВИЯ В ПРОИЗВОДСТВЕННЫХ ПОМЕЩЕНИЯХ для СПК.

{full}

Таблица: Назначение помещений | Площадь м² | Температура °С и влажность % | Освещённость лк | Концентрация вредных веществ мг/м³ | Уровень шума дБА | Спецоборудование

Строки (нормативные значения по ТНПА):
1. Офисное помещение | — | +18..+22 / 40-60% | 300 лк | В норме по ГОСТ 12.1.005 | до 60 дБА | ПК, оргтехника
2. Место хранения инструмента | — | +10..+25 | 150 лк | В норме | до 70 дБА | стеллажи
3. Строительный объект (открытая площадка) | — | по погодным условиям | естественное освещение | В норме | по ТНПА | строит. техника и инструмент

Примечание: значения нормативные согласно СанПиН, ГОСТ 12.1.005, ТКП 45.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_spravka_itr(company, dates, resp, itr, api_key):
    """СПК Документ 2: Справка специалистов ИТР"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    rows = '\n'.join(
        f"{p.get('fio','')} | {p.get('position','')} | высшее/среднее техническое | входной, операционный, приёмочный контроль | протокол аттестации | стаж в строительстве"
        for p in itr if any(k in p.get('position','').lower() for k in ['директор','инженер','прораб','мастер','производитель','техник','зам'])
    )
    prompt = f"""Создай СПРАВКУ СПЕЦИАЛИСТОВ, осуществляющих производственный контроль, для СПК.

{full}

Таблица: ФИО | Должность | Образование | Виды контроля | Дата/№ аттестации | Доп. сведения

{rows if rows else f"{_fio(resp.get('director'))} | {_pos(resp.get('director'))} | высшее техническое | входной, операционный, приёмочный | протокол аттестации | стаж более 5 лет"}

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_orgstruktura(company, dates, resp, itr, api_key):
    """СПК Документ 3: Оргструктура СПК"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    itr_prod = [p for p in itr if any(k in p.get('position','').lower() for k in ['инженер','прораб','мастер','производитель'])]
    prompt = f"""Создай ОРГАНИЗАЦИОННУЮ СТРУКТУРУ системы производственного контроля для СПК.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Организационная структура системы производственного контроля

Схема подчинённости:

Система производственного контроля
    ↓
Директор {_fio(resp.get('director'))} — организация СПК, общее руководство
    ↓
{chr(10).join(f"{p.get('position','')} {p.get('fio','')} — входной, операционный, приёмочный контроль; обеспечение качества работ" for p in itr_prod[:3])}

Функции каждого специалиста в СПК прописаны в должностных инструкциях.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_prikaz(num, name, company, dates, resp, itr, api_key):
    """СПК: Приказ"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    city = company.get('city', 'Минск')

    extras = {
        1: f"Создать/обновить систему производственного контроля. Область: {company.get('scope','')}. Ответственный: {_fio(resp.get('director'))}.",
        2: f"Обучить специалистов СПК: {', '.join(_fio(a) + ' (' + _pos(a) + ')' for a in resp.get('auditors',[])[:3])}. Тема: контроль качества строительно-монтажных работ.",
        3: f"Назначить ответственным за ТО средств измерений: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}. Обеспечить своевременную поверку СИ.",
        4: f"Назначить ответственным за средства малой механизации: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}. Обеспечить исправность и ТО оборудования.",
    }

    prompt = f"""Создай ПРИКАЗ № {num}/СПК: {name}

{full}
ПРИКАЗ
{dates['goals']} г.    № {num}/СПК    г. {city}

{name}

{extras.get(num, '')}

Контроль за исполнением оставляю за собой.

{dir_p(resp)} _____________ {dir_init}

С приказом ознакомлены: {', '.join(_fio(p) for p in itr[:4])}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_polozhenie(company, dates, resp, itr, api_key):
    """СПК Документ 5: Положение о системе производственного контроля"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ПОЛОЖЕНИЕ О СИСТЕМЕ ПРОИЗВОДСТВЕННОГО КОНТРОЛЯ.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
ПОЛОЖЕНИЕ О СИСТЕМЕ ПРОИЗВОДСТВЕННОГО КОНТРОЛЯ {dates['year']}

1. ОБЩИЕ СВЕДЕНИЯ О СИСТЕМЕ ПРОИЗВОДСТВЕННОГО КОНТРОЛЯ
1.1 Область применения: {company.get('scope','')}
1.2 Нормативные ссылки: Закон РБ «Об архитектурной, градостроительной и строительной деятельности», ТКП 45-1.01-41-2006

2. ОБЪЕКТЫ ПРОИЗВОДСТВЕННОГО КОНТРОЛЯ
2.1 Виды контроля: входной, операционный, приёмочный
2.2 Контролируемые параметры по ТНПА

3. РЕСУРСЫ ПРОИЗВОДСТВЕННОГО КОНТРОЛЯ
3.1 Специалисты: {', '.join(_fio(p) + ' (' + _pos(p) + ')' for p in itr[:3])}
3.2 Средства измерений: нивелир, рулетка, уровень
3.3 Нормативная база: актуальные ГОСТ, СТБ, СП, ТКП

4. ПОРЯДОК ОСУЩЕСТВЛЕНИЯ КОНТРОЛЯ
4.1 Входной контроль материалов
4.2 Операционный контроль в процессе работ
4.3 Приёмочный контроль готовых работ

5. ДОКУМЕНТИРОВАНИЕ РЕЗУЛЬТАТОВ
5.1 Журналы входного, операционного, приёмочного контроля
5.2 Акты освидетельствования скрытых работ

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_pasport(company, dates, resp, itr, api_key):
    """СПК Документ 6: Паспорт СПК"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    itr_prod = [p for p in itr if any(k in p.get('position','').lower() for k in ['инженер','прораб','мастер','производитель','директор'])]
    prompt = f"""Создай ПАСПОРТ системы производственного контроля.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
ПАСПОРТ системы производственного контроля
{full}

1. Наименование и адрес: {full}, {company.get('address','')}
2. УНП: {company.get('unp','')}
3. Директор: {_fio(resp.get('director'))}
4. Область деятельности: {company.get('scope','')}
5. Специалисты СПК:
{chr(10).join(f"   — {p.get('fio','')} ({p.get('position','')})" for p in itr_prod)}
6. Средства измерений: нивелир, рулетка, уровень строительный (с актуальными свидетельствами о поверке)
7. Нормативная база: актуальные ТНПА по видам работ
8. Технологические карты: согласно Справке ТТК

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_spravka_ttk(company, dates, resp, work_types, api_key):
    """СПК Документ 7: Справка ТТК"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    if not work_types:
        work_types = [company.get('scope', 'Строительно-монтажные работы')]

    # Типовые ТТК для СМР
    ttk_map = {
        'штукатурн': ('ТТК-191271612.6-2022', 'ТК на выполнение штукатурных работ'),
        'малярн': ('ТТК-191271613.7-2022', 'ТК на выполнение малярных работ'),
        'облицовочн': ('ТТК-100299864.152-2013', 'ТК на устройство облицовки плиткой'),
        'кровельн': ('ТТК-800017207.009-2023', 'ТК на устройство кровли'),
        'электромонтажн': ('ТТК-100299864.155-2013', 'ТК на электромонтажные работы'),
        'общестроительн': ('ТТК-100299864.001-2013', 'ТК на общестроительные работы'),
        'монтаж': ('ТТК-100299864.152-2013', 'ТК на монтажные работы'),
        'ремонт': ('ТТК-191271612.1-2022', 'ТК на ремонтные работы'),
    }

    rows = []
    for i, wt in enumerate(work_types[:8], 1):
        wt_lower = wt.lower()
        ttk = None
        for key, (num, name) in ttk_map.items():
            if key in wt_lower:
                ttk = (num, name)
                break
        if not ttk:
            ttk = ('ТТК-100299864.001-2013', f'ТК на {wt}')
        rows.append(f"{i} | {ttk[0]} | {ttk[1]} | tehkarta.by | 2026")

    prompt = f"""Создай ПЕРЕЧЕНЬ ТИПОВЫХ ТЕХНОЛОГИЧЕСКИХ КАРТ в наличии у {full}.

{full}

Перечень типовых технологических карт
Таблица: № | Учётный № | Наименование | Разработчик | Срок действия

{chr(10).join(rows)}

Доступ: портал tehkarta.by или Стройдокумент.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_spravka_si(company, dates, resp, api_key):
    """СПК Документ 8: Справка СИ (средства измерений)"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    prompt = f"""Создай СВЕДЕНИЯ О СРЕДСТВАХ ИЗМЕРЕНИЯ (СИ) для СПК.

{full}

Сведения о средствах измерения
Таблица: № | Наименование и тип СИ | Технические характеристики | Кол-во | Зав.номер | Сведения о поверке

1 | Нивелир ATLAS KL24 | Класс точности 2,5 мм/км | 1 | — | Свидетельство о поверке
2 | Рулетка измерительная металлическая 50м | ГОСТ 7502, ц.д. 1мм | 2 | — | Поверена
3 | Уровень строительный | ГОСТ 9416, I группа точности | 1 | — | Поверен
4 | Линейка металлическая 300мм | ГОСТ 427 | 2 | — | Поверена
5 | Штангенциркуль ШЦ-150 | ГОСТ 166 | 1 | — | Поверен

Область применения СИ: контроль качества {company.get('scope','')}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_garantiynoe(num, name, company, dates, resp, api_key):
    """СПК: Гарантийное письмо"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    iskhod = f"Исх. № {dates['goals'].replace('.','')[4:]}-0{num} от {dates['goals']} г."

    contents = {
        1: f"гарантирует в дальнейшем осуществлять приобретение необходимых типовых технологических карт (ТТК) на выполняемые виды строительно-монтажных работ, а также своевременно обновлять доступ к базе технологических карт на портале tehkarta.by.",
        3: "гарантирует своевременное заключение договоров с аккредитованными лабораториями по мере необходимости.",
        6: f"сообщает, что в адрес {full} не поступали письменные рекламации (претензии) от заказчиков к качеству выполненных строительно-монтажных работ за отчётный период.",
    }

    bisp_addr = "РУП «Белорусский институт строительного проектирования» Управления делами Президента Республики Беларусь" if num == 3 else "РУП «СтройМедиаПроект»"
    prompt = f"""Создай ГАРАНТИЙНОЕ ПИСЬМО для СПК.

{full}
{iskhod}
{bisp_addr}

ГАРАНТИЙНОЕ ПИСЬМО

Настоящим {full}, в лице директора {_fio(resp.get('director'))}, действующего на основании Устава, {contents.get(num, name)}

Директор _____________ {dir_init}
М.П.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_plan_audita(company, dates, resp, api_key):
    """СПК: План внутреннего аудита"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ПЛАН ВНУТРЕННЕГО АУДИТА системы производственного контроля.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
ПЛАН ВНУТРЕННЕГО АУДИТА на {dates['year']} г.

Таблица: № | Объект аудита | Руководитель | Дата | Отметка

1 | Степень готовности СПК к оценке | {_fio(resp.get('director'))} (Директор) | {dates['goals']} | —
2 | Контроль качества выполненных работ | {_fio(resp.get('process_resp'))} | через 1 месяц | —
3 | Актуальность средств измерений | {_fio(resp.get('process_resp'))} | через 2 месяца | —

Цель аудита: подтверждение соответствия СПК требованиям Инструкции о порядке освидетельствования.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_grafik_poverki(company, dates, resp, api_key):
    """СПК: График периодической поверки СИ"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    year = dates['year']
    prompt = f"""Создай ГРАФИК ПЕРИОДИЧЕСКОЙ ПОВЕРКИ СРЕДСТВ ИЗМЕРЕНИЙ на {year} г.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
ГРАФИК ПЕРИОДИЧЕСКОЙ ПОВЕРКИ СРЕДСТВ ИЗМЕРЕНИЙ на {year} г.

Таблица: Наименование СИ | Тип | Кол-во | Периодичность поверки | Срок поверки | Отметка

1 | Нивелир ATLAS KL24 | 1 шт. | 1 раз в год | {year} г. | —
2 | Рулетка измерительная 50м | 2 шт. | 1 раз в год | {year} г. | —
3 | Уровень строительный | 1 шт. | 1 раз в год | {year} г. | —
4 | Штангенциркуль | 1 шт. | 1 раз в год | {year} г. | —

Ответственный: {_fio(resp.get('process_resp'))} ({_pos(resp.get('process_resp'))})
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_perech_produkcii(company, dates, resp, work_types, api_key):
    """СПК БИСП: Перечень продукции подлежащей входному контролю"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))

    # Типовые материалы для СМР
    materials = [
        ("Шурупы с потайной головкой", "ГОСТ 1144-80", "Вид, размер, покрытие", "Входной/каждая партия"),
        ("Цемент ПЦ 500", "СТБ 1183-99", "Прочность, сроки схватывания", "Входной/каждая партия"),
        ("Кирпич строительный", "СТБ 1160-99", "Марка, размеры, вид", "Входной/выборочно"),
        ("Сухие строительные смеси", "ГОСТ 31189-2015", "Марка, срок годности", "Входной/каждая партия"),
        ("Краски и грунтовки", "ГОСТ 28196-89", "Вид, цвет, консистенция", "Входной/каждая партия"),
        ("Кабель электрический", "ГОСТ 31996-2012", "Сечение, изоляция", "Входной/каждая партия"),
    ]

    rows = '\n'.join(f"{i+1} | {m[0]} | {m[1]} | {m[2]} | {m[3]} | —" for i, m in enumerate(materials))

    prompt = f"""Создай ПЕРЕЧЕНЬ ПРОДУКЦИИ, подлежащей входному контролю.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
Перечень продукции, подлежащей входному контролю

Таблица: № | Наименование | ТНПА | Контролируемые показатели | Вид контроля | Примечание

{rows}

Область работ: {company.get('scope','')}
Ответственный: {_fio(resp.get('process_resp'))}
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_spk_polozhenie_vhod_kontrol(company, dates, resp, api_key):
    """СПК БИСП: Положение о входном контроле"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))
    prompt = f"""Создай ПОЛОЖЕНИЕ О ВХОДНОМ КОНТРОЛЕ для СПК БИСП.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.
ПОЛОЖЕНИЕ О ВХОДНОМ КОНТРОЛЕ {dates['year']}

1. ОБЩИЕ ПОЛОЖЕНИЯ
1.1 Цель входного контроля: установление соответствия поступающих материалов и изделий требованиям ТНПА и проектной документации.
1.2 Область применения: {company.get('scope','')}

2. ПОРЯДОК ПРОВЕДЕНИЯ ВХОДНОГО КОНТРОЛЯ
2.1 Ответственный за входной контроль: {_fio(resp.get('process_resp'))} ({_pos(resp.get('process_resp'))})
2.2 Проверяемые документы: паспорта, сертификаты соответствия, декларации
2.3 Методы контроля: визуальный осмотр, проверка документов, измерительный контроль
2.4 Периодичность: каждая поступающая партия

3. ДОКУМЕНТИРОВАНИЕ
3.1 Журнал входного контроля
3.2 Акты о несоответствии (при выявлении)

4. ДЕЙСТВИЯ ПРИ НЕСООТВЕТСТВИИ
4.1 Изолирование несоответствующей продукции
4.2 Уведомление поставщика
4.3 Возврат или утилизация

Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


def gen_tech_trebovaniya(work_type, company, dates, resp, api_key):
    """Технические требования к виду работ для СПК"""
    full = f"{company.get('form','ООО')} «{_clean(company)}»"
    dir_init = _initials(_fio(resp.get('director')))

    # Библиотека ТНПА для типовых видов СМР
    tnpa_library = {
        'штукатурн': ('СП 1.03.01-2019', 'СП 1.03.07-2023', 'Отделочные работы', 'Отклонение от вертикали: не более 3 мм/м; ровность: не более 4 мм; температура: не ниже +10°С'),
        'малярн': ('СП 1.03.01-2019', 'СП 1.03.07-2023', 'Отделочные работы', 'Температура: не ниже +10°С; влажность не более 60%; ровность покрытия по 2м рейке ≤ 2мм'),
        'облицовочн': ('СП 1.03.01-2019', 'СП 1.03.07-2023', 'Отделочные работы', 'Отклонение плиток: ≤ 2мм; зазоры: 2-3мм; температура: +10°С и выше'),
        'кровельн': ('СН 5.08.01-2019', 'ТКП 45-5.08-277', 'Кровли', 'Уклон: ≥1,5%; температура: по ТНПА; нахлёст полотнищ: ≥100мм'),
        'электромонтажн': ('СП 4.04.06-2024', 'ТКП 339-2022', 'Электроустановки', 'Сечение кабелей по ПУЭ; расстояние при параллельной прокладке ≥25мм; температура: не ниже -15°С'),
        'монтаж стальн': ('СН 1.03.01-2019', 'СП 1.03.10-2023', 'Стальные конструкции', 'Отклонение от вертикали: ≤1/500 высоты; зазор в стыках: по проекту'),
        'бетонн': ('СН 1.03.01-2019', 'СП 1.03.09-2023', 'Монолитные конструкции', 'Температура бетонной смеси: +5..+35°С; класс по прочности: по проекту'),
        'земляные': ('СП 5.01.01-2023', 'ГОСТ 26433.2-94', 'Земляные работы', 'Отклонение отметки дна котлована: +250/-50мм; уплотнение грунта по ТКП'),
        'каменные': ('СН 1.03.01-2019', 'СП 1.03.13-2024', 'Каменные конструкции', 'Отклонение осей: ≤10мм; горизонтальность рядов: +15мм; шов: 10-12мм'),
        'водоснабжение': ('СП 1.03.02-2020', 'СП 4.01.08-2024', 'Внутреннее водоснабжение', 'Уклон: ≥0.003; отклонение от вертикали: ≤2мм/м; давление при испытании'),
        'отопление': ('СП 1.03.02-2020', 'СП 4.02.08-2024', 'Системы отопления', 'Уклон подводок: 5-10мм; расстояние радиатора от пола: 60мм; температура теплоносителя'),
        'деревянные': ('СН 1.03.01-2019', 'СП 1.03.10-2023', 'Деревянные конструкции', 'Влажность древесины: 12±3% (внутри); отклонение от вертикали: ≤3мм/м'),
    }

    wt_lower = work_type.lower()
    tnpa = None
    for key, val in tnpa_library.items():
        if key in wt_lower:
            tnpa = val
            break

    if tnpa:
        sp_main, sp_control, category, key_params = tnpa
        tnpa_text = f"""ТНПА: {sp_main} ({category}), {sp_control} (Контроль качества работ), ГОСТ 26433.2-94

Ключевые параметры контроля:
{key_params}

Средства измерений: нивелир по ГОСТ 10528, рулетка по ГОСТ 7502, уровень строительный по ГОСТ 9416, линейка по ГОСТ 427"""
    else:
        tnpa_text = f"ТНПА: актуальные ГОСТ и СТБ для вида работ: {work_type}"

    prompt = f"""Создай ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ к продукции (виду работ) для СПК.

{full}
УТВЕРЖДАЮ {dir_p(resp)} {full} _____________ {dir_init} {dates['goals']} г.

ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ К ПРОДУКЦИИ,
РЕЖИМЫ ЕЕ ИСПЫТАНИЙ И ИЗМЕРЕНИЙ, СРЕДСТВА ИЗМЕРЕНИЙ

Вид работ: {work_type}

{tnpa_text}

Таблица (8 колонок):
Наименование продукции | ТНПА на продукцию | ТНПА на методы испытаний | Требование из ТНПА | Определяемые величины | Установленные значения | Средства контроля по ТНПА | Средства контроля фактически

Заполни таблицу конкретными параметрами контроля для {work_type} с реальными числовыми значениями из ТНПА.
Минимум 5-8 строк параметров.
Отвечай только текстом документа."""
    return vibe_call([{"role":"user","content":prompt}], api_key)


