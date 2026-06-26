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
    full_name = f"{company.get('form','ООО')} \"{company.get('name','')}\""

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
Название: {company.get('name','')}
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
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""
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

ПРАВИЛО: в тексте используй название {company.get('form','ООО')} "{company.get('name','')}" без искажений.
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
{company.get('form','ООО')} "{company.get('name','')}"

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
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""
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
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""

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
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""

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
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""
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
    header = build_header(f"РЕЕСТР РИСКОВ\nна {dates['year']} г.\n{company.get('form','ООО')} \"{company.get('name','')}\"", 
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
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""
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
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""
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
    objects  = company_data.get('objects', []) or []
    suppliers= company_data.get('suppliers', []) or []

    audit_date = dates_in.get('audit_date', '') or company_data.get('certification', {}).get('audit_date', '')
    dates = calculate_dates(audit_date)

    itr     = [s for s in staff if not s.get('is_worker', False)]
    workers = [s for s in staff if s.get('is_worker', False)]

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
    p("Политика в области качества...")
    add(f"{org} - 1 Политика в области качества.docx",
        gen_policy_iso(company, dates, resp, itr, objects, api_key))

    p("Лист ознакомления с политикой...")
    add(f"{org} - 1.2 Лист ознакомления с политикой.docx",
        gen_awareness_list("Лист ознакомления с Политикой в области качества", company, dates, resp, itr, 'policy', api_key))

    p("Цели в области качества...")
    text = vibe_call([{"role":"user","content": f"""Создай ЦЕЛИ В ОБЛАСТИ КАЧЕСТВА.
{build_ctx(company,dates,resp,itr=itr,objects=objects)}
{build_header(f"Цели в области качества {company.get('form','ООО')} \"{company.get('name','')}\" на {dates['year']} г.", company, dates, resp, 'goals')}
Таблица целей (5 целей): повышение квалификации, улучшение качества, расширение рынка, снижение претензий, внедрение СМК.
Ответственные: {_fio(resp.get('director'))}, {_fio(resp.get('process_resp'))}.
Отвечай только текстом."""}], api_key)
    add(f"{org} - 2.1 Цели в области качества.docx", text)

    p("Лист ознакомления с целями...")
    add(f"{org} - 2.2 Лист ознакомления с целями.docx",
        gen_awareness_list("Лист ознакомления с Целями в области качества", company, dates, resp, itr, 'goals', api_key))

    p("Номенклатура дел...")
    text = vibe_call([{"role":"user","content": f"""Создай НОМЕНКЛАТУРУ ДЕЛ.
{build_header(f"НОМЕНКЛАТУРА ДЕЛ № 1\nг. {company.get('city','Минск')} на {dates['year']} год", company, dates, resp, 'policy')}
Стандартная номенклатура для строительной компании: 01-руководство, 02-персонал, 03-качество, 04-производство.
Отвечай только текстом."""}], api_key)
    add(f"{org} - 3 Номенклатура дел.docx", text)

    # Приказы 1-9 — параллельно
    orders = [
        (1, "О разработке системы менеджмента качества", 'policy',
         f"Разработать СМК в области: {company.get('scope','')}. Ответственный: {_fio(resp.get('director'))}."),
        (2, "О введении в действие документов СМК и внедрении СМК", 'goals',
         f"Ввести в действие с {dates['goals']}: РК СМК 01-{dates['year']}, СТП СМК 02-{dates['year']}, СТП СМК 03-{dates['year']}, СТП СМК 04-{dates['year']}. Считать СМК внедрённой с {dates['goals']}."),
        (3, "О назначении аудиторов для проведения внутреннего аудита", 'goals',
         f"Назначить внутренними аудиторами: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}"),
        (4, "О назначении ответственного за ведение и учёт должностных инструкций", 'goals',
         f"Ответственный: {_fio(resp.get('di_resp'))}, {_pos(resp.get('di_resp'))}."),
        (5, "О проведении оценки и анализа рисков", 'policy',
         f"Экспертная группа: {', '.join(_fio(a) for a in resp.get('risk_group',[]))}. Срок проведения: до {dates['reports']}."),
        (6, "О назначении владельцев процессов", 'goals',
         f"Руководитель процесса производства: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}. Область: {company.get('scope','')}"),
        (7, "О назначении ответственного за управление фондом ТНПА и документов СМК", 'goals',
         f"За СМК: {_fio(resp.get('director'))}. За ТНПА/НПА: {_fio(resp.get('fnpa_resp'))}, {_pos(resp.get('fnpa_resp'))}."),
        (8, "О создании Координационного совета", 'goals',
         f"Председатель: {_fio(resp.get('director'))}. Члены: {', '.join(_fio(a) for a in resp.get('coord_council',[]) if a != resp.get('director'))}."),
        (9, "О проведении внутреннего обучения специалистов", 'goals',
         f"Обучить: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
    ]
    p("Приказы 1-9 (параллельно)...")
    order_tasks = [
        (f"{org} - 3.{num} Приказ {num} {name[:50]}.docx",
         gen_order, (num, name, company, dates, resp, itr, api_key, extra, date_key))
        for num, name, date_key, extra in orders
    ]
    order_results = _parallel(order_tasks, max_workers=4)
    for fname, text in order_results.items():
        add(fname, text)

    p("Протокол КС...")
    text = vibe_call([{"role":"user","content": f"""Создай ПРОТОКОЛ заседания Координационного совета № 1.
{build_ctx(company,dates,resp,itr=itr,objects=objects)}
Дата: {dates['reports']}. Присутствуют: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('coord_council',[]))}.
Решения: СМК признана удовлетворительной. Рекомендации по улучшению.
Отвечай только текстом."""}], api_key)
    add(f"{org} - 3.8.2 Протокол КС.docx", text)

    p("Программа внутреннего обучения...")
    text = vibe_call([{"role":"user","content": f"""Создай ПРОГРАММУ ВНУТРЕННЕГО ОБУЧЕНИЯ по теме СМК/внутренний аудит.
{build_header('Программа семинара «Документальное оформление и порядок разработки СМК. Внутренний аудит»', company, dates, resp, 'goals')}
Продолжительность: 1 день (8 часов). Темы: законодательство РБ, ISO серии 9000, внутренний аудит, документация СМК.
Список обучаемых: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}.
Отвечай только текстом."""}], api_key)
    add(f"{org} - 3.9.3 Программа внутреннего обучения.docx", text)

    p("Программа аудитов...")
    add(f"{org} - 4.1 Программа внутренних аудитов.docx",
        gen_audit_program(company, dates, resp, itr, api_key))

    p("Реестр рисков...")
    add(f"{org} - 10 Реестр рисков.docx",
        gen_risk_register(company, dates, resp, itr, api_key))

    p("Отчёт по процессу строительства...")
    add(f"{org} - Отчёт по процессу строительства.docx",
        gen_report("Отчёт по качеству владельца процесса строительно-монтажных работ", company, dates, resp, objects, api_key))

    p("Отчёт по оценке удовлетворённости...")
    add(f"{org} - Отчёт по оценке удовлетворённости заказчиков.docx",
        gen_satisfaction_report(company, dates, resp, objects, api_key))

    p("Сводный отчёт по СМК...")
    add(f"{org} - Сводный отчёт по СМК.docx",
        gen_report("Отчёт по анализу функционирования СМК", company, dates, resp, objects, api_key))

    # ДИ для каждого ИТР — параллельно
    p(f"Должностные инструкции ({len(itr)} чел., параллельно)...")
    di_tasks = []
    for person in itr:
        pos = person.get('position','')
        fio = person.get('fio','')
        safe = re.sub(r'[^\w\s-]','',pos)[:40]
        di_tasks.append((f"{org} - ДИ {safe}.docx", gen_di, (pos, fio, company, dates, resp, api_key)))
    di_results = _parallel(di_tasks, max_workers=4)
    for fname, text in di_results.items():
        add(fname, text)

    # Карточки поставщиков — параллельно
    p(f"Карточки поставщиков ({len(suppliers[:6])} шт., параллельно)...")
    sup_tasks = []
    for i, sup in enumerate(suppliers[:6], 1):
        safe = re.sub(r'[^\w\s-]','',sup.get('name',f'поставщик_{i}'))[:30]
        sup_tasks.append((f"{org} - Карточка поставщика {i} {safe}.docx",
                          gen_supplier_card, (sup, company, dates, resp, api_key)))
    sup_results = _parallel(sup_tasks, max_workers=4)
    for fname, text in sup_results.items():
        add(fname, text)


def _gen_suot(org, company, dates, resp, itr, workers, professions, api_key, add, p):
    dir_fio  = _fio(resp.get('director'))
    dir_init = _initials(dir_fio)
    full = f"{company.get('form','ООО')} \"{company.get('name','')}\""

    p("Политика в области охраны труда...")
    text = vibe_call([{"role":"user","content": f"""Создай ПОЛИТИКУ В ОБЛАСТИ ОХРАНЫ ТРУДА ISO 45001.
{build_ctx(company,dates,resp,itr=itr,workers=workers)}
{build_header('ПОЛИТИКА В ОБЛАСТИ ОХРАНЫ ТРУДА', company, dates, resp, 'goals')}
Область: {company.get('scope','')}. Обязательства по улучшению условий труда, устранению рисков, вовлечению работников.
Отвечай только текстом."""}], api_key)
    add(f"{org} СУОТ - Политика ОТ.docx", text)

    # Приказы СУОТ 1-10
    suot_orders = [
        (1, "О разработке системы управления охраной труда", 'policy', f"Разработать СУОТ в области: {company.get('scope','')}. Срок установить цели: до {dates['goals']}."),
        (2, "О внедрении системы управления охраной труда", 'goals', f"Считать СУОТ внедрённой с {dates['goals']}."),
        (3, "О введении в действие документов системы управления охраной труда", 'goals', f"Ввести в действие Политику, Руководство, Процедуры. Область: {company.get('scope','')}."),
        (4, "О назначении ответственных лиц в области охраны труда", 'goals', f"Ответственный за инструктажи: {_fio(resp.get('process_resp'))}. За электрохозяйство, пожарную безопасность: {_fio(resp.get('fnpa_resp'))}."),
        (5, "О назначении внутренних аудиторов OH&S", 'goals', f"Аудиторы: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
        (6, "О назначении ответственных за идентификацию рисков OH&S", 'goals', f"Экспертная группа по рискам: {', '.join(_fio(a) for a in resp.get('risk_group',[]))}. Срок: до {dates['reports']}."),
        (7, "О разработке инструкций по охране труда", 'goals', f"Ответственный за инструкции: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}."),
        (8, "Об обучении пожарно-техническому минимуму", 'goals', f"Обучить: {', '.join(_fio(a) for a in resp.get('auditors',[]))}."),
        (9, "О дне охраны труда", 'goals', f"Комиссия: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
        (10, "О проведении внутреннего обучения по СУОТ", 'goals', f"Обучить: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
    ]
    for num, name, dk, extra in suot_orders:
        p(f"СУОТ Приказ {num}: {name[:40]}...")
        add(f"{org} СУОТ - Приказ {num} {name[:50]}.docx",
            gen_order(num, name, company, dates, resp, itr, api_key, extra, dk))

    p("Перечни СУОТ 1-12...")
    itr_positions = list({s.get('position','') for s in itr if s.get('position')})
    worker_positions = list({s.get('position','') for s in workers if s.get('position')}) if workers else []
    text = vibe_call([{"role":"user","content": f"""Создай ПЕРЕЧНИ 1-12 для СУОТ ISO 45001.
{build_ctx(company,dates,resp,itr=itr,workers=workers)}
Дата: {dates['goals']}

Перечень 1 — АС, Перечень 7: только дата и шапка.
Перечень 2 — ИТР: {', '.join(itr_positions)}
Перечень 3 — рабочие: {', '.join(worker_positions) if worker_positions else 'по штатному расписанию'}
Перечень 4 — производственный ИТР (директор, ГИ, прораб, мастер)
Перечень 5,9 — взаимосвязанные
Перечень 6 — все ИТР и рабочие
Перечень 8 — персонал с риском поражения током
Перечень 10 — обычно "Отсутствует"
Перечень 11 — рабочие со сноской
Перечень 12 — СИЗ по нормам для рабочих

Отвечай только текстом (все 12 перечней подряд)."""}], api_key)
    add(f"{org} СУОТ - Перечни 1-12.docx", text)

    p("Листы ознакомления с политикой и целями СУОТ...")
    add(f"{org} СУОТ - Лист ознакомления с политикой ОТ.docx",
        gen_awareness_list("Лист ознакомления с Политикой в области охраны труда", company, dates, resp, itr, 'goals', api_key))

    # Инструкции 1-9 (общие)
    general_instructions = [
        "1 Инструкция о проведении контроля за соблюдением законодательства по охране труда",
        "2 Общеобъектовая инструкция о мерах пожарной безопасности",
        "3 Инструкция по охране труда при работе с персональным компьютером",
        "4 Инструкция по оказанию первой медицинской помощи",
        "5 Инструкция по охране труда при работе с ручным инструментом",
        "6 Инструкция по охране труда при работе на высоте",
        "7 Инструкция по охране труда при выполнении погрузочно-разгрузочных работ",
        "8 Инструкция по охране труда при эксплуатации электроустановок",
        "9 Инструкция по охране труда при выполнении сварочных работ",
    ]
    for instr_name in general_instructions:
        p(f"{instr_name[:50]}...")
        text = vibe_call([{"role":"user","content": f"""Создай: {instr_name}
{build_header(instr_name.upper(), company, dates, resp, 'goals')}
Компания: {full}, область: {company.get('scope','')}.
Стандартная структура: Глава 1 Общие положения, Глава 2 До начала работы, Глава 3 При выполнении, Глава 4 По окончании, Глава 5 Аварийные ситуации.
Отвечай только текстом."""}], api_key)
        safe = re.sub(r'[^\w\s-]','',instr_name)[:60]
        add(f"{org} СУОТ - {safe}.docx", text)

    # Инструкции ОТ по профессиям — параллельно
    p(f"Инструкции ОТ ({len(professions)} профессий, параллельно)...")
    ot_tasks = []
    for prof in professions:
        safe = re.sub(r'[^\w\s-]','',prof)[:40]
        ot_tasks.append((f"{org} СУОТ - Инструкция ОТ {safe}.docx",
                         gen_ot_instruction, (prof, company, dates, resp, api_key)))
    ot_results = _parallel(ot_tasks, max_workers=4)
    for fname, text in ot_results.items():
        add(fname, text)

    # Карты рисков
    p("Карта рисков ИТР (офис)...")
    office_itr = [p2 for p2 in itr if any(k in p2.get('position','').lower() for k in ['директор','бухгалтер','кадр','юрис'])]
    add(f"{org} СУОТ - Карта рисков ИТР офис.docx",
        gen_risk_card('office', [p2.get('position','') for p2 in office_itr] or ['Директор','Бухгалтер'], company, dates, resp, api_key))

    p("Карта рисков ИТР (производство)...")
    prod_itr = [p2 for p2 in itr if any(k in p2.get('position','').lower() for k in ['инженер','прораб','мастер','производитель'])]
    add(f"{org} СУОТ - Карта рисков ИТР производство.docx",
        gen_risk_card('production', [p2.get('position','') for p2 in prod_itr] or ['Главный инженер','Производитель работ'], company, dates, resp, api_key))

    # Карты рисков для рабочих — параллельно
    p(f"Карты рисков рабочих ({len(professions)} шт., параллельно)...")
    risk_tasks = []
    for prof in professions:
        safe = re.sub(r'[^\w\s-]','',prof)[:40]
        risk_tasks.append((f"{org} СУОТ - Карта рисков {safe}.docx",
                           gen_risk_card, ('worker', [prof], company, dates, resp, api_key)))
    risk_results = _parallel(risk_tasks, max_workers=4)
    for fname, text in risk_results.items():
        add(fname, text)

    p("Реестр неприемлемых рисков...")
    text = vibe_call([{"role":"user","content": f"""Создай РЕЕСТР НЕПРИЕМЛЕМЫХ РИСКОВ (уровень > 9).
{build_header('РЕЕСТР НЕПРИЕМЛЕМЫХ РИСКОВ', company, dates, resp, 'goals')}
Для строительной компании: {company.get('scope','')}
Профессии рабочих: {', '.join(professions)}
Неприемлемые риски (OR > 9): падение с высоты (3×4=12), обрушение конструкций (3×4=12), поражение током (2×5=10).
Программа управления ОТ: мероприятия по устранению.
Подписи: {', '.join(_fio(a) for a in resp.get('auditors',[]))}
Отвечай только текстом."""}], api_key)
    add(f"{org} СУОТ - Реестр неприемлемых рисков.docx", text)

    p("Отчёты СУОТ...")
    text = vibe_call([{"role":"user","content": f"""Создай ОТЧЁТ ПО СУОТ ISO 45001.
{build_ctx(company,dates,resp,itr=itr,workers=workers)}
{build_header('ОТЧЁТ по функционированию системы управления охраной труда', company, dates, resp, 'reports')}
Отчётный период: {dates['audit_obj']}
Аудиторов: {len(resp.get('auditors',[]))} чел. Инструктажи проведены. Несчастных случаев нет. Нарушений нет.
Приложение 2: особо опасные условия. Приложение 3: перечень разрешительных документов.
Отвечай только текстом."""}], api_key)
    add(f"{org} СУОТ - Отчёт по СУОТ.docx", text)

    p("Протокол проверки знаний...")
    all_staff_rows = '\n'.join(f"{i+1}. {p2.get('fio','')} — {p2.get('position','')} — ПЕРИОДИЧЕСКАЯ" for i,p2 in enumerate(itr+workers))
    text = vibe_call([{"role":"user","content": f"""Создай ПРОТОКОЛ ПРОВЕРКИ ЗНАНИЙ по охране труда.
{build_header('ПРОТОКОЛ № 1 проверки знаний по охране труда', company, dates, resp, 'goals')}
Все ИТР и рабочие — ПЕРИОДИЧЕСКАЯ проверка знаний:
{all_staff_rows}
Комиссия: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}.
Председатель: {_fio(resp.get('director'))}. Результат: удовлетворительно.
Отвечай только текстом."""}], api_key)
    add(f"{org} СУОТ - Протокол проверки знаний.docx", text)


def _gen_spk(org, company, dates, resp, itr, api_key, add, p, variant='spk_stroy'):
    ctx = build_ctx(company, dates, resp, itr=itr)
    bisp = variant == 'spk_bisp'

    p("Положение о системе производственного контроля...")
    text = vibe_call([{"role":"user","content": f"""Создай ПОЛОЖЕНИЕ О СИСТЕМЕ ПРОИЗВОДСТВЕННОГО КОНТРОЛЯ.
{ctx}
{build_header('ПОЛОЖЕНИЕ О СИСТЕМЕ ПРОИЗВОДСТВЕННОГО КОНТРОЛЯ', company, dates, resp, 'goals')}
Область: {company.get('scope','')}. Функции, права, обязанности специалистов.
Разработано согласно Инструкции о порядке освидетельствования.
Отвечай только текстом."""}], api_key)
    add(f"{org} СПК - Положение о СПК.docx", text)

    for num, name, extra in [
        (1, "О внесении изменений в систему производственного контроля",
         f"Создать СПК. Ответственные: {_fio(resp.get('director'))}, {_fio(resp.get('process_resp'))}."),
        (2, "О проведении внутреннего обучения специалистов",
         f"Обучить: {', '.join(_fio(a)+' ('+_pos(a)+')' for a in resp.get('auditors',[]))}."),
        (3, "О технических осмотрах средств измерений",
         f"Ответственный: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}."),
        (4, "О назначении ответственного за средства малой механизации",
         f"Ответственный: {_fio(resp.get('process_resp'))}, {_pos(resp.get('process_resp'))}."),
    ]:
        p(f"СПК Приказ {num}...")
        add(f"{org} СПК - Приказ {num} {name[:50]}.docx",
            gen_order(num, name, company, dates, resp, itr, api_key, extra, 'goals'))

    p("Перечень специалистов (ИТР)...")
    itr_rows = '\n'.join(f"{i+1}. {p2.get('fio','')} — {p2.get('position','')}" for i,p2 in enumerate(itr))
    text = vibe_call([{"role":"user","content": f"""Создай ПЕРЕЧЕНЬ СПЕЦИАЛИСТОВ для СПК.
{build_header('Справка специалистов, осуществляющих контроль', company, dates, resp, 'goals')}
Таблица: ФИО | Должность | Образование | Виды контроля | Аттестат/диплом
{itr_rows}
Отвечай только текстом."""}], api_key)
    add(f"{org} СПК - Перечень специалистов.docx", text)

    p("Организационная структура СПК...")
    text = vibe_call([{"role":"user","content": f"""Создай ОРГАНИЗАЦИОННУЮ СТРУКТУРУ системы производственного контроля.
{ctx}
{build_header('Организационная структура системы производственного контроля', company, dates, resp, 'goals')}
Иерархия: Директор → Главный инженер → Производитель работ/Мастер.
Функции каждого в СПК.
Отвечай только текстом."""}], api_key)
    add(f"{org} СПК - Организационная структура.docx", text)

    for jname in ['Журнал входного контроля', 'Журнал операционного контроля', 'Журнал приёмо-сдаточного контроля']:
        p(f"{jname}...")
        text = vibe_call([{"role":"user","content": f"""Создай {jname} (шапка + пустая таблица).
{company.get('form','ООО')} "{company.get('name','')}"
{jname.upper()}
Начат: ___________ Окончен: ___________
Таблица с колонками для заполнения.
Отвечай только текстом."""}], api_key)
        add(f"{org} СПК - {jname}.docx", text)

    if bisp:
        bisp_docs = [
            ("Справка об отсутствии рекламаций", f"В адрес {company.get('form','ООО')} \"{company.get('name','')}\" не поступали письменные претензии к качеству."),
            ("Гарантийное письмо по лаборатории", f"{company.get('form','ООО')} \"{company.get('name','')}\" гарантирует заключение договоров с аккредитованными лабораториями."),
            ("Гарантийное письмо по ТТК", f"{company.get('form','ООО')} \"{company.get('name','')}\" гарантирует приобретение технологических карт."),
            ("Гарантийное письмо по входному контролю (ВВК)", f"Гарантирует проведение входного и выходного контроля продукции."),
            ("Справка о предприятии", f"Полные реквизиты {company.get('form','ООО')} \"{company.get('name','')}\": адрес, УНП {company.get('unp','')}, директор {_fio(resp.get('director'))}."),
        ]
        for doc_name, content in bisp_docs:
            p(f"БИСП: {doc_name}...")
            text = vibe_call([{"role":"user","content": f"""Создай: {doc_name}
{build_ctx(company,dates,resp,itr=itr)}
Исх. № от {dates['goals']} г.
РУП «СтройМедиаПроект»
ГАРАНТИЙНОЕ ПИСЬМО / СПРАВКА

{content}

Директор _____________ {_initials(_fio(resp.get('director')))}
М.П.
Отвечай только текстом."""}], api_key)
            safe = re.sub(r'[^\w\s-]','',doc_name)[:50]
            add(f"{org} СПК БИСП - {safe}.docx", text)
