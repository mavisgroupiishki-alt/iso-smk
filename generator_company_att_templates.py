"""
Генератор документов на аттестацию компании — НАСТОЯЩИЕ шаблоны.

В отличие от generator_company_att.py (который СТРОИТ документ заново по описанию
структуры), этот модуль берёт ваши реальные docx-файлы буквально как есть — со всеми
стилями, темой, нумерацией, шрифтами — и меняет ТОЛЬКО текст внутри конкретных
абзацев. Форматирование гарантированно 1-в-1, потому что это тот же самый файл,
просто с другими словами внутри.

Требования к папке att_templates/ рядом с этим файлом:
  1__Заявление.docx, 2__ИТР.docx, 3__Трудовые.docx, 4__Дипломы.docx, 5__Аттестаты.docx
"""
import re, io, zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
TPL_DIR = BASE_DIR / 'att_templates'


# ═══════════════════ Низкоуровневая работа с docx как с архивом ═══════════════════
def _load_parts(filename: str) -> dict:
    path = TPL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Не найден шаблон {path}. Папка att_templates/ с реальными docx-файлами "
            f"должна лежать рядом с generator_company_att_templates.py в репозитории."
        )
    parts = {}
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            parts[name] = z.read(name)
    return parts


def _rebuild(parts: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for name, data in parts.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _paragraphs(xml: str) -> list:
    """Разбивает document.xml на список XML-блоков абзацев <w:p ...>...</w:p>.
    Отдельно матчит самозакрывающиеся пустые абзацы <w:p .../> (без отдельного
    </w:p>) — иначе они склеиваются со следующим реальным абзацем в один "абзац",
    что ломает точечную замену текста."""
    return re.findall(r'<w:p\b[^>]*?/>|<w:p\b[^>]*>.*?</w:p>', xml, re.DOTALL)


def _para_text(para_xml: str) -> str:
    return re.sub(r'<[^>]+>', '', para_xml).strip().replace('\xa0', ' ')


def _esc(s) -> str:
    return (str(s) if s not in (None, '') else '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _replace_para_text(para_xml: str, new_text: str) -> str:
    """Заменяет ВЕСЬ видимый текст абзаца на новый, сохраняя форматирование ПЕРВОГО
    текстового run (шрифт/размер/жирность) — берём его rPr как эталон стиля.
    Если внутри было несколько run'ов (Word раздробил текст) — схлопываем в один,
    что визуально неотличимо, но надёжнее для замены данных."""
    m = re.search(r'(<w:r\b[^P].*?)<w:t[^>]*>.*?</w:t>(.*?</w:r>)', para_xml, re.DOTALL)
    if not m:
        # абзац без текста (пустая строка) — просто вернуть как есть
        return para_xml
    run_prefix = m.group(1)  # <w:r ...><w:rPr>...</w:rPr>
    run_suffix = m.group(2)  # </w:r> (обычно пусто перед этим)
    new_run = f'{run_prefix}<w:t xml:space="preserve">{_esc(new_text)}</w:t>{run_suffix}'
    # Абзац = всё до начала САМОГО ПЕРВОГО run'а (не rPr внутри pPr!) + новый run + </w:p>
    # </w:pPr> — надёжная граница конца свойств абзаца, если pPr вообще есть.
    if '</w:pPr>' in para_xml:
        p_open_end = para_xml.find('</w:pPr>') + len('</w:pPr>')
    else:
        p_open_end = para_xml.find('>') + 1  # сразу после <w:p ...>
    return para_xml[:p_open_end] + new_run + '</w:p>'


def _clone_para_style(template_para_xml: str, new_text: str) -> str:
    """Строит НОВЫЙ абзац с той же структурой pPr/rPr, что и образец, но с другим
    текстом — используется для генерации переменного числа строк (виды работ,
    строки таблиц) на основе одной реальной строки-образца."""
    return _replace_para_text(template_para_xml, new_text)


def _find_para_index(paras: list, predicate) -> int:
    for i, p in enumerate(paras):
        if predicate(_para_text(p)):
            return i
    return -1


# ═══════════════════ Документ 1: Заявление ═══════════════════
def render_zayavlenie(company: dict, work_item_lines: list) -> bytes:
    """
    company: {name, form, address, bank_account, bank_name, bik, unp, phone, email,
              director_position, director_fio}
    work_item_lines: список готовых строк текста для пункта 7 (и опционально 6),
                      каждая строка = один абзац (см. render_work_items_lines в
                      generator_company_att.py — используем ту же функцию).
    """
    parts = _load_parts('1__Заявление.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)

    full_name = f'{company.get("form","ООО")} " {company.get("name","")} "'
    dir_init = _dir_initials(company.get('director_fio', ''))

    # --- Скалярные абзацы по индексам, установленным на реальном образце ---
    replacements = {
        0: full_name,
        1: company.get('address', ''),
        2: f"р/с: {company.get('bank_account','')}",
        3: f"в {company.get('bank_name','')}, БИК {company.get('bik','')}",
        4: f"УНП {company.get('unp','')}",
        5: f"Тел.: {company.get('phone','')}",
        15: full_name,
        16: company.get('address', ''),
        17: f"УНП {company.get('unp','')}",
        18: f"Тел.: {company.get('phone','')}e-mail: {company.get('email','')}",
        24: f"Прошу провести аттестацию {_genitive_form(company)} на право осуществления:",
        84: f'{company.get("form","ООО")} " {company.get("name","")} "   _____________       {dir_init}',
    }

    # email — отдельно (внутри HYPERLINK-поля, аккуратно не трогаем структуру поля)
    email_idx = _find_para_index(paras, lambda t: 'HYPERLINK' in t and 'mailto' in t)

    # Абзац 6 в оригинале имеет вид "e-mail:  HYPERLINK ... email@..." — если email
    # не найден по образцу, просто пропускаем (оставляем как в шаблоне)

    # --- Абзац "Директор" (83) в оригинале ОТДЕЛЬНО от подписи — оставляем как есть ---

    # --- Блок видов работ: абзацы с 25 (заголовок "7. Выполнение...") до 49 (конец
    #     перечня, последний перед "Соответствующей квалификационным") ---
    idx_head = _find_para_index(paras, lambda t: t.startswith('7. Выполнение'))
    idx_end = _find_para_index(paras, lambda t: t.startswith('Соответствующей квалификационным') or
                                                  t.startswith('соответствующей квалификационным'))
    if idx_head == -1 or idx_end == -1:
        raise RuntimeError("Не удалось найти блок видов работ в шаблоне — структура образца изменилась?")

    # В оригинале заголовок "7. Выполнение..." продублирован дважды (опечатка в
    # исходнике) — берём его один раз, не повторяем чужую ошибку.
    style_template = paras[idx_head + 2] if idx_head + 2 < idx_end else paras[idx_head]
    work_paras = [paras[idx_head]]  # один раз "7. Выполнение строительно-монтажных работ:"
    for line in work_item_lines:
        work_paras.append(_clone_para_style(style_template, line))

    # ЛОКАЛЬНАЯ замена: старый блок (paras[idx_head] .. paras[idx_end-1] включительно)
    # заменяем на новый — ищем точную позицию ТОЛЬКО этого диапазона в xml, не трогая
    # остальной документ (там же рядом таблица «Приложение» — её нельзя задевать).
    old_block = ''.join(paras[idx_head:idx_end])
    new_block = ''.join(work_paras)
    if old_block not in xml:
        raise RuntimeError("Блок видов работ не найден как цельная подстрока — структура образца изменилась?")
    xml = xml.replace(old_block, new_block, 1)

    # --- Скалярные замены — каждая СВОЯ точечная замена одного конкретного абзаца ---
    for i, new_text in replacements.items():
        if i >= len(paras):
            continue
        old_para = paras[i]
        if old_para not in xml:
            continue  # уже заменено как часть другого блока (например пересекается с work-items) — пропускаем
        new_para = _replace_para_text(old_para, new_text)
        xml = xml.replace(old_para, new_para, 1)

    if email_idx is not None and email_idx >= 0 and company.get('email'):
        old_para = paras[email_idx]
        if old_para in xml:
            xml = xml.replace(old_para, _replace_email_para(old_para, company['email']), 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)



def _replace_email_para(para_xml: str, new_email: str) -> str:
    """email хранится как поле HYPERLINK — меняем И текст поля, И адрес в mailto:."""
    para_xml = re.sub(r'mailto:[^"]+', f'mailto:{new_email}', para_xml)
    # Видимый текст email (последний <w:t> перед </w:hyperlink> или в конце run'ов поля)
    para_xml = re.sub(r'(<w:t[^>]*>)[^<]*(@[^<]*)(</w:t>)',
                       lambda m: m.group(1) + new_email + m.group(3), para_xml, count=1)
    return para_xml


def _dir_initials(fio: str) -> str:
    parts = (fio or '').strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio or ''


def _genitive_form(company: dict) -> str:
    """Родительный падеж формы собственности + название без склонения."""
    GEN = {
        'ООО': 'Общества с ограниченной ответственностью',
        'ОДО': 'Общества с дополнительной ответственностью',
        'ЗАО': 'Закрытого акционерного общества',
        'ОАО': 'Открытого акционерного общества',
        'ЧУП': 'Частного унитарного предприятия',
        'ЧТУП': 'Частного торгового унитарного предприятия',
    }
    form = (company.get('form') or 'ООО').upper()
    gen_form = GEN.get(form, GEN['ООО'])
    return f'{gen_form} "{company.get("name","")}"'


# ═══════════════════ Работа с таблицами (клонирование строк) ═══════════════════
def _rows(xml_or_block: str) -> list:
    return re.findall(r'<w:tr\b.*?</w:tr>', xml_or_block, re.DOTALL)


def _cells(row_xml: str) -> list:
    return re.findall(r'<w:tc\b.*?</w:tc>', row_xml, re.DOTALL)


def _replace_cell_content(cell_xml: str, lines: list) -> str:
    """Заменяет содержимое ячейки на новый список строк (каждая строка — свой
    абзац), используя стиль ПЕРВОГО абзаца ячейки как образец для всех новых."""
    paras_in_cell = re.findall(r'<w:p\b.*?</w:p>', cell_xml, re.DOTALL)
    if not paras_in_cell:
        return cell_xml
    style_para = paras_in_cell[0]
    tc_pr_match = re.match(r'(<w:tc\b.*?</w:tcPr>)', cell_xml, re.DOTALL)
    tc_pr = tc_pr_match.group(1) if tc_pr_match else cell_xml[:cell_xml.find('<w:p')]
    lines = [l for l in lines if l] or ['—']
    new_paras = ''.join(_replace_para_text(style_para, line) for line in lines)
    return f'{tc_pr}{new_paras}</w:tc>'


def _build_row(template_row_xml: str, cell_values: list) -> str:
    """cell_values: список — каждый элемент либо строка (одна строка в ячейке),
    либо список строк (несколько абзацев в ячейке)."""
    cells = _cells(template_row_xml)
    new_cells = []
    for i, cell in enumerate(cells):
        val = cell_values[i] if i < len(cell_values) else ''
        lines = val if isinstance(val, list) else [val]
        new_cells.append(_replace_cell_content(cell, lines))
    tr_open_end = template_row_xml.find('>', template_row_xml.find('<w:tr')) + 1
    # Сохраняем свойства строки (<w:trPr>), если есть, между <w:tr...> и первой ячейкой
    tr_pr_match = re.search(r'<w:tr\b[^>]*>(<w:trPr>.*?</w:trPr>)?', template_row_xml, re.DOTALL)
    tr_open = template_row_xml[:tr_open_end] + (tr_pr_match.group(1) or '' if tr_pr_match else '')
    return tr_open + ''.join(new_cells) + '</w:tr>'


def _splice_rows(xml: str, old_rows_slice: list, new_rows: list) -> str:
    first, last = old_rows_slice[0], old_rows_slice[-1]
    start = xml.find(first)
    end = xml.find(last) + len(last)
    return xml[:start] + ''.join(new_rows) + xml[end:]


# ═══════════════════ Документ 2: Форма №2 — ИТР + рабочие ═══════════════════
def render_forma2(company: dict, itr_list: list, workers: list, work_scope_text: str, staff_total=None) -> bytes:
    parts = _load_parts('2__ИТР.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    rows = _rows(xml)

    full_name = f'{company.get("form","ООО")} " {company.get("name","")} "'
    n_itr = len(itr_list)
    total = staff_total or (n_itr + sum(w.get('count', 0) or 0 for w in workers))

    # Заголовочные абзацы — по тексту-маркеру, надёжнее чем по индексу (структура
    # таблицы может сдвинуть нумерацию абзацев, а не строк)
    idx_staff = _find_para_index(paras, lambda t: t.startswith('Общая численность'))
    idx_itr_count = _find_para_index(paras, lambda t: t.startswith('численность инженерно'))
    if idx_staff >= 0:
        old_p = paras[idx_staff]
        new_p = _replace_para_text(old_p,
            f"Общая численность работающих {total} чел., в том числе по заявляемому виду "
            f"деятельности {total} чел. по состоянию на ___.___.____")
        xml = xml.replace(old_p, new_p, 1)
    if idx_itr_count >= 0:
        old_p = paras[idx_itr_count]
        new_p = _replace_para_text(old_p,
            f"численность инженерно-технических работников по заявляемому виду деятельности {n_itr} чел.")
        xml = xml.replace(old_p, new_p, 1)

    # --- Таблица ИТР: строки 2..(2+кол-во людей в образце-1) — клонируем под n_itr ---
    itr_template_row = rows[2]
    itr_rows_new = []
    for i, p in enumerate(itr_list, 1):
        edu = (f"{p.get('education_level','')} Диплом {p.get('diploma_number') or '—'} "
               f"выдан {p.get('diploma_date') or '—'} {p.get('diploma_institution','')} "
               f"{p.get('diploma_speciality','')} {p.get('diploma_qualification','')}")
        trud = (f"Трудовая книжка {p.get('trudovaya_number') or '—'}",
                f"Приказ №{p.get('order_number') or '—'} от {p.get('hire_date') or '—'}")
        attestat = p.get('attestat_number') or '—'
        if p.get('attestat_date'):
            attestat += f" от {p.get('attestat_date')}"
        if p.get('attestat_specialization'):
            attestat += f" {p.get('attestat_specialization')}"
        cell_values = [
            str(i), p.get('position', ''), p.get('fio', ''), edu,
            [str(p.get('stage_years') or '—'), str(p.get('stage_years_here') or '—')],
            list(trud), attestat,
        ]
        itr_rows_new.append(_build_row(itr_template_row, cell_values))

    # оригинал использовал 3 строки-образца (индексы 2,3,4) — заменяем их все на новый набор
    old_itr_rows = rows[2:5]
    xml = _splice_rows(xml, old_itr_rows, itr_rows_new)

    # --- Таблица рабочих: перестраиваем полностью (строки 8..10 в образце, потом Итого) ---
    rows2 = _rows(xml)  # пересчитали после первой замены — индексы таблицы рабочих не сдвигаются, но безопаснее перечитать
    # находим таблицу рабочих заново по маркеру "Наименование профессий рабочих"
    w_header_idx = next((i for i, r in enumerate(rows2) if 'Наименование профессий рабочих' in r), None)
    if w_header_idx is not None and workers:
        worker_template_row = rows2[w_header_idx + 3]  # header, разряды, номера, потом первая строка данных
        old_total_idx = next((i for i in range(w_header_idx, len(rows2)) if 'Итого по разрядам' in rows2[i]), None)
        old_worker_rows = rows2[w_header_idx + 3: old_total_idx + 1] if old_total_idx else rows2[w_header_idx+3:w_header_idx+4]

        from generator_company_att import RAZRYAD_COLUMNS
        worker_rows_new = []
        totals = {r: 0 for r in RAZRYAD_COLUMNS}
        for i, w in enumerate(workers, 1):
            razr = str(w.get('razryad', '')).upper().strip()
            count = int(w.get('count') or 0)
            cell_values = [str(i), w.get('profession', '')]
            for r in RAZRYAD_COLUMNS:
                if r == razr:
                    cell_values.append(str(count) if count else '')
                    totals[r] += count
                else:
                    cell_values.append('')
            cell_values.append(str(count) if count else '')
            worker_rows_new.append(_build_row(worker_template_row, cell_values))
        total_row_template = rows2[old_total_idx] if old_total_idx else None
        if total_row_template:
            total_cells = [''] + ['Итого по разрядам:'] + [str(totals[r]) if totals[r] else '' for r in RAZRYAD_COLUMNS] + [str(sum(totals.values()))]
            worker_rows_new.append(_build_row(total_row_template, total_cells))
        xml = _splice_rows(xml, old_worker_rows, worker_rows_new)
    elif w_header_idx is not None and not workers:
        # рабочих нет вовсе — оставляем таблицу с прочерками в одной строке, не выдумываем состав
        pass

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)




# ═══════════════════ Документ 3: Форма №3 — Трудовые ═══════════════════
def render_forma3(company: dict, itr_list: list) -> bytes:
    parts = _load_parts('3__Трудовые.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    rows = _rows(xml)
    template_row = rows[2]
    new_rows = []
    for i, p in enumerate(itr_list, 1):
        cell_values = [str(i), p.get('fio', ''), p.get('position', ''), p.get('trudovaya_number') or '—']
        new_rows.append(_build_row(template_row, cell_values))
    xml = _splice_rows(xml, rows[2:], new_rows)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 4: Форма №4 — Дипломы ═══════════════════
def render_forma4(company: dict, itr_list: list) -> bytes:
    parts = _load_parts('4__Дипломы.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    rows = _rows(xml)
    template_row = rows[2]
    people_with_diploma = [p for p in itr_list if p.get('diploma_number')] or itr_list
    new_rows = []
    for i, p in enumerate(people_with_diploma, 1):
        cell_values = [str(i), p.get('fio', ''), p.get('diploma_number') or '—']
        new_rows.append(_build_row(template_row, cell_values))
    xml = _splice_rows(xml, rows[2:], new_rows)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 5: Форма №5 — Аттестаты ═══════════════════
def render_forma5(company: dict, itr_list: list) -> bytes:
    parts = _load_parts('5__Аттестаты.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    rows = _rows(xml)
    template_row = rows[2]
    people_with_attestat = [p for p in itr_list if p.get('attestat_number')]
    new_rows = []
    for i, p in enumerate(people_with_attestat, 1):
        att = p.get('attestat_number', '')
        period = ''
        if p.get('attestat_date_from'):
            period = f"с {p['attestat_date_from']} г."
            if p.get('attestat_date_to'):
                period += f" по {p['attestat_date_to']} г."
        cell_values = [str(i), p.get('fio', ''), p.get('position', ''),
                       [att, period, p.get('attestat_specialization', '')]]
        new_rows.append(_build_row(template_row, cell_values))
    if not new_rows:
        new_rows = [_build_row(template_row, ['1', '—', '—', 'нет аттестатов среди ИТР'])]
    xml = _splice_rows(xml, rows[2:], new_rows)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Адаптер для реального пайплайна (generator.py) ═══════════════════
# Старый generator_company_att.py (реконструкция без вашего файла как основы) держит
# всю логику предупреждений/расчёта стажа/видов работ — она НЕ зависит от способа
# рендера, поэтому переиспользуем её, а не дублируем. Меняем только САМ рендер
# документов на настоящие шаблоны (render_zayavlenie и т.д. из этого файла).
def generate_company_attestation_package_v2(company: dict, attestation_data: dict,
                                             api_key=None, vibe_call_fn=None, progress_cb=None) -> dict:
    from generator_company_att import (
        find_work_items, render_work_items_lines, calculate_stazh,
        check_category_requirements, _flat_work_items, RAZRYAD_COLUMNS,
        gen_zayavlenie_otmena, gen_form6_opyt,  # ещё не переведены на настоящий шаблон — честно используем старую версию для этих двух редких случаев
    )

    docs = []
    step = [0]
    category = attestation_data.get('category')
    if isinstance(category, str) and category.strip().lower() in ('', 'null', 'none', 'нет'):
        category = None
    total_steps = 1 if attestation_data.get('is_cancellation') else (6 if category else 5)

    def p(msg):
        step[0] += 1
        if progress_cb:
            progress_cb(step[0], total_steps, msg)
        print(f"  [company_att_v2 {step[0]}] {msg}")

    org = company.get('name', 'company')
    itr_list = attestation_data.get('itr', [])
    workers = attestation_data.get('workers', [])
    staff_total = attestation_data.get('staff_total', len(itr_list))
    has_smetchik = attestation_data.get('has_smetchik', False)
    experience_objects = attestation_data.get('experience_objects', [])
    prior_years = attestation_data.get('prior_category_years', 0)

    for person in itr_list:
        periods = person.get('employment_periods')
        if periods and not person.get('stage_years'):
            calc = calculate_stazh(periods, as_of_date=attestation_data.get('as_of_date'))
            person['stage_years'] = calc['display']
        if periods and not person.get('stage_years_here'):
            last_period = periods[-1] if periods else None
            if last_period:
                calc_here = calculate_stazh([last_period], as_of_date=attestation_data.get('as_of_date'))
                person['stage_years_here'] = calc_here['display']

    warnings = []
    if category:
        warnings = check_category_requirements(category, staff_total, has_smetchik, experience_objects, prior_years)
    if len(itr_list) <= 1 and staff_total > 1:
        warnings.append(f"В данных только {len(itr_list)} человек в ИТР, хотя штат указан как {staff_total} — "
                         f"похоже часть людей потерялась при разборе. Проверьте пакет перед подачей.")
    empty_itr = [pp.get('fio', f'#{i+1}') for i, pp in enumerate(itr_list)
                 if not pp.get('diploma_number') and not pp.get('stage_years') and not pp.get('trudovaya_number')]
    if empty_itr:
        warnings.append(f"У этих людей вообще не заполнены диплом/стаж/трудовая (в документе будут прочерки): "
                         f"{', '.join(empty_itr)}.")
    partial_missing = [pp.get('fio', '?') for pp in itr_list if pp.get('diploma_number') and not pp.get('trudovaya_number')]
    if partial_missing:
        warnings.append(f"У этих людей есть диплом, но нет номера трудовой книжки: {', '.join(partial_missing)}.")
    if not workers:
        warnings.append("Реальные данные о рабочих не переданы — раздел «рабочие» в Форме №2 будет пустым, "
                         "а не придуман по виду работ. Уточните у клиента список профессий/разрядов/количества.")
    elif any(w.get('razryad') and str(w.get('razryad')).upper().strip() not in RAZRYAD_COLUMNS for w in workers):
        bad = [w.get('profession', '?') for w in workers if w.get('razryad') and str(w.get('razryad')).upper().strip() not in RAZRYAD_COLUMNS]
        warnings.append(f"У этих рабочих разряд указан не в формате II-VI (римскими цифрами), проверьте: {', '.join(bad)}.")

    if attestation_data.get('is_cancellation'):
        p("Заявление на отмену/исключение")
        docs.append({'name': f"{org} - Заявление на отмену.docx",
                     'bytes': gen_zayavlenie_otmena(company, attestation_data.get('old_attestat_number', ''),
                                                     attestation_data.get('cancellation_reason', 'по заявлению обладателя'))})
        return {'docs': docs, 'warnings': warnings}

    work_items = attestation_data.get('work_items') or []
    if not work_items and attestation_data.get('work_scope_text'):
        work_items = [code for code, _ in find_work_items(attestation_data['work_scope_text'])]
    if not work_items:
        work_items = ['7.4.1']

    work_lines = render_work_items_lines(work_items)
    _flat = _flat_work_items()
    work_scope_text = ', '.join(_flat.get(c, c) for c in work_items)

    p("1. Заявление")
    docs.append({'name': f"{org} - 1. Заявление.docx", 'bytes': render_zayavlenie(company, work_lines)})

    p("2. Форма №2 (ИТР и рабочие)")
    docs.append({'name': f"{org} - 2. Форма №2 ИТР и рабочие.docx",
                 'bytes': render_forma2(company, itr_list, workers, work_scope_text, staff_total)})

    p("3. Форма №3 (Трудовые)")
    docs.append({'name': f"{org} - 3. Форма №3 Трудовые.docx", 'bytes': render_forma3(company, itr_list)})

    p("4. Форма №4 (Дипломы)")
    docs.append({'name': f"{org} - 4. Форма №4 Дипломы.docx", 'bytes': render_forma4(company, itr_list)})

    p("5. Форма №5 (Аттестаты)")
    docs.append({'name': f"{org} - 5. Форма №5 Аттестаты.docx", 'bytes': render_forma5(company, itr_list)})

    if category:
        p("6. Форма №6 (Опыт генподрядчика) — пока старая версия, ещё не переведена на настоящий шаблон")
        docs.append({'name': f"{org} - 6. Форма №6 Опыт.docx",
                     'bytes': gen_form6_opyt(company, experience_objects)})

    return {'docs': docs, 'warnings': warnings}
