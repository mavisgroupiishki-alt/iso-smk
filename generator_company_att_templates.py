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
from datetime import datetime
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


# ═══════════════════ Общие данные и форматирование ═══════════════════
LEGAL_NAMES = {
    'ООО': {
        'nom': 'Общество с ограниченной ответственностью',
        'gen': 'Общества с ограниченной ответственностью',
        'dat': 'Обществу с ограниченной ответственностью',
    },
    'ОДО': {
        'nom': 'Общество с дополнительной ответственностью',
        'gen': 'Общества с дополнительной ответственностью',
        'dat': 'Обществу с дополнительной ответственностью',
    },
    'ЗАО': {
        'nom': 'Закрытое акционерное общество',
        'gen': 'Закрытого акционерного общества',
        'dat': 'Закрытому акционерному обществу',
    },
    'ОАО': {
        'nom': 'Открытое акционерное общество',
        'gen': 'Открытого акционерного общества',
        'dat': 'Открытому акционерному обществу',
    },
    'ЧУП': {
        'nom': 'Частное унитарное предприятие',
        'gen': 'Частного унитарного предприятия',
        'dat': 'Частному унитарному предприятию',
    },
    'ЧТУП': {
        'nom': 'Частное торговое унитарное предприятие',
        'gen': 'Частного торгового унитарного предприятия',
        'dat': 'Частному торговому унитарному предприятию',
    },
    'ИП': {
        'nom': 'Индивидуальный предприниматель',
        'gen': 'Индивидуального предпринимателя',
        'dat': 'Индивидуальному предпринимателю',
    },
}


def _clean_company_name(company: dict) -> str:
    raw = str(company.get('name') or '').strip()
    raw = re.sub(r'^(ООО|ОДО|ЧУП|ЧТУП|ЗАО|ОАО|ИП)\s*[«"“”]?\s*', '', raw, flags=re.I)
    return raw.strip().strip('»«"“” ')


def _company_case(company: dict, case='nom') -> str:
    form = str(company.get('form') or 'ООО').upper().strip()
    legal = LEGAL_NAMES.get(form, LEGAL_NAMES['ООО'])
    return f'{legal[case]} " {_clean_company_name(company)} "'


def _company_short(company: dict) -> str:
    return f'{str(company.get("form") or "ООО").upper()} " {_clean_company_name(company)} "'


def _dir_initials(fio: str) -> str:
    parts = (fio or '').strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio or ''


def _year_from_date(value: str = None) -> str:
    if value:
        for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y'):
            try:
                return str(datetime.strptime(str(value).strip(), fmt).year)
            except ValueError:
                pass
        m = re.search(r'20\d{2}', str(value))
        if m:
            return m.group(0)
    return str(datetime.now().year)


def _date_display(value: str = None) -> str:
    if not value:
        return '___.___.____'
    for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(str(value).strip(), fmt).strftime('%d.%m.%Y')
        except ValueError:
            pass
    return str(value).strip()


def _split_lines(value) -> list:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        result = []
        for item in value:
            result.extend(_split_lines(item))
        return result
    return [x.strip() for x in re.split(r'[\r\n]+', str(value)) if x.strip()]


def _bank_parts(company: dict) -> tuple:
    account = str(company.get('bank_account') or company.get('account') or '').strip()
    bank_name = str(company.get('bank_name') or '').strip()
    bik = str(company.get('bik') or company.get('bic') or '').strip()
    raw = str(company.get('bank_details') or '').strip()
    if raw:
        if not account:
            m = re.search(r'\bBY[0-9A-Z]{20,32}\b', raw, flags=re.I)
            if m:
                account = m.group(0).upper()
        if not bik:
            m = re.search(r'\b(?:БИК|BIC)\s*[:№]?\s*([A-Z0-9]{8,11})', raw, flags=re.I)
            if m:
                bik = m.group(1).upper()
        if not bank_name:
            # Работает и для многострочного, и для однострочного bank_details.
            m = re.search(r'(?:^|[\n;]|\s)в\s+(.+?)(?:,\s*(?:БИК|BIC)|$|\n|;)', raw, flags=re.I)
            if m:
                bank_name = m.group(1).strip()
    return account, bank_name, bik


def _replace_para_at(xml: str, paras: list, index: int, new_text: str) -> str:
    if index < 0 or index >= len(paras):
        return xml
    old = paras[index]
    if old not in xml:
        return xml
    return xml.replace(old, _replace_para_text(old, new_text), 1)


def _replace_matching_paragraphs(xml: str, predicate, new_text) -> str:
    for para in _paragraphs(xml):
        text = _para_text(para)
        if predicate(text):
            value = new_text(text) if callable(new_text) else new_text
            if para in xml:
                xml = xml.replace(para, _replace_para_text(para, value), 1)
    return xml


def _replace_signature_and_date(xml: str, company: dict, as_of_date: str = None) -> str:
    dir_pos = str(company.get('director_position') or 'Директор').strip()
    signature = f'{_company_short(company)}   _____________       {_dir_initials(company.get("director_fio", ""))}'
    xml = _replace_matching_paragraphs(
        xml,
        lambda t: '_____________' in t and re.search(r'\b(ООО|ОДО|ЧУП|ЧТУП|ЗАО|ОАО|ИП)\b', t),
        signature,
    )
    xml = _replace_matching_paragraphs(
        xml,
        lambda t: t.strip() in ('Директор', ' Директор') or (t.strip().endswith('Директор') and '_____________' not in t),
        dir_pos,
    )
    year = _year_from_date(as_of_date)
    xml = _replace_matching_paragraphs(
        xml,
        lambda t: t.startswith('«___»') or t.startswith('"___"'),
        f'«___»                     {year} г.',
    )
    return xml


def _person_diplomas(person: dict) -> list:
    diplomas = person.get('diplomas')
    if isinstance(diplomas, list) and diplomas:
        return [d for d in diplomas if isinstance(d, dict)]
    if any(person.get(k) for k in ('diploma_number', 'diploma_date', 'diploma_institution', 'diploma_speciality', 'diploma_qualification')):
        return [{
            'education_level': person.get('education_level', ''),
            'number': person.get('diploma_number', ''),
            'date': person.get('diploma_date', ''),
            'institution': person.get('diploma_institution', ''),
            'speciality': person.get('diploma_speciality', ''),
            'qualification': person.get('diploma_qualification', ''),
        }]
    return []


def _education_lines(person: dict) -> list:
    explicit = _split_lines(person.get('education_full_text'))
    if explicit:
        return explicit
    lines = []
    for diploma in _person_diplomas(person):
        level = str(diploma.get('education_level') or person.get('education_level') or '').strip()
        number = str(diploma.get('number') or diploma.get('diploma_number') or '').strip()
        date = str(diploma.get('date') or diploma.get('diploma_date') or '').strip()
        institution = str(diploma.get('institution') or diploma.get('diploma_institution') or '').strip()
        speciality = str(diploma.get('speciality') or diploma.get('diploma_speciality') or '').strip()
        qualification = str(diploma.get('qualification') or diploma.get('diploma_qualification') or '').strip()
        parts = []
        if level:
            parts.append(level)
        if number:
            text = f'Диплом {number}'
            if date:
                text += f' выдан {date} г.' if not date.endswith('г.') else f' выдан {date}'
            parts.append(text)
        if institution:
            parts.append(institution)
        if speciality:
            parts.append(speciality)
        if qualification:
            parts.append(qualification)
        if parts:
            lines.extend(parts)
    return lines or ['—']


def _diploma_number_lines(person: dict) -> list:
    explicit = _split_lines(person.get('diploma_numbers'))
    if explicit:
        return explicit
    result = []
    for diploma in _person_diplomas(person):
        number = str(diploma.get('number') or diploma.get('diploma_number') or '').strip()
        if number:
            result.append(number)
    return result or _split_lines(person.get('diploma_number')) or ['—']


def _trudovaya_number_lines(person: dict) -> list:
    values = person.get('trudovye_numbers')
    if isinstance(values, list) and values:
        lines = []
        for item in values:
            if isinstance(item, dict):
                prefix = str(item.get('type') or '').strip()
                number = str(item.get('number') or '').strip()
                lines.append(' '.join(x for x in (prefix, number) if x).strip())
            else:
                lines.extend(_split_lines(item))
        return [x for x in lines if x]
    return _split_lines(person.get('trudovaya_number')) or ['—']


def _trudovaya_cell_lines(person: dict) -> list:
    # В Форме №2 подпись документа может отличаться от краткого номера в Форме №3
    # (например «Трудовая книжка ПК № ...» против «ПК № ...»). Сохраняем оба варианта.
    explicit = _split_lines(person.get('trudovaya_form2_text'))
    if explicit:
        return explicit
    lines = _trudovaya_number_lines(person)
    order_number = str(person.get('order_number') or '').strip()
    hire_date = str(person.get('hire_date') or '').strip()
    if order_number or hire_date:
        text = f'Приказ № {order_number or "—"}'
        if hire_date:
            text += f' от {hire_date} г.' if not hire_date.endswith('г.') else f' от {hire_date}'
        lines.append(text)
    return lines


def _attestation_lines(person: dict, form: str = 'form2') -> list:
    # В реальных формах текст отличается: в Форме №2 обычно номер/дата выдачи,
    # в Форме №5 — номер, полный срок действия и специализация.
    field = 'attestat_form5_text' if form == 'form5' else 'attestat_form2_text'
    explicit = _split_lines(person.get(field)) or _split_lines(person.get('attestat_full_text'))
    if explicit:
        return explicit
    number = str(person.get('attestat_number') or '').strip()
    if not number:
        return ['—']
    line = number
    date_from = str(person.get('attestat_date_from') or person.get('attestat_date') or '').strip()
    date_to = str(person.get('attestat_date_to') or '').strip()
    if date_from:
        prefix = str(person.get('attestat_date_prefix') or 'с').strip()
        line += f' {prefix} {date_from}'
        if not date_from.endswith('г.'):
            line += ' г.'
    if date_to:
        line += f' по {date_to}'
        if not date_to.endswith('г.'):
            line += ' г.'
    lines = [line]
    specialization = str(person.get('attestat_specialization') or '').strip()
    if specialization:
        lines.append(specialization)
    return lines


# ═══════════════════ Работа с таблицами (клонирование строк) ═══════════════════
def _rows(xml_or_block: str) -> list:
    return re.findall(r'<w:tr\b.*?</w:tr>', xml_or_block, re.DOTALL)


def _cells(row_xml: str) -> list:
    return re.findall(r'<w:tc\b.*?</w:tc>', row_xml, re.DOTALL)


def _replace_cell_content(cell_xml: str, lines: list) -> str:
    paras_in_cell = re.findall(r'<w:p\b.*?</w:p>', cell_xml, re.DOTALL)
    if not paras_in_cell:
        return cell_xml
    style_para = paras_in_cell[0]
    tc_pr_match = re.match(r'(<w:tc\b.*?</w:tcPr>)', cell_xml, re.DOTALL)
    tc_pr = tc_pr_match.group(1) if tc_pr_match else cell_xml[:cell_xml.find('<w:p')]
    # Пустая ячейка должна оставаться пустой. Раньше здесь автоматически появлялось
    # «—», из-за чего тире попадали во все незадействованные разряды рабочих.
    raw_lines = list(lines or [''])
    cleaned = [str(x) for x in raw_lines if str(x).strip()]
    if not cleaned:
        cleaned = ['']
    def set_para_text(para_xml: str, line: str) -> str:
        if re.search(r'<w:t\b', para_xml):
            return _replace_para_text(para_xml, line)
        # В шаблоне пустые ячейки разрядов часто содержат абзац без <w:t>.
        # Добавляем текстовый run, иначе значение разряда визуально не появляется.
        run = f'<w:r><w:t xml:space="preserve">{_esc(line)}</w:t></w:r>'
        if '</w:p>' in para_xml:
            return para_xml.replace('</w:p>', run + '</w:p>', 1)
        return f'<w:p>{run}</w:p>'
    new_paras = ''.join(set_para_text(style_para, line) for line in cleaned)
    return f'{tc_pr}{new_paras}</w:tc>'


def _build_row(template_row_xml: str, cell_values: list) -> str:
    cells = _cells(template_row_xml)
    new_cells = []
    for i, cell in enumerate(cells):
        val = cell_values[i] if i < len(cell_values) else ''
        lines = val if isinstance(val, list) else [val]
        new_cells.append(_replace_cell_content(cell, lines))
    tr_open_end = template_row_xml.find('>', template_row_xml.find('<w:tr')) + 1
    tr_pr_match = re.search(r'<w:tr\b[^>]*>(<w:trPr>.*?</w:trPr>)?', template_row_xml, re.DOTALL)
    tr_open = template_row_xml[:tr_open_end] + (tr_pr_match.group(1) or '' if tr_pr_match else '')
    return tr_open + ''.join(new_cells) + '</w:tr>'


def _splice_rows(xml: str, old_rows_slice: list, new_rows: list) -> str:
    if not old_rows_slice:
        return xml
    first, last = old_rows_slice[0], old_rows_slice[-1]
    start = xml.find(first)
    end = xml.find(last, start) + len(last)
    if start < 0 or end < len(last):
        return xml
    return xml[:start] + ''.join(new_rows) + xml[end:]


# ═══════════════════ Документ 1: Заявление ═══════════════════
def render_zayavlenie(company: dict, work_item_lines: list, category=None, as_of_date: str = None) -> bytes:
    parts = _load_parts('1__Заявление.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    account, bank_name, bik = _bank_parts(company)
    full_nom = _company_case(company, 'nom')
    full_gen = _company_case(company, 'gen')
    full_dat = _company_case(company, 'dat')
    year = _year_from_date(as_of_date)

    replacements = {
        0: full_nom,
        1: company.get('address', ''),
        2: f'р/с: {account}',
        3: f'в {bank_name}, БИК {bik}' if bank_name or bik else 'в __________________, БИК __________',
        4: f'УНП {company.get("unp", "")}',
        5: f'Тел.: {company.get("phone", "")}',
        10: f'От___.____.{year} г.',
        15: full_nom,
        16: company.get('address', ''),
        17: f'УНП {company.get("unp", "")}',
        18: f'Тел.: {company.get("phone", "")}\ne-mail: {company.get("email", "")}',
        24: f'Прошу провести аттестацию {full_gen} на право осуществления:',
        54: (f'В соответствии с частью второй пункта 1 статьи 35 Кодекса Республики Беларусь '
             f'об архитектурной, градостроительной и строительной деятельности прошу оформить '
             f'{full_dat} аттестат соответствия на бумажном носителе.'),
        84: f'{_company_short(company)}   _____________       {_dir_initials(company.get("director_fio", ""))}',
    }

    idx_head = _find_para_index(paras, lambda t: t.startswith('7. Выполнение'))
    idx_end = _find_para_index(paras, lambda t: t.lower().startswith('соответствующей квалификационным'))
    if idx_head == -1 or idx_end == -1:
        raise RuntimeError('Не удалось найти блок видов работ в шаблоне заявления.')
    style_template = paras[idx_head + 2] if idx_head + 2 < idx_end else paras[idx_head]
    work_paras = []
    if category:
        work_paras.append(_clone_para_style(
            style_template,
            f'6. Выполнение функций генерального подрядчика, категория {category}.',
        ))
    work_paras.append(_replace_para_text(paras[idx_head], '7. Выполнение строительно-монтажных работ:'))
    for line in work_item_lines:
        if not line.startswith('7. Выполнение'):
            work_paras.append(_clone_para_style(style_template, line))
    old_block = ''.join(paras[idx_head:idx_end])
    xml = xml.replace(old_block, ''.join(work_paras), 1)

    for idx, text in replacements.items():
        xml = _replace_para_at(xml, paras, idx, text)

    email_idx = _find_para_index(paras, lambda t: 'mailto:' in t or t.lower().startswith('e-mail:'))
    if email_idx >= 0 and company.get('email'):
        old = paras[email_idx]
        if old in xml:
            if 'mailto:' in old:
                xml = xml.replace(old, _replace_email_para(old, company['email']), 1)
            else:
                xml = xml.replace(old, _replace_para_text(old, f'e-mail: {company["email"]}'), 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


def _replace_email_para(para_xml: str, new_email: str) -> str:
    para_xml = re.sub(r'mailto:[^" ]+', f'mailto:{new_email}', para_xml)
    text_nodes = list(re.finditer(r'(<w:t[^>]*>)(.*?)(</w:t>)', para_xml, re.DOTALL))
    if text_nodes:
        target = next((m for m in reversed(text_nodes) if '@' in m.group(2)), text_nodes[-1])
        para_xml = para_xml[:target.start()] + target.group(1) + _esc(new_email) + target.group(3) + para_xml[target.end():]
    return para_xml


# ═══════════════════ Документ 2: Форма №2 — ИТР + рабочие ═══════════════════
def render_forma2(company: dict, itr_list: list, workers: list, work_scope_text: str,
                  staff_total=None, as_of_date: str = None) -> bytes:
    parts = _load_parts('2__ИТР.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    xml = _replace_signature_and_date(xml, company, as_of_date)
    paras = _paragraphs(xml)
    rows = _rows(xml)

    n_itr = len(itr_list)
    calculated_total = n_itr + sum(max(0, int(w.get('count') or 0)) for w in workers)
    try:
        provided_total = int(staff_total or 0)
    except (TypeError, ValueError):
        provided_total = 0
    total = max(provided_total, calculated_total)
    date_text = _date_display(as_of_date)

    idx_staff = _find_para_index(paras, lambda t: t.startswith('Общая численность'))
    idx_itr_count = _find_para_index(paras, lambda t: t.startswith('численность инженерно'))
    if idx_staff >= 0:
        xml = _replace_para_at(
            xml, paras, idx_staff,
            f'Общая численность работающих {total} чел., в том числе по заявляемому виду '
            f'деятельности {total} чел. по состоянию на {date_text};',
        )
    if idx_itr_count >= 0:
        xml = _replace_para_at(
            xml, paras, idx_itr_count,
            f'численность инженерно-технических работников по заявляемому виду деятельности {n_itr} чел.',
        )

    rows = _rows(xml)
    itr_template_row = rows[2]
    itr_rows_new = []
    for i, person in enumerate(itr_list, 1):
        stage_lines = [str(person.get('stage_years') or '—'), str(person.get('stage_years_here') or '—')]
        itr_rows_new.append(_build_row(itr_template_row, [
            str(i),
            person.get('position', ''),
            person.get('fio', ''),
            _education_lines(person),
            stage_lines,
            _trudovaya_cell_lines(person),
            _attestation_lines(person, 'form2'),
        ]))
    xml = _splice_rows(xml, rows[2:5], itr_rows_new)

    rows2 = _rows(xml)
    w_header_idx = next((i for i, row in enumerate(rows2) if 'Наименование профессий рабочих' in row), None)
    if w_header_idx is not None:
        old_total_idx = next((i for i in range(w_header_idx, len(rows2)) if 'Итого по разрядам' in rows2[i]), None)
        if old_total_idx is not None:
            worker_template_row = rows2[w_header_idx + 3]
            old_worker_rows = rows2[w_header_idx + 3:old_total_idx + 1]
            from generator_company_att import RAZRYAD_COLUMNS, group_workers_for_form
            totals = {r: 0 for r in RAZRYAD_COLUMNS}
            new_worker_rows = []
            grouped_workers = group_workers_for_form(workers)
            for i, worker in enumerate(grouped_workers, 1):
                cells = [str(i), worker.get('profession', '')]
                counts = worker.get('counts') or {}
                for r in RAZRYAD_COLUMNS:
                    count = max(0, int(counts.get(r) or 0))
                    cells.append(str(count) if count else '')
                    totals[r] += count
                cells.append(str(worker.get('total') or '') if worker.get('total') else '')
                new_worker_rows.append(_build_row(worker_template_row, cells))
            total_cells = ['', 'Итого по разрядам:'] + [str(totals[r]) if totals[r] else '' for r in RAZRYAD_COLUMNS] + [str(sum(totals.values()))]
            new_worker_rows.append(_build_row(rows2[old_total_idx], total_cells))
            xml = _splice_rows(xml, old_worker_rows, new_worker_rows)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 3: Форма №3 — Трудовые ═══════════════════
def render_forma3(company: dict, itr_list: list, as_of_date: str = None) -> bytes:
    parts = _load_parts('3__Трудовые.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    xml = _replace_signature_and_date(xml, company, as_of_date)
    rows = _rows(xml)
    template_row = rows[2]
    new_rows = []
    for i, person in enumerate(itr_list, 1):
        new_rows.append(_build_row(template_row, [
            str(i), person.get('fio', ''), person.get('position', ''), _trudovaya_number_lines(person)
        ]))
    xml = _splice_rows(xml, rows[2:], new_rows)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 4: Форма №4 — Дипломы ═══════════════════
def render_forma4(company: dict, itr_list: list, as_of_date: str = None) -> bytes:
    parts = _load_parts('4__Дипломы.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    xml = _replace_signature_and_date(xml, company, as_of_date)
    rows = _rows(xml)
    template_row = rows[2]
    people = [p for p in itr_list if _diploma_number_lines(p) != ['—']] or itr_list
    new_rows = []
    for i, person in enumerate(people, 1):
        new_rows.append(_build_row(template_row, [str(i), person.get('fio', ''), _diploma_number_lines(person)]))
    xml = _splice_rows(xml, rows[2:], new_rows)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 5: Форма №5 — Аттестаты ═══════════════════
def render_forma5(company: dict, itr_list: list, as_of_date: str = None) -> bytes:
    parts = _load_parts('5__Аттестаты.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    xml = _replace_signature_and_date(xml, company, as_of_date)
    rows = _rows(xml)
    template_row = rows[2]
    people = [p for p in itr_list if p.get('attestat_number') or p.get('attestat_full_text')]
    new_rows = []
    for i, person in enumerate(people, 1):
        new_rows.append(_build_row(template_row, [
            str(i), person.get('fio', ''), person.get('position', ''), _attestation_lines(person, 'form5')
        ]))
    if not new_rows:
        new_rows = [_build_row(template_row, ['1', '—', '—', 'нет аттестатов среди ИТР'])]
    xml = _splice_rows(xml, rows[2:], new_rows)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Адаптер для реального пайплайна ═══════════════════
def generate_company_attestation_package_v2(company: dict, attestation_data: dict,
                                             api_key=None, vibe_call_fn=None, progress_cb=None) -> dict:
    from generator_company_att import (
        resolve_work_items, render_work_items_lines, calculate_stazh,
        calculate_current_company_stazh, select_relevant_periods, resolve_workers,
        check_category_requirements, _flat_work_items, RAZRYAD_COLUMNS,
        gen_zayavlenie_otmena, gen_form6_opyt,
    )

    docs = []
    step = [0]
    category = attestation_data.get('category')
    if isinstance(category, str) and category.strip().lower() in ('', 'null', 'none', 'нет', 'undefined'):
        category = None
    total_steps = 1 if attestation_data.get('is_cancellation') else (6 if category else 5)

    def progress(message):
        step[0] += 1
        if progress_cb:
            progress_cb(step[0], total_steps, message)
        print(f'  [company_att_v3 {step[0]}] {message}')

    org = _clean_company_name(company) or company.get('name', 'company')
    itr_list = [dict(p) for p in (attestation_data.get('itr') or [])]
    work_items = resolve_work_items(attestation_data)
    workers = resolve_workers(attestation_data, work_items)
    as_of_date = attestation_data.get('as_of_date')

    for person in itr_list:
        periods = person.get('employment_periods') or []
        relevant_periods = select_relevant_periods(person, periods)
        # Дословные значения из заполненной Формы №2 имеют приоритет.
        if relevant_periods and not person.get('stage_years'):
            person['stage_years'] = calculate_stazh(relevant_periods, as_of_date=as_of_date)['display']
        if relevant_periods and not person.get('stage_years_here'):
            current = calculate_current_company_stazh(relevant_periods, company, as_of_date=as_of_date)
            if current['years'] or current['months'] or current['days']:
                person['stage_years_here'] = current['display']

    calculated_total = len(itr_list) + sum(max(0, int(w.get('count') or 0)) for w in workers)
    try:
        provided_total = int(attestation_data.get('staff_total') or 0)
    except (TypeError, ValueError):
        provided_total = 0
    staff_total = max(provided_total, calculated_total)

    warnings = []
    if category:
        warnings.extend(check_category_requirements(
            category, staff_total, bool(attestation_data.get('has_smetchik')),
            attestation_data.get('experience_objects') or [],
            int(attestation_data.get('prior_category_years') or 0),
        ))
    if not work_items:
        warnings.append('Не определены виды работ: заявление нельзя подавать без заполненного пункта 7.')
    if not itr_list:
        warnings.append('Не заполнены ИТР для Форм №2–5.')
    if not workers:
        warnings.append('Не заполнены рабочие для Формы №2.')
    missing_stage = [p.get('fio', '?') for p in itr_list if not p.get('stage_years') or not p.get('stage_years_here')]
    if missing_stage:
        warnings.append('Не полностью заполнен стаж: ' + ', '.join(missing_stage))
    for worker in workers:
        if str(worker.get('razryad') or '').upper().strip() not in RAZRYAD_COLUMNS:
            warnings.append(f"Неверный разряд у рабочего {worker.get('profession', '?')}: {worker.get('razryad', '')}")

    if attestation_data.get('is_cancellation'):
        progress('Заявление на отмену/исключение')
        docs.append({
            'name': f'{org} - Заявление на отмену.docx',
            'bytes': gen_zayavlenie_otmena(
                company,
                attestation_data.get('old_attestat_number', ''),
                attestation_data.get('cancellation_reason', 'по заявлению обладателя'),
            ),
        })
        return {'docs': docs, 'warnings': warnings}

    work_lines = render_work_items_lines(work_items)
    flat = _flat_work_items()
    work_scope_text = ', '.join(flat.get(code, code) for code in work_items)

    progress('1. Заявление')
    docs.append({
        'name': f'{org} - 1. Заявление.docx',
        'bytes': render_zayavlenie(company, work_lines, category=category, as_of_date=as_of_date),
    })
    progress('2. Форма №2 (ИТР и рабочие)')
    docs.append({
        'name': f'{org} - 2. Форма №2 ИТР и рабочие.docx',
        'bytes': render_forma2(company, itr_list, workers, work_scope_text, staff_total, as_of_date),
    })
    progress('3. Форма №3 (Трудовые)')
    docs.append({'name': f'{org} - 3. Форма №3 Трудовые.docx', 'bytes': render_forma3(company, itr_list, as_of_date)})
    progress('4. Форма №4 (Дипломы)')
    docs.append({'name': f'{org} - 4. Форма №4 Дипломы.docx', 'bytes': render_forma4(company, itr_list, as_of_date)})
    progress('5. Форма №5 (Аттестаты)')
    docs.append({'name': f'{org} - 5. Форма №5 Аттестаты.docx', 'bytes': render_forma5(company, itr_list, as_of_date)})

    if category:
        progress('6. Форма №6 (Опыт генподрядчика)')
        docs.append({
            'name': f'{org} - 6. Форма №6 Опыт.docx',
            'bytes': gen_form6_opyt(company, attestation_data.get('experience_objects') or []),
        })

    return {'docs': docs, 'warnings': list(dict.fromkeys(warnings))}
