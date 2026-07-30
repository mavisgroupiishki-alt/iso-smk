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


_REVIEW_FILL = 'FFF2CC'
_REVIEW_DASHES = {'', '—', '-', '–'}


def _review_value(value, yellow: bool = False):
    """Value wrapper understood by paragraph/cell renderers."""
    return {'__review_text__': value, '__review_yellow__': bool(yellow)}


def _unwrap_review(value):
    if isinstance(value, dict) and '__review_text__' in value:
        return value.get('__review_text__'), bool(value.get('__review_yellow__'))
    return value, False


def _meaningful(value) -> bool:
    if isinstance(value, (list, tuple)):
        return any(_meaningful(x) for x in value)
    text = str(value or '').strip()
    return bool(text) and text not in _REVIEW_DASHES and 'не найдено' not in text.lower()


def _uncertain_text(value) -> bool:
    if isinstance(value, (list, tuple, set)):
        return any(_uncertain_text(item) for item in value)
    if isinstance(value, dict):
        return any(_uncertain_text(item) for item in value.values())
    text = str(value or '').lower().replace('ё', 'е')
    return bool(text) and any(x in text for x in (
        'неразборчив', 'не уверен', 'требует уточнения', 'две попытки чтения разошлись',
        'предположительно', '[', ']', '?',
    ))


def _field_needs_review(record: dict, field: str, value=None) -> bool:
    if not _meaningful(value):
        return True
    if _uncertain_text(value):
        return True
    if not isinstance(record, dict):
        return False
    uncertain = record.get('uncertain_fields') or []
    if isinstance(uncertain, str):
        uncertain = [x.strip() for x in re.split(r'[,;\n]+', uncertain) if x.strip()]
    normalized = {str(x).strip().lower().replace('ё', 'е') for x in uncertain}
    field_norm = str(field or '').strip().lower().replace('ё', 'е')
    if field_norm in normalized:
        return True
    confidence = None
    field_conf = record.get('field_confidence')
    if isinstance(field_conf, dict):
        confidence = field_conf.get(field)
    if confidence is None:
        confidence = record.get(f'{field}_confidence')
    if isinstance(confidence, (int, float)) and float(confidence) < 0.85:
        return True
    if isinstance(confidence, str) and confidence.lower() in ('low', 'uncertain', 'низкая', 'низкий', 'сомнительно'):
        return True
    return bool(record.get('needs_review')) and not normalized


def _mark_field(record: dict, field: str, value, force: bool = False):
    return _review_value(value, force or _field_needs_review(record, field, value))


def _add_run_highlight(run_prefix: str) -> str:
    tag = '<w:highlight w:val="yellow"/>'
    if tag in run_prefix:
        return run_prefix
    if '</w:rPr>' in run_prefix:
        return run_prefix.replace('</w:rPr>', tag + '</w:rPr>', 1)
    m = re.match(r'(<w:r\b[^>]*>)', run_prefix)
    if m:
        return run_prefix[:m.end()] + '<w:rPr>' + tag + '</w:rPr>' + run_prefix[m.end():]
    return run_prefix


def _shade_tc_pr(tc_pr: str) -> str:
    shd = f'<w:shd w:val="clear" w:color="auto" w:fill="{_REVIEW_FILL}"/>'
    if re.search(r'<w:shd\b', tc_pr):
        return re.sub(r'<w:shd\b[^>]*/>', shd, tc_pr, count=1)
    if '</w:tcPr>' in tc_pr:
        return tc_pr.replace('</w:tcPr>', shd + '</w:tcPr>', 1)
    return tc_pr


_REVIEW_TOKEN_RE = re.compile(
    r'(?:ТРЕБУЕТ\s+УТОЧНЕНИЯ|НЕРАЗБОРЧИВ|НЕ\s+УВЕРЕН|ДВЕ\s+ПОПЫТКИ\s+ЧТЕНИЯ|'
    r'_{3,}|нет\s+аттестата\s*/\s*в\s+процессе)',
    re.IGNORECASE,
)


def highlight_review_tokens(docx_bytes: bytes) -> bytes:
    """Final safety layer: highlight every explicit missing/uncertain placeholder.

    Template-specific renderers already shade known fields. This pass also covers
    auxiliary documents produced by the legacy generator (for example Form №6 or a
    cancellation statement), so a placeholder can never remain visually invisible.
    """
    try:
        with zipfile.ZipFile(BytesIO(docx_bytes), 'r') as source:
            parts = {name: source.read(name) for name in source.namelist()}
    except Exception:
        return docx_bytes
    for name in list(parts):
        if not (name.startswith('word/') and name.endswith('.xml')):
            continue
        xml = parts[name].decode('utf-8', errors='ignore')

        def mark_run(match):
            run = match.group(0)
            visible = ' '.join(re.findall(r'<w:t[^>]*>(.*?)</w:t>', run, re.DOTALL))
            visible = re.sub(r'<[^>]+>', '', visible)
            if not _REVIEW_TOKEN_RE.search(visible):
                return run
            if '<w:highlight w:val="yellow"/>' in run:
                return run
            if '<w:rPr>' in run:
                return run.replace('</w:rPr>', '<w:highlight w:val="yellow"/></w:rPr>', 1)
            opening = re.match(r'(<w:r\b[^>]*>)', run)
            if opening:
                return run[:opening.end()] + '<w:rPr><w:highlight w:val="yellow"/></w:rPr>' + run[opening.end():]
            return run

        xml = re.sub(r'<w:r\b.*?</w:r>', mark_run, xml, flags=re.DOTALL)
        parts[name] = xml.encode('utf-8')
    return _rebuild(parts)


def _replace_para_text(para_xml: str, new_text: str) -> str:
    """Replace visible paragraph text while preserving the first run style.

    A value wrapped with _review_value(..., yellow=True) receives a real Word
    text highlight. This is used for every missing or uncertain field.
    """
    new_text, yellow = _unwrap_review(new_text)
    m = re.search(r'(<w:r\b[^P].*?)<w:t[^>]*>.*?</w:t>(.*?</w:r>)', para_xml, re.DOTALL)
    if not m:
        return para_xml
    run_prefix = m.group(1)
    if yellow:
        run_prefix = _add_run_highlight(run_prefix)
    run_suffix = m.group(2)
    new_run = f'{run_prefix}<w:t xml:space="preserve">{_esc(new_text)}</w:t>{run_suffix}'
    if '</w:pPr>' in para_xml:
        p_open_end = para_xml.find('</w:pPr>') + len('</w:pPr>')
    else:
        p_open_end = para_xml.find('>') + 1
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



def _set_page_break_before(para_xml: str) -> str:
    tag = '<w:pageBreakBefore/>'
    if tag in para_xml:
        return para_xml
    if '<w:pPr>' in para_xml:
        return para_xml.replace('</w:pPr>', tag + '</w:pPr>', 1)
    opening = re.match(r'(<w:p\b[^>]*>)', para_xml)
    if opening:
        return para_xml[:opening.end()] + '<w:pPr>' + tag + '</w:pPr>' + para_xml[opening.end():]
    return para_xml


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


_RU_MONTHS = {
    1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля', 5: 'мая', 6: 'июня',
    7: 'июля', 8: 'августа', 9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря',
}


def _signature_date_text(value: str = None) -> str:
    if value:
        for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y'):
            try:
                d = datetime.strptime(str(value).strip(), fmt)
                return f'«{d.day:02d}» {_RU_MONTHS[d.month]} {d.year} г.'
            except ValueError:
                pass
    return f'«___» __________ {_year_from_date(value)} г.'


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
    director_fio = company.get('director_fio', '')
    signature = f'{_company_short(company)}   _____________       {_dir_initials(director_fio)}'
    signature_review = (
        _field_needs_review(company, 'name', company.get('name'))
        or _field_needs_review(company, 'director_fio', director_fio)
    )
    xml = _replace_matching_paragraphs(
        xml,
        lambda t: '_____________' in t and re.search(r'\b(ООО|ОДО|ЧУП|ЧТУП|ЗАО|ОАО|ИП)\b', t),
        _review_value(signature, signature_review),
    )
    xml = _replace_matching_paragraphs(
        xml,
        lambda t: t.strip() in ('Директор', ' Директор') or (t.strip().endswith('Директор') and '_____________' not in t),
        _mark_field(company, 'director_position', dir_pos),
    )
    signature_date = company.get('signature_date') or company.get('document_date')
    xml = _replace_matching_paragraphs(
        xml,
        lambda t: t.startswith('«___»') or t.startswith('"___"'),
        _review_value(_signature_date_text(signature_date), not bool(signature_date)),
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


def _replace_cell_content(cell_xml: str, lines: list, yellow: bool = False) -> str:
    paras_in_cell = re.findall(r'<w:p\b.*?</w:p>', cell_xml, re.DOTALL)
    if not paras_in_cell:
        return cell_xml
    style_para = paras_in_cell[0]
    tc_pr_match = re.match(r'(<w:tc\b.*?</w:tcPr>)', cell_xml, re.DOTALL)
    tc_pr = tc_pr_match.group(1) if tc_pr_match else cell_xml[:cell_xml.find('<w:p')]
    if yellow:
        tc_pr = _shade_tc_pr(tc_pr)
    raw_lines = list(lines or [''])
    cleaned = [str(x) for x in raw_lines if str(x).strip()]
    if not cleaned:
        cleaned = ['']
    def set_para_text(para_xml: str, line: str) -> str:
        wrapped = _review_value(line, yellow)
        if re.search(r'<w:t\b', para_xml):
            return _replace_para_text(para_xml, wrapped)
        run_pr = '<w:rPr><w:highlight w:val="yellow"/></w:rPr>' if yellow else ''
        run = f'<w:r>{run_pr}<w:t xml:space="preserve">{_esc(line)}</w:t></w:r>'
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
        val, yellow = _unwrap_review(val)
        lines = val if isinstance(val, list) else [val]
        new_cells.append(_replace_cell_content(cell, lines, yellow=yellow))
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
def render_zayavlenie(company: dict, work_item_lines: list, category=None,
                       as_of_date: str = None, review: dict = None) -> bytes:
    parts = _load_parts('1__Заявление.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    account, bank_name, bik = _bank_parts(company)
    full_nom = _company_case(company, 'nom')
    full_gen = _company_case(company, 'gen')
    full_dat = _company_case(company, 'dat')
    year = _year_from_date(as_of_date)
    review = review or {}

    company_name_bad = _field_needs_review(company, 'name', company.get('name'))
    address_bad = _field_needs_review(company, 'address', company.get('address'))
    account_bad = _field_needs_review(company, 'bank_account', account)
    bank_bad = _field_needs_review(company, 'bank_name', bank_name) or _field_needs_review(company, 'bik', bik)
    unp_bad = _field_needs_review(company, 'unp', company.get('unp'))
    phone_bad = _field_needs_review(company, 'phone', company.get('phone'))
    email_bad = _field_needs_review(company, 'email', company.get('email'))
    director_bad = _field_needs_review(company, 'director_fio', company.get('director_fio'))
    outgoing_number = str(review.get('outgoing_number') or company.get('outgoing_number') or '').strip()
    outgoing_date = str(review.get('outgoing_date') or company.get('outgoing_date') or '').strip()
    outgoing_date_text = _date_display(outgoing_date) if outgoing_date else '___.___.____'

    replacements = {
        0: _review_value(full_nom, company_name_bad),
        1: _review_value(company.get('address', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ: юридический адрес', address_bad),
        2: _review_value(f'р/с: {account or "ТРЕБУЕТ УТОЧНЕНИЯ"}', account_bad),
        3: _review_value(
            f'в {bank_name or "ТРЕБУЕТ УТОЧНЕНИЯ"}, БИК {bik or "ТРЕБУЕТ УТОЧНЕНИЯ"}',
            bank_bad,
        ),
        4: _review_value(f'УНП {company.get("unp", "") or "ТРЕБУЕТ УТОЧНЕНИЯ"}', unp_bad),
        5: _review_value(f'Тел.: {company.get("phone", "") or "ТРЕБУЕТ УТОЧНЕНИЯ"}', phone_bad),
        9: _review_value(f'Исх. № {outgoing_number or "ТРЕБУЕТ УТОЧНЕНИЯ"}', not bool(outgoing_number)),
        10: _review_value(f'От {outgoing_date_text} г.', not bool(outgoing_date)),
        15: _review_value(full_nom, company_name_bad),
        16: _review_value(company.get('address', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ: юридический адрес', address_bad),
        17: _review_value(f'УНП {company.get("unp", "") or "ТРЕБУЕТ УТОЧНЕНИЯ"}', unp_bad),
        18: _review_value(
            f'Тел.: {company.get("phone", "") or "ТРЕБУЕТ УТОЧНЕНИЯ"}\n'
            f'e-mail: {company.get("email", "") or "ТРЕБУЕТ УТОЧНЕНИЯ"}',
            phone_bad or email_bad,
        ),
        24: _review_value(f'Прошу провести аттестацию {full_gen} на право осуществления:', company_name_bad),
        54: _review_value(
            f'В соответствии с частью второй пункта 1 статьи 35 Кодекса Республики Беларусь '
            f'об архитектурной, градостроительной и строительной деятельности прошу оформить '
            f'{full_dat} аттестат соответствия на бумажном носителе.',
            company_name_bad,
        ),
        84: _review_value(
            f'{_company_short(company)}   _____________       {_dir_initials(company.get("director_fio", ""))}',
            company_name_bad or director_bad,
        ),
    }

    idx_head = _find_para_index(paras, lambda t: t.startswith('7. Выполнение'))
    idx_end = _find_para_index(paras, lambda t: t.lower().startswith('соответствующей квалификационным'))
    if idx_head == -1 or idx_end == -1:
        raise RuntimeError('Не удалось найти блок видов работ в шаблоне заявления.')
    style_template = paras[idx_head + 2] if idx_head + 2 < idx_end else paras[idx_head]
    work_paras = []
    category_review = bool(review.get('category_needs_review')) or _field_needs_review(review, 'category', category) if category else False
    if category:
        work_paras.append(_clone_para_style(
            style_template,
            _review_value(f'6. Выполнение функций генерального подрядчика, категория {category}.', category_review),
        ))
    work_paras.append(_replace_para_text(paras[idx_head], '7. Выполнение строительно-монтажных работ:'))
    work_review = bool(review.get('work_items_needs_review')) or _field_needs_review(review, 'work_items', work_item_lines)
    visible_lines = [line for line in (work_item_lines or []) if not str(line).startswith('7. Выполнение')]
    if not visible_lines:
        visible_lines = ['ТРЕБУЕТ УТОЧНЕНИЯ: виды строительно-монтажных работ']
        work_review = True
    for line in visible_lines:
        work_paras.append(_clone_para_style(style_template, _review_value(line, work_review or _uncertain_text(line))))
    old_block = ''.join(paras[idx_head:idx_end])
    xml = xml.replace(old_block, ''.join(work_paras), 1)

    for idx, text in replacements.items():
        xml = _replace_para_at(xml, paras, idx, text)

    email_idx = _find_para_index(paras, lambda t: 'mailto:' in t or t.lower().startswith('e-mail:'))
    if email_idx >= 0:
        old = paras[email_idx]
        email_value = company.get('email') or 'ТРЕБУЕТ УТОЧНЕНИЯ'
        if old in xml:
            if 'mailto:' in old and company.get('email'):
                replaced = _replace_email_para(old, company['email'])
                if email_bad:
                    replaced = _replace_para_text(replaced, _review_value(f'e-mail: {company["email"]}', True))
                xml = xml.replace(old, replaced, 1)
            else:
                xml = xml.replace(old, _replace_para_text(old, _review_value(f'e-mail: {email_value}', email_bad)), 1)

    # Page counts depend on the final signed/copied package and cannot be guessed.
    # If the caller did not provide them, make the omission explicit in yellow.
    page_counts = review.get('attachment_page_counts') or {}
    if isinstance(page_counts, (list, tuple)):
        page_counts = {str(i + 2): value for i, value in enumerate(page_counts)}
    rows_now = _rows(xml)
    form_keys = {3: ('2', 'form2'), 4: ('3', 'form3'), 5: ('4', 'form4'), 6: ('5', 'form5')}
    numeric_values = []
    for row_index, keys in form_keys.items():
        if row_index >= len(rows_now):
            continue
        value = ''
        for key in keys:
            if isinstance(page_counts, dict) and page_counts.get(key) not in (None, ''):
                value = str(page_counts.get(key)).strip()
                break
        cells = _cells(rows_now[row_index])
        if len(cells) >= 3:
            display = value or '?'
            new_cell = _replace_cell_content(cells[-1], [display], yellow=not bool(value))
            new_row = rows_now[row_index].replace(cells[-1], new_cell, 1)
            xml = xml.replace(rows_now[row_index], new_row, 1)
        if value.isdigit():
            numeric_values.append(int(value))
    rows_now = _rows(xml)
    if len(rows_now) > 7:
        total_value = ''
        supplied_total = page_counts.get('total') if isinstance(page_counts, dict) else None
        if supplied_total not in (None, ''):
            total_value = str(supplied_total).strip()
        elif len(numeric_values) == len(form_keys):
            total_value = str(sum(numeric_values))
        cells = _cells(rows_now[7])
        if cells:
            display = total_value or '?'
            new_cell = _replace_cell_content(cells[-1], [display], yellow=not bool(total_value))
            new_row = rows_now[7].replace(cells[-1], new_cell, 1)
            xml = xml.replace(rows_now[7], new_row, 1)

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
        staff_review = total <= 0 or date_text.startswith('___')
        xml = _replace_para_at(
            xml, paras, idx_staff,
            _review_value(
                f'Общая численность работающих {total or "ТРЕБУЕТ УТОЧНЕНИЯ"} чел., '
                f'в том числе по заявляемому виду деятельности {total or "ТРЕБУЕТ УТОЧНЕНИЯ"} чел. '
                f'по состоянию на {date_text};',
                staff_review,
            ),
        )
    if idx_itr_count >= 0:
        xml = _replace_para_at(
            xml, paras, idx_itr_count,
            _review_value(
                f'численность инженерно-технических работников по заявляемому виду деятельности '
                f'{n_itr or "ТРЕБУЕТ УТОЧНЕНИЯ"} чел.',
                n_itr <= 0,
            ),
        )

    rows = _rows(xml)
    itr_template_row = rows[2]
    itr_rows_new = []
    for i, person in enumerate(itr_list, 1):
        stage_total = str(person.get('stage_years') or 'ТРЕБУЕТ УТОЧНЕНИЯ')
        stage_here = str(person.get('stage_years_here') or 'ТРЕБУЕТ УТОЧНЕНИЯ')
        stage_review = bool(person.get('stage_needs_review')) or not person.get('stage_is_final')
        education = _education_lines(person)
        labour = _trudovaya_cell_lines(person)
        attest = _attestation_lines(person, 'form2')
        itr_rows_new.append(_build_row(itr_template_row, [
            str(i),
            _mark_field(person, 'position', person.get('position', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            _mark_field(person, 'fio', person.get('fio', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            _review_value(education if education != ['—'] else ['ТРЕБУЕТ УТОЧНЕНИЯ'],
                          _field_needs_review(person, 'education', education)),
            _review_value([stage_total, stage_here], stage_review),
            _review_value(labour if labour and labour != ['—'] else ['ТРЕБУЕТ УТОЧНЕНИЯ'],
                          _field_needs_review(person, 'trudovaya_number', labour)),
            _review_value(attest if attest and attest != ['—'] else ['ТРЕБУЕТ УТОЧНЕНИЯ'],
                          _field_needs_review(person, 'attestat_number', attest)),
        ]))
    if not itr_rows_new:
        itr_rows_new = [_build_row(itr_template_row, [
            '1',
            _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True),
            _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True),
            _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True),
            _review_value(['ТРЕБУЕТ УТОЧНЕНИЯ', 'ТРЕБУЕТ УТОЧНЕНИЯ'], True),
            _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True),
            _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True),
        ])]
    xml = _splice_rows(xml, rows[2:5], itr_rows_new)

    # Keep the workers table together on a fresh page. Without this, LibreOffice can
    # split the two-row header at the page bottom and visually stretch column borders
    # through the signature block when there are only one or two ITR rows.
    for para in _paragraphs(xml):
        if _para_text(para).strip().lower().startswith('рабочих строительных профессий'):
            xml = xml.replace(para, _set_page_break_before(para), 1)
            break

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
                profession_review = _field_needs_review(worker, 'profession', worker.get('profession'))
                counts_review = bool(worker.get('needs_review')) or _field_needs_review(worker, 'razryad', worker.get('counts') or {})
                cells = [str(i), _review_value(worker.get('profession', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ', profession_review)]
                counts = worker.get('counts') or {}
                for r in RAZRYAD_COLUMNS:
                    count = max(0, int(counts.get(r) or 0))
                    cells.append(_review_value(str(count) if count else '', counts_review and bool(count)))
                    totals[r] += count
                total_value = str(worker.get('total') or '') if worker.get('total') else ''
                cells.append(_review_value(total_value or 'ТРЕБУЕТ УТОЧНЕНИЯ', counts_review or not total_value))
                new_worker_rows.append(_build_row(worker_template_row, cells))
            if not new_worker_rows:
                new_worker_rows.append(_build_row(worker_template_row, [
                    '1', _review_value('ТРЕБУЕТ УТОЧНЕНИЯ: рабочие', True), '', '', '', '', '', _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True)
                ]))
            total_cells = ['', 'Итого по разрядам:'] + [str(totals[r]) if totals[r] else '' for r in RAZRYAD_COLUMNS] + [str(sum(totals.values())) if totals else '']
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
        labour = _trudovaya_number_lines(person)
        new_rows.append(_build_row(template_row, [
            str(i),
            _mark_field(person, 'fio', person.get('fio', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            _mark_field(person, 'position', person.get('position', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            _review_value(labour if labour != ['—'] else ['ТРЕБУЕТ УТОЧНЕНИЯ'],
                          _field_needs_review(person, 'trudovaya_number', labour)),
        ]))
    if not new_rows:
        new_rows = [_build_row(template_row, [
            '1', _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True),
            _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True), _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True)
        ])]
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
        diplomas = _diploma_number_lines(person)
        new_rows.append(_build_row(template_row, [
            str(i),
            _mark_field(person, 'fio', person.get('fio', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            _review_value(diplomas if diplomas != ['—'] else ['ТРЕБУЕТ УТОЧНЕНИЯ'],
                          _field_needs_review(person, 'diploma_number', diplomas)),
        ]))
    if not new_rows:
        new_rows = [_build_row(template_row, [
            '1', _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True), _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True)
        ])]
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
    # Keep all ITR visible. If an attestation is missing, the yellow cell makes the
    # omission explicit instead of silently dropping the person from Form 5.
    people = itr_list
    new_rows = []
    for i, person in enumerate(people, 1):
        attest = _attestation_lines(person, 'form5')
        new_rows.append(_build_row(template_row, [
            str(i),
            _mark_field(person, 'fio', person.get('fio', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            _mark_field(person, 'position', person.get('position', '') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            _review_value(attest if attest != ['—'] else ['ТРЕБУЕТ УТОЧНЕНИЯ'],
                          _field_needs_review(person, 'attestat_number', attest)),
        ]))
    if not new_rows:
        new_rows = [_build_row(template_row, [
            '1', _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True),
            _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True), _review_value('ТРЕБУЕТ УТОЧНЕНИЯ', True)
        ])]
    xml = _splice_rows(xml, rows[2:], new_rows)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Нормализация стажа ═══════════════════
_STAGE_DASHES = {'', '—', '-', '–'}


def _stage_text_lines(value) -> list[str]:
    """Нормализует стаж из Form 2 независимо от того, пришёл он списком или строкой.

    Старые карточки иногда сохраняли stage_form2_text как одну строку с переносами,
    а прежний код проходил по ней посимвольно. Из-за этого готовые «49 лет / Менее
    года» терялись и появлялось ложное предупреждение о неполном стаже.
    """
    values = value if isinstance(value, (list, tuple)) else [value]
    lines = []
    for item in values:
        if item is None:
            continue
        for part in re.split(r'[\r\n]+', str(item)):
            text = re.sub(r'\s+', ' ', part).strip()
            if text not in _STAGE_DASHES:
                lines.append(text)
    return lines


def _preserve_stage_reference(person: dict) -> None:
    """Keep completed Form 2 values only as a comparison/reference.

    They are never treated as the calculated experience. The backend calculates
    from employment_periods extracted from the labour book.
    """
    lines = _stage_text_lines(person.get('stage_form2_text'))
    combined = _stage_text_lines(person.get('stage_years'))
    if not lines and combined:
        lines = combined
    if not person.get('stage_reference_total') and lines:
        person['stage_reference_total'] = lines[0]
    if not person.get('stage_reference_here') and len(lines) > 1:
        person['stage_reference_here'] = lines[1]
    if str(person.get('stage_source') or '').lower() in ('document', 'document_reference'):
        # Old cards stored Form 2 text in the live result fields. Move it away so
        # calculate_person_experience() cannot mistake it for a fresh calculation.
        if not person.get('stage_reference_total') and person.get('stage_years'):
            person['stage_reference_total'] = person.get('stage_years')
        if not person.get('stage_reference_here') and person.get('stage_years_here'):
            person['stage_reference_here'] = person.get('stage_years_here')
        person['stage_years'] = ''
        person['stage_years_here'] = ''
        person['stage_is_final'] = False


# ═══════════════════ Адаптер для реального пайплайна ═══════════════════
def generate_company_attestation_package_v2(company: dict, attestation_data: dict,
                                             api_key=None, vibe_call_fn=None, progress_cb=None) -> dict:
    from generator_company_att import (
        resolve_work_items, render_work_items_lines, calculate_person_experience, resolve_workers,
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
    try:
        from company_attestation_source_parser import merge_itr_records
        itr_list = merge_itr_records(attestation_data.get('itr') or [])
    except Exception:
        itr_list = [dict(p) for p in (attestation_data.get('itr') or [])]
    work_items = resolve_work_items(attestation_data)
    workers = resolve_workers(attestation_data, work_items)
    as_of_date = attestation_data.get('as_of_date') or datetime.now().strftime('%d.%m.%Y')
    attestation_data['as_of_date'] = as_of_date

    for person in itr_list:
        _preserve_stage_reference(person)
        calculate_person_experience(person, company, as_of_date=as_of_date)

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
    def _valid_stage(value):
        text = str(value or '').strip()
        return bool(text) and text not in ('—', '-', '–')

    missing_stage = [p.get('fio', '?') for p in itr_list
                     if not _valid_stage(p.get('stage_years')) or not _valid_stage(p.get('stage_years_here'))]
    if missing_stage:
        warnings.append('Не удалось полностью рассчитать стаж по трудовой: ' + ', '.join(missing_stage) + '. Жёлтые поля требуют проверки.')
    for person in itr_list:
        if person.get('stage_needs_review'):
            reasons = '; '.join(person.get('stage_review_reasons') or [])
            warnings.append(f"Стаж {person.get('fio', '?')} требует проверки: {reasons or 'есть неуверенные периоды'}. Поле выделено жёлтым.")
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
        for doc in docs:
            doc['bytes'] = highlight_review_tokens(doc['bytes'])
        return {'docs': docs, 'warnings': warnings}

    work_lines = render_work_items_lines(work_items)
    flat = _flat_work_items()
    work_scope_text = ', '.join(flat.get(code, code) for code in work_items)

    progress('1. Заявление')
    docs.append({
        'name': f'{org} - 1. Заявление.docx',
        'bytes': render_zayavlenie(company, work_lines, category=category, as_of_date=as_of_date, review=attestation_data),
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
        objects = attestation_data.get('experience_objects') or [{
            'name': 'ТРЕБУЕТ УТОЧНЕНИЯ: объект',
            'complexity_class': 'ТРЕБУЕТ УТОЧНЕНИЯ: класс сложности',
        }]
        docs.append({
            'name': f'{org} - 6. Форма №6 Опыт.docx',
            'bytes': gen_form6_opyt(company, objects),
        })

    for doc in docs:
        doc['bytes'] = highlight_review_tokens(doc['bytes'])
    return {'docs': docs, 'warnings': list(dict.fromkeys(warnings))}
