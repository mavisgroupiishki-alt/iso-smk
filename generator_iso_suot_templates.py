"""Fast deterministic ISO 9001 / SUOT package renderer.

V15 priorities:
- never leave sample-company details (Varta) in any Word XML part;
- use the CURRENT certification scope everywhere, not the scope from the sample;
- use the real staff list to choose job descriptions / worker OT instructions;
- build internal-audit programs from the actual ITR positions;
- avoid the slow legacy AI-per-document fallback path.

The module intentionally keeps the original DOCX templates for the large stable body
of the package, but replaces staff-dependent documents with deterministic documents
created from the current card. This keeps generation in seconds rather than tens of
minutes and prevents irrelevant roles/professions from leaking into the package.
"""
from __future__ import annotations

import io
import json as _json
import re
import zipfile
from pathlib import Path
from typing import Iterable

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Pt

BASE_DIR = Path(__file__).parent.resolve()
TPL_DIR = BASE_DIR / 'iso_suot_templates'


def _load_parts(filename: str) -> dict:
    path = TPL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Не найден шаблон {path}.")
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


def _esc(s) -> str:
    return (str(s) if s not in (None, '') else '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _xml_plain_text(fragment: str) -> str:
    parts = re.findall(r'<w:t(?:\s[^>]*)?>(.*?)</w:t>', fragment, flags=re.S)
    text = ''.join(re.sub(r'<[^>]+>', '', p) for p in parts)
    return (text.replace('&amp;', '&').replace('&quot;', '"')
                .replace('&lt;', '<').replace('&gt;', '>'))


def _replace_para_text(para_xml: str, new_text: str) -> str:
    """Replace visible text of a paragraph while preserving its paragraph/run style."""
    runs = list(re.finditer(r'<w:r(?:\s[^>]*)?>.*?</w:r>', para_xml, flags=re.S))
    if not runs:
        return para_xml
    first = runs[0].group(0)
    # keep the first run's rPr if any
    m = re.match(r'(<w:r(?:\s[^>]*)?>)(.*?)(</w:r>)$', first, flags=re.S)
    if not m:
        return para_xml
    open_run, body, close_run = m.groups()
    rpr = ''
    rpr_m = re.search(r'<w:rPr(?:\s[^>]*)?>.*?</w:rPr>', body, flags=re.S)
    if rpr_m:
        rpr = rpr_m.group(0)
    new_run = f'{open_run}{rpr}<w:t xml:space="preserve">{_esc(new_text)}</w:t>{close_run}'
    start = runs[0].start()
    end = runs[-1].end()
    return para_xml[:start] + new_run + para_xml[end:]


# ---------------------------------------------------------------------------
# Sample data embedded in the template library
# ---------------------------------------------------------------------------
_MANIFEST_PATH = TPL_DIR / 'manifest.json'
_MANIFEST = _json.loads(_MANIFEST_PATH.read_text('utf-8')) if _MANIFEST_PATH.exists() else {}

_OLD_COMPANY = {
    'name': 'Варта', 'city': 'г. Лида', 'unp': '500381571',
    'street': 'ул. Лётная, 7', 'bank_account': 'BY69AKBB30120000301344200000',
    'bank_name': 'АСБ «Беларусбанк»', 'postal_code': '231282', 'region': 'Гродненская обл.',
}

# This is the long scope hard-coded into the Varta templates. Match it at paragraph
# text level because Word often splits it across many XML runs.
_OLD_SCOPE_RE = re.compile(
    r'(?:производств(?:о|а)\s+)?строительно-монтажных работ\s*\('
    r'монтаж внутренних систем электроснабжения.*?'
    r'монтажа, наладки и технического обслуживания систем противодымной вентиляции',
    flags=re.I | re.S,
)

_SUOT_CONVERTED_IDX = set(range(1, 17))

# These old people occur in staff-dependent sample tables. When a matching current
# role is absent we replace the person with a yellow-review marker later instead of
# silently keeping the old employee.
_SAMPLE_ROLE_PEOPLE = {
    'Юхно Наталья Владимировна': ('бухгалтер',),
    'Банцевич Елена Генриковна': ('смет', 'договор'),
    'Прокопчик Виктор Владимирович': ('главный инженер', 'гл. инженер'),
    'Почебыт Евгений Николаевич': ('производитель работ', 'прораб'),
    'Юхновский Сергей Александрович': ('проектировщик',),
}

# Job descriptions in the original library. Only applicable descriptions are kept.
_ITR_TEMPLATE_RULES = {
    'instrukciya_4.docx': ('директор',),
    'instrukciya_5.docx': ('производитель работ', 'прораб'),
    'instrukciya_1.docx': ('главный бухгалтер',),
    'instrukciya_2.docx': ('главный специалист',),
    'instrukciya_3.docx': ('проектировщик',),
    'instrukciya_6.docx': ('смет', 'договор'),
    'converted_17.docx': ('главный инженер', 'гл. инженер'),
    'converted_18.docx': ('охране труда', 'охрана труда'),
}

# Profession-specific OT instructions that really exist in the library.
_WORKER_TEMPLATE_RULES = {
    'converted_2.docx': ('слесарь по ремонту', 'вентиляц', 'кондиционир'),
    'converted_3.docx': ('радиомеханик',),
    'converted_4.docx': ('слесарь-сантехник', 'сантехник'),
    'converted_5.docx': ('техник по связи',),
    'converted_6.docx': ('электромонтер опс', 'электромонтёр опс'),
    'converted_7.docx': ('телекоммуникац',),
    'converted_8.docx': ('наладке и испытаниям опс',),
    'converted_10.docx': ('радиомеханик',),
    'converted_11.docx': ('слесарь по ремонту', 'вентиляц', 'кондиционир'),
    'converted_12.docx': ('наладке и испытаниям опс',),
    'converted_13.docx': ('телекоммуникац',),
    'converted_14.docx': ('слесарь-сантехник', 'сантехник'),
    'converted_15.docx': ('техник по связи',),
    'converted_16.docx': ('электромонтер опс', 'электромонтёр опс'),
}

# Staff-specific SUOT risk cards. Do not include a card for a role absent from the
# current staffing schedule.
_SUOT_RISK_ROLE_RULES = {
    'suot_risk_3.docx': ('главный бухгалтер',),
    'suot_risk_4.docx': ('главный инженер', 'гл. инженер'),
    'suot_risk_5.docx': ('производитель работ', 'прораб'),
}

# Documents that are rebuilt from current staff rather than copied from Varta.
_DYNAMIC_KEYS = {
    # ISO staff/scope dependent
    'smk_doc_1.docx',            # policy + old familiarisation list
    'smk_doc_6.docx',            # familiarisation with goals
    'smk_doc_9.docx',            # training protocol
    'smk_doc_10.docx',           # training program
    'smk_doc_11.docx',           # internal audit program
    'kartochka_1.docx', 'kartochka_2.docx', 'kartochka_3.docx',  # rebuild from actual suppliers
    # SUOT staff dependent
    'suot_root_4.docx',          # familiarisation with policy
    'suot_root_6.docx',          # familiarisation with goals
    'suot_perechen_5.docx',      # ITR knowledge list
    'suot_perechen_6.docx',      # worker knowledge list
    'suot_instr_6.docx',         # list of OT instructions
    'suot_znaniya_4.docx',       # knowledge protocol sample people
    'suot_audit_1.docx', 'suot_audit_2.docx', 'suot_audit_3.docx',
    'suot_audit_4.docx', 'suot_audit_5.docx',
}


def _category_of(key: str) -> str:
    if key.startswith('suot_'):
        return 'suot'
    if key.startswith('converted_'):
        idx = int(key.split('_')[1].split('.')[0])
        return 'suot' if idx in _SUOT_CONVERTED_IDX else 'iso'
    # These are explicitly occupational-safety instructions, not ISO 9001 docs.
    if key in {'instrukciya_7.docx', 'instrukciya_8.docx', 'instrukciya_9.docx', 'instrukciya_10.docx'}:
        return 'suot'
    return 'iso'


def _clean_org_name(company: dict) -> str:
    raw = str(company.get('name') or '').strip()
    clean = re.sub(r'^(ООО|ОДО|ЧУП|ЗАО|РУП|ИП|ЧТУП|ЧТУ|ОАО|ЧП)\s*[«"\']?\s*', '', raw, flags=re.I)
    return clean.strip().strip('»"\'') or raw or 'ТРЕБУЕТ УТОЧНЕНИЯ'


def _full_org(company: dict) -> str:
    form = str(company.get('form') or '').strip() or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    return f'{form} «{_clean_org_name(company)}»'


def _full_legal_org(company: dict) -> str:
    form = str(company.get('form') or '').strip().upper()
    legal = {
        'ООО': 'Общество с ограниченной ответственностью',
        'ОДО': 'Общество с дополнительной ответственностью',
        'ОАО': 'Открытое акционерное общество',
        'ЗАО': 'Закрытое акционерное общество',
        'ЧУП': 'Частное унитарное предприятие',
        'ИП': 'Индивидуальный предприниматель',
    }.get(form, str(company.get('form') or 'ТРЕБУЕТ УТОЧНЕНИЯ'))
    return f'{legal} «{_clean_org_name(company)}»'


def _initials(fio: str) -> str:
    parts = str(fio or '').strip().split()
    if len(parts) >= 3:
        return f'{parts[1][0]}.{parts[2][0]}. {parts[0]}'
    if len(parts) == 2:
        return f'{parts[1][0]}. {parts[0]}'
    return fio or 'ТРЕБУЕТ УТОЧНЕНИЯ'


def _norm(value: str) -> str:
    return re.sub(r'\s+', ' ', str(value or '').lower().replace('ё', 'е')).strip()


def _find_person(people: Iterable[dict], keywords: Iterable[str]) -> dict | None:
    kws = tuple(_norm(k) for k in keywords)
    for person in people or []:
        pos = _norm(person.get('position'))
        if any(k in pos for k in kws):
            return person
    return None


def _actual_staff_name_for_sample(sample_name: str, itr: list) -> str:
    person = _find_person(itr, _SAMPLE_ROLE_PEOPLE.get(sample_name, ()))
    return str((person or {}).get('fio') or 'ТРЕБУЕТ УТОЧНЕНИЯ')


def _date_map(dates: dict) -> dict:
    year = str(dates.get('year') or '')
    return {
        '13.04.2026': str(dates.get('goals') or dates.get('policy') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '15.05.2026': str(dates.get('reports') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '27.04.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '11.05.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '17.06.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '27.07.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '11.08.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '15.09.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '14.10.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '11.11.2026': str(dates.get('audit') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
        '2026': year or '2026',
    }


def _rewrite_paragraph_text(text: str, company: dict, scope_text: str, dates: dict) -> str:
    current = text
    # Replace complete company expression so non-ООО forms do not inherit the sample form.
    current = re.sub(r'Общество\s+с\s+ограниченной\s+ответственностью\s*[«"]\s*Варта\s*[»"]',
                     _full_legal_org(company), current, flags=re.I)
    current = re.sub(r'ООО\s*[«"]\s*Варта\s*[»"]', _full_org(company), current, flags=re.I)
    current = current.replace('Варта', _clean_org_name(company))
    # Placeholders still present in two management manuals must be resolved in the
    # actual generated file, not merely highlighted by the post-review layer.
    current = current.replace('[Наименование]', _clean_org_name(company))
    current = current.replace('[Форма_собственности_сокращенная]', str(company.get('form') or 'ТРЕБУЕТ УТОЧНЕНИЯ'))
    current = current.replace('[Форма собственности сокращенная]', str(company.get('form') or 'ТРЕБУЕТ УТОЧНЕНИЯ'))
    if scope_text:
        current = _OLD_SCOPE_RE.sub(scope_text, current)
    # Source-information forms must reflect the ACTUAL staffing schedule.  Never
    # keep Varta's fixed 17/4-person figures.
    staff_total = company.get('_staff_total')
    itr_total = company.get('_itr_total')
    if isinstance(staff_total, int):
        current = re.sub(
            r'(численность\s+персонала,?\s+работающего\s+в\s+организации\s*[–—-]\s*)\d+\s*(?:человек|чел\.?)',
            lambda m: m.group(1) + f'{staff_total} чел.', current, flags=re.I
        )
        current = re.sub(
            r'(численность\s+работающих,?[^;\n]{0,160}?область\s+применения\s+СМК\s*[–—-]\s*)\d+\s*(?:человек|чел\.?)',
            lambda m: m.group(1) + f'{staff_total} чел.', current, flags=re.I
        )
    if isinstance(itr_total, int):
        current = re.sub(
            r'(численность\s+инженерно-технических\s+работников[^;\n]{0,100}?[–—-]\s*)\d+\s*(?:человек|чел\.?)',
            lambda m: m.group(1) + f'{itr_total} чел.', current, flags=re.I
        )
    part_time_total = company.get('_part_time_total')
    if isinstance(part_time_total, int):
        current = re.sub(
            r'(наличие\s+и\s+количество\s+производственного\s+персонала\s+с\s+неполной\s+занятостью[^;\n]{0,180}?[–—-]\s*)\d+\s*(?:человек|чел\.?)',
            lambda m: m.group(1) + f'{part_time_total} чел.', current, flags=re.I
        )
    for old, new in _date_map(dates).items():
        if old and new:
            current = current.replace(old, new)
    return current


def render_generic(template_file: str, company_old: dict, company_new: dict,
                    people_map: dict, extra_replacements: dict | None = None,
                    *, company: dict | None = None, scope_text: str = '', dates: dict | None = None) -> bytes:
    """Render a template across document + headers/footers + notes.

    Missing new requisites NEVER leave the old Varta value: they become
    ``ТРЕБУЕТ УТОЧНЕНИЯ`` and the universal DOCX review layer highlights them yellow.
    """
    parts = _load_parts(template_file)
    current_company = company or {}
    dates = dates or {}

    for part_name, payload in list(parts.items()):
        if not (part_name.startswith('word/') and part_name.endswith('.xml')):
            continue
        try:
            xml = payload.decode('utf-8')
        except UnicodeDecodeError:
            continue

        # Raw XML replacements for simple scalar values.
        if isinstance(company_old, dict):
            for key in ['name', 'city', 'unp', 'address', 'street', 'bank_account', 'bank_name', 'postal_code', 'region']:
                old_val = str(company_old.get(key) or '')
                if not old_val:
                    continue
                new_val = str(company_new.get(key) or '').strip() or 'ТРЕБУЕТ УТОЧНЕНИЯ'
                xml = xml.replace(_esc(old_val), _esc(new_val)).replace(old_val, _esc(new_val))

        for old_name, new_name in (people_map or {}).items():
            replacement = str(new_name or 'ТРЕБУЕТ УТОЧНЕНИЯ')
            xml = xml.replace(_esc(old_name), _esc(replacement)).replace(old_name, _esc(replacement))

        for old_str, new_str in (extra_replacements or {}).items():
            xml = xml.replace(_esc(old_str), _esc(new_str)).replace(old_str, _esc(new_str))

        # Paragraph-level rewrite catches text split over several Word runs.
        def repl_para(match: re.Match) -> str:
            para = match.group(0)
            visible = _xml_plain_text(para)
            if not visible:
                return para
            new_visible = _rewrite_paragraph_text(visible, current_company, scope_text, dates)
            return _replace_para_text(para, new_visible) if new_visible != visible else para

        xml = re.sub(r'<w:p(?:\s[^>]*)?>.*?</w:p>', repl_para, xml, flags=re.S)
        parts[part_name] = xml.encode('utf-8')

    return _rebuild(parts)


# ---------------------------------------------------------------------------
# Dynamic staff-dependent documents
# ---------------------------------------------------------------------------
def _new_doc(company: dict, title: str, dates: dict, date_key: str = 'goals') -> Document:
    doc = Document()
    normal = doc.styles['Normal']
    normal.font.name = 'Times New Roman'
    normal.font.size = Pt(11)

    org = _full_org(company)
    director = str(company.get('director_fio') or 'ТРЕБУЕТ УТОЧНЕНИЯ')
    director_pos = str(company.get('director_position') or 'Директор')
    date = str(dates.get(date_key) or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ')

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p.add_run('УТВЕРЖДАЮ\n').bold = True
    p.add_run(f'{director_pos}\n{org}\n_____________ {_initials(director)}\n{date} г.')
    h = doc.add_paragraph()
    h.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = h.add_run(title)
    r.bold = True
    r.font.size = Pt(14)
    return doc


def _doc_bytes(doc: Document) -> bytes:
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


def _ensure_scope_in_report(data: bytes, scope: str) -> bytes:
    """Make the current certification scope explicit in every report.

    Some legacy SUOT reports never had a scope field at all, so simple replacement
    cannot update them. Insert one directly below the report heading.
    """
    scope = str(scope or '').strip() or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    doc = Document(io.BytesIO(data))
    all_text = '\n'.join(p.text for p in doc.paragraphs)
    if scope.lower() not in all_text.lower():
        inserted = False
        for para in doc.paragraphs:
            upper = para.text.upper().replace('Ё','Е')
            if 'ОТЧЕТ' in upper or 'ОТЧЁТ' in para.text.upper():
                new_p = doc.add_paragraph(f'Область применения: {scope}')
                para._p.addnext(new_p._p)
                inserted = True
                break
        if not inserted:
            new_p = doc.add_paragraph(f'Область применения: {scope}')
            if doc.paragraphs:
                doc.paragraphs[0]._p.addnext(new_p._p)
    return _doc_bytes(doc)


def _add_staff_table(doc: Document, people: list, date: str, include_position: bool = True):
    table = doc.add_table(rows=1, cols=4 if include_position else 3)
    table.style = 'Table Grid'
    headers = ['ФИО', 'Должность', 'Подпись', 'Дата'] if include_position else ['ФИО', 'Подпись', 'Дата']
    for i, text in enumerate(headers):
        table.rows[0].cells[i].text = text
    for person in people:
        cells = table.add_row().cells
        values = [str(person.get('fio') or 'ТРЕБУЕТ УТОЧНЕНИЯ')]
        if include_position:
            values.append(str(person.get('position') or 'ТРЕБУЕТ УТОЧНЕНИЯ'))
        values.extend(['', str(person.get('hire_date') or date)])
        for i, value in enumerate(values):
            cells[i].text = value


def _iso_policy_doc(company: dict, scope: str, itr: list, dates: dict) -> bytes:
    doc = _new_doc(company, 'ПОЛИТИКА В ОБЛАСТИ КАЧЕСТВА', dates, 'policy')
    scope_value = scope or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    for text in (
        f'Область применения системы менеджмента качества: {scope_value}.',
        'Организация принимает обязательства выполнять применимые требования, повышать удовлетворённость заказчиков, поддерживать компетентность персонала и постоянно улучшать результативность СМК.',
        'Политика доводится до работников организации и пересматривается при изменении области деятельности или существенных условий работы.',
    ):
        doc.add_paragraph(text)
    doc.add_paragraph('Лист ознакомления работников с Политикой:')
    _add_staff_table(doc, itr, str(dates.get('policy') or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'))
    return _doc_bytes(doc)


def _awareness_doc(company: dict, title: str, people: list, dates: dict, date_key='goals') -> bytes:
    doc = _new_doc(company, title, dates, date_key)
    _add_staff_table(doc, people, str(dates.get(date_key) or dates.get('goals') or 'ТРЕБУЕТ УТОЧНЕНИЯ'))
    return _doc_bytes(doc)


def _role_audit_points_iso(position: str) -> str:
    p = _norm(position)
    if 'директор' in p and 'замест' not in p:
        return 'п.п. 4.1–4.4, 5.1–5.3, 6.1–6.3, 9.1–9.3, 10.1–10.3'
    if 'охране труда' in p:
        return 'п.п. 6.1, 7.1.4, 7.2–7.5, 8.1, 10.2'
    if 'главный инженер' in p or 'гл. инженер' in p:
        return 'п.п. 6.1, 7.1.3, 7.1.5, 8.1, 8.5, 9.1'
    if 'производитель работ' in p or 'прораб' in p or 'мастер' in p:
        return 'п.п. 7.1–7.5, 8.1, 8.5–8.7, 9.1, 10.2'
    if 'смет' in p or 'договор' in p:
        return 'п.п. 8.2, 8.4, 9.1'
    if 'проектиров' in p:
        return 'п.п. 7.5, 8.3, 8.5, 9.1'
    if 'бухгалтер' in p:
        return 'п.п. 7.1.1, 7.1.2, 7.5, 9.1.3'
    if 'кадр' in p or 'персонал' in p:
        return 'п.п. 7.2, 7.3, 7.5, 9.1'
    return 'п.п. 7.2, 7.5, 8.1, 9.1 — ТРЕБУЕТ УТОЧНЕНИЯ для данной должности'


def _role_audit_points_suot(position: str) -> str:
    p = _norm(position)
    if 'директор' in p and 'замест' not in p:
        return 'п.п. 4.1–4.4, 5.1–5.4, 6.1–6.2, 9.1–9.3, 10.1–10.3'
    if 'охране труда' in p:
        return 'п.п. 6.1–6.2, 7.2–7.5, 8.1, 9.1, 10.2'
    if 'главный инженер' in p or 'гл. инженер' in p or 'производитель работ' in p or 'прораб' in p:
        return 'п.п. 6.1.2, 7.2–7.4, 8.1, 8.1.2, 9.1, 10.2'
    return 'п.п. 7.2–7.5, 8.1, 9.1 — ТРЕБУЕТ УТОЧНЕНИЯ для данной должности'


def _audit_program_doc(company: dict, itr: list, dates: dict, standard: str) -> bytes:
    is_suot = standard == 'suot'
    title = ('ПРОГРАММА ПРОВЕДЕНИЯ ВНУТРЕННИХ АУДИТОВ СУОТ'
             if is_suot else 'ПРОГРАММА ПРОВЕДЕНИЯ ВНУТРЕННИХ АУДИТОВ СМК')
    doc = _new_doc(company, f'{title} НА {dates.get("year") or "ТРЕБУЕТ УТОЧНЕНИЯ"} ГОД', dates, 'goals')
    doc.add_paragraph('Цель: систематическая оценка соответствия системы менеджмента установленным требованиям и внутренним документам организации.')
    table = doc.add_table(rows=1, cols=6)
    table.style = 'Table Grid'
    for i, h in enumerate(['№', 'Проверяемое должностное лицо', 'Критерии аудита', 'Срок', 'Аудитор', 'Отметка']):
        table.rows[0].cells[i].text = h
    auditors = [p for p in itr if p.get('ot_certificate')] or itr[:3]
    for idx, person in enumerate(itr, 1):
        row = table.add_row().cells
        auditor = auditors[(idx - 1) % len(auditors)] if auditors else {}
        points = _role_audit_points_suot(person.get('position', '')) if is_suot else _role_audit_points_iso(person.get('position', ''))
        values = [
            str(idx),
            f"{person.get('position') or 'ТРЕБУЕТ УТОЧНЕНИЯ'} — {person.get('fio') or 'ТРЕБУЕТ УТОЧНЕНИЯ'}",
            points,
            str(dates.get('audit') or dates.get('reports') or 'ТРЕБУЕТ УТОЧНЕНИЯ'),
            f"{auditor.get('position') or 'ТРЕБУЕТ УТОЧНЕНИЯ'} — {auditor.get('fio') or 'ТРЕБУЕТ УТОЧНЕНИЯ'}",
            '',
        ]
        for i, value in enumerate(values): row[i].text = value
    return _doc_bytes(doc)


def _training_doc(company: dict, people: list, dates: dict, standard: str, kind='protocol') -> bytes:
    label = 'СУОТ / ISO 45001' if standard == 'suot' else 'СМК / ISO 9001'
    title = ('ПРОТОКОЛ ВНУТРЕННЕГО ОБУЧЕНИЯ' if kind == 'protocol' else 'ПРОГРАММА ВНУТРЕННЕГО ОБУЧЕНИЯ') + f' — {label}'
    doc = _new_doc(company, title, dates, 'goals')
    doc.add_paragraph(f'Тема: документирование, требования и внутренний аудит {label}.')
    table = doc.add_table(rows=1, cols=4)
    table.style = 'Table Grid'
    for i, h in enumerate(['№', 'ФИО', 'Должность', 'Результат / отметка']): table.rows[0].cells[i].text = h
    for idx, person in enumerate(people, 1):
        row = table.add_row().cells
        vals = [str(idx), str(person.get('fio') or 'ТРЕБУЕТ УТОЧНЕНИЯ'), str(person.get('position') or 'ТРЕБУЕТ УТОЧНЕНИЯ'), '']
        for i, v in enumerate(vals): row[i].text = v
    return _doc_bytes(doc)


def _positions_list_doc(company: dict, people: list, dates: dict, workers=False) -> bytes:
    if workers:
        title = 'ПЕРЕЧЕНЬ ПРОФЕССИЙ РАБОЧИХ, КОТОРЫЕ ДОЛЖНЫ ПРОХОДИТЬ ПРОВЕРКУ ЗНАНИЙ ПО ВОПРОСАМ ОХРАНЫ ТРУДА'
        field = 'position'
    else:
        title = 'ПЕРЕЧЕНЬ ДОЛЖНОСТЕЙ РУКОВОДИТЕЛЕЙ И СПЕЦИАЛИСТОВ, КОТОРЫЕ ДОЛЖНЫ ПРОХОДИТЬ ПРОВЕРКУ ЗНАНИЙ ПО ВОПРОСАМ ОХРАНЫ ТРУДА'
        field = 'position'
    doc = _new_doc(company, title, dates, 'goals')
    table = doc.add_table(rows=1, cols=2)
    table.style = 'Table Grid'
    table.rows[0].cells[0].text = '№'
    table.rows[0].cells[1].text = 'Профессия' if workers else 'Должность'
    unique = []
    seen = set()
    for p in people:
        value = str(p.get(field) or '').strip()
        key = _norm(value)
        if value and key not in seen:
            seen.add(key); unique.append(value)
    if not unique:
        unique = ['ТРЕБУЕТ УТОЧНЕНИЯ']
    for idx, value in enumerate(unique, 1):
        row = table.add_row().cells; row[0].text = str(idx); row[1].text = value
    return _doc_bytes(doc)


def _generic_job_description(company: dict, position: str, dates: dict) -> bytes:
    doc = _new_doc(company, f'ДОЛЖНОСТНАЯ ИНСТРУКЦИЯ — {position or "ТРЕБУЕТ УТОЧНЕНИЯ"}', dates, 'goals')
    for heading, body in (
        ('1. Общие положения', f'Настоящая должностная инструкция определяет обязанности, права и ответственность работника по должности «{position or "ТРЕБУЕТ УТОЧНЕНИЯ"}».'),
        ('2. Должностные обязанности', 'Выполнять обязанности в пределах своей должности, соблюдать применимые требования СМК/СУОТ, внутренние регламенты и требования охраны труда. ТРЕБУЕТ УТОЧНЕНИЯ: дополнить профильными обязанностями по штатной должности.'),
        ('3. Права', 'Получать информацию и ресурсы, необходимые для выполнения обязанностей; вносить предложения по улучшению процессов и условий труда.'),
        ('4. Ответственность', 'Нести ответственность в пределах законодательства и локальных документов организации за ненадлежащее исполнение обязанностей.'),
    ):
        p = doc.add_paragraph(); p.add_run(heading).bold = True; doc.add_paragraph(body)
    return _doc_bytes(doc)


def _generic_worker_instruction(company: dict, profession: str, dates: dict) -> bytes:
    doc = _new_doc(company, f'ИНСТРУКЦИЯ ПО ОХРАНЕ ТРУДА ДЛЯ ПРОФЕССИИ «{profession or "ТРЕБУЕТ УТОЧНЕНИЯ"}»', dates, 'goals')
    sections = [
        ('1. Общие требования охраны труда', 'К работе допускаются работники, прошедшие предусмотренные законодательством обучение, инструктаж и проверку знаний, а также медицинский осмотр, когда он обязателен.'),
        ('2. Требования перед началом работы', 'Проверить рабочее место, инструмент, оборудование и средства индивидуальной защиты. ТРЕБУЕТ УТОЧНЕНИЯ: дополнить специфическими требованиями и опасностями для данной профессии.'),
        ('3. Требования во время работы', 'Соблюдать технологическую последовательность, требования инструкций изготовителей, ограждения и средства защиты. Не выполнять работы при выявленной опасности до её устранения.'),
        ('4. Аварийные ситуации', 'Прекратить работу, сообщить непосредственному руководителю, действовать согласно планам реагирования и инструкциям организации.'),
        ('5. После окончания работы', 'Отключить оборудование, привести рабочее место в безопасное состояние, убрать инструмент и сообщить о выявленных неисправностях.'),
    ]
    for h, b in sections:
        p = doc.add_paragraph(); p.add_run(h).bold = True; doc.add_paragraph(b)
    return _doc_bytes(doc)


def _ot_instruction_list_doc(company: dict, worker_professions: list[str], dates: dict) -> bytes:
    doc = _new_doc(company, 'ПЕРЕЧЕНЬ ИНСТРУКЦИЙ ПО ОХРАНЕ ТРУДА', dates, 'goals')
    general = [
        'Инструкция о проведении контроля за соблюдением законодательства об охране труда',
        'Общеобъектовая инструкция о мерах пожарной безопасности',
        'Инструкция по охране труда при работе с персональным компьютером',
        'Инструкция по оказанию первой помощи',
        'Инструкция действий работников при возникновении пожара',
        'Инструкция по безопасной эвакуации работников',
    ]
    rows = general + [f'Инструкция по охране труда для профессии «{p}»' for p in worker_professions]
    table = doc.add_table(rows=1, cols=3); table.style = 'Table Grid'
    for i, h in enumerate(['№', 'Наименование инструкции', 'Срок пересмотра']): table.rows[0].cells[i].text = h
    for idx, title in enumerate(rows, 1):
        row = table.add_row().cells; row[0].text = str(idx); row[1].text = title; row[2].text = '3 года'
    return _doc_bytes(doc)


def _supplier_card_doc(company: dict, supplier: dict, dates: dict, index: int) -> bytes:
    name = str((supplier or {}).get('name') or 'ТРЕБУЕТ УТОЧНЕНИЯ')
    supply_type = str((supplier or {}).get('type') or (supplier or {}).get('product') or 'ТРЕБУЕТ УТОЧНЕНИЯ')
    doc = _new_doc(company, f'КАРТОЧКА ОЦЕНКИ ПОСТАВЩИКА № {index}', dates, 'goals')
    doc.add_paragraph(f'Поставщик: {name}')
    doc.add_paragraph(f'Поставляемая продукция / работы: {supply_type}')
    table = doc.add_table(rows=1, cols=4)
    table.style = 'Table Grid'
    for i, h in enumerate(['Критерий', 'Оценка', 'Комментарий', 'Итог']): table.rows[0].cells[i].text = h
    criteria = ['Цена', 'Качество', 'Сроки поставки', 'Условия оплаты', 'Опыт работы с поставщиком']
    for criterion in criteria:
        row = table.add_row().cells
        row[0].text = criterion
        row[1].text = 'ТРЕБУЕТ УТОЧНЕНИЯ'
        row[2].text = ''
        row[3].text = ''
    doc.add_paragraph('Решение о приемлемости поставщика: ТРЕБУЕТ УТОЧНЕНИЯ')
    return _doc_bytes(doc)


def _matching_template(position: str, rules: dict) -> str | None:
    p = _norm(position)
    for key, keywords in rules.items():
        if any(_norm(k) in p for k in keywords):
            return key
    return None


def _actual_worker_rows(workers: list) -> list[dict]:
    result = []
    seen = set()
    for w in workers or []:
        if isinstance(w, str):
            position = w
            row = {'fio': '', 'position': position, 'is_worker': True}
        else:
            row = dict(w or {})
            position = row.get('position') or row.get('profession') or row.get('name') or ''
            row['position'] = position
        key = _norm(position)
        if position and key not in seen:
            seen.add(key); result.append(row)
    return result



# ---------------------------------------------------------------------------
# Safe execution of learned document-text rules
# ---------------------------------------------------------------------------
def _knowledge_rule_text(rule: dict) -> str:
    chunks = [str(rule.get('instruction') or '')]
    chunks.extend(str(x) for x in (rule.get('actions') or []) if x)
    return '\n'.join(x for x in chunks if x).strip()


def _knowledge_replacements(company: dict, rules: list | None) -> list[tuple[str, str]]:
    """Translate only safe, explicit learned rules into DOCX text replacements.

    Free-form learning still influences data extraction through the AI context, but
    arbitrary prose must not silently rewrite legal/technical documents.  Here we
    execute only deterministic operations we can verify: explicit ``replace X with
    Y`` rules and the common rule to remove a sample-company name in favour of the
    current organisation.
    """
    replacements: list[tuple[str, str]] = []
    current_clean = _clean_org_name(company)
    current_full = _full_org(company)
    stale_names = ('Варта', 'Кастом-Инвест', 'Сфера Секьюрити', 'МонТехБел')

    for rule in rules or []:
        if not isinstance(rule, dict) or not rule.get('active', True):
            continue
        text = _knowledge_rule_text(rule)
        if not text:
            continue
        low = _norm(text)

        # "Везде должна быть текущая организация", "не использовать Варту" etc.
        if any(word in low for word in ('текущая организац', 'актуальная организац',
                                        'организац из шаблон', 'название из шаблон',
                                        'не использовать варта', 'убрать варта',
                                        'удалить варта')):
            for stale in stale_names:
                replacements.append((f'ООО «{stale}»', current_full))
                replacements.append((f'ООО "{stale}"', current_full))
                replacements.append((stale, current_clean))

        # Quoted explicit replacements are the safest and preferred syntax.
        quoted = re.compile(
            r'замен(?:ить|и|ять)\s+[«"“„\']([^»"”“\']{1,160})[»"”“\']\s+'
            r'(?:на|→|->)\s+[«"“„\']([^»"”“\']{1,240})[»"”“\']',
            flags=re.I,
        )
        for match in quoted.finditer(text):
            old, new = match.group(1).strip(), match.group(2).strip()
            if old and new and old != new:
                replacements.append((old, new))

        # Also allow a short unquoted command on one line: "заменить X на Y".
        simple = re.compile(
            r'(?im)^\s*замен(?:ить|и|ять)\s+(.{1,100}?)\s+(?:на|→|->)\s+(.{1,160}?)\s*[.;]?$'
        )
        for match in simple.finditer(text):
            old, new = match.group(1).strip(' «»"\''), match.group(2).strip(' «»"\'')
            if old and new and old != new and '\n' not in old and '\n' not in new:
                replacements.append((old, new))

    # Stable order, no duplicates.
    out = []
    seen = set()
    for old, new in replacements:
        key = (old, new)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _apply_text_replacements_to_docx(data: bytes, replacements: list[tuple[str, str]]) -> bytes:
    if not replacements:
        return data
    try:
        parts = {}
        with zipfile.ZipFile(io.BytesIO(data), 'r') as src:
            for name in src.namelist():
                parts[name] = src.read(name)
    except zipfile.BadZipFile:
        return data

    for part_name, payload in list(parts.items()):
        if not (part_name.startswith('word/') and part_name.endswith('.xml')):
            continue
        try:
            xml = payload.decode('utf-8')
        except UnicodeDecodeError:
            continue

        # Direct XML replacement handles headers/footers/text boxes when the text
        # is stored in one XML text node.
        for old, new in replacements:
            xml = xml.replace(_esc(old), _esc(new)).replace(old, _esc(new))

        # Paragraph rewrite also handles values split across multiple Word runs.
        def repl_para(match: re.Match) -> str:
            para = match.group(0)
            visible = _xml_plain_text(para)
            if not visible:
                return para
            changed = visible
            for old, new in replacements:
                changed = changed.replace(old, new)
            return _replace_para_text(para, changed) if changed != visible else para

        xml = re.sub(r'<w:p(?:\s[^>]*)?>.*?</w:p>', repl_para, xml, flags=re.S)
        parts[part_name] = xml.encode('utf-8')
    return _rebuild(parts)


def generate_iso_suot_package_v2(company: dict, itr: list, dates: dict, resp: dict,
                                  product: str = 'iso_suot', progress_cb=None,
                                  workers: list | None = None, objects: list | None = None,
                                  suppliers: list | None = None, iso_suot: dict | None = None,
                                  knowledge_text: str = '', knowledge_rules: list | None = None) -> dict:
    """Generate ISO/SUOT package in seconds using current card data only."""
    org = _clean_org_name(company)
    scope = str(company.get('scope') or '').strip()
    workers_rows = _actual_worker_rows(workers or [])

    director = (resp.get('director') or {}) if isinstance(resp, dict) else {}
    director_fio = str(company.get('director_fio') or director.get('fio') or '').strip()
    dir_parts = director_fio.split()
    dir_surname = dir_parts[0] if dir_parts else ''
    dir_initials = ('.'.join(p[0] for p in dir_parts[1:] if p) + '.') if len(dir_parts) > 1 else ''

    # Prefer semantic roles instead of "the next two people in the list".
    process_person = (resp.get('process_resp') or {}) if isinstance(resp, dict) else {}
    fnpa_person = (resp.get('fnpa_resp') or {}) if isinstance(resp, dict) else {}
    process_parts = str(process_person.get('fio') or '').split()
    fnpa_parts = str(fnpa_person.get('fio') or '').split()

    def surname(parts): return parts[0] if parts else 'ТРЕБУЕТ УТОЧНЕНИЯ'
    def inits(parts): return ('.'.join(x[0] for x in parts[1:] if x) + '.') if len(parts) > 1 else 'ТРЕБУЕТ УТОЧНЕНИЯ'

    render_company = dict(company or {})
    all_staff_rows = list(itr or []) + list(workers_rows)
    def _part_time(person):
        text = ' '.join(str(person.get(k) or '') for k in ('employment_type','work_type','schedule','notes')).lower().replace('ё','е')
        return bool(person.get('part_time')) or 'совмест' in text or 'непол' in text or 'договор подряда' in text
    render_company['_staff_total'] = len(all_staff_rows)
    render_company['_itr_total'] = len(itr or [])
    render_company['_worker_total'] = len(workers_rows)
    render_company['_part_time_total'] = sum(1 for person in all_staff_rows if _part_time(person))

    company_new = {
        'name': org,
        'city': (f"г. {company.get('city','')}" if company.get('city') and not str(company.get('city','')).startswith('г.') else company.get('city','')),
        'unp': company.get('unp', ''),
        'street': company.get('address', ''),
        'bank_account': company.get('bank_account', ''),
        'bank_name': company.get('bank_name', ''),
        'postal_code': company.get('postal_code', ''),
        'region': company.get('region', ''),
    }

    people_map = {
        'Василенко': dir_surname or 'ТРЕБУЕТ УТОЧНЕНИЯ',
        'С.Ф.': dir_initials or 'ТРЕБУЕТ УТОЧНЕНИЯ',
        'Кормилицин': surname(process_parts), 'П.А.': inits(process_parts),
        'Вершалович': surname(fnpa_parts), 'А.П.': inits(fnpa_parts), 'А.М.': inits(fnpa_parts),
    }
    for old_full in _SAMPLE_ROLE_PEOPLE:
        people_map[old_full] = _actual_staff_name_for_sample(old_full, itr)

    wanted_categories = {'iso_suot': {'iso', 'suot'}, 'iso': {'iso'}, 'suot': {'suot'}}.get(product, {'iso', 'suot'})
    keys = sorted(k for k in _MANIFEST if _category_of(k) in wanted_categories)

    # Remove staff/profession templates that are not applicable. They are rebuilt below.
    applicable_itr_template_keys = set()
    for person in itr or []:
        tpl = _matching_template(person.get('position', ''), _ITR_TEMPLATE_RULES)
        if tpl:
            applicable_itr_template_keys.add(tpl)
    applicable_worker_template_keys = set()
    for person in workers_rows:
        tpl = _matching_template(person.get('position', ''), _WORKER_TEMPLATE_RULES)
        if tpl:
            applicable_worker_template_keys.add(tpl)

    filtered_keys = []
    for key in keys:
        if key == 'converted_1.docx':  # known corrupt pseudo-docx
            continue
        if key in _DYNAMIC_KEYS:
            continue
        if key in _ITR_TEMPLATE_RULES and key not in applicable_itr_template_keys:
            continue
        if key in _WORKER_TEMPLATE_RULES and key not in applicable_worker_template_keys:
            continue
        if key in _SUOT_RISK_ROLE_RULES and not _find_person(itr, _SUOT_RISK_ROLE_RULES[key]):
            continue
        if key == 'smk_doc_4.docx' and not bool(company.get('has_welding')):
            continue
        filtered_keys.append(key)

    docs = []
    warnings = []
    extra = _date_map(dates)
    total = len(filtered_keys) + 12 + len(itr or []) + len(workers_rows)
    done = 0

    def prog(message):
        nonlocal done
        done += 1
        if progress_cb:
            progress_cb(done, max(total, 1), message)

    for key in filtered_keys:
        friendly = re.sub(r'^\s*Варта\s*-\s*', '', str(_MANIFEST[key]), flags=re.I)
        category = _category_of(key)
        prefix = f"{org} СУОТ" if category == 'suot' else org
        out_name = f"{prefix} - {friendly}.docx"
        prog(friendly[:50])
        try:
            data = render_generic(
                key, _OLD_COMPANY, company_new, people_map, extra,
                company=render_company, scope_text=scope, dates=dates,
            )
            if 'отчет' in friendly.lower().replace('ё','е'):
                data = _ensure_scope_in_report(data, scope)
            docs.append({'name': out_name, 'bytes': data})
        except Exception as e:
            message = f"Не сформирован документ «{friendly}» ({key}): {type(e).__name__}: {e}"
            warnings.append(message)
            print(f"  ❌ {message}")

    # --- Deterministic dynamic ISO documents ---
    if 'iso' in wanted_categories:
        prog('Политика качества — актуальная область и штат')
        docs.append({'name': f'{org} - 1 Политика в области качества.docx', 'bytes': _iso_policy_doc(company, scope, itr, dates)})
        prog('Лист ознакомления с целями СМК')
        docs.append({'name': f'{org} - 2.2 Лист ознакомления с целями.docx', 'bytes': _awareness_doc(company, 'ЛИСТ ОЗНАКОМЛЕНИЯ С ЦЕЛЯМИ В ОБЛАСТИ КАЧЕСТВА', itr, dates)})
        prog('Протокол внутреннего обучения СМК')
        docs.append({'name': f'{org} - 3.9.2 Протокол внутреннего обучения СМК.docx', 'bytes': _training_doc(company, resp.get('auditors') or itr[:3], dates, 'iso', 'protocol')})
        prog('Программа внутреннего обучения СМК')
        docs.append({'name': f'{org} - 3.9.3 Программа внутреннего обучения СМК.docx', 'bytes': _training_doc(company, resp.get('auditors') or itr[:3], dates, 'iso', 'program')})
        prog('Программа внутренних аудитов СМК по фактическим ИТР')
        docs.append({'name': f'{org} - 4.1 Программа внутренних аудитов СМК.docx', 'bytes': _audit_program_doc(company, itr, dates, 'iso')})

        # Supplier evaluation cards must use the current supplier list, never the
        # unrelated suppliers from the sample company.
        supplier_rows = [dict(x) for x in (suppliers or []) if isinstance(x, dict)]
        if not supplier_rows:
            supplier_rows = [{'name':'ТРЕБУЕТ УТОЧНЕНИЯ','type':'ТРЕБУЕТ УТОЧНЕНИЯ'}]
        for idx, supplier in enumerate(supplier_rows, 1):
            prog(f'Карточка оценки поставщика: {supplier.get("name") or idx}')
            docs.append({'name': f'{org} - Карточка оценки поставщика {idx}.docx',
                         'bytes': _supplier_card_doc(company, supplier, dates, idx)})

        # Job descriptions for EVERY actual ITR. If no exact template exists, create
        # a generic draft and mark profile-specific duties for review.
        used_positions = set()
        for person in itr or []:
            position = str(person.get('position') or '').strip()
            key = _norm(position)
            if not position or key in used_positions:
                continue
            used_positions.add(key)
            if _matching_template(position, _ITR_TEMPLATE_RULES):
                # A matching original template is already present in docs.
                continue
            prog(f'Должностная инструкция: {position}')
            docs.append({'name': f'{org} - ДИ {position}.docx', 'bytes': _generic_job_description(company, position, dates)})

    # --- Deterministic dynamic SUOT documents ---
    if 'suot' in wanted_categories:
        all_people = list(itr or []) + workers_rows
        prog('Лист ознакомления с политикой СУОТ')
        docs.append({'name': f'{org} СУОТ - 2.2 Лист ознакомления с политикой.docx', 'bytes': _awareness_doc(company, 'ЛИСТ ОЗНАКОМЛЕНИЯ С ПОЛИТИКОЙ В ОБЛАСТИ ОХРАНЫ ТРУДА', all_people, dates, 'goals')})
        prog('Лист ознакомления с целями СУОТ')
        docs.append({'name': f'{org} СУОТ - 2.4 Лист ознакомления с целями.docx', 'bytes': _awareness_doc(company, 'ЛИСТ ОЗНАКОМЛЕНИЯ С ЦЕЛЯМИ В ОБЛАСТИ ОХРАНЫ ТРУДА', all_people, dates, 'goals')})
        prog('Перечень должностей ИТР для проверки знаний ОТ')
        docs.append({'name': f'{org} СУОТ - Перечень должностей ИТР для проверки знаний.docx', 'bytes': _positions_list_doc(company, itr, dates, workers=False)})
        prog('Перечень профессий рабочих для проверки знаний ОТ')
        docs.append({'name': f'{org} СУОТ - Перечень профессий рабочих для проверки знаний.docx', 'bytes': _positions_list_doc(company, workers_rows, dates, workers=True)})
        prog('Программа внутренних аудитов СУОТ по фактическим ИТР')
        docs.append({'name': f'{org} СУОТ - Программа внутренних аудитов.docx', 'bytes': _audit_program_doc(company, itr, dates, 'suot')})
        prog('Протокол внутреннего обучения СУОТ')
        docs.append({'name': f'{org} СУОТ - Протокол внутреннего обучения.docx', 'bytes': _training_doc(company, resp.get('auditors') or itr[:3], dates, 'suot', 'protocol')})

        worker_professions = [str(w.get('position') or '').strip() for w in workers_rows if str(w.get('position') or '').strip()]
        prog('Перечень инструкций ОТ по фактическому штату')
        docs.append({'name': f'{org} СУОТ - Перечень инструкций по ОТ.docx', 'bytes': _ot_instruction_list_doc(company, worker_professions, dates)})

        # Create instructions for professions not covered by a concrete template.
        for worker in workers_rows:
            profession = str(worker.get('position') or '').strip()
            if not profession or _matching_template(profession, _WORKER_TEMPLATE_RULES):
                continue
            prog(f'Инструкция ОТ: {profession}')
            docs.append({'name': f'{org} СУОТ - ИОТ {profession}.docx', 'bytes': _generic_worker_instruction(company, profession, dates)})

    # Execute the safe subset of active learned rules directly in generated Word
    # files.  This makes training observable: explicit text replacements and rules
    # about the current organisation affect the NEXT generated package, including
    # headers and footers. Other free-form rules still influence structured data
    # extraction but are never falsely reported as a completed template edit.
    learned_replacements = _knowledge_replacements(company, knowledge_rules)
    if learned_replacements:
        docs = [
            {**doc, 'bytes': _apply_text_replacements_to_docx(doc.get('bytes', b''), learned_replacements)}
            if str(doc.get('name') or '').lower().endswith('.docx') else doc
            for doc in docs
        ]

    return {'docs': docs, 'warnings': warnings}
