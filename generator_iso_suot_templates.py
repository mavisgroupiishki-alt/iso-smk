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
    parts = re.findall(r'<(?:w|a):t(?:\s[^>]*)?>(.*?)</(?:w|a):t>', fragment, flags=re.S)
    text = ''.join(re.sub(r'<[^>]+>', '', p) for p in parts)
    return (text.replace('&amp;', '&').replace('&quot;', '"')
                .replace('&lt;', '<').replace('&gt;', '>'))


def _replace_para_text(para_xml: str, new_text: str) -> str:
    """Replace visible text while preserving the first Word/DrawingML run style."""
    for prefix in ('w', 'a'):
        runs = list(re.finditer(rf'<{prefix}:r(?:\s[^>]*)?>.*?</{prefix}:r>', para_xml, flags=re.S))
        if not runs:
            continue
        first = runs[0].group(0)
        m = re.match(rf'(<{prefix}:r(?:\s[^>]*)?>)(.*?)(</{prefix}:r>)$', first, flags=re.S)
        if not m:
            continue
        open_run, body, close_run = m.groups()
        rpr = ''
        rpr_m = re.search(rf'<{prefix}:rPr(?:\s[^>]*)?>.*?</{prefix}:rPr>', body, flags=re.S)
        if rpr_m:
            rpr = rpr_m.group(0)
        tag = f'{prefix}:t'
        new_run = f'{open_run}{rpr}<{tag} xml:space="preserve">{_esc(new_text)}</{tag}>{close_run}'
        return para_xml[:runs[0].start()] + new_run + para_xml[runs[-1].end():]
    return para_xml


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
        xml = re.sub(r'<a:p(?:\s[^>]*)?>.*?</a:p>', repl_para, xml, flags=re.S)
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
    """Keep the report's scope exactly equal to the scope of THIS package.

    Do not allow a legacy/template phrase such as ``разработка проектной документации``
    to be appended unless it is explicitly present in the selected current scope.
    """
    scope = str(scope or '').strip() or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    doc = Document(io.BytesIO(data))

    # Project design is not implied by construction/installation work.  If the
    # current package scope does not explicitly request it, remove common variants
    # that old templates/models append on their own.
    scope_has_design = bool(re.search(r'проектн(?:ая|ой|ые|ых)\s+документац', scope, re.I))
    design_re = re.compile(
        r'(?:[,;:/\-–—]\s*)?(?:и\s+)?разработк(?:а|и|у|ой|е)\s+проектн(?:ой|ую|ая|ых|ыми)\s+документац(?:ии|ию|ией|иях)\b',
        re.I,
    )
    if not scope_has_design:
        for para in doc.paragraphs:
            if para.text and re.search(r'проектн(?:ая|ой|ые|ых)\s+документац', para.text, re.I):
                cleaned = design_re.sub('', para.text)
                cleaned = re.sub(r'\s{2,}', ' ', cleaned)
                cleaned = re.sub(r'\s+([,.;])', r'\1', cleaned)
                cleaned = re.sub(r'([:;])\s*$', r'\1', cleaned)
                if cleaned != para.text:
                    for run in para.runs:
                        run.text = ''
                    if para.runs:
                        para.runs[0].text = cleaned
                    else:
                        para.add_run(cleaned)

    all_text = '\n'.join(p.text for p in doc.paragraphs)
    if scope.lower() not in all_text.lower():
        inserted = False
        for para in doc.paragraphs:
            upper = para.text.upper().replace('Ё','Е')
            if 'ОТЧЕТ' in upper or 'ОТЧЁТ' in para.text.upper():
                # Scope is a visible part of the report heading, rather than a
                # footnote at the end.  This matters when one company has, for
                # example, CMR plus design work, or manufactures steel structures.
                new_p = doc.add_paragraph(f'В ОБЛАСТИ: {scope}')
                para._p.addnext(new_p._p)
                inserted = True
                break
        if not inserted:
            new_p = doc.add_paragraph(f'В ОБЛАСТИ: {scope}')
            if doc.paragraphs:
                doc.paragraphs[0]._p.addnext(new_p._p)
    return _doc_bytes(doc)


def _ensure_satisfaction_objects(data: bytes, objects: list | None) -> bytes:
    """Put the supplied project names into the customer-satisfaction report.

    The source template has a generic customer table.  Reusing or guessing its
    sample rows is unsafe: the report must identify the exact projects recorded in
    the company card.  Customers are shown only when supplied; a missing customer is
    deliberately marked for review instead of fabricated.
    """
    rows = []
    seen = set()
    for item in objects or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        key = (name, str(item.get('customer') or '').strip())
        if key in seen:
            continue
        seen.add(key)
        rows.append((name, key[1] or 'ТРЕБУЕТ УТОЧНЕНИЯ'))

    if not rows:
        return data

    doc = Document(io.BytesIO(data))
    table = next(
        (
            candidate for candidate in doc.tables
            if candidate.rows and candidate.columns
            and 'заказчик' in candidate.rows[0].cells[0].text.lower()
            and any('рекламац' in cell.text.lower() for cell in candidate.rows[0].cells[1:])
        ),
        None,
    )
    if table is None:
        # A future template may omit the standard table.  In that case add a
        # compact one rather than silently losing the supplied project list.
        table = doc.add_table(rows=1, cols=3)
        table.style = 'Table Grid'
        for cell, label in zip(table.rows[0].cells, ('Объект / заказчик', 'Рекламации', 'Оценка')):
            cell.text = label
    else:
        table.rows[0].cells[0].text = 'Объект / заказчик'
        # The template's rows belong to its sample company.  Remove them all
        # before inserting current objects, so no stale project can remain.
        for row in list(table.rows[1:]):
            row._tr.getparent().remove(row._tr)

    for name, customer in rows:
        cells = table.add_row().cells
        cells[0].text = f'{name}\n{customer}'
        cells[1].text = '—'
        cells[2].text = '5'
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



def _job_duties_for_position(position: str) -> list[str]:
    """Deterministic role-specific duties for ISO job descriptions.

    The goal is to avoid useless yellow placeholders such as
    "дополнить профильными обязанностями" when the exact staff position is known.
    These are operational baseline duties, not invented personal facts.
    """
    p = _norm(position)

    profiles = [
        (('экономист', 'финансов'), [
            'Формирует и актуализирует бюджеты, финансовые планы и план-факт анализ по направлениям деятельности организации.',
            'Анализирует доходы, расходы, себестоимость, рентабельность и причины отклонений финансово-экономических показателей.',
            'Готовит управленческую и экономическую отчётность, расчёты и аналитические материалы для руководителя.',
            'Участвует в планировании движения денежных средств, контроле платёжного календаря и финансовой дисциплины.',
            'Проверяет экономическую обоснованность договорных условий, цен, тарифов, смет и иных расчётов в пределах компетенции.',
            'Участвует в разработке мероприятий по снижению затрат и повышению эффективности использования ресурсов.',
            'Обеспечивает сохранность и актуальность финансово-экономических документов и исходных данных для расчётов.',
        ]),
        (('главный бухгалтер', 'бухгалтер'), [
            'Организует и ведёт бухгалтерский учёт хозяйственных операций, имущества и обязательств организации.',
            'Контролирует оформление и обработку первичных учётных документов.',
            'Обеспечивает своевременное формирование бухгалтерской, налоговой и иной обязательной отчётности.',
            'Контролирует расчёты с поставщиками, заказчиками, работниками, бюджетом и иными контрагентами.',
            'Участвует в контроле финансовой дисциплины, сохранности имущества и рационального использования ресурсов.',
            'Организует хранение бухгалтерских документов и данных в соответствии с установленными сроками.',
        ]),
        (('смет',), [
            'Подготавливает и проверяет сметную документацию и расчёты стоимости работ.',
            'Проверяет объёмы работ, расценки, коэффициенты и исходные данные, применяемые в сметах.',
            'Участвует в подготовке договорных цен, актов выполненных работ и расчётов по объектам.',
            'Анализирует изменения стоимости материалов, работ и ресурсов и отражает их в расчётах.',
            'Ведёт учёт и хранение сметной и расчётной документации.',
        ]),
        (('закуп', 'снабж', 'логист'), [
            'Формирует потребность в материалах, оборудовании и услугах по заявкам подразделений.',
            'Осуществляет поиск и оценку поставщиков, собирает коммерческие предложения и условия поставки.',
            'Организует согласование заказов, договоров, сроков и условий поставки.',
            'Контролирует комплектность, сроки поставки и наличие сопроводительных документов.',
            'Передаёт сведения для входного контроля и участвует в оценке результативности поставщиков.',
            'Ведёт и актуализирует перечень поставщиков и закупочную документацию.',
        ]),
        (('кадр', 'персонал', 'hr', 'инспектор по кадрам'), [
            'Ведёт кадровый учёт работников и оформляет приём, перевод, отпуск и увольнение.',
            'Формирует и актуализирует личные дела, сведения о должностях, квалификации и обучении работников.',
            'Контролирует наличие документов, подтверждающих образование, квалификацию и обязательное обучение.',
            'Участвует в подготовке штатного расписания, табелей и кадровой отчётности.',
            'Обеспечивает хранение и конфиденциальность кадровых документов.',
        ]),
        (('юрист', 'юрисконсульт'), [
            'Проводит правовую экспертизу договоров, приказов и иных документов организации.',
            'Готовит проекты договоров, претензий, ответов и иных правовых документов.',
            'Контролирует соответствие деятельности организации применимым требованиям законодательства.',
            'Участвует в урегулировании претензионных и договорных вопросов.',
            'Консультирует работников и руководителей по правовым вопросам в пределах компетенции.',
        ]),
        (('охране труда', 'охрана труда'), [
            'Организует работу по охране труда, контролирует соблюдение требований законодательства и локальных документов.',
            'Участвует в идентификации опасностей, оценке профессиональных рисков и разработке мер управления.',
            'Организует обучение, инструктажи и проверку знаний по вопросам охраны труда.',
            'Контролирует обеспечение работников средствами индивидуальной защиты и безопасными условиями труда.',
            'Участвует в расследовании происшествий и контроле выполнения корректирующих мероприятий.',
            'Ведёт установленную документацию и отчётность по охране труда.',
        ]),
        (('главный инженер', 'гл. инженер'), [
            'Организует техническую подготовку и техническое руководство производственной деятельностью организации.',
            'Обеспечивает выполнение работ в соответствии с проектной, технологической и нормативной документацией.',
            'Контролирует качество работ, применение материалов, оборудования и средств измерений.',
            'Координирует деятельность инженерно-технических работников и производственных подразделений.',
            'Организует мероприятия по повышению технического уровня, безопасности и эффективности производства.',
            'Участвует в анализе рисков, несоответствий и результативности процессов СМК/СУОТ.',
        ]),
        (('производитель работ', 'прораб'), [
            'Организует и контролирует выполнение работ на закреплённых объектах и участках.',
            'Обеспечивает соблюдение проектной и технологической документации, сроков и требований качества.',
            'Организует рабочие места, выдаёт производственные задания и контролирует их выполнение.',
            'Контролирует использование материалов, инструмента, оборудования и средств защиты.',
            'Проводит необходимые инструктажи и обеспечивает соблюдение требований охраны труда.',
            'Ведёт производственную и исполнительную документацию в пределах компетенции.',
        ]),
        (('инженер-проект', 'проектиров', 'конструктор'), [
            'Разрабатывает и проверяет проектные и технические решения по закреплённому направлению.',
            'Выполняет расчёты и подготавливает проектную документацию в соответствии с исходными данными и требованиями ТНПА.',
            'Согласовывает технические решения со смежными специалистами и заинтересованными сторонами.',
            'Вносит изменения в проектную документацию и контролирует её актуальность.',
            'Участвует в авторском сопровождении и устранении замечаний по проектной документации.',
        ]),
        (('инженер', 'техник'), [
            'Выполняет инженерно-технические работы по закреплённому направлению деятельности.',
            'Готовит, проверяет и актуализирует техническую документацию и исходные данные.',
            'Контролирует соответствие выполняемых работ установленным требованиям и нормативной документации.',
            'Участвует в анализе технических вопросов, несоответствий и разработке корректирующих мероприятий.',
            'Ведёт установленную отчётность и предоставляет руководителю необходимую техническую информацию.',
        ]),
        (('качест', 'смк'), [
            'Поддерживает в актуальном состоянии документы системы менеджмента качества.',
            'Участвует в планировании и проведении внутренних аудитов и контроле выполнения корректирующих действий.',
            'Регистрирует и анализирует несоответствия, риски, показатели процессов и мероприятия по улучшению.',
            'Контролирует актуальность записей и документов СМК.',
            'Готовит сведения для анализа СМК со стороны руководства.',
        ]),
        (('заведующий хозяйством', 'завхоз'), [
            'Организует хозяйственное обеспечение помещений и рабочих мест.',
            'Контролирует наличие, исправность и рациональное использование имущества, инвентаря и хозяйственных материалов.',
            'Организует получение, хранение и выдачу хозяйственных ценностей.',
            'Участвует в организации ремонта, обслуживания помещений и оборудования.',
            'Ведёт установленный учёт и документацию по закреплённому имуществу.',
        ]),
        (('директор',), [
            'Осуществляет общее руководство деятельностью организации и принимает управленческие решения в пределах полномочий.',
            'Определяет распределение ответственности и полномочий работников.',
            'Обеспечивает ресурсами выполнение договорных обязательств, требований СМК и СУОТ.',
            'Утверждает локальные документы, планы, цели и мероприятия организации.',
            'Проводит анализ результативности деятельности и принимает решения по улучшению.',
        ]),
    ]

    for markers, duties in profiles:
        if any(_norm(m) in p for m in markers):
            return duties

    role = position or 'работник'
    return [
        f'Выполняет функции по должности «{role}» в соответствии с распределёнными полномочиями и задачами организации.',
        'Обеспечивает своевременное и качественное выполнение порученных работ и ведение связанных с ними записей.',
        'Соблюдает применимые требования законодательства, локальных документов, СМК и СУОТ.',
        'Предоставляет непосредственному руководителю достоверную информацию о ходе и результатах своей работы.',
        'Участвует в выявлении рисков, несоответствий и разработке мероприятий по улучшению в пределах своей компетенции.',
        'Обеспечивает сохранность документов, имущества и информации, используемых при выполнении должностных обязанностей.',
    ]


def _generic_job_description(company: dict, position: str, dates: dict) -> bytes:
    title_position = position or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    doc = _new_doc(company, f'ДОЛЖНОСТНАЯ ИНСТРУКЦИЯ — {title_position}', dates, 'goals')

    p = doc.add_paragraph()
    p.add_run('1. Общие положения').bold = True
    doc.add_paragraph(
        f'Настоящая должностная инструкция определяет обязанности, права и ответственность '
        f'работника по должности «{title_position}». Работник в своей деятельности руководствуется '
        f'законодательством Республики Беларусь, локальными документами организации, требованиями '
        f'системы менеджмента качества и системы управления охраной труда.'
    )

    p = doc.add_paragraph()
    p.add_run('2. Должностные обязанности').bold = True
    for idx, duty in enumerate(_job_duties_for_position(position), 1):
        doc.add_paragraph(f'2.{idx}. {duty}')

    p = doc.add_paragraph()
    p.add_run('3. Права').bold = True
    rights = [
        'Получать документы, информацию и ресурсы, необходимые для выполнения должностных обязанностей.',
        'Вносить предложения по улучшению процессов, качества работ и условий труда.',
        'Запрашивать у работников и подразделений сведения, необходимые для выполнения задач в пределах компетенции.',
        'Сообщать руководителю о выявленных несоответствиях, рисках и препятствиях для выполнения работы.',
    ]
    for idx, text in enumerate(rights, 1):
        doc.add_paragraph(f'3.{idx}. {text}')

    p = doc.add_paragraph()
    p.add_run('4. Ответственность').bold = True
    responsibilities = [
        'За ненадлежащее исполнение должностных обязанностей в пределах, установленных законодательством и локальными документами.',
        'За достоверность подготовленных документов, расчётов и сведений в пределах своей компетенции.',
        'За нарушение требований охраны труда, пожарной безопасности, СМК и СУОТ, относящихся к выполняемой работе.',
        'За сохранность переданных документов, имущества и конфиденциальной информации.',
    ]
    for idx, text in enumerate(responsibilities, 1):
        doc.add_paragraph(f'4.{idx}. {text}')
    return _doc_bytes(doc)


def _worker_specific_controls(profession: str) -> list[str]:
    p = _norm(profession)
    profiles = [
        (('электр',), [
            'Перед началом работы проверить отсутствие повреждений кабелей, инструмента, защитных устройств и применяемых средств защиты.',
            'Не приступать к работам на токоведущих частях без предусмотренных организационных и технических мероприятий.',
            'Применять инструмент и средства защиты, соответствующие характеру выполняемых электромонтажных работ.',
        ]),
        (('свар',), [
            'Проверить исправность сварочного оборудования, кабелей, держателя, заземления и средств индивидуальной защиты.',
            'Удалить горючие материалы из опасной зоны либо обеспечить их защиту; иметь доступные средства пожаротушения.',
            'При выполнении сварочных работ применять защиту глаз, лица, рук и органов дыхания согласно условиям работы.',
        ]),
        (('штукатур', 'маляр', 'облицовщик', 'плиточник'), [
            'Проверить исправность подмостей, лестниц, ручного и электроинструмента, а также устойчивость рабочего места.',
            'При работе со смесями, красками и иными материалами применять предусмотренные средства защиты кожи, глаз и органов дыхания.',
            'Соблюдать требования безопасности при работе на высоте и при применении электроинструмента.',
        ]),
        (('кровель',), [
            'До начала работ проверить ограждения, точки крепления, средства защиты от падения и состояние поверхности кровли.',
            'Не выполнять кровельные работы при условиях, при которых безопасность не может быть обеспечена.',
            'Материалы и инструмент размещать так, чтобы исключить их падение и самопроизвольное перемещение.',
        ]),
        (('сантех', 'трубопровод'), [
            'Проверить исправность инструмента, приспособлений и оборудования для резки, соединения и монтажа трубопроводов.',
            'Перед работой на действующих системах убедиться в отключении, сбросе давления и отсутствии опасных сред.',
            'При подъёме и перемещении труб и оборудования применять исправные грузозахватные приспособления.',
        ]),
        (('монтажник', 'стропальщик', 'такелаж'), [
            'Проверить исправность грузозахватных приспособлений, инструмента и средств связи.',
            'Не находиться и не допускать нахождение людей под перемещаемым грузом.',
            'Соблюдать безопасные зоны и установленный порядок строповки, подъёма и монтажа конструкций.',
        ]),
        (('подсоб', 'разнораб'), [
            'Получить конкретное задание и инструктаж по безопасным способам его выполнения.',
            'Использовать только исправный инструмент и предусмотренные средства индивидуальной защиты.',
            'Не выполнять работы, требующие специальной квалификации или допуска, без соответствующего обучения и разрешения.',
        ]),
    ]
    for markers, rules in profiles:
        if any(_norm(m) in p for m in markers):
            return rules
    return [
        'Перед началом работы проверить исправность используемого инструмента, оборудования, приспособлений и средств индивидуальной защиты.',
        'Выполнять только порученную работу безопасными методами и в пределах имеющейся квалификации и допуска.',
        'Немедленно прекратить работу при возникновении опасности, неисправности оборудования или отсутствии необходимых средств защиты.',
    ]


def _generic_worker_instruction(company: dict, profession: str, dates: dict) -> bytes:
    title_prof = profession or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    doc = _new_doc(company, f'ИНСТРУКЦИЯ ПО ОХРАНЕ ТРУДА ДЛЯ ПРОФЕССИИ «{title_prof}»', dates, 'goals')
    sections = [
        ('1. Общие требования охраны труда',
         'К работе допускаются работники, прошедшие предусмотренные законодательством обучение, инструктаж и проверку знаний, а также медицинский осмотр, когда он обязателен.'),
        ('2. Требования перед началом работы',
         'Проверить рабочее место, инструмент, оборудование, ограждения и средства индивидуальной защиты.'),
        ('3. Требования во время работы',
         'Соблюдать технологическую последовательность, требования инструкций изготовителей, установленные безопасные методы работы и применять предусмотренные средства защиты.'),
        ('4. Требования в аварийных ситуациях',
         'Прекратить работу, вывести людей из опасной зоны при необходимости, сообщить непосредственному руководителю и действовать согласно установленному порядку реагирования.'),
        ('5. Требования после окончания работы',
         'Отключить оборудование, привести рабочее место в безопасное состояние, убрать инструмент и сообщить руководителю о выявленных неисправностях и отклонениях.'),
    ]
    for heading, body in sections:
        p = doc.add_paragraph()
        p.add_run(heading).bold = True
        doc.add_paragraph(body)
        if heading.startswith('2.'):
            for rule in _worker_specific_controls(profession):
                doc.add_paragraph('• ' + rule)
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
    """Render the supplier card in the same structure as the approved ISO template.

    Supplier identity/product come from the client's supplier list.
    Evaluation scores are *not* invented: if no factual assessment was supplied,
    the score cells remain blank and only one compact review note is added.
    """
    supplier = dict(supplier or {})
    name = str(supplier.get('name') or '').strip() or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    product_value = supplier.get('type') or supplier.get('product') or supplier.get('products') or ''
    if isinstance(product_value, (list, tuple)):
        supply_type = ', '.join(str(x) for x in product_value if x)
    else:
        supply_type = str(product_value or '').strip()
    if not supply_type:
        supply_type = 'ТРЕБУЕТ УТОЧНЕНИЯ'

    doc = Document(TPL_DIR / 'kartochka_1.docx')

    org_full = _full_org(company)
    director_fio = str(company.get('director_fio') or '').strip()
    director_short = _initials(director_fio) if director_fio else 'ТРЕБУЕТ УТОЧНЕНИЯ'
    date_text = str((dates or {}).get('reports') or (dates or {}).get('goals') or '').strip()

    # Replace visible template text, preserving the original layout.
    replacements = {
        'ООО «Варта»': org_full,
        'ООО "Варта"': org_full,
        'С.Ф. Василенко': director_short,
        '13.04.2026 г.': (date_text + ' г.') if date_text else 'ТРЕБУЕТ УТОЧНЕНИЯ',
        'Кабельная продукция': supply_type,
    }

    def replace_in_paragraph(paragraph):
        if not paragraph.text:
            return
        text = paragraph.text
        new = text
        for old, repl in replacements.items():
            new = new.replace(old, repl)
        if new != text:
            for r in paragraph.runs:
                r.text = ''
            if paragraph.runs:
                paragraph.runs[0].text = new
            else:
                paragraph.add_run(new)

    for p in doc.paragraphs:
        replace_in_paragraph(p)
    for section in doc.sections:
        for p in section.header.paragraphs:
            replace_in_paragraph(p)
        for p in section.footer.paragraphs:
            replace_in_paragraph(p)

    # Original template table: first 3 rows are headers, rows 4-5 are suppliers.
    table = doc.tables[0]
    while len(table.rows) < 4:
        table.add_row()
    actual = table.rows[3].cells
    values = ['1', name, '', '', '', '', '', '', '', '', '+', '']
    for i, value in enumerate(values):
        actual[i].text = value

    # Remove the sample competitor row instead of inventing a fake competitor.
    if len(table.rows) > 4:
        tr = table.rows[4]._tr
        tr.getparent().remove(tr)

    # If structured scores were supplied by the user, use them.
    score_keys = ['price', 'quality', 'volume', 'delivery', 'payment', 'known', 'status']
    scores = supplier.get('scores') if isinstance(supplier.get('scores'), dict) else {}
    if scores:
        total = 0.0
        any_score = False
        for col_idx, key in enumerate(score_keys, start=2):
            value = scores.get(key)
            if value not in (None, ''):
                actual[col_idx].text = str(value).replace('.', ',')
                try:
                    total += float(str(value).replace(',', '.'))
                    any_score = True
                except Exception:
                    pass
        if any_score:
            actual[9].text = str(total).replace('.', ',')

    # Always show what is actually known about the supplier evaluation.
    status = str(supplier.get('status') or '').strip()
    decision = str(supplier.get('decision') or supplier.get('applicability') or '').strip()
    p = doc.add_paragraph(); p.add_run('Сведения об оценке поставщика').bold = True
    doc.add_paragraph(f'Статус в перечне: {status or "действующий поставщик по предоставленному перечню"}.')
    if scores:
        if actual[9].text.strip(): doc.add_paragraph(f'Суммарная балльная оценка: {actual[9].text.strip()}.')
        doc.add_paragraph(f'Решение о применимости: {decision or "ТРЕБУЕТ УТОЧНЕНИЯ"}.')
    else:
        note = doc.add_paragraph()
        note.add_run(
            'ТРЕБУЕТ УТОЧНЕНИЯ: отсутствуют фактические баллы по цене, качеству, срокам, '
            'условиям оплаты и опыту работы. Заполните оценку по фактическим условиям сотрудничества.'
        )

    return _doc_bytes(doc)



def _person_order_label(person: dict) -> str:
    if not person:
        return 'ТРЕБУЕТ УТОЧНЕНИЯ'
    pos = str(person.get('position') or 'ТРЕБУЕТ УТОЧНЕНИЯ').strip()
    fio = str(person.get('fio') or 'ТРЕБУЕТ УТОЧНЕНИЯ').strip()
    return f'{pos} — {fio}'


def _unique_people(people) -> list[dict]:
    out, seen = [], set()
    for p in people or []:
        if not isinstance(p, dict):
            continue
        key = _norm(p.get('fio'))
        if not key or key in seen:
            continue
        seen.add(key); out.append(p)
    return out


def _appointment_order_doc(company: dict, dates: dict, number: str, title: str,
                           clauses: list[str], acquainted: list[dict] | None = None,
                           scope: str = '') -> bytes:
    doc = _new_doc(company, f'ПРИКАЗ № {number}', dates, 'goals')
    p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(title.upper()); r.bold = True
    if scope:
        p = doc.add_paragraph(); p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p.add_run(f'В ОБЛАСТИ: {scope}').bold = True
    p = doc.add_paragraph(); p.add_run('ПРИКАЗЫВАЮ:').bold = True
    for i, clause in enumerate(clauses, 1):
        doc.add_paragraph(f'{i}. {clause}')
    if acquainted:
        p = doc.add_paragraph(); p.add_run('С приказом ознакомлены:').bold = True
        for person in _unique_people(acquainted):
            doc.add_paragraph(f'{_person_order_label(person)} __________________')
    return _doc_bytes(doc)


def _dynamic_role_orders(company: dict, itr: list, dates: dict, resp: dict,
                         wanted_categories: set[str]) -> list[dict]:
    """Orders that mention employees are rebuilt from the current staff card.

    Sample names/job titles from Varta are never reused, so FIO and position cannot
    become cross-wired when the new company has a different staffing structure.
    """
    docs = []
    director = (resp.get('director') or {}) if isinstance(resp, dict) else {}
    auditors = _unique_people((resp.get('auditors') or []) if isinstance(resp, dict) else [])
    if not auditors:
        auditors = _unique_people(([director] if director else []) + list(itr or []))[:3]
    risk_group = _unique_people((resp.get('risk_group') or auditors) if isinstance(resp, dict) else auditors)
    council = _unique_people((resp.get('coord_council') or auditors) if isinstance(resp, dict) else auditors)
    process_person = (resp.get('process_resp') or {}) if isinstance(resp, dict) else {}
    fnpa_person = (resp.get('fnpa_resp') or {}) if isinstance(resp, dict) else {}
    di_person = (resp.get('di_resp') or {}) if isinstance(resp, dict) else {}

    def labels(people): return '; '.join(_person_order_label(p) for p in _unique_people(people)) or 'ТРЕБУЕТ УТОЧНЕНИЯ'
    org = _clean_org_name(company)
    scope = str(company.get('scope') or '').strip() or 'ТРЕБУЕТ УТОЧНЕНИЯ'

    if 'iso' in wanted_categories:
        items = [
            ('3-СМК', 'О назначении аудиторов для проведения внутреннего аудита',
             [f'Область применения СМК: {scope}.', f'Назначить внутренними аудиторами: {labels(auditors)}.', 'Контроль за исполнением приказа оставляю за директором.'], auditors),
            ('5-СМК', 'О проведении оценки и анализа рисков',
             [f'Область применения СМК: {scope}.', f'Создать рабочую группу по идентификации и оценке рисков в составе: {labels(risk_group)}.', 'Рабочей группе провести идентификацию и оценку рисков по действующим процессам организации.'], risk_group),
            ('6-СМК', 'О назначении владельца процесса',
             [f'Назначить владельцем процесса «{scope}»: {_person_order_label(process_person)}.', 'Владельцу процесса обеспечить мониторинг показателей и управление рисками процесса.'], [process_person]),
            ('7-СМК', 'О назначении ответственного за фонд ТНПА, НПА и документов СМК',
             [f'Область применения СМК: {scope}.', f'Назначить ответственным за актуализацию и управление фондом документов: {_person_order_label(fnpa_person)}.'], [fnpa_person]),
            ('8-СМК', 'О создании Координационного совета',
             [f'Область применения СМК: {scope}.', f'Создать Координационный совет в составе: {labels(council)}.', 'Совету осуществлять мониторинг результативности СМК и мероприятий по улучшению.'], council),
            ('9-СМК', 'О проведении внутреннего обучения специалистов',
             [f'Провести внутреннее обучение специалистов для области «{scope}»: {labels(auditors)}.', 'Тематика обучения: документированная информация СМК и внутренний аудит.'], auditors),
            ('10-СМК', 'О назначении ответственного за входной контроль',
             [f'Для области «{scope}» назначить ответственным за организацию входного контроля: {_person_order_label(process_person)}.'], [process_person]),
            ('11-СМК', 'О технических осмотрах средств измерений',
             [f'Для области «{scope}» назначить ответственным за учёт, технический осмотр и своевременную поверку средств измерений: {_person_order_label(process_person or fnpa_person)}.'], [process_person or fnpa_person]),
        ]
        for number,title,clauses,people in items:
            docs.append({'name': f'{org} - Приказ {number} {title}.docx', 'bytes': _appointment_order_doc(company,dates,number,title,clauses,people,scope)})

    if 'suot' in wanted_categories:
        ot_people = _unique_people(auditors)
        items = [
            ('4-OH&S', 'О назначении ответственных лиц СУОТ',
             [f'Назначить ответственными за организацию работы по охране труда: {labels(ot_people)}.',
              f'Ответственным за учёт и ведение инструкций по охране труда назначить: {_person_order_label(di_person or director)}.'], ot_people),
            ('5-OH&S', 'О назначении аудиторов СУОТ',
             [f'Назначить внутренними аудиторами СУОТ: {labels(ot_people)}.'], ot_people),
            ('6-OH&S', 'О пересмотре и разработке карт оценки рисков',
             [f'Создать рабочую группу по идентификации опасностей и оценке рисков в составе: {labels(risk_group)}.'], risk_group),
            ('7-OH&S', 'О назначении ответственного за инструкции по охране труда',
             [f'Назначить ответственным за учёт, актуализацию и выдачу инструкций по охране труда: {_person_order_label(di_person or director)}.'], [di_person or director]),
        ]
        for number,title,clauses,people in items:
            docs.append({'name': f'{org} СУОТ - Приказ {number} {title}.docx', 'bytes': _appointment_order_doc(company,dates,number,title,clauses,people)})
    return docs


def _matching_template(position: str, rules: dict) -> str | None:
    p = _norm(position)
    for key, keywords in rules.items():
        if any(_norm(k) in p for k in keywords):
            return key
    return None


def _order_identity(name: str) -> str | None:
    """Return a stable identity for ISO/SUOT appointment orders.

    The same order can arrive once from the legacy detailed template and once from
    the dynamic current-staff generator. They are one document for the user.
    """
    n = str(name or '').lower().replace('ё', 'е')
    if 'приказ' not in n:
        return None
    if 'суот' in n or 'oh&s' in n:
        m = re.search(r'(?:приказ\s+|приказ\s+№\s*)?(\d+)(?:[\.\-]|\s)', n)
        return f'suot:{m.group(1)}' if m else None
    if 'смк' in n or 'приказ' in n:
        m = re.search(r'(?:приказ\s+|приказ\s+№\s*)?(\d+)(?:[\.\-]|\s)', n)
        return f'iso:{m.group(1)}' if m else None
    return None


def _dedupe_order_docs(docs: list[dict]) -> list[dict]:
    """Keep one variant of each ISO/SUOT order, preferring the fuller document.

    The legacy approved templates usually contain the more detailed wording. When
    two variants have the same order identity, keep the larger DOCX payload, which
    is a deterministic proxy for the fuller template, and otherwise keep the first.
    """
    chosen: dict[str, dict] = {}
    order: list[str] = []
    for doc in docs:
        name = str(doc.get('name') or '')
        key = _order_identity(name)
        if not key:
            continue
        if key not in chosen:
            chosen[key] = doc
            order.append(key)
            continue
        old = chosen[key]
        if len(doc.get('bytes') or b'') > len(old.get('bytes') or b''):
            chosen[key] = doc
    keep_ids = {id(v) for v in chosen.values()}
    out = []
    emitted = set()
    for doc in docs:
        key = _order_identity(str(doc.get('name') or ''))
        if not key:
            out.append(doc)
        elif id(doc) in keep_ids and key not in emitted:
            out.append(doc); emitted.add(key)
    return out


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
        xml = re.sub(r'<a:p(?:\s[^>]*)?>.*?</a:p>', repl_para, xml, flags=re.S)
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
    _DYNAMIC_ROLE_ORDER_KEYS = {
        'prikaz_smk_3.docx','prikaz_smk_5.docx','prikaz_smk_6.docx','prikaz_smk_7.docx',
        'prikaz_smk_8.docx','prikaz_smk_9.docx','prikaz_smk_10.docx','prikaz_smk_11.docx',
        'suot_prikaz_7.docx','suot_prikaz_8.docx','suot_prikaz_9.docx','suot_prikaz_10.docx',
    }
    for key in keys:
        if key == 'converted_1.docx':  # known corrupt pseudo-docx
            continue
        if key in _DYNAMIC_KEYS or key in _DYNAMIC_ROLE_ORDER_KEYS:
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
            if key == 'smk_doc_14.docx':
                data = _ensure_satisfaction_objects(data, objects)
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

    # Staff-dependent orders are rebuilt from the current card so each FIO keeps
    # its real current position. This eliminates the old Varta role/name cross-wiring.
    for order_doc in _dynamic_role_orders(company, itr, dates, resp, wanted_categories):
        prog(order_doc['name'][:50])
        docs.append(order_doc)

    # A legacy detailed order and a newer dynamic order can otherwise appear side by
    # side. Collapse them before the final package is returned.
    docs = _dedupe_order_docs(docs)

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
