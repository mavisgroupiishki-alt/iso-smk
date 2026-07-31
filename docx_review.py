"""Inline review highlighting for all generated DOCX packages.

The generator must never silently present guessed or missing data as confirmed.
Every doubtful value is highlighted directly where it appears in the generated
document. Missing values are written as a yellow ``ТРЕБУЕТ УТОЧНЕНИЯ`` marker
next to the corresponding field; no separate review document is created.
"""
from __future__ import annotations

import html
import io
import re
import zipfile
from typing import Any, Dict, Iterable, List, Sequence, Tuple

YELLOW_TAG = '<w:highlight w:val="yellow"/>'

# Only explicit review markers are highlighted. A normal signature line made of
# underscores is deliberately not considered missing data.
EXPLICIT_MARKERS = (
    'требует уточнения',
    'требуется уточнение',
    'не заполнено',
    'не указано',
    'нет данных',
    'неразборчиво',
    'не распознано',
    'не удалось определить',
    'проверьте по оригиналу',
    'нужно проверить',
    'сомнительно',
    'предположительно',
    'данные отсутствуют',
    'unknown',
    'n/a',
)

EXACT_PLACEHOLDERS = {
    '—', '–', '-', '?',
    '[пусто]', '[нет данных]', '[не указано]', '[не заполнено]',
    'требует уточнения', 'требуется уточнение',
}

_METADATA_KEYS = {
    'uncertain_fields', 'needs_review', 'confidence', 'source', 'sources',
    'review_reason', 'review_reasons', 'raw_text', 'ocr_text',
}

STALE_TEMPLATE_MARKERS = (
    'Варта', 'Кастом-Инвест', 'Сфера Секьюрити', 'МонТехБел',
)


def _norm(value: Any) -> str:
    return re.sub(r'\s+', ' ', str(value or '')).strip().casefold().replace('ё', 'е')


def _looks_uncertain_text(value: Any) -> bool:
    text = _norm(value)
    if not text:
        return False
    if any(marker in text for marker in EXPLICIT_MARKERS):
        return True
    # Vision alternatives such as "[38 или 36]" must remain visible.
    if re.search(r'\[[^\]]+\s+или\s+[^\]]+\]', text):
        return True
    if '[ошибка' in text or '⚠' in text:
        return True
    return False


def _iter_scalar_values(value: Any) -> Iterable[str]:
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        text = str(value).strip()
        if text:
            yield text
    elif isinstance(value, list):
        for item in value:
            yield from _iter_scalar_values(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            if key not in _METADATA_KEYS:
                yield from _iter_scalar_values(item)


def _get_field(obj: Dict[str, Any], field: str) -> Any:
    current: Any = obj
    for part in str(field).replace('[', '.').replace(']', '').split('.'):
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            idx = int(part)
            current = current[idx] if idx < len(current) else None
        else:
            return None
    return current


def collect_review_tokens_and_items(data: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, str]]]:
    """Collect exact values marked uncertain by the AI/source parser.

    ``uncertain_fields`` is preferred because it allows highlighting only the
    doubtful cell instead of an entire person. If a record has low confidence but
    no field list, all its non-metadata scalar values are conservatively marked.
    """
    tokens: List[str] = []
    items: List[Dict[str, str]] = []

    def add_item(path: str, value: Any, reason: str) -> None:
        text = str(value or '').strip()
        item = {'field': path or 'данные', 'value': text or 'НЕ ЗАПОЛНЕНО', 'reason': reason}
        signature = (_norm(item['field']), _norm(item['value']), _norm(item['reason']))
        if not any((_norm(x['field']), _norm(x['value']), _norm(x['reason'])) == signature for x in items):
            items.append(item)
        if text and len(text) >= 2 and text not in tokens:
            tokens.append(text)

    def walk(value: Any, path: str = '') -> None:
        if isinstance(value, dict):
            uncertain_fields = value.get('uncertain_fields') or []
            if isinstance(uncertain_fields, str):
                uncertain_fields = [uncertain_fields]
            confidence = value.get('confidence')
            low_confidence = False
            try:
                low_confidence = confidence not in (None, '') and float(confidence) < 0.85
            except (TypeError, ValueError):
                pass
            needs_review = bool(value.get('needs_review')) or low_confidence
            reasons = value.get('review_reasons') or value.get('review_reason') or ''
            if isinstance(reasons, list):
                reasons = '; '.join(str(x) for x in reasons if x)
            default_reason = str(reasons or ('низкая уверенность распознавания' if low_confidence else 'требует проверки'))

            if uncertain_fields:
                for field in uncertain_fields:
                    field_value = _get_field(value, str(field))
                    field_path = f'{path}.{field}'.strip('.')
                    if field_value in (None, '', [], {}):
                        add_item(field_path, '', default_reason)
                    else:
                        for token in _iter_scalar_values(field_value):
                            add_item(field_path, token, default_reason)
            elif needs_review:
                found = False
                for key, child in value.items():
                    if key in _METADATA_KEYS:
                        continue
                    for token in _iter_scalar_values(child):
                        found = True
                        add_item(f'{path}.{key}'.strip('.'), token, default_reason)
                if not found:
                    add_item(path, '', default_reason)

            for key, child in value.items():
                if key not in _METADATA_KEYS:
                    walk(child, f'{path}.{key}'.strip('.'))
        elif isinstance(value, list):
            for idx, child in enumerate(value):
                walk(child, f'{path}[{idx}]')
        elif _looks_uncertain_text(value):
            add_item(path, value, 'значение содержит пометку неуверенного распознавания')

    walk(data)
    # Top-level review_items are produced for cross-document conflicts where the
    # doubtful value may not belong to one nested record.
    for item in data.get('review_items') or []:
        if not isinstance(item, dict):
            continue
        add_item(
            str(item.get('field') or 'данные'),
            item.get('value') or '',
            str(item.get('reason') or 'требует сверки с оригиналом'),
        )
    # Longer tokens first prevents a surname from shadowing a full doubtful value.
    tokens.sort(key=len, reverse=True)
    return tokens, items


def _missing(path: str, label: str, items: List[Dict[str, str]]) -> None:
    items.append({'field': path, 'value': 'НЕ ЗАПОЛНЕНО', 'reason': f'обязательное поле: {label}'})


def collect_required_items(data: Dict[str, Any], product: str) -> List[Dict[str, str]]:
    """Product-specific minimum data that should not be silently omitted."""
    items: List[Dict[str, str]] = []
    company = data.get('company') or {}
    cert = data.get('certification') or {}
    dates = data.get('dates') or {}
    staff = data.get('staff') or []

    for key, label in (
        ('name', 'название компании'),
        ('form', 'организационно-правовая форма'),
        ('director_fio', 'ФИО руководителя'),
    ):
        if not company.get(key):
            _missing(f'company.{key}', label, items)

    if product in ('iso', 'suot', 'iso_suot'):
        if not (cert.get('scope') or company.get('scope')):
            _missing('certification.scope', 'область сертификации/деятельности', items)
        if not (dates.get('audit_date') or cert.get('audit_date')):
            _missing('dates.audit_date', 'дата выезда эксперта', items)
        if not staff:
            _missing('staff', 'актуальный список сотрудников', items)
        if product in ('suot', 'iso_suot'):
            workers = data.get('workers') or [p for p in staff if p.get('is_worker')]
            if not workers:
                _missing('workers', 'рабочие профессии для СУОТ', items)

    elif product in ('spk_stroy', 'spk_bisp'):
        if not (data.get('work_types') or company.get('work_types')):
            _missing('work_types', 'заявляемые виды работ СПК', items)
        itr = [p for p in staff if not p.get('is_worker')]
        if len(itr) < 2:
            items.append({'field': 'staff', 'value': f'НАЙДЕНО ИТР: {len(itr)}',
                          'reason': 'для СПК требуется проверить достаточность ИТР'})
        spk = data.get('spk') or {}
        for key, label in (
            ('measurement_tools', 'средства измерений и поверка'),
            ('ttk', 'технологическая документация/ТТК'),
            ('premises', 'производственные помещения или основание пользования'),
        ):
            if not spk.get(key):
                _missing(f'spk.{key}', label, items)

    elif product == 'att':
        persons = (data.get('attestation') or {}).get('persons') or []
        if not persons:
            _missing('attestation.persons', 'специалист для аттестации', items)
        for idx, person in enumerate(persons):
            for key, label in (
                ('fio', 'ФИО специалиста'),
                ('education_level', 'уровень образования'),
                ('diploma_number', 'номер диплома'),
            ):
                if not person.get(key):
                    _missing(f'attestation.persons[{idx}].{key}', label, items)
            if not person.get('requests'):
                _missing(f'attestation.persons[{idx}].requests', 'специализация аттестации', items)

    elif product == 'company_att':
        att = data.get('company_attestation') or {}
        if not (att.get('work_items') or att.get('work_scope_text')):
            _missing('company_attestation.work_items', 'заявляемые виды работ', items)
        if not att.get('itr'):
            _missing('company_attestation.itr', 'ИТР для Формы №2', items)

    # Explicit warning/error flags are review items for every product.
    for idx, flag in enumerate(data.get('flags') or []):
        if str(flag.get('type', '')).lower() in ('warning', 'error'):
            items.append({
                'field': f'flags[{idx}]',
                'value': str(flag.get('text') or 'ТРЕБУЕТ ПРОВЕРКИ'),
                'reason': 'предупреждение проверки данных',
            })
    return items


def _extract_text(xml_fragment: str) -> str:
    parts = re.findall(r'<w:t(?:\s[^>]*)?>(.*?)</w:t>', xml_fragment, flags=re.S)
    return html.unescape(''.join(re.sub(r'<[^>]+>', '', p) for p in parts))


def _add_highlight_to_run(run_xml: str) -> str:
    if YELLOW_TAG in run_xml:
        return run_xml
    if '<w:rPr' in run_xml:
        if '</w:rPr>' in run_xml:
            return run_xml.replace('</w:rPr>', YELLOW_TAG + '</w:rPr>', 1)
        # Self-closing rPr is rare but valid.
        return re.sub(r'<w:rPr([^>]*)/>', r'<w:rPr\1>' + YELLOW_TAG + '</w:rPr>', run_xml, count=1)
    opening = re.search(r'<w:r(?:\s[^>]*)?>', run_xml)
    if not opening:
        return run_xml
    return run_xml[:opening.end()] + '<w:rPr>' + YELLOW_TAG + '</w:rPr>' + run_xml[opening.end():]


def _text_needs_highlight(text: str, token_norms: Sequence[str]) -> bool:
    normalized = _norm(text)
    if not normalized:
        return False
    if normalized in {_norm(x) for x in EXACT_PLACEHOLDERS}:
        return True
    # Typical generated field placeholders: «УНП: —», «выдан —», «от —».
    if re.search(r'(?:^|[:;]|\b(?:выдан|от|номер|дата|стаж|адрес|унп))\s*[—–-]\s*$', normalized):
        return True
    if any(marker in normalized for marker in EXPLICIT_MARKERS):
        return True
    if re.search(r'\[[^\]]+\s+или\s+[^\]]+\]', normalized):
        return True
    return any(token and token in normalized for token in token_norms)


def _highlight_xml(xml_text: str, review_tokens: Sequence[str]) -> str:
    token_norms = [_norm(t) for t in review_tokens if len(_norm(t)) >= 2]

    # First pass: exact run values and values that wholly contain a doubtful token.
    def run_repl(match: re.Match) -> str:
        run = match.group(0)
        text = _extract_text(run)
        return _add_highlight_to_run(run) if _text_needs_highlight(text, token_norms) else run

    result = re.sub(r'<w:r(?:\s[^>]*)?>.*?</w:r>', run_repl, xml_text, flags=re.S)

    # Second pass: a value may be split across several Word runs. If a paragraph or
    # table cell contains the token but no individual run did, highlight the runs
    # carrying actual text in that block.
    def block_repl(match: re.Match) -> str:
        block = match.group(0)
        text = _extract_text(block)
        if not _text_needs_highlight(text, token_norms):
            return block
        return re.sub(
            r'<w:r(?:\s[^>]*)?>.*?</w:r>',
            lambda m: _add_highlight_to_run(m.group(0)) if _extract_text(m.group(0)).strip() else m.group(0),
            block,
            flags=re.S,
        )

    result = re.sub(r'<w:tc(?:\s[^>]*)?>.*?</w:tc>', block_repl, result, flags=re.S)
    result = re.sub(r'<w:p(?:\s[^>]*)?>.*?</w:p>', block_repl, result, flags=re.S)
    return result



# Labels used to locate the actual field in a generated document when the value is
# missing. The first matching field in each document is annotated in place.
_FIELD_LABELS = {
    'company.name': ('наименование организации', 'наименование компании', 'название организации', 'название компании'),
    'company.form': ('организационно-правовая форма', 'форма собственности'),
    'company.director_fio': ('фио руководителя', 'фио директора', 'руководитель', 'директор'),
    'company.address': ('юридический адрес', 'адрес организации', 'адрес компании', 'адрес:'),
    'company.unp': ('унп',),
    'company.phone': ('телефон', 'тел.:'),
    'company.email': ('e-mail', 'email', 'электронная почта'),
    'certification.scope': ('область сертификации', 'область применения', 'сфера деятельности', 'вид деятельности'),
    'dates.audit_date': ('дата выезда', 'дата аудита', 'дата проведения аудита'),
    'staff': ('список сотрудников', 'штатное расписание', 'состав работников', 'персонал', 'итр'),
    'workers': ('рабочие профессии', 'состав рабочих', 'рабочие'),
    'work_types': ('заявляемые виды работ', 'виды работ', 'перечень работ'),
    'spk.measurement_tools': ('средства измерений', 'перечень си', 'измерительное оборудование'),
    'spk.ttk': ('технологическая документация', 'технологические карты', 'ттк'),
    'spk.premises': ('производственные помещения', 'площадь помещения', 'помещение', 'договор аренды'),
    'company_attestation.work_items': ('заявляемые виды работ', 'виды работ'),
    'company_attestation.itr': ('инженерно-технические работники', 'итр'),
}


_FIELD_SUFFIX_LABELS = {
    'fio': ('фио', 'фамилия, собственное имя, отчество', 'фамилия'),
    'director_fio': ('фио руководителя', 'фио директора', 'директор', 'руководитель'),
    'position': ('должность', 'профессия'),
    'profession': ('наименование профессии', 'профессия'),
    'education_level': ('уровень образования', 'образование'),
    'education': ('образование', 'сведения об образовании'),
    'diploma_number': ('номер диплома', 'диплом'),
    'diploma_date': ('дата выдачи диплома', 'диплом выдан'),
    'specialty': ('специальность',),
    'qualification': ('квалификация',),
    'trudovaya_number': ('номер трудовой книжки', 'трудовая книжка'),
    'work_book_number': ('номер трудовой книжки', 'трудовая книжка'),
    'labour_book': ('номер трудовой книжки', 'трудовая книжка'),
    'hire_date': ('дата приема', 'дата принятия', 'приказ о приеме'),
    'stage': ('стаж работы', 'стаж'),
    'stage_total': ('общий стаж', 'стаж работы'),
    'stage_here': ('стаж у данного юридического лица', 'стаж в организации'),
    'attestat_number': ('номер квалификационного аттестата', 'квалификационный аттестат', 'аттестат'),
    'certificate_number': ('номер удостоверения', 'номер свидетельства', 'номер сертификата'),
    'certificate_date': ('дата выдачи удостоверения', 'дата выдачи свидетельства', 'дата сертификата'),
    'ot_certificate': ('удостоверение по охране труда', 'проверка знаний по охране труда'),
    'name': ('наименование', 'название'),
    'address': ('адрес',),
    'unp': ('унп',),
    'phone': ('телефон', 'тел.:'),
    'email': ('e-mail', 'email', 'электронная почта'),
    'scope': ('область сертификации', 'область применения', 'сфера деятельности'),
    'audit_date': ('дата выезда', 'дата аудита'),
    'serial_number': ('заводской номер', 'серийный номер'),
    'verification_number': ('номер свидетельства о поверке', 'номер поверки'),
    'verification_date': ('дата поверки', 'дата калибровки'),
    'valid_until': ('действительно до', 'срок действия'),
    'area': ('площадь помещения', 'площадь'),
    'code': ('шифр', 'код', 'номер ттк'),
}


def _field_labels(path: str, reason: str = '') -> Tuple[str, ...]:
    normalized_path = re.sub(r'\[\d+\]', '', str(path or ''))
    if normalized_path in _FIELD_LABELS:
        return _FIELD_LABELS[normalized_path]
    for prefix, labels in _FIELD_LABELS.items():
        if normalized_path.startswith(prefix):
            return labels
    suffix = normalized_path.rsplit('.', 1)[-1]
    if suffix in _FIELD_SUFFIX_LABELS:
        return _FIELD_SUFFIX_LABELS[suffix]
    label = str(reason or '')
    label = re.sub(r'^обязательное поле:\s*', '', label, flags=re.I).strip()
    return (label,) if len(label) >= 4 else ()


def _yellow_marker_run(text: str = 'ТРЕБУЕТ УТОЧНЕНИЯ') -> str:
    safe = html.escape(text, quote=False)
    return (
        '<w:r><w:rPr><w:highlight w:val="yellow"/>'
        '<w:color w:val="9C0006"/><w:b/></w:rPr>'
        f'<w:t xml:space="preserve"> {safe}</w:t></w:r>'
    )


def _block_has_value_after_label(text: str, labels: Sequence[str]) -> bool:
    norm = _norm(text)
    for label in labels:
        label_norm = _norm(label)
        pos = norm.find(label_norm)
        if pos < 0:
            continue
        tail = norm[pos + len(label_norm):].strip(' :;.-—–_')
        if tail and tail not in {_norm(x) for x in EXACT_PLACEHOLDERS}:
            # A long narrative mention is not a form field. Require a compact tail.
            if len(tail) <= 120:
                return True
    return False


def _inject_marker_into_paragraph(paragraph_xml: str) -> str:
    if 'ТРЕБУЕТ УТОЧНЕНИЯ' in paragraph_xml:
        return paragraph_xml
    pos = paragraph_xml.rfind('</w:p>')
    if pos < 0:
        return paragraph_xml
    return paragraph_xml[:pos] + _yellow_marker_run() + paragraph_xml[pos:]


def _inject_marker_into_cell(cell_xml: str) -> str:
    if 'ТРЕБУЕТ УТОЧНЕНИЯ' in cell_xml:
        return cell_xml
    # Prefer the last paragraph in the cell. Word cells must contain a paragraph.
    matches = list(re.finditer(r'<w:p(?:\s[^>]*)?>.*?</w:p>', cell_xml, flags=re.S))
    if matches:
        m = matches[-1]
        paragraph = _inject_marker_into_paragraph(m.group(0))
        return cell_xml[:m.start()] + paragraph + cell_xml[m.end():]
    pos = cell_xml.rfind('</w:tc>')
    if pos < 0:
        return cell_xml
    return cell_xml[:pos] + '<w:p>' + _yellow_marker_run() + '</w:p>' + cell_xml[pos:]


def _inject_missing_items(xml_text: str, missing_items: Sequence[Dict[str, str]]) -> str:
    """Write yellow missing markers next to the actual field in the DOCX XML.

    The method is conservative: per missing field and per document it annotates
    only the first suitable table row or compact paragraph. This avoids painting
    decorative empty cells, signature areas, or every narrative occurrence.
    """
    result = xml_text
    for item in missing_items:
        if _norm(item.get('value')) not in ('не заполнено', ''):
            continue
        labels = tuple(x for x in _field_labels(item.get('field', ''), item.get('reason', '')) if x)
        if not labels:
            continue
        label_norms = tuple(_norm(x) for x in labels)
        inserted = False

        # Most forms use a label cell followed by a value cell.
        rows = list(re.finditer(r'<w:tr(?:\s[^>]*)?>.*?</w:tr>', result, flags=re.S))
        for row_match in rows:
            row = row_match.group(0)
            cells = list(re.finditer(r'<w:tc(?:\s[^>]*)?>.*?</w:tc>', row, flags=re.S))
            if not cells:
                continue
            texts = [_norm(_extract_text(c.group(0))) for c in cells]
            label_idx = next((i for i, text in enumerate(texts) if any(lbl in text for lbl in label_norms)), None)
            if label_idx is None:
                continue
            target_idx = label_idx + 1 if label_idx + 1 < len(cells) else label_idx
            target = cells[target_idx]
            target_text = _norm(_extract_text(target.group(0)))
            if target_idx != label_idx and target_text and target_text not in {_norm(x) for x in EXACT_PLACEHOLDERS}:
                continue
            new_target = _inject_marker_into_cell(target.group(0))
            new_row = row[:target.start()] + new_target + row[target.end():]
            result = result[:row_match.start()] + new_row + result[row_match.end():]
            inserted = True
            break
        if inserted:
            continue

        # Compact paragraphs such as "УНП: —" or "Область применения:".
        paragraphs = list(re.finditer(r'<w:p(?:\s[^>]*)?>.*?</w:p>', result, flags=re.S))
        for p_match in paragraphs:
            paragraph = p_match.group(0)
            text = _extract_text(paragraph)
            norm = _norm(text)
            if not any(lbl in norm for lbl in label_norms):
                continue
            if _block_has_value_after_label(text, labels):
                continue
            new_p = _inject_marker_into_paragraph(paragraph)
            result = result[:p_match.start()] + new_p + result[p_match.end():]
            inserted = True
            break
    return result

def highlight_docx(
    docx_bytes: bytes,
    review_tokens: Sequence[str],
    missing_items: Sequence[Dict[str, str]] | None = None,
) -> bytes:
    """Highlight doubtful values and write missing markers directly in Word fields."""
    if not docx_bytes:
        return docx_bytes
    try:
        source = zipfile.ZipFile(io.BytesIO(docx_bytes), 'r')
    except zipfile.BadZipFile:
        return docx_bytes

    output = io.BytesIO()
    with source, zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as target:
        for info in source.infolist():
            payload = source.read(info.filename)
            if (info.filename.startswith('word/') and info.filename.endswith('.xml') and
                    any(part in info.filename for part in ('document.xml', 'header', 'footer', 'footnotes', 'endnotes'))):
                try:
                    xml = payload.decode('utf-8')
                    xml = _inject_missing_items(xml, missing_items or [])
                    payload = _highlight_xml(xml, review_tokens).encode('utf-8')
                except Exception:
                    pass
            target.writestr(info, payload)
    return output.getvalue()


def _review_docx(items: Sequence[Dict[str, str]], product: str) -> bytes:
    """Create a compact yellow checklist included only when review is needed."""
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

    def esc(value: Any) -> str:
        return html.escape(str(value or ''), quote=False)

    paras = [
        '<w:p><w:pPr><w:jc w:val="center"/></w:pPr><w:r><w:rPr><w:b/><w:sz w:val="28"/></w:rPr><w:t>ПРОВЕРКА ДАННЫХ ПЕРЕД ПОДАЧЕЙ</w:t></w:r></w:p>',
        f'<w:p><w:r><w:t>Продукт: {esc(product)}</w:t></w:r></w:p>',
        '<w:p><w:r><w:t>Жёлтым отмечены сведения, которые отсутствуют, распознаны неуверенно или требуют сверки с оригиналом.</w:t></w:r></w:p>',
    ]
    for idx, item in enumerate(items, 1):
        text = f"{idx}. {item.get('field','данные')}: {item.get('value','НЕ ЗАПОЛНЕНО')} — {item.get('reason','требует проверки')}"
        paras.append(
            '<w:p><w:r><w:rPr>' + YELLOW_TAG + '<w:sz w:val="24"/></w:rPr>'
            f'<w:t xml:space="preserve">{esc(text)}</w:t></w:r></w:p>'
        )
    document = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>
{''.join(paras)}
<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1134" w:right="850" w:bottom="1134" w:left="1701"/></w:sectPr>
</w:body></w:document>'''
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types)
        zf.writestr('_rels/.rels', rels)
        zf.writestr('word/document.xml', document)
        zf.writestr('word/_rels/document.xml.rels', word_rels)
    return buf.getvalue()


def _docx_plain_text(docx_bytes: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(docx_bytes), 'r') as zf:
            chunks = []
            for name in zf.namelist():
                if name.startswith('word/') and name.endswith('.xml') and any(
                    part in name for part in ('document.xml', 'header', 'footer', 'footnotes', 'endnotes')
                ):
                    xml = zf.read(name).decode('utf-8', errors='ignore')
                    chunks.append(_extract_text(xml))
            return '\n'.join(chunks)
    except Exception:
        return ''


def _scan_rendered_documents(docs: Sequence[Dict[str, Any]], data: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, str]]]:
    """Find unresolved template data after rendering.

    A copied sample-company name or an unresolved bracket placeholder is neither
    confirmed nor acceptable. It is therefore highlighted and listed for review.
    """
    tokens: List[str] = []
    items: List[Dict[str, str]] = []
    current_name = _norm((data.get('company') or {}).get('name'))
    placeholder_re = re.compile(
        r'\[(?:[^\]]*(?:наименование|форма_собственности|фио|унп|адрес|дата|номер|заполнить|указать)[^\]]*)\]',
        flags=re.I,
    )
    for doc in docs or []:
        name = str(doc.get('name') or '')
        payload = doc.get('bytes')
        if not name.lower().endswith('.docx') or not isinstance(payload, (bytes, bytearray)):
            continue
        text = _docx_plain_text(bytes(payload))
        if not text:
            continue
        for marker in STALE_TEMPLATE_MARKERS:
            if marker in text and _norm(marker) != current_name:
                tokens.append(marker)
                items.append({
                    'field': name,
                    'value': marker,
                    'reason': 'в документе осталось название компании из шаблона',
                })
        for placeholder in sorted(set(placeholder_re.findall(text))):
            tokens.append(placeholder)
            items.append({
                'field': name,
                'value': placeholder,
                'reason': 'неразрешённый шаблонный маркер',
            })
    return tokens, items


def apply_package_review(
    docs: Sequence[Dict[str, Any]],
    data: Dict[str, Any],
    product: str,
    generator_warnings: Sequence[str] | None = None,
) -> Tuple[List[Dict[str, Any]], List[str], List[Dict[str, str]]]:
    """Apply universal review rules to every DOCX in a package."""
    review_tokens, items = collect_review_tokens_and_items(data)
    items.extend(collect_required_items(data, product))
    rendered_tokens, rendered_items = _scan_rendered_documents(docs, data)
    review_tokens.extend(rendered_tokens)
    items.extend(rendered_items)
    for warning in generator_warnings or []:
        if warning:
            items.append({'field': 'генератор', 'value': str(warning), 'reason': 'предупреждение формирования'})

    # De-duplicate items.
    unique: List[Dict[str, str]] = []
    seen = set()
    for item in items:
        key = (_norm(item.get('field')), _norm(item.get('value')), _norm(item.get('reason')))
        if key not in seen:
            seen.add(key)
            unique.append(item)
    items = unique

    # Values from review items are also exact tokens when they are not generic placeholders.
    for item in items:
        value = str(item.get('value') or '').strip()
        if value and _norm(value) not in {_norm(x) for x in EXACT_PLACEHOLDERS} and value != 'НЕ ЗАПОЛНЕНО':
            review_tokens.append(value)

    processed: List[Dict[str, Any]] = []
    for doc in docs or []:
        copy = dict(doc)
        name = str(copy.get('name') or '')
        if name.lower().endswith('.docx') and isinstance(copy.get('bytes'), (bytes, bytearray)):
            copy['bytes'] = highlight_docx(bytes(copy['bytes']), review_tokens, items)
        processed.append(copy)

    # Review information is embedded in the original documents themselves.
    # A separate checklist is intentionally not added.

    warnings = [
        f"{item['field']}: {item['value']} ({item['reason']})"
        for item in items
    ]
    return processed, warnings, items
