"""
Генератор документов ИСО 9001 / СУОТ ISO 45001 — НАСТОЯЩИЕ шаблоны, универсальный движок.

В отличие от company_att/att/spk (где под каждый документ — своя функция с точным
разбором абзацев), здесь используется ОБЩИЙ движок глобальной замены: подавляющее
большинство из 153 документов ИСО/СУОТ — это компания + несколько именованных людей
(директор, гл.специалисты) внутри в основном стандартного текста. Резонно и безопасно
менять эти конкретные значения глобально по всему документу, не разбирая каждый абзац
вручную — экономит время на 150+ документах, при этом сохраняя 100% форматирования
оригинала (правим только текст, не структуру).

Требует: iso_suot_templates/*.docx рядом с этим файлом.
"""
import re, io, zipfile
from pathlib import Path

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


def _paragraphs(xml: str) -> list:
    return re.findall(r'<w:p\b[^>]*?/>|<w:p\b[^>]*>.*?</w:p>', xml, re.DOTALL)


def _esc(s) -> str:
    return (str(s) if s not in (None, '') else '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _replace_para_text(para_xml: str, new_text: str) -> str:
    m = re.search(r'(<w:r\b[^P].*?)<w:t[^>]*>.*?</w:t>(.*?</w:r>)', para_xml, re.DOTALL)
    if not m:
        return para_xml
    run_prefix, run_suffix = m.group(1), m.group(2)
    new_run = f'{run_prefix}<w:t xml:space="preserve">{_esc(new_text)}</w:t>{run_suffix}'
    if '</w:pPr>' in para_xml:
        p_open_end = para_xml.find('</w:pPr>') + len('</w:pPr>')
    else:
        p_open_end = para_xml.find('>') + 1
    return para_xml[:p_open_end] + new_run + '</w:p>'


def _find_para_index(paras: list, predicate) -> int:
    for i, p in enumerate(paras):
        text = re.sub(r'<[^>]+>', '', p).strip().replace('\xa0', ' ')
        if predicate(text):
            return i
    return -1


def _dir_initials(fio: str) -> str:
    """'Иванов Иван Иванович' -> 'И.И. Иванов' (как подписывает Варта: инициалы
    сначала)."""
    parts = (fio or '').strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio or ''


# ═══════════════════ Работа с таблицами (клонирование строк, где нужно) ═══════════════════
def _rows(xml: str) -> list:
    return re.findall(r'<w:tr\b[^>]*?/>|<w:tr\b[^>]*>.*?</w:tr>', xml, re.DOTALL)


def _cells(row_xml: str) -> list:
    return re.findall(r'<w:tc\b.*?</w:tc>', row_xml, re.DOTALL)


def _replace_cell_content(cell_xml: str, lines: list) -> str:
    paras_in_cell = re.findall(r'<w:p\b[^>]*?/>|<w:p\b[^>]*>.*?</w:p>', cell_xml, re.DOTALL)
    if not paras_in_cell:
        return cell_xml
    style_para = paras_in_cell[0]
    tc_pr_match = re.match(r'(<w:tc\b.*?</w:tcPr>)', cell_xml, re.DOTALL)
    tc_pr = tc_pr_match.group(1) if tc_pr_match else cell_xml[:cell_xml.find('<w:p')]
    lines = [l for l in lines if l] or ['—']
    new_paras = ''.join(_replace_para_text(style_para, line) for line in lines)
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
    first, last = old_rows_slice[0], old_rows_slice[-1]
    start = xml.find(first)
    end = xml.find(last) + len(last)
    return xml[:start] + ''.join(new_rows) + xml[end:]


# ═══════════════════ УНИВЕРСАЛЬНЫЙ движок: глобальная замена компании + людей ═══════════════════
def render_generic(template_file: str, company_old: dict, company_new: dict,
                    people_map: dict, extra_replacements: dict = None) -> bytes:
    """
    Базовая функция для документов, где достаточно ГЛОБАЛЬНОЙ замены.

    company_old/company_new: словари с ключами 'name','city','unp','address',
    'bank_account','bank_name','postal_code','region' — КАЖДОЕ поле, которое
    реально встречается в реквизитах компании в шапках документов, должно быть
    заменено, а не только название. Утечка адреса/УНП/банковских реквизитов
    старой компании — то же самое нарушение, что и утечка чужого имени.

    - people_map: {старая_фамилия: новое_ФИО_или_фамилия}
    - extra_replacements: {точная_старая_строка: новая_строка} — для дат, номеров
      приказов и т.п.
    """
    parts = _load_parts(template_file)
    xml = parts['word/document.xml'].decode('utf-8')

    if isinstance(company_old, str):
        # обратная совместимость со старым вызовом (только имя) — но теперь
        # ПРЕДУПРЕЖДАЕМ, так как это неполно
        print(f"  ⚠️ {template_file}: render_generic вызван только с именем компании — "
              f"адрес/УНП/банк НЕ заменяются, используйте словарь company_old/company_new!")
        xml = xml.replace(company_old, company_new)
    else:
        for key in ['name', 'city', 'unp', 'address', 'street', 'bank_account', 'bank_name', 'postal_code', 'region']:
            old_val = company_old.get(key)
            new_val = company_new.get(key)
            if old_val and new_val:
                xml = xml.replace(old_val, new_val)

    for old_name, new_name in (people_map or {}).items():
        xml = xml.replace(old_name, new_name)

    for old_str, new_str in (extra_replacements or {}).items():
        xml = xml.replace(old_str, new_str)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Адаптер для реального пайплайна (generator.py) ═══════════════════
import json as _json

_MANIFEST_PATH = TPL_DIR / 'manifest.json'
_MANIFEST = _json.loads(_MANIFEST_PATH.read_text('utf-8')) if _MANIFEST_PATH.exists() else {}

# Реальные реквизиты компании-образца (Варта), зашитые во все 155 шаблонов —
# единая точка правды, чтобы не дублировать в разных вызовах.
_OLD_COMPANY = {
    'name': 'Варта', 'city': 'г. Лида', 'unp': '500381571',
    'street': 'ул. Лётная, 7', 'bank_account': 'BY69AKBB30120000301344200000',
    'bank_name': 'АСБ «Беларусбанк»', 'postal_code': '231282', 'region': 'Гродненская обл.',
}
_OLD_PEOPLE = {
    'Василенко': 'С.Ф.', 'Кормилицин': 'П.А.', 'Вершалович': ['А.П.', 'А.М.'],
}
# СУОТ-файлы определяются по префиксу ключа в манифесте; converted_1..16 (кроме 17,18) тоже СУОТ.
_SUOT_CONVERTED_IDX = set(range(1, 17))


def _category_of(key: str) -> str:
    if key.startswith('suot_'):
        return 'suot'
    if key.startswith('converted_'):
        idx = int(key.split('_')[1].split('.')[0])
        return 'suot' if idx in _SUOT_CONVERTED_IDX else 'iso'
    return 'iso'


def generate_iso_suot_package_v2(company: dict, itr: list, dates: dict, resp: dict,
                                  product: str = 'iso_suot', progress_cb=None) -> dict:
    """
    company: {name, form, unp, address, city, phone, email, director_fio, ...}
    itr: список [{fio, position}]
    dates: результат calculate_dates() (не используется напрямую — реквизиты в шаблонах
           почти не зависят от дат, в отличие от старого ИИ-генератора)
    resp: результат select_responsible(itr) — director/process_resp/auditors и т.д.
    product: 'iso' | 'suot' | 'iso_suot' — какие категории файлов включать
    """
    org = company.get('name', 'company')
    director_fio = company.get('director_fio', '') or (resp.get('director') or {}).get('fio', '')

    # Разбираем реальное ФИО директора на фамилию+инициалы для замены
    d_parts = (director_fio or '').strip().split()
    dir_surname = d_parts[0] if d_parts else ''
    dir_initials = ('.'.join(p[0] for p in d_parts[1:] if p) + '.') if len(d_parts) > 1 else ''

    # Два дополнительных человека (гл.инженер/специалисты), упоминаемых в некоторых
    # приказах — берём следующих по списку ИТР после директора, если они есть.
    others = [p for p in itr if p.get('fio') != director_fio][:2]
    other_names = []
    for p in others:
        parts = (p.get('fio') or '').strip().split()
        if parts:
            surname = parts[0]
            inits = ('.'.join(x[0] for x in parts[1:] if x) + '.') if len(parts) > 1 else ''
            other_names.append((surname, inits))

    company_new = {
        'name': org,
        'city': f"г. {company.get('city','')}" if company.get('city') and not company.get('city','').startswith('г.') else company.get('city',''),
        'unp': company.get('unp', ''),
        'street': company.get('address', ''),
        'bank_account': company.get('bank_account', ''),
        'bank_name': company.get('bank_name', ''),
        'postal_code': company.get('postal_code', ''),
        'region': company.get('region', ''),
    }

    people_map = {}
    if dir_surname:
        people_map['Василенко'] = dir_surname
    if dir_initials:
        people_map['С.Ф.'] = dir_initials
    if len(other_names) >= 1:
        people_map['Кормилицин'] = other_names[0][0]
        if other_names[0][1]:
            people_map['П.А.'] = other_names[0][1]
    if len(other_names) >= 2:
        people_map['Вершалович'] = other_names[1][0]
        if other_names[1][1]:
            people_map['А.П.'] = other_names[1][1]
            people_map['А.М.'] = other_names[1][1]

    wanted_categories = {'iso_suot': {'iso', 'suot'}, 'iso': {'iso'}, 'suot': {'suot'}}.get(product, {'iso', 'suot'})
    keys = sorted(k for k in _MANIFEST if _category_of(k) in wanted_categories)

    docs = []
    total = len(keys) or 1
    for i, key in enumerate(keys, 1):
        friendly = _MANIFEST[key]
        category = _category_of(key)
        prefix = f"{org} СУОТ" if category == 'suot' else org
        out_name = f"{prefix} - {friendly}.docx"
        if progress_cb:
            progress_cb(i, total, f"{friendly[:50]}")
        try:
            data = render_generic(key, _OLD_COMPANY, company_new, people_map)
            docs.append({'name': out_name, 'bytes': data})
        except Exception as e:
            print(f"  ❌ Ошибка генерации {key} ({friendly}): {e}")

    return {'docs': docs, 'warnings': []}
