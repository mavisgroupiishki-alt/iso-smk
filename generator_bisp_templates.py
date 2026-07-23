"""
Генератор документов СПК БИСП (входной контроль) — дополнение к СПК Строй.

12 из 19 документов БИСП структурно идентичны СПК Строй (см. generator_spk_templates.py)
и переиспользуют те же функции. Этот модуль — только 7 документов, специфичных для БИСП:
гарантийные письма (9.1, 9.3, 9.6), план внутреннего аудита, план-график поверки СИ,
перечень продукции подлежащей входному контролю, положение о входном контроле.

Требует: bisp_templates/*.docx рядом с этим файлом.
"""
import re, io, zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
TPL_DIR = BASE_DIR / 'bisp_templates'


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
    parts = (fio or '').strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio or ''


def _import_declension():
    import importlib.util
    spec = importlib.util.spec_from_file_location('att_tpl', str(BASE_DIR / 'generator_att_templates.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.decline_fio

decline_fio = _import_declension()  # родительный падеж = винительный для мужских одушевлённых имён


# ═══════════════════ Гарантийные письма (9.1, 9.3, 9.6) — общая логика ═══════════════════
def _render_garantiya(template_file: str, company: dict, director_fio: str,
                       letter_number: str, letter_date: str, recipient: str) -> bytes:
    parts = _load_parts(template_file)
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    dir_init = _dir_initials(director_fio)
    dir_gen = decline_fio(director_fio, 'acc')  # родительный = винительный для муж. одуш.

    idx_company = 0
    idx_number = _find_para_index(paras, lambda t: t.startswith('Исх. №'))
    idx_date = _find_para_index(paras, lambda t: t.startswith('от '))
    idx_recipient = _find_para_index(paras, lambda t: t.startswith('РУП') or t.startswith('УП') or t.startswith('ООО'))
    idx_body = _find_para_index(paras, lambda t: t.startswith('Настоящим'))

    if paras[idx_company] in xml:
        xml = xml.replace(paras[idx_company], _replace_para_text(paras[idx_company], full_name), 1)
    if idx_number >= 0 and paras[idx_number] in xml:
        xml = xml.replace(paras[idx_number], _replace_para_text(paras[idx_number], f"Исх. № {letter_number}"), 1)
    if idx_date >= 0 and paras[idx_date] in xml:
        xml = xml.replace(paras[idx_date], _replace_para_text(paras[idx_date], f"от {letter_date} г."), 1)
    if idx_recipient >= 0 and paras[idx_recipient] in xml and recipient:
        xml = xml.replace(paras[idx_recipient], _replace_para_text(paras[idx_recipient], recipient), 1)
    if idx_body >= 0 and paras[idx_body] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_body]).strip().replace('\xa0', ' ')
        # Заменяем "ООО «Компания»" целиком на новую
        new_t = re.sub(r'ООО\s*«[^»]+»', full_name, old_t)
        # Заменяем "директора ... ," (всё между "директора" и следующей запятой) целиком
        new_t = re.sub(r'директора\s+[^,]+,', f'директора {dir_gen},', new_t, count=1)
        xml = xml.replace(paras[idx_body], _replace_para_text(paras[idx_body], new_t), 1)

    idx_sig = _find_para_index(paras, lambda t: t.startswith('Директор') and '_' in t)
    if idx_sig >= 0 and paras[idx_sig] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_sig]).strip().replace('\xa0', ' ')
        # В этих письмах подпись в формате "И.О. Фамилия" (инициалы сначала) —
        # другой порядок, чем в остальных документах СПК ("Фамилия И.О.")
        new_t = re.sub(r'[А-ЯЁ]\.\s*[А-ЯЁ]\.\s*[А-ЯЁ][а-яё]+\s*$', dir_init, old_t)
        xml = xml.replace(paras[idx_sig], _replace_para_text(paras[idx_sig], new_t), 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


def render_garantiya_ttk(company, director_fio, letter_number, letter_date, recipient='РУП «Белстройцентр»'):
    """Гарантийное письмо на приобретение ТТК."""
    return _render_garantiya('1_garantiya_ttk.docx', company, director_fio, letter_number, letter_date, recipient)


def render_garantiya_labs(company, director_fio, letter_number, letter_date, recipient='РУП «Белстройцентр»'):
    """Гарантийное письмо о заключении договоров с лабораториями."""
    return _render_garantiya('2_garantiya_labs.docx', company, director_fio, letter_number, letter_date, recipient)


def render_garantiya_reklamacii(company, director_fio, letter_number, letter_date, recipient='РУП «Белстройцентр»'):
    """Гарантийное письмо об отсутствии претензий по качеству."""
    return _render_garantiya('3_garantiya_reklamacii.docx', company, director_fio, letter_number, letter_date, recipient)


# ═══════════════════ Работа с таблицами (клонирование строк) ═══════════════════
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


# ═══════════════════ Документ 4: План внутреннего аудита ═══════════════════
def render_plan_audita(company: dict, director_fio: str, year: str, audit_dates: list = None) -> bytes:
    """
    7 стандартных пунктов аудита (фиксированные темы, не меняются от компании к
    компании) — руководитель группы по аудиту всегда директор. audit_dates:
    список из 7 дат (строками), если не передан — используется "__.__.{year}".
    """
    parts = _load_parts('4_plan_audita.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    dir_init = _dir_initials(director_fio)

    idx_company = _find_para_index(paras, lambda t: t.startswith('ООО') or t.startswith('ОДО') or t.startswith('ЧУП'))
    idx_sig = _find_para_index(paras, lambda t: t.startswith('_____'))
    idx_year = _find_para_index(paras, lambda t: t.startswith('на ') and 'г.' in t)

    if idx_company >= 0 and paras[idx_company] in xml:
        xml = xml.replace(paras[idx_company], _replace_para_text(paras[idx_company], full_name), 1)
    if idx_sig >= 0 and paras[idx_sig] in xml:
        xml = xml.replace(paras[idx_sig], _replace_para_text(paras[idx_sig], f"______________ {dir_init}"), 1)
    if idx_year >= 0 and paras[idx_year] in xml:
        xml = xml.replace(paras[idx_year], _replace_para_text(paras[idx_year], f"на {year} г."), 1)

    rows = _rows(xml)
    template_row = rows[1]
    default_topics = [
        "Степень готовности системы производственного контроля к оценке",
        "Проверка наличия и состояния фонда ТНПА, ТК",
        "Проверка наличия и состояния средств измерений",
        "Оценка системы регистрации результатов контроля качества",
        "Проверка соблюдения требований Положения о системе производственного контроля",
        "Проверка соблюдения требований Положения о входном контроле",
        "Анализ работы за год. Наличие жалоб от Заказчиков",
    ]
    dates = audit_dates or ['__.__.' + year] * 7
    new_rows = []
    for i, (topic, date) in enumerate(zip(default_topics, dates), 1):
        cell_values = [str(i), topic, [dir_init, ', Директор'], f"{date} г."]
        new_rows.append(_build_row(template_row, cell_values))
    xml = _splice_rows(xml, rows[1:], new_rows)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 5: Положение о входном контроле ═══════════════════
def render_polozhenie_vhod(company: dict, director_fio: str) -> bytes:
    """Стандартный регламентный документ (189 абзацев) — почти без переменных
    данных, только название компании (3 упоминания) и подпись директора (1)."""
    parts = _load_parts('5_polozhenie_vhod.docx')
    xml = parts['word/document.xml'].decode('utf-8')

    old_company = 'Кастом-Инвест'
    new_company = company.get('name', '')
    xml = xml.replace(old_company, new_company)

    dir_init = _dir_initials(director_fio)
    paras = _paragraphs(xml)
    idx_sig = _find_para_index(paras, lambda t: 'Юковец' in t)
    if idx_sig >= 0 and paras[idx_sig] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_sig]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'[А-ЯЁ]\.\s*[А-ЯЁ]\.\s*[А-ЯЁ][а-яё]+\s*$', dir_init, old_t)
        xml = xml.replace(paras[idx_sig], _replace_para_text(paras[idx_sig], new_t), 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 6: План-график поверки СИ ═══════════════════
def render_grafik_poverki(company: dict, director_fio: str, year: str = None) -> bytes:
    """Стандартный справочный график поверки средств измерений (313 абзацев,
    21 строка таблиц) — компания нигде не упоминается напрямую, меняется только
    подпись директора."""
    parts = _load_parts('6_grafik_poverki.docx')
    xml = parts['word/document.xml'].decode('utf-8')

    dir_init = _dir_initials(director_fio)
    paras = _paragraphs(xml)
    idx_sig = _find_para_index(paras, lambda t: 'Юковец' in t)
    if idx_sig >= 0 and paras[idx_sig] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_sig]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'[А-ЯЁ]\.\s*[А-ЯЁ]\.\s*[А-ЯЁ][а-яё]+\s*$', dir_init, old_t)
        xml = xml.replace(paras[idx_sig], _replace_para_text(paras[idx_sig], new_t), 1)
    if year:
        idx_year = _find_para_index(paras, lambda t: 'на 2026' in t or ('на 20' in t and 'г' in t))
        if idx_year >= 0 and paras[idx_year] in xml:
            old_t = re.sub(r'<[^>]+>', '', paras[idx_year]).strip().replace('\xa0', ' ')
            new_t = re.sub(r'20\d\d', year, old_t, count=1)
            xml = xml.replace(paras[idx_year], _replace_para_text(paras[idx_year], new_t), 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 7: Перечень продукции, подлежащей входному контролю ═══════════════════
def render_perechen_produkcii(company: dict, director_fio: str) -> bytes:
    """Стандартный отраслевой перечень (399 абзацев, 38 строк таблиц) — минимум
    переменных данных: название компании (1 упоминание) и подпись директора (1)."""
    parts = _load_parts('7_perechen_produkcii.docx')
    xml = parts['word/document.xml'].decode('utf-8')

    old_company = 'Кастом-Инвест'
    new_company = company.get('name', '')
    xml = xml.replace(old_company, new_company)

    dir_init = _dir_initials(director_fio)
    paras = _paragraphs(xml)
    idx_sig = _find_para_index(paras, lambda t: 'Юковец' in t)
    if idx_sig >= 0 and paras[idx_sig] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_sig]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'[А-ЯЁ]\.\s*[А-ЯЁ]\.\s*[А-ЯЁ][а-яё]+\s*$', dir_init, old_t)
        xml = xml.replace(paras[idx_sig], _replace_para_text(paras[idx_sig], new_t), 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)
