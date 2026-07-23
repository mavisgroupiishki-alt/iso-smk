"""
Генератор документов СПК (Строй/БИСП) — НАСТОЯЩИЕ шаблоны.

Тот же принцип, что для company_att и att: берём реальный поданный docx
(Сфера Секьюрити — СПК Строй, Кастом-Инвест — СПК БИСП) буквально как есть.

Требует: spk_templates/*.docx рядом с этим файлом.

СТАТУС: в разработке — начато с "1. Условия в производственных помещениях".
Остальные документы (Справка ИТР, Оргструктура, приказы 4.1-4.4, Положение,
Паспорт, Справка ТТК/СИ, гарантийные письма БИСП) добавляются по одному,
каждый с тем же уровнем проверки.
"""
import re, io, zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
TPL_DIR = BASE_DIR / 'spk_templates'


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
    # Регекс должен отдельно матчить самозакрывающиеся пустые абзацы <w:p .../>
    # (без отдельного </w:p>) — иначе они склеиваются со следующим реальным
    # абзацем в один "абзац", что ломает точечную замену текста.
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
        # Word часто ставит НЕРАЗРЫВНЫЙ пробел (\xa0) после "1." в нумерованных
        # пунктах вместо обычного — без нормализации поиск по .startswith() тихо
        # не находит абзац, и замена данных просто не происходит.
        text = re.sub(r'<[^>]+>', '', p).strip().replace('\xa0', ' ')
        if predicate(text):
            return i
    return -1


def _dir_initials(fio: str) -> str:
    parts = (fio or '').strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio or ''


# ═══════════════════ Документ 1: Условия в производственных помещениях ═══════════════════
def render_usloviya(company: dict) -> bytes:
    """
    Таблица требований к помещениям — СТАНДАРТНАЯ (одинаковая у всех заявителей,
    это требования ТНПА, а не данные конкретной компании). Меняется только
    подпись директора внизу.
    company: {director_fio}
    """
    parts = _load_parts('1_usloviya.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)

    dir_init = _dir_initials(company.get('director_fio', ''))
    idx_sig = _find_para_index(paras, lambda t: t.startswith('Директор'))
    if idx_sig >= 0:
        old_para = paras[idx_sig]
        new_para = _replace_para_text(old_para, f"Директор       _____________ {dir_init}")
        xml = xml.replace(old_para, new_para, 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Склонение ФИО (переиспользуем логику из att_templates) ═══════════════════
def _import_declension():
    import importlib.util
    spec = importlib.util.spec_from_file_location('att_tpl', str(BASE_DIR / 'generator_att_templates.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.decline_fio

decline_fio = _import_declension()


def _fio_initials_surname_first(fio: str) -> str:
    """'Туник Дмитрий Иванович' -> 'Туника Д.И.' (родительный, инициалы после)."""
    parts = (fio or '').strip().split()
    if len(parts) < 3:
        return fio
    surname_gen = decline_fio(parts[0] + ' ' + parts[1] + ' ' + parts[2], 'acc').split()[0]
    return f"{surname_gen} {parts[1][0]}.{parts[2][0]}."


def _fio_initials_dative(fio: str) -> str:
    parts = (fio or '').strip().split()
    if len(parts) < 3:
        return fio
    declined = decline_fio(fio, 'dat').split()
    return f"{declined[0]} {parts[1][0]}.{parts[2][0]}."




def _replace_director_signature(xml: str, paras: list, dir_init: str) -> str:
    """Находит абзац подписи директора по маркеру 'Директор' в конце документа и
    меняет ФИО, сохраняя формат подписи (пробелы/подчёркивания как в оригинале)."""
    idx = None
    for i in range(len(paras) - 1, -1, -1):
        t = re.sub(r'<[^>]+>', '', paras[i]).strip()
        if t.startswith('Директор') and '_' in t:
            idx = i
            break
    if idx is None or paras[idx] not in xml:
        return xml
    old_text = re.sub(r'<[^>]+>', '', paras[idx]).strip()
    # Заменяем только ФИО в конце строки (после последней группы подчёркиваний/пробелов)
    new_text = re.sub(r'[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.\s*$', dir_init, old_text)
    if new_text == old_text:  # маска не сработала — подставим в конец
        new_text = re.sub(r'\S+\s*$', dir_init, old_text)
    return xml.replace(paras[idx], _replace_para_text(paras[idx], new_text), 1)


# ═══════════════════ Документ 2: Приказ о СПК (назначение ответственных) ═══════════════════
def render_prikaz_spk(company: dict, order_number: str, order_date: str, city: str,
                       director_fio: str, gl_inzhener_fio: str, foremen_fio: list) -> bytes:
    """
    director_fio: ФИО директора (обязателен, всегда в списке).
    gl_inzhener_fio: ФИО главного инженера (может быть пустым, если нет такой роли).
    foremen_fio: список ФИО производителей работ (прорабов) — минимум 1.
    Ответственности за каждой ролью — стандартные (из оригинала), меняются только ФИО.
    """
    parts = _load_parts('2_prikaz_spk.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    dir_init = _dir_initials(director_fio)

    replacements = {0: full_name, 4: f"{order_date} № {order_number}", 6: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)

    # --- Список лиц, задействованных в СПК (абзацы 16-19 в образце): по одному на
    #     каждого человека, формат "Фамилия И.О., Должность" (родительный падеж) ---
    idx_list_start = _find_para_index(paras, lambda t: t.startswith('2. В системе'))
    idx_resp_dir = _find_para_index(paras, lambda t: t.startswith('3. Директора'))
    if idx_list_start >= 0 and idx_resp_dir >= 0:
        people = [(director_fio, 'Директора')]
        if gl_inzhener_fio:
            people.append((gl_inzhener_fio, 'Главного инженера'))
        for f in foremen_fio:
            people.append((f, 'Производителя работ'))
        style_line = paras[idx_list_start + 1]
        new_lines = []
        for fio, pos_gen in people:
            surname_gen = _fio_initials_surname_first(fio)
            new_lines.append(_replace_para_text(style_line, f"{surname_gen}, {pos_gen}"))
        old_block = ''.join(paras[idx_list_start + 1: idx_resp_dir])
        if old_block in xml:
            xml = xml.replace(old_block, ''.join(new_lines), 1)

    # --- Абзацы ответственности: 3. Директора X ... (20), 4. Гл.инженера Y ... (21-22),
    #     5. Производителей работ Z1, Z2 ... (23-24) — меняем только ФИО в начале фразы ---
    idx3 = _find_para_index(paras, lambda t: t.startswith('3. Директора'))
    idx4 = _find_para_index(paras, lambda t: t.startswith('4. Главного инженера'))
    idx5 = _find_para_index(paras, lambda t: t.startswith('5. Производителей работ') or
                                               t.startswith('5. Производителя работ'))
    if idx3 >= 0 and paras[idx3] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx3]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Директора [^\s]+ [^\s]+', f'Директора {_fio_initials_surname_first(director_fio)}', old_t, count=1)
        xml = xml.replace(paras[idx3], _replace_para_text(paras[idx3], new_t), 1)
    if idx4 >= 0 and paras[idx4] in xml and gl_inzhener_fio:
        old_t = re.sub(r'<[^>]+>', '', paras[idx4]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Главного инженера [^\s]+ [^\s]+', f'Главного инженера {_fio_initials_surname_first(gl_inzhener_fio)}', old_t, count=1)
        xml = xml.replace(paras[idx4], _replace_para_text(paras[idx4], new_t), 1)
    if idx5 >= 0 and paras[idx5] in xml and foremen_fio:
        if len(foremen_fio) == 1:
            new_t = f"5. Производителя работ {_fio_initials_surname_first(foremen_fio[0])}, назначить ответственным за Входной, операционный, приемочный контроль;"
        else:
            names_part = ', '.join(f"производителя работ {_fio_initials_surname_first(f)}" for f in foremen_fio)
            new_t = f"5. {names_part}, назначить ответственными за Входной, операционный, приемочный контроль;"
        xml = xml.replace(paras[idx5], _replace_para_text(paras[idx5], new_t), 1)

    xml = _replace_director_signature(xml, paras, dir_init)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 3: Приказ о внутреннем обучении ═══════════════════
def render_prikaz_obuchenie(company: dict, order_number: str, order_date: str, city: str,
                             director_fio: str, deadline_date: str) -> bytes:
    parts = _load_parts('3_prikaz_obuchenie.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    dir_init = _dir_initials(director_fio)

    idx_deadline = _find_para_index(paras, lambda t: t.startswith('2. До'))
    replacements = {0: full_name, 4: f"{order_date} № {order_number}", 6: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)
    if idx_deadline >= 0 and paras[idx_deadline] in xml:
        old_text = re.sub(r'<[^>]+>', '', paras[idx_deadline]).strip()
        new_text = re.sub(r'\d{2}\.\d{2}\.\d{4}', deadline_date, old_text, count=1)
        xml = xml.replace(paras[idx_deadline], _replace_para_text(paras[idx_deadline], new_text), 1)

    xml = _replace_director_signature(xml, paras, dir_init)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 4: Приказ о ТО средств измерений ═══════════════════
def render_prikaz_to_si(company: dict, order_number: str, order_date: str, city: str,
                         director_fio: str, responsible_fio: str) -> bytes:
    parts = _load_parts('4_prikaz_to_si.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    dir_init = _dir_initials(director_fio)
    resp_acc = _fio_initials_surname_first(responsible_fio)   # "Туника Д.И."
    resp_dat = _fio_initials_dative(responsible_fio)          # "Тунику Д.И."

    replacements = {0: full_name, 4: f"{order_date} № {order_number}", 6: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)

    idx1 = _find_para_index(paras, lambda t: t.startswith('1. Производителя работ'))
    idx2 = _find_para_index(paras, lambda t: t.startswith('2. Производителю работ'))
    if idx1 >= 0 and paras[idx1] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx1]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Производителя работ [^\s]+ [^\s]+', f'Производителя работ {resp_acc}', old_t, count=1)
        xml = xml.replace(paras[idx1], _replace_para_text(paras[idx1], new_t), 1)
    if idx2 >= 0 and paras[idx2] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx2]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Производителю работ [^\s]+ [^\s]+', f'Производителю работ {resp_dat}', old_t, count=1)
        xml = xml.replace(paras[idx2], _replace_para_text(paras[idx2], new_t), 1)

    xml = _replace_director_signature(xml, paras, dir_init)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 5: Приказ о назначении ответственного за машины ═══════════════════
def render_prikaz_mashiny(company: dict, order_number: str, order_date: str, city: str,
                           director_fio: str, responsible_fio: str) -> bytes:
    parts = _load_parts('5_prikaz_mashiny.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    resp_acc = _fio_initials_surname_first(responsible_fio)
    dir_init = _dir_initials(director_fio)

    replacements = {0: full_name, 3: f"{order_date} № {order_number}", 5: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)

    idx1 = _find_para_index(paras, lambda t: t.startswith('1. Назначить ответственным'))
    if idx1 >= 0 and paras[idx1] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx1]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Производителя работ [^\s]+ [^\s]+', f'Производителя работ {resp_acc}', old_t, count=1)
        xml = xml.replace(paras[idx1], _replace_para_text(paras[idx1], new_t), 1)

    xml = _replace_director_signature(xml, paras, dir_init)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Работа с таблицами (клонирование строк) ═══════════════════
def _rows(xml: str) -> list:
    return re.findall(r'<w:tr\b.*?</w:tr>', xml, re.DOTALL)


def _cells(row_xml: str) -> list:
    return re.findall(r'<w:tc\b.*?</w:tc>', row_xml, re.DOTALL)


def _replace_cell_content(cell_xml: str, lines: list) -> str:
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


# Стандартные обязанности по ролям — как в приказе о СПК (столбец "Проводимые виды контроля")
ROLE_RESPONSIBILITIES = {
    'директор': "Функционирование СПК; организация проведения внутренних аудитов; входной контроль ПСД; ведение Журнала учета рекламаций по качеству СМР и принятия мер по ним;",
    'главный инженер': "Входной, операционный, приемочный контроль; Обеспечение и содержание в рабочем состоянии машин и механизмов; учет, хранение, актуализация, выдача ТНПА, ТК; метрологическое обеспечение.",
    'производитель работ': "Входной, операционный, приемочный контроль; Обеспечение и содержание в рабочем состоянии машин и механизмов; учет, хранение, актуализация, выдача ТНПА, ТК; метрологическое обеспечение.",
}


# ═══════════════════ Документ 6: Справка ИТР ═══════════════════
def render_spravka_itr(company: dict, people: list) -> bytes:
    """
    people: [{fio, position, education_level, diploma_number, diploma_date,
              diploma_institution, diploma_speciality, diploma_qualification,
              protocol_number, protocol_date, stage_years, trudovaya_number,
              role_key}]  # role_key: 'директор'|'главный инженер'|'производитель работ'
              — для подстановки стандартных обязанностей по столбцу 4.
    """
    parts = _load_parts('6_spravka_itr.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    rows = _rows(xml)
    template_row = rows[1]

    new_rows = []
    for p in people:
        edu = (f"{p.get('education_level','')} Диплом {p.get('diploma_number') or '—'} "
               f"выдан {p.get('diploma_date') or '—'} {p.get('diploma_institution','')} "
               f"{p.get('diploma_speciality','')} {p.get('diploma_qualification','')}")
        role_key = (p.get('role_key') or '').lower()
        responsibility = ROLE_RESPONSIBILITIES.get(role_key, p.get('responsibility', ''))
        protocol = f"Протокол №{p.get('protocol_number','')} от {p.get('protocol_date','')} г." if p.get('protocol_number') else '—'
        extra = f"Стаж – {p.get('stage_years','—')} Трудовая книжка {p.get('trudovaya_number','—')}"
        cell_values = [p.get('fio', ''), p.get('position', ''), edu, responsibility, protocol, extra]
        new_rows.append(_build_row(template_row, cell_values))

    xml = _splice_rows(xml, rows[1:], new_rows)

    paras = _paragraphs(xml)
    dir_fio = next((p.get('fio') for p in people if (p.get('role_key') or '').lower() == 'директор'), '')
    xml = _replace_director_signature(xml, paras, _dir_initials(dir_fio))

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 7: Организационная структура СПК (органиграмма) ═══════════════════
def render_orgstruktura(company: dict, director_fio: str, gl_inzhener_fio: str, foremen_fio: list) -> bytes:
    """
    Графическая схема (текстовые блоки-фигуры) — надёжнее менять глобальной заменой
    известных старых значений на новые, а не по индексу абзаца (содержимое фигур
    в DOCX часто дублируется для старого/нового формата отображения).
    """
    parts = _load_parts('7_orgstruktura.docx')
    xml = parts['word/document.xml'].decode('utf-8')

    old_company = 'Сфера Секьюрити'
    new_company = company.get('name', '')
    xml = xml.replace(old_company, new_company)

    # Старые ФИО в образце (фамилия и имя-отчество идут отдельными run'ами —
    # заменяем как две независимые подстроки, это надёжнее целой фразы)
    old_people = [
        ('Пеганов', 'Владимир Николаевич'),
        ('Артюх', 'Андрей Владимирович'),
        ('Туник', 'Дмитрий Иванович'),
        ('Чернейко', 'Николай Александрович'),
    ]
    new_people = [director_fio, gl_inzhener_fio] + list(foremen_fio)
    for i, (old_surname, old_rest) in enumerate(old_people):
        if i < len(new_people) and new_people[i]:
            parts_new = new_people[i].strip().split()
            new_surname = parts_new[0] if parts_new else old_surname
            new_rest = ' '.join(parts_new[1:]) if len(parts_new) > 1 else old_rest
        else:
            # Слотов в шаблоне больше, чем реальных людей — ОБЯЗАТЕЛЬНО стираем
            # чужие старые ФИО, а не оставляем их (иначе в документе останется
            # реальный человек из другой компании).
            new_surname, new_rest = '—', '—'
        xml = xml.replace(old_surname, new_surname)
        xml = xml.replace(old_rest, new_rest)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 8: Протокол о внутреннем обучении ═══════════════════
def render_protokol_obuchenie(company: dict, protocol_number: str, protocol_date: str,
                               city: str, order_date: str, order_number: str, people: list) -> bytes:
    """people: [{fio, position, result}]  # result по умолчанию "Хорошо" """
    parts = _load_parts('8_protokol_obuchenie.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    LEGAL_FULL = {'ООО':'Общество с ограниченной ответственностью','ОДО':'Общество с дополнительной ответственностью',
                  'ЧУП':'Частное унитарное предприятие','ЗАО':'Закрытое акционерное общество','ОАО':'Открытое акционерное общество'}
    legal_full_name = f'{LEGAL_FULL.get((company.get("form") or "ООО").upper(), "Общество с ограниченной ответственностью")} «{company.get("name","")}»'

    replacements = {0: full_name, 2: f"ПРОТОКОЛ № {protocol_number}", 4: f"{protocol_date} г.", 6: city}
    idx_order_ref = _find_para_index(paras, lambda t: t.startswith('В соответствии с приказом'))
    idx_result = _find_para_index(paras, lambda t: t.startswith('Считать'))
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)
    if idx_order_ref >= 0 and paras[idx_order_ref] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_order_ref]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'от \d{2}\.\d{2}\.\d{4} № \S+', f'от {order_date} № {order_number}', old_t, count=1)
        xml = xml.replace(paras[idx_order_ref], _replace_para_text(paras[idx_order_ref], new_t), 1)
    if idx_result >= 0 and paras[idx_result] in xml:
        new_t = f"Считать {legal_full_name} прошедшим внутреннее обучение."
        xml = xml.replace(paras[idx_result], _replace_para_text(paras[idx_result], new_t), 1)

    rows = _rows(xml)
    template_row = rows[1]
    new_rows = []
    for i, p in enumerate(people, 1):
        cell_values = [str(i), protocol_date + ' г.', p.get('fio', ''), p.get('position', ''), p.get('result', 'Хорошо'), ' ']
        new_rows.append(_build_row(template_row, cell_values))
    xml = _splice_rows(xml, rows[1:], new_rows)

    dir_fio = next((p.get('fio') for p in people if 'директор' in p.get('position','').lower()), '')
    xml = _replace_director_signature(xml, _paragraphs(xml), _dir_initials(dir_fio))

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 9: Положение о системе производственного контроля ═══════════════════
def render_polozhenie(company: dict, director_fio: str) -> bytes:
    """Стандартный регламентный документ (337 абзацев), почти без переменных данных —
    только название компании (7 упоминаний) и подпись директора (1). Меняем глобально."""
    parts = _load_parts('9_polozhenie.docx')
    xml = parts['word/document.xml'].decode('utf-8')

    old_company = 'Сфера Секьюрити'
    new_company = company.get('name', '')
    xml = xml.replace(old_company, new_company)

    dir_init = _dir_initials(director_fio)
    paras = _paragraphs(xml)
    idx_sig = _find_para_index(paras, lambda t: 'Пеганов' in t)
    if idx_sig >= 0 and paras[idx_sig] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_sig]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.\s*$', dir_init, old_t)
        xml = xml.replace(paras[idx_sig], _replace_para_text(paras[idx_sig], new_t), 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 10: Паспорт системы производственного контроля ═══════════════════
def render_pasport(company: dict, director_fio: str, director_phone: str, address: str,
                    people: list, cert_number: str = '', cert_date: str = '') -> bytes:
    """people: [{fio, position}] — все, кто в СПК (директор первым)."""
    parts = _load_parts('10_pasport.docx')
    xml = parts['word/document.xml'].decode('utf-8')

    old_company = 'Сфера Секьюрити'
    new_company = company.get('name', '')
    xml = xml.replace(old_company, new_company)

    dir_init = _dir_initials(director_fio)
    paras0 = _paragraphs(xml)
    idx_approve_sig = _find_para_index(paras0, lambda t: '______' in t and ('Пеганов' in t or t.strip().endswith('.')))
    if idx_approve_sig >= 0 and paras0[idx_approve_sig] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras0[idx_approve_sig]).strip()
        new_t = re.sub(r'[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.\s*$', dir_init, old_t)
        xml = xml.replace(paras0[idx_approve_sig], _replace_para_text(paras0[idx_approve_sig], new_t), 1)
    xml = xml.replace('Пеганов В.Н.', dir_init)  # если попадётся цельным куском где-то ещё

    paras = _paragraphs(xml)
    idx_addr = _find_para_index(paras, lambda t: t.startswith(': 220') or ': ' in t and t.startswith(':'))
    idx_dir_phone = _find_para_index(paras, lambda t: t.startswith('Директор Пеганова'))
    idx_phone = _find_para_index(paras, lambda t: t.startswith('Тел.:'))
    idx_roles = _find_para_index(paras, lambda t: t.startswith('Директор;Главный') or
                                                    (t.startswith('Директор') and 'Производитель' in t))
    idx_people_start = _find_para_index(paras, lambda t: t.startswith('Пеганов Владимир'))

    if idx_addr >= 0 and paras[idx_addr] in xml:
        xml = xml.replace(paras[idx_addr], _replace_para_text(paras[idx_addr], f": {address}"), 1)
    if idx_dir_phone >= 0 and paras[idx_dir_phone] in xml:
        xml = xml.replace(paras[idx_dir_phone], _replace_para_text(paras[idx_dir_phone], f"Директор {_fio_initials_surname_first(director_fio)}"), 1)
    if idx_phone >= 0 and paras[idx_phone] in xml:
        xml = xml.replace(paras[idx_phone], _replace_para_text(paras[idx_phone], f"Тел.: {director_phone}"), 1)
    if idx_roles >= 0 and paras[idx_roles] in xml:
        roles_text = '; '.join(p.get('position', '') for p in people)
        xml = xml.replace(paras[idx_roles], _replace_para_text(paras[idx_roles], roles_text), 1)

    if idx_people_start >= 0:
        # Ищем конец блока людей — следующий непустой абзац после последнего человека
        # в оригинале (в образце 4 человека, абзацы 35-38 подряд)
        end_idx = idx_people_start
        while end_idx < len(paras) and (', ' in re.sub(r'<[^>]+>', '', paras[end_idx]).strip() or end_idx == idx_people_start):
            t = re.sub(r'<[^>]+>', '', paras[end_idx]).strip()
            if ',' not in t and end_idx != idx_people_start:
                break
            end_idx += 1
        old_block = ''.join(paras[idx_people_start:end_idx])
        if old_block in xml:
            style_line = paras[idx_people_start]
            new_lines = ''.join(_replace_para_text(style_line, f"{p.get('fio','')}, {p.get('position','')}") for p in people)
            xml = xml.replace(old_block, new_lines, 1)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 11: Справка о наличии ТТК ═══════════════════
def render_spravka_ttk(company: dict, director_fio: str, ttk_list: list) -> bytes:
    """ttk_list: [{code, name, organization, validity}] — реальный перечень
    технологических карт компании (не стандартный текст, полностью от клиента)."""
    parts = _load_parts('11_spravka_ttk.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    xml = xml.replace('Сфера Секьюрити', company.get('name', ''))
    rows = _rows(xml)
    template_row = rows[2]  # первая строка данных в образце (после заголовка и нумерации)

    new_rows = []
    for i, t in enumerate(ttk_list, 1):
        cell_values = [str(i), t.get('code', ''), '', t.get('name', ''), t.get('organization', ''), t.get('validity', '')]
        new_rows.append(_build_row(template_row, cell_values))
    if new_rows:
        xml = _splice_rows(xml, rows[2:], new_rows)

    paras = _paragraphs(xml)
    xml = _replace_director_signature(xml, paras, _dir_initials(director_fio))
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 12: Справка о наличии СИ ═══════════════════
def render_spravka_si(company: dict, director_fio: str, si_list: list) -> bytes:
    """si_list: [{name, characteristics, count, number, verification}] — реальный
    перечень средств измерений компании (полностью от клиента)."""
    parts = _load_parts('12_spravka_si.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    rows = _rows(xml)
    template_row = rows[2]

    new_rows = []
    for i, s in enumerate(si_list, 1):
        cell_values = [str(i), s.get('name', ''), s.get('characteristics', ''),
                       str(s.get('count', 1)), s.get('number', ''), s.get('verification', '')]
        new_rows.append(_build_row(template_row, cell_values))
    if new_rows:
        xml = _splice_rows(xml, rows[2:], new_rows)

    paras = _paragraphs(xml)
    xml = _replace_director_signature(xml, paras, _dir_initials(director_fio))
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)



# ═══════════════════ Адаптер для реального пайплайна (generator.py) ═══════════════════
def _find_person(itr, *keywords):
    for p in itr:
        pos = (p.get('position') or '').lower()
        if any(k in pos for k in keywords):
            return p
    return None


def generate_spk_package_v2(company: dict, itr: list, workers: list, dates: dict, resp: dict,
                             variant: str = 'spk_stroy', progress_cb=None) -> dict:
    """
    company: {name, form, unp, address, city, director_fio, director_position, phone, bisp_org}
    itr: список [{fio, position}] — сотрудники (используем чтобы найти гл.инженера/прорабов)
    dates: результат calculate_dates() из generator.py (goals, year и т.д.)
    resp: результат select_responsible(itr) из generator.py
    variant: 'spk_stroy' | 'spk_bisp'
    """
    org = company.get('name', 'company')
    director_fio = company.get('director_fio', '') or (resp.get('director') or {}).get('fio', '')
    gl_person = _find_person(itr, 'главный инженер', 'гл. инженер') or resp.get('process_resp')
    gl_inzhener_fio = (gl_person or {}).get('fio', '') if gl_person != resp.get('director') else ''
    foremen = [p.get('fio', '') for p in itr
               if any(k in (p.get('position') or '').lower() for k in ('прораб', 'производитель работ'))
               and p.get('fio') != director_fio]
    if not foremen:
        alt = _find_person(itr, 'мастер')
        if alt and alt.get('fio') != director_fio:
            foremen = [alt.get('fio', '')]
    if not foremen:
        foremen = [gl_inzhener_fio] if gl_inzhener_fio else ['']

    all_people = [{'fio': director_fio, 'position': company.get('director_position', 'Директор'),
                   'role_key': 'директор'}]
    if gl_inzhener_fio:
        all_people.append({'fio': gl_inzhener_fio, 'position': (gl_person or {}).get('position', 'Главный инженер'),
                           'role_key': 'главный инженер'})
    for f in foremen:
        if f:
            fp = next((p for p in itr if p.get('fio') == f), {})
            all_people.append({'fio': f, 'position': fp.get('position', 'Производитель работ'),
                               'role_key': 'производитель работ'})

    order_date = dates.get('goals', '')
    city = company.get('city', 'Минск')
    year = dates.get('year', '')
    docs = []
    step = [0]
    total = 19 if variant == 'spk_bisp' else 12

    def p(msg):
        step[0] += 1
        if progress_cb:
            progress_cb(step[0], total, msg)
        print(f"  [spk_v2 {step[0]}/{total}] {msg}")

    def add(name, data_bytes):
        docs.append({'name': name, 'bytes': data_bytes})

    p("1. Условия в производственных помещениях")
    add(f"{org} СПК - 1 Условия в производственных помещениях.docx", render_usloviya(company))

    p("2. Приказ о СПК")
    add(f"{org} СПК - 4.1 Приказ о СПК.docx",
        render_prikaz_spk(company, '1/СПК', order_date, city, director_fio, gl_inzhener_fio, foremen))

    p("3. Приказ о внутреннем обучении")
    add(f"{org} СПК - 4.2.1 Приказ о внутреннем обучении.docx",
        render_prikaz_obuchenie(company, '2/СПК', order_date, city, director_fio, order_date))

    p("4. Приказ о ТО средств измерений")
    resp_si = foremen[0] if foremen and foremen[0] else director_fio
    add(f"{org} СПК - 4.3 Приказ о ТО СИ.docx",
        render_prikaz_to_si(company, '3/СПК', order_date, city, director_fio, resp_si))

    p("5. Приказ о назначении ответственного за машины")
    add(f"{org} СПК - 4.4 Приказ о машинах.docx",
        render_prikaz_mashiny(company, '4/СПК', order_date, city, director_fio, resp_si))

    p("6. Справка ИТР")
    people_itr = [dict(pp, protocol_number='1', protocol_date=order_date) for pp in all_people]
    add(f"{org} СПК - 2 Справка ИТР.docx", render_spravka_itr(company, people_itr))

    p("7. Организационная структура")
    add(f"{org} СПК - 3 Организационная структура.docx",
        render_orgstruktura(company, director_fio, gl_inzhener_fio, foremen))

    p("8. Протокол о внутреннем обучении")
    add(f"{org} СПК - 4.2.2 Протокол обучения.docx",
        render_protokol_obuchenie(company, '1', order_date, city, order_date, '2/СПК', all_people))

    p("9. Положение о СПК")
    add(f"{org} СПК - 5 Положение о СПК.docx", render_polozhenie(company, director_fio))

    p("10. Паспорт СПК")
    add(f"{org} СПК - 6 Паспорт СПК.docx",
        render_pasport(company, director_fio, company.get('phone', ''), company.get('address', ''), all_people))

    p("11. Справка ТТК")
    work_types = company.get('work_types', [company.get('scope', 'Общестроительные работы')])
    ttk_list = [{'code': f'ТТК-{i+1:03d}', 'name': f'ТК на {wt}', 'organization': 'РУП «Стройтехнорм»',
                 'validity': str(int(year) + 2) if year else ''} for i, wt in enumerate(work_types[:6])]
    add(f"{org} СПК - 7 Справка ТТК.docx", render_spravka_ttk(company, director_fio, ttk_list))

    p("12. Справка СИ")
    si_list = [
        {'name': 'Рулетка измерительная', 'characteristics': '(0-3000) мм', 'count': 1, 'number': '—', 'verification': f'Свидетельство о поверке {year} г.'},
        {'name': 'Уровень строительный', 'characteristics': 'ГОСТ 9416, I группа точности', 'count': 1, 'number': '—', 'verification': f'Свидетельство о поверке {year} г.'},
    ]
    add(f"{org} СПК - 8 Справка СИ.docx", render_spravka_si(company, director_fio, si_list))

    if variant == 'spk_bisp':
        try:
            from generator_bisp_templates import (
                render_garantiya_ttk, render_garantiya_labs, render_garantiya_reklamacii,
                render_plan_audita, render_polozhenie_vhod, render_grafik_poverki, render_perechen_produkcii,
            )
            recipient = company.get('bisp_org', 'РУП «СтройМедиаПроект»')

            p("13. Гарантийное письмо на ТТК")
            add(f"{org} СПК БИСП - 9.1 Гарантийное письмо на ТТК.docx",
                render_garantiya_ttk(company, director_fio, '1-01', order_date, recipient))

            p("14. Гарантийное письмо по лабораториям")
            add(f"{org} СПК БИСП - 9.3 Гарантийное письмо по лабораториям.docx",
                render_garantiya_labs(company, director_fio, '1-02', order_date, recipient))

            p("15. Гарантийное письмо об отсутствии рекламаций")
            add(f"{org} СПК БИСП - 9.6 Гарантийное письмо об отсутствии рекламаций.docx",
                render_garantiya_reklamacii(company, director_fio, '1-03', order_date, recipient))

            p("16. План внутреннего аудита")
            add(f"{org} СПК БИСП - План внутреннего аудита.docx",
                render_plan_audita(company, director_fio, year))

            p("17. Положение о входном контроле")
            add(f"{org} СПК БИСП - 5.2 Положение о входном контроле.docx",
                render_polozhenie_vhod(company, director_fio))

            p("18. График поверки СИ")
            add(f"{org} СПК БИСП - График поверки СИ.docx",
                render_grafik_poverki(company, director_fio, year))

            p("19. Перечень продукции входного контроля")
            add(f"{org} СПК БИСП - Перечень продукции входного контроля.docx",
                render_perechen_produkcii(company, director_fio))
        except Exception as e:
            print(f"  ❌ Ошибка генерации документов БИСП: {e}")

    warnings = []
    if not gl_inzhener_fio:
        warnings.append("Не найден главный инженер в штате — в документах СПК это поле осталось пустым.")
    if not any(foremen):
        warnings.append("Не найден прораб/производитель работ в штате — использован главный инженер как ответственный по умолчанию.")

    return {'docs': docs, 'warnings': warnings}
