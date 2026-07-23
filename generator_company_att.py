"""
Модуль генерации документов на аттестацию ЮРИДИЧЕСКОГО ЛИЦА (компании) —
аттестат соответствия (СТ — подряд, ГС — генподряд).

АРХИТЕКТУРА: документы строятся программно через ручной OOXML (реальные таблицы Word,
без внешних зависимостей — не python-docx/lxml, которые требуют lxml и иногда не
устанавливаются на минимальных серверных окружениях).

ТОЧНОСТЬ: структура, заголовки, ширины колонок (в твипах) и нумерация строк таблиц
сверены построчно с реальными поданными и принятыми документами (ООО «Асецкий и К» —
без генподряда, ЧУП «СК76» — с генподрядом), включая детали которых не было в
предыдущей версии: строка нумерации колонок "1 2 3...", таблица рабочих с разбивкой
по разрядам II-VI (не просто "разряд+количество"), "Всего:" как строка таблицы а не
отдельный абзац, точная формулировка Формы №6.
"""
import json, re, io, zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
_CLASSIFIER_PATH = BASE_DIR / 'classifier_company_att.json'
if not _CLASSIFIER_PATH.exists():
    raise FileNotFoundError(
        f"Не найден classifier_company_att.json по пути {_CLASSIFIER_PATH}. "
        f"Файл должен лежать в той же папке репозитория, что и server.py/generator.py."
    )
CLASSIFIER = json.loads(_CLASSIFIER_PATH.read_text('utf-8'))

LEGAL_FORMS = {
    'ООО':  {'nom': 'Общество с ограниченной ответственностью',
             'gen': 'Общества с ограниченной ответственностью',
             'dat': 'Обществу с ограниченной ответственностью', 'quote': '«»'},
    'ОДО':  {'nom': 'Общество с дополнительной ответственностью',
             'gen': 'Общества с дополнительной ответственностью',
             'dat': 'Обществу с дополнительной ответственностью', 'quote': '«»'},
    'ЗАО':  {'nom': 'Закрытое акционерное общество',
             'gen': 'Закрытого акционерного общества',
             'dat': 'Закрытому акционерному обществу', 'quote': '«»'},
    'ОАО':  {'nom': 'Открытое акционерное общество',
             'gen': 'Открытого акционерного общества',
             'dat': 'Открытому акционерному обществу', 'quote': '«»'},
    'ЧУП':  {'nom': 'Частное унитарное предприятие',
             'gen': 'Частного унитарного предприятия',
             'dat': 'Частному унитарному предприятию', 'quote': '""'},
    'ЧТУП': {'nom': 'Частное торговое унитарное предприятие',
             'gen': 'Частного торгового унитарного предприятия',
             'dat': 'Частному торговому унитарному предприятию', 'quote': '""'},
    'ИП':   {'nom': 'Индивидуальный предприниматель',
             'gen': 'Индивидуального предпринимателя',
             'dat': 'Индивидуальному предпринимателю', 'quote': '""'},
}

RAZRYAD_COLUMNS = ['II', 'III', 'IV', 'V', 'VI']


def _legal(form):
    return LEGAL_FORMS.get((form or 'ООО').upper(), LEGAL_FORMS['ООО'])


def _quoted_name(company, case='nom'):
    L = _legal(company.get('form'))
    name = company.get('name', '')
    q = L['quote']
    return f"{L[case]} {q[0]}{name}{q[1]}"


def _normalize_category(category):
    if category is None:
        return None
    s = str(category).strip().lower()
    if s in ('', 'null', 'none', 'нет', 'undefined'):
        return None
    return str(category).strip()


def _get_category_code(code: str) -> str:
    """'7.4.1' -> '7.4' (родительская категория), '7.1' -> '7.1' (уже верхний уровень)."""
    parts = code.split('.')
    if len(parts) >= 3:
        return f"{parts[0]}.{parts[1]}"
    return code


def _flat_work_items() -> dict:
    """Плоский словарь ВСЕХ кодов (и категорий, и подпунктов) -> текст — для поиска
    по ключевым словам и для сводного текста типа 'область деятельности: ...'."""
    flat = {}
    for cat_code, cat in CLASSIFIER['punkt_7_smr']['categories'].items():
        flat[cat_code] = cat['text']
        for sub_code, sub_text in cat.get('sub', {}).items():
            flat[sub_code] = sub_text
    return flat


# Разговорные термины клиентов не совпадают по словам с официальными формулировками
# классификатора — задаём известные типовые наборы явно. "Общестрой" сверен по
# реальному поданному документу (ООО «АК СтройФемили») — это разделы 7.2-7.6
# целиком (основания, фундаменты, конструкции, антикоррозия, кровли), а не узкий
# список из нескольких пунктов, как предполагалось раньше.
COMMON_BUNDLES = {
    'общестрой': ['7.2', '7.3', '7.4', '7.5', '7.6'],
    'общестроительные': ['7.2', '7.3', '7.4', '7.5', '7.6'],
    'общестроительный': ['7.2', '7.3', '7.4', '7.5', '7.6'],
}


def find_work_items(query: str, max_items=10):
    q = query.lower()
    for keyword, codes in COMMON_BUNDLES.items():
        if keyword in q:
            flat = _flat_work_items()
            return [(code, flat.get(code, code)) for code in codes]

    q_stems = {w[:5] for w in re.findall(r'[а-яё]{5,}', q)}
    flat = _flat_work_items()
    found = []
    for code, text in flat.items():
        tl = text.lower()
        t_stems = re.findall(r'[а-яё]{5,}', tl)
        score = sum(1 for w in t_stems if w[:5] in q_stems)
        if score > 0:
            found.append((score, code, text))
    found.sort(key=lambda x: -x[0])
    return [(code, text) for _, code, text in found[:max_items]]


def render_work_items_lines(work_items: list) -> list:
    """Строит строки пункта 7 заявления с правильной вложенностью — как в реальном
    документе: категория с подпунктами → "7.4 текст:" затем "7.4.1 текст;" на каждый
    выбранный подпункт. Категория БЕЗ подпунктов (лист) → "7.7. текст;" одной строкой.
    Если передан код категории целиком (например "7.4") — разворачиваем ВСЕ её
    подпункты; если переданы только конкретные подкоды — используем только их."""
    categories = CLASSIFIER['punkt_7_smr']['categories']
    selected = {}  # cat_code -> 'ALL' | set(sub_codes)
    for code in work_items:
        cat_code = _get_category_code(code)
        if cat_code not in categories:
            continue
        if code == cat_code:
            selected[cat_code] = 'ALL'
        else:
            if selected.get(cat_code) != 'ALL':
                selected.setdefault(cat_code, set()).add(code)

    lines = []
    for cat_code, cat in categories.items():  # сохраняем естественный порядок классификатора
        if cat_code not in selected:
            continue
        subs = cat.get('sub', {})
        if not subs:
            lines.append(f"{cat_code}. {cat['text']};")
            continue
        chosen = list(subs.keys()) if selected[cat_code] == 'ALL' else [s for s in subs if s in selected[cat_code]]
        if not chosen:
            continue
        lines.append(f"{cat_code} {cat['text']}:")
        for sub_code in chosen:
            lines.append(f"{sub_code} {subs[sub_code]};")
    return lines


def calculate_stazh(periods: list, as_of_date: str = None) -> dict:
    """Календарный расчёт стажа (Приказ №91): 30 дней=месяц, 12 месяцев=год."""
    from datetime import datetime as _dt

    def _parse(d):
        if not d:
            return None
        for fmt in ('%d.%m.%Y', '%d.%m.%y', '%Y-%m-%d'):
            try:
                return _dt.strptime(d.strip(), fmt)
            except (ValueError, AttributeError):
                continue
        return None

    today = _parse(as_of_date) or _dt.now()
    total_days = 0
    for period in (periods or []):
        start = _parse(period.get('start'))
        end = _parse(period.get('end')) or today
        if not start or end < start:
            continue
        total_days += (end - start).days + 1

    months, days = divmod(total_days, 30)
    years, months = divmod(months, 12)
    return {
        'years': years, 'months': months, 'days': days,
        'total_years_rounded': round(years + months / 12 + days / 365, 1),
        'display': f"{years} лет {months} мес. {days} дн." if years or months else f"{days} дн.",
    }


def check_category_requirements(category, staff_total: int, has_smetchik: bool,
                                 experience_objects: list, prior_category_years: int = 0) -> list:
    category = _normalize_category(category)
    if category is None:
        return []
    warnings = []
    thresholds = CLASSIFIER['_meta']['category_thresholds'].get(str(category))
    if not thresholds:
        return [f"Категория '{category}' не входит в список 1-4 — проверьте, что имелось в виду."]
    if staff_total < thresholds['min_staff']:
        warnings.append(
            f"Недостаточно штата для категории {category}: нужно минимум {thresholds['min_staff']} чел. "
            f"по основному месту работы, у клиента {staff_total}. Реалистичный вариант — категория ниже."
        )
    if not has_smetchik:
        warnings.append("Нет аттестованного инженера по сметной работе (сметчика) — обязателен для любой категории генподряда.")
    objects_required = thresholds['objects_required']
    if objects_required > 0:
        n_objects = len(experience_objects or [])
        if n_objects < objects_required:
            warnings.append(
                f"Для категории {category} нужно подтвердить опыт минимум по {objects_required} объектам "
                f"(генподрядчик, привлекавший субподряд, введён в эксплуатацию не позднее 5 лет назад, "
                f"не текущий ремонт). Предоставлено объектов: {n_objects}."
            )
        if thresholds.get('prior_category') and prior_category_years < thresholds.get('prior_years', 0):
            warnings.append(
                f"Для категории {category} нужно не менее {thresholds['prior_years']} лет владения категорией "
                f"{thresholds['prior_category']} — по данным клиента стаж владения {prior_category_years} лет."
            )
    return warnings


def _dir_init(fio: str) -> str:
    parts = (fio or '').strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio or ''


# ═══════════════════ РУЧНОЙ OOXML (без внешних зависимостей) ═══════════════════
def _esc(s):
    return (str(s if s not in (None, '') else '—')
            .replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;'))


def _para(text='', align='left', bold=False, size=30, space_after=120):
    align_xml = {'left': 'left', 'center': 'center', 'right': 'right', 'justify': 'both'}.get(align, 'left')
    b = '<w:b/>' if bold else ''
    return (f'<w:p><w:pPr><w:jc w:val="{align_xml}"/><w:spacing w:after="{space_after}" w:line="276" w:lineRule="auto"/></w:pPr>'
            f'<w:r><w:rPr>{b}<w:sz w:val="{size}"/><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/></w:rPr>'
            f'<w:t xml:space="preserve">{_esc(text) if text else " "}</w:t></w:r></w:p>')


def _cell(text, w, bold=False, align='left', size=24):
    align_xml = {'left': 'left', 'center': 'center'}.get(align, 'left')
    b = '<w:b/>' if bold else ''
    return (f'<w:tc><w:tcPr><w:tcW w:w="{w}" w:type="dxa"/><w:vAlign w:val="center"/></w:tcPr>'
            f'<w:p><w:pPr><w:jc w:val="{align_xml}"/><w:spacing w:after="0"/></w:pPr>'
            f'<w:r><w:rPr>{b}<w:sz w:val="{size}"/><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/></w:rPr>'
            f'<w:t xml:space="preserve">{_esc(text)}</w:t></w:r></w:p></w:tc>')


def _table_fixed(headers, rows, widths_twips, number_row=True, cell_size=24):
    """Таблица с ТОЧНЫМИ ширинами колонок в твипах (взяты из реальных документов),
    с опциональной строкой нумерации "1 2 3..." под заголовками — как в оригиналах."""
    grid = ''.join(f'<w:gridCol w:w="{w}"/>' for w in widths_twips)
    hdr = '<w:tr>' + ''.join(_cell(h, w, True, 'center', cell_size) for h, w in zip(headers, widths_twips)) + '</w:tr>'
    num_row = ''
    if number_row:
        nums = [str(i+1) for i in range(len(headers))]
        num_row = '<w:tr>' + ''.join(_cell(n, w, False, 'center', cell_size) for n, w in zip(nums, widths_twips)) + '</w:tr>'
    body = ''
    for row in rows:
        body += '<w:tr>' + ''.join(_cell(v, w, size=cell_size) for v, w in zip(row, widths_twips)) + '</w:tr>'
    borders = ('<w:tblBorders>'
               '<w:top w:val="single" w:sz="4" w:color="000000"/>'
               '<w:left w:val="single" w:sz="4" w:color="000000"/>'
               '<w:bottom w:val="single" w:sz="4" w:color="000000"/>'
               '<w:right w:val="single" w:sz="4" w:color="000000"/>'
               '<w:insideH w:val="single" w:sz="4" w:color="000000"/>'
               '<w:insideV w:val="single" w:sz="4" w:color="000000"/>'
               '</w:tblBorders>')
    return (f'<w:tbl><w:tblPr><w:tblW w:w="0" w:type="auto"/>{borders}</w:tblPr>'
            f'<w:tblGrid>{grid}</w:tblGrid>{hdr}{num_row}{body}</w:tbl>')


_CONTENT_TYPES = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                   '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                   '<Default Extension="xml" ContentType="application/xml"/>'
                   '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
                   '</Types>')
_RELS = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
         '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
         '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
         '</Relationships>')
_WORD_RELS = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
              '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"/>')


def _build_docx(body_blocks, landscape=False, margins=None) -> bytes:
    body = ''.join(body_blocks)
    if margins:
        top, right, bottom, left = margins
    elif landscape:
        top, right, bottom, left = 850, 850, 850, 850
    else:
        top, right, bottom, left = 850, 1417, 850, 1417
    if landscape:
        sect = f'<w:sectPr><w:pgSz w:w="16838" w:h="11906" w:orient="landscape"/><w:pgMar w:top="{top}" w:right="{right}" w:bottom="{bottom}" w:left="{left}"/></w:sectPr>'
    else:
        sect = f'<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="{top}" w:right="{right}" w:bottom="{bottom}" w:left="{left}"/></w:sectPr>'
    doc_xml = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
               '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               f'<w:body>{body}{sect}</w:body></w:document>')
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', _CONTENT_TYPES)
        zf.writestr('_rels/.rels', _RELS)
        zf.writestr('word/document.xml', doc_xml)
        zf.writestr('word/_rels/document.xml.rels', _WORD_RELS)
    return buf.getvalue()


# ═══════════════════ Документ 1: Заявление ═══════════════════
def gen_zayavlenie_company(company: dict, work_items: list, category: str) -> bytes:
    category = _normalize_category(category)
    L = _legal(company.get('form'))
    full_nom = _quoted_name(company, 'nom')
    full_gen = _quoted_name(company, 'gen')
    full_dat = _quoted_name(company, 'dat')
    dir_pos = company.get('director_position', 'Директор')
    dir_init = _dir_init(company.get('director_fio', ''))

    blocks = []
    blocks.append(_para(full_nom, bold=True))
    blocks.append(_para(company.get('address', '')))
    if company.get('bank_details'):
        blocks.append(_para(f"р/с: {company.get('bank_details','')}"))
    blocks.append(_para(f"УНП {company.get('unp','')}"))
    blocks.append(_para(f"Тел./факс: {company.get('phone','')}"))
    blocks.append(_para(f"e-mail: {company.get('email','')}"))
    blocks.append(_para(""))
    blocks.append(_para("Исх. № ___ от ___.___.____ г.", align='right'))
    blocks.append(_para(""))
    blocks.append(_para("РУП «БЕЛСТРОЙЦЕНТР»"))
    blocks.append(_para("ул. Р. Люксембург, 101"))
    blocks.append(_para("220036, г. Минск"))
    blocks.append(_para(""))
    blocks.append(_para(full_nom))
    blocks.append(_para(company.get('address', '')))
    blocks.append(_para(f"УНП {company.get('unp','')}"))
    blocks.append(_para(f"Тел.: {company.get('phone','')}"))
    blocks.append(_para(f"e-mail: {company.get('email','')}"))
    blocks.append(_para(""))
    blocks.append(_para("ЗАЯВЛЕНИЕ", align='center', bold=True, size=32))
    blocks.append(_para("о получении аттестата соответствия", align='center', bold=True))
    blocks.append(_para(""))
    blocks.append(_para(f"Прошу провести аттестацию {full_gen} на право осуществления:", align='justify'))

    if category:
        blocks.append(_para(
            f"6. Выполнение функций генерального подрядчика со стоимостью строительства свыше "
            f"{CLASSIFIER['_meta']['genpodryad_min_cost']}. Соответствующей квалификационным "
            f"требованиям, предъявляемым для получения аттестата соответствия {category} "
            f"классов(а) сложности.", align='justify'))

    blocks.append(_para("7. Выполнение строительно-монтажных работ:", align='justify'))
    for line in render_work_items_lines(work_items):
        blocks.append(_para(line, align='justify'))

    blocks.append(_para("соответствующей квалификационным требованиям, предъявляемым для получения "
                         "аттестатов(а) соответствия 1-4 классов(а) сложности.", align='justify'))
    blocks.append(_para(""))
    blocks.append(_para("Сведения об обособленных подразделениях, в том числе филиалах (при их наличии): нет"))
    blocks.append(_para(""))
    blocks.append(_para(
        f"В соответствии с {CLASSIFIER['_meta']['legal_basis']} прошу оформить {full_dat} "
        f"аттестат соответствия на бумажном носителе. Сведения, изложенные в заявлении и "
        f"прилагаемых к нему документах, достоверны.", align='justify'))
    blocks.append(_para(""))
    blocks.append(_para("Приложение:", bold=True))

    # № п/п | Наименование документа | Кол-во листов — реальные ширины [567, 8492, 828]
    prilozhenie_rows = [
        ["1.", "Легализованная выписка из торгового реестра страны, в которой иностранная "
               "организация учреждена, или иное эквивалентное доказательство юридического статуса "
               "иностранной организации в соответствии с законодательством страны ее учреждения "
               "(для заявителя – нерезидента).", ""],
        ["2.", "Сведения о составе и профессиональной квалификации руководящих работников, "
               "специалистов и рабочих, работающих по основному месту работы (форма № 2).", ""],
        ["3.", "Сводный список и копии трудовых книжек руководящих работников, специалистов, "
               "работающих по основному месту работы (форма № 3).", ""],
        ["4.", "Сводный список и копии дипломов руководящих работников, специалистов, работающих "
               "по основному месту работы (форма № 4).", ""],
        ["5.", "Сводный список и копии квалификационных аттестатов руководящих работников, "
               "специалистов, работающих по основному месту работы (форма № 5).", ""],
    ]
    if category:
        prilozhenie_rows.append(
            ["6.", "Сведения о наличии опыта генерального подрядчика (форма № 6).", ""]
        )
    prilozhenie_rows.append(["Всего:", "", ""])
    hdr_widths = [567, 8492, 828]
    grid = ''.join(f'<w:gridCol w:w="{w}"/>' for w in hdr_widths)
    hdr = '<w:tr>' + ''.join(_cell(h, w, True, 'center') for h, w in zip(["№ п/п", "Наименование документа", "Кол-во листов"], hdr_widths)) + '</w:tr>'
    body = ''
    for row in prilozhenie_rows:
        body += '<w:tr>' + ''.join(_cell(v, w) for v, w in zip(row, hdr_widths)) + '</w:tr>'
    borders = ('<w:tblBorders><w:top w:val="single" w:sz="4" w:color="000000"/>'
               '<w:left w:val="single" w:sz="4" w:color="000000"/><w:bottom w:val="single" w:sz="4" w:color="000000"/>'
               '<w:right w:val="single" w:sz="4" w:color="000000"/><w:insideH w:val="single" w:sz="4" w:color="000000"/>'
               '<w:insideV w:val="single" w:sz="4" w:color="000000"/></w:tblBorders>')
    blocks.append(f'<w:tbl><w:tblPr><w:tblW w:w="0" w:type="auto"/>{borders}</w:tblPr><w:tblGrid>{grid}</w:tblGrid>{hdr}{body}</w:tbl>')
    blocks.append(_para(""))
    L_sig = _legal(company.get('form'))
    short_form = (company.get('form') or 'ООО').upper()
    q_sig = L_sig['quote']
    sig_name = f"{short_form} {q_sig[0]}{company.get('name','')}{q_sig[1]}"
    blocks.append(_para(f"{dir_pos} {sig_name} _____________ {dir_init}"))

    return _build_docx(blocks)


def gen_zayavlenie_otmena(company: dict, old_attestat_number: str, reason: str) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    blocks = [
        _para(full_nom, bold=True),
        _para(company.get('address', '')),
        _para(f"УНП {company.get('unp','')}"),
        _para(f"Тел.: {company.get('phone','')}"),
        _para(f"e-mail: {company.get('email','')}"),
        _para(""),
        _para("Исх. № ___ от ___.___.____ г.", align='right'),
        _para(""),
        _para("РУП «БЕЛСТРОЙЦЕНТР»"),
        _para("ул. Р. Люксембург, 101"),
        _para("220036, г. Минск"),
        _para(""),
        _para(full_nom),
        _para(company.get('address', '')),
        _para(f"УНП {company.get('unp','')}"),
        _para(""),
        _para("ЗАЯВЛЕНИЕ", align='center', bold=True, size=32),
        _para("о прекращении действия аттестата соответствия", align='center', bold=True),
        _para(""),
        _para(f"{full_nom} просит прекратить действие выданного ранее аттестата соответствия "
              f"от ___.___.____ г. № {old_attestat_number}.", align='justify'),
        _para(""),
        _para(f"Причина: {reason}", align='justify'),
        _para(""),
        _para("В соответствии со статьёй 36 Кодекса Республики Беларусь об архитектурной, "
              "градостроительной и строительной деятельности.", align='justify'),
        _para(""),
        _para(f"Директор _____________ {dir_init}"),
    ]
    return _build_docx(blocks)


# ═══════════════════ Документ 2: Форма №2 — ИТР + рабочие по разрядам (landscape) ═══════════════════
def gen_form2_itr(company: dict, itr_list: list, workers: list, work_scope_text: str) -> bytes:
    """Точные ширины колонок и структура — из реального документа ООО «Асецкий и К».
    Таблица рабочих — с разбивкой по разрядам II-VI (не просто "разряд+количество"),
    как в оригинале."""
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    total_staff = company.get('staff_total') or (len(itr_list) + sum(w.get('count', 0) or 0 for w in workers))
    n_itr = len(itr_list)

    blocks = [
        _para(full_nom, bold=True, size=26),
        _para(""),
        _para("Форма № 2", align='right', bold=False, size=26),
        _para("СВЕДЕНИЯ о составе и профессиональной квалификации руководящих работников, "
              "специалистов и рабочих, работающих по основному месту работы", align='center', bold=True, size=24),
        _para(""),
        _para(f"Общая численность работающих {total_staff} чел., в том числе по заявляемому виду "
              f"деятельности {total_staff} чел. по состоянию на ___.___.____ ; численность "
              f"инженерно-технических работников по заявляемому виду деятельности {n_itr} чел.", size=26),
        _para(f"Область деятельности: {work_scope_text}", size=26),
        _para(""),
    ]

    # Реальные ширины (twips) из Асецкий: 425,1485,1950,2025,1316,2126,1519
    itr_widths = [425, 1485, 1950, 2025, 1316, 2126, 1519]
    itr_headers = ["№ п/п",
                   "Наименование должности руководящего работника, специалиста в соответствии с записью в трудовой книжке",
                   "Фамилия, собственное имя, отчество (если таковое имеется)",
                   "Уровень образования, наименование учреждения образования, номер и дата выдачи диплома, специальность, квалификация",
                   "Стаж работы по заявляемому виду деятельности, в т.ч. у данного нанимателя, лет",
                   "Номер трудовой книжки, номер и дата приказа о приёме на работу",
                   "Номер и дата выдачи квалификационного аттестата, специализация аттестации"]
    itr_rows = []
    for i, p_ in enumerate(itr_list, 1):
        obrazovanie = (f"{p_.get('education_level','')} Диплом {p_.get('diploma_number') or '—'} "
                        f"выдан {p_.get('diploma_date') or '—'} {p_.get('diploma_institution','')} "
                        f"{p_.get('diploma_speciality','')} {p_.get('diploma_qualification','')}")
        stazh = f"{p_.get('stage_years') or '—'} / {p_.get('stage_years_here') or '—'}"
        trudovaya = (f"Трудовая книжка {p_.get('trudovaya_number') or '—'} Приказ №{p_.get('order_number') or '—'} "
                     f"от {p_.get('hire_date') or '—'}")
        attestat = p_.get('attestat_number') or '—'
        if p_.get('attestat_date'):
            attestat += f" от {p_.get('attestat_date')}"
        if p_.get('attestat_specialization'):
            attestat += f" {p_.get('attestat_specialization')}"
        itr_rows.append([str(i), p_.get('position',''), p_.get('fio',''), obrazovanie, stazh, trudovaya, attestat])

    blocks.append(_table_fixed(itr_headers, itr_rows, itr_widths, cell_size=26))

    blocks.append(_para(""))
    blocks.append(_para("Рабочие строительных профессий, соответствующих заявляемым видам деятельности "
                         "в области строительства согласно технологической документации на производство "
                         "строительно-монтажных работ, работающих по основному месту работы:", size=26))

    if workers:
        # Реальная структура: № | Профессия | II | III | IV | V | VI | Итого — ширины из СК76/Асецкий
        w_widths = [548, 4254, 749, 749, 749, 749, 750, 1505]
        w_headers = ["№ п/п", "Наименование профессий рабочих"] + RAZRYAD_COLUMNS + ["Итого"]
        w_rows = []
        totals = {r: 0 for r in RAZRYAD_COLUMNS}
        for i, w in enumerate(workers, 1):
            razr = str(w.get('razryad', '')).upper().strip()
            count = int(w.get('count') or 0)
            row = [str(i), w.get('profession', '')]
            for r in RAZRYAD_COLUMNS:
                if r == razr:
                    row.append(str(count) if count else '')
                    totals[r] += count
                else:
                    row.append('')
            row.append(str(count) if count else '')
            w_rows.append(row)
        total_row = ["", "Итого по разрядам:"] + [str(totals[r]) if totals[r] else '' for r in RAZRYAD_COLUMNS] + [str(sum(totals.values()))]
        w_rows.append(total_row)
        blocks.append(_table_fixed(w_headers, w_rows, w_widths, number_row=True, cell_size=26))
    else:
        blocks.append(_para("Сведения о рабочих не предоставлены на момент подготовки документа.", size=26))

    blocks.append(_para(""))
    blocks.append(_para(f"Директор {full_nom} _____________ {dir_init}"))
    blocks.append(_para("«___» _______ 202_ г."))
    return _build_docx(blocks, landscape=False, margins=(567, 567, 567, 1418))


# ═══════════════════ Документы 3-5: сводные списки (точные ширины из оригиналов) ═══════════════════
def gen_form3_trudovye(company: dict, itr_list: list) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = [[str(i), p_.get('fio',''), p_.get('position',''), p_.get('trudovaya_number') or '—']
            for i, p_ in enumerate(itr_list, 1)]
    blocks = [
        _para(full_nom, bold=True), _para(""),
        _para("Форма № 3", align='right', bold=False, size=26),
        _para("СВОДНЫЙ СПИСОК трудовых книжек руководящих работников, специалистов, работающих по "
              "основному месту работы", align='center', bold=True),
        _para(""),
    ]
    # Реальные ширины из Асецкий: 396,1682,1569,1353
    blocks.append(_table_fixed(
        ["№ п/п", "Ф.И.О.", "Должность в соответствии с записью в трудовой книжке", "Номер трудовой книжки"],
        rows, [396, 1682, 1569, 1353]))
    blocks += [_para(""), _para(f"Директор _____________ {dir_init}"), _para("«___» _______ 202_ г.")]
    return _build_docx(blocks)


def gen_form4_diplomy(company: dict, itr_list: list) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = [[str(i), p_.get('fio',''), p_.get('diploma_number') or '—'] for i, p_ in enumerate(itr_list, 1)]
    blocks = [
        _para(full_nom, bold=True), _para(""),
        _para("Форма № 4", align='right', bold=False, size=26),
        _para("СВОДНЫЙ СПИСОК дипломов руководящих работников, специалистов, работающих по "
              "основному месту работы", align='center', bold=True),
        _para(""),
    ]
    # Реальные ширины из Асецкий: 439,2378,2183
    blocks.append(_table_fixed(["№ п/п", "Ф.И.О.", "Номер диплома"], rows, [439, 2378, 2183]))
    blocks += [_para(""), _para(f"Директор _____________ {dir_init}"), _para("«___» _______ 202_ г.")]
    return _build_docx(blocks)


def gen_form5_attestaty(company: dict, itr_list: list) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = []
    for i, p_ in enumerate(itr_list, 1):
        att = p_.get('attestat_number', '')
        if att:
            info = f"{att} с {p_.get('attestat_date_from','')} г. по {p_.get('attestat_date_to','')} г. {p_.get('attestat_specialization','')}"
        else:
            info = "нет аттестата / в процессе получения"
        rows.append([str(i), p_.get('fio',''), p_.get('position',''), info])
    blocks = [
        _para(full_nom, bold=True), _para(""),
        _para("Форма № 5", align='right', bold=False, size=26),
        _para("СВОДНЫЙ СПИСОК квалификационных аттестатов руководящих работников, специалистов, "
              "работающих по основному месту работы", align='center', bold=True),
        _para(""),
    ]
    # Реальные ширины из Асецкий: 675,2085,2876,4501
    blocks.append(_table_fixed(
        ["№ п/п", "Ф.И.О.", "Должность в соответствии с записью в трудовой книжке",
         "Номер и срок действия (с __.__.20__г. по __.__.20__г.) квалификационного аттестата, специализация"],
        rows, [675, 2085, 2876, 4501]))
    blocks += [_para(""), _para(f"Директор _____________ {dir_init}"), _para("«___» _______ 202_ г.")]
    return _build_docx(blocks)


def gen_form6_opyt(company: dict, experience_objects: list) -> bytes:
    """Точная формулировка и ширины — из реального документа ЧУП «СК76» (проще, чем моя
    предыдущая версия): просто "СВЕДЕНИЯ о наличии опыта генерального подрядчика"."""
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    if experience_objects:
        rows = [[str(i), o.get('name',''), o.get('complexity_class','')] for i, o in enumerate(experience_objects, 1)]
    else:
        rows = [["1", "-", "-"], ["2", "-", "-"]]
    blocks = [
        _para(full_nom, bold=True), _para(""),
        _para("СВЕДЕНИЯ о наличии опыта генерального подрядчика", align='center', bold=True),
        _para(""),
    ]
    # Реальные ширины из СК76: 542,3030,1428
    blocks.append(_table_fixed(["№ п/п", "Наименование объекта", "Класс сложности согласно СН 3.02.07-2020"],
                                rows, [542, 3030, 1428]))
    blocks += [_para(""), _para(f"Директор {full_nom} _____________ {dir_init}")]
    return _build_docx(blocks)


# ═══════════════════ Главный конвейер ═══════════════════
def generate_company_attestation_package(company: dict, attestation_data: dict, api_key, vibe_call_fn,
                                          progress_cb=None) -> dict:
    """api_key/vibe_call_fn не используются (генерация детерминирована), оставлены для
    совместимости сигнатуры с существующим вызовом из generator.py."""
    docs = []
    step = [0]
    category_for_total = _normalize_category(attestation_data.get('category'))
    if attestation_data.get('is_cancellation'):
        total_steps = 1
    elif category_for_total:
        total_steps = 6
    else:
        total_steps = 5

    def p(msg):
        step[0] += 1
        if progress_cb:
            progress_cb(step[0], total_steps, msg)
        print(f"  [company_att {step[0]}] {msg}")

    org = company.get('name', 'company')
    category = _normalize_category(attestation_data.get('category'))
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
        warnings.append(
            f"В данных только {len(itr_list)} человек в ИТР, хотя штат указан как {staff_total} — "
            f"похоже часть людей потерялась при разборе. Проверьте пакет перед подачей."
        )
    empty_itr = [p_.get('fio', f'#{i+1}') for i, p_ in enumerate(itr_list)
                 if not p_.get('diploma_number') and not p_.get('stage_years') and not p_.get('trudovaya_number')]
    if empty_itr:
        warnings.append(
            f"У этих людей вообще не заполнены диплом/стаж/трудовая (в документе будут прочерки): "
            f"{', '.join(empty_itr)}."
        )
    partial_missing_trudovaya = [p_.get('fio', '?') for p_ in itr_list
                                  if p_.get('diploma_number') and not p_.get('trudovaya_number')]
    if partial_missing_trudovaya:
        warnings.append(
            f"У этих людей есть диплом, но нет номера трудовой книжки: {', '.join(partial_missing_trudovaya)}."
        )
    if not workers:
        warnings.append(
            "Реальные данные о рабочих не переданы — раздел «рабочие» в Форме №2 будет пустым, "
            "а не придуман по виду работ. Уточните у клиента список профессий/разрядов/количества."
        )
    elif any(w.get('razryad') and str(w.get('razryad')).upper().strip() not in RAZRYAD_COLUMNS for w in workers):
        bad = [w.get('profession','?') for w in workers if w.get('razryad') and str(w.get('razryad')).upper().strip() not in RAZRYAD_COLUMNS]
        warnings.append(f"У этих рабочих разряд указан не в формате II-VI (римскими цифрами), проверьте: {', '.join(bad)}.")

    if attestation_data.get('is_cancellation'):
        p("Заявление на отмену/исключение")
        docs.append({
            'name': f"{org} - Заявление на отмену.docx",
            'bytes': gen_zayavlenie_otmena(
                company, attestation_data.get('old_attestat_number', ''),
                attestation_data.get('cancellation_reason', 'по заявлению обладателя')
            )
        })
        return {'docs': docs, 'warnings': warnings}

    work_items = attestation_data.get('work_items') or []
    if not work_items and attestation_data.get('work_scope_text'):
        found = find_work_items(attestation_data['work_scope_text'])
        work_items = [code for code, _ in found]
    if not work_items:
        work_items = ['7.4.1']

    p("1. Заявление")
    docs.append({'name': f"{org} - 1. Заявление.docx",
                  'bytes': gen_zayavlenie_company(company, work_items, category)})

    _flat = _flat_work_items()
    work_scope_text = ', '.join(_flat.get(c, c) for c in work_items)

    p("2. Форма №2 (ИТР и рабочие)")
    docs.append({'name': f"{org} - 2. Форма №2 ИТР и рабочие.docx",
                  'bytes': gen_form2_itr(company, itr_list, workers, work_scope_text)})

    p("3. Форма №3 (Трудовые)")
    docs.append({'name': f"{org} - 3. Форма №3 Трудовые.docx",
                  'bytes': gen_form3_trudovye(company, itr_list)})

    p("4. Форма №4 (Дипломы)")
    docs.append({'name': f"{org} - 4. Форма №4 Дипломы.docx",
                  'bytes': gen_form4_diplomy(company, itr_list)})

    p("5. Форма №5 (Аттестаты)")
    docs.append({'name': f"{org} - 5. Форма №5 Аттестаты.docx",
                  'bytes': gen_form5_attestaty(company, itr_list)})

    if category:
        p("6. Форма №6 (Опыт генподрядчика)")
        docs.append({'name': f"{org} - 6. Форма №6 Опыт.docx",
                      'bytes': gen_form6_opyt(company, experience_objects)})

    return {'docs': docs, 'warnings': warnings}
