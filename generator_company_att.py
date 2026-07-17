"""
Модуль генерации документов на аттестацию ЮРИДИЧЕСКОГО ЛИЦА (компании) —
аттестат соответствия (СТ — подряд, ГС — генподряд).

АРХИТЕКТУРА: документы строятся программно через РУЧНОЙ OOXML (реальные таблицы Word),
БЕЗ внешних зависимостей (не python-docx/lxml — они требуют lxml, которая иногда не
устанавливается на минимальных серверных окружениях и роняла весь модуль ошибкой
"No module named 'docx'"). Используется только встроенный модуль zipfile — гарантированно
работает на любом сервере с Python, без риска сбоя установки пакетов.
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


def find_work_items(query: str, max_items=10):
    q = query.lower()
    q_stems = {w[:5] for w in re.findall(r'[а-яё]{5,}', q)}
    items = CLASSIFIER['punkt_7_smr']['items']
    found = []
    for code, text in items.items():
        tl = text.lower()
        t_stems = re.findall(r'[а-яё]{5,}', tl)
        score = sum(1 for w in t_stems if w[:5] in q_stems)
        if score > 0:
            found.append((score, code, text))
    found.sort(key=lambda x: -x[0])
    return [(code, text) for _, code, text in found[:max_items]]


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


def _para(text='', align='left', bold=False, size=22, space_after=120):
    align_xml = {'left': 'left', 'center': 'center', 'right': 'right', 'justify': 'both'}.get(align, 'left')
    b = '<w:b/>' if bold else ''
    return (f'<w:p><w:pPr><w:jc w:val="{align_xml}"/><w:spacing w:after="{space_after}" w:line="276" w:lineRule="auto"/></w:pPr>'
            f'<w:r><w:rPr>{b}<w:sz w:val="{size}"/><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/></w:rPr>'
            f'<w:t xml:space="preserve">{_esc(text) if text else " "}</w:t></w:r></w:p>')


def _cell(text, w, bold=False, align='left'):
    align_xml = {'left': 'left', 'center': 'center'}.get(align, 'left')
    b = '<w:b/>' if bold else ''
    return (f'<w:tc><w:tcPr><w:tcW w:w="{w}" w:type="dxa"/><w:vAlign w:val="center"/></w:tcPr>'
            f'<w:p><w:pPr><w:jc w:val="{align_xml}"/><w:spacing w:after="0"/></w:pPr>'
            f'<w:r><w:rPr>{b}<w:sz w:val="18"/><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/></w:rPr>'
            f'<w:t xml:space="preserve">{_esc(text)}</w:t></w:r></w:p></w:tc>')


def _table(headers, rows, widths):
    """widths — относительные веса колонок, сумма пересчитывается в твипы под ширину страницы."""
    total_w = 9350  # ширина текстовой области A4 portrait минус поля, в твипах (~16.5 см)
    wsum = sum(widths)
    widths_tw = [int(total_w * w / wsum) for w in widths]
    grid = ''.join(f'<w:gridCol w:w="{w}"/>' for w in widths_tw)
    hdr = '<w:tr>' + ''.join(_cell(h, w, True, 'center') for h, w in zip(headers, widths_tw)) + '</w:tr>'
    body = ''
    for row in rows:
        body += '<w:tr>' + ''.join(_cell(v, w) for v, w in zip(row, widths_tw)) + '</w:tr>'
    borders = ('<w:tblBorders>'
               '<w:top w:val="single" w:sz="4" w:color="000000"/>'
               '<w:left w:val="single" w:sz="4" w:color="000000"/>'
               '<w:bottom w:val="single" w:sz="4" w:color="000000"/>'
               '<w:right w:val="single" w:sz="4" w:color="000000"/>'
               '<w:insideH w:val="single" w:sz="4" w:color="000000"/>'
               '<w:insideV w:val="single" w:sz="4" w:color="000000"/>'
               '</w:tblBorders>')
    return (f'<w:tbl><w:tblPr><w:tblW w:w="0" w:type="auto"/>{borders}</w:tblPr>'
            f'<w:tblGrid>{grid}</w:tblGrid>{hdr}{body}</w:tbl>')


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


def _build_docx(body_blocks, landscape=False) -> bytes:
    body = ''.join(body_blocks)
    if landscape:
        sect = '<w:sectPr><w:pgSz w:w="16838" w:h="11906" w:orient="landscape"/><w:pgMar w:top="850" w:right="850" w:bottom="850" w:left="850"/></w:sectPr>'
    else:
        sect = '<w:sectPr><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="850" w:right="1417" w:bottom="850" w:left="1417"/></w:sectPr>'
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
    blocks.append(_para("ЗАЯВЛЕНИЕ", align='center', bold=True, size=26))
    blocks.append(_para("о получении аттестата соответствия", align='center', bold=True))
    blocks.append(_para(""))
    blocks.append(_para(f"Прошу провести аттестацию {full_gen} на право осуществления:", align='justify'))

    if category:
        blocks.append(_para(
            f"6. Выполнение функций генерального подрядчика со стоимостью строительства свыше "
            f"{CLASSIFIER['_meta']['genpodryad_min_cost']}. Соответствующей квалификационным "
            f"требованиям, предъявляемым для получения аттестата соответствия {category} "
            f"класса(ов) сложности.", align='justify'))

    blocks.append(_para("7. Выполнение строительно-монтажных работ:", align='justify'))
    for code in work_items:
        text = CLASSIFIER['punkt_7_smr']['items'].get(code, code)
        blocks.append(_para(f"{code}. {text};", align='justify'))

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

    prilozhenie_rows = [
        ["1", "Легализованная выписка из торгового реестра страны, в которой иностранная "
              "организация учреждена, или иное эквивалентное доказательство юридического статуса "
              "иностранной организации в соответствии с законодательством страны ее учреждения "
              "(для заявителя – нерезидента).", ""],
        ["2", "Сведения о составе и профессиональной квалификации руководящих работников, "
              "специалистов и рабочих, работающих по основному месту работы (форма № 2).", ""],
        ["3", "Сводный список и копии трудовых книжек руководящих работников, специалистов, "
              "работающих по основному месту работы (форма № 3).", ""],
        ["4", "Сводный список и копии дипломов руководящих работников, специалистов, работающих "
              "по основному месту работы (форма № 4).", ""],
        ["5", "Сводный список и копии квалификационных аттестатов руководящих работников, "
              "специалистов, работающих по основному месту работы (форма № 5).", ""],
    ]
    if category:
        prilozhenie_rows.append(
            ["6", "Сведения о наличии опыта выполнения работ (оказания услуг) по заявляемому виду "
                  "деятельности в области строительства за последние пять лет в качестве "
                  "генерального подрядчика (форма № 6).", ""]
        )
    blocks.append(_table(["№ п/п", "Наименование документа", "Кол-во листов"], prilozhenie_rows,
                          widths=[1, 8, 1.5]))
    blocks.append(_para(""))
    blocks.append(_para("Всего:"))
    blocks.append(_para(""))
    blocks.append(_para(f"{dir_pos} _____________ {dir_init}"))

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
        _para("ЗАЯВЛЕНИЕ", align='center', bold=True, size=26),
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


# ═══════════════════ Документ 2: Форма №2 — ИТР + рабочие (landscape) ═══════════════════
def gen_form2_itr(company: dict, itr_list: list, workers: list, work_scope_text: str) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    total_staff = company.get('staff_total') or (len(itr_list) + sum(w.get('count', 0) or 0 for w in workers))
    n_itr = len(itr_list)

    blocks = [
        _para(full_nom, bold=True),
        _para(""),
        _para("Форма № 2", align='center', bold=True),
        _para("СВЕДЕНИЯ о составе и профессиональной квалификации руководящих работников, "
              "специалистов и рабочих, работающих по основному месту работы", align='center', bold=True),
        _para(""),
        _para(f"Общая численность работающих {total_staff} чел., в том числе по заявляемому виду "
              f"деятельности {total_staff} чел. по состоянию на ___.___.____ ; численность "
              f"инженерно-технических работников по заявляемому виду деятельности {n_itr} чел."),
        _para(f"Область деятельности: {work_scope_text}"),
        _para(""),
    ]

    itr_rows = []
    for i, p_ in enumerate(itr_list, 1):
        obrazovanie = (f"{p_.get('education_level','')}, диплом {p_.get('diploma_number') or '—'} "
                        f"выдан {p_.get('diploma_date') or '—'}, {p_.get('diploma_institution','')}, "
                        f"{p_.get('diploma_speciality','')}, {p_.get('diploma_qualification','')}")
        stazh = f"{p_.get('stage_years') or '—'} / {p_.get('stage_years_here') or '—'}"
        trudovaya = (f"{p_.get('trudovaya_number') or '—'}, Пр.№{p_.get('order_number') or '—'} "
                     f"от {p_.get('hire_date') or '—'}")
        attestat = p_.get('attestat_number') or '—'
        if p_.get('attestat_date'):
            attestat += f" от {p_.get('attestat_date')}"
        if p_.get('attestat_specialization'):
            attestat += f" {p_.get('attestat_specialization')}"
        itr_rows.append([str(i), p_.get('position',''), p_.get('fio',''), obrazovanie, stazh, trudovaya, attestat])

    blocks.append(_table(
        ["№", "Должность", "ФИО", "Образование (уровень, диплом, учреждение, специальность, квалификация)",
         "Стаж (по деятельности / у нанимателя)", "Трудовая книжка + приказ", "Аттестат, специализация"],
        itr_rows, widths=[0.6, 2.2, 2.4, 4.5, 1.8, 2.6, 3.0]))

    blocks.append(_para(""))
    blocks.append(_para("Раздел 2 — рабочие строительных профессий, соответствующих заявляемым видам "
                         "деятельности в области строительства согласно технологической документации на "
                         "производство строительно-монтажных работ, работающих по основному месту работы:"))
    if workers:
        w_rows = [[str(i), w.get('profession',''), w.get('razryad','') or '—', str(w.get('count','') or '—')]
                  for i, w in enumerate(workers, 1)]
        blocks.append(_table(["№", "Профессия рабочего", "Разряд", "Количество человек"], w_rows,
                              widths=[0.6, 5, 2, 2.5]))
    else:
        blocks.append(_para("Сведения о рабочих не предоставлены на момент подготовки документа."))

    blocks.append(_para(""))
    blocks.append(_para(f"Директор {full_nom} _____________ {dir_init}"))
    blocks.append(_para("«___» _______ 202_ г."))
    return _build_docx(blocks, landscape=True)


# ═══════════════════ Документы 3-5: сводные списки ═══════════════════
def gen_form3_trudovye(company: dict, itr_list: list) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = [[str(i), p_.get('fio',''), p_.get('position',''), p_.get('trudovaya_number') or '—']
            for i, p_ in enumerate(itr_list, 1)]
    blocks = [
        _para(full_nom, bold=True), _para(""),
        _para("Форма № 3", align='center', bold=True),
        _para("СВОДНЫЙ СПИСОК трудовых книжек руководящих работников, специалистов, работающих по "
              "основному месту работы", align='center', bold=True),
        _para(""),
    ]
    blocks.append(_table(["№ п/п", "Ф.И.О.", "Должность в соответствии с записью в трудовой книжке",
                           "Номер трудовой книжки"], rows, widths=[1, 3.5, 4.5, 2.5]))
    blocks += [_para(""), _para(f"Директор _____________ {dir_init}"), _para("«___» _______ 202_ г.")]
    return _build_docx(blocks)


def gen_form4_diplomy(company: dict, itr_list: list) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = [[str(i), p_.get('fio',''), p_.get('diploma_number') or '—'] for i, p_ in enumerate(itr_list, 1)]
    blocks = [
        _para(full_nom, bold=True), _para(""),
        _para("Форма № 4", align='center', bold=True),
        _para("СВОДНЫЙ СПИСОК дипломов руководящих работников, специалистов, работающих по "
              "основному месту работы", align='center', bold=True),
        _para(""),
    ]
    blocks.append(_table(["№ п/п", "Ф.И.О.", "Номер диплома"], rows, widths=[1, 5, 4]))
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
        _para("Форма № 5", align='center', bold=True),
        _para("СВОДНЫЙ СПИСОК квалификационных аттестатов руководящих работников, специалистов, "
              "работающих по основному месту работы", align='center', bold=True),
        _para(""),
    ]
    blocks.append(_table(["№ п/п", "Ф.И.О.", "Должность", "Номер и срок действия аттестата, специализация"],
                          rows, widths=[1, 3.5, 3, 5.5]))
    blocks += [_para(""), _para(f"Директор _____________ {dir_init}"), _para("«___» _______ 202_ г.")]
    return _build_docx(blocks)


def gen_form6_opyt(company: dict, experience_objects: list) -> bytes:
    full_nom = _quoted_name(company, 'nom')
    dir_init = _dir_init(company.get('director_fio', ''))
    if experience_objects:
        rows = [[str(i), o.get('name',''), o.get('complexity_class','')] for i, o in enumerate(experience_objects, 1)]
    else:
        rows = [["1", "-", "-"], ["2", "-", "-"]]
    blocks = [
        _para(full_nom, bold=True), _para(""),
        _para("Форма № 6", align='center', bold=True),
        _para("Сведения о наличии опыта выполнения работ (оказания услуг) по заявляемому виду "
              "деятельности в области строительства за последние пять лет в качестве генерального "
              "подрядчика", align='center', bold=True),
        _para(""),
    ]
    blocks.append(_table(["№", "Наименование объекта", "Класс сложности согласно СН 3.02.07-2020"],
                          rows, widths=[1, 6.5, 3.5]))
    blocks += [_para(""), _para(f"Директор _____________ {dir_init}")]
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

    # Автоматический расчёт стажа из сырых дат (employment_periods), если он ещё не посчитан
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
    elif all(not w.get('count') for w in workers):
        warnings.append("В разделе «рабочие» Формы №2 не указано количество человек по профессиям.")

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

    work_scope_text = ', '.join(CLASSIFIER['punkt_7_smr']['items'].get(c, c) for c in work_items)

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
