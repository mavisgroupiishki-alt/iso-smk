"""
Модуль генерации документов на аттестацию ЮРИДИЧЕСКОГО ЛИЦА (компании) —
аттестат соответствия (СТ — подряд, ГС — генподряд).

Отличие от generator_att.py (аттестация ФИЗЛИЦА/специалиста):
- статья 35 Кодекса (не 40)
- документ на компанию целиком, не на одного человека
- зависит от уже полученных индивидуальных аттестатов специалистов (Форма №5)
"""
import json, re, io, zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
CLASSIFIER = json.loads((BASE_DIR / 'classifier_company_att.json').read_text('utf-8'))


def find_work_items(query: str, max_items=10):
    """Ищет пункты 7.x классификатора по свободному тексту клиента.
    Сравнивает по началу слова (5+ букв), а не по точной форме — русский язык
    сильно склоняется ("кровли"/"кровель"/"кровельный"), точное совпадение почти
    никогда не сработает."""
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


def check_category_requirements(category: str, staff_total: int, has_smetchik: bool,
                                 experience_objects: list, prior_category_years: int = 0) -> list:
    """
    Возвращает список предупреждений (пустой список = всё ок).
    Игорь ОБЯЗАН вызывать эту проверку перед формированием пакета с генподрядом (пункт 6).
    """
    warnings = []
    thresholds = CLASSIFIER['_meta']['category_thresholds'].get(str(category))
    if not thresholds:
        return [f"Неизвестная категория: {category}"]

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
                f"(генподрядчик, привлекавший субподряд, объект введён в эксплуатацию не позднее 5 лет назад, "
                f"не текущий ремонт). Предоставлено объектов: {n_objects}."
            )
        if thresholds.get('prior_category') and prior_category_years < thresholds.get('prior_years', 0):
            warnings.append(
                f"Для категории {category} нужно не менее {thresholds['prior_years']} лет владения категорией "
                f"{thresholds['prior_category']} — по данным клиента стаж владения {prior_category_years} лет."
            )
    return warnings


def _fio_init(fio: str) -> str:
    parts = fio.strip().split()
    if len(parts) >= 2:
        return f"{parts[0][0]}.{parts[1][0] if len(parts)>1 else ''}." if False else fio
    return fio


def _full_name(company: dict) -> str:
    form = company.get('form', 'ООО')
    name = company.get('name', '')
    return f'{form} "{name}"' if form.upper() != 'ЧУП' else f'Частное унитарное предприятие "{name}"'


def _dir_init(fio: str) -> str:
    parts = fio.strip().split()
    if len(parts) >= 3:
        return f"{parts[1][0]}.{parts[2][0]}. {parts[0]}"
    return fio


def vibe_call(prompt, api_key, vibe_call_fn, max_tokens=2500):
    return vibe_call_fn([{"role": "user", "content": prompt}], api_key, max_tokens=max_tokens)


def gen_zayavlenie_company(company: dict, work_items: list, category: str, api_key, vibe_call_fn) -> str:
    """
    Заявление о получении аттестата соответствия (юрлицо).
    category=None → аттестат "Подрядчика" (СТ), только пункт 7.
    category="1".."4" → аттестат генподрядчика (ГС), пункт 6 + пункт 7.
    """
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    dir_pos = company.get('director_position', 'Директора')

    items_text = '\n'.join(f"{code}. {CLASSIFIER['punkt_7_smr']['items'].get(code, code)};" for code in work_items)

    genpodryad_block = ""
    if category:
        genpodryad_block = (
            f"6. Выполнение функций генерального подрядчика со стоимостью строительства свыше "
            f"{CLASSIFIER['_meta']['genpodryad_min_cost']}. Соответствующей квалификационным требованиям, "
            f"предъявляемым для получения аттестата соответствия {category} класса(ов) сложности.\n"
        )

    prompt = f"""Составь ЗАЯВЛЕНИЕ о получении аттестата соответствия для ЮРИДИЧЕСКОГО ЛИЦА
(Беларусь, РУП «Белстройцентр»), строго по структуре и формулировкам реальных поданных образцов.

Шапка (заявитель):
{full}
{company.get('address','')}
р/с: {company.get('bank_details','')}
УНП {company.get('unp','')}
Тел./факс: {company.get('phone','')}
e-mail: {company.get('email','')}
Директор: {company.get('director_fio','')}
Главный бухгалтер: {company.get('glavbukh_fio','')}

Исх. № от ___.___.____ г.
РУП «БЕЛСТРОЙЦЕНТР»
ул. Р. Люксембург, 101
220036, г. Минск

{full}
{company.get('address','')}
УНП {company.get('unp','')}
Тел.: {company.get('phone','')}
e-mail: {company.get('email','')}

Заголовок:
ЗАЯВЛЕНИЕ
о получении аттестата соответствия

Тело:
Прошу провести аттестацию {full} на право осуществления:

{genpodryad_block}7. Выполнение строительно-монтажных работ:
{items_text}

Сведения об обособленных подразделениях, в том числе филиалах (при их наличии): нет.

В соответствии с {CLASSIFIER['_meta']['legal_basis']} прошу оформить {full} аттестат соответствия на
бумажном носителе. Сведения, изложенные в заявлении и прилагаемых к нему документах, достоверны.

Приложение (список копий документов с количеством листов — используй список для юрлица, подставь
разумные плейсхолдеры "___" для количества листов там где неизвестно):
{chr(10).join(CLASSIFIER['prilozhenie_company'])}

{dir_pos.replace('Директора','Директор')}    _____________    {dir_init}

ПРАВИЛО: если category=None — блок про пункт 6 не пиши вообще, заявление только про пункт 7.
Отвечай только текстом документа, без markdown."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=2500)


def gen_opis(company: dict, work_scope_text: str, api_key, vibe_call_fn) -> str:
    """
    Опись — отдельный документ для аттестации КОМПАНИИ (в отличие от аттестации специалиста,
    где опись не наша зона — для юрлица это делаем мы).
    Содержит: область аттестации, полное наименование организации, ФИО директора.
    """
    full = _full_name(company)
    prompt = f"""Составь ОПИСЬ документов для аттестации организации (Беларусь, РУП «Белстройцентр»).

ОПИСЬ

Организация: {full}
Директор: {company.get('director_fio','')}
Область аттестации: {work_scope_text}

Перечень документов в папке (пронумерованный список с указанием количества листов, используй
плейсхолдеры "___ л." там где неизвестно):
1. Заявление
2. Опись
3. Справка ИТР
4. Сведения о рабочих
5. Сводный список трудовых книжек
6. Сводный список дипломов
7. Сводный список аттестатов
8. Справка по опыту (если применимо)

Директор    _____________    {company.get('director_fio','')}

Отвечай только текстом документа."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1000)


def gen_svedeniya_o_rabochih(company: dict, workers: list, work_scope_text: str, api_key, vibe_call_fn) -> str:
    """
    Сведения о рабочих — отдельный документ (не ИТР!) для аттестации компании.
    Заполняется соотнесением выполняемых видов работ и профессий рабочих.
    workers: [{profession, count, razryad}]
    """
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = '\n'.join(
        f"{i}. {w.get('profession','')} | {w.get('count','')} чел. | разряд {w.get('razryad','')}"
        for i, w in enumerate(workers, 1)
    ) if workers else "1. — | — | —"

    prompt = f"""Составь документ «СВЕДЕНИЯ О РАБОЧИХ» для аттестации организации (Беларусь, Белстройцентр).

{full}

СВЕДЕНИЯ О РАБОЧИХ

Область деятельности: {work_scope_text}

Таблица (№ | Профессия рабочего (в соответствии с выполняемыми видами работ) | Количество человек |
Разряд):
{rows}

Директор    _____________    {dir_init}

ПРАВИЛО: профессии рабочих должны логически соответствовать заявленным видам СМР (например, для
кровельных работ — кровельщик, для общестроя — каменщик/бетонщик и т.д.). Отвечай только текстом
документа."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1200)


def gen_zayavlenie_otmena(company: dict, old_attestat_number: str, reason: str, api_key, vibe_call_fn) -> str:
    """Заявление о прекращении действия / исключении части видов работ из аттестата соответствия."""
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    prompt = f"""Составь ЗАЯВЛЕНИЕ о прекращении действия аттестата соответствия (или исключении отдельных
видов работ — в зависимости от причины ниже), строго по формату реальных образцов Беларуси (РУП «Белстройцентр»).

{full}
{company.get('address','')}
УНП {company.get('unp','')}
Тел.: {company.get('phone','')}
e-mail: {company.get('email','')}

Исх. № от ___.___.____ г.
РУП «БЕЛСТРОЙЦЕНТР»
ул. Р. Люксембург, 101
220036, г. Минск

{full}
{company.get('address','')}
УНП {company.get('unp','')}

ЗАЯВЛЕНИЕ
о прекращении действия аттестата соответствия

{full} просит прекратить действие выданного ранее аттестата соответствия от ___ г. № {old_attestat_number}.

Причина: {reason}

В соответствии со статьёй 36 Кодекса Республики Беларусь об архитектурной, градостроительной и
строительной деятельности.

Директор    _____________    {dir_init}

Отвечай только текстом документа."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1200)


def gen_form2_itr(company: dict, itr_list: list, work_scope_text: str, api_key, vibe_call_fn) -> str:
    """Форма №2 — Сведения о составе и профессиональной квалификации ИТР (самая подробная таблица)."""
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    total_staff = company.get('staff_total', len(itr_list))
    n_itr = len(itr_list)

    rows = []
    for i, p in enumerate(itr_list, 1):
        rows.append(
            f"{i}. {p.get('position','')} | {p.get('fio','')} | {p.get('education_level','')}, "
            f"диплом {p.get('diploma_number','')} выдан {p.get('diploma_date','')}, "
            f"{p.get('diploma_institution','')}, {p.get('diploma_speciality','')}, {p.get('diploma_qualification','')} | "
            f"стаж по виду деятельности: {p.get('stage_years','')} лет, у данного нанимателя: {p.get('stage_years_here','')} | "
            f"Трудовая книжка {p.get('trudovaya_number','')}, Приказ №{p.get('order_number','')} от {p.get('hire_date','')} | "
            f"{p.get('attestat_number','—')} {('от '+p.get('attestat_date','')) if p.get('attestat_date') else ''} "
            f"{p.get('attestat_specialization','')}"
        )

    prompt = f"""Составь ФОРМУ №2 — «СВЕДЕНИЯ о составе и профессиональной квалификации руководящих работников,
специалистов и рабочих, работающих по основному месту работы» — строго по официальному формату Беларуси
(РУП «Белстройцентр»), 7-колоночная таблица.

{full}

Форма № 2
СВЕДЕНИЯ о составе и профессиональной квалификации руководящих работников, специалистов и рабочих,
работающих по основному месту работы

Общая численность работающих {total_staff} чел., в том числе по заявляемому виду деятельности {total_staff} чел.
по состоянию на ___.___.____ ; численность инженерно-технических работников по заявляемому виду деятельности {n_itr} чел.

Область деятельности: {work_scope_text}

Таблица (колонки: № | Должность | ФИО | Образование (уровень, диплом №, дата, учреждение, специальность,
квалификация) | Стаж работы по виду деятельности / у данного нанимателя | Трудовая книжка + приказ о приёме |
Номер и дата аттестата, специализация):

{chr(10).join(rows)}

Директор    _____________    {dir_init}

ПРАВИЛО: сохрани все данные из строк выше буквально, не выдумывай. Если у человека нет ещё аттестата —
пиши "—" в последней колонке, не придумывай номер. Отвечай только текстом документа (в виде таблицы построчно)."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=3000)


def gen_form3_trudovye(company: dict, itr_list: list, api_key, vibe_call_fn) -> str:
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = '\n'.join(
        f"{i}. {p.get('fio','')} | {p.get('position','')} | {p.get('trudovaya_number','')}"
        for i, p in enumerate(itr_list, 1)
    )
    prompt = f"""Составь ФОРМУ №3 — «СВОДНЫЙ СПИСОК трудовых книжек руководящих работников, специалистов,
работающих по основному месту работы» (Беларусь, Белстройцентр).

{full}

Форма № 3
СВОДНЫЙ СПИСОК трудовых книжек руководящих работников, специалистов, работающих по основному месту работы

Таблица (№ | Ф.И.О. | Должность в соответствии с записью в трудовой книжке | Номер трудовой книжки):
{rows}

Директор    _____________    {dir_init}
«___» _______ 202_ г.

Отвечай только текстом документа."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1500)


def gen_form4_diplomy(company: dict, itr_list: list, api_key, vibe_call_fn) -> str:
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = '\n'.join(
        f"{i}. {p.get('fio','')} | {p.get('diploma_number','')}"
        for i, p in enumerate(itr_list, 1)
    )
    prompt = f"""Составь ФОРМУ №4 — «СВОДНЫЙ СПИСОК дипломов руководящих работников, специалистов,
работающих по основному месту работы» (Беларусь, Белстройцентр).

{full}

Форма № 4
СВОДНЫЙ СПИСОК дипломов руководящих работников, специалистов, работающих по основному месту работы

Таблица (№ | Ф.И.О. | Номер диплома):
{rows}

Директор    _____________    {dir_init}
«___» _______ 202_ г.

Отвечай только текстом документа."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1500)


def gen_form5_attestaty(company: dict, itr_list: list, api_key, vibe_call_fn) -> str:
    """Сводный список квалификационных аттестатов — прямая зависимость от продукта 'att'."""
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    rows = []
    for i, p in enumerate(itr_list, 1):
        att = p.get('attestat_number', '')
        if att:
            att_info = f"{att} с {p.get('attestat_date_from','')} г. по {p.get('attestat_date_to','')} г. {p.get('attestat_specialization','')}"
        else:
            att_info = "нет аттестата / в процессе получения"
        rows.append(f"{i}. {p.get('fio','')} | {p.get('position','')} | {att_info}")

    prompt = f"""Составь ФОРМУ №5 — «СВОДНЫЙ СПИСОК квалификационных аттестатов руководящих работников,
специалистов, работающих по основному месту работы» (Беларусь, Белстройцентр).

{full}

Форма № 5
СВОДНЫЙ СПИСОК квалификационных аттестатов руководящих работников, специалистов, работающих по основному месту работы

Таблица (№ | Ф.И.О. | Должность в соответствии с записью в трудовой книжке | Номер и срок действия
квалификационного аттестата, специализация аттестации):
{chr(10).join(rows)}

Директор    _____________    {dir_init}
«___» _______ 202_ г.

ПРАВИЛО: если у человека нет аттестата — пиши буквально "нет аттестата / в процессе получения", не выдумывай номер.
Отвечай только текстом документа."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1800)


def gen_form6_opyt(company: dict, experience_objects: list, api_key, vibe_call_fn) -> str:
    """Сведения о наличии опыта генерального подрядчика — только для категорий 1-3."""
    full = _full_name(company)
    dir_init = _dir_init(company.get('director_fio', ''))
    if experience_objects:
        rows = '\n'.join(
            f"{i}. {o.get('name','')} | {o.get('complexity_class','')}"
            for i, o in enumerate(experience_objects, 1)
        )
    else:
        rows = "1. - | -\n2. - | -"

    prompt = f"""Составь документ «СВЕДЕНИЯ о наличии опыта генерального подрядчика» (Беларусь, Белстройцентр).

{full}

СВЕДЕНИЯ о наличии опыта генерального подрядчика

Таблица (№ | Наименование объекта | Класс сложности согласно СН 3.02.07-2020):
{rows}

Директор    _____________    {dir_init}

Отвечай только текстом документа, без лишних пояснений."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1000)


def create_docx_from_text(text: str) -> bytes:
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

    def escape(s):
        return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

    def make_para(line):
        line = escape(line)
        is_heading = (line.isupper() or line.startswith('ЗАЯВЛЕНИЕ') or line.startswith('Форма №') or
                      line.startswith('СВЕДЕНИЯ')) and len(line) < 130
        align = 'center' if is_heading else 'both'
        bold = 'true' if is_heading else 'false'
        return (f'<w:p><w:pPr><w:jc w:val="{align}"/><w:spacing w:line="360" w:lineRule="auto"/></w:pPr>'
                f'<w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/>'
                f'<w:b w:val="{bold}"/><w:sz w:val="22"/></w:rPr>'
                f'<w:t xml:space="preserve">{line if line.strip() else " "}</w:t></w:r></w:p>')

    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    paras = '\n'.join(make_para(l) for l in lines)
    doc_xml = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
               f'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               f'<w:body>{paras}<w:sectPr><w:pgSz w:w="16838" w:h="11906" w:orient="landscape"/>'
               f'<w:pgMar w:top="850" w:right="850" w:bottom="850" w:left="850"/></w:sectPr>'
               f'</w:body></w:document>')

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types)
        zf.writestr('_rels/.rels', rels)
        zf.writestr('word/document.xml', doc_xml)
        zf.writestr('word/_rels/document.xml.rels', word_rels)
    return buf.getvalue()


def generate_company_attestation_package(company: dict, attestation_data: dict, api_key, vibe_call_fn,
                                          progress_cb=None) -> dict:
    """
    attestation_data:
      category: "1"|"2"|"3"|"4"|None
      work_items: [список кодов "7.x"] или [] (тогда сформируется из scope-текста)
      work_scope_text: свободный текст видов работ для поиска в классификаторе
      itr: [{fio, position, education_level, diploma_number, diploma_date, diploma_institution,
             diploma_speciality, diploma_qualification, stage_years, stage_years_here,
             trudovaya_number, order_number, hire_date,
             attestat_number, attestat_date_from, attestat_date_to, attestat_specialization}]
      staff_total: int
      has_smetchik: bool
      experience_objects: [{name, complexity_class}]
      prior_category_years: int
      is_cancellation: bool
      old_attestat_number: str
      cancellation_reason: str
    Возвращает {'docs': [...], 'warnings': [...]}
    """
    docs = []
    step = [0]
    category_for_total = attestation_data.get('category')
    if attestation_data.get('is_cancellation'):
        total_steps = 1
    else:
        thresholds_for_total = CLASSIFIER['_meta']['category_thresholds'].get(str(category_for_total), {})
        total_steps = 7 + (1 if category_for_total and thresholds_for_total.get('objects_required', 0) > 0 else 0)

    def p(msg):
        step[0] += 1
        if progress_cb:
            progress_cb(step[0], total_steps, msg)
        print(f"  [company_att {step[0]}] {msg}")

    org = company.get('name', 'company')
    category = attestation_data.get('category')
    itr_list = attestation_data.get('itr', [])
    staff_total = attestation_data.get('staff_total', len(itr_list))
    has_smetchik = attestation_data.get('has_smetchik', False)
    experience_objects = attestation_data.get('experience_objects', [])
    prior_years = attestation_data.get('prior_category_years', 0)

    warnings = []
    if category:
        warnings = check_category_requirements(category, staff_total, has_smetchik, experience_objects, prior_years)

    if attestation_data.get('is_cancellation'):
        p("Заявление на отмену/исключение")
        text = gen_zayavlenie_otmena(
            company, attestation_data.get('old_attestat_number', ''),
            attestation_data.get('cancellation_reason', 'по заявлению обладателя'),
            api_key, vibe_call_fn
        )
        docs.append({'name': f"{org} - Заявление на отмену.docx", 'bytes': create_docx_from_text(text)})
        return {'docs': docs, 'warnings': warnings}

    work_items = attestation_data.get('work_items') or []
    if not work_items and attestation_data.get('work_scope_text'):
        found = find_work_items(attestation_data['work_scope_text'])
        work_items = [code for code, _ in found]
    if not work_items:
        work_items = ['7.4.1']  # безопасный дефолт, лучше чем пустое заявление

    p("1. Заявление")
    text = gen_zayavlenie_company(company, work_items, category, api_key, vibe_call_fn)
    docs.append({'name': f"{org} - 1. Заявление.docx", 'bytes': create_docx_from_text(text)})

    work_scope_text = ', '.join(CLASSIFIER['punkt_7_smr']['items'].get(c, c) for c in work_items)

    p("2. Опись")
    text = gen_opis(company, work_scope_text, api_key, vibe_call_fn)
    docs.append({'name': f"{org} - 2. Опись.docx", 'bytes': create_docx_from_text(text)})

    p("3. Форма ИТР")
    text = gen_form2_itr(company, itr_list, work_scope_text, api_key, vibe_call_fn)
    docs.append({'name': f"{org} - 3. ИТР.docx", 'bytes': create_docx_from_text(text)})

    workers = attestation_data.get('workers', [])
    p("4. Сведения о рабочих")
    text = gen_svedeniya_o_rabochih(company, workers, work_scope_text, api_key, vibe_call_fn)
    docs.append({'name': f"{org} - 4. Сведения о рабочих.docx", 'bytes': create_docx_from_text(text)})

    p("5. Трудовые")
    text = gen_form3_trudovye(company, itr_list, api_key, vibe_call_fn)
    docs.append({'name': f"{org} - 5. Трудовые.docx", 'bytes': create_docx_from_text(text)})

    p("6. Дипломы")
    text = gen_form4_diplomy(company, itr_list, api_key, vibe_call_fn)
    docs.append({'name': f"{org} - 6. Дипломы.docx", 'bytes': create_docx_from_text(text)})

    p("7. Аттестаты")
    text = gen_form5_attestaty(company, itr_list, api_key, vibe_call_fn)
    docs.append({'name': f"{org} - 7. Аттестаты.docx", 'bytes': create_docx_from_text(text)})

    if category:
        thresholds = CLASSIFIER['_meta']['category_thresholds'].get(str(category), {})
        if thresholds.get('objects_required', 0) > 0:
            p("8. Опыт генподрядчика")
            text = gen_form6_opyt(company, experience_objects, api_key, vibe_call_fn)
            docs.append({'name': f"{org} - 8. Опыт.docx", 'bytes': create_docx_from_text(text)})

    return {'docs': docs, 'warnings': warnings}
