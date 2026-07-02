"""
Модуль генерации заявлений на аттестацию специалистов (РУП «Белстройцентр»).
На основе Постановления Минстройархитектуры РБ от 14.06.2024 № 70.

Один специалист может подаваться на несколько специализаций —
для каждой генерируется отдельное заявление (+ справка-расшифровка при необходимости).
"""
import json, re, io, zipfile
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent.resolve()
CLASSIFIER = json.loads((BASE_DIR / 'classifier_att.json').read_text('utf-8'))

GRADE_ORDER = ["Мастер", "Производитель работ (прораб)", "Главный инженер"]

THRESHOLDS_MONTHS = {
    "Мастер":                       {"высшее": 3,  "среднее специальное": 6},
    "Производитель работ (прораб)": {"высшее": 12, "среднее специальное": 36},
    "Главный инженер":              {"высшее": 60, "среднее специальное": 96},
}


def find_specialization(query: str):
    """Ищет специализацию в классификаторе по свободному тексту клиента."""
    q = query.lower().strip()
    best, best_score = None, 0
    for spec in CLASSIFIER['specializations']:
        key = spec['key'].lower()
        score = 0
        for word in re.findall(r'[а-яёa-z]{4,}', q):
            if word in key:
                score += 1
        if key in q or q in key:
            score += 5
        if score > best_score:
            best_score, best = score, spec
    return best


def calculate_grade(education_level: str, stage_months: int) -> str:
    """
    education_level: 'высшее' | 'среднее специальное'
    stage_months: стаж по данной специализации в месяцах
    Возвращает наивысший грейд, на который хватает стажа (грейд не понижаем искусственно
    ниже того, что просит клиент — эта функция даёт рекомендацию).
    """
    edu = 'высшее' if 'высш' in education_level.lower() else 'среднее специальное'
    grade = None
    for g in GRADE_ORDER:
        need = THRESHOLDS_MONTHS[g][edu]
        if stage_months >= need:
            grade = g
    return grade or "Недостаточно стажа даже для Мастера"


def _fio_short(fio: str) -> str:
    parts = fio.strip().split()
    if len(parts) >= 3:
        return f"{parts[0]} {parts[1][0]}.{parts[2][0]}."
    return fio


def vibe_call(prompt, api_key, vibe_call_fn, max_tokens=2500):
    """vibe_call_fn — прокидывается из основного server.py/generator.py, чтобы не дублировать HTTP-логику."""
    return vibe_call_fn([{"role": "user", "content": prompt}], api_key, max_tokens=max_tokens)


def gen_zayavlenie(company: dict, person: dict, specialization: str, grade: str, api_key, vibe_call_fn) -> str:
    """
    Заявление о проведении аттестации специалиста.
    Структура и формулировки — 1:1 с реальными образцами (Хмара/Шемет/Потоцкий).
    """
    full_name = f"{company.get('form','ООО')} «{company.get('name','')}»"
    dir_fio = company.get('director_fio', '')
    dir_pos = company.get('director_position', 'Директора')

    prompt = f"""Ты — оформитель заявлений на аттестацию специалистов (Беларусь, форма по Приложению 3
к Постановлению Минстройархитектуры РБ от 14.06.2024 № 70). Составь ЗАЯВЛЕНИЕ строго по этой структуре
и формулировкам, один в один как в реальных поданных образцах:

Шапка (заявитель — юрлицо):
Исх. № ___ от ___
{full_name}
(полное наименование юридического лица)
{company.get('form','ООО')} «{company.get('name','')}» (сокращённое наименование)
место государственной регистрации: {company.get('address','')}
учётный номер плательщика: {company.get('unp','')}
банковские реквизиты: {company.get('bank_details','')}
телефон: {company.get('phone','')}, электронный адрес: {company.get('email','')}
почтовый адрес: {company.get('address','')}
в лице {dir_pos} {dir_fio}, действующего на основании Устава

Заголовок:
ЗАЯВЛЕНИЕ
о проведении аттестации

Тело:
Прошу провести аттестацию руководителя, специалиста
{person.get('fio','')} (в родительном падеже — "Иванова Ивана Ивановича")

специальность по диплому: {person.get('diploma_speciality','')}
квалификация по диплому: {person.get('diploma_qualification','')}

документ, удостоверяющий личность: паспорт
серия: {person.get('passport_series','')}, № {person.get('passport_number','')}
идентификационный номер: {person.get('id_number','')}
код органа/орган, выдавший документ: {person.get('passport_issuer','')}

место жительства (место пребывания): {person.get('address','')}
контактный телефон: {company.get('phone','')}, e-mail: {company.get('email','')}

наименование вида деятельности в области архитектурной, градостроительной, строительной деятельности:
Строительно-монтажные работы

специализация аттестации: {grade} ({specialization})

В соответствии с частью второй пункта 1 статьи 40 Кодекса Республики Беларусь об архитектурной,
градостроительной и строительной деятельности прошу оформить {person.get('fio_dative', person.get('fio',''))}
(в дательном падеже) квалификационный аттестат на бумажном носителе.
Сведения, изложенные в заявлении и прилагаемых к нему документах, достоверны.

Приложение:
Копия диплома о {"высшем" if "высш" in person.get('education_level','высшее').lower() else "среднем специальном"} образовании {person.get('diploma_number','')}
Копия трудовой книжки {person.get('trudovaya_number','')}
Фотография 3х4

Подпись:
{dir_pos.replace('Директора','Директор')}    _____________    {dir_fio}
                    (подпись)              (инициалы, фамилия)

ПРАВИЛО: заполняй все поля из данных выше буквально, ничего не выдумывай и не меняй формулировки
статьи закона. Формат — обычный текст документа, без markdown разметки, максимально близко к
официальному бланку. Отвечай только текстом документа."""

    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=2000)


def gen_spravka_rasshifrovka(company: dict, person: dict, specialization: str, work_experience_text: str,
                              api_key, vibe_call_fn) -> str:
    """
    Справка-расшифровка — нужна когда должность в трудовой не совпадает напрямую со специализацией.
    Текст полностью разный под каждое направление — перечисляет конкретные объекты и виды работ.
    """
    full_name = f"{company.get('form','ООО')} «{company.get('name','')}»"
    dir_fio = company.get('director_fio', '')
    prompt = f"""Составь СПРАВКУ-РАСШИФРОВКУ опыта работы специалиста для аттестации по специализации
«{specialization}».

{full_name}

СПРАВКА-РАСШИФРОВКА

Дана {person.get('fio','')} в том, что он(а) действительно выполнял(а) следующие работы,
соответствующие специализации «{specialization}»:

{work_experience_text}

Справка выдана для предоставления в РУП «Белстройцентр» для прохождения аттестации.

{company.get('director_position','Директор')}    _____________    {dir_fio}

ПРАВИЛО: перечисли конкретные объекты и виды работ из текста выше, свяжи их логически со
специализацией «{specialization}». Отвечай только текстом документа."""
    return vibe_call(prompt, api_key, vibe_call_fn, max_tokens=1500)


def create_docx_from_text(text: str) -> bytes:
    """Тот же лёгкий генератор docx что и в основном generator.py — без внешних зависимостей."""
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
        is_heading = (line.isupper() or line.startswith('ЗАЯВЛЕНИЕ') or line.startswith('СПРАВКА')) and len(line) < 120
        align = 'center' if is_heading else 'both'
        bold = 'true' if is_heading else 'false'
        return (f'<w:p><w:pPr><w:jc w:val="{align}"/><w:spacing w:line="360" w:lineRule="auto"/></w:pPr>'
                f'<w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/>'
                f'<w:b w:val="{bold}"/><w:sz w:val="24"/></w:rPr>'
                f'<w:t xml:space="preserve">{line if line.strip() else " "}</w:t></w:r></w:p>')

    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    paras = '\n'.join(make_para(l) for l in lines)
    doc_xml = (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
               f'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               f'<w:body>{paras}<w:sectPr><w:pgSz w:w="11906" w:h="16838"/>'
               f'<w:pgMar w:top="1134" w:right="850" w:bottom="1134" w:left="1701"/></w:sectPr>'
               f'</w:body></w:document>')

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types)
        zf.writestr('_rels/.rels', rels)
        zf.writestr('word/document.xml', doc_xml)
        zf.writestr('word/_rels/document.xml.rels', word_rels)
    return buf.getvalue()


def generate_attestation_package(company: dict, person: dict, requests: list, api_key, vibe_call_fn,
                                  progress_cb=None) -> list:
    """
    requests: [{'specialization': str, 'grade': str или None (тогда считаем сами),
                'stage_months': int, 'need_spravka': bool, 'work_experience_text': str}]
    Возвращает список {'name': .., 'bytes': ..} — по каждому направлению отдельный комплект.
    """
    docs = []
    step = [0]

    def p(msg):
        step[0] += 1
        if progress_cb:
            progress_cb(step[0], len(requests) * 2, msg)

    fio_safe = re.sub(r'[^\w\s-]', '', person.get('fio', 'Специалист'))[:40]

    for req in requests:
        spec = req['specialization']
        grade = req.get('grade')
        if not grade:
            grade = calculate_grade(person.get('education_level', 'высшее'), req.get('stage_months', 0))

        p(f"Заявление — {grade} ({spec})")
        text = gen_zayavlenie(company, person, spec, grade, api_key, vibe_call_fn)
        spec_safe = re.sub(r'[^\w\s-]', '', spec)[:40]
        docs.append({
            'name': f"Заявление - {fio_safe} - {grade} ({spec_safe}).docx",
            'bytes': create_docx_from_text(text)
        })

        if req.get('need_spravka'):
            p(f"Справка-расшифровка — {spec}")
            text2 = gen_spravka_rasshifrovka(company, person, spec,
                                              req.get('work_experience_text', ''), api_key, vibe_call_fn)
            docs.append({
                'name': f"Справка-расшифровка - {fio_safe} - ({spec_safe}).docx",
                'bytes': create_docx_from_text(text2)
            })

    return docs
