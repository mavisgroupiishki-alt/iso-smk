"""
Умный генератор документов ИСО/СУОТ/СПК.
Игорь анализирует данные компании и генерирует каждый документ через ИИ,
опираясь на логику и правила — не на механическую замену текста.
"""
import os, json, re, zipfile, io, shutil, tempfile
from pathlib import Path
from datetime import datetime, timedelta
import requests as req_lib

BASE_DIR = Path(__file__).parent.resolve()
VIBE_URL = "https://vibecode.bitrix24.tech/v1/ai/chat/completions"
VIBE_MODEL = "bitrix/bitrixgpt-5.5"
VIBE_MODEL_VISION = "bitrix/bitrixgpt-5.5-thinking"

# Загружаем библиотеки
LIBS_PATH = BASE_DIR / 'libs.json'
LIBS = json.loads(LIBS_PATH.read_text('utf-8')) if LIBS_PATH.exists() else {'di': {}, 'ri': {}}

# ── База знаний (логика оформления) ──────────────────────────
ISO_RULES = """
ПРАВИЛА ОФОРМЛЕНИЯ ИСО 9001 (СМК):

ДАТЫ (от даты выезда эксперта):
- Политика = выезд минус 34 дня (примерно месяц)  
- Цели, приказы 2-9 = политика + 5 дней
- Реестр рисков = дата политики
- Отчёты = выезд минус 7 дней
- Дата выезда эксперта = аудит

ОТВЕТСТВЕННЫЕ (выбираем из ИТР):
- За СМК (приказ 1) = директор
- За процесс (приказ 6) = главный инженер или прораб или директор  
- Аудиторы (приказ 3) = 2-3 человека: директор + ИТР с удостоверением ОТ (главный инженер, главный бухгалтер, прораб)
- Координационный совет = те же что аудиторы
- За риски (приказ 5) = те же что аудиторы
- За ДИ (приказ 4) = директор или специалист по кадрам или бухгалтер

ЛИСТЫ ОЗНАКОМЛЕНИЯ:
- Вносятся все ИТР (без рабочих)
- Дата = дата документа, если сотрудник принят позже — дата приёма

ВНУТРЕННИЕ АУДИТЫ:
- Критерии для производственного ИТР: 4.1-4.4, 5.1-5.3, 6.1-6.3, 7.1-7.5, 8.1, 8.2, 8.4-8.7, 9.1-9.3, 10.1-10.3
- Для бухгалтера/кадров: 4.1, 4.2, 5.3, 6.1-6.3, 7.2-7.5, 10.3

ОБЪЕКТЫ: если были — пишем количество и 100%, если нет — "не предоставляется возможным"
"""

SUOT_RULES = """
ПРАВИЛА ОФОРМЛЕНИЯ СУОТ (ISO 45001):

ДАТЫ: те же что ИСО, от даты выезда эксперта

ОТВЕТСТВЕННЫЕ:
- Все должны иметь удостоверения ОТ (минимум 3 человека)
- Аудиторы = те у кого есть ОТ
- Ответственные за риски = те же что аудиторы

ПЕРЕЧНИ:
- Перечень 2: все должности ИТР (без дублей)
- Перечень 3: рабочие из штатного расписания
- Перечень 4: ИТР в производстве (директор, ГИ, прораб, мастер)
- Перечень 6: все ИТР и рабочие
- Перечень 12: СИЗ по нормам для рабочих

ИНСТРУКЦИИ ПО ОТ:
- Для каждой уникальной профессии рабочих
- Содержание адаптируется под вид работ компании

КАРТЫ РИСКОВ:
- Для офисных ИТР: риски ПК, освещения, нервного напряжения
- Для производственных ИТР: падение, травмы, электричество
- Для рабочих: специфические риски их профессии + вид работ компании

ПРОВЕРКА ЗНАНИЙ:
- Все ИТР проходят если нет удостоверения ОТ: ПЕРИОДИЧЕСКАЯ
- Рабочие: все

АУДИТЫ:
- Минимум 1 аудит проводится директору
"""

SPK_RULES = """
ПРАВИЛА ОФОРМЛЕНИЯ СПК:

ДОКУМЕНТЫ:
- Положение о системе производственного контроля
- Приказ о СПК
- Приказы о назначении ответственных
- Перечень специалистов с дипломами и аттестатами
- Перечень СИ (средств измерений) с датами поверки
- Перечень ТТК (техкарт)
- Протоколы обучения
- Журналы (входной, операционный, приёмо-сдаточный контроль)

БИСП ДОПОЛНИТЕЛЬНО:
- Справка о рекламациях
- Договор с лабораторией
- Справка о предприятии (с ФИО главбуха, реквизитами)
- Организационная структура
- Гарантийное письмо по ВВК
- Гарантийное письмо по лабораториям
- Перечень продукции входного контроля
"""


def vibe_call(messages, api_key, model=None):
    """Вызов Vibe Code AI"""
    resp = req_lib.post(
        VIBE_URL,
        headers={"Content-Type": "application/json", "X-Api-Key": api_key},
        json={
            "model": model or VIBE_MODEL,
            "max_tokens": 4000,
            "messages": messages
        },
        timeout=120
    )
    resp.raise_for_status()
    data = resp.json()
    return "".join(c.get("message", {}).get("content", "") for c in data.get("choices", []))


def find_di_in_library(position: str) -> dict | None:
    """Ищет ДИ в библиотеке по названию должности (нечёткий поиск)"""
    pos_upper = position.upper().strip()
    
    # Точное совпадение
    if pos_upper in LIBS['di']:
        return LIBS['di'][pos_upper]
    
    # Частичное совпадение
    for key, val in LIBS['di'].items():
        if pos_upper in key or key in pos_upper:
            return val
        # По ключевым словам
        words = [w for w in pos_upper.split() if len(w) > 4]
        if any(w in key for w in words):
            return val
    
    return None


def find_ri_in_library(profession: str) -> dict | None:
    """Ищет инструкцию ОТ по профессии"""
    prof_upper = profession.upper().strip()
    
    if prof_upper in LIBS['ri']:
        return LIBS['ri'][prof_upper]
    
    for key, val in LIBS['ri'].items():
        words = [w for w in prof_upper.split() if len(w) > 4]
        if any(w in key for w in words):
            return val
    
    return None


def calculate_dates(audit_date_str: str) -> dict:
    """Рассчитывает все даты от даты выезда эксперта"""
    try:
        parts = audit_date_str.replace('.', '/').split('/')
        if len(parts) == 3:
            d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
            audit = datetime(y, m, d)
        else:
            audit = datetime.now() + timedelta(days=30)
    except:
        audit = datetime.now() + timedelta(days=30)
    
    policy = audit - timedelta(days=34)
    goals = policy + timedelta(days=5)
    reports = audit - timedelta(days=7)
    risks = policy + timedelta(days=2)
    
    fmt = lambda d: d.strftime('%d.%m.%Y')
    
    return {
        'audit': fmt(audit),           # дата выезда эксперта
        'policy': fmt(policy),          # политика (месяц до выезда)
        'goals': fmt(goals),            # цели, приказы
        'reports': fmt(reports),         # отчёты (за неделю до выезда)
        'risks': fmt(risks),             # реестр рисков
        'year': str(audit.year),
        'year_prev': str(audit.year - 1),
        'audit_obj': f"{fmt(policy)} по {fmt(reports)}"  # отчётный период
    }


def select_responsible(staff: list, dates: dict) -> dict:
    """
    Выбирает ответственных из списка ИТР согласно правилам:
    - Аудиторы: 3 чел. с удостоверением ОТ (или опытные ИТР)
    - За процесс: главный инженер / прораб / директор
    - За ДИ: директор / кадровик / бухгалтер
    """
    itr = [s for s in staff if not s.get('is_worker', False)]
    
    # Директор
    director = next((s for s in itr if 'директор' in s.get('position', '').lower()), itr[0] if itr else None)
    
    # С удостоверением ОТ
    with_ot = [s for s in itr if s.get('ot_certificate')]
    
    # Главный инженер / прораб
    process_resp = next((s for s in itr if any(k in s.get('position', '').lower() 
                        for k in ['главный инженер', 'прораб', 'производитель работ'])), director)
    
    # Аудиторы — берём ОТ-шников, если мало — добавляем опытных ИТР
    auditors = with_ot[:3] if len(with_ot) >= 3 else (with_ot + [s for s in itr if s not in with_ot])[:3]
    if not auditors:
        auditors = itr[:3]
    
    # За ДИ
    di_resp = next((s for s in itr if any(k in s.get('position', '').lower()
                   for k in ['кадр', 'персонал', 'бухгалтер'])), director)
    
    # За ФНПА
    fnpa_resp = process_resp or director
    
    return {
        'director': director,
        'process_resp': process_resp,
        'auditors': auditors,
        'di_resp': di_resp,
        'fnpa_resp': fnpa_resp,
        'risk_group': auditors,  # те же что аудиторы
        'coord_council': auditors,  # те же что аудиторы
    }


def generate_document_ai(doc_type: str, company: dict, staff: dict, 
                          dates: dict, resp: dict, api_key: str,
                          extra: dict = None) -> str:
    """
    Генерирует текст документа через ИИ.
    doc_type: тип документа (policy, order_1, di_director, etc.)
    Возвращает готовый текст документа в формате для DOCX.
    """
    extra = extra or {}
    
    # Формируем контекст компании
    company_ctx = f"""
КОМПАНИЯ:
- Название: {company.get('name', '')} 
- Форма: {company.get('form', 'ООО')}
- Полное: {company.get('form', 'ООО')} «{company.get('name', '')}»
- УНП: {company.get('unp', '')}
- Адрес: {company.get('address', '')}
- Город: {company.get('city', 'Минск')}
- Директор ФИО: {company.get('director_fio', '')}
- Должность директора: {company.get('director_position', 'Директор')}
- Область деятельности: {company.get('scope', '')}

ДАТЫ:
- Политика: {dates['policy']}
- Цели/Приказы: {dates['goals']}
- Реестр рисков: {dates['risks']}
- Отчёты: {dates['reports']}
- Аудит (выезд эксперта): {dates['audit']}
- Отчётный период: {dates['audit_obj']}
- Год: {dates['year']}

ОТВЕТСТВЕННЫЕ:
- Директор: {_fio(resp.get('director'))}
- За процесс: {_fio(resp.get('process_resp'))} ({_pos(resp.get('process_resp'))})
- Аудиторы: {', '.join(_fio(a) + ' (' + _pos(a) + ')' for a in resp.get('auditors', []))}
- За ДИ: {_fio(resp.get('di_resp'))} ({_pos(resp.get('di_resp'))})
- За ФНПА: {_fio(resp.get('fnpa_resp'))} ({_pos(resp.get('fnpa_resp'))})
- Группа рисков: {', '.join(_fio(a) for a in resp.get('risk_group', []))}
"""
    
    prompts = {
        'policy_iso': f"""
Ты — оформитель документов ИСО 9001 (Беларусь). 
Создай ПОЛИТИКУ В ОБЛАСТИ КАЧЕСТВА для компании.

{company_ctx}

ПРАВИЛА ИСО:
{ISO_RULES}

Документ должен содержать:
1. Шапку: {company.get('form', 'ООО')} «{company.get('name', '')}»
2. УТВЕРЖДАЮ: {company.get('director_position', 'Директор')} {company.get('form', 'ООО')} «{company.get('name', '')}» _____________ {_initials(company.get('director_fio', ''))} {dates['policy']} г.
3. Название: ПОЛИТИКА В ОБЛАСТИ КАЧЕСТВА
4. Текст политики с упоминанием области деятельности: {company.get('scope', '')}
5. Подпись директора

Пиши как профессиональный юрист-оформитель. Текст должен соответствовать СТБ ISO 9001-2015.
Отвечай только текстом документа, без пояснений.
""",

        'policy_suot': f"""
Ты — оформитель документов СУОТ ISO 45001 (Беларусь).
Создай ПОЛИТИКУ В ОБЛАСТИ ОХРАНЫ ТРУДА.

{company_ctx}

ПРАВИЛА СУОТ:
{SUOT_RULES}

Документ должен содержать:
1. Название: ПОЛИТИКА В ОБЛАСТИ ОХРАНЫ ТРУДА
2. УТВЕРЖДАЮ: {company.get('director_position', 'Директор')} {company.get('form', 'ООО')} «{company.get('name', '')}» _____________ {_initials(company.get('director_fio', ''))} {dates['goals']} г.
3. Текст политики с областью: {company.get('scope', '')}
4. Цели политики по охране труда
5. Обязательства руководства
6. Подпись

Отвечай только текстом документа.
""",

        'order_1_iso': f"""
Создай ПРИКАЗ № 1 О РАЗРАБОТКЕ СИСТЕМЫ МЕНЕДЖМЕНТА КАЧЕСТВА (ИСО 9001).

{company_ctx}

ПРИКАЗЫВАЮ:
1. Разработать СМК в соответствии с СТБ ISO 9001-2015
2. Ответственный за разработку: директор {_fio(resp.get('director'))}
3. Область: {company.get('scope', '')}
4. Срок: до {dates['goals']}

Формат: официальный приказ организации РБ.
Отвечай только текстом документа.
""",
    }
    
    prompt = prompts.get(doc_type)
    if not prompt:
        # Универсальный промпт для остальных документов
        prompt = f"""
Ты — оформитель документов {extra.get('product', 'ИСО')} (Беларусь).
Создай документ: {doc_type}

{company_ctx}

Дополнительные данные: {json.dumps(extra, ensure_ascii=False)}

Правила:
{ISO_RULES if 'suot' not in doc_type else SUOT_RULES}

Создай профессиональный документ соответствующий требованиям стандарта.
Отвечай только текстом документа, без пояснений.
"""
    
    return vibe_call([{"role": "user", "content": prompt}], api_key)


def _fio(person: dict | None) -> str:
    if not person: return ''
    return person.get('fio', '')

def _pos(person: dict | None) -> str:
    if not person: return ''
    return person.get('position', '')

def _initials(fio: str) -> str:
    """Иванов Иван Иванович → И.И. Иванов"""
    parts = fio.strip().split()
    if len(parts) >= 2:
        surname = parts[0]
        inits = '.'.join(p[0] for p in parts[1:] if p) + '.'
        return f"{inits} {surname}"
    return fio


def create_docx_from_text(text: str, filename: str) -> bytes:
    """
    Создаёт DOCX файл из текста.
    Использует минимальный шаблон Word с правильными стилями.
    """
    # Минимальный DOCX шаблон
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

    def text_to_xml(t: str) -> str:
        """Конвертирует текст в Word XML параграфы"""
        lines = t.replace('\r\n', '\n').replace('\r', '\n').split('\n')
        paragraphs = []
        for line in lines:
            line = line.strip()
            # Экранируем XML спецсимволы
            line = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            # Определяем стиль
            is_heading = line.isupper() and len(line) > 5 and len(line) < 100
            is_center = line.startswith('УТВЕРЖДАЮ') or line.startswith('ПРИКАЗ') or is_heading
            
            align = 'center' if is_center else 'both'
            bold = 'true' if is_heading else 'false'
            sz = '28' if is_heading else '24'  # 14pt или 12pt
            
            para = f'''<w:p>
  <w:pPr>
    <w:jc w:val="{align}"/>
    <w:spacing w:line="360" w:lineRule="auto"/>
  </w:pPr>
  <w:r>
    <w:rPr>
      <w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/>
      <w:b w:val="{bold}"/>
      <w:sz w:val="{sz}"/>
      <w:szCs w:val="{sz}"/>
    </w:rPr>
    <w:t xml:space="preserve">{line if line else ' '}</w:t>
  </w:r>
</w:p>'''
            paragraphs.append(para)
        
        return '\n'.join(paragraphs)

    document_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:wpc="http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas"
            xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml">
<w:body>
{text_to_xml(text)}
<w:sectPr>
  <w:pgSz w:w="11906" w:h="16838"/>
  <w:pgMar w:top="1134" w:right="850" w:bottom="1134" w:left="1701" w:header="709" w:footer="709" w:gutter="0"/>
</w:sectPr>
</w:body>
</w:document>'''

    word_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
</Relationships>'''

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('[Content_Types].xml', content_types)
        zf.writestr('_rels/.rels', rels)
        zf.writestr('word/document.xml', document_xml)
        zf.writestr('word/_rels/document.xml.rels', word_rels)
    
    return buf.getvalue()


def adapt_di_from_library(di_text: str, company: dict, person: dict, 
                           dates: dict, api_key: str) -> str:
    """
    Адаптирует ДИ из библиотеки под конкретную компанию через ИИ.
    Не просто меняет шапку — ИИ адаптирует содержание под область деятельности.
    """
    prompt = f"""
Ты — оформитель должностных инструкций (Беларусь, строительная отрасль).

У тебя есть ШАБЛОН ДОЛЖНОСТНОЙ ИНСТРУКЦИИ из библиотеки:
---
{di_text[:2000]}
---

Адаптируй эту ДИ под следующую компанию:
- Компания: {company.get('form', 'ООО')} «{company.get('name', '')}»
- Область деятельности: {company.get('scope', '')}
- Сотрудник: {person.get('fio', '')}
- Должность: {person.get('position', '')}
- Директор: {company.get('director_fio', '')}
- Дата: {dates['goals']}

Что нужно изменить:
1. Шапку (название компании, директор, дата)
2. Конкретные обязанности под область деятельности компании
3. Требования к квалификации (если нужно)
4. Убери специфику другой компании, добавь специфику этой

Сохрани структуру и профессиональный стиль документа.
Отвечай только текстом адаптированной ДИ.
"""
    return vibe_call([{"role": "user", "content": prompt}], api_key)


def generate_di_new(position: str, company: dict, person: dict,
                    dates: dict, api_key: str) -> str:
    """Генерирует новую ДИ если не найдена в библиотеке"""
    prompt = f"""
Ты — юрист-оформитель должностных инструкций (Беларусь).

Разработай должностную инструкцию для должности: {position}

Компания: {company.get('form', 'ООО')} «{company.get('name', '')}»
Область деятельности: {company.get('scope', '')}
Сотрудник: {person.get('fio', '')}
Директор: {company.get('director_fio', '')} 
Дата: {dates['goals']}

Структура ДИ:
1. Шапка: УТВЕРЖДАЮ Директор {company.get('form', 'ООО')} «{company.get('name', '')}» ___ {_initials(company.get('director_fio', ''))} {dates['goals']} г.
2. ДОЛЖНОСТНАЯ ИНСТРУКЦИЯ {position.upper()} № ДИ-ХХ
3. ОБЩИЕ ПОЛОЖЕНИЯ (категория, требования к образованию, стажу, кому подчиняется)
4. ДОЛЖНОСТНЫЕ ОБЯЗАННОСТИ (конкретные для этой должности и области деятельности)
5. ПРАВА
6. ОТВЕТСТВЕННОСТЬ

Соответствует законодательству РБ и специфике строительной отрасли.
Отвечай только текстом ДИ.
"""
    return vibe_call([{"role": "user", "content": prompt}], api_key)


def generate_ot_instruction(profession: str, company: dict, 
                              dates: dict, api_key: str) -> str:
    """Генерирует инструкцию по ОТ для профессии"""
    # Ищем в библиотеке
    ri = find_ri_in_library(profession)
    
    if ri:
        # Адаптируем существующую
        prompt = f"""
Ты — оформитель инструкций по охране труда (Беларусь).

ШАБЛОН инструкции для {profession}:
---
{ri['text'][:2000]}
---

Адаптируй под компанию:
- Компания: {company.get('form', 'ООО')} «{company.get('name', '')}»
- Область: {company.get('scope', '')}
- Директор: {company.get('director_fio', '')}
- Дата: {dates['goals']}

Обнови шапку, адаптируй содержание под вид работ компании.
Отвечай только текстом инструкции.
"""
    else:
        # Генерируем новую
        prompt = f"""
Ты — специалист по охране труда (Беларусь, строительная отрасль).

Разработай ИНСТРУКЦИЮ ПО ОХРАНЕ ТРУДА ДЛЯ {profession.upper()}

Компания: {company.get('form', 'ООО')} «{company.get('name', '')}»
Область: {company.get('scope', '')}
Директор: {company.get('director_fio', '')}
Дата: {dates['goals']}

Структура:
1. Шапка УТВЕРЖДАЮ
2. ИНСТРУКЦИЯ ПО ОХРАНЕ ТРУДА ДЛЯ {profession.upper()} № ОТ-ХХ
3. Глава 1: Общие требования по охране труда
4. Глава 2: Требования по охране труда перед началом работы
5. Глава 3: Требования по охране труда при выполнении работы
6. Глава 4: Требования по охране труда по окончании работы
7. Глава 5: Требования по охране труда в аварийных ситуациях

Адаптируй под специфику профессии и вид работ компании.
Отвечай только текстом инструкции.
"""
    
    return vibe_call([{"role": "user", "content": prompt}], api_key)


def generate_risk_card(profession: str, company: dict,
                        dates: dict, api_key: str) -> str:
    """Генерирует карту рисков для профессии/рабочего места"""
    is_office = any(k in profession.lower() for k in ['бухгалтер', 'директор', 'инженер', 'специалист', 'кадр'])
    
    prompt = f"""
Ты — специалист по охране труда и оценке рисков (Беларусь, ISO 45001).

Создай КАРТУ РИСКОВ для должности/профессии: {profession}

Компания: {company.get('form', 'ООО')} «{company.get('name', '')}»
Область деятельности: {company.get('scope', '')}
Дата: {dates['goals']}
Директор: {company.get('director_fio', '')}

{'Это офисный сотрудник — риски ПК, освещения, нервного напряжения.' if is_office else 'Это производственный сотрудник/рабочий — учти специфические риски профессии и строительных работ.'}

Структура карты рисков:
1. Шапка: УТВЕРЖДАЮ Директор... 
2. КАРТА РИСКОВ для {profession}
3. Таблица:
   | № | Опасность/Вредный фактор | Возможные последствия | Вероятность (1-3) | Тяжесть (1-3) | Уровень риска (произведение) | Меры управления |
4. Не менее 5-7 рисков
5. Подписи ответственных

Уровень риска = вероятность × тяжесть. Неприемлемый риск > 9.
Адаптируй риски под специфику области деятельности компании.
Отвечай только текстом документа.
"""
    return vibe_call([{"role": "user", "content": prompt}], api_key)


# Основная функция генерации пакета
def generate_package(company_data: dict, api_key: str, 
                      product: str, progress_cb=None) -> dict:
    """
    Генерирует полный пакет документов.
    product: 'iso' | 'suot' | 'iso_suot' | 'spk_stroy' | 'spk_bisp'
    progress_cb: callback(step, total, message)
    """
    company = company_data.get('company', {})
    staff_all = company_data.get('staff', [])
    dates_raw = company_data.get('dates', {})
    objects_list = company_data.get('objects', [])
    suppliers_list = company_data.get('suppliers', [])
    
    # Рассчитываем даты
    audit_date = dates_raw.get('audit_date', '')
    dates = calculate_dates(audit_date)
    
    # Разделяем штат
    itr = [s for s in staff_all if not s.get('is_worker', False)]
    workers = [s for s in staff_all if s.get('is_worker', False)]
    
    # Выбираем ответственных
    resp = select_responsible(itr, dates)
    
    # Уникальные профессии рабочих
    worker_professions = list({s.get('position', '') for s in workers if s.get('position')})
    
    docs = []  # [{name, bytes}]
    
    def add_doc(name: str, text: str):
        try:
            doc_bytes = create_docx_from_text(text, name)
            docs.append({'name': name, 'bytes': doc_bytes})
        except Exception as e:
            print(f"Ошибка создания {name}: {e}")
    
    def progress(step, total, msg):
        if progress_cb:
            progress_cb(step, total, msg)
        print(f"[{step}/{total}] {msg}")
    
    if product in ('iso', 'iso_suot'):
        _generate_iso_docs(company, itr, dates, resp, objects_list, 
                           suppliers_list, api_key, add_doc, progress)
    
    if product in ('suot', 'iso_suot'):
        _generate_suot_docs(company, itr, workers, worker_professions,
                            dates, resp, api_key, add_doc, progress)
    
    if product in ('spk_stroy', 'spk_bisp'):
        _generate_spk_docs(company, itr, dates, resp, api_key, 
                           add_doc, progress, variant=product)
    
    return {
        'docs': docs,
        'dates': dates,
        'responsible': resp,
        'itr_count': len(itr),
        'workers_count': len(workers),
        'professions': worker_professions
    }


def _generate_iso_docs(company, itr, dates, resp, objects, suppliers, 
                        api_key, add_doc, progress):
    """Генерирует пакет ИСО 9001"""
    total_steps = 15 + len(itr)  # примерно
    step = 0
    
    def p(msg):
        nonlocal step; step += 1
        progress(step, total_steps, msg)
    
    # 1. Политика в области качества
    p("Политика в области качества...")
    text = generate_document_ai('policy_iso', company, {'itr': itr}, dates, resp, api_key)
    add_doc(f"{company.get('name', 'org')} - 1 Политика в области качества.docx", text)
    
    # 2. Лист ознакомления с политикой
    p("Лист ознакомления с политикой...")
    itr_list = '\n'.join(f"{i+1}. {s.get('fio', '')} — {s.get('position', '')} — {dates['policy'] if not s.get('hire_date') or s.get('hire_date') <= dates['policy'] else s.get('hire_date', '')}" for i, s in enumerate(itr))
    text = generate_document_ai('awareness_policy', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'itr_list': itr_list, 'doc': 'лист ознакомления с политикой качества'})
    add_doc(f"{company.get('name', 'org')} - 2.2 Лист ознакомления с политикой.docx", text)
    
    # 3. Цели в области качества  
    p("Цели в области качества...")
    text = generate_document_ai('goals_iso', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'doc': 'цели в области качества', 'date': dates['goals']})
    add_doc(f"{company.get('name', 'org')} - 2.1 Цели в области качества.docx", text)

    # 4-10. Приказы 1-7
    orders = [
        ('order_1', 'Приказ 1 о разработке СМК', dates['policy']),
        ('order_2', 'Приказ 2 о введении в действие документов СМК', dates['goals']),
        ('order_3', 'Приказ 3 о назначении внутренних аудиторов', dates['goals']),
        ('order_4', 'Приказ 4 о назначении ответственного за ДИ', dates['goals']),
        ('order_5', 'Приказ 5 о назначении ответственных за риски', dates['policy']),
        ('order_6', 'Приказ 6 о назначении руководителя процесса', dates['goals']),
        ('order_7', 'Приказ 7 о назначении ответственных за ФНПА', dates['goals']),
    ]
    for order_key, order_name, order_date in orders:
        p(f"{order_name}...")
        text = generate_document_ai(order_key, company, {'itr': itr}, dates, resp, api_key,
                                     extra={'order_name': order_name, 'date': order_date})
        add_doc(f"{company.get('name', 'org')} - {order_name}.docx", text)
    
    # Должностные инструкции для ИТР
    for person in itr:
        pos = person.get('position', '')
        fio = person.get('fio', '')
        p(f"ДИ: {pos} ({fio})...")
        
        di = find_di_in_library(pos)
        if di:
            text = adapt_di_from_library(di['text'], company, person, dates, api_key)
        else:
            text = generate_di_new(pos, company, person, dates, api_key)
        
        safe_pos = re.sub(r'[^\w\s-]', '', pos)[:40]
        add_doc(f"{company.get('name', 'org')} - ДИ {safe_pos}.docx", text)
    
    # Реестр рисков
    p("Реестр рисков СМК...")
    text = generate_document_ai('risk_register_iso', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'doc': 'реестр рисков и возможностей СМК', 'date': dates['risks']})
    add_doc(f"{company.get('name', 'org')} - Реестр рисков.docx", text)
    
    # Отчёт по процессу
    p("Отчёт по процессу строительства...")
    has_objects = len(objects) > 0
    text = generate_document_ai('process_report', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'objects': objects, 'has_objects': has_objects,
                                        'doc': 'отчёт по процессу строительства'})
    add_doc(f"{company.get('name', 'org')} - Отчёт по процессу.docx", text)
    
    # Сводный отчёт по СМК
    p("Сводный отчёт по СМК...")
    text = generate_document_ai('summary_report', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'objects': objects, 'has_objects': has_objects,
                                        'doc': 'сводный отчёт по СМК'})
    add_doc(f"{company.get('name', 'org')} - Сводный отчёт СМК.docx", text)
    
    # Карточки поставщиков
    for i, sup in enumerate(suppliers[:5]):
        p(f"Карточка поставщика {i+1}...")
        text = generate_document_ai('supplier_card', company, {'itr': itr}, dates, resp, api_key,
                                     extra={'supplier': sup, 'doc': 'карточка оценки поставщика'})
        safe_name = re.sub(r'[^\w\s-]', '', sup.get('name', f'поставщик_{i+1}'))[:30]
        add_doc(f"{company.get('name', 'org')} - Карточка поставщика {safe_name}.docx", text)


def _generate_suot_docs(company, itr, workers, professions,
                         dates, resp, api_key, add_doc, progress):
    """Генерирует пакет СУОТ ISO 45001"""
    step = 0
    total = 12 + len(professions) + len(professions)  # приказы + инструкции + карты
    
    def p(msg):
        nonlocal step; step += 1
        progress(step, total, msg)
    
    # Политика СУОТ
    p("Политика в области охраны труда...")
    text = generate_document_ai('policy_suot', company, {'itr': itr}, dates, resp, api_key)
    add_doc(f"{company.get('name', 'org')} СУОТ - Политика ОТ.docx", text)
    
    # Приказы СУОТ (1-10)
    suot_orders = [
        ('suot_order_1', 'Приказ 1 о разработке OH&S', dates['policy']),
        ('suot_order_2', 'Приказ 2 о внедрении OH&S', dates['goals']),
        ('suot_order_3', 'Приказ 3 о введении в действие документов OH&S', dates['goals']),
        ('suot_order_4', 'Приказ 4 о назначении ответственных лиц', dates['goals']),
        ('suot_order_5', 'Приказ 5 о назначении внутренних аудиторов OH&S', dates['goals']),
        ('suot_order_6', 'Приказ 6 о назначении ответственных за риски OH&S', dates['goals']),
        ('suot_order_7', 'Приказ 7 о разработке инструкций по ОТ', dates['goals']),
        ('suot_order_9', 'Приказ 9 о дне охраны труда', dates['goals']),
    ]
    for key, name, date in suot_orders:
        p(f"{name}...")
        text = generate_document_ai(key, company, {'itr': itr}, dates, resp, api_key,
                                     extra={'order_name': name, 'date': date, 'product': 'СУОТ'})
        add_doc(f"{company.get('name', 'org')} СУОТ - {name}.docx", text)
    
    # Перечни
    p("Перечни СУОТ...")
    all_positions_itr = list({s.get('position', '') for s in itr if s.get('position')})
    all_positions_workers = list({s.get('position', '') for s in workers if s.get('position')})
    
    text = generate_document_ai('suot_lists', company, {'itr': itr}, dates, resp, api_key,
                                 extra={
                                     'itr_positions': all_positions_itr,
                                     'worker_positions': all_positions_workers,
                                     'doc': 'перечни 1-12 СУОТ'
                                 })
    add_doc(f"{company.get('name', 'org')} СУОТ - Перечни.docx", text)
    
    # Инструкции по ОТ для каждой профессии
    for i, prof in enumerate(professions):
        p(f"Инструкция ОТ: {prof}...")
        text = generate_ot_instruction(prof, company, dates, api_key)
        safe = re.sub(r'[^\w\s-]', '', prof)[:40]
        add_doc(f"{company.get('name', 'org')} СУОТ - Инструкция ОТ {safe}.docx", text)
    
    # Карты рисков
    # Для ИТР — две карты (офисные и производственные)
    p("Карта рисков ИТР (офис)...")
    text = generate_risk_card('Директор, специалисты (офис)', company, dates, api_key)
    add_doc(f"{company.get('name', 'org')} СУОТ - Карта рисков ИТР офис.docx", text)
    
    p("Карта рисков ИТР (производство)...")
    text = generate_risk_card('Производственный ИТР (прораб, главный инженер)', company, dates, api_key)
    add_doc(f"{company.get('name', 'org')} СУОТ - Карта рисков ИТР производство.docx", text)
    
    # Карты рисков для каждой профессии рабочих
    for prof in professions:
        p(f"Карта рисков: {prof}...")
        text = generate_risk_card(prof, company, dates, api_key)
        safe = re.sub(r'[^\w\s-]', '', prof)[:40]
        add_doc(f"{company.get('name', 'org')} СУОТ - Карта рисков {safe}.docx", text)
    
    # Реестр неприемлемых рисков
    p("Реестр неприемлемых рисков...")
    text = generate_document_ai('risk_register_suot', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'doc': 'реестр неприемлемых рисков СУОТ', 'professions': professions})
    add_doc(f"{company.get('name', 'org')} СУОТ - Реестр неприемлемых рисков.docx", text)


def _generate_spk_docs(company, itr, dates, resp, api_key, add_doc, progress, variant='spk_stroy'):
    """Генерирует пакет СПК"""
    step = 0
    total = 8 if variant == 'spk_stroy' else 13
    
    def p(msg):
        nonlocal step; step += 1
        progress(step, total, msg)
    
    # Положение о СПК
    p("Положение о системе производственного контроля...")
    text = generate_document_ai('spk_policy', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'doc': 'положение о системе производственного контроля', 'variant': variant})
    add_doc(f"{company.get('name', 'org')} СПК - Положение о СПК.docx", text)
    
    # Приказы СПК
    for key, name in [('spk_order_1', 'Приказ о СПК'), ('spk_order_2', 'Приказ об ответственных')]:
        p(f"{name}...")
        text = generate_document_ai(key, company, {'itr': itr}, dates, resp, api_key,
                                     extra={'doc': name, 'variant': variant})
        add_doc(f"{company.get('name', 'org')} СПК - {name}.docx", text)
    
    # Перечень специалистов
    p("Перечень специалистов (ИТР)...")
    itr_list = json.dumps([{'fio': s.get('fio'), 'position': s.get('position')} for s in itr], ensure_ascii=False)
    text = generate_document_ai('spk_specialists', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'doc': 'перечень специалистов с дипломами и аттестатами', 'itr_list': itr_list})
    add_doc(f"{company.get('name', 'org')} СПК - Перечень специалистов.docx", text)
    
    # Организационная структура
    p("Организационная структура...")
    text = generate_document_ai('org_structure', company, {'itr': itr}, dates, resp, api_key,
                                 extra={'doc': 'организационная структура', 'itr': [{'fio': s.get('fio'), 'position': s.get('position')} for s in itr]})
    add_doc(f"{company.get('name', 'org')} СПК - Организационная структура.docx", text)
    
    # Журналы (шапка)
    for journal_name in ['Журнал входного контроля', 'Журнал операционного контроля', 'Журнал приёмо-сдаточного контроля']:
        p(f"{journal_name}...")
        text = generate_document_ai('journal', company, {'itr': itr}, dates, resp, api_key,
                                     extra={'doc': journal_name})
        add_doc(f"{company.get('name', 'org')} СПК - {journal_name}.docx", text)
    
    # БИСП — дополнительные документы
    if variant == 'spk_bisp':
        bisp_docs = [
            ('spk_bisp_complaints', 'Справка об отсутствии рекламаций'),
            ('spk_bisp_lab', 'Гарантийное письмо по лаборатории'),
            ('spk_bisp_vvk', 'Гарантийное письмо ВВК'),
            ('spk_bisp_company', 'Справка о предприятии'),
            ('spk_bisp_products', 'Перечень продукции входного контроля'),
        ]
        for key, name in bisp_docs:
            p(f"{name} (БИСП)...")
            text = generate_document_ai(key, company, {'itr': itr}, dates, resp, api_key,
                                         extra={'doc': name, 'variant': 'bisp'})
            add_doc(f"{company.get('name', 'org')} СПК БИСП - {name}.docx", text)


print("✅ generator.py готов")
