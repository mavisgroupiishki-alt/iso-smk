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

# Стандартный справочник рабочих для Формы №2.
# Источник: пользовательский документ «ОБРАЗЕЦ Сведения о рабочих (15).docx».
# В нём для каждого кода работ 7.* задан перечень профессий. Когда в источнике
# не указан разряд/количество, приложение использует безопасный редактируемый
# стандарт: III разряд, 1 человек. Пользователь может изменить или удалить строку.
STANDARD_WORKER_RAZRYAD = 'III'
STANDARD_WORKER_COUNT = 1

COMPANY_ATT_WORKER_RULES = {'7.1': [],
 '7.2': ['Землекоп'],
 '7.2.1': ['Арматурщик', 'Бетонщик', 'Такелажник', 'Каменщик'],
 '7.2.2': ['Монтажник строительных конструкций',
           'Машинист крана автомобильного',
           'Стропальщик',
           'Бетонщик',
           'Плотник',
           'Арматурщик'],
 '7.2.3': ['Монтажник строительных конструкций', 'Бетонщик', 'Плотник', 'Арматурщик'],
 '7.3': [],
 '7.3.1': ['Арматурщик',
           'Такелажник',
           'Машинист башенного крана',
           'Подсобный рабочий',
           'Электросварщик ручной сварки',
           'Слесарь строительный'],
 '7.3.2': ['Стропальщик', 'Монтажник строительных конструкций', 'Машинист крана'],
 '7.3.3': ['Монтажник строительных конструкций', 'Машинист крана'],
 '7.3.4': ['Подсобный рабочий', 'Стропальщик', 'Такелажник', 'Машинист'],
 '7.4': [],
 '7.4.1': ['Монтажник строительных конструкций',
           'Каменщик',
           'Арматурщик',
           'Электросварщик ручной сварки',
           'Плотник',
           'Такелажник',
           'Подсобный рабочий',
           'Машинист башенного крана'],
 '7.4.2': ['Бетонщик',
           'Плотник',
           'Арматурщик',
           'Машинист башенного крана',
           'Электросварщик ручной сварки',
           'Такелажник',
           'Подсобный рабочий',
           'Слесарь строительный',
           'Каменщик'],
 '7.4.3': ['Бетонщик',
           'Арматурщик',
           'Машинист башенного крана',
           'Слесарь строительный',
           'Электросварщик ручной сварки',
           'Такелажник',
           'Подсобный рабочий',
           'Стропальщик',
           'Каменщик'],
 '7.4.4': ['Монтажник строительных конструкций', 'Машинист крана автомобильного', 'Электросварщик ручной сварки'],
 '7.4.5': ['Монтажник строительных конструкций', 'Плотник', 'Подсобный рабочий'],
 '7.4.6': ['Машинист крана', 'Такелажник', 'Стропальщик', 'Монтажник строительных конструкций'],
 '7.5': ['Маляр',
         'Подсобный рабочий',
         'Машинист аппарата безвоздушного распыления',
         'Машинист компрессора',
         'Изолировщик на антикоррозионной изоляции'],
 '7.6': ['Кровельщик по металлическим кровлям', 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов'],
 '7.6.1': ['Кровельщик по металлическим кровлям', 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов'],
 '7.6.2': ['Кровельщик по металлическим кровлям', 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов'],
 '7.6.3': ['Кровельщик по металлическим кровлям', 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов'],
 '7.6.4': ['Кровельщик по металлическим кровлям', 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов'],
 '7.6.5': ['Кровельщик по металлическим кровлям', 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов'],
 '7.7': ['Изолировщик на термоизоляции'],
 '7.8': [],
 '7.8.1': ['Монтажник санитарно-технических систем и оборудования', 'Подсобный рабочий'],
 '7.8.2': ['Монтажник санитарно-технических систем и оборудования', 'Подсобный рабочий'],
 '7.8.3': ['Монтажник санитарно-технических систем и оборудования', 'Слесарь', 'Подсобный рабочий'],
 '7.8.4': ['Монтажник систем вентиляции и пневмотранспорта',
           'Подсобный рабочий',
           'Монтажник санитарно-технических систем и оборудования'],
 '7.8.5': ['Монтажник систем газоснабжения'],
 '7.9': ['Монтажник наружных трубопроводов',
         'Машинист крана-трубоукладчика',
         'Электросварщик ручной сварки',
         'Машинист автокрана',
         'Машинист бульдозера',
         'Стропальщик'],
 '7.9.1': ['Монтажник наружных трубопроводов',
           'Машинист крана-трубоукладчика',
           'Электросварщик ручной сварки',
           'Машинист автокрана',
           'Машинист бульдозера',
           'Стропальщик'],
 '7.9.2': ['Монтажник наружных трубопроводов',
           'Машинист крана-трубоукладчика',
           'Электросварщик ручной сварки',
           'Машинист автокрана',
           'Машинист бульдозера',
           'Стропальщик'],
 '7.10': ['Электромонтажник по кабельным сетям',
          'Монтажник связи-кабельщик',
          'Подсобный рабочий',
          'Слесарь-электромонтажник'],
 '7.11': ['Монтажник связи-кабельщик',
          'Электромонтажник по кабельным сетям',
          'Такелажник',
          'Землекоп',
          'Электромонтажник по электрооборудованию, силовым и осветительным сетям',
          'Машинист'],
 '7.12': ['Электромонтер охранно-пожарной сигнализации', 'Монтажник связи-кабельщик'],
 '7.13': ['Электромонтажник по электрооборудованию, силовым и осветительным сетям',
          'Электросварщик ручной сварки',
          'Монтажник приборов и систем автоматики'],
 '7.14': ['Монтажник систем холодоснабжения'],
 '7.15': ['Монтажник технологических трубопроводов', 'Машинист', 'Слесарь', 'Сварщик'],
 '7.16': ['Изолировщик на термоизоляции (теплоизоляции)', 'Штукатур', 'Подсобный рабочий'],
 '7.19': [],
 '7.19.1': ['Дорожный рабочий', 'Машинист катка', 'Монтажник', 'Стропальщик'],
 '7.19.2': ['Машинист экскаватора', 'Дорожный рабочий', 'Землекоп'],
 '7.19.3': ['Маляр', 'Дорожный рабочий', 'Машинист разметочной машины'],
 '7.24': [],
 '7.24.1': ['Дорожный рабочий', 'Плиточник', 'Подсобный рабочий', 'Стропальщик'],
 '7.24.2': ['Дорожный рабочий', 'Асфальтобетонщик', 'Машинист']}

# Точные стандартные таблицы из страниц 4–5 источника.
# Одна профессия может встречаться в нескольких разрядах — в готовой Форме №2
# эти записи объединяются в одну строку с заполнением нескольких колонок.
COMPANY_ATT_WORKER_PRESETS = {
    'общестрой_без_фасадов': [{'profession': 'Монтажник строительных конструкций', 'razryad': 'III', 'count': 1},
 {'profession': 'Монтажник строительных конструкций', 'razryad': 'IV', 'count': 1},
 {'profession': 'Плотник', 'razryad': 'II', 'count': 1},
 {'profession': 'Слесарь строительный', 'razryad': 'III', 'count': 1},
 {'profession': 'Стропальщик', 'razryad': 'II', 'count': 1},
 {'profession': 'Арматурщик', 'razryad': 'III', 'count': 1},
 {'profession': 'Арматурщик', 'razryad': 'IV', 'count': 1},
 {'profession': 'Электросварщик ручной сварки', 'razryad': 'III', 'count': 1},
 {'profession': 'Электросварщик ручной сварки', 'razryad': 'IV', 'count': 1},
 {'profession': 'Каменщик', 'razryad': 'II', 'count': 1},
 {'profession': 'Такелажник', 'razryad': 'II', 'count': 1},
 {'profession': 'Бетонщик', 'razryad': 'II', 'count': 1},
 {'profession': 'Подсобный рабочий', 'razryad': 'II', 'count': 1},
 {'profession': 'Маляр', 'razryad': 'II', 'count': 1},
 {'profession': 'Маляр', 'razryad': 'III', 'count': 1},
 {'profession': 'Изолировщик на антикоррозионной изоляции', 'razryad': 'III', 'count': 1},
 {'profession': 'Изолировщик на антикоррозионной изоляции', 'razryad': 'IV', 'count': 1},
 {'profession': 'Кровельщик по металлическим кровлям', 'razryad': 'III', 'count': 2},
 {'profession': 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов', 'razryad': 'III', 'count': 1},
 {'profession': 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов', 'razryad': 'IV', 'count': 1},
 {'profession': 'Штукатур', 'razryad': 'III', 'count': 1},
 {'profession': 'Землекоп', 'razryad': 'IV', 'count': 1}],
    'общестрой_с_фасадами': [{'profession': 'Монтажник строительных конструкций', 'razryad': 'III', 'count': 1},
 {'profession': 'Монтажник строительных конструкций', 'razryad': 'IV', 'count': 1},
 {'profession': 'Плотник', 'razryad': 'II', 'count': 1},
 {'profession': 'Слесарь строительный', 'razryad': 'III', 'count': 1},
 {'profession': 'Стропальщик', 'razryad': 'II', 'count': 1},
 {'profession': 'Арматурщик', 'razryad': 'III', 'count': 1},
 {'profession': 'Арматурщик', 'razryad': 'IV', 'count': 1},
 {'profession': 'Электросварщик ручной сварки', 'razryad': 'III', 'count': 1},
 {'profession': 'Электросварщик ручной сварки', 'razryad': 'IV', 'count': 1},
 {'profession': 'Каменщик', 'razryad': 'II', 'count': 1},
 {'profession': 'Такелажник', 'razryad': 'II', 'count': 1},
 {'profession': 'Бетонщик', 'razryad': 'II', 'count': 1},
 {'profession': 'Подсобный рабочий', 'razryad': 'II', 'count': 1},
 {'profession': 'Маляр', 'razryad': 'II', 'count': 1},
 {'profession': 'Изолировщик на антикоррозионной изоляции', 'razryad': 'III', 'count': 1},
 {'profession': 'Изолировщик на антикоррозионной изоляции', 'razryad': 'IV', 'count': 1},
 {'profession': 'Кровельщик по металлическим кровлям', 'razryad': 'III', 'count': 2},
 {'profession': 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов', 'razryad': 'III', 'count': 1},
 {'profession': 'Кровельщик по рулонным кровлям и по кровлям из штучных материалов', 'razryad': 'IV', 'count': 1},
 {'profession': 'Изолировщик на термоизоляции', 'razryad': 'III', 'count': 2}],
}


def _norm_text(value) -> str:
    return re.sub(r'\s+', ' ', str(value or '').lower().replace('ё', 'е')).strip()


def _scope_text(attestation_data: dict) -> str:
    return _norm_text(' '.join([
        str(attestation_data.get('work_scope_text') or ''),
        str(attestation_data.get('scope') or ''),
    ]))


def is_common_construction_scope(attestation_data: dict) -> bool:
    text = _scope_text(attestation_data)
    return 'общестроит' in text or text == 'общестрой'


def is_facade_scope(attestation_data: dict, work_items: list = None) -> bool:
    text = _scope_text(attestation_data)
    return 'фасад' in text or '7.7' in set(work_items or [])


def resolve_work_items(attestation_data: dict) -> list:
    """Возвращает полный дедуплицированный перечень кодов видов работ.

    Общие названия разворачиваются в стандартные наборы. Режимы document_exact
    и manual_exact сохраняют переданный перечень без расширения.
    """
    explicit = [str(x).strip() for x in (attestation_data.get('work_items') or []) if str(x).strip()]
    source = _norm_text(attestation_data.get('work_items_source'))
    if source in ('document_exact', 'manual_exact'):
        return list(dict.fromkeys(explicit))

    resolved = list(explicit)
    scope = _scope_text(attestation_data)
    if 'общестроит' in scope or scope == 'общестрой':
        resolved.extend(['7.2', '7.3', '7.4', '7.5', '7.6'])
        if 'фасад' in scope:
            resolved.append('7.7')
    elif any(k in scope for k in ('сантех', 'водоснабж', 'канализац', 'отоплен', 'вентиляц')):
        resolved.extend(['7.8', '7.9'])
    elif any(k in scope for k in ('электромонтаж', 'электрик', 'электроснабж', 'слаботоч', 'автоматизац')):
        resolved.extend(['7.10', '7.11', '7.12', '7.13'])
    elif any(k in scope for k in ('дорог', 'дорожн')):
        resolved.append('7.19')
    elif 'благоустрой' in scope:
        resolved.append('7.24')
    elif source == 'document':
        return list(dict.fromkeys(explicit))
    elif scope:
        resolved.extend(code for code, _ in find_work_items(scope, max_items=50))
    return list(dict.fromkeys(resolved))


def _worker_key(worker: dict) -> tuple:
    profession = _norm_text(worker.get('profession'))
    razryad = str(worker.get('razryad') or '').strip().upper()
    return profession, razryad


def merge_workers(workers: list) -> list:
    """Объединяет одинаковую профессию+разряд без суммирования дублей.

    Автоматические повторы берут максимальное количество. Строка из документа,
    ручная или экспертная правка заменяет автоматическую, в том числе позволяет
    уменьшить количество.
    """
    merged = {}
    order = []
    for raw in workers or []:
        if not isinstance(raw, dict) or not str(raw.get('profession') or '').strip():
            continue
        item = dict(raw)
        item['profession'] = str(item.get('profession')).strip()
        item['razryad'] = str(item.get('razryad') or STANDARD_WORKER_RAZRYAD).strip().upper()
        try:
            item['count'] = max(1, int(item.get('count') or STANDARD_WORKER_COUNT))
        except (TypeError, ValueError):
            item['count'] = STANDARD_WORKER_COUNT
        key = _worker_key(item)
        source = _norm_text(item.get('source'))
        if key not in merged:
            order.append(key)
            merged[key] = item
        elif source in ('document', 'manual', 'expert'):
            merged[key] = item
        else:
            prev = merged[key]
            prev['count'] = max(prev.get('count', 1), item['count'])
    return [merged[k] for k in order]


def group_workers_for_form(workers: list) -> list:
    """Группирует несколько разрядов одной профессии в одну строку Формы №2."""
    grouped = {}
    order = []
    for worker in merge_workers(workers):
        profession = str(worker.get('profession') or '').strip()
        key = _norm_text(profession)
        if not key:
            continue
        if key not in grouped:
            order.append(key)
            grouped[key] = {
                'profession': profession,
                'counts': {r: 0 for r in RAZRYAD_COLUMNS},
                'total': 0,
            }
        razryad = str(worker.get('razryad') or '').upper().strip()
        count = max(0, int(worker.get('count') or 0))
        if razryad in RAZRYAD_COLUMNS:
            grouped[key]['counts'][razryad] += count
            grouped[key]['total'] += count
    return [grouped[k] for k in order]


def _expanded_worker_rule_codes(work_items: list) -> list:
    """Добавляет родительскую категорию и подпункты выбранной категории."""
    categories = CLASSIFIER['punkt_7_smr']['categories']
    result = []
    for raw_code in work_items or []:
        code = str(raw_code).strip()
        if not code:
            continue
        parent = _get_category_code(code)
        for candidate in (parent, code):
            if candidate not in result:
                result.append(candidate)
        category = categories.get(code)
        if category:
            for sub_code in category.get('sub', {}):
                if sub_code not in result:
                    result.append(sub_code)
        # Пользовательский справочник содержит новые подпункты, которых может ещё
        # не быть в старой версии classifier_company_att.json (например 7.19.1–7.19.3).
        for sub_code in COMPANY_ATT_WORKER_RULES:
            if sub_code.startswith(code + '.') and sub_code not in result:
                result.append(sub_code)
    return result


def build_standard_workers(work_items: list) -> list:
    """Строит стандартный редактируемый состав по всем выбранным кодам."""
    professions = []
    seen = set()
    for code in _expanded_worker_rule_codes(work_items):
        for profession in COMPANY_ATT_WORKER_RULES.get(code, []):
            key = _norm_text(profession)
            if key and key not in seen:
                seen.add(key)
                professions.append(profession)
    return [
        {
            'profession': profession,
            'razryad': STANDARD_WORKER_RAZRYAD,
            'count': STANDARD_WORKER_COUNT,
            'source': 'auto',
        }
        for profession in professions
    ]


def resolve_workers(attestation_data: dict, work_items: list) -> list:
    """Источник истины: заполненная Форма №2 → ручные правки → стандарт по видам работ."""
    incoming = [dict(w) for w in (attestation_data.get('workers') or []) if isinstance(w, dict)]
    confirmed = [w for w in incoming if _norm_text(w.get('source')) in ('document', 'manual', 'expert')]
    document_workers = [w for w in confirmed if _norm_text(w.get('source')) == 'document']

    # Если пользователь загрузил готовую Форму №2, не расширяем её стандартом.
    if document_workers:
        return merge_workers(confirmed)

    excluded = set()
    for item in attestation_data.get('excluded_workers') or []:
        if isinstance(item, dict):
            excluded.add(_worker_key(item))
        else:
            parts = str(item or '').split('|', 1)
            excluded.add((_norm_text(parts[0]), (parts[1] if len(parts) > 1 else '').strip().upper()))

    common_codes = {'7.2', '7.3', '7.4', '7.5', '7.6'}
    use_common_preset = is_common_construction_scope(attestation_data) or common_codes.issubset(set(work_items or []))
    if use_common_preset:
        preset_key = 'общестрой_с_фасадами' if is_facade_scope(attestation_data, work_items) else 'общестрой_без_фасадов'
        auto = [dict(w, source='auto') for w in COMPANY_ATT_WORKER_PRESETS[preset_key]]
    else:
        auto = build_standard_workers(work_items)

    auto = [w for w in auto if _worker_key(w) not in excluded]
    if not auto and incoming:
        auto = [w for w in incoming if _norm_text(w.get('source')) == 'auto']
    return merge_workers(auto + confirmed)



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



def _parse_exact_work_date(value):
    """Parse only a complete date. Never invent day/month for a year-only OCR result."""
    from datetime import datetime as _dt
    if not value:
        return None
    text = str(value).strip()
    for fmt in ('%d.%m.%Y', '%d.%m.%y', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            return _dt.strptime(text, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _confidence_is_low(record: dict, field: str = '') -> bool:
    if not isinstance(record, dict):
        return False
    uncertain = record.get('uncertain_fields') or []
    if isinstance(uncertain, str):
        uncertain = [x.strip() for x in re.split(r'[,;\n]+', uncertain) if x.strip()]
    uncertain_norm = {_norm_text(x) for x in uncertain}
    if field and (_norm_text(field) in uncertain_norm or any(_norm_text(field).endswith(x) for x in uncertain_norm if x)):
        return True
    confidence = None
    field_conf = record.get('field_confidence')
    if isinstance(field_conf, dict) and field in field_conf:
        confidence = field_conf.get(field)
    if confidence is None and field:
        confidence = record.get(f'{field}_confidence')
    if confidence is None:
        confidence = record.get('confidence')
    if isinstance(confidence, (int, float)):
        return float(confidence) < 0.85
    if isinstance(confidence, str):
        return _norm_text(confidence) in ('low', 'низкая', 'низкий', 'uncertain', 'не уверен', 'сомнительно')
    return bool(record.get('needs_review')) and (not field or not uncertain_norm)


def _looks_uncertain_text(value) -> bool:
    text = _norm_text(value)
    return bool(text) and any(marker in text for marker in (
        'неразборчив', 'не уверен', 'требует уточнения', 'две попытки чтения разошлись',
        'или', '[', ']', '?', 'предположительно',
    ))


_MANAGEMENT_POSITION_MARKERS = (
    'производитель работ', 'прораб', 'мастер', 'начальник участка',
    'главный инженер', 'заместитель директора-главный инженер',
    'зам директора-главный инженер', 'заместитель директора по строитель',
    'зам директора по строитель', 'руководитель в области строитель',
)
_DESIGN_POSITION_MARKERS = ('проектиров', 'архитектор', 'конструктор')
_ESTIMATE_POSITION_MARKERS = ('смет',)
_WORKER_POSITION_MARKERS = (
    'каменщик', 'бетонщик', 'арматурщик', 'монтажник', 'электросварщик', 'сварщик',
    'плотник', 'стропальщик', 'такелажник', 'маляр', 'штукатур', 'землекоп',
    'подсобный рабочий', 'рабочий', 'слесарь', 'машинист', 'кровельщик',
    'изолировщик', 'электромонтажник', 'дорожный рабочий', 'плиточник',
)


def _target_role_family(person: dict) -> str:
    position = _norm_text(person.get('position'))
    specialization = _norm_text(person.get('attestat_specialization') or person.get('requested_specialization'))
    target = f'{position} {specialization}'.strip()
    if any(x in target for x in _DESIGN_POSITION_MARKERS):
        return 'design'
    if any(x in target for x in _ESTIMATE_POSITION_MARKERS):
        return 'estimate'
    if any(x in target for x in _MANAGEMENT_POSITION_MARKERS) or 'общестроит' in target:
        return 'management'
    if 'директор' in position and 'замест' not in position:
        # A director may have relevant experience as a director of a construction company,
        # but a director entry must never substitute a chief-engineer/prorab entry.
        return 'director'
    return 'construction'


def _position_match_status(person: dict, period: dict) -> tuple[bool, bool, str]:
    """Return (eligible, uncertain, reason) for a labour-book position."""
    position = _norm_text(period.get('position') or period.get('job_title'))
    if not position:
        return False, True, 'не распознана должность'
    if any(x in position for x in _WORKER_POSITION_MARKERS) and not any(x in position for x in _MANAGEMENT_POSITION_MARKERS):
        return False, False, 'рабочая профессия не включается в стаж ИТР'

    family = _target_role_family(person)
    if family == 'design':
        return (any(x in position for x in _DESIGN_POSITION_MARKERS), False, 'должность не относится к проектированию')
    if family == 'estimate':
        return (any(x in position for x in _ESTIMATE_POSITION_MARKERS), False, 'должность не относится к сметному делу')
    if family == 'management':
        if any(x in position for x in _MANAGEMENT_POSITION_MARKERS):
            return True, False, ''
        # Binding rule from the user: a plain director entry does not count as
        # chief engineer/prorab. It counts only when the labour book explicitly
        # records the secondary/combined construction position.
        if 'директор' in position:
            explicit = _norm_text(period.get('secondary_position') or period.get('explicit_construction_role'))
            if any(x in explicit for x in _MANAGEMENT_POSITION_MARKERS):
                return True, _confidence_is_low(period, 'secondary_position'), ''
            return False, False, 'запись директора без перевода/совмещения на строительную должность'
        return False, False, 'должность не относится к руководящим строительным должностям'
    if family == 'director':
        if 'директор' in position or any(x in position for x in _MANAGEMENT_POSITION_MARKERS):
            return True, False, ''
        return False, False, 'должность не относится к руководству строительством'

    # Unknown construction ITR: include clear engineering/management positions,
    # but flag ambiguous generic roles for review.
    if any(x in position for x in _MANAGEMENT_POSITION_MARKERS) or 'инженер' in position:
        return True, _confidence_is_low(period, 'position'), ''
    return False, True, 'не удалось уверенно отнести должность к заявляемой деятельности'


def _employer_match_status(person: dict, period: dict) -> tuple[bool, bool, str]:
    activity = _norm_text(period.get('employer_activity'))
    explicit = period.get('is_construction_employer')
    family = _target_role_family(person)
    if explicit is False:
        return False, False, 'работодатель не относится к нужной деятельности'
    if explicit is True:
        return True, _confidence_is_low(period, 'employer_activity'), ''

    if family == 'design':
        markers = ('проектир', 'архитектур', 'строител')
    elif family == 'estimate':
        markers = ('смет', 'строител', 'проектир')
    else:
        markers = ('строител', 'смр', 'монтаж', 'генподряд', 'подряд')
    if activity and any(x in activity for x in markers):
        return True, _confidence_is_low(period, 'employer_activity'), ''
    if period.get('relevant') is True:
        return True, True, 'строительная деятельность работодателя указана неявно'
    return False, True, 'не подтверждена деятельность работодателя'


def assess_relevant_periods(person: dict, periods: list) -> dict:
    """Classify every labour-book period without doing arithmetic in the model.

    Only complete dates enter the confirmed calculation. Potentially relevant rows
    with unreadable/partial dates are retained as review reasons, so the resulting
    minimum confirmed experience is highlighted yellow instead of silently guessed.
    """
    confirmed, uncertain, excluded = [], [], []
    for index, raw in enumerate(periods or []):
        if not isinstance(raw, dict):
            continue
        period = dict(raw)
        period['_index'] = index
        if period.get('relevant') is False:
            excluded.append({'period': period, 'reason': 'явно исключён из стажа'})
            continue
        role_ok, role_uncertain, role_reason = _position_match_status(person, period)
        employer_ok, employer_uncertain, employer_reason = _employer_match_status(person, period)
        start = _parse_exact_work_date(period.get('start'))
        end_value = period.get('end')
        end = _parse_exact_work_date(end_value) if end_value else None
        date_uncertain = (
            not start
            or (bool(end_value) and not end)
            or _confidence_is_low(period, 'start')
            or _confidence_is_low(period, 'end')
            or _looks_uncertain_text(period.get('start_text'))
            or _looks_uncertain_text(period.get('end_text'))
        )
        reasons = [x for x in (role_reason if not role_ok else '', employer_reason if not employer_ok else '') if x]
        if not role_ok or not employer_ok:
            item = {'period': period, 'reason': '; '.join(reasons) or 'период не соответствует правилам'}
            # An unknown employer/role may still be relevant; show it as uncertain rather
            # than silently discarding it when there is not an explicit negative answer.
            if role_uncertain or employer_uncertain:
                uncertain.append(item)
            else:
                excluded.append(item)
            continue
        if date_uncertain:
            uncertain.append({'period': period, 'reason': 'нет полной подтверждённой даты начала/окончания'})
            continue
        if end is not None and end < start:
            uncertain.append({'period': period, 'reason': 'дата окончания раньше даты начала'})
            continue
        period['start'] = start.strftime('%d.%m.%Y')
        period['end'] = end.strftime('%d.%m.%Y') if end else None
        period['_period_uncertain'] = bool(role_uncertain or employer_uncertain or _confidence_is_low(period))
        confirmed.append(period)
        if period['_period_uncertain']:
            uncertain.append({'period': period, 'reason': 'период включён, но часть реквизитов требует проверки'})
    return {'confirmed': confirmed, 'uncertain': uncertain, 'excluded': excluded}


def _ru_count(value: int, one: str, few: str, many: str) -> str:
    n = abs(int(value))
    if 11 <= n % 100 <= 14:
        word = many
    elif n % 10 == 1:
        word = one
    elif n % 10 in (2, 3, 4):
        word = few
    else:
        word = many
    return f'{value} {word}'


def calculate_stazh(periods: list, as_of_date: str = None) -> dict:
    """Deterministic inclusive calculation from exact labour-book dates.

    Overlapping intervals are merged so the same calendar day is never counted twice.
    The final normalization follows the common HR calculator convention used for work
    records: 30 days = 1 month, 12 months = 1 year.
    """
    from datetime import datetime as _dt, timedelta as _td

    today = _parse_exact_work_date(as_of_date) or _dt.now()
    intervals = []
    invalid = []
    for period in (periods or []):
        if not isinstance(period, dict) or period.get('relevant') is False:
            continue
        start = _parse_exact_work_date(period.get('start'))
        end = _parse_exact_work_date(period.get('end')) if period.get('end') else today
        if not start or not end or end < start:
            invalid.append(period)
            continue
        intervals.append((start, end))

    intervals.sort(key=lambda x: x[0])
    merged = []
    for start, end in intervals:
        if not merged or start > merged[-1][1] + _td(days=1):
            merged.append([start, end])
        elif end > merged[-1][1]:
            merged[-1][1] = end

    total_days = sum((end - start).days + 1 for start, end in merged)
    months, days = divmod(total_days, 30)
    years, months = divmod(months, 12)
    if total_days <= 0:
        display = ''
    else:
        parts = []
        if years:
            parts.append(_ru_count(years, 'год', 'года', 'лет'))
        if months:
            parts.append(_ru_count(months, 'месяц', 'месяца', 'месяцев'))
        if days or not parts:
            parts.append(_ru_count(days, 'день', 'дня', 'дней'))
        display = ' '.join(parts)
    return {
        'years': years, 'months': months, 'days': days,
        'total_days': total_days,
        'total_years_rounded': round(total_days / 365.2425, 1) if total_days else 0,
        'display': display,
        'intervals': [
            {'start': start.strftime('%d.%m.%Y'), 'end': end.strftime('%d.%m.%Y')}
            for start, end in merged
        ],
        'invalid_periods': invalid,
    }


def select_relevant_periods(person: dict, periods: list) -> list:
    """Compatibility wrapper: only confirmed, exact, role-matching periods."""
    return assess_relevant_periods(person, periods).get('confirmed', [])


def _period_is_current_company(period: dict, company: dict) -> tuple[bool, bool]:
    company_name = _norm_text(company.get('name'))
    company_unp = re.sub(r'\D', '', str(company.get('unp') or ''))
    employer = _norm_text(period.get('employer'))
    employer_unp = re.sub(r'\D', '', str(period.get('employer_unp') or ''))
    if period.get('is_current_employer') is True:
        return True, _confidence_is_low(period, 'is_current_employer')
    if company_unp and employer_unp:
        return company_unp == employer_unp, False
    if company_name and employer:
        # Strip legal form and quotes before fuzzy containment.
        compact_company = re.sub(r'\b(ооо|одо|оао|зао|чуп|чтуп|ип)\b', '', company_name).strip()
        compact_employer = re.sub(r'\b(ооо|одо|оао|зао|чуп|чтуп|ип)\b', '', employer).strip()
        if compact_company and (compact_company in compact_employer or compact_employer in compact_company):
            return True, _confidence_is_low(period, 'employer')
    return False, False


def calculate_current_company_stazh(periods: list, company: dict, as_of_date: str = None) -> dict:
    selected = []
    uncertain_match = False
    for period in periods or []:
        if not isinstance(period, dict):
            continue
        matches, uncertain = _period_is_current_company(period, company)
        if matches:
            selected.append(period)
            uncertain_match = uncertain_match or uncertain
    result = calculate_stazh(selected, as_of_date=as_of_date)
    result['employer_match_uncertain'] = uncertain_match
    return result


def _stage_reference_lines(person: dict) -> tuple[str, str]:
    """Read completed Form №2 values only as an independent control reference."""
    first = str(person.get('stage_reference_total') or '').strip()
    second = str(person.get('stage_reference_here') or '').strip()
    raw = person.get('stage_form2_text') or []
    values = raw if isinstance(raw, (list, tuple)) else [raw]
    lines = []
    for value in values:
        for part in re.split(r'[\r\n]+', str(value or '')):
            text = re.sub(r'\s+', ' ', part).strip()
            if text and text not in ('—', '-', '–'):
                lines.append(text)
    return first or (lines[0] if lines else ''), second or (lines[1] if len(lines) > 1 else '')


def _reference_stage_range_days(value: str):
    """Return an approximate acceptable range for a rounded Form №2 reference.

    Completed forms often contain only ``49 лет`` or ``менее года``. Such a value is
    not an exact calculation and therefore must not make a precise labour-book result
    yellow merely because it also contains months and days. The range is used only for
    a sanity check; it never supplies the live stage value.
    """
    text = _norm_text(value)
    if not text:
        return None
    if 'менее года' in text or 'меньше года' in text:
        return (0, 365)
    years_m = re.search(r'(\d+)\s*(?:год|года|лет)', text)
    months_m = re.search(r'(\d+)\s*месяц', text)
    days_m = re.search(r'(\d+)\s*(?:день|дня|дней)', text)
    if not any((years_m, months_m, days_m)):
        return None
    years = int(years_m.group(1)) if years_m else 0
    months = int(months_m.group(1)) if months_m else 0
    days = int(days_m.group(1)) if days_m else 0
    base = years * 365 + months * 30 + days
    # If only full years/months are written, the document is normally rounded down.
    if years_m and not months_m and not days_m:
        return (base, base + 365)
    if months_m and not days_m:
        return (base, base + 31)
    return (max(0, base - 2), base + 3)


def _reference_conflicts(value: str, calculation: dict) -> bool:
    bounds = _reference_stage_range_days(value)
    total_days = int((calculation or {}).get('total_days') or 0)
    if not bounds or total_days <= 0:
        return False
    return not (bounds[0] <= total_days < bounds[1])


def _labour_hire_date(person: dict):
    """Use a standalone hire date only when its source is explicitly the labour book."""
    source = _norm_text(person.get('hire_date_source'))
    if source not in ('трудовая книжка', 'labor book', 'labour book', 'employment periods', 'employment_periods'):
        return None
    return _parse_exact_work_date(person.get('hire_date'))


def calculate_person_experience(person: dict, company: dict, as_of_date: str = None) -> dict:
    """Calculate both Form №2 stage values only from labour-book chronology.

    The model extracts exact employment intervals; this backend performs all date
    arithmetic, filters irrelevant roles/employers, merges overlapping intervals and
    separately calculates the period at the current employer. Completed Form №2 values
    are comparison-only and can never be copied into the generated result.
    """
    as_of_date = as_of_date or __import__('datetime').datetime.now().strftime('%d.%m.%Y')
    periods = person.get('employment_periods') or []
    assessment = assess_relevant_periods(person, periods)
    confirmed = assessment['confirmed']
    total = calculate_stazh(confirmed, as_of_date=as_of_date)
    current = calculate_current_company_stazh(confirmed, company, as_of_date=as_of_date)
    reference_total, reference_here = _stage_reference_lines(person)

    reasons = []
    if assessment['uncertain']:
        reasons.extend(item.get('reason', 'период требует проверки') for item in assessment['uncertain'])
    if not periods:
        reasons.append('в трудовой книжке не распознаны периоды работы')

    # Live values are NEVER taken from completed Form №2.
    person['stage_years'] = total.get('display', '')
    person['stage_total_source'] = 'labour_book_calculation' if total.get('display') else 'missing'

    if current.get('display'):
        person['stage_years_here'] = current['display']
        person['stage_here_source'] = 'labour_book_calculation'
    else:
        # Compatibility for an exact date that was independently extracted from the
        # labour book but not yet converted into an employment_periods row.
        hire_date = _labour_hire_date(person)
        if hire_date:
            role_ok, role_uncertain, role_reason = _position_match_status(
                person, {'position': person.get('position')}
            )
            if role_ok:
                fallback = calculate_stazh(
                    [{'start': hire_date.strftime('%d.%m.%Y'), 'end': None}],
                    as_of_date,
                )
                person['stage_years_here'] = fallback.get('display', '')
                person['stage_here_source'] = 'labour_book_hire_date'
                current = fallback
                if role_uncertain:
                    reasons.append('должность по записи текущего нанимателя требует проверки')
            else:
                person['stage_years_here'] = ''
                person['stage_here_source'] = 'missing'
                reasons.append(role_reason or 'текущая должность не соответствует заявляемой деятельности')
        else:
            person['stage_years_here'] = ''
            person['stage_here_source'] = 'missing'

    if not person.get('stage_years'):
        reasons.append('не удалось рассчитать общий стаж по точным записям трудовой книжки')
    if not person.get('stage_years_here'):
        reasons.append('не удалось рассчитать стаж у текущего нанимателя по трудовой книжке')

    # A completed Form №2 is only a control. Rounded values are compared as ranges so
    # ``49 лет`` does not conflict with ``49 лет 4 месяца 10 дней``.
    person['stage_reference_mismatch'] = False
    if reference_total and _reference_conflicts(reference_total, total):
        person['stage_reference_mismatch'] = True
        reasons.append(
            f'расчёт общего стажа по трудовой ({total.get("display") or "нет значения"}) '
            f'существенно отличается от справочного значения Формы №2 ({reference_total})'
        )
    if reference_here and _reference_conflicts(reference_here, current):
        person['stage_reference_mismatch'] = True
        reasons.append(
            f'расчёт стажа у нанимателя по трудовой ({current.get("display") or "нет значения"}) '
            f'существенно отличается от справочного значения Формы №2 ({reference_here})'
        )

    if current.get('employer_match_uncertain'):
        reasons.append('совпадение текущего работодателя требует проверки')
    if any(p.get('_period_uncertain') for p in confirmed):
        reasons.append('один из включённых периодов распознан неуверенно')

    person['stage_source'] = 'calculated' if person.get('stage_years') else 'missing'
    person['stage_is_final'] = bool(
        person.get('stage_years')
        and person.get('stage_years_here')
        and not reasons
    )
    person['stage_needs_review'] = bool(reasons)
    person['stage_review_reasons'] = list(dict.fromkeys(reasons))
    person['stage_calculation'] = {
        'as_of_date': as_of_date,
        'role_family': _target_role_family(person),
        'confirmed_periods': confirmed,
        'uncertain_periods': assessment['uncertain'],
        'excluded_periods': assessment['excluded'],
        'total': total,
        'current_company': current,
        'reference_total': reference_total,
        'reference_here': reference_here,
        'reference_used_as_value': False,
    }
    return person

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
        # Реальная структура: № | Профессия | II | III | IV | V | VI | Итого.
        # Несколько разрядов одной профессии объединяются в одну строку.
        w_widths = [548, 4254, 749, 749, 749, 749, 750, 1505]
        w_headers = ["№ п/п", "Наименование профессий рабочих"] + RAZRYAD_COLUMNS + ["Итого"]
        w_rows = []
        totals = {r: 0 for r in RAZRYAD_COLUMNS}
        grouped_workers = group_workers_for_form(workers)
        for i, w in enumerate(grouped_workers, 1):
            row = [str(i), w.get('profession', '')]
            for r in RAZRYAD_COLUMNS:
                count = int((w.get('counts') or {}).get(r) or 0)
                row.append(str(count) if count else '')
                totals[r] += count
            row.append(str(w.get('total') or '') if w.get('total') else '')
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

    as_of_date = attestation_data.get('as_of_date') or __import__('datetime').datetime.now().strftime('%d.%m.%Y')
    attestation_data['as_of_date'] = as_of_date
    for person in itr_list:
        calculate_person_experience(person, company, as_of_date=as_of_date)

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

    work_items = resolve_work_items(attestation_data)
    workers = resolve_workers(attestation_data, work_items)
    if not work_items:
        warnings.append('Не определены виды работ для заявления. Заполните work_items или work_scope_text.')

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
