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


SPK_ACTIVITY_PROFILES = {
    'construction': {
        'order_purpose': ('С целью повышения качества, конкурентоспособности и укрепления экономического потенциала '
                          'организации, решения задач по дальнейшему укреплению доверия потребителей к деятельности '
                          'организации по производству строительных работ, достижения и поддержания высокого уровня '
                          'качества выполняемых работ'),
        'training_purpose': ('С целью подготовки специалистов к проведению контроля качества работ, обеспечению '
                             'достоверности результатов контроля качества, получения объективной оценки качества '
                             'выполняемых работ на всех стадиях производства работ (входной контроль, операционный, '
                             'приемочный контроль)'),
        'protocol_scope': 'строительно-монтажных работ',
        'itr_experience_field': 'стаж работы в области строительства',
        'director_responsibility': ('Функционирование СПК; организация проведения внутренних аудитов; входной контроль '
                                    'ПСД; ведение Журнала учета рекламаций по качеству СМР и принятия мер по ним;'),
        'polozhenie_replacements': (),
    },
    'construction_metal': {
        'order_purpose': ('С целью повышения качества, конкурентоспособности и укрепления экономического потенциала '
                          'организации, решения задач по дальнейшему укреплению доверия потребителей к деятельности '
                          'организации по производству строительных работ, производству металлоконструкций достижения '
                          'и поддержания высокого уровня качества выполняемых работ'),
        'training_purpose': ('С целью подготовки специалистов к проведению контроля качества работ, обеспечению '
                             'достоверности результатов контроля качества, получения объективной оценки качества '
                             'выполняемых работ на всех стадиях производства работ и продукции (входной контроль, '
                             'операционный, приемочный контроль)'),
        'protocol_scope': 'строительно-монтажных работ и производства металлоконструкций',
        'itr_experience_field': 'стаж работы в области строительства и производства металлоконструкций',
        'director_responsibility': ('Функционирование СПК; организация проведения внутренних аудитов; входной контроль '
                                    'ПСД; ведение Журнала учета рекламаций по качеству СМР и принятия мер по ним;'),
        'polozhenie_replacements': (),
    },
    'metal_only': {
        'order_purpose': ('С целью повышения качества, конкурентоспособности и укрепления экономического потенциала '
                          'организации, решения задач по дальнейшему укреплению доверия потребителей к деятельности '
                          'организации по производству металлоконструкций, достижения и поддержания высокого уровня '
                          'качества выпускаемой продукции'),
        'training_purpose': ('С целью подготовки специалистов к проведению контроля качества производства '
                             'металлоконструкций, обеспечению достоверности результатов контроля качества, получения '
                             'объективной оценки качества продукции на всех стадиях производства (входной контроль, '
                             'операционный, приемо-сдаточный и периодический контроль)'),
        'protocol_scope': 'производства металлоконструкций',
        'itr_experience_field': 'стаж работы в области производства металлоконструкций',
        'director_responsibility': ('Функционирование СПК; организация проведения внутренних аудитов; входной контроль '
                                    'материалов и документации; ведение Журнала учета рекламаций по качеству '
                                    'производимых металлоконструкций и принятия мер по ним;'),
        'polozhenie_replacements': (
            ('Основной задачей производственного контроля',
             'Основной задачей производственного контроля является предотвращение выпуска некачественных металлоконструкций и достижение постоянного соответствия выпускаемой продукции требованиям технических нормативных правовых актов (далее по тексту – ТНПА) и технологической документации.'),
            ('Основной функцией производственного контроля',
             'Основной функцией производственного контроля является обеспечение входного, операционного, приемо-сдаточного и периодического контролей производимой продукции.'),
            ('проводить операционный и приемочный контроль выполняемых работ с регистрацией',
             'проводить операционный, приемо-сдаточный и периодический контроль производимых металлоконструкций с регистрацией результатов в соответствующих журналах контроля;'),
            ('Операционный контроль при производстве СМР проводит',
             'Операционный контроль при производстве металлоконструкций проводит лицо, назначенное приказом. Все результаты контроля заносятся в журнал операционного контроля.'),
            ('Приемочный контроль при производстве СМР осуществляется',
             'Приемо-сдаточный контроль металлоконструкций осуществляется в соответствии с требованиями ТНПА и технологической документации на каждый вид выпускаемой продукции.'),
            ('приемку некачественно выполненных работ в строительстве',
             '- приемку некачественных металлоконструкций;'),
            ('проводить входной контроль закупаемой продукции, операционный и приемочный',
             '- проводить входной контроль закупаемой продукции, операционный, приемо-сдаточный и периодический контроль производимых металлоконструкций;'),
            ('Порядок применения технологической документации',
             'Технологическая документация на производство металлоконструкций применяется в соответствии с действующими ТНПА, технологическими регламентами и технологическими картами организации.'),
            ('Порядок приобретения технологической документации',
             'Необходимая технологическая документация на производство металлоконструкций приобретается у разработчиков либо разрабатывается и утверждается организацией в установленном порядке.'),
            ('Предприятие приобретает необходимые типовые технологические карты',
             'Оригиналы технологических регламентов и технологических карт на производство металлоконструкций хранятся у назначенного приказом специалиста; рабочие экземпляры регистрируются и выдаются по необходимости.'),
            ('внешнее взаимодействие при контроле качества выполненных СМР',
             '- внешнее взаимодействие при контроле качества выпускаемых металлоконструкций — с аккредитованной лабораторией в части необходимых испытаний материалов, сварных соединений и покрытий;'),
            ('В состав системы производственного контроля входит персонал',
             'В состав системы производственного контроля входит персонал, работающий на предприятии на постоянной основе и имеющий соответствующее образование, профессиональную подготовку, технические знания и опыт работы в области контроля качества металлоконструкций и метрологического обеспечения производства не менее одного года.'),
        ),
    },
    'low_voltage_systems': {
        'order_purpose': ('С целью обеспечения качества выполнения электромонтажных работ, работ по монтажу '
                          'слаботочных систем, систем пожарной сигнализации, пожаротушения, связи, диспетчеризации, '
                          'видеонаблюдения, охранной сигнализации, линейно-кабельных сооружений электросвязи, '
                          'соблюдения требований технических нормативных правовых актов и проектной документации'),
        'training_purpose': ('С целью подготовки специалистов к проведению контроля качества электромонтажных работ, '
                             'работ по монтажу слаботочных систем, систем пожарной сигнализации, пожаротушения, связи, '
                             'диспетчеризации, видеонаблюдения, охранной сигнализации, линейно-кабельных сооружений '
                             'электросвязи, обеспечения достоверности результатов контроля качества, получения '
                             'объективной оценки качества выполняемых работ на всех стадиях производства работ '
                             '(входной контроль, операционный, приёмочный контроль)'),
        'protocol_scope': ('электромонтажных работ, работ по монтажу слаботочных систем, систем пожарной сигнализации, '
                           'пожаротушения, связи, диспетчеризации, видеонаблюдения, охранной сигнализации, '
                           'линейно-кабельных сооружений электросвязи'),
        'itr_experience_field': 'стаж работы в области электромонтажных и слаботочных систем',
        'director_responsibility': ('Функционирование СПК; организация проведения внутренних аудитов; входной контроль '
                                    'ПСД; ведение Журнала учета рекламаций по качеству электромонтажных и слаботочных '
                                    'работ и принятия мер по ним;'),
        'polozhenie_replacements': (
            ('Система производственного контроля предприятия создана',
             'Система производственного контроля предприятия создана с целью обеспечения возможности достижения требуемых показателей качества выполняемых электромонтажных работ, работ по монтажу слаботочных систем, систем пожарной сигнализации, пожаротушения, связи, диспетчеризации, видеонаблюдения, охранной сигнализации, линейно-кабельных сооружений электросвязи, обеспечения единства и достоверности результатов контроля, получения объективной оценки качества выполняемых работ и обеспечения гарантии качества выполняемых работ.'),
            ('Основной задачей производственного контроля',
             'Основной задачей производственного контроля является предотвращение некачественного производства электромонтажных и слаботочных работ, монтажа систем пожарной сигнализации, пожаротушения, связи, видеонаблюдения, охранной сигнализации, линейно-кабельных сооружений и достижение постоянного соответствия производимых работ требованиям технических нормативных правовых актов (далее — ТНПА) и проектной документации.'),
            ('Основной функцией производственного контроля',
             'Основной функцией производственного контроля является обеспечение входного, операционного и приемочного контроля выполняемых работ.'),
            ('проводить операционный и приемочный контроль выполняемых работ с регистрацией',
             'проводить операционный и приемочный контроль выполняемых работ с регистрацией в «Журнале производства работ» (СН 1.03.04-2020);'),
            ('Операционный контроль при производстве СМР проводит',
             'Операционный контроль проводит лицо, назначенное приказом. Все результаты контроля заносятся в журнал производства работ (СН 1.03.04-2020).'),
            ('Приемочный контроль при производстве СМР осуществляется',
             'Приемочный контроль осуществляется в соответствии с требованиями постановления Совмина от 06.06.2011 №716 «Об утверждении положения о порядке приемки в эксплуатацию объектов строительства» в части приёмки электромонтажных, слаботочных работ и работ по монтажу систем пожарной сигнализации, пожаротушения, связи, видеонаблюдения, охранной сигнализации, линейно-кабельных сооружений.'),
            ('приемку некачественно выполненных работ в строительстве',
             '- приемку некачественно выполненных электромонтажных и слаботочных работ, работ по монтажу систем пожарной сигнализации, пожаротушения, связи, видеонаблюдения, охранной сигнализации, линейно-кабельных сооружений;'),
            ('проводить входной контроль закупаемой продукции, операционный и приемочный',
             '- проводить входной контроль закупаемой продукции, операционный и приемочный контроль электромонтажных и слаботочных работ;'),
            ('Порядок применения технологической документации',
             'Предприятие приобретает необходимые типовые технологические карты у разработчиков. Лицо, назначенное приказом, регистрирует технологические карты в журнале учета ТТК и ТНПА; оригиналы хранятся у назначенного специалиста, рабочие экземпляры выдаются по необходимости.'),
            ('Порядок приобретения технологической документации',
             'Ответственным за учет, регистрацию, хранение, актуализацию и выдачу технологических документов является лицо, назначенное приказом по предприятию.'),
            ('Предприятие приобретает необходимые типовые технологические карты',
             'Технологическая документация по электромонтажным и слаботочным системам пересматривается по мере изменения ТНПА, проектных решений или технологии выполнения работ.'),
            ('внешнее взаимодействие при контроле качества выполненных СМР',
             '- с производственными участками электромонтажных, слаботочных систем, монтажа СПС, АПТ, связи, видеонаблюдения, охранной сигнализации и линейно-кабельных сооружений — в части контроля качества работ и получения необходимой информации о ходе работ;'),
            ('В состав системы производственного контроля входит персонал',
             'В состав системы производственного контроля входит персонал, работающий на предприятии на постоянной основе и имеющий соответствующее образование, профессиональную подготовку, технические знания и опыт работы в области контроля качества электромонтажных, слаботочных работ и работ по монтажу систем пожарной сигнализации, пожаротушения, связи, видеонаблюдения, охранной сигнализации, линейно-кабельных сооружений и метрологического обеспечения производства не менее одного года.'),
        ),
    },
}


def _spk_activity_profile(spk_data=None):
    key = str((spk_data or {}).get('activity_profile') or 'construction').strip().lower()
    return key, SPK_ACTIVITY_PROFILES.get(key, SPK_ACTIVITY_PROFILES['construction'])


def _responsible_position_forms(position: str) -> tuple[str, str]:
    """Return accusative and dative role labels without changing the person's real title."""
    value = str(position or '').lower().replace('ё', 'е')
    if 'мастер' in value:
        return 'Мастера производственного участка', 'Мастеру производственного участка'
    if 'заместител' in value and 'главн' in value and 'инженер' in value:
        return 'Заместителя директора-главного инженера', 'Заместителю директора-главному инженеру'
    if 'главн' in value and 'инженер' in value:
        return 'Главного инженера', 'Главному инженеру'
    return 'Производителя работ', 'Производителю работ'


def _replace_paragraphs_by_marker(xml: str, replacements) -> str:
    """Replace whole paragraphs while retaining the Word style from the source template."""
    for marker, replacement in replacements:
        matching = [
            paragraph for paragraph in _paragraphs(xml)
            if marker.lower() in re.sub(r'<[^>]+>', '', paragraph).lower()
        ]
        for paragraph in matching:
            if paragraph in xml:
                xml = xml.replace(paragraph, _replace_para_text(paragraph, replacement), 1)
    return xml


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
                       director_fio: str, gl_inzhener_fio: str, foremen_fio: list,
                       profile: dict = None, operational_position: str = '') -> bytes:
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
    profile = profile or SPK_ACTIVITY_PROFILES['construction']
    operational_position_acc, _ = _responsible_position_forms(operational_position)

    replacements = {0: full_name, 4: f"{order_date} № {order_number}", 6: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)
    xml = _replace_paragraphs_by_marker(xml, [
        ('С целью повышения качества', profile['order_purpose']),
    ])

    # --- Список лиц, задействованных в СПК (абзацы 16-19 в образце): по одному на
    #     каждого человека, формат "Фамилия И.О., Должность" (родительный падеж) ---
    idx_list_start = _find_para_index(paras, lambda t: t.startswith('2. В системе'))
    idx_resp_dir = _find_para_index(paras, lambda t: t.startswith('3. Директора'))
    if idx_list_start >= 0 and idx_resp_dir >= 0:
        people = [(director_fio, 'Директора')]
        if gl_inzhener_fio:
            people.append((gl_inzhener_fio, 'Главного инженера'))
        for f in foremen_fio:
            people.append((f, operational_position_acc))
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
        new_t = (
            f'3. Директора {_fio_initials_surname_first(director_fio)} назначить ответственным за '
            f'{profile["director_responsibility"].rstrip(";")}. '
        )
        xml = xml.replace(paras[idx3], _replace_para_text(paras[idx3], new_t), 1)
    if idx4 >= 0 and paras[idx4] in xml and gl_inzhener_fio:
        old_t = re.sub(r'<[^>]+>', '', paras[idx4]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Главного инженера [^\s]+ [^\s]+', f'Главного инженера {_fio_initials_surname_first(gl_inzhener_fio)}', old_t, count=1)
        xml = xml.replace(paras[idx4], _replace_para_text(paras[idx4], new_t), 1)
    if idx5 >= 0 and paras[idx5] in xml and foremen_fio:
        if len(foremen_fio) == 1:
            new_t = f"5. {operational_position_acc} {_fio_initials_surname_first(foremen_fio[0])}, назначить ответственным за Входной, операционный, приемочный контроль;"
        else:
            names_part = ', '.join(f"{operational_position_acc.lower()} {_fio_initials_surname_first(f)}" for f in foremen_fio)
            new_t = f"5. {names_part}, назначить ответственными за Входной, операционный, приемочный контроль;"
        xml = xml.replace(paras[idx5], _replace_para_text(paras[idx5], new_t), 1)
    elif idx5 >= 0 and paras[idx5] in xml:
        # The source template may contain sample foremen. Do not retain them
        # when the confirmed SPK staff only has a director and chief engineer.
        xml = xml.replace(paras[idx5], _replace_para_text(paras[idx5], ''), 1)

    xml = _replace_director_signature(xml, paras, dir_init)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 3: Приказ о внутреннем обучении ═══════════════════
def render_prikaz_obuchenie(company: dict, order_number: str, order_date: str, city: str,
                             director_fio: str, deadline_date: str, profile: dict = None) -> bytes:
    parts = _load_parts('3_prikaz_obuchenie.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    dir_init = _dir_initials(director_fio)
    profile = profile or SPK_ACTIVITY_PROFILES['construction']

    idx_deadline = _find_para_index(paras, lambda t: t.startswith('2. До'))
    replacements = {0: full_name, 4: f"{order_date} № {order_number}", 6: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)
    xml = _replace_paragraphs_by_marker(xml, [
        ('С целью подготовки специалистов', profile['training_purpose']),
    ])
    if idx_deadline >= 0 and paras[idx_deadline] in xml:
        old_text = re.sub(r'<[^>]+>', '', paras[idx_deadline]).strip()
        new_text = re.sub(r'\d{2}\.\d{2}\.\d{4}', deadline_date, old_text, count=1)
        xml = xml.replace(paras[idx_deadline], _replace_para_text(paras[idx_deadline], new_text), 1)

    xml = _replace_director_signature(xml, paras, dir_init)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 4: Приказ о ТО средств измерений ═══════════════════
def render_prikaz_to_si(company: dict, order_number: str, order_date: str, city: str,
                         director_fio: str, responsible_fio: str, responsible_position: str = '') -> bytes:
    parts = _load_parts('4_prikaz_to_si.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    dir_init = _dir_initials(director_fio)
    resp_acc = _fio_initials_surname_first(responsible_fio)   # "Туника Д.И."
    resp_dat = _fio_initials_dative(responsible_fio)          # "Тунику Д.И."
    position_acc, position_dat = _responsible_position_forms(responsible_position)

    replacements = {0: full_name, 4: f"{order_date} № {order_number}", 6: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)

    idx1 = _find_para_index(paras, lambda t: t.startswith('1. Производителя работ'))
    idx2 = _find_para_index(paras, lambda t: t.startswith('2. Производителю работ'))
    if idx1 >= 0 and paras[idx1] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx1]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Производителя работ [^\s]+ [^\s]+', f'{position_acc} {resp_acc}', old_t, count=1)
        xml = xml.replace(paras[idx1], _replace_para_text(paras[idx1], new_t), 1)
    if idx2 >= 0 and paras[idx2] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx2]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Производителю работ [^\s]+ [^\s]+', f'{position_dat} {resp_dat}', old_t, count=1)
        xml = xml.replace(paras[idx2], _replace_para_text(paras[idx2], new_t), 1)

    xml = _replace_director_signature(xml, paras, dir_init)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 5: Приказ о назначении ответственного за машины ═══════════════════
def render_prikaz_mashiny(company: dict, order_number: str, order_date: str, city: str,
                           director_fio: str, responsible_fio: str, responsible_position: str = '') -> bytes:
    parts = _load_parts('5_prikaz_mashiny.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    resp_acc = _fio_initials_surname_first(responsible_fio)
    position_acc, _ = _responsible_position_forms(responsible_position)
    dir_init = _dir_initials(director_fio)

    replacements = {0: full_name, 3: f"{order_date} № {order_number}", 5: city}
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)

    idx1 = _find_para_index(paras, lambda t: t.startswith('1. Назначить ответственным'))
    if idx1 >= 0 and paras[idx1] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx1]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'Производителя работ [^\s]+ [^\s]+', f'{position_acc} {resp_acc}', old_t, count=1)
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
    'мастер': "Входной, операционный, приемочный контроль; Обеспечение и содержание в рабочем состоянии машин и механизмов; учет, хранение, актуализация, выдача ТНПА, ТК; метрологическое обеспечение.",
}


# ═══════════════════ Документ 6: Справка ИТР ═══════════════════
def _is_ptu_diploma(diploma: dict) -> bool:
    """ПТУ в СПК-справку не включается по правилу оформителя."""
    text = ' '.join(str(diploma.get(key) or '') for key in (
        'number', 'institution', 'speciality', 'qualification', 'education_level', 'full_text',
    )).lower().replace('ё', 'е')
    return bool(re.search(r'\bпту\b|профессионально[ -]техническ', text))


def _itr_diploma_lines(person: dict) -> list:
    """Возвращает все подтверждённые непрофессионально-технические дипломы."""
    values = person.get('diplomas') or []
    if not values:
        values = [{
            'number': person.get('diploma_number', ''),
            'date': person.get('diploma_date', ''),
            'institution': person.get('diploma_institution', ''),
            'speciality': person.get('diploma_speciality', ''),
            'qualification': person.get('diploma_qualification', ''),
            'education_level': person.get('education_level', ''),
        }]

    lines, seen = [], set()
    for value in values:
        diploma = dict(value) if isinstance(value, dict) else {'number': str(value)}
        if _is_ptu_diploma(diploma):
            continue
        full_text = str(diploma.get('full_text') or '').strip()
        if full_text:
            key = full_text.lower()
            if key not in seen:
                seen.add(key)
                lines.append(full_text)
            continue
        key = tuple(str(diploma.get(field) or '').strip().lower() for field in (
            'number', 'date', 'institution', 'speciality', 'qualification', 'education_level',
        ))
        if key in seen or not any(key):
            continue
        seen.add(key)
        parts = []
        if diploma.get('education_level'):
            parts.append(str(diploma['education_level']))
        parts.append(f"Диплом {diploma.get('number') or 'ТРЕБУЕТ УТОЧНЕНИЯ: номер'}")
        if diploma.get('date'):
            parts.append(f"выдан {diploma['date']}")
        if diploma.get('institution'):
            parts.append(str(diploma['institution']))
        if diploma.get('speciality'):
            parts.append(str(diploma['speciality']))
        if diploma.get('qualification'):
            parts.append(str(diploma['qualification']))
        lines.append(' '.join(parts))
    return lines or ['ТРЕБУЕТ УТОЧНЕНИЯ: данные диплома']


def _itr_workbook_lines(person: dict) -> list:
    """Не теряет вкладыши и несколько трудовых книжек одного специалиста."""
    values = list(person.get('trudovye_numbers') or [])
    if person.get('trudovaya_number'):
        values.append(person['trudovaya_number'])
    unique, seen = [], set()
    for value in values:
        text = str(value or '').strip()
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            unique.append(text)
    return [f"Трудовые книжки: {'; '.join(unique)}"] if unique else [
        'ТРЕБУЕТ УТОЧНЕНИЯ: номер трудовой книжки'
    ]


def render_spravka_itr(company: dict, people: list, profile: dict = None) -> bytes:
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
    profile = profile or SPK_ACTIVITY_PROFILES['construction']
    xml = _replace_paragraphs_by_marker(xml, [
        ('стаж работы в области строительства', profile['itr_experience_field']),
    ])

    new_rows = []
    for p in people:
        edu = _itr_diploma_lines(p)
        role_key = (p.get('role_key') or '').lower()
        responsibility = (
            profile['director_responsibility'] if role_key == 'директор'
            else ROLE_RESPONSIBILITIES.get(role_key, p.get('responsibility', ''))
        )
        protocol = f"Протокол №{p.get('protocol_number','')} от {p.get('protocol_date','')} г." if p.get('protocol_number') else '—'
        extra = [f"Стаж – {p.get('stage_years','—')}", *_itr_workbook_lines(p)]
        cell_values = [p.get('fio', ''), p.get('position', ''), edu, responsibility, protocol, extra]
        new_rows.append(_build_row(template_row, cell_values))

    xml = _splice_rows(xml, rows[1:], new_rows)

    paras = _paragraphs(xml)
    dir_fio = next((p.get('fio') for p in people if (p.get('role_key') or '').lower() == 'директор'), '')
    xml = _replace_director_signature(xml, paras, _dir_initials(dir_fio))

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 7: Организационная структура СПК (органиграмма) ═══════════════════
def render_orgstruktura(company: dict, director_fio: str, gl_inzhener_fio: str, foremen_fio: list,
                         profile: dict = None) -> bytes:
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
    profile = profile or SPK_ACTIVITY_PROFILES['construction']
    xml = _replace_paragraphs_by_marker(xml, [
        ('Функционирование СПК; организация проведения внутренних аудитов', profile['director_responsibility']),
    ])

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
                               city: str, order_date: str, order_number: str, people: list,
                               profile: dict = None) -> bytes:
    """people: [{fio, position, result}]  # result по умолчанию "Хорошо" """
    parts = _load_parts('8_protokol_obuchenie.docx')
    xml = parts['word/document.xml'].decode('utf-8')
    paras = _paragraphs(xml)
    full_name = f'{company.get("form","ООО")} «{company.get("name","")}»'
    LEGAL_FULL = {'ООО':'Общество с ограниченной ответственностью','ОДО':'Общество с дополнительной ответственностью',
                  'ЧУП':'Частное унитарное предприятие','ЗАО':'Закрытое акционерное общество','ОАО':'Открытое акционерное общество'}
    legal_full_name = f'{LEGAL_FULL.get((company.get("form") or "ООО").upper(), "Общество с ограниченной ответственностью")} «{company.get("name","")}»'
    profile = profile or SPK_ACTIVITY_PROFILES['construction']

    replacements = {0: full_name, 2: f"ПРОТОКОЛ № {protocol_number}", 4: f"{protocol_date} г.", 6: city}
    idx_order_ref = _find_para_index(paras, lambda t: t.startswith('В соответствии с приказом'))
    idx_result = _find_para_index(paras, lambda t: t.startswith('Считать'))
    for i, new_text in replacements.items():
        if i < len(paras) and paras[i] in xml:
            xml = xml.replace(paras[i], _replace_para_text(paras[i], new_text), 1)
    if idx_order_ref >= 0 and paras[idx_order_ref] in xml:
        new_t = (
            f'В соответствии с приказом от {order_date} № {order_number} проведено внутреннее обучение '
            f'по контролю качества {profile["protocol_scope"]} специалистов, задействованных в системе '
            'производственного контроля.'
        )
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
def render_polozhenie(company: dict, director_fio: str, approval_date: str = '', profile: dict = None) -> bytes:
    """Стандартный регламентный документ (337 абзацев), почти без переменных данных —
    только название компании (7 упоминаний) и подпись директора (1). Меняем глобально."""
    parts = _load_parts('9_polozhenie.docx')
    xml = parts['word/document.xml'].decode('utf-8')

    old_company = 'Сфера Секьюрити'
    new_company = company.get('name', '')
    xml = xml.replace(old_company, new_company)
    profile = profile or SPK_ACTIVITY_PROFILES['construction']
    xml = _replace_paragraphs_by_marker(xml, profile.get('polozhenie_replacements') or ())

    dir_init = _dir_initials(director_fio)
    paras = _paragraphs(xml)
    idx_sig = _find_para_index(paras, lambda t: 'Пеганов' in t)
    if idx_sig >= 0 and paras[idx_sig] in xml:
        old_t = re.sub(r'<[^>]+>', '', paras[idx_sig]).strip().replace('\xa0', ' ')
        new_t = re.sub(r'[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.\s*$', dir_init, old_t)
        xml = xml.replace(paras[idx_sig], _replace_para_text(paras[idx_sig], new_t), 1)
    if approval_date:
        xml = xml.replace('12.06.2026', approval_date)

    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)


# ═══════════════════ Документ 10: Паспорт системы производственного контроля ═══════════════════
def render_pasport(company: dict, director_fio: str, director_phone: str, address: str,
                    people: list, cert_number: str = '', cert_date: str = '',
                    approval_date: str = '') -> bytes:
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
    if approval_date:
        xml = xml.replace('12.06.2026', approval_date)

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

    # Номер свидетельства — факт заявителя. Старый номер из Word-образца нельзя
    # переносить в новый пакет, если он не был подтверждён во входных данных.
    paras = _paragraphs(xml)
    idx_cert = _find_para_index(paras, lambda t: 'Свидетельство о технической компетентности' in t)
    if idx_cert >= 0 and paras[idx_cert] in xml:
        cert_text = (
            f"Свидетельство о технической компетентности № {cert_number} от {cert_date} г."
            if cert_number and cert_date
            else 'ТРЕБУЕТ УТОЧНЕНИЯ: свидетельство о технической компетентности'
        )
        xml = xml.replace(paras[idx_cert], _replace_para_text(paras[idx_cert], cert_text), 1)

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
def render_spravka_si(company: dict, director_fio: str, si_list: list, as_of_date: str = '') -> bytes:
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
    if as_of_date:
        xml = xml.replace('12.06.2026', as_of_date)
    parts['word/document.xml'] = xml.encode('utf-8')
    return _rebuild(parts)



def _norm_si_text(value):
    return re.sub(r'[^а-яa-z0-9]+', ' ', str(value or '').lower().replace('ё', 'е')).strip()


_SPK_STANDARD_SI_CHARACTERISTICS = (
    # Базовые значения из утверждённой строки справки СИ. Они не являются
    # данными конкретного клиента и должны оставаться, пока паспорт/поверка
    # не содержит явно другую характеристику.
    ('рейка нивелир', 'Диапазон измерений: (0-5 000) мм'),
    ('рейка контроль', 'Диапазон измерений: (0-3000) мм'),
    ('нивелир', 'Класс точности (погрешность): 2,5 мм/км'),
    ('плотномер', 'Масса гири: 2,49 кг; Высота падения гири: 300 мм; Диаметр основания конуса: 16 мм; Угол при вершине конуса: 60°; Диаметр штампа: 100 см'),
    ('линейк', 'Диапазон измерений: (0-1 000) мм'),
    ('рулетк', 'Диапазон измерений: (0-5 000) мм'),
    ('уров', 'Диапазон измерений: ±90°, ±100%, ±1000 мм/м'),
    ('штангенциркул', 'Диапазон измерений: (0-125) мм'),
    ('угольник повероч', 'Диапазон измерений: 250 × 160 мм'),
    ('термометр', 'Диапазон измерений: (-35 +50) °С'),
    ('теодолит', 'Диапазон измерений: (0-360)°'),
    ('шаблон сварщика', 'Диапазон измерений: 4-14 мм'),
    ('ушс', 'Диапазон измерений: 4-14 мм'),
)

def _standard_si_characteristics(tool: dict) -> str:
    name = _norm_si_text(' '.join(str(tool.get(key) or '') for key in ('name', 'model')))
    for marker, characteristics in _SPK_STANDARD_SI_CHARACTERISTICS:
        if marker in name:
            return characteristics
    return ''


def _build_real_si_list(spk_data: dict) -> tuple[list, list]:
    """Build SPK SI rows only from client measurement/verification/calibration data.

    Returns (rows, warnings). Missing verification never deletes the instrument: the
    row is still created and its verification cell receives an explicit review token
    which the universal DOCX review layer highlights yellow.
    """
    spk_data = spk_data or {}
    tools = [dict(x) for x in (spk_data.get('measurement_tools') or []) if isinstance(x, dict)]
    verifications = [dict(x) for x in (spk_data.get('verification_documents') or []) if isinstance(x, dict)]
    calibrations = [dict(x) for x in (spk_data.get('calibration_documents') or []) if isinstance(x, dict)]
    docs = verifications + calibrations
    warnings = []

    def doc_text(d):
        bits = []
        number = d.get('number') or d.get('certificate_number') or ''
        date = d.get('date') or d.get('verification_date') or d.get('calibration_date') or ''
        valid = d.get('valid_until') or d.get('expiry_date') or ''
        kind = d.get('type') or ('Калибровка' if d in calibrations else 'Поверка')
        if number:
            bits.append(f'{kind} № {number}')
        elif date or valid:
            bits.append(kind)
        if date:
            bits.append(f'от {date}')
        if valid:
            bits.append(f'действует до {valid}')
        return ' '.join(bits).strip()

    def matches(tool, d):
        factory = _norm_si_text(tool.get('factory_number') or tool.get('number'))
        d_factory = _norm_si_text(d.get('factory_number') or d.get('tool_number'))
        # Two explicit, different factory numbers are decisive: matching the
        # generic tool name after that would assign another instrument's
        # calibration to this row.
        if factory and d_factory:
            return factory == d_factory
        tn = _norm_si_text(' '.join(str(tool.get(k) or '') for k in ('name','model')))
        dn = _norm_si_text(' '.join(str(d.get(k) or '') for k in ('tool','name','model')))
        if not tn or not dn:
            return False
        tool_words = tn.split()
        document_words = dn.split()
        # A shared generic word is not sufficient evidence: "нивелир" and
        # "рейка нивелирная" are different SI. Exact names, or a multi-word
        # extension such as "линейка измерительная металлическая", are safe.
        return (
            tn == dn or
            (len(tool_words) > 1 and set(tool_words).issubset(document_words)) or
            (len(document_words) > 1 and set(document_words).issubset(tool_words))
        )

    rows = []
    used_docs = set()
    for tool in tools:
        related = []
        for idx, d in enumerate(docs):
            if matches(tool, d):
                text = doc_text(d)
                if text:
                    related.append(text)
                used_docs.add(idx)
        # A range/characteristic actually read from the submitted passport or
        # verification overrides the standard template. Without such evidence,
        # retain the template value instead of turning a familiar tool yellow.
        source_range_or_characteristics = [
            x for x in [tool.get('range', ''), tool.get('characteristics', '')] if x
        ]
        model = str(tool.get('model') or '').strip()
        standard_characteristics = _standard_si_characteristics(tool)
        # A model identifies the instrument but is not its measurement range.
        # Keep the template range until a passport or verification supplies a
        # specific replacement.
        characteristics = '; '.join(
            x for x in ([model, *source_range_or_characteristics]
                        if source_range_or_characteristics
                        else [model, standard_characteristics]) if x
        )
        verification = '; '.join(dict.fromkeys(related)) or 'ТРЕБУЕТ УТОЧНЕНИЯ: поверка/калибровка'
        if not related:
            warnings.append(f"Для СИ «{tool.get('name') or tool.get('model') or 'без названия'}» не найден документ поверки/калибровки.")
        rows.append({
            'name': tool.get('name') or tool.get('model') or 'ТРЕБУЕТ УТОЧНЕНИЯ: наименование СИ',
            'characteristics': characteristics or 'ТРЕБУЕТ УТОЧНЕНИЯ: характеристики',
            'count': tool.get('quantity') or tool.get('count') or 1,
            'number': tool.get('factory_number') or tool.get('number') or 'ТРЕБУЕТ УТОЧНЕНИЯ: заводской номер',
            'verification': verification,
        })

    # A verification document may be present even when the equipment register was not
    # separately parsed. Keep the evidence instead of losing the entire SI certificate.
    for idx, d in enumerate(docs):
        if idx in used_docs:
            continue
        tool_name = d.get('tool') or d.get('name') or d.get('model')
        if not tool_name:
            continue
        rows.append({
            'name': tool_name,
            'characteristics': _standard_si_characteristics(d) or d.get('model') or 'ТРЕБУЕТ УТОЧНЕНИЯ: характеристики',
            'count': 1,
            'number': d.get('factory_number') or d.get('tool_number') or 'ТРЕБУЕТ УТОЧНЕНИЯ: заводской номер',
            'verification': doc_text(d) or 'ТРЕБУЕТ УТОЧНЕНИЯ: поверка/калибровка',
        })

    if not rows:
        rows = [{
            'name': 'ТРЕБУЕТ УТОЧНЕНИЯ: средства измерений',
            'characteristics': 'ТРЕБУЕТ УТОЧНЕНИЯ',
            'count': 1,
            'number': 'ТРЕБУЕТ УТОЧНЕНИЯ',
            'verification': 'ТРЕБУЕТ УТОЧНЕНИЯ: поверка/калибровка',
        }]
        warnings.append('Не распознан перечень средств измерений для Справки СИ.')
    return rows, list(dict.fromkeys(warnings))


# ═══════════════════ Адаптер для реального пайплайна (generator.py) ═══════════════════
def _find_person(itr, *keywords):
    for p in itr:
        pos = (p.get('position') or '').lower()
        if any(k in pos for k in keywords):
            return p
    return None


def generate_spk_package_v2(company: dict, itr: list, workers: list, dates: dict, resp: dict,
                             variant: str = 'spk_stroy', progress_cb=None, spk_data: dict = None) -> dict:
    """
    company: {name, form, unp, address, city, director_fio, director_position, phone, bisp_org}
    itr: список [{fio, position}] — сотрудники (используем чтобы найти гл.инженера/прорабов)
    dates: результат calculate_dates() из generator.py (goals, year и т.д.)
    resp: результат select_responsible(itr) из generator.py
    variant: 'spk_stroy' | 'spk_bisp'
    """
    org = company.get('name', 'company')
    _profile_key, profile = _spk_activity_profile(spk_data)
    director_fio = company.get('director_fio', '') or (resp.get('director') or {}).get('fio', '')
    gl_person = _find_person(itr, 'главный инженер', 'гл. инженер')
    gl_inzhener_fio = (gl_person or {}).get('fio', '') if gl_person != resp.get('director') else ''
    foremen = [p.get('fio', '') for p in itr
               if any(k in (p.get('position') or '').lower() for k in ('прораб', 'производитель работ'))
               and p.get('fio') != director_fio]
    if not foremen:
        alt = _find_person(itr, 'мастер')
        if alt and alt.get('fio') != director_fio:
            foremen = [alt.get('fio', '')]

    responsible_person = next((p for p in itr if p.get('fio') in foremen), None)
    if not responsible_person:
        responsible_person = gl_person or next((p for p in itr if p.get('fio') == director_fio), None) or {}
    responsible_fio = responsible_person.get('fio') or director_fio
    responsible_position = responsible_person.get('position') or company.get('director_position', 'Директор')

    def _person_copy(source, role_key, fallback_fio='', fallback_position=''):
        item = dict(source or {})
        item['fio'] = item.get('fio') or fallback_fio
        item['position'] = item.get('position') or fallback_position
        item['role_key'] = role_key
        # Compatibility aliases used by the SPK Word template.
        diplomas = item.get('diplomas') or []
        if diplomas and isinstance(diplomas[0], dict):
            d = diplomas[0]
            item.setdefault('diploma_number', d.get('number', ''))
            item.setdefault('diploma_date', d.get('date', ''))
            item.setdefault('diploma_institution', d.get('institution', ''))
            item.setdefault('diploma_speciality', d.get('speciality', ''))
            item.setdefault('diploma_qualification', d.get('qualification', ''))
        item.setdefault('trudovaya_number', (item.get('trudovye_numbers') or [''])[0] if item.get('trudovye_numbers') else '')
        return item

    all_people = []
    seen_people = set()
    director_person = next((p for p in itr if p.get('fio') == director_fio), None)
    for source, role_key, fallback_fio, fallback_pos in [
        (director_person, 'директор', director_fio, company.get('director_position', 'Директор')),
        (gl_person, 'главный инженер', gl_inzhener_fio, 'Главный инженер'),
    ]:
        person = _person_copy(source, role_key, fallback_fio, fallback_pos)
        fio_key = (person.get('fio') or '').strip().lower()
        if fio_key and fio_key not in seen_people:
            seen_people.add(fio_key)
            all_people.append(person)
    for f in foremen:
        fp = next((p for p in itr if p.get('fio') == f), {})
        role_key = 'мастер' if 'мастер' in (fp.get('position') or '').lower() else 'производитель работ'
        person = _person_copy(fp, role_key, f, 'Производитель работ')
        fio_key = (person.get('fio') or '').strip().lower()
        if fio_key and fio_key not in seen_people:
            seen_people.add(fio_key)
            all_people.append(person)

    order_date = dates.get('goals', '')
    policy_date = dates.get('policy', order_date)
    report_date = dates.get('reports', order_date)
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
        render_prikaz_spk(company, '1/СПК', order_date, city, director_fio, gl_inzhener_fio, foremen,
                          profile, responsible_position))

    p("3. Приказ о внутреннем обучении")
    add(f"{org} СПК - 4.2.1 Приказ о внутреннем обучении.docx",
        render_prikaz_obuchenie(company, '2/СПК', order_date, city, director_fio, order_date, profile))

    p("4. Приказ о ТО средств измерений")
    add(f"{org} СПК - 4.3 Приказ о ТО СИ.docx",
        render_prikaz_to_si(company, '3/СПК', order_date, city, director_fio, responsible_fio, responsible_position))

    p("5. Приказ о назначении ответственного за машины")
    add(f"{org} СПК - 4.4 Приказ о машинах.docx",
        render_prikaz_mashiny(company, '4/СПК', order_date, city, director_fio, responsible_fio, responsible_position))

    p("6. Справка ИТР")
    people_itr = []
    for pp in all_people:
        item = dict(pp)
        # Protocol details are personal facts. Never invent "№1" and the package date.
        # Use only a real OT/training protocol extracted from the employee documents.
        if not item.get('protocol_number'):
            item['protocol_number'] = item.get('ot_protocol_number') or item.get('training_protocol_number') or ''
        if not item.get('protocol_date'):
            item['protocol_date'] = item.get('ot_protocol_date') or item.get('training_protocol_date') or ''
        people_itr.append(item)
    add(f"{org} СПК - 2 Справка ИТР.docx", render_spravka_itr(company, people_itr, profile))

    p("7. Организационная структура")
    add(f"{org} СПК - 3 Организационная структура.docx",
        render_orgstruktura(company, director_fio, gl_inzhener_fio, foremen, profile))

    p("8. Протокол о внутреннем обучении")
    add(f"{org} СПК - 4.2.2 Протокол обучения.docx",
        render_protokol_obuchenie(company, '1', order_date, city, order_date, '2/СПК', all_people, profile))

    p("9. Положение о СПК")
    add(f"{org} СПК - 5 Положение о СПК.docx", render_polozhenie(company, director_fio, policy_date, profile))

    p("10. Паспорт СПК")
    competence = (spk_data or {}).get('technical_competence') or {}
    cert_number = competence.get('number') or competence.get('certificate_number') or ''
    cert_date = competence.get('date') or competence.get('certificate_date') or ''
    add(f"{org} СПК - 6 Паспорт СПК.docx",
        render_pasport(company, director_fio, company.get('phone', ''), company.get('address', ''), all_people,
                       cert_number, cert_date, order_date))

    p("11. Справка ТТК")
    real_ttk = [dict(x) for x in ((spk_data or {}).get('ttk') or []) if isinstance(x, dict)]
    if real_ttk:
        ttk_list = [{
            'code': x.get('code') or 'ТРЕБУЕТ УТОЧНЕНИЯ: шифр ТТК',
            'name': x.get('name') or x.get('work_type') or 'ТРЕБУЕТ УТОЧНЕНИЯ: наименование ТТК',
            'organization': x.get('developer') or x.get('organization') or 'ТРЕБУЕТ УТОЧНЕНИЯ: разработчик',
            'validity': x.get('valid_until') or x.get('validity') or 'ТРЕБУЕТ УТОЧНЕНИЯ: срок действия',
        } for x in real_ttk]
    else:
        ttk_list = [{
            'code': 'ТРЕБУЕТ УТОЧНЕНИЯ: шифр ТТК',
            'name': 'ТРЕБУЕТ УТОЧНЕНИЯ: ТТК',
            'organization': 'ТРЕБУЕТ УТОЧНЕНИЯ: разработчик',
            'validity': 'ТРЕБУЕТ УТОЧНЕНИЯ: срок действия',
        }]
    add(f"{org} СПК - 7 Справка ТТК.docx", render_spravka_ttk(company, director_fio, ttk_list))

    p("12. Справка СИ")
    si_list, si_warnings = _build_real_si_list(spk_data or {})
    add(f"{org} СПК - 8 Справка СИ.docx", render_spravka_si(company, director_fio, si_list, report_date))

    warnings = list(si_warnings)

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
                render_plan_audita(company, director_fio, year, approval_date=order_date))

            p("17. Положение о входном контроле")
            add(f"{org} СПК БИСП - 5.2 Положение о входном контроле.docx",
                render_polozhenie_vhod(company, director_fio, policy_date))

            p("18. График поверки СИ")
            add(f"{org} СПК БИСП - График поверки СИ.docx",
                render_grafik_poverki(company, director_fio, year, order_date, si_list))

            p("19. Перечень продукции входного контроля")
            add(f"{org} СПК БИСП - Перечень продукции входного контроля.docx",
                render_perechen_produkcii(company, director_fio, order_date))
        except Exception as e:
            message = f"Часть документов БИСП не сформирована: {type(e).__name__}: {e}"
            warnings.append(message)
            print(f"  ❌ {message}")

    if not gl_inzhener_fio:
        warnings.append("Не найден главный инженер в штате — в документах СПК это поле осталось пустым.")
    if not any(foremen):
        warnings.append("Не найден прораб/производитель работ в штате — использован главный инженер как ответственный по умолчанию.")

    return {'docs': docs, 'warnings': warnings}
