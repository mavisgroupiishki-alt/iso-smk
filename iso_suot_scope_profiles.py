"""Deterministic ISO/SUOT activity profiles and source provenance.

Profiles are deliberately data, not model prompts.  A profile changes only after
an approved human reference has been reviewed.  An unrecognised scope can still
be rendered as an AI draft, but it is never reported as an approved profile.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import re
import unicodedata


PROFILE_REGISTRY_VERSION = '2026.10.14.1'
OFFICIAL_SOURCES_CHECKED_ON = '2026-10-14'


# This is a registry of *where a legal reference may be verified*.  It is not a
# list of conclusions about applicability for a particular employer: that still
# depends on its actual works, equipment and workplaces.
OFFICIAL_SOURCE_REGISTRY = (
    {
        'id': 'by-pravo',
        'title': 'Национальный правовой Интернет-портал Республики Беларусь',
        'url': 'https://pravo.by/',
        'authority': 'Национальный центр законодательства и правовой информации Республики Беларусь',
        'coverage': 'нормативные правовые акты Республики Беларусь',
    },
    {
        'id': 'by-tnpa',
        'title': 'Национальный фонд технических нормативных правовых актов Республики Беларусь',
        'url': 'https://tnpa.by/',
        'authority': 'Госстандарт Республики Беларусь',
        'coverage': 'технические нормативные правовые акты и сведения об их статусе',
    },
    {
        'id': 'eec-tech',
        'title': 'Евразийская экономическая комиссия — техническое регулирование',
        'url': 'https://eec.eaeunion.org/comission/department/deptexreg/',
        'authority': 'Евразийская экономическая комиссия',
        'coverage': 'технические регламенты и решения Евразийского экономического союза',
    },
)


def _profile(id_, title, patterns, *, process_name, process_steps, hazards,
             controls, instructions, audit_focus, sample):
    return {
        'id': id_,
        'version': PROFILE_REGISTRY_VERSION,
        'status': 'approved_reference',
        'title': title,
        'patterns': tuple(patterns),
        'process_name': process_name,
        'process_steps': tuple(process_steps),
        'hazards': tuple(hazards),
        'controls': tuple(controls),
        'instructions': tuple(instructions),
        'audit_focus': audit_focus,
        'sample': sample,
        'official_source_ids': ('by-pravo', 'by-tnpa', 'eec-tech'),
    }


_PROFILES = (
    _profile(
        'construction_design',
        'СМР и разработка разделов проектной документации',
        ('(?:строительно-монтаж|смр)', 'проектн.*документ'),
        process_name='Выполнение строительно-монтажных работ и разработка разделов проектной документации',
        process_steps=(
            'анализ исходных данных, договора и требований заказчика',
            'разработка и согласование проектной и сметной документации',
            'планирование, выполнение и контроль строительно-монтажных работ',
            'входной контроль материалов и оформление исполнительной документации',
        ),
        hazards=(
            'падение с высоты и падение предметов',
            'работа с электроинструментом и электрооборудованием',
            'перемещение грузов, монтаж конструкций и работа в зоне техники',
        ),
        controls=(
            'до начала работ проверять ограждения, средства защиты от падения и безопасные зоны',
            'применять исправный электроинструмент и предусмотренные средства индивидуальной защиты',
            'выполнять монтаж и перемещение грузов по технологической последовательности и установленным зонам',
        ),
        instructions=(
            'Инструкция по охране труда при выполнении работ на высоте',
            'Инструкция по охране труда при работе с электроинструментом',
            'Инструкция по охране труда при погрузочно-разгрузочных работах',
        ),
        audit_focus='управление проектной документацией, входной контроль, выполнение работ и исполнительная документация',
        sample='проверенный человеческий пакет «АмперСистем»',
    ),
    _profile(
        'construction_general',
        'Строительно-монтажные и ремонтно-отделочные работы',
        ('(?:строительно-монтаж|смр|текущий ремонт|отделочн)',),
        process_name='Выполнение строительно-монтажных и ремонтно-отделочных работ',
        process_steps=(
            'планирование работ, анализ задания и требований объекта',
            'входной контроль материалов и подготовка рабочего места',
            'выполнение работ с операционным контролем качества',
            'оформление исполнительных записей и сдача результата работ',
        ),
        hazards=(
            'падение, скольжение и падение предметов',
            'работа с ручным и электроинструментом',
            'перемещение материалов и работа в зоне производства работ',
        ),
        controls=(
            'содержать рабочее место, проходы и зоны складирования в безопасном состоянии',
            'применять исправный инструмент и предусмотренные средства индивидуальной защиты',
            'до начала работ проверить ограждения, освещение и безопасные зоны',
        ),
        instructions=(
            'Инструкция по охране труда при работе с электроинструментом',
            'Инструкция по охране труда при погрузочно-разгрузочных работах',
        ),
        audit_focus='входной контроль, организация работ, операционный контроль качества и исполнительные записи',
        sample='базовый проверенный строительный профиль',
    ),
    _profile(
        'geodesy_surveys',
        'Геодезические работы и инженерно-геодезические изыскания',
        ('геодез', 'изыскан'),
        process_name='Выполнение инженерно-геодезических изысканий и геодезических работ',
        process_steps=(
            'получение задания, исходных данных и согласование программы работ',
            'полевые измерения с применением поверенных средств измерений',
            'камеральная обработка, контроль результатов и оформление материалов изысканий',
            'передача заказчику результатов и ведение записей по средствам измерений',
        ),
        hazards=(
            'работа на местности и вблизи транспорта',
            'неблагоприятные погодные условия и неровный рельеф',
            'применение средств измерений и перенос оборудования',
        ),
        controls=(
            'до выезда согласовывать маршрут, безопасные зоны и порядок связи',
            'контролировать состояние площадки, погодные условия и видимость',
            'использовать исправные средства измерений с актуальными сведениями о поверке',
        ),
        instructions=(
            'Инструкция по охране труда при выполнении геодезических работ',
            'Инструкция по охране труда для инженера-геодезиста',
            'Инструкция по охране труда при работе с персональным компьютером',
        ),
        audit_focus='управление заданиями на изыскания, прослеживаемость измерений, поверка средств измерений и контроль камеральной обработки',
        sample='проверенный человеческий пакет «АР идея»',
    ),
    _profile(
        'fire_systems',
        'Монтаж, наладка и обслуживание систем пожарной безопасности',
        ('(?:пожарн|оповещ|огнезащ|соуэ|противодымн)',),
        process_name='Монтаж, наладка и техническое обслуживание систем пожарной сигнализации, СОУЭ и огнезащиты',
        process_steps=(
            'анализ проектной документации и требований объекта',
            'монтаж оборудования и кабельных линий с входным контролем материалов',
            'наладка, проверка работоспособности и документирование результатов',
            'техническое обслуживание систем и оформление актов выполненных работ',
        ),
        hazards=(
            'работы на высоте и в стесненных условиях',
            'электрическое воздействие при монтаже и наладке',
            'работа с огнезащитными составами и средствами нанесения',
        ),
        controls=(
            'организовывать безопасный доступ к месту работ и применять защиту от падения',
            'до начала работ проверять отсутствие напряжения и исправность инструмента',
            'применять СИЗ органов дыхания, зрения и кожи при работах с огнезащитными составами',
        ),
        instructions=(
            'Инструкция по охране труда при монтаже и наладке систем пожарной сигнализации',
            'Инструкция по охране труда для работ с огнезащитными составами',
            'Инструкция по охране труда при работе на высоте',
        ),
        audit_focus='контроль проектной документации, монтаж и наладка систем, записи о проверке работоспособности и техническом обслуживании',
        sample='проверенный человеческий пакет «Беларм-плюс»',
    ),
)


def _normalise(value: str) -> str:
    text = unicodedata.normalize('NFKD', str(value or '')).lower().replace('ё', 'е')
    return re.sub(r'\s+', ' ', text).strip()


def known_scope_profile(scope: str) -> dict | None:
    """Return an approved profile only when all of its required signals match."""
    text = _normalise(scope)
    if not text:
        return None
    for profile in _PROFILES:
        if all(re.search(pattern, text, flags=re.I) for pattern in profile['patterns']):
            return deepcopy(profile)
    return None


def resolve_scope_profile(scope: str, requested_mode: str = '') -> dict:
    """Resolve approved scope or explicitly labelled AI draft.

    ``requested_mode`` is intentionally opt-in.  A blank/unknown scope does not
    silently turn into an approved construction profile.
    """
    profile = known_scope_profile(scope)
    if profile:
        profile['requested_mode'] = 'approved_reference'
        return profile
    mode = str(requested_mode or '').strip().lower()
    return {
        'id': 'ai_draft',
        'version': PROFILE_REGISTRY_VERSION,
        'status': 'ai_draft' if mode == 'ai_draft' else 'needs_profile_choice',
        'requested_mode': mode,
        'title': 'ИИ-черновик профиля области',
        'process_name': str(scope or 'Область работ требует уточнения').strip(),
        'process_steps': (
            'уточнить состав выполняемых работ, оборудование, материалы и условия на объектах',
            'сопоставить процессы, опасности и инструкции с фактическими рабочими местами',
        ),
        'hazards': ('опасности определяются по фактическим рабочим местам',),
        'controls': ('до утверждения профиля проверить применимость мер управления рисками и инструкций по ОТ',),
        'instructions': ('Перечень профильных инструкций требует предметной проверки',),
        'audit_focus': 'профильные критерии аудита требуют ручной проверки до утверждения',
        'sample': '',
        'official_source_ids': ('by-pravo', 'by-tnpa', 'eec-tech'),
    }


def source_rows(profile: dict) -> list[dict]:
    ids = set(profile.get('official_source_ids') or ())
    return [dict(item, checked_on=OFFICIAL_SOURCES_CHECKED_ON)
            for item in OFFICIAL_SOURCE_REGISTRY if item['id'] in ids]


def provenance(scope: str, profile: dict) -> dict:
    """Machine-readable package report.  It intentionally avoids legal claims."""
    is_draft = profile.get('status') == 'ai_draft'
    return {
        'format': 'igor.iso_suot.profile_provenance.v1',
        'created_on': date.today().isoformat(),
        'scope': str(scope or ''),
        'profile': {
            'id': profile.get('id'),
            'title': profile.get('title'),
            'version': profile.get('version'),
            'status': profile.get('status'),
            'reference_sample': profile.get('sample') or None,
        },
        'official_source_registry': source_rows(profile),
        'unverified_assumptions': (
            ['Профиль сформирован как ИИ-черновик. До применения требуется проверить состав работ, риски, инструкции и применимые НПА/ТНПА.']
            if is_draft else []
        ),
        'manual_review_required': (
            ['профильные нормативные ссылки', 'применимость инструкций по ОТ', 'оценка рисков по рабочим местам']
            if is_draft else ['применимость НПА/ТНПА к фактическим работам и объектам']
        ),
    }
