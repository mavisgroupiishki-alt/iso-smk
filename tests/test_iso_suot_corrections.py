import io
import sys
import zipfile
from pathlib import Path

from docx import Document

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import generator
import server
from generator_iso_suot_templates import _order_identity, generate_iso_suot_package_v2


def _xml_text(doc_bytes):
    with zipfile.ZipFile(io.BytesIO(doc_bytes)) as archive:
        return '\n'.join(
            archive.read(name).decode('utf-8', 'ignore')
            for name in archive.namelist()
            if name.startswith('word/') and name.endswith('.xml')
        )


def _base_data():
    return {
        'company': {
            'name': '3Д Климат', 'form': 'ООО', 'unp': '123456789',
            'director_fio': 'Гацко Александр Дмитриевич',
            'director_position': 'Директор',
            'scope': 'Производство строительно-монтажных работ',
        },
        'staff': [
            {'fio': 'Гацко Александр Дмитриевич', 'position': 'Директор', 'is_worker': False},
            {'fio': 'Смирнов Дмитрий Петрович', 'position': 'Заместитель директора', 'is_worker': False},
            {'fio': 'Телепнев Олег Анатольевич', 'position': 'Производитель работ', 'is_worker': False},
        ],
    }


def test_iso_suot_archive_transfers_scope_and_customer_report_rows():
    source = '''--- ИСО 9001/1 Приказы/Приказ 3-СМК.docx ---
В ОБЛАСТИ: Производство строительно-монтажных работ

--- ИСО 9001/3 СМК и карточки/Отчет по оценке удовлетворенности заказчиков.docx ---
Объект / заказчик Количество рекламаций Бальная оценка Прокладка воздуховодов и монтаж оборудования в аг. Лесной — 5
'''

    data = server._iso_suot_archive_card_data(source)

    assert data['company']['scope'] == 'Производство строительно-монтажных работ'
    assert data['certification']['scope'] == 'Производство строительно-монтажных работ'
    assert data['objects'] == [{'name': 'Прокладка воздуховодов и монтаж оборудования в аг. Лесной'}]


def test_archive_extracts_reference_roles_from_human_job_descriptions():
    source = '''--- инструкции и журналы/должностные инструкции/1 ДИ Директор.docx ---
текст
--- инструкции и журналы/должностные инструкции/2 ДИ ведущий_Инженер.docx ---
текст
--- инструкции и журналы/должностные инструкции/3 ДИ Бухгалтер.docx ---
текст
--- инструкции и журналы/должностные инструкции/5 ДИ юрисконсульту.docx ---
текст
--- инструкции и журналы/должностные инструкции/6 ДИ Мастер.rtf ---
текст
--- инструкции и журналы/должностные инструкции/7 ДИ агент по снабжению (3).docx ---
текст
--- инструкции и журналы/должностные инструкции/8 ДИ ведущий инженер ПТО.docx ---
текст
--- инструкции и журналы/должностные инструкции/9 ДИ производитель работ.rtf ---
текст
'''

    roles = server._iso_suot_archive_card_data(source)['iso_suot']['reference_positions']

    assert [row['position'] for row in roles] == [
        'Директор', 'Ведущий инженер', 'Бухгалтер', 'Юрисконсульт', 'Мастер',
        'Агент по снабжению', 'Ведущий инженер ПТО', 'Производитель работ (прораб)',
    ]


def test_iso_reference_roles_rebuild_di_register_audits_and_source_information():
    data = _base_data()
    data['staff'].append({'fio': 'Петрова Любовь Сергеевна', 'position': 'Менеджер', 'is_worker': False})
    itr = data['staff']
    result = generate_iso_suot_package_v2(
        data['company'], itr, generator.calculate_dates('20.10.2026'),
        generator.select_responsible(itr), product='iso',
        iso_suot={'reference_positions': [
            {'position': 'Бухгалтер'}, {'position': 'Юрисконсульт'}, {'position': 'Мастер'},
            {'position': 'Агент по снабжению'}, {'position': 'Ведущий инженер ПТО'},
            {'position': 'Производитель работ (прораб)'},
        ]},
    )
    names = [doc['name'] for doc in result['docs']]
    for position in ('Директор', 'Заместитель директора', 'Производитель работ', 'Менеджер',
                     'Бухгалтер', 'Юрисконсульт', 'Мастер', 'Агент по снабжению', 'Ведущий инженер ПТО'):
        assert any(f'ДИ {position}' in name for name in names), position
    assert any('Исходная информация СМК' in name for name in names)

    journal = next(doc for doc in result['docs'] if 'Журнал учета должност' in doc['name'])
    journal_doc = Document(io.BytesIO(journal['bytes']))
    journal_text = '\n'.join(cell.text for table in journal_doc.tables for row in table.rows for cell in row.cells)
    assert 'Главному специалисту' not in journal_text
    assert 'агенту по снабжению' in journal_text.casefold()
    assert 'ведущему инженеру пто' in journal_text.casefold()
    assert 'производителю работ' in journal_text.casefold()
    assert 'ДИ-09' in journal_text

    audit = next(doc for doc in result['docs'] if 'Программа внутренних аудитов СМК' in doc['name'])
    audit_doc = Document(io.BytesIO(audit['bytes']))
    audit_text = '\n'.join(cell.text for table in audit_doc.tables for row in table.rows for cell in row.cells)
    assert 'Юрисконсульт' in audit_text
    assert 'Ведущий инженер ПТО' in audit_text
    assert 'п.п. 4.1, 4.2, 7.2, 7.5, 10.3' in audit_text  # менеджер
    # The programme is a plan, not fabricated evidence of completed audits.
    assert all(not row.cells[0].text.strip() for row in audit_doc.tables[1].rows[3:])


def test_iso_audit_evidence_preserves_human_schedule_grouping_and_journal_dates():
    data = _base_data()
    itr = data['staff'] + [
        {'fio': 'Иванова Анна Сергеевна', 'position': 'Бухгалтер', 'is_worker': False},
        {'fio': 'Петров Олег Павлович', 'position': 'Мастер', 'is_worker': False},
        {'fio': 'Сидоров Роман Викторович', 'position': 'Производитель работ (прораб)', 'is_worker': False},
    ]
    result = generate_iso_suot_package_v2(
        data['company'], itr, generator.calculate_dates('20.10.2026'),
        generator.select_responsible(itr), product='iso',
        iso_suot={'audit_evidence': {
            'approval_date': '05.03.2026',
            'rows': [
                {'position': 'Директор', 'criteria': 'п.п. 4.1–4.4', 'month': 6},
                {'position': 'Производитель работ (прораб) / Мастер', 'criteria': 'п.п. 8.1', 'month': 11},
            ],
            'journal': [
                {'position': 'Директор', 'criteria': 'п.п. 4.1–4.4', 'date': '14.07.2026',
                 'auditor': 'Ведущий инженер — Смирнов Дмитрий Петрович'},
                {'position': 'Производитель работ (прораб) / Мастер', 'criteria': 'п.п. 8.1'},
            ],
        }},
    )
    audit = next(doc for doc in result['docs'] if 'Программа внутренних аудитов СМК' in doc['name'])
    audit_doc = Document(io.BytesIO(audit['bytes']))
    schedule, journal = audit_doc.tables[:2]
    schedule_rows = [row.cells for row in schedule.rows[3:]]
    assert len(schedule_rows) == 2
    assert schedule_rows[1][1].text == 'Производитель работ (прораб) / Мастер'
    assert schedule_rows[0][3 + 6].text == 'Х'  # July from the human programme
    assert schedule_rows[1][3 + 11].text == 'Х'  # December from the human programme
    assert journal.rows[3].cells[0].text == '14.07.2026'
    assert journal.rows[3].cells[7].text == 'Ведущий инженер — Смирнов Дмитрий Петрович'
    assert journal.rows[4].cells[0].text == ''
    paragraphs = '\n'.join(paragraph.text for paragraph in audit_doc.paragraphs)
    assert '05.03.2026' in paragraphs


def test_policy_evidence_is_preserved_for_initial_package_and_awareness_sheet():
    data = _base_data()
    result = generate_iso_suot_package_v2(
        data['company'], data['staff'], generator.calculate_dates('20.10.2026'),
        generator.select_responsible(data['staff']), product='iso',
        iso_suot={'policy_evidence': {
            'approval_date': '03.03.2026',
            'familiarizations': [
                {'fio': 'Гацко Александр Дмитриевич', 'date': '03.03.2026'},
                {'fio': 'Шумович Елена Александровна', 'date': '24.04.2026'},
            ],
        }},
    )
    policy = next(doc for doc in result['docs'] if 'Политика в области качества' in doc['name'])
    policy_text = _xml_text(policy['bytes'])
    assert '03.03.2026' in policy_text
    assert 'Шумович Елена Александровна' in policy_text
    assert '24.04.2026' in policy_text
    awareness = next(doc for doc in result['docs'] if 'Лист ознакомления с целями' in doc['name'])
    assert 'Шумович Елена Александровна' in _xml_text(awareness['bytes'])


def test_explicit_commission_replaces_automatic_people_in_orders():
    data = _base_data()
    itr = data['staff']
    dates = generator.calculate_dates('20.10.2026')
    result = generate_iso_suot_package_v2(
        data['company'], itr, dates, generator.select_responsible(itr), product='iso_suot',
        iso_suot={'commission': [
            {'fio': 'Гацко Александр Дмитриевич', 'position': 'Директор', 'role': 'chair'},
            {'fio': 'Ковалев Сергей Петрович', 'position': 'Инженер по охране труда', 'role': 'member'},
        ]},
    )

    order = next(doc for doc in result['docs'] if 'Приказ 4-OH&S' in doc['name'])
    text = _xml_text(order['bytes'])
    assert 'Ковалев Сергей Петрович' in text
    assert 'Инженер по охране труда' in text
    assert 'члены комиссии — Инженер по охране труда — Ковалев Сергей Петрович' in text
    assert 'члены комиссии — Заместитель директора — Смирнов Дмитрий Петрович' not in text


def test_initial_iso_order_is_not_replaced_by_order_three():
    """The section prefix ``3.1`` is not the number of the ISO start order."""
    data = _base_data()
    itr = data['staff']
    result = generate_iso_suot_package_v2(
        data['company'], itr, generator.calculate_dates('20.10.2026'),
        generator.select_responsible(itr), product='iso_suot',
    )

    names = [doc['name'] for doc in result['docs']]
    start_order = next(doc for doc in result['docs'] if '3.1 Приказ 1 о начале разработки СМК' in doc['name'])
    suot_order = next(doc for doc in result['docs'] if '1 Приказ о разработке OH&S' in doc['name'])
    text = '\n'.join(paragraph.text for paragraph in Document(io.BytesIO(start_order['bytes'])).paragraphs)
    suot_text = '\n'.join(paragraph.text for paragraph in Document(io.BytesIO(suot_order['bytes'])).paragraphs)

    assert _order_identity(start_order['name']) == 'iso:1'
    assert _order_identity('ООО «Тест» - Приказ 3-СМК.docx') == 'iso:3'
    assert _order_identity('ООО «Тест» СУОТ - Приказ 10.1-OH&S.docx') == 'suot:10.1'
    assert any('Приказ 3-СМК' in name for name in names)
    assert 'О разработке системы менеджмента качества' in text
    assert 'Разработать документацию системы менеджмента качества' in text
    assert 'Назначить директора' in text
    assert 'Варта' not in text
    assert 'О разработке системы менеджмента здоровья и безопасности' in suot_text
    assert 'Разработать документацию системы менеджмента здоровья и безопасности' in suot_text
    assert '4.1. в срок до' in suot_text
    assert 'Варта' not in suot_text


def test_technical_correction_reply_does_not_tell_user_to_regenerate():
    text = ('Правка не применилась: backend diagnostics ' * 60)

    visible = server._humanize_user_visible_text(text)

    assert 'сформируйте пакет заново' not in visible.lower()
