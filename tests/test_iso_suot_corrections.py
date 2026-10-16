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
from generator_iso_suot_templates import generate_iso_suot_package_v2


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


def test_technical_correction_reply_does_not_tell_user_to_regenerate():
    text = ('Правка не применилась: backend diagnostics ' * 60)

    visible = server._humanize_user_visible_text(text)

    assert 'сформируйте пакет заново' not in visible.lower()
