"""Regression checks for the approved human SPK Word forms.

Content is deliberately dynamic, but page geometry and the shared Word design
must remain identical to the source template.  A text-only test cannot detect a
replacement that silently changes margins, table widths or fonts.
"""
import io
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from docx import Document
import generator
from generator_spk_templates import (
    _ACCEPTANCE_CONTROL_LEGAL_TEXT,
    generate_spk_package_v2,
)


ROOT = Path(__file__).resolve().parents[1]
WORD_NS = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
STATIC_PARTS = (
    'word/styles.xml', 'word/fontTable.xml', 'word/settings.xml',
    'word/webSettings.xml', 'word/theme/theme1.xml',
)
TEMPLATE_BY_OUTPUT = {
    'СПК - 1 Условия в производственных помещениях.docx': ROOT / 'spk_templates/1_usloviya.docx',
    'СПК - 4.1 Приказ о СПК.docx': ROOT / 'spk_templates/2_prikaz_spk.docx',
    'СПК - 4.2.1 Приказ о внутреннем обучении.docx': ROOT / 'spk_templates/3_prikaz_obuchenie.docx',
    'СПК - 4.3 Приказ о ТО СИ.docx': ROOT / 'spk_templates/4_prikaz_to_si.docx',
    'СПК - 4.4 Приказ о машинах.docx': ROOT / 'spk_templates/5_prikaz_mashiny.docx',
    'СПК - 2 Справка ИТР.docx': ROOT / 'spk_templates/6_spravka_itr.docx',
    'СПК - 3 Организационная структура.docx': ROOT / 'spk_templates/7_orgstruktura.docx',
    'СПК - 4.2.2 Протокол обучения.docx': ROOT / 'spk_templates/8_protokol_obuchenie.docx',
    'СПК - 5 Положение о системе производственного контроля.docx': ROOT / 'spk_templates/9_polozhenie.docx',
    'СПК - 6 Паспорт СПК.docx': ROOT / 'spk_templates/10_pasport.docx',
    'СПК - 7 Справка ТТК.docx': ROOT / 'spk_templates/11_spravka_ttk.docx',
    'СПК - 8 Справка СИ.docx': ROOT / 'spk_templates/12_spravka_si.docx',
    'СПК БИСП - 9.1 Гарантийное письмо на ТТК.docx': ROOT / 'bisp_templates/1_garantiya_ttk.docx',
    'СПК БИСП - 9.3 Гарантийное письмо по лабораториям.docx': ROOT / 'bisp_templates/2_garantiya_labs.docx',
    'СПК БИСП - 9.6 Гарантийное письмо об отсутствии рекламаций.docx': ROOT / 'bisp_templates/3_garantiya_reklamacii.docx',
    'СПК БИСП - План внутреннего аудита.docx': ROOT / 'bisp_templates/4_plan_audita.docx',
    'СПК БИСП - 5.2 Положение о входном контроле.docx': ROOT / 'bisp_templates/5_polozhenie_vhod.docx',
    'СПК БИСП - График поверки СИ.docx': ROOT / 'bisp_templates/6_grafik_poverki.docx',
    'СПК БИСП - Перечень продукции входного контроля.docx': ROOT / 'bisp_templates/7_perechen_produkcii.docx',
}


def _parts(docx_bytes):
    with zipfile.ZipFile(io.BytesIO(docx_bytes)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def _layout_signature(document_xml):
    root = ET.fromstring(document_xml)
    sections = [ET.tostring(node, encoding='unicode') for node in root.findall('.//w:sectPr', WORD_NS)]
    tables = []
    for table in root.findall('.//w:tbl', WORD_NS):
        properties = table.find('w:tblPr', WORD_NS)
        grid = table.find('w:tblGrid', WORD_NS)
        tables.append((
            ET.tostring(properties, encoding='unicode') if properties is not None else '',
            ET.tostring(grid, encoding='unicode') if grid is not None else '',
        ))
    return sections, tables


def _source_parts(path):
    with zipfile.ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def test_spk_bisp_keeps_every_approved_template_layout():
    company = {
        'name': 'Тестовая организация', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор',
    }
    people = [
        {'fio': 'Иванов Иван Иванович', 'position': 'Директор'},
        {'fio': 'Петров Петр Петрович', 'position': 'Главный инженер'},
        {'fio': 'Сидоров Сидор Сидорович', 'position': 'Производитель работ'},
    ]
    package = generate_spk_package_v2(
        company, people, [], generator.calculate_dates('17.09.2026'),
        generator.select_responsible(people), variant='spk_bisp',
    )

    for generated in package['docs']:
        suffix = next((key for key in TEMPLATE_BY_OUTPUT if generated['name'].endswith(key)), None)
        assert suffix, generated['name']
        source = _source_parts(TEMPLATE_BY_OUTPUT[suffix])
        output = _parts(generated['bytes'])

        for name in STATIC_PARTS:
            if name in source:
                assert output.get(name) == source[name], f'{generated["name"]}: {name}'
        for name, source_bytes in source.items():
            if name.startswith(('word/header', 'word/footer')):
                assert output.get(name) == source_bytes, f'{generated["name"]}: {name}'
        assert _layout_signature(output['word/document.xml']) == _layout_signature(source['word/document.xml'])


def test_bisp_audit_plan_keeps_audit_lead_in_the_template_cell_paragraph():
    company = {
        'name': 'Тестовая организация', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор',
    }
    people = [
        {'fio': 'Иванов Иван Иванович', 'position': 'Директор'},
        {'fio': 'Петров Петр Петрович', 'position': 'Главный инженер'},
    ]
    package = generate_spk_package_v2(
        company, people, [], generator.calculate_dates('17.09.2026'),
        generator.select_responsible(people), variant='spk_bisp',
    )
    audit_plan = next(doc for doc in package['docs'] if 'План внутреннего аудита' in doc['name'])
    cell = Document(io.BytesIO(audit_plan['bytes'])).tables[0].cell(1, 2)

    assert len(cell.paragraphs) == 1
    assert cell.text.strip() == 'И.И. Иванов, Директор'


def test_spk_regulation_uses_the_current_acceptance_control_references():
    company = {
        'name': 'Строймедиапроект', 'form': 'ООО', 'city': 'Минск',
        'address': 'г. Минск', 'director_fio': 'Иванов Иван Иванович',
        'director_position': 'Директор',
    }
    people = [
        {'fio': 'Иванов Иван Иванович', 'position': 'Директор'},
        {'fio': 'Петров Петр Петрович', 'position': 'Главный инженер'},
    ]
    package = generate_spk_package_v2(
        company, people, [], generator.calculate_dates('17.09.2026'),
        generator.select_responsible(people), variant='spk_stroy',
    )
    regulation = next(doc for doc in package['docs'] if 'Положение о системе' in doc['name'])
    text = '\n'.join(paragraph.text for paragraph in Document(io.BytesIO(regulation['bytes'])).paragraphs)

    assert _ACCEPTANCE_CONTROL_LEGAL_TEXT in text
    assert 'от 06.06.2011 №716' not in text
