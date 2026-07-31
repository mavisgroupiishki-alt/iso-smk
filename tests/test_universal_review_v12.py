from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
import zipfile

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from docx_review import apply_package_review  # noqa: E402
from generator import create_docx_from_text  # noqa: E402


def xml(blob: bytes) -> str:
    with zipfile.ZipFile(BytesIO(blob)) as archive:
        return archive.read('word/document.xml').decode('utf-8')


def test_review_is_inline_and_no_separate_checklist() -> None:
    doc = create_docx_from_text(
        'ООО «Варта»\nУНП: —\nФИО: [Иванов или Ивашов]\nОбласть сертификации:'
    )
    data = {
        'company': {'name': 'Новая компания', 'form': 'ООО', 'director_fio': 'Иванов Иван Иванович'},
        'dates': {'audit_date': '31.08.2026'},
        'staff': [{'fio': 'Иванов Иван Иванович', 'position': 'Директор'}],
        'workers': ['Штукатур'],
    }
    docs, warnings, items = apply_package_review(
        [{'name': 'iso.docx', 'bytes': doc}], data, 'iso'
    )
    assert [d['name'] for d in docs] == ['iso.docx']
    body = xml(docs[0]['bytes'])
    assert 'ТРЕБУЕТ УТОЧНЕНИЯ' in body
    assert body.count('w:highlight w:val="yellow"') >= 4
    assert any(item['field'] == 'certification.scope' for item in items)
    assert warnings


def test_uncertain_value_is_highlighted_where_it_appears() -> None:
    doc = create_docx_from_text('ФИО руководителя: Тураницкий Олег Иванович')
    data = {
        'company': {'name': 'ОмиТрейд', 'form': 'ООО', 'director_fio': 'Тураницкий Олег Иванович'},
        'review_items': [{
            'field': 'company.director_fio',
            'value': 'Тураницкий Олег Иванович',
            'reason': 'конфликт с именем файла',
        }],
    }
    docs, _, _ = apply_package_review([{'name': 'doc.docx', 'bytes': doc}], data, 'att')
    body = xml(docs[0]['bytes'])
    assert 'Тураницкий Олег Иванович' in body
    assert 'w:highlight w:val="yellow"' in body


def test_all_products_keep_review_inside_original_document() -> None:
    base_doc = create_docx_from_text('Область сертификации:\nУНП: —')
    common = {
        'company': {'name': 'Тест', 'form': 'ООО', 'director_fio': 'Иванов Иван Иванович'},
        'staff': [{'fio': 'Иванов Иван Иванович', 'position': 'Директор'}],
        'dates': {},
    }
    for product in ('iso', 'suot', 'iso_suot', 'spk_stroy', 'spk_bisp', 'att', 'company_att'):
        docs, _, _ = apply_package_review([{'name': f'{product}.docx', 'bytes': base_doc}], common, product)
        assert len(docs) == 1
        assert docs[0]['name'] == f'{product}.docx'
        body = xml(docs[0]['bytes'])
        assert 'w:highlight w:val="yellow"' in body


if __name__ == '__main__':
    test_review_is_inline_and_no_separate_checklist()
    test_uncertain_value_is_highlighted_where_it_appears()
    test_all_products_keep_review_inside_original_document()
    print('OK')
