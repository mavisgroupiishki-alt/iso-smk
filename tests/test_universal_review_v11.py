from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys
import zipfile

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

import server  # noqa: E402
from docx_review import apply_package_review  # noqa: E402
from generator import create_docx_from_text  # noqa: E402


def xml(blob: bytes) -> str:
    with zipfile.ZipFile(BytesIO(blob)) as archive:
        return archive.read('word/document.xml').decode('utf-8')


def test_excel_sheet_is_not_error() -> None:
    assert server._is_extraction_error_text('[Лист: TDSheet]\n1 | Иванов') is False
    assert server._is_extraction_error_text('[XLS: не удалось прочитать]') is True


def test_compact_summary_hides_raw_and_finds_conflict() -> None:
    raw = '''=== 📦 СОСТАВ АРХИВА — ФАЙЛЫ ФИЗИЧЕСКИ НАЙДЕНЫ ===
- test/Список сотрудников.xls (43 КБ) — прочитан/передан в анализ
- test/Удостоверение Глушинский О.И..pdf (427 КБ) — прочитан/передан в анализ

=== 📚 ИСХОДНЫЕ ДОКУМЕНТЫ ИЗ АРХИВА — ИСПОЛЬЗОВАТЬ ДЛЯ ВЫБРАННОГО ПРОДУКТА ===
--- test/Список сотрудников.xls ---
[Лист: TDSheet]
Таб. № | ФИО | Должность
1 | Иванов Иван | Директор

--- test/Удостоверение Глушинский О.И..pdf ---
Кому выдано: Тураницкий Олег Иванович
Должность: Директор
'''
    summary = server._compact_archive_summary(raw)
    assert 'найдено 2 файлов' in summary
    assert 'строк: 1' in summary
    assert 'разные фамилии' in summary
    assert 'Таб. №' not in summary
    assert 'ИСХОДНЫЕ ДОКУМЕНТЫ' not in summary


def test_all_products_use_yellow_review_layer() -> None:
    doc = create_docx_from_text(
        'ООО «Варта»\nУНП: —\nФИО: [Иванов или Ивашов]\nАдрес: Минск'
    )
    data = {
        'company': {'name': 'Новая компания', 'form': 'ООО', 'director_fio': 'Иванов Иван Иванович'},
        'certification': {'scope': 'строительные работы'},
        'dates': {'audit_date': '31.08.2026'},
        'staff': [{'fio': 'Иванов Иван Иванович', 'position': 'Директор'}],
        'workers': ['Штукатур'],
    }
    for product in ('iso', 'suot', 'iso_suot', 'spk_stroy', 'spk_bisp', 'att', 'company_att'):
        docs, warnings, items = apply_package_review(
            [{'name': f'{product}.docx', 'bytes': doc}], data, product
        )
        assert docs[0]['name'].startswith('00 ПРОВЕРКА')
        body = xml(next(d['bytes'] for d in docs if d['name'] == f'{product}.docx'))
        assert body.count('w:highlight w:val="yellow"') >= 3
        assert any(item['value'] == 'Варта' for item in items)
        assert warnings


if __name__ == '__main__':
    test_excel_sheet_is_not_error()
    test_compact_summary_hides_raw_and_finds_conflict()
    test_all_products_use_yellow_review_layer()
    print('OK')
