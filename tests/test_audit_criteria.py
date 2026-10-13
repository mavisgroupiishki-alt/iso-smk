import io
from pathlib import Path

from docx import Document

from generator_iso_suot_templates import (
    _audit_program_doc,
    _role_audit_points_iso,
    _role_audit_points_suot,
)


def test_iso_support_roles_have_deterministic_audit_criteria():
    expected = 'п.п. 7.2, 7.5, 8.1, 9.1'

    for position in ('Заместитель директора', 'Заведующий хозяйством', 'Техник-программист'):
        assert _role_audit_points_iso(position) == expected


def test_suot_support_roles_have_deterministic_audit_criteria():
    expected = 'п.п. 7.2–7.5, 8.1, 9.1'

    for position in ('Заместитель директора', 'Заведующий хозяйством', 'Техник-программист'):
        assert _role_audit_points_suot(position) == expected


def test_iso_audit_program_does_not_export_review_markers_for_support_roles():
    template = Path('iso_suot_templates/smk_doc_11.docx').read_bytes()
    audit_program = _audit_program_doc(
        {'name': 'Тест', 'form': 'ООО'},
        [
            {'fio': 'Иванов Иван Иванович', 'position': 'Заместитель директора'},
            {'fio': 'Петров Петр Петрович', 'position': 'Заведующий хозяйством'},
            {'fio': 'Сидоров Сидор Сидорович', 'position': 'Техник-программист'},
        ],
        {'year': '2026', 'reporting_year': '2026', 'document': '13.10.2026'},
        'iso',
        template,
    )
    document = Document(io.BytesIO(audit_program))
    text = '\n'.join(
        cell.text for table in document.tables for row in table.rows for cell in row.cells
    )

    assert 'ТРЕБУЕТ УТОЧНЕНИЯ' not in text
    assert text.count('п.п. 7.2, 7.5, 8.1, 9.1') >= 3
