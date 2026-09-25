import io
import http.client
import json
import os
import subprocess
import sys
import tempfile
import threading
import zipfile
from pathlib import Path
from types import SimpleNamespace
import socketserver

import server


def _zip_with_text_file() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('СИ/перечень.txt', 'Средство измерений: рулетка')
    return buffer.getvalue()


def _zip_with_scans() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('СИ/свидетельство.pdf', b'pdf-scan')
        archive.writestr('Люди/трудовая.jpg', b'photo-scan')
    return buffer.getvalue()


def _docx_with_embedded_scan(image_bytes=b'embedded-scan') -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as document:
        document.writestr('word/document.xml', '<w:document xmlns:w="urn:test"><w:body/></w:document>')
        document.writestr('word/media/image1.jpg', image_bytes)
    return buffer.getvalue()


def test_archive_processing_slot_rejects_parallel_jobs():
    server.release_archive_processing()

    assert server.reserve_archive_processing() is True
    assert server.reserve_archive_processing() is False

    server.release_archive_processing()
    assert server.reserve_archive_processing() is True
    server.release_archive_processing()


def test_archive_keeps_heavy_pdfs_serial_but_reads_small_scans_in_parallel():
    entries = [
        ('one.pdf', 'one.pdf', 5 * 1024 * 1024, 'pdf'),
        ('work.pdf', 'work трудовая.pdf', 512 * 1024, 'pdf'),
        ('two.pdf', 'two.pdf', 512 * 1024, 'pdf'),
        ('three.pdf', 'three.pdf', 1024 * 1024, 'pdf'),
        ('two.jpg', 'two.jpg', 1 * 1024 * 1024, 'image'),
        ('three.jpg', 'three.jpg', 1 * 1024 * 1024, 'image'),
    ]

    batches = server._archive_vision_batches(entries)

    assert [(len(batch), workers) for batch, workers in batches] == [(2, 1), (2, 2), (2, 2)]


def test_rar_upload_uses_the_existing_zip_recognition_pipeline(monkeypatch):
    monkeypatch.setattr(server, '_rar_to_zip_bytes', lambda *_: _zip_with_text_file())
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)

    result = server.extract_archive_with_vision(b'not-a-real-rar', 'СПК.rar', 'unused')

    assert 'СИ/перечень.txt' in result['text']
    assert 'рулетка' in result['text']


def test_single_pdf_is_packed_for_the_same_background_recognition_pipeline():
    packed, worker_name = server._single_visual_as_zip(b'%PDF-scan', 'Иванов трудовая.pdf')

    assert worker_name.endswith('.zip')
    with zipfile.ZipFile(io.BytesIO(packed)) as archive:
        assert archive.namelist() == ['Иванов трудовая.pdf']
        assert archive.read('Иванов трудовая.pdf') == b'%PDF-scan'


def test_non_visual_file_is_not_repacked_for_the_archive_worker():
    data, worker_name = server._single_visual_as_zip(b'data', 'штатное расписание.docx')

    assert data == b'data'
    assert worker_name == 'штатное расписание.docx'


def test_legacy_doc_is_read_with_antiword(monkeypatch):
    calls = []

    class Result:
        returncode = 0
        stdout = ('Перечень средств измерений:\n'
                  '- термометр -50 °С - +50 °С;\n'
                  '- нивелир;').encode('utf-8')
        stderr = b''

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return Result()

    monkeypatch.setattr(server.subprocess, 'run', fake_run)

    text = server.extract_text_from_file(b'legacy-word-binary', 'Перечень СИ.doc')

    assert 'Перечень средств измерений' in text
    assert 'нивелир' in text
    assert calls[0][0][0] == 'antiword'


def test_legacy_doc_retries_antiword_without_an_optional_encoding_map(monkeypatch):
    calls = []

    class FailedResult:
        returncode = 1
        stdout = b''
        stderr = b'encoding map unavailable'

    class SuccessResult:
        returncode = 0
        stdout = 'Перечень средств измерений: нивелир'.encode('utf-8')
        stderr = b''

    def fake_run(command, **kwargs):
        calls.append(command)
        return FailedResult() if len(calls) == 1 else SuccessResult()

    monkeypatch.setattr(server.subprocess, 'run', fake_run)

    text = server.extract_text_from_file(b'legacy-word-binary', 'Перечень СИ.doc')

    assert 'Перечень средств измерений' in text
    assert calls == [['antiword', '-m', 'UTF-8.txt', calls[0][-1]], ['antiword', calls[0][-1]]]


def test_legacy_doc_reads_unicode_worddocument_stream_without_system_converter(monkeypatch):
    import struct

    text = 'Перечень средств измерений: термометр; нивелир;'
    encoded = text.encode('utf-16le')
    stream = bytearray(0x40 + len(encoded))
    struct.pack_into('<II', stream, 0x18, 0x40, 0x40 + len(encoded))
    stream[0x40:] = encoded

    class FakeOle:
        def __init__(self, _source):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def openstream(self, name):
            assert name == 'WordDocument'
            return io.BytesIO(stream)

    monkeypatch.setitem(sys.modules, 'olefile', SimpleNamespace(OleFileIO=FakeOle))

    result = server.extract_text_from_file(b'legacy-word-binary', 'Перечень СИ.doc')

    assert result == text


def test_empty_task_error_is_not_presented_as_a_failure():
    assert server._friendly_public_error('') == ''


def test_docx_with_only_embedded_scans_uses_vision_in_archive(monkeypatch):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr('Спецы/Трон Ф.А..docx', _docx_with_embedded_scan())
    calls = []
    monkeypatch.setattr(server, 'vision_extract_with_retry', lambda data, name, *_args, **_kwargs: (
        calls.append((data, name)) or ('Диплом инженера-строителя № АБ-1', False)
    ))
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)

    result = server.extract_archive_with_vision(archive.getvalue(), 'спецы.zip', 'unused', product='spk_bisp')

    assert calls == [(b'embedded-scan', 'Трон Ф.А._страница_1.jpg')]
    assert 'Диплом инженера-строителя № АБ-1' in result['text']


def test_shared_specialists_folder_groups_named_files_and_one_unnamed_photo():
    texts = [
        '--- Белеогрин/Спецы/Трон Ф.А..docx ---\nДиплом',
        '--- Белеогрин/Спецы/Трудовая Трон.docx ---\nТрудовая книжка',
        '--- Белеогрин/Спецы/photo_2026-09-24.jpg ---\nФото диплома',
    ]

    groups, _order = server._group_blocks_by_person(texts)

    assert len(groups['Трон']) == 3
    assert groups['Спецы'] == []


def test_person_folder_surname_wins_over_unclear_labour_book_reading(monkeypatch):
    monkeypatch.setattr(server, '_simple_ai_call', lambda *_args, **_kwargs: '''
1) ФИО: Бирон Федор Александрович
Должность/роль для СПК: главный инженер
Дипломы: Диплом № 1
Трудовая книжка и вкладыши: ГТ-I № 7166864
''')

    summary = server._reconcile_person_summary('Трон', ['--- Трон Ф.А..docx ---\nДиплом'], 'unused', 1)

    assert '1) ФИО: Трон Федор Александрович' in summary


def test_spk_si_certificate_facts_are_extracted_without_waiting_for_chat_model():
    source = '''
--- СИ/Калибровка манометра.pdf ---
Свидетельство о калибровке № К-123/26 от 19.03.2026
Средство измерений: Манометр МП3
Заводской номер: 4512
Действительно до 19.03.2027
--- СИ/Поверка линейки.pdf ---
Свидетельство о поверке № П-925 от 23.03.2026
Линейка измерительная металлическая
Зав. № 2224
'''

    evidence = server._extract_spk_si_evidence(source)

    assert evidence['measurement_tools'] == [
        {'name': 'Манометр', 'factory_number': '4512', 'quantity': 1, 'source': 'verification_or_calibration'},
        {'name': 'Линейка измерительная', 'factory_number': '2224', 'quantity': 1, 'source': 'verification_or_calibration'},
    ]
    assert evidence['calibration_documents'][0]['number'] == 'К-123/26'
    assert evidence['calibration_documents'][0]['date'] == '19.03.2026'
    assert evidence['calibration_documents'][0]['valid_until'] == '19.03.2027'
    assert evidence['verification_documents'][0]['number'] == 'П-925'
    assert evidence['verification_documents'][0]['factory_number'] == '2224'


def test_spk_si_machine_readable_pages_keep_inventory_and_each_certificate_separate():
    source = '''
--- СИ/4.СИЗ.pdf ---
--- СТРАНИЦА 1 ---
СИ | наименование: Термометр технический стеклянный | модель: ТЖ-М | заводской номер: 91526 | количество: 1
СИ | наименование: Влагомер | модель: МГ4-У | заводской номер: 1983 | количество: 1
--- СТРАНИЦА 2 ---
ПОВЕРКА | наименование: Термометр технический стеклянный | заводской номер: 91526 | номер: 1-000845170-2026 | дата: 30.08.2026 | действует до: 30.08.2030
--- СТРАНИЦА 3 ---
ПОВЕРКА | наименование: Влагомер | заводской номер: 1983 | номер: 1-000842462-2026 | дата: 30.08.2026 | действует до: 30.08.2027
'''

    evidence = server._extract_spk_si_evidence(source)

    assert evidence['measurement_tools'] == [
        {'name': 'Термометр', 'model': 'ТЖ-М', 'factory_number': '91526', 'quantity': 1,
         'source': 'si_inventory'},
        {'name': 'Влагомер', 'model': 'МГ4-У', 'factory_number': '1983', 'quantity': 1,
         'source': 'si_inventory'},
    ]
    assert [(x['tool'], x['number'], x['factory_number']) for x in evidence['verification_documents']] == [
        ('Термометр', '1-000845170-2026', '91526'),
        ('Влагомер', '1-000842462-2026', '1983'),
    ]


def test_spk_si_parser_does_not_treat_a_serial_number_as_certificate_number():
    source = '''--- СИ/манометр.txt ---
Манометр. Заводской номер: 4512. Свидетельство о калибровке приложено.
'''

    evidence = server._extract_spk_si_evidence(source)

    assert evidence['measurement_tools'][0]['factory_number'] == '4512'
    assert evidence['calibration_documents'] == []


def test_spk_si_parser_rejects_ocr_prose_as_factory_number():
    evidence = server._extract_spk_si_evidence(
        'ПОВЕРКА | наименование: Клин для контроля зазоров | заводской номер: проверяются | номер: 1-25 | дата: 01.01.2026'
    )

    assert evidence['measurement_tools'][0]['factory_number'] == ''


def test_spk_si_certificate_for_leveling_staff_does_not_match_level_in_merge():
    source = '''--- СИ/поверка рейки.pdf ---
Свидетельство о поверке № Р-18 от 01.03.2026. Рейка нивелирная. Зав № 87А.
'''

    evidence = server._extract_spk_si_evidence(source)
    merged = server._merge_spk_si_evidence({
        'measurement_tools': [{'name': 'Нивелир', 'factory_number': 'Н-1'}],
    }, evidence)

    assert merged['measurement_tools'][0]['name'] == 'Нивелир'
    assert merged['measurement_tools'][1]['name'] == 'Рейка нивелирная'
    assert merged['measurement_tools'][1]['factory_number'] == '87А'


def test_spk_copy_list_keeps_ranges_for_two_verified_thermometers():
    evidence = server._extract_spk_si_evidence('''
ПОВЕРКА | наименование: Термометр | заводской номер: 91526 | номер: 1-1 | дата: 01.01.2026
ПОВЕРКА | наименование: Термометр | заводской номер: 102 | номер: 1-2 | дата: 02.01.2026
''')
    baseline = [
        {'name': 'Термометр', 'range': 'Диапазон измерений: (-50 +50) °С', 'quantity': 1},
        {'name': 'Термометр', 'range': 'Диапазон измерений: (0 +200) °С', 'quantity': 1},
    ]

    merged = server._merge_spk_copy_list_baseline({}, evidence, baseline)

    assert [(item['factory_number'], item['range']) for item in merged['measurement_tools']] == [
        ('91526', 'Диапазон измерений: (-50 +50) °С'),
        ('102', 'Диапазон измерений: (0 +200) °С'),
    ]


def test_spk_copy_list_is_authoritative_over_extra_ocr_inventory_rows():
    evidence = server._extract_spk_si_evidence('''
СИ | наименование: Термометр | заводской номер: 111 | количество: 1
СИ | наименование: Термометр | заводской номер: 222 | количество: 1
СИ | наименование: Влагомер | заводской номер: 333 | количество: 1
ПОВЕРКА | наименование: Термометр | заводской номер: 111 | номер: П-1 | дата: 01.01.2026
''')
    baseline = [{'name': 'Термометр', 'range': 'Диапазон измерений: (-50 +50) °С', 'quantity': 1}]

    merged = server._merge_spk_copy_list_baseline({}, evidence, baseline)

    assert merged['measurement_tools'] == [{
        'name': 'Термометр', 'range': 'Диапазон измерений: (-50 +50) °С',
        'quantity': 1, 'factory_number': '111',
    }]
    assert merged['verification_documents'][0]['number'] == 'П-1'


def test_spk_copy_list_prefers_the_named_document_over_generic_proposal():
    source = '''
--- Коммерческое предложение.docx ---
Перечень средств измерения: - термометр -50 °С - +50 °С; - влагомер;

--- Перечень копий СПК.doc ---
Перечень средств измерения: - нивелир; - рейка нивелирная;
'''

    tools = server._extract_spk_tools_from_copy_list(source)

    assert [tool['name'] for tool in tools] == ['Нивелир', 'Рейка нивелирная']


def test_archive_warning_names_parent_pdf_not_internal_page_heading():
    text = '''--- СИ/4.СИЗ.pdf ---
--- СТРАНИЦЫ 9-9 ---
[Не удалось прочитать страницы PDF: распознавание не завершилось вовремя.]
'''

    assert server._archive_read_warnings(text) == ['СИ/4.СИЗ.pdf']
    assert 'СИ/4.СИЗ.pdf' in server._compact_archive_summary(text)


def test_archive_summary_does_not_claim_success_when_personnel_facts_are_missing():
    text = '''
=== 📦 СОСТАВ АРХИВА — ФАЙЛЫ ФИЗИЧЕСКИ НАЙДЕНЫ ===
- Спецы/трудовая.pdf (30 КБ) — прочитан/передан в анализ

1) Иванов Иван Иванович
Трудовая книжка и вкладыши: не найдено
НЕУВЕРЕННЫЕ ПОЛЯ:
- номер трудовой неразборчив
'''

    summary = server._compact_archive_summary(text)

    assert 'не для всех специалистов найден номер трудовой книжки' in summary
    assert 'Критических ошибок чтения не обнаружено.' not in summary


def test_rar_scans_reach_the_pdf_and_image_recognition_path(monkeypatch):
    seen = []
    monkeypatch.setattr(server, '_rar_to_zip_bytes', lambda *_: _zip_with_scans())
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)
    monkeypatch.setattr(server, 'extract_text_from_file', lambda *_args, **_kwargs: '[PDF_SCAN: файл является сканом]')

    def fake_vision(_data, filename, *_args, **_kwargs):
        seen.append(filename)
        return f'распознан {filename}'

    monkeypatch.setattr(server, 'vision_extract', fake_vision)

    result = server.extract_archive_with_vision(b'not-a-real-rar', 'СПК.rar', 'unused')

    assert seen == ['свидетельство.pdf', 'трудовая.jpg']
    assert 'распознан свидетельство.pdf' in result['text']
    assert 'распознан трудовая.jpg' in result['text']


def test_spk_si_pdf_reads_the_full_register_one_page_at_a_time(monkeypatch):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr('СИ/4.СИЗ.pdf', b'pdf-scan')
    calls = []
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)
    monkeypatch.setattr(server, 'extract_text_from_file', lambda *_args, **_kwargs: '[PDF_SCAN: файл является сканом]')

    def fake_vision(_data, _filename, *_args, **kwargs):
        calls.append(kwargs)
        return 'СИ | наименование: Термометр | модель: ТЖ-М | заводской номер: 1 | количество: 1'

    monkeypatch.setattr(server, 'vision_extract', fake_vision)

    server.extract_archive_with_vision(archive.getvalue(), 'СПК.zip', 'unused', product='spk_bisp')

    assert len(calls) == 1
    assert calls[0]['max_pages_override'] == 32
    assert calls[0]['prompt_override'] == server.SPK_SI_VISION_PROMPT
    assert calls[0]['single_page_batches'] is True


def test_spk_si_prompt_bypasses_plain_tesseract_for_exact_certificate_fields(monkeypatch):
    local_ocr_calls, vision_calls = [], []
    monkeypatch.setattr(server, '_try_tesseract_first', lambda *_args, **_kwargs: local_ocr_calls.append(True) or 'обычный OCR текст')
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args: 1)
    monkeypatch.setattr(server, '_pdf_pages_to_images', lambda *_args, **_kwargs: ['aGVsbG8='])

    class Response:
        def raise_for_status(self):
            return None
        def json(self):
            return {'choices': [{'message': {'content': 'ПОВЕРКА | наименование: Термометр | заводской номер: 91526 | номер: 1-000845170-2026 | дата: 30.08.2026 | действует до: 30.08.2030'}}]}

    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **_kwargs: vision_calls.append(True) or Response())

    text = server.vision_extract(b'pdf', 'поверка.pdf', 'unused', prompt_override=server.SPK_SI_VISION_PROMPT,
                                 single_page_batches=True)

    assert local_ocr_calls == []
    assert len(vision_calls) == 1
    assert '1-000845170-2026' in text


def test_spk_si_pages_are_requested_independently_and_returned_in_page_order(monkeypatch):
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args: 2)
    monkeypatch.setattr(server, '_pdf_pages_to_images', lambda *_args, **_kwargs: ['cGFnZTE=', 'cGFnZTI='])
    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: type('Response', (), {
        'raise_for_status': lambda self: None,
        'json': lambda self: {'choices': [{'message': {'content': next(
            block['image_url']['url'].rsplit(',', 1)[-1]
            for block in kwargs['json']['messages'][0]['content']
            if block['type'] == 'image_url')}}]},
    })())

    text = server.vision_extract(b'pdf', 'поверка.pdf', 'unused', prompt_override=server.SPK_SI_VISION_PROMPT,
                                 single_page_batches=True)

    assert '--- СТРАНИЦЫ 1-1 ---\ncGFnZTE=' in text
    assert '--- СТРАНИЦЫ 2-2 ---\ncGFnZTI=' in text
    assert text.index('СТРАНИЦЫ 1-1') < text.index('СТРАНИЦЫ 2-2')


def test_short_pdf_page_groups_can_run_in_parallel_without_changing_order(monkeypatch):
    monkeypatch.setattr(server, '_try_tesseract_first', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args: 4)
    monkeypatch.setattr(server, '_pdf_pages_to_images', lambda *_args, **_kwargs: ['a', 'b', 'c', 'd'])
    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: type('Response', (), {
        'raise_for_status': lambda self: None,
        'json': lambda self: {'choices': [{'message': {'content': next(
            block['image_url']['url'].rsplit(',', 1)[-1]
            for block in kwargs['json']['messages'][0]['content']
            if block['type'] == 'image_url')}}]},
    })())

    text = server.vision_extract(b'pdf', 'трудовая.pdf', 'unused', parallel_page_batches=True)

    assert '--- СТРАНИЦЫ 1-2 ---\na' in text
    assert '--- СТРАНИЦЫ 3-4 ---\nc' in text
    assert text.index('СТРАНИЦЫ 1-2') < text.index('СТРАНИЦЫ 3-4')


def test_spk_si_folder_stays_a_source_block_and_reaches_evidence_parser(monkeypatch):
    texts = [
        '--- Клиент/СИ/реестр.pdf ---\nСИ | наименование: Термометр | модель: ТТЖ-М | заводской номер: 91526 | количество: 1',
        '--- Клиент/СИ/поверка.pdf ---\nПОВЕРКА | наименование: Термометр | заводской номер: 91526 | номер: 1-000845170-2026 | дата: 30.08.2026 | действует до: 30.08.2030',
    ]
    monkeypatch.setattr(server, '_simple_ai_call', lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('SI folder must not be sent to person reconciliation')))

    result = '\n\n'.join(server._reconcile_all_people(texts, 'unused'))
    evidence = server._extract_spk_si_evidence(result)

    assert '--- Клиент/СИ/реестр.pdf ---' in result
    assert evidence['verification_documents'] == [{
        'tool': 'Термометр', 'number': '1-000845170-2026', 'date': '30.08.2026',
        'valid_until': '30.08.2030', 'factory_number': '91526', 'type': 'Поверка',
        'source': 'verification_or_calibration',
    }]


def test_rar_upload_reports_an_unavailable_extractor(monkeypatch):
    monkeypatch.setattr(server, '_rar_to_zip_bytes', lambda *_: None)

    result = server.extract_archive_with_vision(b'not-a-real-rar', 'СПК.rar', 'unused')

    assert result['text'].startswith('[RAR:')
    assert 'не удалось открыть' in result['text'].lower()


def test_durable_archive_task_can_finish_after_process_restart(monkeypatch, tmp_path):
    uploads = tmp_path / 'uploads'
    tasks_dir = tmp_path / 'tasks'
    uploads.mkdir()
    tasks_dir.mkdir()
    task_id = 'recover01'
    (uploads / f'{task_id}.upload').write_bytes(b'archive')
    task = {
        'status': 'running', 'kind': 'archive', 'owner_user_id': 'owner',
        'filename': 'Белеогрин.rar', 'product': 'spk_bisp',
        'archive_upload': f'{task_id}.upload', 'progress': [],
    }
    monkeypatch.setattr(server, 'ARCHIVE_UPLOAD_DIR', uploads)
    monkeypatch.setattr(server, 'TASKS_DIR', tasks_dir)
    monkeypatch.setattr(server, 'TASKS', {task_id: task})
    monkeypatch.setenv('VIBE_API_KEY', 'test-key')
    monkeypatch.setattr(server, 'extract_archive_with_vision', lambda data, filename, key, **kwargs: {
        'text': 'архив прочитан', 'analysis_text': 'анализ', 'summary': 'сводка',
        'structured_data': {'staff': []},
    })

    server._run_archive_task(task_id)

    assert server.TASKS[task_id]['status'] == 'done'
    assert server.TASKS[task_id]['text'] == 'архив прочитан'
    assert not (uploads / f'{task_id}.upload').exists()


def test_visual_ocr_does_not_repeat_a_successful_read(monkeypatch):
    """A normal passport/certificate upload must make one recognition call."""
    calls = []

    def fake_vision(*_args, **_kwargs):
        calls.append(True)
        return 'Паспорт прочитан'

    monkeypatch.setattr(server, 'vision_extract', fake_vision)

    text, retried = server.vision_extract_with_retry(b'image', 'паспорт.jpg', 'unused')

    assert text == 'Паспорт прочитан'
    assert retried is False
    assert len(calls) == 1


def test_visual_ocr_retries_once_only_after_a_read_failure(monkeypatch):
    outcomes = iter([
        '[vision: таймаут после 90 сек]',
        'Свидетельство о поверке № 12',
    ])
    calls = []

    def fake_vision(*_args, **_kwargs):
        calls.append(True)
        return next(outcomes)

    monkeypatch.setattr(server, 'vision_extract', fake_vision)

    text, retried = server.vision_extract_with_retry(b'pdf', 'поверка.pdf', 'unused')

    assert text == 'Свидетельство о поверке № 12'
    assert retried is True
    assert len(calls) == 2


def test_archive_warnings_name_only_files_that_were_not_read():
    text = (
        '--- СИ/поверка.pdf --- ⚠️ ОШИБКА\n[Не удалось прочитать файл.]\n\n'
        '--- Люди/диплом.pdf ---\nДиплом инженера'
    )

    assert server._archive_read_warnings(text) == ['СИ/поверка.pdf']


def test_archive_warning_includes_an_oversized_pdf_that_was_not_read():
    text = (
        '--- СИ/скан.pdf ---\n'
        '[Скан слишком большой для распознавания.]\n\n'
        '--- СИ/перечень.xlsx ---\n[Лист: СИ]'
    )

    assert server._archive_read_warnings(text) == ['СИ/скан.pdf']


def test_pdf_retries_only_the_failed_page_batch(monkeypatch):
    monkeypatch.setattr(server, '_try_tesseract_first', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(server, '_pdf_pages_to_images', lambda *_args, **_kwargs: ['a', 'b'])
    calls = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {'choices': [{'message': {'content': 'Распознанный текст'}}]}

    def fake_post(*_args, **_kwargs):
        calls.append(True)
        if len(calls) == 1:
            raise server.req_lib.exceptions.Timeout()
        return Response()

    monkeypatch.setattr(server.req_lib, 'post', fake_post)

    text = server.vision_extract(b'pdf', 'поверка.pdf', 'unused')

    assert text.count('Распознанный текст') == 1
    assert len(calls) == 2


def test_pdf_reports_progress_for_each_recognition_batch(monkeypatch):
    monkeypatch.setattr(server, '_try_tesseract_first', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args, **_kwargs: 3)
    monkeypatch.setattr(server, '_pdf_pages_to_images', lambda *_args, **_kwargs: ['a', 'b', 'c'])
    progress = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {'choices': [{'message': {'content': 'Распознано'}}]}

    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **_kwargs: Response())

    server.vision_extract(b'pdf', 'трудовая.pdf', 'unused', progress_cb=progress.append)

    assert 'Распознаю страницы 1–2 из 3' in progress
    assert 'Прочитаны страницы 1–2 из 3' in progress
    assert 'Распознаю страницы 3–3 из 3' in progress


def test_rar_converter_preserves_nested_pdf_and_jpg_paths(monkeypatch):
    def fake_extract(command, **_kwargs):
        output_dir = Path(command[command.index('-C') + 1])
        si_dir = output_dir / 'ИК СПК инфа' / 'СИ'
        people_dir = output_dir / 'ИК СПК инфа' / 'Люди'
        si_dir.mkdir(parents=True)
        people_dir.mkdir(parents=True)
        (si_dir / 'свидетельство.pdf').write_bytes(b'pdf')
        (people_dir / 'трудовая.jpg').write_bytes(b'jpg')
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(server.subprocess, 'run', fake_extract)

    converted = server._rar_to_zip_bytes(b'not-a-real-rar', 'СПК.rar')

    assert converted is not None
    with zipfile.ZipFile(io.BytesIO(converted)) as archive:
        assert sorted(archive.namelist()) == [
            'ИК СПК инфа/Люди/трудовая.jpg',
            'ИК СПК инфа/СИ/свидетельство.pdf',
        ]


def test_rar_converter_uses_libarchive_when_no_system_extractor_exists(monkeypatch):
    class FakeEntry:
        def __init__(self, pathname, data):
            self.pathname = pathname
            self._data = data

        def get_blocks(self):
            yield self._data

    class FakeReader:
        def __enter__(self):
            return [
                FakeEntry('ИК СПК инфа/СИ/свидетельство.pdf', b'pdf'),
                FakeEntry('ИК СПК инфа/Люди/трудовая.jpg', b'jpg'),
            ]

        def __exit__(self, *_args):
            return False

    fake_libarchive = SimpleNamespace(file_reader=lambda _path: FakeReader())
    monkeypatch.setitem(sys.modules, 'libarchive', fake_libarchive)
    monkeypatch.setattr(server.shutil, 'which', lambda _command: None)

    converted = server._rar_to_zip_bytes(b'not-a-real-rar', 'СПК.rar')

    assert converted is not None
    with zipfile.ZipFile(io.BytesIO(converted)) as archive:
        assert sorted(archive.namelist()) == [
            'ИК СПК инфа/Люди/трудовая.jpg',
            'ИК СПК инфа/СИ/свидетельство.pdf',
        ]


def test_periodika_reads_one_nested_rar_with_the_previous_iso_suot_package(monkeypatch):
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('Старый пакет/Политика.txt', 'ISO 9001 и СУОТ: прежний пакет организации')

    outer = io.BytesIO()
    with zipfile.ZipFile(outer, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('ЯрмолюкСтрой/Список сотрудников.txt', 'Ярмолюк Альбин Владимирович — директор')
        archive.writestr('ЯрмолюкСтрой/Старые доки.rar', b'not-a-real-rar')

    monkeypatch.setattr(server, '_rar_to_zip_bytes', lambda *_: inner.getvalue())
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)

    result = server.extract_archive_with_vision(
        outer.getvalue(), 'ЯрмолюкСтрой.zip', 'unused', product='iso_suot'
    )

    assert 'Ярмолюк Альбин Владимирович' in result['text']
    assert 'прежний пакет организации' in result['text']
    assert 'Старый пакет/Политика.txt' in result['text']


def test_archive_task_returns_structured_data_to_the_browser():
    """The async archive client depends on this payload to keep SPK tools."""
    temp = tempfile.TemporaryDirectory()
    root = Path(temp.name)
    previous = {
        'AUTH_DIR': server.AUTH_DIR,
        'USERS_FILE': server.USERS_FILE,
        'SESSIONS_FILE': server.SESSIONS_FILE,
        'owner_password': os.environ.get('IGOR_OWNER_PASSWORD'),
        'owner_username': os.environ.get('IGOR_OWNER_USERNAME'),
        'tasks': dict(server.TASKS),
    }
    server.AUTH_DIR = root / 'auth'
    server.USERS_FILE = server.AUTH_DIR / 'users.json'
    server.SESSIONS_FILE = server.AUTH_DIR / 'sessions.json'
    os.environ['IGOR_OWNER_PASSWORD'] = 'owner-secret-password-123'
    os.environ['IGOR_OWNER_USERNAME'] = 'owner'
    server.TASKS.clear()
    httpd = socketserver.TCPServer(('127.0.0.1', 0), server.H)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        server.auth_bootstrap_owner()
        server.TASKS['archive-structured'] = {
            'status': 'done', 'kind': 'archive', 'owner_user_id': 'owner',
            'text': 'full archive text', 'analysis_text': 'focused archive text',
            'summary': 'archive summary', 'structured_data': {
                'spk': {'measurement_tools': [{'name': 'Нивелир', 'quantity': 1}]}
            },
        }
        conn = http.client.HTTPConnection('127.0.0.1', httpd.server_address[1], timeout=5)
        conn.request('POST', '/api/auth/login', body=json.dumps({
            'username': 'owner', 'password': 'owner-secret-password-123'
        }), headers={'Content-Type': 'application/json'})
        response = conn.getresponse()
        assert response.status == 200
        response.read()
        cookie = response.getheader('Set-Cookie').split(';', 1)[0]

        conn.request('GET', '/api/task/archive-structured', headers={'Cookie': cookie})
        response = conn.getresponse()
        payload = json.loads(response.read())

        assert response.status == 200
        assert payload['analysis_text'] == 'focused archive text'
        assert payload['summary'] == 'archive summary'
        assert payload['structured_data']['spk']['measurement_tools'][0]['name'] == 'Нивелир'
    finally:
        httpd.shutdown()
        httpd.server_close()
        server.TASKS.clear()
        server.TASKS.update(previous['tasks'])
        server.AUTH_DIR = previous['AUTH_DIR']
        server.USERS_FILE = previous['USERS_FILE']
        server.SESSIONS_FILE = previous['SESSIONS_FILE']
        for name, value in (('IGOR_OWNER_PASSWORD', previous['owner_password']),
                            ('IGOR_OWNER_USERNAME', previous['owner_username'])):
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        temp.cleanup()
