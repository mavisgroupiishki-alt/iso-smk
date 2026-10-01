import io
import base64
import http.client
import json
import os
import subprocess
import sys
import tempfile
import threading
import zipfile
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
import socketserver

import server


def _zip_with_text_file() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('СИ/перечень.txt', 'Средство измерений: рулетка')
    return buffer.getvalue()


def test_company_att_reads_complete_personnel_pdf_from_generic_scan_name():
    path = 'лидинг/прораб/Отсканированный документ 8.pdf'

    assert server._archive_pdf_page_limit(path, 'company_att') == 24
    assert server._archive_pdf_page_limit('лидинг/Счет-заказ.pdf', 'company_att') is None
    assert server._archive_pdf_page_limit(path, 'spk_bisp') is None


def test_personnel_pdf_retry_rotates_a_sideways_page():
    from PIL import Image

    image = Image.new('RGB', (80, 40), 'white')
    original = server._image_to_jpeg_b64(image)
    rotated = server._rotate_pdf_page_b64(original)

    decoded = Image.open(io.BytesIO(base64.b64decode(rotated)))
    assert decoded.size == (40, 80)


def test_company_att_passes_generic_personnel_pdf_as_complete_scan(monkeypatch):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr('лидинг/прораб/Отсканированный документ 8.pdf', b'pdf-scan')

    calls = []
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)
    monkeypatch.setattr(server, 'extract_text_from_file', lambda *_args, **_kwargs: '[PDF_SCAN: файл является сканом]')

    def fake_vision(_data, filename, *_args, **kwargs):
        calls.append((filename, kwargs.get('max_pages_override')))
        return 'Трудовая книжка Алексеева'

    monkeypatch.setattr(server, 'vision_extract_with_retry', lambda *args, **kwargs: (fake_vision(*args, **kwargs), False))

    result = server.extract_archive_with_vision(
        archive.getvalue(), 'лидинг.zip', 'unused', product='company_att',
    )

    assert calls == [('лидинг/прораб/Отсканированный документ 8.pdf', 24)]
    assert 'Трудовая книжка Алексеева' in result['text']


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


def _docx_with_ordered_images(names) -> bytes:
    buffer = io.BytesIO()
    rels = []
    blips = []
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as document:
        for index, name in enumerate(names, 1):
            rel_id = f'rId{index}'
            rels.append(
                '<Relationship Id="%s" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
                'Target="media/%s"/>' % (rel_id, name)
            )
            blips.append('<a:blip r:embed="%s"/>' % rel_id)
            document.writestr('word/media/' + name, ('scan-' + name).encode())
        document.writestr(
            'word/document.xml',
            '<w:document xmlns:w="urn:test" '
            'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            '<w:body>%s</w:body></w:document>' % ''.join(blips),
        )
        document.writestr(
            'word/_rels/document.xml.rels',
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">%s</Relationships>'
            % ''.join(rels),
        )
    return buffer.getvalue()


def test_archive_processing_slot_serializes_worker_execution():
    server.release_archive_processing()

    assert server.reserve_archive_processing() is True
    assert server.reserve_archive_processing() is False

    server.release_archive_processing()
    assert server.reserve_archive_processing() is True
    server.release_archive_processing()


def test_archive_queue_accepts_second_user_and_reports_position(monkeypatch, tmp_path):
    task_dir = tmp_path / 'tasks'
    upload_dir = tmp_path / 'uploads'
    task_dir.mkdir()
    upload_dir.mkdir()
    monkeypatch.setattr(server, 'TASKS_DIR', task_dir)
    monkeypatch.setattr(server, 'ARCHIVE_UPLOAD_DIR', upload_dir)
    monkeypatch.setattr(server, 'TASKS', {})

    started = []

    class FakeThread:
        def __init__(self, *, target, args, daemon):
            started.append(args[0])

        def start(self):
            return None

    monkeypatch.setattr(server.threading, 'Thread', FakeThread)
    (upload_dir / 'first.upload').write_bytes(b'one')
    (upload_dir / 'second.upload').write_bytes(b'two')
    first = {
        'kind': 'archive', 'status': 'queued', 'queued_at': '2026-10-01T12:00:00',
        'filename': 'first.zip', 'archive_upload': 'first.upload', 'progress': [],
    }
    second = {
        'kind': 'archive', 'status': 'queued', 'queued_at': '2026-10-01T12:01:00',
        'filename': 'second.zip', 'archive_upload': 'second.upload', 'progress': [],
    }

    server.release_archive_processing()
    server.queue_archive_task('first', first)
    server.queue_archive_task('second', second)

    assert first['status'] == 'running'
    assert second['status'] == 'queued'
    assert server.archive_queue_position('second') == 2
    assert started == ['first']
    server.release_archive_processing()


def test_finished_archive_starts_the_next_queued_file(monkeypatch, tmp_path):
    upload_dir = tmp_path / 'uploads'
    task_dir = tmp_path / 'tasks'
    upload_dir.mkdir()
    task_dir.mkdir()
    monkeypatch.setattr(server, 'ARCHIVE_UPLOAD_DIR', upload_dir)
    monkeypatch.setattr(server, 'TASKS_DIR', task_dir)
    (upload_dir / 'first.upload').write_bytes(b'archive')
    task = {
        'kind': 'archive', 'status': 'running', 'filename': 'first.zip',
        'archive_upload': 'first.upload', 'progress': [], 'product': 'all',
    }
    monkeypatch.setattr(server, 'TASKS', {'first': task})
    monkeypatch.setenv('VIBE_API_KEY', 'test-key')
    monkeypatch.setattr(server, 'extract_archive_with_vision', lambda *args, **kwargs: {
        'text': 'прочитано', 'analysis_text': 'прочитано', 'summary': 'готово',
        'structured_data': {},
    })
    next_starts = []
    monkeypatch.setattr(server, 'start_next_archive_task', lambda: next_starts.append(True))

    server.reserve_archive_processing()
    server._run_archive_task('first')

    assert task['status'] == 'done'
    assert next_starts == [True]
    assert not (upload_dir / 'first.upload').exists()


def test_cancelling_archive_prevents_an_automatic_resume(monkeypatch):
    task = {'kind': 'archive', 'status': 'running', 'progress': ['Читаю файл']}
    monkeypatch.setattr(server, 'TASKS', {'task-1': task})
    saved = []
    monkeypatch.setattr(server, 'save_task', lambda task_id, value: saved.append((task_id, value['status'])))

    cancelled = server.cancel_archive_task('task-1')

    assert cancelled is task
    assert task['status'] == 'cancelled'
    assert saved == [('task-1', 'cancelled')]
    assert 'Файл не внесён' in task['progress'][-1]


def test_upload_retry_finds_the_task_already_saved_for_the_same_browser_upload(monkeypatch):
    task = {
        'kind': 'archive', 'status': 'running', 'owner_user_id': 'operator-1',
        'client_upload_id': 'upload-abc',
    }
    monkeypatch.setattr(server, 'TASKS', {'task-1': task})

    found = server.find_archive_task_by_upload_id('operator-1', 'upload-abc')

    assert found == ('task-1', task)
    assert server.find_archive_task_by_upload_id('operator-2', 'upload-abc') is None


def test_streamed_archive_upload_saves_docx_without_copying_multipart_to_memory(monkeypatch, tmp_path):
    boundary = 'test-boundary'
    docx_bytes = _docx_with_embedded_scan()
    body = (
        f'--{boundary}\r\nContent-Disposition: form-data; name="product"\r\n\r\nspk_bisp\r\n'.encode()
        + f'--{boundary}\r\nContent-Disposition: form-data; name="upload_id"\r\n\r\nupload-test\r\n'.encode()
        + f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="Тагиев ТК.docx"\r\n'
          f'Content-Type: application/octet-stream\r\n\r\n'.encode()
        + docx_bytes + f'\r\n--{boundary}--\r\n'.encode()
    )
    headers = Message()
    headers['Content-Type'] = f'multipart/form-data; boundary={boundary}'
    headers['Content-Length'] = str(len(body))

    class FakeHandler:
        def _json(self, value, status=200):
            self.response = (value, status)

    started = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            started.append((args, kwargs))

        def start(self):
            return None

    monkeypatch.setattr(server, 'ARCHIVE_UPLOAD_DIR', tmp_path)
    monkeypatch.setattr(server, 'TASKS_DIR', tmp_path / 'tasks')
    server.TASKS_DIR.mkdir()
    monkeypatch.setattr(server, 'TASKS', {})
    monkeypatch.setattr(server.threading, 'Thread', FakeThread)
    server.release_archive_processing()
    handler = FakeHandler()
    handler.rfile = io.BytesIO(body)
    handler.headers = headers
    try:
        server.H._handle_large_archive_upload(handler, {'id': 'operator-1'}, len(body))
        payload, status = handler.response
        assert status == 200
        assert payload['success'] is True
        assert started
        task = next(iter(server.TASKS.values()))
        second_handler = FakeHandler()
        second_handler.rfile = io.BytesIO(body.replace(b'upload-test', b'upload-next'))
        second_handler.headers = headers
        server.H._handle_large_archive_upload(second_handler, {'id': 'operator-2'}, len(body))
        second_payload, second_status = second_handler.response
        assert second_status == 200
        assert second_payload['success'] is True
        assert len(server.TASKS) == 2
        assert sorted(item['status'] for item in server.TASKS.values()) == ['queued', 'running']
        with zipfile.ZipFile(tmp_path / task['archive_upload']) as archive:
            assert archive.namelist() == ['Тагиев ТК.docx']
            assert archive.read('Тагиев ТК.docx') == docx_bytes
    finally:
        server.release_archive_processing()


def test_restart_does_not_resume_an_archive_that_would_exhaust_service_memory(monkeypatch, tmp_path):
    task_dir = tmp_path / 'tasks'
    upload_dir = tmp_path / 'uploads'
    task_dir.mkdir()
    upload_dir.mkdir()
    task = {
        'kind': 'archive', 'status': 'running', 'filename': 'large.rar',
        'archive_upload': 'task-1.upload', 'progress': [],
    }
    (task_dir / 'task-1.json').write_text(json.dumps(task), encoding='utf-8')
    (upload_dir / 'task-1.upload').write_bytes(b'x' * 9)
    monkeypatch.setattr(server, 'TASKS_DIR', task_dir)
    monkeypatch.setattr(server, 'ARCHIVE_UPLOAD_DIR', upload_dir)
    monkeypatch.setattr(server, 'MAX_ARCHIVE_WORKER_BYTES', 8)
    monkeypatch.setattr(server, 'TASKS', {})
    server.release_archive_processing()

    server._resume_pending_archive_task()

    stored = json.loads((task_dir / 'task-1.json').read_text('utf-8'))
    assert stored['status'] == 'error'
    assert 'слишком большой' in stored['error']
    assert not (upload_dir / 'task-1.upload').exists()


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


def test_docx_is_packed_for_the_archive_worker_so_embedded_scans_are_read():
    data, worker_name = server._single_visual_as_zip(b'data', 'штатное расписание.docx')

    assert worker_name.endswith('.zip')
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        assert archive.namelist() == ['штатное расписание.docx']
        assert archive.read('штатное расписание.docx') == b'data'


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


def test_embedded_docx_pages_follow_word_document_order_not_media_filename_order():
    document = _docx_with_ordered_images(['image1.jpeg', 'image10.jpeg', 'image2.jpeg'])

    pages = server._embedded_docx_images(document, 'трудовая.docx')

    assert [name for name, _data in pages] == ['image1.jpeg', 'image10.jpeg', 'image2.jpeg']


def test_scanned_docx_reads_more_than_the_old_twelve_page_limit(monkeypatch):
    document = _docx_with_ordered_images([f'image{index}.jpeg' for index in range(1, 14)])
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr('Люди/Тагиев ТК.docx', document)
    calls = []
    monkeypatch.setattr(server, 'vision_extract_with_retry', lambda data, name, *_args, **_kwargs: (
        calls.append((data, name)) or ('Сведения о работе', False)
    ))
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)

    result = server.extract_archive_with_vision(archive.getvalue(), 'люди.zip', 'unused')

    assert len(calls) == 13
    assert {name for _data, name in calls} == {
        f'Тагиев ТК_страница_{index}.jpeg' for index in range(1, 14)
    }
    assert 'Сведения о работе' in result['text']


def test_large_docx_scan_is_not_silently_skipped_at_text_file_limit(monkeypatch):
    # A DOCX scan can be larger than a normal text document because it stores
    # a high-resolution page. Its OCR must still run.
    document = _docx_with_embedded_scan(os.urandom(4 * 1024 * 1024 + 1024))
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr('Люди/Тагиев ТК.docx', document)
    calls = []
    monkeypatch.setattr(server, 'vision_extract_with_retry', lambda data, name, *_args, **_kwargs: (
        calls.append(name) or ('Сведения о работе', False)
    ))
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)

    result = server.extract_archive_with_vision(archive.getvalue(), 'люди.zip', 'unused')

    assert calls == ['Тагиев ТК_страница_1.jpg']
    assert 'Сведения о работе' in result['text']


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


def test_spk_si_invoice_and_technical_passport_fill_inventory_without_inventing_verification():
    source = '''
--- Счёт на средства измерений.pdf ---
СЧЁТ № 18. Рулетка измерительная. Модель: Р-5. Заводской номер: R-2026. Количество: 2 шт.
--- Технический паспорт термометра.pdf ---
Технический паспорт средства измерений. Термометр. Тип: ТТЖ-М. Заводской номер: 91526.
Диапазон измерений: -50 °С до +50 °С.
'''

    evidence = server._extract_spk_si_evidence(source)
    tools = {item['name']: item for item in evidence['measurement_tools']}

    assert tools['Рулетка измерительная'] == {
        'name': 'Рулетка измерительная', 'model': 'Р-5', 'factory_number': 'R-2026',
        'range': '', 'quantity': 2, 'source': 'invoice_or_technical_passport',
    }
    assert tools['Термометр'] == {
        'name': 'Термометр', 'model': 'ТТЖ-М', 'factory_number': '91526',
        'range': '-50 °С до +50 °С', 'quantity': 1, 'source': 'invoice_or_technical_passport',
    }
    assert evidence['verification_documents'] == []
    assert evidence['calibration_documents'] == []

    merged = server._merge_spk_copy_list_baseline(
        {}, evidence, [{'name': 'Рулетка измерительная', 'quantity': 1}],
    )
    merged_tools = {item['name']: item for item in merged['measurement_tools']}
    assert merged_tools['Рулетка измерительная']['factory_number'] == 'R-2026'
    assert merged_tools['Рулетка измерительная']['quantity'] == 2
    assert merged_tools['Термометр']['range'] == '-50 °С до +50 °С'


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


def test_spk_si_prompt_uses_complete_local_ocr_for_exact_certificate_fields(monkeypatch):
    vision_calls = []
    monkeypatch.setattr(
        server, '_tesseract_pdf_pages',
        lambda *_args, **_kwargs: (1, ['aGVsbG8='], [
            'Термометр. Свидетельство о поверке № '
            '1-000845170-2026 от 30.08.2026 действует до 30.08.2030'
        ]),
    )

    class Response:
        def raise_for_status(self):
            return None
        def json(self):
            return {'choices': [{'message': {'content': 'ПОВЕРКА | наименование: Термометр | заводской номер: 91526 | номер: 1-000845170-2026 | дата: 30.08.2026 | действует до: 30.08.2030'}}]}

    payloads = []
    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **_kwargs: (
        payloads.append(_kwargs['json']) or vision_calls.append(True) or Response()
    ))

    text = server.vision_extract(b'pdf', 'поверка.pdf', 'unused', prompt_override=server.SPK_SI_VISION_PROMPT,
                                 single_page_batches=True)

    assert vision_calls == []
    assert payloads == []
    assert '1-000845170-2026' in text


def test_wrongly_named_labour_book_pdf_uses_visual_reader(monkeypatch):
    monkeypatch.setattr(
        server, '_check_tesseract',
        lambda: {'available': True, 'has_rus': True, 'data_dir': '/tmp'},
    )
    monkeypatch.setattr(
        server,
        '_tesseract_pdf_pages',
        lambda *_args, **_kwargs: (1, ['aGVsbG8='], [
            'ТРУДОВАЯ КНИЖКА\nСведения о работе\nКремень Таиса Леонидовна'
        ]),
    )

    assert server._try_tesseract_first(b'pdf', 'Диплом зам директора.pdf') is None


def test_vision_prompt_classifies_document_by_its_contents_not_filename():
    assert 'Название файла не доказывает вид документа' in server.VISION_PROMPT
    assert 'ВИД ДОКУМЕНТА:' in server.VISION_PROMPT


def test_generic_pdf_fallback_uses_detailed_page_render(monkeypatch):
    rendered = []
    monkeypatch.setattr(server, '_try_tesseract_first', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args: 1)
    monkeypatch.setattr(
        server,
        '_pdf_pages_to_images',
        lambda *_args, **kwargs: rendered.append(kwargs) or ['aGVsbG8='],
    )

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {'choices': [{'message': {'content': 'ВИД ДОКУМЕНТА: трудовая книжка'}}]}

    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **_kwargs: Response())

    server.vision_extract(b'%PDF', 'ТК сотрудника.pdf', 'unused')

    assert rendered == [{'max_pages': 8, 'max_dim': 2400}]


def test_detailed_pdf_fallback_reads_each_page_separately(monkeypatch):
    calls = []
    monkeypatch.setattr(server, '_try_tesseract_first', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args: 2)
    monkeypatch.setattr(server, '_pdf_pages_to_images', lambda *_args, **_kwargs: ['cGFnZTE=', 'cGFnZTI='])

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {'choices': [{'message': {'content': 'ВИД ДОКУМЕНТА: трудовая книжка'}}]}

    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: calls.append(kwargs['json']) or Response())

    server.vision_extract(b'%PDF', 'ТК сотрудника.pdf', 'unused')

    assert len(calls) == 2
    assert all(payload['max_tokens'] == 3500 for payload in calls)
    assert all(
        len([block for block in payload['messages'][0]['content'] if block['type'] == 'image_url']) == 1
        for payload in calls
    )


def test_unlabelled_hard_to_read_pdf_uses_detailed_page_reader(monkeypatch):
    rendered = []
    calls = []
    monkeypatch.setattr(server, '_try_tesseract_first', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args: 2)
    monkeypatch.setattr(
        server,
        '_pdf_pages_to_images',
        lambda *_args, **kwargs: rendered.append(kwargs) or ['cGFnZTE=', 'cGFnZTI='],
    )

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {'choices': [{'message': {'content': 'ВИД ДОКУМЕНТА: аттестат'}}]}

    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: calls.append(kwargs['json']) or Response())

    server.vision_extract(b'%PDF', 'scan-001.pdf', 'unused')

    assert rendered == [{'max_pages': 8, 'max_dim': 2400}]
    assert len(calls) == 2
    assert all(payload['max_tokens'] == 3500 for payload in calls)


def test_pdf_page_rendering_releases_native_pdf_resources(monkeypatch):
    """A many-page scan must not retain every PDFium page bitmap in memory."""
    from PIL import Image

    state = {'document_closed': False, 'page_closed': False, 'bitmap_closed': False}

    class FakeBitmap:
        def to_pil(self):
            return Image.new('RGB', (10, 10), 'white')

        def close(self):
            state['bitmap_closed'] = True

    class FakePage:
        def render(self, **_kwargs):
            return FakeBitmap()

        def close(self):
            state['page_closed'] = True

    class FakeDocument:
        def __len__(self):
            return 1

        def __getitem__(self, _index):
            return FakePage()

        def close(self):
            state['document_closed'] = True

    fake_document = FakeDocument()
    monkeypatch.setitem(sys.modules, 'pypdfium2', SimpleNamespace(PdfDocument=lambda _data: fake_document))

    images = server._pdf_pages_to_images(b'%PDF-test', max_pages=1)

    assert len(images) == 1
    assert state == {'document_closed': True, 'page_closed': True, 'bitmap_closed': True}


def test_spk_si_prompt_sends_ambiguous_local_ocr_to_vision(monkeypatch):
    monkeypatch.setattr(server, '_tesseract_pdf_pages', lambda *_args, **_kwargs: (1, ['aGVsbG8='], ['обычный OCR текст']))

    class Response:
        def raise_for_status(self):
            return None
        def json(self):
            return {'choices': [{'message': {'content': 'ПОВЕРКА | номер: 123'}}]}

    calls = []
    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **_kwargs: calls.append(True) or Response())

    text = server.vision_extract(b'pdf', 'поверка.pdf', 'unused', prompt_override=server.SPK_SI_VISION_PROMPT,
                                 single_page_batches=True)

    assert calls == [True]
    assert 'ПОВЕРКА | номер: 123' in text


def test_spk_si_fallback_requests_only_structured_facts_with_bounded_response(monkeypatch):
    """A weak SI page must not spend a minute transcribing a whole form."""
    monkeypatch.setattr(server, '_tesseract_pdf_pages', lambda *_args, **_kwargs: (1, ['aGVsbG8='], ['неразборчивый скан']))
    payloads = []

    class Response:
        def raise_for_status(self):
            return None
        def json(self):
            return {'choices': [{'message': {'content': 'ПОВЕРКА | наименование: Термометр | заводской номер: 1 | номер: 2'}}]}

    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: payloads.append(kwargs['json']) or Response())

    server.vision_extract(b'pdf', 'поверка.pdf', 'unused', prompt_override=server.SPK_SI_VISION_PROMPT,
                          single_page_batches=True)

    assert payloads[0]['max_tokens'] == 2000
    prompt = payloads[0]['messages'][0]['content'][-1]['text']
    assert 'Извлеки весь текст' not in prompt
    assert 'Верни только структурированные строки' in prompt
    assert 'паспорт, руководство или техническое описание' in prompt


def test_spk_si_local_ocr_accepts_inventory_and_certificate_without_issue_date():
    inventory = (
        '--- СТРАНИЦА 1 ---\nКвитанция возврата средств измерений\n'
        'Термометры жидкостные ТТЖ-М\nМанометр\nРулетка измерительная\n'
    )
    certificate = (
        '--- СТРАНИЦА 2 ---\nСвидетельство об уполномочивании № 1\n'
        'Свидетельство о государственной поверке средств измерений № 1-000845170-2026\n'
        'Действительно до 30 августа 2030 г.\nТермометры технические жидкостные ТТЖ-М\n'
        'Заводской номер 91526'
    )

    assert server._spk_si_tesseract_result_is_complete(inventory)
    assert server._spk_si_tesseract_result_is_complete(certificate)
    evidence = server._extract_spk_si_evidence(certificate)
    assert evidence['verification_documents'][0]['number'] == '1-000845170-2026'
    assert evidence['verification_documents'][0]['date'] == ''


def test_spk_si_local_ocr_accepts_supporting_attestation_without_turning_it_into_verification():
    attestation = (
        'АТТЕСТАТ № 3512-4126 от 7 августа 2026 г.\n'
        'Рейка контрольная с длиной рабочей поверхности 3001 мм\n'
        'Срок действия аттестата до 7 августа 2027 г.'
    )
    protocol = (
        'ПРОТОКОЛ ИЗМЕРЕНИЙ № 628-4126\n'
        'Клиновой шаблон для контроля зазоров № 340\n'
        'Дата измерений 24.08.2026 г.'
    )

    assert server._spk_si_tesseract_result_is_complete(attestation)
    assert server._spk_si_tesseract_result_is_complete(protocol)
    assert server._extract_spk_si_evidence(attestation)['verification_documents'] == []


def test_spk_si_local_ocr_accepts_certificate_for_tool_outside_approved_copy_list():
    certificate = (
        'Свидетельство о калибровке\n'
        'Номер свидетельства ВУ 01 № 0023520-4126-В\n'
        'Дата калибровки 26.08.2026 г.\n'
        'Объект калибровки Угломер с нониусом № 4-11100374\n'
        'Диапазон измерений 0° – 360°'
    )

    assert server._spk_si_tesseract_result_is_complete(certificate)
    # The approved copy list remains the only source of SI rows.  A readable
    # certificate for a different device must neither cause a retry nor add it.
    assert server._extract_spk_si_evidence(certificate)['measurement_tools'] == []


def test_spk_si_extracts_date_and_serial_from_calibration_form():
    certificate = '''
Свидетельство о калибровке
Номер свидетельства ВУ 01 № 0023915-4126-В  Дата калибровки — _04.09.2026 г.
Объект калибровки - Рулетка измерительная металлическая № Б-10
'''

    document = server._extract_spk_si_evidence(certificate)['calibration_documents'][0]

    assert document['number'] == '0023915-4126-В'
    assert document['date'] == '04.09.2026'
    assert document['factory_number'] == 'Б-10'


def test_spk_hiring_order_enriches_personal_folder_without_labour_book():
    summary = '''
1) ФИО
Евневич Алексей Александрович
Должность/роль для СПК: не найдено
Дипломы: А № 0083147, Белорусский технический техникум
Трудовая книжка и вкладыши: не найдено
'''
    order = '''
ПРИКАЗ
ПРИНЯТЬ:
Евневич Алексей Александрович на должность производитель работ
(сантехник) с заключением трудового договора с 21.09.2026.
'''

    candidates = server._extract_spk_staff_from_person_summaries(summary, include_unconfirmed=True)
    orders = server._extract_spk_staff_from_hiring_orders(order)
    staff = server._merge_spk_staff_rows(candidates, orders)

    assert len(staff) == 1
    assert staff[0]['fio'] == 'Евневич Алексей Александрович'
    assert staff[0]['position'] == 'производитель работ (сантехник)'
    assert staff[0]['needs_review'] is False
    assert staff[0]['diplomas'] == [{'full_text': 'А № 0083147, Белорусский технический техникум'}]


def test_spk_named_diploma_in_shared_folder_is_attached_to_appointed_person():
    source = '''
--- Клиент/Спецы/photo.jpg ---
Тип документа: Диплом
Номер документа: А № 0186143
Кому выдан (ФИО): Зенченко Александр Николаевич
Организация: Молодечненский политехнический техникум
Специальность: Электротехника
Присвоенная квалификация: техник-электрик
'''
    order = [{'fio': 'Зенченко Александр Николаевич', 'position': 'главный инженер'}]

    staff = server._merge_spk_staff_rows(order, server._extract_spk_diplomas_from_named_sources(source))

    assert len(staff) == 1
    assert 'А № 0186143' in staff[0]['diplomas'][0]['full_text']
    assert 'техник-электрик' in staff[0]['diplomas'][0]['full_text']


def test_spk_named_diploma_accepts_markdown_labels_from_vision():
    source = '''
--- Клиент/Спецы/photo.jpg ---
**ФИО:** Зенченко Александр Николаевич
**Должности / Квалификации:** техник-электрик
**Номера документов:**
* Номер диплома: А № 0186143
* Регистрационный номер: 1150
'''

    rows = server._extract_spk_diplomas_from_named_sources(source)

    assert rows[0]['fio'] == 'Зенченко Александр Николаевич'
    assert 'А № 0186143' in rows[0]['diplomas'][0]['full_text']
    assert 'техник-электрик' in rows[0]['diplomas'][0]['full_text']


def test_spk_named_diploma_accepts_real_kumu_vydano_label():
    source = '''
--- Клиент/Спецы/photo_2026-09-24.jpg ---
**Тип документа:** ДИПЛОМ
**Номер документа:** № 0186143
**Кому выдано (ФИО):** Зенченко Александру Николаевичу
**Квалификация:** техник-электрик
'''

    rows = server._extract_spk_diplomas_from_named_sources(source)

    assert rows[0]['fio'] == 'Зенченко Александру Николаевичу'
    assert '№ 0186143' in rows[0]['diplomas'][0]['full_text']


def test_spk_diploma_holder_in_dative_case_merges_with_hiring_order():
    order = [{'fio': 'Зенченко Александр Николаевич', 'position': 'главный инженер'}]
    diploma = [{
        'fio': 'Зенченко Александру Николаевичу',
        'diplomas': [{'full_text': 'Диплом № 0186143'}],
    }]

    staff = server._merge_spk_staff_rows(order, diploma)

    assert len(staff) == 1
    assert staff[0]['fio'] == 'Зенченко Александр Николаевич'
    assert staff[0]['diplomas'] == [{'full_text': 'Диплом № 0186143'}]
    assert not server._spk_staff_same_person(
        'Зенченко Александр Николаевич', 'Евневич Александру Николаевичу',
    )


def test_phone_gallery_overlay_retries_with_document_only_prompt(monkeypatch):
    calls = []

    def fake_vision(file_bytes, filename, api_key, **kwargs):
        calls.append((file_bytes, kwargs))
        return 'Закрыть   Мультимедиа   диплом' if len(calls) == 1 else 'ДИПЛОМ № 0186143'

    monkeypatch.setattr(server, 'vision_extract', fake_vision)

    text, retried = server.vision_extract_with_retry(b'not-an-image', 'diplom.jpg', 'unused')

    assert text == 'ДИПЛОМ № 0186143'
    assert retried is True
    assert 'Игнорируй интерфейс телефона' in calls[1][1]['prompt_override']


def test_tesseract_rejects_short_plausible_looking_photo_noise():
    noise = 'лыиЛОМ, ууу СЕ рибта. П Вавтратовиско / 7 заФ ТОАУ фенениен Вкр реглго. 9 гелоуй ам 7170'
    document = 'ДИПЛОМ № 0186143. Настоящий диплом выдан Александру Николаевичу. Специальность: электротехническая.'

    assert not server._tesseract_text_is_usable(noise)
    assert server._tesseract_text_is_usable(document)


def test_spk_si_retries_only_an_ambiguous_page_in_the_correct_orientation(monkeypatch):
    calls = []

    class Image:
        def rotate(self, degrees, expand):
            calls.append((degrees, expand))
            return f'rotated-{degrees}'

    monkeypatch.setattr(
        server,
        '_tesseract_ocr_image',
        lambda image: 'неразборчиво' if image == 'rotated-90' else (
            'Свидетельство о поверке № 7-2026\nТермометр\nЗаводской номер 1'
            if image == 'rotated-270' else None
        ),
    )

    text = server._spk_si_best_local_ocr(Image(), 'неразборчиво')

    assert '№ 7-2026' in text
    assert calls == [(90, True), (270, True)]


def test_spk_si_uses_vision_only_for_ambiguous_page(monkeypatch):
    monkeypatch.setattr(
        server, '_tesseract_pdf_pages',
        lambda *_args, **_kwargs: (2, ['cGFnZTE=', 'cGFnZTI='], [
            'Термометр. Свидетельство о поверке № 7-2026 от 01.09.2026',
            'неразборчивый скан',
        ]),
    )
    payloads = []
    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: payloads.append(kwargs['json']) or type('Response', (), {
        'raise_for_status': lambda self: None,
        'json': lambda self: {'choices': [{'message': {'content': 'ПОВЕРКА | номер: 8-2026'}}]},
    })())

    text = server.vision_extract(b'pdf', 'поверка.pdf', 'unused', prompt_override=server.SPK_SI_VISION_PROMPT,
                                 single_page_batches=True)

    assert len(payloads) == 1
    assert '--- СТРАНИЦЫ 1-1 ---\nТермометр' in text
    assert '--- СТРАНИЦЫ 2-2 ---\nПОВЕРКА | номер: 8-2026' in text


def test_spk_si_image_uses_complete_response_budget(monkeypatch):
    payloads = []

    class Response:
        def raise_for_status(self):
            return None
        def json(self):
            return {'choices': [{'message': {'content': 'ПОВЕРКА | номер: 123'}}]}

    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: payloads.append(kwargs['json']) or Response())

    text = server.vision_extract(b'photo', 'поверка.jpg', 'unused', prompt_override=server.SPK_SI_VISION_PROMPT)

    assert 'номер: 123' in text
    assert payloads[0]['max_tokens'] == 8000


def test_spk_si_pages_are_requested_independently_and_returned_in_page_order(monkeypatch):
    monkeypatch.setattr(server, '_pdf_total_pages', lambda *_args: 2)
    monkeypatch.setattr(server, '_pdf_pages_to_images', lambda *_args, **_kwargs: ['cGFnZTE=', 'cGFnZTI='])
    calls = []
    monkeypatch.setattr(server.req_lib, 'post', lambda *_args, **kwargs: calls.append(kwargs) or type('Response', (), {
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
    assert {call['timeout'] for call in calls} == {70}


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

    assert '--- СТРАНИЦЫ 1-1 ---\na' in text
    assert '--- СТРАНИЦЫ 4-4 ---\nd' in text
    assert text.index('СТРАНИЦЫ 1-1') < text.index('СТРАНИЦЫ 4-4')


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

    def fake_post(*_args, **kwargs):
        page = next(
            block['image_url']['url'].rsplit(',', 1)[-1]
            for block in kwargs['json']['messages'][0]['content']
            if block['type'] == 'image_url'
        )
        calls.append(page)
        if page == 'a' and calls.count('a') == 1:
            raise server.req_lib.exceptions.Timeout()
        return Response()

    monkeypatch.setattr(server.req_lib, 'post', fake_post)

    progress = []
    text = server.vision_extract(b'pdf', 'поверка.pdf', 'unused', progress_cb=progress.append)

    assert text.count('Распознанный текст') == 2
    assert calls.count('a') == 2
    assert calls.count('b') == 1
    assert any('Страницы 1–1 читаются дольше обычного' in message for message in progress)


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

    assert 'Распознаю страницы 1–1 из 3' in progress
    assert 'Прочитаны страницы 1–1 из 3' in progress
    assert 'Распознаю страницы 2–2 из 3' in progress
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
