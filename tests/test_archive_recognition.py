import io
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

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


def test_archive_processing_slot_rejects_parallel_jobs():
    server.release_archive_processing()

    assert server.reserve_archive_processing() is True
    assert server.reserve_archive_processing() is False

    server.release_archive_processing()
    assert server.reserve_archive_processing() is True
    server.release_archive_processing()


def test_archive_reads_pdfs_serially_but_keeps_photos_parallel():
    entries = [
        ('one.pdf', 'one.pdf', 5 * 1024 * 1024, 'pdf'),
        ('two.jpg', 'two.jpg', 1 * 1024 * 1024, 'image'),
        ('three.jpg', 'three.jpg', 1 * 1024 * 1024, 'image'),
    ]

    batches = server._archive_vision_batches(entries)

    assert [(len(batch), workers) for batch, workers in batches] == [(1, 1), (2, 2)]


def test_rar_upload_uses_the_existing_zip_recognition_pipeline(monkeypatch):
    monkeypatch.setattr(server, '_rar_to_zip_bytes', lambda *_: _zip_with_text_file())
    monkeypatch.setattr(server, '_reconcile_all_people', lambda texts, *_args, **_kwargs: texts)

    result = server.extract_archive_with_vision(b'not-a-real-rar', 'СПК.rar', 'unused')

    assert 'СИ/перечень.txt' in result['text']
    assert 'рулетка' in result['text']


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


def test_rar_upload_reports_an_unavailable_extractor(monkeypatch):
    monkeypatch.setattr(server, '_rar_to_zip_bytes', lambda *_: None)

    result = server.extract_archive_with_vision(b'not-a-real-rar', 'СПК.rar', 'unused')

    assert result['text'].startswith('[RAR:')
    assert 'не удалось открыть' in result['text'].lower()


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
