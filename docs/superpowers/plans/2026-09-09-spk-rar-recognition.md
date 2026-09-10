# SPK RAR Recognition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Process RAR uploads through the same PDF and image recognition path already used for ZIP uploads, so SPK source archives are not silently reduced to unreadable text files.

**Architecture:** Add a narrow RAR-to-ZIP adapter ahead of `extract_archive_with_vision`. It extracts only into a temporary directory using an installed archive utility, rejects unsafe paths, rebuilds an in-memory ZIP, then reuses the existing ZIP pipeline unchanged for file limits, PDF rendering, vision recognition, inventory, and product analysis.

**Tech Stack:** Python 3, `subprocess`, `zipfile`, pytest, Render native Python service.

## Global Constraints

- Do not invent SPK data when a source file cannot be read.
- Preserve the full folder path of every archive member in the inventory and recognition context.
- Process JPG, PDF, DOCX, XLSX, and other currently supported ZIP entries identically after RAR conversion.
- Return a user-readable error when the server has no RAR-capable utility; do not claim an unread file is absent.
- Keep ISO/SUOT generation separate from the SPK archive-recognition path.
- Production deployment is out of scope until the Render environment is deliberately configured with an RAR-capable extractor.

---

### Task 1: Cover RAR dispatch and failure reporting

**Files:**
- Modify: `tests/test_archive_recognition.py`
- Modify: `server.py:2051-2068`

**Interfaces:**
- Consumes: `extract_archive_with_vision(file_bytes, filename, api_key, progress_cb=None, product="all") -> dict`
- Produces: RAR inputs are converted before the existing ZIP reader is invoked; an unavailable extractor returns a visible `[RAR: ...]` error.

- [ ] **Step 1: Write the failing tests**

```python
import io
import zipfile

import server


def test_rar_upload_uses_converted_zip_pipeline(monkeypatch):
    converted = io.BytesIO()
    with zipfile.ZipFile(converted, "w") as archive:
        archive.writestr("СИ/свидетельство.pdf", b"pdf")

    monkeypatch.setattr(server, "_rar_to_zip_bytes", lambda *_: converted.getvalue())
    result = server.extract_archive_with_vision(b"rar", "Клиент.rar", "unused")

    assert "свидетельство.pdf" in result["text"]


def test_rar_upload_reports_missing_extractor(monkeypatch):
    monkeypatch.setattr(server, "_rar_to_zip_bytes", lambda *_: None)
    result = server.extract_archive_with_vision(b"rar", "Клиент.rar", "unused")

    assert result["text"].startswith("[RAR:")
    assert "не удалось распаковать" in result["text"].lower()
```

- [ ] **Step 2: Run the tests to verify failure**

Run: `pytest tests/test_archive_recognition.py -q`

Expected: FAIL because `extract_archive_with_vision` opens RAR bytes as a ZIP file.

- [ ] **Step 3: Implement the smallest dispatch point**

```python
ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
if ext == 'rar':
    file_bytes = _rar_to_zip_bytes(file_bytes, filename)
    if file_bytes is None:
        return {'text': '[RAR: не удалось распаковать архив на сервере.]',
                'structured_data': {}}
elif ext != 'zip':
    return {'text': '[Архив: поддерживаются ZIP и RAR.]', 'structured_data': {}}
```

- [ ] **Step 4: Run the tests to verify the dispatch semantics**

Run: `pytest tests/test_archive_recognition.py -q`

Expected: The RAR failure-reporting test passes; the converted-ZIP test can proceed once Task 2 supplies a fixture whose PDF text path is deterministic.

### Task 2: Add the safe RAR-to-ZIP adapter

**Files:**
- Modify: `server.py:before extract_archive_with_vision`
- Modify: `tests/test_archive_recognition.py`

**Interfaces:**
- Consumes: `_rar_to_zip_bytes(file_bytes: bytes, filename: str) -> bytes | None`
- Produces: A valid ZIP preserving relative member paths, or `None` after every supported extractor fails.

- [ ] **Step 1: Extend the failing test with a synthetic archive payload**

```python
def test_rar_converter_preserves_nested_member_paths(monkeypatch, tmp_path):
    def fake_extract(command, **_):
        output_dir = Path(command[command.index("-C") + 1])
        nested = output_dir / "ИК СПК инфа" / "СИ"
        nested.mkdir(parents=True)
        (nested / "свидетельство.pdf").write_bytes(b"pdf")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(server.subprocess, "run", fake_extract)
    converted = server._rar_to_zip_bytes(b"synthetic", "spk-client.rar")
    assert converted is not None
    with zipfile.ZipFile(io.BytesIO(converted)) as archive:
        assert "ИК СПК инфа/СИ/свидетельство.pdf" in archive.namelist()
```

- [ ] **Step 2: Implement extraction into an isolated temporary directory**

```python
def _rar_to_zip_bytes(file_bytes, filename):
    with tempfile.TemporaryDirectory(prefix="igor-rar-") as temp_dir:
        # Write the RAR to temp_dir/input.rar; run bsdtar, unrar, then 7z if present.
        # Return None only after every available command fails.
        # Rebuild a ZIP from regular files whose resolved path remains under temp_dir/extracted.
        # Store each filename with POSIX separators and preserve its relative directory path.
```

- [ ] **Step 3: Keep command diagnostics server-side and return a non-technical user error**

```python
print(f"  ⚠️ RAR {filename}: no supported extractor succeeded")
return None
```

- [ ] **Step 4: Run the focused tests**

Run: `pytest tests/test_archive_recognition.py -q`

Expected: PASS locally when `bsdtar` is available; otherwise the test skips with an explicit `shutil.which("bsdtar") is None` guard.

### Task 3: Verify the supplied SPK archive reaches the main recognition pipeline

**Files:**
- Modify: `tests/test_archive_recognition.py`

**Interfaces:**
- Consumes: `_rar_to_zip_bytes` and `extract_archive_with_vision`
- Produces: Evidence that the client archive contains every expected RAR member after conversion and that PDFs/images are classified as recognisable work items.

- [ ] **Step 1: Add an inventory-level regression test without adding client files to Git**

```python
def test_rar_converter_keeps_pdf_and_jpg_members(monkeypatch):
    def fake_extract(command, **_):
        output_dir = Path(command[command.index("-C") + 1])
        (output_dir / "СИ").mkdir(parents=True)
        (output_dir / "Люди").mkdir()
        (output_dir / "СИ" / "свидетельство.pdf").write_bytes(b"pdf")
        (output_dir / "Люди" / "трудовая.jpg").write_bytes(b"jpg")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(server.subprocess, "run", fake_extract)
    converted = server._rar_to_zip_bytes(b"synthetic", "spk-client.rar")
    with zipfile.ZipFile(io.BytesIO(converted)) as archive:
        members = archive.namelist()
    assert any(name.endswith(".pdf") and name.startswith("СИ/") for name in members)
    assert any(name.endswith(".jpg") and name.startswith("Люди/") for name in members)
```

- [ ] **Step 2: Run focused regression tests**

Run: `pytest tests/test_archive_recognition.py -q`

Expected: PASS.

- [ ] **Step 3: Run a manual, local check against the user-supplied RAR without copying it into Git**

Run: `python3 -c "import server; from pathlib import Path; data = Path('/path/to/client.rar').read_bytes(); converted = server._rar_to_zip_bytes(data, 'client.rar'); print(bool(converted))"`

Expected: `True`; inspect only the member count and extensions. Do not add, copy, or commit client documents or personal data.

- [ ] **Step 4: Run the relevant existing SPK tests**

Run: `pytest tests/test_feedback_v18.py -q`

Expected: PASS; no regression in the established rules for real SI data and the absence of a chief engineer.

- [ ] **Step 5: Check the diff before any commit**

Run: `git diff --check && git diff -- server.py tests/test_archive_recognition.py`

Expected: no whitespace errors; only the RAR adapter and tests are changed.

## Deployment Gate

The source code can support `bsdtar`, `unrar`, and `7z`. The repository includes a Dockerfile that installs `libarchive-tools` (`bsdtar`) and the Russian Tesseract data package. Render cannot change the runtime of an existing native Python service to Docker in place, so production migration must create a replacement Docker web service, copy the required environment variables and persistent data, then switch traffic only after an upload check confirms the archive is processed.

## Follow-up: ISO/СМК scope-specific output

- [ ] Generate an ISO package using a combined or non-standard scope.
- [ ] Confirm orders 3-СМК through 11-СМК show that exact scope in their heading and clauses.
- [ ] Confirm every report adds the exact scope under its heading.
- [ ] Confirm the customer-satisfaction report lists the exact object names and only the customer names supplied in the card.
