# SPK People SI and Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent duplicate or incomplete personnel rows in SPK documents, use SI facts from invoices and technical passports, and make the mandatory production-control policy unmistakable in BISP packages.

**Architecture:** Preserve every uncertain person in the intake card, but permit only a complete, deduplicated identity in formal SPK documents. Merge SI passport/invoice facts into instrument rows without treating them as a verification. Keep the existing policy template and rename its delivered document to the full required title.

**Tech Stack:** Python, python-docx template renderer, pytest.

## Global Constraints

- Do not invent handwritten diploma, work-book, serial-number, verification, or personnel data.
- A technical passport or invoice may prove an SI name, model, serial number, range, and quantity; it must never be labelled as a verification/calibration.
- The BISP director remains excluded from ITR and organisational-structure personnel rows.
- Retain all non-PTU diplomas and all supplied work-book numbers for accepted personnel.

---

### Task 1: Safe personnel canonicalisation

**Files:**
- Modify: `generator_spk_templates.py`.
- Test: `tests/test_spk_template_facts.py`.

**Interfaces:**
- Produces `_dedupe_spk_people(people) -> tuple[list[dict], list[str]]`.
- `generate_spk_package_v2()` consumes the deduplicated list and includes warnings for incomplete FIO.

- [x] Write a failing BISP test with the same employee represented twice and a separate surname-only row.
- [x] Verify that one merged employee appears in ITR, organisation structure and protocol; the surname-only row is absent and yields a plain review warning.
- [x] Implement name comparison using surname plus matching name/patronymic or initials; merge supplied document fields without replacing a filled field by an empty one.
- [x] Run the focused test and the SPK template test module.

### Task 2: SI facts from invoices and technical passports

**Files:**
- Modify: `server.py`.
- Test: `tests/test_archive_recognition.py`.

**Interfaces:**
- `_extract_spk_si_evidence(text)` returns invoice/passport tools in `measurement_tools` with `source: invoice_or_technical_passport`.
- It does not add an item to `verification_documents` or `calibration_documents` for those sources.

- [x] Write a failing parser test for a technical passport and an invoice containing an SI model, factory number, range and quantity.
- [x] Implement the narrowly-scoped source detector and field extraction.
- [x] Expand the SI scan-path detector only when an invoice/passport filename itself names an instrument or SI context.
- [x] Run archive-recognition tests and verify the SI table continues to reserve its verification column for actual certificates.

### Task 3: Required policy naming

**Files:**
- Modify: `generator_spk_templates.py`.
- Test: `tests/test_spk_template_facts.py`.

**Interfaces:**
- BISP output includes a filename containing `Положение о системе производственного контроля`.

- [x] Write a failing assertion against the BISP document list.
- [x] Rename the existing rendered policy file only; do not duplicate the document.
- [x] Run focused SPK tests and the full maintained SPK/archive regression group.
