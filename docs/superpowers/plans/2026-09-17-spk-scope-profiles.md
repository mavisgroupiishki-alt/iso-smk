# SPK Scope Profiles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate source-backed SPK documents for metal-structure production, construction-plus-metal, and low-voltage systems while preserving the current construction profile.

**Architecture:** Keep `spk_stroy` and `spk_bisp` as the issuing-body products. Store the activity in `spk.activity_profile` and pass it into deterministic Word renderers. The scope profile changes only scope-dependent sentences; company, personnel, TTK, SI, dates and template layout remain unchanged.

**Tech Stack:** Python, OOXML DOCX templates, browser JavaScript, existing SPK tests and rendered DOCX QA.

## Global Constraints

- Valid values: `construction`, `metal_only`, `construction_metal`, `low_voltage_systems`.
- The absent value means `construction`.
- Do not invent work types, TTK, technical rules, certificates or staff roles.
- The user-provided ZIPs are the authority for wording.
- The metal sample uses the combined formulation: construction works plus production of metal structures. The user also requires a separate metal-production-only option.
- The low-voltage sample includes electrical installation, low-voltage systems, fire alarm/firefighting, communications, dispatching, video surveillance, intrusion alarm and telecom cable structures.

---

### Task 1: Preserve the activity profile from chat to generator

**Files:**

- Modify: `server.py:704-712`, `server.py:835-860`, `server.py:1085-1127`
- Modify: `index.html:2718-2726`, `index.html:2896-2901`
- Test: `tests/test_spk_template_facts.py`, `tests/test_spk_frontend.js`

**Interfaces:**

- Consumes: `data.spk.activity_profile`.
- Produces: one validated profile stored in `company_data['spk']` and passed unchanged to `generate_spk_package_v2`.

- [ ] Write a failing test that `metal_structures` survives `_sanitize_ai_visible_response` for `spk_stroy`.
- [ ] Extend the SPK JSON schema and prompt with `activity_profile: construction|metal_only|construction_metal|low_voltage_systems`.
- [ ] Map “только металл” to `metal_only`, “СМР и металлоконструкции” to `construction_metal`, and “слаботочка”, “пожарная сигнализация”, “видеонаблюдение”, “охранная сигнализация” and “связь” to `low_voltage_systems`.
- [ ] Reject unknown profile strings to `construction` and add a visible review question instead of guessing.
- [ ] Merge and display this scalar in the browser data card without changing `certification.standard`.
- [ ] Run `python3 -m py_compile server.py` and `node tests/test_spk_frontend.js`.

### Task 2: Add scope wording profiles to the deterministic templates

**Files:**

- Modify: `generator_spk_templates.py:155-259`, `generator_spk_templates.py:344-441`, `generator_spk_templates.py:498-606`, `generator_spk_templates.py:788-936`
- Test: `tests/test_spk_template_facts.py`

**Interfaces:**

- Consumes: `spk_data['activity_profile']`.
- Produces: profile-specific wording in the order, internal-training order, training protocol, regulation and ITR responsibilities.

- [ ] Write failing tests for the metal order and low-voltage training protocol.
- [ ] Add one `SPK_ACTIVITY_PROFILES` table with source-backed `order_purpose`, `training_scope`, `protocol_scope`, `regulation_scope` and `director_responsibility` strings.
- [ ] Pass the selected profile through `render_prikaz_spk`, `render_prikaz_obuchenie`, `render_protokol_obuchenie`, `render_polozhenie` and `render_spravka_itr`.
- [ ] Locate and replace full paragraphs by stable source markers; do not globally replace “СМР”.
- [ ] Support “Мастер производственного участка” as the operational responsible role for metal and “Заместитель директора — главный инженер” for low voltage. Retain the yellow review warning if no confirmed role is present.
- [ ] Leave premises, organisational structure, passport, TTK and SI templates unchanged except for existing company/person/data substitutions.
- [ ] Run the full `tests/test_spk_template_facts.py` set.

### Task 3: Render QA, regression checks and release

**Files:**

- Create: temporary `tmp/spk-profile-qa/` artefacts only
- Modify: `README_ЗАМЕНА_v21.txt`

**Interfaces:**

- Consumes: generated DOCX bytes for each profile.
- Produces: visual evidence that the changed documents have clean layout and an end-user release note.

- [ ] Generate one neutral fixture package for `metal_only`, one for `construction_metal`, one for `low_voltage_systems`, and one construction control package.
- [ ] Render every changed DOCX with `render_docx.py` and inspect the PNG pages at full size.
- [ ] Confirm each package contains only its own scope wording and that dates, signatures, tables and yellow review tokens are intact.
- [ ] Run `git diff --check`, `python3 -m py_compile server.py generator.py generator_spk_templates.py`, `node tests/test_spk_frontend.js`, and focused SPK tests.
- [ ] Update the Russian release note, commit, push and verify the deployed version.

## Self-review

- Tasks 1–2 cover data capture, deterministic rendering and profile-specific roles.
- Task 3 covers visual layout, regression isolation and release verification.
- Metal-profile scope is confirmed: implement both production-only and the supplied combined construction-plus-metal wording.
