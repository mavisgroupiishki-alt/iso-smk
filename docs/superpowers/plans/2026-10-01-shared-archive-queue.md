# Shared Archive Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let several employees submit document archives independently without one person's OCR job rejecting or losing another person's file.

**Architecture:** Keep one memory-intensive OCR worker on the 512 MB web service, but persist every accepted upload as a durable FIFO task. The browser shows the queue position and continues polling; the worker starts the next task after cleanup. This avoids concurrent OCR OOM while removing cross-user refusal.

**Tech Stack:** Python `http.server`, persistent Render disk, vanilla JavaScript, pytest and Node regression tests.

## Global Constraints

- Preserve access isolation: a user can only view their own task.
- Do not run more than one archive OCR job at once on the current 512 MB instance.
- Reject only when persistent disk free space cannot safely stage another archive.
- User-facing messages must be in plain Russian without internal errors.

---

### Task 1: Durable FIFO scheduler

**Files:**
- Modify: `server.py` archive reservation, release, resume, and upload handlers.
- Test: `tests/test_archive_recognition.py`.

**Interfaces:**
- Produces `queue_archive_task(task_id)` and `start_next_archive_task()`.
- Each archive task uses `status: queued|running|done|error|cancelled` and `queued_at`.

- [x] Write tests that two archive tasks are accepted, ordered FIFO, and only the first starts.
- [x] Replace request-time refusal with durable `queued` task creation and a single worker dispatcher.
- [x] Start the next task after final cleanup and resume queued work after restart.
- [x] Verify queue tests and current archive tests.

### Task 2: Clear progress and safe capacity

**Files:**
- Modify: `server.py` task status response and staging checks.
- Modify: `index.html` archive progress display.
- Test: `tests/test_archive_recognition.py`, `tests/test_visual_upload_frontend.js`.

**Interfaces:**
- Browser receives `queuePosition` for queued tasks.
- Staging refusal describes lack of working space, never generic file-reading failure.

- [x] Add a free-space reservation before accepting each upload.
- [x] Return queue position in `/api/task/<id>`.
- [x] Render “in queue, ahead: N” while waiting; retain the existing finished-file path.
- [x] Verify original symptom and upload regressions.
