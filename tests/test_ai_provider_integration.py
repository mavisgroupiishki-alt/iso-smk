"""Safe integration checks: existing entry points must use the central router."""
import tempfile
import unittest
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import generator
import server
from ai_router import AIProviderRouter, AIProviderError


class Response:
    status_code = 200
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class ProviderIntegrationTests(unittest.TestCase):
    def build_router(self, temp, payload):
        env = {"GEMINI_API_KEY": "test-key"}
        return AIProviderRouter(
            Path(temp) / "ai", environ=env,
            http_post=lambda *_args, **_kwargs: Response(payload),
            now=lambda: datetime.fromisoformat("2026-10-16T00:00:00+00:00"),
        )

    def test_chat_entry_point_uses_gemini_first(self):
        with tempfile.TemporaryDirectory() as temp:
            router = self.build_router(temp, {"candidates": [{"content": {"parts": [{"text": '{"message":"готово","questions":[],"data":{}}'}]}}]})
            with patch.object(server, "AI_ROUTER", router):
                answer = server.call_ai([{"role": "user", "content": "безопасный пример"}], "legacy-vibe-key", product="iso")
        self.assertIn("готово", answer)

    def test_chat_rejects_non_json_reply_before_showing_it_to_employee(self):
        with tempfile.TemporaryDirectory() as temp:
            calls = []
            def post(url, **_kwargs):
                calls.append(url)
                if "generativelanguage" in url:
                    return Response({"candidates": [{"content": {"parts": [{"text": "не по формату"}]}}]})
                return Response({"choices": [{"message": {"content": '{"message":"проверено","questions":[],"data":{}}'}}]})
            router = AIProviderRouter(
                Path(temp) / "ai",
                environ={"GEMINI_API_KEY": "test", "OPENROUTER_API_KEY": "test"},
                http_post=post,
                now=lambda: datetime.fromisoformat("2026-10-16T00:00:00+00:00"),
            )
            with patch.object(server, "AI_ROUTER", router):
                answer = server.call_ai([{"role": "user", "content": "безопасный пример"}], "", product="iso")
        self.assertIn("проверено", answer)
        self.assertEqual(len(calls), 2)

    def test_generator_entry_point_uses_router_without_direct_vibe_call(self):
        with tempfile.TemporaryDirectory() as temp:
            router = self.build_router(temp, {"candidates": [{"content": {"parts": [{"text": "Текст документа"}]}}]})
            with patch.object(generator, "_AI_ROUTER", router):
                answer = generator.vibe_call([{"role": "user", "content": "создай тест"}], "legacy-vibe-key")
        self.assertEqual(answer, "Текст документа")

    def test_paid_budget_pause_keeps_generation_input_in_queue(self):
        task_id = "budget-pause-safe-test"
        task = {
            "kind": "generation", "status": "running", "owner_user_id": "test",
            "_generation_payload": {"ai_data": {"company": {"name": "Тест"}}, "product": "iso", "user_role": "owner"},
        }
        with patch.dict(server.TASKS, {task_id: task}, clear=True), \
             patch.object(server, "generate_package", side_effect=AIProviderError("paid_budget_exhausted")), \
             patch.object(server, "save_task"), \
             patch.object(server, "start_next_generation_task"), \
             patch.object(server, "release_generation_processing"):
            server._run_generation_task(task_id)
        self.assertEqual(task["status"], "awaiting_budget")
        self.assertIn("_generation_payload", task)

    def test_restart_requeues_only_a_budget_paused_generation(self):
        with tempfile.TemporaryDirectory() as temp:
            task_id = "budget-paused-after-config-update"
            task = {
                "kind": "generation", "status": "awaiting_budget",
                "_generation_payload": {"ai_data": {"company": {"name": "Тест"}}, "product": "iso"},
                "progress": ["Остановлено без списания"],
            }
            Path(temp, f"{task_id}.json").write_text(json.dumps(task), encoding="utf-8")
            with patch.object(server, "TASKS_DIR", Path(temp)), \
                patch.dict(server.TASKS, {}, clear=True), \
                 patch.object(server, "start_next_generation_task") as start:
                resumed = server._resume_budget_generation_tasks()
                self.assertEqual(resumed, 1)
                self.assertEqual(server.TASKS[task_id]["status"], "queued")
                start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
