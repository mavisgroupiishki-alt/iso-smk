import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ai_router import AIProviderRouter, AIProviderError


class FakeResponse:
    def __init__(self, payload, status=200, headers=None):
        self.payload = payload
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            error = RuntimeError(f"HTTP {self.status_code}")
            error.status_code = self.status_code
            raise error

    def json(self):
        return self.payload


class AIRouterTests(unittest.TestCase):
    def router(self, env, post, at="2026-10-16T12:00:00+00:00"):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        now = lambda: datetime.fromisoformat(at)
        return AIProviderRouter(Path(temp.name), environ=env, http_post=post, now=now)

    def test_quota_failure_switches_from_gemini_to_free_openrouter_and_caches(self):
        calls = []
        def post(url, **kwargs):
            calls.append(url)
            if "generativelanguage" in url:
                return FakeResponse({"error": {"status": "RESOURCE_EXHAUSTED"}}, 429, {"Retry-After": "60"})
            self.assertIn(":free", kwargs["json"]["model"])
            return FakeResponse({"choices": [{"message": {"content": "готово"}}], "usage": {"prompt_tokens": 10, "completion_tokens": 2}})

        router = self.router({"GEMINI_API_KEY": "test", "OPENROUTER_API_KEY": "test"}, post)
        first = router.complete_text(system="system", messages=[{"role": "user", "content": "проверка"}], task_type="safe_test")
        second = router.complete_text(system="system", messages=[{"role": "user", "content": "проверка"}], task_type="safe_test")
        self.assertEqual((first.provider, first.text), ("openrouter", "готово"))
        self.assertTrue(second.cached)
        self.assertEqual(len(calls), 2)

    def test_invalid_structured_answer_falls_back_without_paid_model(self):
        calls = []
        def post(url, **kwargs):
            calls.append(kwargs["json"]["model"])
            if "generativelanguage" in url:
                return FakeResponse({"candidates": [{"content": {"parts": [{"text": "не JSON"}]}}]})
            return FakeResponse({"choices": [{"message": {"content": '{"company":"ООО Тест"}'}}]})

        router = self.router({"GEMINI_API_KEY": "test", "OPENROUTER_API_KEY": "test"}, post)
        result = router.complete_text(system="", messages=[{"role": "user", "content": "json"}], require_json=True, required_fields=("company",))
        self.assertEqual(result.provider, "openrouter")
        self.assertTrue(all(model.endswith(":free") or model.startswith("gemini") for model in calls))

    def test_audio_starts_with_groq_whisper(self):
        seen = []
        def post(url, **kwargs):
            seen.append((url, kwargs["files"]["model"][1]))
            return FakeResponse({"text": "тестовая расшифровка", "duration": 2.5})

        router = self.router({"GROQ_API_KEY": "test"}, post)
        result = router.transcribe_audio(audio_bytes=b"audio", filename="call.mp3")
        self.assertEqual(result.provider, "groq")
        self.assertEqual(seen[0][1], "whisper-large-v3-turbo")

    def test_aiai_never_runs_without_explicit_budget_and_price(self):
        calls = []
        router = self.router(
            {"AIAI_API_KEY": "test", "AIAI_BASE_URL": "https://example.invalid/v1", "AIAI_MODEL": "model", "AIAI_PAID_ENABLED": "true"},
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )
        with self.assertRaises(AIProviderError):
            router.complete_text(system="", messages=[{"role": "user", "content": "тест"}])
        self.assertEqual(calls, [])

    def test_openrouter_non_free_model_is_not_sent(self):
        calls = []
        router = self.router(
            {"OPENROUTER_API_KEY": "test", "OPENROUTER_TEXT_MODEL": "openai/gpt-4o"},
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )
        with self.assertRaises(AIProviderError):
            router.complete_text(system="", messages=[{"role": "user", "content": "тест"}])
        self.assertEqual(calls, [])

    def test_cloudflare_stops_before_its_free_daily_neuron_limit(self):
        calls = []
        router = self.router(
            {
                "CLOUDFLARE_API_TOKEN": "test", "CLOUDFLARE_ACCOUNT_ID": "account",
                "CLOUDFLARE_FREE_DAILY_NEURONS": "1000", "CLOUDFLARE_REQUEST_NEURONS": "1001",
            },
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )
        with self.assertRaises(AIProviderError):
            router.complete_text(system="", messages=[{"role": "user", "content": "тест"}])
        self.assertEqual(calls, [])

    def test_cloudflare_cooldown_does_not_reserve_more_free_quota(self):
        router = self.router(
            {
                "CLOUDFLARE_API_TOKEN": "test", "CLOUDFLARE_ACCOUNT_ID": "account",
                "CLOUDFLARE_REQUEST_NEURONS": "500",
            },
            lambda *_args, **_kwargs: FakeResponse({"result": {"response": "ok"}}),
        )
        router._set_cooldown("cloudflare", 60)
        with self.assertRaises(AIProviderError):
            router.complete_text(system="", messages=[{"role": "user", "content": "тест"}])
        events = [json.loads(line) for line in router.usage_file.read_text("utf-8").splitlines()]
        self.assertFalse(any(event["outcome"] == "reserved" for event in events))

    def test_vibecode_is_not_called_before_reset_then_returns_to_chain(self):
        before_calls = []
        before = self.router(
            {"VIBE_API_KEY": "test"}, lambda *args, **kwargs: before_calls.append((args, kwargs)),
            at="2026-11-04T23:59:59+00:00",
        )
        with self.assertRaises(AIProviderError):
            before.complete_text(system="", messages=[{"role": "user", "content": "тест"}])
        self.assertEqual(before_calls, [])

        after_calls = []
        def post(_url, **kwargs):
            after_calls.append(kwargs["json"]["model"])
            return FakeResponse({"choices": [{"message": {"content": "ответ после сброса"}}]})
        after = self.router({"VIBE_API_KEY": "test"}, post, at="2026-11-05T00:00:00+00:00")
        result = after.complete_text(system="", messages=[{"role": "user", "content": "тест"}])
        self.assertEqual(result.provider, "vibecode")
        self.assertEqual(after_calls, ["bitrix/bitrixgpt-5.5"])


if __name__ == "__main__":
    unittest.main()
