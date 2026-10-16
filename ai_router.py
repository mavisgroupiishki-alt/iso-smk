"""Server-side AI routing with cache, quota cooldowns and auditable usage.

The router deliberately has no browser dependencies and never persists prompts,
documents or credentials in the usage log.  It is synchronous because the
existing generator/server entry points are synchronous.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import requests


VIBE_REENABLE_AT = "2026-11-05T00:00:00+00:00"


class AIProviderError(RuntimeError):
    """A safe, provider-neutral error for callers and jobs."""


@dataclass(frozen=True)
class AIResult:
    text: str
    provider: str
    model: str
    cached: bool = False
    usage: dict[str, Any] | None = None
    cost: float | None = None


@dataclass(frozen=True)
class Candidate:
    provider: str
    model: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_float(value: Any) -> Optional[float]:
    try:
        number = float(str(value).strip())
        return number if number >= 0 else None
    except (TypeError, ValueError):
        return None


class AIProviderRouter:
    """Route compatible AI requests without silently crossing paid boundaries."""

    def __init__(
        self,
        state_dir: str | Path,
        *,
        environ: Optional[dict[str, str]] = None,
        http_post: Optional[Callable[..., Any]] = None,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.env = environ if environ is not None else os.environ
        # Keep the default lookup dynamic: existing safe tests patch
        # ``requests.post`` and production can still inject a dedicated
        # transport. Never capture a credential-bearing client globally.
        self.post = http_post
        self.now = now
        configured = self.env.get("AI_SHARED_QUOTA_LEDGER_DIR") or state_dir
        self.state_dir = Path(configured).expanduser().resolve()
        self.cache_dir = self.state_dir / "cache"
        self.state_file = self.state_dir / "provider_state.json"
        self.usage_file = self.state_dir / "usage.jsonl"
        self._lock = threading.RLock()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------- public request methods -----------------------
    def complete_text(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        max_tokens: int = 3000,
        task_type: str = "text",
        require_json: bool = False,
        required_fields: Iterable[str] = (),
        cache_ttl: int | None = None,
        legacy_vibe_key: str = "",
    ) -> AIResult:
        payload = {
            "system": str(system or ""),
            "messages": self._compact_messages(messages),
            "max_tokens": min(max(64, int(max_tokens or 3000)), 8000),
        }
        return self._run(
            kind="text", task_type=task_type, payload=payload,
            candidates=self._text_candidates(), require_json=require_json,
            required_fields=tuple(required_fields), cache_ttl=cache_ttl,
            legacy_vibe_key=legacy_vibe_key,
        )

    def read_image(
        self,
        *,
        image_b64: str,
        prompt: str,
        mime_type: str = "image/jpeg",
        max_tokens: int = 3500,
        task_type: str = "document_ocr",
        cache_ttl: int | None = None,
        legacy_vibe_key: str = "",
    ) -> AIResult:
        if not image_b64:
            raise AIProviderError("Нет изображения для распознавания.")
        payload = {
            "image_b64": str(image_b64),
            "prompt": str(prompt or ""),
            "mime_type": str(mime_type or "image/jpeg"),
            "max_tokens": min(max(128, int(max_tokens or 3500)), 8000),
        }
        return self._run(
            kind="vision", task_type=task_type, payload=payload,
            candidates=self._vision_candidates(), cache_ttl=cache_ttl,
            legacy_vibe_key=legacy_vibe_key,
        )

    def transcribe_audio(
        self,
        *,
        audio_bytes: bytes,
        filename: str,
        mime_type: str = "audio/mpeg",
        task_type: str = "audio_transcription",
        cache_ttl: int | None = None,
    ) -> AIResult:
        if not audio_bytes:
            raise AIProviderError("Нет аудио для расшифровки.")
        payload = {
            "audio_b64": base64.b64encode(audio_bytes).decode("ascii"),
            "filename": str(filename or "audio"),
            "mime_type": str(mime_type or "audio/mpeg"),
        }
        return self._run(
            kind="audio", task_type=task_type, payload=payload,
            candidates=self._audio_candidates(), cache_ttl=cache_ttl,
        )

    def status(self) -> dict[str, Any]:
        """Safe operational status; no provider key or prompt is exposed."""
        return {
            "quota_scope": "shared" if self.env.get("AI_SHARED_QUOTA_LEDGER_DIR") else "local",
            "vibecode_available_after": VIBE_REENABLE_AT,
            "aiai_enabled": self._aiai_enabled(),
            "cooldowns": self._load_state().get("cooldowns", {}),
        }

    # -------------------------- routing policy ---------------------------
    def _text_candidates(self) -> list[Candidate]:
        candidates = [
            Candidate("gemini", self.env.get("GEMINI_TEXT_MODEL", "gemini-2.5-flash-lite")),
            Candidate("openrouter", self.env.get("OPENROUTER_TEXT_MODEL", "meta-llama/llama-3.3-70b-instruct:free")),
            Candidate("cloudflare", self.env.get("CLOUDFLARE_TEXT_MODEL", "@cf/meta/llama-3.3-70b-instruct-fp8-fast")),
        ]
        if self._vibe_allowed():
            candidates.append(Candidate("vibecode", self.env.get("VIBE_MODEL", "bitrix/bitrixgpt-5.5")))
        candidates.append(Candidate("aiai", self.env.get("AIAI_MODEL", "")))
        return candidates

    def _vision_candidates(self) -> list[Candidate]:
        candidates = [
            Candidate("gemini", self.env.get("GEMINI_VISION_MODEL", self.env.get("GEMINI_TEXT_MODEL", "gemini-2.5-flash-lite"))),
        ]
        # Vision over OpenRouter is never guessed: a configured free vision
        # model is required because not every :free model accepts images.
        configured_or_vision = self.env.get("OPENROUTER_VISION_MODEL", "")
        if configured_or_vision.endswith(":free"):
            candidates.append(Candidate("openrouter", configured_or_vision))
        candidates.append(Candidate("cloudflare", self.env.get("CLOUDFLARE_VISION_MODEL", "@cf/meta/llama-3.2-11b-vision-instruct")))
        if self._vibe_allowed():
            candidates.append(Candidate("vibecode", self.env.get("VIBE_VISION_MODEL", "bitrix/bitrixgpt-5.5-thinking")))
        candidates.append(Candidate("aiai", self.env.get("AIAI_VISION_MODEL", self.env.get("AIAI_MODEL", ""))))
        return candidates

    def _audio_candidates(self) -> list[Candidate]:
        candidates = [Candidate("groq", self.env.get("GROQ_WHISPER_MODEL", "whisper-large-v3-turbo"))]
        if self._vibe_allowed():
            candidates.append(Candidate("vibecode", self.env.get("VIBE_TRANSCRIBE_MODEL", "bitrix/deepdml/faster-whisper-large-v3-turbo-ct2")))
        candidates.append(Candidate("aiai", self.env.get("AIAI_AUDIO_MODEL", "")))
        return candidates

    # --------------------------- main mechanism --------------------------
    def _run(
        self, *, kind: str, task_type: str, payload: dict[str, Any], candidates: list[Candidate],
        require_json: bool = False, required_fields: tuple[str, ...] = (),
        cache_ttl: int | None = None, legacy_vibe_key: str = "",
    ) -> AIResult:
        cache_key = self._cache_key(kind, task_type, payload, require_json, required_fields)
        ttl = int(cache_ttl if cache_ttl is not None else self.env.get("AI_CACHE_TTL_SECONDS", "86400"))
        cached = self._get_cache(cache_key, ttl)
        if cached:
            return AIResult(cached["text"], cached["provider"], cached["model"], True, cached.get("usage"), cached.get("cost"))

        failures: list[str] = []
        for candidate in candidates:
            reason = self._candidate_unavailable(candidate, kind, payload, task_type)
            if reason:
                failures.append(f"{candidate.provider}:{reason}")
                self._record(candidate, task_type, "skipped", reason=reason)
                continue
            try:
                result = self._call(candidate, kind, payload, legacy_vibe_key)
                self._validate(result.text, require_json=require_json, required_fields=required_fields)
                self._save_cache(cache_key, result)
                self._record(candidate, task_type, "success", usage=result.usage, cost=result.cost,
                             switch_reason=failures[-1] if failures else "")
                return result
            except Exception as exc:
                classification, retry_after = self._classify_error(exc)
                failures.append(f"{candidate.provider}:{classification}")
                self._record(candidate, task_type, "failure", reason=classification,
                             switch_reason=failures[-2] if len(failures) > 1 else "")
                if classification in {"quota", "temporary", "quality"}:
                    self._set_cooldown(candidate.provider, retry_after)

        reason = ";".join(failures[-8:])
        self._record(Candidate("router", ""), task_type, "exhausted", reason=reason)
        if any(item.startswith("aiai:budget") for item in failures):
            raise AIProviderError("paid_budget_exhausted")
        raise AIProviderError("providers_exhausted")

    def _call(self, candidate: Candidate, kind: str, payload: dict[str, Any], legacy_vibe_key: str) -> AIResult:
        if candidate.provider == "gemini":
            return self._call_gemini(candidate, kind, payload)
        if candidate.provider == "openrouter":
            return self._call_openrouter(candidate, kind, payload)
        if candidate.provider == "cloudflare":
            return self._call_cloudflare(candidate, kind, payload)
        if candidate.provider == "vibecode":
            return self._call_vibecode(candidate, kind, payload, legacy_vibe_key)
        if candidate.provider == "groq":
            return self._call_groq(candidate, payload)
        if candidate.provider == "aiai":
            return self._call_aiai(candidate, kind, payload)
        raise AIProviderError("Неизвестный провайдер.")

    # --------------------------- provider adapters -----------------------
    def _call_gemini(self, candidate: Candidate, kind: str, payload: dict[str, Any]) -> AIResult:
        key = self._secret("GEMINI_API_KEY")
        url = "https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent" % candidate.model
        if kind == "vision":
            parts = [
                {"inline_data": {"mime_type": payload["mime_type"], "data": payload["image_b64"]}},
                {"text": payload["prompt"]},
            ]
            body = {"contents": [{"parts": parts}], "generationConfig": {"temperature": 0, "maxOutputTokens": payload["max_tokens"]}}
        else:
            contents = []
            for item in payload["messages"]:
                contents.append({"role": "model" if item["role"] == "assistant" else "user", "parts": [{"text": item["content"]}]})
            body = {"contents": contents, "generationConfig": {"temperature": 0, "maxOutputTokens": payload["max_tokens"]}}
            if payload["system"]:
                body["systemInstruction"] = {"parts": [{"text": payload["system"]}]}
        response = self._post(url, headers={"Content-Type": "application/json", "x-goog-api-key": key}, json=body, timeout=self._timeout(kind))
        data = self._json_response(response)
        text = "\n".join(str(part.get("text") or "").strip() for part in ((data.get("candidates") or [{}])[0].get("content") or {}).get("parts", []) if isinstance(part, dict)).strip()
        return AIResult(text, "gemini", candidate.model, usage=self._usage(data))

    def _call_openrouter(self, candidate: Candidate, kind: str, payload: dict[str, Any]) -> AIResult:
        if not candidate.model.endswith(":free"):
            raise AIProviderError("openrouter_paid_model_blocked")
        key = self._secret("OPENROUTER_API_KEY")
        messages = self._openai_messages(kind, payload)
        response = self._post("https://openrouter.ai/api/v1/chat/completions", headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, json={"model": candidate.model, "messages": messages, "temperature": 0, "max_tokens": payload["max_tokens"]}, timeout=self._timeout(kind))
        data = self._json_response(response)
        text = self._openai_text(data)
        return AIResult(text, "openrouter", candidate.model, usage=self._usage(data), cost=None)

    def _call_cloudflare(self, candidate: Candidate, kind: str, payload: dict[str, Any]) -> AIResult:
        token = self._secret("CLOUDFLARE_API_TOKEN")
        account = self._secret("CLOUDFLARE_ACCOUNT_ID")
        url = f"https://api.cloudflare.com/client/v4/accounts/{account}/ai/run/{candidate.model}"
        if kind == "vision":
            body = {"messages": self._openai_messages(kind, payload), "max_tokens": payload["max_tokens"], "options": {"rejectIfBusy": True}}
        else:
            body = {"messages": self._openai_messages(kind, payload), "max_tokens": payload["max_tokens"], "options": {"rejectIfBusy": True}}
        response = self._post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json=body, timeout=self._timeout(kind))
        data = self._json_response(response)
        result = data.get("result") or {}
        text = str(result.get("response") or result.get("description") or result.get("text") or "").strip()
        if not text and isinstance(result.get("messages"), list):
            text = self._openai_text({"choices": [{"message": result["messages"][-1]}]})
        return AIResult(text, "cloudflare", candidate.model, usage=self._usage(data))

    def _call_vibecode(self, candidate: Candidate, kind: str, payload: dict[str, Any], legacy_key: str) -> AIResult:
        key = legacy_key or self._secret("VIBE_API_KEY")
        if kind == "audio":
            audio = base64.b64decode(payload["audio_b64"])
            response = self._post(
                "https://vibecode.bitrix24.tech/v1/audio/transcriptions",
                headers={"X-Api-Key": key},
                files={"file": (payload["filename"], audio, payload["mime_type"])},
                data={"model": candidate.model}, timeout=self._timeout(kind),
            )
            data = self._json_response(response)
            return AIResult(str(data.get("text") or "").strip(), "vibecode", candidate.model, usage=self._usage(data))
        response = self._post("https://vibecode.bitrix24.tech/v1/ai/chat/completions", headers={"Content-Type": "application/json", "X-Api-Key": key}, json={"model": candidate.model, "max_tokens": payload["max_tokens"], "messages": self._openai_messages(kind, payload)}, timeout=self._timeout(kind))
        data = self._json_response(response)
        return AIResult(self._openai_text(data), "vibecode", candidate.model, usage=self._usage(data))

    def _call_groq(self, candidate: Candidate, payload: dict[str, Any]) -> AIResult:
        key = self._secret("GROQ_API_KEY")
        audio = base64.b64decode(payload["audio_b64"])
        response = self._post("https://api.groq.com/openai/v1/audio/transcriptions", headers={"Authorization": f"Bearer {key}"}, files={"file": (payload["filename"], audio, payload["mime_type"]), "model": (None, candidate.model), "response_format": (None, "verbose_json")}, timeout=self._timeout("audio"))
        data = self._json_response(response)
        duration = data.get("duration")
        usage = {"audio_seconds": duration} if duration is not None else {}
        return AIResult(str(data.get("text") or "").strip(), "groq", candidate.model, usage=usage)

    def _call_aiai(self, candidate: Candidate, kind: str, payload: dict[str, Any]) -> AIResult:
        if not self._aiai_enabled():
            raise AIProviderError("paid_budget_not_configured")
        reserved_cost = self._configured_aiai_price()
        if reserved_cost is None or not self._within_aiai_budget(reserved_cost):
            raise AIProviderError("paid_budget_exhausted")
        key = self._secret("AIAI_API_KEY")
        base_url = self._required("AIAI_BASE_URL").rstrip("/")
        if kind == "audio":
            audio = base64.b64decode(payload["audio_b64"])
            response = self._post(base_url + "/audio/transcriptions", headers={"Authorization": f"Bearer {key}"}, files={"file": (payload["filename"], audio, payload["mime_type"]), "model": (None, candidate.model)}, timeout=self._timeout(kind))
            data = self._json_response(response)
            text = str(data.get("text") or "").strip()
        else:
            response = self._post(base_url + "/chat/completions", headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"}, json={"model": candidate.model, "messages": self._openai_messages(kind, payload), "temperature": 0, "max_tokens": payload.get("max_tokens", 3000)}, timeout=self._timeout(kind))
            data = self._json_response(response)
            text = self._openai_text(data)
        usage = self._usage(data)
        cost = self._aiai_cost(usage)
        return AIResult(text, "aiai", candidate.model, usage=usage, cost=cost)

    # --------------------------- safeguards ------------------------------
    def _candidate_unavailable(self, candidate: Candidate, kind: str, payload: dict[str, Any], task_type: str) -> str:
        if candidate.provider == "aiai" and not self._aiai_enabled():
            return "paid_budget_not_configured"
        if candidate.provider == "vibecode" and not self._vibe_allowed():
            return "vibecode_waiting_for_reset"
        if candidate.provider == "openrouter" and not candidate.model.endswith(":free"):
            return "openrouter_paid_model_blocked"
        if not candidate.model:
            return "model_not_configured"
        env_key = {"gemini": "GEMINI_API_KEY", "openrouter": "OPENROUTER_API_KEY", "cloudflare": "CLOUDFLARE_API_TOKEN", "vibecode": "VIBE_API_KEY", "groq": "GROQ_API_KEY", "aiai": "AIAI_API_KEY"}.get(candidate.provider)
        if env_key and not self.env.get(env_key):
            return "key_not_configured"
        if candidate.provider == "cloudflare" and not self.env.get("CLOUDFLARE_ACCOUNT_ID"):
            return "account_not_configured"
        until = self._load_state().get("cooldowns", {}).get(candidate.provider)
        if until and float(until) > time.time():
            return "cooldown"
        if candidate.provider == "cloudflare":
            # The Workers Paid plan begins charging after its included daily
            # allocation.  Reserve a conservative, model-aware number of
            # neurons before the request, so a fallback never silently crosses
            # that boundary.  An unsuccessful reservation remains consumed for
            # the day: this deliberately favors cost safety over one more retry.
            reserve = self._estimate_cloudflare_neurons(candidate, kind, payload)
            if not self._reserve_cloudflare_free_neurons(candidate, task_type, reserve):
                return "free_neuron_quota_exhausted"
        return ""

    def _vibe_allowed(self) -> bool:
        try:
            return self.now() >= datetime.fromisoformat(VIBE_REENABLE_AT)
        except ValueError:
            return False

    def _aiai_enabled(self) -> bool:
        return (
            str(self.env.get("AIAI_PAID_ENABLED", "")).lower() == "true"
            and bool(self.env.get("AIAI_API_KEY")) and bool(self.env.get("AIAI_BASE_URL"))
            and _as_float(self.env.get("AIAI_DAILY_BUDGET_BEL")) not in (None, 0)
            and _as_float(self.env.get("AIAI_MONTHLY_BUDGET_BEL")) not in (None, 0)
            and self._configured_aiai_price() is not None
        )

    def _configured_aiai_price(self) -> Optional[float]:
        fixed = _as_float(self.env.get("AIAI_REQUEST_COST_BEL"))
        if fixed is not None and fixed > 0:
            return fixed
        # A per-token price alone cannot prove a hard cap before a request is
        # sent (the provider may emit more tokens than expected).  Require a
        # conservative fixed reservation until a proper shared reservation
        # service is configured; this is intentionally fail-closed for paid AI.
        return None

    def _estimate_cloudflare_neurons(self, candidate: Candidate, kind: str, payload: dict[str, Any]) -> int:
        """Bound free-plan use before a Workers AI request.

        Estimates use the public token-to-neuron prices for the two built-in
        defaults.  An operator can raise/lower the reserve per request for a
        custom model, but it can never make the daily included allocation
        larger than the configured ceiling.
        """
        override = _as_float(self.env.get("CLOUDFLARE_REQUEST_NEURONS"))
        if override is not None and override > 0:
            return int(override)
        rates = {
            "@cf/meta/llama-3.3-70b-instruct-fp8-fast": (26668, 204805),
            "@cf/meta/llama-3.2-11b-vision-instruct": (4410, 61493),
        }
        input_rate, output_rate = rates.get(candidate.model, (50000, 250000))
        if kind == "vision":
            # Image accounting differs per model; reserve a bounded extra
            # allowance rather than pretending base64 characters are tokens.
            input_tokens = 8000
        else:
            input_tokens = sum(len(str(item.get("content") or "")) for item in payload.get("messages") or []) // 4
            input_tokens += len(str(payload.get("system") or "")) // 4
        output_tokens = int(payload.get("max_tokens") or 3000)
        return max(1, int((input_tokens * input_rate + output_tokens * output_rate) / 1_000_000) + 1)

    def _reserve_cloudflare_free_neurons(self, candidate: Candidate, task_type: str, neurons: int) -> bool:
        limit = _as_float(self.env.get("CLOUDFLARE_FREE_DAILY_NEURONS", "10000"))
        if limit is None or limit <= 0:
            return False
        day_prefix = self.now().strftime("%Y-%m-%d")
        with self._lock:
            used = 0.0
            if self.usage_file.exists():
                for line in self.usage_file.read_text("utf-8").splitlines():
                    try:
                        event = json.loads(line)
                        if (event.get("provider") == "cloudflare" and event.get("outcome") == "reserved"
                                and str(event.get("at") or "").startswith(day_prefix)):
                            used += float((event.get("usage") or {}).get("reserved_neurons") or 0)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
            if used + neurons > limit:
                return False
            self._record(candidate, task_type, "reserved", usage={"reserved_neurons": neurons},
                         reason="free_daily_neuron_reservation")
            return True

    def _aiai_cost(self, usage: dict[str, Any] | None) -> Optional[float]:
        fixed = _as_float(self.env.get("AIAI_REQUEST_COST_BEL"))
        if fixed is not None and fixed > 0:
            return fixed
        return None

    def _within_aiai_budget(self, request_cost: float) -> bool:
        daily = _as_float(self.env.get("AIAI_DAILY_BUDGET_BEL"))
        monthly = _as_float(self.env.get("AIAI_MONTHLY_BUDGET_BEL"))
        if daily is None or monthly is None:
            return False
        now = self.now()
        day_prefix, month_prefix = now.strftime("%Y-%m-%d"), now.strftime("%Y-%m")
        day_total = month_total = 0.0
        if self.usage_file.exists():
            for line in self.usage_file.read_text("utf-8").splitlines():
                try:
                    event = json.loads(line)
                    if event.get("provider") != "aiai" or event.get("outcome") != "success":
                        continue
                    cost = _as_float(event.get("cost"))
                    stamp = str(event.get("at") or "")
                    if cost is not None and stamp.startswith(day_prefix): day_total += cost
                    if cost is not None and stamp.startswith(month_prefix): month_total += cost
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
        return day_total + request_cost <= daily and month_total + request_cost <= monthly

    def _validate(self, text: str, *, require_json: bool, required_fields: tuple[str, ...]) -> None:
        if len(str(text or "").strip()) < 2:
            raise AIProviderError("quality_empty")
        if not require_json:
            return
        try:
            data = json.loads(text)
        except (TypeError, ValueError) as exc:
            raise AIProviderError("quality_not_json") from exc
        for field in required_fields:
            if not isinstance(data, dict) or not data.get(field):
                raise AIProviderError("quality_missing_required_field")

    # ------------------------------ state --------------------------------
    def _cache_key(self, kind: str, task_type: str, payload: dict[str, Any], require_json: bool, fields: tuple[str, ...]) -> str:
        normalized = json.dumps({"kind": kind, "task_type": task_type, "payload": payload, "require_json": require_json, "fields": fields}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _get_cache(self, key: str, ttl: int) -> Optional[dict[str, Any]]:
        if ttl <= 0:
            return None
        path = self.cache_dir / f"{key}.json"
        try:
            row = json.loads(path.read_text("utf-8"))
            if time.time() - float(row.get("saved_at", 0)) <= ttl:
                return row
        except (OSError, ValueError, TypeError):
            return None
        return None

    def _save_cache(self, key: str, result: AIResult) -> None:
        row = {"saved_at": time.time(), "text": result.text, "provider": result.provider, "model": result.model, "usage": result.usage or {}, "cost": result.cost}
        self._atomic_write(self.cache_dir / f"{key}.json", json.dumps(row, ensure_ascii=False))

    def _load_state(self) -> dict[str, Any]:
        try:
            return json.loads(self.state_file.read_text("utf-8"))
        except (OSError, ValueError, TypeError):
            return {"cooldowns": {}}

    def _set_cooldown(self, provider: str, retry_after: int | None) -> None:
        seconds = min(max(int(retry_after or self.env.get("AI_PROVIDER_COOLDOWN_SECONDS", "300")), 30), 3600)
        with self._lock:
            state = self._load_state()
            state.setdefault("cooldowns", {})[provider] = time.time() + seconds
            self._atomic_write(self.state_file, json.dumps(state, ensure_ascii=False))

    def _record(self, candidate: Candidate, task_type: str, outcome: str, *, usage: dict[str, Any] | None = None, cost: float | None = None, reason: str = "", switch_reason: str = "") -> None:
        event = {"at": self.now().isoformat(), "provider": candidate.provider, "model": candidate.model, "task_type": task_type, "outcome": outcome, "usage": usage or {}, "cost": cost if cost is not None else "unknown", "reason": reason[:160], "switch_reason": switch_reason[:160], "quota_scope": "shared" if self.env.get("AI_SHARED_QUOTA_LEDGER_DIR") else "local"}
        with self._lock:
            with self.usage_file.open("a", encoding="utf-8") as file:
                file.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _atomic_write(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(path.name + ".tmp")
        temp.write_text(text, encoding="utf-8")
        os.replace(temp, path)

    # ------------------------------- utils --------------------------------
    def _secret(self, name: str) -> str:
        return self._required(name)

    def _required(self, name: str) -> str:
        value = str(self.env.get(name) or "").strip()
        if not value:
            raise AIProviderError("key_not_configured")
        return value

    def _post(self, *args: Any, **kwargs: Any) -> Any:
        return (self.post or requests.post)(*args, **kwargs)

    def _timeout(self, kind: str) -> int:
        defaults = {"text": 75, "vision": 45, "audio": 120}
        return min(max(int(self.env.get("AI_REQUEST_TIMEOUT_SECONDS", defaults.get(kind, 75))), 10), 180)

    def _compact_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, str]]:
        # Existing callers already compact source archives. Preserve the last
        # messages and cap aggregate context before it reaches a shared quota.
        items = []
        remaining = int(self.env.get("AI_MAX_CONTEXT_CHARS", "50000"))
        for message in (messages or [])[-10:]:
            if not isinstance(message, dict):
                continue
            content = str(message.get("content") or "")
            if not content:
                continue
            content = content[-remaining:]
            remaining -= len(content)
            items.append({"role": "assistant" if str(message.get("role", "")).lower() == "assistant" else "user", "content": content})
            if remaining <= 0:
                break
        return items

    def _openai_messages(self, kind: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if kind == "vision":
            return [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": f"data:{payload['mime_type']};base64,{payload['image_b64']}"}}, {"type": "text", "text": payload["prompt"]}]}]
        items = []
        if payload.get("system"):
            items.append({"role": "system", "content": payload["system"]})
        items.extend(payload.get("messages") or [])
        return items

    def _json_response(self, response: Any) -> dict[str, Any]:
        try:
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            status = getattr(response, "status_code", None)
            retry_after = getattr(response, "headers", {}).get("Retry-After") if getattr(response, "headers", None) else None
            error = AIProviderError(f"http_{status or 'unknown'}")
            setattr(error, "status_code", status)
            setattr(error, "retry_after", retry_after)
            raise error from exc

    @staticmethod
    def _openai_text(data: dict[str, Any]) -> str:
        choices = data.get("choices") or []
        message = choices[0].get("message") if choices and isinstance(choices[0], dict) else {}
        content = message.get("content") if isinstance(message, dict) else ""
        return str(content or "").strip()

    @staticmethod
    def _usage(data: dict[str, Any]) -> dict[str, Any]:
        raw = data.get("usage") or data.get("usageMetadata") or {}
        if not isinstance(raw, dict):
            return {}
        aliases = {"promptTokenCount": "prompt_tokens", "candidatesTokenCount": "completion_tokens", "totalTokenCount": "total_tokens", "input_tokens": "prompt_tokens", "output_tokens": "completion_tokens"}
        return {aliases.get(k, k): v for k, v in raw.items() if isinstance(v, (int, float))}

    @staticmethod
    def _classify_error(exc: Exception) -> tuple[str, Optional[int]]:
        text = str(exc).lower()
        status = getattr(exc, "status_code", None)
        retry_raw = getattr(exc, "retry_after", None)
        try:
            retry = int(float(retry_raw)) if retry_raw is not None else None
        except (TypeError, ValueError):
            retry = None
        if "quality_" in text:
            return "quality", retry
        if "budget" in text:
            return "budget", retry
        if status == 429 or "quota" in text or "resource_exhausted" in text or "rate" in text:
            return "quota", retry
        if status in (401, 403) or "key_not_configured" in text or "model_not_configured" in text or "unsupported" in text or "paid_model" in text:
            return "configuration", retry
        if status and status >= 500 or "timeout" in text or "connection" in text:
            return "temporary", retry
        return "provider_error", retry
