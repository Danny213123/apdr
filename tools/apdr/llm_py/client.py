"""Multi-provider LLM client using litellm + instructor.

Improvements:
- #1:  Ollama native JSON schema constraint (format= parameter)
- #3:  Model swap support (auto-detects available models)
- #4:  Two-pass architecture (reasoning + structuring)
- #7:  LiteLLM local disk caching
- #8:  LiteLLM model fallback chain via Router
- #9:  Structured scratchpad chain-of-thought
- New: Semantic entropy gating (per-import confidence estimation)
- REC-03: Prompt hash-based cache invalidation
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import math
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, TypeVar

import instructor
import litellm
import requests as _requests_lib
from pydantic import BaseModel

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

logger = logging.getLogger("apdr_llm")

# Suppress litellm's verbose logging
litellm.suppress_debug_info = True
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

T = TypeVar("T", bound=BaseModel)

# --- #8/#13: OLLAMA_KEEP_ALIVE — keep model loaded in GPU memory ---
# Set at import time so it applies to all Ollama requests.
os.environ.setdefault("OLLAMA_KEEP_ALIVE", "-1")

# --- #7: Enable LiteLLM disk caching ---
_cache_initialized = False
_json_completion_cache: dict[str, str] = {}
_json_completion_cache_lock = threading.Lock()
_provider_gate_registry_lock = threading.Lock()
_provider_gate_thread_locks: dict[str, threading.Lock] = {}


def _provider_gate_cache_dir() -> Path:
    candidates = [
        Path(__file__).resolve().parents[3] / ".apdr-cache" / "llm-queue",
        Path.home() / ".apdr-cache" / "llm-queue",
        Path(tempfile.gettempdir()) / "apdr-cache" / "llm-queue",
    ]
    for directory in candidates:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            return directory
        except OSError:
            continue
    return candidates[-1]


def _provider_gate_key(provider: str, model: str, base_url: str) -> str:
    canonical = f"{provider.strip().lower()}|{model.strip()}|{base_url.strip().rstrip('/')}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _provider_gate_lock_path(provider: str, model: str, base_url: str) -> Path:
    return _provider_gate_cache_dir() / f"{_provider_gate_key(provider, model, base_url)}.lock"


def _provider_gate_thread_lock(provider: str, model: str, base_url: str) -> threading.Lock:
    key = _provider_gate_key(provider, model, base_url)
    with _provider_gate_registry_lock:
        lock = _provider_gate_thread_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _provider_gate_thread_locks[key] = lock
        return lock


@contextmanager
def _provider_call_gate(provider: str, model: str, base_url: str):
    """Serialize local-provider calls to avoid stampeding Ollama."""
    if provider.strip().lower() != "ollama":
        yield
        return

    thread_lock = _provider_gate_thread_lock(provider, model, base_url)
    with thread_lock:
        lock_path = _provider_gate_lock_path(provider, model, base_url)
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = lock_path.open("a+", encoding="utf-8")
        except OSError:
            yield
            return

        with handle:
            if fcntl is not None:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                except OSError:
                    yield
                    return
            try:
                yield
            finally:
                if fcntl is not None:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    except OSError:
                        pass


def _is_ollama_busy_response(resp) -> bool:
    """Check if an Ollama HTTP response is a 503 'server busy' error."""
    if resp.status_code == 503:
        return True
    if resp.status_code == 429:
        return True
    try:
        body = resp.text.lower()
        if "server busy" in body or "maximum pending requests" in body:
            return True
    except Exception:
        pass
    return False


def _ollama_post_with_retry(
    url: str,
    json_payload: dict,
    timeout: float,
    provider: str,
    model: str,
    base_url: str,
    max_retries: int = 5,
    initial_backoff: float = 2.0,
):
    """POST to Ollama with exponential backoff retry on 503/429 'server busy'.

    Returns the requests.Response on success, or the last failed response
    if all retries are exhausted.
    """
    import time
    import requests as req_lib

    backoff = initial_backoff
    last_resp = None

    for attempt in range(1 + max_retries):
        with _provider_call_gate(provider, model, base_url):
            last_resp = req_lib.post(url, json=json_payload, timeout=timeout)

        if last_resp.ok or not _is_ollama_busy_response(last_resp):
            return last_resp

        if attempt < max_retries:
            logger.warning(
                "Ollama busy (HTTP %d), retry %d/%d in %.1fs",
                last_resp.status_code, attempt + 1, max_retries, backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    logger.error(
        "Ollama busy after %d retries, giving up (HTTP %d)",
        max_retries, last_resp.status_code if last_resp else 0,
    )
    return last_resp


def _timeout_policy_for_action(action_name: str) -> dict[str, float]:
    normalized = (action_name or "").strip().lower()
    if normalized == "resolve":
        return {
            "time_budget": 35.0,
            "json_timeout": 20.0,
            "raw_timeout": 10.0,
        }
    if normalized == "recovery":
        return {
            "time_budget": 60.0,
            "json_timeout": 35.0,
            "raw_timeout": 12.0,
        }
    return {
        "time_budget": 45.0,
        "json_timeout": 25.0,
        "raw_timeout": 10.0,
    }


def classify_failure_reason(reason: str) -> str:
    text = str(reason or "").strip()
    lowered = text.lower()
    if not lowered:
        return "empty-output"
    if "timeout" in lowered or "timed out" in lowered or "readtimeout" in lowered:
        return "timeout"
    if any(
        marker in lowered
        for marker in (
            "server busy",
            "maximum pending requests exceeded",
            "too many requests",
            "http 429",
            "http 503",
            "status code 429",
            "status code 503",
        )
    ):
        return "provider-tooling-failure"
    if (
        ("json" in lowered and "validation" in lowered)
        or "schema failure" in lowered
        or "schema_validation" in lowered
        or "pydantic" in lowered
    ):
        return "schema-validation-failure"
    if "could not extract json" in lowered or "not valid json" in lowered or "json" in lowered:
        return "invalid-json"
    if any(
        marker in lowered
        for marker in (
            "connection",
            "connecterror",
            "transport",
            "refused",
            "dns",
            "http ",
            "httpx",
            "remoteprotocolerror",
        )
    ):
        return "transport-failure"
    if any(
        marker in lowered
        for marker in (
            "provider not available",
            "provider",
            "ollama",
            "litellm",
            "instructor",
            "tool",
            "unsupported",
        )
    ):
        return "provider-tooling-failure"
    return "empty-output"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "").strip())
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip())
    except ValueError:
        return default


def _init_cache(cache_dir: str = "") -> None:
    global _cache_initialized
    if _cache_initialized:
        return
    from litellm.caching.caching import Cache

    candidate_dirs: list[Path] = []
    if cache_dir:
        candidate_dirs.append(Path(cache_dir))

    repo_cache_dir = Path(__file__).resolve().parents[3] / ".apdr-cache" / "llm-cache"
    candidate_dirs.extend(
        [
            Path.home() / ".apdr-cache" / "llm-cache",
            repo_cache_dir,
            Path(tempfile.gettempdir()) / "apdr-cache" / "llm-cache",
        ]
    )

    last_error: Exception | None = None
    for disk_path in candidate_dirs:
        try:
            disk_path.mkdir(parents=True, exist_ok=True)
            litellm.cache = Cache(type="disk", disk_cache_dir=str(disk_path))
            _cache_initialized = True
            logger.info("LiteLLM disk cache enabled at %s", disk_path)
            return
        except Exception as e:
            last_error = e
            logger.debug("LiteLLM cache init failed at %s: %s", disk_path, e)

    logger.warning("Failed to enable LiteLLM cache: %s", last_error)


_prewarm_done = False


def prewarm_ollama(base_url: str = "http://localhost:11434", model: str = "") -> None:
    """Send a tiny request to load the model into GPU memory (eliminates cold-start)."""
    global _prewarm_done
    if _prewarm_done:
        return
    _prewarm_done = True
    if not model:
        return
    try:
        with _provider_call_gate("ollama", model, base_url):
            _requests_lib.post(
                f"{base_url.rstrip('/')}/api/chat",
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": "hi"}],
                    "stream": False,
                    "options": {"num_predict": 1, "num_ctx": 256},
                    "keep_alive": -1,
                },
                timeout=30,
            )
        logger.info("Ollama pre-warm complete for %s", model)
    except Exception as e:
        logger.debug("Ollama pre-warm failed (non-fatal): %s", e)


def _extract_json_from_text(text: str) -> dict | list | None:
    """Tolerantly extract a JSON object or array from LLM output text."""
    if not text:
        return None
    cleaned = text.strip()

    # 1. Direct parse
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, (dict, list)):
            return obj
    except json.JSONDecodeError:
        pass

    # 2. Extract from markdown code blocks
    for pattern in (r"```json\s*\n?(.*?)```", r"```\s*\n?(.*?)```"):
        m = re.search(pattern, cleaned, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(1).strip())
                if isinstance(obj, (dict, list)):
                    return obj
            except json.JSONDecodeError:
                pass

    # 3. Find first { ... last } or first [ ... last ]
    for open_ch, close_ch in ("{", "}"), ("[", "]"):
        start = cleaned.find(open_ch)
        end = cleaned.rfind(close_ch)
        if start != -1 and end > start:
            candidate = cleaned[start : end + 1]
            try:
                obj = json.loads(candidate)
                if isinstance(obj, (dict, list)):
                    return obj
            except json.JSONDecodeError:
                pass

    # 4. Strip single-line // comments and trailing commas, retry
    sanitized = re.sub(r"//[^\n]*", "", cleaned)
    sanitized = re.sub(r",\s*([\]}])", r"\1", sanitized)
    for open_ch, close_ch in ("{", "}"), ("[", "]"):
        start = sanitized.find(open_ch)
        end = sanitized.rfind(close_ch)
        if start != -1 and end > start:
            try:
                obj = json.loads(sanitized[start : end + 1])
                if isinstance(obj, (dict, list)):
                    return obj
            except json.JSONDecodeError:
                pass

    return None


def _format_schema_instructions(model: type[BaseModel]) -> str:
    """Generate human-readable JSON format instructions from a Pydantic model."""
    schema = model.model_json_schema()
    props = schema.get("properties", {})
    required = set(schema.get("required", []))
    lines = ["Respond ONLY with a JSON object. Required keys:"]
    for key, meta in props.items():
        typ = meta.get("type", "any")
        desc = meta.get("description", "")
        items = meta.get("items", {})
        if typ == "array" and items:
            item_props = items.get("properties", {})
            if item_props:
                inner = ", ".join(
                    f'"{k}": ({v.get("type", "any")})'
                    for k, v in item_props.items()
                )
                typ = f"array of objects with {{{inner}}}"
            else:
                typ = f'array of {items.get("type", "any")}'
        req = " (required)" if key in required else ""
        suffix = f" - {desc}" if desc else ""
        lines.append(f'  "{key}": ({typ}){req}{suffix}')
    return "\n".join(lines)


def build_optional_langchain_chat_model(
    provider: str,
    model: str,
    base_url: str,
    *,
    temperature: float = 0.0,
    max_tokens: int = 1024,
) -> tuple[Any | None, str]:
    """Build a LangChain-compatible chat model with lazy optional imports."""
    try:
        from langchain_community.chat_models import ChatLiteLLM
    except ImportError as exc:
        logger.info("Optional LangChain chat model unavailable: %s", exc)
        return None, f"Optional LangChain dependency unavailable: {exc}"

    model_name = f"ollama_chat/{model}" if provider == "ollama" else model
    try:
        llm = ChatLiteLLM(
            model=model_name,
            api_base=base_url if provider == "ollama" else None,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    except Exception as exc:
        logger.warning("Failed to initialize optional LangChain chat model: %s", exc)
        return None, f"Failed to initialize optional LangChain chat model: {exc}"
    return llm, ""


class LlmClient:
    """Provider-agnostic LLM client backed by litellm + instructor."""

    def __init__(self, provider: str, model: str, base_url: str) -> None:
        self.provider = provider
        self.model = model
        self.base_url = base_url.rstrip("/")
        self._last_failure_reason = ""
        self._last_failure_lock = threading.Lock()
        # --- REC-03: Compute prompt version hash for cache invalidation ---
        self._prompt_version_hash = self._compute_prompt_version()
        self._instructor_client = instructor.from_litellm(litellm.completion)
        # --- #8: Detect fallback models ---
        self._fallback_models = self._detect_fallback_models()
        # --- #7: Init cache ---
        _init_cache()
        # --- REC-03: Configure cache with prompt versioning ---
        self._init_cache_with_versioning()
        # --- #13: Pre-warm Ollama to load model into GPU memory ---
        if self.provider == "ollama":
            prewarm_ollama(self.base_url, self.model)

    def _remember_failure(self, reason: str) -> None:
        with self._last_failure_lock:
            self._last_failure_reason = str(reason or "").strip()

    def last_failure_reason(self) -> str:
        with self._last_failure_lock:
            return self._last_failure_reason

    def failure_details(self) -> dict[str, str]:
        reason = self.last_failure_reason()
        return {
            "failure_class": classify_failure_reason(reason),
            "diagnostic_preview": self._truncate_debug(reason, limit=400),
        }

    def _truncate_debug(self, value: Any, limit: int = 240) -> str:
        text = str(value or "").strip().replace("\n", "\\n")
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."

    def _compute_prompt_version(self) -> str:
        """Compute SHA256 hash of prompt templates for cache versioning.

        Returns first 16 chars of hex digest (64-bit collision resistance).
        Hash includes template structure ONLY, not dynamic content.
        """
        from . import prompts

        # Collect all prompt templates (structure only, not dynamic content)
        templates = {
            "recovery_system": prompts.RECOVERY_SYSTEM,
            "recovery_user_template": self._extract_user_template(prompts.recovery_user),
            "solvability_system": getattr(prompts, "SOLVABILITY_SYSTEM", ""),
            "resolution_system": getattr(prompts, "RESOLUTION_SYSTEM", ""),
            "model": self.model,  # Include model ID per D-11
        }

        # Stable serialization (sort_keys ensures deterministic output)
        canonical = json.dumps(templates, sort_keys=True)

        # Hash and truncate to first 16 chars (64-bit collision resistance)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return digest[:16]

    def _extract_user_template(self, prompt_fn) -> str:
        """Extract template structure from prompt function.

        Uses function source code to capture template structure.
        Dynamic placeholders (error_log, resolved_packages) are included
        as-is - we hash the template structure, not the content.
        """
        try:
            source = inspect.getsource(prompt_fn)
            return source
        except Exception:
            # Fallback: use function name if source unavailable
            return prompt_fn.__name__

    def _action_name_for_prompt(self, system_prompt: str) -> str:
        """Classify the invoking workflow for per-action cache metrics."""
        from . import prompts

        if system_prompt == prompts.RECOVERY_SYSTEM:
            return "recovery"
        if system_prompt == getattr(prompts, "SOLVABILITY_SYSTEM", ""):
            return "solvability"
        if system_prompt == getattr(prompts, "RESOLUTION_SYSTEM", ""):
            return "resolve"
        return "unknown"

    def _json_cache_key(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
        num_ctx: int,
    ) -> str:
        payload = {
            "provider": self.provider,
            "model": self.model,
            "base_url": self.base_url,
            "prompt_version": self._prompt_version_hash,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "num_ctx": num_ctx,
        }
        if self.provider == "ollama":
            policy = self._ollama_policy(
                model_name=self.model,
                temperature=temperature,
                num_ctx=num_ctx,
                max_tokens=max_tokens,
            )
            payload.update(
                {
                    "temperature": policy["temperature"],
                    "num_ctx": policy["num_ctx"],
                    "top_p": policy["top_p"],
                    "top_k": policy["top_k"],
                    "thinking_mode": policy["thinking_mode"],
                    "policy_label": policy["policy_label"],
                }
            )
        canonical = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _init_cache_with_versioning(self):
        """Configure LiteLLM cache with prompt version injection.

        Wraps the global cache's get_cache_key() to prepend prompt version hash.
        This ensures cache misses when prompts change without breaking existing cache.
        """
        if not hasattr(litellm, 'cache') or litellm.cache is None:
            return

        cache = litellm.cache
        original_get_key = getattr(cache, "_apdr_original_get_cache_key", cache.get_cache_key)
        prompt_hash = self._prompt_version_hash

        if getattr(cache, "_apdr_prompt_hash", None) == prompt_hash:
            return

        def versioned_get_cache_key(*args, **kwargs):
            """Inject prompt version into cache key."""
            base_key = original_get_key(*args, **kwargs)
            # Prepend prompt version to ensure invalidation on prompt change
            return f"v{prompt_hash}:{base_key}"

        cache._apdr_original_get_cache_key = original_get_key
        cache._apdr_prompt_hash = prompt_hash
        cache.get_cache_key = versioned_get_cache_key
        logger.debug("LiteLLM cache configured with prompt version %s", prompt_hash)

    def _detect_fallback_models(self) -> list[str]:
        """Detect additional Ollama models available for fallback."""
        if self.provider != "ollama":
            return []
        try:
            import requests
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            if not resp.ok:
                return []
            data = resp.json()
            models = data.get("models", [])
            available = [m.get("name", "") for m in models]
            # Preferred fallback order
            preferred = [
                "qwen3:14b", "qwen3:8b", "qwen2.5-coder:14b",
                "gemma3:12b", "gemma3:4b", "llama3.1:8b",
            ]
            fallbacks = []
            for pref in preferred:
                for avail in available:
                    if avail == pref or avail.startswith(f"{pref}:"):
                        if pref != self.model and pref not in fallbacks:
                            fallbacks.append(pref)
            return fallbacks[:2]  # Max 2 fallbacks
        except Exception:
            return []

    def _litellm_model(self, model_override: str | None = None) -> str:
        """Build the litellm model string for the configured provider."""
        model = model_override or self.model
        if self.provider == "ollama":
            return f"ollama_chat/{model}"
        return model

    def _default_top_p(self, model_name: str) -> float:
        if "qwen3.5" in model_name.lower():
            return 0.95
        return 1.0

    def _default_top_k(self, model_name: str) -> int:
        if "qwen3.5" in model_name.lower():
            return 40
        return 0

    def _thinking_mode(self, model_name: str) -> str:
        requested = str(os.environ.get("APDR_THINKING_MODE", "inherited")).strip().lower()
        if requested in {"off", "on", "routed", "inherited"}:
            return requested
        if "qwen3" in model_name.lower():
            return "routed"
        return "inherited"

    def _thinking_enabled(
        self,
        model_name: str,
        *,
        max_tokens: int,
        temperature: float,
    ) -> bool | None:
        if "qwen3" not in model_name.lower():
            return None
        mode = self._thinking_mode(model_name)
        if mode == "off":
            return False
        if mode == "on":
            return True
        if mode == "routed":
            return max_tokens >= 768 or temperature > 0.0
        return True

    def _ollama_policy(
        self,
        *,
        model_name: str,
        temperature: float,
        num_ctx: int,
        max_tokens: int,
    ) -> dict[str, Any]:
        effective_temperature = _env_float("APDR_TEMPERATURE", temperature)
        effective_ctx = num_ctx if num_ctx > 0 else _env_int("APDR_NUM_CTX", 16384)
        return {
            "temperature": effective_temperature,
            "top_p": _env_float("APDR_TOP_P", self._default_top_p(model_name)),
            "top_k": _env_int("APDR_TOP_K", self._default_top_k(model_name)),
            "num_ctx": effective_ctx,
            "thinking": self._thinking_enabled(
                model_name,
                max_tokens=max_tokens,
                temperature=effective_temperature,
            ),
            "thinking_mode": self._thinking_mode(model_name),
            "policy_label": str(os.environ.get("APDR_POLICY_LABEL", "")).strip(),
        }

    def _base_kwargs(
        self,
        temperature: float = 0.0,
        max_tokens: int = 1024,
        model_override: str | None = None,
        num_ctx: int = 0,
    ) -> dict[str, Any]:
        """Build common kwargs for litellm calls."""
        kwargs: dict[str, Any] = {
            "model": self._litellm_model(model_override),
            "temperature": temperature,
            "max_tokens": max_tokens,
            "timeout": 60,
        }
        if self.provider == "ollama":
            kwargs["api_base"] = self.base_url
            policy = self._ollama_policy(
                model_name=model_override or self.model,
                temperature=temperature,
                num_ctx=num_ctx,
                max_tokens=max_tokens,
            )
            kwargs["temperature"] = policy["temperature"]
            kwargs["num_ctx"] = policy["num_ctx"]
            kwargs["top_p"] = policy["top_p"]
            kwargs["top_k"] = policy["top_k"]
        elif self.base_url and self.base_url != "http://localhost:11434":
            kwargs["api_base"] = self.base_url
        return kwargs

    # ------------------------------------------------------------------
    # #1: Ollama native schema constraint
    # ------------------------------------------------------------------

    def complete_ollama_native(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        num_ctx: int = 0,
        model_override: str | None = None,
    ) -> T | None:
        """Use Ollama's native format= parameter for schema-constrained decoding.

        This bypasses instructor and sends the JSON schema directly to Ollama,
        which converts it to a GBNF grammar for token-level enforcement.
        Falls back to instructor-based completion on failure.
        """
        if self.provider != "ollama":
            return self.complete_json(
                system_prompt, user_prompt, response_model,
                temperature=temperature, max_tokens=max_tokens,
            )
        effective_model = model_override or self.model
        try:
            import requests as req_lib
            timeout_policy = _timeout_policy_for_action(
                self._action_name_for_prompt(system_prompt)
            )
            policy = self._ollama_policy(
                model_name=effective_model,
                temperature=temperature,
                num_ctx=num_ctx,
                max_tokens=max_tokens,
            )
            schema = response_model.model_json_schema()
            payload = {
                "model": effective_model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "format": schema,
                "stream": False,
                "keep_alive": -1,
                "options": {
                    "temperature": policy["temperature"],
                    "num_ctx": policy["num_ctx"],
                    "num_predict": max_tokens,
                    "top_p": policy["top_p"],
                    "top_k": policy["top_k"],
                    "num_gpu": 99,
                    "num_batch": 1024,
                },
            }
            if policy["thinking"] is not None:
                payload["think"] = policy["thinking"]
            resp = _ollama_post_with_retry(
                f"{self.base_url}/api/chat",
                json_payload=payload,
                timeout=timeout_policy["json_timeout"],
                provider=self.provider,
                model=effective_model,
                base_url=self.base_url,
            )
            if resp.ok:
                data = resp.json()
                content = data.get("message", {}).get("content", "")
                if content:
                    return response_model.model_validate_json(content)
        except Exception as e:
            logger.debug("Ollama native schema failed, falling back to complete_json: %s", e)

        return self.complete_json(
            system_prompt, user_prompt, response_model,
            temperature=temperature, max_tokens=max_tokens,
            num_ctx=num_ctx,
        )

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> str | None:
        """Raw text completion with system/user message separation."""
        text, diagnostic = self._complete_text_with_diagnostics(
            system_prompt,
            user_prompt,
            temperature,
            max_tokens,
        )
        self._remember_failure("" if text else diagnostic)
        return text

    def _complete_text_with_diagnostics(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
        timeout: float = 60,
    ) -> tuple[str | None, str]:
        import time as _time

        kwargs = self._base_kwargs(temperature, max_tokens)
        kwargs["timeout"] = timeout
        kwargs["messages"] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        max_busy_retries = 5
        backoff = 2.0
        for attempt in range(1 + max_busy_retries):
            try:
                with _provider_call_gate(self.provider, self.model, self.base_url):
                    response = litellm.completion(**kwargs)
                text = response.choices[0].message.content
                if text:
                    return text.strip(), ""
                return None, "raw text completion returned empty content"
            except Exception as e:
                err_lower = str(e).lower()
                is_busy = any(
                    m in err_lower
                    for m in ("server busy", "maximum pending requests", "503", "429")
                )
                if is_busy and attempt < max_busy_retries:
                    logger.warning(
                        "Ollama busy (litellm), retry %d/%d in %.1fs: %s",
                        attempt + 1, max_busy_retries, backoff, e,
                    )
                    _time.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue
                logger.warning("LLM completion failed: %s", e)
                return None, f"raw text completion failed: {type(e).__name__}: {e}"
        return None, "raw text completion exhausted busy retries"

    # ------------------------------------------------------------------
    # Tolerant JSON completion (primary path for small models)
    # ------------------------------------------------------------------

    def complete_json(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_retries: int = 2,
        num_ctx: int = 0,
    ) -> T | None:
        result, diagnostic = self.complete_json_with_diagnostics(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=response_model,
            temperature=temperature,
            max_tokens=max_tokens,
            max_retries=max_retries,
            num_ctx=num_ctx,
        )
        self._remember_failure("" if result is not None else diagnostic)
        return result

    def complete_json_with_diagnostics(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_retries: int = 2,
        num_ctx: int = 0,
    ) -> tuple[T | None, str]:
        """JSON completion with tolerant parsing — works with small models.

        Uses Ollama's format="json" mode (simple JSON enforcement without
        full schema GBNF) and parses the response tolerantly. No instructor
        dependency, no field_validators during generation.
        """
        import time
        started = time.time()
        action_name = self._action_name_for_prompt(system_prompt)
        timeout_policy = _timeout_policy_for_action(action_name)

        schema_instructions = _format_schema_instructions(response_model)
        augmented_prompt = f"{user_prompt}\n\n{schema_instructions}"
        diagnostics: list[str] = []

        # Use an action-specific budget so one intake failure does not stall the run.
        time_budget = timeout_policy["time_budget"]

        for attempt in range(1 + max_retries):
            elapsed = time.time() - started
            if attempt > 0 and elapsed >= time_budget:
                diagnostics.append(
                    f"attempt {attempt + 1}: skipped — time budget exhausted ({elapsed:.0f}s >= {time_budget:.0f}s)"
                )
                break
            content, transport_diagnostic = self._complete_json_raw_with_diagnostics(
                system_prompt, augmented_prompt, temperature, max_tokens,
                num_ctx=num_ctx,
                request_timeout=timeout_policy["json_timeout"],
                raw_timeout=timeout_policy["raw_timeout"],
            )
            if not content:
                logger.debug("complete_json attempt %d: empty response", attempt + 1)
                diagnostics.append(
                    f"attempt {attempt + 1}: {transport_diagnostic or 'completion backend returned no content'}"
                )
                continue

            parsed = _extract_json_from_text(content)
            if parsed is None:
                logger.debug(
                    "complete_json attempt %d: JSON extraction failed from: %.300s",
                    attempt + 1, content,
                )
                diagnostics.append(
                    f"attempt {attempt + 1}: could not extract JSON from "
                    f"{self._truncate_debug(content)}"
                )
                augmented_prompt = (
                    f"{user_prompt}\n\n{schema_instructions}\n\n"
                    f"Your previous response was not valid JSON. "
                    f"Return ONLY a JSON object, no other text."
                )
                continue

            try:
                result = response_model.model_validate(parsed)

                duration_ms = int((time.time() - started) * 1000)

                # Detect cache hit - LiteLLM may not expose this directly
                # Heuristic: very fast responses (<100ms) are likely cache hits
                cache_hit = duration_ms < 100

                logger.info(
                    "LLM completion finished",
                    extra={
                        "event": "llm_completion",
                        "action": action_name,
                        "cache_hit": cache_hit,
                        "duration_ms": duration_ms,
                        "model": self.model,
                        "prompt_version": self._prompt_version_hash,
                    }
                )

                return result, ""
            except Exception as e:
                logger.debug(
                    "complete_json attempt %d: validation failed: %s", attempt + 1, e,
                )
                diagnostics.append(
                    f"attempt {attempt + 1}: JSON validation failed: {self._truncate_debug(e)}"
                )
                augmented_prompt = (
                    f"{user_prompt}\n\n{schema_instructions}\n\n"
                    f"Your previous response had a validation error: {e}. "
                    f"Please fix and return valid JSON."
                )

        diagnostic = "; ".join(diagnostics[-4:])
        if not diagnostic:
            diagnostic = "LLM returned no usable JSON response"
        return None, diagnostic

    def _complete_json_raw(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
        num_ctx: int = 0,
    ) -> str | None:
        content, _diagnostic = self._complete_json_raw_with_diagnostics(
            system_prompt,
            user_prompt,
            temperature,
            max_tokens,
            num_ctx=num_ctx,
        )
        return content

    def _complete_json_raw_with_diagnostics(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
        num_ctx: int = 0,
        request_timeout: float = 60,
        raw_timeout: float = 60,
    ) -> tuple[str | None, str]:
        """Get raw JSON text from the LLM, using format=json for Ollama."""
        if self.provider == "ollama":
            cache_key = self._json_cache_key(
                system_prompt,
                user_prompt,
                temperature,
                max_tokens,
                num_ctx,
            )
            with _json_completion_cache_lock:
                cached = _json_completion_cache.get(cache_key)
            if cached is not None:
                return cached, ""
            try:
                import requests as req_lib
                policy = self._ollama_policy(
                    model_name=self.model,
                    temperature=temperature,
                    num_ctx=num_ctx,
                    max_tokens=max_tokens,
                )
                payload = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "format": "json",
                    "stream": False,
                    "keep_alive": -1,
                    "options": {
                        "temperature": policy["temperature"],
                        "num_ctx": policy["num_ctx"],
                        "num_predict": max_tokens,
                        "top_p": policy["top_p"],
                        "top_k": policy["top_k"],
                        "num_gpu": 99,
                        "num_batch": 1024,
                    },
                }
                if policy["thinking"] is not None:
                    payload["think"] = policy["thinking"]
                resp = _ollama_post_with_retry(
                    f"{self.base_url}/api/chat",
                    json_payload=payload,
                    timeout=request_timeout,
                    provider=self.provider,
                    model=self.model,
                    base_url=self.base_url,
                )
                if resp.ok:
                    data = resp.json()
                    content = data.get("message", {}).get("content", "")
                    if content:
                        cleaned = content.strip()
                        with _json_completion_cache_lock:
                            _json_completion_cache[cache_key] = cleaned
                        return cleaned, ""
                    message = data.get("message", {})
                    diagnostic = (
                        "ollama json mode returned empty message.content"
                        if isinstance(message, dict)
                        else "ollama json mode returned empty message payload"
                    )
                    if isinstance(message, dict):
                        diagnostic = (
                            f"{diagnostic} (message keys: {', '.join(sorted(message.keys())) or 'none'})"
                        )
                elif _is_ollama_busy_response(resp):
                    # All retries exhausted on 503 — don't fall through to raw text
                    # fallback (that would just add more load to busy Ollama).
                    diagnostic = (
                        f"ollama server busy after retries (HTTP {resp.status_code}): "
                        f"{self._truncate_debug(resp.text)}"
                    )
                    return None, diagnostic
                else:
                    diagnostic = (
                        f"ollama json mode returned HTTP {resp.status_code}: "
                        f"{self._truncate_debug(resp.text)}"
                    )
            except Exception as e:
                logger.warning("Ollama JSON call failed: %s", e)
                diagnostic = f"ollama json mode failed: {type(e).__name__}: {e}"
            fallback_text, fallback_diagnostic = self._complete_text_with_diagnostics(
                system_prompt,
                user_prompt,
                temperature,
                max_tokens,
                timeout=raw_timeout,
            )
            if fallback_text:
                return fallback_text, ""
            if fallback_diagnostic:
                diagnostic = f"{diagnostic}; {fallback_diagnostic}" if diagnostic else fallback_diagnostic
            return None, diagnostic

        # Non-Ollama: use raw text completion only.
        return self._complete_text_with_diagnostics(
            system_prompt,
            user_prompt,
            temperature,
            max_tokens,
        )

    def complete_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_retries: int = 3,
    ) -> T | None:
        result, diagnostic = self.complete_structured_with_diagnostics(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=response_model,
            temperature=temperature,
            max_tokens=max_tokens,
            max_retries=max_retries,
        )
        self._remember_failure("" if result is not None else diagnostic)
        return result

    def complete_structured_with_diagnostics(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_retries: int = 3,
    ) -> tuple[T | None, str]:
        """Structured output completion using instructor.

        Returns a validated Pydantic model instance or None on failure.
        Pydantic field_validators run inside the retry loop — if validation
        fails (e.g. package doesn't exist on PyPI), Instructor retries with
        the validation error as context.
        """
        kwargs = self._base_kwargs(temperature, max_tokens)
        kwargs["messages"] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        diagnostics: list[str] = []
        import time as _time
        max_busy_retries = 5
        backoff = 2.0
        for busy_attempt in range(1 + max_busy_retries):
            try:
                with _provider_call_gate(self.provider, self.model, self.base_url):
                    result = self._instructor_client.chat.completions.create(
                        response_model=response_model,
                        max_retries=max_retries,
                        **kwargs,
                    )
                return result, ""
            except Exception as e:
                err_lower = str(e).lower()
                is_busy = any(
                    m in err_lower
                    for m in ("server busy", "maximum pending requests", "503", "429")
                )
                if is_busy and busy_attempt < max_busy_retries:
                    logger.warning(
                        "Ollama busy (instructor), retry %d/%d in %.1fs: %s",
                        busy_attempt + 1, max_busy_retries, backoff, e,
                    )
                    _time.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue
                logger.warning("Structured completion failed: %s", e)
                diagnostics.append(f"instructor primary failed: {type(e).__name__}: {e}")
                break

        # --- #8: Fallback to alternative models ---
        for fallback_model in self._fallback_models:
            try:
                fb_kwargs = self._base_kwargs(temperature, max_tokens, model_override=fallback_model)
                fb_kwargs["messages"] = kwargs["messages"]
                with _provider_call_gate(self.provider, fallback_model, self.base_url):
                    result = self._instructor_client.chat.completions.create(
                        response_model=response_model,
                        max_retries=2,
                        **fb_kwargs,
                    )
                logger.info("Fallback model %s succeeded", fallback_model)
                return result, ""
            except Exception as e2:
                logger.warning("Fallback model %s failed: %s", fallback_model, e2)
                diagnostics.append(
                    f"instructor fallback {fallback_model} failed: {type(e2).__name__}: {e2}"
                )
                continue

        fallback_result, fallback_diagnostic = self.complete_json_with_diagnostics(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_model=response_model,
            temperature=temperature,
            max_tokens=max_tokens,
            max_retries=1,
        )
        if fallback_result is not None:
            logger.info("Structured completion recovered via tolerant JSON fallback")
            return fallback_result, ""
        if fallback_diagnostic:
            diagnostics.append(f"tolerant json fallback failed: {fallback_diagnostic}")

        diagnostic = "; ".join(diagnostics[-4:])
        if not diagnostic:
            diagnostic = "structured completion returned no usable output"
        return None, diagnostic

    # ------------------------------------------------------------------
    # #4 + #9: Two-pass with structured scratchpad
    # ------------------------------------------------------------------

    def complete_two_pass(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        max_tokens: int = 1024,
    ) -> T | None:
        """Two-pass architecture — scratchpad reasoning then structured extraction.

        Pass 1: Structured scratchpad reasoning (no JSON constraint)
        Pass 2: Structured extraction from the reasoning
        """
        from . import prompts

        # Pass 1: scratchpad reasoning (#9)
        reasoning = self.complete(
            system_prompt=prompts.SCRATCHPAD_SYSTEM,
            user_prompt=user_prompt,
            temperature=0.0,
            max_tokens=512,
        )

        if not reasoning:
            # Fall through to single-pass
            return self.complete_structured(
                system_prompt, user_prompt, response_model, max_tokens=max_tokens,
            )

        # Pass 2: structured extraction from scratchpad
        extraction_prompt = (
            f"Based on this analysis, extract the final answer.\n\n"
            f"Analysis:\n{reasoning}\n\n"
            f"Original query:\n{user_prompt}"
        )

        return self.complete_structured(
            system_prompt=system_prompt,
            user_prompt=extraction_prompt,
            response_model=response_model,
            max_tokens=max_tokens,
        )

    # ------------------------------------------------------------------
    # #7: Semantic entropy gating
    # ------------------------------------------------------------------

    def complete_with_entropy(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        n: int = 3,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> tuple[T | None, float]:
        """Generate N samples and compute semantic entropy as confidence.

        Returns (best_result, confidence) where confidence is based on
        agreement across samples. High entropy = low confidence.
        """
        results: list[T] = []
        diagnostics: list[str] = []

        def _single_call() -> tuple[T | None, str]:
            return self.complete_structured_with_diagnostics(
                system_prompt,
                user_prompt,
                response_model,
                temperature=temperature,
                max_tokens=max_tokens,
                max_retries=1,
            )

        with ThreadPoolExecutor(max_workers=min(n, 3)) as executor:
            futures = [executor.submit(_single_call) for _ in range(n)]
            for future in as_completed(futures):
                try:
                    result, diagnostic = future.result()
                    if result is not None:
                        results.append(result)
                    elif diagnostic:
                        diagnostics.append(diagnostic)
                except Exception:
                    pass

        if not results:
            diagnostic = "; ".join(dict.fromkeys(diagnostics))
            if not diagnostic:
                diagnostic = "semantic entropy voting produced no usable responses"
            self._remember_failure(diagnostic)
            return None, 0.0

        # Count unique answers by JSON serialization
        counts: dict[str, tuple[int, T]] = {}
        for r in results:
            key = r.model_dump_json(exclude_none=True)
            if key in counts:
                counts[key] = (counts[key][0] + 1, counts[key][1])
            else:
                counts[key] = (1, r)

        total = sum(c for c, _ in counts.values())
        best_count, best_result = max(counts.values(), key=lambda x: x[0])

        # Compute confidence from agreement ratio
        confidence = best_count / total if total > 0 else 0.0

        # Compute Shannon entropy (lower = more agreement = higher confidence)
        entropy = 0.0
        for count, _ in counts.values():
            p = count / total
            if p > 0:
                entropy -= p * math.log2(p)

        # Normalize: entropy 0 = perfect agreement (confidence 1.0)
        # entropy log2(n) = maximum disagreement (confidence ~0.33)
        max_entropy = math.log2(total) if total > 1 else 1.0
        entropy_confidence = 1.0 - (entropy / max_entropy) if max_entropy > 0 else 1.0

        # Blend agreement and entropy-based confidence
        final_confidence = (confidence + entropy_confidence) / 2.0

        self._remember_failure("")
        return best_result, final_confidence

    def complete_with_voting(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        n: int = 3,
        temperature: float = 0.7,
        max_tokens: int = 1024,
    ) -> T | None:
        """Self-consistency voting: call N times in parallel, return majority."""
        result, confidence = self.complete_with_entropy(
            system_prompt, user_prompt, response_model,
            n=n, temperature=temperature, max_tokens=max_tokens,
        )
        return result

    def is_available(self) -> bool:
        """Check if the LLM provider is reachable."""
        if self.provider == "ollama":
            try:
                import requests
                resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
                if resp.ok:
                    data = resp.json()
                    models = data.get("models", [])
                    return any(
                        m.get("name", "") == self.model
                        or m.get("name", "").startswith(f"{self.model}:")
                        for m in models
                    )
            except Exception:
                pass
            return False
        if self.provider == "openai":
            return bool(os.environ.get("OPENAI_API_KEY"))
        if self.provider == "anthropic":
            return bool(os.environ.get("ANTHROPIC_API_KEY"))
        return False
