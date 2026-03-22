"""Multi-provider LLM client using litellm + instructor.

Improvements:
- #1:  Ollama native JSON schema constraint (format= parameter)
- #3:  Model swap support (auto-detects available models)
- #4:  Two-pass architecture (reasoning + structuring)
- #7:  LiteLLM local disk caching
- #8:  LiteLLM model fallback chain via Router
- #9:  Structured scratchpad chain-of-thought
- New: Semantic entropy gating (per-import confidence estimation)
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, TypeVar

import instructor
import litellm
from pydantic import BaseModel

logger = logging.getLogger("apdr_llm")

# Suppress litellm's verbose logging
litellm.suppress_debug_info = True
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

T = TypeVar("T", bound=BaseModel)

# --- #7: Enable LiteLLM disk caching ---
_cache_initialized = False


def _init_cache(cache_dir: str = "") -> None:
    global _cache_initialized
    if _cache_initialized:
        return
    _cache_initialized = True
    try:
        from litellm.caching.caching import Cache
        disk_dir = cache_dir or str(Path.home() / ".apdr-cache" / "llm-cache")
        Path(disk_dir).mkdir(parents=True, exist_ok=True)
        litellm.cache = Cache(type="disk", disk_cache_dir=disk_dir)
        logger.info("LiteLLM disk cache enabled at %s", disk_dir)
    except Exception as e:
        logger.warning("Failed to enable LiteLLM cache: %s", e)


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


class LlmClient:
    """Provider-agnostic LLM client backed by litellm + instructor."""

    def __init__(self, provider: str, model: str, base_url: str) -> None:
        self.provider = provider
        self.model = model
        self.base_url = base_url.rstrip("/")
        self._instructor_client = instructor.from_litellm(litellm.completion)
        # --- #8: Detect fallback models ---
        self._fallback_models = self._detect_fallback_models()
        # --- #7: Init cache ---
        _init_cache()

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

    def _base_kwargs(
        self,
        temperature: float = 0.0,
        max_tokens: int = 1024,
        model_override: str | None = None,
    ) -> dict[str, Any]:
        """Build common kwargs for litellm calls."""
        kwargs: dict[str, Any] = {
            "model": self._litellm_model(model_override),
            "temperature": temperature,
            "max_tokens": max_tokens,
            "timeout": 90,
        }
        if self.provider == "ollama":
            kwargs["api_base"] = self.base_url
            kwargs["num_ctx"] = 8192
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
        try:
            import requests as req_lib
            schema = response_model.model_json_schema()
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "format": schema,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_ctx": 8192,
                    "num_predict": max_tokens,
                },
            }
            resp = req_lib.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=90,
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
        )

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> str | None:
        """Raw text completion with system/user message separation."""
        kwargs = self._base_kwargs(temperature, max_tokens)
        kwargs["messages"] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        try:
            response = litellm.completion(**kwargs)
            text = response.choices[0].message.content
            return text.strip() if text else None
        except Exception as e:
            logger.warning("LLM completion failed: %s", e)
            return None

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
    ) -> T | None:
        """JSON completion with tolerant parsing — works with small models.

        Uses Ollama's format="json" mode (simple JSON enforcement without
        full schema GBNF) and parses the response tolerantly. No instructor
        dependency, no field_validators during generation.
        """
        schema_instructions = _format_schema_instructions(response_model)
        augmented_prompt = f"{user_prompt}\n\n{schema_instructions}"

        for attempt in range(1 + max_retries):
            content = self._complete_json_raw(
                system_prompt, augmented_prompt, temperature, max_tokens,
            )
            if not content:
                logger.debug("complete_json attempt %d: empty response", attempt + 1)
                continue

            parsed = _extract_json_from_text(content)
            if parsed is None:
                logger.debug(
                    "complete_json attempt %d: JSON extraction failed from: %.300s",
                    attempt + 1, content,
                )
                augmented_prompt = (
                    f"{user_prompt}\n\n{schema_instructions}\n\n"
                    f"Your previous response was not valid JSON. "
                    f"Return ONLY a JSON object, no other text."
                )
                continue

            try:
                return response_model.model_validate(parsed)
            except Exception as e:
                logger.debug(
                    "complete_json attempt %d: validation failed: %s", attempt + 1, e,
                )
                augmented_prompt = (
                    f"{user_prompt}\n\n{schema_instructions}\n\n"
                    f"Your previous response had a validation error: {e}. "
                    f"Please fix and return valid JSON."
                )

        return None

    def _complete_json_raw(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> str | None:
        """Get raw JSON text from the LLM, using format=json for Ollama."""
        if self.provider == "ollama":
            try:
                import requests as req_lib
                payload = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "format": "json",
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_ctx": 8192,
                        "num_predict": max_tokens,
                    },
                }
                resp = req_lib.post(
                    f"{self.base_url}/api/chat",
                    json=payload,
                    timeout=90,
                )
                if resp.ok:
                    data = resp.json()
                    content = data.get("message", {}).get("content", "")
                    if content:
                        return content.strip()
            except Exception as e:
                logger.warning("Ollama JSON call failed: %s", e)
        # Non-Ollama or Ollama failed: use raw text completion
        return self.complete(system_prompt, user_prompt, temperature, max_tokens)

    def complete_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[T],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_retries: int = 3,
    ) -> T | None:
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
        try:
            result = self._instructor_client.chat.completions.create(
                response_model=response_model,
                max_retries=max_retries,
                **kwargs,
            )
            return result
        except Exception as e:
            logger.warning("Structured completion failed: %s", e)

        # --- #8: Fallback to alternative models ---
        for fallback_model in self._fallback_models:
            try:
                fb_kwargs = self._base_kwargs(temperature, max_tokens, model_override=fallback_model)
                fb_kwargs["messages"] = kwargs["messages"]
                result = self._instructor_client.chat.completions.create(
                    response_model=response_model,
                    max_retries=2,
                    **fb_kwargs,
                )
                logger.info("Fallback model %s succeeded", fallback_model)
                return result
            except Exception as e2:
                logger.warning("Fallback model %s failed: %s", fallback_model, e2)
                continue

        return None

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

        def _single_call() -> T | None:
            return self.complete_structured(
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
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception:
                    pass

        if not results:
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
