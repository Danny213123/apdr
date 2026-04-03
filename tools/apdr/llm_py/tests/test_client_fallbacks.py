from __future__ import annotations

import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from pydantic import BaseModel

# Ensure the llm_py package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.client import (
    LlmClient,
    _provider_call_gate,
    _timeout_policy_for_action,
    classify_failure_reason,
)


class _DummyResult(BaseModel):
    value: str


def _bare_client() -> LlmClient:
    client = LlmClient.__new__(LlmClient)
    client.provider = "ollama"
    client.model = "test-model"
    client.base_url = "http://localhost:11434"
    client._last_failure_reason = ""
    client._last_failure_lock = threading.Lock()
    client._prompt_version_hash = "test"
    client._instructor_client = MagicMock()
    client._fallback_models = []
    client._base_kwargs = MagicMock(return_value={})
    return client


def test_complete_structured_falls_back_to_tolerant_json():
    client = _bare_client()
    client._instructor_client.chat.completions.create.side_effect = RuntimeError("schema failure")
    client.complete_json_with_diagnostics = MagicMock(
        return_value=(_DummyResult(value="ok"), "")
    )

    result = client.complete_structured(
        system_prompt="system",
        user_prompt="user",
        response_model=_DummyResult,
    )

    assert result is not None
    assert result.value == "ok"
    client.complete_json_with_diagnostics.assert_called_once()
    assert client.last_failure_reason() == ""


def test_complete_json_records_failure_reason_for_empty_backend_response():
    client = _bare_client()
    client._complete_json_raw_with_diagnostics = MagicMock(
        return_value=(None, "ollama json mode returned empty message.content")
    )

    result = client.complete_json(
        system_prompt="system",
        user_prompt="user",
        response_model=_DummyResult,
        max_retries=0,
    )

    assert result is None
    assert "empty message.content" in client.last_failure_reason()


def test_phase26_intake_classifies_schema_failure():
    failure_class = classify_failure_reason(
        "instructor primary failed: RuntimeError: schema failure; "
        "tolerant json fallback failed: attempt 1: could not extract JSON"
    )
    assert failure_class == "schema-validation-failure"


def test_phase28_intake_classifies_provider_busy_as_tooling_failure():
    failure_class = classify_failure_reason(
        'attempt 1: ollama json mode returned HTTP 503: {"error":"server busy, please try again.  '
        'maximum pending requests exceeded"}; raw text completion failed: '
        'APIConnectionError: litellm.APIConnectionError: Ollama_chatException - '
        '{"error":"server busy, please try again.  maximum pending requests exceeded"}'
    )
    assert failure_class == "provider-tooling-failure"


def test_phase29_ollama_gate_serializes_threads():
    order: list[str] = []
    release_first = threading.Event()
    first_acquired = threading.Event()

    with tempfile.TemporaryDirectory() as td:
        lock_path = Path(td) / "ollama.lock"
        with patch("llm_py.client._provider_gate_lock_path", return_value=lock_path):
            def first_worker() -> None:
                with _provider_call_gate("ollama", "test-model", "http://localhost:11434"):
                    order.append("first")
                    first_acquired.set()
                    release_first.wait(timeout=1.0)

            def second_worker() -> None:
                first_acquired.wait(timeout=1.0)
                with _provider_call_gate("ollama", "test-model", "http://localhost:11434"):
                    order.append("second")

            thread_one = threading.Thread(target=first_worker)
            thread_two = threading.Thread(target=second_worker)
            thread_one.start()
            assert first_acquired.wait(timeout=1.0)
            thread_two.start()
            time.sleep(0.1)
            assert order == ["first"]
            release_first.set()
            thread_one.join(timeout=1.0)
            thread_two.join(timeout=1.0)

    assert order == ["first", "second"]


def test_phase26_intake_failure_details_include_preview():
    client = _bare_client()
    client._remember_failure(
        "ollama json mode failed: ReadTimeout: request timed out after 120 seconds"
    )

    details = client.failure_details()

    assert details["failure_class"] == "timeout"
    assert "timed out" in details["diagnostic_preview"]


def test_phase29_resolve_timeout_policy_is_tighter_than_default():
    policy = _timeout_policy_for_action("resolve")

    assert policy["time_budget"] < _timeout_policy_for_action("recovery")["time_budget"]
    assert policy["json_timeout"] < 60
    assert policy["raw_timeout"] < 60


def test_phase29_provider_gate_allows_body_exception_to_propagate():
    with tempfile.TemporaryDirectory() as td:
        lock_path = Path(td) / "ollama.lock"
        with patch("llm_py.client._provider_gate_lock_path", return_value=lock_path):
            try:
                with _provider_call_gate("ollama", "test-model", "http://localhost:11434"):
                    raise OSError("body failure")
            except OSError as exc:
                assert str(exc) == "body failure"
            else:
                raise AssertionError("expected OSError from guarded body")
