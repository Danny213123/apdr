from __future__ import annotations

import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock

from pydantic import BaseModel

# Ensure the llm_py package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.client import LlmClient, classify_failure_reason


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


def test_phase26_intake_failure_details_include_preview():
    client = _bare_client()
    client._remember_failure(
        "ollama json mode failed: ReadTimeout: request timed out after 120 seconds"
    )

    details = client.failure_details()

    assert details["failure_class"] == "timeout"
    assert "timed out" in details["diagnostic_preview"]
