"""Unit tests for the agentic resolve path.

These tests focus on package resolution, not recovery.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure llm_py is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.actions.resolve import MappingsResult, SelfRefineResult, handle
from llm_py.models import PackageMapping, ResolutionRequest, ResolutionResponse


def _make_request(**overrides) -> ResolutionRequest:
    defaults = {
        "action": "resolve",
        "imports": ["sklearn"],
        "python_version": "3.10",
        "context": [],
        "benchmark_context": "",
        "attribute_usage": {},
        "snippet_source": "import sklearn",
        "tier2_candidates": {},
        "provider": "ollama",
        "model": "test-model",
        "base_url": "http://localhost:11434",
        "cache_path": "",
    }
    defaults.update(overrides)
    return ResolutionRequest(**defaults)


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_single_import_uses_two_pass_reasoning(mock_client_cls, monkeypatch):
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_two_pass.return_value = MappingsResult(
        mappings=[PackageMapping(import_name="sklearn", package_name="scikit-learn")]
    )
    mock_client.complete_json.return_value = SelfRefineResult(
        all_correct=True,
        corrections=[],
    )
    mock_client_cls.return_value = mock_client

    monkeypatch.setattr(
        "llm_py.actions.resolve.package_exists_on_pypi",
        lambda package_name: package_name.lower() == "scikit-learn",
    )

    resp = handle(_make_request())

    assert resp.error == ""
    assert resp.unresolved == []
    assert [(m.import_name, m.package_name) for m in resp.mappings] == [
        ("sklearn", "scikit-learn")
    ]
    mock_client.complete_two_pass.assert_called_once()
    mock_client.complete_with_entropy.assert_not_called()


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_multi_import_low_confidence_falls_back_to_react(mock_client_cls, monkeypatch):
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_with_entropy.return_value = (
        MappingsResult(
            mappings=[
                PackageMapping(import_name="cv2", package_name="opencv-python"),
                PackageMapping(import_name="flask_cors", package_name="flask_cors"),
            ]
        ),
        0.40,
    )
    mock_client.complete_json.return_value = SelfRefineResult(
        all_correct=True,
        corrections=[],
    )
    mock_client_cls.return_value = mock_client

    valid = {"opencv-python", "flask-cors"}
    monkeypatch.setattr(
        "llm_py.actions.resolve.package_exists_on_pypi",
        lambda package_name: package_name.lower() in valid,
    )

    react_response = ResolutionResponse(
        mappings=[
            PackageMapping(import_name="cv2", package_name="opencv-python"),
            PackageMapping(import_name="flask_cors", package_name="Flask-Cors"),
        ],
        notes=["Resolved via ReAct fallback"],
        prompts_issued=2,
    )

    with patch("llm_py.actions.react_agent.handle", return_value=react_response) as react_handle:
        resp = handle(
            _make_request(
                imports=["cv2", "flask_cors"],
                snippet_source="import cv2\nfrom flask_cors import CORS",
            )
        )

    assert resp.error == ""
    assert resp.unresolved == []
    assert sorted((m.import_name, m.package_name) for m in resp.mappings) == [
        ("cv2", "opencv-python"),
        ("flask_cors", "Flask-Cors"),
    ]
    assert any("Agent fallback resolved flask_cors -> Flask-Cors" in note for note in resp.notes)
    react_handle.assert_called_once()


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_agent_fallback_can_leave_import_unresolved(mock_client_cls, monkeypatch):
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_with_entropy.return_value = (
        MappingsResult(mappings=[PackageMapping(import_name="mysterypkg", package_name="mysterypkg")]),
        0.25,
    )
    mock_client.complete_json.return_value = None
    mock_client_cls.return_value = mock_client

    monkeypatch.setattr(
        "llm_py.actions.resolve.package_exists_on_pypi",
        lambda package_name: False,
    )

    react_response = ResolutionResponse(
        unresolved=["mysterypkg"],
        notes=["Agent could not verify a package mapping"],
        prompts_issued=2,
    )

    with patch("llm_py.actions.react_agent.handle", return_value=react_response):
        resp = handle(
            _make_request(
                imports=["mysterypkg", "anotherpkg"],
                snippet_source="import mysterypkg\nimport anotherpkg",
            )
        )

    assert resp.error == ""
    assert sorted(resp.unresolved) == ["anotherpkg", "mysterypkg"]
    assert resp.mappings == []
