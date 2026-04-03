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
from llm_py.models import (
    AuthoredCasePlan,
    IntakeFailureRecord,
    PackageMapping,
    ResolutionRequest,
    ResolutionResponse,
)


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
        "agent_mode": "direct",
        "tool_profile": "full",
        "retrieval_profile": "none",
        "policy_label": "",
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
def test_multi_import_no_output_surfaces_diagnostics(mock_client_cls):
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_with_entropy.return_value = (None, 0.0)
    mock_client.last_failure_reason.return_value = (
        "instructor primary failed: RuntimeError: schema failure; "
        "tolerant json fallback failed: attempt 1: could not extract JSON"
    )
    mock_client_cls.return_value = mock_client

    resp = handle(
        _make_request(
            imports=["cv2", "flask_cors"],
            snippet_source="import cv2\nfrom flask_cors import CORS",
        )
    )

    assert resp.mappings == []
    assert sorted(resp.unresolved) == ["cv2", "flask_cors"]
    assert resp.abstain_reason == "LLM package-resolution call returned no output."
    assert "schema failure" in resp.failure_reason
    assert any("LLM diagnostics:" in note for note in resp.notes)


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_phase26_intake_success_returns_authored_case_plan(mock_client_cls, monkeypatch):
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

    assert resp.authored_plan_status == "available"
    assert isinstance(resp.authored_plan, AuthoredCasePlan)
    assert resp.intake_failure is None
    assert resp.authored_plan is not None
    assert resp.authored_plan.extracted_imports == ["sklearn"]
    assert resp.authored_plan.package_mappings[0].package_name == "scikit-learn"
    assert resp.authored_plan.smoke_strategy.mode == "import"
    assert resp.authored_plan.smoke_strategy.import_targets == ["sklearn"]
    assert resp.authored_plan.authorship == "llm-authored"


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_phase26_intake_no_output_returns_structured_failure(mock_client_cls):
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_with_entropy.return_value = (None, 0.0)
    mock_client.last_failure_reason.return_value = (
        "instructor primary failed: RuntimeError: schema failure; "
        "tolerant json fallback failed: attempt 1: could not extract JSON"
    )
    mock_client_cls.return_value = mock_client

    resp = handle(
        _make_request(
            imports=["cv2", "flask_cors"],
            snippet_source="import cv2\nfrom flask_cors import CORS",
        )
    )

    assert resp.authored_plan is None
    assert resp.authored_plan_status == "unusable"
    assert isinstance(resp.intake_failure, IntakeFailureRecord)
    assert resp.intake_failure is not None
    assert resp.intake_failure.failure_class == "schema-validation-failure"
    assert "schema failure" in resp.intake_failure.diagnostic_preview
    assert resp.intake_failure.llm_only_behavior == "fail"


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_phase26_zero_dependency_case_returns_deterministic_authored_plan(mock_client_cls):
    resp = handle(
        _make_request(
            imports=[],
            snippet_source="print('hello')",
        )
    )

    assert resp.error == ""
    assert resp.prompts_issued == 0
    assert resp.authored_plan_status == "available"
    assert resp.intake_failure is None
    assert isinstance(resp.authored_plan, AuthoredCasePlan)
    assert resp.authored_plan is not None
    assert resp.authored_plan.extracted_imports == []
    assert resp.authored_plan.package_mappings == []
    assert resp.authored_plan.unresolved_imports == []
    assert (
        "no third-party imports were detected, so dependency resolution is empty by design"
        in resp.authored_plan.runtime_assumptions
    )
    assert "No third-party imports required package resolution." in resp.notes
    mock_client_cls.assert_not_called()


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_phase29_evidence_backed_exact_mappings_skip_llm_client(mock_client_cls):
    resp = handle(
        _make_request(
            imports=["memcache", "redis", "requests", "sqlalchemy"],
            context=[
                "known import mapping: memcache -> python-memcached",
                "known import mapping: redis -> redis",
                "known import mapping: requests -> requests",
                "known import mapping: sqlalchemy -> SQLAlchemy",
                "Known package: python-memcached",
                "Known package: redis",
                "Known package: requests",
                "Known package: sqlalchemy",
            ],
            tier2_candidates={
                "memcache": ["python-memcached", "pymemcache"],
                "redis": ["redis", "fakeredis"],
                "requests": ["requests", "requests-file"],
                "sqlalchemy": ["sqlalchemy", "flask-sqlalchemy"],
            },
            snippet_source=(
                "import memcache\n"
                "import redis\n"
                "import requests\n"
                "import sqlalchemy\n"
            ),
            attribute_usage={
                "memcache": ["Client"],
                "redis": ["StrictRedis"],
                "requests": ["Session"],
            },
        )
    )

    assert resp.error == ""
    assert resp.prompts_issued == 0
    assert resp.unresolved == []
    assert sorted((m.import_name, m.package_name) for m in resp.mappings) == [
        ("memcache", "python-memcached"),
        ("redis", "redis"),
        ("requests", "requests"),
        ("sqlalchemy", "sqlalchemy"),
    ]
    assert resp.authored_plan is not None
    assert resp.authored_plan_status == "available"
    assert any("Deterministic evidence-backed mapping memcache -> python-memcached" in note for note in resp.notes)
    assert "evidence-backed-import-mapping" in resp.authored_plan.deterministic_fallback_sections
    assert any(
        mapping.source == "deterministic-evidence" and mapping.import_name == "sqlalchemy"
        for mapping in resp.authored_plan.package_mappings
    )
    mock_client_cls.assert_not_called()


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
    assert resp.agent_mode == "manual"
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
        agent_mode="manual",
        tool_profile="full",
        retrieval_profile="none",
        policy_label="manual-full",
        abstain_reason="Agent could not verify a package mapping",
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
    assert resp.abstain_reason == "Agent could not verify a package mapping"
    assert resp.agent_mode == "manual"


@pytest.mark.unit
@patch("llm_py.actions.resolve.LlmClient")
def test_explicit_agent_mode_routes_through_agent_seam(mock_client_cls, monkeypatch):
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

    react_response = ResolutionResponse(
        mappings=[PackageMapping(import_name="sklearn", package_name="scikit-learn")],
        notes=["Resolved via explicit manual agent seam"],
        prompts_issued=1,
        agent_mode="manual",
        tool_profile="reduced-toolset",
        retrieval_profile="failure-memory",
        policy_label="manual-reduced",
    )

    with patch("llm_py.actions.react_agent.handle", return_value=react_response) as react_handle:
        resp = handle(
            _make_request(
                agent_mode="manual",
                tool_profile="reduced-toolset",
                retrieval_profile="failure-memory",
                policy_label="manual-reduced",
            )
        )

    assert resp.error == ""
    assert resp.unresolved == []
    assert [(m.import_name, m.package_name) for m in resp.mappings] == [
        ("sklearn", "scikit-learn")
    ]
    assert resp.agent_mode == "manual"
    assert resp.tool_profile == "reduced-toolset"
    assert resp.retrieval_profile == "failure-memory"
    assert resp.policy_label == "manual-reduced"
    assert any("Explicit agent seam requested via agent_mode=manual" in note for note in resp.notes)
    react_handle.assert_called_once()
