"""Tests for REC-01: PyPI package validation.

Verifies that hallucinated package names are rejected before reaching Docker,
and that namespace validation prevents invalid package swaps.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure llm_py is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.actions.recovery import RecoveryResult, handle
from llm_py.models import ResolutionRequest


def _make_request(**overrides) -> ResolutionRequest:
    """Create a minimal ResolutionRequest for testing."""
    defaults = {
        "action": "recovery",
        "resolved_packages": ["scrapy (import: scrapy)"],
        "error_log": "ERROR: Build failed",
        "snippet_source": "import scrapy",
        "python_version": "3.10",
        "error_type": "BuildFailure",
        "previous_attempts": [],
        "provider": "ollama",
        "model": "test-model",
        "base_url": "http://localhost:11434",
    }
    defaults.update(overrides)
    return ResolutionRequest(**defaults)


@pytest.mark.unit
@patch("llm_py.actions.recovery.LlmClient")
def test_reject_nonexistent_package(mock_client_cls, mock_pypi_checker):
    """REC-01: LLM suggests package that doesn't exist on PyPI - should be rejected."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="scrapy",
        correct_package="fake-package-12345",
        reasoning="Use the fake package fork.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request()
    resp = handle(req)

    # Should reject: fix_possible=False + note about PyPI
    assert resp.fix_possible is False
    assert any("not found on PyPI" in note for note in resp.notes)
    assert "fake-package-12345" in resp.notes[0]


@pytest.mark.unit
@patch("llm_py.actions.recovery.LlmClient")
def test_accept_valid_package(mock_client_cls, mock_pypi_checker):
    """REC-01: LLM suggests valid PyPI package - should be accepted."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
        reasoning="Use pre-built binary.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=["psycopg2 (import: psycopg2)"],
        error_log="pg_config executable not found",
    )
    resp = handle(req)

    # Should accept: fix_possible=True
    assert resp.fix_possible is True
    assert resp.correct_package == "psycopg2-binary"
    assert not any("not found on PyPI" in note for note in resp.notes)


@pytest.mark.unit
@patch("llm_py.actions.recovery.LlmClient")
def test_reject_add_package_invalid(mock_client_cls, mock_pypi_checker):
    """REC-01: LLM suggests add_package that doesn't exist - should clear it."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="",
        correct_package="",
        add_package="nonexistent-transitive-dep",
        reasoning="Add missing dependency.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=["tensorflow==1.15.0 (import: tensorflow)"],
        error_log="ModuleNotFoundError: No module named 'protobuf'",
    )
    resp = handle(req)

    # Should clear add_package but not fail the entire fix
    # Since there's no swap/version pin, and add_package is cleared, fix_possible=False
    assert resp.fix_possible is False
    assert resp.add_package == ""
    assert any("add_package" in note and "not found on PyPI" in note for note in resp.notes)


@pytest.mark.unit
@patch("llm_py.actions.recovery.LlmClient")
def test_namespace_validation_rejects_incompatible(mock_client_cls, mock_pypi_checker):
    """REC-01: LLM suggests swap that breaks import namespace - should be rejected."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="requests",
        correct_package="beautifulsoup4",
        reasoning="Try BeautifulSoup instead.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=["requests (import: requests)"],
        error_log="ImportError: cannot import name 'Session' from 'requests'",
    )
    resp = handle(req)

    # Should reject: namespace mismatch (requests import != beautifulsoup4 package)
    assert resp.fix_possible is False
    assert any("does not preserve import namespace" in note for note in resp.notes)


@pytest.mark.unit
@patch("llm_py.actions.recovery.LlmClient")
def test_explicit_namespace_mapping_accepts(mock_client_cls, mock_pypi_checker):
    """REC-01: LLM suggests swap with explicit namespace mapping - should accept."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
        reasoning="Use pre-compiled binary.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=["psycopg2 (import: psycopg2)"],
        error_log="pg_config executable not found",
    )
    resp = handle(req)

    # Should accept: psycopg2 → psycopg2-binary is explicitly allowed
    assert resp.fix_possible is True
    assert resp.wrong_package == "psycopg2"
    assert resp.correct_package == "psycopg2-binary"
    assert not any("does not preserve import namespace" in note for note in resp.notes)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
