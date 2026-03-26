"""Tests for REC-02: RAG build error pattern library.

Verifies that known build error patterns are detected and injected
as context into LLM recovery prompts.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure llm_py is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.actions.recovery import handle
from llm_py.build_error_patterns import (
    ERROR_PATTERNS,
    format_error_context,
    match_error_patterns,
)
from llm_py.models import ResolutionRequest


def _make_request(**overrides) -> ResolutionRequest:
    """Create a minimal ResolutionRequest for testing."""
    defaults = {
        "action": "recovery",
        "resolved_packages": ["psycopg2 (import: psycopg2)"],
        "error_log": "ERROR: Build failed",
        "snippet_source": "import psycopg2",
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
def test_pg_config_pattern_matches(sample_error_logs):
    """REC-02: pg_config error pattern is detected."""
    matches = match_error_patterns(sample_error_logs["pg_config"])

    assert len(matches) > 0
    # Most specific match should be the pg_config pattern
    pg_pattern = next((m for m in matches if "pg_config" in m.pattern), None)
    assert pg_pattern is not None
    assert "PostgreSQL development headers" in pg_pattern.diagnosis
    assert "psycopg2-binary" in pg_pattern.suggested_fix


@pytest.mark.unit
def test_mysql_config_pattern_matches(sample_error_logs):
    """REC-02: mysql_config error pattern is detected."""
    matches = match_error_patterns(sample_error_logs["mysql_config"])

    assert len(matches) > 0
    mysql_pattern = next((m for m in matches if "mysql_config" in m.pattern), None)
    assert mysql_pattern is not None
    assert "MySQL development headers" in mysql_pattern.diagnosis
    assert "PyMySQL" in mysql_pattern.suggested_fix


@pytest.mark.unit
def test_top_3_injection(sample_error_logs):
    """REC-02: format_error_context returns exactly top 3 matches, not all."""
    # Create error log matching multiple patterns
    multi_error = sample_error_logs["pg_config"] + "\n" + sample_error_logs["python_h"]
    matches = match_error_patterns(multi_error)

    # Should have at least 2 matches (pg_config + python_h + generic gcc)
    assert len(matches) >= 2

    # format_error_context should return top 3 only
    context = format_error_context(multi_error)
    assert context != ""
    assert "Known error patterns detected" in context

    # Count how many bullet points (each match is "  - ")
    bullet_count = context.count("  - ")
    assert bullet_count <= 3, f"Expected ≤3 patterns in context, got {bullet_count}"


@pytest.mark.unit
def test_pattern_ordering_most_specific_first():
    """REC-02: Most specific patterns match first (pg_config before generic gcc)."""
    error_log = """
        running build_ext
        building 'psycopg2._psycopg' extension
        Error: pg_config executable not found.
        error: command 'gcc' failed with exit status 1
    """

    matches = match_error_patterns(error_log)

    # Should match both pg_config and gcc patterns
    assert len(matches) >= 2

    # pg_config is more specific than generic gcc failure
    # ERROR_PATTERNS is ordered most-specific first, so matches preserve that order
    pg_idx = next((i for i, m in enumerate(matches) if "pg_config" in m.pattern), None)
    gcc_idx = next((i for i, m in enumerate(matches) if "gcc" in m.pattern), None)

    assert pg_idx is not None
    assert gcc_idx is not None
    # In ERROR_PATTERNS list, pg_config appears before generic gcc
    # So pg_idx should be before gcc_idx in matches
    assert pg_idx < gcc_idx, "pg_config pattern should appear before generic gcc pattern"


@pytest.mark.unit
@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_note_added_on_pattern_match(mock_client_cls, mock_pypi, sample_error_logs):
    """REC-02: When pattern matches, note is added to response."""
    from llm_py.actions.recovery import RecoveryResult

    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
        reasoning="Use pre-built binary.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(error_log=sample_error_logs["pg_config"])
    resp = handle(req)

    # Should have note about pattern library matching
    assert any("Build error pattern library matched" in note for note in resp.notes)


@pytest.mark.unit
def test_no_match_returns_empty():
    """REC-02: Error log with no matching patterns returns empty context."""
    error_log = "Some random error message that doesn't match any patterns."

    matches = match_error_patterns(error_log)
    assert len(matches) == 0

    context = format_error_context(error_log)
    assert context == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
