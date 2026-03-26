"""Mock tests for the LLM recovery pipeline.

Tests that the recovery action correctly handles various LLM responses
without requiring a live Ollama instance.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure the llm_py package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.actions.recovery import RecoveryResult, handle
from llm_py.models import ResolutionRequest, ResolutionResponse


def _make_request(**overrides) -> ResolutionRequest:
    """Create a minimal ResolutionRequest for recovery testing."""
    defaults = dict(
        action="recovery",
        resolved_packages=["scrapy (import: scrapy)", "peewee (import: peewee)"],
        error_log="ERROR: Command errored out with exit status 1",
        snippet_source="from scrapy import Item\nimport peewee",
        python_version="2.7",
        error_type="BuildFailure",
        previous_attempts=[],
        provider="ollama",
        model="test-model",
        base_url="http://localhost:11434",
    )
    defaults.update(overrides)
    return ResolutionRequest(**defaults)


# ---------------------------------------------------------------------------
# Test: package swap (wrong → correct)
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_swap_package(mock_client_cls, mock_pypi):
    """LLM suggests replacing one package with another."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
        reasoning="psycopg2 requires pg_config; use pre-built binary.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=["psycopg2 (import: psycopg2)"],
        error_log="pg_config executable not found",
        error_type="BuildFailure",
    )
    resp = handle(req)

    assert resp.fix_possible is True
    assert resp.wrong_package == "psycopg2"
    assert resp.correct_package == "psycopg2-binary"
    assert resp.prompts_issued == 1


# ---------------------------------------------------------------------------
# Test: version pin (same package, different version)
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_version_pin(mock_client_cls, mock_pypi):
    """LLM suggests pinning an existing package to an older version."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="scrapy",
        correct_package="scrapy",
        version="1.8.3",
        reasoning="Scrapy 2.x dropped Python 2.7 support. Use 1.8.3.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request()
    resp = handle(req)

    assert resp.fix_possible is True
    assert resp.wrong_package == "scrapy"
    assert resp.correct_package == "scrapy"
    assert resp.version == "1.8.3"
    assert resp.prompts_issued == 1


# ---------------------------------------------------------------------------
# Test: add transitive dependency
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_add_transitive_dep(mock_client_cls, mock_pypi):
    """LLM suggests adding a missing transitive dependency."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="",
        correct_package="",
        add_package="protobuf==3.20.3",
        reasoning="TF 1.x requires protobuf<4.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=["tensorflow==1.15.0 (import: tensorflow)"],
        error_log="Descriptors cannot not be created directly",
        error_type="Unknown",
    )
    resp = handle(req)

    assert resp.fix_possible is True
    assert resp.add_package == "protobuf==3.20.3"
    assert resp.prompts_issued == 1


# ---------------------------------------------------------------------------
# Test: swap + add_package combined
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_swap_plus_add_dep(mock_client_cls, mock_pypi):
    """LLM suggests both a package swap and adding a transitive dep."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
        add_package="cryptography==3.3.2",
        reasoning="psycopg2 needs pg_config; psycopg2-binary ships wheels.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=["psycopg2 (import: psycopg2)"],
        error_log="pg_config executable not found",
        error_type="BuildFailure",
    )
    resp = handle(req)

    assert resp.fix_possible is True
    assert resp.wrong_package == "psycopg2"
    assert resp.correct_package == "psycopg2-binary"
    assert resp.add_package == "cryptography==3.3.2"


# ---------------------------------------------------------------------------
# Test: fix_possible=False
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_no_fix(mock_client_cls, mock_pypi):
    """LLM says no fix is possible."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=False,
        reasoning="Package requires CUDA, cannot be installed without GPU.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(error_log="nvcc not found")
    resp = handle(req)

    assert resp.fix_possible is False
    assert resp.prompts_issued == 1


# ---------------------------------------------------------------------------
# Test: LLM returns None (no output)
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.LlmClient")
def test_llm_no_output(mock_client_cls):
    """LLM returns None (timeout, parse failure, etc.)."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = None
    mock_client_cls.return_value = mock_client

    req = _make_request()
    resp = handle(req)

    assert resp.fix_possible is False
    assert "no output" in resp.error.lower()


# ---------------------------------------------------------------------------
# Test: LLM provider not available
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.LlmClient")
def test_provider_unavailable(mock_client_cls):
    """LLM provider is not reachable."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = False
    mock_client_cls.return_value = mock_client

    req = _make_request()
    resp = handle(req)

    assert resp.error == "LLM provider not available"


# ---------------------------------------------------------------------------
# Test: PyPI validation rejects hallucinated package
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=False)
@patch("llm_py.actions.recovery.LlmClient")
def test_pypi_validation_rejects_hallucination(mock_client_cls, mock_pypi):
    """LLM suggests a package that doesn't exist on PyPI."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="scrapy",
        correct_package="scrapy-python27",
        reasoning="Use the Python 2.7 fork.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request()
    resp = handle(req)

    assert resp.fix_possible is False
    assert any("not found on PyPI" in n for n in resp.notes)


# ---------------------------------------------------------------------------
# Test: same package without version is rejected
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_same_package_no_version_rejected(mock_client_cls, mock_pypi):
    """LLM returns same package name but no version — should be rejected."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="scrapy",
        correct_package="scrapy",
        version="",
        reasoning="Try reinstalling.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request()
    resp = handle(req)

    # Same package + no version = useless, should be rejected
    assert resp.fix_possible is False


@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
@pytest.mark.parametrize(
    ("resolved_packages", "wrong_package", "correct_package"),
    [
        (["PySide (import: PySide)"], "PySide", "PySide6"),
        (["python-ldap (import: ldap)"], "python-ldap", "ldap3"),
        (["python-Levenshtein (import: Levenshtein)"], "python-Levenshtein", "fuzzywuzzy"),
        (["oaipmh (import: oaipmh)"], "oaipmh", "a"),
        (["mosquitto (import: mosquitto)"], "mosquitto", "paho-mqtt"),
    ],
)
def test_namespace_incompatible_swap_rejected(
    mock_client_cls,
    mock_pypi,
    resolved_packages,
    wrong_package,
    correct_package,
):
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package=wrong_package,
        correct_package=correct_package,
        reasoning="Try a different package.",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        resolved_packages=resolved_packages,
        error_type="ModuleNotFound",
    )
    resp = handle(req)

    assert resp.fix_possible is False
    assert any("does not preserve import namespace" in note for note in resp.notes)


# ---------------------------------------------------------------------------
# Test: build error pattern RAG context is prepended
# ---------------------------------------------------------------------------

@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_build_error_patterns_injected(mock_client_cls, mock_pypi):
    """Build error pattern library context is injected into the prompt."""
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
    )
    mock_client_cls.return_value = mock_client

    req = _make_request(
        error_log="pg_config executable not found",
    )
    resp = handle(req)

    # Verify the client was called with pattern context in the prompt
    call_args = mock_client.complete_json.call_args
    user_prompt = call_args.kwargs.get("user_prompt") or call_args[1].get("user_prompt", "")
    assert "Known error patterns" in user_prompt
    assert "Build error pattern library matched" in resp.notes


# ---------------------------------------------------------------------------
# Test: max retry limit contract (REC-05)
# ---------------------------------------------------------------------------

@pytest.mark.rust_contract
def test_max_retries_contract():
    """REC-05: Max retry limit is enforced in Rust, not Python recovery action.

    Max retry enforcement happens in Rust resolver:
    - Retry loop: src/resolver/mod.rs:842 (for attempt_index in 0..=config.max_retries)
    - Config default: lib.rs:217 (max_retries = 5)

    Python recovery action is stateless:
    - It receives previous_attempts count in ResolutionRequest
    - It doesn't enforce the limit - just uses it for prompt context
    - Rust controls the retry loop and stops after max_retries

    This test documents the contract boundary for REC-05.
    Actual retry limit enforcement is tested in Rust integration tests.

    See: tests/test_resolver.rs for Rust-side retry limit tests.
    """
    # Python recovery action receives previous_attempts but doesn't enforce limit
    # The Rust resolver controls when to stop calling recovery

    # Verify ResolutionRequest includes previous_attempts field
    req = _make_request(previous_attempts=[
        ["psycopg2"],  # Attempt 1 failed
        ["psycopg2==2.8.0"],  # Attempt 2 failed
        ["psycopg2-binary"],  # Attempt 3 failed
    ])

    assert hasattr(req, "previous_attempts")
    assert len(req.previous_attempts) == 3

    # Python doesn't enforce max - Rust does
    # If Rust calls recovery with previous_attempts >= max_retries, that's a Rust bug
    # Python just processes the request regardless of attempt count


# ---------------------------------------------------------------------------
# Test: error log truncation sends tail, not head
# ---------------------------------------------------------------------------

def test_recovery_prompt_uses_tail_of_error_log():
    """Verify that recovery_user sends the LAST 50 lines, not the first."""
    from llm_py import prompts

    # Build a 100-line error log where the useful info is at the end
    lines = [f"installing line {i}" for i in range(80)]
    lines.append("ERROR: Command errored out with exit status 1")
    lines.append("  error: subprocess-exited-with-error")
    lines.append("  Building wheel for scrapy (setup.py) ... error")
    error_log = "\n".join(lines)

    prompt = prompts.recovery_user(
        resolved_packages=["scrapy"],
        error_log=error_log,
        snippet_source="import scrapy",
        python_version="2.7",
        error_type="BuildFailure",
        previous_attempts=[],
    )

    # The actual error should be in the prompt (it's in the last 50 lines)
    assert "Command errored out" in prompt
    # First few lines should NOT be (they'd be in head-50 but not tail-50)
    assert "installing line 0" not in prompt


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
