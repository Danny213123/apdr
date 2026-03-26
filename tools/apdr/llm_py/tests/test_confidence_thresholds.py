"""Tests for REC-04: Confidence threshold enforcement.

NOTE: Confidence thresholds are enforced in Rust resolver (src/resolver/mod.rs:747),
NOT in Python recovery action. These tests document the Rust/Python contract boundary.

The Python recovery action returns RecoveryResult without a confidence field.
The Rust resolver makes solvability decisions based on LLM confidence, then
decides whether to call recovery or mark as unsolvable.

REC-04 implementation:
- Solvability threshold (0.4): resolver/mod.rs:747
- Unsolvable cache threshold (0.95): resolver/mod.rs:750
- Config defaults: lib.rs:217

Future enhancement (not in this phase):
- D-20: Add confidence field to RecoveryResult for better observability
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure llm_py is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.actions.recovery import RecoveryResult
from llm_py.models import ResolutionResponse


@pytest.mark.rust_contract
def test_recovery_result_has_no_confidence_field():
    """REC-04: RecoveryResult does NOT include confidence (enforced in Rust)."""
    # Create a RecoveryResult - should not have confidence field
    result = RecoveryResult(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
        reasoning="Use pre-built binary.",
    )

    # Verify confidence is not part of the model
    assert not hasattr(result, "confidence")

    # Fields that ARE present
    assert hasattr(result, "fix_possible")
    assert hasattr(result, "wrong_package")
    assert hasattr(result, "correct_package")
    assert hasattr(result, "version")
    assert hasattr(result, "add_package")
    assert hasattr(result, "remove_package")
    assert hasattr(result, "reasoning")


@pytest.mark.rust_contract
def test_document_confidence_gap():
    """REC-04: Document that confidence enforcement is tested in Rust, not Python.

    Confidence thresholds are implemented in Rust resolver:
    - Solvability check (0.4): src/resolver/mod.rs:747
    - Unsolvable cache (0.95): src/resolver/mod.rs:750
    - Max retries (5): src/resolver/mod.rs:842

    Python recovery action is stateless - it doesn't:
    1. Know the LLM's confidence in solvability
    2. Track retry attempts
    3. Make caching decisions

    All of that happens in Rust. Python just returns RecoveryResult,
    and Rust decides whether to apply it based on broader context.

    See: tests/test_resolver.rs for Rust-side integration tests.
    """
    # This test exists purely for documentation
    # Actual confidence enforcement is tested in Rust integration tests
    pass


@pytest.mark.rust_contract
def test_recovery_response_contract():
    """REC-04: Verify ResolutionResponse structure matches Rust expectations."""
    # Create a ResolutionResponse as returned by recovery.handle()
    response = ResolutionResponse(
        fix_possible=True,
        wrong_package="psycopg2",
        correct_package="psycopg2-binary",
        notes=["Recovery suggestion accepted"],
        prompts_issued=1,
    )

    # Rust expects these fields for recovery responses
    assert hasattr(response, "fix_possible")
    assert hasattr(response, "wrong_package")
    assert hasattr(response, "correct_package")
    assert hasattr(response, "add_package")
    assert hasattr(response, "remove_package")
    assert hasattr(response, "version")
    assert hasattr(response, "notes")
    assert hasattr(response, "prompts_issued")
    assert hasattr(response, "error")

    # Confidence field IS present in ResolutionResponse (for solvability action)
    # but NOT used by recovery action
    assert hasattr(response, "confidence")

    # Verify the contract works correctly
    assert response.fix_possible is True
    assert response.wrong_package == "psycopg2"
    assert response.correct_package == "psycopg2-binary"
    assert response.prompts_issued == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
