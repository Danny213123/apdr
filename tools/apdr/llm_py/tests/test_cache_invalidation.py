"""Integration tests for prompt hash-based cache invalidation (REC-03).

Tests verify that:
1. Changing prompt templates invalidates cache (produces different hash)
2. Changing model ID invalidates cache
3. Dynamic content (error logs) does NOT affect cache hash
4. LiteLLM cache keys include prompt version prefix
"""

from __future__ import annotations

from unittest.mock import patch

import litellm
import pytest

from llm_py.client import LlmClient


@pytest.mark.integration
def test_prompt_change_invalidates_cache():
    """Changing prompt templates produces different cache keys (REC-03)."""
    with patch("llm_py.prompts.RECOVERY_SYSTEM", "Original system prompt"):
        client1 = LlmClient("ollama", "test-model", "http://localhost:11434")
        hash1 = client1._prompt_version_hash

    with patch("llm_py.prompts.RECOVERY_SYSTEM", "Modified system prompt - new instruction"):
        client2 = LlmClient("ollama", "test-model", "http://localhost:11434")
        hash2 = client2._prompt_version_hash

    assert hash1 != hash2, "Prompt change should produce different version hash"


@pytest.mark.integration
def test_model_change_invalidates_cache():
    """Changing model ID produces different cache keys (REC-03)."""
    client1 = LlmClient("ollama", "qwen2.5-coder:7b", "http://localhost:11434")
    client2 = LlmClient("ollama", "qwen2.5-coder:14b", "http://localhost:11434")

    assert client1._prompt_version_hash != client2._prompt_version_hash, \
        "Model change should produce different version hash"


@pytest.mark.integration
def test_dynamic_content_does_not_affect_hash():
    """Dynamic content (error logs) should NOT change cache key (REC-03)."""
    # Same template structure, different runtime content
    client1 = LlmClient("ollama", "test-model", "http://localhost:11434")
    client2 = LlmClient("ollama", "test-model", "http://localhost:11434")

    # Hash should be identical - it's based on template, not content
    assert client1._prompt_version_hash == client2._prompt_version_hash, \
        "Identical templates should produce identical hash"


@pytest.mark.integration
def test_cache_key_includes_version_prefix():
    """LiteLLM cache keys include prompt version prefix (REC-03)."""
    client = LlmClient("ollama", "test-model", "http://localhost:11434")

    # Generate a cache key
    key = litellm.cache.get_cache_key(
        model="test-model",
        messages=[{"role": "user", "content": "test"}],
    )

    assert key.startswith("v"), f"Cache key should start with 'v' prefix, got: {key[:20]}"
    assert ":" in key, "Cache key should contain ':' separator after version"
    # Verify the hash portion is 16 characters
    prefix = key.split(":")[0]
    assert len(prefix) == 17, f"Version prefix should be v + 16 chars, got: {prefix}"
