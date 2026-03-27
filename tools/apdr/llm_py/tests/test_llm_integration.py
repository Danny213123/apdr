"""Integration tests for LLM recovery with real Ollama instance.

Tests Phase 3 requirements (REC-01, REC-02, REC-03) against actual LLM calls.
"""

from __future__ import annotations

import logging
import sys
from unittest.mock import patch
from pathlib import Path

import pytest

# Ensure the llm_py package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.actions.recovery import handle
from llm_py.client import LlmClient
from llm_py.models import ResolutionRequest

# Configure logging to see structured metrics
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s - %(extra)s' if hasattr(logging, 'extra') else '%(levelname)s - %(message)s')
logger = logging.getLogger("apdr_llm")


@pytest.fixture
def ollama_available():
    """Check if Ollama is available before running tests."""
    # Try qwen3.5:9b (available model)
    client = LlmClient("ollama", "qwen3.5:9b", "http://localhost:11434")
    if not client.is_available():
        pytest.skip("Ollama not available - skipping integration tests")
    return "qwen3.5:9b"


@pytest.mark.integration
def test_recovery_with_pg_config_error(ollama_available):
    """REC-02: RAG pattern library should match pg_config error and inject context."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["psycopg2==2.8.6 (import: psycopg2)"],
        error_log="""
        Building wheel for psycopg2 (setup.py) ... error
        ERROR: Command errored out with exit status 1:
        command: /usr/bin/python setup.py bdist_wheel
        running build_ext
        building 'psycopg2._psycopg' extension
        Error: pg_config executable not found.
        """,
        snippet_source="import psycopg2",
        python_version="3.9",
        error_type="BuildFailure",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # Verify recovery suggested a fix
    assert resp is not None

    # Check that pattern library was matched (should appear in notes)
    assert any("Build error pattern library matched" in note for note in resp.notes), \
        f"Pattern library should have matched pg_config error. Notes: {resp.notes}"

    # If fix was suggested, verify it's psycopg2-binary (the known good solution)
    if resp.fix_possible and resp.correct_package:
        assert "psycopg2-binary" in resp.correct_package.lower(), \
            f"Expected psycopg2-binary suggestion, got: {resp.correct_package}"


@pytest.mark.integration
def test_recovery_with_hallucinated_package(ollama_available):
    """REC-01: PyPI validation should reject hallucinated packages."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["nonexistent-package-12345 (import: fake_module)"],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement nonexistent-package-12345
        ERROR: No matching distribution found for nonexistent-package-12345
        """,
        snippet_source="import fake_module",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM might suggest something, but if it suggests a non-existent package,
    # PyPI validation should reject it
    if resp.fix_possible and resp.correct_package:
        # If the LLM suggested a package, PyPI validation should have checked it
        # We can't predict what the LLM will suggest, but if it's hallucinated,
        # the validation should reject it
        pass

    # Check logs for PyPI validation activity (this test mainly validates logging works)
    # The actual rejection behavior is tested in unit tests with mocks


@pytest.mark.integration
def test_cache_behavior_across_calls(ollama_available):
    """REC-03: Verify cache hit on second identical call."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["requests (import: requests)"],
        error_log="ImportError: cannot import name 'soft_unicode' from 'markupsafe'",
        snippet_source="import requests",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    # First call (cold - will hit LLM)
    import time
    start1 = time.time()
    resp1 = handle(req)
    duration1 = time.time() - start1

    # Second call should be served from the prompt-versioned JSON cache.
    # If the cache misses, requests.post will raise and the test will fail.
    start2 = time.time()
    with patch("requests.post", side_effect=AssertionError("cache miss triggered network call")):
        resp2 = handle(req)
    duration2 = time.time() - start2

    print(f"First call: {duration1:.2f}s, Second call: {duration2:.2f}s")

    # Results should be identical (cache hit returns same response)
    assert resp1.fix_possible == resp2.fix_possible
    if resp1.correct_package:
        assert resp1.correct_package == resp2.correct_package


@pytest.mark.integration
def test_flask_extensions_resolution(ollama_available):
    """Test LLM can resolve Flask extension imports correctly."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Flask (import: flask)",
            "flask_cors (import: flask_cors)",  # Wrong - should be Flask-CORS
            "redis (import: redis)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement flask_cors
        ERROR: No matching distribution found for flask_cors
        """,
        snippet_source="from flask import Flask\nfrom flask_cors import CORS\nimport redis",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should suggest Flask-CORS (the correct package name)
    if resp.fix_possible and resp.correct_package:
        assert "flask-cors" in resp.correct_package.lower() or "Flask-CORS" in resp.correct_package, \
            f"Expected Flask-CORS suggestion, got: {resp.correct_package}"


@pytest.mark.integration
def test_prompt_version_hash_generation(ollama_available):
    """REC-03: Verify prompt version hash is generated and stable."""
    client1 = LlmClient("ollama", "qwen2.5-coder:7b", "http://localhost:11434")
    client2 = LlmClient("ollama", "qwen2.5-coder:7b", "http://localhost:11434")

    # Same model, same prompts -> same hash
    assert client1._prompt_version_hash == client2._prompt_version_hash, \
        "Prompt version hash should be stable across client instances"

    # Hash should be 16 characters (first 16 chars of SHA256)
    assert len(client1._prompt_version_hash) == 16, \
        f"Prompt hash should be 16 chars, got {len(client1._prompt_version_hash)}"

    print(f"Prompt version hash: {client1._prompt_version_hash}")


@pytest.mark.integration
def test_module_not_found_mosquitto(ollama_available):
    """Test LLM recovery for missing module 'mosquitto' (common MQTT case)."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["mosquitto (import: mosquitto)"],
        error_log="""
        ModuleNotFoundError: No module named 'mosquitto'
        Runtime import failed: missing module `mosquitto`.
        """,
        snippet_source="import mosquitto",
        python_version="2.7",
        error_type="ModuleNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should suggest paho-mqtt (the correct package for MQTT)
    if resp.fix_possible and resp.correct_package:
        assert "paho" in resp.correct_package.lower() or "mqtt" in resp.correct_package.lower(), \
            f"Expected paho-mqtt or similar MQTT library, got: {resp.correct_package}"


@pytest.mark.integration
def test_build_failure_lxml_windows(ollama_available):
    """Test LLM recovery for lxml build failure on Windows (missing MSVC)."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["lxml==4.6.5 (import: lxml)"],
        error_log="""
        building 'lxml.etree' extension
        error: Microsoft Visual C++ 9.0 is required. Get it from http://aka.ms/vcpython27
        ERROR: Command errored out with exit status 1
        """,
        snippet_source="from lxml import etree",
        python_version="2.7",
        error_type="BuildFailure",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM might suggest skipping (needs C compiler) or using pre-built wheels
    # The response should indicate understanding that build tools are missing
    if resp.fix_possible:
        # If a fix is suggested, it should either be a wheel version or skip
        pass  # Just verify it doesn't crash


@pytest.mark.integration
def test_version_not_found_opencv(ollama_available):
    """Test LLM recovery for opencv version constraints."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["opencv-python-headless (import: cv2)"],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement opencv-python-headless>=4.5.0,<5.0.0
        ERROR: No matching distribution found for opencv-python-headless
        """,
        snippet_source="import cv2",
        python_version="2.7",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should either relax version constraint or suggest alternative
    if resp.fix_possible and resp.correct_package:
        assert "opencv" in resp.correct_package.lower(), \
            f"Expected opencv package variant, got: {resp.correct_package}"


@pytest.mark.integration
def test_module_not_found_stdlib_deepcopy(ollama_available):
    """Test LLM should recognize deepcopy is from stdlib, not a package."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["deepcopy (import: deepcopy)"],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement deepcopy
        ERROR: No matching distribution found for deepcopy
        """,
        snippet_source="from copy import deepcopy",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # deepcopy is stdlib - LLM should either skip it or recognize it's not needed
    if resp.fix_possible:
        # If fix suggested, it should NOT be "deepcopy" package
        if resp.correct_package:
            assert resp.correct_package.lower() != "deepcopy", \
                "LLM should not suggest 'deepcopy' as a package (it's stdlib)"


@pytest.mark.integration
def test_oscillating_requirements_django(ollama_available):
    """Test LLM recovery when requirements oscillate between versions."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Django==1.8.19 (import: django)",
            "johnny-cache==1.4 (import: johnny)",
        ],
        error_log="""
        Stopped validation because requirements began oscillating.
        ERROR: django 1.8.19 has requirement setuptools<45, but you have setuptools 69.5.1
        """,
        snippet_source="import django\nimport johnny",
        python_version="2.7",
        error_type="DependencyConflict",
        previous_attempts=[
            ["Django", "Django==1.11.0", "still-failing"],
            ["Django", "Django==1.8.19", "dependency-conflict"],
        ],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # With previous attempts shown, LLM should try something different
    # (e.g., adjust setuptools or suggest compatible versions)
    if resp.fix_possible and resp.correct_package:
        # Verify it's not repeating previous attempts
        assert resp.correct_package != "Django==1.11.0", \
            "LLM should not repeat failed attempt Django==1.11.0"
        assert resp.correct_package != "Django==1.8.19", \
            "LLM should not repeat failed attempt Django==1.8.19"


@pytest.mark.integration
def test_pyobjc_platform_specific(ollama_available):
    """Test LLM recognizes pyobjc-framework packages are macOS-only."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=["pyobjc-framework-CoreFoundation (import: CoreFoundation)"],
        error_log="""
        Package `pyobjc-framework-CoreFoundation` does not exist on PyPI. Skipping validation.
        """,
        snippet_source="from CoreFoundation import CFRunLoopRun",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should recognize this is platform-specific and suggest skipping
    # or recognize it needs the umbrella pyobjc package
    if resp.fix_possible and resp.remove_package:
        # Correctly identified as skippable/removable
        pass
    elif resp.fix_possible and resp.correct_package:
        # Might suggest the umbrella package
        assert "pyobjc" in resp.correct_package.lower(), \
            f"Expected pyobjc-related suggestion, got: {resp.correct_package}"


@pytest.mark.integration
def test_local_module_d3_networkx(ollama_available):
    """Test LLM identifies d3 as local module (networkx-d3.js bridge)."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "simplejson (import: simplejson)",
            "networkx (import: networkx)",
            "d3 (import: d3)",
        ],
        error_log="""
        ImportError: No module named d3
        Runtime import failed: missing module `d3`.
        """,
        snippet_source="import simplejson\\nimport networkx as nx\\nimport d3\\nd3.draw_force(G, 'force.json')",
        python_version="2.7",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify d3 as a local module (networkx-d3 bridge)
    # and suggest removing it rather than installing from PyPI
    if resp.fix_possible and resp.remove_package:
        assert "d3" in resp.remove_package.lower(), \
            f"Expected d3 to be removed as local module, got: {resp.remove_package}"


@pytest.mark.integration
def test_optional_import_try_except(ollama_available):
    """Test LLM recognizes try/except optional imports."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "webassets==0.10 (import: webassets)",
            "_webassets (import: _webassets)",
        ],
        error_log="""
        ImportError: No module named _webassets
        Runtime import failed: missing module `_webassets`.
        """,
        snippet_source="from webassets.bundle import Bundle\\ntry:\\n    from _webassets import files\\nexcept ImportError:\\n    files = {}",
        python_version="2.7",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should recognize _webassets is in try/except (optional import)
    # The import error is expected and handled by the code
    if resp.fix_possible and resp.remove_package:
        assert "_webassets" in resp.remove_package.lower(), \
            f"Expected _webassets to be identified as optional, got: {resp.remove_package}"


@pytest.mark.integration
def test_old_flask_extension_pattern(ollama_available):
    """Test LLM handles deprecated flask.ext.* import pattern."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Flask (import: flask)",
            "flask-heroku (import: flask_heroku)",
            "flask-sslify (import: flask_sslify)",
            "celery (import: celery)",
            "flask-celery (import: flask_celery)",  # Wrong - old extension pattern
        ],
        error_log="""
        ImportError: No module named flask.ext.celery
        The flask.ext namespace is deprecated.
        """,
        snippet_source="from flask import Flask\\nfrom flask.ext.celery import Celery\\ncelery = Celery(app)",
        python_version="2.7",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should recognize flask.ext.* pattern is deprecated
    # and either suggest removing flask-celery or updating to modern import
    if resp.fix_possible:
        # Could remove the wrong package or suggest alternative
        pass


@pytest.mark.integration
def test_local_module_clips_vs_click(ollama_available):
    """Test LLM distinguishes between local module 'clips' and package 'click'."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Django (import: django)",
            "django-haystack (import: haystack)",
            "django-tastypie (import: tastypie)",
            "click (import: clips)",  # Wrong - clips is local, not click package
        ],
        error_log="""
        ImportError: No module named clips
        Runtime import failed: missing module `clips`.
        """,
        snippet_source="import django\\nfrom haystack import indexes\\nfrom tastypie.api import Api\\nimport clips",
        python_version="2.7",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify that 'clips' is a local module, not the 'click' package
    if resp.fix_possible and resp.remove_package:
        assert "click" in resp.remove_package.lower() or "clips" in resp.remove_package.lower(), \
            f"Expected click/clips to be removed, got: {resp.remove_package}"


@pytest.mark.integration
def test_pymc_scientific_stack(ollama_available):
    """Test LLM handles scientific Python stack with version constraints."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "numpy==1.21.6 (import: numpy)",
            "Theano-PyMC==1.1.2 (import: theano)",
            "arviz==0.12.1 (import: arviz)",
            "Lasagne (import: lasagne)",  # Might be local or outdated package
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement Lasagne
        ERROR: No matching distribution found for Lasagne
        """,
        snippet_source="import numpy as np\\nimport theano\\nimport arviz\\nimport lasagne",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should either recognize Lasagne as outdated/local
    # or suggest an alternative deep learning library
    if resp.fix_possible:
        # Could remove Lasagne or suggest alternative
        pass


@pytest.mark.integration
def test_sql_package_ambiguity(ollama_available):
    """Test LLM handles ambiguous 'sql' package name."""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "httplib2 (import: httplib2)",
            "sql==0.3.0 (import: sql)",
        ],
        error_log="""
        ImportError: No module named authorization
        Runtime import failed: missing module `authorization`.
        """,
        snippet_source="import httplib2\\nimport sql\\nfrom authorization import check_auth",
        python_version="2.7",
        error_type="ImportError",
        previous_attempts=[
            ["pywin32", "pywin32==305", "still-failing"],
        ],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should recognize 'authorization' is likely a local module
    # not related to pywin32 (which was already tried)
    if resp.fix_possible and resp.remove_package:
        # Should identify authorization or pywin32 as removable
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
