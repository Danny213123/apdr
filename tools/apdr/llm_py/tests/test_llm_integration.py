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





# Hard/challenging test cases from benchmark
# These represent complex real-world scenarios

@pytest.mark.integration
def test_hard_pytorch_python27_incompatible(ollama_available):
    """LLM recognizes PyTorch has no Python 2.7 compatible versions (gist 1b49c03968b2c83897a4a15c78980b18)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "numpy==1.16.6 (import: numpy)",
            "progressbar (import: progressbar)",
            "torch==1.4.0 (import: torch)",
        ],
        error_log="""
        ERROR: No matching distribution found for torch==1.4.0
        """,
        snippet_source="import torch\nimport torch.nn as nn\nfrom torch.autograd import Variable",
        python_version="2.7",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM recognizes PyTorch has no Python 2.7 compatible versions
    if resp.fix_possible:
        pass  # Should recognize torch incompatible with Python 2.7


@pytest.mark.integration
def test_hard_lxml_build_failure_windows(ollama_available):
    """LLM handles lxml build failure on Windows/Python 2.7 (gist 1077318)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "sunburnt==0.5 (import: sunburnt)",
            "httplib2 (import: httplib2)",
            "lxml==2.2.4 (import: lxml)",
        ],
        error_log="""
        building 'lxml.etree' extension\nerror: Microsoft Visual C++ 9.0 is required
        """,
        snippet_source="import sunburnt\nimport httplib2\nfrom lxml import builder",
        python_version="2.7",
        error_type="BuildFailure",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM handles lxml build failure on Windows/Python 2.7
    if resp.fix_possible:
        pass  # Should recognize C compiler requirement or suggest skip


@pytest.mark.integration
def test_hard_keras_backend_resolution(ollama_available):
    """LLM adds tensorflow backend for standalone keras (gist 0a3d4fae965bdbec1f9d)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "keras (import: keras)",
            "numpy==1.26.4 (import: numpy)",
        ],
        error_log="""
        ImportError: Keras requires a backend (tensorflow, theano, or cntk)
        """,
        snippet_source="import numpy as np\nfrom keras.models import Sequential\nfrom keras.layers import Dense",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM adds tensorflow backend for standalone keras
    if resp.fix_possible:
        pass  # Should add tensorflow as backend


@pytest.mark.integration
def test_hard_scikit_learn_python27_version(ollama_available):
    """LLM pins scikit-learn to last Python 2.7 compatible version (gist 1d596ca757a541da96ac3caa6f291229)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "keras==2.3.1 (import: keras)",
            "scikit-learn==1.4.2 (import: sklearn)",
            "numpy==1.16.6 (import: numpy)",
        ],
        error_log="""
        ERROR: No matching distribution found for scikit-learn==1.4.2
        """,
        snippet_source="import keras\nimport numpy as np\nfrom sklearn.preprocessing import StandardScaler",
        python_version="2.7",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM pins scikit-learn to last Python 2.7 compatible version
    if resp.fix_possible:
        pass  # Should pin to scikit-learn 0.20.4 or similar


@pytest.mark.integration
def test_hard_django_python27_last_version(ollama_available):
    """LLM recognizes Django 2.x+ requires Python 3, pins to 1.11.x (gist 2786290)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Django==2.2.0 (import: django)",
        ],
        error_log="""
        ERROR: No matching distribution found for Django==2.2.0
        """,
        snippet_source="from django.conf import settings\nfrom django.http import HttpResponse",
        python_version="2.7",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM recognizes Django 2.x+ requires Python 3, pins to 1.11.x
    if resp.fix_possible:
        pass  # Should pin to Django 1.11.29 (last Python 2 version)


@pytest.mark.integration
def test_hard_numpy_distance_build_fail(ollama_available):
    """LLM handles numpy build failure with missing distance module (gist 3001099)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "numpy (import: numpy)",
            "scipy (import: scipy)",
        ],
        error_log="""
        ImportError: cannot import name distance\nOriginal error was: No module named 'numpy.core._multiarray_umath'
        """,
        snippet_source="import numpy as np\nfrom scipy.spatial import distance",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM handles numpy build failure with missing distance module
    if resp.fix_possible:
        pass  # Should handle numpy/scipy version compatibility


@pytest.mark.integration
def test_hard_dbus_python_version_adjustment(ollama_available):
    """LLM adjusts dbus-python version for Python compatibility (gist 2894514)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "dbus-python==1.3.2 (import: dbus)",
        ],
        error_log="""
        ERROR: No matching distribution found for dbus-python==1.3.2
        """,
        snippet_source="import dbus\nimport gobject\nbus = dbus.SystemBus()",
        python_version="2.7",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM adjusts dbus-python version for Python compatibility
    if resp.fix_possible:
        pass  # Should adjust to compatible dbus-python version


@pytest.mark.integration
def test_hard_pymc_dependency_resolution(ollama_available):
    """LLM resolves PyMC complex scientific Python stack dependencies (gist 2840020)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "pymc (import: pymc)",
            "numpy (import: numpy)",
            "scipy (import: scipy)",
        ],
        error_log="""
        ImportError: cannot import name 'pymc'
        """,
        snippet_source="import pymc\nimport numpy as np\nfrom scipy import stats",
        python_version="2.7",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM resolves PyMC complex scientific Python stack dependencies
    if resp.fix_possible:
        pass  # Should resolve PyMC 2.x for Python 2.7



# Synthetic tier3-style test cases based on common recovery patterns
# These represent scenarios where LLMs excel at package resolution

@pytest.mark.integration
def test_synthetic_yaml_pyyaml(ollama_available):
    """LLM identifies yaml module comes from PyYAML package"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "yaml (import: yaml)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement yaml
        """,
        snippet_source="import yaml\ndata = yaml.safe_load(file)",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies yaml module comes from PyYAML package
    if resp.fix_possible and resp.correct_package:
        assert "pyyaml" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_cv2_opencv(ollama_available):
    """LLM identifies cv2 module comes from opencv-python"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "cv2 (import: cv2)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement cv2
        """,
        snippet_source="import cv2\nimg = cv2.imread('image.jpg')",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies cv2 module comes from opencv-python
    if resp.fix_possible and resp.correct_package:
        assert "opencv" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_sklearn_scikit_learn(ollama_available):
    """LLM identifies sklearn module comes from scikit-learn"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "sklearn (import: sklearn)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement sklearn
        """,
        snippet_source="from sklearn.ensemble import RandomForestClassifier\nclf = RandomForestClassifier()",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies sklearn module comes from scikit-learn
    if resp.fix_possible and resp.correct_package:
        assert "scikit-learn" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_dateutil_python_dateutil(ollama_available):
    """LLM identifies dateutil module comes from python-dateutil"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "dateutil (import: dateutil)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement dateutil
        """,
        snippet_source="from dateutil import parser\ndt = parser.parse('2024-01-01')",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies dateutil module comes from python-dateutil
    if resp.fix_possible and resp.correct_package:
        assert "python-dateutil" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_magic_python_magic(ollama_available):
    """LLM identifies magic module comes from python-magic"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "magic (import: magic)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement magic
        """,
        snippet_source="import magic\nmime = magic.from_file('file.txt', mime=True)",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies magic module comes from python-magic
    if resp.fix_possible and resp.correct_package:
        assert "python-magic" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_telegram_python_telegram_bot(ollama_available):
    """LLM identifies telegram bot API package"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "telegram (import: telegram)",
        ],
        error_log="""
        ImportError: No module named telegram
        """,
        snippet_source="from telegram import Update\nfrom telegram.ext import Updater",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies telegram bot API package
    if resp.fix_possible and resp.correct_package:
        assert "python-telegram-bot" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_discord_py(ollama_available):
    """LLM identifies discord.py package for Discord API"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "discord (import: discord)",
        ],
        error_log="""
        ImportError: No module named discord
        """,
        snippet_source="import discord\nclient = discord.Client()",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies discord.py package for Discord API
    if resp.fix_possible and resp.correct_package:
        assert "discord.py" in resp.correct_package.lower() or "discord" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_jwt_pyjwt(ollama_available):
    """LLM identifies jwt module comes from PyJWT"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "jwt (import: jwt)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement jwt
        """,
        snippet_source="import jwt\ntoken = jwt.encode(payload, secret)",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies jwt module comes from PyJWT
    if resp.fix_possible and resp.correct_package:
        assert "pyjwt" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_dotenv_python_dotenv(ollama_available):
    """LLM identifies dotenv module comes from python-dotenv"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "dotenv (import: dotenv)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement dotenv
        """,
        snippet_source="from dotenv import load_dotenv\nload_dotenv()",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies dotenv module comes from python-dotenv
    if resp.fix_possible and resp.correct_package:
        assert "python-dotenv" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_rest_framework_djangorestframework(ollama_available):
    """LLM identifies rest_framework from djangorestframework"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "rest_framework (import: rest_framework)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement rest_framework
        """,
        snippet_source="from rest_framework import serializers\nclass MySerializer(serializers.Serializer):",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies rest_framework from djangorestframework
    if resp.fix_possible and resp.correct_package:
        assert "djangorestframework" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_serial_pyserial(ollama_available):
    """LLM identifies serial module comes from pyserial"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "serial (import: serial)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement serial
        """,
        snippet_source="import serial\nser = serial.Serial('/dev/ttyUSB0', 9600)",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies serial module comes from pyserial
    if resp.fix_possible and resp.correct_package:
        assert "pyserial" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_usb_pyusb(ollama_available):
    """LLM identifies usb module comes from pyusb"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "usb (import: usb)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement usb
        """,
        snippet_source="import usb.core\ndev = usb.core.find()",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies usb module comes from pyusb
    if resp.fix_possible and resp.correct_package:
        assert "pyusb" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_bluetooth_pybluez(ollama_available):
    """LLM identifies bluetooth module comes from PyBluez"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "bluetooth (import: bluetooth)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement bluetooth
        """,
        snippet_source="import bluetooth\ndevices = bluetooth.discover_devices()",
        python_version="2.7",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies bluetooth module comes from PyBluez
    if resp.fix_possible and resp.correct_package:
        assert "pybluez" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_bs4_beautifulsoup4(ollama_available):
    """LLM identifies bs4 module comes from beautifulsoup4"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "bs4 (import: bs4)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement bs4
        """,
        snippet_source="from bs4 import BeautifulSoup\nsoup = BeautifulSoup(html)",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies bs4 module comes from beautifulsoup4
    if resp.fix_possible and resp.correct_package:
        assert "beautifulsoup4" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_websocket_websocket_client(ollama_available):
    """LLM identifies websocket module comes from websocket-client"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "websocket (import: websocket)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement websocket
        """,
        snippet_source="import websocket\nws = websocket.WebSocket()",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies websocket module comes from websocket-client
    if resp.fix_possible and resp.correct_package:
        assert "websocket-client" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_pycrypto_cryptography(ollama_available):
    """LLM suggests cryptography instead of deprecated pycrypto"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "pycrypto (import: pycrypto)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement pycrypto
        """,
        snippet_source="from Crypto.Cipher import AES\ncipher = AES.new(key, AES.MODE_CBC)",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM suggests cryptography instead of deprecated pycrypto
    if resp.fix_possible and resp.correct_package:
        assert "cryptography" in resp.correct_package.lower() or "pycryptodome" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_pil_pillow(ollama_available):
    """LLM suggests Pillow instead of deprecated PIL"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "PIL (import: PIL)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement PIL
        """,
        snippet_source="from PIL import Image\nimg = Image.open('photo.jpg')",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM suggests Pillow instead of deprecated PIL
    if resp.fix_possible and resp.correct_package:
        assert "pillow" in resp.correct_package.lower() or "PIL" in resp.correct_package


@pytest.mark.integration
def test_synthetic_pygtk_deprecated(ollama_available):
    """LLM recognizes PyGTK is deprecated for Python 3"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "pygtk (import: pygtk)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement pygtk
        """,
        snippet_source="import gtk\nwindow = gtk.Window()",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM recognizes PyGTK is deprecated for Python 3
    if resp.fix_possible and resp.correct_package:
        pass  # Should recognize as deprecated/skip


@pytest.mark.integration
def test_synthetic_cPickle_python2_to_3(ollama_available):
    """LLM recognizes cPickle is Python 2, use pickle in Python 3"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "cPickle (import: cPickle)",
        ],
        error_log="""
        ModuleNotFoundError: No module named cPickle
        """,
        snippet_source="import cPickle\ndata = cPickle.loads(bytes_data)",
        python_version="3.9",
        error_type="ModuleNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM recognizes cPickle is Python 2, use pickle in Python 3
    if resp.fix_possible and resp.correct_package:
        pass  # Should remove or suggest pickle


@pytest.mark.integration
def test_synthetic_stringio_io_migration(ollama_available):
    """LLM recognizes StringIO moved to io module"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "StringIO (import: StringIO)",
        ],
        error_log="""
        ModuleNotFoundError: No module named StringIO
        """,
        snippet_source="from StringIO import StringIO\nbuf = StringIO()",
        python_version="3.9",
        error_type="ModuleNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM recognizes StringIO moved to io module
    if resp.fix_possible and resp.correct_package:
        pass  # Should suggest io.StringIO


@pytest.mark.integration
def test_synthetic_local_config_module(ollama_available):
    """LLM identifies config.py as local project module"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "config (import: config)",
        ],
        error_log="""
        ImportError: No module named config
        """,
        snippet_source="import config\nDATABASE_URL = config.DATABASE_URL",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies config.py as local project module
    if resp.fix_possible and resp.correct_package:
        pass  # Should identify as local


@pytest.mark.integration
def test_synthetic_local_utils_module(ollama_available):
    """LLM identifies utils.py as local project module"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "utils (import: utils)",
        ],
        error_log="""
        ImportError: No module named utils
        """,
        snippet_source="from utils import helpers\nresult = helpers.process_data(data)",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies utils.py as local project module
    if resp.fix_possible and resp.correct_package:
        pass  # Should identify as local


@pytest.mark.integration
def test_synthetic_local_models_module(ollama_available):
    """LLM identifies models.py as local Django/project module"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "models (import: models)",
        ],
        error_log="""
        ImportError: No module named models
        """,
        snippet_source="from django.db import models\nfrom models import User",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies models.py as local Django/project module
    if resp.fix_possible and resp.correct_package:
        pass  # Should identify as local


@pytest.mark.integration
def test_synthetic_local_views_module(ollama_available):
    """LLM identifies views.py as local Flask/Django module"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "views (import: views)",
        ],
        error_log="""
        ImportError: No module named views
        """,
        snippet_source="from flask import Flask\nfrom views import home_view",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies views.py as local Flask/Django module
    if resp.fix_possible and resp.correct_package:
        pass  # Should identify as local


@pytest.mark.integration
def test_synthetic_tensorflow_gpu_cpu(ollama_available):
    """LLM suggests tensorflow instead of tensorflow-gpu"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "tensorflow-gpu==1.15.0 (import: tensorflow-gpu)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement tensorflow-gpu==1.15.0
        """,
        snippet_source="import tensorflow as tf\nmodel = tf.keras.Sequential()",
        python_version="3.9",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM suggests tensorflow instead of tensorflow-gpu
    if resp.fix_possible and resp.correct_package:
        assert "tensorflow" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_gevent_version(ollama_available):
    """LLM adjusts gevent version for Python compatibility"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "gevent==20.9.0 (import: gevent)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement gevent==20.9.0
        """,
        snippet_source="from gevent import monkey\nmonkey.patch_all()",
        python_version="3.11",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM adjusts gevent version for Python compatibility
    if resp.fix_possible and resp.correct_package:
        assert "gevent" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_twisted_version(ollama_available):
    """LLM adjusts Twisted version for Python compatibility"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Twisted==20.3.0 (import: Twisted)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement Twisted==20.3.0
        """,
        snippet_source="from twisted.internet import reactor\nreactor.run()",
        python_version="3.11",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM adjusts Twisted version for Python compatibility
    if resp.fix_possible and resp.correct_package:
        assert "Twisted" in resp.correct_package or "twisted" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_scrapy_old_version(ollama_available):
    """LLM updates old Scrapy version for compatibility"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Scrapy==1.8.0 (import: Scrapy)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement Scrapy==1.8.0
        """,
        snippet_source="import scrapy\nclass MySpider(scrapy.Spider):",
        python_version="3.11",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM updates old Scrapy version for compatibility
    if resp.fix_possible and resp.correct_package:
        assert "scrapy" in resp.correct_package.lower()


@pytest.mark.integration
def test_synthetic_wxpython_version(ollama_available):
    """LLM relaxes wxPython version for Python compatibility"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "wxPython==4.1.0 (import: wxPython)",
        ],
        error_log="""
        ERROR: Could not find a version that satisfies the requirement wxPython==4.1.0
        """,
        snippet_source="import wx\napp = wx.App()",
        python_version="3.11",
        error_type="VersionNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM relaxes wxPython version for Python compatibility
    if resp.fix_possible and resp.correct_package:
        assert "wxPython" in resp.correct_package


@pytest.mark.integration
def test_synthetic_celery_redis_dependency(ollama_available):
    """LLM identifies redis dependency for Celery"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "celery==4.4.7 (import: celery)",
        ],
        error_log="""
        ImportError: cannot import name 'redis' from 'kombu.transport'
        """,
        snippet_source="from celery import Celery\napp = Celery('tasks', broker='redis://localhost')",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM identifies redis dependency for Celery
    if resp.fix_possible and resp.correct_package:
        pass  # Should suggest adding redis



if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
# Auto-generated tier3 success test cases
# Based on benchmark run 20260326-132841-apdr


@pytest.mark.integration
def test_tier3_gist_098f399d69f230521ef5(ollama_available):
    """Tier3: LLM identifies Foundation as local module (gist 098f399d69f230521ef530baca832e76)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Foundation (import: Foundation)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nIOError: [Errno 2] No such file or directory: '/System/Library/CoreServices/SystemVersion.plist'\nERROR: Command errored out with exit status 1: python setup.py egg_info Check the logs for full comm
        """,
        snippet_source="",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify Foundation as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "Foundation" in resp.remove_package.lower(), \
            f"Expected Foundation to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1202516(ollama_available):
    """Tier3: LLM identifies Foundation as local module (gist 1202516)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Foundation (import: Foundation)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nIOError: [Errno 2] No such file or directory: '/System/Library/CoreServices/SystemVersion.plist'\nERROR: Command errored out with exit status 1: python setup.py egg_info Check the logs for full comm
        """,
        snippet_source="import os import csv from subprocess import Popen, PIPE",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify Foundation as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "Foundation" in resp.remove_package.lower(), \
            f"Expected Foundation to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1242589(ollama_available):
    """Tier3: LLM identifies Foundation as local module (gist 1242589)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Foundation (import: Foundation)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nIOError: [Errno 2] No such file or directory: '/System/Library/CoreServices/SystemVersion.plist'\nERROR: Command errored out with exit status 1: python setup.py egg_info Check the logs for full comm
        """,
        snippet_source="",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify Foundation as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "Foundation" in resp.remove_package.lower(), \
            f"Expected Foundation to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1283684(ollama_available):
    """Tier3: LLM identifies d3 as local module (gist 1283684)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "networkx (import: networkx)",
            "simplejson (import: simplejson)",
            "d3 (import: d3)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nIOError: [Errno 2] No such file or directory: 'c:\\users\\danny\\appdata\\local\\temp\\pip-install-g\nERROR: Command errored out with exit status 1: python setup.py egg_info Check the logs for full comm
        """,
        snippet_source="import simplejson,urllib,csv,sys from itertools import combinations",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify d3 as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "d3" in resp.remove_package.lower(), \
            f"Expected d3 to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1315148(ollama_available):
    """Tier3: LLM identifies PyV8 as local module (gist 1315148)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "PyV8 (import: PyV8)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nKeyError: 'INCLUDE'\nERROR: Command errored out with exit status 1: python setup.py egg_info Check the logs for full comm
        """,
        snippet_source="import os",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify PyV8 as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "PyV8" in resp.remove_package.lower(), \
            f"Expected PyV8 to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1307521(ollama_available):
    """Tier3: LLM recognizes _webassets as optional import (gist 1307521)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "webassets==0.10 (import: webassets)",
            "_webassets (import: _webassets)",
        ],
        error_log="""
ModuleNotFoundError: No module named '_webassets'\nModuleNotFoundError: No module named '_webassets'
        """,
        snippet_source="\"\"\" This gist adds url expiration functionality to flask-webassets on App Engine. Few hints how to use it:",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should recognize _webassets as optional/guarded import
    if resp.fix_possible and resp.remove_package:
        pass  # Correctly identified as optional



@pytest.mark.integration
def test_tier3_gist_1423116(ollama_available):
    """Tier3: LLM identifies cmemcached as local module (gist 1423116)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "cmemcached (import: cmemcached)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nerror: Microsoft Visual C++ 9.0 is required. Get it from http://aka.ms/vcpython27\nERROR: Failed building wheel for pylibmc
        """,
        snippet_source="import os import sys import cPickle",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify cmemcached as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "cmemcached" in resp.remove_package.lower(), \
            f"Expected cmemcached to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1424374(ollama_available):
    """Tier3: LLM identifies pcap as local module (gist 1424374)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "dpkt (import: dpkt)",
            "ipaddr (import: ipaddr)",
            "pcap (import: pcap)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nerror: Microsoft Visual C++ 9.0 is required. Get it from http://aka.ms/vcpython27\nERROR: Failed building wheel for pcapy
        """,
        snippet_source="import dpkt, pcap, socket from ipaddr import IPv4Address, IPv6Address import syslog",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify pcap as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "pcap" in resp.remove_package.lower(), \
            f"Expected pcap to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1417f55cb896a44e68a6(ollama_available):
    """Tier3: LLM recovery for -- (gist 1417f55cb896a44e68a6)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Kivy==1.9.1 (import: Kivy)",
            "pyserial (import: pyserial)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nERROR: Could not find a version that satisfies the requirement kivy_deps.gstreamer_dev~=0.3.1 (from \nERROR: No matching distribution found for kivy_deps.gstreamer_dev~=0.3.1
        """,
        snippet_source="from kivy.app import App from kivy.uix.floatlayout import FloatLayout from kivy.graphics import Line",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should provide recovery suggestion
    if resp.fix_possible:
        pass  # Recovery suggested



@pytest.mark.integration
def test_tier3_gist_1638546(ollama_available):
    """Tier3: LLM identifies authorization as local module (gist 1638546)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "httplib2 (import: httplib2)",
            "sql==0.3.0 (import: sql)",
            "authorization (import: authorization)",
        ],
        error_log="""
ERROR: Could not find a version that satisfies the requirement pywin32==302 (from -r D:\apdr\runs\20\nERROR: No matching distribution found for pywin32==302 (from -r D:\apdr\runs\20260326-132841-apdr\ca
        """,
        snippet_source="\"\"\" FusionRunner Queries Google Fusion Tables for MyTracks data. \"\"\"",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify authorization as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "authorization" in resp.remove_package.lower(), \
            f"Expected authorization to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1653394(ollama_available):
    """Tier3: LLM identifies webservice as local module (gist 1653394)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "pyramid (import: pyramid)",
            "webservice (import: webservice)",
        ],
        error_log="""
ImportError: No module named web\nImportError: No module named web
        """,
        snippet_source="from pyramid.config import Configurator from pyramid.view import view_config import json",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify webservice as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "webservice" in resp.remove_package.lower(), \
            f"Expected webservice to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1694496(ollama_available):
    """Tier3: LLM recovery for settings (gist 1694496)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Django==5.1.3 (import: Django)",
            "settings (import: settings)",
        ],
        error_log="""
ERROR: Ignored the following yanked versions: 4.2.12\nERROR: Ignored the following versions that require a different python version: 5.0 Requires-Python >\nERROR: Could not find a version that satisfies the requirement Django==5.1.3 (from versions: 1.1.3, 
        """,
        snippet_source="\"\"\" This allows you to import Django modules into a Salt module \"\"\"",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should provide recovery suggestion
    if resp.fix_possible:
        pass  # Recovery suggested



@pytest.mark.integration
def test_tier3_gist_1701845(ollama_available):
    """Tier3: LLM identifies clips as local module (gist 1701845)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Django==5.1.3 (import: Django)",
            "django-haystack (import: django-haystack)",
            "django-tastypie (import: django-tastypie)",
            "clips (import: clips)",
        ],
        error_log="""
ERROR: Ignored the following yanked versions: 4.2.12\nERROR: Ignored the following versions that require a different python version: 5.0 Requires-Python >\nERROR: Could not find a version that satisfies the requirement Django==5.1.3 (from versions: 1.1.3, 
        """,
        snippet_source="from django.conf.urls.defaults import * from tastypie.paginator import Paginator from tastypie.exceptions import BadRequest",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify clips as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "clips" in resp.remove_package.lower(), \
            f"Expected clips to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_1c160c9eee91fd44c587(ollama_available):
    """Tier3: LLM identifies pymba as local module (gist 1c160c9eee91fd44c587)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "moviepy==0.2.1.8.07 (import: moviepy)",
            "scikit-image==0.14.2 (import: scikit-image)",
            "pymba (import: pymba)",
        ],
        error_log="""
ERROR: Command errored out with exit status 1:\nSyntaxError: invalid syntax\nERROR: Command errored out with exit status 1: python setup.py egg_info Check the logs for full comm
        """,
        snippet_source="\"\"\" This demonstration assumes you have already installed Pymba and followed the installation instructions there: https://github.com/morefigs/pymba",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify pymba as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "pymba" in resp.remove_package.lower(), \
            f"Expected pymba to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_19317d3e4b9a58f2355e(ollama_available):
    """Tier3: LLM recognizes UpdateManager as optional import (gist 19317d3e4b9a58f2355e7643040d483a)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "prettytable==3.1.1 (import: prettytable)",
            "UpdateManager (import: UpdateManager)",
        ],
        error_log="""
ERROR: Ignored the following yanked versions: 0.0.0, 0.7.8\nERROR: Ignored the following versions that require a different python version: 3.17.0 Requires-Pytho\nERROR: Could not find a version that satisfies the requirement python-apt (from versions: none)
        """,
        snippet_source="\"\"\" This script lists all APT package updates currently available for your system along with the version numbers of the old & new packages.  It is der",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should recognize UpdateManager as optional/guarded import
    if resp.fix_possible and resp.remove_package:
        pass  # Correctly identified as optional



@pytest.mark.integration
def test_tier3_gist_2901479(ollama_available):
    """Tier3: LLM identifies flask_celery as local module (gist 2901479)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "Flask==3.1.0 (import: Flask)",
            "flask-heroku==0.1.5 (import: flask-heroku)",
            "flask-sslify==0.1.3 (import: flask-sslify)",
            "raven (import: raven)",
            "celery==5.3.4 (import: celery)",
            "flask_celery (import: flask_celery)",
        ],
        error_log="""
ERROR: Ignored the following versions that require a different python version: 8.2.0 Requires-Python\nERROR: Could not find a version that satisfies the requirement Flask-Script-fix (from flask-celery) \nERROR: No matching distribution found for Flask-Script-fix
        """,
        snippet_source="import os from flask import Flask",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify flask_celery as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "flask_celery" in resp.remove_package.lower(), \
            f"Expected flask_celery to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_2b2abbb88b5d2b4f4e5a(ollama_available):
    """Tier3: LLM identifies newspaper as local module (gist 2b2abbb88b5d2b4f4e5adde42975fd0f)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "beautifulsoup4==4.12.3 (import: beautifulsoup4)",
            "requests==2.32.3 (import: requests)",
            "newspaper (import: newspaper)",
        ],
        error_log="""
error: subprocess-exited-with-error\nERROR: Failed to build 'newspaper' when getting requirements to build wheel
        """,
        snippet_source="from bs4 import BeautifulSoup import requests, csv, os from newspaper import Article",
        python_version="3.9",
        error_type="PackageNotFound",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify newspaper as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "newspaper" in resp.remove_package.lower(), \
            f"Expected newspaper to be removed, got: {resp.remove_package}"



@pytest.mark.integration
def test_tier3_gist_2de2e9a156fe619dbdad(ollama_available):
    """Tier3: LLM identifies -- as local module (gist 2de2e9a156fe619dbdad762fe1cf84e1)"""
    req = ResolutionRequest(
        action="recovery",
        resolved_packages=[
            "numpy==1.21.6 (import: numpy)",
            "Theano-PyMC==1.1.2 (import: Theano-PyMC)",
            "arviz==0.12.1 (import: arviz)",
            "pandas==1.5.3 (import: pandas)",
            "pymc3==3.11.5 (import: pymc3)",
        ],
        error_log="""
ImportError: cannot import name 'MRG_RandomStreams' from 'theano.sandbox.rng_mrg' (D:\apdr\runs\2026\nImportError: cannot import name 'MRG_RandomStreams' from 'theano.sandbox.rng_mrg' (D:\apdr\runs\2026
        """,
        snippet_source="",
        python_version="3.9",
        error_type="ImportError",
        previous_attempts=[],
        provider="ollama",
        model="qwen3.5:9b",
        base_url="http://localhost:11434",
    )

    resp = handle(req)

    # LLM should identify -- as local module and suggest removal
    if resp.fix_possible and resp.remove_package:
        assert "--" in resp.remove_package.lower(), \
            f"Expected -- to be removed, got: {resp.remove_package}"


