"""Shared pytest fixtures for LLM recovery action tests.

Provides reusable test utilities:
- temp_cache_dir: Isolated cache directory for LiteLLM tests
- mock_llm_response: Factory for creating RecoveryResult objects
- mock_pypi_checker: Mocked PyPI package validation
- sample_error_logs: Known error patterns for testing RAG injection
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def temp_cache_dir(tmp_path: Path) -> Path:
    """Temporary directory for LiteLLM cache tests.

    Used by Plan 02 (cache invalidation) to test cache_path behavior
    without polluting the real cache directory.

    Returns:
        Path to a temporary directory that pytest will clean up automatically.
    """
    cache_dir = tmp_path / "llm_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


@pytest.fixture
def mock_llm_response():
    """Factory fixture for creating RecoveryResult objects with defaults.

    Usage:
        result = mock_llm_response(fix_possible=True, wrong_package="psycopg2")

    Returns:
        Factory function that creates RecoveryResult instances.
    """
    from llm_py.actions.recovery import RecoveryResult

    def _create(**overrides: Any) -> RecoveryResult:
        defaults = {
            "fix_possible": False,
            "wrong_package": "",
            "correct_package": "",
            "version": "",
            "add_package": "",
            "remove_package": "",
            "reasoning": "",
        }
        defaults.update(overrides)
        return RecoveryResult(**defaults)

    return _create


@pytest.fixture
def mock_pypi_checker(monkeypatch):
    """Mock package_exists_on_pypi() to avoid network calls.

    Known valid packages: scrapy, requests, flask, beautifulsoup4,
                          psycopg2-binary, pymysql, protobuf
    Known invalid packages: fake-package, scrapy-python27, nonexistent-pkg,
                           nonexistent-transitive-dep

    Usage:
        def test_example(mock_pypi_checker):
            # PyPI checks are now mocked
            assert not package_exists_on_pypi("fake-package")
    """
    valid_packages = {
        "scrapy",
        "requests",
        "flask",
        "beautifulsoup4",
        "psycopg2-binary",
        "pymysql",
        "protobuf",
        "cryptography",
        "peewee",
        "tensorflow",
    }

    def _mock_exists(package_name: str) -> bool:
        normalized = package_name.strip().lower()
        return normalized in valid_packages

    monkeypatch.setattr("llm_py.actions.recovery.package_exists_on_pypi", _mock_exists)


@pytest.fixture
def sample_error_logs() -> dict[str, str]:
    """Dictionary of known error patterns for testing RAG pattern matching.

    Keys:
        - pg_config: psycopg2 → psycopg2-binary pattern
        - mysql_config: mysqlclient → pymysql pattern
        - python_h: Missing Python.h system dependency
        - cuda: CUDA system dependency pattern
        - protobuf: TensorFlow 1.x + protobuf 4+ incompatibility

    Returns:
        Dict mapping pattern name to example error log text.
    """
    return {
        "pg_config": """
            Building wheel for psycopg2 (setup.py) ... error
            error: subprocess-exited-with-error

            × python setup.py bdist_wheel did not run successfully.
            │ exit code: 1
            ╰─> [23 lines of output]
                running bdist_wheel
                running build
                running build_ext
                building 'psycopg2._psycopg' extension
                Error: pg_config executable not found.

                Please add the directory containing pg_config to the PATH
                or specify the full executable path with the option:

                    python setup.py build_ext --pg-config /path/to/pg_config build ...
        """,
        "mysql_config": """
            Building wheel for mysqlclient (setup.py) ... error
            error: subprocess-exited-with-error

            × python setup.py bdist_wheel did not run successfully.
            │ exit code: 1
            ╰─> [29 lines of output]
                /bin/sh: mysql_config not found
                Traceback (most recent call last):
                  File "<string>", line 2, in <module>
                  File "<pip-setuptools-caller>", line 34, in <module>
                  File "/tmp/pip-install-xyz/mysqlclient_abc/setup.py", line 15, in <module>
                    metadata, options = get_config()
                  File "/tmp/pip-install-xyz/mysqlclient_abc/setup_posix.py", line 65, in get_config
                    libs = mysql_config("libs")
                  File "/tmp/pip-install-xyz/mysqlclient_abc/setup_posix.py", line 31, in mysql_config
                    raise OSError("{} not found".format(_mysql_config_path))
                OSError: mysql_config not found
        """,
        "python_h": """
            Building wheel for greenlet (setup.py) ... error
            error: subprocess-exited-with-error

            × python setup.py bdist_wheel did not run successfully.
            │ exit code: 1
            ╰─> [18 lines of output]
                running bdist_wheel
                running build
                running build_ext
                building 'greenlet._greenlet' extension
                creating build/temp.linux-x86_64-3.7
                gcc -pthread -B /usr/local/lib -Wno-unused-result -Wsign-compare -DNDEBUG -g -fwrapv -O3 -Wall -fPIC -I/usr/local/include/python3.7m -c src/greenlet/greenlet.c -o build/temp.linux-x86_64-3.7/src/greenlet/greenlet.o
                src/greenlet/greenlet.c:5:10: fatal error: Python.h: No such file or directory
                    5 | #include <Python.h>
                      |          ^~~~~~~~~~
                compilation terminated.
                error: command 'gcc' failed with exit status 1
        """,
        "cuda": """
            Building wheel for torch (setup.py) ... error
            error: subprocess-exited-with-error

            × python setup.py bdist_wheel did not run successfully.
            │ exit code: 1
            ╰─> [47 lines of output]
                running bdist_wheel
                running build
                running build_deps
                Building NVCC (CUDA) targets
                nvcc fatal   : Unsupported gpu architecture 'compute_35'
                CMake Error at cuda_compile_generated_THCReduceApplyUtils.cu.o.cmake:219 (message):
                  Error generating file
                  /pytorch/build/caffe2/caffe2/CMakeFiles/torch_cuda.dir/__/aten/src/THC/./torch_cuda_generated_THCReduceApplyUtils.cu.o

                CUDA_HOME not found in the system.
        """,
        "protobuf": """
            Traceback (most recent call last):
              File "test_model.py", line 3, in <module>
                import tensorflow as tf
              File "/usr/local/lib/python3.7/site-packages/tensorflow/__init__.py", line 24, in <module>
                from tensorflow.python import pywrap_tensorflow  # pylint: disable=unused-import
              File "/usr/local/lib/python3.7/site-packages/tensorflow/python/__init__.py", line 49, in <module>
                from tensorflow.python import pywrap_tensorflow
              File "/usr/local/lib/python3.7/site-packages/tensorflow/python/pywrap_tensorflow.py", line 74, in <module>
                raise ImportError(msg)
            ImportError: Traceback (most recent call last):
              File "/usr/local/lib/python3.7/site-packages/tensorflow/python/pywrap_tensorflow.py", line 58, in <module>
                from tensorflow.python.pywrap_tensorflow_internal import *
              File "/usr/local/lib/python3.7/site-packages/tensorflow/python/pywrap_tensorflow_internal.py", line 28, in <module>
                _pywrap_tensorflow_internal = swig_import_helper()
              File "/usr/local/lib/python3.7/site-packages/tensorflow/python/pywrap_tensorflow_internal.py", line 18, in swig_import_helper
                return importlib.import_module('_pywrap_tensorflow_internal')
            TypeError: Descriptors cannot not be created directly.
            If this call came from a _pb2.py file, your generated code is out of date and must be regenerated with protoc >= 3.19.0.
            If you cannot immediately regenerate your protos, some other possible workarounds are:
             1. Downgrade the protobuf package to 3.20.x or lower.
        """,
    }
