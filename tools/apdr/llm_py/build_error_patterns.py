"""#12: Build error pattern library for recovery.

Structured library of known build error patterns with fixes,
injected as RAG context during recovery to help the LLM.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ErrorPattern:
    pattern: str          # Regex-like pattern to match in error log
    diagnosis: str        # Human-readable diagnosis
    fix_type: str         # "system_dep" | "wrong_package" | "version" | "skip"
    suggested_fix: str    # Suggested fix description


# Ordered from most specific to most general
ERROR_PATTERNS: list[ErrorPattern] = [
    # System dependency errors
    ErrorPattern(
        pattern="fatal error: Python.h: No such file",
        diagnosis="Missing Python development headers",
        fix_type="system_dep",
        suggested_fix="Needs python3-dev or python-dev system package. Cannot fix via pip.",
    ),
    ErrorPattern(
        pattern="fatal error: ffi.h: No such file",
        diagnosis="Missing libffi development headers",
        fix_type="system_dep",
        suggested_fix="Needs libffi-dev system package. Cannot fix via pip.",
    ),
    ErrorPattern(
        pattern="pg_config executable not found",
        diagnosis="Missing PostgreSQL development headers for psycopg2",
        fix_type="wrong_package",
        suggested_fix="Replace psycopg2 with psycopg2-binary (pre-compiled, no pg_config needed).",
    ),
    ErrorPattern(
        pattern="mysql_config not found",
        diagnosis="Missing MySQL development headers for mysqlclient",
        fix_type="wrong_package",
        suggested_fix="Replace mysqlclient with PyMySQL (pure-Python MySQL driver).",
    ),
    ErrorPattern(
        pattern="error: Microsoft Visual C\\+\\+ .* is required",
        diagnosis="Windows-only build dependency",
        fix_type="skip",
        suggested_fix="Package requires Windows build tools. Skip on Linux/Docker.",
    ),
    ErrorPattern(
        pattern="No module named '_ctypes'",
        diagnosis="Missing libffi for ctypes",
        fix_type="system_dep",
        suggested_fix="Needs libffi-dev system package.",
    ),
    ErrorPattern(
        pattern="fatal error: openssl/ssl.h",
        diagnosis="Missing OpenSSL development headers",
        fix_type="system_dep",
        suggested_fix="Needs libssl-dev system package.",
    ),
    ErrorPattern(
        pattern="fatal error: libxml/",
        diagnosis="Missing libxml2 development headers",
        fix_type="system_dep",
        suggested_fix="Needs libxml2-dev and libxslt1-dev system packages.",
    ),
    ErrorPattern(
        pattern="error: command 'gcc' failed",
        diagnosis="C compilation failed — missing headers or libraries",
        fix_type="system_dep",
        suggested_fix="Package requires C compilation. Check for missing system dev packages.",
    ),
    ErrorPattern(
        pattern="Could not find a version that satisfies the requirement",
        diagnosis="Package version not found",
        fix_type="version",
        suggested_fix="No compatible version exists for target Python. Try a different version.",
    ),
    ErrorPattern(
        pattern="No matching distribution found for",
        diagnosis="Package does not exist or has no compatible wheel",
        fix_type="wrong_package",
        suggested_fix="Package name may be wrong. Check PyPI for the correct name.",
    ),
    # CUDA/GPU errors
    ErrorPattern(
        pattern="CUDA|cuda|nvcc|libcudart",
        diagnosis="Requires NVIDIA CUDA toolkit",
        fix_type="system_dep",
        suggested_fix="Package requires CUDA. Use CPU-only variant if available.",
    ),
    # Java errors
    ErrorPattern(
        pattern="JAVA_HOME|javac|jni\\.h",
        diagnosis="Requires Java JDK",
        fix_type="system_dep",
        suggested_fix="Package requires Java. Cannot fix via pip alone.",
    ),
    # Protobuf descriptor incompatibility (TF 1.x + protobuf 4+)
    ErrorPattern(
        pattern="Descriptors cannot not be created directly",
        diagnosis="protobuf version incompatibility with TensorFlow 1.x",
        fix_type="add_dep",
        suggested_fix="Add protobuf==3.20.3 as an explicit dependency. TensorFlow 1.x requires protobuf<4.",
    ),
    ErrorPattern(
        pattern="_CheckCalledFromGeneratedFile",
        diagnosis="protobuf version incompatibility with generated _pb2.py files",
        fix_type="add_dep",
        suggested_fix="Add protobuf==3.20.3. Generated protobuf code is incompatible with protobuf>=4.",
    ),
    # Rust-based packages
    ErrorPattern(
        pattern="error: can't find Rust compiler",
        diagnosis="Package requires Rust compiler (cargo/rustc)",
        fix_type="system_dep",
        suggested_fix="Install Rust toolchain or use a pre-built wheel.",
    ),
]


def match_error_patterns(error_log: str) -> list[ErrorPattern]:
    """Find all matching error patterns in a build error log.

    Returns patterns in priority order (most specific first).
    """
    import re
    matches = []
    for pattern in ERROR_PATTERNS:
        try:
            if re.search(pattern.pattern, error_log, re.IGNORECASE):
                matches.append(pattern)
        except re.error:
            # Fallback to simple substring match
            if pattern.pattern.lower() in error_log.lower():
                matches.append(pattern)
    return matches


def format_error_context(error_log: str) -> str:
    """Build RAG context from error pattern matching for recovery prompts."""
    matches = match_error_patterns(error_log)
    if not matches:
        return ""

    lines = ["Known error patterns detected in the build log:"]
    for m in matches[:3]:  # Top 3 matches
        lines.append(f"  - {m.diagnosis}: {m.suggested_fix}")
    return "\n".join(lines)
