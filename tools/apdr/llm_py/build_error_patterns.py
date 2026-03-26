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
    # Python 2/3 syntax errors during build or runtime
    ErrorPattern(
        pattern="SyntaxError.*Missing parentheses in call to 'print'",
        diagnosis="Code contains Python 2 print statements",
        fix_type="python_version",
        suggested_fix="Package or its dependencies require Python 2.7. Try Python 2.7 if code uses print statements.",
    ),
    ErrorPattern(
        pattern="SyntaxError.*multiple exception types must be parenthesized",
        diagnosis="Old-style exception handling syntax",
        fix_type="python_version",
        suggested_fix="Code uses Python 2 'except Type1, Type2:' syntax. Requires Python 2.7 or code modernization.",
    ),
    ErrorPattern(
        pattern="SyntaxError.*Lambda expression parameters cannot be parenthesized",
        diagnosis="Lambda syntax incompatible with Python 3.11+",
        fix_type="python_version",
        suggested_fix="Code has invalid lambda syntax for Python 3.11+. Try Python 3.9 or 3.10.",
    ),
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
    ErrorPattern(
        pattern="Unable to find JAVA_HOME|java_home",
        diagnosis="Runtime missing JAVA_HOME for pyjnius/jnius",
        fix_type="system_dep",
        suggested_fix="Install a JDK and set JAVA_HOME, or escalate to Docker with Java installed.",
    ),
    ErrorPattern(
        pattern="geos_c\\.dll|lib geos_c|gdal-config|GDAL API version",
        diagnosis="Missing GEOS/GDAL native libraries",
        fix_type="system_dep",
        suggested_fix="Install GEOS/GDAL system libraries or escalate to Docker with geospatial deps.",
    ),
    ErrorPattern(
        pattern="soft_unicode|url_quote|markupsafe|werkzeug",
        diagnosis="Legacy Flask/Jinja2/MarkupSafe compatibility break",
        fix_type="version",
        suggested_fix="Pin a coherent legacy Flask family bundle (Flask/Jinja2/MarkupSafe/Werkzeug/itsdangerous).",
    ),
    ErrorPattern(
        pattern="urllib3|cfscrape",
        diagnosis="cfscrape compatibility break with modern urllib3",
        fix_type="version",
        suggested_fix="Pin urllib3<2 alongside cfscrape.",
    ),
    ErrorPattern(
        pattern="ggplot|pandas",
        diagnosis="Legacy ggplot compatibility break with modern pandas",
        fix_type="version",
        suggested_fix="Pin an older pandas release alongside ggplot.",
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
    # NumPy compatibility errors
    ErrorPattern(
        pattern="AttributeError.*numpy.*has no attribute.*bool",
        diagnosis="NumPy 1.24+ removed numpy.bool alias",
        fix_type="version",
        suggested_fix="Pin numpy<1.24 or update code to use bool instead of numpy.bool.",
    ),
    ErrorPattern(
        pattern="module 'numpy' has no attribute 'bool'",
        diagnosis="NumPy 1.24+ removed numpy.bool",
        fix_type="add_dep",
        suggested_fix="Add 'numpy<1.24' as explicit dependency for compatibility.",
    ),
    # Pandas version compatibility
    ErrorPattern(
        pattern="No matching distribution found for pandas==0\\.25",
        diagnosis="Old pandas version unavailable for current Python",
        fix_type="version",
        suggested_fix="pandas 0.25.x requires Python 3.5-3.8. Use newer pandas or older Python.",
    ),
    # D-Bus system dependency
    ErrorPattern(
        pattern="No matching distribution found for dbus-python",
        diagnosis="dbus-python requires system D-Bus libraries",
        fix_type="system_dep",
        suggested_fix="dbus-python needs libdbus-1-dev system package. Cannot install via pip alone.",
    ),
    # Pkg-config false positives
    ErrorPattern(
        pattern="Found pkg-config.*YES",
        diagnosis="False positive - pkg-config found but other dep missing",
        fix_type="system_dep",
        suggested_fix="Build failed despite pkg-config being present. Check for other missing system deps.",
    ),
    # Double requirement errors
    ErrorPattern(
        pattern="ERROR: Double requirement given",
        diagnosis="Same package listed twice in requirements",
        fix_type="dedup",
        suggested_fix="Remove duplicate package from requirements. Check transitive dependencies.",
    ),
    # Setup.py import errors (package imports dependencies during its own setup)
    ErrorPattern(
        pattern="Getting requirements to build wheel.+No module named 'numpy'",
        diagnosis="FuncDesigner imports numpy during setup.py",
        fix_type="add_dep",
        suggested_fix="Add 'numpy' as explicit dependency BEFORE FuncDesigner. FuncDesigner's setup.py imports numpy.",
    ),
    ErrorPattern(
        pattern="Getting requirements to build wheel.+No module named 'Cython'",
        diagnosis="Package needs Cython at build time",
        fix_type="add_dep",
        suggested_fix="Add 'Cython' as explicit build dependency before this package. Setup.py imports Cython.",
    ),
    ErrorPattern(
        pattern="pycrayon.+No module named 'crayon'",
        diagnosis="pycrayon has circular self-import in setup.py",
        fix_type="skip",
        suggested_fix="pycrayon is broken - it imports itself during setup. Skip this package.",
    ),
    ErrorPattern(
        pattern="Getting requirements to build wheel.+No module named 'pkg_resources'",
        diagnosis="Package requires older setuptools with pkg_resources",
        fix_type="version",
        suggested_fix="Pin 'setuptools<58' for pkg_resources compatibility. Modern setuptools removed pkg_resources from setup.py context.",
    ),
    ErrorPattern(
        pattern="Django.+No module named 'turbogears'",
        diagnosis="Old Django package incorrectly listed as dependency",
        fix_type="wrong_package",
        suggested_fix="This may be a misidentified package. Check if 'django' should be 'Django-TurboGears' or similar.",
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
            if re.search(pattern.pattern, error_log, re.IGNORECASE | re.DOTALL):
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
