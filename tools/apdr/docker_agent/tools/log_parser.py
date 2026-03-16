"""Build log pattern matching — port of tools/apdr/src/recovery/patterns.rs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class PatternMatch:
    error_type: str
    conflict_class: str
    fix_hint: str
    matched_pattern: str


# 16 built-in patterns from patterns.rs, ordered by specificity
_PATTERNS: list[tuple[str, str, str, str]] = [
    ("No matching distribution found", "VersionNotFound", "TPL-TPL",
     "Try an adjacent compatible version."),
    ("Cannot install", "DependencyConflict", "TPL-TPL",
     "Relax or realign version constraints."),
    ("ResolutionImpossible", "DependencyConflict", "TPL-TPL",
     "Rebuild the requirement set around a coherent version bundle."),
    ("requires a different python version", "PythonVersionMismatch", "TPL-Python",
     "Choose a package version that matches the Python version."),
    ("ModuleNotFoundError", "ModuleNotFound", "TPL-TPL",
     "Add the missing package."),
    ("ImportError", "ImportError", "TPL-TPL",
     "Pin the dependency to an API-compatible version."),
    ("SyntaxError", "SyntaxError", "TPL-Python",
     "Retry on an adjacent Python version."),
    ("permission denied while trying to connect to the docker api", "DockerPermissionDenied", "TPL-OS",
     "Start Docker Desktop or grant Docker socket access."),
    ("cannot connect to the docker daemon", "DockerDaemonUnavailable", "TPL-OS",
     "Start Docker Desktop."),
    ("could not build wheels", "BuildFailure", "TPL-OS",
     "Add system headers or choose a version with prebuilt wheels."),
    ("libxml2 and libxslt development packages are installed", "BuildFailure", "TPL-OS",
     "Install libxml2-dev and libxslt1-dev."),
    ("python.h: no such file or directory", "BuildFailure", "TPL-OS",
     "Install Python development headers."),
    ("Cannot import 'setuptools.build_meta'", "BuildBackendUnavailable", "TPL-OS",
     "Preinstall setuptools and wheel."),
    ("No local interpreter found for Python", "PythonInterpreterUnavailable", "TPL-OS",
     "Install a matching Python interpreter."),
    ("Failed to establish a new connection", "NetworkUnavailable", "TPL-OS",
     "Check network access to PyPI."),
    ("No space left on device", "DiskFull", "TPL-OS",
     "Free disk space."),
]


def match_known_patterns(log: str) -> Optional[PatternMatch]:
    lower = log.lower()
    for pattern, error_type, conflict_class, fix_hint in _PATTERNS:
        if pattern.lower() in lower:
            return PatternMatch(
                error_type=error_type,
                conflict_class=conflict_class,
                fix_hint=fix_hint,
                matched_pattern=pattern,
            )
    return None


def extract_package_from_log(log: str, error_type: str) -> str:
    """Extract the offending package name from a build/run log."""
    if error_type in ("VersionNotFound", "DependencyConflict"):
        # "No matching distribution found for numpy==1.24.0"
        m = re.search(r"(?:found for|Cannot install)\s+(\S+?)(?:==\S+)?(?:\s|$)", log)
        if m:
            return m.group(1).lower()
    if error_type in ("ModuleNotFound", "ImportError"):
        # "ModuleNotFoundError: No module named 'foo'"
        m = re.search(r"No module named ['\"]?(\w[\w.]*)['\"]?", log)
        if m:
            return m.group(1).split(".")[0].lower()
    if error_type == "PythonVersionMismatch":
        m = re.search(r"requires a different python version.*?(\S+)==", log, re.IGNORECASE)
        if m:
            return m.group(1).lower()
    return "unknown"


def extract_module_from_log(log: str) -> Optional[str]:
    """Extract missing module name from ModuleNotFoundError/ImportError."""
    m = re.search(r"No module named ['\"]?(\w[\w.]*)['\"]?", log)
    if m:
        return m.group(1)
    m = re.search(r"ImportError:.*?from\s+(\w[\w.]*)\s+import", log)
    if m:
        return m.group(1)
    return None
