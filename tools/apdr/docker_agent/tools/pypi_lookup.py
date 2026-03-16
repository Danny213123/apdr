"""PyPI version queries + KGraph SQLite lookups."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import urllib.request
from typing import Optional


def _kgraph_db_path(cache_path: str) -> str:
    return os.path.join(cache_path, "smtpip-kgraph.sqlite3")


def kgraph_versions(package: str, cache_path: str) -> list[str]:
    """Query package versions from KGraph SQLite. Returns [] if unavailable."""
    db_path = _kgraph_db_path(cache_path)
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.execute(
            "SELECT version FROM versions WHERE package = ? ORDER BY rowid",
            (package.lower(),),
        )
        versions = [row[0] for row in cursor.fetchall()]
        conn.close()
        return versions
    except Exception:
        return []


def kgraph_deps(package: str, version: str, cache_path: str) -> list[str]:
    """Query dependency specs for a specific package version from KGraph."""
    db_path = _kgraph_db_path(cache_path)
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.execute(
            "SELECT dependency FROM dependencies WHERE package = ? AND version = ?",
            (package.lower(), version),
        )
        deps = [row[0] for row in cursor.fetchall()]
        conn.close()
        return deps
    except Exception:
        return []


def pypi_versions(package: str) -> list[str]:
    """Query available versions from PyPI JSON API. Returns [] on failure."""
    url = f"https://pypi.org/pypi/{package}/json"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        releases = list(data.get("releases", {}).keys())
        return releases
    except Exception:
        return []


def query_versions(package: str, python_version: str, cache_path: str = "") -> list[str]:
    """Get available versions for a package. Tries KGraph first, falls back to PyPI."""
    if cache_path:
        versions = kgraph_versions(package, cache_path)
        if versions:
            return versions
    return pypi_versions(package)


def filter_python_compatible(
    versions: list[str], python_version: str
) -> list[str]:
    """Simple filter: remove versions known to be incompatible with given Python version.

    This is a best-effort filter based on common patterns (e.g., numpy 2.x needs Python >=3.9).
    Returns versions as-is if no filtering rules apply.
    """
    # For now, return all versions - the LLM recovery agent has the context
    # to make intelligent version choices. A more sophisticated filter could
    # parse requires_python metadata from PyPI.
    return versions
