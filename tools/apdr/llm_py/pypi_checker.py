"""PyPI existence checker with caching.

Used by Pydantic field_validators inside the Instructor retry loop
to reject hallucinated package names before the model finalizes output.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger("apdr_llm")

# Thread-safe cache: package_name -> exists (bool)
_cache: dict[str, bool] = {}
_cache_lock = threading.Lock()


def package_exists_on_pypi(package_name: str) -> bool:
    """Check if a package exists on PyPI. Results are cached."""
    normalized = package_name.strip().lower()
    if not normalized:
        return False

    with _cache_lock:
        if normalized in _cache:
            return _cache[normalized]

    try:
        import requests
        resp = requests.head(
            f"https://pypi.org/pypi/{package_name}/json",
            timeout=5,
            allow_redirects=True,
        )
        exists = resp.status_code != 404
    except Exception:
        # Network error — assume it exists to avoid false negatives
        exists = True

    with _cache_lock:
        _cache[normalized] = exists

    return exists


def preload_known_packages(package_names: list[str]) -> None:
    """Pre-populate the cache with known-valid packages from Rust store."""
    with _cache_lock:
        for name in package_names:
            _cache[name.strip().lower()] = True


def check_multiple(packages: list[str]) -> list[str]:
    """Check multiple packages, return list of non-existent ones."""
    nonexistent = []
    for pkg in packages:
        if not package_exists_on_pypi(pkg):
            nonexistent.append(pkg)
    return nonexistent
