"""#4: Local module detection heuristics.

Detects imports that are almost certainly local/project modules (not on PyPI)
by checking naming patterns and code context, BEFORE wasting an LLM call.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("apdr_llm")

# Import names that are almost always project-local
LOCAL_MODULE_NAMES: frozenset[str] = frozenset({
    # Django patterns
    "settings", "config", "conf", "constants", "urls", "api", "app", "apps",
    "views", "models", "forms", "admin", "tests", "manage", "wsgi", "asgi",
    "conftest", "tasks", "celery_tasks",
    # Generic project patterns
    "util", "utils", "helper", "helpers", "common", "shared", "base", "core",
    "main", "run", "setup", "__version__", "version",
    # Data layer
    "db", "database", "middleware", "serializers", "permissions", "signals",
    "routers", "schemas", "exceptions", "mixins", "decorators",
    # Django-specific
    "context_processors", "templatetags", "management", "fixtures", "migrations",
    # Test patterns
    "factory", "factories", "mocks", "stubs", "testutils", "test_helpers",
    # Config/entry point
    "local_settings", "production_settings", "development_settings",
    "celeryconfig", "gunicorn_config", "uwsgi",
    # Common project modules in benchmarks
    "input_data", "input", "output", "data", "result", "results",
    "solution", "answer", "submission", "benchmark", "train", "test",
    "evaluate", "predict", "preprocess", "postprocess",
})

# Framework submodules that resolve to their parent package
FRAMEWORK_SUBMODULE_PARENTS: dict[str, str] = {
    "django.conf": "django",
    "django.db": "django",
    "django.core": "django",
    "django.http": "django",
    "django.urls": "django",
    "django.views": "django",
    "django.forms": "django",
    "django.template": "django",
    "django.contrib": "django",
    "django.utils": "django",
    "django.test": "django",
    "django.middleware": "django",
    "django.dispatch": "django",
    "flask.views": "flask",
    "flask.cli": "flask",
    "flask.json": "flask",
    "flask.testing": "flask",
    "celery.task": "celery",
    "celery.result": "celery",
    "celery.schedules": "celery",
    "sqlalchemy.orm": "sqlalchemy",
    "sqlalchemy.ext": "sqlalchemy",
    "sqlalchemy.engine": "sqlalchemy",
}



# Well-known import->package mappings that are always correct (Pattern A/B/E/F).
# These bypass the LLM entirely with confidence=1.0.
KNOWN_IMPORT_MAPPINGS: dict[str, str] = {
    # Pattern A: C-extension wrappers
    "cv2": "opencv-python-headless",
    "PIL": "Pillow",
    "yaml": "PyYAML",
    "gi": "PyGObject",
    # Pattern B: python- prefix
    "ldap": "python-ldap",
    "daemon": "python-daemon",
    "dotenv": "python-dotenv",
    "dateutil": "python-dateutil",
    "magic": "python-magic",
    "memcached": "python-memcached",
    "xlib": "python-xlib",
    "Levenshtein": "python-Levenshtein",
    # Pattern E: Py prefix
    "serial": "pyserial",
    "usb": "pyusb",
    "enchant": "pyenchant",
    "cups": "pycups",
    "audio": "pyaudio",
    "jwt": "PyJWT",
    "modbus": "pymodbus",
    # Pattern F: completely different names
    "bs4": "beautifulsoup4",
    "sklearn": "scikit-learn",
    "git": "GitPython",
    "dns": "dnspython",
    "Crypto": "pycryptodome",
    "wx": "wxPython",
    "nmap": "python-nmap",
    "cassandra": "cassandra-driver",
    "impala": "impyla",
    # Common aliases
    "pkg_resources": "setuptools",
    "MySQLdb": "mysqlclient",
    "Image": "Pillow",
    "ImageDraw": "Pillow",
    "ImageFont": "Pillow",
    "ImageEnhance": "Pillow",
}

# Normalized lookup index built once
_KNOWN_IMPORT_MAPPINGS_NORMALIZED: dict[str, str] = {
    k.strip().lower().replace("-", "_").replace(".", "_"): v
    for k, v in KNOWN_IMPORT_MAPPINGS.items()
}


def _normalize(name: str) -> str:
    return name.strip().lower().replace("-", "_").replace(".", "_")


def get_known_mapping(import_name: str) -> str | None:
    """Return a deterministic PyPI package for a well-known import, or None."""
    return _KNOWN_IMPORT_MAPPINGS_NORMALIZED.get(_normalize(import_name))


def is_likely_local(import_name: str) -> bool:
    """Check if an import name is likely a local/project module."""
    norm = _normalize(import_name)
    return norm in LOCAL_MODULE_NAMES


def get_framework_parent(import_name: str) -> str | None:
    """If the import is a framework submodule, return the parent package name.

    E.g. "django.conf" -> "django", "flask.views" -> "flask"
    """
    # Check exact match first
    if import_name in FRAMEWORK_SUBMODULE_PARENTS:
        return FRAMEWORK_SUBMODULE_PARENTS[import_name]

    # Check dotted prefix match (e.g. "django.contrib.auth" -> "django")
    parts = import_name.split(".")
    if len(parts) >= 2:
        prefix = f"{parts[0]}.{parts[1]}"
        if prefix in FRAMEWORK_SUBMODULE_PARENTS:
            return FRAMEWORK_SUBMODULE_PARENTS[prefix]

    return None


def filter_imports(
    import_names: list[str],
) -> tuple[list[str], list[str], dict[str, str], dict[str, str]]:
    """Partition imports into (needs_llm, local_skips, framework_mappings, known_mappings).

    Returns:
        needs_llm: imports that should be sent to the LLM
        local_skips: imports detected as local modules (skipped)
        framework_mappings: import -> parent package for framework submodules
        known_mappings: import -> package for well-known deterministic mappings
    """
    needs_llm: list[str] = []
    local_skips: list[str] = []
    framework_mappings: dict[str, str] = {}
    known_mappings: dict[str, str] = {}

    for imp in import_names:
        if is_likely_local(imp):
            local_skips.append(imp)
        elif (parent := get_framework_parent(imp)) is not None:
            framework_mappings[imp] = parent
        elif (known_pkg := get_known_mapping(imp)) is not None:
            known_mappings[imp] = known_pkg
        else:
            needs_llm.append(imp)

    return needs_llm, local_skips, framework_mappings, known_mappings
