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


def _normalize(name: str) -> str:
    return name.strip().lower().replace("-", "_").replace(".", "_")


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
) -> tuple[list[str], list[str], dict[str, str]]:
    """Partition imports into (needs_llm, local_skips, framework_mappings).

    Returns:
        needs_llm: imports that should be sent to the LLM
        local_skips: imports detected as local modules (skipped)
        framework_mappings: import -> parent package for framework submodules
    """
    needs_llm: list[str] = []
    local_skips: list[str] = []
    framework_mappings: dict[str, str] = {}

    for imp in import_names:
        if is_likely_local(imp):
            local_skips.append(imp)
        elif (parent := get_framework_parent(imp)) is not None:
            framework_mappings[imp] = parent
        else:
            needs_llm.append(imp)

    return needs_llm, local_skips, framework_mappings
