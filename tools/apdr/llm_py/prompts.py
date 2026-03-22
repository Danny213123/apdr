"""Prompt templates for APDR LLM service.

Improvements:
- #5:  Negative example bank for common local modules
- #6:  Stdlib + framework submodule allowlist
- #10: Failure pair few-shot examples (preferred + rejected)
- #11: Import-pattern taxonomy in system prompt
- System/user message separation
- Tier2 candidate injection
- JSON schema embedded in prompt body
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# #11: Import-pattern taxonomy — teaches underlying PATTERNS, not just examples
# ---------------------------------------------------------------------------

IMPORT_PATTERN_TAXONOMY = """\
Import-to-package naming patterns (learn the RULES, not just individual cases):

Pattern A — C-extension wrappers (import name is the C module name):
  cv2 -> opencv-python | PIL -> Pillow | yaml -> PyYAML | lxml -> lxml
  _bsddb -> bsddb3 | gi -> PyGObject | objc -> pyobjc

Pattern B — "python-" prefix (import name is short, package adds python-):
  ldap -> python-ldap | daemon -> python-daemon | dotenv -> python-dotenv
  dateutil -> python-dateutil | magic -> python-magic | memcached -> python-memcached
  xlib -> python-xlib | Levenshtein -> python-Levenshtein

Pattern C — "django-" prefix (Django contrib apps):
  taggit -> django-taggit | storages -> django-storages | compressor -> django-compressor
  crispy_forms -> django-crispy-forms | ckeditor -> django-ckeditor
  rest_framework -> djangorestframework | mptt -> django-mptt
  allauth -> django-allauth | cors_headers -> django-cors-headers

Pattern D — "Flask-" prefix (Flask extensions, hyphenated with capital F):
  flask_cors -> Flask-Cors | flask_login -> Flask-Login | flask_wtf -> Flask-WTF
  flask_sqlalchemy -> Flask-SQLAlchemy | flask_mail -> Flask-Mail
  flask_restful -> flask-restful | flask_classy -> Flask-Classy

Pattern E — "Py" prefix (Python bindings for C/C++ libraries):
  serial -> pyserial | usb -> pyusb | enchant -> pyenchant
  cups -> pycups | audio -> pyaudio | jwt -> PyJWT | modbus -> pymodbus

Pattern F — completely different names:
  bs4 -> beautifulsoup4 | sklearn -> scikit-learn | cv2 -> opencv-python
  git -> GitPython | discord -> discord.py | cassandra -> cassandra-driver
  impala -> impyla | dns -> dnspython | Crypto -> pycryptodome
  wx -> wxPython | nmap -> python-nmap

Pattern G — identity mappings (import name == package name):
  requests -> requests | flask -> flask | django -> django
  celery -> celery | redis -> redis | numpy -> numpy | pandas -> pandas
  pymongo -> pymongo | boto3 -> boto3 | sqlalchemy -> SQLAlchemy
"""

# ---------------------------------------------------------------------------
# #10: Failure pair few-shot examples — shows WRONG then CORRECT
# ---------------------------------------------------------------------------

FAILURE_PAIR_EXAMPLES = """\
Common mistakes and their corrections (learn from these failures):
  WRONG: johnny -> johnny        CORRECT: johnny -> johnny-cache
  WRONG: MySQLdb -> MySQLdb      CORRECT: MySQLdb -> mysqlclient (or PyMySQL)
  WRONG: memcache -> memcache    CORRECT: memcache -> python-memcached (or pylibmc)
  WRONG: wx -> wx                CORRECT: wx -> wxPython
  WRONG: gi -> gi                CORRECT: gi -> PyGObject
  WRONG: objc -> objc            CORRECT: objc -> pyobjc
  WRONG: magic -> magic          CORRECT: magic -> python-magic
  WRONG: cups -> cups            CORRECT: cups -> pycups
  WRONG: Crypto -> Crypto        CORRECT: Crypto -> pycryptodome
  WRONG: xtls -> xtls            CORRECT: xtls is NOT a PyPI package — skip it
  WRONG: flotilla -> flotilla    CORRECT: flotilla is NOT a real PyPI package — skip it
  WRONG: simplegui -> simplegui  CORRECT: simplegui -> SimpleGUICS2Pygame (or skip if CodeSkulptor)
  WRONG: apt -> apt              CORRECT: apt is a system package — skip it
  WRONG: rpm -> rpm              CORRECT: rpm is a system package — skip it

CRITICAL RULE: If you cannot confidently identify a real PyPI package for an
import, skip it entirely. Never echo the import name back as the package name
unless you are CERTAIN a PyPI package with that exact name exists and provides
that import (e.g. requests, flask, numpy, pandas).
"""

# ---------------------------------------------------------------------------
# #5: Negative example bank — common local/project modules to SKIP
# ---------------------------------------------------------------------------

LOCAL_MODULE_BANK = """\
These are ALWAYS local/project modules — never map them to PyPI packages:
  settings, config, conf, constants, urls, api, app, apps, views, models,
  forms, admin, tests, manage, wsgi, asgi, conftest, tasks, celery_tasks,
  util, utils, helper, helpers, common, shared, base, core, main, run,
  setup, __version__, version, db, database, middleware, serializers,
  permissions, signals, routers, schemas, exceptions, mixins, decorators,
  context_processors, templatetags, management, fixtures, migrations,
  factory, factories, mocks, stubs, testutils, test_helpers
"""

# ---------------------------------------------------------------------------
# #6: Framework submodule allowlist — submodules that DON'T need separate packages
# ---------------------------------------------------------------------------

FRAMEWORK_SUBMODULE_ALLOWLIST = """\
These are submodules of their parent package — do NOT map them separately:
  django.conf, django.db, django.core, django.http, django.urls,
  django.views, django.forms, django.template, django.contrib,
  django.utils, django.test, django.middleware, django.dispatch,
  flask.views, flask.cli, flask.json, flask.testing,
  celery.task, celery.result, celery.schedules,
  sqlalchemy.orm, sqlalchemy.ext, sqlalchemy.engine,
  numpy.linalg, numpy.random, numpy.fft,
  pandas.io, pandas.core, pandas.api,
  scipy.stats, scipy.optimize, scipy.signal, scipy.sparse,
  matplotlib.pyplot, matplotlib.figure, matplotlib.patches,
  tensorflow.keras, tensorflow.python, tensorflow.core,
  torch.nn, torch.optim, torch.utils, torch.cuda,
  sklearn.model_selection, sklearn.preprocessing, sklearn.metrics
If you see "from django.conf import settings", the package is "django", not "django-conf".
"""


def compress_benchmark_context(context: str, max_chars: int = 4096) -> str:
    if not context.strip():
        return "- none"
    if len(context) <= max_chars:
        return context
    trimmed = context[len(context) - max_chars:]
    pos = trimmed.find("===== ")
    if pos >= 0:
        return f"[earlier context omitted]\n{trimmed[pos:]}"
    return f"[earlier context omitted]\n{trimmed}"


def _candidates_section(tier2_candidates: dict[str, list[str]]) -> str:
    """Format tier2 candidate packages for injection into prompts."""
    if not tier2_candidates:
        return ""
    lines = []
    for import_name, candidates in tier2_candidates.items():
        if candidates:
            lines.append(
                f"  Known candidate packages for `{import_name}`: "
                + ", ".join(candidates[:5])
            )
    if not lines:
        return ""
    return (
        "\nCandidate packages from fuzzy matching (prefer these over inventing new names):\n"
        + "\n".join(lines)
        + "\n"
    )


def _attribute_section(
    attribute_usage: dict[str, list[str]],
    unresolved_imports: list[str],
) -> str:
    lines = []
    for module, attrs in attribute_usage.items():
        if module in unresolved_imports and attrs:
            lines.append(f"  {module} uses: {', '.join(attrs)}")
    if not lines:
        return ""
    return "\nAttribute usage (helps disambiguate imports):\n" + "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# System prompts (constant, cached by provider)
# ---------------------------------------------------------------------------

RESOLUTION_SYSTEM = f"""\
You are resolving Python imports to PyPI package names.

{IMPORT_PATTERN_TAXONOMY}
{FAILURE_PAIR_EXAMPLES}
{LOCAL_MODULE_BANK}
{FRAMEWORK_SUBMODULE_ALLOWLIST}
Think step by step for each import:
1. Is this a Python standard library module? If yes, skip it entirely.
2. Is this a local/project module from the LOCAL_MODULE_BANK list? If yes, skip it.
3. Is this a framework submodule (e.g. django.conf)? If yes, map to the parent package.
4. Does the import match a known pattern (A-G above)? Apply the pattern.
5. Do the tier2 candidates or reverse-index context suggest a real package? Prefer those.
6. If you are NOT confident a real PyPI package exists, skip the import. Never guess.

Only include imports that map to VERIFIED real PyPI packages.
Return a JSON object with a "mappings" array. Each entry has "import_name" and "package_name".
"""

SOLVABILITY_SYSTEM = """\
You are triaging whether a Python snippet is solvable in a generic Docker + PyPI environment.
Decide whether APDR should try dependency resolution or skip the snippet.
Treat these as NOT solvable in generic Docker:
- Host-application runtimes: Maya, Blender, ArcGIS, Houdini, Rhino, Unreal, Nuke, Sublime Text, GIMP, IDA Pro, Cinema4D, HexChat
- Platform-specific APIs: COM/Win32 Windows APIs, macOS Objective-C frameworks (Foundation, CoreFoundation, AppKit, SystemConfiguration, OpenDirectory), Raspberry Pi GPIO/camera
- Java/Jython interop: javax.*, java.*, com.android.*
- Local project modules not available on PyPI

Think step by step:
1. Identify which imports are standard library modules (skip those).
2. For each remaining import, determine if it is a known PyPI package, a host-app runtime, or a local module.
3. If ALL non-stdlib imports are host-app or platform-specific, decision=skip. Otherwise decision=solve.

Return a JSON object with these fields:
- decision: "solve" or "skip"
- confidence: 0.00 to 1.00
- reason: short explanation
- unsolvable_modules: array of import names that cannot be resolved from PyPI (empty array if decision=solve)
"""

SELF_REFINE_SYSTEM = f"""\
Review Python import-to-PyPI-package mappings for correctness.

{IMPORT_PATTERN_TAXONOMY}
For each mapping, verify:
1. Does this PyPI package actually exist?
2. Does the package provide the import name shown?
3. Is there a more common/correct package for this import?
4. Is the import actually a local module that should be skipped?

Return a JSON object with:
- all_correct: true if all mappings are correct
- corrections: array of objects with import_name, original_package, corrected_package, reason
  (empty array if all correct)
"""

RECOVERY_SYSTEM = f"""\
You are fixing a Python dependency installation failure.

{IMPORT_PATTERN_TAXONOMY}
{FAILURE_PAIR_EXAMPLES}
Think step by step:
1. Read the error log carefully to identify which package caused the failure.
2. Check if the package name is wrong (needs a prefix like `django-` or `python-`).
3. Check if the package needs system C libraries and a pure-Python alternative exists.
4. Check if the wrong package was installed (same name on PyPI but unrelated project).

Return a JSON object with:
- fix_possible: true or false
- wrong_package: the package that caused the error (exact name from resolved list)
- correct_package: the correct PyPI package name
- reasoning: brief explanation
If no fix is possible, set fix_possible=false.
"""

# ---------------------------------------------------------------------------
# #9: Structured scratchpad CoT system prompt
# ---------------------------------------------------------------------------

SCRATCHPAD_SYSTEM = """\
You are resolving a Python import to its PyPI package name.
Fill in this structured scratchpad step by step, then give your final answer.

SCRATCHPAD:
1. Is it stdlib? [yes/no]
2. Is it a known local module (settings, config, urls, models, etc.)? [yes/no]
3. Is it a framework submodule (e.g. django.conf -> django)? [yes/no/NA]
4. Pattern match:
   - C-extension wrapper? (cv2, PIL, yaml)
   - python- prefix? (ldap, daemon, dotenv)
   - django- prefix? (taggit, storages, crispy_forms)
   - Flask- prefix? (flask_cors, flask_login)
   - Py prefix? (serial, usb, enchant)
   - Completely different name? (bs4, sklearn, git)
   - Identity mapping? (requests, flask, numpy)
5. Do the provided candidates/context suggest a package? [yes: which one / no]
6. Confidence that a real PyPI package exists? [high/medium/low]

FINAL ANSWER: {"import_name": "...", "package_name": "..."} or SKIP if no real package.
"""


# ---------------------------------------------------------------------------
# User prompt builders
# ---------------------------------------------------------------------------


def package_resolution_user(
    unresolved_imports: list[str],
    python_version: str,
    context: list[str],
    benchmark_context: str,
    attribute_usage: dict[str, list[str]],
    tier2_candidates: dict[str, list[str]] | None = None,
) -> str:
    ctx_str = "\n".join(context) if context else "- none"
    bm_str = compress_benchmark_context(benchmark_context, 6144)
    attr_sec = _attribute_section(attribute_usage, unresolved_imports)
    cand_sec = _candidates_section(tier2_candidates or {})
    return (
        f"Target Python version: {python_version}\n\n"
        f"Context:\n{ctx_str}\n"
        f"Benchmark context:\n{bm_str}"
        f"{attr_sec}"
        f"{cand_sec}\n"
        f"Imports to resolve:\n" + "\n".join(unresolved_imports)
    )


def solvability_user(
    source: str,
    imports: list[str],
    import_paths: list[str],
    benchmark_context: str,
) -> str:
    imp_str = ", ".join(imports) if imports else "- none"
    path_str = ", ".join(import_paths) if import_paths else "- none"
    bm_str = compress_benchmark_context(benchmark_context, 4096)
    return (
        f"Imports: {imp_str}\n"
        f"Import paths: {path_str}\n"
        f"Benchmark context:\n{bm_str}\n"
        f"Snippet:\n```python\n{source}\n```"
    )


def self_refine_user(
    mappings: list[tuple[str, str]],
    python_version: str,
    snippet_excerpt: str,
) -> str:
    mapping_lines = "\n".join(f"  {imp} -> {pkg}" for imp, pkg in mappings)
    return (
        f"Target Python version: {python_version}\n\n"
        f"Current mappings:\n{mapping_lines}\n\n"
        f"Code context:\n```python\n{snippet_excerpt}\n```"
    )


def recovery_user(
    resolved_packages: list[str],
    error_log: str,
    snippet_source: str,
    python_version: str,
    error_type: str,
    previous_attempts: list[list[str]],
) -> str:
    pkg_list = "\n".join(resolved_packages)
    log_excerpt = "\n".join(error_log.splitlines()[:50])
    snippet_excerpt = "\n".join(snippet_source.splitlines()[:50])
    history = ""
    if previous_attempts:
        lines = []
        for attempt in previous_attempts:
            if len(attempt) >= 3:
                lines.append(
                    f"- Tried replacing `{attempt[0]}` with `{attempt[1]}`: {attempt[2]}"
                )
        if lines:
            history = (
                "\nPrevious recovery attempts (DO NOT repeat these):\n"
                + "\n".join(lines) + "\n"
            )
    return (
        f"Target Python version: {python_version}\n"
        f"Error type: {error_type}\n\n"
        f"Currently resolved packages:\n{pkg_list}\n\n"
        f"Installation/import error:\n```\n{log_excerpt}\n```\n\n"
        f"Python snippet being resolved:\n```python\n{snippet_excerpt}\n```\n"
        f"{history}"
    )


def version_user(
    package_name: str,
    versions: list[str],
    python_version: str,
    benchmark_context: str,
) -> str:
    bm_str = compress_benchmark_context(benchmark_context, 2048)
    return (
        f"Choose one installable version for the Python package '{package_name}'.\n"
        f"Target Python version: {python_version}\n"
        f"Allowed versions (oldest to newest): {', '.join(versions)}\n"
        f"Benchmark context:\n{bm_str}\n"
        f'Return a JSON object with a single field "version" set to the exact version string from the list above.\n'
        f'If none look viable, set version to "NONE".'
    )
