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

import re
from typing import Any

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

Pattern H — version-sensitive frameworks (API usage reveals version):
  TensorFlow 1.x indicators: tf.placeholder, tf.Session, tf.unpack, tf.concat(axis, []),
    tf.initialize_all_variables(), tf.train.*, tf.nn.rnn_cell, contrib.*
    -> tensorflow==1.15.0 (+ protobuf==3.20.3 transitive dep)
  TensorFlow 2.x indicators: tf.keras.*, tf.function, tf.data.Dataset, tf.GradientTape
    -> tensorflow (latest)
  OpenCV with GUI: cv2.imshow, cv2.VideoCapture, cv2.waitKey -> opencv-python
  OpenCV headless (no GUI): cv2.imread, cv2.resize, cv2.cvtColor -> opencv-python-headless
    (prefer headless — has pre-built wheels, no system deps needed)

Pattern I — unsolvable platform-specific imports (SKIP these entirely):
  macOS frameworks: Foundation, CoreFoundation, AppKit, SystemConfiguration,
    OpenDirectory, ScriptingBridge, Cocoa, objc (when used as pyobjc bridge to macOS)
  Windows APIs: win32com, win32api, win32gui, pythoncom, wmi, winreg (stdlib on Windows)
  Hardware/embedded: ev3dev, gameduino, RPi.GPIO, picamera, sense_hat, smbus
  Host-app runtimes: maya.cmds, pymel, bpy (Blender), arcpy, nuke, hou (Houdini)
  Private/defunct packages: xtls, flotilla (Pimoroni), plist (use biplist instead)
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
  WRONG: plist -> biplist        CORRECT: plist is NOT a real PyPI package — skip it (biplist is a different API)
  WRONG: ev3dev -> evdev         CORRECT: ev3dev is a hardware-specific package for LEGO EV3 — skip it
  WRONG: gameduino -> gameduino  CORRECT: gameduino is NOT on PyPI — skip it (Arduino hardware lib)
  WRONG: OpenDirectory -> pyobjc CORRECT: OpenDirectory is a macOS framework — skip it
  WRONG: CoreFoundation -> pyobjc CORRECT: CoreFoundation is a macOS framework — skip it
  WRONG: Foundation -> pyobjc    CORRECT: Foundation is a macOS framework — skip it
  WRONG: SystemConfiguration -> pyobjc CORRECT: SystemConfiguration is macOS framework — skip it
  WRONG: Quandl -> Quandl        CORRECT: Quandl -> quandl (lowercase on PyPI, but also check for Nasdaq Data Link)
  WRONG: txredisapi -> txredisapi CORRECT: txredisapi -> txredisapi (verify it actually exists on PyPI)
  WRONG: pylearn2 -> pylearn2    CORRECT: pylearn2 is NOT on PyPI — skip it (install from GitHub only)
  WRONG: opencv-python==4.10.0   CORRECT: opencv-python-headless (prefer headless — avoids system deps)
  WRONG: sunburnt -> sunburnt    CORRECT: sunburnt -> sunburnt==0.5 (must pin old version, package is archived)
  WRONG: deployment -> deployment CORRECT: deployment is a local module — skip it (common in Fabric projects)
  WRONG: taggit_autocomplete -> taggit-autocomplete CORRECT: taggit_autocomplete -> django-taggit-autosuggest (or django-taggit-autocomplete-modified)

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


_APDR_START_KEYS = (
    "tool",
    "dataset",
    "total_snippets",
    "resumed_completed",
    "effective_workers",
)
_APDR_STABLE_KEYS = (
    "build_profile",
    "validation_backend",
    "llm_validation_policy",
    "allow_llm",
    "validate",
    "range",
    "max_retries",
)


def _split_benchmark_blocks(context: str) -> list[tuple[str, list[str]]]:
    blocks: list[tuple[str, list[str]]] = []
    header = ""
    lines: list[str] = []
    for raw_line in context.splitlines():
        line = raw_line.rstrip("\n")
        if line.startswith("===== ") and line.endswith(" ====="):
            if header:
                blocks.append((header, lines))
            header = line
            lines = []
            continue
        if header:
            lines.append(line)
    if header:
        blocks.append((header, lines))
    return blocks


def _parse_block_values(lines: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            values[key] = value
    return values


def _summarize_apdr_benchmark_context(context: str) -> str:
    if "kind=benchmark-start" not in context and "kind=apdr-" not in context:
        return ""

    start_values: dict[str, str] = {}
    stable_values: dict[str, set[str]] = {key: set() for key in _APDR_STABLE_KEYS}
    saw_apdr_blocks = False

    for header, lines in _split_benchmark_blocks(context):
        kind_match = re.search(r"kind=([A-Za-z0-9_-]+)", header)
        kind = kind_match.group(1) if kind_match else ""
        values = _parse_block_values(lines)
        if not kind:
            continue
        if kind == "benchmark-start":
            saw_apdr_blocks = True
            for key in _APDR_START_KEYS:
                value = values.get(key, "")
                if value:
                    start_values[key] = value
            warnings = values.get("preflight_warnings", "")
            if warnings and warnings != "[]":
                start_values["preflight_warnings"] = warnings
            continue
        if kind.startswith("apdr-"):
            saw_apdr_blocks = True
            for key in _APDR_STABLE_KEYS:
                value = values.get(key, "")
                if value:
                    stable_values[key].add(value)

    if not saw_apdr_blocks:
        return ""

    summary_lines = ["APDR benchmark summary:"]
    for key in _APDR_START_KEYS:
        value = start_values.get(key, "")
        if value:
            summary_lines.append(f"{key}={value}")
    if "preflight_warnings" in start_values:
        summary_lines.append(f"preflight_warnings={start_values['preflight_warnings']}")
    for key in _APDR_STABLE_KEYS:
        values = stable_values.get(key, set())
        if len(values) == 1:
            summary_lines.append(f"{key}={next(iter(values))}")
    summary_lines.append("shared_case_log=omitted")
    return "\n".join(summary_lines)


def _summarize_partial_apdr_benchmark_context(context: str) -> str:
    interesting_keys = ("model", "base_url") + _APDR_START_KEYS + _APDR_STABLE_KEYS
    values: dict[str, str] = {}

    for raw_line in context.splitlines():
        line = raw_line.strip().strip(",")
        if not line or "=" not in line:
            continue
        if line.startswith("#") or line.startswith("{") or line.startswith("}"):
            continue
        key, value = line.split("=", 1)
        key = key.strip().strip('"')
        value = value.strip().strip('"').strip(",")
        if key in interesting_keys and value and value not in {"[]", "{}", "--"}:
            values.setdefault(key, value)

    if not values:
        return ""

    summary_lines = ["APDR benchmark summary:"]
    for key in ("tool", "model", "base_url") + _APDR_START_KEYS:
        value = values.get(key, "")
        if value:
            summary_lines.append(f"{key}={value}")
    for key in _APDR_STABLE_KEYS:
        value = values.get(key, "")
        if value:
            summary_lines.append(f"{key}={value}")
    summary_lines.append("shared_case_log=omitted")
    return "\n".join(summary_lines)


def compress_benchmark_context(context: str, max_chars: int = 8192) -> str:
    if not context.strip():
        return "- none"
    apdr_summary = _summarize_apdr_benchmark_context(context)
    if apdr_summary:
        if len(apdr_summary) <= max_chars:
            return apdr_summary
        context = apdr_summary
    partial_apdr_summary = _summarize_partial_apdr_benchmark_context(context)
    if partial_apdr_summary:
        if len(partial_apdr_summary) <= max_chars:
            return partial_apdr_summary
        context = partial_apdr_summary
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

_RESOLUTION_SYSTEM_BASE = """\
You are resolving Python imports to PyPI package names.

{taxonomy}
{failure_examples}
{local_bank}
{framework_allowlist}
Think step by step for each import:
1. Is this a Python standard library module? If yes, skip it entirely.
2. Is this a local/project module (settings, config, urls, models, etc.)? If yes, skip it.
3. Is this a framework submodule (e.g. django.conf)? If yes, map to the parent package.
4. Does the import match a known pattern (A-G above)? Apply the pattern.
5. Do the tier2 candidates or reverse-index context suggest a real package? Prefer those.
6. If you are NOT confident a real PyPI package exists, skip the import. Never guess.

Only include imports that map to VERIFIED real PyPI packages.
Return a JSON object with a "mappings" array. Each entry has "import_name" and "package_name".
"""

# Full system prompt (used as default / for multi-import cases)
RESOLUTION_SYSTEM = _RESOLUTION_SYSTEM_BASE.format(
    taxonomy=IMPORT_PATTERN_TAXONOMY,
    failure_examples=FAILURE_PAIR_EXAMPLES,
    local_bank=LOCAL_MODULE_BANK,
    framework_allowlist=FRAMEWORK_SUBMODULE_ALLOWLIST,
)


def resolution_system_for_imports(imports: list[str]) -> str:
    """Build a resolve system prompt tailored to the specific imports.

    For single-import cases or cases where most context is irrelevant,
    this produces a shorter prompt that keeps the model focused.
    """
    has_dotted = any("." in imp for imp in imports)

    # Always include core taxonomy and failure examples
    taxonomy = IMPORT_PATTERN_TAXONOMY
    failure_examples = FAILURE_PAIR_EXAMPLES

    # Only include framework allowlist if imports contain dotted names
    framework_allowlist = FRAMEWORK_SUBMODULE_ALLOWLIST if has_dotted else ""

    # Only include local module bank if imports could plausibly be local
    # (the pre-LLM filter already removed obvious locals, so this is for edge cases)
    local_bank = LOCAL_MODULE_BANK

    return _RESOLUTION_SYSTEM_BASE.format(
        taxonomy=taxonomy,
        failure_examples=failure_examples,
        local_bank=local_bank,
        framework_allowlist=framework_allowlist,
    )

SOLVABILITY_SYSTEM = """\
You are triaging whether a Python snippet is solvable in a generic Docker + PyPI environment.
Decide whether APDR should try dependency resolution or skip the snippet.

NOT solvable in generic Docker (flag as unsolvable):
- Host-application runtimes: Maya (maya.cmds, maya.mel, pymel), Blender (bpy), ArcGIS (arcpy),
  Houdini (hou), Rhino (rhinoscriptsyntax), Unreal, Nuke (nuke), Sublime Text, GIMP, IDA Pro,
  Cinema4D, HexChat
- macOS Objective-C frameworks: Foundation, CoreFoundation, AppKit, SystemConfiguration,
  OpenDirectory, ScriptingBridge, Cocoa, LaunchServices, Security, IOKit, DiskArbitration,
  CoreGraphics, CoreText, Quartz, WebKit, CoreBluetooth, CoreWLAN, CoreLocation
  (These are macOS system frameworks accessed via pyobjc but CANNOT be installed in Docker)
- Windows APIs: win32com, win32api, win32gui, pythoncom, wmi, comtypes (Windows-only)
- Hardware/embedded: ev3dev (LEGO EV3), gameduino (Arduino), RPi.GPIO, picamera, sense_hat,
  smbus, spidev, serial (when used for hardware), gpiozero, Adafruit_DHT, board, busio, digitalio
- Java/Jython interop: javax.*, java.*, com.android.*, pyjnius (when not in Docker)
- Private/GitHub-only packages: pylearn2, xtls, theano (dead), lasagne (dead)
- Platform-specific GUI: tkinter on headless Docker, pygame (needs display), kivy (needs display)

Think step by step:
1. Identify which imports are standard library modules (skip those).
2. For each remaining import, determine if it is a known PyPI package, a host-app runtime,
   a platform-specific framework, a hardware lib, or a local module.
3. If ALL non-stdlib imports are unsolvable, decision=skip.
4. If SOME non-stdlib imports are unsolvable but others are solvable PyPI packages,
   decision=solve but list the unsolvable ones in unsolvable_modules.

Return a JSON object with these fields:
- decision: "solve" or "skip"
- confidence: 0.00 to 1.00
- reason: short explanation
- unsolvable_modules: array of import names that cannot be resolved from PyPI
"""

SELF_REFINE_SYSTEM = f"""\
Review Python import-to-PyPI-package mappings for correctness.

{IMPORT_PATTERN_TAXONOMY}
For each mapping, verify:
1. Does this PyPI package actually exist on PyPI?
2. Does the package provide the import name shown?
3. Is there a more common/correct package for this import?
4. Is the import actually a local module that should be skipped?
5. Is the import a macOS/Windows/hardware-specific module that cannot be installed in Docker?
   If so, set corrected_package to "SKIP".
6. For opencv-python: should it be opencv-python-headless instead (avoids system deps)?
7. For tensorflow: does the code use TF1 API (tf.placeholder, tf.Session)? If so, pin ==1.15.0.

Return a JSON object with:
- all_correct: true if all mappings are correct
- corrections: array of objects with import_name, original_package, corrected_package, reason
  (empty array if all correct; use corrected_package="SKIP" to remove unsolvable imports)
"""

RECOVERY_SYSTEM = """\
You are fixing a Python dependency installation failure.
Read the error log, identify the root cause, and return a JSON fix.

Here are concrete examples of correct recovery fixes:

EXAMPLE 1 — Build failure, replace with headless variant:
  Error: "Failed building wheel for opencv-python" + "libGL.so.1: cannot open"
  Packages: opencv-python==4.8.0, numpy==1.24.0
  Fix: {"fix_possible": true, "wrong_package": "opencv-python", "correct_package": "opencv-python-headless", "reasoning": "headless has pre-built wheels, no system deps"}

EXAMPLE 2 — Wrong package name, swap preserving namespace:
  Error: "ModuleNotFoundError: No module named 'MySQLdb'"
  Packages: MySQL-python==1.2.5
  Fix: {"fix_possible": true, "wrong_package": "MySQL-python", "correct_package": "mysqlclient", "reasoning": "mysqlclient provides the MySQLdb import"}

EXAMPLE 3 — Py2 incompatibility, pin to last compatible version:
  Error: "SyntaxError: invalid syntax" (f-string in setup.py, Python 2.7)
  Packages: MarkupSafe==2.1.3
  Fix: {"fix_possible": true, "wrong_package": "MarkupSafe", "correct_package": "MarkupSafe", "version": "1.1.1", "reasoning": "2.x requires Py3, pin to last Py2 version"}

EXAMPLE 4 — Local module incorrectly resolved, remove it:
  Error: "error in deployment setup" (package 'deployment' has broken setup.py)
  Packages: deployment==1.0, fabric==2.7.1
  Fix: {"fix_possible": true, "remove_package": "deployment", "reasoning": "deployment is a local project module, not a PyPI package"}

EXAMPLE 5 — Transitive dependency conflict, add version pin:
  Error: "tensorflow 1.15.0 requires protobuf<4, but you have protobuf 4.25.0"
  Packages: tensorflow==1.15.0, protobuf==4.25.0
  Fix: {"fix_possible": true, "add_package": "protobuf==3.20.3", "reasoning": "TF 1.x needs protobuf<4"}

EXAMPLE 6 — macOS framework, unsolvable in Docker:
  Error: "Failed building wheel for pyobjc-framework-SystemConfiguration"
  Packages: pyobjc-framework-SystemConfiguration==10.0
  Fix: {"fix_possible": true, "remove_package": "pyobjc-framework-SystemConfiguration", "reasoning": "macOS framework, cannot install in Docker/Linux"}

Return a JSON object with these fields:
- fix_possible: true or false
- wrong_package: package that caused the error (exact name from resolved list)
- correct_package: correct PyPI package (can be SAME name if only pinning version)
- version: (optional) version to pin, e.g. "1.8.3"
- add_package: (optional) new dependency to add with pin, e.g. "protobuf==3.20.3"
- remove_package: (optional) package to REMOVE (local module or wrong package)
- recovery_outcome: "applied" or "abstained"
- failure_class: short reason class
- diagnostic_preview: one-line summary
- reasoning: brief explanation

CRITICAL: only suggest swaps that preserve the imported namespace.
VALID: ldap -> python-ldap, psycopg2 -> psycopg2-binary, cv2 -> opencv-python-headless
INVALID: PySide -> PySide6, ldap -> ldap3, Levenshtein -> fuzzywuzzy
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
    retrieval_profile: str = "none",
    snippet_source: str = "",
) -> str:
    ctx_str = "\n".join(context) if context else "- none"
    bm_str = compress_benchmark_context(benchmark_context, 12288)
    attr_sec = _attribute_section(attribute_usage, unresolved_imports)
    cand_sec = _candidates_section(tier2_candidates or {})
    snippet_sec = ""
    if snippet_source.strip():
        snippet_excerpt = _extract_import_section(snippet_source)
        snippet_sec = f"\nCode context:\n```python\n{snippet_excerpt}\n```\n"
    return (
        f"Target Python version: {python_version}\n\n"
        f"Retrieval profile: {retrieval_profile or 'none'}\n\n"
        f"Context:\n{ctx_str}\n"
        f"Benchmark context:\n{bm_str}"
        f"{attr_sec}"
        f"{cand_sec}"
        f"{snippet_sec}\n"
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
    bm_str = compress_benchmark_context(benchmark_context, 8192)
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


def _extract_failing_package(error_log: str) -> str:
    """Extract the failing package name from pip error output."""
    import re
    # Pattern: "Collecting X==Y" or "Building wheel for X" or "error in X setup"
    patterns = [
        r'(?:ERROR:.*?Command errored out.*?|error:.*?)(?:cwd:|File).*/([a-zA-Z0-9_-]+)/',
        r'(?:Collecting|Downloading|Building wheel for)\s+([a-zA-Z0-9_.-]+)',
        r'(?:Failed building wheel for|Could not build wheels for)\s+([a-zA-Z0-9_.-]+)',
        r'(?:No matching distribution found for)\s+([a-zA-Z0-9_.-]+)',
    ]
    for pattern in patterns:
        m = re.search(pattern, error_log, re.IGNORECASE)
        if m:
            return m.group(1).split("==")[0].split(">=")[0].split("<=")[0]
    return ""


def _plan_summary(plan: Any) -> str:
    if plan is None:
        return "- none"
    imports = list(getattr(plan, "extracted_imports", []) or [])
    mappings = list(getattr(plan, "package_mappings", []) or [])
    runtime_assumptions = list(getattr(plan, "runtime_assumptions", []) or [])
    smoke_strategy = getattr(plan, "smoke_strategy", None)
    smoke_mode = getattr(smoke_strategy, "mode", "") if smoke_strategy else ""
    smoke_targets = list(getattr(smoke_strategy, "import_targets", []) or []) if smoke_strategy else []
    unresolved = list(getattr(plan, "unresolved_imports", []) or [])
    return (
        f"- extracted_imports: {', '.join(imports) or 'none'}\n"
        f"- package_mappings: {len(mappings)}\n"
        f"- unresolved_imports: {', '.join(unresolved) or 'none'}\n"
        f"- runtime_assumptions: {', '.join(runtime_assumptions) or 'none'}\n"
        f"- smoke_strategy: mode={smoke_mode or 'none'} targets={', '.join(smoke_targets) or 'none'}"
    )


def _docker_plan_summary(plan: Any) -> str:
    if plan is None:
        return "- none"
    base_image = getattr(plan, "base_image", "") or "none"
    system_packages = list(getattr(plan, "system_packages", []) or [])
    env_vars = list(getattr(plan, "environment_variables", []) or [])
    command = list(getattr(plan, "command", []) or [])
    return (
        f"- base_image: {base_image}\n"
        f"- system_packages: {', '.join(system_packages) or 'none'}\n"
        f"- environment_variables: {', '.join(env_vars) or 'none'}\n"
        f"- command: {' '.join(command) or 'none'}"
    )


def _intake_failure_summary(failure: Any) -> str:
    if failure is None:
        return "- none"
    failure_class = getattr(failure, "failure_class", "") or "unknown"
    reason = getattr(failure, "reason", "") or "none"
    preview = getattr(failure, "diagnostic_preview", "") or "none"
    return (
        f"- failure_class: {failure_class}\n"
        f"- reason: {reason}\n"
        f"- diagnostic_preview: {preview}"
    )


def _artifact_pointer_summary(recovery_artifacts: Any) -> str:
    # NOTE: Artifact pointers are file paths the LLM cannot read.
    # Only include the executed_image_ref which is semantically useful.
    if recovery_artifacts is None:
        return ""
    image_ref = str(getattr(recovery_artifacts, "executed_image_ref", "") or "").strip()
    if image_ref:
        return f"- executed_image_ref: {image_ref}"
    return ""


def _build_error_specific_hint(
    error_type: str,
    error_log: str,
    python_version: str,
    failing_pkg: str,
) -> str:
    """Build a focused, single-strategy hint based on error type and context.

    Instead of listing all possible strategies, identify the most likely root
    cause and give one targeted recommendation.
    """
    lower_log = error_log.lower()
    is_py2 = python_version.startswith("2")
    pkg_lower = failing_pkg.lower()

    # Header with structured classification
    header = f"=== ERROR: {error_type} ==="
    if failing_pkg:
        header += f"\nFailing package: {failing_pkg}"

    strategy = ""

    if error_type == "BuildFailure":
        # Pick the ONE most likely fix based on the failing package
        if pkg_lower in ("opencv-python", "cv2"):
            strategy = "REPLACE opencv-python with opencv-python-headless (same API, pre-built wheels, no system deps)."
        elif "pyobjc" in pkg_lower or "foundation" in pkg_lower or "appkit" in pkg_lower:
            strategy = "REMOVE this package — it's macOS-only and cannot build in Docker/Linux."
        elif pkg_lower in ("dlib",):
            strategy = "PIN dlib==19.22.1 (last version with pre-built wheels) or remove if not essential."
        elif is_py2 and ("f-string" in lower_log or "walrus" in lower_log or "invalid syntax" in lower_log):
            strategy = (
                "Package dropped Python 2 support. Pin to last Py2 version "
                "(numpy==1.16.6, pandas==0.24.2, scipy==1.2.3, Flask==1.1.4, "
                "Django==1.11.29, Jinja2==2.11.3, MarkupSafe==1.1.1, cryptography==3.3.2)."
            )
        elif pkg_lower in ("domain", "core", "base", "api", "utils", "app", "common", "models"):
            strategy = f"'{failing_pkg}' looks like a local project module, not a PyPI package. Use remove_package."
        elif "no matching distribution" in lower_log:
            strategy = "Package name or version is wrong. Check if the name needs a prefix (python-, django-, Flask-)."
        else:
            strategy = "Try pinning to an older version with pre-built wheels, or check if the package name is wrong (local module?)."

    elif error_type in ("ModuleNotFound", "ImportError"):
        if "local" in lower_log or failing_pkg in ("settings", "config", "utils", "helpers"):
            strategy = "This is likely a local project module. Use remove_package to remove it from requirements."
        else:
            strategy = (
                "The import-to-package mapping may be wrong. Check known patterns "
                "(cv2->opencv-python, PIL->Pillow, yaml->PyYAML, serial->pyserial). "
                "Or a transitive dependency is missing — use add_package."
            )

    elif error_type == "VersionConflict":
        if "protobuf" in lower_log and "tensorflow" in lower_log:
            strategy = "Pin protobuf==3.20.3 (TF 1.x requires protobuf<4). Use add_package."
        elif "numpy" in lower_log:
            strategy = "Pin numpy to a compatible version (numpy<1.24 for older scipy, numpy<1.20 for Py2)."
        else:
            strategy = "Identify the conflicting packages from the error. Pin the less critical one to a compatible version."

    elif error_type == "Oscillation":
        strategy = (
            "Requirements are cycling. One or more packages may be fundamentally wrong. "
            "Check if any package name is a local module (remove_package). "
            "Do NOT repeat previous attempts."
        )

    elif error_type == "SyntaxError":
        if is_py2:
            strategy = "Package uses Python 3 syntax. Pin to last Py2-compatible version, or remove if it's a local module."
        else:
            strategy = "Package source has a syntax error. Try an older stable version or check if the package name is wrong."

    elif error_type == "RuntimeConfig":
        strategy = "Dependencies are likely correct but the app needs runtime config. Set fix_possible=false."

    else:
        strategy = "Read the error log. Look for: wrong package name, missing distribution, build failure, or local module."

    return f"{header}\nSTRATEGY: {strategy}"


def _extract_diagnostic_lines(error_log: str, error_type: str) -> str:
    """Extract only the most diagnostic lines from an error log based on error type.

    Research shows LLM accuracy *improves* with shorter, more focused input.
    Instead of sending the last 50 lines, extract the signal-rich section.
    """
    import re

    lines = error_log.splitlines()
    if len(lines) <= 20:
        return error_log

    if error_type in ("ModuleNotFound", "ImportError"):
        # For import errors, the traceback is the signal — find it
        diagnostic = []
        in_traceback = False
        for line in reversed(lines):
            if "ModuleNotFoundError" in line or "ImportError" in line:
                diagnostic.insert(0, line)
                in_traceback = True
            elif in_traceback and (line.strip().startswith("File ") or
                                   line.strip().startswith("from ") or
                                   line.strip().startswith("import ") or
                                   line.strip().startswith("Traceback")):
                diagnostic.insert(0, line)
            elif in_traceback and not line.strip():
                break
        if len(diagnostic) >= 3:
            return "\n".join(diagnostic[-20:])

    elif error_type == "BuildFailure":
        # For build failures, extract the error section (usually at the end)
        diagnostic = []
        for line in reversed(lines):
            diagnostic.insert(0, line)
            if any(marker in line.lower() for marker in [
                "error:", "failed", "command errored out",
                "subprocess-exited-with-error", "syntaxerror",
                "no matching distribution", "could not find",
            ]):
                # Include 5 lines before the error marker for context
                idx = lines.index(line) if line in lines else len(lines) - len(diagnostic)
                start = max(0, idx - 5)
                return "\n".join(lines[start:idx + 15])
            if len(diagnostic) > 30:
                break

    elif error_type == "VersionConflict":
        # Version conflicts have specific pip error messages
        diagnostic = []
        for line in lines:
            if any(marker in line.lower() for marker in [
                "incompatible", "conflict", "requires", "but you have",
                "not satisfied", "no matching distribution",
            ]):
                diagnostic.append(line)
        if diagnostic:
            return "\n".join(diagnostic[-15:])

    # Fallback: last 30 lines (not 50) to keep context tight
    return "\n".join(lines[-30:])


def _extract_import_section(snippet_source: str) -> str:
    """Extract only the import section + a few lines of context from a snippet.

    The full snippet is often irrelevant to recovery — the imports are the signal.
    """
    lines = snippet_source.splitlines()
    if len(lines) <= 30:
        return snippet_source

    import_lines = []
    last_import_idx = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(("import ", "from ")) and not stripped.startswith("#"):
            import_lines.append(i)
            last_import_idx = i

    if not import_lines:
        return "\n".join(lines[:30])

    # Include all import lines + 5 lines of surrounding context
    start = max(0, import_lines[0] - 2)
    end = min(len(lines), last_import_idx + 6)
    return "\n".join(lines[start:end])


def recovery_user(
    resolved_packages: list[str],
    error_log: str,
    snippet_source: str,
    python_version: str,
    error_type: str,
    previous_attempts: list[list[str]],
    authored_plan: Any = None,
    docker_plan: Any = None,
    intake_failure: Any = None,
    recovery_artifacts: Any = None,
) -> str:
    pkg_list = "\n".join(resolved_packages)

    # Extract focused diagnostic lines instead of raw tail (improves accuracy)
    log_excerpt = _extract_diagnostic_lines(error_log, error_type)
    # Extract only the import section from the snippet (reduces noise)
    snippet_excerpt = _extract_import_section(snippet_source)

    # Extract structured error info for better LLM reasoning
    failing_pkg = _extract_failing_package(error_log)
    structured_hint = _build_error_specific_hint(
        error_type, error_log, python_version, failing_pkg
    )

    # Structured iteration summaries (compact JSON-like format)
    history = ""
    if previous_attempts:
        lines = []
        for attempt in previous_attempts:
            if len(attempt) >= 3:
                lines.append(
                    f"  {{pkg: \"{attempt[0]}\", fix: \"{attempt[1]}\", result: \"{attempt[2]}\"}}"
                )
        if lines:
            history = (
                "\nPrevious attempts (DO NOT repeat — try something different):\n"
                + "\n".join(lines) + "\n"
            )
    parts = [
        f"Target Python version: {python_version}",
        f"Error type: {error_type}",
        structured_hint,
        f"Currently resolved packages:\n{pkg_list}",
    ]

    # Only include plan/failure sections when they have content
    plan_summary = _plan_summary(authored_plan)
    if authored_plan is not None:
        parts.append(f"Authored case plan:\n{plan_summary}")

    docker_summary = _docker_plan_summary(docker_plan)
    if docker_plan is not None:
        parts.append(f"Authored Docker plan:\n{docker_summary}")

    if intake_failure is not None:
        parts.append(f"Prior intake failure:\n{_intake_failure_summary(intake_failure)}")

    artifact_ref = _artifact_pointer_summary(recovery_artifacts)
    if artifact_ref:
        parts.append(artifact_ref)

    parts.append(f"Installation/import error:\n```\n{log_excerpt}\n```")
    parts.append(f"Python snippet imports:\n```python\n{snippet_excerpt}\n```")
    if history:
        parts.append(history)

    return "\n\n".join(p for p in parts if p.strip())


def version_user(
    package_name: str,
    versions: list[str],
    python_version: str,
    benchmark_context: str,
) -> str:
    bm_str = compress_benchmark_context(benchmark_context, 4096)
    return (
        f"Choose one installable version for the Python package '{package_name}'.\n"
        f"Target Python version: {python_version}\n"
        f"Allowed versions (oldest to newest): {', '.join(versions)}\n"
        f"Benchmark context:\n{bm_str}\n"
        f'Return a JSON object with a single field "version" set to the exact version string from the list above.\n'
        f'If none look viable, set version to "NONE".'
    )
