"""Recovery package hint action handler.

Uses tolerant JSON completion for better compatibility with small models.
Integrates #12 build error pattern library for RAG-enriched recovery prompts.
"""

from __future__ import annotations

import logging
import re

from pydantic import BaseModel

from ..build_error_patterns import format_error_context
from ..client import LlmClient
from ..models import ResolutionRequest, ResolutionResponse
from ..pypi_checker import package_exists_on_pypi
from .. import prompts

logger = logging.getLogger("apdr_llm")

EXPLICIT_NAMESPACE_MAPPINGS = {
    ("pkg_resources", "setuptools"),
    ("pil", "pillow"),
    ("image", "pillow"),
    ("imagedraw", "pillow"),
    ("imagefont", "pillow"),
    ("imageenhance", "pillow"),
    ("imagegrab", "pillow"),
    ("yaml", "pyyaml"),
    ("cv2", "opencv-python"),
    ("gi", "pygobject"),
    ("gi.repository", "pygobject"),
    ("rest_framework", "djangorestframework"),
    ("sklearn", "scikit-learn"),
    ("bs4", "beautifulsoup4"),
    ("serial", "pyserial"),
    ("usb", "pyusb"),
    ("psycopg2", "psycopg2-binary"),
    ("systemconfiguration", "pyobjc-framework-systemconfiguration"),
    ("ldap", "python-ldap"),
    ("levenshtein", "python-levenshtein"),
    ("mysqldb", "mysqlclient"),
}

PACKAGE_NAMESPACE_MAP = {
    "python-ldap": {"ldap"},
    "ldap": {"ldap"},
    "ldap3": {"ldap3"},
    "python-levenshtein": {"levenshtein"},
    "fuzzywuzzy": {"fuzzywuzzy"},
    "rapidfuzz": {"rapidfuzz"},
    "psycopg2-binary": {"psycopg2"},
    "mysqlclient": {"mysqldb"},
    "pymysql": {"pymysql"},
    "pyside": {"pyside"},
    "pyside2": {"pyside2"},
    "pyside6": {"pyside6"},
    "pygobject": {"gi"},
    "setuptools": {"pkg_resources", "setuptools"},
    "pillow": {"pil", "image", "imagedraw", "imagefont", "imageenhance", "imagegrab"},
    "pyyaml": {"yaml"},
    "opencv-python": {"cv2"},
    "opencv-python-headless": {"cv2"},
}


def _normalize(value: str) -> str:
    return value.strip().lower().replace("_", "-").replace(".", "-")


def _extract_import_map(resolved_packages: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in resolved_packages:
        match = re.search(r"^(?P<pkg>.+?)\s+\(import:\s*(?P<imp>[^)]+)\)", item)
        if not match:
            continue
        pkg = match.group("pkg").split("==", 1)[0].strip()
        imp = match.group("imp").strip()
        mapping[_normalize(pkg)] = imp
    return mapping


def _namespace_mapping_allowed(import_name: str, package_name: str) -> bool:
    import_norm = _normalize(import_name)
    package_norm = _normalize(package_name)
    if not import_norm or not package_norm:
        return False
    if import_norm == package_norm:
        return True
    if (import_norm, package_norm) in EXPLICIT_NAMESPACE_MAPPINGS:
        return True
    provided = PACKAGE_NAMESPACE_MAP.get(package_norm, set())
    top_level = import_name.split(".", 1)[0].strip().lower()
    return top_level in provided or import_name.strip().lower() in provided


class RecoveryResult(BaseModel):
    fix_possible: bool = False
    wrong_package: str = ""
    correct_package: str = ""
    version: str = ""           # specific version pin (e.g. "1.8.3")
    add_package: str = ""       # new transitive dep to add (e.g. "protobuf==3.20.3")
    remove_package: str = ""    # package to remove entirely (local module / wrong package)
    reasoning: str = ""


def handle(req: ResolutionRequest) -> ResolutionResponse:
    client = LlmClient(req.provider, req.model, req.base_url)
    if not client.is_available():
        return ResolutionResponse(error="LLM provider not available")

    notes: list[str] = []
    import_map = _extract_import_map(req.resolved_packages)

    # --- #12: Enrich recovery context with known build error patterns ---
    error_pattern_ctx = ""
    if req.error_log:
        error_pattern_ctx = format_error_context(req.error_log)
        if error_pattern_ctx:
            # ADD structured logging BEFORE adding note
            from ..build_error_patterns import match_error_patterns
            matched_patterns = match_error_patterns(req.error_log)

            logger.info(
                "RAG pattern library matched",
                extra={
                    "event": "pattern_match",
                    "action": "recovery",
                    "patterns_matched": len(matched_patterns),
                    "top_pattern": matched_patterns[0].diagnosis if matched_patterns else None,
                    "fix_type": matched_patterns[0].fix_type if matched_patterns else None,
                }
            )
            notes.append("Build error pattern library matched")

    user_prompt = prompts.recovery_user(
        resolved_packages=req.resolved_packages,
        error_log=req.error_log,
        snippet_source=req.snippet_source,
        python_version=req.python_version,
        error_type=req.error_type,
        previous_attempts=req.previous_attempts,
    )

    # Prepend error pattern context if we have matches
    if error_pattern_ctx:
        user_prompt = f"{error_pattern_ctx}\n\n{user_prompt}"

    # Tolerant JSON call — no field validators during generation
    result = client.complete_json(
        system_prompt=prompts.RECOVERY_SYSTEM,
        user_prompt=user_prompt,
        response_model=RecoveryResult,
        max_tokens=1024,  # Larger for thinking mode output
        num_ctx=8192,  # Recovery needs enough context for error log + snippet
    )

    if result is None:
        return ResolutionResponse(
            fix_possible=False,
            error="LLM recovery returned no output",
            prompts_issued=1,
        )

    # Post-hoc PyPI validation
    if result.fix_possible and result.correct_package:
        pkg_name = result.correct_package.split("==")[0].split(">=")[0].split("<=")[0]
        if not package_exists_on_pypi(pkg_name):
            # ADD structured logging BEFORE adding note
            logger.info(
                "PyPI validation rejected hallucinated package",
                extra={
                    "event": "pypi_rejection",
                    "action": "recovery",
                    "package": pkg_name,
                    "suggested_by": "llm",
                    "import_name": result.wrong_package,
                }
            )
            notes.append(f"Recovery suggestion '{result.correct_package}' not found on PyPI")
            result.fix_possible = False
        elif result.wrong_package and result.correct_package and result.wrong_package != result.correct_package:
            import_name = import_map.get(_normalize(result.wrong_package), "")
            if import_name and not _namespace_mapping_allowed(import_name, result.correct_package):
                # ADD structured logging
                logger.info(
                    "Namespace validation rejected package swap",
                    extra={
                        "event": "namespace_rejection",
                        "action": "recovery",
                        "wrong_package": result.wrong_package,
                        "correct_package": result.correct_package,
                        "import_name": import_name,
                    }
                )
                notes.append(
                    f"Recovery suggestion '{result.wrong_package}' -> '{result.correct_package}' "
                    f"does not preserve import namespace '{import_name}'"
                )
                result.fix_possible = False

    if result.fix_possible and result.add_package:
        add_pkg_name = result.add_package.split("==")[0].split(">=")[0].split("<=")[0]
        if not package_exists_on_pypi(add_pkg_name):
            # ADD structured logging
            logger.info(
                "PyPI validation rejected add_package",
                extra={
                    "event": "pypi_rejection",
                    "action": "recovery",
                    "package": add_pkg_name,
                    "field": "add_package",
                }
            )
            notes.append(f"Recovery add_package '{result.add_package}' not found on PyPI")
            result.add_package = ""

    # Validate remove_package against PyPI too — but removal doesn't need PyPI check
    if result.fix_possible and result.remove_package:
        # remove_package is always valid — it just removes from the resolved list
        pass

    if not result.fix_possible:
        # Even if swap isn't possible, adding a dep or removing one might help
        if result.add_package or result.remove_package:
            if result.reasoning:
                notes.append(result.reasoning)
            return ResolutionResponse(
                fix_possible=True,
                add_package=result.add_package,
                remove_package=result.remove_package,
                notes=notes,
                prompts_issued=1,
            )
        return ResolutionResponse(fix_possible=False, notes=notes, prompts_issued=1)

    # Allow same-package version pin: wrong_package == correct_package + version
    has_swap = (
        result.wrong_package
        and result.correct_package
        and result.wrong_package != result.correct_package
    )
    has_version_pin = (
        result.wrong_package
        and result.correct_package
        and result.version
        and result.wrong_package == result.correct_package
    )

    if not has_swap and not has_version_pin:
        # No valid swap or version pin, but maybe there's an add_package or remove_package
        if result.add_package or result.remove_package:
            if result.reasoning:
                notes.append(result.reasoning)
            return ResolutionResponse(
                fix_possible=True,
                add_package=result.add_package,
                remove_package=result.remove_package,
                notes=notes,
                prompts_issued=1,
            )
        return ResolutionResponse(fix_possible=False, notes=notes, prompts_issued=1)

    if result.reasoning:
        notes.append(result.reasoning)

    return ResolutionResponse(
        fix_possible=True,
        wrong_package=result.wrong_package,
        correct_package=result.correct_package,
        version=result.version,
        add_package=result.add_package,
        remove_package=result.remove_package,
        notes=notes,
        prompts_issued=1,
    )
