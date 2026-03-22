"""Long-running JSON-line subprocess for APDR LLM calls.

Protocol: Reads one JSON object per line from stdin, writes one JSON
object per line to stdout. Each request has an "action" field that
dispatches to the appropriate handler.

Usage:
    python -m llm_py                     # start the service
    python -m llm_py --test              # run self-tests
    echo '{"action":"resolve",...}' | python -m llm_py
"""

from __future__ import annotations

import json
import logging
import sys
import traceback

from .models import ResolutionRequest, ResolutionResponse
from .actions import resolve, solvability, recovery, version, single

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("apdr_llm")


def dispatch(req: ResolutionRequest) -> ResolutionResponse:
    """Route a request to the appropriate action handler."""
    match req.action:
        case "resolve":
            return resolve.handle(req)
        case "solvability":
            return solvability.handle(req)
        case "recovery":
            return recovery.handle(req)
        case "version":
            return version.handle(req)
        case "single":
            return single.handle(req)
        case "react":
            from .actions import react_agent
            return react_agent.handle(req)
        case _:
            return ResolutionResponse(error=f"Unknown action: {req.action}")


def main() -> None:
    """Read JSON-line requests from stdin, write responses to stdout."""
    # Signal readiness
    sys.stdout.write('{"ready":true}\n')
    sys.stdout.flush()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = ResolutionRequest.model_validate_json(line)
            response = dispatch(req)
        except Exception as e:
            logger.error("Error processing request: %s\n%s", e, traceback.format_exc())
            response = ResolutionResponse(error=str(e))

        sys.stdout.write(response.model_dump_json() + "\n")
        sys.stdout.flush()


def run_self_tests() -> None:
    """Quick smoke tests for the Python LLM service."""
    print("Testing models...")
    req = ResolutionRequest(
        action="resolve",
        imports=["cv2", "PIL"],
        python_version="3.10",
        provider="ollama",
        model="gemma3:12b",
        base_url="http://localhost:11434",
    )
    json_str = req.model_dump_json()
    parsed = ResolutionRequest.model_validate_json(json_str)
    assert parsed.action == "resolve"
    assert parsed.imports == ["cv2", "PIL"]
    print("  models OK")

    print("Testing prompts...")
    from . import prompts
    user = prompts.package_resolution_user(
        unresolved_imports=["cv2"],
        python_version="3.10",
        context=[],
        benchmark_context="",
        attribute_usage={},
    )
    assert "cv2" in user
    assert "3.10" in user
    print("  prompts OK")

    print("Testing failure memory...")
    import tempfile
    from .failure_memory import FailureMemory
    with tempfile.TemporaryDirectory() as td:
        fm = FailureMemory.load(td)
        fm.record_failure("cv2", "opencv", "not found", "3.10")
        assert fm.has_failed("cv2", "opencv")
        assert not fm.has_failed("cv2", "opencv-python")
        ctx = fm.format_context(["cv2"])
        assert "opencv" in ctx
        fm.save()
        fm2 = FailureMemory.load(td)
        assert fm2.has_failed("cv2", "opencv")
    print("  failure_memory OK")

    print("Testing PyPI checker...")
    from .pypi_checker import package_exists_on_pypi, preload_known_packages
    preload_known_packages(["requests", "flask", "django"])
    assert package_exists_on_pypi("requests")
    assert package_exists_on_pypi("flask")
    print("  pypi_checker OK")

    print("Testing reverse index...")
    from .reverse_index import load, lookup, enrich_context
    load()
    ctx = enrich_context(["cv2", "PIL"])
    assert isinstance(ctx, list)
    print("  reverse_index OK")

    print("Testing validated mapping model...")
    from .actions.resolve import ValidatedPackageMapping, ValidatedMappingsResult
    m = ValidatedPackageMapping(import_name="requests", package_name="requests")
    assert m.package_name == "requests"
    mr = ValidatedMappingsResult(mappings=[])
    assert mr.mappings == []
    print("  validated models OK")

    print("Testing constrained decoding model builder...")
    from .actions.resolve import _build_constrained_model
    model = _build_constrained_model({"cv2": ["opencv-python", "opencv-contrib-python"]})
    assert model is not None
    assert _build_constrained_model({"cv2": ["opencv-python"]}) is None
    print("  constrained decoding OK")

    print("Testing ReAct agent tools...")
    from .actions.react_agent import search_seed_data, _extract_mappings_from_text
    result = json.loads(search_seed_data("cv2"))
    assert "import_name" in result
    test_json = '{"mappings": [{"import_name": "cv2", "package_name": "opencv-python"}]}'
    mappings = _extract_mappings_from_text(test_json, ["cv2"])
    assert len(mappings) == 1
    assert mappings[0].package_name == "opencv-python"
    test_arrow = "cv2 -> opencv-python\nPIL -> Pillow"
    mappings2 = _extract_mappings_from_text(test_arrow, ["cv2", "PIL"])
    assert len(mappings2) == 2
    print("  react_agent tools OK")

    print("Testing local detector...")
    from .local_detector import filter_imports, is_likely_local, get_framework_parent
    assert is_likely_local("settings")
    assert is_likely_local("conftest")
    assert not is_likely_local("numpy")
    assert get_framework_parent("django.conf") == "django"
    assert get_framework_parent("flask.views") == "flask"
    assert get_framework_parent("numpy") is None
    needs_llm, skips, fw = filter_imports(["numpy", "settings", "django.conf"])
    assert needs_llm == ["numpy"]
    assert "settings" in skips
    assert "django.conf" in fw and fw["django.conf"] == "django"
    print("  local_detector OK")

    print("Testing build error patterns...")
    from .build_error_patterns import match_error_patterns, format_error_context
    matches = match_error_patterns("fatal error: Python.h: No such file or directory")
    assert len(matches) >= 1
    assert matches[0].fix_type == "system_dep"
    ctx = format_error_context("pg_config executable not found")
    assert "psycopg2-binary" in ctx
    empty = format_error_context("everything is fine")
    assert empty == ""
    print("  build_error_patterns OK")

    print("Testing active learning...")
    from .active_learning import extract_passed_mappings, extract_failed_mappings
    # Just verify the functions exist and handle empty input gracefully
    assert extract_passed_mappings("/nonexistent") == []
    assert extract_failed_mappings("/nonexistent") == []
    print("  active_learning OK")

    print("\nAll self-tests passed!")


if __name__ == "__main__":
    if "--test" in sys.argv:
        run_self_tests()
    else:
        main()
