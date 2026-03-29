#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark_ui.run_contract import REQUIRED_RUN_CONTRACT_KEYS


TIMING_KEYS = (
    "solve_duration_ms",
    "validation_duration_ms",
    "llm_duration_ms",
    "env_create_duration_ms",
    "install_duration_ms",
    "docker_startup_duration_ms",
    "smoke_duration_ms",
)
RUN_CONTRACT_FLAT_KEYS = (
    "model_name",
    "base_url",
    "run_intent",
    "execution_mode",
    "cache_state",
    "llm_context_window",
    "inference_policy",
    "build_profile",
)
ALLOWED_EXECUTION_MODES = {"env-fast", "docker-proof", "llm-hybrid", "mixed"}
ALLOWED_CACHE_STATES = {"warm", "cold", "mixed", "unknown"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that a report JSON satisfies the Phase 13 measurement contract."
    )
    parser.add_argument(
        "--sample-json",
        action="append",
        default=[],
        help="Path to a generated or hand-authored Phase 13 report JSON artifact.",
    )
    args = parser.parse_args()
    if not args.sample_json:
        parser.error("Provide at least one --sample-json path.")
    return args


def as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def validate_timing_block(scope: str, payload: Mapping[str, Any], errors: list[str]) -> None:
    for key in TIMING_KEYS:
        require(key in payload, f"{scope}: missing timing key `{key}`", errors)
        if key not in payload:
            continue
        try:
            value = float(payload[key])
        except (TypeError, ValueError):
            errors.append(f"{scope}: timing key `{key}` must be numeric")
            continue
        if value < 0:
            errors.append(f"{scope}: timing key `{key}` must be non-negative")


def validate_contract(scope: str, payload: Mapping[str, Any], errors: list[str]) -> None:
    contract = as_mapping(payload.get("run_contract"))
    require(bool(contract), f"{scope}: missing nested `run_contract`", errors)
    for key in REQUIRED_RUN_CONTRACT_KEYS:
        require(str(contract.get(key, "")).strip() != "", f"{scope}: run_contract missing `{key}`", errors)
    for key in RUN_CONTRACT_FLAT_KEYS:
        require(str(payload.get(key, "")).strip() != "", f"{scope}: missing flat `{key}`", errors)

    execution_mode = str(payload.get("execution_mode") or contract.get("execution_mode") or "").strip()
    cache_state = str(payload.get("cache_state") or contract.get("cache_state") or "").strip()
    require(
        execution_mode in ALLOWED_EXECUTION_MODES,
        f"{scope}: execution_mode must be one of {sorted(ALLOWED_EXECUTION_MODES)}",
        errors,
    )
    require(
        cache_state in ALLOWED_CACHE_STATES,
        f"{scope}: cache_state must be one of {sorted(ALLOWED_CACHE_STATES)}",
        errors,
    )


def validate_report(path: Path) -> list[str]:
    errors: list[str] = []
    payload = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(payload, dict), f"{path}: root JSON value must be an object", errors)
    if errors:
        return errors

    require(isinstance(payload.get("samples"), list), f"{path}: missing `samples` list", errors)
    samples = payload.get("samples") if isinstance(payload.get("samples"), list) else []
    require(bool(samples), f"{path}: `samples` must not be empty", errors)
    validate_contract(str(path), payload, errors)
    validate_timing_block(str(path), payload, errors)
    require(str(payload.get("validation_backend", "")).strip() != "", f"{path}: missing `validation_backend`", errors)
    require(str(payload.get("model_name", "")).strip() != "", f"{path}: missing `model_name`", errors)

    for index, sample in enumerate(samples, start=1):
        scope = f"{path} sample[{index}]"
        require(isinstance(sample, dict), f"{scope}: sample must be an object", errors)
        if not isinstance(sample, dict):
            continue
        validate_contract(scope, sample, errors)
        validate_timing_block(scope, sample, errors)
        require(str(sample.get("status", "")).strip() != "", f"{scope}: missing `status`", errors)
        require(
            str(sample.get("validation_backend", "")).strip() != "",
            f"{scope}: missing `validation_backend`",
            errors,
        )
        require(str(sample.get("relative_path", "")).strip() != "", f"{scope}: missing `relative_path`", errors)
    return errors


def main() -> int:
    args = parse_args()
    failed = False
    for raw_path in args.sample_json:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            print(f"FAILED {path}: file does not exist")
            failed = True
            continue
        errors = validate_report(path)
        if errors:
            failed = True
            print(f"FAILED {path}")
            for error in errors:
                print(f"  - {error}")
            continue
        print(f"OK {path}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
