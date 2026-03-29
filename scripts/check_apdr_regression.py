#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


TOTAL_METRICS = [
    "solve_duration_ms",
    "validation_duration_ms",
    "env_create_duration_ms",
    "install_duration_ms",
    "smoke_duration_ms",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare a candidate APDR replay artifact against a baseline."
    )
    parser.add_argument("--baseline", required=True, help="Path to the baseline JSON artifact.")
    parser.add_argument("--candidate", required=True, help="Path to the candidate JSON artifact.")
    parser.add_argument(
        "--max-total-regression-pct",
        type=float,
        default=10.0,
        help="Maximum allowed increase in synthetic total duration percentage.",
    )
    parser.add_argument(
        "--max-seconds-per-case-regression-pct",
        type=float,
        default=None,
        help="Optional maximum allowed increase in seconds-per-case percentage.",
    )
    parser.add_argument(
        "--max-validation-regression-pct",
        type=float,
        default=15.0,
        help="Maximum allowed increase in validation duration percentage.",
    )
    parser.add_argument(
        "--max-llm-regression-pct",
        type=float,
        default=None,
        help="Optional maximum allowed increase in LLM duration percentage.",
    )
    parser.add_argument(
        "--max-docker-startup-regression-pct",
        type=float,
        default=None,
        help="Optional maximum allowed increase in Docker startup duration percentage.",
    )
    parser.add_argument(
        "--min-pass-rate-delta",
        type=float,
        default=0.0,
        help="Minimum acceptable candidate pass_rate - baseline pass_rate delta.",
    )
    parser.add_argument(
        "--max-env-create-regression-pct",
        type=float,
        default=None,
        help="Optional maximum allowed increase in env creation duration percentage.",
    )
    parser.add_argument(
        "--max-install-regression-pct",
        type=float,
        default=None,
        help="Optional maximum allowed increase in install duration percentage.",
    )
    parser.add_argument(
        "--max-smoke-regression-pct",
        type=float,
        default=None,
        help="Optional maximum allowed increase in smoke duration percentage.",
    )
    return parser.parse_args()


def load_json(path_text: str) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"Unable to read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Unable to parse {path}: {exc}") from exc


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def as_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def total_duration(data: dict[str, Any]) -> float:
    return sum(as_float(data.get(metric)) for metric in TOTAL_METRICS)


def sample_count(data: dict[str, Any]) -> int:
    count = as_int(data.get("sample_count"))
    if count > 0:
        return count
    samples = data.get("samples")
    if isinstance(samples, list):
        return len(samples)
    return 0


def seconds_per_case(data: dict[str, Any]) -> float:
    count = sample_count(data)
    if count <= 0:
        return 0.0
    return total_duration(data) / 1000.0 / count


def regression_pct(baseline_value: float, candidate_value: float) -> float:
    if baseline_value <= 0:
        if candidate_value <= 0:
            return 0.0
        return float("inf")
    return ((candidate_value - baseline_value) / baseline_value) * 100.0


def status_text(ok: bool) -> str:
    return "OK" if ok else "FAIL"


def optional_threshold_status(pct: float, threshold: float | None) -> str:
    if threshold is None:
        return "-"
    return status_text(pct <= threshold)


def format_regression(pct: float) -> str:
    return "inf%" if pct == float("inf") else f"{pct:+.2f}%"


def print_table(rows: list[tuple[str, str, str, str, str]]) -> None:
    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    for idx, row in enumerate(rows):
        line = " | ".join(value.ljust(widths[col]) for col, value in enumerate(row))
        print(line)
        if idx == 0:
            print("-+-".join("-" * width for width in widths))


def sample_key(sample: dict[str, Any]) -> str:
    for key in ("relative_path", "snippet", "command"):
        value = str(sample.get(key) or "").strip()
        if value:
            return value
    return ""


def normalized_sample_status(sample: dict[str, Any]) -> str:
    status = str(sample.get("status") or "").strip().lower()
    if status.startswith("skipped"):
        return "skipped"
    return status


def sample_lookup(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    samples = data.get("samples")
    if not isinstance(samples, list):
        return lookup
    for raw_sample in samples:
        if not isinstance(raw_sample, dict):
            continue
        key = sample_key(raw_sample)
        if not key:
            continue
        lookup[key] = raw_sample
    return lookup


def preserved_case_result(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    preserved_status: str,
) -> tuple[int, int, list[str]]:
    baseline_lookup = sample_lookup(baseline)
    candidate_lookup = sample_lookup(candidate)
    expected = sorted(
        key
        for key, sample in baseline_lookup.items()
        if normalized_sample_status(sample) == preserved_status
    )
    regressed = [
        key
        for key in expected
        if normalized_sample_status(candidate_lookup.get(key, {})) != preserved_status
    ]
    preserved = len(expected) - len(regressed)
    return len(expected), preserved, regressed


def compare_metadata_field(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    field: str,
    label: str,
    failures: list[str],
    rows: list[tuple[str, str, str, str, str]],
) -> None:
    baseline_value = str(baseline.get(field) or "").strip()
    candidate_value = str(candidate.get(field) or "").strip()
    if not baseline_value and not candidate_value:
        return
    ok = baseline_value == candidate_value
    rows.append(
        (
            label,
            baseline_value or "--",
            candidate_value or "--",
            "match" if ok else "mismatch",
            status_text(ok),
        )
    )
    if not ok:
        failures.append(
            f"{field} mismatch: baseline={baseline_value or '--'} candidate={candidate_value or '--'}"
        )


def compare_artifacts(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    baseline_total = total_duration(baseline)
    candidate_total = total_duration(candidate)
    baseline_seconds = seconds_per_case(baseline)
    candidate_seconds = seconds_per_case(candidate)
    baseline_validation = as_float(baseline.get("validation_duration_ms"))
    candidate_validation = as_float(candidate.get("validation_duration_ms"))
    baseline_llm = as_float(baseline.get("llm_duration_ms"))
    candidate_llm = as_float(candidate.get("llm_duration_ms"))
    baseline_docker_startup = as_float(baseline.get("docker_startup_duration_ms"))
    candidate_docker_startup = as_float(candidate.get("docker_startup_duration_ms"))
    baseline_pass_rate = as_float(baseline.get("pass_rate"))
    candidate_pass_rate = as_float(candidate.get("pass_rate"))
    baseline_env_create = as_float(baseline.get("env_create_duration_ms"))
    candidate_env_create = as_float(candidate.get("env_create_duration_ms"))
    baseline_install = as_float(baseline.get("install_duration_ms"))
    candidate_install = as_float(candidate.get("install_duration_ms"))
    baseline_smoke = as_float(baseline.get("smoke_duration_ms"))
    candidate_smoke = as_float(candidate.get("smoke_duration_ms"))

    total_regression = regression_pct(baseline_total, candidate_total)
    seconds_per_case_regression = regression_pct(baseline_seconds, candidate_seconds)
    validation_regression = regression_pct(baseline_validation, candidate_validation)
    llm_regression = regression_pct(baseline_llm, candidate_llm)
    docker_startup_regression = regression_pct(
        baseline_docker_startup, candidate_docker_startup
    )
    env_create_regression = regression_pct(baseline_env_create, candidate_env_create)
    install_regression = regression_pct(baseline_install, candidate_install)
    smoke_regression = regression_pct(baseline_smoke, candidate_smoke)
    pass_rate_delta = candidate_pass_rate - baseline_pass_rate

    failures: list[str] = []
    rows: list[tuple[str, str, str, str, str]] = [
        ("Metric", "Baseline", "Candidate", "Delta", "Status"),
    ]

    compare_metadata_field(baseline, candidate, "slice_id", "slice_id", failures, rows)
    compare_metadata_field(
        baseline, candidate, "execution_mode", "execution_mode", failures, rows
    )
    compare_metadata_field(baseline, candidate, "cache_state", "cache_state", failures, rows)
    compare_metadata_field(
        baseline, candidate, "build_profile", "build_profile", failures, rows
    )

    total_ok = total_regression <= args.max_total_regression_pct
    seconds_ok = (
        True
        if args.max_seconds_per_case_regression_pct is None
        else seconds_per_case_regression <= args.max_seconds_per_case_regression_pct
    )
    validation_ok = validation_regression <= args.max_validation_regression_pct
    llm_ok = (
        True if args.max_llm_regression_pct is None else llm_regression <= args.max_llm_regression_pct
    )
    docker_startup_ok = (
        True
        if args.max_docker_startup_regression_pct is None
        else docker_startup_regression <= args.max_docker_startup_regression_pct
    )
    pass_rate_ok = pass_rate_delta >= args.min_pass_rate_delta
    env_create_ok = (
        True
        if args.max_env_create_regression_pct is None
        else env_create_regression <= args.max_env_create_regression_pct
    )
    install_ok = (
        True
        if args.max_install_regression_pct is None
        else install_regression <= args.max_install_regression_pct
    )
    smoke_ok = (
        True
        if args.max_smoke_regression_pct is None
        else smoke_regression <= args.max_smoke_regression_pct
    )

    baseline_preserved_pass, candidate_preserved_pass, regressed_pass = preserved_case_result(
        baseline, candidate, "passed"
    )
    baseline_preserved_skip, candidate_preserved_skip, regressed_skip = preserved_case_result(
        baseline, candidate, "skipped"
    )
    preserved_pass_ok = candidate_preserved_pass == baseline_preserved_pass
    preserved_skip_ok = candidate_preserved_skip == baseline_preserved_skip

    rows.extend(
        [
            (
                "pass_rate",
                f"{baseline_pass_rate:.4f}",
                f"{candidate_pass_rate:.4f}",
                f"{pass_rate_delta:+.4f}",
                status_text(pass_rate_ok),
            ),
            (
                "total_duration_ms",
                f"{baseline_total:.0f}",
                f"{candidate_total:.0f}",
                format_regression(total_regression),
                status_text(total_ok),
            ),
            (
                "seconds_per_case",
                f"{baseline_seconds:.3f}",
                f"{candidate_seconds:.3f}",
                format_regression(seconds_per_case_regression),
                optional_threshold_status(
                    seconds_per_case_regression, args.max_seconds_per_case_regression_pct
                ),
            ),
            (
                "validation_duration_ms",
                f"{baseline_validation:.0f}",
                f"{candidate_validation:.0f}",
                format_regression(validation_regression),
                status_text(validation_ok),
            ),
            (
                "llm_duration_ms",
                f"{baseline_llm:.0f}",
                f"{candidate_llm:.0f}",
                format_regression(llm_regression),
                optional_threshold_status(llm_regression, args.max_llm_regression_pct),
            ),
            (
                "docker_startup_duration_ms",
                f"{baseline_docker_startup:.0f}",
                f"{candidate_docker_startup:.0f}",
                format_regression(docker_startup_regression),
                optional_threshold_status(
                    docker_startup_regression, args.max_docker_startup_regression_pct
                ),
            ),
            (
                "env_create_duration_ms",
                f"{baseline_env_create:.0f}",
                f"{candidate_env_create:.0f}",
                format_regression(env_create_regression),
                optional_threshold_status(
                    env_create_regression, args.max_env_create_regression_pct
                ),
            ),
            (
                "install_duration_ms",
                f"{baseline_install:.0f}",
                f"{candidate_install:.0f}",
                format_regression(install_regression),
                optional_threshold_status(
                    install_regression, args.max_install_regression_pct
                ),
            ),
            (
                "smoke_duration_ms",
                f"{baseline_smoke:.0f}",
                f"{candidate_smoke:.0f}",
                format_regression(smoke_regression),
                optional_threshold_status(smoke_regression, args.max_smoke_regression_pct),
            ),
            (
                "preserved_pass_cases",
                str(baseline_preserved_pass),
                str(candidate_preserved_pass),
                f"{candidate_preserved_pass - baseline_preserved_pass:+d}",
                status_text(preserved_pass_ok),
            ),
            (
                "preserved_skip_cases",
                str(baseline_preserved_skip),
                str(candidate_preserved_skip),
                f"{candidate_preserved_skip - baseline_preserved_skip:+d}",
                status_text(preserved_skip_ok),
            ),
        ]
    )

    if not pass_rate_ok:
        failures.append("pass rate dropped beyond threshold")
    if not total_ok:
        failures.append("total duration regressed beyond threshold")
    if not seconds_ok:
        failures.append("seconds per case regressed beyond threshold")
    if not validation_ok:
        failures.append("validation duration regressed beyond threshold")
    if args.max_llm_regression_pct is not None and not llm_ok:
        failures.append("llm duration regressed beyond threshold")
    if args.max_docker_startup_regression_pct is not None and not docker_startup_ok:
        failures.append("docker startup duration regressed beyond threshold")
    if args.max_env_create_regression_pct is not None and not env_create_ok:
        failures.append("env create duration regressed beyond threshold")
    if args.max_install_regression_pct is not None and not install_ok:
        failures.append("install duration regressed beyond threshold")
    if args.max_smoke_regression_pct is not None and not smoke_ok:
        failures.append("smoke duration regressed beyond threshold")
    if not preserved_pass_ok:
        failures.append(
            "preserved pass cases regressed: " + ", ".join(regressed_pass[:5])
            if regressed_pass
            else "preserved pass cases regressed"
        )
    if not preserved_skip_ok:
        failures.append(
            "preserved skip cases regressed: " + ", ".join(regressed_skip[:5])
            if regressed_skip
            else "preserved skip cases regressed"
        )

    threshold_parts = [
        f"total <= {args.max_total_regression_pct:.2f}% regression",
        f"validation <= {args.max_validation_regression_pct:.2f}% regression",
        f"pass_rate delta >= {args.min_pass_rate_delta:.4f}",
        "preserved pass cases unchanged",
        "preserved skip cases unchanged",
    ]
    if args.max_seconds_per_case_regression_pct is not None:
        threshold_parts.append(
            f"seconds_per_case <= {args.max_seconds_per_case_regression_pct:.2f}% regression"
        )
    if args.max_llm_regression_pct is not None:
        threshold_parts.append(f"llm <= {args.max_llm_regression_pct:.2f}% regression")
    if args.max_docker_startup_regression_pct is not None:
        threshold_parts.append(
            f"docker_startup <= {args.max_docker_startup_regression_pct:.2f}% regression"
        )
    if args.max_env_create_regression_pct is not None:
        threshold_parts.append(
            f"env_create <= {args.max_env_create_regression_pct:.2f}% regression"
        )
    if args.max_install_regression_pct is not None:
        threshold_parts.append(
            f"install <= {args.max_install_regression_pct:.2f}% regression"
        )
    if args.max_smoke_regression_pct is not None:
        threshold_parts.append(
            f"smoke <= {args.max_smoke_regression_pct:.2f}% regression"
        )

    ok = (
        not failures
        and total_ok
        and validation_ok
        and pass_rate_ok
        and seconds_ok
        and llm_ok
        and docker_startup_ok
        and env_create_ok
        and install_ok
        and smoke_ok
        and preserved_pass_ok
        and preserved_skip_ok
    )
    return {
        "rows": rows,
        "threshold_parts": threshold_parts,
        "failures": failures,
        "ok": ok,
    }


def main() -> int:
    args = parse_args()
    baseline = load_json(args.baseline)
    candidate = load_json(args.candidate)
    result = compare_artifacts(baseline, candidate, args)

    print_table(result["rows"])
    print()
    print(f"Thresholds: {', '.join(result['threshold_parts'])}")

    if result["ok"]:
        print("Result: candidate is within the configured regression thresholds.")
        return 0

    print(f"Result: regression check failed - {', '.join(result['failures'])}.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
