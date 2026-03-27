#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


METRICS = [
    "solve_duration_ms",
    "validation_duration_ms",
    "env_create_duration_ms",
    "install_duration_ms",
    "smoke_duration_ms",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare a candidate APDR baseline artifact against a committed baseline."
    )
    parser.add_argument("--baseline", required=True, help="Path to the committed baseline JSON artifact.")
    parser.add_argument("--candidate", required=True, help="Path to the candidate JSON artifact.")
    parser.add_argument(
        "--max-total-regression-pct",
        type=float,
        default=10.0,
        help="Maximum allowed increase in total duration percentage.",
    )
    parser.add_argument(
        "--max-validation-regression-pct",
        type=float,
        default=15.0,
        help="Maximum allowed increase in validation duration percentage.",
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


def total_duration(data: dict[str, Any]) -> float:
    return sum(as_float(data.get(metric)) for metric in METRICS)


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


def main() -> int:
    args = parse_args()
    baseline = load_json(args.baseline)
    candidate = load_json(args.candidate)

    baseline_total = total_duration(baseline)
    candidate_total = total_duration(candidate)
    baseline_validation = as_float(baseline.get("validation_duration_ms"))
    candidate_validation = as_float(candidate.get("validation_duration_ms"))
    baseline_pass_rate = as_float(baseline.get("pass_rate"))
    candidate_pass_rate = as_float(candidate.get("pass_rate"))
    baseline_env_create = as_float(baseline.get("env_create_duration_ms"))
    candidate_env_create = as_float(candidate.get("env_create_duration_ms"))
    baseline_install = as_float(baseline.get("install_duration_ms"))
    candidate_install = as_float(candidate.get("install_duration_ms"))
    baseline_smoke = as_float(baseline.get("smoke_duration_ms"))
    candidate_smoke = as_float(candidate.get("smoke_duration_ms"))

    total_regression = regression_pct(baseline_total, candidate_total)
    validation_regression = regression_pct(baseline_validation, candidate_validation)
    env_create_regression = regression_pct(baseline_env_create, candidate_env_create)
    install_regression = regression_pct(baseline_install, candidate_install)
    smoke_regression = regression_pct(baseline_smoke, candidate_smoke)
    pass_rate_delta = candidate_pass_rate - baseline_pass_rate

    total_ok = total_regression <= args.max_total_regression_pct
    validation_ok = validation_regression <= args.max_validation_regression_pct
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

    rows = [
        ("Metric", "Baseline", "Candidate", "Delta", "Status"),
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
            "validation_duration_ms",
            f"{baseline_validation:.0f}",
            f"{candidate_validation:.0f}",
            format_regression(validation_regression),
            status_text(validation_ok),
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
            optional_threshold_status(install_regression, args.max_install_regression_pct),
        ),
        (
            "smoke_duration_ms",
            f"{baseline_smoke:.0f}",
            f"{candidate_smoke:.0f}",
            format_regression(smoke_regression),
            optional_threshold_status(smoke_regression, args.max_smoke_regression_pct),
        ),
    ]

    print_table(rows)
    print()
    threshold_parts = [
        f"total <= {args.max_total_regression_pct:.2f}% regression",
        f"validation <= {args.max_validation_regression_pct:.2f}% regression",
        f"pass_rate delta >= {args.min_pass_rate_delta:.4f}",
    ]
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
    print(f"Thresholds: {', '.join(threshold_parts)}")

    if total_ok and validation_ok and pass_rate_ok and env_create_ok and install_ok and smoke_ok:
        print("Result: candidate is within the configured regression thresholds.")
        return 0

    failures = []
    if not pass_rate_ok:
        failures.append("pass rate dropped beyond threshold")
    if not total_ok:
        failures.append("total duration regressed beyond threshold")
    if not validation_ok:
        failures.append("validation duration regressed beyond threshold")
    if args.max_env_create_regression_pct is not None and not env_create_ok:
        failures.append("env create duration regressed beyond threshold")
    if args.max_install_regression_pct is not None and not install_ok:
        failures.append("install duration regressed beyond threshold")
    if args.max_smoke_regression_pct is not None and not smoke_ok:
        failures.append("smoke duration regressed beyond threshold")
    print(f"Result: regression check failed - {', '.join(failures)}.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
