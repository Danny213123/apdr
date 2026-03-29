#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from check_apdr_regression import compare_artifacts, load_json


class _Args:
    def __init__(
        self,
        *,
        max_total_regression_pct: float,
        max_seconds_per_case_regression_pct: float | None,
        max_validation_regression_pct: float,
        max_llm_regression_pct: float | None,
        max_docker_startup_regression_pct: float | None,
        min_pass_rate_delta: float,
        max_env_create_regression_pct: float | None,
        max_install_regression_pct: float | None,
        max_smoke_regression_pct: float | None,
    ) -> None:
        self.max_total_regression_pct = max_total_regression_pct
        self.max_seconds_per_case_regression_pct = max_seconds_per_case_regression_pct
        self.max_validation_regression_pct = max_validation_regression_pct
        self.max_llm_regression_pct = max_llm_regression_pct
        self.max_docker_startup_regression_pct = max_docker_startup_regression_pct
        self.min_pass_rate_delta = min_pass_rate_delta
        self.max_env_create_regression_pct = max_env_create_regression_pct
        self.max_install_regression_pct = max_install_regression_pct
        self.max_smoke_regression_pct = max_smoke_regression_pct


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the full Phase 14 proof package for macOS replay gains and Windows guardrails."
    )
    parser.add_argument("--macos-before", required=True, help="Path to the macOS baseline artifact.")
    parser.add_argument("--macos-after", required=True, help="Path to the macOS candidate artifact.")
    parser.add_argument("--windows-before", required=True, help="Path to the Windows baseline artifact.")
    parser.add_argument("--windows-after", required=True, help="Path to the Windows candidate artifact.")
    parser.add_argument("--macos-md", required=True, help="Path to the macOS proof note.")
    parser.add_argument("--windows-md", required=True, help="Path to the Windows guardrail note.")
    parser.add_argument(
        "--macos-min-improvement-pct",
        type=float,
        default=20.0,
        help="Minimum required improvement percentage for macOS total duration and seconds per case.",
    )
    parser.add_argument(
        "--windows-max-regression-pct",
        type=float,
        default=10.0,
        help="Maximum allowed Windows total-duration and seconds-per-case regression percentage.",
    )
    return parser.parse_args()


def read_text(path_text: str) -> str:
    path = Path(path_text).expanduser().resolve()
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"Unable to read {path}: {exc}") from exc


def require_doc_sections(
    label: str,
    contents: str,
    required_sections: Iterable[str],
    required_terms: Iterable[str],
    failures: list[str],
) -> None:
    for section in required_sections:
        if section not in contents:
            failures.append(f"{label} missing required section: {section}")
    for term in required_terms:
        if term not in contents:
            failures.append(f"{label} missing required term: {term}")


def main() -> int:
    args = parse_args()
    macos_before = load_json(args.macos_before)
    macos_after = load_json(args.macos_after)
    windows_before = load_json(args.windows_before)
    windows_after = load_json(args.windows_after)

    macos_args = _Args(
        max_total_regression_pct=-args.macos_min_improvement_pct,
        max_seconds_per_case_regression_pct=-args.macos_min_improvement_pct,
        max_validation_regression_pct=0.0,
        max_llm_regression_pct=None,
        max_docker_startup_regression_pct=None,
        min_pass_rate_delta=0.0,
        max_env_create_regression_pct=None,
        max_install_regression_pct=None,
        max_smoke_regression_pct=None,
    )
    windows_args = _Args(
        max_total_regression_pct=args.windows_max_regression_pct,
        max_seconds_per_case_regression_pct=args.windows_max_regression_pct,
        max_validation_regression_pct=args.windows_max_regression_pct,
        max_llm_regression_pct=None,
        max_docker_startup_regression_pct=None,
        min_pass_rate_delta=0.0,
        max_env_create_regression_pct=None,
        max_install_regression_pct=None,
        max_smoke_regression_pct=None,
    )

    macos_result = compare_artifacts(macos_before, macos_after, macos_args)
    windows_result = compare_artifacts(windows_before, windows_after, windows_args)

    failures: list[str] = []
    if not macos_result["ok"]:
        failures.append("macOS comparison failed: " + "; ".join(macos_result["failures"]))
    if not windows_result["ok"]:
        failures.append("Windows comparison failed: " + "; ".join(windows_result["failures"]))

    macos_md = read_text(args.macos_md)
    windows_md = read_text(args.windows_md)
    require_doc_sections(
        "macOS proof note",
        macos_md,
        ["## Commands", "## Artifact Links", "## Before/After Verdict", "## Requirement Mapping"],
        [
            "MAC-04",
            "14-macos-before",
            "14-macos-after",
        ],
        failures,
    )
    require_doc_sections(
        "Windows guardrail note",
        windows_md,
        ["## Commands", "## Artifact Links", "## Guardrail Verdict", "## Requirement Mapping"],
        [
            "WIN-01",
            "14-windows-before",
            "14-windows-after",
        ],
        failures,
    )

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    print("PASS: macOS replay proof package meets the Phase 14 contract.")
    print(
        "PASS: Windows guardrail proof package stays within the configured non-regression threshold."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
