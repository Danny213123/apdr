#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from build_phase7_parity_manifest import load_pllm_pass_counts, load_summary, select_overlap_cases


REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_BASELINE_HEADINGS = (
    "## Commands",
    "## Artifact Links",
    "## Canonical Slice",
    "## Normalized Buckets",
    "## Touched Family Snapshots",
    "## Tier1 Watchlist",
    "## Verification",
    "## Phase 8 Handoff",
)


class InvariantError(RuntimeError):
    def __init__(self, name: str, detail: str) -> None:
        super().__init__(f"{name}: {detail}")
        self.name = name
        self.detail = detail


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Phase 7 parity manifest, family snapshot manifest, and baseline note."
    )
    parser.add_argument("--summary-json", required=True, help="Path to APDR summary.json.")
    parser.add_argument("--pllm-csv", required=True, help="Path to the pllm comparison CSV.")
    parser.add_argument("--parity-manifest", required=True, help="Path to the Phase 7 parity manifest.")
    parser.add_argument("--family-manifest", required=True, help="Path to the Phase 7 family snapshot manifest.")
    parser.add_argument("--baseline-md", required=True, help="Path to the Phase 7 baseline note.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} does not contain a JSON object")
    return payload


def clean_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"", "--", "none", "null"}:
        return ""
    return text


def require(condition: bool, name: str, detail: str) -> None:
    if not condition:
        raise InvariantError(name, detail)


def repo_relative_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def validate_parity_manifest(
    summary_json: Path,
    pllm_csv: Path,
    parity_manifest_path: Path,
) -> dict[str, Any]:
    parity_manifest = load_json(parity_manifest_path)
    summary_payload, results = load_summary(summary_json)
    pllm_pass_counts = load_pllm_pass_counts(pllm_csv)
    canonical_cases, tier1_watchlist = select_overlap_cases(results, pllm_pass_counts)

    expected_canonical_ids = [case["case_id"] for case in canonical_cases]
    expected_watchlist_ids = [case["case_id"] for case in tier1_watchlist]

    manifest_case_ids = parity_manifest.get("canonical_case_ids") or []
    manifest_watchlist_ids = parity_manifest.get("tier1_watchlist_case_ids") or []
    manifest_cases = parity_manifest.get("cases") or []

    require(
        parity_manifest.get("canonical_case_count") == 70,
        "canonical_case_count",
        f"expected 70, found {parity_manifest.get('canonical_case_count')!r}",
    )
    require(
        parity_manifest.get("tier1_watchlist_count") == 17,
        "tier1_watchlist_count",
        f"expected 17, found {parity_manifest.get('tier1_watchlist_count')!r}",
    )
    require(
        parity_manifest.get("pllm_overlap_case_count") == 87,
        "pllm_overlap_case_count",
        f"expected 87, found {parity_manifest.get('pllm_overlap_case_count')!r}",
    )
    require(
        manifest_case_ids == expected_canonical_ids,
        "canonical_case_ids",
        "parity manifest canonical IDs no longer match the raw summary/pllm overlap",
    )
    require(
        manifest_watchlist_ids == expected_watchlist_ids,
        "tier1_watchlist_case_ids",
        "parity manifest tier1 watchlist IDs no longer match the raw summary/pllm overlap",
    )
    require(
        [clean_text(case.get("case_id")) for case in manifest_cases] == expected_canonical_ids,
        "manifest_cases",
        "parity manifest case entries do not align with canonical_case_ids",
    )

    normalized_bucket_totals = Counter(clean_text(case.get("normalized_bucket")) for case in manifest_cases)
    normalized_bucket_totals.pop("", None)
    require(
        dict(sorted(normalized_bucket_totals.items())) == dict(
            sorted((parity_manifest.get("normalized_bucket_totals") or {}).items())
        ),
        "normalized_bucket_totals",
        "normalized bucket totals do not match the per-case normalized_bucket values",
    )

    require(
        clean_text(parity_manifest.get("summary_json")) == "runs/20260327-150339-apdr/summary.json",
        "summary_json_path",
        "parity manifest no longer points at the locked Phase 7 APDR summary input",
    )
    require(
        clean_text(parity_manifest.get("pllm_csv")) == "pllm_results/csv/summary-all-runs.csv",
        "pllm_csv_path",
        "parity manifest no longer points at the locked Phase 7 pllm input",
    )

    return parity_manifest


def validate_family_manifest(
    family_manifest_path: Path,
    parity_manifest: dict[str, Any],
) -> dict[str, Any]:
    family_manifest = load_json(family_manifest_path)
    family_cases = family_manifest.get("cases") or []
    selected_ids = family_manifest.get("selected_case_ids") or []
    canonical_ids = set(parity_manifest.get("canonical_case_ids") or [])

    require(
        family_manifest.get("selected_case_count") == len(family_cases) == len(selected_ids),
        "selected_case_count",
        "family manifest selected counts do not agree with the stored case list",
    )

    seen_ids: list[str] = []
    for case in family_cases:
        case_id = clean_text(case.get("case_id"))
        fixture_path_text = clean_text(case.get("fixture_path"))
        selection_reasons = case.get("selection_reasons") or []
        require(case_id in canonical_ids, "family_case_membership", f"{case_id} is not in the canonical parity slice")
        require(fixture_path_text != "", "fixture_path", f"{case_id} is missing fixture_path")
        require(
            repo_relative_path(fixture_path_text).is_file(),
            "fixture_exists",
            f"{case_id} fixture path does not exist: {fixture_path_text}",
        )
        require(
            isinstance(selection_reasons, list) and len(selection_reasons) > 0,
            "selection_reasons",
            f"{case_id} is missing selection_reasons",
        )
        seen_ids.append(case_id)

    require(
        seen_ids == selected_ids,
        "selected_case_ids",
        "family manifest case ordering does not match selected_case_ids",
    )
    return family_manifest


def validate_baseline_note(
    baseline_md_path: Path,
    parity_manifest: dict[str, Any],
    family_manifest: dict[str, Any],
) -> None:
    text = baseline_md_path.read_text(encoding="utf-8")
    text_lower = text.lower()
    for heading in REQUIRED_BASELINE_HEADINGS:
        require(heading in text, "baseline_heading", f"missing heading {heading}")

    require(
        "70-case tier3 APDR-failed and `pllm`-passing slice from March 27, 2026" in text,
        "baseline_canonical_slice_text",
        "baseline note does not state the canonical 70-case tier3 slice explicitly",
    )
    require(
        "17 overlap cases" in text and "outside the Phase 7 contract" in text,
        "baseline_tier1_watchlist_text",
        "baseline note does not state that the 17 overlap cases stay outside the Phase 7 contract",
    )
    require(
        "only the touched-family subset is protected for the first data-driven migration pass" in text_lower,
        "baseline_phase8_handoff_text",
        "baseline note does not state the Phase 8 touched-family protection boundary",
    )

    require(
        str(parity_manifest.get("canonical_case_count")) in text
        and str(family_manifest.get("selected_case_count")) in text
        and str(parity_manifest.get("tier1_watchlist_count")) in text,
        "baseline_counts",
        "baseline note does not reference the canonical, family-snapshot, and watchlist counts",
    )


def main() -> int:
    args = parse_args()
    summary_json = Path(args.summary_json).expanduser().resolve()
    pllm_csv = Path(args.pllm_csv).expanduser().resolve()
    parity_manifest_path = Path(args.parity_manifest).expanduser().resolve()
    family_manifest_path = Path(args.family_manifest).expanduser().resolve()
    baseline_md_path = Path(args.baseline_md).expanduser().resolve()

    try:
        parity_manifest = validate_parity_manifest(summary_json, pllm_csv, parity_manifest_path)
        family_manifest = validate_family_manifest(family_manifest_path, parity_manifest)
        validate_baseline_note(baseline_md_path, parity_manifest, family_manifest)
    except InvariantError as error:
        print(f"FAILED: {error.name}")
        print(error.detail)
        return 1

    print("Phase 7 baseline check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
