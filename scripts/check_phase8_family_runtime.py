#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_HEADINGS = (
    "## Data Files",
    "## Touched Runtime Coverage",
    "## Diagnostics",
    "## Phase 9 Handoff",
)
REQUIRED_MAPPINGS = {
    "pkg_resources": "setuptools",
    "Image": "Pillow",
    "sklearn": "scikit-learn",
}
SURFACE_RULES = {
    "setuptools": {"families": {"setuptools"}, "rules": {"pkg-resources"}, "needles": ("pkg_resources",)},
    "pil": {"families": {"pil"}, "rules": {"legacy-pillow"}, "needles": ("pillow", "image", "pil")},
    "sklearn": {"families": {"sklearn"}, "rules": set(), "needles": ("sklearn", "scikit-learn")},
    "legacy-pymc3": {
        "families": {"legacy-pymc3"},
        "rules": {"legacy-pymc3"},
        "needles": ("pymc3", "theano-pymc", "lasagne", "xarray-einstats"),
    },
    "legacy-tensorflow": {
        "families": {"legacy-tensorflow"},
        "rules": {"legacy-tensorflow", "keras-backend"},
        "needles": ("tensorflow", "keras"),
    },
    "legacy-ggplot": {
        "families": {"legacy-ggplot"},
        "rules": {"legacy-ggplot"},
        "needles": ("ggplot",),
    },
}


class InvariantError(RuntimeError):
    def __init__(self, name: str, detail: str) -> None:
        super().__init__(f"{name}: {detail}")
        self.name = name
        self.detail = detail


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Phase 8 touched-family runtime coverage note and curated data."
    )
    parser.add_argument("--family-manifest", required=True, help="Path to the Phase 7 family snapshot manifest.")
    parser.add_argument("--families-json", required=True, help="Path to touched_families.json.")
    parser.add_argument("--recovery-json", required=True, help="Path to touched_recovery_rules.json.")
    parser.add_argument("--baseline-md", required=True, help="Path to 08-FAMILY-RUNTIME.md.")
    return parser.parse_args()


def require(condition: bool, name: str, detail: str) -> None:
    if not condition:
        raise InvariantError(name, detail)


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(payload, dict), "json_shape", f"{path} does not contain a JSON object")
    return payload


def clean_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"", "--", "none", "null"}:
        return ""
    return text


def repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def validate_manifest(path: Path) -> dict[str, Any]:
    manifest = load_json(path)
    cases = manifest.get("cases") or []
    selected_ids = manifest.get("selected_case_ids") or []
    count = manifest.get("selected_case_count")

    require(count == 17, "selected_case_count", f"expected 17, found {count!r}")
    require(
        isinstance(cases, list) and isinstance(selected_ids, list),
        "manifest_lists",
        "cases and selected_case_ids must both be lists",
    )
    require(
        len(cases) == len(selected_ids) == count,
        "manifest_count_alignment",
        "selected_case_count, cases, and selected_case_ids do not agree",
    )

    seen_ids: list[str] = []
    manifest_blob_parts: list[str] = []
    for case in cases:
        case_id = clean_text(case.get("case_id"))
        fixture_path = clean_text(case.get("fixture_path"))
        require(case_id != "", "case_id", "family manifest contains a case without case_id")
        require(fixture_path != "", "fixture_path", f"{case_id} is missing fixture_path")
        require(
            repo_path(fixture_path).is_file(),
            "fixture_exists",
            f"{case_id} fixture does not exist at {fixture_path}",
        )
        seen_ids.append(case_id)
        manifest_blob_parts.append(json.dumps(case, sort_keys=True))

    require(seen_ids == selected_ids, "selected_case_ids", "case ordering no longer matches selected_case_ids")
    manifest["__blob__"] = "\n".join(manifest_blob_parts).lower()
    return manifest


def validate_curated_data(families_path: Path, recovery_path: Path, manifest: dict[str, Any]) -> None:
    families_payload = load_json(families_path)
    recovery_payload = load_json(recovery_path)
    family_names = {
        clean_text(family.get("name"))
        for family in (families_payload.get("families") or [])
        if clean_text(family.get("name"))
    }
    rule_ids = {
        clean_text(rule.get("id"))
        for rule in (recovery_payload.get("rules") or [])
        if clean_text(rule.get("id"))
    }
    mappings = {
        clean_text(mapping.get("import_alias")): clean_text(mapping.get("package_name"))
        for mapping in (families_payload.get("explicit_namespace_mappings") or [])
        if clean_text(mapping.get("import_alias"))
    }
    manifest_blob = manifest["__blob__"]

    for alias, package_name in REQUIRED_MAPPINGS.items():
        require(
            mappings.get(alias) == package_name,
            "explicit_namespace_mapping",
            f"expected {alias!r} -> {package_name!r}, found {mappings.get(alias)!r}",
        )

    for surface_name, surface in SURFACE_RULES.items():
        require(
            any(needle in manifest_blob for needle in surface["needles"]),
            "manifest_surface",
            f"Phase 7 family manifest no longer shows the touched surface {surface_name!r}",
        )
        for family_name in surface["families"]:
            require(
                family_name in family_names,
                "curated_family",
                f"missing curated family {family_name!r} for surface {surface_name!r}",
            )
        for rule_id in surface["rules"]:
            require(
                rule_id in rule_ids,
                "curated_rule",
                f"missing curated recovery rule {rule_id!r} for surface {surface_name!r}",
            )


def validate_runtime_note(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    for heading in REQUIRED_HEADINGS:
        require(heading in text, "baseline_heading", f"missing heading {heading}")

    require(
        "17 snapshot cases" in text,
        "snapshot_boundary",
        "runtime note no longer states that Phase 8 is anchored to the 17 snapshot cases",
    )
    require(
        "70-case" in text and "17 overlap cases" in text,
        "phase7_boundary",
        "runtime note no longer preserves the Phase 7 canonical slice and overlap-watchlist boundary",
    )
    require(
        "Phase 9" in text and "do not reopen the migration boundary" in text,
        "phase9_handoff",
        "runtime note no longer states the Phase 9 handoff boundary",
    )


def main() -> int:
    args = parse_args()
    family_manifest_path = Path(args.family_manifest).expanduser().resolve()
    families_path = Path(args.families_json).expanduser().resolve()
    recovery_path = Path(args.recovery_json).expanduser().resolve()
    baseline_md_path = Path(args.baseline_md).expanduser().resolve()

    try:
        manifest = validate_manifest(family_manifest_path)
        validate_curated_data(families_path, recovery_path, manifest)
        validate_runtime_note(baseline_md_path)
    except InvariantError as error:
        print(f"FAILED: {error.name}")
        print(error.detail)
        return 1

    print("Phase 8 family runtime check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
