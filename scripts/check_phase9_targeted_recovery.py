#!/usr/bin/env python3
"""Deterministic checker for Phase 9 targeted-recovery policy coverage.

Validates that:
1. The bounded Phase 9 policy files (module_rules.json, compatibility_rules.json)
   still cover the chosen canonical module and compatibility clusters.
2. The reviewer note (09-TARGETED-RECOVERY.md) contains the exact required headings.
3. The reviewer note still references the existing Phase 8 runtime boundary.
4. The parity manifest is consistent with the policy files.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]

REQUIRED_PHASE9_HEADINGS = (
    "## Target Scope",
    "## Module Recovery Coverage",
    "## Compatibility Recovery Coverage",
    "## Diagnostics",
    "## Phase 10 Handoff",
)

# Canonical module clusters that the module_rules.json must cover.
REQUIRED_MODULE_RULE_IDS = {
    "mod-pkg-resources",
    "mod-pil-image",
    "mod-django-rest-framework",
}

# Canonical compatibility clusters that the compatibility_rules.json must cover.
REQUIRED_COMPATIBILITY_CLUSTER_IDS = {
    "compat-torch",
    "compat-tensorflow",
    "compat-scikit-learn",
}

# Companion rules that must be present.
REQUIRED_COMPANION_RULE_IDS = {
    "companion-pyjwt",
    "companion-python-dateutil",
}

# Phase 8 boundary strings that the Phase 9 note must reference.
PHASE8_BOUNDARY_NEEDLES = (
    "Phase 8",
    "family runtime",
    "curated",
)


class InvariantError(RuntimeError):
    def __init__(self, name: str, detail: str) -> None:
        super().__init__(f"{name}: {detail}")
        self.name = name
        self.detail = detail


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Phase 9 targeted-recovery policy coverage and reviewer note."
    )
    parser.add_argument(
        "--parity-manifest",
        required=True,
        help="Path to the Phase 7 tier3 parity manifest (07-tier3-parity-manifest.json).",
    )
    parser.add_argument(
        "--phase8-md",
        required=True,
        help="Path to 08-FAMILY-RUNTIME.md.",
    )
    parser.add_argument(
        "--phase9-md",
        required=True,
        help="Path to 09-TARGETED-RECOVERY.md.",
    )
    parser.add_argument(
        "--module-rules",
        required=True,
        help="Path to module_rules.json.",
    )
    parser.add_argument(
        "--compatibility-rules",
        required=True,
        help="Path to compatibility_rules.json.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def check_parity_manifest(manifest_path: Path) -> dict[str, Any]:
    """Validate the parity manifest exists and has the expected shape."""
    manifest = load_json(manifest_path)
    if "canonical_case_count" not in manifest:
        raise InvariantError(
            "parity-manifest-shape",
            "Missing 'canonical_case_count' in parity manifest.",
        )
    if "canonical_case_ids" not in manifest:
        raise InvariantError(
            "parity-manifest-shape",
            "Missing 'canonical_case_ids' in parity manifest.",
        )
    return manifest


def check_module_rules(module_rules_path: Path, manifest: dict[str, Any]) -> None:
    """Validate the module rules cover required clusters."""
    rules_data = load_json(module_rules_path)

    # Check required module provider rule IDs
    provider_rule_ids = {
        rule["id"] for rule in rules_data.get("module_provider_rules", [])
    }
    missing_provider = REQUIRED_MODULE_RULE_IDS - provider_rule_ids
    if missing_provider:
        raise InvariantError(
            "module-rules-coverage",
            f"Missing required module provider rule IDs: {sorted(missing_provider)}",
        )

    # Check stop reason rules exist
    stop_rules = rules_data.get("stop_reason_rules", [])
    if not stop_rules:
        raise InvariantError(
            "module-rules-stop-reasons",
            "No stop_reason_rules found in module_rules.json.",
        )

    # Validate all referenced case IDs are in the canonical set
    canonical_ids = set(manifest.get("canonical_case_ids", []))
    watchlist_ids = set(manifest.get("tier1_watchlist_case_ids", []))
    all_valid_ids = canonical_ids | watchlist_ids

    if all_valid_ids:
        for rule in rules_data.get("module_provider_rules", []):
            for case_id in rule.get("canonical_case_ids", []):
                if case_id not in all_valid_ids:
                    raise InvariantError(
                        "module-rules-case-id",
                        f"Rule '{rule['id']}' references unknown case ID '{case_id}'.",
                    )


def check_compatibility_rules(
    compat_rules_path: Path, manifest: dict[str, Any]
) -> None:
    """Validate the compatibility rules cover required clusters."""
    rules_data = load_json(compat_rules_path)

    # Check required compatibility cluster IDs
    cluster_ids = {
        cluster["id"] for cluster in rules_data.get("compatibility_clusters", [])
    }
    missing_clusters = REQUIRED_COMPATIBILITY_CLUSTER_IDS - cluster_ids
    if missing_clusters:
        raise InvariantError(
            "compat-rules-coverage",
            f"Missing required compatibility cluster IDs: {sorted(missing_clusters)}",
        )

    # Check required companion rule IDs
    companion_ids = {
        rule["id"] for rule in rules_data.get("companion_rules", [])
    }
    missing_companions = REQUIRED_COMPANION_RULE_IDS - companion_ids
    if missing_companions:
        raise InvariantError(
            "compat-rules-companions",
            f"Missing required companion rule IDs: {sorted(missing_companions)}",
        )

    # Check that the tensorflow cluster references the Phase 8 family runtime
    for cluster in rules_data.get("compatibility_clusters", []):
        if cluster["id"] == "compat-tensorflow":
            family_ref = cluster.get("family_ref")
            if not family_ref:
                raise InvariantError(
                    "compat-tensorflow-family-ref",
                    "compat-tensorflow cluster must have a family_ref pointing to Phase 8 curated family.",
                )
            break

    # Check python ceiling rules exist
    ceiling_rules = rules_data.get("python_ceiling_rules", [])
    if not ceiling_rules:
        raise InvariantError(
            "compat-rules-ceilings",
            "No python_ceiling_rules found in compatibility_rules.json.",
        )

    # Validate case IDs
    canonical_ids = set(manifest.get("canonical_case_ids", []))
    watchlist_ids = set(manifest.get("tier1_watchlist_case_ids", []))
    all_valid_ids = canonical_ids | watchlist_ids

    if all_valid_ids:
        for cluster in rules_data.get("compatibility_clusters", []):
            for case_id in cluster.get("canonical_case_ids", []):
                if case_id not in all_valid_ids:
                    raise InvariantError(
                        "compat-rules-case-id",
                        f"Cluster '{cluster['id']}' references unknown case ID '{case_id}'.",
                    )


def check_phase9_note(phase9_md_path: Path) -> str:
    """Validate the Phase 9 reviewer note has required headings."""
    text = phase9_md_path.read_text(encoding="utf-8")

    for heading in REQUIRED_PHASE9_HEADINGS:
        if heading not in text:
            raise InvariantError(
                "phase9-note-heading",
                f"Missing required heading '{heading}' in {phase9_md_path.name}.",
            )

    # Check Phase 8 boundary references
    for needle in PHASE8_BOUNDARY_NEEDLES:
        if needle.lower() not in text.lower():
            raise InvariantError(
                "phase9-note-phase8-ref",
                f"Phase 9 note must reference '{needle}' to preserve the Phase 8 boundary.",
            )

    return text


def check_phase8_boundary(phase8_md_path: Path) -> None:
    """Validate the Phase 8 runtime note still exists and has expected handoff."""
    text = phase8_md_path.read_text(encoding="utf-8")
    if "## Phase 9 Handoff" not in text:
        raise InvariantError(
            "phase8-handoff",
            "Phase 8 note must contain '## Phase 9 Handoff' heading.",
        )


def main() -> int:
    args = parse_args()
    errors: list[InvariantError] = []

    # Validate all inputs exist
    for label, path_str in [
        ("parity-manifest", args.parity_manifest),
        ("phase8-md", args.phase8_md),
        ("phase9-md", args.phase9_md),
        ("module-rules", args.module_rules),
        ("compatibility-rules", args.compatibility_rules),
    ]:
        if not Path(path_str).exists():
            errors.append(InvariantError(f"file-exists-{label}", f"File not found: {path_str}"))

    if errors:
        for e in errors:
            print(f"FAIL: {e}", file=sys.stderr)
        return 1

    manifest_path = Path(args.parity_manifest)
    phase8_md_path = Path(args.phase8_md)
    phase9_md_path = Path(args.phase9_md)
    module_rules_path = Path(args.module_rules)
    compat_rules_path = Path(args.compatibility_rules)

    checks = [
        ("parity manifest", lambda: check_parity_manifest(manifest_path)),
        ("module rules", lambda: check_module_rules(module_rules_path, check_parity_manifest(manifest_path))),
        ("compatibility rules", lambda: check_compatibility_rules(compat_rules_path, check_parity_manifest(manifest_path))),
        ("Phase 9 note", lambda: check_phase9_note(phase9_md_path)),
        ("Phase 8 boundary", lambda: check_phase8_boundary(phase8_md_path)),
    ]

    passed = 0
    for name, check_fn in checks:
        try:
            check_fn()
            print(f"  PASS: {name}")
            passed += 1
        except InvariantError as e:
            errors.append(e)
            print(f"  FAIL: {name} -- {e.detail}", file=sys.stderr)
        except Exception as e:
            errors.append(InvariantError(name, str(e)))
            print(f"  FAIL: {name} -- {e}", file=sys.stderr)

    total = len(checks)
    print(f"\nPhase 9 targeted recovery check: {passed}/{total} passed.")

    if errors:
        print(f"\n{len(errors)} invariant(s) broken:", file=sys.stderr)
        for e in errors:
            print(f"  - {e.name}: {e.detail}", file=sys.stderr)
        return 1

    print("All Phase 9 invariants hold.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
