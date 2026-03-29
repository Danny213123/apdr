#!/usr/bin/env python3
"""Validate Phase 16 closeout evidence mode and proof-note alignment."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class ArtifactRecord:
    label: str
    path_text: str
    path: Path
    exists: bool
    mode: str
    expected_live_path: str
    payload: dict[str, Any] | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Phase 16 milestone closeout evidence and proof-note contract."
    )
    parser.add_argument("--phase14-macos-before", required=True, help="Path to the Phase 14 macOS baseline artifact.")
    parser.add_argument("--phase14-macos-after", required=True, help="Path to the Phase 14 macOS candidate artifact.")
    parser.add_argument("--phase14-windows-before", required=True, help="Path to the Phase 14 Windows baseline artifact.")
    parser.add_argument("--phase14-windows-after", required=True, help="Path to the Phase 14 Windows candidate artifact.")
    parser.add_argument("--phase15-baseline", required=True, help="Path to the Phase 15 baseline artifact.")
    parser.add_argument("--phase15-candidate", required=True, help="Path to the Phase 15 candidate artifact.")
    parser.add_argument("--status-json", default="", help="Optional path to write the closeout evidence status JSON.")
    parser.add_argument("--evidence-md", default="", help="Optional Phase 16 evidence inventory note to validate.")
    parser.add_argument("--macos-md", default="", help="Optional Phase 16 macOS comparison note to validate.")
    parser.add_argument("--windows-md", default="", help="Optional Phase 16 Windows non-regression note to validate.")
    parser.add_argument("--llm-md", default="", help="Optional Phase 16 LLM-quality delta note to validate.")
    parser.add_argument("--closeout-md", default="", help="Optional Phase 16 milestone closeout note to validate.")
    return parser.parse_args()


def resolve_path(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"Artifact must be a JSON object: {path}")
    return payload


def is_sample_artifact(path: Path, payload: dict[str, Any] | None) -> bool:
    lowered = path.name.lower()
    if "-sample." in lowered or lowered.endswith("sample.json"):
        return True
    if payload is None:
        return False
    notes = payload.get("notes")
    if isinstance(notes, list) and any("sample" in str(note).lower() for note in notes):
        return True
    output_json = str(payload.get("output_json", "")).lower()
    return "-sample." in output_json or output_json.endswith("sample.json")


def expected_live_path(path: Path) -> str:
    name = path.name
    if "-sample" in name:
        return str(path.with_name(name.replace("-sample", "", 1)))
    return str(path)


def artifact_record(label: str, path_text: str) -> ArtifactRecord:
    path = resolve_path(path_text)
    payload = load_json_if_exists(path)
    exists = payload is not None
    if not exists:
        mode = "missing"
    elif is_sample_artifact(path, payload):
        mode = "sample"
    else:
        mode = "live"
    return ArtifactRecord(
        label=label,
        path_text=path_text,
        path=path,
        exists=exists,
        mode=mode,
        expected_live_path=expected_live_path(path),
        payload=payload,
    )


def classify_group(records: Iterable[ArtifactRecord]) -> str:
    modes = {record.mode for record in records}
    if modes == {"missing"}:
        return "missing"
    if len(modes) == 1:
        return next(iter(modes))
    return "mixed"


def read_text(path_text: str) -> str:
    path = resolve_path(path_text)
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"Unable to read {path}: {exc}") from exc


def require_doc_sections(
    label: str,
    path_text: str,
    required_sections: Iterable[str],
    required_terms: Iterable[str],
    failures: list[str],
) -> None:
    if not path_text:
        return
    path = resolve_path(path_text)
    if not path.exists():
        failures.append(f"{label} is missing: {path}")
        return
    contents = read_text(path_text)
    for section in required_sections:
        if section not in contents:
            failures.append(f"{label} missing required section: {section}")
    for term in required_terms:
        if term and term not in contents:
            failures.append(f"{label} missing required term: {term}")


def record_to_json(record: ArtifactRecord) -> dict[str, Any]:
    payload = record.payload or {}
    return {
        "path": record.path_text,
        "resolved_path": str(record.path),
        "exists": record.exists,
        "mode": record.mode,
        "expected_live_path": record.expected_live_path,
        "slice_id": payload.get("slice_id", ""),
        "build_profile": payload.get("build_profile", ""),
        "validation_backend": payload.get("validation_backend", ""),
        "created_at": payload.get("created_at", ""),
    }


def compare_keys(
    label: str,
    left: ArtifactRecord,
    right: ArtifactRecord,
    keys: Iterable[str],
    failures: list[str],
) -> None:
    if not left.exists or not right.exists:
        return
    assert left.payload is not None
    assert right.payload is not None
    for key in keys:
        if left.payload.get(key) != right.payload.get(key):
            failures.append(
                f"{label} artifacts must match on {key}: "
                f"{left.payload.get(key)!r} != {right.payload.get(key)!r}"
            )


def missing_live_paths(records: Iterable[ArtifactRecord]) -> list[str]:
    missing: list[str] = []
    for record in records:
        if record.mode != "live":
            missing.append(record.expected_live_path)
    return sorted(set(missing))


def choose_terminal_state(evidence_mode: str, failures: list[str]) -> str:
    if failures:
        return "contract-invalid"
    if evidence_mode == "live":
        return "ready-for-live-signoff"
    if evidence_mode == "sample":
        return "sample-contract-only"
    if evidence_mode == "missing":
        return "missing-evidence"
    return "mixed-evidence"


def write_status_json(path_text: str, payload: dict[str, Any]) -> None:
    if not path_text:
        return
    path = resolve_path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()

    macos_before = artifact_record("phase14_macos_before", args.phase14_macos_before)
    macos_after = artifact_record("phase14_macos_after", args.phase14_macos_after)
    windows_before = artifact_record("phase14_windows_before", args.phase14_windows_before)
    windows_after = artifact_record("phase14_windows_after", args.phase14_windows_after)
    phase15_baseline = artifact_record("phase15_baseline", args.phase15_baseline)
    phase15_candidate = artifact_record("phase15_candidate", args.phase15_candidate)

    phase14_macos_mode = classify_group((macos_before, macos_after))
    phase14_windows_mode = classify_group((windows_before, windows_after))
    phase14_mode = classify_group(
        (
            ArtifactRecord("phase14_macos", "", Path("."), True, phase14_macos_mode, "", None),
            ArtifactRecord("phase14_windows", "", Path("."), True, phase14_windows_mode, "", None),
        )
    )
    phase15_mode = classify_group((phase15_baseline, phase15_candidate))
    evidence_mode = classify_group(
        (
            ArtifactRecord("phase14", "", Path("."), True, phase14_mode, "", None),
            ArtifactRecord("phase15", "", Path("."), True, phase15_mode, "", None),
        )
    )

    failures: list[str] = []

    if phase14_macos_mode in {"missing", "mixed"}:
        failures.append(f"Phase 14 macOS artifact pair is {phase14_macos_mode}.")
    if phase14_windows_mode in {"missing", "mixed"}:
        failures.append(f"Phase 14 Windows artifact pair is {phase14_windows_mode}.")
    if phase15_mode in {"missing", "mixed"}:
        failures.append(f"Phase 15 artifact pair is {phase15_mode}.")

    compare_keys(
        "Phase 14 macOS",
        macos_before,
        macos_after,
        ("slice_id", "execution_mode", "cache_state", "build_profile", "validation_backend"),
        failures,
    )
    compare_keys(
        "Phase 14 Windows",
        windows_before,
        windows_after,
        ("slice_id", "execution_mode", "cache_state", "build_profile", "validation_backend"),
        failures,
    )
    compare_keys(
        "Phase 15",
        phase15_baseline,
        phase15_candidate,
        ("slice_id", "sample_count", "validation_backend", "build_profile"),
        failures,
    )

    missing_live = missing_live_paths(
        (
            macos_before,
            macos_after,
            windows_before,
            windows_after,
            phase15_baseline,
            phase15_candidate,
        )
    )
    blocker_reason = ""
    if evidence_mode != "live":
        blocker_reason = "Live macOS, Windows, and Phase 15 tier3 artifacts are not all present yet."

    status_payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evidence_mode": evidence_mode,
        "contract_ready": not failures,
        "live_signoff_ready": evidence_mode == "live" and not failures,
        "terminal_state": choose_terminal_state(evidence_mode, failures),
        "blocker_reason": blocker_reason,
        "blockers": failures.copy(),
        "missing_live_artifacts": missing_live,
        "phase14": {
            "mode": phase14_mode,
            "macos": {
                "mode": phase14_macos_mode,
                "before": record_to_json(macos_before),
                "after": record_to_json(macos_after),
            },
            "windows": {
                "mode": phase14_windows_mode,
                "before": record_to_json(windows_before),
                "after": record_to_json(windows_after),
            },
        },
        "phase15": {
            "mode": phase15_mode,
            "baseline": record_to_json(phase15_baseline),
            "candidate": record_to_json(phase15_candidate),
        },
    }

    write_status_json(args.status_json, status_payload)

    evidence_mode_term = evidence_mode
    require_doc_sections(
        "Phase 16 evidence note",
        args.evidence_md,
        ["## Artifact Inputs", "## Evidence Modes", "## Missing Live Artifacts", "## Command Contract"],
        ["14-macos-before", "15-tier3-baseline", evidence_mode_term],
        failures,
    )
    require_doc_sections(
        "Phase 16 macOS comparison note",
        args.macos_md,
        ["## macOS Performance", "## Evidence Mode", "## Artifact Links", "## Reviewer Verdict"],
        ["14-MACOS-REPLAY.md", evidence_mode_term],
        failures,
    )
    require_doc_sections(
        "Phase 16 Windows comparison note",
        args.windows_md,
        ["## Windows Guardrail", "## Evidence Mode", "## Artifact Links", "## Reviewer Verdict"],
        ["14-WINDOWS-GUARDRAIL.md", evidence_mode_term],
        failures,
    )
    require_doc_sections(
        "Phase 16 LLM-quality note",
        args.llm_md,
        ["## LLM Quality", "## Evidence Mode", "## Policy Attribution", "## Reviewer Verdict"],
        ["15-AGENT-QUALITY.md", "15-QWEN-POLICY-MATRIX.md", evidence_mode_term],
        failures,
    )
    require_doc_sections(
        "Phase 16 milestone closeout note",
        args.closeout_md,
        ["## Evidence Mode", "## macOS Performance", "## Windows Guardrail", "## LLM Quality", "## Requirement Verdicts", "## Final Signoff"],
        ["EVD-04", "EVD-06", evidence_mode_term],
        failures,
    )

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    print(f"PASS: Phase 16 closeout evidence contract is valid in {evidence_mode!r} mode.")
    if blocker_reason:
        print(f"NOTE: {blocker_reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
