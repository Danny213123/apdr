#!/usr/bin/env python3
"""Check the Phase 25 docker-first milestone closeout contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED_VERDICT_SNIPPETS = (
    "pass delta",
    "docker_startup_duration_seconds",
    "fixed-slice",
    "Phase 23",
    "Recommendation",
)

REQUIRED_PROOF_SNIPPETS = (
    "What This Verdict Proves",
    "What This Verdict Does Not Prove",
    "Remaining Debt",
    "Recommendation Boundary",
    "fixed-slice",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check the Phase 25 docker-first milestone closeout contract."
    )
    parser.add_argument("--inputs-json", required=True, help="Path to 25-DECISION-INPUTS.json")
    parser.add_argument("--verdict-md", required=True, help="Path to 25-MILESTONE-VERDICT.md")
    parser.add_argument("--proof-md", help="Optional path to 25-CLOSEOUT-PROOF.md")
    parser.add_argument("--status-json", required=True, help="Path to write checker status")
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate the frozen closeout contract without requiring additional live evidence.",
    )
    return parser.parse_args()


def load_json_object(path_text: str, label: str) -> dict[str, Any]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"{label} not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON: {path} ({exc})") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return payload


def load_text(path_text: str, label: str) -> str:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"{label} not found: {path}")
    return path.read_text(encoding="utf-8")


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def write_status(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def extract_verdict(verdict_text: str) -> str:
    for line in verdict_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("verdict:"):
            return normalize_text(stripped.split(":", 1)[1])
    raise ValueError("Verdict document must start with a 'verdict:' metadata line")


def require_snippets(text: str, snippets: tuple[str, ...], label: str) -> None:
    missing = [snippet for snippet in snippets if snippet not in text]
    if missing:
        raise ValueError(f"{label} is missing required content: {', '.join(missing)}")


def validate_inputs(inputs: dict[str, Any]) -> None:
    if normalize_text(inputs.get("phase")) != "25":
        raise ValueError("Decision inputs must keep phase=25")
    allowed = inputs.get("allowed_verdicts")
    if allowed != ["replace", "optional", "reject"]:
        raise ValueError("Decision inputs must keep allowed_verdicts ['replace', 'optional', 'reject']")
    evidence_scope = inputs.get("evidence_scope")
    if not isinstance(evidence_scope, dict) or evidence_scope.get("fixed_slice_only") is not True:
        raise ValueError("Decision inputs must preserve fixed_slice_only=true")
    phase23 = inputs.get("phase23_human_uat")
    if not isinstance(phase23, dict):
        raise ValueError("Decision inputs must include phase23_human_uat")
    phase24 = inputs.get("phase24_sample_delta")
    if not isinstance(phase24, dict):
        raise ValueError("Decision inputs must include phase24_sample_delta")
    if phase24.get("pass_delta") != 2:
        raise ValueError("Decision inputs drifted from the frozen Phase 24 pass_delta=2 contract")
    timing = phase24.get("timing_deltas")
    if not isinstance(timing, dict) or timing.get("docker_startup_duration_seconds") != 61.0:
        raise ValueError("Decision inputs drifted from the frozen docker_startup_duration_seconds=61.0 contract")


def validate_verdict(inputs: dict[str, Any], verdict_text: str, verdict: str) -> None:
    allowed = inputs["allowed_verdicts"]
    if verdict not in allowed:
        raise ValueError(f"Verdict must be one of {allowed}, got {verdict!r}")
    require_snippets(verdict_text, REQUIRED_VERDICT_SNIPPETS, "Verdict document")

    live_paired = inputs.get("live_paired_replay")
    if not isinstance(live_paired, dict):
        raise ValueError("Decision inputs must include live_paired_replay")
    phase23 = inputs["phase23_human_uat"]
    pending = int(phase23.get("pending", 0))
    live_available = bool(live_paired.get("available"))

    if verdict == "replace" and pending > 0 and not live_available:
        raise ValueError(
            "Unsupported replace verdict: Phase 23 human verification is still pending and no stronger live paired replay evidence is frozen."
        )


def validate_proof(proof_text: str | None) -> None:
    if proof_text is None:
        return
    require_snippets(proof_text, REQUIRED_PROOF_SNIPPETS, "Closeout proof")


def main() -> int:
    args = parse_args()
    status: dict[str, Any] = {
        "phase": "25",
        "probe_only": bool(args.probe_only),
        "mode": "probe" if args.probe_only else "contract",
        "passed": False,
        "errors": [],
    }
    try:
        inputs = load_json_object(args.inputs_json, "Phase 25 decision inputs")
        verdict_text = load_text(args.verdict_md, "Phase 25 verdict document")
        proof_text = load_text(args.proof_md, "Phase 25 closeout proof") if args.proof_md else None

        validate_inputs(inputs)
        verdict = extract_verdict(verdict_text)
        validate_verdict(inputs, verdict_text, verdict)
        validate_proof(proof_text)

        status.update(
            {
                "passed": True,
                "verdict": verdict,
                "evidence_scope": inputs["evidence_scope"],
                "phase23_human_uat": inputs["phase23_human_uat"],
            }
        )
    except Exception as exc:  # noqa: BLE001 - deterministic CLI gate
        status["errors"].append(str(exc))

    write_status(args.status_json, status)
    return 0 if status["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
