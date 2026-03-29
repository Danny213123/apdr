#!/usr/bin/env python3
"""Phase 15 tier3 replay benchmark harness.

This script establishes a deterministic artifact contract for replay-slice
quality comparisons across agent modes, retrieval strategies, and small-model
policies. Phase 15 starts with probe-only validation of artifact shape; later
plans can extend the same entrypoint with live APDR execution.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import shlex
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmark_ui.run_contract import build_run_contract


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate Phase 15 replay-slice benchmark artifacts for tier3 agent quality. "
            "Probe mode validates artifact shape without requiring a live model."
        )
    )
    parser.add_argument("--manifest-json", required=True, help="Path to the locked replay manifest JSON.")
    parser.add_argument("--fixtures-root", default="", help="Root of deterministic fixture snippets.")
    parser.add_argument("--dataset-root", default="", help="Root of dataset snippets for later live runs.")
    parser.add_argument("--output-json", required=True, help="Path for the benchmark JSON artifact.")
    parser.add_argument("--output-md", default="", help="Optional Markdown summary output.")
    parser.add_argument("--mode", choices=("baseline", "candidate"), required=True, help="Benchmark mode.")
    parser.add_argument(
        "--validation-backend",
        choices=("env", "docker", "llm"),
        default="env",
        help="Validation backend label used in the run contract.",
    )
    parser.add_argument("--build-profile", default="release", help="Build profile label for the run contract.")
    parser.add_argument("--model-name", default="qwen3.5:9b", help="Model name recorded in the artifact.")
    parser.add_argument("--base-url", default="http://localhost:11434", help="Base URL recorded in the artifact.")
    parser.add_argument("--agent-mode", default="manual", help="Tier3 agent mode label.")
    parser.add_argument("--tool-profile", default="full", help="Tool-surface label for the benchmark artifact.")
    parser.add_argument(
        "--retrieval-profile",
        default="none",
        help="Retrieval or memory profile label for the benchmark artifact.",
    )
    parser.add_argument(
        "--thinking-mode",
        default="inherited",
        help="Thinking-mode label, e.g. inherited, off, on, or routed.",
    )
    parser.add_argument(
        "--policy-label",
        default="",
        help="Explicit policy label. Defaults to '<mode>-<agent_mode>'.",
    )
    parser.add_argument(
        "--llm-context-window",
        default="",
        help="Context-window setting recorded in the run contract.",
    )
    parser.add_argument(
        "--inference-policy",
        default="",
        help="Inference-policy label recorded in the run contract.",
    )
    parser.add_argument(
        "--cache-state",
        default="",
        help="Cache-state label. Defaults to cold for baseline and warm for candidate.",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Validate artifact wiring and emit placeholder per-case statuses without live execution.",
    )
    parser.add_argument(
        "--execute-live",
        action="store_true",
        help="Reserved for later Phase 15 live APDR execution support.",
    )
    args = parser.parse_args()
    if args.execute_live and args.probe_only:
        parser.error("Use either --probe-only or --execute-live, not both.")
    if not args.probe_only and not args.execute_live:
        parser.error("Phase 15 currently requires --probe-only or --execute-live.")
    return args


def shell_join(parts: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(parts)  # type: ignore[name-defined]
    return shlex.join(parts)


def load_manifest(path_text: str) -> tuple[Path, dict[str, Any]]:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise SystemExit(f"Manifest JSON not found: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid manifest JSON at {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit(f"Manifest must be a JSON object: {path}")
    cases = data.get("cases")
    if not data.get("slice_id") or not isinstance(cases, list) or not cases:
        raise SystemExit(f"Manifest must include slice_id and a non-empty cases list: {path}")
    return path, data


def resolve_snippet_roots(args: argparse.Namespace) -> list[Path]:
    roots: list[Path] = []
    for raw in (args.fixtures_root, args.dataset_root):
        text = str(raw or "").strip()
        if not text:
            continue
        root = Path(text).expanduser().resolve()
        if not root.exists():
            raise SystemExit(f"Snippet root does not exist: {root}")
        roots.append(root)
    if not roots:
        raise SystemExit("Provide at least one of --fixtures-root or --dataset-root.")
    return roots


def resolve_case_path(relative_path: str, roots: list[Path]) -> Path:
    for root in roots:
        candidate = root / relative_path
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()
    raise SystemExit(
        f"Manifest case not found in provided roots: {relative_path}. "
        f"Checked: {', '.join(str(root) for root in roots)}"
    )


def extract_imports(snippet_path: Path) -> list[str]:
    try:
        tree = ast.parse(snippet_path.read_text(encoding="utf-8"))
    except (SyntaxError, OSError, UnicodeDecodeError):
        return []
    seen: set[str] = set()
    ordered: list[str] = []
    for node in ast.walk(tree):
        module_name = ""
        if isinstance(node, ast.Import):
            for alias in node.names:
                module_name = alias.name.split(".", 1)[0].strip()
                if module_name and module_name not in seen:
                    seen.add(module_name)
                    ordered.append(module_name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            module_name = node.module.split(".", 1)[0].strip()
            if module_name and module_name not in seen:
                seen.add(module_name)
                ordered.append(module_name)
    return ordered


def default_cache_state(mode: str, cache_state: str) -> str:
    text = str(cache_state or "").strip()
    if text:
        return text
    return "cold" if mode == "baseline" else "warm"


def default_policy_label(mode: str, agent_mode: str, policy_label: str) -> str:
    text = str(policy_label or "").strip()
    if text:
        return text
    normalized_agent = str(agent_mode or "manual").strip() or "manual"
    return f"{mode}-{normalized_agent}"


def make_probe_samples(
    manifest: dict[str, Any],
    roots: list[Path],
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for entry in manifest["cases"]:
        relative_path = str(entry.get("relative_path") or "").strip()
        reason = str(entry.get("reason") or "").strip()
        snippet_path = resolve_case_path(relative_path, roots)
        samples.append(
            {
                "relative_path": relative_path,
                "snippet_path": str(snippet_path),
                "imports": extract_imports(snippet_path),
                "tier3_status": "probe-placeholder",
                "status_reason": "Probe-only artifact; no live APDR or model invocation executed.",
                "manifest_reason": reason,
                "agent_mode": args.agent_mode,
                "tool_profile": args.tool_profile,
                "retrieval_profile": args.retrieval_profile,
                "thinking_mode": args.thinking_mode,
                "policy_label": default_policy_label(args.mode, args.agent_mode, args.policy_label),
            }
        )
    return samples


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Phase 15 Tier3 Benchmark",
        "",
        f"- Mode: `{payload['mode']}`",
        f"- Slice: `{payload['slice_id']}`",
        f"- Agent mode: `{payload['agent_mode']}`",
        f"- Retrieval profile: `{payload['retrieval_profile']}`",
        f"- Tool profile: `{payload['tool_profile']}`",
        f"- Thinking mode: `{payload['thinking_mode']}`",
        f"- Policy label: `{payload['policy_label']}`",
        f"- Context window: `{payload['llm_context_window']}`",
        f"- Inference policy: `{payload['inference_policy']}`",
        f"- Probe only: `{payload['probe_only']}`",
        "",
        "## Status Counts",
        "",
    ]
    counts = payload.get("tier3_status_counts", {})
    if isinstance(counts, dict):
        for key, value in sorted(counts.items()):
            lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Samples", ""])
    for sample in payload.get("samples", [])[:10]:
        imports = ", ".join(sample.get("imports", [])) or "none"
        lines.append(
            f"- `{sample.get('relative_path', '')}` — `{sample.get('tier3_status', '')}` "
            f"(imports: {imports})"
        )
    return "\n".join(lines) + "\n"


def build_probe_payload(args: argparse.Namespace, manifest_path: Path, manifest: dict[str, Any], roots: list[Path]) -> dict[str, Any]:
    run_config = {
        "run_intent": "tier3-benchmark",
        "cache_state": default_cache_state(args.mode, args.cache_state),
        "llm_context_window": args.llm_context_window,
        "inference_policy": args.inference_policy,
        "build_profile": args.build_profile,
    }
    run_contract = build_run_contract(
        repo_root=REPO_ROOT,
        tool="apdr",
        model_name=args.model_name,
        base_url=args.base_url,
        temperature=None,
        validation_backend=args.validation_backend,
        run_config=run_config,
        runner_command=[sys.executable],
    )
    samples = make_probe_samples(manifest, roots, args)
    counts = Counter(sample["tier3_status"] for sample in samples)
    payload: dict[str, Any] = {
        "phase": "15",
        "mode": args.mode,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "command": shell_join([sys.executable, *sys.argv]),
        "repo_root": str(REPO_ROOT),
        "manifest_json": str(manifest_path),
        "slice_id": str(manifest.get("slice_id")),
        "probe_only": True,
        "execute_live": False,
        "agent_mode": args.agent_mode,
        "tool_profile": args.tool_profile,
        "retrieval_profile": args.retrieval_profile,
        "thinking_mode": args.thinking_mode,
        "policy_label": default_policy_label(args.mode, args.agent_mode, args.policy_label),
        "sample_count": len(samples),
        "resolved": counts.get("resolved", 0),
        "abstained": counts.get("abstained", 0),
        "failed": counts.get("failed", 0),
        "skipped": counts.get("skipped", 0),
        "success_rate": 0.0,
        "success_rate_percent": 0.0,
        "tier3_status_counts": dict(sorted(counts.items())),
        "samples": samples,
        "notes": [
            "Probe-only artifact validates benchmark schema without live APDR execution.",
        ],
    }
    payload.update(run_contract)
    return payload


def main() -> int:
    args = parse_args()
    manifest_path, manifest = load_manifest(args.manifest_json)
    roots = resolve_snippet_roots(args)

    if args.execute_live:
        raise SystemExit(
            "Phase 15 live APDR execution is not implemented in 15-01. "
            "Use --probe-only for the initial benchmark contract."
        )

    payload = build_probe_payload(args, manifest_path, manifest, roots)

    output_json = Path(args.output_json).expanduser().resolve()
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    if args.output_md:
        output_md = Path(args.output_md).expanduser().resolve()
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(render_markdown(payload), encoding="utf-8")

    print(f"Wrote Phase 15 benchmark artifact to {output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
