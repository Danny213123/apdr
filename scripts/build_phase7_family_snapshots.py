#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_SECTIONS = {
    "resolved_dependencies",
    "config_dependencies",
    "unresolved",
    "notes",
    "validation_attempts",
}
EXPLICIT_NAMESPACE_MAPPINGS = {
    "pkg_resources": "setuptools",
    "pil": "pillow",
    "image": "pillow",
    "imagedraw": "pillow",
    "imagefont": "pillow",
    "imageenhance": "pillow",
    "imagegrab": "pillow",
    "cv2": "opencv-python",
    "rest_framework": "djangorestframework",
    "sklearn": "scikit-learn",
    "bs4": "beautifulsoup4",
}
FAMILY_BUNDLE_ANCHORS = (
    "pymc3",
    "theano",
    "theano-pymc",
    "lasagne",
    "arviz",
    "xarray-einstats",
    "flask_security",
    "flask_principal",
    "flask_admin",
    "flask_sqlalchemy",
    "johnny",
    "johnny-cache",
    "scrapy",
    "cfscrape",
    "ggplot",
    "simplecv",
    "tensorflow",
    "keras",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the Phase 7 touched-family snapshot manifest and fixture corpus."
    )
    parser.add_argument("--parity-manifest", required=True, help="Path to the canonical Phase 7 manifest.")
    parser.add_argument("--cases-root", required=True, help="Path to the benchmark case root.")
    parser.add_argument("--fixtures-root", required=True, help="Path to the isolated Phase 7 fixture root.")
    parser.add_argument("--output-json", required=True, help="Destination for the family snapshot manifest.")
    parser.add_argument("--output-md", required=True, help="Destination for the Markdown summary.")
    return parser.parse_args()


def clean_text(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower() in {"", "--", "none", "null"}:
        return ""
    return text


def normalize_token(value: Any) -> str:
    text = clean_text(value).strip("'\"")
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def repo_relative_text(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def load_parity_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise ValueError(f"{path} is missing a top-level 'cases' array")
    return payload


def parse_resolution_report(path: Path) -> tuple[dict[str, str], dict[str, list[str]]]:
    metadata: dict[str, str] = {}
    sections = {name: [] for name in REPORT_SECTIONS}
    if not path.exists():
        return metadata, sections

    current_section = ""
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.endswith(":") and stripped[:-1] in REPORT_SECTIONS:
            current_section = stripped[:-1]
            continue
        if current_section:
            if stripped.startswith("- "):
                sections[current_section].append(stripped[2:])
            else:
                sections[current_section].append(stripped)
            continue
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        metadata[key.strip()] = value.strip()
    return metadata, sections


def requirement_name(requirement: str) -> str:
    raw = clean_text(requirement).strip("'\"")
    if not raw:
        return ""
    return re.split(r"[<>=!~;\[\s]", raw, maxsplit=1)[0].strip()


def build_source_tokens(case: dict[str, Any], report_metadata: dict[str, str]) -> list[tuple[str, str, str]]:
    tokens: list[tuple[str, str, str]] = []
    for requirement in case.get("requirements") or []:
        name = requirement_name(str(requirement))
        norm = normalize_token(name)
        if norm:
            tokens.append(("requirement", str(requirement), norm))

    for field in ("missing_module", "failing_package"):
        raw = clean_text(report_metadata.get(field))
        name = requirement_name(raw)
        norm = normalize_token(name or raw)
        if norm:
            tokens.append((field, raw, norm))

    return tokens


def family_strategy_reasons(resolved_lines: list[str]) -> list[str]:
    reasons: list[str] = []
    seen: set[str] = set()
    for line in resolved_lines:
        for match in re.findall(r"family:[^\]\s|]+", line, flags=re.IGNORECASE):
            reason = f"resolved_dependencies uses `{match}`"
            if reason not in seen:
                reasons.append(reason)
                seen.add(reason)
    return reasons


def family_note_reasons(notes: list[str]) -> list[str]:
    reasons: list[str] = []
    for note in notes:
        if "family knowledge" in note.lower():
            reasons.append(f"notes mention Family knowledge: {note}")
    return dedupe_preserve_order(reasons)


def explicit_mapping_reasons(source_tokens: list[tuple[str, str, str]]) -> list[str]:
    reasons: list[str] = []
    for source_kind, raw_value, normalized in source_tokens:
        for import_alias, package_name in EXPLICIT_NAMESPACE_MAPPINGS.items():
            if normalized != normalize_token(import_alias):
                continue
            reasons.append(
                f"{source_kind} `{raw_value}` matches explicit namespace mapping `{import_alias}` -> `{package_name}`"
            )
    return dedupe_preserve_order(reasons)


def bundle_anchor_reasons(source_tokens: list[tuple[str, str, str]]) -> list[str]:
    reasons: list[str] = []
    for source_kind, raw_value, normalized in source_tokens:
        for anchor in FAMILY_BUNDLE_ANCHORS:
            if normalized != normalize_token(anchor):
                continue
            reasons.append(f"{source_kind} `{raw_value}` matches family bundle anchor `{anchor}`")
    return dedupe_preserve_order(reasons)


def dedupe_preserve_order(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        ordered.append(value)
        seen.add(value)
    return ordered


def resolve_repo_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def is_readable_file(path: Path) -> bool:
    try:
        return path.is_file()
    except OSError:
        return False


def resolve_snippet_source(case: dict[str, Any], report_path: Path) -> Path:
    primary = resolve_repo_path(clean_text(case.get("snippet")))
    if is_readable_file(primary):
        return primary

    attempts_root = report_path.parent / ".apdr-debug" / "attempts"
    fallback_candidates = sorted(attempts_root.glob("attempt-*/snippet.py"))
    for candidate in fallback_candidates:
        if is_readable_file(candidate):
            return candidate

    raise FileNotFoundError(
        f"no readable snippet source found for {clean_text(case.get('case_id'))}: {primary}"
    )


def copy_fixture_snippet(source_path: Path, fixture_root: Path, case_id: str) -> Path:
    case_root = fixture_root / case_id
    case_root.mkdir(parents=True, exist_ok=True)
    destination = case_root / "snippet.py"
    destination.write_bytes(source_path.read_bytes())
    return destination


def build_case_entry(
    case: dict[str, Any],
    fixtures_root: Path,
) -> dict[str, Any] | None:
    case_id = clean_text(case.get("case_id"))
    if not case_id:
        raise ValueError("manifest case is missing case_id")

    report_path = resolve_repo_path(clean_text(case.get("report_path")))
    report_metadata, report_sections = parse_resolution_report(report_path)
    source_tokens = build_source_tokens(case, report_metadata)

    reasons = []
    reasons.extend(family_strategy_reasons(report_sections["resolved_dependencies"]))
    reasons.extend(family_note_reasons(report_sections["notes"]))
    reasons.extend(explicit_mapping_reasons(source_tokens))
    reasons.extend(bundle_anchor_reasons(source_tokens))
    reasons = dedupe_preserve_order(reasons)
    if not reasons:
        return None

    snippet_source = resolve_snippet_source(case, report_path)
    fixture_path = copy_fixture_snippet(snippet_source, fixtures_root, case_id)

    return {
        "case_id": case_id,
        "normalized_bucket": clean_text(case.get("normalized_bucket")),
        "pllm_pass_count": case.get("pllm_pass_count"),
        "requirements": case.get("requirements") or [],
        "report_path": repo_relative_text(report_path),
        "snippet": clean_text(case.get("snippet")),
        "fixture_path": repo_relative_text(fixture_path),
        "selection_reasons": reasons,
    }


def write_readme(fixtures_root: Path, parity_manifest_path: Path) -> None:
    readme_path = fixtures_root / "README.md"
    readme_path.write_text(
        "\n".join(
            [
                "# Phase 7 benchmark-derived family snapshots",
                "",
                "- These fixtures are benchmark-derived.",
                f"- They were selected from the canonical Phase 7 manifest at `{repo_relative_text(parity_manifest_path)}`.",
                "- They intentionally do not live under `tools/apdr/tests/fixtures/` so older continuity sampling stays stable.",
                "",
                "Each case directory contains the copied source snippet used to lock the current family-owned behavior before Phase 8 changes it.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def build_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        "# Phase 7 Family Snapshots",
        "",
        "## Selection Rule",
        "- Start from the canonical Phase 7 parity manifest only.",
        "- Include cases whose report already shows `family:` strategies or `Family knowledge` notes.",
        "- Include cases whose requirements, missing module, or failing package match the explicit namespace mappings or family bundle anchors owned by the current family runtime.",
        "",
        "## Snapshot Cases",
        "| Case ID | Bucket | Fixture | Reasons |",
        "| --- | --- | --- | --- |",
    ]
    for case in manifest["cases"]:
        reasons = "; ".join(case["selection_reasons"]).replace("|", "\\|")
        lines.append(
            f"| `{case['case_id']}` | `{case['normalized_bucket']}` | `{case['fixture_path']}` | {reasons} |"
        )

    lines.extend(
        [
            "",
            "## Fixture Layout",
            f"- Fixture root: `{manifest['fixtures_root']}`",
            "- Each selected case has a dedicated directory containing `snippet.py`.",
            "- `README.md` documents why these Phase 7 benchmark-derived family snapshots stay isolated from the legacy continuity fixture root.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    parity_manifest_path = Path(args.parity_manifest).expanduser().resolve()
    cases_root = Path(args.cases_root).expanduser().resolve()
    fixtures_root = Path(args.fixtures_root).expanduser().resolve()
    output_json = Path(args.output_json).expanduser().resolve()
    output_md = Path(args.output_md).expanduser().resolve()

    if not cases_root.exists():
        raise FileNotFoundError(f"cases root does not exist: {cases_root}")

    parity_manifest = load_parity_manifest(parity_manifest_path)
    if int(parity_manifest.get("canonical_case_count", 0)) != 70:
        raise ValueError("parity manifest does not contain the expected 70 canonical cases")

    if fixtures_root.exists():
        shutil.rmtree(fixtures_root)
    fixtures_root.mkdir(parents=True, exist_ok=True)

    selected_cases: list[dict[str, Any]] = []
    for case in sorted(parity_manifest["cases"], key=lambda item: clean_text(item.get("case_id"))):
        entry = build_case_entry(case, fixtures_root)
        if entry is not None:
            selected_cases.append(entry)

    write_readme(fixtures_root, parity_manifest_path)

    manifest = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "parity_manifest": repo_relative_text(parity_manifest_path),
        "cases_root": repo_relative_text(cases_root),
        "fixtures_root": repo_relative_text(fixtures_root),
        "selected_case_count": len(selected_cases),
        "selected_case_ids": [case["case_id"] for case in selected_cases],
        "selection_rule": {
            "report_markers": ["resolved_dependencies contains family:", "notes mention Family knowledge"],
            "explicit_namespace_mappings": [
                f"{import_alias} -> {package_name}"
                for import_alias, package_name in EXPLICIT_NAMESPACE_MAPPINGS.items()
            ],
            "family_bundle_anchors": list(FAMILY_BUNDLE_ANCHORS),
        },
        "cases": selected_cases,
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    output_md.write_text(build_markdown(manifest), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
