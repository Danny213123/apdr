---
phase: 07
slug: failure-baseline-parity-slice
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-28
---

# Phase 7 - Validation Strategy

> Validation contract for the canonical tier3 parity manifest, touched-family snapshot corpus, and baseline checker handoff into Phase 8.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python artifact scripts, `python -m py_compile`, structural `rg` checks, and targeted `cargo test` |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python -m py_compile scripts/build_phase7_parity_manifest.py scripts/build_phase7_family_snapshots.py scripts/check_phase7_baseline.py` |
| **Full suite command** | `python -m py_compile scripts/build_phase7_parity_manifest.py scripts/build_phase7_family_snapshots.py scripts/check_phase7_baseline.py && python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` |
| **Estimated runtime** | ~2-5 minutes once the artifacts exist |

---

## Sampling Rate

- **After every task commit:** Run the task-specific Python compile, artifact generation, grep, or targeted resolver command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** Full suite must be green and the three Phase 7 generated artifact sets must exist
- **Max feedback latency:** keep script syntax checks and targeted resolver coverage under 3 minutes; allow the artifact-generation tasks to run longer because they are the phase deliverable

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 07-01-01 | 01 | 1 | REC-01 | script syntax | `python -m py_compile scripts/build_phase7_parity_manifest.py` | no | pending |
| 07-01-02 | 01 | 1 | REC-01 | artifact generation | `python scripts/build_phase7_parity_manifest.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md` | yes | pending |
| 07-02-01 | 02 | 2 | FAM-04, REC-01 | script syntax | `python -m py_compile scripts/build_phase7_family_snapshots.py` | no | pending |
| 07-02-02 | 02 | 2 | FAM-04 | artifact generation + fixture copy | `python scripts/build_phase7_family_snapshots.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --cases-root runs/20260327-150339-apdr/cases --fixtures-root tools/apdr/tests/phase7_family_fixtures --output-json .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md` | yes | pending |
| 07-03-01 | 03 | 3 | REC-01, FAM-04 | script syntax | `python -m py_compile scripts/check_phase7_baseline.py` | no | pending |
| 07-03-02 | 03 | 3 | REC-01 | baseline note sections | `rg -n '## Canonical Slice|## Normalized Buckets|## Touched Family Snapshots|## Tier1 Watchlist|## Phase 8 Handoff' .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md` | yes | pending |
| 07-03-03 | 03 | 3 | REC-01, FAM-04 | checker + targeted resolver tests | `python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md && cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` | yes | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.
- No new benchmark framework should be added; Phase 7 should reuse the stopped-run artifacts already checked into the workspace.
- No new Python dependency should be required for the Phase 7 scripts.
- Benchmark-derived fixtures must stay outside `tools/apdr/tests/fixtures/`.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The canonical slice stays fixed to the 70 tier3 cases and does not quietly absorb the 17 tier1 watchlist cases | REC-01 | Automation can prove counts and IDs, but a reviewer still needs to confirm the narrative keeps the tier1 watchlist out of contract | Read `07-TIER3-PARITY-MANIFEST.md` and `07-BASELINE.md` and confirm both documents describe the tier1 overlap as a watchlist rather than part of the canonical baseline |
| The family snapshot set is scoped to touched family-runtime behavior rather than broad generic dependency failures | FAM-04 | A script can record `selection_reasons`, but a reviewer must still judge whether the rule stays anchored to family-owned surfaces | Read `07-FAMILY-SNAPSHOTS.md` and verify the recorded `selection_reasons` trace back to `family:` markers, `Family knowledge` notes, or the explicit family and namespace anchors documented in the report |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity includes both artifact-generation checks and one targeted resolver test guardrail
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded outside the intentional artifact-generation tasks
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** approved 2026-03-28
