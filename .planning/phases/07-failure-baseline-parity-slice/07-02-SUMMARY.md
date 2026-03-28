---
phase: 07-failure-baseline-parity-slice
plan: 02
subsystem: testing
tags:
  - apdr
  - family-knowledge
  - fixtures
  - snapshots
  - python
dependency_graph:
  requires:
    - 07-01
  provides:
    - phase-7-family-snapshot-manifest
    - isolated-phase-7-family-fixtures
    - deterministic-family-selection-rule
  affects:
    - 07-03
    - 08
tech_stack:
  added: []
  patterns:
    - canonical-manifest-only-fixture-selection
    - case-local-snippet-fallback-for-locked-source-trees
    - isolated-phase7-family-fixture-root
key_files:
  created:
    - .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json
    - .planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md
    - .planning/phases/07-failure-baseline-parity-slice/07-02-SUMMARY.md
    - tools/apdr/tests/phase7_family_fixtures/README.md
  modified:
    - scripts/build_phase7_family_snapshots.py
key-decisions:
  - Selected touched-family cases from the canonical Phase 7 manifest only so the snapshot corpus stays bounded to the fixed 70-case contract.
  - Matched the explicit namespace triggers using the exact alias-side tokens named in the plan, not the package-side names, to avoid over-selecting unrelated cases.
  - Fell back to the benchmark case's `.apdr-debug/attempts/*/snippet.py` copies when the original `hard-gists` source tree was not readable from the workspace.
patterns-established:
  - "Phase-local regression fixtures should live under `tools/apdr/tests/phase7_family_fixtures/`, not the legacy continuity fixture root."
  - "Touched-family selection stays explainable by recording per-case `selection_reasons` derived from report markers, namespace aliases, and bundle anchors."
requirements-completed:
  - FAM-04
  - REC-01
metrics:
  completed_date: "2026-03-28"
  tasks_completed: 2
  verification_tests: 6
---

# Phase 7 Plan 02 Summary

**Deterministic 17-case touched-family snapshot manifest with isolated benchmark-derived fixtures for the current family-owned runtime boundary.**

## Accomplishments

- Added `scripts/build_phase7_family_snapshots.py` to select touched-family cases from the canonical Phase 7 manifest using report markers, exact namespace aliases, and family bundle anchors.
- Generated `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json` and `.planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md` for the bounded `17`-case touched-family subset.
- Copied benchmark-derived snippets into `tools/apdr/tests/phase7_family_fixtures/` and documented why that corpus stays outside `tools/apdr/tests/fixtures/`.

## Verification Results

- `python -m py_compile scripts/build_phase7_family_snapshots.py` passed.
- `python scripts/build_phase7_family_snapshots.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --cases-root runs/20260327-150339-apdr/cases --fixtures-root tools/apdr/tests/phase7_family_fixtures --output-json .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md` passed.
- `rg -n 'selection_reasons|fixture_path|family:' .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json` passed.
- `Get-ChildItem tools/apdr/tests/phase7_family_fixtures -Recurse -Filter snippet.py | Measure-Object` reported `Count : 17`.
- `rg -n '## Selection Rule|## Snapshot Cases|## Fixture Layout' .planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md` passed.
- `Select-String -Path 'tools/apdr/tests/phase7_family_fixtures/README.md' -Pattern 'Phase 7 benchmark-derived family snapshots','do not live under \`tools/apdr/tests/fixtures/\`'` passed.

## Files Created/Modified

- `scripts/build_phase7_family_snapshots.py` - deterministic selector and fixture copier for the touched-family subset.
- `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json` - machine-readable manifest for the selected touched-family cases, their reasons, and copied fixture paths.
- `.planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md` - reviewer-facing summary of the selection rule, selected cases, and fixture layout.
- `tools/apdr/tests/phase7_family_fixtures/README.md` - explains the benchmark-derived fixture root and why it is isolated from the legacy continuity root.
- `tools/apdr/tests/phase7_family_fixtures/` - copied `snippet.py` fixtures for the 17 touched-family cases.

## Decisions Made

- The touched-family corpus is selected from the canonical manifest only; Phase 7 does not rescan the raw benchmark corpus once the parity slice is fixed.
- Explicit namespace selection follows the exact alias tokens named in the plan (`pkg_resources`, `Image`, `sklearn`, and similar), which keeps package-side names like `scikit-learn` from widening the scope incorrectly.
- The copied fixture source can come from the benchmark case's attempt-local `snippet.py` fallback when the original `hard-gists` tree is ACL-blocked.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added a case-local snippet fallback for unreadable `hard-gists` paths**
- **Found during:** Task 1 (Implement the touched-family snapshot builder with the Phase 7 selection rule)
- **Issue:** The manifest's original snippet paths under `hard-gists/` were not readable in the current workspace, so copying the selected fixtures failed with `WinError 5`.
- **Fix:** Updated the selector to fall back to each case's `.apdr-debug/attempts/*/snippet.py` copies, which preserve the benchmark-derived source text locally.
- **Files modified:** `scripts/build_phase7_family_snapshots.py`
- **Verification:** Re-ran the generator successfully and confirmed `17` copied `snippet.py` fixtures under `tools/apdr/tests/phase7_family_fixtures/`
- **Committed in:** `1055bd2`

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Limited to the fixture-copy source path. The selection rule and output contract stayed unchanged.

## Issues Encountered

- The first selector draft over-matched package-side names for explicit namespace mappings and produced a 25-case subset; tightening the matcher to the exact alias-side tokens restored the intended 17-case boundary before the task-1 commit.
- Existing unrelated local changes in `tools/apdr/src/lib.rs` and `tools/apdr/llm_py/tests/test_llm_integration.py` were left untouched.

## Next Phase Readiness

- `07-03` can now verify that every family snapshot case remains a member of the canonical manifest and that every recorded `fixture_path` exists on disk.
- Phase 8 has a bounded 17-case touched-family corpus that protects the first data-driven migration pass without perturbing the older continuity fixture root.

## Self-Check: PASSED

- `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json` contains `"selection_reasons"`, `"fixture_path"`, and `selected_case_count: 17`.
- `.planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md` contains `## Selection Rule`, `## Snapshot Cases`, and `## Fixture Layout`.
- `tools/apdr/tests/phase7_family_fixtures/README.md` contains `Phase 7 benchmark-derived family snapshots` and `do not live under \`tools/apdr/tests/fixtures/\``.

---
*Phase: 07-failure-baseline-parity-slice*
*Completed: 2026-03-28*
