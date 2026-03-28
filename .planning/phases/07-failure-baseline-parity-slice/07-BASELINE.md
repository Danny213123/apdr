# Phase 7 Baseline

## Commands
- `python scripts/build_phase7_parity_manifest.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --output-json .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md`
- `python scripts/build_phase7_family_snapshots.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --cases-root runs/20260327-150339-apdr/cases --fixtures-root tools/apdr/tests/phase7_family_fixtures --output-json .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --output-md .planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md`
- `python scripts/check_phase7_baseline.py --summary-json runs/20260327-150339-apdr/summary.json --pllm-csv pllm_results/csv/summary-all-runs.csv --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --baseline-md .planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`

## Artifact Links
- Canonical manifest: `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json`
- Canonical summary: `.planning/phases/07-failure-baseline-parity-slice/07-TIER3-PARITY-MANIFEST.md`
- Family snapshot manifest: `.planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json`
- Family snapshot summary: `.planning/phases/07-failure-baseline-parity-slice/07-FAMILY-SNAPSHOTS.md`
- Fixture root: `tools/apdr/tests/phase7_family_fixtures/`

## Canonical Slice
The Phase 7 baseline is the 70-case tier3 APDR-failed and `pllm`-passing slice from March 27, 2026. The contract is fixed to the canonical manifest so later accuracy work measures against one bounded target instead of reopening the stopped benchmark overlap each time.

## Normalized Buckets
- `environment-build-failed`: `21`
- `module-not-found`: `19`
- `dependency-conflict`: `12`
- `version-not-found`: `11`
- `syntax-error`: `5`
- `import-error`: `2`

## Touched Family Snapshots
The family snapshot corpus contains `17` touched-family cases selected from the canonical manifest only. Their copied benchmark-derived snippets live under `tools/apdr/tests/phase7_family_fixtures/` so the family migration can keep a stable regression boundary without perturbing the older continuity fixture root.

## Tier1 Watchlist
The remaining `17 overlap cases` are documented but outside the Phase 7 contract. They stay in the parity manifest watchlist for later milestone work instead of expanding the canonical tier3 baseline.

## Verification
`scripts/check_phase7_baseline.py` re-derives the raw overlap from `runs/20260327-150339-apdr/summary.json` and `pllm_results/csv/summary-all-runs.csv`, confirms the canonical `70`/`17` split, checks the normalized bucket totals, verifies every family snapshot fixture path, and validates this note's required headings and handoff text. The targeted resolver guardrail remains `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`.

## Phase 8 Handoff
Only the touched-family subset is protected for the first data-driven migration pass. Phase 8 should move the current family-owned behavior for those `17` snapshot cases into validated data files while keeping the canonical 70-case slice and the 17 overlap cases outside the Phase 7 contract stable for future comparisons.
