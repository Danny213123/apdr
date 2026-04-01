# Phase 20: Dominant Bucket Recovery Gains - Research

**Researched:** 2026-04-01
**Domain:** Measurable tier3 recovery improvements on a fixed live-derived slice after Phase 17 fallback repair, Phase 18 backend escalation, and Phase 19 accounting cleanup
**Confidence:** High

## Summary

Phase 20 should be treated as a bounded recovery-gain phase, not as a full-corpus benchmark rerun. The March 30, 2026 wrapper summary now has truthful routing and accounting context thanks to Phases 17-19, which means the next phase can finally measure real gains on a selected tier3 slice without mixing in fallback crashes, backend-path ambiguity, or historical resume contamination. The roadmap already scopes the requirement to a "selected v2.3 tier3 benchmark slice", and the local evidence strongly supports using that boundary: many dominant-bucket cases are recoverable with targeted resolver improvements, while others are genuinely obsolete or runtime-bound and should not determine whether the recovery work shipped.

The live baseline remains concentrated. On the current `runs/20260330-020943-apdr/summary.json` tier3 rows, the most common failure buckets are `module-not-found` (155), `environment-build-failed` (112), and `version-not-found` (69). Existing data-driven recovery surfaces are small compared with that failure surface: `tools/apdr/data/recovery/module_rules.json` contains only 3 provider rules and 9 stop-reason rules, while `tools/apdr/data/recovery/compatibility_rules.json` contains 8 compatibility clusters, 2 companion rules, and 2 Python-ceiling rules. That gap is large enough that Phase 20 should explicitly expand the targeted recovery data and the retry-loop behavior that consumes it.

Three code seams stand out from the live evidence. First, recurrent `module-not-found` cases still fall through as generic mapping failures even when they are either recoverable aliases or obviously non-PyPI/runtime-specific imports. Second, several `version-not-found` and `environment-build-failed` cases show legacy-package replacement or interpreter-floor problems that the current compatibility schema cannot express. Third, the repo already contains enough replay and comparison tooling to prove gains on a fixed slice, so Phase 20 should reuse that pattern instead of inventing a new benchmark artifact format.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| AGT-09 | APDR resolves more cases successfully on the selected v2.3 tier3 benchmark slice than the March 30, 2026 baseline for the same run mode and model | The roadmap intentionally scopes this to a selected slice rather than the full tier3 corpus, which matches the repo’s existing replay-proof pattern and avoids letting unrecoverable cases swamp measurable gains. |
| VAL-03 | APDR reduces failures in `module-not-found`, `environment-build-failed`, and `version-not-found` on the selected slice compared with the March 30, 2026 baseline | The Phase 19 accounting fix now makes bucket deltas trustworthy, and the live baseline clearly shows these three buckets dominating the remaining failure surface. |

## Evidence That Should Drive Planning

### 1. The dominant bucket surface is still much larger than the current targeted recovery data

The current live-derived counts show `module-not-found` 155, `environment-build-failed` 112, and `version-not-found` 69 on tier3 rows. Against that, the bounded deterministic recovery data under `tools/apdr/data/recovery/` is still tiny:

- `module_rules.json`: 3 provider rules, 9 stop-reason rules
- `compatibility_rules.json`: 8 compatibility clusters, 2 companion rules, 2 Python-ceiling rules

That mismatch suggests Phase 20 should extend the data-backed recovery layer first rather than piling more one-off branches into the retry loop.

### 2. `module-not-found` still mixes alias problems with obviously non-recoverable runtime imports

A quick reason-string extraction from the March 30 tier3 rows shows recurring missing modules such as:

- `numpy` (5)
- `builtins` (5)
- `simplegui` (5)
- `mosquitto` (3)
- `numpy.distutils.core` (3)
- `canvas` (3)
- `eyeD3` (2)
- `request` (2)

Representative evidence:

- `runs/20260329-165524-apdr/cases/04ef258fa29e4e685287a30cf60462d0/resolution-report.txt` ends with `Missing module \`numpy\` persisted across multiple dependency sets; ending recovery as a mapping failure.` even though the loop only ever re-tried env validation and never had the Phase 18 Docker middle hop available.
- `runs/20260329-165524-apdr/cases/101323115e70bb6671d3/...` ends on `Missing module \`Cython.Distutils\`...`, which is a concrete provider/alias family rather than an arbitrary unknown import.
- `runs/20260330-020943-apdr/summary.json` still contains module-not-found rows for clearly non-PyPI/runtime-specific imports like `simplegui`, `canvas`, and `mosquitto`.

Phase 20 should therefore split this bucket into two targeted levers:

- add deterministic provider rules for recoverable alias families
- stop early with explicit non-generic outcomes for runtime-specific or non-PyPI imports so they leave the dominant bucket

### 3. Some `version-not-found` cases are actually interpreter-floor and legacy-replacement problems

The extracted `version-not-found` patterns are not just random missing versions. The recurrent names include:

- `numpy==1.21.6` (5)
- `opencv-python-headless` (4)
- `BeautifulSoup` (4)
- `cv==1` (3)
- `PyAudio` (3)
- `setuptools>=61.2` (3)

Representative evidence from `runs/20260329-165524-apdr/cases/10295174/resolution-report.txt` is especially important:

- family knowledge first builds a coherent PyMC3 stack for Python 3.10
- SMT pre-solve then falls back because `pylearn2` lacks metadata
- the actual terminal attempt runs on Python 2.7
- the case then fails with `Package \`numpy==1.21.6\` is unavailable for the selected Python version.`

That is not a pure package-name problem; it is a compatibility-cluster plus interpreter-floor problem. Phase 20 should therefore let compatibility policy express:

- replacement packages for legacy names like `BeautifulSoup` and `MySQL-python`
- interpreter floors as well as ceilings for legacy families that must not fall back below Python 3.x

### 4. The `environment-build-failed` bucket is dominated by repeatable build classes, not a single generic failure mode

The March 30 build-failure reasons fall into a few recurring classes:

- `sdist-build-failure` (28)
- `budget-exhausted` (21)
- `opaque-build-failure` (18)
- `system-library-build` (13)
- `python-version-mismatch` (8)
- `oscillating-requirements` (6)

Two concrete examples:

- `runs/20260329-165524-apdr/cases/056626de3fbdc7cf7b59de1d9f6279d1/resolution-report.txt` loops through `opencv-python-headless` plus multiple `numpy` wheel candidates and ends on `Repeated failure signature \`BuildFailure|TPL-OS||numpy\`...`.
- `runs/20260329-165524-apdr/cases/00e9638c0efad1adac878522cf172484/resolution-report.txt` fails on a legacy TensorFlow stack with a vague `hint: See above for details.` build terminal.

Because Phase 18 already inserted Docker as a targeted middle hop for eligible `llm` cases, Phase 20 does not need to invent a new backend path. It should instead stabilize the package choices that feed that path:

- lock better preferred versions for recurring legacy build clusters
- stop repeated contradictory wheel/pin churn earlier
- preserve interpreter floors so the loop does not "fix" one missing package by choosing a Python version that breaks the rest of the stack

### 5. The repo already has the right comparison pattern for a fixed live-derived slice

`scripts/run_phase15_tier3_benchmark.py` already provides a replay-slice artifact contract with probe mode and run-contract metadata, and the Phase 17-19 proof scripts already demonstrate the repo’s preferred pattern:

- freeze a fixed live-derived slice manifest
- validate it with a deterministic checker
- use the same checker later for live replay artifacts

Phase 20 should reuse that pattern for recovery deltas:

- a fixed dominant-bucket slice manifest drawn from the March 30 baseline
- a baseline artifact extracted from the March 30 run
- a candidate artifact produced by rerunning the exact same slice with the same run mode and model
- a checker that confirms pass gains and dominant-bucket reductions without accepting config drift

## Implementation Recommendations

### 1. Expand bounded module recovery and bucket exits first

Recommended files:

- `tools/apdr/data/recovery/module_rules.json`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/tests/test_resolver.rs`

Recommended responsibilities:

- add concrete provider aliases for recoverable module families such as `request -> requests`, `eyeD3 -> eyed3`, and `Cython.Distutils -> Cython`
- add stop-reason rules for clearly non-PyPI/runtime-specific families such as `simplegui`, `canvas`, and `mosquitto`
- convert targeted stop reasons with prefixes like `host-runtime`, `project-local`, or `removed-runtime` into non-generic terminal statuses/buckets so they leave `module-not-found`
- add focused Rust tests for both provider recovery and dominant-bucket exit semantics

### 2. Extend compatibility policy to express replacements and interpreter floors

Recommended files:

- `tools/apdr/data/recovery/compatibility_rules.json`
- `tools/apdr/src/resolver/targeted_recovery.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/tests/test_resolver.rs`

Recommended responsibilities:

- extend compatibility policy with explicit replacement-package support for legacy names like `BeautifulSoup -> beautifulsoup4` and `MySQL-python -> mysqlclient`
- add interpreter-floor support so legacy Python-3 families cannot fall back to Python 2.7 when unrelated packages lack metadata
- add or tighten clusters for the recurring `opencv-python-headless`, legacy TensorFlow/PyMC3, `setuptools`, and `numpy` wheel-selection paths
- add Rust tests that prove the retry loop applies replacements/floors instead of re-entering the same `version-not-found` and `environment-build-failed` churn

### 3. Prove gains on a frozen dominant-bucket slice, not the entire corpus

Recommended files:

- `scripts/run_phase20_recovery_benchmark.py`
- `scripts/check_phase20_recovery_delta.py`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-recovery-proof-status.json`
- `.planning/phases/20-dominant-bucket-recovery-gains/20-RECOVERY-DELTA.md`

Recommended responsibilities:

- freeze a fixed 9-case or 12-case dominant-bucket slice from the March 30 baseline that contains recoverable `module-not-found`, `version-not-found`, and `environment-build-failed` cases
- emit both probe-only sample artifacts and a live-capable candidate artifact format using the same run-contract fields
- compare baseline and candidate with hard gates for same slice, same run mode, same model, more passes, and fewer dominant-bucket failures

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_module_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase20_compat_ -- --nocapture`
- `python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json .planning/phases/20-dominant-bucket-recovery-gains/20-recovery-proof-status.json --probe-only`

### Artifact checks

- `rg -n 'request|eyeD3|Cython.Distutils|simplegui|canvas|mosquitto' tools/apdr/data/recovery/module_rules.json`
- `rg -n 'replacement|python_floor|opencv|BeautifulSoup|PyAudio|MySQL-python|numpy==1.21.6' tools/apdr/data/recovery/compatibility_rules.json tools/apdr/src/resolver/targeted_recovery.rs tools/apdr/src/resolver/retry_loop.rs`
- `rg -n 'slice_id|baseline_status|delta_passes|dominant bucket' scripts/run_phase20_recovery_benchmark.py scripts/check_phase20_recovery_delta.py .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json .planning/phases/20-dominant-bucket-recovery-gains/20-RECOVERY-DELTA.md`

### Phase-close checks

- inspect the candidate artifact and confirm it uses the same slice, run mode, and model as the baseline artifact
- inspect the delta checker output and confirm it reports both pass gains and reductions in `module-not-found`, `environment-build-failed`, and `version-not-found`
- inspect at least one recovered case from each dominant bucket and confirm the saved artifact reflects the expected new path rather than only a relabeled failure

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-VERIFICATION.md`
- `.planning/phases/18-backend-escalation-and-path-truth/18-VERIFICATION.md`
- `.planning/phases/19-failure-classification-and-run-accounting-integrity/19-VERIFICATION.md`
- `runs/20260330-020943-apdr/summary.json`
- `runs/20260329-165524-apdr/cases/04ef258fa29e4e685287a30cf60462d0/resolution-report.txt`
- `runs/20260329-165524-apdr/cases/10295174/resolution-report.txt`
- `runs/20260329-165524-apdr/cases/056626de3fbdc7cf7b59de1d9f6279d1/resolution-report.txt`
- `tools/apdr/data/recovery/module_rules.json`
- `tools/apdr/data/recovery/compatibility_rules.json`
- `tools/apdr/src/resolver/targeted_recovery.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/tests/test_resolver.rs`
- `scripts/run_phase15_tier3_benchmark.py`
- `scripts/check_phase19_accounting.py`

## Out of Scope For This Phase

- rerunning the full 396-case tier3 surface before a fixed dominant-bucket slice exists
- redesigning the benchmark UI or the accounting/provenance surfaces already repaired in Phase 19
- reopening fallback-state or backend-path work from Phases 17 and 18 except where regression coverage is needed
- broad deterministic rule-table expansion detached from the March 30 dominant-bucket evidence

## Source Base

No external browsing was required for Phase 20 planning. The source of truth is the repo’s own roadmap and requirement files, the completed Phase 17-19 artifacts, the March 30 run summaries and case reports already stored in the workspace, and the existing APDR resolver plus benchmark replay scripts.

---
*Research created: 2026-04-01*
*Phase: 20-dominant-bucket-recovery-gains*
