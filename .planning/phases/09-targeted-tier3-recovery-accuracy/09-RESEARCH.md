# Phase 9: Targeted Tier3 Recovery Accuracy - Research

**Researched:** 2026-03-28
**Domain:** Improving APDR recovery on the locked Phase 7 parity slice without reopening the Phase 8 family-runtime migration boundary
**Confidence:** Medium

## Summary

Phase 9 should improve recovery only on the bounded canonical slice that Phase 7 locked and Phase 8 carried forward. The remaining work is not a broad benchmark rerun problem. It is a concentrated retry-policy problem: `19` canonical `module-not-found` cases still mix fixable provider or alias mismatches with project-local, removed-runtime, and internal-extension imports; `11` canonical `version-not-found` cases are dominated by a small contradictory-pin cluster around `torch` and `torchvision` plus a few transitive-specifier and Python-ceiling misses; and `12` canonical `dependency-conflict` cases are dominated by the TensorFlow or Keras family that Phase 8 already moved behind curated data. The Phase 9 plan should therefore add one bounded targeted-recovery policy layer, use it to improve module and compatibility handling before the generic retry fallbacks fire, and finish with deterministic regression and checker coverage instead of another round of ad hoc one-off scripts.

Primary recommendation: plan Phase 9 as three sequential waves. First, add repo-shipped targeted recovery policy files plus a strict loader or validator that anchors rules to the canonical Phase 7 parity clusters. Second, route `module-not-found` handling through targeted module-provider and stop-reason policies so fixable alias cases recover deterministically and local or removed-runtime cases stop with inspectable reasons instead of generic mapping failure. Third, normalize version or conflict parsing and apply targeted compatibility policies for the `torch`, `torchvision`, `tensorflow`, `keras`, `tensorboard`, and legacy-ceiling cases, then lock the phase behind targeted regression tests and a deterministic Phase 9 checker while keeping the Phase 8 family-runtime checker green.

## Phase Requirements

| ID | Requirement | Research Support |
|----|-------------|------------------|
| REC-02 | APDR reduces `module-not-found` outcomes on the targeted parity slice compared with the 2026-03-27 baseline | The canonical slice still has `19` `module-not-found` cases, but they collapse into a small number of repeatable provider, alias, local-module, and removed-runtime patterns rather than `19` unrelated bugs. |
| REC-03 | APDR reduces `version-not-found` and dependency-mapping failures on the targeted parity slice compared with the 2026-03-27 baseline | The `11` `version-not-found` cases and `12` `dependency-conflict` cases are concentrated around a few compatibility clusters that can be expressed as deterministic recovery policies instead of left to broad pin stripping. |
| REC-04 | APDR improves the number of APDR-failed but `pllm`-passing cases it can recover on the targeted slice | Phase 7 already bounded the improvement target to the `87` APDR-failed but `pllm`-passing overlap cases, so Phase 9 can measure progress against concrete case IDs without widening scope. |

## Evidence That Should Drive Planning

### The Phase 7 parity target is already bounded and should not move

`07-tier3-parity-manifest.json` already fixes the milestone comparison surface:

- `87` APDR-failed but `pllm`-passing overlap cases
- `70` canonical tier3 cases in scope for targeted improvement
- `17` watchlist cases kept outside the canonical contract
- canonical normalized bucket totals of `21` `environment-build-failed`, `19` `module-not-found`, `12` `dependency-conflict`, `11` `version-not-found`, `5` `syntax-error`, and `2` `import-error`

Phase 9 does not need a new benchmark slice. It needs deterministic policies and regression coverage that stay anchored to this manifest.

### Phase 8 stabilized the touched family runtime and should now be treated as the platform

`08-FAMILY-RUNTIME.md` explicitly hands Phase 9 a stable base:

- the touched family runtime now lives in curated files under `tools/apdr/data/family_knowledge`
- the `17` touched-family snapshot cases are already protected by `phase7_family_` tests
- `scripts/check_phase8_family_runtime.py` already proves the migration boundary remains intact

That means Phase 9 should build on the curated family path rather than reintroduce new hardcoded family branches. Recovery-quality improvements for TensorFlow, Keras, Pillow, PyMC3, ggplot, or setuptools should use the data-driven runtime that Phase 8 established.

### The `module-not-found` slice is concentrated into provider, local, and removed-runtime groups

The canonical `19` `module-not-found` cases are not evenly distributed:

- `3` cases are the already-known `pkg_resources` provider gap: `1e2600ed62d5e76b21ee`, `263113`, `3a6e4d618afc344aab81`
- `2` cases are removed-runtime or deprecated namespace signals already adjacent to Phase 8 family ownership: `imp` in `1231964e784ab9acb65d` and `numpy.distutils` in `4882342eba2b57376ed1`
- `1` case is the legacy PIL import `Image` in `3682135`, which is already part of the touched family runtime
- several cases look like deterministic provider or alias mismatches instead of arbitrary missing packages: `djangorestframework`, `Stencil`, `turbogears`, `i3`, and `ib`
- several other cases look project-local, internal-extension, or benchmark-snippet-only rather than PyPI-installable: `taggit_autocomplete`, `gisutils`, `clips`, `pizzanuvola_teaser`, `api`, and `_distance_wrap`
- `elementtree` and `numpy` also look like runtime-compatibility or packaging-shape issues rather than straight import-to-package mapping misses

The current retry loop already has a few generic stop conditions for local Django settings modules, guarded imports, and Unix-only stdlib modules, but the canonical Phase 9 cases show that this guard set is too small. The phase should add targeted stop reasons for removed-runtime and internal-module patterns while giving deterministic provider rules a chance to recover before the generic "mapping failure" break fires.

### The `version-not-found` slice exposes a parsing gap and a small compatibility policy surface

The canonical `11` `version-not-found` cases are highly concentrated:

- `4` cases end on contradictory-pin exhaustion for `torch`
- `1` case ends on contradictory-pin exhaustion for `torchvision`
- `1` case reports `odfpy==0.9` unavailable for the selected Python version
- `1` case reports `numpy==1.21.6` unavailable for the selected Python version inside the legacy PyMC3 stack
- `1` case reports `setuptools>=58.0.0` missing while building `pyjnius`
- `1` case ends on contradictory pins for `mitmproxy`
- `2` cases carry transitive requirement strings that are currently hard for the retry path to use: `PyJWT>=2.0.0` and `python-dateutil<2.0,>=2.1`

The current extractor in `recovery_diagnostics.rs` reliably handles `package==version`, but it does not normalize range specifiers into a package key plus constraint. That leaves the retry loop without a clean package name for several transitive misses. Phase 9 should fix that parsing gap before adding more special cases.

### The `dependency-conflict` slice is mostly a family-aware compatibility problem

The canonical `12` `dependency-conflict` cases cluster into four groups:

- `8` TensorFlow or Keras conflicts, including the repeated `keras==3.0.0` versus `tensorflow==2.18.0` mismatch and one `tensorboard==2.4.0` versus `tensorflow==2.18.0` mismatch
- `2` `scikit-learn==0.20.4` conflicts
- `1` `pymc==5.28.2` versus `numpy==1.26.4` conflict
- `1` `impacket` plus `termcolor` conflict

This is exactly where the Phase 8 family-runtime work matters. The dominant cluster is already part of the touched family surface, so Phase 9 should refine bundle selection and compatibility policies through curated data or a new bounded policy layer that consults the family runtime rather than bypassing it.

### The current retry loop is still too generic for the canonical accuracy target

The current recovery loop in `retry_loop.rs` still leans on broad fallback behavior:

- `module-not-found` exits after the same missing module persists across two dependency sets
- `version-not-found` and `dependency-conflict` exit after two contradictory version attempts for the same package
- generic fallback strips a single version pin, then strips all pins, before giving up
- LLM recovery still runs after many deterministic failure signals have already appeared

Those mechanics are useful as a safety net, but they are too blunt for the bounded canonical slice. Phase 9 should add deterministic targeted policies before these generic fallbacks and should surface explicit notes such as "removed stdlib module", "project-local helper", or "family compatibility bundle reapplied" instead of burying the reason behind "mapping failure" or "contradictory pins" when a more precise explanation exists.

### The older analysis scripts are not enough for closeout

The repo already has `scripts/analyze_missing_modules.py`, `scripts/analyze_build_failures.py`, and `scripts/find_more_aliases.py`, but they are exploratory scripts aimed at earlier run directories rather than deterministic Phase 9 closeout. They are useful for background context, but the phase needs one checker that reads the locked Phase 7 manifest and the Phase 9 note or policy files directly so reviewers can rerun the exact same closeout logic later.

## Implementation Recommendations

### 1. Add one bounded targeted-recovery policy layer

Recommended new files:

- `tools/apdr/src/resolver/targeted_recovery.rs`
- `tools/apdr/data/recovery/module_rules.json`
- `tools/apdr/data/recovery/compatibility_rules.json`
- `tools/apdr/data/recovery/README.md`

Recommended responsibilities:

- define serde-backed policy structs for module-provider rules, explicit stop-reason rules, compatibility bundles, companion packages, Python ceilings, and conflict anchors
- add an initialization path such as `init_targeted_recovery_policy(tool_root: &Path)` that mirrors the deterministic loader pattern Phase 8 used
- validate duplicate rule IDs, duplicate module aliases, duplicate package anchors, empty trigger sets, and any case IDs that are not part of the canonical Phase 7 slice
- keep the new policy layer separate from `touched_families.json` so the Phase 8 migration boundary stays readable, but allow rules to reference Phase 8 family IDs or rule IDs where appropriate

### 2. Route `module-not-found` handling through targeted provider and stop-reason policies

Recommended runtime entrypoints to update:

- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/tier3_llm/core.rs`
- `tools/apdr/tests/test_resolver.rs`

Recommended behavior:

- consult targeted module-provider rules before the generic mapping-failure break triggers
- allow deterministic recovery for provider or alias cases such as `pkg_resources`, `Image`, and any additional Phase 9 module rules that the bounded data files define
- add explicit stop reasons for removed-runtime or internal-module cases such as `imp`, `numpy.distutils`, `elementtree`, `_distance_wrap`, `api`, or other canonical helpers that should not trigger repeated LLM or package-swap retries
- keep failure notes inspectable and case-specific so reviewers can still see why a case stopped even when it is not recovered

### 3. Add normalized version-spec parsing and compatibility policies before broad unpinning

Recommended runtime entrypoints to update:

- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/family_knowledge/core.rs`
- `tools/apdr/tests/test_resolver.rs`

Recommended behavior:

- parse requirement strings like `PyJWT>=2.0.0` and `python-dateutil<2.0,>=2.1` into a package key plus constraint instead of treating the whole string as the package name
- consult targeted compatibility policies for the repeated `torch` and `torchvision` contradictory-pin cluster
- refine TensorFlow, Keras, and tensorboard handling through the Phase 8 family runtime instead of relying only on generic pin stripping
- add targeted notes for Py2 ceilings, companion packages, or known incompatible bundles so `version-not-found` and `dependency-conflict` reasons stay reviewer-readable

### 4. Close the phase with bounded regression and one deterministic checker

Recommended new artifacts:

- `scripts/check_phase9_targeted_recovery.py`
- `.planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md`

Recommended regression approach:

- add `phase9_targeted_policy_`, `phase9_targeted_module_`, and `phase9_targeted_compatibility_` tests in `tools/apdr/tests/test_resolver.rs`
- keep `phase7_family_` and `data_driven_family_` tests in the Phase 9 closeout suite so the family-runtime boundary stays green while accuracy logic changes land
- make the Phase 9 checker verify that the targeted policy files still cover the chosen canonical clusters and that the reviewer note preserves the locked boundaries from Phases 7 and 8

## Validation Architecture

### Quick checks

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_policy_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture`
- `python -m py_compile scripts/check_phase9_targeted_recovery.py`

### Artifact checks

- `rg -n 'module_rules.json|compatibility_rules.json|Phase 9 targeted recovery scope' tools/apdr/data/recovery/README.md`
- `rg -n 'torch|torchvision|tensorflow|keras|tensorboard|PyJWT|python-dateutil|pkg_resources|Image' tools/apdr/data/recovery/module_rules.json tools/apdr/data/recovery/compatibility_rules.json`
- `python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json`

### Phase-close checks

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_policy_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`
- `python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json`

## Canonical Files For Planning

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-BASELINE.md`
- `.planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-RESEARCH.md`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-VALIDATION.md`
- `.planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`
- `tools/apdr/src/resolver/retry_loop.rs`
- `tools/apdr/src/resolver/recovery_diagnostics.rs`
- `tools/apdr/src/resolver/tier3_llm/core.rs`
- `tools/apdr/src/resolver/family_knowledge/core.rs`
- `tools/apdr/src/resolver/family_knowledge/data.rs`
- `tools/apdr/tests/test_resolver.rs`
- `scripts/analyze_missing_modules.py`
- `scripts/analyze_build_failures.py`
- `scripts/find_more_aliases.py`

## Out-of-Scope For This Phase

- changing the canonical `70`-case slice, the `17`-case watchlist split, or the `87`-case APDR versus `pllm` overlap set
- reopening the Phase 8 touched-family migration boundary or moving unrelated families into `touched_families.json`
- rerunning the full benchmark as part of Phase 9 closeout; that belongs to Phase 10
- broad performance work, async refactors, or provider replacement
- touching unrelated local edits in `tools/apdr/src/lib.rs` or `tools/apdr/llm_py/tests/test_llm_integration.py`

---
*Research created: 2026-03-28*
*Phase: 09-targeted-tier3-recovery-accuracy*
