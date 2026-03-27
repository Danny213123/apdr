---
phase: 04-module-layout-and-boundary-cleanup
plan: 03
subsystem: resolver-support
tags:
  - apdr
  - rust
  - resolver
  - pypi
  - llm
  - family-knowledge
  - module-layout
dependency_graph:
  requires:
    - 04-02
  provides:
    - family-knowledge-facade
    - pypi-client-facade
    - tier3-llm-facade
    - learned-family-submodule
    - smartpip-submodule
    - llm-context-and-failure-memory-submodules
  affects:
    - 05
    - 06
tech_stack:
  added: []
  patterns:
    - directory-backed-facades
    - support-module-extraction
    - deterministic-cache-pruning
key_files:
  created:
    - tools/apdr/src/resolver/family_knowledge/mod.rs
    - tools/apdr/src/resolver/family_knowledge/detection.rs
    - tools/apdr/src/resolver/family_knowledge/learned.rs
    - tools/apdr/src/resolver/family_knowledge/legacy_bundles.rs
    - tools/apdr/src/resolver/family_knowledge/core.rs
    - tools/apdr/src/resolver/pypi_client/mod.rs
    - tools/apdr/src/resolver/pypi_client/host_python.rs
    - tools/apdr/src/resolver/pypi_client/smartpip.rs
    - tools/apdr/src/resolver/pypi_client/version_matching.rs
    - tools/apdr/src/resolver/pypi_client/core.rs
    - tools/apdr/src/resolver/tier3_llm/mod.rs
    - tools/apdr/src/resolver/tier3_llm/process.rs
    - tools/apdr/src/resolver/tier3_llm/context.rs
    - tools/apdr/src/resolver/tier3_llm/failure_memory.rs
    - tools/apdr/src/resolver/tier3_llm/core.rs
  modified:
    - tools/apdr/src/cache/maintenance.rs
key-decisions:
  - Added a `core.rs` implementation file behind each new directory-backed resolver module so `mod.rs` could stay as a small facade and still preserve the public module paths.
  - Kept public entrypoints stable by re-exporting through `mod.rs` rather than forcing callers to adopt new internal paths.
  - Fixed deterministic wheelhouse pruning while verifying the full suite because the structural refactor surfaced an existing mtime-tie failure in cache tests on this filesystem.
patterns-established:
  - "Large support-heavy Rust modules should collapse into thin `mod.rs` facades plus named sibling modules."
  - "When structural splits expose unrelated nondeterminism in verification, fix the deterministic behavior instead of weakening the test."
requirements-completed:
  - ARCH-02
  - ARCH-03
  - ARCH-04
  - ARCH-05
metrics:
  completed_date: "2026-03-27"
  tasks_completed: 3
  verification_tests: 6
---

# Phase 4 Plan 03 Summary

**Finished the Phase 4 boundary cleanup by turning the remaining support-heavy resolver files into directory-backed facades with named internal modules, then reverified the full Rust suite.**

## Accomplishments

- Split [`family_knowledge`](D:\apdr\tools\apdr\src\resolver\family_knowledge\mod.rs) into a thin facade plus [`detection.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\detection.rs), [`legacy_bundles.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\legacy_bundles.rs), [`learned.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\learned.rs), and [`core.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\core.rs).
- Split [`pypi_client`](D:\apdr\tools\apdr\src\resolver\pypi_client\mod.rs) into a facade plus [`smartpip.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\smartpip.rs), [`version_matching.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\version_matching.rs), [`host_python.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\host_python.rs), and [`core.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\core.rs).
- Split [`tier3_llm`](D:\apdr\tools\apdr\src\resolver\tier3_llm\mod.rs) into a facade plus [`process.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\process.rs), [`context.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\context.rs), [`failure_memory.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\failure_memory.rs), and [`core.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\core.rs).
- Preserved the public module call sites while pushing implementation families into sibling modules with explicit ownership names.
- Fixed [`prune_wheelhouse(...)`](D:\apdr\tools\apdr\src\cache\maintenance.rs) to break mtime ties by path so full-suite cache pruning stays deterministic on Windows filesystems with coarse timestamp resolution.

## Verification Results

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture` passed
- `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings` passed
- `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture` passed
- `rg -n "mod legacy_bundles;|mod learned;|mod detection;|mod smartpip;|mod version_matching;|mod host_python;|mod process;|mod context;|mod failure_memory;" tools/apdr/src/resolver` passed
- `@(Get-Content tools/apdr/src/resolver/family_knowledge/mod.rs).Count` returned `11`
- `@(Get-Content tools/apdr/src/resolver/pypi_client/mod.rs).Count` returned `6`
- `@(Get-Content tools/apdr/src/resolver/tier3_llm/mod.rs).Count` returned `5`

## Files Created/Modified

- [`family_knowledge/mod.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\mod.rs) - facade and stable re-exports for family knowledge.
- [`family_knowledge/detection.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\detection.rs) - namespace validation and legacy-stack detection helpers.
- [`family_knowledge/legacy_bundles.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\legacy_bundles.rs) - curated bundle application and compatibility pinning helpers.
- [`family_knowledge/learned.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\learned.rs) - learned-family persistence and lookup behavior.
- [`pypi_client/mod.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\mod.rs) - facade and public client re-exports.
- [`pypi_client/smartpip.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\smartpip.rs) - SmartPip TCP/process orchestration and KGraph access helpers.
- [`pypi_client/version_matching.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\version_matching.rs) - constraint parsing and version comparison logic.
- [`pypi_client/host_python.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\host_python.rs) - host Python discovery and execution helpers.
- [`tier3_llm/mod.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\mod.rs) - facade and stable Tier 3 exports.
- [`tier3_llm/process.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\process.rs) - Python subprocess lifecycle and IPC.
- [`tier3_llm/context.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\context.rs) - request context assembly and local-helper filtering.
- [`tier3_llm/failure_memory.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\failure_memory.rs) - prior-failure loading, formatting, and trace persistence.
- [`maintenance.rs`](D:\apdr\tools\apdr\src\cache\maintenance.rs) - deterministic wheelhouse prune ordering for stable verification.

## Decisions Made

- Used `core.rs` implementation files as the internal landing zone for the remaining public logic so the top-level `mod.rs` files stayed reviewer-friendly and below the line-count targets.
- Re-exported `version_satisfies`, learned-family APIs, and namespace helpers from the facade modules to keep existing module paths intact for tests and callers.
- Accepted one tightly scoped non-Phase-4 fix in cache maintenance because it was the only blocker to a passing full verification run and it improved deterministic behavior rather than changing feature scope.

## Deviations from Plan

- Added `core.rs` under each new directory-backed module in addition to the planned sibling files. This kept the public module paths stable while satisfying the requirement that `mod.rs` stay small and reviewable.

## Issues Encountered

- Windows PowerShell made large text moves awkward enough that the split had to be done as mechanical file rewrites rather than hand-editing thousands of lines.
- The family-knowledge split needed extra visibility tightening (`pub(super)`) because sibling modules now owned helpers that used to be in one flat file.
- Full verification exposed nondeterministic wheelhouse pruning unrelated to the structural split; that had to be fixed before the full suite would pass consistently.

## Next Phase Readiness

- Phase 5 can now document behavior and standardize error handling against named modules instead of monolithic files.
- Reviewers can trace resolver support behavior through facades first, then dive into the specific module that owns detection, network lookup, persistence, or process logic.

## Self-Check: PASSED

- [`family_knowledge/mod.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\mod.rs) contains `mod legacy_bundles;`, `mod learned;`, and `mod detection;`
- [`pypi_client/mod.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\mod.rs) contains `mod smartpip;`, `mod version_matching;`, and `mod host_python;`
- [`tier3_llm/mod.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\mod.rs) contains `mod process;`, `mod context;`, and `mod failure_memory;`
- [`legacy_bundles.rs`](D:\apdr\tools\apdr\src\resolver\family_knowledge\legacy_bundles.rs) contains `fn apply_legacy_scrapy_bundle(`
- [`version_matching.rs`](D:\apdr\tools\apdr\src\resolver\pypi_client\version_matching.rs) contains `fn compare_versions(`
- [`failure_memory.rs`](D:\apdr\tools\apdr\src\resolver\tier3_llm\failure_memory.rs) contains `fn load_failure_memory(`
- All planned Wave 3 verification commands passed

---
*Phase: 04-module-layout-and-boundary-cleanup*
*Completed: 2026-03-27*
