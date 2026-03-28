# Milestone closeout

## Milestone Outcome

- Phases 1 through 6 now provide a full modernization trail: baseline capture, bounded performance deltas, validation telemetry, module-boundary cleanup, reviewer docs, panic-path hardening, and split benchmark evidence.
- The milestone evidence is strong enough to show that the modernization landed and that benchmark behavior is measurable, but v2.0 is not ready to close today.
- Two blockers remain open: `BENCH-03` is still mixed in `.planning/phases/06-benchmark-verification-and-v2-closeout/06-BENCHMARK-VERIFICATION.md`, and the final Rust verification gate is not fully green.

## Benchmark Evidence

- The benchmark-side signoff package is `.planning/phases/06-benchmark-verification-and-v2-closeout/06-BENCHMARK-VERIFICATION.md`.
- That package already separates the bounded continuity gate, the bounded hard-gists slice, the representative memory comparison, and the retained Phase 3 host-variance caveat instead of collapsing them into one claim.
- Read `.planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-DELTA.md` and `.planning/phases/06-benchmark-verification-and-v2-closeout/06-HARD-GISTS-SLICE.md` for the detailed metrics. This closeout keeps the signoff package split on purpose.

## Review Readiness

- `.planning/phases/04-module-layout-and-boundary-cleanup/04-01-SUMMARY.md`, `.planning/phases/04-module-layout-and-boundary-cleanup/04-02-SUMMARY.md`, and `.planning/phases/04-module-layout-and-boundary-cleanup/04-03-SUMMARY.md` establish the five modernized Rust areas as reviewable facades with named ownership boundaries.
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-REVIEWER-GUIDE.md` gives reviewers one stable map for those facades, their fallback behavior, and the inherited verification loop.
- `.planning/phases/05-documentation-error-handling-and-review-readiness/05-VALIDATION.md` and `.planning/phases/05-documentation-error-handling-and-review-readiness/05-03-SUMMARY.md` define the exact five-command review gate reused here.
- The 2026-03-27 rerun of that exact gate stayed green for `cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check`, `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver resolver_ -- --nocapture`, `cargo test --manifest-path tools/apdr/Cargo.toml validation_pipeline_ -- --nocapture`, and `cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings`.

## Remaining Variance and Risk

- `BENCH-03` remains mixed. `.planning/phases/06-benchmark-verification-and-v2-closeout/06-BENCHMARK-VERIFICATION.md` records that the representative `peak_rss_bytes` comparison rose from `19,595,264` to `19,845,120` (`+249,856 bytes`, `+1.28%`) on the apples-to-apples rerun.
- The final broad Rust gate failed on `cargo test --manifest-path tools/apdr/Cargo.toml -- --nocapture` in `wheelhouse_prune_removes_oldest_files` (`tools/apdr/tests/test_cache.rs`).
- The concrete failure was `assert_eq!(removed, 100)` with actual `removed == 150`.
- The current `prune_wheelhouse(...)` implementation in `tools/apdr/src/cache/maintenance.rs` sorts equal-mtime files by path. On this Windows filesystem, `old.whl` and `new.whl` can share the same modified timestamp, so the alphabetical tie-break can prune `new.whl` first and force both files to be removed to get under the byte cap.
- That blocker is not caused by the unrelated dirty worktree files `tools/apdr/src/lib.rs` or `tools/apdr/llm_py/tests/test_llm_integration.py`. Those files were intentionally left untouched throughout Phase 6.
- The retained Phase 3 Windows Docker forced-validation variance is still non-blocking host evidence, but it continues to limit strong claims about forced-validation performance on this machine.

## Final Signoff

- v2.0 is not ready for milestone completion on 2026-03-27.
- `BENCH-05` remains open until the wheelhouse-pruning gate failure is resolved and the exact five-command review loop from `.planning/phases/05-documentation-error-handling-and-review-readiness/05-REVIEWER-GUIDE.md` and `.planning/phases/05-documentation-error-handling-and-review-readiness/05-VALIDATION.md` passes again.
- After that gate is green, the project still needs an explicit decision on `BENCH-03`: either capture stronger memory evidence or accept the representative-memory result as a documented milestone caveat before archive.
