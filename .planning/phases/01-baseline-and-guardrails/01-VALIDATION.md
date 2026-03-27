---
phase: 01
slug: baseline-and-guardrails
status: complete
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-26
---

# Phase 1 - Validation Strategy

> Validation contract for baseline tooling, hotspot ranking, and regression gates.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test + Python smoke checks |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python -m py_compile scripts/measure_apdr_baseline.py scripts/profile_apdr_memory.py scripts/check_apdr_regression.py` |
| **Full suite command** | `cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli && python scripts/measure_apdr_baseline.py --help && python scripts/profile_apdr_memory.py --help && python scripts/check_apdr_regression.py --help` |
| **Estimated runtime** | ~45 seconds |

---

## Sampling Rate

- **After every task commit:** Run the quick command plus the task-specific verify command
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** Re-run the full suite command and spot-check the generated baseline artifacts
- **Max feedback latency:** 60 seconds for code changes, longer only for the bounded baseline capture step

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 1 | BASE-01 | script smoke + Rust contract | `python scripts/measure_apdr_baseline.py --help && cargo test --manifest-path tools/apdr/Cargo.toml --test test_cli` | yes | completed |
| 01-01-02 | 01 | 1 | BASE-02 | script smoke | `python scripts/profile_apdr_memory.py --help` | yes | completed |
| 01-01-03 | 01 | 1 | BASE-01 | artifact generation | `python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/01-baseline-and-guardrails/01-baseline.json --output-md .planning/phases/01-baseline-and-guardrails/01-BASELINE.md` | yes | completed |
| 01-02-01 | 02 | 2 | BASE-05 | script smoke | `python scripts/check_apdr_regression.py --help` | yes | completed |
| 01-02-02 | 02 | 2 | BASE-04 | artifact verification | `rg -n "resolver/mod.rs|docker/builder.rs|pre_solve.rs" .planning/phases/01-baseline-and-guardrails/01-HOTSPOT-AUDIT.md` | yes | completed |
| 01-02-03 | 02 | 2 | BASE-03 | docs verification | `rg -n "cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check|cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings|python scripts/check_apdr_regression.py" tools/apdr/README.md` | yes | completed |

---

## Wave 0 Requirements

- Existing infrastructure covers all phase requirements.
- No framework install is required before execution.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Representative baseline capture runs against the intended sample set | BASE-01, BASE-02 | Runtime depends on local interpreters, dataset availability, and chosen validation backend | Run the documented baseline command, confirm the sample list is recorded, and verify the resulting JSON and Markdown artifacts describe the same run |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit artifact check
- [x] Sampling continuity avoids long stretches without automated feedback
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Quick feedback latency stays under 60 seconds for code paths
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** passed
