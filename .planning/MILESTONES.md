# Milestones

## v2.3 Tier3 Validation Recovery and Reliability (Shipped: 2026-04-01)

**Delivered:** A live-evidence-backed reliability milestone that repaired `llm` fallback stability, added targeted `env -> docker -> llm-agent` routing with truthful backend-path accounting, separated environment-specific failures from dependency misses, and proved fixed-slice tier3 recovery gains without hiding interrupted tail cases.

**Phases completed:** 17-21 (15 plans, 30 tasks)

**Key accomplishments:**

- Removed the LangGraph `confidence` collision and made non-pass fallback outcomes first-class artifact fields.
- Added targeted Docker escalation plus truthful `validation_path` and `escalated_backend` metadata for `llm` validation.
- Split environment-specific failures from dependency-resolution misses and cleaned resumed-run provenance and skip accounting.
- Added bounded dominant-bucket recovery rules and compatibility policy that improved the fixed March 30 slice.
- Closed v2.3 with a live evidence pack showing the fixed-slice baseline move from `0/9` passes to `2/9` passes, plus representative case artifacts and an explicit interrupted tail case.

**Stats:**

- 120 files modified
- 12,305 insertions, 431 deletions
- Milestone work: 2026-03-30 -> 2026-04-01, archived 2026-04-01

**Git range:** `0cda517` -> `8f02cd2`

**Audit note:** No standalone `v2.3-MILESTONE-AUDIT.md` was recorded; milestone completion is based on 15 completed plan summaries, 11/11 completed v2.3 requirements, and the Phase 21 live evidence pack.

**What's next:** Define the next milestone and recreate active requirements with `$gsd-new-milestone`.

---

## v2.0 Rust Codebase Modernization (Shipped: 2026-03-28)

**Delivered:** A benchmark-backed modernization of APDR's Rust core that improved hot-path efficiency, validation throughput, reviewability, and closeout evidence without dropping Windows or Docker support.

**Phases completed:** 1-6 (17 plans, 22 tasks)

**Key accomplishments:**

- Captured committed baseline, hotspot, regression, and memory guardrails before optimization work.
- Reduced resolver ownership churn and retry-loop overhead, then recorded before or after candidate deltas.
- Improved validation-path throughput with better env-attempt staging, cache reuse, and benchmark telemetry.
- Split the largest Rust modules into reviewable facades and added reviewer-facing docs plus a stable verification guide.
- Closed v2.0 with a green Rust review gate and targeted direct-APDR memory evidence that moved `BENCH-03` to pass.

**Stats:**

- 127 files modified
- 21,629 insertions, 9,271 deletions
- Milestone work: 2026-03-26 -> 2026-03-27, archived 2026-03-28

**Git range:** `759e5a1` -> `80e8d49`

**Audit note:** No standalone `v2.0-MILESTONE-AUDIT.md` was recorded; milestone completion is based on 17 completed plan summaries, 30/30 completed requirements, and the Phase 6 benchmark and signoff package.

**What's next:** Define the next milestone and recreate active requirements with `$gsd-new-milestone`.

---

## v1.0 Accuracy & Performance (Shipped: 2026-03-27)

**Phases completed:** 3 phases, 10 plans, 10 tasks

**Key accomplishments:**

- Files Modified

---
