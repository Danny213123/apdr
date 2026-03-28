# Roadmap: APDR

**Project:** APDR v2.1 - Data-Driven Family Knowledge & LLM Recovery Accuracy
**Created:** 2026-03-27
**Granularity:** Standard (4 phases)

## Milestones

- [ ] **v2.1 Data-Driven Family Knowledge & LLM Recovery Accuracy** - Phases 7-10
- [x] `v2.0` Rust Codebase Modernization - shipped 2026-03-28, archived in `.planning/milestones/v2.0-ROADMAP.md`
- [x] `v1.0` Accuracy & Performance - shipped 2026-03-27, archived in `.planning/milestones/v1.0-ROADMAP.md`

## Roadmap v2.1: Data-Driven Family Knowledge & LLM Recovery Accuracy

## Phases

- [x] **Phase 7: Failure Baseline & Parity Slice** - Turn the stopped APDR run and `pllm` summary into a reproducible target slice with baseline fixtures (completed 2026-03-28)
- [x] **Phase 8: Data-Driven Family Knowledge Runtime** - Move touched family-knowledge behavior into validated data files with regression protection (completed 2026-03-28)
- [ ] **Phase 9: Targeted Tier3 Recovery Accuracy** - Improve APDR recovery on the dominant parity-slice failure buckets
- [ ] **Phase 10: Benchmark Verification & Accuracy Closeout** - Rerun the targeted slice, prove deltas, and package the remaining gaps

## Phase Details

### Phase 7: Failure Baseline & Parity Slice
**Goal**: Turn the stopped APDR run and `pllm` comparison into a reproducible, bounded milestone target before changing behavior

**Depends on**: Nothing (foundation phase for v2.1)

**Requirements**: FAM-04, REC-01

**Success Criteria** (what must be TRUE):
1. A reproducible case list exists for the APDR-failed and `pllm`-passing slice shared by `runs\20260327-150339-apdr` and `pllm_results`
2. Target cases are labeled by tier and dominant failure bucket so later fixes can be measured instead of guessed
3. Touched family-knowledge cases have regression fixtures or snapshots that lock current intended behavior before migration
4. The milestone has a bounded improvement target rather than an open-ended accuracy wish list

**Plans:** 3/3 plans complete

---

### Phase 8: Data-Driven Family Knowledge Runtime
**Goal**: Replace hardcoded touched family-knowledge behavior with validated data-driven configuration

**Depends on**: Phase 7 (bounded target slice and baseline fixtures)

**Requirements**: FAM-01, FAM-02, FAM-03

**Success Criteria** (what must be TRUE):
1. Touched family-knowledge bundles move from hardcoded Rust tables into data files with a validated load path
2. Maintainers can update aliases, mappings, and rejection hints for touched families without changing Rust code
3. Invalid or conflicting family-knowledge data fails with actionable diagnostics
4. Existing intended family-knowledge behavior for the touched slice stays covered by tests or fixtures

**Plans:** 3/3 plans complete

---

### Phase 9: Targeted Tier3 Recovery Accuracy
**Goal**: Improve APDR recovery on the dominant failure buckets from the parity slice using the new family-knowledge path and targeted recovery fixes

**Depends on**: Phase 7 (target slice), Phase 8 (data-driven family knowledge)

**Requirements**: REC-02, REC-03, REC-04

**Success Criteria** (what must be TRUE):
1. Targeted `module-not-found` outcomes fall on the milestone parity slice
2. Targeted `version-not-found` and dependency-mapping failures fall on the parity slice
3. APDR recovers more of the 87 APDR-failed but `pllm`-passing cases than the March 27, 2026 baseline
4. Recovery changes keep failure reasons inspectable instead of hiding them behind generic retries

---

### Phase 10: Benchmark Verification & Accuracy Closeout
**Goal**: Rerun the targeted benchmark slice, prove the accuracy delta, and record the remaining unrecovered cases clearly

**Depends on**: Phase 9 (targeted recovery changes)

**Requirements**: REC-05, EVD-01, EVD-02

**Success Criteria** (what must be TRUE):
1. Benchmark artifacts report case-level APDR versus baseline versus `pllm` deltas for the targeted slice
2. Existing passed cases and expected skip behavior remain intact on the rerun
3. Remaining unrecovered parity cases are grouped by dominant failure bucket with follow-on notes
4. Milestone closeout leaves the next benchmark comparison path repeatable and reviewer-readable

---

## Progress

| Phase | Requirements | Status | Completed |
|-------|--------------|--------|-----------|
| 7. Failure Baseline & Parity Slice | FAM-04, REC-01 | Complete | 2026-03-28 |
| 8. Data-Driven Family Knowledge Runtime | FAM-01, FAM-02, FAM-03 | Complete | 2026-03-28 |
| 9. Targeted Tier3 Recovery Accuracy | 1/3 | In Progress|  |
| 10. Benchmark Verification & Accuracy Closeout | 1/3 | In Progress|  |

---

## Dependencies

```
Phase 7: Failure Baseline & Parity Slice (FOUNDATION)
   -> Phase 8: Data-Driven Family Knowledge Runtime
      -> Phase 9: Targeted Tier3 Recovery Accuracy
         -> Phase 10: Benchmark Verification & Accuracy Closeout
```

**Critical Path**: Phase 7 -> Phase 8 -> Phase 9 -> Phase 10

**Parallel Opportunities**:
- Small data-model validation helpers can begin once the Phase 7 slice and baseline fixtures exist
- Phase 10 artifact structure can be outlined while Phase 9 fixes are landing, but final evidence waits on the rerun

---

*Roadmap created: 2026-03-27*
*Last updated: 2026-03-28 after Phase 8 completion*
