# Roadmap: APDR Enhancement

**Project:** APDR Enhancement - Accuracy & Performance
**Created:** 2026-03-25
**Granularity:** Standard (5-8 phases)

## Phases

- [ ] **Phase 1: Non-blocking UI Foundation** - Responsive UI with real-time streaming
- [ ] **Phase 2: Result Categorization & Insights** - Deterministic/LLM split view with filtering
- [ ] **Phase 3: LLM Recovery Accuracy** - Improved error recovery with validation and confidence scoring
- [ ] **Phase 4: LLM Performance Optimization** - Batching, caching, and parallel inference
- [ ] **Phase 5: Docker Parallel Validation** - Concurrent Python version testing with BuildKit
- [ ] **Phase 6: End-to-End Validation** - Metrics verification and performance benchmarking

## Phase Details

### Phase 1: Non-blocking UI Foundation
**Goal**: Users experience responsive UI with real-time progress during benchmark runs

**Depends on**: Nothing (foundation phase)

**Requirements**: UI-01, UI-02, UI-03, UI-04, UI-05, RT-01, RT-02, RT-03, RT-04, RT-05

**Success Criteria** (what must be TRUE):
1. User sees interactive UI within 500ms of page load (no 3+ second freeze)
2. User sees results appear within 50ms of case completion (incremental updates)
3. User can interact with browser during 100+ case runs (no freeze/hang)
4. User observes smooth animations at 60fps during result updates (no jank)
5. User runs 30-minute benchmarks without memory growth >50MB (stable performance)

**Plans**: 3 plans in 2 waves

Plans:
- [x] 01-01-PLAN.md — Backend SSE infrastructure (Wave 1, autonomous)
- [ ] 01-02-PLAN.md — Frontend real-time client (Wave 1, autonomous)
- [ ] 01-03-PLAN.md — UI component integration with verification checkpoint (Wave 2)

**UI hint**: yes

---

### Phase 2: Result Categorization & Insights
**Goal**: Users see deterministic results separated from LLM attempts with rich filtering and insights

**Depends on**: Phase 1 (streaming infrastructure)

**Requirements**: CAT-01, CAT-02, CAT-03, CAT-04, CAT-05, CAT-06, CAT-07, CAT-08, LLM-01, LLM-02, LLM-03, LLM-04

**Success Criteria** (what must be TRUE):
1. User sees two distinct result sections: deterministic (tier1/tier2) and LLM (tier3)
2. User filters results by status, tier, and Python version with immediate updates
3. User searches cases by ID or snippet content
4. User sees color-coded pass/fail indicators at a glance
5. User views cache hit rate dashboard showing tier1/tier2/tier3 breakdown with percentages
6. User sees confidence-based skip indicators when LLM skips low-confidence cases
7. User expands case details to view full logs and resolution path

**Plans**: TBD

**UI hint**: yes

---

### Phase 3: LLM Recovery Accuracy
**Goal**: LLM recovery suggestions are validated and contextually accurate

**Depends on**: Phase 1 (result streaming)

**Requirements**: REC-01, REC-02, REC-03, REC-04, REC-05

**Success Criteria** (what must be TRUE):
1. User sees only PyPI-validated package suggestions (no invalid packages)
2. User benefits from RAG-enhanced recovery using error pattern library
3. User's cached suggestions invalidate when prompts or models change (no stale answers)
4. User sees recovery attempts skip when confidence score <0.4 (avoid bad suggestions)
5. User observes max 5 recovery attempts per case (prevents infinite retry loops)

**Plans**: TBD

---

### Phase 4: LLM Performance Optimization
**Goal**: LLM inference is fast and cost-efficient through batching and caching

**Depends on**: Phase 1 (streaming infrastructure), Phase 3 (recovery logic)

**Requirements**: PERF-01, PERF-02, PERF-03, PERF-04, PERF-05

**Success Criteria** (what must be TRUE):
1. User experiences prompt cache warmup on startup (2-4s initial, 90% savings after)
2. User benefits from batched LLM requests (5-10 parallel instead of sequential)
3. User's duplicate import combinations return cached responses (no redundant calls)
4. User observes Ollama configured for parallel execution (OLLAMA_NUM_PARALLEL=4-8)
5. User sees P95 latency <3 seconds for batch completion

**Plans**: TBD

---

### Phase 5: Docker Parallel Validation
**Goal**: Docker validation runs Python versions concurrently with optimized caching

**Depends on**: Phase 1 (result streaming for parallel builds)

**Requirements**: DOCK-01, DOCK-02, DOCK-03, DOCK-04, DOCK-05

**Success Criteria** (what must be TRUE):
1. User sees 4 Python versions validated in parallel (not sequential)
2. User benefits from BuildKit cache mounts for pip (70%+ build time reduction)
3. User experiences zero cache corruption from parallel builds (sharing=locked prevents races)
4. User observes Dockerfile layer ordering prevents cache invalidation on code changes
5. User completes parallel validation in 80s for 4 versions (down from 240s sequential)

**Plans**: TBD

---

### Phase 6: End-to-End Validation
**Goal**: Performance and accuracy metrics meet or exceed targets across all phases

**Depends on**: Phase 2 (result categorization), Phase 3 (recovery accuracy), Phase 4 (LLM performance), Phase 5 (Docker performance)

**Requirements**: MET-01, MET-02, MET-03, MET-04, MET-05

**Success Criteria** (what must be TRUE):
1. User observes overall accuracy >75% baseline on hard-gists benchmark
2. User sees LLM-assisted cases achieve ≥50% pass rate relative to total LLM cases
3. User runs 30-minute benchmarks with browser memory growth <50MB
4. User experiences LLM throughput of 4-8 requests/sec (up from 1 req/sec)
5. User completes Docker validation 60%+ faster than sequential baseline

**Plans**: TBD

---

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Non-blocking UI Foundation | 1/3 | In Progress|  |
| 2. Result Categorization & Insights | 0/TBD | Not started | - |
| 3. LLM Recovery Accuracy | 0/TBD | Not started | - |
| 4. LLM Performance Optimization | 0/TBD | Not started | - |
| 5. Docker Parallel Validation | 0/TBD | Not started | - |
| 6. End-to-End Validation | 0/TBD | Not started | - |

---

## Dependencies

```
Phase 1: Non-blocking UI Foundation (FOUNDATION)
   ├─→ Phase 2: Result Categorization & Insights
   ├─→ Phase 3: LLM Recovery Accuracy
   │      └─→ Phase 4: LLM Performance Optimization
   └─→ Phase 5: Docker Parallel Validation
          └─→ Phase 6: End-to-End Validation
```

**Critical Path**: Phase 1 → Phase 3 → Phase 4 → Phase 6

**Parallel Opportunities**:
- Phase 2 can run parallel with Phase 3
- Phase 5 can run parallel with Phase 3-4 (independent infrastructure)

---

*Roadmap created: 2026-03-25*
*Last updated: 2026-03-25 (Phase 1 planned)*
