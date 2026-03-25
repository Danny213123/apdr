# Requirements: APDR Enhancement

**Defined:** 2026-03-25
**Core Value:** The benchmark UI must stay responsive and show real-time progress during runs

## v1 Requirements

Requirements for accuracy and performance improvements. Each maps to roadmap phases.

### UI Responsiveness

- [ ] **UI-01**: UI becomes interactive within 500ms of page load (not 3+ seconds)
- [ ] **UI-02**: Results stream to UI within 50ms of worker completion (no batching delay)
- [ ] **UI-03**: Browser stays responsive during 100+ concurrent case runs (no freeze)
- [ ] **UI-04**: Frame rate sustains 60fps during result updates (no jank)
- [ ] **UI-05**: Memory usage remains stable in long-running benchmarks (no leaks)

### Real-time Updates

- [ ] **RT-01**: Benchmark progress updates in real-time via Server-Sent Events
- [ ] **RT-02**: Visual progress bar shows completion percentage
- [ ] **RT-03**: Results appear incrementally as cases complete (not after entire batch)
- [ ] **RT-04**: Case status updates immediately (pending → running → pass/fail/skip)
- [ ] **RT-05**: Active case count updates in real-time

### Result Categorization

- [ ] **CAT-01**: Deterministic results display separately from LLM results (two sections)
- [ ] **CAT-02**: Results filterable by status (pass, fail, skip, timeout)
- [ ] **CAT-03**: Results filterable by resolution tier (tier1 cache, tier2 heuristic, tier3 LLM)
- [ ] **CAT-04**: Results filterable by Python version
- [ ] **CAT-05**: Case search by ID or snippet content
- [ ] **CAT-06**: Pass/fail status indicators color-coded and visible at glance
- [ ] **CAT-07**: Error messages categorized (build failure, import error, version conflict, timeout)
- [ ] **CAT-08**: Expandable case details show full logs and resolution path

### LLM Insights

- [ ] **LLM-01**: Cache hit rate dashboard shows tier1/tier2/tier3 breakdown with percentages
- [ ] **LLM-02**: Confidence-based skip indicators surface when LLM skips case (<0.4 threshold)
- [ ] **LLM-03**: Import-set cache reuse indicator shows when exact import combination cached
- [ ] **LLM-04**: LLM recovery attempts show which error pattern triggered from pattern library

### Recovery Accuracy

- [ ] **REC-01**: Recovery suggestions validate package exists on PyPI before suggesting
- [ ] **REC-02**: Error pattern matching uses RAG-enhanced recovery prompts
- [ ] **REC-03**: Cache invalidation based on prompt hash + model ID (prevent stale suggestions)
- [ ] **REC-04**: Recovery confidence scoring to skip low-confidence suggestions
- [ ] **REC-05**: Recovery attempt limit enforced (max 5 attempts per case)

### LLM Performance

- [ ] **PERF-01**: Prompt cache warmed on startup (2-4s initial cost, 90% savings on subsequent calls)
- [ ] **PERF-02**: LLM requests batched (5-10 parallel requests instead of sequential)
- [ ] **PERF-03**: Response caching prevents duplicate LLM calls for same imports
- [ ] **PERF-04**: Ollama configured for parallel execution (OLLAMA_NUM_PARALLEL=4-8)
- [ ] **PERF-05**: Batch size tuned for P95 latency <3 seconds

### Docker Performance

- [ ] **DOCK-01**: Docker validation runs 4 Python versions in parallel (not sequential)
- [ ] **DOCK-02**: BuildKit cache mounts configured for pip (70%+ build time reduction)
- [ ] **DOCK-03**: BuildKit cache mount locking prevents race conditions (sharing=locked)
- [ ] **DOCK-04**: Dockerfile layer ordering optimized to prevent cache invalidation
- [ ] **DOCK-05**: Parallel builds complete in 80s for 4 versions (currently 240s sequential)

### Metrics

- [ ] **MET-01**: Overall accuracy increases from 75% baseline
- [ ] **MET-02**: LLM-assisted cases achieve ≥50% pass rate relative to total LLM cases
- [ ] **MET-03**: Browser memory growth <50MB over 30-minute benchmark run
- [ ] **MET-04**: LLM throughput increases to 4-8 requests/sec (from 1 req/sec)
- [ ] **MET-05**: Docker validation time reduced by ≥60% vs sequential baseline

## v2 Requirements

Deferred to future enhancements based on user feedback.

### Advanced Visualizations

- **VIZ-01**: LLM recovery attempt timeline visualization (shows retry sequence)
- **VIZ-02**: Parallel execution timeline (shows Docker build concurrency)
- **VIZ-03**: Historical comparison view (compare runs over time)
- **VIZ-04**: Pattern library match annotations (highlight which RAG patterns triggered)

### Performance Deep Dive

- **PERF-06**: Async Python migration (Flask → FastAPI, sync → async)
- **PERF-07**: Web Workers for heavy result parsing (only if >1000 results)
- **PERF-08**: Virtual scrolling for large result sets

## Out of Scope

Explicitly excluded to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Framework rewrite (React/Vue/Svelte) | Vanilla JS + Vite sufficient, rewrite adds zero user value |
| WebSocket bidirectional communication | SSE covers 95% of real-time use cases, simpler than WebSockets |
| Custom LLM provider abstraction | LiteLLM already provides multi-provider support |
| Mobile-responsive UI | Benchmark tool is desktop/server context, not mobile |
| Optimizing .clone() overhead (329 calls) | <5% of runtime, focus on sequential validation bottleneck (80%) |
| Multi-language support beyond Python | APDR is Python-specific tool |
| Removing Docker validation | Ground truth verification, necessary for correctness |

## Traceability

Populated during roadmap creation. Each requirement maps to exactly one phase.

| Requirement | Phase | Status |
|-------------|-------|--------|
| TBD | TBD | Pending |

**Coverage:**
- v1 requirements: 42 total
- Mapped to phases: 0 (awaiting roadmap)
- Unmapped: 42 ⚠️

---
*Requirements defined: 2026-03-25*
*Last updated: 2026-03-25 after initial definition*
