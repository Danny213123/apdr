# Feature Landscape

**Domain:** Benchmark UI + LLM Error Recovery Systems
**Researched:** 2026-03-25
**Confidence:** HIGH (verified against current tools and patterns)

## Table Stakes

Features users expect. Missing = product feels incomplete or broken.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| **Non-blocking UI during runs** | Browser freezes = unacceptable UX in 2025+ web apps | Medium | Users expect progressive loading, not 3+ second hangs. Next.js streaming, SSE are standard patterns. |
| **Real-time progress updates** | Waiting for entire batch = poor feedback loop | Medium | Test runners (Vitest, Wallaby.js), ML tools (Neptune.ai) all stream progress. Users expect "live" dashboards with updates every 1-2 seconds. |
| **Results as they complete** | Immediate feedback > waiting for batch completion | Medium | Standard pattern: show deterministic cache hits immediately (~5ms), then stream LLM results as ready. SSE handles 95% of real-time use cases. |
| **Visual progress indicators** | Users need to know "is it working?" | Low | Progress bars, gauges, percentage complete. Management dashboards universally show real-time KPI tracking. Missing this = looks unfinished. |
| **Error categorization** | "It failed" is useless without reason | Medium | Test automation dashboards (ReportPortal, Allure) categorize failures: Product Bugs, Automation Bugs, System Issues. LLM evaluation tools separate model failures from non-model issues. |
| **Pass/fail status indicators** | Binary outcome must be obvious at glance | Low | Color-coded status (green/yellow/red), visual indicators. Universal pattern in CI/CD and test runners. |
| **Filterable results** | Users need to focus on specific subsets | Medium | Filter by status, category, tier, Python version. Up to 10 levels of categorization in modern test dashboards. Mandatory for >100 test cases. |
| **Historical comparison** | "Did this improve?" requires baseline | Medium | Test automation tools show trends across builds. ML benchmarks compare configurations. At minimum: show previous run vs current run delta. |
| **Detailed failure logs** | Root cause analysis requires full context | Low | Link to stack traces, build logs, error messages. Every test automation tool provides drill-down. Missing this = can't debug failures. |
| **Retry/rerun capability** | Flaky tests and transient errors are reality | Low | Standard in CI/CD (retry once before fail). Users expect to rerun individual failed cases without re-running entire suite. |

## Differentiators

Features that set product apart. Not expected, but valued.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Deterministic vs LLM split view** | Unique to APDR's multi-tier architecture | Medium | Show tier1/tier2 (cache/heuristic) separate from tier3 (LLM). Helps users understand **why** results differ and **where** time is spent. Analogous to: ML benchmarks separating training vs inference time. |
| **Confidence-based skip indicators** | Transparency into LLM's self-assessment | Low | Surface when confidence agent skips case (<0.4 threshold). Users see "skipped: low confidence" not just "failed". Shows system intelligence. |
| **LLM recovery attempt visualization** | Show iterative fixing process (max 5 attempts) | High | Timeline/graph showing: attempt 1 (import error) → recovery → attempt 2 (version conflict) → recovery → success. Unique to multi-agent LLM systems. Educational for users. |
| **Cache hit rate dashboard** | Performance insight users care about | Low | Show tier1/tier2/tier3 breakdown with percentages. Helps users understand if their workload benefits from caching. Actionable: "warm cache to improve hit rate". |
| **Pattern library match annotations** | Show which error pattern triggered recovery | Medium | When recovery agent matches build failure to known pattern, annotate result: "Matched: legacy Flask stack pattern". Unique to RAG-enhanced recovery. |
| **Comparative tier performance** | Show what each tier contributes | Medium | Side-by-side: tier1 (100 passes, 5ms avg), tier2 (50 passes, 50ms avg), tier3 (25 passes, 30s avg). Helps justify LLM cost/latency tradeoff. |
| **Import-set cache reuse indicator** | Show when exact import combination is cached | Low | Badge: "Cached solution (5ms path)". Users see instant gratification when cache works. Encourages running related benchmarks. |
| **Unsolvable module learning tracker** | Show LLM's persistent learnings | Medium | Display when case skips due to prior LLM determination (unsolvable_modules.tsv). Shows system "learns" from failures. Prevents wasted retries. |
| **Recovery action breakdown** | Categorize recovery actions attempted | Medium | Pie chart: 40% version changes, 30% add packages, 20% system deps, 10% Python version switch. Helps understand what recovery strategies work. |
| **Parallel execution timeline** | Visualize concurrent Docker builds | High | Gantt chart showing parallel Python version tests (3.9, 3.10, 3.11 running simultaneously). Unique to APDR's parallel validation. Shows parallelization benefit. |

## Anti-Features

Features to explicitly NOT build.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| **Framework rewrite (React/Vue/Svelte)** | Vanilla JS + Vite works, rewrite adds zero user value | Enhance existing vanilla JS with modern patterns (SSE streaming, progressive loading). Tech stack churn is anti-pattern. |
| **WebSocket bidirectional communication** | APDR is server → client updates only. SSE simpler and covers 95% of real-time use cases. | Use Server-Sent Events (SSE) for streaming results. WebSocket overkill when client doesn't send updates during run. |
| **Custom LLM provider abstraction layer** | LiteLLM already provides this. Reinventing wheel. | Stick with LiteLLM's provider abstraction. Focus on prompt quality, not infrastructure. |
| **Real-time collaborative features** | APDR is single-user tool. Multi-user editing/commenting irrelevant. | Keep focus on individual researcher workflow. Don't add social features nobody asked for. |
| **Automatic performance tuning** | Users need control over validation backend, cache settings, parallelism. "Magic" settings break transparency. | Provide knobs (CLI flags, config), not auto-tuning. Explicit > implicit for research tools. |
| **Custom dependency solver UI** | PubGrub solver is implementation detail. Users care about results, not algorithm internals. | Show final lockfile, not solver visualization. Keep complexity hidden unless debugging. |
| **LLM prompt playground** | APDR is benchmark tool, not prompt engineering IDE. Scope creep. | Prompts are code (Python files). Users edit with IDE, not in-browser playground. |
| **Mobile-responsive UI** | Benchmark runs on desktop/server. Mobile access is non-goal. | Desktop-first UI. Don't waste effort on responsive breakpoints. |
| **Export to 20 formats** | JSON + CSV covers 99% of use cases. Format proliferation adds maintenance burden. | JSON (machine-readable) + CSV (spreadsheet import). Add formats only if users request specific one. |

## Feature Dependencies

```
Real-time progress updates → Non-blocking UI (can't stream if UI blocks)
Deterministic vs LLM split view → Error categorization (need to tag tier source)
LLM recovery attempt visualization → Detailed failure logs (need attempt history)
Cache hit rate dashboard → Results as they complete (need tier tracking)
Historical comparison → Filterable results (need consistent schema across runs)
Parallel execution timeline → Real-time progress updates (need concurrent event stream)
```

## MVP Recommendation

### Phase 1: Foundation (Non-blocking + Streaming)

Prioritize:
1. **Non-blocking UI during runs** — Fixes critical UX issue (browser freezes)
2. **Real-time progress updates** — Stream via SSE, update every 1-2 seconds
3. **Results as they complete** — Show each case immediately (don't batch)
4. **Visual progress indicators** — Progress bar, X/Y complete, ETA

**Rationale:** Eliminates "feels broken" experience. Users can see system is working.

### Phase 2: Categorization (Core Table Stakes)

Prioritize:
5. **Error categorization** — Product bug, automation bug, system issue, LLM recovery failed
6. **Pass/fail status indicators** — Color-coded, obvious at glance
7. **Filterable results** — By status, tier, Python version
8. **Detailed failure logs** — Link to build.log, run.log, combined.log

**Rationale:** Users need to understand **why** cases pass/fail. Categorization is mandatory for >50 cases.

### Phase 3: Differentiation (APDR-Specific Value)

Prioritize:
9. **Deterministic vs LLM split view** — Unique to APDR's architecture
10. **Cache hit rate dashboard** — Show tier1/tier2/tier3 breakdown
11. **Confidence-based skip indicators** — Surface LLM's self-assessment
12. **Import-set cache reuse indicator** — Show 5ms path wins

**Rationale:** Features that showcase APDR's unique multi-tier + LLM approach. Differentiates from generic test runners.

### Phase 4: Advanced Insights (Nice-to-Have)

Defer:
- **LLM recovery attempt visualization** — High complexity, educational but not critical
- **Pattern library match annotations** — Medium complexity, helps debugging but not MVP
- **Parallel execution timeline** — High complexity, shows parallelization but users feel benefit without visualization
- **Historical comparison** — Medium complexity, valuable but not blocking

**Defer reasoning:** These require significant UI work and don't block core workflow. Add based on user feedback.

## Complexity Breakdown

### Low Complexity (1-2 days each)
- Visual progress indicators (progress bar component)
- Pass/fail status indicators (color-coded badges)
- Detailed failure logs (links to artifact files)
- Retry/rerun capability (API endpoint + UI button)
- Cache hit rate dashboard (aggregate tier stats)
- Import-set cache reuse indicator (badge when cached)
- Confidence-based skip indicators (show skip reason)

### Medium Complexity (3-5 days each)
- Non-blocking UI during runs (refactor Flask server to SSE)
- Real-time progress updates (SSE event stream + client handler)
- Results as they complete (incremental DOM updates)
- Error categorization (taxonomy + classifier integration)
- Filterable results (filter UI + state management)
- Historical comparison (schema versioning + diff view)
- Deterministic vs LLM split view (two-column layout + routing)
- Pattern library match annotations (link to matched pattern)
- Comparative tier performance (aggregation + chart component)
- Unsolvable module learning tracker (query cache + display)
- Recovery action breakdown (categorize + pie chart)

### High Complexity (1-2 weeks each)
- LLM recovery attempt visualization (timeline/graph component + data model)
- Parallel execution timeline (Gantt chart + event correlation)

## Sources

**Real-time Dashboard Patterns:**
- Next.js App Router Streaming (2025) — https://nextjs.org/learn/dashboard-app/streaming
- SSE vs WebSockets (2025) — Server-Sent Events cover 95% of real-time use cases, simpler than WebSockets for one-way streaming
- Wallaby.js test runner — Real-time code coverage updates as you type

**Test Automation Dashboards:**
- ReportPortal — Up to 10 levels of categorization, failure classification (Product/Automation/System), custom filters
- Allure Report — Stack traces, screenshots, detailed failure analysis
- Test automation best practices (2025) — Retry failed tests once before marking failed

**LLM Evaluation Tools:**
- Confident AI, Braintrust, Langfuse (2025-2026) — Custom dashboards, threshold-based alerts, multi-step trace visualization
- Datadog LLM Observability — Live dashboards with drill-down, error pattern surfacing, latency/token tracking

**ML Benchmark Tools:**
- Neptune.ai — Real-time monitoring, configurable dashboards, metrics visualization
- LLM Benchmark Suite (GitHub) — Non-blocking UI with threaded execution, live updates every 1 second
- UMLAUT End-to-End ML Benchmark — Metrics visualization across all pipeline stages

**Error Recovery Patterns:**
- LLMLOOP framework (ICLR 2025) — Multiple feedback loops (compilation errors, test validation, static analysis)
- LangGraph error classification (2025) — 4 categories: repairable, transient, fatal, silent errors
- Retry patterns (2025) — Exponential backoff with jitter, adaptive retry strategies

**Confidence:** HIGH — Patterns verified across multiple current tools (2025-2026). Table stakes features are universal (all test runners, ML tools have them). Differentiators are APDR-specific but follow established visualization patterns.
