# Phase 2: Result Categorization & Insights - Context

**Gathered:** 2026-03-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 delivers rich result categorization and insights for benchmark runs. Users see deterministic results (tier1/tier2 cache hits) separated from LLM-based resolution attempts (tier3), with filtering, search capabilities, and cache hit rate visualization. This phase surfaces the existing tier system from the resolver into the UI.

**In Scope:**
- Split results into two sections: Deterministic (tier1/tier2) vs LLM (tier3)
- Add per-section filtering by status (pass/fail/skip/timeout), tier, and Python version
- Add per-section search by case ID or snippet content
- Display cache hit rate dashboard showing tier1/tier2/tier3 breakdown with percentages
- Show confidence scores and skip indicators for LLM cases
- Add expandable case details showing full logs and resolution path

**Out of Scope:**
- Advanced visualizations (timeline charts, historical comparisons) — Phase 2 v2 or later
- Performance optimizations (virtual scrolling, web workers) — only if needed
- Framework rewrites — keep vanilla JS

</domain>

<decisions>
## Implementation Decisions

### Result Split Strategy
- **D-01**: Repurpose existing two-panel structure — rename "Completed Cases" → "Deterministic Results" (tier1/tier2), keep "LLM Cases" for tier3
- **D-02**: Maintain collapsible panel pattern from Phase 1 — consistent UX, allows users to focus on one section
- **D-03**: Backend categorizes cases by resolution tier (tier1/tier2 → deterministic, tier3 → LLM) and sends to appropriate panel
- **D-04**: Both panels show in real-time as cases stream via SSE (from Phase 1)

### Filter & Search UI
- **D-05**: Per-section filter controls — each panel gets its own filter toolbar
- **D-06**: Deterministic section filters: status (pass/fail/skip), tier (tier1/tier2), Python version
- **D-07**: LLM section filters: status (pass/fail/skip), confidence threshold (slider or range), Python version
- **D-08**: Search input per section: searches case ID and snippet content (real-time filter as user types)
- **D-09**: Use existing custom-select dropdown pattern for filter dropdowns (matches current UI aesthetic)
- **D-10**: Filter state preserved during SSE updates — no reset when new cases arrive

### Cache Hit Rate Dashboard
- **D-11**: Dedicated metrics section above result panels — makes cache hit rates prominent
- **D-12**: Display tier breakdown: tier1 cache hits (%), tier2 heuristic hits (%), tier3 LLM calls (%)
- **D-13**: Use existing `.metrics-line` pattern from progress section — consistent with current UI
- **D-14**: Show absolute counts alongside percentages: "Tier1: 45/100 (45%)"
- **D-15**: Update metrics in real-time as cases complete (SSE-driven)

### Confidence & Skip Indicators
- **D-16**: Icon badges with tooltips for compact display — shows confidence level or skip icon in table
- **D-17**: Expandable case details — click case row to expand full logs, resolution path, confidence breakdown
- **D-18**: Confidence badges: color-coded icons (green >0.7, yellow 0.4-0.7, red <0.4) with tooltip showing exact score
- **D-19**: Skip indicators: icon badge (⊘ or similar) with tooltip explaining skip reason (e.g., "Skipped: confidence <0.4 threshold")
- **D-20**: Expanded view shows: full error logs, resolution tier path (tier1→tier2→tier3 waterfall), LLM prompt/response if applicable, confidence scoring breakdown
- **D-21**: Expandable details use slide-down animation (match existing collapsible panel behavior)

### Claude's Discretion
- **Color coding**: Choose specific color values and icon designs that fit the terminal aesthetic (dark theme, muted colors)
- **Animation timing**: Decide exact duration for expand/collapse transitions
- **Tooltip styling**: Design tooltip appearance (position, delay, styling)
- **Empty state messaging**: Write user-friendly messages when no results match filters
- **Loading skeleton**: Design skeleton UI while results are loading

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 1 Artifacts
- `.planning/phases/01-non-blocking-ui-foundation/01-UI-SPEC.md` — Terminal aesthetic guidelines, spacing grid, component patterns
- `.planning/phases/01-non-blocking-ui-foundation/01-01-SUMMARY.md` — SSE backend implementation patterns
- `.planning/phases/01-non-blocking-ui-foundation/01-02-SUMMARY.md` — EventSource client patterns, throttling approach
- `.planning/phases/01-non-blocking-ui-foundation/01-03-SUMMARY.md` — UI component wiring patterns, progressive loading

### Project Requirements
- `.planning/REQUIREMENTS.md` — Phase 2 requirements CAT-01 through CAT-08, LLM-01 through LLM-04

### Codebase Maps
- `.planning/codebase/STRUCTURE.md` — Web UI file structure, benchmark_ui organization
- `.planning/codebase/CONVENTIONS.md` — Naming patterns, code style
- `.planning/codebase/STACK.md` — Tech stack constraints (vanilla JS, no frameworks)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **Two-panel structure**: `#cases-panel` (Completed Cases) and `#llm-cases-panel` (LLM Cases) already exist in HTML — rename and repurpose for deterministic/LLM split
- **Collapsible panel component**: `.terminal-section.collapsible-panel` pattern established, with `<details>` elements — reuse for both sections
- **Custom select dropdowns**: `.custom-select` component already used for run history selection — reuse for filters
- **Case table structure**: `.case-table-head` and case row rendering already exist — extend with new columns/badges
- **SSE streaming**: Real-time event handling from Phase 1 — use to populate panels as cases complete
- **Metrics display**: `.metrics-line` pattern in progress section — reuse for cache hit rate dashboard

### Established Patterns
- **Terminal aesthetic**: 14px monospace font, dark theme, 4px spacing grid, single font size (from Phase 1 UI-SPEC)
- **State management**: Global `state` object in main.js with `state.currentRun` containing case results
- **DOM rendering**: `renderRunPage()` updates UI from state, separate functions for each section
- **Collapsible sections**: `<details>` elements with `.collapsible-summary` and `.collapsible-body`
- **Real-time updates**: SSE events trigger `handleSSEEvent()` → state update → `renderRunPage()`

### Integration Points
- **Backend service** (`benchmark_ui/service.py`): Needs to categorize cases by tier and include tier metadata in SSE events
- **Runner** (`benchmark_ui/runner.py`): Already emits tier information in results — ensure it's passed through to SSE events
- **Frontend main.js**: `handleSSEEvent()` needs case categorization logic to route to correct panel
- **HTML structure**: Extend existing panels with filter controls, rename sections, add metrics area

### Resolution Tier Data
From codebase analysis, resolution tiers are:
- **Tier 1** (cache): Resolved from seed data or dynamic cache (import-set cache, validated lockfiles)
- **Tier 2** (heuristic): Resolved via pattern matching (namespace heuristics, fuzzy matching, family knowledge)
- **Tier 3** (LLM): Resolved via LLM subprocess (Python LiteLLM/Instructor service)

Cases already track which tier resolved them — this data exists in backend, just needs to flow to frontend.

</code_context>

<specifics>
## Specific Ideas

**Icon badge design**: User prefers compact icon badges with tooltips over text labels — keeps table dense, tooltip provides detail on hover

**Expandable details**: User wants both icon badges (for quick scanning) AND expandable details (for deep investigation) — click row to expand full context

**Per-section controls**: User emphasized per-section filtering over global controls — allows different filter needs for deterministic vs LLM sections (e.g., LLM needs confidence filter, deterministic doesn't)

**Real-time metrics**: Cache hit rate dashboard should update live as cases complete — not just at end of run

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 02-result-categorization-insights*
*Context gathered: 2026-03-25*
