# Phase 2: Result Categorization & Insights - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-25
**Phase:** 02-result-categorization-insights
**Areas discussed:** Result Split Strategy, Filter & Search UI, Cache Hit Rate Dashboard, Confidence & Skip Indicators

---

## Result Split Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Repurpose existing panels | Rename "Completed Cases" → "Deterministic Results" (tier1/tier2), keep "LLM Cases" for tier3. Minimizes HTML changes, reuses collapsible pattern. | ✓ |
| Redesign from scratch | Create new panel structure with different layout. More flexibility but requires more implementation work. | |
| Single panel with tabs | One panel with tier1/tier2/tier3 tabs. Compact but doesn't emphasize deterministic vs LLM split. | |

**User's choice:** Repurpose existing panels (recommended option)
**Notes:** User confirmed recommended approach — minimal changes, maintains existing UX patterns from Phase 1

---

## Filter & Search UI

| Option | Description | Selected |
|--------|-------------|----------|
| Per-section filter controls | Each panel gets its own filter toolbar. Allows different filter needs (e.g., LLM section needs confidence filter). | ✓ |
| Global controls affecting both | Single filter bar above both sections. Simpler but less flexible for tier-specific filtering. | |
| Chip-based selection | Filter tags/chips instead of dropdowns. More modern but doesn't match terminal aesthetic. | |

**User's choice:** Per-section filter controls (recommended option)
**Notes:** User confirmed per-section approach — allows LLM section to have confidence threshold filter that deterministic section doesn't need

---

## Cache Hit Rate Dashboard

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated metrics section above panels | Makes cache hit rates prominent, can use existing `.metrics-line` pattern. | ✓ |
| Inline summary bar | Embedded in panel headers. More compact but less visible. | |
| Plain numbers only | Simple text display. Minimal but less visual impact. | |

**User's choice:** Dedicated metrics section above result panels (recommended option)
**Notes:** User confirmed dedicated section — prominence is important for understanding tier performance

---

## Confidence & Skip Indicators

| Option | Description | Selected |
|--------|-------------|----------|
| Icon badges with tooltips + expandable details | Compact icon badges for quick scanning, click row to expand full logs/confidence breakdown. Best of both worlds. | ✓ |
| Icon badges with tooltips only | Compact, doesn't add table columns. Hover reveals details. | |
| Expandable details only | No inline indicators, must expand every row to see confidence. More clicks required. | |

**User's choice:** Icon badges with tooltips + expandable details (modified recommended option)
**Notes:** User wanted BOTH icon badges (for quick scanning) AND expandable details (for deep investigation). Modified recommended option to include both instead of just icon badges.

**Additional clarifications:**
- Confidence badges: color-coded icons (green >0.7, yellow 0.4-0.7, red <0.4) with tooltip showing exact score
- Skip indicators: icon badge (⊘ or similar) with tooltip explaining skip reason
- Expanded view shows: full error logs, resolution tier path, LLM prompt/response, confidence scoring breakdown
- Slide-down animation matching existing collapsible panel behavior

---

## Claude's Discretion

Areas where user said "Claude decides":
- Specific color values and icon designs (must fit terminal aesthetic)
- Animation timing for expand/collapse transitions
- Tooltip styling (position, delay, appearance)
- Empty state messaging when no results match filters
- Loading skeleton design while results are loading

---

## Deferred Ideas

None — discussion stayed within phase scope.

