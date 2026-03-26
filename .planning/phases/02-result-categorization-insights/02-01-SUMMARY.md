---
phase: 02-result-categorization-insights
plan: 01
subsystem: backend-categorization
tags: [sse, tier-metadata, confidence, cached, backend]
dependency_graph:
  requires: [sse-endpoint, event-streaming]
  provides: [tier-metadata, tier-stats, cached-status]
  affects: [result-categorization, llm-insights]
tech_stack:
  added: [tier-extraction, confidence-parsing, cache-hit-detection]
  patterns: [metadata-extraction, real-time-stats, best-effort-streaming]
key_files:
  created: [benchmark_ui/test_service_tier_stats.py]
  modified: [benchmark_ui/runner.py, benchmark_ui/service.py, benchmark_ui/test_runner_events.py]
decisions:
  - Extract tier from output_metadata first, fallback to log parsing
  - Emit tier_stats from runner (not service) for real-time updates
  - Store tier/confidence/cached in result dict for downstream processing
  - Default tier to "unknown" when not detected (graceful degradation)
  - Confidence field only present for tier3 LLM cases
  - Cached field indicates import-set cache hits (LLM-03)
metrics:
  duration_seconds: 349
  tasks_completed: 3
  files_modified: 3
  files_created: 1
  commits: 3
  tests_added: 9
  completed_date: "2026-03-26"
---

# Phase 2 Plan 01: Backend Tier Metadata Summary

**One-liner:** SSE events extended with tier/confidence/cached metadata for deterministic vs LLM categorization and cache hit rate tracking

## What Was Built

Added tier metadata extraction and emission to SSE event stream for real-time result categorization:

1. **Tier Metadata Extraction** (`benchmark_ui/runner.py`)
   - Added `_extract_tier()` method: Parses tier from output_metadata or log tail
   - Detects tier1 (cache hits), tier2 (heuristic), tier3 (LLM), defaults to "unknown"
   - Added `_extract_confidence()` method: Parses LLM confidence scores (0.0-1.0)
   - Added `_extract_cached_status()` method: Detects import-set cache hits (LLM-03)
   - Extended case_complete event emission with tier/confidence/cached fields
   - Stored tier metadata in result dict for downstream categorization

2. **Tier Stats Emission** (`benchmark_ui/runner.py`)
   - Added `_emit_tier_stats_event()` method: Calculates tier breakdown in real-time
   - Emits tier_stats event after each case completion (sequential and parallel modes)
   - Calculates tier1/tier2/tier3 counts and percentages with 1 decimal precision
   - Best-effort streaming (doesn't block runner on queue errors)

3. **Tier Stats Calculation Helper** (`benchmark_ui/service.py`)
   - Added `_calculate_tier_stats()` method: Reusable tier breakdown calculation
   - Handles empty results gracefully (returns 0.0% for all tiers)
   - Used by service for historical run analysis

4. **Test Coverage** (`benchmark_ui/test_runner_events.py`, `benchmark_ui/test_service_tier_stats.py`)
   - TDD approach: RED (failing tests) → GREEN (implementation) → commit cycle
   - 9 new tests verifying tier metadata emission and stats calculation:
     - `test_case_complete_includes_tier_metadata`: Validates tier1/tier2/tier3 emission
     - `test_tier_defaults_to_unknown_when_not_detected`: Validates graceful degradation
     - `test_cached_field_for_import_set_cache_hits`: Validates LLM-03 cached field
     - `test_llm_case_includes_confidence_field`: Validates tier3 confidence scores
     - `test_tier_stats_calculation`: Validates percentage calculation accuracy
     - `test_tier_stats_emitted_after_case_complete`: Documents event sequence
     - `test_tier_stats_handles_missing_tier_field`: Validates missing field handling
     - `test_tier_stats_with_empty_results`: Validates zero-case handling
     - `test_tier_stats_percentage_precision`: Validates 1 decimal rounding
   - All tests passing

## Event Schema Extensions

**case_complete** (extended):
```json
{
  "type": "case_complete",
  "caseId": "test-001",
  "status": "pass",
  "tier": "tier3",
  "confidence": 0.85,
  "cached": true,
  "timestamp": "2026-03-26T01:35:00.123456"
}
```

**tier_stats** (new):
```json
{
  "type": "tier_stats",
  "stats": {
    "tier1": {"count": 45, "percent": 45.0},
    "tier2": {"count": 30, "percent": 30.0},
    "tier3": {"count": 25, "percent": 25.0},
    "total": 100
  },
  "timestamp": "2026-03-26T01:35:00.123456"
}
```

## Implementation Details

**Tier Detection Strategy** (priority order):
1. Check `output_metadata["resolution_tier"]` field (APDR may write tier to YAML)
2. Parse log tail for tier markers: "tier1", "tier2", "tier3", "llm", "heuristic", "cache hit"
3. Default to "unknown" if not detected (graceful degradation)

**Confidence Parsing Strategy**:
1. Check `output_metadata["confidence"]` field
2. Parse log tail with regex: `confidence[:\s=]+([0-9.]+)`
3. Validate range 0.0-1.0
4. Return `None` if not available (only tier3 cases have confidence)

**Cached Status Detection** (LLM-03):
1. Check `output_metadata["import_set_cached"]` field
2. Parse log tail for "import-set cache hit" or "cache hit" + "import" patterns
3. Default to `False` for tier3 cases without cache hit

**Tier Stats Emission**:
- Emitted from runner (not service) after each case completion
- Provides real-time updates to SSE stream
- Calculated from `summary["results"]` with thread-safe locking
- Used by frontend for live cache hit rate dashboard

## Deviations from Plan

None - plan executed exactly as written. All planned features implemented:
- Tier metadata extraction from output_metadata and logs
- Confidence score parsing for LLM cases
- Cached status detection for import-set cache hits (LLM-03)
- Tier stats event emission after case completion
- Test coverage for all functionality

## Key Decisions

1. **Tier extraction priority**: Check metadata first, fallback to log parsing (handles both future APDR improvements and current log-based detection)
2. **Stats emission location**: Runner emits tier_stats events (not service) because runner has authoritative access to `summary["results"]` in real-time
3. **Confidence field conditional**: Only include confidence in case_complete for tier3 cases (reduces event size for tier1/tier2)
4. **Cached field**: Always include for tier3 cases, defaults to False (LLM-03 requirement for cache hit rate visualization)
5. **Best-effort streaming**: Use put_nowait() for event emission (don't block runner on queue errors)
6. **Percentage precision**: Round to 1 decimal place (matches UI-SPEC requirement for clean display)

## Testing Approach

**TDD Cycle**:
1. **RED**: Created test files with 9 failing tests documenting expected behavior
2. **GREEN**: Implemented tier extraction, stats calculation, event emission - all tests pass
3. **REFACTOR**: Not needed (clean implementation on first pass)

**Manual Verification** (from plan):
```bash
# Start benchmark via API
curl -X POST http://localhost:5173/api/benchmark/start \
  -H "Content-Type: application/json" \
  -d '{"tool":"apdr","validationBackend":"env","dataset":"hard-gists","limit":"5"}'

# Subscribe to SSE stream
curl -N http://localhost:5173/api/stream/benchmark/{runId}

# Expected output:
# data: {"type":"init","progress":{"completed":0,"total":5,"percent":0},"timestamp":"..."}
#
# data: {"type":"case_complete","caseId":"001","status":"pass","tier":"tier1","timestamp":"..."}
#
# data: {"type":"tier_stats","stats":{"tier1":{"count":1,"percent":100.0},"tier2":{"count":0,"percent":0.0},"tier3":{"count":0,"percent":0.0},"total":1},"timestamp":"..."}
#
# (tier3 case with confidence and cached fields)
# data: {"type":"case_complete","caseId":"002","status":"pass","tier":"tier3","confidence":0.85,"cached":true,"timestamp":"..."}
```

## Requirements Satisfied

- **CAT-01**: Deterministic results (tier1/tier2) separable from LLM results (tier3) via tier field
- **CAT-03**: Results filterable by resolution tier (backend emits tier metadata)
- **LLM-01**: Cache hit rate dashboard data (tier_stats events with tier breakdown)
- **LLM-02**: Confidence-based skip indicators (confidence field emission for tier3)
- **LLM-03**: Import-set cache reuse indicator (cached field emission for tier3) ← **CRITICAL**

## Commits

| Commit | Type | Message | Files |
|--------|------|---------|-------|
| 164619c | test | Add failing tests for tier metadata emission | test_runner_events.py, test_service_tier_stats.py |
| c1047a7 | feat | Add tier metadata extraction to runner | runner.py |
| 2b066e8 | feat | Add tier breakdown calculation to service | service.py |

## Known Stubs

None - all data wiring complete. Tier metadata flows from:
1. APDR tool output → output_metadata YAML
2. Runner extracts tier/confidence/cached → emits in SSE events
3. Service calculates tier_stats → frontend receives categorization data

No placeholder data or hardcoded values. All extraction logic implemented with graceful degradation.

## Self-Check: PASSED

**Files created:**
- `benchmark_ui/test_service_tier_stats.py` - ✓ exists

**Files modified:**
- `benchmark_ui/runner.py` - ✓ contains _extract_tier, _extract_confidence, _extract_cached_status, _emit_tier_stats_event
- `benchmark_ui/service.py` - ✓ contains _calculate_tier_stats
- `benchmark_ui/test_runner_events.py` - ✓ contains tier metadata tests

**Commits exist:**
- 164619c - ✓ found
- c1047a7 - ✓ found
- 2b066e8 - ✓ found

**Tests passing:**
- All 14 tests pass (9 new + 5 existing)

All claims verified. Implementation ready for Phase 2 Plan 02 (frontend categorization UI).
