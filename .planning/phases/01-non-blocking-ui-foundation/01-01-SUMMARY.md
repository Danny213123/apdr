---
phase: 01-non-blocking-ui-foundation
plan: 01
subsystem: backend-sse
tags: [sse, real-time, streaming, flask, backend]
dependency_graph:
  requires: []
  provides: [sse-endpoint, event-streaming, heartbeat-mechanism]
  affects: [benchmark-ui-backend, real-time-progress]
tech_stack:
  added: [server-sent-events, queue-based-events]
  patterns: [sse-generator, heartbeat-timeout, best-effort-streaming]
key_files:
  created: [benchmark_ui/test_runner_events.py]
  modified: [benchmark_ui/server.py, benchmark_ui/service.py, benchmark_ui/runner.py]
decisions:
  - Use Queue for event distribution (thread-safe, non-blocking)
  - 15-second heartbeat interval prevents proxy buffering
  - Best-effort event emission with put_nowait (don't block runner)
  - Event queue attached via service to worker before start
metrics:
  duration_seconds: 250
  tasks_completed: 3
  files_modified: 4
  commits: 4
  tests_added: 5
  completed_date: "2026-03-25"
---

# Phase 1 Plan 01: Backend SSE Infrastructure Summary

**One-liner:** Server-Sent Events endpoint with Queue-based event streaming, 15s heartbeat, and non-blocking runner emission

## What Was Built

Created complete SSE infrastructure for real-time benchmark progress streaming:

1. **SSE Endpoint Route** (`benchmark_ui/server.py`)
   - Added `/api/stream/benchmark/{runId}` GET handler in BenchmarkRequestHandler.do_GET()
   - Sets proper SSE headers: `text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`, `X-Accel-Buffering: no`
   - Calls `service.stream_benchmark_progress(run_id)` generator
   - Formats events as SSE protocol: `data: {json}\n\n`
   - Handles client disconnect gracefully (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)

2. **Event Generator with Heartbeat** (`benchmark_ui/service.py`)
   - Implemented `stream_benchmark_progress(run_id)` generator method (returns `Generator[dict[str, Any], None, None]`)
   - Validates run_id and retrieves run state (current or historical)
   - Initializes `_event_queue` on `_current_run` if not present
   - Yields initial `init` event with current progress state
   - Event loop with 15-second timeout for heartbeat mechanism
   - Yields events from queue, `heartbeat` on timeout
   - Yields `complete` event when run status is completed/stopped/failed
   - Handles historical runs (yields init + complete only)

3. **Runner Event Emission** (`benchmark_ui/runner.py`)
   - Added Queue import for event distribution
   - Created `emit_event` helper function in `_run_single`:
     - Constructs event dict with type, timestamp (ISO format), and kwargs
     - Uses `put_nowait()` for best-effort streaming (doesn't block runner)
     - Gracefully handles queue errors (returns early if queue unavailable)
   - Emits `status_update` when case starts (caseId, status="running")
   - Emits `case_complete` when case finishes (caseId, status="pass"/"fail"/"skip")
   - Emits `progress` after each case (progress dict with completed, total, percent)
   - Service attaches `_event_queue` to worker via `_current_run_event_queue` attribute before start

4. **Test Coverage** (`benchmark_ui/test_runner_events.py`)
   - TDD approach: RED (failing tests) → GREEN (implementation) → commit cycle
   - 5 tests verifying event emission behavior:
     - `test_emit_event_helper_puts_events_to_queue`: Verifies emit_event constructs and queues events
     - `test_runner_emits_status_update_on_case_start`: Validates status_update event structure
     - `test_runner_emits_case_complete_on_finish`: Validates case_complete event structure
     - `test_runner_emits_progress_after_each_case`: Validates progress event with stats
     - `test_events_contain_required_fields`: Documents expected event schema
   - All tests passing

## Event Schema

**status_update**:
```json
{
  "type": "status_update",
  "caseId": "test-001",
  "status": "running",
  "timestamp": "2026-03-25T23:35:00.123456"
}
```

**case_complete**:
```json
{
  "type": "case_complete",
  "caseId": "test-001",
  "status": "pass",
  "timestamp": "2026-03-25T23:35:05.789012"
}
```

**progress**:
```json
{
  "type": "progress",
  "progress": {
    "completed": 1,
    "total": 5,
    "percent": 20.0
  },
  "timestamp": "2026-03-25T23:35:05.789050"
}
```

**heartbeat**:
```json
{
  "type": "heartbeat",
  "timestamp": "2026-03-25T23:35:20.000000"
}
```

**init**:
```json
{
  "type": "init",
  "progress": {
    "completed": 0,
    "total": 5,
    "percent": 0.0
  },
  "timestamp": "2026-03-25T23:35:00.000000"
}
```

**complete**:
```json
{
  "type": "complete",
  "status": "completed",
  "timestamp": "2026-03-25T23:36:00.000000"
}
```

## Deviations from Plan

None - plan executed exactly as written.

## Key Decisions

1. **Queue-based event distribution**: Used Python's `Queue` class for thread-safe, non-blocking event passing between runner and SSE generator
2. **15-second heartbeat interval**: Prevents proxy buffering and connection timeouts (nginx, Cloudflare)
3. **Best-effort streaming**: Runner uses `put_nowait()` and catches exceptions - never blocks on queue errors
4. **Event queue attachment**: Service attaches `_event_queue` to worker via `_current_run_event_queue` attribute before `worker.start()`
5. **Historical run handling**: SSE endpoint yields init + complete for past runs (no live streaming)

## Testing Approach

**TDD Cycle**:
1. **RED**: Created `test_runner_events.py` with 5 failing tests documenting expected behavior
2. **GREEN**: Implemented runner event emission, service wiring, all tests pass
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
# data: {"type":"status_update","caseId":"001","status":"running","timestamp":"..."}
#
# data: {"type":"case_complete","caseId":"001","status":"pass","timestamp":"..."}
#
# data: {"type":"progress","progress":{"completed":1,"total":5,"percent":20.0},"timestamp":"..."}
#
# data: {"type":"heartbeat","timestamp":"..."}
# (repeats every 15s during idle periods)
```

## Implementation Notes

**SSE Endpoint Route**:
- Placed after `/api/runs/{runId}` handler (line 60) for logical grouping
- Uses ThreadingHTTPServer's existing connection handling (no new threading)
- Graceful disconnect handling prevents server crashes on client close

**Event Generator**:
- Placed after `stop_benchmark()` method (line 227) for logical flow
- Handles both current run streaming and historical run snapshots
- Thread-safe lock usage when accessing `_current_run` state

**Runner Event Emission**:
- `emit_event` defined as closure within `_run_single` (access to case_id)
- Events emitted at strategic points:
  - **Before** subprocess execution: status_update (running)
  - **After** result determination: case_complete (pass/fail/skip)
  - **After** case_complete: progress (updated stats)
- Progress percent calculated: `round((index / total * 100), 1)`

## Commits

| Commit | Type | Message | Files |
|--------|------|---------|-------|
| 54993f9 | feat | Add SSE endpoint route to Flask server | benchmark_ui/server.py |
| de5c917 | feat | Implement SSE event generator in BenchmarkService | benchmark_ui/service.py |
| 09af5ad | test | Add failing test for runner event emission | benchmark_ui/test_runner_events.py |
| 6f6001c | feat | Wire benchmark runner to emit progress events | benchmark_ui/runner.py, benchmark_ui/service.py, benchmark_ui/test_runner_events.py |

## Known Stubs

None - all data wiring complete.

## Self-Check: PASSED

**Files created:**
- `benchmark_ui/test_runner_events.py` - ✓ exists

**Files modified:**
- `benchmark_ui/server.py` - ✓ contains SSE route handler
- `benchmark_ui/service.py` - ✓ contains stream_benchmark_progress generator
- `benchmark_ui/runner.py` - ✓ contains emit_event and event emission calls

**Commits exist:**
- 54993f9 - ✓ found
- de5c917 - ✓ found
- 09af5ad - ✓ found
- 6f6001c - ✓ found

All claims verified.
