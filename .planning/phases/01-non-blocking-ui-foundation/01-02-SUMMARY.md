---
phase: 01-non-blocking-ui-foundation
plan: 02
subsystem: frontend-sse-client
tags: [sse, real-time, eventsource, requestanimationframe, dom-optimization, frontend]
dependency_graph:
  requires: [sse-endpoint, event-streaming]
  provides: [sse-client, throttled-dom-updates, reconnection-logic]
  affects: [benchmark-ui-frontend, real-time-progress-display]
tech_stack:
  added: [EventSource, requestAnimationFrame]
  patterns: [exponential-backoff, batched-dom-updates, frame-throttling, auto-pruning]
key_files:
  created: []
  modified: [web/src/main.js, web/src/styles.css]
decisions:
  - EventSource native API for SSE connection (no dependencies)
  - Exponential backoff reconnection (1s, 2s, 4s, 8s, 16s, max 30s)
  - requestAnimationFrame throttling for 60fps DOM updates
  - Activity stream auto-prunes to max 10 items (memory leak prevention)
  - Batch all events per frame to prevent layout thrashing
metrics:
  duration_seconds: 162
  tasks_completed: 3
  files_modified: 2
  commits: 3
  tests_added: 0
  completed_date: "2026-03-25"
---

# Phase 1 Plan 02: Frontend Real-Time SSE Client Summary

**One-liner:** EventSource client with exponential backoff reconnection and requestAnimationFrame-throttled DOM updates for 60fps real-time progress

## What Was Built

Created complete frontend SSE client infrastructure with performance optimizations:

1. **EventSource Client with Reconnection** (`web/src/main.js`)
   - Extended state object with SSE connection tracking properties:
     - `sseConnection`: EventSource instance
     - `sseReconnectAttempts`: Counter for exponential backoff calculation
     - `sseReconnectTimer`: Timer handle for reconnection scheduling
     - `sseConnectionState`: "disconnected" | "connecting" | "connected"
     - `ssePendingUpdates`: Event queue for batched processing
     - `sseUpdateScheduled`: Flag to prevent duplicate requestAnimationFrame calls
   - Implemented `setupSSE(runId)` function:
     - Creates EventSource connection to `/api/stream/benchmark/{runId}`
     - Sets connection state to "connecting" and updates UI indicator
     - Handles `onopen`: Sets state to "connected", resets reconnect attempts
     - Handles `onmessage`: Parses JSON event data, delegates to handleSSEEvent()
     - Handles `onerror`: Implements exponential backoff (1s, 2s, 4s... max 30s)
   - Implemented `teardownSSE()` function:
     - Clears reconnection timer
     - Closes EventSource connection
     - Resets connection state and attempts counter
     - Updates UI indicator
   - Integrated into benchmark lifecycle:
     - `setupSSE()` called on benchmark start (startButton click)
     - `teardownSSE()` called on benchmark stop (stopButton, runStopButton)

2. **Throttled DOM Update Functions** (`web/src/main.js`)
   - Implemented `handleSSEEvent(event)`:
     - Queues events in `ssePendingUpdates` array
     - Schedules `processPendingSSEUpdates()` via requestAnimationFrame (max 1 per frame)
   - Implemented `processPendingSSEUpdates()`:
     - Drains event queue using `splice(0)` for batch processing
     - Dispatches events by type: init, progress, status_update, case_complete, heartbeat, complete
     - Processes all queued events in single animation frame (16ms budget)
   - Implemented `updateProgressBar(progress)`:
     - Updates progress label: `{completed}/{total}`
     - Updates progress percent: `{percent}%`
     - Animates progress fill width via CSS transition
   - Implemented `updateCaseStatus(caseId, status)`:
     - Finds existing case row by `data-case-id` attribute
     - Updates status badge in-place (no re-render)
     - Delegates to `appendCaseRow()` for new completed cases
   - Implemented `addActivityItem(event)`:
     - Creates activity item with timestamp and action
     - Prepends to activity list (newest first)
     - Auto-prunes to max 10 items (prevents unbounded memory growth)
     - Fades in with CSS animation
   - Implemented `handleBenchmarkComplete()`:
     - Tears down SSE connection on completion
     - Placeholder for future enhancement in Task 3

3. **SSE Status Indicator and Streaming Badge CSS** (`web/src/styles.css`)
   - Added `.sse-status-indicator` base styles:
     - 8px circular dot with border-radius 50%
     - 8px right margin for spacing
   - Added state-specific indicator styles:
     - `.connected`: Green background (var(--green))
     - `.connecting`: Yellow background with pulse animation (var(--yellow))
     - `.disconnected`: Red background (var(--red))
   - Created `@keyframes pulse-dot` animation:
     - 1.5s ease-in-out infinite cycle
     - Opacity transitions from 1 → 0.4 → 1
   - Added `.streaming-badge` for LIVE indicator:
     - Yellow background with 15% opacity (rgba(255, 217, 94, 0.15))
     - Yellow text (var(--yellow))
     - Uppercase text with 0.5px letter spacing
     - Terminal aesthetic with compact padding
   - Added `.activity-item` with fade-in animation:
     - Text color using var(--text)
     - 13px font size for subtle appearance
     - `fade-in-activity` animation on insertion
   - Created `@keyframes fade-in-activity`:
     - 200ms ease-in transition
     - Fades in from opacity 0 to 1
     - Slides from translateX(-4px) to 0

## Event Handling Flow

**Connection Lifecycle**:
1. User clicks "Start Benchmark"
2. API call succeeds → `setupSSE(runId)` creates EventSource
3. EventSource connects → `onopen` fires → state = "connected"
4. Events stream in → `onmessage` parses JSON → `handleSSEEvent()` queues
5. User clicks "Stop" → `teardownSSE()` closes connection

**DOM Update Throttling**:
1. SSE event arrives → `handleSSEEvent()` pushes to queue
2. If not scheduled → `requestAnimationFrame(processPendingSSEUpdates)`
3. Browser schedules callback for next frame (16ms budget at 60fps)
4. `processPendingSSEUpdates()` drains queue, dispatches all events
5. DOM writes batched in single frame → no layout thrashing

**Reconnection on Error**:
1. Connection drops → `onerror` fires
2. Calculate delay: `Math.min(1000 * Math.pow(2, attempts), 30000)`
3. Increment attempts counter
4. Schedule reconnection with delay (1s, 2s, 4s, 8s, 16s, 30s max)
5. Timer fires → `setupSSE(runId)` retries connection
6. On success → attempts counter resets to 0

## Deviations from Plan

None - plan executed exactly as written.

## Key Decisions

1. **Native EventSource API**: No library dependencies - uses browser built-in SSE support
2. **Exponential backoff reconnection**: Prevents server overload during connection issues (1s → 30s max)
3. **requestAnimationFrame throttling**: Guarantees max 1 DOM update per frame (60fps target)
4. **Event queue batching**: Multiple rapid events processed together in single frame
5. **Auto-pruning activity stream**: Max 10 items prevents unbounded memory growth
6. **In-place case updates**: Finds existing DOM nodes by attribute, updates status without re-render
7. **Placeholder functions**: `updateSSEStatusIndicator()` and `appendCaseRow()` ready for Task 3 integration

## Testing Approach

**Browser DevTools Verification** (from plan):

1. **EventSource Connection**:
   - Open DevTools → Network tab → Filter "EventStream"
   - Start benchmark
   - Verify `/api/stream/benchmark/{runId}` appears with type `text/event-stream`
   - Confirm events stream incrementally (not batch after completion)

2. **Frame Rate Test** (UI-04):
   - Open DevTools → Performance tab → Record
   - Start benchmark with 50+ cases
   - Stop after 10 seconds
   - Verify FPS stays >55 (green line, no red drops)

3. **Memory Leak Test** (UI-05):
   - Open DevTools → Memory tab → Heap snapshot
   - Start benchmark, complete 100 cases
   - Take second snapshot
   - Compare: event listener count stable, activity stream max 10 nodes

4. **Reconnection Test**:
   - Start benchmark
   - DevTools → Network → Right-click SSE → "Block request URL"
   - Verify indicator turns red, then yellow (reconnecting)
   - Unblock URL
   - Verify indicator turns green, events resume

5. **Latency Test** (UI-02):
   - Add `console.time('event-to-dom')` in handleSSEEvent
   - Add `console.timeEnd('event-to-dom')` in processPendingSSEUpdates
   - Verify P95 latency <50ms from event receipt to DOM update

**No automated tests**: Vanilla JS frontend without test framework. Manual verification via DevTools.

## Implementation Notes

**State Management**:
- All SSE state centralized in global `state` object (lines 21-26)
- Connection state machine: disconnected → connecting → connected → disconnected
- Reconnection timer cleared on manual teardown (prevents memory leak)

**DOM Update Optimization**:
- requestAnimationFrame ensures browser-native 60fps scheduling
- Queue drains completely each frame (no partial processing)
- Batch all reads before writes (prevents layout thrashing)
- Activity stream prunes on every insert (no deferred cleanup)

**Error Handling**:
- JSON parse errors caught and logged (malformed SSE events)
- Queue operations graceful (no throw if queue unavailable)
- EventSource errors trigger reconnection (not UI failure)

**Integration Points**:
- `setupSSE()` called after successful `/api/benchmark/start` response
- `teardownSSE()` called before `/api/benchmark/stop` request
- Placeholder functions ready for Task 3 HTML integration

## Commits

| Commit | Type | Message | Files |
|--------|------|---------|-------|
| 65fbf4a | feat | Add EventSource client with reconnection logic | web/src/main.js |
| 46b41e4 | feat | Implement throttled DOM updates with requestAnimationFrame | web/src/main.js |
| 3d74f6f | feat | Add SSE status indicator and streaming badge CSS | web/src/styles.css |

## Known Stubs

1. **updateSSEStatusIndicator()** (line 1161):
   - Purpose: Update DOM element with class `.sse-status-indicator` based on connection state
   - Reason: HTML integration deferred to Task 3 (UI component integration)
   - Wiring needed: Query selector for indicator element, set class based on `state.sseConnectionState`

2. **appendCaseRow(caseId, status)** (line 1231):
   - Purpose: Clone case row template, populate with data, append to cases scroll
   - Reason: Full case row rendering handled in Task 3 integration
   - Current: Logs to console for debugging
   - Wiring needed: Template cloning, data population, DOM insertion

These stubs are intentional - Plan 01-03 will integrate SSE client with HTML UI components.

## Self-Check: PASSED

**Files modified:**
- `web/src/main.js` - ✓ contains setupSSE, EventSource, exponential backoff
- `web/src/styles.css` - ✓ contains .sse-status-indicator, .streaming-badge, fade-in-activity

**Commits exist:**
- 65fbf4a - ✓ found
- 46b41e4 - ✓ found
- 3d74f6f - ✓ found

**Key patterns verified:**
- EventSource instantiation: ✓ line 1111
- Exponential backoff: ✓ line 1136 (Math.min(1000 * Math.pow(2, attempts), 30000))
- requestAnimationFrame: ✓ line 1172
- Queue batching: ✓ line 1177 (splice drains queue)
- Auto-pruning: ✓ line 1253 (while loop removes children > 10)

All claims verified.
