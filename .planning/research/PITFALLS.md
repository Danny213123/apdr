# Domain Pitfalls

**Domain:** Real-time UI, LLM optimization, parallel Docker validation
**Researched:** 2026-03-25

## Critical Pitfalls

Mistakes that cause rewrites or major issues.

### Pitfall 1: Event Listener Memory Leaks in Real-Time UIs
**What goes wrong:** Event listeners attached during UI updates accumulate without cleanup, causing browser memory to grow until tab freezes or crashes. After 20 minutes of benchmark runs, UI stutters, scroll lags, animations drop frames. In production cases, memory consumption increases 500MB per hour until out-of-memory crash.

**Why it happens:** Single-page applications dynamically add/remove DOM elements (progress rows, result cards, status indicators) without cleaning up attached listeners. When a benchmark case completes and its DOM element updates, old listeners remain attached even though the element context changed. JavaScript's garbage collector cannot reclaim memory because event listeners hold references to DOM nodes and their closures.

**Consequences:**
- Browser tab becomes unresponsive after 50-100 benchmark cases
- Memory usage climbs from 100MB to 1GB+ during long runs
- User must refresh page to continue, losing UI state
- Parallel worker updates amplify problem (N workers = N×leak rate)

**Prevention:**
1. **Use addEventListener with AbortController pattern:**
   ```javascript
   const controller = new AbortController();
   element.addEventListener('click', handler, { signal: controller.signal });
   // Later: controller.abort(); // Removes ALL listeners tied to this controller
   ```
2. **Leverage {once: true} for one-time events:**
   ```javascript
   button.addEventListener('click', handler, { once: true }); // Auto-cleanup after first trigger
   ```
3. **Track listeners in component lifecycle:**
   ```javascript
   class BenchmarkRow {
     constructor() { this.listeners = []; }
     addListener(element, event, handler) {
       element.addEventListener(event, handler);
       this.listeners.push({ element, event, handler });
     }
     destroy() {
       this.listeners.forEach(({ element, event, handler }) =>
         element.removeEventListener(event, handler)
       );
     }
   }
   ```
4. **Use event delegation instead of per-element listeners:**
   ```javascript
   // BAD: Attach listener to each row (100 rows = 100 listeners)
   rows.forEach(row => row.addEventListener('click', handler));

   // GOOD: Single listener on parent container
   container.addEventListener('click', (e) => {
     if (e.target.matches('.benchmark-row')) handler(e);
   });
   ```

**Detection:**
- Chrome DevTools → Memory → Take heap snapshot before/after benchmark run
- Look for "Detached HTMLElement" objects that should have been garbage collected
- Monitor "Listeners" count in heap snapshot (should stay constant during updates)
- Browser's Task Manager shows tab memory climbing >200MB/minute
- Console warning: "Listener added but context already destroyed"

**Relevant Phase:** Phase 1 (UI real-time updates) — Must address during UI streaming refactor

---

### Pitfall 2: DOM Thrashing from Synchronous Layout Reads
**What goes wrong:** Real-time benchmark updates trigger forced synchronous layout recalculations by interleaving DOM writes (update result text) and reads (measure element height). Each read forces browser to stop, recalculate layout for entire page, then continue. With parallel workers completing 10-20 cases simultaneously, browser performs hundreds of unnecessary layout calculations per second, freezing the UI.

**Why it happens:**
```javascript
// ANTI-PATTERN: Write → Read → Write → Read (forces layout 4 times)
resultElement.textContent = newText;           // Write (queues layout)
const height = resultElement.offsetHeight;      // Read (forces layout NOW)
parentContainer.style.height = height + 'px';   // Write (queues layout)
const scrollPos = container.scrollTop;          // Read (forces layout NOW)
```
Browser optimizes by batching layout calculations at end of frame, but reading layout properties (offsetHeight, scrollTop, getBoundingClientRect) forces immediate calculation. Each force-layout blocks main thread for 5-50ms.

**Consequences:**
- UI appears frozen during heavy update bursts
- Scroll stutters when new results stream in
- Chrome DevTools Performance shows purple "Layout" bars dominating timeline
- Interaction to Next Paint (INP) metric exceeds 500ms (should be <200ms)
- Users report "browser hangs" during parallel benchmark runs

**Prevention:**
1. **Batch all reads before writes (read phase → write phase):**
   ```javascript
   // GOOD: Batch reads, then batch writes
   const measurements = elements.map(el => ({
     element: el,
     height: el.offsetHeight,
     scroll: el.scrollTop
   }));
   measurements.forEach(({ element, height, scroll }) => {
     element.style.height = height + 'px';
     element.scrollTop = scroll + 10;
   });
   ```
2. **Use ResizeObserver instead of measuring in update loop:**
   ```javascript
   const observer = new ResizeObserver(entries => {
     for (const entry of entries) {
       // Browser provides measurements without forcing layout
       const height = entry.contentRect.height;
     }
   });
   observer.observe(resultContainer);
   ```
3. **Throttle updates during high-frequency streaming:**
   ```javascript
   let pendingUpdates = [];
   let rafScheduled = false;

   function scheduleUpdate(data) {
     pendingUpdates.push(data);
     if (!rafScheduled) {
       rafScheduled = true;
       requestAnimationFrame(() => {
         applyBatchUpdates(pendingUpdates);
         pendingUpdates = [];
         rafScheduled = false;
       });
     }
   }
   ```
4. **Cache layout measurements that don't change:**
   ```javascript
   const rowHeight = resultRow.offsetHeight; // Read once
   // Reuse cached value for 100 similar rows
   ```

**Detection:**
- Chrome DevTools → Performance → Record during benchmark run
- Look for repeated "Recalculate Style" and "Layout" events (purple bars)
- "Layout Shift" warnings in console
- Performance timeline shows >50% time in layout vs rendering
- Long tasks blocking main thread >50ms

**Relevant Phase:** Phase 1 (UI real-time updates) — Critical for streaming result updates

---

### Pitfall 3: Main Thread Blocking from Synchronous Processing
**What goes wrong:** Large synchronous operations (parsing 1000-row benchmark dataset on startup, processing completed results, rendering virtual scroll windows) execute on main thread, freezing UI for 3+ seconds. User sees blank screen or unresponsive controls. Current CONCERNS.md notes 3+ second database load blocks UI interaction.

**Why it happens:** JavaScript is single-threaded. When main thread executes computationally expensive task (parsing JSON, filtering arrays, sorting results), it cannot process user input or render updates. Browser's event loop is blocked until task completes.

**Consequences:**
- 3+ second blank screen on page load (database loading)
- UI freezes when clicking "Run Benchmark" while preparing worker tasks
- Scroll stutters when filtering 500+ results
- Click events delayed 1-2 seconds during heavy processing
- Browser shows "Page Unresponsive" dialog on slower machines

**Prevention:**
1. **Offload heavy computation to Web Workers:**
   ```javascript
   // Main thread
   const worker = new Worker('processor.js');
   worker.postMessage({ action: 'parseDataset', data: rawJson });
   worker.onmessage = (e) => {
     const parsed = e.data.result;
     updateUI(parsed); // Main thread stays responsive
   };

   // processor.js (Web Worker)
   self.onmessage = (e) => {
     if (e.data.action === 'parseDataset') {
       const result = heavyParsing(e.data.data);
       self.postMessage({ result });
     }
   };
   ```
2. **Use progressive/incremental loading instead of all-at-once:**
   ```javascript
   // BAD: Load entire dataset synchronously
   const allData = await fetch('/api/benchmark/cases').then(r => r.json());
   renderAll(allData); // Blocks main thread

   // GOOD: Stream and render incrementally
   const response = await fetch('/api/benchmark/stream');
   const reader = response.body.getReader();
   const decoder = new TextDecoder();

   while (true) {
     const { done, value } = await reader.read();
     if (done) break;
     const chunk = decoder.decode(value);
     renderChunk(chunk); // Small chunks, non-blocking
   }
   ```
3. **Split synchronous work across frames with requestIdleCallback:**
   ```javascript
   function processLargeDataset(items) {
     const chunkSize = 50;
     let index = 0;

     function processChunk(deadline) {
       while (index < items.length && deadline.timeRemaining() > 0) {
         processItem(items[index++]);
       }
       if (index < items.length) {
         requestIdleCallback(processChunk);
       }
     }
     requestIdleCallback(processChunk);
   }
   ```
4. **Show immediate UI skeleton while loading:**
   ```javascript
   // Show placeholder UI immediately
   showLoadingSkeleton();

   // Load data asynchronously
   loadDataInBackground().then(data => {
     hideLoadingSkeleton();
     renderData(data);
   });
   ```

**Detection:**
- Chrome DevTools → Performance → Long Tasks (red triangles) >50ms
- Main thread shows solid block of JavaScript execution
- "Blocking the main thread" warnings
- Lighthouse audit flags "Minimize main-thread work"
- User interactions queue up and execute in burst after delay

**Relevant Phase:** Phase 1 (UI startup optimization) — Address during non-blocking startup implementation

---

### Pitfall 4: LLM Cache Invalidation Staleness
**What goes wrong:** Cached LLM responses become stale when package ecosystem changes (new PyPI releases, deprecated packages), prompt templates evolve, or error patterns shift. System serves wrong answers from cache (yesterday's correct resolution is today's wrong one). User sees "resolved" package that no longer exists or uses deprecated import patterns. APDR's current accuracy issues may stem from serving stale cached suggestions.

**Why it happens:** LLM caches optimize for speed by storing prompt→response mappings with simple TTL expiration (cache for 24 hours). Cache key doesn't capture:
- Prompt template version (change few-shot examples → same key, different semantics)
- External state changes (PyPI deprecated package → cache says it's valid)
- Model updates (switch from Llama 3.2 to 3.3 → different output for same prompt)

**Consequences:**
- Recovery suggestions recommend packages that fail pip install (cache says package X exists, but it was deprecated)
- Pattern taxonomy changes don't invalidate old cached resolutions
- A/B testing prompt improvements impossible (cache serves old responses)
- Build error patterns evolve but cached recovery actions stay static
- LLM-assisted accuracy plateaus at 75% despite prompt improvements

**Prevention:**
1. **Version cache keys by prompt template hash + model identifier:**
   ```python
   import hashlib

   def cache_key(prompt: str, model: str, template_version: int) -> str:
       # Invalidate automatically when template or model changes
       key_parts = f"{prompt}|{model}|v{template_version}"
       return hashlib.sha256(key_parts.encode()).hexdigest()

   # In code:
   TEMPLATE_VERSION = 3  # Increment when few-shot examples change
   key = cache_key(prompt, "llama3.3", TEMPLATE_VERSION)
   ```
2. **Implement semantic cache with staleness detection:**
   ```python
   # Cache with metadata
   cache_entry = {
       'response': llm_output,
       'timestamp': datetime.now(),
       'dependencies': ['package-x==1.2.3'],  # Track what response depends on
       'template_hash': hash(prompt_template)
   }

   # Before serving from cache, validate dependencies
   def is_stale(entry) -> bool:
       if entry['template_hash'] != current_template_hash:
           return True  # Prompt changed
       if any(is_package_deprecated(pkg) for pkg in entry['dependencies']):
           return True  # Ecosystem changed
       return False
   ```
3. **Use tiered TTL based on volatility:**
   ```python
   # Low volatility: Package family mappings (sklearn → scikit-learn)
   FAMILY_MAPPING_TTL = 30 * 24 * 3600  # 30 days

   # Medium volatility: Version constraints for stable packages
   STABLE_PACKAGE_TTL = 7 * 24 * 3600  # 7 days

   # High volatility: Build error recovery patterns
   ERROR_PATTERN_TTL = 1 * 24 * 3600  # 1 day

   # Critical: Active development packages
   BLEEDING_EDGE_TTL = 3600  # 1 hour
   ```
4. **Event-driven invalidation for known changes:**
   ```python
   # When updating prompt template
   def update_prompt_template(new_template):
       global CURRENT_TEMPLATE_VERSION
       CURRENT_TEMPLATE_VERSION += 1
       cache.clear_prefix(f"llm:v{CURRENT_TEMPLATE_VERSION - 1}")
       save_template(new_template)

   # When detecting package deprecation
   def mark_package_deprecated(package_name):
       cache.invalidate_matching(lambda key, val:
           package_name in val.get('dependencies', [])
       )
   ```
5. **Cache metadata for debugging:**
   ```python
   # Always store why cache entry was created
   cache_entry = {
       'response': output,
       'created_at': datetime.now(),
       'model': 'llama3.3',
       'template_version': 3,
       'prompt_hash': hash(prompt),
       'hit_count': 0,  # Track reuse
       'last_validated': datetime.now()
   }
   ```

**Detection:**
- Monitor cache hit rate vs accuracy correlation (high hit rate + low accuracy = staleness)
- Log cache age distribution (majority >7 days old = likely stale)
- Track "cache hit but validation failed" events
- Compare cached response vs fresh inference on sample (>10% divergence = staleness)
- Alert on PyPI package deprecation matching cached dependencies

**Relevant Phase:** Phase 2 (LLM accuracy improvements) — Critical for prompt engineering and cache optimization

---

### Pitfall 5: Premature Optimization Without Measurement
**What goes wrong:** Team optimizes code based on assumptions instead of profiling data. Spend days optimizing .clone() calls (329 instances in CONCERNS.md) that contribute <5% of runtime, while ignoring actual bottleneck (sequential Docker validation consuming 80% of wall-clock time). Optimizations add complexity without measurable user benefit.

**Why it happens:**
- Cognitive biases: "This looks slow" feels more actionable than "measure first"
- Availability bias: Recent code review flagged clones, so team assumes that's the problem
- Action bias: Optimizing feels productive even without validating impact
- Sunk cost: After investing time in optimization, team commits to it regardless of results

**Consequences:**
- Week spent refactoring Arc/Mutex patterns, 0.2% speed improvement
- Code complexity increases (harder to read Cow<str> than String)
- Real bottleneck (sequential validation) remains unaddressed
- Team morale drops when "optimization sprint" yields no user-visible improvement
- Technical debt from over-engineered solutions

**Prevention:**
1. **Profile before optimizing — establish baseline metrics:**
   ```bash
   # Measure current state
   cargo bench --bench resolver_benchmark > before.txt
   hyperfine --warmup 3 'apdr bench --test hard-gists' > baseline.json

   # After optimization, compare
   cargo bench --bench resolver_benchmark > after.txt
   hyperfine --warmup 3 'apdr bench --test hard-gists' > optimized.json

   # Did it improve? By how much?
   python scripts/compare_benchmarks.py baseline.json optimized.json
   ```
2. **Use flamegraphs to find actual hotspots:**
   ```bash
   # Linux: perf + flamegraph
   cargo build --release
   perf record --call-graph dwarf ./target/release/apdr bench
   perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg

   # macOS: Instruments or samply
   samply record ./target/release/apdr bench
   ```
3. **Set concrete success criteria before starting:**
   ```markdown
   ## Optimization Goal: Reduce benchmark runtime

   **Baseline:** 150 seconds for 100 hard-gist cases
   **Target:** <100 seconds (33% improvement)
   **Must improve:** End-to-end wall-clock time (not micro-benchmark)
   **Acceptable cost:** <10% code complexity increase

   If target not met → revert changes
   ```
4. **Optimize in priority order (highest impact first):**
   ```markdown
   Based on profiling:
   1. Sequential Docker validation: 120s (80% of runtime) ← START HERE
   2. LLM inference: 20s (13% of runtime)
   3. PyPI metadata fetching: 8s (5% of runtime)
   4. Import parsing: 2s (1% of runtime)
   5. .clone() overhead: <1s (<1% of runtime) ← SKIP FOR NOW
   ```
5. **Distinguish between necessary design choices and premature optimization:**
   ```rust
   // NOT premature: Algorithm choice matters
   // Use HashMap instead of linear Vec search (O(1) vs O(n))
   let mut cache: HashMap<String, Package> = HashMap::new();

   // Premature: Micro-optimization without measurement
   // Using Cow<str> everywhere "just in case" cloning is slow
   // (adds complexity, unclear if it helps)
   ```

**Detection:**
- Optimization PR lacks benchmark comparison in description
- Team discusses optimization without profiling data in thread
- Code review shows increased complexity but no measurements
- "This should be faster" without evidence
- Optimization targets code that runs <1% of total time

**Relevant Phase:** ALL PHASES — Continuous discipline, especially Phase 3 (Docker parallelization)

---

### Pitfall 6: Parallel Docker Build Race Conditions
**What goes wrong:** Multiple Python versions (2.7, 3.8, 3.9, 3.10, 3.11) validate same requirements.txt in parallel using shared cache mounts. Without proper locking, two builds concurrently write to pip cache (~/.cache/pip), corrupting index files. Builds fail with "pip cache corrupted" or deadlock waiting for lock that never releases. BuildKit's default cache mount sharing=shared allows concurrent writes.

**Why it happens:** Package managers like pip and apt need exclusive access to their cache directories to safely update indexes and metadata. When BuildKit cache mount uses sharing=shared (default), multiple builds access same cache simultaneously:
```dockerfile
# DANGEROUS: Default sharing=shared allows concurrent writes
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
```
Build A and Build B both run pip install → both try to update ~/.cache/pip/http → file corruption or lock contention.

**Consequences:**
- Random validation failures: "Pip's cache is corrupted (Missing: SHA256)"
- Builds hang indefinitely waiting for cache lock
- Cache corruption forces full rebuild (losing cache benefit)
- Non-deterministic failures (works locally, fails in CI 30% of time)
- Debugging nightmare (race conditions are timing-dependent)

**Prevention:**
1. **Use sharing=locked for package managers requiring exclusive access:**
   ```dockerfile
   # CORRECT: Serialize access to pip cache
   RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
       pip install -r requirements.txt

   # CORRECT: Serialize access to apt cache
   RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
       --mount=type=cache,target=/var/lib/apt,sharing=locked \
       apt-get update && apt-get install -y python3-dev
   ```
2. **Use sharing=shared only for lock-free package managers:**
   ```dockerfile
   # OK: pnpm and yarn designed for concurrent cache access
   RUN --mount=type=cache,target=/root/.pnpm-store,sharing=shared \
       pnpm install --frozen-lockfile
   ```
3. **Understand performance trade-off (correctness vs throughput):**
   ```markdown
   sharing=locked:
   - Pro: Prevents corruption, guaranteed correctness
   - Con: Serializes builds (100s → 300s with 3 parallel workers)

   sharing=shared:
   - Pro: Maximum parallelism (100s → 50s with 3 workers)
   - Con: Risk of cache corruption with unsafe package managers

   Decision: Start with locked for correctness, optimize later
   ```
4. **Per-Python-version cache mounts to reduce lock contention:**
   ```dockerfile
   ARG PYTHON_VERSION
   # Each Python version gets its own cache (reduces lock wait time)
   RUN --mount=type=cache,target=/root/.cache/pip-${PYTHON_VERSION},sharing=locked \
       pip install -r requirements.txt
   ```
5. **Monitor for deadlocks with build timeouts:**
   ```rust
   // In builder.rs
   const BUILD_TIMEOUT: Duration = Duration::from_secs(300);

   let build_result = tokio::time::timeout(
       BUILD_TIMEOUT,
       docker_build_command.output()
   ).await.map_err(|_| Error::BuildDeadlock)?;
   ```

**Detection:**
- Build logs show "Waiting for cache lock..." >30 seconds
- BuildKit cache mount shows concurrent access in buildx debug logs
- Intermittent "corrupted cache" errors (works in retry)
- Build duration variance >50% between runs (race condition timing)
- Docker build hangs at pip install step with no output

**Relevant Phase:** Phase 3 (Docker parallel validation) — MUST address before parallelizing builds

---

### Pitfall 7: Server-Sent Events Buffering and Connection Drops
**What goes wrong:** Real-time benchmark progress uses Server-Sent Events (SSE) to stream results from Flask backend to browser. Reverse proxies (nginx, IIS on Windows), load balancers, or Flask's built-in buffering hold chunks in memory instead of sending immediately. User sees 30-second delays for "real-time" updates or connections drop after 4 minutes (idle timeout). SSE spec warns "HTTP chunking can have unexpected negative effects on reliability" but provides no control when you don't own the network infrastructure.

**Why it happens:**
- Flask's response buffering: Collects chunks until buffer full (4KB-8KB) before sending
- nginx proxy_buffering: Buffers upstream responses by default
- Load balancer idle timeouts: Close connections with no data for >60 seconds
- IIS on Windows: Buffers responses to optimize throughput, breaking streaming

**Consequences:**
- "Real-time" updates arrive in 30-second bursts instead of live stream
- Connection drops mid-benchmark, user sees "Connection lost" error
- First result takes 30+ seconds to appear (waiting for buffer to fill)
- Works perfectly in development (no proxy), fails in production (load balancer)
- Windows deployment broken (IIS buffering prevents SSE)

**Prevention:**
1. **Disable Flask response buffering for SSE endpoints:**
   ```python
   from flask import Response, stream_with_context

   @app.route('/benchmark/stream')
   def benchmark_stream():
       def generate():
           for result in run_benchmark():
               # Yield with explicit flush hint
               yield f"data: {json.dumps(result)}\n\n"

       return Response(
           stream_with_context(generate()),
           mimetype='text/event-stream',
           headers={
               'Cache-Control': 'no-cache',
               'X-Accel-Buffering': 'no',  # Disable nginx buffering
               'Content-Type': 'text/event-stream'
           }
       )
   ```
2. **Send heartbeat events to prevent idle timeouts:**
   ```python
   import time

   def generate_with_keepalive():
       last_heartbeat = time.time()

       for result in run_benchmark():
           yield f"data: {json.dumps(result)}\n\n"

           # Send heartbeat every 15 seconds if no data
           if time.time() - last_heartbeat > 15:
               yield ": keepalive\n\n"  # SSE comment (ignored by client)
               last_heartbeat = time.time()
   ```
3. **Configure nginx to disable buffering for SSE:**
   ```nginx
   location /benchmark/stream {
       proxy_pass http://flask_backend;
       proxy_buffering off;              # Disable response buffering
       proxy_cache off;                  # Disable caching
       proxy_set_header Connection '';   # HTTP/1.1 persistent connection
       proxy_http_version 1.1;           # Required for chunked encoding
       chunked_transfer_encoding on;     # Enable chunked transfer
   }
   ```
4. **Detect and handle connection drops on client:**
   ```javascript
   const eventSource = new EventSource('/benchmark/stream');

   eventSource.onerror = (error) => {
       console.error('SSE connection error:', error);
       eventSource.close();

       // Retry with exponential backoff
       setTimeout(() => {
           reconnectSSE();
       }, retryDelay);
   };

   // Detect stale connections (no data for >30s)
   let lastEventTime = Date.now();
   setInterval(() => {
       if (Date.now() - lastEventTime > 30000) {
           console.warn('SSE connection stale, reconnecting');
           eventSource.close();
           reconnectSSE();
       }
   }, 5000);
   ```
5. **Flush explicitly after each event (Python):**
   ```python
   import sys

   def generate():
       for result in run_benchmark():
           yield f"data: {json.dumps(result)}\n\n"
           sys.stdout.flush()  # Force immediate send
   ```

**Detection:**
- Browser DevTools → Network → EventStream shows long gaps between messages
- Server logs timestamp vs client receipt timestamp differ by >5 seconds
- Connection terminates exactly at proxy timeout (60s, 120s, 240s)
- Works in curl test but not browser (proxy strips headers)
- SSE reconnects repeatedly in production but not localhost

**Relevant Phase:** Phase 1 (UI real-time streaming) — Must address for reliable progress updates

---

## Moderate Pitfalls

### Pitfall 8: LLM Batch Size vs Latency Trade-off Mistuning
**What goes wrong:** Team implements LLM batching to improve throughput (resolve 10 imports in single call instead of 10 serial calls). Choose aggressive batch_size=50 to maximize GPU utilization. Individual request latency increases 5×, causing timeout failures and poor user experience. System optimizes for throughput at the expense of responsiveness.

**Why it happens:** After certain batch size, system crosses from memory-bound to compute-bound regime. Every doubling of batch size increases latency without increasing throughput. Team tunes for server metrics (GPU utilization, tokens/second) instead of user metrics (time to first result).

**Prevention:**
1. **Measure latency percentiles, not just throughput:**
   ```python
   # Track P50, P95, P99 latency — not just average
   import time

   latencies = []
   for batch in batches:
       start = time.time()
       result = llm_inference(batch)
       latency = time.time() - start
       latencies.append(latency)

   # Alert if P95 > 5 seconds (user-facing timeout threshold)
   p95 = np.percentile(latencies, 95)
   if p95 > 5.0:
       log.warning(f"P95 latency {p95:.2f}s exceeds threshold")
   ```
2. **Use adaptive batching based on queue depth:**
   ```python
   # Small batches when queue empty (low latency)
   # Large batches when queue full (high throughput)

   def get_batch_size(queue_depth: int) -> int:
       if queue_depth < 5:
           return 1  # Low latency mode
       elif queue_depth < 20:
           return 5  # Balanced
       else:
           return 10  # High throughput mode
   ```
3. **Set maximum batch size based on latency SLA:**
   ```python
   # Benchmark to find batch size where latency <3s
   # batch_size=1:  latency=0.5s, throughput=2 req/s
   # batch_size=5:  latency=1.2s, throughput=8 req/s ← CHOOSE THIS
   # batch_size=10: latency=2.8s, throughput=10 req/s
   # batch_size=20: latency=5.5s, throughput=10 req/s (no gain)

   MAX_BATCH_SIZE = 5  # Based on empirical testing
   ```

**Detection:**
- User-reported timeouts increase after "optimization"
- Throughput improves but P95 latency degrades
- Benchmark shows latency scaling >2× per batch size doubling
- GPU utilization >90% but user complaints increase

**Relevant Phase:** Phase 2 (LLM inference optimization) — Address during batching implementation

---

### Pitfall 9: Virtual Scrolling with Non-Fixed Row Heights
**What goes wrong:** Benchmark results UI renders 500+ test cases in table. Implement virtual scrolling (only render visible rows) for performance. Assume fixed row height for simple math (viewport height ÷ row height = visible rows). Some results have stack traces (5 lines), others are simple (1 line). Virtual scroll calculations wrong → missing rows, incorrect scroll position, jumping during scroll.

**Why it happens:** Virtual scrolling requires knowing total scrollable height and which items are visible. With fixed heights, math is trivial: item_100_position = 100 × row_height. With variable heights, need to measure each row or estimate and correct.

**Prevention:**
1. **Use CSS to enforce fixed heights with overflow:**
   ```css
   .benchmark-row {
     height: 60px;        /* Fixed height */
     overflow: hidden;    /* Clip long content */
   }

   .benchmark-row.expanded {
     height: auto;        /* Expand on click */
     max-height: 300px;
   }
   ```
2. **Measure and cache actual heights:**
   ```javascript
   const rowHeights = new Map();

   function getRowHeight(index) {
     if (!rowHeights.has(index)) {
       const row = renderRowOffscreen(index);
       rowHeights.set(index, row.offsetHeight);
     }
     return rowHeights.get(index);
   }
   ```
3. **Use library with variable height support:**
   ```javascript
   // TanStack Virtual handles variable heights
   import { useVirtualizer } from '@tanstack/react-virtual';

   const virtualizer = useVirtualizer({
     count: benchmarkResults.length,
     getScrollElement: () => containerRef.current,
     estimateSize: () => 60,  // Estimate, library measures actual
     overscan: 5,
   });
   ```

**Detection:**
- Scroll position jumps when scrolling fast
- Some rows not rendered (blank spaces)
- Scrollbar size changes while scrolling
- "Unable to calculate scroll position" errors

**Relevant Phase:** Phase 1 (UI performance) — Consider if rendering >100 results

---

### Pitfall 10: Docker Layer Cache Invalidation from ORDER Changes
**What goes wrong:** Dockerfile installs system dependencies, copies application code, then pip installs Python packages. Developer changes one line of Rust code → Dockerfile COPY invalidates → all downstream layers (pip install) rebuild → 5 minute validation instead of 30 seconds.

**Why it happens:** Docker layer cache follows strict rule: when a layer changes, ALL downstream layers rebuild. Instruction order matters critically:
```dockerfile
# BAD: Code changes invalidate expensive pip install
COPY . /app
RUN pip install -r requirements.txt  # Rebuilds on any code change
```

**Prevention:**
1. **Copy dependency files first, then install, then copy code:**
   ```dockerfile
   # GOOD: Dependency changes rare, code changes frequent
   COPY requirements.txt /app/
   RUN pip install -r requirements.txt  # Cached unless requirements.txt changes
   COPY . /app  # Code changes don't invalidate pip cache
   ```
2. **Use .dockerignore to prevent cache invalidation from irrelevant files:**
   ```dockerignore
   # .dockerignore
   .git/
   target/       # Rust build artifacts
   *.md          # Documentation changes don't invalidate
   tests/        # Test changes don't invalidate production build
   .planning/
   ```
3. **Separate rarely-changing layers from frequently-changing:**
   ```dockerfile
   # Layer 1: Base OS (changes never)
   FROM python:3.11

   # Layer 2: System deps (changes monthly)
   RUN apt-get update && apt-get install -y build-essential

   # Layer 3: Python deps (changes weekly)
   COPY requirements.txt .
   RUN pip install -r requirements.txt

   # Layer 4: Application code (changes hourly)
   COPY . /app
   ```

**Detection:**
- Build logs show "pip install" running on every build
- BuildKit cache stats show low cache hit rate
- Build time varies 10× between runs (cache hit vs miss)
- docker build --progress=plain shows "CACHED" for few layers

**Relevant Phase:** Phase 3 (Docker validation optimization) — Review Dockerfile layer ordering

---

## Minor Pitfalls

### Pitfall 11: LLM Prompt Version Tracking Gaps
**What goes wrong:** Team improves few-shot examples in recovery prompt. Some LLM calls use new prompt, others use old cached version. A/B comparison shows no improvement (50% old, 50% new = average unchanged). Can't determine if prompt change helped.

**Prevention:**
- Include prompt version in cache key (see Pitfall 4)
- Log prompt hash with every LLM call
- Increment PROMPT_VERSION constant when changing templates

**Detection:**
- Prompt improvements show no measured effect
- Same input sometimes resolves correctly, sometimes fails
- Cache hit rate unusually high after prompt change

**Relevant Phase:** Phase 2 (LLM accuracy) — Implement during prompt engineering

---

### Pitfall 12: Web Worker Communication Overhead
**What goes wrong:** Move heavy processing to Web Worker to unblock main thread. Send 10MB dataset to worker via postMessage. Main thread freezes for 2 seconds during structured cloning. Worker speedup negated by communication cost.

**Prevention:**
1. **Use Transferable objects to avoid copying:**
   ```javascript
   const buffer = new ArrayBuffer(10_000_000);
   worker.postMessage({ data: buffer }, [buffer]);  // Transfer ownership
   ```
2. **Process data in chunks instead of all-at-once:**
   ```javascript
   // Send small chunks, process incrementally
   for (const chunk of dataChunks) {
     worker.postMessage({ chunk });
   }
   ```

**Detection:**
- Main thread freezes during postMessage call
- DevTools shows "Structured Clone" taking >100ms
- Worker speedup less than expected

**Relevant Phase:** Phase 1 (UI performance) — If using Web Workers

---

### Pitfall 13: Progressive Rendering Without Skeleton Loading
**What goes wrong:** Implement progressive loading (show results as they arrive). First result takes 5 seconds. User sees blank page for 5 seconds, assumes page broken, refreshes.

**Prevention:**
- Show loading skeleton immediately (fake UI placeholders)
- Display "Loading benchmark cases (0/100)" progress indicator
- Render UI structure before data arrives

**Detection:**
- User complaints: "Is it loading?" or "Page seems broken"
- High bounce rate during initial load
- Analytics show refresh actions during load

**Relevant Phase:** Phase 1 (UI startup) — Include in non-blocking startup work

---

### Pitfall 14: SSE Event ID Not Persisted for Reconnection
**What goes wrong:** SSE connection drops at result 47/100. Browser reconnects with Last-Event-ID header. Server doesn't track event IDs, restarts stream from beginning. User sees duplicate results 1-47, progress lost.

**Prevention:**
```python
def generate_with_ids():
    event_id = 0
    for result in run_benchmark():
        event_id += 1
        yield f"id: {event_id}\n"
        yield f"data: {json.dumps(result)}\n\n"

@app.route('/benchmark/stream')
def stream():
    last_id = request.headers.get('Last-Event-ID', 0)
    return Response(generate_from(int(last_id)))
```

**Detection:**
- Duplicate results appear after reconnection
- Client-side result count doesn't match server count
- Event logs show "Reconnected but restarted from 0"

**Relevant Phase:** Phase 1 (UI streaming) — Add if implementing SSE reconnection

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Phase 1: UI Real-Time Updates | Event listener leaks (Pitfall 1) + DOM thrashing (Pitfall 2) + SSE buffering (Pitfall 7) | Implement AbortController cleanup, batch DOM updates, disable proxy buffering |
| Phase 1: UI Startup | Main thread blocking (Pitfall 3) | Web Workers for dataset parsing, progressive loading, skeleton UI |
| Phase 2: LLM Accuracy | Cache staleness (Pitfall 4) + prompt versioning (Pitfall 11) | Hash-based cache keys, event-driven invalidation, prompt version tracking |
| Phase 2: LLM Inference | Batch size tuning (Pitfall 8) | Start conservative (batch_size=5), measure P95 latency, tune based on SLA |
| Phase 3: Docker Parallel Validation | Race conditions (Pitfall 6) + layer cache invalidation (Pitfall 10) | sharing=locked for pip/apt, optimize Dockerfile layer ordering |
| Phase 3: Parallelization | Premature optimization (Pitfall 5) | Profile first, optimize highest-impact bottleneck, measure improvement |
| All Phases | Optimization without measurement (Pitfall 5) | Establish baseline, profile, set success criteria, compare before/after |

---

## Sources

**Real-Time UI & Memory Leaks:**
- https://stackinsight.dev/blog/memory-leak-empirical-study/ (2025 study, 500-repo analysis, HIGH confidence)
- https://medium.com/@deval93/javascript-memory-leaks-in-2025-how-to-detect-prevent-and-fix-them-ade013bd8b46 (January 2025, MEDIUM confidence)
- https://dev.to/alex_aslam/how-to-avoid-memory-leaks-in-javascript-event-listeners-4hna (AbortController patterns, HIGH confidence)

**DOM Performance & Virtual Scrolling:**
- https://james-priest.github.io/udacity-nanodegree-mws/course-notes/browser-rendering-optimization.html (DOM thrashing patterns, HIGH confidence)
- https://medium.com/@sohail_saifi/implementing-virtual-scrolling-for-lists-with-100k-items-65867980c917 (2024, virtual scrolling best practices, MEDIUM confidence)
- https://kitemetric.com/blogs/virtual-scroll (TanStack Virtual, 2025, MEDIUM confidence)

**Web Workers & Main Thread:**
- https://medium.com/@QuarkAndCode/web-workers-in-javascript-limits-usage-best-practices-2025-a365b36beaa2 (November 2025, MEDIUM confidence)
- https://web.dev/off-main-thread/ (Official Chrome guidance, HIGH confidence)

**LLM Optimization & Caching:**
- https://www.systemoverflow.com/learn/ml-model-serving/model-monitoring-observability/semantic-caching-and-retrieval-invalidation (2025, cache invalidation, MEDIUM confidence)
- https://dasroot.net/posts/2026/02/caching-strategies-for-llm-responses/ (February 2026, staleness detection, MEDIUM confidence)
- https://amitkoth.com/llm-caching-strategies/ (2025, prompt versioning, MEDIUM confidence)
- https://www.databricks.com/blog/llm-inference-performance-engineering-best-practices (Official Databricks, batch tuning, HIGH confidence)
- https://medium.com/@sumanta.boral/strategies-for-reducing-llm-inference-latency-and-making-tradeoffs-lessons-from-building-9434a98e91bc (August 2025, latency-throughput tradeoffs, MEDIUM confidence)

**Docker BuildKit & Parallel Builds:**
- https://docs.docker.com/build/cache/optimize/ (Official Docker docs, layer cache, HIGH confidence)
- https://yuki-nakamura.com/2024/03/08/use-a-locked-run-cache-between-builds-in-buildkit/ (March 2024, sharing=locked, HIGH confidence)
- https://depot.dev/blog/how-to-use-cache-mount-to-speed-up-docker-builds (2025, cache mount patterns, MEDIUM confidence)

**Server-Sent Events & Streaming:**
- https://dev.to/miketalbot/server-sent-events-are-still-not-production-ready-after-a-decade-a-lesson-for-me-a-warning-for-you-2gie (2024, SSE production pitfalls, MEDIUM confidence)
- https://learn.microsoft.com/en-us/answers/questions/5573038/issues-with-sse-(server-side-events)-on-azure-app (Azure/IIS buffering, HIGH confidence)
- https://maxhalford.github.io/blog/flask-sse-no-deps/ (Flask SSE implementation, MEDIUM confidence)

**Premature Optimization:**
- https://stackify.com/premature-optimization-evil/ (Classic reference, HIGH confidence)
- https://medium.com/@satyendra.jaiswal/the-premature-optimization-pitfall-anti-pattern-navigating-the-maze-of-efficient-code-afc150b91bd2 (2024, cognitive biases, MEDIUM confidence)
- https://www.qt.io/quality-assurance/blog/premature-optimization (Stop over-engineering, MEDIUM confidence)

**Overall Confidence:** MEDIUM-HIGH
- Critical pitfalls (1-7) verified with multiple sources including official docs and 2025-2026 publications
- Moderate/minor pitfalls (8-14) based on established patterns and recent articles
- All findings align with known APDR issues from CONCERNS.md (memory leaks, blocking, cache problems, sequential validation)
