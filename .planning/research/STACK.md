# Technology Stack: Performance Optimization

**Project:** APDR Enhancement - Real-time UI, LLM Inference, Docker Validation
**Researched:** 2026-03-25
**Confidence:** MEDIUM-HIGH (WebSearch verified with official docs where available)

## Executive Summary

This stack focuses on **optimization patterns** for APDR's existing vanilla JS + Flask + Ollama + Docker architecture. No framework rewrites - only performance improvements through:

1. **Real-time Web UI**: Web Workers + Server-Sent Events + Progressive Loading
2. **LLM Inference**: Prompt caching + Request batching + Parallel execution
3. **Docker Validation**: BuildKit cache mounts + Parallel stages + Layer optimization

All recommendations target 2025/2026 best practices with minimal dependency additions.

---

## 1. Real-Time Web UI Optimization

### Core Technologies

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| **Web Workers API** | Native (ES2015+) | Offload benchmark processing to background thread | Prevents UI freeze during parallel worker execution. Built-in browser API, no dependencies. |
| **Server-Sent Events (SSE)** | Native (HTML5) | Stream real-time updates from Flask to browser | Unidirectional server→client updates, simpler than WebSockets for this use case. Native browser API. |
| **IndexedDB API** | Native (HTML5) | Progressive database loading | Non-blocking startup, load benchmark data incrementally. File System Access API (2023+) offers 4x better performance but IndexedDB has universal support. |
| **requestAnimationFrame** | Native | Throttle DOM updates to display refresh rate | Smooth UI updates aligned with browser rendering cycle (60fps). Prevents over-rendering. |

**Confidence:** HIGH - All are mature browser APIs with extensive MDN documentation and 2025 best practices.

### Supporting Patterns

#### Pattern 1: Web Worker Message Passing
**What:** Main thread delegates benchmark execution to worker, receives progress updates via `postMessage`.
**When:** Benchmark runs with 10+ parallel workers.
**Implementation:**
```javascript
// main.js
const worker = new Worker('/static/js/benchmark-worker.js', { type: 'module' });
worker.postMessage({ action: 'run', config: benchmarkConfig });
worker.onmessage = (e) => {
  if (e.data.type === 'progress') updateUI(e.data.result);
  if (e.data.type === 'complete') finalizeUI(e.data.summary);
};

// benchmark-worker.js
self.onmessage = async (e) => {
  for (const testCase of e.data.config.cases) {
    const result = await runTest(testCase);
    self.postMessage({ type: 'progress', result });
  }
};
```

**Source:** [MDN Web Workers API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API) (MEDIUM confidence)

#### Pattern 2: Server-Sent Events with Flask
**What:** Flask endpoint streams results as they complete using generator + `stream_with_context`.
**When:** Real-time progress updates from backend.
**Implementation:**
```python
# Flask backend
from flask import Response, stream_with_context
import json

@app.route('/api/benchmark/stream')
def benchmark_stream():
    def generate():
        for result in run_benchmark_cases():
            yield f"data: {json.dumps(result)}\n\n"
    return Response(stream_with_context(generate()),
                    mimetype='text/event-stream')

# JavaScript frontend
const eventSource = new EventSource('/api/benchmark/stream');
eventSource.onmessage = (e) => {
  const result = JSON.parse(e.data);
  updateBenchmarkUI(result);
};
```

**Key Requirements:**
- Flask must run in threaded mode (default in recent versions)
- WSGI server must use async workers if using Gunicorn (e.g., `worker_class=gevent`)
- Use `stream_with_context()` to maintain request context during generator execution

**Sources:**
- [Flask SSE Tutorial](https://medium.com/@alfininfo/flask-tutorial-implementing-server-sent-events-sse-for-real-time-updates-60103cd89fbf) (MEDIUM confidence)
- [Flask Official Streaming Docs](https://flask.palletsprojects.com/en/stable/patterns/streaming/) (HIGH confidence - official docs)

#### Pattern 3: Progressive IndexedDB Loading
**What:** Load seed data asynchronously at startup, show UI immediately.
**When:** Database load >500ms blocks user interaction.
**Implementation:**
```javascript
// Non-blocking startup
async function initDatabase() {
  const db = await openIndexedDB('apdr-cache');

  // Show UI immediately
  showUI();

  // Load data progressively in background
  const data = await fetch('/api/seed-data');
  const chunks = data.body.pipeThrough(new TextDecoderStream()).getReader();

  while (true) {
    const { done, value } = await chunks.read();
    if (done) break;
    await db.add('cache', JSON.parse(value));
    updateLoadingProgress();
  }
}
```

**Caveats:**
- IndexedDB transactions auto-close if you `await` inside them (cannot hold transaction across await point)
- Batch writes for performance (single transaction per 100+ records)
- Consider File System Access API for 4x performance boost (Chrome/Edge 86+, Safari 15.2+)

**Sources:**
- [IndexedDB Tutorial](https://javascript.info/indexeddb) (MEDIUM confidence)
- [IndexedDB Performance Analysis 2025](https://blog.logrocket.com/offline-first-frontend-apps-2025-indexeddb-sqlite/) (MEDIUM confidence)

#### Pattern 4: requestAnimationFrame Throttling
**What:** Batch DOM updates to run once per frame (16.67ms @ 60fps).
**When:** High-frequency updates (100+ events/sec) cause jank.
**Implementation:**
```javascript
let pendingUpdate = null;
let pendingResults = [];

function scheduleUIUpdate(result) {
  pendingResults.push(result);

  if (!pendingUpdate) {
    pendingUpdate = requestAnimationFrame(() => {
      updateDOM(pendingResults);
      pendingResults = [];
      pendingUpdate = null;
    });
  }
}
```

**When NOT to use:** API calls, analytics, non-visual updates (use throttle/debounce instead).

**Source:** [rAF Performance Guide 2025](https://dev.to/tawe/requestanimationframe-explained-why-your-ui-feels-laggy-and-how-to-fix-it-3ep2) (MEDIUM confidence)

### Dependencies

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| None | - | All browser native APIs | - |

**Anti-Pattern:** Adding React/Vue/Svelte for real-time updates. The existing vanilla JS + Vite stack can handle this with native APIs.

---

## 2. LLM Inference Optimization

### Core Technologies

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| **Ollama** | Latest (0.2.0+ Python SDK) | Local LLM inference | Already in stack. Supports prompt caching and parallel requests out-of-box. |
| **LiteLLM** | >=1.40 (current) | Unified LLM API + batching + caching | Already in stack. Adds batch_completion, prompt caching auto-injection, response caching. |
| **Instructor** | >=1.3 (current) | Structured output with Pydantic | Already in stack. Supports streaming for incremental validation. |

**Confidence:** HIGH - These are existing dependencies with official documentation.

### Optimization Techniques

#### Technique 1: Ollama Prompt Caching
**What:** Ollama caches KV tensors from prefill phase when prompt prefix matches byte-for-byte.
**Impact:** Skips expensive prefill computation for repeated prompt patterns.
**Configuration:**
```python
# Per-request control
response = ollama.chat(
    model='llama3.1',
    messages=messages,
    options={
        'keep_alive': '10m',  # Keep model loaded for 10 minutes
    }
)

# Environment-level control
# Set OLLAMA_KEEP_ALIVE=10m (default: 5m, '0' unloads immediately)
```

**Key Rules:**
- Prompt prefix must be **identical byte-for-byte** for cache hit
- Default `keep_alive=5m` unloads model from VRAM after 5 minutes idle
- Cache persists only while model is loaded in memory
- Use same system prompt + few-shot examples across requests

**Sources:**
- [Ollama Prompt Caching Guide](https://leanpub.com/read/ollama/prompt-caching) (HIGH confidence - official book)
- [Ollama FAQ](https://docs.ollama.com/faq) (HIGH confidence - official docs)

#### Technique 2: Ollama Parallel Requests
**What:** Ollama processes multiple requests concurrently for same model if memory permits.
**Impact:** 4x throughput improvement with `num_parallel=4` (35 tok/sec → linear scaling).
**Configuration:**
```bash
# Environment configuration
export OLLAMA_NUM_PARALLEL=4          # Default: auto (1 or 4 based on memory)
export OLLAMA_MAX_LOADED_MODELS=3     # Default: 3 * num_GPUs
export OLLAMA_MAX_QUEUE=512           # Default: 512 queued requests

# Memory requirement scales linearly
# RAM needed = OLLAMA_NUM_PARALLEL * OLLAMA_CONTEXT_LENGTH * model_size
# Example: 4 parallel * 2K context = 8K effective context → 4x memory
```

**Trade-offs:**
- Higher parallelism = more memory usage
- Diminishing returns beyond 4-8 parallel (memory bandwidth saturates)
- Queue overflow returns HTTP 503 (retry with backoff)

**Sources:**
- [Ollama Parallel Requests Analysis](https://www.glukhov.org/post/2025/05/how-ollama-handles-parallel-requests/) (HIGH confidence - detailed technical breakdown)
- [Ollama FAQ: Concurrency](https://docs.ollama.com/faq) (HIGH confidence - official docs)

#### Technique 3: LiteLLM Batch Completion
**What:** Process multiple prompts in single API call with concurrent execution.
**Impact:** Reduces overhead from per-request serialization, connection pooling.
**Implementation:**
```python
from litellm import batch_completion

# Batch multiple import resolution requests
messages_batch = [
    [{"role": "user", "content": f"Resolve import: {imp}"}]
    for imp in unresolved_imports
]

responses = batch_completion(
    model="ollama/llama3.1",
    messages=messages_batch,
    max_workers=4  # Parallel execution
)
```

**Alternative Methods:**
- `batch_completion_models()`: Race multiple models, return first response
- `batch_completion_models_all_responses()`: Query multiple models, return all

**Source:** [LiteLLM Batching Docs](https://docs.litellm.ai/docs/completion/batching) (HIGH confidence - official docs)

#### Technique 4: LiteLLM Prompt Caching Auto-Injection
**What:** Automatically inject cache_control directives without modifying application code.
**Impact:** Reduces cost/latency for repeated long prompts (1024+ tokens).
**Configuration:**
```yaml
# litellm_config.yaml
model_list:
  - model_name: llama3.1
    litellm_params:
      model: ollama/llama3.1
      cache_control_injection_points:
        - role: system  # Cache the system prompt
        - role: user
          index: 0      # Cache first user message (few-shot examples)
```

**Provider Support:**
- **OpenAI**: Automatic for 1024+ token prompts, optional `prompt_cache_key` for explicit control
- **Anthropic/Gemini**: Requires `cache_control: {type: "ephemeral"}` in messages
- **Ollama**: Native KV cache (no annotations needed)

**Source:** [LiteLLM Prompt Caching](https://docs.litellm.ai/docs/tutorials/prompt_caching) (HIGH confidence - official docs)

#### Technique 5: LiteLLM Response Caching
**What:** Cache LLM responses for identical requests (in-memory, Redis, or disk).
**Impact:** Instant response for duplicate queries (e.g., repeated package resolution).
**Configuration:**
```python
import litellm
from litellm import completion
from litellm.caching import Cache

# In-memory cache (development)
litellm.cache = Cache()

# Redis cache (production)
litellm.cache = Cache(
    type="redis",
    host="localhost",
    port=6379,
    ttl=3600  # 1 hour TTL
)

# Requests with identical params hit cache
response1 = completion(model="ollama/llama3.1", messages=messages)
response2 = completion(model="ollama/llama3.1", messages=messages)  # Cached
```

**Cache Backends:**
- In-memory: Fast, ephemeral (development)
- Redis: Distributed, persistent (production)
- Disk: Local persistence, slower than Redis
- S3/GCS: Cloud-native, highest latency

**Source:** [LiteLLM Caching](https://docs.litellm.ai/docs/caching/all_caches) (HIGH confidence - official docs)

#### Technique 6: Instructor Streaming Validation
**What:** Receive and validate Pydantic models incrementally as LLM generates.
**Impact:** Lower perceived latency, early error detection.
**Implementation:**
```python
import instructor
from pydantic import BaseModel
from typing import Iterable

class PackageResolution(BaseModel):
    import_name: str
    package_name: str
    confidence: float

client = instructor.from_litellm(litellm.completion)

# Stream partial results
for partial in client.chat.completions.create_partial(
    model="ollama/llama3.1",
    messages=messages,
    response_model=PackageResolution,
    stream=True
):
    # Each partial is a validated (possibly incomplete) Pydantic model
    if partial.confidence and partial.confidence > 0.8:
        yield partial
```

**Benefits:**
- Progressive UI updates (show results as they arrive)
- Early validation errors (fail fast on malformed output)
- Lighter weight than LangChain/LlamaIndex

**Source:** [Instructor Docs](https://python.useinstructor.com/) (MEDIUM confidence - official site but lacks streaming examples)

### Recommended Configuration

```bash
# Ollama environment (.env)
OLLAMA_NUM_PARALLEL=4          # 4 concurrent requests per model
OLLAMA_KEEP_ALIVE=10m          # Keep models loaded 10 minutes
OLLAMA_MAX_LOADED_MODELS=2     # Tier3 LLM + recovery agent
OLLAMA_MAX_QUEUE=256           # Moderate queue depth

# Application-level
MAX_LLM_BATCH_SIZE=10          # Batch up to 10 imports per LiteLLM call
ENABLE_RESPONSE_CACHE=true     # Cache repeated resolutions
RESPONSE_CACHE_TTL=3600        # 1 hour cache TTL
```

### Anti-Patterns to Avoid

| Anti-Pattern | Why Bad | Instead |
|--------------|---------|---------|
| Per-import LLM call without batching | 10x overhead from connection/serialization | Use `batch_completion()` for 5-10 imports |
| Different system prompt per request | Breaks prompt cache (no prefix match) | Standardize system prompt, vary only user message |
| Setting `keep_alive=0` | Reloads model from disk every request (~5-10s penalty) | Use `keep_alive=10m` minimum |
| Synchronous LLM calls in Flask route | Blocks worker thread, limits concurrency | Move to background task or use async Flask |

---

## 3. Docker Validation Optimization

### Core Technologies

| Technology | Version | Purpose | Why |
|------------|---------|---------|-----|
| **Docker BuildKit** | Default since Docker 23.0 | Parallel builds, cache mounts, layer optimization | Industry standard (2025). Mandatory for modern Docker performance. |
| **BuildKit Cache Mounts** | Dockerfile 1.3+ syntax | Persistent pip cache across builds | Avoids re-downloading packages. 70%+ build time reduction. |
| **Multi-stage Parallel Builds** | Native BuildKit | Build independent stages concurrently | Parallel Python version validation (py37, py38, py39, py310 in parallel). |

**Confidence:** HIGH - Official Docker documentation, widely adopted in 2025.

### Optimization Techniques

#### Technique 1: BuildKit Cache Mounts for pip
**What:** Mount persistent cache directory for pip downloads/wheels across builds.
**Impact:** 70%+ faster builds by reusing downloaded packages.
**Implementation:**
```dockerfile
# syntax=docker/dockerfile:1.3

FROM python:3.11-slim

# Cache pip downloads
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt
```

**Key Points:**
- Cache persists in Docker's internal storage (not in final image)
- Same cache location works for pip, pipenv, poetry (`/root/.cache/pip`)
- Add `sharing=locked` for parallel builds to same cache: `--mount=type=cache,target=/root/.cache/pip,sharing=locked`
- Cache survives container deletion but not Docker daemon restart (in ephemeral CI)

**Sources:**
- [Docker BuildKit Cache Optimization](https://docs.docker.com/build/cache/optimize/) (HIGH confidence - official docs)
- [pip Cache with BuildKit](https://pythonspeed.com/articles/docker-cache-pip-downloads/) (MEDIUM confidence - expert blog)

#### Technique 2: Parallel Multi-Stage Builds
**What:** BuildKit analyzes build graph, runs independent stages concurrently.
**Impact:** Validate 4 Python versions in parallel instead of sequentially.
**Implementation:**
```dockerfile
# syntax=docker/dockerfile:1.3

# Independent validation stages run in parallel
FROM python:3.8-slim AS validate-py38
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt && python smoke_test.py

FROM python:3.9-slim AS validate-py39
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt && python smoke_test.py

FROM python:3.10-slim AS validate-py310
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt && python smoke_test.py

# Final stage depends on all validations
FROM python:3.11-slim AS final
COPY --from=validate-py38 /app /tmp/py38-ok
COPY --from=validate-py39 /app /tmp/py39-ok
COPY --from=validate-py310 /app /tmp/py310-ok
```

**BuildKit automatically:**
- Runs `validate-py38`, `validate-py39`, `validate-py310` in parallel
- Shares cache mounts with `sharing=locked` to avoid conflicts
- Blocks `final` stage until all `COPY --from` dependencies complete

**Source:** [BuildKit Parallel Builds](https://www.gasparevitta.com/posts/advanced-docker-multistage-parallel-build-buildkit/) (MEDIUM confidence - expert blog)

#### Technique 3: Layer Ordering Optimization
**What:** Order Dockerfile commands from least-to-most frequently changing.
**Impact:** Maximizes layer cache hits across builds.
**Pattern:**
```dockerfile
# 1. System dependencies (changes rarely)
RUN apt-get update && apt-get install -y gcc

# 2. Python version lock (changes rarely)
FROM python:3.11.8-slim

# 3. Requirements file (changes occasionally)
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# 4. Application code (changes frequently)
COPY . /app
```

**Rule:** Each `RUN`, `COPY`, `ADD` creates a layer. Layers are cached until their content or prior layers change.

**Source:** [Docker Layer Caching Best Practices](https://docs.docker.com/build/cache/optimize/) (HIGH confidence - official docs)

#### Technique 4: BuildKit External Cache (CI/CD)
**What:** Export build cache to registry/S3, import on next build (for ephemeral CI).
**Impact:** Persistent cache across CI runs (otherwise cache lost on runner termination).
**Implementation:**
```bash
# Export cache to registry
docker buildx build \
  --cache-to type=registry,ref=myregistry/apdr-cache \
  --cache-from type=registry,ref=myregistry/apdr-cache \
  -t apdr:latest .

# Or export to local directory
docker buildx build \
  --cache-to type=local,dest=/tmp/buildkit-cache \
  --cache-from type=local,src=/tmp/buildkit-cache \
  -t apdr:latest .
```

**Cache Backends:**
- `type=inline`: Cache stored in image layers (simplest, larger images)
- `type=registry`: Separate cache manifest in registry (recommended for CI)
- `type=local`: Directory on disk (fast, requires persistent storage)
- `type=s3`: AWS S3 (cloud-native, higher latency)

**Source:** [Docker External Caching](https://docs.docker.com/build/cache/optimize/) (HIGH confidence - official docs)

#### Technique 5: Limit BuildKit Log Output
**What:** Prevent `[output clipped, log limit 2MiB reached]` errors during verbose builds.
**Impact:** See full build logs for debugging.
**Configuration:**
```bash
# Linux: Add to Docker systemd service
BUILDKIT_STEP_LOG_MAX_SIZE=-1      # Disable size limit (default: 2MiB)
BUILDKIT_STEP_LOG_MAX_SPEED=-1     # Disable throughput limit (default: 200KiB/s)

# Docker Compose
services:
  builder:
    environment:
      - BUILDKIT_STEP_LOG_MAX_SIZE=-1
      - BUILDKIT_STEP_LOG_MAX_SPEED=-1
```

**Windows Note:** Configuring these variables in Docker Desktop is undocumented. Workaround: Use `--progress=plain` to pipe logs directly.

**Source:** [BuildKit Log Limits Discussion](https://github.com/docker/for-mac/issues/6332) (MEDIUM confidence - GitHub issue)

#### Technique 6: Parallel Build Concurrency Control
**What:** Limit BuildKit parallelism for low-resource environments.
**Impact:** Prevents OOM on CI runners with limited CPU/RAM.
**Configuration:**
```toml
# buildkitd.toml
[worker.oci]
  max-parallelism = 4  # Limit to 4 concurrent build steps
```

```bash
# Create builder with custom config
docker buildx create \
  --name limited-builder \
  --buildkitd-config buildkitd.toml \
  --use
```

**Source:** [BuildKit Configuration](https://docs.docker.com/build/buildkit/configure/) (HIGH confidence - official docs)

### Recommended Dockerfile Structure

```dockerfile
# syntax=docker/dockerfile:1.3

# Stage 1: Base dependencies (cached)
FROM python:3.11-slim AS base
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make && \
    rm -rf /var/lib/apt/lists/*

# Stage 2: Python dependencies (cached with pip cache mount)
FROM base AS dependencies
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install --no-cache-dir -r requirements.txt

# Stage 3-6: Parallel validation for Python 3.8-3.11
FROM python:3.8-slim AS validate-py38
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /tmp/deps
COPY smoke_test.py .
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install -r requirements.txt && python smoke_test.py

# (Repeat for py39, py310, py311)

# Stage 7: Final image
FROM base AS final
COPY --from=dependencies /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=validate-py38 /tmp/py38-ok /tmp/py38-ok
COPY . /app
WORKDIR /app
CMD ["python", "main.py"]
```

### Anti-Patterns to Avoid

| Anti-Pattern | Why Bad | Instead |
|--------------|---------|---------|
| `RUN pip install` without cache mount | Re-downloads packages every build | Use `RUN --mount=type=cache,target=/root/.cache/pip` |
| Sequential Python version validation | 4x slower (serial builds) | Use multi-stage parallel builds |
| `COPY . /app` before `COPY requirements.txt` | Busts cache on code changes | Copy requirements first, then code |
| Using legacy Docker builder | No parallelism, no cache mounts | Set `DOCKER_BUILDKIT=1` (default since v23) |
| Building in CI without external cache | Starts from scratch every run | Use `--cache-to/--cache-from type=registry` |

---

## 4. Cross-Cutting Optimization Patterns

### Pattern: Async Python for I/O-Bound Tasks
**What:** Use `asyncio` for concurrent LLM calls and Docker API operations.
**Why:** Current synchronous code blocks on I/O (network requests, subprocess execution).
**Impact:** 4-10x throughput for I/O-bound workloads.

**Example:**
```python
import asyncio
from litellm import acompletion

async def resolve_imports_async(imports: list[str]) -> list[Resolution]:
    tasks = [
        acompletion(
            model="ollama/llama3.1",
            messages=[{"role": "user", "content": f"Resolve: {imp}"}]
        )
        for imp in imports
    ]
    return await asyncio.gather(*tasks)

# Usage
resolutions = asyncio.run(resolve_imports_async(unresolved_imports))
```

**Caveats:**
- Requires `async`/`await` throughout call stack (or use `asyncio.run()` at boundaries)
- LiteLLM supports `acompletion()` for async calls
- Flask requires async framework (Quart, FastAPI) or background tasks (Celery, RQ)

**Confidence:** HIGH - Python 3.11+ asyncio is mature, LiteLLM has native async support.

### Pattern: Progressive Enhancement for UI
**What:** Show deterministic results immediately, LLM results as they complete.
**Implementation:**
```javascript
// Tier 1/2 results render immediately (cache hits, heuristics)
renderDeterministicResults(tier1Results, tier2Results);

// Tier 3 results stream via SSE
const eventSource = new EventSource('/api/tier3/stream');
eventSource.onmessage = (e) => {
  const llmResult = JSON.parse(e.data);
  appendLLMResult(llmResult);  // Incremental append
};
```

**Confidence:** HIGH - Standard pattern for progressive web apps.

---

## Installation & Configuration

### Web UI Dependencies
```bash
# No new dependencies - all native browser APIs
# Existing: Vite 6.2.0 for build tooling
```

### Python Dependencies (Additions)
```bash
# Core dependencies already present in requirements.txt
pip install litellm>=1.40 instructor>=1.3 pydantic>=2.5

# Optional: async support
pip install aiohttp>=3.9  # For async HTTP in LiteLLM
```

### Docker BuildKit
```bash
# Enable BuildKit (default since Docker 23.0)
export DOCKER_BUILDKIT=1

# Verify BuildKit is active
docker buildx version
```

### Ollama Configuration
```bash
# Add to .env
OLLAMA_NUM_PARALLEL=4
OLLAMA_KEEP_ALIVE=10m
OLLAMA_MAX_LOADED_MODELS=2
OLLAMA_MAX_QUEUE=256
```

---

## Alternatives Considered

| Category | Recommended | Alternative | Why Not |
|----------|-------------|-------------|---------|
| Real-time UI | Server-Sent Events | WebSockets | SSE is simpler for unidirectional updates, no handshake overhead |
| Real-time UI | Web Workers | Service Workers | Service Workers are for offline/caching, not background computation |
| Progressive Loading | IndexedDB | File System Access API | IndexedDB has universal support (FSAA requires Chrome 86+) |
| LLM Batching | LiteLLM batch_completion | Custom asyncio.gather | LiteLLM handles rate limits, retries, provider quirks |
| Docker Caching | BuildKit cache mounts | Docker volumes | Cache mounts are ephemeral (not in image), simpler than volume management |
| Async Python | asyncio (stdlib) | Trio, Curio | asyncio is stdlib, widest ecosystem support (LiteLLM, aiohttp) |

---

## Performance Targets

| Metric | Current | Target | Technique |
|--------|---------|--------|-----------|
| **UI Responsiveness** | Browser freezes during runs | <16ms frame time (60fps) | Web Workers + requestAnimationFrame |
| **Startup Time** | 3+ sec database load | <500ms to interactive | Progressive IndexedDB loading |
| **LLM Throughput** | 1 request/sec (sequential) | 4-8 requests/sec | Ollama parallel + LiteLLM batching |
| **Docker Build Time** | 4x sequential builds | Parallel validation | BuildKit multi-stage parallel + cache mounts |
| **LLM Latency** (cache hit) | 5-10s model load | <100ms response | Ollama keep_alive + prompt caching |

---

## Migration Path

### Phase 1: Web UI (Low Risk)
1. Add Web Worker for benchmark execution
2. Implement SSE endpoint in Flask (`/api/benchmark/stream`)
3. Add requestAnimationFrame throttling to DOM updates
4. Progressive IndexedDB loading for seed data

**Risk:** Low - All browser APIs, graceful degradation possible.

### Phase 2: LLM Optimization (Medium Risk)
1. Configure Ollama environment variables (`OLLAMA_NUM_PARALLEL`, `OLLAMA_KEEP_ALIVE`)
2. Standardize system prompts for cache hit rate
3. Batch import resolutions using `litellm.batch_completion()`
4. Add LiteLLM response caching (in-memory initially)

**Risk:** Medium - Requires testing with production workload to tune batch size, cache TTL.

### Phase 3: Docker Optimization (Medium Risk)
1. Update Dockerfiles to `syntax=docker/dockerfile:1.3`
2. Add cache mounts to `RUN pip install` commands
3. Restructure validation as parallel multi-stage builds
4. Configure BuildKit external cache for CI

**Risk:** Medium - BuildKit is default but requires Dockerfile syntax changes.

### Phase 4: Async Migration (High Risk - Optional)
1. Migrate Flask routes to async (FastAPI or Quart)
2. Convert LLM calls to `acompletion()`
3. Use `asyncio.gather()` for concurrent Docker operations

**Risk:** High - Requires architectural changes, async propagation through call stack.

---

## Sources

### Official Documentation (HIGH Confidence)
- [MDN Web Workers API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API)
- [Flask Streaming Patterns](https://flask.palletsprojects.com/en/stable/patterns/streaming/)
- [Docker BuildKit Cache Optimization](https://docs.docker.com/build/cache/optimize/)
- [Ollama FAQ](https://docs.ollama.com/faq)
- [LiteLLM Batching](https://docs.litellm.ai/docs/completion/batching)
- [LiteLLM Prompt Caching](https://docs.litellm.ai/docs/tutorials/prompt_caching)

### Expert Blogs & Tutorials (MEDIUM Confidence)
- [Flask SSE Tutorial 2025](https://medium.com/@alfininfo/flask-tutorial-implementing-server-sent-events-sse-for-real-time-updates-60103cd89fbf)
- [Ollama Parallel Requests Deep Dive](https://www.glukhov.org/post/2025/05/how-ollama-handles-parallel-requests/)
- [BuildKit Parallel Builds](https://www.gasparevitta.com/posts/advanced-docker-multistage-parallel-build-buildkit/)
- [requestAnimationFrame Performance 2025](https://dev.to/tawe/requestanimationframe-explained-why-your-ui-feels-laggy-and-how-to-fix-it-3ep2)
- [IndexedDB Offline-First 2025](https://blog.logrocket.com/offline-first-frontend-apps-2025-indexeddb-sqlite/)

### Community Discussions (LOW-MEDIUM Confidence)
- [BuildKit Log Limits Issue](https://github.com/docker/for-mac/issues/6332)
- [Ollama Parallel Requests Issue](https://github.com/ollama/ollama/issues/358)

---

## Confidence Assessment

| Area | Confidence | Reason |
|------|------------|--------|
| **Web Workers & SSE** | HIGH | MDN official docs, Flask official docs, mature APIs |
| **IndexedDB** | MEDIUM | MDN docs confirmed, but 2025 performance claims from blog posts |
| **Ollama Optimization** | HIGH | Official Ollama docs + detailed community analysis |
| **LiteLLM Features** | HIGH | Official LiteLLM documentation, verified API examples |
| **BuildKit Caching** | HIGH | Docker official docs, widely adopted pattern in 2025 |
| **Async Python** | MEDIUM | Python official docs, but LiteLLM async support less documented |

**Overall Confidence:** MEDIUM-HIGH

**Gaps:**
- No official benchmarks for Instructor streaming performance (inferred from docs)
- File System Access API performance claims (4x vs IndexedDB) from single blog post
- Windows BuildKit environment variable configuration undocumented
- LiteLLM async performance vs sync not quantified

---

*Stack research: 2026-03-25*
