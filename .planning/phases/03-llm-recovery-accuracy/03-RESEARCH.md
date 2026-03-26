# Phase 3: LLM Recovery Accuracy - Research

**Researched:** 2026-03-25
**Domain:** LLM caching, prompt versioning, test infrastructure, metrics collection
**Confidence:** MEDIUM

## Summary

Phase 3 focuses on improving LLM recovery accuracy by validating suggestions and implementing proper cache invalidation. Research reveals that **4 of 5 requirements are already implemented** (REC-01, REC-02, REC-04, REC-05), with only **REC-03 (cache invalidation based on prompt hash)** requiring new work.

The primary technical challenge is injecting a prompt version hash into LiteLLM's cache keys to prevent stale suggestions when prompts change. LiteLLM generates cache keys automatically from `(model, messages, params)` but does NOT include prompt template content. This means changing the recovery system prompt or user prompt template won't invalidate existing cache entries.

**Primary recommendation:** Use LiteLLM's custom `cache.get_cache_key()` override to inject a SHA256 hash of `(system_prompt + user_template + model_id)` into the cache key generation logic. This provides automatic invalidation without breaking existing cache entries or requiring manual cache clearing.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**D-01**: Keep post-hoc PyPI validation approach (after LLM completion) — simpler than in-loop validation

**D-02**: Validate ALL suggested packages: `correct_package`, `add_package` (currently implemented)

**D-03**: Cache PyPI validation results in-memory for session lifetime (currently implemented)

**D-04**: On network errors, assume package exists to avoid false negatives (currently: line 39)

**D-05**: Add test coverage for hallucinated package rejection scenarios

**D-06**: Keep top-3 pattern injection to avoid prompt bloating (currently implemented)

**D-07**: Pattern matching uses regex with fallback to substring match for malformed patterns (currently implemented)

**D-08**: Patterns ordered most-specific to most-general (currently implemented)

**D-09**: Add "Build error pattern library matched" note when patterns trigger (currently: line 121)

**D-10**: Implement prompt version hash injection into LiteLLM cache keys

**D-11**: Hash strategy: SHA256 of (system_prompt + user_prompt_template + model_id)

**D-12**: User prompt template = user prompt with placeholders for dynamic content (error_log, packages, etc.)

**D-13**: Include model ID in cache key (already handled by LiteLLM, verify it works)

**D-14**: Cache invalidation is automatic — changing hash produces new cache key, old entries expire naturally

**D-15**: Add cache hit/miss metrics to track effectiveness

**D-16**: Do NOT invalidate on dynamic content changes (error logs, package lists) — only on prompt structure or model changes

**D-17**: Keep 0.4 threshold for solvability skipping (validated in prior work)

**D-18**: Keep 0.95 threshold for unsolvable cache persistence (prevents false positives)

**D-19**: Recovery confidence scores follow stratified approach:
  - LLM-based recovery: 0.65 (recovery.py:1037)
  - Heuristic version adjustments: 0.60-0.78 (resolver/mod.rs)
  - Last-resort stripping: 0.50-0.55 (resolver/mod.rs)

**D-20**: Add confidence score to LLM recovery response model if not present

**D-21**: Surface confidence scores in UI (already done in Phase 2 for tier3 cases)

**D-22**: Keep default of 5 attempts (validated balance between success rate and runtime)

**D-23**: Each attempt can include multiple recovery strategies in sequence:
  - LLM recovery hint application
  - Seed fallback
  - Version stripping
  - Last-resort constraint relaxation

**D-24**: Attempt limit applies to full validation loop (not just LLM calls)

**D-25**: Add attempt count to result metadata for observability

**D-26**: Add metrics for average attempts per case and success rate by attempt number

### Claude's Discretion

- **Test coverage strategy**: Choose between unit tests vs integration tests based on component isolation
- **Metrics implementation**: Decide on logging format, aggregation level, and storage location
- **Error handling**: Design fallback behavior when PyPI validation fails or pattern matching errors occur
- **Performance optimization**: Decide if prompt hash should be pre-computed or computed on-demand

### Deferred Ideas (OUT OF SCOPE)

**Advanced Pattern Matching:**
- Fuzzy pattern matching with edit distance
- Pattern confidence scores based on match strength
- Auto-learning new patterns from failure logs
→ Deferred to future phase (v2 requirement)

**Multi-Model Confidence Voting:**
- Run recovery with multiple models and vote on suggestions
- Average confidence scores across models
- Use highest-confidence suggestion
→ Deferred to Phase 4 (LLM Performance Optimization)

**Prompt Engineering Experiments:**
- A/B testing different recovery prompt templates
- Chain-of-thought vs direct completion comparison
- Few-shot example optimization
→ Deferred to future research phase (not in current roadmap)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| REC-01 | Recovery suggestions validate package exists on PyPI before suggesting | ✅ Already implemented via `pypi_checker.py` + post-hoc validation in `recovery.py:153-165` |
| REC-02 | Error pattern matching uses RAG-enhanced recovery prompts | ✅ Already implemented via `build_error_patterns.py` + `format_error_context()` injection |
| REC-03 | Cache invalidation based on prompt hash + model ID (prevent stale suggestions) | ⚠️ PRIMARY WORK AREA — needs custom `cache.get_cache_key()` implementation |
| REC-04 | Recovery confidence scoring to skip low-confidence suggestions | ✅ Already implemented via confidence thresholds (0.4 solvability, 0.95 unsolvable cache) |
| REC-05 | Recovery attempt limit enforced (max 5 attempts per case) | ✅ Already implemented via `resolver/mod.rs:842` loop |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| litellm | 1.65+ | Multi-provider LLM client with caching | Industry standard for LLM abstraction, built-in disk cache, custom cache key support |
| instructor | 1.7+ | Structured LLM outputs via Pydantic | De facto standard for typed LLM responses, retry logic with validation errors |
| pytest | 8.3+ | Test framework | Python testing standard, extensive mocking ecosystem |
| pytest-mock | 3.14+ | Pytest fixture for unittest.mock | Cleaner mock syntax, automatic cleanup |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| requests | 2.32+ | PyPI package validation | Already used for HTTP checks |
| hashlib | stdlib | SHA256 hashing for cache keys | Built-in, no external dependency |
| json | stdlib | Stable serialization for hashing | Deterministic ordering via `sort_keys=True` |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| litellm custom cache key | Append hash to model name | Model name modification simpler but pollutes model strings, harder to debug |
| SHA256 | MD5 | MD5 faster but less collision-resistant; SHA256 is industry standard for cache keys |
| pytest | unittest | unittest is stdlib but pytest has better fixtures, parametrization, and mocking support |

**Installation:**
```bash
# Already installed in project
pip install litellm instructor pytest pytest-mock requests
```

**Version verification:**
```bash
pip show litellm instructor pytest pytest-mock | grep -E "(Name|Version)"
```

Current verified versions (as of 2026-03-25):
- litellm: 1.65+ (disk cache support confirmed in client.py:43-59)
- instructor: 1.7+ (used in client.py:178)
- pytest: 8.3+ (existing tests in `test_recovery_mock.py`)
- pytest-mock: Not yet installed — recommend adding to dev dependencies

## Cache Key Injection Mechanisms

LiteLLM generates cache keys automatically from `(model, messages, params)` but does NOT include prompt template content. Three approaches exist for injecting a prompt version hash:

### Approach 1: Custom `cache.get_cache_key()` Override (RECOMMENDED)

**How it works:**
```python
import hashlib
import json
from litellm.caching.caching import Cache

def custom_get_cache_key(*args, **kwargs):
    # Extract base parameters
    model = kwargs.get("model", "")
    messages = kwargs.get("messages", [])
    temperature = kwargs.get("temperature", 0.0)

    # Compute prompt hash from templates (not dynamic content)
    prompt_hash = compute_prompt_hash()

    # Build cache key with prompt version
    key = f"{model}:{prompt_hash}:{str(messages)}:t{temperature}"
    return key

# Initialize cache with custom key builder
cache = Cache(type="disk", disk_cache_dir="~/.apdr-cache/llm-cache")
cache.get_cache_key = custom_get_cache_key
litellm.cache = cache
```

**Pros:**
- Clean separation: prompt versioning logic lives in cache layer
- Doesn't pollute model names or messages
- Cache keys are human-readable with hash component
- Easy to debug: log cache key at generation time

**Cons:**
- Requires accessing litellm internals (`litellm.caching.caching.Cache`)
- Must be set BEFORE any LLM calls (during `LlmClient.__init__`)

### Approach 2: Inject Hash into Model Name

**How it works:**
```python
def _litellm_model(self, model_override: str | None = None) -> str:
    model = model_override or self.model
    prompt_hash = self._compute_prompt_hash()[:8]  # First 8 chars
    if self.provider == "ollama":
        return f"ollama_chat/{model}#{prompt_hash}"
    return f"{model}#{prompt_hash}"
```

**Pros:**
- Simple implementation (modify existing `_litellm_model()` method)
- No LiteLLM internals required
- Works with all cache backends

**Cons:**
- Pollutes model names with hash suffix (appears in logs, errors)
- Hash must be short to avoid model name length limits
- Provider-specific parsing may break with `#` suffix
- Harder to debug: hash embedded in model string

### Approach 3: Inject Hash into Messages Metadata

**How it works:**
```python
kwargs["messages"] = [
    {"role": "system", "content": system_prompt},
    {"role": "user", "content": user_prompt},
    {"role": "metadata", "prompt_version": prompt_hash},  # Non-standard
]
```

**Pros:**
- Hash travels with messages, not model name
- LiteLLM cache keys include full messages list

**Cons:**
- Non-standard messages format may break provider parsing
- Some providers reject non-standard roles
- Doesn't work with all LiteLLM backends

### Comparison Table

| Approach | Implementation Complexity | Debuggability | Provider Compatibility | Maintainability |
|----------|---------------------------|---------------|------------------------|-----------------|
| Custom `get_cache_key()` | Medium (access internals) | High (explicit key) | High (no provider changes) | High (isolated logic) |
| Model name suffix | Low (modify 1 method) | Medium (hash in logs) | Medium (may break parsing) | Medium (scattered logic) |
| Messages metadata | Low (modify kwargs) | Low (invisible hash) | Low (provider rejection risk) | Low (non-standard) |

**Recommendation:** Use **Approach 1 (custom `get_cache_key()`)** for production. It's the cleanest separation of concerns and most debuggable. Use Approach 2 as fallback if LiteLLM internals prove unstable across versions.

## Prompt Hashing Implementation

### Hash Computation Strategy

Per decision D-11, hash **template structure** not dynamic content:

```python
import hashlib
import json

class LlmClient:
    def __init__(self, provider: str, model: str, base_url: str):
        self.provider = provider
        self.model = model
        self.base_url = base_url
        self._prompt_version_hash = self._compute_prompt_version()
        # ... rest of init

    def _compute_prompt_version(self) -> str:
        """Compute SHA256 hash of (system prompts + user templates + model).

        Returns first 16 chars of hex digest for brevity.
        """
        from .. import prompts

        # Collect all prompt templates used by this client
        templates = {
            "recovery_system": prompts.RECOVERY_SYSTEM,
            "recovery_user_template": self._extract_template(prompts.recovery_user),
            "solvability_system": getattr(prompts, "SOLVABILITY_SYSTEM", ""),
            "model": self.model,
        }

        # Stable serialization
        canonical = json.dumps(templates, sort_keys=True)

        # Hash
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

        # Return first 16 chars for brevity (64-bit collision resistance)
        return digest[:16]

    def _extract_template(self, prompt_fn) -> str:
        """Extract template structure from prompt function.

        Replace dynamic placeholders with fixed tokens to create stable template.
        Example:
          Input:  "Resolved packages:\n{resolved_packages}\n\nError:\n{error_log}"
          Output: "Resolved packages:\n{PLACEHOLDER}\n\nError:\n{PLACEHOLDER}"
        """
        import inspect
        source = inspect.getsource(prompt_fn)
        # Extract f-string or .format() template
        # Normalize placeholders to {PLACEHOLDER}
        # Return template structure only
        return source  # Simplified — real impl would parse template
```

### Template vs. Content Distinction

**Template** (INCLUDE in hash):
- System prompt constant strings
- User prompt structure and static text
- Placeholder positions (e.g., `{error_log}`)
- Model ID

**Content** (EXCLUDE from hash):
- Actual error logs (change per case)
- Resolved package lists (dynamic)
- Snippet source code (varies)
- Python version (config parameter)

**Example:**

```python
# Template (hash THIS):
recovery_user_template = """
Resolved packages:
{resolved_packages}

Build error:
{error_log}

Suggest a fix.
"""

# Content (do NOT hash):
resolved_packages = ["scrapy==1.8.0 (import: scrapy)"]
error_log = "ERROR: Command errored out with exit status 1..."
```

Hashing the template ensures cache invalidation when prompt **structure** changes (e.g., adding a new instruction) but NOT when **data** changes (different error logs for different cases).

### Hash Computation Cost

SHA256 hashing cost:
- Input size: ~2-5 KB (typical prompt templates)
- Computation time: ~0.1-0.5 ms (negligible vs 500-2000ms LLM call)
- Frequency: Once per `LlmClient` instantiation (cached in `self._prompt_version_hash`)

**Decision:** Compute on-demand during `__init__` and cache in instance variable. No need for pre-computation or persistent storage.

## Architecture Patterns

### Recommended Project Structure
```
tools/apdr/llm_py/
├── client.py               # LlmClient with cache key customization
├── actions/
│   └── recovery.py         # Recovery action handler (add metrics logging)
├── build_error_patterns.py # RAG pattern library (already complete)
├── pypi_checker.py         # PyPI validation cache (already complete)
├── prompts.py              # Prompt templates (extract template structure for hashing)
├── tests/
│   ├── test_recovery_mock.py       # Existing mock tests (expand coverage)
│   ├── test_cache_invalidation.py  # NEW: prompt hash integration tests
│   ├── test_pypi_validation.py     # NEW: PyPI rejection integration tests
│   └── conftest.py                 # NEW: shared fixtures
```

### Pattern 1: Cache Key Override

**What:** Custom cache key generation with prompt versioning

**When to use:** During `LlmClient.__init__` to configure caching before any LLM calls

**Example:**
```python
# Source: Research findings + LiteLLM docs
from litellm.caching.caching import Cache
import litellm

class LlmClient:
    def __init__(self, provider: str, model: str, base_url: str):
        self.provider = provider
        self.model = model
        self.base_url = base_url
        self._prompt_version_hash = self._compute_prompt_version()

        # Initialize instructor client
        self._instructor_client = instructor.from_litellm(litellm.completion)

        # Configure cache with custom key builder
        self._init_cache_with_versioning()

        # ... rest of init

    def _init_cache_with_versioning(self):
        """Initialize LiteLLM cache with prompt version injection."""
        from litellm.caching.caching import Cache

        cache_dir = str(Path.home() / ".apdr-cache" / "llm-cache")
        Path(cache_dir).mkdir(parents=True, exist_ok=True)

        cache = Cache(type="disk", disk_cache_dir=cache_dir)

        # Inject custom key builder
        original_get_key = cache.get_cache_key
        prompt_hash = self._prompt_version_hash

        def versioned_get_cache_key(*args, **kwargs):
            base_key = original_get_key(*args, **kwargs)
            # Prepend prompt version to cache key
            return f"v{prompt_hash}:{base_key}"

        cache.get_cache_key = versioned_get_cache_key
        litellm.cache = cache

        logger.info("LiteLLM cache enabled with prompt version %s", prompt_hash)
```

### Pattern 2: Metrics Logging

**What:** Structured logging for cache hits, PyPI rejections, pattern matches

**When to use:** At key decision points in recovery flow

**Example:**
```python
# Source: Research on Python LLM testing best practices
import logging
import time

logger = logging.getLogger("apdr_llm")

def handle(req: ResolutionRequest) -> ResolutionResponse:
    started = time.time()

    # Track cache hit/miss (if litellm exposes this)
    cache_hit = False  # Detect from litellm response metadata

    # ... recovery logic ...

    # Log PyPI validation rejection
    if result.fix_possible and result.correct_package:
        pkg_name = result.correct_package.split("==")[0]
        if not package_exists_on_pypi(pkg_name):
            logger.info(
                "PyPI validation rejected package",
                extra={
                    "action": "recovery",
                    "package": pkg_name,
                    "reason": "not_found_on_pypi",
                    "suggested_by": "llm",
                }
            )
            result.fix_possible = False

    # Log pattern match
    if error_pattern_ctx:
        logger.info(
            "RAG pattern library matched",
            extra={
                "action": "recovery",
                "patterns_matched": len(matched_patterns),
                "top_pattern": matched_patterns[0].diagnosis if matched_patterns else None,
            }
        )

    # Log cache metrics
    logger.info(
        "Recovery action completed",
        extra={
            "action": "recovery",
            "cache_hit": cache_hit,
            "duration_ms": int((time.time() - started) * 1000),
            "fix_possible": result.fix_possible,
        }
    )
```

### Pattern 3: Integration Test with Cache Invalidation

**What:** Test that changing prompts invalidates cache

**When to use:** In test suite to verify cache versioning works

**Example:**
```python
# Source: Research on pytest LLM testing strategies
import tempfile
from pathlib import Path
from unittest.mock import patch

def test_prompt_change_invalidates_cache():
    """Changing prompt templates should produce different cache keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir) / "cache"

        # First client with original prompts
        with patch("llm_py.prompts.RECOVERY_SYSTEM", "Original system prompt"):
            client1 = LlmClient("ollama", "test-model", "http://localhost:11434")
            hash1 = client1._prompt_version_hash

        # Second client with modified prompts
        with patch("llm_py.prompts.RECOVERY_SYSTEM", "Modified system prompt"):
            client2 = LlmClient("ollama", "test-model", "http://localhost:11434")
            hash2 = client2._prompt_version_hash

        # Hashes should differ
        assert hash1 != hash2, "Prompt change should produce different version hash"
```

### Anti-Patterns to Avoid

- **Hashing dynamic content:** Including error logs or package lists in hash defeats caching purpose
- **Ignoring hash collisions:** Use at least 64 bits (16 hex chars) of SHA256 digest to avoid collisions
- **Caching PyPI validation failures:** Network errors should not be cached as "package exists" (current impl correct)
- **Global mock pollution:** Use `@patch` decorators, not `unittest.mock.patch.object` on global state

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| LLM response caching | Custom Redis/SQLite cache layer | LiteLLM built-in disk cache | Already integrated, handles TTL, key generation, serialization |
| Structured LLM outputs | Manual JSON parsing + retry logic | Instructor library | Field-level validation, automatic retries with error context, Pydantic integration |
| Prompt template versioning | Git SHA or timestamp-based versioning | Content-based SHA256 hash | Git SHA changes on unrelated commits, timestamps drift across machines, content hash is deterministic |
| PyPI package validation | Scraping PyPI HTML or custom index | HEAD request to `pypi.org/pypi/{pkg}/json` | Official JSON API, redirects work, 404 = not found, already implemented |
| Test fixtures for LLM mocking | Custom mock class inheritance | pytest-mock + `@patch` decorators | Automatic cleanup, cleaner syntax, integrates with pytest parametrize |

**Key insight:** Prompt versioning is a cache invalidation problem, not a prompt management problem. Hashing template content provides automatic invalidation without manual version tracking or Git integration complexity.

## Test Infrastructure

### Existing Test Framework

**Detected:**
- Framework: pytest (inferred from `test_recovery_mock.py` imports)
- Config file: None found — using pytest defaults
- Test directory: `tools/apdr/llm_py/tests/`
- Existing tests: 13 test cases in `test_recovery_mock.py`
- Mock strategy: `unittest.mock.patch` decorators

**Quick run command:**
```bash
cd tools/apdr && python -m pytest llm_py/tests/ -v
```

**Full suite command:**
```bash
cd tools/apdr && python -m pytest llm_py/tests/ -v --tb=short
```

### Recommended Test Structure

```
tools/apdr/llm_py/tests/
├── __init__.py                    # Existing
├── conftest.py                    # NEW: shared fixtures
├── test_recovery_mock.py          # Existing: 13 mock tests
├── test_cache_invalidation.py     # NEW: prompt hash integration tests
├── test_pypi_validation.py        # NEW: PyPI rejection flow
├── test_pattern_matching.py       # NEW: RAG pattern library tests
└── test_confidence_thresholds.py  # NEW: confidence enforcement tests
```

### Mock Strategy

**Unit tests (fast, no network):**
- Mock `package_exists_on_pypi()` to return `True`/`False`
- Mock `LlmClient.complete_json()` to return `RecoveryResult` objects
- Mock `litellm.completion()` if testing cache layer directly

**Integration tests (slower, requires Ollama):**
- Use real LiteLLM cache with temporary directory
- Use real PyPI HEAD requests (add retry/timeout for flakiness)
- Use real pattern matching against sample error logs

**Example fixture (conftest.py):**
```python
import pytest
import tempfile
from pathlib import Path

@pytest.fixture
def temp_cache_dir():
    """Temporary cache directory for LiteLLM disk cache tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture
def mock_llm_response():
    """Factory fixture for creating RecoveryResult objects."""
    from llm_py.actions.recovery import RecoveryResult

    def _make_result(**overrides):
        defaults = dict(
            fix_possible=True,
            wrong_package="psycopg2",
            correct_package="psycopg2-binary",
            reasoning="Use binary package to avoid pg_config dependency.",
        )
        defaults.update(overrides)
        return RecoveryResult(**defaults)

    return _make_result

@pytest.fixture
def mock_pypi_checker(monkeypatch):
    """Mock PyPI checker to avoid network calls."""
    def _exists(pkg: str) -> bool:
        # Known-good packages
        return pkg.lower() not in {"fake-package", "scrapy-python27"}

    monkeypatch.setattr("llm_py.actions.recovery.package_exists_on_pypi", _exists)
```

### Test Coverage Map

| Requirement | Test Type | Automated Command | File |
|-------------|-----------|-------------------|------|
| REC-01: PyPI validation | Integration | `pytest llm_py/tests/test_pypi_validation.py -v` | NEW |
| REC-02: RAG patterns | Unit | `pytest llm_py/tests/test_pattern_matching.py -v` | NEW |
| REC-03: Cache invalidation | Integration | `pytest llm_py/tests/test_cache_invalidation.py -v` | NEW |
| REC-04: Confidence thresholds | Unit | `pytest llm_py/tests/test_confidence_thresholds.py -v` | NEW |
| REC-05: Max retry limit | Unit | `pytest llm_py/tests/test_recovery_mock.py::test_max_retries -v` | NEW case in existing file |

**Sampling rate:**
- Per task commit: `pytest llm_py/tests/ -x` (fail fast)
- Per wave merge: `pytest llm_py/tests/ -v` (full suite)
- Phase gate: Full suite green + manual smoke test with real Ollama

## Metrics Collection Points

### Where to Add Logging

| Location | Metric | Event | Format |
|----------|--------|-------|--------|
| `recovery.py:156` | PyPI rejection rate | Package validation fails | `logger.info("pypi_validation_rejected", extra={"package": pkg, "action": "recovery"})` |
| `recovery.py:120` | RAG pattern match rate | Pattern library triggers | `logger.info("pattern_matched", extra={"count": len(matches), "top": matches[0].diagnosis})` |
| `client.py:_init_cache()` | Cache configuration | Cache initialization | `logger.info("cache_initialized", extra={"prompt_version": hash})` |
| `client.py:complete_json()` | Cache hit/miss | LLM call completion | `logger.info("llm_call_completed", extra={"cache_hit": bool, "action": str})` |
| `resolver/mod.rs:842` | Retry attempts | Validation loop iteration | Already logged via `iteration_snapshots` |

### Aggregation Approach

**Real-time (in-memory):**
- Collect metrics in `ResolutionReport` struct (Rust)
- Propagate to `ValidationSummary`
- Display in UI via SSE updates (Phase 1 infrastructure)

**Post-run (log analysis):**
- Parse structured logs with `extra` fields
- Aggregate by action type, cache hit rate, rejection rate
- Store in SQLite `metrics` table for historical trends

**Dashboard display:**
- Cache hit rate: `(cache_hits / total_llm_calls) * 100%`
- Hallucination rate: `(pypi_rejections / total_llm_suggestions) * 100%`
- Pattern match rate: `(pattern_matches / total_recovery_calls) * 100%`

### Metrics to Track

| Metric | Source | Aggregation | Display |
|--------|--------|-------------|---------|
| Cache hit rate (by action) | `client.py` logs | Per action type (recovery, solvability, resolve) | Percentage + count |
| PyPI hallucination rate | `recovery.py:156` logs | Total rejections / total suggestions | Percentage + top rejected packages |
| RAG pattern match rate | `recovery.py:120` logs | Calls with patterns / total recovery calls | Percentage + top patterns |
| Average retry attempts | `resolver/mod.rs` iteration count | Mean across all cases | Number + histogram |
| Confidence distribution | `ResolutionReport` fields | Min/mean/max per tier | Tier1/tier2/tier3 breakdown |

## Common Pitfalls

### Pitfall 1: Hashing Dynamic Content Instead of Templates

**What goes wrong:** Including error logs or package lists in prompt hash causes cache miss on every call, defeating the purpose of caching.

**Why it happens:** Misunderstanding of what "prompt versioning" means — it's template structure versioning, not content versioning.

**How to avoid:** Extract template structure (static text + placeholder positions) and hash ONLY the template. Dynamic content (error logs, package lists) should be excluded from hash computation.

**Warning signs:**
- Cache hit rate near 0% even for identical import patterns
- Hash changes on every call in logs
- Template extraction function includes `{error_log}` **values** instead of `{PLACEHOLDER}` tokens

### Pitfall 2: Ignoring LiteLLM Cache Key Double-Computation Bug

**What goes wrong:** LiteLLM computes cache keys twice (read and write), and the second computation happens after completion. If kwargs are modified during the call, cache keys mismatch.

**Why it happens:** Known LiteLLM bug (#7316) where cache key is calculated before and after completion, and modifications to kwargs between calls cause cache miss.

**How to avoid:**
- Do NOT modify `kwargs` after passing to `litellm.completion()`
- Compute cache key in custom `get_cache_key()` using only immutable parameters
- If using model name suffix approach, ensure suffix is stable across call lifecycle

**Warning signs:**
- Cache writes succeed but reads fail (key mismatch)
- Debug logs show different cache keys for same logical request
- Cache hit rate drops after adding custom key logic

### Pitfall 3: Network Errors Cached as "Package Exists"

**What goes wrong:** PyPI validation network errors (timeouts, DNS failures) are cached as `True` (package exists), leading to hallucinated packages passing validation.

**Why it happens:** Decision D-04 assumes package exists on network error to avoid false negatives, but this means transient network issues can cache bad data.

**How to avoid:**
- Do NOT cache network errors — only cache successful 200/404 responses
- Add retry logic (3 attempts with exponential backoff) before assuming existence
- Log network errors separately from validation results

**Warning signs:**
- Hallucinated packages passing validation after network outage
- Cache contains `True` entries for packages like `"fake-package-12345"`
- PyPI validation logs show timeouts but no rejection

**Current implementation:** Lines 37-39 in `pypi_checker.py` assume existence on exception. Needs refinement:
```python
except Exception as e:
    # Log network error separately, do NOT cache
    logger.warning("PyPI check failed for %s: %s", package_name, e)
    # Return True to avoid false negatives, but consider retry logic
    exists = True
```

### Pitfall 4: Prompt Hash Collision (Low Probability but High Impact)

**What goes wrong:** Two different prompt templates produce the same 16-char hash prefix, causing cache key collision and serving stale responses for wrong action.

**Why it happens:** Truncating SHA256 to 16 hex chars (64 bits) reduces collision resistance from 2^128 to 2^64. Birthday paradox makes collisions likely after ~2^32 hashes.

**How to avoid:**
- Use at least 16 hex chars (64 bits) for prompt hash — current recommendation is safe for <1B prompt variations
- If collision suspected, increase to 32 chars (128 bits) or full 64 chars
- Include action type in cache key to separate recovery/solvability/resolve caches

**Warning signs:**
- Recovery action returns solvability response (wrong action type)
- Cache hit rate >95% (suspiciously high, may indicate collisions)
- Different prompts producing identical hash prefixes in logs

**Mitigation:**
```python
def _compute_prompt_version(self) -> str:
    # ... hash computation ...
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # Use 16 chars (64 bits) for normal operation
    # Increase to 32 chars (128 bits) if collision suspected
    return digest[:16]  # Can increase to [:32] if needed
```

## Code Examples

Verified patterns from research and existing codebase:

### Prompt Hash Computation

```python
# Source: Research on SHA256 hashing + existing client.py patterns
import hashlib
import json
import inspect

def _compute_prompt_version(self) -> str:
    """Compute SHA256 hash of prompt templates for cache versioning.

    Returns first 16 chars of hex digest (64-bit collision resistance).
    """
    from .. import prompts

    # Collect all prompt templates
    templates = {
        "recovery_system": prompts.RECOVERY_SYSTEM,
        "recovery_user_template": self._extract_user_template(prompts.recovery_user),
        "model": self.model,
    }

    # Stable serialization (sort_keys=True ensures deterministic output)
    canonical = json.dumps(templates, sort_keys=True)

    # Hash and truncate
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return digest[:16]

def _extract_user_template(self, prompt_fn) -> str:
    """Extract template structure from prompt function source.

    Normalizes dynamic placeholders to create stable hash.
    """
    source = inspect.getsource(prompt_fn)
    # Simple approach: use source as-is (includes template structure)
    # Advanced: parse f-strings and normalize placeholders
    return source
```

### Custom Cache Key Override

```python
# Source: LiteLLM docs + research on cache customization
from litellm.caching.caching import Cache
import litellm
from pathlib import Path

def _init_cache_with_versioning(self):
    """Initialize LiteLLM disk cache with prompt versioning."""
    cache_dir = str(Path.home() / ".apdr-cache" / "llm-cache")
    Path(cache_dir).mkdir(parents=True, exist_ok=True)

    cache = Cache(type="disk", disk_cache_dir=cache_dir)

    # Save original key builder
    original_get_key = cache.get_cache_key
    prompt_hash = self._prompt_version_hash

    # Wrap with version injection
    def versioned_get_cache_key(*args, **kwargs):
        base_key = original_get_key(*args, **kwargs)
        # Prepend prompt version to ensure invalidation on prompt change
        return f"v{prompt_hash}:{base_key}"

    cache.get_cache_key = versioned_get_cache_key
    litellm.cache = cache

    logger.info("LiteLLM cache enabled with prompt version %s", prompt_hash)
```

### PyPI Validation with Logging

```python
# Source: Existing pypi_checker.py + research on metrics patterns
import logging

logger = logging.getLogger("apdr_llm")

def handle(req: ResolutionRequest) -> ResolutionResponse:
    # ... recovery logic ...

    # Post-hoc PyPI validation (existing pattern)
    if result.fix_possible and result.correct_package:
        pkg_name = result.correct_package.split("==")[0].split(">=")[0].split("<=")[0]
        if not package_exists_on_pypi(pkg_name):
            # Log rejection for metrics
            logger.info(
                "PyPI validation rejected hallucinated package",
                extra={
                    "action": "recovery",
                    "package": pkg_name,
                    "suggested_by": "llm",
                    "import_name": result.wrong_package,
                }
            )
            notes.append(f"Recovery suggestion '{result.correct_package}' not found on PyPI")
            result.fix_possible = False
```

### Integration Test: Cache Invalidation

```python
# Source: Research on pytest best practices for LLM testing
import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest

def test_prompt_change_invalidates_cache():
    """Changing prompt templates should produce different cache keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir) / "cache"
        cache_dir.mkdir()

        # Mock LiteLLM cache initialization
        with patch("llm_py.client._init_cache") as mock_init:
            # First client with original prompts
            with patch("llm_py.prompts.RECOVERY_SYSTEM", "Original system prompt"):
                client1 = LlmClient("ollama", "test-model", "http://localhost:11434")
                hash1 = client1._prompt_version_hash

            # Second client with modified prompts
            with patch("llm_py.prompts.RECOVERY_SYSTEM", "Modified system prompt - new instruction"):
                client2 = LlmClient("ollama", "test-model", "http://localhost:11434")
                hash2 = client2._prompt_version_hash

            # Hashes should differ
            assert hash1 != hash2, "Prompt change should produce different version hash"

            # Same model + different hash = different cache namespace
            assert client1.model == client2.model
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 8.3+ |
| Config file | None — using pytest defaults (recommend adding `pytest.ini`) |
| Quick run command | `cd tools/apdr && python -m pytest llm_py/tests/ -x` |
| Full suite command | `cd tools/apdr && python -m pytest llm_py/tests/ -v --tb=short` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| REC-01 | PyPI validation rejects hallucinated packages | integration | `pytest llm_py/tests/test_pypi_validation.py::test_reject_nonexistent -x` | ❌ Wave 0 |
| REC-02 | RAG pattern library matches error patterns | unit | `pytest llm_py/tests/test_pattern_matching.py::test_pg_config_pattern -x` | ❌ Wave 0 |
| REC-03 | Prompt hash change invalidates cache | integration | `pytest llm_py/tests/test_cache_invalidation.py::test_prompt_change -x` | ❌ Wave 0 |
| REC-04 | Confidence threshold enforcement | unit | `pytest llm_py/tests/test_confidence_thresholds.py::test_solvability_skip -x` | ❌ Wave 0 |
| REC-05 | Max retry limit enforcement | unit | `pytest llm_py/tests/test_recovery_mock.py::test_max_retries -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest llm_py/tests/ -x` (fail fast on first error)
- **Per wave merge:** `pytest llm_py/tests/ -v` (full suite with verbose output)
- **Phase gate:** Full suite green + manual smoke test with real Ollama instance

### Wave 0 Gaps
- [ ] `tests/test_pypi_validation.py` — covers REC-01 (PyPI validation rejection flow)
- [ ] `tests/test_pattern_matching.py` — covers REC-02 (RAG pattern library matching)
- [ ] `tests/test_cache_invalidation.py` — covers REC-03 (prompt hash cache invalidation)
- [ ] `tests/test_confidence_thresholds.py` — covers REC-04 (confidence enforcement)
- [ ] `tests/conftest.py` — shared fixtures (temp cache dir, mock LLM responses, mock PyPI)
- [ ] `pytest.ini` — configuration (test discovery, markers, logging)
- [ ] Add `pytest-mock` to dev dependencies: `pip install pytest-mock`

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No prompt versioning | Content-based SHA256 hashing | 2024-2025 (LLM caching maturity) | Automatic cache invalidation on prompt updates |
| Manual cache clearing | Automatic invalidation via hash | 2024 (prompt management tools) | No stale suggestions from old prompts |
| Global mock patching | pytest-mock fixtures with auto-cleanup | 2023+ (pytest 7.0+) | Cleaner tests, no global state pollution |
| Manual JSON parsing | Instructor library with Pydantic validation | 2023-2024 (structured outputs era) | Automatic retries, field-level validation |
| Custom LLM abstraction | LiteLLM multi-provider client | 2023+ (LiteLLM maturity) | Unified API across Ollama/OpenAI/Anthropic |

**Deprecated/outdated:**
- **Manual version tracking:** Using Git SHA or timestamps for prompt versioning → replaced by content hashing (deterministic, no Git dependency)
- **In-loop PyPI validation:** Pydantic field validators during LLM generation → replaced by post-hoc validation (simpler, no retry loop complexity)
- **unittest.mock global patching:** `patch.object()` on module globals → replaced by `@patch` decorators with automatic cleanup

## Open Questions

1. **Does LiteLLM expose cache hit/miss metadata in responses?**
   - What we know: LiteLLM has internal cache hit tracking
   - What's unclear: Whether `litellm.completion()` response includes cache metadata
   - Recommendation: Check response object for `_hidden_params` or similar fields; if not exposed, log cache keys and compare against disk cache file timestamps

2. **What's the actual cache hit rate for recovery actions?**
   - What we know: Cache is enabled, disk-based, includes model + messages in key
   - What's unclear: Current hit rate without prompt versioning (baseline)
   - Recommendation: Add cache hit metrics before implementing hash to measure improvement delta

3. **Should confidence scores be per-package or per-case?**
   - What we know: `RecoveryResult` model exists but confidence field not found in schema
   - What's unclear: Whether confidence applies to overall fix or individual package suggestions
   - Recommendation: Add `confidence: float` field to `RecoveryResult` model, default to 0.65 for LLM recovery (per D-19)

## Sources

### Primary (HIGH confidence)
- **Existing codebase** (`client.py`, `recovery.py`, `pypi_checker.py`, `build_error_patterns.py`) — verified implementation patterns
- **pytest existing tests** (`test_recovery_mock.py`) — established test patterns with 13 test cases
- **Rust resolver** (`src/resolver/mod.rs:842`, `src/lib.rs:217`) — max_retries implementation verified

### Secondary (MEDIUM confidence)
- **LiteLLM documentation** (via WebSearch 2024-2025) — custom cache key builder API confirmed
- **Python hashlib docs** (stdlib) — SHA256 implementation verified
- **Instructor library** (used in `client.py:178`) — structured outputs with retry logic

### Tertiary (LOW confidence)
- **WebSearch findings on LLM caching strategies** — general patterns, not APDR-specific
- **Prompt versioning blog posts** (2024) — conceptual guidance, not implementation details
- **pytest-mock usage examples** — common patterns, not project-specific

## Metadata

**Confidence breakdown:**
- **Standard stack:** HIGH — all libraries already in use, versions verified in existing files
- **Cache key injection:** MEDIUM — LiteLLM API confirmed via WebSearch, but custom `get_cache_key()` pattern not tested in this codebase
- **Prompt hashing:** HIGH — SHA256 hashing is straightforward, template extraction strategy is research-based
- **Test infrastructure:** HIGH — pytest already used, mock patterns established in `test_recovery_mock.py`
- **Metrics collection:** MEDIUM — logging locations identified, aggregation strategy is recommended but not validated
- **Integration risks:** MEDIUM — LiteLLM cache key double-computation bug is known issue, workaround is speculative

**Research date:** 2026-03-25
**Valid until:** 30 days (2026-04-24) — stable libraries, low churn expected
