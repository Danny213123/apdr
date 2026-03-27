# Phase 3: LLM Recovery Accuracy - Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 3 improves LLM recovery accuracy by ensuring suggestions are validated, contextually enhanced, properly cached, and skipped when confidence is low. This phase builds on the existing recovery infrastructure to make it more reliable and prevent bad suggestions from reaching Docker validation.

**In Scope:**
- Enhance PyPI validation to reject hallucinated packages during LLM generation (REC-01)
- Verify RAG-enhanced recovery prompts are working correctly (REC-02)
- Implement cache invalidation based on prompt hash + model ID (REC-03)
- Verify confidence-based skip logic is working (REC-04)
- Verify recovery attempt limit enforcement (REC-05)

**Out of Scope:**
- Improving LLM inference performance (batching, parallelization) — Phase 4
- Changing the error pattern library content — this phase validates the mechanism works, not the patterns themselves
- Rewriting the recovery prompts — focus on validation and caching correctness
- Multi-model fallback orchestration — existing LiteLLM router is sufficient

</domain>

<decisions>
## Implementation Decisions

### REC-01: PyPI Package Validation

**Current State:** ✅ Already implemented
- `pypi_checker.py` provides thread-safe cached validation (lines 19-44)
- Recovery action uses post-hoc validation after LLM response (recovery.py:153-165)
- Invalid packages trigger `fix_possible=False` and add notes

**Decisions:**
- **D-01**: Keep post-hoc validation approach (after LLM completion) — simpler than in-loop validation
- **D-02**: Validate ALL suggested packages: `correct_package`, `add_package` (currently implemented)
- **D-03**: Cache PyPI validation results in-memory for session lifetime (currently implemented)
- **D-04**: On network errors, assume package exists to avoid false negatives (currently: line 39)
- **D-05**: Add test coverage for hallucinated package rejection scenarios

**Work Required:**
- Add integration tests for PyPI validation rejection flow
- Add metrics logging for hallucination rate (how often invalid packages are rejected)

### REC-02: RAG-Enhanced Recovery Prompts

**Current State:** ✅ Already implemented
- `build_error_patterns.py` contains 20+ structured error patterns (lines 21-153)
- `format_error_context()` matches patterns and injects top 3 into recovery prompt (lines 174-183)
- Recovery action prepends pattern context if matches found (recovery.py:116-134)

**Decisions:**
- **D-06**: Keep top-3 pattern injection to avoid prompt bloating (currently implemented)
- **D-07**: Pattern matching uses regex with fallback to substring match for malformed patterns (currently implemented)
- **D-08**: Patterns ordered most-specific to most-general (currently implemented)
- **D-09**: Add "Build error pattern library matched" note when patterns trigger (currently: line 121)

**Work Required:**
- Add test coverage verifying pattern matching triggers correctly
- Add metrics for pattern match rate (how often RAG context is injected)
- Verify pattern library covers common failure modes from hard-gists benchmark

### REC-03: Cache Invalidation (Prompt Hash + Model ID)

**Current State:** ⚠️ PARTIALLY IMPLEMENTED — needs work
- LiteLLM disk caching is enabled (client.py:43-59)
- Cache location: `~/.apdr-cache/llm-cache/`
- Cache keys include model name BUT NOT prompt content or prompt version
- Changing prompts or models may return stale cached responses

**Decisions:**
- **D-10**: Implement prompt version hash injection into LiteLLM cache keys
- **D-11**: Hash strategy: SHA256 of (system_prompt + user_prompt_template + model_id)
- **D-12**: User prompt template = user prompt with placeholders for dynamic content (error_log, packages, etc.)
- **D-13**: Include model ID in cache key (already handled by LiteLLM, verify it works)
- **D-14**: Cache invalidation is automatic — changing hash produces new cache key, old entries expire naturally
- **D-15**: Add cache hit/miss metrics to track effectiveness
- **D-16**: Do NOT invalidate on dynamic content changes (error logs, package lists) — only on prompt structure or model changes

**Work Required:**
- Add prompt version hashing to LlmClient
- Inject prompt hash into LiteLLM cache metadata or model name
- Add integration test verifying prompt change invalidates cache
- Add metrics for cache hit rate by action type (resolve, recovery, solvability)

### REC-04: Confidence-Based Skip Logic

**Current State:** ✅ Already implemented
- Confidence threshold of 0.4 for solvability assessment (resolver/mod.rs:747)
- Unsolvable cache only matches at confidence ≥0.95 (resolver/mod.rs:3830, 3839)
- Recovery actions set confidence scores (0.50-0.78 range based on strategy)

**Decisions:**
- **D-17**: Keep 0.4 threshold for solvability skipping (validated in prior work)
- **D-18**: Keep 0.95 threshold for unsolvable cache persistence (prevents false positives)
- **D-19**: Recovery confidence scores follow stratified approach:
  - LLM-based recovery: 0.65 (recovery.py:1037)
  - Heuristic version adjustments: 0.60-0.78 (resolver/mod.rs)
  - Last-resort stripping: 0.50-0.55 (resolver/mod.rs)
- **D-20**: Add confidence score to LLM recovery response model if not present
- **D-21**: Surface confidence scores in UI (already done in Phase 2 for tier3 cases)

**Work Required:**
- Verify RecoveryResult model includes confidence field
- Add tests for confidence threshold enforcement
- Add metrics for skip rate due to low confidence

### REC-05: Recovery Attempt Limit

**Current State:** ✅ Already implemented
- max_retries config defaults to 5 (lib.rs:217)
- Retry loop enforced in resolver/mod.rs:842 (for attempt_index in 0..=config.max_retries)
- Configurable via --max-retries CLI flag (main.rs:75)

**Decisions:**
- **D-22**: Keep default of 5 attempts (validated balance between success rate and runtime)
- **D-23**: Each attempt can include multiple recovery strategies in sequence:
  - LLM recovery hint application
  - Seed fallback
  - Version stripping
  - Last-resort constraint relaxation
- **D-24**: Attempt limit applies to full validation loop (not just LLM calls)
- **D-25**: Add attempt count to result metadata for observability
- **D-26**: Add metrics for average attempts per case and success rate by attempt number

**Work Required:**
- Add attempt count tracking to result metadata
- Add test coverage for max retry enforcement
- Add metrics for attempt distribution

### Claude's Discretion

- **Test coverage strategy**: Choose between unit tests vs integration tests based on component isolation
- **Metrics implementation**: Decide on logging format, aggregation level, and storage location
- **Error handling**: Design fallback behavior when PyPI validation fails or pattern matching errors occur
- **Performance optimization**: Decide if prompt hash should be pre-computed or computed on-demand

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Prior Phase Artifacts
- `.planning/phases/01-non-blocking-ui-foundation/01-01-SUMMARY.md` — SSE backend for real-time updates
- `.planning/phases/02-result-categorization-insights/02-CONTEXT.md` — UI confidence display patterns
- `.planning/REQUIREMENTS.md` — Phase 3 requirements REC-01 through REC-05

### Codebase Maps
- `.planning/codebase/STRUCTURE.md` — Rust/Python architecture boundaries
- `.planning/codebase/STACK.md` — Tech stack constraints

### Relevant Source Files
- `tools/apdr/llm_py/actions/recovery.py` — Recovery action handler (main work target)
- `tools/apdr/llm_py/pypi_checker.py` — PyPI validation cache
- `tools/apdr/llm_py/build_error_patterns.py` — RAG pattern library
- `tools/apdr/llm_py/client.py` — LLM client with caching
- `tools/apdr/llm_py/prompts.py` — Recovery prompt templates
- `tools/apdr/src/resolver/mod.rs` — Retry loop and confidence thresholds
- `tools/apdr/src/lib.rs` — Config struct with max_retries

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

**PyPI Validation Infrastructure:**
- `package_exists_on_pypi()` — Thread-safe cached validation with 5s timeout
- `preload_known_packages()` — Batch cache warming from Rust store
- `check_multiple()` — Batch validation for multiple packages
- Network error tolerance: assumes package exists on request failure (line 39)

**RAG Pattern Library:**
- 20+ structured ErrorPattern objects with regex, diagnosis, fix_type, suggested_fix
- Ordered from most-specific to most-general for priority matching
- `match_error_patterns()` — Regex-based pattern matching with substring fallback
- `format_error_context()` — Top-3 pattern injection for recovery prompts

**LLM Client Caching:**
- LiteLLM disk cache enabled at `~/.apdr-cache/llm-cache/`
- Cache initialized on first LlmClient instantiation (client.py:47-59)
- Ollama keep-alive set to -1 (keep model in GPU memory indefinitely)
- Pre-warming support via prewarm_ollama() (client.py:65-87)

**Confidence Scoring:**
- Solvability skip threshold: 0.4 (resolver/mod.rs:747)
- Unsolvable cache persistence threshold: 0.95 (resolver/mod.rs:3830)
- Recovery strategy confidence ranges:
  - Build alternative packages: 0.75-0.78
  - LLM fixes: 0.65-0.70
  - Version adjustments: 0.60-0.74
  - Last-resort stripping: 0.50-0.55

### Established Patterns

**Recovery Flow:**
1. LLM generates RecoveryResult with fix suggestions
2. Post-hoc PyPI validation checks correct_package and add_package
3. Namespace mapping validation (import name must match package namespace)
4. Notes added for rejection reasons ("not found on PyPI", "namespace mismatch")
5. fix_possible set to False if validation fails

**Error Pattern Matching:**
1. Iterate ERROR_PATTERNS in priority order
2. Regex match against error_log (with fallback to substring)
3. Collect all matches
4. Format top-3 matches as RAG context
5. Prepend to user_prompt before LLM call

**Retry Loop:**
1. for attempt_index in 0..=max_retries (default: 0-5, total 6 attempts)
2. Validate requirements with Docker
3. On failure, classify error type
4. Apply recovery strategies in sequence
5. Repeat until success or max_retries exceeded

### Integration Points

**Python → Rust Result Flow:**
- RecoveryResult from Python action → ResolutionResponse
- ResolutionResponse fields: fix_possible, wrong_package, correct_package, version, add_package, remove_package, notes
- Rust applies recovery hints in resolver/mod.rs:1034-1040

**Cache Key Construction:**
- LiteLLM auto-generates cache keys from: (model, messages, params)
- Does NOT include prompt content hash — this is the gap to fix
- Need to inject prompt version hash into model name or messages metadata

**Confidence Propagation:**
- LLM actions set confidence scores on Dependency objects
- Resolver aggregates min_confidence and mean_confidence (resolver/mod.rs:350-355)
- UI displays confidence badges for tier3 cases (Phase 2)

### Known Gaps (Phase 3 Work)

1. **Cache invalidation**: No prompt hash in cache keys
2. **Metrics**: No tracking for PyPI rejection rate, pattern match rate, cache hit rate by action
3. **Test coverage**: Missing integration tests for:
   - Hallucinated package rejection
   - Pattern library matching
   - Prompt change cache invalidation
   - Confidence threshold enforcement
   - Max retry enforcement

</code_context>

<specifics>
## Specific Ideas

**Prompt Hash Strategy:**
Since LiteLLM cache keys are auto-generated from (model, messages, params), we can inject a prompt version hash into the model name as a suffix:
- Original model: "qwen2.5-coder:7b"
- With hash: "qwen2.5-coder:7b#abc123ef" (truncated SHA256 of system+user template)
- This forces cache miss when prompts change without breaking model calls

**Test Strategy:**
- Unit tests for individual validators (PyPI checker, pattern matcher, hash generator)
- Integration tests for end-to-end flows (recovery with PyPI rejection, pattern injection, cache invalidation)
- Mock-based tests to avoid network calls in CI (except dedicated PyPI validation tests)

**Metrics Collection:**
Add structured logging in recovery.py and client.py:
```python
logger.info("recovery.pypi_validation", extra={"rejected": bool, "package": str, "reason": str})
logger.info("recovery.pattern_match", extra={"matched_count": int, "patterns": [str]})
logger.info("llm.cache", extra={"hit": bool, "action": str, "model": str})
```

Aggregate metrics in runner or service layer for dashboard display.

</specifics>

<deferred>
## Deferred Ideas

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

</deferred>

---

*Phase: 03-llm-recovery-accuracy*
*Context gathered: 2026-03-26 (auto mode)*
