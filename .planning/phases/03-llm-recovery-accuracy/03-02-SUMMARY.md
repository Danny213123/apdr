---
phase: 03-llm-recovery-accuracy
plan: 02
subsystem: llm-integration
tags: [litellm, cache-invalidation, prompt-hashing, sha256, instructor]

# Dependency graph
requires:
  - phase: 03-llm-recovery-accuracy
    provides: Test infrastructure (pytest fixtures, conftest.py, shared test utilities)
provides:
  - Prompt hash-based cache invalidation for LiteLLM (SHA256 of prompts + model ID)
  - Custom cache key override mechanism injecting prompt version prefix
  - Integration tests verifying cache invalidation behavior
  - Foundation for automatic cache invalidation on prompt engineering changes
affects: [prompt-engineering, llm-optimization, cache-tuning]

# Tech tracking
tech-stack:
  added: []  # No new dependencies - uses existing hashlib, inspect, json
  patterns:
    - "SHA256 hashing of prompt templates for cache versioning"
    - "Custom cache key override by wrapping litellm.cache.get_cache_key()"
    - "Template extraction via inspect.getsource() for hash stability"

key-files:
  created:
    - tools/apdr/llm_py/tests/test_cache_invalidation.py
  modified:
    - tools/apdr/llm_py/client.py

key-decisions:
  - "Hash template structure (function source code) not dynamic content - preserves cache hits across different error logs"
  - "Use first 16 chars of SHA256 (64-bit collision resistance) for compact cache keys"
  - "Include model ID in hash per D-11 - model changes invalidate cache"
  - "Wrap global litellm.cache.get_cache_key() method - safe in single-threaded LLM subprocess"
  - "Extract user template via inspect.getsource() for deterministic hashing"

patterns-established:
  - "Prompt versioning pattern: v{hash}:{base_key} format for LiteLLM cache keys"
  - "Template hashing pattern: include all system prompts, user templates, and model ID"
  - "Cache override pattern: store original method, wrap with version injection, replace globally"

requirements-completed: [REC-03]

# Metrics
duration: 3.6min
completed: 2026-03-26
---

# Phase 3 Plan 2: Prompt Hash Cache Invalidation Summary

**SHA256-based cache invalidation prevents stale LLM suggestions when prompts or models change**

## Performance

- **Duration:** 3.6 min (216 seconds)
- **Started:** 2026-03-26T04:05:59Z
- **Completed:** 2026-03-26T04:09:35Z
- **Tasks:** 3
- **Files modified:** 2 (1 created, 1 modified)

## Accomplishments

- Implemented `_compute_prompt_version()` method computing SHA256 hash of RECOVERY_SYSTEM, recovery_user template, SOLVABILITY_SYSTEM, RESOLUTION_SYSTEM, and model ID
- Added `_init_cache_with_versioning()` to inject prompt version prefix into LiteLLM cache keys
- Created 4 integration tests verifying cache invalidation on prompt/model changes
- All 35 tests pass (31 existing + 4 new), no regressions

## Task Commits

Each task was committed atomically:

1. **Tasks 1-2: Implement prompt hashing and cache key override** - `52fbc24` (feat)
   - Added `_compute_prompt_version()` and `_extract_user_template()` methods
   - Added `_init_cache_with_versioning()` to wrap litellm.cache.get_cache_key()
   - Cache keys now prefixed with `v{hash}:` format

2. **Task 3: Create integration tests** - `ea357b8` (test)
   - test_prompt_change_invalidates_cache: Different prompts → different hashes ✓
   - test_model_change_invalidates_cache: Different models → different hashes ✓
   - test_dynamic_content_does_not_affect_hash: Same templates → same hash ✓
   - test_cache_key_includes_version_prefix: Keys have v{hash}: prefix ✓

## Files Created/Modified

- `tools/apdr/llm_py/client.py` - Added prompt version hashing and cache key override to LlmClient
- `tools/apdr/llm_py/tests/test_cache_invalidation.py` - 4 integration tests for REC-03

## Decisions Made

1. **Hash template structure not content** - Used `inspect.getsource()` to hash the prompt function source code, not runtime values. This ensures dynamic content like error logs doesn't affect the hash, preserving cache hit rates while still invalidating on template changes.

2. **16-char hash for compactness** - Truncated SHA256 to first 16 characters (64-bit collision resistance). Full 256-bit hash unnecessary for cache versioning use case.

3. **Global cache override safe in subprocess** - LlmClient modifies global `litellm.cache.get_cache_key()` method. Safe because APDR's LLM service runs as single-threaded subprocess with one client instance.

4. **Include model ID in hash** - Per D-11 decision from research, hash includes model name. Model swaps (qwen2.5-coder:7b → :14b) now invalidate cache automatically.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None - all tasks completed without issues. Tests passed on first run.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- REC-03 requirement satisfied: cache invalidates on prompt/model change
- 4/5 Phase 3 requirements complete (REC-01, REC-02, REC-03, REC-04, REC-05 done; only REC-04 remaining but already covered in Plan 01)
- Ready to proceed with prompt engineering improvements - cache invalidation prevents stale suggestions
- Integration tests provide foundation for verifying cache behavior in future LLM changes

## Self-Check: PASSED

All files created/modified and commits verified:
- ✓ tools/apdr/llm_py/client.py exists
- ✓ tools/apdr/llm_py/tests/test_cache_invalidation.py exists
- ✓ .planning/phases/03-llm-recovery-accuracy/03-02-SUMMARY.md exists
- ✓ Commit 52fbc24 (feat: prompt hash implementation) found
- ✓ Commit ea357b8 (test: cache invalidation tests) found
- ✓ Commit d53c036 (docs: plan metadata) found

---
*Phase: 03-llm-recovery-accuracy*
*Completed: 2026-03-26*
