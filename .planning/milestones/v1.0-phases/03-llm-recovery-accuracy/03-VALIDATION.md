# Phase 3: LLM Recovery Accuracy - Validation

**Phase:** 03-llm-recovery-accuracy
**Created:** 2026-03-26
**Framework:** pytest 8.3+

## Requirement Validation Map

| Req ID | Success Criterion | Validation Method | Automated Test | Plan |
|--------|------------------|-------------------|----------------|------|
| REC-01 | User sees only PyPI-validated package suggestions (no invalid packages) | Integration test: Mock PyPI with invalid package, verify rejection | `pytest llm_py/tests/test_pypi_validation.py::test_reject_nonexistent -x` | 03-01 |
| REC-02 | User benefits from RAG-enhanced recovery using error pattern library | Unit test: Match known error patterns, verify context injection | `pytest llm_py/tests/test_pattern_matching.py::test_pg_config_pattern -x` | 03-01 |
| REC-03 | User's cached suggestions invalidate when prompts or models change | Integration test: Change prompt, verify cache miss | `pytest llm_py/tests/test_cache_invalidation.py::test_prompt_change -x` | 03-02 |
| REC-04 | User sees recovery attempts skip when confidence score <0.4 | Unit test: Document Rust contract with test case | `pytest llm_py/tests/test_confidence_thresholds.py::test_solvability_skip -x` | 03-01 |
| REC-05 | User observes max 5 recovery attempts per case | Unit test: Document Rust contract with test case | `pytest llm_py/tests/test_recovery_mock.py::test_max_retries -x` | 03-01 |

## Test Sampling Strategy

### Per-Commit Validation
```bash
cd tools/apdr && python -m pytest llm_py/tests/ -x
```
**When:** After each task commit
**Purpose:** Fail fast on first error during development

### Per-Wave Validation
```bash
cd tools/apdr && python -m pytest llm_py/tests/ -v --tb=short
```
**When:** After completing Wave 1, Wave 2, Wave 3
**Purpose:** Full suite with verbose output to catch regressions

### Phase Gate Validation
```bash
cd tools/apdr && python -m pytest llm_py/tests/ -v
```
**Plus:** Manual verification checkpoint (Plan 03-03 Task 4)

**When:** After Wave 3 complete
**Purpose:** Full automated suite + human verification with real Ollama instance

## Test Files Created

| File | Lines | Tests | Coverage |
|------|-------|-------|----------|
| `tools/apdr/llm_py/tests/conftest.py` | ~50 | N/A | Shared fixtures |
| `tools/apdr/llm_py/tests/test_pypi_validation.py` | ~120 | 5 | REC-01 |
| `tools/apdr/llm_py/tests/test_pattern_matching.py` | ~150 | 6 | REC-02 |
| `tools/apdr/llm_py/tests/test_confidence_thresholds.py` | ~80 | 3 | REC-04 |
| `tools/apdr/llm_py/tests/test_recovery_mock.py` | ~60 | 1 (extends existing) | REC-05 |
| `tools/apdr/llm_py/tests/test_cache_invalidation.py` | ~140 | 4 | REC-03 |
| `tools/apdr/pytest.ini` | ~15 | N/A | Test configuration |

**Total:** ~615 lines, 19+ test cases

## Human Verification Checkpoints

### Checkpoint 1: Plan 03-03 Task 4
**Location:** After automated tests pass
**Verifier:** Developer
**Scope:** End-to-end validation with real Ollama instance

**What to verify:**
1. Run benchmark case with hallucinated package → verify PyPI rejection in logs
2. Run benchmark case with known error pattern → verify RAG context injection in logs
3. Change recovery prompt → verify cache miss on next LLM call
4. Observe confidence-based skip in logs (already implemented in Rust)
5. Observe max retry limit enforcement (already implemented in Rust)

**Pass criteria:**
- All 5 behaviors observable in logs
- No false positives (valid packages rejected)
- No stale cache hits after prompt changes

## Coverage Gaps

**Rust-side enforcement (REC-04, REC-05):**
- Confidence threshold (0.4) enforced in `tools/apdr/src/resolver/mod.rs:747`
- Max retry limit (5) enforced in `tools/apdr/src/resolver/mod.rs:842`
- Python tests document the contract but don't re-implement enforcement
- Integration testing via manual checkpoint (Plan 03-03 Task 4)

**Metrics aggregation:**
- Structured logging added (Plan 03-03)
- Dashboard integration deferred to future phase
- Logs can be manually inspected or aggregated post-phase

---

*Validation map created: 2026-03-26*
*Test framework: pytest 8.3+*
*Total automated tests: 19+*
