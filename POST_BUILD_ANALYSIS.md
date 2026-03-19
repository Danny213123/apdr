# APDR Post-Build Analysis - Run 20260317-191132

**Date**: March 18, 2026
**Total Cases**: 2,891
**Success Rate**: 60.4%
**Total Runtime**: ~440 hours (distributed across cases)

## Executive Summary

The latest benchmark run demonstrates that recent optimizations (import-set caching, high-confidence skipping, early host-runtime detection) are working excellently, with **69.4% of cases completing with 0s validation time**. However, 394 cases (13.6%) waste significant time on non-existent PyPI packages suggested by the LLM, taking an average of 1300s each to fail across multiple Python versions.

## Success Metrics

| Metric | Count | Percentage |
|--------|-------|------------|
| ✓ Succeeded | 1,747 | 60.4% |
| ✓ Import-set cache hits | 1,092 | 37.8% |
| ✓ High-confidence skips | 491 | 17.0% |
| ⊘ Skipped (host-runtime) | 375 | 13.0% |
| ✗ Failed | 747 | 25.8% |

## Major Failure Categories

### 1. VERSION-NOT-FOUND (394 cases, 13.6%)
**Problem**: LLM suggests non-existent package names (e.g., `create_sentiment_featuresets` which is actually a local module, not a PyPI package).

**Impact**:
- Wastes ~1300s per case trying 4 Python versions
- Total wasted time: ~141 hours

**Example Case**: `2e90566e9497725065388859762a7185`
```
LLM resolved: create_sentiment_featuresets -> create_sentiment_featuresets
Pre-solve: Package has no KGraph metadata (doesn't exist on PyPI)
Validation: Tried Python 3.9, 3.10, 3.11, 3.12 (all failed)
Error: "Could not find a version that satisfies the requirement"
Time: 1377s solve + 403s validation = 1780s total
```

### 2. ENVIRONMENT-BUILD-FAILED (148 cases, 5.1%)
**Problem**: `Unknown` or `NonZeroCode` error types with no automatic recovery pattern.

**Impact**: Build failures from system dependencies, compilation errors, etc.

### 3. MODULE-NOT-FOUND (111 cases, 3.8%)
**Problem**: Dependencies install successfully but runtime imports still fail.

**Impact**: Missing transitive dependencies or namespace package issues.

## Performance Analysis

### Solve Time
- Average: 151.4s
- **Max: 1377.8s (23 minutes!)**
- 95th percentile: 958.2s

### Validation Time
- Average (non-zero): 272.3s
- **Max: 1588.8s (26 minutes!)**
- **Zero-time validations: 2006 cases (69.4%)** ← Excellent!

### Slow Cases (>5 minutes total)
770 cases took over 5 minutes:
- 368 version-not-found
- 142 environment-build-failed
- 108 module-not-found
- 87 passed (but took long time)

## Recommended Fixes (Prioritized by Impact)

### Fix #1: PyPI Package Existence Check (HIGHEST IMPACT)
**Estimated savings: 141 hours per run (32% reduction)**

**Implementation**:
```rust
// Before validation, check if package exists on PyPI/KGraph
fn package_exists(package_name: &str, store: &CacheStore) -> bool {
    // Check KGraph first (fast, local)
    if kgraph_db::get_versions(package_name).is_some() {
        return true;
    }
    // Check PyPI API as fallback
    match query_pypi_simple(package_name) {
        Ok(versions) if !versions.is_empty() => true,
        _ => false
    }
}

// In validate_with_retries, before first attempt:
for dep in &resolved {
    if !package_exists(&dep.package_name, store) {
        return ValidationSummary {
            succeeded: false,
            status: "package-does-not-exist".to_string(),
            reason: Some(format!("Package `{}` does not exist on PyPI", dep.package_name)),
            ...
        };
    }
}
```

**Benefits**:
- Fail in ~10s instead of ~1300s
- Avoid wasting time on 4 Python version attempts
- Can cache negative results to avoid retrying

### Fix #2: Improve LLM Prompts
**Estimated savings: Reduce false positives by 50% → 20 hours**

**Implementation**:
- Add to LLM prompt: "Only suggest actual PyPI packages, not local project modules"
- Show RAG examples of real PyPI package names
- Ask LLM to verify package existence before suggesting

### Fix #3: Fail-Fast on Non-Existent Packages
**Estimated savings: 33 hours**

**Implementation**:
```rust
// After first VersionNotFound, check if it's a non-existent package
if error_type == ErrorType::VersionNotFound {
    if !package_exists(&failed_package, store) {
        // Mark as permanently failed, skip other Python versions
        store.save_unsolvable_module(
            &failed_package,
            "non-existent-package",
            "Package does not exist on PyPI",
            0.95
        );
        return ValidationSummary {
            succeeded: false,
            status: "package-does-not-exist".to_string(),
            ...
        };
    }
}
```

### Fix #4: Better Unknown Error Classification
**Estimated savings: 15 hours (from faster failure classification)**

**Implementation**:
- Extend `extract_build_dependency()` patterns
- Add detection for:
  - Compilation errors (missing gcc, missing headers)
  - System library errors (libssl, libpq, etc.)
  - Network/timeout errors
- Consider LLM-assisted log parsing for truly unknown cases

### Fix #5: Transitive Dependency Verification
**Estimated savings: 10 hours**

**Implementation**:
```rust
// After pip install succeeds, verify imports work
fn verify_imports_installed(env_python: &Path, imports: &[String]) -> Result<Vec<String>> {
    let mut missing = Vec::new();
    for import_name in imports {
        let result = Command::new(env_python)
            .args(&["-c", &format!("import {}", import_name)])
            .output()?;
        if !result.status.success() {
            missing.push(import_name.clone());
        }
    }
    Ok(missing)
}
```

### Fix #6: Pre-Solve Timeout
**Estimated savings: 5 hours**

**Implementation**:
- Add timeout to pre-solve: if >60s, mark as likely unsolvable
- Skip to next Python version or fail early

## Estimated Total Impact

| Fix | Savings | Implementation Effort |
|-----|---------|----------------------|
| #1: PyPI existence check | 141 hours | 2-3 hours (Medium) |
| #2: LLM prompt improvement | 20 hours | 1 hour (Easy) |
| #3: Fail-fast non-existent | 33 hours | 1 hour (Easy) |
| #4: Better error classification | 15 hours | 4-5 hours (Hard) |
| #5: Import verification | 10 hours | 2 hours (Medium) |
| #6: Pre-solve timeout | 5 hours | 1 hour (Easy) |
| **TOTAL** | **224 hours** | **11-13 hours** |

**Current benchmark time**: 440 hours
**Optimized benchmark time**: 216 hours
**Improvement**: 51% reduction

## Implementation Priority

1. **Immediate (Easy wins)**:
   - Fix #2: LLM prompt improvement (1 hour)
   - Fix #3: Fail-fast on non-existent packages (1 hour)
   - Fix #6: Pre-solve timeout (1 hour)

2. **High Priority (High ROI)**:
   - Fix #1: PyPI existence check (2-3 hours, saves 141 hours)

3. **Medium Priority**:
   - Fix #5: Import verification (2 hours, saves 10 hours)

4. **Lower Priority (Complex)**:
   - Fix #4: Better error classification (4-5 hours, saves 15 hours)

## Conclusion

The recent optimizations (import-set cache, high-confidence skip, early host-runtime detection) are working excellently, achieving 69.4% zero-validation-time cases. The main remaining bottleneck is LLM hallucination of non-existent packages, which can be addressed with relatively simple existence checks before validation. Implementing the top 3 fixes would save ~194 hours (44% improvement) with only 4-5 hours of development work.
