# Testing Patterns

**Analysis Date:** 2026-03-25

## Test Framework

**Rust Testing:**
- Built-in test framework (no external runner)
- Config: Default Cargo test settings
- Assertion library: Standard Rust `assert!`, `assert_eq!`, `assert_ne!`

**Python Testing:**
- pytest
- unittest.mock for mocking
- Pydantic for schema validation in tests

**Run Commands:**
```bash
# Rust tests
cargo test                           # Run all tests
cargo test --test test_resolver      # Run specific test file
cargo test resolver_maps             # Run tests matching pattern
cargo test -- --nocapture            # Show print output

# Python tests
pytest tools/apdr/llm_py/tests/      # Run all Python tests
pytest tools/apdr/llm_py/tests/test_recovery_mock.py -v  # Verbose single file
python -m pytest                     # Alternative invocation
```

## Test File Organization

**Rust:**
- Integration tests: `tools/apdr/tests/*.rs`
- Test fixtures: `tools/apdr/tests/fixtures/*.py` (Python snippets for testing)
- Pattern: Separate `tests/` directory at package root

**Test Files:**
- `tests/test_resolver.rs` - 1,146 lines, 67+ test cases
- `tests/test_parser.rs` - 51 lines, 5 test cases
- `tests/test_cache.rs` - Cache functionality tests
- `tests/test_classifier.rs` - Error classification tests
- `tests/test_cli.rs` - CLI interface tests
- `tests/test_llm_integration.rs` - LLM pipeline tests

**Python:**
- Tests co-located: `llm_py/tests/test_recovery_mock.py`
- Unit tests for actions: Recovery, resolution, version pinning

**Test Fixtures:**
- Location: `tests/fixtures/*.py`
- Real-world Python snippets with known dependency issues
- Examples: `legacy_pymc3_snippet.py`, `flask_extensions_snippet.py`, `cv2_serial_snippet.py`

## Test Structure

**Rust Test Pattern:**
```rust
#[test]
fn descriptive_test_name() {
    // Arrange
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/sample_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.validate = false;

    // Act
    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    // Assert
    assert!(result.requirements_txt.contains("requests==2.32.3"));
    assert_eq!(result.python_version, "2.7");
}
```

**Python Test Pattern:**
```python
@patch("llm_py.actions.recovery.package_exists_on_pypi", return_value=True)
@patch("llm_py.actions.recovery.LlmClient")
def test_swap_package(mock_client_cls, mock_pypi):
    """LLM suggests replacing one package with another."""
    # Arrange
    mock_client = MagicMock()
    mock_client.is_available.return_value = True
    mock_client.complete_json.return_value = RecoveryResult(...)

    # Act
    resp = handle(req)

    # Assert
    assert resp.fix_possible is True
    assert resp.wrong_package == "psycopg2"
```

**Common Patterns:**
- Arrange-Act-Assert structure
- Fixture-based testing for integration scenarios
- Isolated unit tests with mocking for external dependencies

## Mocking and Test Doubles

**Python Mocking:**
- Framework: `unittest.mock.MagicMock`, `@patch` decorator
- Mock LLM client to avoid network calls
- Mock PyPI API for package validation

**Mocking Pattern:**
```python
@patch("llm_py.actions.recovery.LlmClient")
def test_provider_unavailable(mock_client_cls):
    mock_client = MagicMock()
    mock_client.is_available.return_value = False
    mock_client_cls.return_value = mock_client
    # Test graceful degradation
```

**Rust Test Doubles:**
- No mocking framework detected
- Use in-memory test fixtures
- Temporary directories for cache tests: `tool_root.join("target/test-*")`

**What to Mock:**
- External LLM API calls
- PyPI network requests
- Docker operations (when testing isolation logic)

**What NOT to Mock:**
- Core resolution logic (test actual behavior)
- File I/O (use temp directories)
- Internal cache operations

## Fixtures and Factories

**Test Data Location:**
- `tools/apdr/tests/fixtures/` - Python code snippets
- Each fixture represents a real dependency resolution scenario

**Fixture Examples:**
- `sample_snippet.py` - Basic import resolution
- `python2_pil_stringio_snippet.py` - Python 2.7 stdlib detection
- `legacy_pymc3_snippet.py` - Complex package family bundles
- `flask_extensions_snippet.py` - Flask plugin namespace resolution
- `cv2_serial_snippet.py` - C-extension wrapper mappings

**Factory Pattern:**
```python
def _make_request(**overrides) -> ResolutionRequest:
    """Create a minimal ResolutionRequest for recovery testing."""
    defaults = dict(
        action="recovery",
        resolved_packages=["scrapy (import: scrapy)", "peewee (import: peewee)"],
        error_log="ERROR: Command errored out with exit status 1",
        snippet_source="from scrapy import Item\nimport peewee",
        python_version="2.7",
        # ...
    )
    defaults.update(overrides)
    return ResolutionRequest(**defaults)
```

**Fixture Helper Function:**
```rust
fn resolve_fixture(snippet_name: &str, output_name: &str) -> apdr::ResolveResult {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures").join(snippet_name);
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target").join(output_name);
    config.validate = false;
    apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap()
}
```

## Test Categories

**Integration Tests (Rust):**
- End-to-end resolution scenarios
- Validate against real Python code fixtures
- Test cross-module interactions (parser → resolver → validator)
- File: `tests/test_resolver.rs`

**Unit Tests (Python):**
- Mock-based tests for LLM actions
- Isolate individual action handlers
- File: `llm_py/tests/test_recovery_mock.py`

**Regression Tests:**
- Tests named after bug scenarios: `py2_memcache_maps_to_python_memcached_with_version_cap()`
- Each test documents the issue being prevented

**Property-Based Tests:**
- Not detected in current codebase

## Coverage

**Requirements:**
- No explicit coverage targets enforced
- Coverage tools not configured in visible config

**Current Coverage:**
- Core resolution logic: Well-covered with 67+ test cases
- Parser: Basic coverage (5 tests)
- LLM recovery: Comprehensive mock-based tests (12+ scenarios)
- Edge cases: Extensive regression test suite

**View Coverage:**
```bash
# Rust (requires tarpaulin or similar)
cargo tarpaulin --out Html
# Not configured by default

# Python
pytest --cov=llm_py --cov-report=html
# Not configured by default
```

## Test Types by Scenario

**Dependency Resolution Tests:**
- Seed-based mapping: `resolver_maps_seeded_imports_to_packages()`
- Fuzzy matching prevention: `tier2_does_not_fuzzy_match_short_imports_to_unrelated_packages()`
- Namespace resolution: `tier1_resolves_specific_namespace_aliases_from_import_paths()`

**Version Compatibility Tests:**
- Python 2.7 handling: `resolver_pins_legacy_pillow_for_python2_pil_snippets()`
- Legacy stack bundles: `resolver_normalizes_legacy_pymc3_stack_to_compatible_versions()`
- Version fallback: `tier1_discrepancy_versions_fall_back_to_latest_python_compatible_release()`

**Parser Tests:**
- Import extraction: `parser_extracts_non_stdlib_imports()`
- Python version detection: `parser_detects_python27_and_uses_python27_stdlib()`
- Config file scanning: `parser_ignores_generated_benchmark_requirements()`

**LLM Recovery Tests:**
- Package swap: `test_swap_package()`
- Version pinning: `test_version_pin()`
- Transitive deps: `test_add_transitive_dep()`
- Validation rejection: `test_pypi_validation_rejects_hallucination()`
- Namespace validation: `test_namespace_incompatible_swap_rejected()`

## Common Patterns

**Temporary Test Artifacts:**
```rust
let cache_path = tool_root.join("target/test-fuzzy-cache");
let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
// ... test logic ...
std::fs::remove_dir_all(cache_path).unwrap();  // Cleanup
```

**Parameterized Tests (Python):**
```python
@pytest.mark.parametrize(
    ("resolved_packages", "wrong_package", "correct_package"),
    [
        (["PySide (import: PySide)"], "PySide", "PySide6"),
        (["python-ldap (import: ldap)"], "python-ldap", "ldap3"),
        # ...
    ],
)
def test_namespace_incompatible_swap_rejected(mock_client_cls, mock_pypi, ...):
    # Test body runs for each parameter set
```

**Async Testing:**
- Not used (synchronous test execution)

**Error Testing:**
```rust
// Verify graceful failure
assert!(!result.satisfiable);
assert!(result.hard_unsat);
assert!(result.reason.is_some());
```

**String Assertion Patterns:**
```rust
// Flexible matching for dynamic content
assert!(
    result.requirements_txt.contains("python-memcached"),
    "Expected python-memcached in requirements, got: {}",
    result.requirements_txt,
);
```

## Test Data Management

**Isolated Cache Directories:**
- Each test creates unique cache: `target/test-{scenario}-cache`
- Ensures test isolation
- Cleanup in teardown

**Fixture Versioning:**
- Fixtures represent known-working scenarios
- Named after GitHub gist IDs or scenario descriptions
- Committed to repository for reproducibility

## Testing Best Practices

**Assertions:**
- Include failure messages with context
- Show actual vs. expected values
- Use descriptive assertion messages

**Test Independence:**
- Each test cleans up after itself
- No shared mutable state between tests
- Unique output directories per test

**Test Naming:**
- Start with component: `parser_`, `resolver_`, `tier1_`, `test_`
- Describe behavior: `_extracts_`, `_maps_to_`, `_resolves_`
- Include scenario context when relevant

**Error Messages:**
- Custom messages explain what was expected:
  ```rust
  "Expected python-memcached with Py2-compatible version cap, got: {}"
  ```

---

*Testing analysis: 2026-03-25*
