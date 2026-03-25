# Coding Conventions

**Analysis Date:** 2026-03-25

## Languages and Style

**Primary Language:**
- Rust (Edition 2021)
- Python 3.11+ for LLM integration components

**Rust Style:**
- Standard rustfmt formatting (no custom .rustfmt.toml detected)
- Clippy linting enabled

## Naming Patterns

**Files (Rust):**
- `snake_case.rs` for modules: `build_cache.rs`, `tier1_cache.rs`, `pubgrub_solver.rs`
- `mod.rs` for module entry points

**Files (Python):**
- `snake_case.py` for modules: `build_error_patterns.py`, `active_learning.py`
- `test_*.py` for test files: `test_recovery_mock.py`

**Functions:**
- Rust: `snake_case` - `parse_snippet()`, `resolve_path()`, `validate_requirements()`
- Python: `snake_case` - `handle()`, `package_exists_on_pypi()`, `prewarm_ollama()`

**Types and Structs:**
- `PascalCase` for structs: `ParseResult`, `ResolveConfig`, `ValidationSummary`
- `PascalCase` for enums (implied from code patterns)

**Constants:**
- `SCREAMING_SNAKE_CASE`: `VALIDATION_BACKEND_ENV`, `VALIDATION_BACKEND_DOCKER`

**Variables:**
- `snake_case` for local variables and struct fields

## Module Organization

**Rust Module Structure:**
- Modules declared in parent `mod.rs` or `lib.rs`
- Submodule pattern: `pub mod cache;` exposes `cache/mod.rs`
- Deep nesting: `cache::build_cache`, `resolver::tier1_cache`

**Key Modules:**
- `tools/apdr/src/lib.rs` - Core public types and configuration
- `tools/apdr/src/parser/` - AST parsing and import extraction
- `tools/apdr/src/resolver/` - Dependency resolution logic
- `tools/apdr/src/cache/` - Multi-tier caching system
- `tools/apdr/src/docker/` - Validation backend
- `tools/apdr/llm_py/` - Python LLM integration service

**Python Module Structure:**
- `__init__.py` for package markers
- `__main__.py` for CLI entry points
- Organized by action: `actions/recovery.py`, `actions/resolve.py`, `actions/solvability.py`

## Import Organization

**Rust Import Order:**
1. Standard library: `use std::collections::BTreeMap;`
2. External crates: `use serde_json::Value;`
3. Internal crate modules: `use crate::ParseResult;`
4. Relative imports: `use super::something;`

**Path Aliases:**
- `crate::` for internal modules
- No custom path aliases detected in Cargo.toml

**Python Import Order:**
1. Future imports: `from __future__ import annotations`
2. Standard library
3. Third-party packages
4. Local modules

## Documentation Standards

**Rust Doc Comments:**
- `///` for public API documentation
- `//!` for module-level documentation
- Example: `/// Extract a short error hint (≤120 chars) from a log excerpt.`

**Docstring Coverage:**
- Module-level docs: Present for complex modules (e.g., `pubgrub_solver.rs`)
- Function-level docs: Used for key public functions
- Type-level docs: Used for major structs

**Python Docstrings:**
- Triple-quoted strings for module and function docs
- Example: `"""Mock tests for the LLM recovery pipeline."""`

## Error Handling

**Rust Patterns:**
- `Result<T, io::Error>` for I/O operations
- `Result<T, String>` for CLI operations
- `Option<T>` for nullable values
- `.unwrap()` only in tests or after explicit validation
- `?` operator for error propagation

**Python Patterns:**
- Exception handling with try/except blocks
- Pydantic validation for data models
- Return `None` for LLM failures (graceful degradation)

## Code Style Enforcement

**Rust:**
- rustfmt 1.8.0-stable
- clippy 0.1.94
- No custom formatting configuration

**Python:**
- No explicit formatter config detected
- Pydantic for runtime type checking
- Type hints using `from __future__ import annotations`

## Logging and Debugging

**Rust:**
- `eprintln!()` for user-facing messages
- No structured logging framework detected
- Debug output via `println!()` in development

**Python:**
- `logging` module: `logger = logging.getLogger("apdr_llm")`
- Log level control: `logging.getLogger("LiteLLM").setLevel(logging.WARNING)`
- Suppress verbose third-party logs

## Configuration Management

**Environment Variables:**
- Read via `std::env::var()` with defaults
- Pattern: `env_flag()`, `env_usize()`, `env_optional_gib()` helper functions
- Examples: `APDR_VALIDATION_TIMEOUT_SECS`, `APDR_ENABLE_PACKAGE_REPOSITORY_CACHE`, `OLLAMA_KEEP_ALIVE`

**Default Values:**
- Centralized in `ResolveConfig::for_tool_root()`
- Builder pattern not used; struct initialization with named fields

## Data Structures

**Preferred Collections:**
- `BTreeMap` over `HashMap` for deterministic ordering
- `BTreeSet` for unique, sorted collections
- `Vec<String>` for lists

**Serialization:**
- Pydantic models for Python (JSON schema validation)
- Serde implied (dependency present) but not heavily used in visible code

## Performance Patterns

**Optimizations:**
- `once_cell` for lazy static initialization
- Connection pooling: Custom implementation in `kgraph_db.rs`
- Parallel execution: `rayon` not detected; uses standard threads
- LRU caching with custom eviction policies

**Release Profile (Cargo.toml):**
```toml
[profile.release]
lto = "fat"
codegen-units = 1
opt-level = 3
strip = "symbols"
panic = "abort"
```

## Testing Conventions

**Test Function Naming:**
- Descriptive names: `test_swap_package()`, `resolver_maps_seeded_imports_to_packages()`
- Pattern: `{action}_{expected_behavior}` or `{component}_{scenario}`

**Assertion Style:**
- `assert!()` for boolean conditions
- `assert_eq!()` for equality checks
- `assert!(condition, "message with context")` for failures

**Test Organization:**
- Integration tests in `tests/` directory
- Unit tests co-located with source (not heavily used)
- Fixture-based tests: `tests/fixtures/` directory

## Comments

**When to Comment:**
- Complex algorithms requiring explanation
- Non-obvious design decisions
- Bug workarounds with context

**Comment Style:**
- Inline: `// Comment explaining next line`
- Block: Used sparingly for multi-line explanations
- TODO/FIXME: Not prevalent in reviewed files

## Function Design

**Size:**
- Long functions accepted for main logic flows (e.g., `validate_requirements()`)
- Helper functions extracted for reusability

**Parameters:**
- Borrowed references preferred: `&Path`, `&str`, `&[String]`
- Mutable borrows when necessary: `&mut CacheStore`
- Configuration structs passed by reference: `&ResolveConfig`

**Return Values:**
- `Result<T, E>` for fallible operations
- Structs for complex return values: `ValidationSummary`, `ResolveResult`
- Avoid tuples for more than 2 values

## Python Specific

**Type Hints:**
- Comprehensive usage in Python 3.11+ style
- Pydantic `BaseModel` for data classes
- Generic types: `TypeVar` for parameterized functions

**Async/Await:**
- Not used in visible code (synchronous execution model)

**Decorator Usage:**
- `@patch` for mocking in tests
- `@pytest.mark.parametrize` for parameterized tests

## Cross-Language IPC

**Rust ↔ Python Communication:**
- JSON-line protocol over stdin/stdout
- Pydantic models define schema: `ResolutionRequest`, `ResolutionResponse`
- Rust invokes Python subprocess for LLM operations

---

*Convention analysis: 2026-03-25*
