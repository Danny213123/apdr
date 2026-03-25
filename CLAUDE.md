<!-- GSD:project-start source:PROJECT.md -->
## Project

**APDR Enhancement: Accuracy & Performance**

APDR (Automated Python Dependency Resolution) improvements focused on three areas: (1) responsive real-time UI with deterministic/LLM result separation, (2) better LLM recovery accuracy for build failures, and (3) faster inference and validation throughput.

**Core Value:** The benchmark UI must stay responsive and show real-time progress during runs. Users need to see deterministic passes immediately (tier1/tier2 cache hits) separate from LLM-based resolution attempts, without browser hangs or stale data.

### Constraints

- **Tech Stack**: Rust + Python + vanilla JS — no framework rewrites
- **LLM Provider**: Ollama local inference — must work offline
- **Validation**: Docker-based ground truth — no shortcuts
- **Compatibility**: Windows support required (current issues with BuildKit deadlock workaround)
- **Performance**: Must handle parallel workers without UI freezing
- **Data**: Hard-gists benchmark dataset — fixed test suite
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Rust (2021 edition) - Core APDR resolver implementation
- Python 3.11 - LLM service and benchmark utilities
- JavaScript (ES modules) - Web UI for benchmark control plane
- Shell/Bash - Build scripts and Docker entrypoints
## Runtime
- Rust 2021 edition (compiled native binary)
- Python 3.11 (for LLM service and Docker agent)
- Node.js (for web UI build tooling)
- Cargo (Rust) - `Cargo.toml`, `Cargo.lock` present at `/d/apdr/tools/apdr/`
- pip (Python) - `requirements.txt` files in `llm_py/` and `docker_agent/`
- npm (JavaScript) - `package.json`, `package-lock.json` in `/d/apdr/web/`
## Frameworks
- None (vanilla Rust standard library for main resolver)
- LiteLLM >=1.40 - Multi-provider LLM gateway for Python service
- Instructor >=1.3 - Structured LLM output extraction
- Pydantic >=2.5 - Python data validation and models
- Built-in Rust test framework - Tests in `/d/apdr/tools/apdr/tests/`
- Python unittest/pytest implied - Tests in `/d/apdr/tools/apdr/llm_py/tests/`
- Cargo (Rust build system)
- Vite 6.2.0 - Frontend build tool for web UI
- Docker Compose - Development environment orchestration
## Key Dependencies
- `rusqlite` 0.32 (bundled) - Embedded SQLite database for knowledge graph cache
- `pubgrub` 0.3 - PubGrub version resolution algorithm
- `version-ranges` 0.1 - Python version constraint parsing
- `serde_json` 1.0 - JSON serialization/deserialization
- `ureq` 3 - HTTP client for PyPI API calls
- `mimalloc` 0.1 - High-performance memory allocator (release builds)
- `flate2` 1.0, `tar` 0.4, `zstd` 0.13 - Archive compression/decompression
- `rustc-hash` 2 - Fast hash map for hot paths
- `once_cell` 1.19 - Lazy static initialization
- `tempfile` 3.27 - Temporary file/directory management
- `litellm` >=1.40 - Unified LLM API (supports Ollama, OpenAI, Anthropic, etc.)
- `instructor` >=1.3 - Constrained LLM output with Pydantic schemas
- `pydantic` >=2.5 - Type validation and structured data models
- `requests` >=2.28 - HTTP client for API calls
- `langgraph` >=0.2 - ReAct agent orchestration (feature #10)
- `langchain-core` >=0.3, `langchain-community` >=0.3 - Agent tooling
- `dspy` >=2.5 - Prompt optimization framework (feature #5)
- `unsloth`, `trl` >=0.7, `transformers` >=4.40, `datasets` >=2.18 - LoRA fine-tuning (feature #9)
- `docker` 7.1.0 (Python) - Docker SDK for container management
- `ollama` 0.2.0 (Python) - Ollama Python client
## Configuration
- Environment variables configured via `.env` files (examples in `.env.example`)
- Required vars: `USER`, `UID`, `GID`, `DOCKER_GID` for Docker container setup
- LLM configuration via Python environment (provider, model, base URL)
- `Cargo.toml` - Rust package manifest and release optimization settings
- `vite.config.js` - Frontend dev server and API proxy configuration
- `docker-compose.yml` - Multi-container orchestration (pllm service + Ollama)
## Platform Requirements
- Rust toolchain (2021 edition)
- Python 3.11+
- Node.js (for web UI)
- Docker and Docker Compose
- Ollama (for LLM inference)
- Compiled Rust binary (platform-specific)
- Docker runtime for validation environments
- Access to PyPI (pypi.org)
- LLM provider (Ollama local or cloud API)
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Languages and Style
- Rust (Edition 2021)
- Python 3.11+ for LLM integration components
- Standard rustfmt formatting (no custom .rustfmt.toml detected)
- Clippy linting enabled
## Naming Patterns
- `snake_case.rs` for modules: `build_cache.rs`, `tier1_cache.rs`, `pubgrub_solver.rs`
- `mod.rs` for module entry points
- `snake_case.py` for modules: `build_error_patterns.py`, `active_learning.py`
- `test_*.py` for test files: `test_recovery_mock.py`
- Rust: `snake_case` - `parse_snippet()`, `resolve_path()`, `validate_requirements()`
- Python: `snake_case` - `handle()`, `package_exists_on_pypi()`, `prewarm_ollama()`
- `PascalCase` for structs: `ParseResult`, `ResolveConfig`, `ValidationSummary`
- `PascalCase` for enums (implied from code patterns)
- `SCREAMING_SNAKE_CASE`: `VALIDATION_BACKEND_ENV`, `VALIDATION_BACKEND_DOCKER`
- `snake_case` for local variables and struct fields
## Module Organization
- Modules declared in parent `mod.rs` or `lib.rs`
- Submodule pattern: `pub mod cache;` exposes `cache/mod.rs`
- Deep nesting: `cache::build_cache`, `resolver::tier1_cache`
- `tools/apdr/src/lib.rs` - Core public types and configuration
- `tools/apdr/src/parser/` - AST parsing and import extraction
- `tools/apdr/src/resolver/` - Dependency resolution logic
- `tools/apdr/src/cache/` - Multi-tier caching system
- `tools/apdr/src/docker/` - Validation backend
- `tools/apdr/llm_py/` - Python LLM integration service
- `__init__.py` for package markers
- `__main__.py` for CLI entry points
- Organized by action: `actions/recovery.py`, `actions/resolve.py`, `actions/solvability.py`
## Import Organization
- `crate::` for internal modules
- No custom path aliases detected in Cargo.toml
## Documentation Standards
- `///` for public API documentation
- `//!` for module-level documentation
- Example: `/// Extract a short error hint (≤120 chars) from a log excerpt.`
- Module-level docs: Present for complex modules (e.g., `pubgrub_solver.rs`)
- Function-level docs: Used for key public functions
- Type-level docs: Used for major structs
- Triple-quoted strings for module and function docs
- Example: `"""Mock tests for the LLM recovery pipeline."""`
## Error Handling
- `Result<T, io::Error>` for I/O operations
- `Result<T, String>` for CLI operations
- `Option<T>` for nullable values
- `.unwrap()` only in tests or after explicit validation
- `?` operator for error propagation
- Exception handling with try/except blocks
- Pydantic validation for data models
- Return `None` for LLM failures (graceful degradation)
## Code Style Enforcement
- rustfmt 1.8.0-stable
- clippy 0.1.94
- No custom formatting configuration
- No explicit formatter config detected
- Pydantic for runtime type checking
- Type hints using `from __future__ import annotations`
## Logging and Debugging
- `eprintln!()` for user-facing messages
- No structured logging framework detected
- Debug output via `println!()` in development
- `logging` module: `logger = logging.getLogger("apdr_llm")`
- Log level control: `logging.getLogger("LiteLLM").setLevel(logging.WARNING)`
- Suppress verbose third-party logs
## Configuration Management
- Read via `std::env::var()` with defaults
- Pattern: `env_flag()`, `env_usize()`, `env_optional_gib()` helper functions
- Examples: `APDR_VALIDATION_TIMEOUT_SECS`, `APDR_ENABLE_PACKAGE_REPOSITORY_CACHE`, `OLLAMA_KEEP_ALIVE`
- Centralized in `ResolveConfig::for_tool_root()`
- Builder pattern not used; struct initialization with named fields
## Data Structures
- `BTreeMap` over `HashMap` for deterministic ordering
- `BTreeSet` for unique, sorted collections
- `Vec<String>` for lists
- Pydantic models for Python (JSON schema validation)
- Serde implied (dependency present) but not heavily used in visible code
## Performance Patterns
- `once_cell` for lazy static initialization
- Connection pooling: Custom implementation in `kgraph_db.rs`
- Parallel execution: `rayon` not detected; uses standard threads
- LRU caching with custom eviction policies
## Testing Conventions
- Descriptive names: `test_swap_package()`, `resolver_maps_seeded_imports_to_packages()`
- Pattern: `{action}_{expected_behavior}` or `{component}_{scenario}`
- `assert!()` for boolean conditions
- `assert_eq!()` for equality checks
- `assert!(condition, "message with context")` for failures
- Integration tests in `tests/` directory
- Unit tests co-located with source (not heavily used)
- Fixture-based tests: `tests/fixtures/` directory
## Comments
- Complex algorithms requiring explanation
- Non-obvious design decisions
- Bug workarounds with context
- Inline: `// Comment explaining next line`
- Block: Used sparingly for multi-line explanations
- TODO/FIXME: Not prevalent in reviewed files
## Function Design
- Long functions accepted for main logic flows (e.g., `validate_requirements()`)
- Helper functions extracted for reusability
- Borrowed references preferred: `&Path`, `&str`, `&[String]`
- Mutable borrows when necessary: `&mut CacheStore`
- Configuration structs passed by reference: `&ResolveConfig`
- `Result<T, E>` for fallible operations
- Structs for complex return values: `ValidationSummary`, `ResolveResult`
- Avoid tuples for more than 2 values
## Python Specific
- Comprehensive usage in Python 3.11+ style
- Pydantic `BaseModel` for data classes
- Generic types: `TypeVar` for parameterized functions
- Not used in visible code (synchronous execution model)
- `@patch` for mocking in tests
- `@pytest.mark.parametrize` for parameterized tests
## Cross-Language IPC
- JSON-line protocol over stdin/stdout
- Pydantic models define schema: `ResolutionRequest`, `ResolutionResponse`
- Rust invokes Python subprocess for LLM operations
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## Pattern Overview
- Tiered resolution strategy (cache → heuristic → LLM)
- Multi-backend validation system (env, Docker, LLM)
- Iterative error recovery with classifier-based pattern matching
- Multi-agent LLM orchestration using LangGraph state machines
- Extensive caching at multiple layers (import mappings, lockfiles, build artifacts, validated environments)
## Layers
- Purpose: Extract imports and Python version requirements from source code
- Location: `tools/apdr/src/parser/`
- Contains: AST analysis, config file scanning, stdlib detection, import path analysis
- Depends on: stdlib module data (`tools/apdr/data/stdlib_modules/`)
- Used by: Resolver layer
- Purpose: Map imports to PyPI packages using tiered strategy
- Location: `tools/apdr/src/resolver/`
- Contains: Three-tier resolution (cache/heuristic/LLM), PubGrub solver, PyPI client, version sampling
- Depends on: Parser output, cache store, LLM Python bridge
- Used by: Docker validation layer
- Purpose: Persist and retrieve resolution artifacts across runs
- Location: `tools/apdr/src/cache/`
- Contains: Import mappings, lockfiles, failure patterns, build artifacts, PyPI index, dependency graph, validated environment cache
- Depends on: Filesystem storage at `.apdr-cache/`
- Used by: All layers for artifact reuse
- Purpose: Build isolated Python environments and validate solutions
- Location: `tools/apdr/src/docker/`
- Contains: Dockerfile generation, parallel version testing, smoke test execution, system dependency detection
- Depends on: Resolver output, Docker daemon
- Used by: Validation orchestrator
- Purpose: Diagnose build failures and suggest corrective actions
- Location: `tools/apdr/src/recovery/`
- Contains: Error log classifier, conflict taxonomy, pattern library, LLM-based recovery
- Depends on: Build logs, failure pattern cache
- Used by: Iterative resolution loop
- Purpose: Interface between Rust and Python LLM libraries
- Location: `tools/apdr/llm_py/`
- Contains: Ollama client, resolution actions, solvability assessment, error recovery, RAG pipelines
- Depends on: Pydantic, instructor library, Ollama API
- Used by: Tier3 LLM resolver, recovery layer
- Purpose: Orchestrate iterative dependency resolution with autonomous agents
- Location: `tools/apdr/docker_agent/`
- Contains: LangGraph state machine, analyst/builder/recovery/confidence agents, RAG-enhanced prompts
- Depends on: LangGraph, LangChain, Ollama
- Used by: LLM validation backend
- Purpose: Web and CLI interface for running batch evaluations
- Location: `benchmark_ui/`
- Contains: Flask server, terminal UI, run management, configuration presets, doctor diagnostics
- Depends on: APDR tool, hard-gists dataset
- Used by: Competition participants and researchers
## Data Flow
- Immutable data structures in Rust (BTreeMap, BTreeSet)
- Persistent cache in `.apdr-cache/` (TSV files + SQLite)
- LangGraph state machine for multi-agent coordination
- Benchmark context log for reproducibility (`benchmark-context.log`)
## Key Abstractions
- Purpose: Normalized representation of snippet requirements
- Examples: `src/lib.rs:28-39`
- Pattern: Immutable struct with confidence scoring
- Purpose: Binding of import name to package + version
- Examples: `src/lib.rs:67-73`
- Pattern: Strategy-tagged resolution (cache/heuristic/llm)
- Purpose: Unified interface to all cache layers
- Examples: `src/cache/store.rs:41-57`
- Pattern: Lazy-loaded BTreeMap collections with TSV backing
- Purpose: Detailed outcome of build + smoke test
- Examples: `src/lib.rs:134-166`
- Pattern: Comprehensive telemetry for benchmarking
- Purpose: Shared state for multi-agent graph execution
- Examples: `docker_agent/state.py`
- Pattern: Dictionary-based state machine with typed updates
## Entry Points
- Location: `tools/apdr/src/main.rs`
- Triggers: Command-line invocation (`apdr resolve`, `apdr cache`, `apdr classify-log`)
- Responsibilities: Argument parsing, config initialization, command dispatch
- Location: `benchmark_ui/server.py`
- Triggers: HTTP requests to Flask API
- Responsibilities: Run orchestration, progress tracking, model configuration
- Location: `benchmark_ui/cli_app.py`
- Triggers: Terminal UI actions
- Responsibilities: Interactive benchmark control, run management
- Location: `llm_py/actions/resolve.py` (and siblings)
- Triggers: IPC from Rust tier3_llm module
- Responsibilities: JSON-RPC style request/response for LLM calls
- Location: `docker_agent/graph.py`
- Triggers: LLM validation backend invocation
- Responsibilities: Stateful iterative resolution with autonomous agents
## Error Handling
- Result types in Rust (`io::Result`, custom `Result<_, String>`)
- Try-catch in Python with structured logging
- Classifier-based error categorization (build_failed, runtime_failed, import_missing)
- Pattern library matching for known failure modes
- LLM fallback for novel errors
- Persistent failure learning (cache unsolvable modules)
- `env`: Local virtualenv (fast, limited isolation)
- `docker`: Container build (slower, full isolation)
- `llm`: Multi-agent iterative repair (slowest, highest success rate)
- Platform-specific modules (microbit, RPi.GPIO, binaryninja)
- Legacy incompatible packages (cfscrape on Py3.13)
- Host runtime dependencies (simplecv with non-installable system deps)
## Cross-Cutting Concerns
- Rust: stderr output with `eprintln!`
- Python: `logging` module with structured context
- Benchmark: Append-only `benchmark-context.log` with action/data pairs
- Debug artifacts: `.apdr-debug/` with per-attempt logs, metadata JSON
- Import name normalization (lowercase, `-` instead of `_`)
- PyPI package existence checks before resolution
- Namespace mapping validation (explicit allowlist)
- Confidence thresholds (skip tier1 fuzzy matches < 0.5)
- Import-set validation cache (skip re-validation of identical imports)
- None (uses public PyPI and local Ollama server)
- Multi-level: import mappings → lockfiles → build artifacts → validated envs
- Eviction: LRU with size limits (configurable GB thresholds)
- Invalidation: Manual via `apdr cache prune`
- Warming: `apdr cache warm --top-packages 5000`
- Parallel Python version testing (`src/docker/parallel.rs`)
- Single-threaded LLM subprocess (persistent process for latency)
- Thread-safe cache store (BTreeMap read-heavy, occasional write)
- mimalloc global allocator (`src/main.rs:2`)
- Release profile: LTO, single codegen unit, strip symbols
- Import-set cache (5ms path vs 300s full resolution)
- Validated environment cache (reuse Docker builds)
- PyPI metadata SQLite cache (knowledge_cache.db)
- Seed data preloading (top 5000 packages)
<!-- GSD:architecture-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd:quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd:debug` for investigation and bug fixing
- `/gsd:execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->



<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd:profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
