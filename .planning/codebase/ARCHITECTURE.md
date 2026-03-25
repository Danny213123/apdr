# Architecture

**Analysis Date:** 2026-03-25

## Pattern Overview

**Overall:** Multi-stage resolution pipeline with hybrid Rust/Python architecture

**Key Characteristics:**
- Tiered resolution strategy (cache → heuristic → LLM)
- Multi-backend validation system (env, Docker, LLM)
- Iterative error recovery with classifier-based pattern matching
- Multi-agent LLM orchestration using LangGraph state machines
- Extensive caching at multiple layers (import mappings, lockfiles, build artifacts, validated environments)

## Layers

**Parser Layer:**
- Purpose: Extract imports and Python version requirements from source code
- Location: `tools/apdr/src/parser/`
- Contains: AST analysis, config file scanning, stdlib detection, import path analysis
- Depends on: stdlib module data (`tools/apdr/data/stdlib_modules/`)
- Used by: Resolver layer

**Resolver Layer:**
- Purpose: Map imports to PyPI packages using tiered strategy
- Location: `tools/apdr/src/resolver/`
- Contains: Three-tier resolution (cache/heuristic/LLM), PubGrub solver, PyPI client, version sampling
- Depends on: Parser output, cache store, LLM Python bridge
- Used by: Docker validation layer

**Cache Layer:**
- Purpose: Persist and retrieve resolution artifacts across runs
- Location: `tools/apdr/src/cache/`
- Contains: Import mappings, lockfiles, failure patterns, build artifacts, PyPI index, dependency graph, validated environment cache
- Depends on: Filesystem storage at `.apdr-cache/`
- Used by: All layers for artifact reuse

**Docker Layer:**
- Purpose: Build isolated Python environments and validate solutions
- Location: `tools/apdr/src/docker/`
- Contains: Dockerfile generation, parallel version testing, smoke test execution, system dependency detection
- Depends on: Resolver output, Docker daemon
- Used by: Validation orchestrator

**Recovery Layer:**
- Purpose: Diagnose build failures and suggest corrective actions
- Location: `tools/apdr/src/recovery/`
- Contains: Error log classifier, conflict taxonomy, pattern library, LLM-based recovery
- Depends on: Build logs, failure pattern cache
- Used by: Iterative resolution loop

**LLM Python Bridge:**
- Purpose: Interface between Rust and Python LLM libraries
- Location: `tools/apdr/llm_py/`
- Contains: Ollama client, resolution actions, solvability assessment, error recovery, RAG pipelines
- Depends on: Pydantic, instructor library, Ollama API
- Used by: Tier3 LLM resolver, recovery layer

**Multi-Agent System:**
- Purpose: Orchestrate iterative dependency resolution with autonomous agents
- Location: `tools/apdr/docker_agent/`
- Contains: LangGraph state machine, analyst/builder/recovery/confidence agents, RAG-enhanced prompts
- Depends on: LangGraph, LangChain, Ollama
- Used by: LLM validation backend

**Benchmark UI:**
- Purpose: Web and CLI interface for running batch evaluations
- Location: `benchmark_ui/`
- Contains: Flask server, terminal UI, run management, configuration presets, doctor diagnostics
- Depends on: APDR tool, hard-gists dataset
- Used by: Competition participants and researchers

## Data Flow

**Primary Resolution Flow:**

1. **Parse** (`src/parser/mod.rs::parse_snippet`)
   - Read Python source → AST analysis → extract imports
   - Detect Python version constraints from syntax/config files
   - Filter stdlib modules using version-specific data
   - Output: `ParseResult` with imports, version range, config deps

2. **Pre-solve Assessment** (`src/resolver/pre_solve.rs`)
   - Check for host-runtime dependencies (platform-specific, hardware)
   - Query unsolvable module cache (persistent LLM learnings)
   - Query import-set cache (validated solutions for exact import combinations)
   - Short-circuit with skip/cached result if applicable

3. **Tier 1: Cache Resolution** (`src/resolver/tier1_cache.rs`)
   - Lookup import → package mappings in seed data + dynamic cache
   - Resolve config file dependencies (requirements.txt, setup.py)
   - Filter fuzzy matches (defer to tier2)
   - Output: High-confidence mappings from trusted sources

4. **Tier 2: Heuristic Resolution** (`src/resolver/tier2_heuristic.rs`)
   - Apply namespace package rules (google.cloud.*, azure.*)
   - Use package family knowledge (opencv variants, tensorflow flavors)
   - Fuzzy match with popularity ranking as tiebreaker
   - Attribute usage analysis (resolve ambiguous imports like `cv2.imread`)
   - Output: Medium-confidence mappings from pattern matching

5. **Tier 3: LLM Resolution** (`src/resolver/tier3_llm.rs` + `llm_py/actions/resolve.py`)
   - Spawn persistent Python subprocess with Ollama client
   - RAG-enhanced prompts with PyPI metadata, error patterns
   - Version range sampling for compatibility
   - Output: LLM-suggested package mappings with confidence scores

6. **PubGrub Solving** (`src/resolver/pubgrub_solver.rs`)
   - Take resolved package → version mappings
   - Fetch dependency metadata from PyPI (with SQLite cache)
   - Run constraint satisfaction solver
   - Output: Lockfile with pinned versions or conflict report

7. **Validation** (`src/docker/builder.rs`, `src/docker/smoke_test.rs`)
   - Generate Dockerfile with Python version + requirements
   - Build container, install packages, execute snippet
   - Capture build logs and runtime output
   - Output: Pass/fail status with diagnostic logs

8. **Error Recovery** (`src/recovery/classifier.rs`, `llm_py/actions/recovery.py`)
   - Classify error type (import failure, build error, version conflict)
   - Match against failure pattern library
   - LLM-based recovery suggestion (change version, add package, add system dep)
   - Apply fix and retry (max 5 attempts)
   - Output: Repaired requirements or definitive failure reason

9. **Result Caching** (`src/cache/store.rs`)
   - Save successful import mappings to dynamic cache
   - Save validated lockfiles with build artifacts
   - Save failure patterns for future classification
   - Save import-set solutions for instant reuse
   - Save unsolvable modules (persistent LLM learnings)

**Multi-Agent Validation Flow (LLM backend):**

1. **Confidence Node** (`docker_agent/agents/confidence.py`)
   - Assess solvability from import names + Python version
   - Output: Confidence score (< 0.4 = skip)

2. **Builder Node** (`docker_agent/agents/builder.py`)
   - Execute Docker build + smoke test
   - Output: Pass/fail status with logs

3. **Analyst Node** (`docker_agent/agents/analyst.py`)
   - Classify error from build/run logs
   - Match against known patterns
   - Output: Error type, conflict class, missing package

4. **Recovery Node** (`docker_agent/agents/recovery.py`)
   - Generate fix action (change_version, add_package, try_next_python)
   - RAG-enhanced with error pattern library
   - Output: Fix directive

5. **Apply Fix** (`docker_agent/graph.py::_apply_fix_node`)
   - Update requirements.txt or system dependencies
   - Loop back to builder (max 5 attempts)

6. **Termination** (END state)
   - Success: Return validated solution
   - Failure: Return classified error with repair history

**State Management:**
- Immutable data structures in Rust (BTreeMap, BTreeSet)
- Persistent cache in `.apdr-cache/` (TSV files + SQLite)
- LangGraph state machine for multi-agent coordination
- Benchmark context log for reproducibility (`benchmark-context.log`)

## Key Abstractions

**ParseResult:**
- Purpose: Normalized representation of snippet requirements
- Examples: `src/lib.rs:28-39`
- Pattern: Immutable struct with confidence scoring

**ResolvedDependency:**
- Purpose: Binding of import name to package + version
- Examples: `src/lib.rs:67-73`
- Pattern: Strategy-tagged resolution (cache/heuristic/llm)

**CacheStore:**
- Purpose: Unified interface to all cache layers
- Examples: `src/cache/store.rs:41-57`
- Pattern: Lazy-loaded BTreeMap collections with TSV backing

**ValidationSummary:**
- Purpose: Detailed outcome of build + smoke test
- Examples: `src/lib.rs:134-166`
- Pattern: Comprehensive telemetry for benchmarking

**AgentState:**
- Purpose: Shared state for multi-agent graph execution
- Examples: `docker_agent/state.py`
- Pattern: Dictionary-based state machine with typed updates

## Entry Points

**CLI Binary:**
- Location: `tools/apdr/src/main.rs`
- Triggers: Command-line invocation (`apdr resolve`, `apdr cache`, `apdr classify-log`)
- Responsibilities: Argument parsing, config initialization, command dispatch

**Benchmark Server:**
- Location: `benchmark_ui/server.py`
- Triggers: HTTP requests to Flask API
- Responsibilities: Run orchestration, progress tracking, model configuration

**Benchmark CLI:**
- Location: `benchmark_ui/cli_app.py`
- Triggers: Terminal UI actions
- Responsibilities: Interactive benchmark control, run management

**LLM Python Subprocess:**
- Location: `llm_py/actions/resolve.py` (and siblings)
- Triggers: IPC from Rust tier3_llm module
- Responsibilities: JSON-RPC style request/response for LLM calls

**Multi-Agent Graph:**
- Location: `docker_agent/graph.py`
- Triggers: LLM validation backend invocation
- Responsibilities: Stateful iterative resolution with autonomous agents

## Error Handling

**Strategy:** Layered error recovery with progressive escalation

**Patterns:**
- Result types in Rust (`io::Result`, custom `Result<_, String>`)
- Try-catch in Python with structured logging
- Classifier-based error categorization (build_failed, runtime_failed, import_missing)
- Pattern library matching for known failure modes
- LLM fallback for novel errors
- Persistent failure learning (cache unsolvable modules)

**Validation backends:**
- `env`: Local virtualenv (fast, limited isolation)
- `docker`: Container build (slower, full isolation)
- `llm`: Multi-agent iterative repair (slowest, highest success rate)

**Skip conditions:**
- Platform-specific modules (microbit, RPi.GPIO, binaryninja)
- Legacy incompatible packages (cfscrape on Py3.13)
- Host runtime dependencies (simplecv with non-installable system deps)

## Cross-Cutting Concerns

**Logging:**
- Rust: stderr output with `eprintln!`
- Python: `logging` module with structured context
- Benchmark: Append-only `benchmark-context.log` with action/data pairs
- Debug artifacts: `.apdr-debug/` with per-attempt logs, metadata JSON

**Validation:**
- Import name normalization (lowercase, `-` instead of `_`)
- PyPI package existence checks before resolution
- Namespace mapping validation (explicit allowlist)
- Confidence thresholds (skip tier1 fuzzy matches < 0.5)
- Import-set validation cache (skip re-validation of identical imports)

**Authentication:**
- None (uses public PyPI and local Ollama server)

**Caching Strategy:**
- Multi-level: import mappings → lockfiles → build artifacts → validated envs
- Eviction: LRU with size limits (configurable GB thresholds)
- Invalidation: Manual via `apdr cache prune`
- Warming: `apdr cache warm --top-packages 5000`

**Concurrency:**
- Parallel Python version testing (`src/docker/parallel.rs`)
- Single-threaded LLM subprocess (persistent process for latency)
- Thread-safe cache store (BTreeMap read-heavy, occasional write)

**Performance Optimizations:**
- mimalloc global allocator (`src/main.rs:2`)
- Release profile: LTO, single codegen unit, strip symbols
- Import-set cache (5ms path vs 300s full resolution)
- Validated environment cache (reuse Docker builds)
- PyPI metadata SQLite cache (knowledge_cache.db)
- Seed data preloading (top 5000 packages)

---

*Architecture analysis: 2026-03-25*
