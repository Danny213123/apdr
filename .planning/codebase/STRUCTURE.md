# Codebase Structure

**Analysis Date:** 2026-03-25

## Directory Layout

```
apdr/
├── benchmark_ui/          # Web + CLI benchmark interface
├── figures/               # Documentation images
├── hard-gists/            # Test dataset (2.9K Python snippets)
├── tools/
│   ├── apdr/              # Main APDR tool (Rust + Python)
│   └── pllm/              # Baseline comparison tool
├── scripts/               # Utility scripts
├── .planning/             # GSD codebase analysis outputs
├── .claude/               # Claude Code session data
├── CHANGELOG.md           # Version history
├── LICENSE                # GPLv3
└── README.md              # Competition documentation
```

## Directory Purposes

**`tools/apdr/`:**
- Purpose: Core APDR dependency resolution engine
- Contains: Rust binary + library, Python LLM bridge, multi-agent system, data files, tests
- Key files:
  - `Cargo.toml`: Rust project manifest (version 0.2.14)
  - `src/`: Rust source code (parser, resolver, cache, docker, recovery)
  - `llm_py/`: Python LLM integration modules
  - `docker_agent/`: LangGraph multi-agent orchestration
  - `data/`: Seed data, stdlib modules, knowledge base
  - `tests/`: Integration tests with fixtures
  - `.apdr-cache/`: Runtime cache directory (not committed)

**`benchmark_ui/`:**
- Purpose: Web and terminal interface for batch evaluation
- Contains: Flask server, CLI app, run management, configuration
- Key files:
  - `server.py`: Flask API backend
  - `cli_app.py`: Terminal UI with rich
  - `runner.py`: Benchmark execution logic
  - `service.py`: Shared state management
  - `__main__.py`: Entry point (`python -m benchmark_ui`)

**`hard-gists/`:**
- Purpose: HG2.9K dataset - 2900+ Python snippets with dependency conflicts
- Contains: Subdirectories named by GitHub gist ID, each with `snippet.py`
- Generated: Extracted from `hard-gists.tar.gz` on first benchmark run
- Committed: No (dataset archive committed, extracted files ignored)

**`tools/pllm/`:**
- Purpose: Baseline PLLM tool for comparison (from Bartlett et al. 2025)
- Contains: Docker-based RAG+LLM pipeline implementation
- Key files:
  - `Dockerfile`: Container definition
  - `docker-compose.yml`: Service orchestration
  - `test_executor.py`: Main execution script

**`.planning/`:**
- Purpose: GSD codebase analysis documents
- Contains: Architecture, structure, conventions, testing, stack docs
- Generated: By `/gsd:map-codebase` command
- Committed: Yes (planning artifacts committed)

## APDR Tool Structure (tools/apdr/)

**`src/` - Rust Source Code:**

**Core Modules:**
- `lib.rs`: Public API, data structures, configuration
- `main.rs`: CLI argument parsing, command dispatch
- `context.rs`: Debug layout, benchmark logging

**Parser Subsystem (`src/parser/`):**
- `mod.rs`: Entry point, orchestrates parsing
- `ast.rs`: Python AST traversal, stdlib loading
- `imports.rs`: Import extraction, attribute usage tracking
- `config_files.rs`: requirements.txt, setup.py scanning
- `version_detect.rs`: Python version inference from syntax

**Resolver Subsystem (`src/resolver/`):**
- `mod.rs`: Main resolution pipeline (tier1→tier2→tier3)
- `tier1_cache.rs`: Cache-based resolution (seed data + dynamic)
- `tier2_heuristic.rs`: Pattern-based resolution (namespace, fuzzy matching)
- `tier3_llm.rs`: LLM-based resolution (Python subprocess IPC)
- `pre_solve.rs`: Solvability assessment, skip detection
- `pypi_client.rs`: PyPI metadata fetching, version compatibility
- `pubgrub_solver.rs`: Constraint satisfaction solving
- `version_sampler.rs`: Smart version selection strategies
- `family_knowledge.rs`: Package variant knowledge (opencv, tensorflow)
- `kgraph_db.rs`: SQLite-backed knowledge graph cache

**Cache Subsystem (`src/cache/`):**
- `mod.rs`: Module exports
- `store.rs`: Unified cache interface, import-set solutions
- `import_map.rs`: Import → package mapping cache
- `version_map.rs`: Version constraint cache
- `lockfile_cache.rs`: Validated lockfile cache
- `build_cache.rs`: Docker build artifact cache
- `failure_cache.rs`: Error pattern cache
- `pypi_index.rs`: PyPI metadata cache
- `dep_graph.rs`: Package dependency graph cache
- `maintenance.rs`: Cache pruning, disk usage tracking

**Docker Subsystem (`src/docker/`):**
- `mod.rs`: Module exports
- `builder.rs`: Dockerfile generation, container orchestration
- `templates.rs`: Dockerfile template rendering
- `smoke_test.rs`: Snippet execution in container
- `system_deps.rs`: System dependency detection
- `parallel.rs`: Parallel Python version testing

**Recovery Subsystem (`src/recovery/`):**
- `mod.rs`: Module exports
- `classifier.rs`: Error log classification
- `conflict_taxonomy.rs`: Error type taxonomy
- `patterns.rs`: Failure pattern library
- `llm_recovery.rs`: LLM-based error recovery

**`llm_py/` - Python LLM Bridge:**

**Actions (`llm_py/actions/`):**
- `resolve.py`: Import → package resolution
- `version.py`: Version selection
- `batch_version.py`: Batch version resolution
- `solvability.py`: Solvability assessment
- `recovery.py`: Error recovery suggestions
- `react_agent.py`: ReAct-style reasoning agent
- `single.py`: Single import resolution

**Core Modules:**
- `client.py`: Ollama API client wrapper
- `models.py`: Pydantic data models (request/response)
- `prompts.py`: Prompt templates for LLM actions
- `rag.py`: RAG pipeline for PyPI metadata retrieval
- `pypi_checker.py`: PyPI package existence validation
- `build_error_patterns.py`: Error pattern library (#12)
- `failure_memory.py`: Persistent failure learning
- `active_learning.py`: Confidence-based learning
- `local_detector.py`: Local module detection
- `dspy_optimizer.py`: DSPy optimization experiments

**Fine-tuning (`llm_py/finetune/`):**
- `train.py`: Fine-tuning script for custom models
- `__init__.py`: Module initialization

**Tests (`llm_py/tests/`):**
- `test_recovery_mock.py`: Recovery action unit tests

**`docker_agent/` - Multi-Agent System:**

**Agents (`docker_agent/agents/`):**
- `analyst.py`: Error log analysis agent
- `builder.py`: Docker build execution agent
- `confidence.py`: Solvability assessment agent
- `recovery.py`: Error recovery agent
- `llm_utils.py`: Shared LLM utilities

**Core Modules:**
- `graph.py`: LangGraph state machine definition
- `state.py`: AgentState type definition
- `rag.py`: RAG-enhanced prompt generation

**Prompts (`docker_agent/prompts/`):**
- `templates.py`: Agent prompt templates
- `schemas.py`: Pydantic output schemas

**Tools (`docker_agent/tools/`):**
- `docker_ops.py`: Docker API wrapper
- `import_mapper.py`: Import mapping tool

**Other Files:**
- `requirements.txt`: Python dependencies (LangGraph, LangChain)

**`data/` - Seed Data and Knowledge:**

**Seed Data (`data/seed/`):**
- `top_5000_mappings.tsv`: Curated import → package mappings
- `reference_aliases.tsv`: Package name aliases
- `unsolvable_modules.tsv`: Known unsolvable imports
- `high_centrality_packages.tsv`: Popular packages for cache warming
- `version_constraints.tsv`: Known version requirements
- `failure_patterns.tsv`: Error pattern library

**Stdlib Modules (`data/stdlib_modules/`):**
- `2.6.txt`, `2.7.txt`, `3.x.txt`: Python stdlib module lists by version

**Knowledge Base (`data/knowledge/`):**
- Domain-specific knowledge for heuristics

**`tests/` - Integration Tests:**
- `test_resolver.rs`: Resolver pipeline tests
- `fixtures/`: Test case snippets
  - `cfscrape_snippet.py`: Legacy package test
  - `legacy_flask_stack_snippet.py`: Version conflict test
  - `simplecv_snippet.py`: System dependency test
  - `skip_*_snippet.py`: Platform-specific skip tests
  - `vendor_caffe_snippet.py`: Vendored package test

**`helpers/` - Utility Files:**
- `ref_files/python_versions.json`: Python version metadata
- `ref_files/module_link.json`: Module linking data

**`.apdr-cache/` - Runtime Cache (not committed):**
- `lockfiles/`: Validated lockfile cache
- `wheelhouse/`: Downloaded Python packages
- `validated-envs/`: Reusable Docker layer cache
- `package-repository/`: Full PyPI metadata mirror (optional)
- `knowledge_cache.db`: SQLite cache for PyPI metadata
- `dynamic_import_mappings.tsv`: Runtime-learned mappings
- `dynamic_failure_patterns.tsv`: Runtime-learned errors
- `dynamic_unsolvable_modules.tsv`: Runtime-learned skips
- `import_set_solutions.tsv`: Validated import-set cache

## Key File Locations

**Entry Points:**
- `tools/apdr/src/main.rs`: APDR CLI binary
- `benchmark_ui/__main__.py`: Benchmark UI entry
- `benchmark_ui/server.py`: Flask web server
- `benchmark_ui/cli_app.py`: Terminal UI

**Configuration:**
- `tools/apdr/Cargo.toml`: Rust dependencies and build config
- `tools/apdr/llm_py/requirements.txt`: Python LLM dependencies (not present - relies on system install)
- `tools/apdr/docker_agent/requirements.txt`: Multi-agent dependencies
- `benchmark_ui/requirements.txt`: UI dependencies (implicit - Flask, rich)

**Core Logic:**
- `tools/apdr/src/resolver/mod.rs`: Main resolution pipeline
- `tools/apdr/src/docker/builder.rs`: Validation orchestration
- `tools/apdr/src/cache/store.rs`: Cache management
- `tools/apdr/llm_py/actions/resolve.py`: LLM resolution
- `docker_agent/graph.py`: Multi-agent orchestration

**Testing:**
- `tools/apdr/tests/test_resolver.rs`: Rust integration tests
- `tools/apdr/llm_py/tests/test_recovery_mock.py`: Python unit tests

**Data Files:**
- `tools/apdr/data/seed/*.tsv`: Curated knowledge base
- `tools/apdr/data/stdlib_modules/*.txt`: Standard library catalogs

## Naming Conventions

**Files:**
- Rust modules: `snake_case.rs` (e.g., `tier1_cache.rs`, `pubgrub_solver.rs`)
- Python modules: `snake_case.py` (e.g., `build_error_patterns.py`, `react_agent.py`)
- Test files: `test_*.rs`, `test_*.py` (e.g., `test_resolver.rs`, `test_recovery_mock.py`)
- Data files: `descriptive_name.tsv|txt|json` (e.g., `top_5000_mappings.tsv`)

**Directories:**
- Rust subsystems: `snake_case` (e.g., `src/parser`, `src/resolver`)
- Python packages: `snake_case` (e.g., `llm_py`, `docker_agent`)
- Data directories: `descriptive` (e.g., `seed`, `stdlib_modules`)

**Rust Types:**
- Structs: `PascalCase` (e.g., `ParseResult`, `ResolvedDependency`, `CacheStore`)
- Enums: `PascalCase` (e.g., `ValidationBackend` - represented as string constants)
- Functions: `snake_case` (e.g., `resolve_path`, `parse_snippet`, `classify_log`)
- Constants: `SCREAMING_SNAKE_CASE` (e.g., `VALIDATION_BACKEND_ENV`, `MAX_IMPORT_SET_SOLUTIONS`)

**Python Naming:**
- Classes: `PascalCase` (e.g., `RecoveryResult`, `AgentState`, `LlmClient`)
- Functions: `snake_case` (e.g., `format_error_context`, `package_exists_on_pypi`)
- Constants: `SCREAMING_SNAKE_CASE` (e.g., `EXPLICIT_NAMESPACE_MAPPINGS`)

## Where to Add New Code

**New Resolution Strategy:**
- Primary code: `tools/apdr/src/resolver/` (new tier module or extend existing tier)
- Tests: `tools/apdr/tests/test_resolver.rs`
- Integration: Update `tools/apdr/src/resolver/mod.rs` to call new strategy

**New Error Pattern:**
- Seed data: Add to `tools/apdr/data/seed/failure_patterns.tsv`
- Classifier: Extend `tools/apdr/src/recovery/classifier.rs`
- Recovery: Add LLM prompt case in `tools/apdr/llm_py/actions/recovery.py`

**New LLM Action:**
- Implementation: `tools/apdr/llm_py/actions/<action_name>.py`
- Prompts: Add to `tools/apdr/llm_py/prompts.py`
- Tests: `tools/apdr/llm_py/tests/test_<action_name>.py`
- Integration: Add IPC handler in `tools/apdr/src/resolver/tier3_llm.rs`

**New Multi-Agent Node:**
- Agent: `tools/apdr/docker_agent/agents/<agent_name>.py`
- Graph: Register in `tools/apdr/docker_agent/graph.py`
- State: Extend `AgentState` in `tools/apdr/docker_agent/state.py`
- Prompts: Add template in `tools/apdr/docker_agent/prompts/templates.py`

**New Cache Layer:**
- Implementation: `tools/apdr/src/cache/<cache_name>.rs`
- Store integration: Update `tools/apdr/src/cache/store.rs`
- Maintenance: Update `tools/apdr/src/cache/maintenance.rs` for pruning
- Module export: Add to `tools/apdr/src/cache/mod.rs`

**New Validation Backend:**
- Implementation: `tools/apdr/src/docker/<backend_name>.rs`
- Entry point: Update `tools/apdr/src/docker/builder.rs::validate_solution()`
- Constant: Add to `tools/apdr/src/lib.rs` (e.g., `VALIDATION_BACKEND_X`)
- CLI: Update `tools/apdr/src/main.rs` argument parsing

**New Benchmark Feature:**
- Backend: Add route in `benchmark_ui/server.py`
- Frontend (web): `web/src/` (not present in this scan - likely separate repo)
- CLI: Add screen in `benchmark_ui/cli_app.py`
- State: Update `benchmark_ui/state.py`

## Special Directories

**`.apdr-cache/`:**
- Purpose: Runtime cache for all resolution artifacts
- Generated: On first `apdr resolve` run
- Committed: No (.gitignore)
- Subdirectories:
  - `lockfiles/`: Pinned requirements by hash
  - `wheelhouse/`: Downloaded .whl/.tar.gz packages
  - `validated-envs/`: Docker build layer cache
  - `package-repository/`: Full PyPI mirror (optional, disabled by default)
  - `knowledge_cache.db`: SQLite for PyPI metadata

**`.apdr-debug/` (within output directory):**
- Purpose: Per-resolution debug artifacts
- Generated: For each `apdr resolve` invocation
- Committed: No (ephemeral debugging)
- Subdirectories:
  - `attempts/`: Per-attempt build logs, metadata JSON
  - `iterations/`: LLM iteration traces
  - Files: `benchmark-context.log`, `parse-result.txt`, `resolved-state-*.txt`

**`hard-gists/`:**
- Purpose: Extracted test dataset
- Generated: From `hard-gists.tar.gz` by benchmark UI
- Committed: No (archive committed, extracted files in .gitignore)
- Structure: 2900+ subdirectories (gist IDs), each with `snippet.py`

**`target/` (within tools/apdr/):**
- Purpose: Rust build artifacts
- Generated: By `cargo build`
- Committed: No (standard Rust .gitignore)

**`.planning/`:**
- Purpose: GSD codebase analysis outputs
- Generated: By Claude Code `/gsd:map-codebase`
- Committed: Yes (planning documentation)
- Files: `ARCHITECTURE.md`, `STRUCTURE.md`, etc.

## File Organization Patterns

**Rust Module Pattern:**
- Each subsystem has a `mod.rs` that exports submodules
- Public API defined in `src/lib.rs`
- Binary entry point in `src/main.rs`
- Tests co-located in `tests/` directory (integration) or inline (unit)

**Python Package Pattern:**
- `__init__.py` for package initialization
- `__main__.py` for CLI entry (`python -m package`)
- Subpackages organized by responsibility (`actions/`, `agents/`, `prompts/`, `tools/`)

**Data File Pattern:**
- TSV format for cache files (tab-separated, human-readable)
- JSON for structured metadata (Python versions, module links)
- TXT for simple lists (stdlib modules)
- SQLite for high-performance indexed lookups (PyPI metadata)

**Test Fixture Pattern:**
- Fixtures as standalone `.py` files in `tests/fixtures/`
- Named by test scenario (e.g., `skip_microbit_snippet.py`, `legacy_flask_stack_snippet.py`)
- Tests reference fixtures by path

**Cache Key Pattern:**
- Hash-based keys for content-addressed lookups (lockfiles, build artifacts)
- Normalized keys for import mappings (lowercase, `-` instead of `_`)
- Composite keys with tab separators (Python version, package, version)

---

*Structure analysis: 2026-03-25*
