# Technology Stack

**Analysis Date:** 2026-03-25

## Languages

**Primary:**
- Rust (2021 edition) - Core APDR resolver implementation
- Python 3.11 - LLM service and benchmark utilities

**Secondary:**
- JavaScript (ES modules) - Web UI for benchmark control plane
- Shell/Bash - Build scripts and Docker entrypoints

## Runtime

**Environment:**
- Rust 2021 edition (compiled native binary)
- Python 3.11 (for LLM service and Docker agent)
- Node.js (for web UI build tooling)

**Package Manager:**
- Cargo (Rust) - `Cargo.toml`, `Cargo.lock` present at `/d/apdr/tools/apdr/`
- pip (Python) - `requirements.txt` files in `llm_py/` and `docker_agent/`
- npm (JavaScript) - `package.json`, `package-lock.json` in `/d/apdr/web/`

## Frameworks

**Core:**
- None (vanilla Rust standard library for main resolver)
- LiteLLM >=1.40 - Multi-provider LLM gateway for Python service
- Instructor >=1.3 - Structured LLM output extraction
- Pydantic >=2.5 - Python data validation and models

**Testing:**
- Built-in Rust test framework - Tests in `/d/apdr/tools/apdr/tests/`
- Python unittest/pytest implied - Tests in `/d/apdr/tools/apdr/llm_py/tests/`

**Build/Dev:**
- Cargo (Rust build system)
- Vite 6.2.0 - Frontend build tool for web UI
- Docker Compose - Development environment orchestration

## Key Dependencies

**Critical Rust Dependencies:**
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

**Critical Python Dependencies:**
- `litellm` >=1.40 - Unified LLM API (supports Ollama, OpenAI, Anthropic, etc.)
- `instructor` >=1.3 - Constrained LLM output with Pydantic schemas
- `pydantic` >=2.5 - Type validation and structured data models
- `requests` >=2.28 - HTTP client for API calls

**Optional Python Dependencies (commented in requirements.txt):**
- `langgraph` >=0.2 - ReAct agent orchestration (feature #10)
- `langchain-core` >=0.3, `langchain-community` >=0.3 - Agent tooling
- `dspy` >=2.5 - Prompt optimization framework (feature #5)
- `unsloth`, `trl` >=0.7, `transformers` >=4.40, `datasets` >=2.18 - LoRA fine-tuning (feature #9)

**Infrastructure:**
- `docker` 7.1.0 (Python) - Docker SDK for container management
- `ollama` 0.2.0 (Python) - Ollama Python client

## Configuration

**Environment:**
- Environment variables configured via `.env` files (examples in `.env.example`)
- Required vars: `USER`, `UID`, `GID`, `DOCKER_GID` for Docker container setup
- LLM configuration via Python environment (provider, model, base URL)

**Build:**
- `Cargo.toml` - Rust package manifest and release optimization settings
  - LTO: fat
  - codegen-units: 1
  - opt-level: 3
  - strip: symbols
  - panic: abort
- `vite.config.js` - Frontend dev server and API proxy configuration
- `docker-compose.yml` - Multi-container orchestration (pllm service + Ollama)

## Platform Requirements

**Development:**
- Rust toolchain (2021 edition)
- Python 3.11+
- Node.js (for web UI)
- Docker and Docker Compose
- Ollama (for LLM inference)

**Production:**
- Compiled Rust binary (platform-specific)
- Docker runtime for validation environments
- Access to PyPI (pypi.org)
- LLM provider (Ollama local or cloud API)

---

*Stack analysis: 2026-03-25*
