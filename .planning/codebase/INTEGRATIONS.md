# External Integrations

**Analysis Date:** 2026-03-25

## APIs & External Services

**PyPI (Python Package Index):**
- Service: https://pypi.org
- Purpose: Primary package metadata and download source
- Implementation: HTTP client in `src/resolver/pypi_client.rs`
- Caching: Local PyPI index cache in `src/cache/pypi_index.rs`
- Client: `ureq` 3 (Rust HTTP library)

**LLM Providers (via LiteLLM):**
- Ollama - Local LLM inference (primary/default)
  - Default model: `qwen3.5:9b` (as of v0.2.14)
  - Python client: `ollama` 0.2.0
  - Base URL configurable (default: http://ollama:11434 in Docker)
- OpenAI - Cloud API support via LiteLLM
- Anthropic Claude - Cloud API support via LiteLLM
- Other providers - Any LiteLLM-compatible endpoint
- Purpose: Import resolution, solvability assessment, error recovery, version selection
- Implementation: Python service in `llm_py/` directory

## Data Storage

**Databases:**
- SQLite (embedded via rusqlite)
  - Location: Knowledge graph cache (`kgraph.db`)
  - Connection pooling in `src/resolver/kgraph_db.rs`
  - Purpose: Dependency graph metadata, import mappings
  - Mode: Read-optimized (immutable data, concurrent readers)

**File Storage:**
- Local filesystem
  - Cache directory: `.apdr-cache/` (configurable)
  - Validated environments: `.apdr-cache/validated-envs/` (zstd-compressed archives)
  - Wheelhouse cache: `.apdr-cache/wheelhouse/` (package downloads)
  - Seed data: `data/seed/*.tsv` (curated import→package mappings)
  - Knowledge data: `data/knowledge/*.shrink` (compressed graph data)
  - Build artifacts: `.apdr-debug/` directories per snippet

**Caching:**
- Multi-tier caching system:
  - Import-set cache (validated combinations)
  - Lockfile cache (resolved dependency sets)
  - Build cache (Docker image layers)
  - Package repository cache (optional, opt-in)
  - Failure memory cache (failed package attempts)
- Compression: gzip, tar, zstd for cache artifacts
- Maintenance: Disk usage tracking, configurable retention limits

## Authentication & Identity

**Auth Provider:**
- None required
  - PyPI API is public (no authentication)
  - LLM providers configured via environment variables (API keys if using cloud)
  - Docker socket access via Unix socket (`/var/run/docker.sock`)

## Monitoring & Observability

**Error Tracking:**
- None (standalone tool)
- Build error classification in `src/recovery/classifier.rs`
- Error pattern matching in `llm_py/build_error_patterns.py`
- Failure memory persistence in `llm_py/failure_memory.py`

**Logs:**
- Structured logging to stdout/stderr
- Per-attempt artifact logs:
  - `build.log` - Docker build output
  - `run.log` - Validation execution output
  - `combined.log` - Merged build+run logs
  - `metadata.json` - Attempt metadata
- Benchmark context logs (optional, configurable path)

## CI/CD & Deployment

**Hosting:**
- Local/self-hosted only
- Distributed as source code or compiled binary
- No cloud deployment infrastructure

**CI Pipeline:**
- None detected
- Manual testing via test suites in `tests/` directory

## Environment Configuration

**Required env vars:**
- `USER`, `UID`, `GID`, `DOCKER_GID` - Docker container identity mapping
- `DOCKER_HOST` - Docker daemon socket (default: `unix:///var/run/docker.sock`)

**Optional env vars (for LLM service):**
- LLM provider API keys (if using cloud providers)
- `LITELLM_LOG` - LiteLLM logging level
- Cache paths and retention settings (configurable via CLI flags)

**Secrets location:**
- `.env` files (not committed, see `.env.example`)
- Environment variables in Docker Compose

## Webhooks & Callbacks

**Incoming:**
- None

**Outgoing:**
- None

## Development Tools Integration

**Docker:**
- Service: Docker Engine (local daemon)
- Purpose: Isolated Python environment validation
- Implementation:
  - Rust Docker client in `src/docker/builder.rs`
  - Python Docker SDK (docker 7.1.0) in `docker_agent/`
  - Dockerfile generation in `src/docker/templates.rs`
- Multi-platform: linux/amd64 (primary)

**Vite Dev Server:**
- Purpose: Web UI development and hot reload
- Proxy: `/api` → `http://127.0.0.1:8765` (benchmark server)
- Ports: 4173 (dev/preview)

## Data Sources

**Curated Seed Data (TSV files in `data/seed/`):**
- `top_5000_mappings.tsv` - High-confidence import→package pairs
- `top_level_harvest.tsv` - Harvested package top-level imports
- `reference_aliases.tsv` - Known package name aliases
- `name_discrepancies.tsv` - Import name vs package name mappings
- `pipreqs_mapping.tsv` - pipreqs compatibility mappings
- `dependency_graph.tsv` - Package dependency relationships
- `high_centrality_packages.tsv` - Ecosystem hub packages
- `common_failure_patterns.tsv` - Known build failure patterns
- `pypi_version_index.tsv` - PyPI version availability data
- `unsolvable_modules.tsv` - Platform/OS-specific modules to skip

**Knowledge Graph Data:**
- Pre-computed dependency graphs in `data/knowledge/`
- Compressed shrink format (`.shrink` files)
- Loaded into SQLite for fast querying

## Inter-Process Communication

**Rust ↔ Python (LLM service):**
- Protocol: JSON-line over stdin/stdout
- Request model: `ResolutionRequest` (defined in `llm_py/models.py`)
- Response model: `ResolutionResponse` (JSON-line output)
- Process management: Persistent Python subprocess with connection pooling
- Implementation: `src/resolver/tier3_llm.rs` manages subprocess lifecycle

**Web UI ↔ Benchmark Server:**
- Protocol: HTTP REST API
- Proxy: Vite dev server forwards `/api/*` to backend
- Backend port: 8765 (localhost)

---

*Integration audit: 2026-03-25*
