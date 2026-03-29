# Architecture Research

**Domain:** APDR architecture for agent-quality tuning and macOS benchmark performance
**Researched:** 2026-03-28
**Confidence:** HIGH

## Standard Architecture

### System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                    Measurement Layer                        │
├─────────────────────────────────────────────────────────────┤
│  Benchmark UI / CLI   Run metadata   Per-stage timings      │
│  Saved run summaries  Benchmark slices  Proof artifacts      │
├─────────────────────────────────────────────────────────────┤
│                     Execution Layer                         │
├─────────────────────────────────────────────────────────────┤
│  Native APDR Rust CLI   Env validator   Docker fallback     │
│  Python LLM service     Ollama API      Benchmark runner    │
├─────────────────────────────────────────────────────────────┤
│                    State / Cache Layer                      │
│  .apdr-cache wheelhouse  validated-envs  llm-cache          │
│  failure memory         run artifacts    loadouts           │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|------------------------|
| Benchmark control plane | Start runs, save run metadata, present summaries | `benchmark_ui/` with explicit backend and architecture state |
| APDR native CLI | Perform resolution and validation work | `tools/apdr` release or benchmark-tuned profile |
| LLM agent service | Candidate generation, critique, tool use, memory lookups | `tools/apdr/llm_py` talking to local Ollama |
| Env validator | Fast macOS inner-loop validation | Existing env backend with `uv`, wheelhouse, and validated-env reuse |
| Docker proof path | Linux-style validation for selected cases | Docker Desktop with VMM/file-sharing settings surfaced explicitly |
| Artifact pipeline | Keep benchmark outputs comparable | `runs/`, saved summaries, delta reports, and milestone docs |

## Recommended Project Structure

```text
benchmark_ui/
├── service.py          # run orchestration and saved-run management
├── runner.py           # benchmark execution control
└── state.py            # environment, doctor, and platform detection

tools/apdr/
├── src/                # native resolver and validation backends
├── llm_py/             # LLM client, prompts, tool loops, memory
├── build.sh            # plain release build
└── build-pgo.sh        # benchmark-trained binary build

.planning/
├── research/           # milestone research set
├── phases/             # roadmap execution artifacts
└── milestones/         # archived milestone state

runs/
└── <run-id>/           # saved benchmark summaries and case artifacts
```

### Structure Rationale

- **`benchmark_ui/`:** the right place for arch detection, backend-intent modes, and saved-run metadata because it already owns benchmark orchestration.
- **`tools/apdr/src/`:** the right place for runtime performance work, env fast paths, and benchmark-tuned build profiles.
- **`tools/apdr/llm_py/`:** the right place for agent-quality changes because it already owns LLM calls, prompting, and tool hooks.
- **`runs/`:** the right place for proof artifacts because the milestone needs comparable saved evidence, not transient logs.

## Architectural Patterns

### Pattern 1: Two-Lane Benchmark Execution

**What:** Separate macOS runs into a fast env lane and a slower Docker proof lane.
**When to use:** Whenever iteration speed and Linux-style proof have different costs.
**Trade-offs:** Faster local iteration and clearer attribution, but the UI and run schema need one more concept.

**Example:**
```text
intent=env-fast     -> native arm64 APDR + env backend + canonical slice
intent=docker-proof -> selected cases + Docker Desktop + parity artifacts
```

### Pattern 2: Architecture-Aware Run Contract

**What:** Save host and backend metadata with every benchmark run.
**When to use:** Always, for any macOS performance claim.
**Trade-offs:** Slightly larger summaries, much higher evidence quality.

**Example:**
```json
{
  "host_arch": "arm64",
  "apdr_binary_arch": "arm64",
  "python_arch": "arm64",
  "validation_backend": "env",
  "run_intent": "env-fast",
  "cache_state": "warm"
}
```

### Pattern 3: Agentic Tier3 Loop

**What:** Let the LLM alternate between structured reasoning, tool use, and benchmark-informed reflection.
**When to use:** For tier3 cases where one-shot JSON completion is not enough.
**Trade-offs:** More system complexity than a single completion call, but far better alignment with the user's "general intelligence" direction.

**Example:**
```text
draft -> tool calls -> verified candidates -> critique/refine -> final mapping
                                         \-> memory update from outcome
```

## Data Flow

### Request Flow

```text
User starts benchmark
    ↓
Benchmark runner stamps host metadata and run intent
    ↓
APDR native CLI resolves case
    ↓
Env backend or Docker proof backend validates result
    ↓
LLM service handles tier3 tool loop when needed
    ↓
Per-stage timings and artifacts are written
    ↓
Saved run summary and comparison views are updated
```

### State Management

```text
Loadout / run config
    ↓
Benchmark service
    ↓
APDR + LLM + validator outputs
    ↓
Saved run summary / milestone proof docs
```

### Key Data Flows

1. **macOS inner loop:** canonical slice -> native env backend -> stage timings -> compare against previous slice run.
2. **Agent-learning loop:** tier3 failure -> tool loop -> measured outcome -> memory / evidence update.
3. **Proof loop:** promising change -> broader rerun -> stable summary -> milestone artifact.

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| 0-100 benchmark cases | Single-machine saved runs are enough; keep artifacts local. |
| 100-1,000 benchmark cases | Use locked slices plus promoted reruns; avoid full-corpus runs for every change. |
| 1,000+ benchmark cases | Separate fast qualification runs from final proof runs and rely on run metadata to keep comparisons sane. |

### Scaling Priorities

1. **First bottleneck:** macOS validation and Docker/file-system overhead — address with env-fast lane, Docker tuning, and stage timings.
2. **Second bottleneck:** tier3 reasoning quality — address with tool calling, reflection memory, and better benchmark feedback loops.

## Anti-Patterns

### Anti-Pattern 1: One Benchmark Mode for Everything

**What people do:** Run the same backend and same corpus for every experiment.
**Why it's wrong:** It makes macOS iteration too slow and hides where time is actually going.
**Do this instead:** Separate fast local qualification from slower parity-proof runs.

### Anti-Pattern 2: Performance Claims Without Host Metadata

**What people do:** Compare total runtime numbers across runs with different architecture, cache, and backend state.
**Why it's wrong:** The results are not attributable, so optimizations become superstition.
**Do this instead:** Record architecture, backend, cache state, and stage timings with every run.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Ollama | Local HTTP API for structured outputs and tool calling | Keep the provider stable while improving agent behavior. |
| Docker Desktop | Optional proof backend on macOS | Surface VMM and file-sharing constraints in Doctor and saved runs. |
| Xcode Instruments | Manual and scripted profiling aid | Use before rewriting runtime paths or making strong macOS performance claims. |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| `benchmark_ui` <-> `tools/apdr` | subprocess / saved artifacts | Good place to stamp run metadata before execution starts. |
| `tools/apdr` <-> `llm_py` | long-lived JSON-line subprocess | Already exists; v2.2 should enrich behavior, not replace transport. |
| env backend <-> Docker backend | validation backend selection | Needs an explicit intent model so macOS runs pick the right lane. |

## Sources

- Apple Developer: https://developer.apple.com/documentation/xcode/performance-and-metrics
- Apple Support: https://support.apple.com/en-us/102527
- Docker Desktop settings on Mac: https://docs.docker.com/desktop/settings-and-maintenance/settings/
- Docker VMM: https://docs.docker.com/desktop/features/vmm/
- Docker build cache optimization: https://docs.docker.com/build/cache/optimize/
- uv overview: https://docs.astral.sh/uv/
- Cargo build timings: https://doc.rust-lang.org/cargo/reference/timings.html
- Cargo profiles: https://doc.rust-lang.org/cargo/reference/profiles.html
- rustc PGO: https://doc.rust-lang.org/rustc/profile-guided-optimization.html
- Ollama structured outputs: https://docs.ollama.com/capabilities/structured-outputs
- Ollama tool calling: https://docs.ollama.com/capabilities/tool-calling
- ReAct: https://arxiv.org/abs/2210.03629
- Reflexion: https://arxiv.org/abs/2303.11366

---
*Architecture research for: APDR architecture for agent-quality tuning and macOS benchmark performance*
*Researched: 2026-03-28*
