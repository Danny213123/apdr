# Stack Research

**Domain:** APDR LLM-agent quality and macOS benchmark performance
**Researched:** 2026-03-28
**Confidence:** HIGH

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Native Apple silicon toolchain | Current arm64 macOS toolchain | Run APDR, Python, and supporting tools without Rosetta overhead | Apple says native Apple silicon builds are preferred for optimal performance and future compatibility. |
| Xcode Instruments | Current Xcode | Measure CPU, memory, disk, and responsiveness on macOS | Apple recommends Instruments as the main measurement loop for investigating performance and resource use. |
| `uv` | Current | Fast env creation, package install, and Python version management | uv is documented as 10-100x faster than `pip`, with a global cache and a pip-compatible interface. |
| Docker Desktop on macOS | 4.35+ on Apple Silicon when available | Linux-parity validation fallback for benchmark cases that cannot stay on the env backend | Docker VMM and VirtioFS are the current Docker-supported performance path on Apple Silicon Macs. |
| Rust release profiles + PGO | Stable Cargo profiles, `rustc` PGO workflow | Improve APDR CLI runtime on the real benchmark workload | Rust officially supports PGO, ThinLTO, and codegen tuning for runtime-focused builds. |
| Ollama local API | Current | Local tool-calling and structured-output inference for APDR's LLM path | Ollama officially supports structured outputs, tool calling, and multi-turn agent loops without changing providers. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| Docker BuildKit cache mounts | Current Docker BuildKit | Speed Docker-based validation by persisting package caches and shrinking rebuild work | Use when Docker remains part of the macOS verification loop. |
| Cargo `--timings` | Current Cargo | Measure Rust build hot spots and compile-time contention | Use when local build latency is part of the benchmark iteration problem. |
| Cargo profile tuning (`lto`, `codegen-units`, `strip`) | Current Cargo/rustc | Trade build time for faster benchmark binaries when needed | Use for benchmark-only profiles, not as an unconditional dev default. |
| Ollama structured outputs | Current Ollama | Enforce schema-safe LLM responses for candidate generation and critiques | Use for all machine-consumed resolver steps. |
| Ollama tool calling | Current Ollama | Let the model invoke search/verification tools in an explicit agent loop | Use for tier3 resolution flows that need reasoning plus action, not a single-shot guess. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| Instruments / Xcode | Profile APDR on macOS | Use Time Profiler and disk-oriented traces before changing benchmark architecture. |
| `cargo build --timings` | Measure Rust compile bottlenecks | Writes an HTML report under `target/cargo-timings/`. |
| `./build-pgo.sh` | Build a benchmark-trained APDR binary | The repo already contains a PGO build script that collects workload profiles before rebuilding. |
| Benchmark loadouts in `benchmark_ui/` | Make macOS benchmark runs repeatable | Extend loadouts with arch, backend, and cache-state metadata instead of relying on memory. |

## Existing Repo Leverage

The repo already contains useful macOS-oriented building blocks:

- `tools/apdr/src/docker/builder/env_backend.rs` already prefers `uv venv` and `uv pip install` before slower fallbacks.
- `tools/apdr/src/docker/builder/env_backend.rs` already keeps a `.hot` validated-env copy on macOS and attempts CoW clone reuse on APFS.
- `tools/apdr/build-pgo.sh` already implements a Rust PGO workflow.
- `benchmark_ui/state.py` already contains macOS-specific Docker Desktop startup logic, so Docker settings can be surfaced in Doctor instead of remaining tribal knowledge.

That means v2.2 should focus on stronger measurement, better defaults, and fewer slow-path fallbacks, not a ground-up tooling rewrite.

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| Native arm64 APDR + Python + Ollama | Rosetta / Intel binaries on Apple Silicon | Only when an unavoidable Intel-only dependency still blocks migration. |
| Env backend for inner-loop macOS runs | Docker backend for every macOS run | Only use Docker as the default when Linux parity is the primary goal of that run. |
| Docker VMM + VirtioFS | Apple Virtualization + legacy file-sharing modes | Use Apple Virtualization when Docker VMM limitations block a needed amd64/Rosetta workflow. |
| PGO / targeted release tuning | Plain `cargo build --release` forever | Use plain release when measurement shows APDR runtime is not the bottleneck. |
| Tool-calling LLM agent | More deterministic recovery tables | Use deterministic rules only for truly stable invariants, not as the main path to higher LLM success. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| Docker as the default macOS inner-loop backend | Docker Desktop bind mounts and VM startup add avoidable macOS overhead | Use env validation for fast iteration, then rerun selected cases with Docker for parity proof. |
| Rosetta-first benchmark execution on Apple Silicon | Native performance and future compatibility both get worse | Require native Apple silicon builds wherever possible and record exceptions explicitly. |
| Whole-corpus reruns for every prompt tweak | Local iteration becomes too slow to learn from failures | Use a locked failure slice, then promote to broader benchmark passes. |
| More hardcoded recovery tables as the main intelligence strategy | This raises maintenance cost and does not improve generalization | Use tool calling, self-critique, episodic memory, and benchmark-driven feedback loops. |
| Large host bind mounts for caches in Docker | Docker documents host file sharing overhead on macOS | Keep mutable caches in Docker volumes or inside the Linux VM where possible. |

## Stack Patterns by Variant

**If the goal is fast local iteration on macOS:**

- Use the env backend first.
- Keep APDR, Python, and Ollama native on arm64.
- Benchmark on the canonical failure slice, not the whole corpus.
- Capture per-stage timings so slowdowns are attributable.

**If the goal is Linux-parity proof on macOS:**

- Use Docker Desktop with Docker VMM on Apple Silicon if compatible.
- Keep VirtioFS enabled.
- Share only the minimal source directory into containers.
- Move caches and databases into Docker-managed volumes when possible.

**If the goal is APDR runtime speed itself:**

- Compare plain release vs PGO vs tuned release profiles.
- Use Cargo timings for compile-time cost and Instruments for runtime cost.
- Keep benchmark binaries separate from default dev builds.

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|-----------------|-------|
| Docker VMM | Docker Desktop 4.35+ on Apple Silicon | Docker documents this as the most performant VMM path on Apple Silicon. |
| VirtioFS | Docker Desktop on macOS 12.5+ | Docker says it is the only file sharing implementation supported by Docker VMM. |
| Docker VMM | No Rosetta support | Docker documents that amd64 emulation is slow under Docker VMM because Rosetta is not supported there. |
| uv | Existing APDR env backend | The repo already calls `uv venv` and `uv pip install`, so broader adoption is low-risk. |
| Rust PGO | `llvm-profdata` via `llvm-tools-preview` | Required to merge `.profraw` files into `.profdata`. |

## Sources

- Apple Developer: https://developer.apple.com/documentation/xcode/performance-and-metrics
- Apple Support: https://support.apple.com/en-us/102527
- Docker Desktop settings on Mac: https://docs.docker.com/desktop/settings-and-maintenance/settings/
- Docker VMM: https://docs.docker.com/desktop/features/vmm/
- Docker build cache optimization: https://docs.docker.com/build/cache/optimize/
- uv overview: https://docs.astral.sh/uv/
- uv pip interface: https://docs.astral.sh/uv/pip/
- Cargo profiles: https://doc.rust-lang.org/cargo/reference/profiles.html
- Cargo build timings: https://doc.rust-lang.org/cargo/reference/timings.html
- rustc PGO: https://doc.rust-lang.org/rustc/profile-guided-optimization.html
- rustc codegen options: https://doc.rust-lang.org/rustc/codegen-options/index.html
- Ollama structured outputs: https://docs.ollama.com/capabilities/structured-outputs
- Ollama tool calling: https://docs.ollama.com/capabilities/tool-calling

---
*Stack research for: APDR LLM-agent quality and macOS benchmark performance*
*Researched: 2026-03-28*
