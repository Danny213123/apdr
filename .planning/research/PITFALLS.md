# Pitfalls Research

**Domain:** APDR agent-quality and macOS benchmark optimization
**Researched:** 2026-03-28
**Confidence:** HIGH

## Critical Pitfalls

### Pitfall 1: Benchmarking Under Rosetta or Mixed Architectures

**What goes wrong:**
macOS runs look slower or noisier than expected because APDR, Python, Docker-side components, or helper tools are running as Intel binaries on Apple Silicon.

**Why it happens:**
Rosetta is easy to forget when a tool "just works," and benchmark summaries usually do not record architecture state.

**How to avoid:**
Record native-vs-Rosetta metadata on every run, fail Doctor loudly for Intel-only hot-path tools, and prefer native Apple silicon builds by default.

**Warning signs:**
- The machine is Apple Silicon, but one or more benchmark binaries report `x86_64`.
- Performance changes do not line up with code changes.
- The same benchmark slice has large unexplained variance across runs.

**Phase to address:**
Phase 13 - measurement and run-contract hardening

---

### Pitfall 2: Treating Docker File Sharing as Free on macOS

**What goes wrong:**
Docker-based proof runs are dominated by file-sharing and VM overhead instead of APDR behavior.

**Why it happens:**
Docker Desktop on macOS runs Linux in a VM, and Docker documents real overhead from broad host file sharing.

**How to avoid:**
Use env validation for the inner loop, keep Docker for proof runs, enable VirtioFS, share only the directories you need, and move mutable caches into Docker-managed volumes when possible.

**Warning signs:**
- High CPU during otherwise simple container work.
- Docker runs stay slow even when benchmark cases are trivial.
- Shared home-directory mounts or giant bind-mounted cache trees.

**Phase to address:**
Phase 14 - macOS execution-path optimization

---

### Pitfall 3: Comparing Warm and Cold Runs as If They Were Equivalent

**What goes wrong:**
Benchmark results are not reproducible because cache warmth, Docker VM state, and model warm-up state are mixed together.

**Why it happens:**
Saved runs often preserve totals but not enough context about what was already hot.

**How to avoid:**
Persist explicit warm/cold labels, capture cache and model state in summaries, and compare like with like.

**Warning signs:**
- The first run is dramatically slower than the second with no code changes.
- LLM latency drops sharply after the first few cases.
- Docker proof runs pay startup cost unpredictably.

**Phase to address:**
Phase 13 - measurement and run-contract hardening

---

### Pitfall 4: Chasing LLM Pass Rate With More Deterministic Fix Tables

**What goes wrong:**
The apparent success rate rises on touched cases, but the resolver becomes more brittle and does not generalize.

**Why it happens:**
Hardcoded rules feel faster than improving the agent loop, especially when benchmark failures are repetitive.

**How to avoid:**
Keep deterministic rules for truly stable invariants only, and move most recovery effort into tool calling, reflection, verification, and benchmark-fed memory.

**Warning signs:**
- New recovery logic mostly lands in static alias or rule tables.
- Success improves only on recently touched cases.
- The system cannot explain why a new case should benefit.

**Phase to address:**
Phase 15 - agentic tier3 intelligence improvements

---

### Pitfall 5: Measuring Only Total Runtime

**What goes wrong:**
You know a run is slow, but not whether the bottleneck is Rust resolution, Python env creation, pip install, Docker startup, LLM inference, or file-system overhead.

**Why it happens:**
A single number is easy to display, and stage timing is annoying to thread through a benchmark pipeline.

**How to avoid:**
Record per-stage timings in the APDR path and persist them in saved run summaries.

**Warning signs:**
- Regressions get argued about instead of measured.
- "macOS is slow" is the main diagnosis.
- Different fixes are attempted without stage-level evidence.

**Phase to address:**
Phase 13 - measurement and run-contract hardening

---

### Pitfall 6: Optimizing the Wrong Layer

**What goes wrong:**
Time is spent on Rust build tuning or UI polish when the dominant bottleneck is env creation, Docker bind mounts, or tier3 reasoning.

**Why it happens:**
The most visible code is not always the hottest code.

**How to avoid:**
Profile first with Instruments, stage timings, Cargo timings, and saved benchmark evidence before making structural changes.

**Warning signs:**
- Compile-time wins are reported, but benchmark runtime does not improve.
- The biggest wall-clock phases are not the ones being changed.
- Broad refactors happen without a before/after benchmark slice.

**Phase to address:**
Phase 14 - macOS execution-path optimization

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Add another deterministic recovery table | Quick win on a narrow case family | Maintenance debt and poor generalization | Only for stable invariants that are not really "intelligence" problems |
| Keep one generic benchmark mode | Fewer UI controls | Slower iteration and muddy evidence | Never for this milestone |
| Store only total runtime | Simpler summaries | No attribution, no trustworthy macOS proof | Never for this milestone |
| Use Docker for all macOS validation | Closer to Linux by default | Slower local inner loop | Only for final proof runs or clearly Docker-only cases |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Docker Desktop VMM | Assume Docker VMM and Rosetta can be used together freely | Docker documents that Rosetta is not supported under Docker VMM; choose the mode intentionally |
| Docker file sharing | Bind mount broad host trees including caches and datasets | Share only the minimal source tree and keep mutable caches inside the Linux VM or named volumes |
| Ollama | Treat cold-start and model residency as invisible | Record model warm state and use keep-alive / warm-up intentionally |
| Instruments | Start rewriting without profiling | Use Instruments to identify CPU, disk, and memory hot spots first |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Broad Docker bind mounts on macOS | High CPU, slow filesystem, noisy proof runs | Use VirtioFS, minimal shared dirs, named volumes for caches | Immediately on medium-to-large repos |
| Whole-corpus reruns for prompt tweaks | Very slow local loop, little learning | Use a canonical slice and promote only promising changes | As soon as runs take more than a few minutes |
| No arch/back-end metadata in saved runs | Unreliable comparisons | Save host arch, binary arch, backend, cache state, and run intent | Immediately |
| Plain release build forever | Runtime improvements plateau | Compare release, tuned release, and PGO on the real workload | When Rust runtime is a meaningful portion of the run |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Logging secrets or private paths in benchmark artifacts | Sensitive data leaks into saved runs | Sanitize logs and keep benchmark context payloads focused |
| Exposing local LLM endpoints beyond intended scope | Unnecessary local attack surface | Keep Ollama local unless there is a deliberate networking requirement |
| Over-sharing host directories to Docker | Containers can see more host data than needed | Share only required project directories |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Hidden arch/backend state | Users trust invalid comparisons | Surface arch, backend, and cache labels directly in the run summary |
| One giant "performance" score | Users cannot tell what improved | Show stage-level deltas and proof notes |
| No distinction between tuning and proof runs | Users run the slow path too often | Make run intent explicit |

## "Looks Done But Isn't" Checklist

- [ ] **macOS benchmark performance:** Often missing architecture metadata - verify each run records native vs Rosetta state.
- [ ] **Docker optimization:** Often missing file-sharing and VMM details - verify Docker settings are captured in proof runs.
- [ ] **LLM improvement:** Often missing a locked replay slice - verify agent changes are validated on a stable case set first.
- [ ] **Performance claim:** Often missing per-stage timings - verify totals are backed by stage breakdowns.
- [ ] **General intelligence claim:** Often missing reduction in deterministic fixes - verify new wins come from agent behavior, not rule growth.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Mixed-arch benchmarking | MEDIUM | Add architecture detection, invalidate old comparisons, rerun the canonical slice |
| Docker file-sharing overhead | MEDIUM | Reduce shared dirs, move caches to volumes, switch inner loop to env backend |
| Warm/cold state confusion | LOW | Add cache-state metadata and rerun the comparison with explicit labels |
| Deterministic-rule sprawl | HIGH | Revert strategy drift, isolate true invariants, move effort into the agent loop |
| Missing stage timings | MEDIUM | Thread timing fields through APDR and regenerate saved summaries |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Mixed architectures | Phase 13 | Saved runs show host arch and binary arch clearly |
| Docker file-sharing overhead | Phase 14 | macOS proof runs show improved stage timings or reduced Docker overhead |
| Warm/cold confusion | Phase 13 | Every comparison run carries cache-state metadata |
| Deterministic-rule sprawl | Phase 15 | New wins trace to tool use, critique, or memory rather than new static tables |
| Total-runtime-only measurement | Phase 13 | Run summaries show stage-level timing fields |
| Wrong-layer optimization | Phase 14 | Before/after evidence ties code changes to the stage that improved |

## Sources

- Apple Developer: https://developer.apple.com/documentation/xcode/performance-and-metrics
- Apple Support: https://support.apple.com/en-us/102527
- Docker Desktop settings on Mac: https://docs.docker.com/desktop/settings-and-maintenance/settings/
- Docker VMM: https://docs.docker.com/desktop/features/vmm/
- Docker build cache optimization: https://docs.docker.com/build/cache/optimize/
- uv overview: https://docs.astral.sh/uv/
- Cargo build timings: https://doc.rust-lang.org/cargo/reference/timings.html
- rustc PGO: https://doc.rust-lang.org/rustc/profile-guided-optimization.html
- Ollama tool calling: https://docs.ollama.com/capabilities/tool-calling
- ReAct: https://arxiv.org/abs/2210.03629
- Reflexion: https://arxiv.org/abs/2303.11366

---
*Pitfalls research for: APDR agent-quality and macOS benchmark optimization*
*Researched: 2026-03-28*
