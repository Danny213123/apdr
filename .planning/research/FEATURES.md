# Feature Research

**Domain:** APDR benchmark optimization and agent-quality tuning on macOS
**Researched:** 2026-03-28
**Confidence:** HIGH

## Feature Landscape

### Table Stakes (Users Expect These)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Native-arch run metadata | macOS performance claims are not credible if arm64 vs Rosetta is hidden | LOW | Every saved run should record host arch, APDR binary arch, Python arch, and whether any component is using Rosetta. |
| Per-stage benchmark timings | "The run was slow" is too vague to fix | MEDIUM | Break timings out into resolve, LLM, env create, install, smoke/run, Docker startup, and artifact write phases. |
| Warm-vs-cold cache distinction | macOS benchmark variance is dominated by cache state | MEDIUM | Saved runs need explicit cache mode labels or comparisons become noisy. |
| Fast canonical slice / replay mode | Inner-loop tuning dies if every experiment reruns the whole corpus | MEDIUM | Keep a locked benchmark slice for LLM and macOS performance experiments. |
| Backend-intent modes | macOS users need one fast local mode and one parity-verification mode | MEDIUM | Make `env-fast` and `docker-proof` first-class run intents, not hidden combinations of checkboxes. |
| Native binary / tooling guardrails | Users expect the benchmark tool to tell them when they are on a slow path | LOW | Doctor should flag Rosetta, Intel-only Python, legacy Docker settings, and missing `uv`. |
| Stable benchmark evidence | Users need to compare runs without reading raw logs | MEDIUM | Save comparable summaries, not just ad hoc terminal output. |

### Differentiators (Competitive Advantage)

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Tool-calling tier3 agent loop | Moves APDR toward genuine agentic recovery instead of one-shot guessing | HIGH | Aligns with the user's "no more hardcoded/deterministic fixes" constraint. |
| Reflection memory from benchmark feedback | Turns failed or successful cases into better next-run behavior | HIGH | Benchmark feedback becomes agent training signal without fine-tuning weights. |
| macOS performance proof pack | Makes runtime claims reviewer-readable | MEDIUM | Include architecture, backend, Docker settings, warm/cold state, and per-stage deltas. |
| Model and build-profile matrix | Lets APDR compare reasoning quality and runtime cost in the same workflow | HIGH | Needed to decide whether wins come from prompting, model choice, build tuning, or backend choice. |
| Failure-slice promotion pipeline | Fast inner-loop experiments can graduate into wider benchmark reruns safely | MEDIUM | Keeps local tuning practical without losing milestone proof quality. |

### Anti-Features (Commonly Requested, Often Problematic)

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Bigger deterministic fix tables | Looks like a fast way to raise pass count | Does not improve generalization and grows maintenance debt | Use tool calling, reflection, and benchmark-driven memory instead. |
| Full-corpus reruns for every local tweak | Feels "more correct" | Kills iteration speed on macOS and hides signal in noise | Use a locked slice for tuning, then promote promising changes. |
| Docker for every macOS run | Feels closer to Linux truth | Docker Desktop adds VM and file-sharing overhead on macOS | Default to env for inner loop, reserve Docker for proof runs. |
| One total runtime number | Easy to display | Impossible to attribute regressions | Record stage-level timings and host metadata. |
| Provider rewrite before behavior rewrite | Feels like a reset button | Changes too many variables at once | First improve the current agent loop and benchmarking discipline. |

## Feature Dependencies

```
Native-arch run metadata
    └──supports──> Stable benchmark evidence
                        └──supports──> macOS performance proof pack

Fast canonical slice / replay mode
    └──supports──> Tool-calling tier3 agent loop
                        └──supports──> Reflection memory from benchmark feedback

Backend-intent modes
    └──supports──> Per-stage benchmark timings
                        └──supports──> Model and build-profile matrix
```

### Dependency Notes

- **Stable benchmark evidence requires native-arch metadata:** otherwise macOS performance comparisons mix arm64, Rosetta, and Docker overhead invisibly.
- **Tool-calling agent work requires a fast replay slice:** otherwise each reasoning experiment is too expensive to validate.
- **Model/build-profile comparisons require backend-intent modes:** otherwise improvements can be caused by backend drift instead of the change being tested.

## MVP Definition

### Launch With (v1)

- [ ] Native-arch run metadata — makes macOS performance evidence trustworthy.
- [ ] Per-stage benchmark timings — gives a direct bottleneck map.
- [ ] Fast canonical slice / replay mode — keeps local iteration practical.
- [ ] Backend-intent modes — separates fast env tuning from Docker proof runs.
- [ ] Tool-calling tier3 agent loop — primary path for raising LLM quality without new hardcoded fixes.
- [ ] Stable benchmark evidence — allows milestone-level comparison.

### Add After Validation (v1.x)

- [ ] Reflection memory from benchmark feedback — add once the tool loop is stable enough to benefit from remembered outcomes.
- [ ] macOS performance proof pack — add once per-stage timings and metadata exist.
- [ ] Model and build-profile matrix — add after the inner loop can compare runs cheaply.

### Future Consideration (v2+)

- [ ] Automatic benchmark-slice mining from saved runs — useful after the milestone proves the smaller slice workflow.
- [ ] Cross-machine normalization for macOS benchmark results — only worth it once local measurement is disciplined.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Native-arch run metadata | HIGH | LOW | P1 |
| Per-stage benchmark timings | HIGH | MEDIUM | P1 |
| Fast canonical slice / replay mode | HIGH | MEDIUM | P1 |
| Backend-intent modes | HIGH | MEDIUM | P1 |
| Tool-calling tier3 agent loop | HIGH | HIGH | P1 |
| Stable benchmark evidence | HIGH | MEDIUM | P1 |
| Reflection memory from benchmark feedback | HIGH | HIGH | P2 |
| macOS performance proof pack | MEDIUM | MEDIUM | P2 |
| Model and build-profile matrix | MEDIUM | HIGH | P2 |

## Competitor Feature Analysis

| Feature | Generic benchmark tools | Generic agent loops | Our Approach |
|---------|-------------------------|---------------------|--------------|
| macOS stage timings | Usually total runtime only | Usually not benchmark-oriented | Tie stage timings to APDR's resolution and validation phases. |
| Native-arch metadata | Rarely emphasized | Rarely emphasized | Make architecture and backend part of the saved-run contract. |
| Tool-calling resolver | N/A | Often demo-quality, not benchmark-grounded | Bind tool calling to APDR's real resolution tools and outcome metrics. |
| Reflection memory | N/A | Often abstract or free-form | Feed memory from benchmark outcomes and resolved case evidence. |
| Env-fast vs Docker-proof modes | Rarely explicit | N/A | Separate inner-loop speed from final proof runs. |

## Sources

- Apple Developer: https://developer.apple.com/documentation/xcode/performance-and-metrics
- Apple Support: https://support.apple.com/en-us/102527
- Docker Desktop settings on Mac: https://docs.docker.com/desktop/settings-and-maintenance/settings/
- Docker VMM: https://docs.docker.com/desktop/features/vmm/
- Docker build cache optimization: https://docs.docker.com/build/cache/optimize/
- uv overview: https://docs.astral.sh/uv/
- Ollama structured outputs: https://docs.ollama.com/capabilities/structured-outputs
- Ollama tool calling: https://docs.ollama.com/capabilities/tool-calling
- ReAct: https://arxiv.org/abs/2210.03629
- Reflexion: https://arxiv.org/abs/2303.11366

---
*Feature research for: APDR benchmark optimization and agent-quality tuning on macOS*
*Researched: 2026-03-28*
