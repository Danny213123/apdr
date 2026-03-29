---
phase: 15
slug: langchain-langgraph-tier3-intelligence-improvements
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-29
---

# Phase 15 - Validation Strategy

> Validation contract for replay-slice tier3 benchmarking, explicit agent-runtime evaluation, benchmark-fed memory/context engineering, and representative small-model policy proof.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python scripts, structural `rg` checks, Python module compilation, and Rust compile checks for the resolver boundary |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python3 scripts/run_phase15_tier3_benchmark.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --fixtures-root tools/apdr/tests/fixtures --mode baseline --output-json /tmp/phase15-benchmark-probe.json --probe-only` |
| **Full suite command** | `python3 -m py_compile scripts/run_phase15_tier3_benchmark.py scripts/check_phase15_agent_quality.py tools/apdr/llm_py/actions/resolve.py tools/apdr/llm_py/actions/react_agent.py tools/apdr/llm_py/client.py tools/apdr/llm_py/active_learning.py tools/apdr/llm_py/failure_memory.py tools/apdr/llm_py/rag.py tools/apdr/llm_py/prompts.py && cargo build --manifest-path tools/apdr/Cargo.toml && python3 scripts/run_phase15_tier3_benchmark.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --fixtures-root tools/apdr/tests/fixtures --mode baseline --output-json /tmp/phase15-benchmark-probe.json --probe-only` |
| **Estimated runtime** | ~3-12 minutes before live model runs; longer when baseline and candidate replay captures are executed on the benchmark host |

---

## Sampling Rate

- **After every task commit:** Run the task-specific compile, structural, or probe command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** Phase 15 benchmark artifacts, the agent-quality checker, and the Rust compile boundary must all be green
- **Max feedback latency:** keep task-level checks under 5 minutes by preferring `py_compile`, `cargo build`, `rg`, and probe-mode benchmark runs before any live replay capture

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 15-01-01 | 01 | 1 | AGT-03, AGT-05, AGT-06 | benchmark harness compile | `python3 -m py_compile scripts/run_phase15_tier3_benchmark.py` | no | pending |
| 15-01-02 | 01 | 1 | AGT-03, AGT-05, AGT-06 | probe artifact plus contract docs | `python3 scripts/run_phase15_tier3_benchmark.py --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json --fixtures-root tools/apdr/tests/fixtures --mode baseline --output-json /tmp/phase15-benchmark-probe.json --probe-only && rg -n '## Comparison Contract|## Requirement Mapping' .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-TIER3-BENCHMARK.md` | no | pending |
| 15-02-01 | 02 | 2 | AGT-01, AGT-04 | Rust-to-Python boundary compile | `cargo build --manifest-path tools/apdr/Cargo.toml` | yes | pending |
| 15-02-02 | 02 | 2 | AGT-01, AGT-04 | resolver seam compile | `python3 -m py_compile tools/apdr/llm_py/actions/resolve.py tools/apdr/llm_py/actions/react_agent.py tools/apdr/llm_py/models.py tools/apdr/llm_py/client.py` | yes | pending |
| 15-03-01 | 03 | 3 | AGT-02 | memory update path compile | `python3 -m py_compile tools/apdr/llm_py/active_learning.py tools/apdr/llm_py/failure_memory.py scripts/run_phase15_tier3_benchmark.py` | yes | pending |
| 15-03-02 | 03 | 3 | AGT-05 | retrieval and context-folding compile | `python3 -m py_compile tools/apdr/llm_py/rag.py tools/apdr/llm_py/prompts.py tools/apdr/llm_py/actions/resolve.py` | yes | pending |
| 15-04-01 | 04 | 4 | AGT-06 | model-policy surface compile | `python3 -m py_compile tools/apdr/llm_py/client.py scripts/run_phase15_tier3_benchmark.py` | yes | pending |
| 15-04-02 | 04 | 4 | AGT-01, AGT-03, AGT-04, AGT-06 | checker and proof docs | `python3 -m py_compile scripts/check_phase15_agent_quality.py && rg -n '## Policy Variants|## Requirement Mapping' .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-QWEN-POLICY-MATRIX.md && rg -n '## Comparison Verdict|## Requirement Mapping' .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-AGENT-QUALITY.md` | no | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers the phase.
- Phase 15 adds one replay benchmark harness, one checker, and bounded proof artifacts, but it does not need a new test framework.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The Phase 15 proof notes attribute gains to agent behavior, context strategy, and policy changes instead of vague "LLM got better" prose | AGT-03, AGT-05, AGT-06 | Automation can verify headings and artifact presence, but not whether the narrative overclaims causality | Read `15-AGENT-QUALITY.md` and `15-QWEN-POLICY-MATRIX.md`, then confirm each claimed gain is tied to explicit baseline-versus-candidate artifacts and policy labels |
| The phase stays focused on inherent agent intelligence rather than new deterministic rule growth | AGT-01, AGT-02 | Automation can inspect files, but a reviewer still needs to judge whether the implementation drifted back toward hardcoded fix lore | Review the final diffs for `tools/apdr/llm_py/*` and confirm the work centers on agent runtime, memory, retrieval, and policy configuration rather than expanded rule tables |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity covers the replay harness, the Rust boundary, and the final checker/doc package
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded by compile, `rg`, and probe-mode checks before live replay runs
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** planned 2026-03-29
