# Phase 1 Hotspot Audit

This audit ranks the first Rust optimization targets for the v2.0 modernization milestone using Phase 1 baseline evidence plus static code signals.

## Phase 1 Evidence Snapshot

- Baseline sample: 3 deterministic fixture snippets
- Status totals: 1 passed, 1 failed, 1 skipped
- Pass rate: 33.33%
- Solve duration: 553 ms total
- Validation duration: 40,237 ms total
- Install duration: 1,077 ms total
- Representative peak RSS: 19,595,264 bytes on `tools/apdr/tests/fixtures/sample_snippet.py`
- Notable host signal: `cfscrape_snippet.py` escalated from env validation to Docker and failed because Docker buildx access was denied on this Windows machine

## Ranked Hotspots

| Rank | File | Runtime signal from `01-baseline.json` | Memory/process signal from `01-memory-profile.json` | Static signal | Why it should be attacked next |
|------|------|-----------------------------------------|-----------------------------------------------------|---------------|--------------------------------|
| 1 | `tools/apdr/src/docker/builder.rs` | Validation dominates the current baseline at `40,237 ms`; the only hard failure in the sample came from env -> Docker escalation. | The representative APDR run peaked at `19,595,264` bytes, and validation orchestration is the main long-lived process path. | `2,712` lines, `22` `.clone()` calls, multiple `summary.attempts.last().unwrap()` fallback checks. | Phase 3 throughput depends on making backend fallback and environment orchestration cheaper and easier to reason about. |
| 2 | `tools/apdr/src/resolver/mod.rs` | All three sampled cases flowed through the core solve path, which accounts for the full `553 ms` solve budget. | The same representative process peak covers the resolver path as well, so every avoidable allocation here contributes to the process ceiling. | `4,674` lines, `192` `.clone()` calls, largest monolithic Rust file in the codebase. | This is the highest-leverage Phase 2 target for reducing ownership churn and simplifying the hottest control flow. |
| 3 | `tools/apdr/src/resolver/pre_solve.rs` | The solve budget is small in absolute terms today, which makes pre-solve a good candidate for cheap early wins before larger benchmark runs. | Representative peak RSS shows there is little slack for gratuitous shared-state copying once benchmark size grows. | `766` lines, `28` `.clone()` calls, several `Arc::try_unwrap(...).unwrap()` and other panic-prone lock exits. | It is compact enough to refactor early and directly addresses shared-state contention and panic risk in solver setup. |
| 4 | `tools/apdr/src/resolver/tier3_llm.rs` | The baseline needed `1` LLM call on `cfscrape_snippet.py`, showing the fallback path is already on the hot path for unresolved cases. | The representative process peak reflects the cost of subprocess-based resolution and IPC when LLM fallback activates. | `1,139` lines, `29` `.clone()` calls, `expect()` on subprocess pipe setup. | Once validation throughput is stable, this file is the next place to reduce subprocess overhead and brittle fallback plumbing. |
| 5 | `tools/apdr/src/resolver/pypi_client.rs` | The failed sample still resolved requirements and checked package state before validation, so PyPI lookup work remains part of every benchmarked solve. | Peak RSS is moderate, but every repeated metadata fetch competes for the same process memory budget. | `1,302` lines, `8` `.clone()` calls, temp-path and cache-heavy code with several test-only `unwrap()`s nearby. | It is a strong follow-on target after resolver control-flow cleanup because cache efficiency here influences both solve speed and repeatability. |

## Recommended Order of Attack

### Phase 2: Resolver Memory & Algorithm Efficiency

1. `tools/apdr/src/resolver/mod.rs`
2. `tools/apdr/src/resolver/pre_solve.rs`
3. `tools/apdr/src/resolver/pypi_client.rs`

### Phase 3: Validation Pipeline Throughput

1. `tools/apdr/src/docker/builder.rs`
2. `tools/apdr/src/resolver/tier3_llm.rs`

## Notes

- `tools/apdr/src/resolver/family_knowledge.rs` remains large and policy-heavy, but the baseline evidence points first to resolver control flow and validation orchestration rather than another family-bundle pass.
- The Windows Docker permission failure recorded in the baseline is useful evidence, not noise. It shows the current fallback chain is both slow and fragile on this host.
