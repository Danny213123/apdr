# Phase 15 Tier3 Benchmark Contract

## Commands

```text
python3 scripts/run_phase15_tier3_benchmark.py \
  --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json \
  --fixtures-root tools/apdr/tests/fixtures \
  --mode baseline \
  --output-json .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline.json \
  --execute-live \
  --agent-mode manual \
  --tool-profile full \
  --retrieval-profile none \
  --thinking-mode inherited \
  --llm-context-window 16384 \
  --inference-policy "temperature=0.0;think=inherited"

python3 scripts/run_phase15_tier3_benchmark.py \
  --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json \
  --fixtures-root tools/apdr/tests/fixtures \
  --mode candidate \
  --output-json .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate.json \
  --execute-live \
  --agent-mode langgraph \
  --tool-profile reduced-toolset \
  --retrieval-profile failure-memory+reverse-index \
  --thinking-mode routed \
  --llm-context-window 32768 \
  --inference-policy "temperature=0.2;top_p=0.95;think=routed;self_consistency=3"
```

Probe-only schema check:

```text
python3 scripts/run_phase15_tier3_benchmark.py \
  --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json \
  --fixtures-root tools/apdr/tests/fixtures \
  --mode baseline \
  --output-json /tmp/phase15-benchmark-probe.json \
  --probe-only
```

## Artifact Links

- Sample baseline schema: `15-tier3-baseline-sample.json`
- Sample candidate schema: `15-tier3-candidate-sample.json`
- Expected live baseline artifact: `15-tier3-baseline.json`
- Expected live candidate artifact: `15-tier3-candidate.json`

## Comparison Contract

The baseline and candidate artifacts must share the same `slice_id`, `validation_backend`, `build_profile`, and replay manifest. The candidate artifact may change `agent_mode`, `tool_profile`, `retrieval_profile`, `thinking_mode`, `llm_context_window`, and `inference_policy`, but those knobs must be recorded directly in the artifact so later checker work can attribute gains to policy changes instead of hidden prompt edits.

Tier3 comparison uses per-case `tier3_status` entries plus top-level `resolved`, `abstained`, `failed`, `skipped`, `success_rate`, and `tier3_status_counts`. Candidate runs should improve `resolved` outcomes on the locked replay slice while preserving expected skip behavior for platform-limited cases such as `skip_binaryninja_snippet.py`.

## Requirement Mapping

- `AGT-03`: baseline-versus-candidate artifacts provide the replay-slice success-rate surface needed to prove improved LLM-path resolution outcomes.
- `AGT-05`: each artifact records `llm_context_window`, so larger-context experiments can be compared against the same locked slice.
- `AGT-06`: each artifact records `model_name`, `thinking_mode`, and `inference_policy`, which makes sub-10GB-VRAM policy tuning reviewer-readable for `qwen3.5:9b` or comparable local models.
