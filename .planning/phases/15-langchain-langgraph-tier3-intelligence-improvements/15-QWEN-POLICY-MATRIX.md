# Phase 15 Qwen Policy Matrix

## Commands

```text
python3 scripts/run_phase15_tier3_benchmark.py \
  --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json \
  --fixtures-root tools/apdr/tests/fixtures \
  --mode baseline \
  --output-json .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline.json \
  --execute-live \
  --model-name qwen3.5:9b \
  --agent-mode manual \
  --tool-profile full \
  --retrieval-profile none \
  --memory-profile none \
  --thinking-mode inherited \
  --llm-context-window 16384 \
  --temperature 0.0 \
  --top-p 0.0 \
  --top-k 1 \
  --policy-label baseline-qwen35-deterministic

python3 scripts/run_phase15_tier3_benchmark.py \
  --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json \
  --fixtures-root tools/apdr/tests/fixtures \
  --mode candidate \
  --output-json .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate.json \
  --execute-live \
  --model-name qwen3.5:9b \
  --agent-mode langgraph \
  --tool-profile reduced-toolset \
  --retrieval-profile failure-memory+summary-fold \
  --memory-profile replay-outcomes \
  --thinking-mode routed \
  --llm-context-window 32768 \
  --temperature 0.2 \
  --top-p 0.95 \
  --top-k 40 \
  --self-consistency-passes 3 \
  --verifier-passes 1 \
  --policy-label candidate-qwen35-routed
```

## Artifact Links

- Sample baseline artifact: `15-tier3-baseline-sample.json`
- Sample candidate artifact: `15-tier3-candidate-sample.json`
- Expected live baseline artifact: `15-tier3-baseline.json`
- Expected live candidate artifact: `15-tier3-candidate.json`
- Checker: `scripts/check_phase15_agent_quality.py`

## Policy Variants

| Label | Thinking | Temperature | Top P | Top K | Context | Tool/Profile | Retrieval/Memory | Extra passes |
|------|------|------|------|------|------|------|------|------|
| `baseline-qwen35-deterministic` | inherited | 0.0 | 0.0 | 1 | 16384 | manual + full | none + none | none |
| `candidate-qwen35-routed` | routed | 0.2 | 0.95 | 40 | 32768 | langgraph + reduced-toolset | failure-memory+summary-fold + replay-outcomes | self-consistency=3, verifier=1 |
| `candidate-qwen35-nonthink-verify` | off | 0.1 | 0.9 | 20 | 24576 | manual + reduced-toolset | reverse-index + replay-outcomes | verifier=2 |

## Requirement Mapping

- `AGT-06`: the representative `qwen3.5:9b` path is benchmarked under explicit policy labels and non-greedy decoding controls instead of one hidden default.
- `AGT-01`: the matrix keeps agent runtime and tool-surface choices attributable alongside the Qwen policy knobs.
- `AGT-03`: the matrix is designed for side-by-side replay-slice comparison so gains can be measured rather than asserted.
