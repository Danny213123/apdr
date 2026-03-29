# Phase 15 Agent Quality Proof

## Commands

```text
python3 scripts/check_phase15_agent_quality.py \
  --baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json \
  --candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json
```

Live proof command:

```text
python3 scripts/check_phase15_agent_quality.py \
  --baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline.json \
  --candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate.json
```

## Artifact Links

- Sample baseline artifact: `15-tier3-baseline-sample.json`
- Sample candidate artifact: `15-tier3-candidate-sample.json`
- Expected live baseline artifact: `15-tier3-baseline.json`
- Expected live candidate artifact: `15-tier3-candidate.json`
- Policy matrix note: `15-QWEN-POLICY-MATRIX.md`

## Comparison Verdict

The candidate artifact must keep the same replay slice, validation backend, build profile, and skipped-count contract as the baseline. It must also improve at least one of:

- resolved count
- success rate
- failed count
- abstain quality when resolved count does not regress

The candidate must additionally change at least one attributable runtime or policy field such as `agent_mode`, `tool_profile`, `retrieval_profile`, `thinking_mode`, `llm_context_window`, `inference_policy`, or `policy_label`.

## Requirement Mapping

- `AGT-03`: the checker validates that the candidate improves replay-slice quality versus the baseline instead of relying on anecdotal outputs.
- `AGT-04`: the checker rejects candidate artifacts that improve only by increasing failed cases or obscuring abstentions.
- `AGT-06`: policy attribution remains explicit because the comparison requires changed and non-empty policy metadata.
