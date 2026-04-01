# Phase 21 Live Runbook

## Baseline Source

- Artifact: `.planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json`
- Extracted from: `runs/20260330-020943-apdr/summary.json`
- Locked slice: `phase20-dominant-bucket-proof-2026-03-30`
- Requested backend and model: `llm` with `qwen3.5:9b`

## Candidate Source

- First live attempt: `runs/20260401-162919-apdr`
- Resume source: `runs/20260401-162919-apdr` with `5/9` rows already recorded
- Resumed live attempt: `runs/20260401-173232-apdr`
- Candidate artifact: `.planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json`
- Candidate source summary: `runs/20260401-173232-apdr/summary.json`
- Resume note: the resumed summary carries `5` `historical_results` from `runs/20260401-162919-apdr` and `4` live rows from `runs/20260401-173232-apdr`
- Interrupted-case note: `hard-gists/1239373/snippet.py` did not emit `resolution-report.txt` before operator cleanup; the candidate artifact preserves that row as `validation_status: interrupted` and `validation_path: interrupted`

## Commands

```bash
python3 scripts/run_phase20_recovery_benchmark.py \
  --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json \
  --summary-json runs/20260330-020943-apdr/summary.json \
  --output-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json \
  --mode baseline \
  --validation-backend llm \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --probe-only

python3 scripts/run_phase20_recovery_benchmark.py \
  --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json \
  --dataset-root hard-gists \
  --output-json /tmp/phase21-candidate-live-escalated.json \
  --mode candidate \
  --validation-backend llm \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --execute-live

python3 scripts/run_phase20_recovery_benchmark.py \
  --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json \
  --dataset-root hard-gists \
  --output-json /tmp/phase21-candidate-live-escalated.json \
  --mode candidate \
  --validation-backend llm \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --workers 3 \
  --resume-run-id 20260401-162919-apdr \
  --execute-live

python3 scripts/run_phase20_recovery_benchmark.py \
  --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json \
  --summary-json runs/20260401-173232-apdr/summary.json \
  --output-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json \
  --mode candidate \
  --validation-backend llm \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --probe-only

python3 scripts/check_phase20_recovery_delta.py \
  --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json \
  --baseline-json .planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json \
  --candidate-json .planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json \
  --status-json /tmp/phase21-delta-status.json
```

## Result Contract

- Same locked slice on both sides: `phase20-dominant-bucket-proof-2026-03-30`
- Same requested backend on both sides: `llm`
- Same model on both sides: `qwen3.5:9b`
- March 30, 2026 baseline counts: `0` passes, `9` failures, dominant buckets `3/3/3`
- v2.3 candidate counts: `2` passes, `7` failures, dominant buckets `0/0/0`
- Pass delta on the fixed slice: `+2`
- Candidate artifact mix is explicit rather than hidden: `5` carried-forward historical rows, `4` resumed live rows, and `1` of those live rows is surfaced as an interrupted case instead of being silently dropped
