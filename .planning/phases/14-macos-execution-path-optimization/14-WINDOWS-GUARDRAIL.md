# Phase 14 Windows Guardrail Proof

## Commands

```text
python3 scripts/run_phase14_replay.py \
  --manifest-json .planning/phases/14-macos-execution-path-optimization/14-windows-guardrail-slice.json \
  --dataset-root <dataset-root> \
  --baseline-json .planning/phases/14-macos-execution-path-optimization/14-windows-before.json \
  --candidate-json .planning/phases/14-macos-execution-path-optimization/14-windows-after.json \
  --validation-backend env \
  --build-profile release
```

## Artifact Links

- Sample baseline schema: `14-windows-before-sample.json`
- Sample candidate schema: `14-windows-after-sample.json`
- Expected live baseline artifact: `14-windows-before.json`
- Expected live candidate artifact: `14-windows-after.json`

## Guardrail Verdict

The Windows comparison pair must keep the same `slice_id`, `execution_mode`, `cache_state`, and `build_profile`. The candidate artifact may not regress total duration or seconds per case by more than 10%, and it may not lose any baseline `passed` or `skipped*` cases.

## Requirement Mapping

- `WIN-01`: explicit Windows non-regression evidence for the macOS-focused benchmark-performance work

