# Phase 14 macOS Replay Proof

## Commands

```text
python3 scripts/run_phase14_replay.py \
  --manifest-json .planning/phases/14-macos-execution-path-optimization/14-macos-replay-slice.json \
  --dataset-root <dataset-root> \
  --baseline-json .planning/phases/14-macos-execution-path-optimization/14-macos-before.json \
  --candidate-json .planning/phases/14-macos-execution-path-optimization/14-macos-after.json \
  --prewarm \
  --validation-backend env \
  --build-profile release
```

## Artifact Links

- Sample baseline schema: `14-macos-before-sample.json`
- Sample candidate schema: `14-macos-after-sample.json`
- Expected live baseline artifact: `14-macos-before.json`
- Expected live candidate artifact: `14-macos-after.json`

## Before/After Verdict

The candidate replay artifact must keep the same `slice_id`, `execution_mode`, `cache_state`, and `build_profile` as the baseline artifact. It must also improve both total duration and seconds per case by at least 20% while preserving every baseline `passed` case and every baseline `skipped*` case.

## Requirement Mapping

- `MAC-04`: reviewer-readable before/after macOS replay evidence with substantial gains and no pass/skip preservation regression

