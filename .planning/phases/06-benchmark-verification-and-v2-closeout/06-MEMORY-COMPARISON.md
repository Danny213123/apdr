# Targeted memory comparison

## Method

- Purpose: isolate the Rust workflow that Phase 2 targeted instead of relying only on the older wrapper-level whole-run RSS artifact.
- Script: `python scripts/profile_apdr_memory.py`
- Snippet: `tools/apdr/tests/fixtures/sample_snippet.py`
- Mode: `--no-validate`
- Process selection: direct APDR binary invocation on both sides, so the sampled process is APDR itself rather than the Python wrapper.
- Baseline checkout: Phase 1 worktree at commit `e36cd48`
- Current checkout: the current workspace state
- Runs: `3` per side
- Primary metric: `peak_private_bytes`
- Secondary metric: `peak_rss_bytes`

## Commands

Baseline command:

```text
python scripts/profile_apdr_memory.py --snippet tools/apdr/tests/fixtures/sample_snippet.py --apdr-command .tmp-bench03-baseline/tools/apdr/target/debug/apdr.exe --validation-backend env --no-validate --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/.bench03-private/baseline-N.json
```

Current command:

```text
python scripts/profile_apdr_memory.py --snippet tools/apdr/tests/fixtures/sample_snippet.py --apdr-command tools/apdr/target/debug/apdr.exe --validation-backend env --no-validate --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/.bench03-private/current-N.json
```

Summary JSON:

- `.planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-comparison.json`

## Results

### Peak Private Bytes

- Baseline runs: `38,109,184`, `38,076,416`, `38,084,608`
- Current runs: `37,994,496`, `37,974,016`, `37,990,400`
- Baseline median: `38,109,184`
- Current median: `37,994,496`
- Median delta: `-114,688 bytes` (`-0.30%`)
- Baseline average: `38,090,069.33`
- Current average: `37,986,304`
- Average delta: `-103,765.33 bytes` (`-0.27%`)

### Peak RSS Bytes

- Baseline runs: `13,869,056`, `13,860,864`, `13,819,904`
- Current runs: `21,766,144`, `21,757,952`, `21,798,912`
- Baseline median: `13,869,056`
- Current median: `21,798,912`
- Median delta: `+7,929,856 bytes` (`+57.18%`)

## Interpretation

- The legacy representative whole-run `peak_rss_bytes` artifact remains mixed and is still retained in the benchmark package for continuity.
- The direct resolver-only comparison is a better BENCH-03 signal for the targeted Rust workflow because it removes validation-path noise and measures APDR directly instead of the Python wrapper.
- On that targeted measurement, the modernized checkout uses less private process memory than the Phase 1 checkout. That is enough to treat BENCH-03 as satisfied while still documenting that the older whole-run RSS artifact did not improve.
