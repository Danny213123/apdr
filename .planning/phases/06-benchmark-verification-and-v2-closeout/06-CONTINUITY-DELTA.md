# Continuity delta

## Commands

Baseline command:

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/01-baseline-and-guardrails/01-baseline.json --output-md .planning/phases/01-baseline-and-guardrails/01-BASELINE.md
```

Phase 6 continuity command:

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json --output-md .planning/phases/06-benchmark-verification-and-v2-closeout/06-CONTINUITY-CANDIDATE.md
```

Phase 6 memory command:

```text
python scripts/profile_apdr_memory.py --snippet tools/apdr/tests/fixtures/sample_snippet.py --validation-backend env --output-json .planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-profile.json
```

Regression gate:

```text
python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json
```

## Artifact Links

- Baseline benchmark: `.planning/phases/01-baseline-and-guardrails/01-baseline.json`
- Phase 6 continuity benchmark: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json`
- Baseline memory profile: `.planning/phases/01-baseline-and-guardrails/01-memory-profile.json`
- Phase 6 memory profile: `.planning/phases/06-benchmark-verification-and-v2-closeout/06-memory-profile.json`
- Phase 3 artifacts: `.planning/phases/03-validation-pipeline-throughput/03-VALIDATION-DELTA.md` and `.planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json`

## Summary

- Pass-rate delta: `+0.3333` (`33.33%` -> `66.67%`)
- Total-duration delta: `-35,260 ms` (`41,867 ms` -> `6,607 ms`, `-84.22%`)
- Solve-duration delta: `+6,054 ms` (`553 ms` -> `6,607 ms`, `+1094.76%`)
- Validation-duration delta: `-40,237 ms` (`40,237 ms` -> `0 ms`, `-100.00%`)
- Regression gate result: `within configured thresholds`

## Memory Comparison

- Baseline `peak_rss_bytes`: `19,595,264`
- Phase 6 `peak_rss_bytes`: `19,845,120`
- Delta: `+249,856 bytes` (`+1.28%`)

## Interpretation

- The bounded continuity gate improved pass rate because `cfscrape_snippet.py` no longer failed in the three-case continuity sample, and both `cfscrape_snippet.py` and `cv2_serial_snippet.py` reused previously validated import-set solutions.
- The solve-time portion of this capture is slower than the committed Phase 1 baseline because `apple_private_framework_snippet.py` spent much longer in front-end solve work while still ending in the same host-runtime skip.
- The continuity gate is still the right milestone regression check because it reruns the exact Phase 1 contract on the same bounded fixture set and stays within the configured thresholds.
- Forced-validation host variance is documented separately in the Phase 3 artifacts and is not being blended into this continuity gate. The retained Windows Docker forced-validation evidence stays in `03-VALIDATION-DELTA.md` and `03-validation-candidate-forced.json`.

## Gate Output

`python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/06-benchmark-verification-and-v2-closeout/06-continuity-candidate.json` reported:

- `pass_rate`: `OK`
- `total_duration_ms`: `OK`
- `validation_duration_ms`: `OK`
- Final result: `candidate is within the configured regression thresholds`
