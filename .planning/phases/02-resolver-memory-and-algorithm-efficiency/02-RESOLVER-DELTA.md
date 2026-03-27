# Resolver hot-path delta

## Commands

Baseline command:

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/01-baseline-and-guardrails/01-baseline.json --output-md .planning/phases/01-baseline-and-guardrails/01-BASELINE.md
```

Candidate command:

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json --output-md .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-RESOLVER-CANDIDATE.md
```

Regression gate:

```text
python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json
```

## Artifact Links

- Baseline: `.planning/phases/01-baseline-and-guardrails/01-baseline.json`
- Candidate: `.planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json`

## Summary

- Pass-rate delta: `+33.33 pts` (`33.33%` -> `66.67%`)
- Solve duration delta: `-123 ms` (`553 ms` -> `430 ms`, `-22.24%`)
- Validation duration delta: `-40,237 ms` (`40,237 ms` -> `0 ms`, `-100.00%`)
- Total duration delta: `-41,437 ms` (`41,867 ms` -> `430 ms`, `-98.97%`)
- Regression gate result: `within configured thresholds`

## Interpretation

- The bounded sample shows a real solve-time improvement in the resolver hot path: total solve time dropped from `553 ms` to `430 ms` across the same three lexicographically selected fixtures.
- The large pass-rate improvement comes from `cfscrape_snippet.py` no longer failing in the bounded sample.
- The much larger validation and total-duration drop should not be read as a pure resolver win. The Phase 1 baseline spent `40,237 ms` failing `cfscrape_snippet.py` after env validation escalated into the Windows Docker permission issue (`C:\Users\danny\.docker\buildx\instances` access denied), while the Phase 2 candidate reused a previously validated import-set solution and therefore recorded `0 ms` validation time for that case.
- Because the candidate run stayed on cached validation paths for both passing snippets, the cleanest resolver-specific comparison in this bounded sample is the solve-duration delta, not the total wall clock delta.

## Gate Output

`python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/02-resolver-memory-and-algorithm-efficiency/02-resolver-candidate.json` reported:

- `pass_rate`: `OK`
- `total_duration_ms`: `OK`
- `validation_duration_ms`: `OK`
- Final result: `candidate is within the configured regression thresholds`

## Reviewer Notes

- The benchmark settings stayed apples-to-apples: same fixture root, same sample limit, same lexicographic selection rule, and same `env` validation backend.
- Remaining host/runtime variance is still dominated by Windows-specific validation behavior and cache reuse, not by the resolver code paths changed in `02-01` and `02-02`.
