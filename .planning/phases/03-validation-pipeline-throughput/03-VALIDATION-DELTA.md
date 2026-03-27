# Validation pipeline delta

Phase 3 closes with two bounded artifacts for the same three-fixture sample rule:

- Continuity artifact: `.planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json`
- Forced artifact: `.planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json`

## Commands

Baseline command from `01-baseline.json`:

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/01-baseline-and-guardrails/01-baseline.json --output-md .planning/phases/01-baseline-and-guardrails/01-BASELINE.md
```

Continuity command:

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE.md
```

Forced-validation command:

```text
scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --force-validate --output-json .planning/phases/03-validation-pipeline-throughput/03-validation-candidate-forced.json --output-md .planning/phases/03-validation-pipeline-throughput/03-VALIDATION-CANDIDATE-FORCED.md
```

## Continuity comparison

Regression gate command:

```text
python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate .planning/phases/03-validation-pipeline-throughput/03-validation-candidate.json
```

Result: passed with the default thresholds (`total <= 10% regression`, `validation <= 15% regression`, `pass_rate delta >= 0.0`).

| Metric | Phase 1 baseline | Phase 3 continuity | Delta |
|--------|------------------|--------------------|-------|
| Pass rate | `33.33%` | `66.67%` | `+33.34 pp` |
| Solve duration | `553 ms` | `6178 ms` | `+5625 ms` |
| Validation duration | `40237 ms` | `0 ms` | `-40237 ms` |
| Env create duration | `0 ms` | `0 ms` | `0 ms` |
| Install duration | `1077 ms` | `0 ms` | `-1077 ms` |
| Smoke duration | `0 ms` | `0 ms` | `0 ms` |

What changed:

- The continuity artifact is materially better on pass rate and total runtime, but the win is almost entirely warm-path reuse.
- `cfscrape_snippet.py` and `cv2_serial_snippet.py` both resolved through `import-set-cache`, so they show `validation_duration_ms = 0` and `install_duration_ms = 0`.
- The updated Phase 3 reporting now makes that explicit instead of hiding it inside total validation time: both samples show backend `import-set-cache`, cache detail `import-set`, and zero stage costs.
- Solve time is higher than the original baseline because this capture spent more time in front-end solve work on `apple_private_framework_snippet.py`; the continuity win still holds because the validation path dropped from `40237 ms` to `0 ms`.

Interpretation:

- This continuity comparison is valid for milestone bookkeeping and regression gating.
- It is not sufficient evidence, by itself, that the real env or Docker validation path got cheaper, because the dominant effect is cache reuse.

## Forced-validation throughput snapshot

The forced artifact disables warm-path reuse with `--force-validate`, so it reflects real validation behavior on this Windows host.

| Metric | Phase 1 baseline | Phase 3 forced | Delta |
|--------|------------------|----------------|-------|
| Pass rate | `33.33%` | `0.00%` | `-33.33 pp` |
| Solve duration | `553 ms` | `9062 ms` | `+8509 ms` |
| Validation duration | `40237 ms` | `171903 ms` | `+131666 ms` |
| Env create duration | `0 ms` | `0 ms` | `0 ms` |
| Install duration | `1077 ms` | `3403 ms` | `+2326 ms` |
| Smoke duration | `0 ms` | `0 ms` | `0 ms` |

What changed when cache reuse was disabled:

- `apple_private_framework_snippet.py` still skipped for the same host-runtime reason as the baseline.
- `cfscrape_snippet.py` and `cv2_serial_snippet.py` both escalated from `env` to `docker`, which the new benchmark report now renders as backend path `env -> docker`.
- Both forced failures ended with the same Windows Docker host issue: `CreateFile C:\Users\danny\.docker\buildx\instances: Access is denied.`
- Forced validation therefore did exercise the real retry and backend-escalation path, but it did not demonstrate a throughput win on this host because Docker remained unavailable.
- The forced snapshot also surfaced stage-level cost that earlier phases could not see directly: `3403 ms` of install time across the bounded sample, `8` env builds, and `8` LLM calls.

## Host variance and remaining constraint

- The main remaining variance is still host-specific Docker availability on Windows, not hidden benchmark noise.
- Phase 3 improved observability and reduced repeated backend-probe overhead, but the forced snapshot shows that Windows Docker access is still the practical ceiling for these validation-heavy cases.
- Any future claim of real validation-path throughput improvement needs either a host with working Docker buildx permissions or a benchmark slice that does not hinge on the same Windows Docker failure mode.

## Bottom line

- `03-validation-candidate.json` is the correct continuity artifact for comparison against `01-baseline.json`, and it passes the configured regression gate.
- `03-validation-candidate-forced.json` is the correct evidence artifact for the real validation path, and it shows that cache reuse is not the same thing as cheaper forced validation on this host.
- Phase 3 succeeded on telemetry and measurement clarity, but the forced snapshot keeps the Windows Docker constraint visible instead of overstating performance wins.
