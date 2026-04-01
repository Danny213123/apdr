# Phase 20 Recovery Delta

## Slice Contract

Phase 20 proof stays anchored to `.planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json`, which freezes the March 30, 2026 dominant-bucket review surface to these nine tier3 snippets:

- `hard-gists/04ef258fa29e4e685287a30cf60462d0/snippet.py`
- `hard-gists/09648344984565f9477a/snippet.py`
- `hard-gists/101323115e70bb6671d3/snippet.py`
- `hard-gists/10295174/snippet.py`
- `hard-gists/1096373/snippet.py`
- `hard-gists/1239373/snippet.py`
- `hard-gists/00e9638c0efad1adac878522cf172484/snippet.py`
- `hard-gists/056626de3fbdc7cf7b59de1d9f6279d1/snippet.py`
- `hard-gists/03de5c4c21138da5c29d/snippet.py`

The fixed baseline sample in `.planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json` records the observed March 30 status for each of those cases. The candidate sample in `.planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json` uses the same `slice_id`, `validation_backend: llm`, and `model_name: qwen3.5:9b`, while preserving Phase 18 `validation_path` truth and Phase 19 `resultOrigin` provenance fields.

## Probe Command

```text
python3 scripts/check_phase20_recovery_delta.py --slice-json .planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json --baseline-json .planning/phases/20-dominant-bucket-recovery-gains/20-baseline-sample.json --candidate-json .planning/phases/20-dominant-bucket-recovery-gains/20-candidate-sample.json --status-json .planning/phases/20-dominant-bucket-recovery-gains/20-recovery-proof-status.json --probe-only
```

This deterministic gate requires two things at the same time:

- candidate passes must exceed baseline passes on the locked slice
- candidate counts for `module-not-found`, `version-not-found`, and `environment-build-failed` must each be lower than baseline

## Before/After Review

Before the Phase 20 changes, the locked March 30 slice was a perfect three-way split across the dominant buckets: three `module-not-found` failures, three `version-not-found` failures, and three `environment-build-failed` failures, with zero passes.

After the Phase 20 changes, reviewers should expect the candidate slice to show:

- module-provider recovery converting the `Cython.Distutils` case out of the dominant `module-not-found` bucket
- replacement-package recovery converting the `BeautifulSoup` case out of the `version-not-found` bucket
- compatibility-floor and OpenCV convergence recovery turning the PyMC3/OpenCV cases into passes instead of repeat version/build churn
- unchanged hard cases still preserving truthful `validation_path` and `resultOrigin` metadata so any remaining failures are attributable to recovery limits rather than accounting drift

If the checker reports a non-positive `delta_passes`, preserves any dominant bucket at the same count, loses the fixed slice ordering, or allows baseline and candidate to differ on `slice_id`, `validation_backend`, or `model_name`, the Phase 20 proof contract has not been met.
