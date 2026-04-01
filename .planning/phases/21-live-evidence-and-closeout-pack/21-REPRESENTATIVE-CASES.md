# Phase 21 Representative Cases

## Representative Cases

The machine-readable case list is `.planning/phases/21-live-evidence-and-closeout-pack/21-case-index.json`. Reviewers should inspect `validation_path`, `failure_family`, `fallback_outcome`, and `resultOrigin` first, then open the linked `artifact_dir` or `resolution-report.txt` for the concrete attempt history.

## Recovered Cases

- `hard-gists/09648344984565f9477a/snippet.py`
  March 30, 2026 baseline: `module-not-found`
  v2.3 candidate: `passed`
  Why it matters: this is the clearest `recovered-delta` case in the fixed slice, and the candidate report shows `validation_path: env->docker->llm-agent` plus an LLM-applied recovery pin.

- `hard-gists/056626de3fbdc7cf7b59de1d9f6279d1/snippet.py`
  March 30, 2026 baseline: `environment-build-failed`
  v2.3 candidate: `passed`
  Why it matters: this proves the milestone did not only add routed fallback metadata; it also created a direct `env` recovery win on the locked slice.

## Truth-Surface Cases

- `hard-gists/04ef258fa29e4e685287a30cf60462d0/snippet.py`
  March 30, 2026 baseline: `module-not-found`
  v2.3 candidate: `failed`
  Fields to inspect: `failure_family`, `fallback_outcome`, `validation_path`, `resultOrigin`
  Why it matters: the candidate artifact keeps a truthful dependency-resolution non-pass with visible fallback outcome instead of collapsing back to unlabeled env-only metadata.

- `hard-gists/1239373/snippet.py`
  March 30, 2026 baseline: `version-not-found`
  v2.3 candidate: `interrupted`
  Fields to inspect: `validation_path`, `validation_status`, `validation_reason`, `resultOrigin`
  Why it matters: the resumed live run did not produce `resolution-report.txt` for this case before operator cleanup, and the candidate artifact records that interruption explicitly instead of inventing a normal validation result.

## Remaining Limits

- This pack is fixed-slice evidence, not a full-corpus claim.
- The dominant bucket counts fell to zero on this slice because the candidate terminal states moved into `passed`, `failed`, or `interrupted`; reviewers should read the representative case artifacts rather than assuming the residual failures disappeared.
- `hard-gists/1239373/snippet.py` remains the noisiest tail case in the live replay, so the closeout note keeps that limitation explicit.
