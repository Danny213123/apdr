# Phase 21 Live Evidence

## Locked Slice

This evidence pack uses the fixed nine-case slice `phase20-dominant-bucket-proof-2026-03-30` from `.planning/phases/20-dominant-bucket-recovery-gains/20-dominant-bucket-slice.json`. The March 30, 2026 baseline artifact is `.planning/phases/21-live-evidence-and-closeout-pack/21-baseline-live.json`, and the v2.3 candidate artifact is `.planning/phases/21-live-evidence-and-closeout-pack/21-candidate-live.json`.

## Before/After Bucket Counts

| Metric | March 30, 2026 baseline | v2.3 candidate |
| --- | --- | --- |
| Passes | 0 | 2 |
| Failures | 9 | 7 |
| Skips | 0 | 0 |
| `module-not-found` | 3 | 0 |
| `version-not-found` | 3 | 0 |
| `environment-build-failed` | 3 | 0 |

The fixed-slice pass delta is `+2`. On this slice, the dominant baseline buckets no longer appear as the terminal candidate buckets because v2.3 either recovered the case into `passed` or surfaced a different truthful terminal state such as `failed` or `interrupted`.

## Run Contract

- Baseline source run: `runs/20260330-020943-apdr`
- Candidate source run: `runs/20260401-173232-apdr`
- Candidate resume origin: `runs/20260401-162919-apdr`
- Validation backend: `llm`
- Model name: `qwen3.5:9b`
- Candidate provenance: `5` historical rows plus `4` live rows in the resumed summary
- Interrupted case: `hard-gists/1239373/snippet.py` is kept in the candidate artifact as `validation_status: interrupted` and `validation_path: interrupted`

## Review Notes

- The strongest recovered-delta case is `hard-gists/09648344984565f9477a/snippet.py`, which moves from a baseline failure into a candidate pass on `validation_path: env->docker->llm-agent`.
- A second recovered case is `hard-gists/056626de3fbdc7cf7b59de1d9f6279d1/snippet.py`, which now passes directly on `validation_path: env`.
- The best truth-surface non-pass case is `hard-gists/04ef258fa29e4e685287a30cf60462d0/snippet.py`, which keeps `failure_family: dependency-resolution`, `fallback_outcome: failed`, and `resultOrigin: live` visible.
- The resumed candidate run is live evidence for the fixed slice, not a broader claim about full-corpus benchmark stability.
