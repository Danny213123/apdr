# Phase 17 Fallback Proof

## Slice Contract

Phase 17 proof stays anchored to `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-live-fallback-slice.json`, which freezes the March 30, 2026 live-derived review surface to these five snippets:

- `hard-gists/00e9638c0efad1adac878522cf172484/snippet.py`
- `hard-gists/01c99322cf985e771827/snippet.py`
- `hard-gists/01b8b8e1909ae0f601c85e142f2bd15b/snippet.py`
- `hard-gists/026a4d6400b1efac9a13a3296f16e655/snippet.py`
- `hard-gists/1233846/snippet.py`

The wrapper summary at `runs/20260330-020943-apdr/summary.json` resolves those fixed `relative_path` entries to saved case artifacts under the resumed predecessor run `runs/20260329-165524-apdr/cases/...`. That keeps the review surface stable even though the March 30 wrapper run merged resumed results.

## Sample Outcome Contract

`.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-agent-outcome-sample.json` is the bounded contract for reviewer-facing fallback truth. It contains exactly three sample rows and every row uses only these keys: `fallback_invoked`, `fallback_outcome`, `fallback_reason`, and `validation_status`.

- `passed` pairs fallback success with `validation_status: passed`.
- `abstained` preserves a non-pass validation status while recording a non-empty fallback reason.
- `failed` preserves a non-pass validation status while recording the terminal fallback failure reason.

The deterministic probe command writes `.planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-fallback-proof-status.json`, whose `sample_contract` section is the machine-readable confirmation that this bounded contract is still intact.

## Live Replay Command

```text
python3 scripts/check_phase17_fallback_artifacts.py --run-dir runs/20260330-020943-apdr --slice-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-live-fallback-slice.json --sample-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-agent-outcome-sample.json --status-json .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-fallback-proof-status.json --proof-md .planning/phases/17-llm-fallback-stability-and-outcome-tracing/17-FALLBACK-PROOF.md
```

This is the deterministic review gate for the fixed slice. Against the frozen March 30 baseline it is expected to fail, because that benchmark context log still shows the old crash and the saved case outputs predate the fallback metadata contract. A post-fix replay should use the same checker so only the run artifact changes, not the proof surface or the required keys.

## Before/After Review

Before the Phase 17 fixes, the live March 30 benchmark context log still contains the removed `confidence` crash signature: `ValueError: 'confidence' is already being used as a state key`. The saved March 30 case outputs also predate the new fallback metadata fields, so the fixed slice does not yet show `fallback_invoked`, `fallback_outcome`, and `fallback_reason`.

After replaying the fixed slice with the repaired fallback path, reviewers should require two things:

- the checked run no longer contains the removed `confidence` crash signature in its benchmark context log
- every checked slice artifact exposes `fallback_invoked`, `fallback_outcome`, and `fallback_reason`

If either condition fails, the Phase 17 proof contract has not been met for that replay.
