# Phase 18 Backend Path Proof

## Slice Contract

Phase 18 proof stays anchored to `.planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json`, which freezes the March 30, 2026 live-derived review surface to these five snippets:

- `hard-gists/00e9638c0efad1adac878522cf172484/snippet.py`
- `hard-gists/056626de3fbdc7cf7b59de1d9f6279d1/snippet.py`
- `hard-gists/01c99322cf985e771827/snippet.py`
- `hard-gists/10295174/snippet.py`
- `hard-gists/1231964e784ab9acb65d/snippet.py`

The March 30 wrapper run at `runs/20260330-020943-apdr/summary.json` still points these fixed `relative_path` entries at resumed case artifacts under `runs/20260329-165524-apdr/cases/...`. That keeps the reviewer surface stable even though the wrapper run merged resumed results.

## Live Replay Command

```text
python3 scripts/check_phase18_backend_path.py --run-dir runs/20260330-020943-apdr --slice-json .planning/phases/18-backend-escalation-and-path-truth/18-live-backend-slice.json --status-json .planning/phases/18-backend-escalation-and-path-truth/18-backend-path-proof-status.json --proof-md .planning/phases/18-backend-escalation-and-path-truth/18-BACKEND-PROOF.md
```

This is the deterministic review gate for the fixed slice. Against the frozen March 30 baseline it is expected to fail, because those case outputs still report `validation_backend: env` and do not yet expose `validation_path` or `escalated_backend`. A post-fix replay should use the same checker so only the run artifact changes, not the proof surface or the required route-truth contract.

## Before/After Review

Before the Phase 18 routing changes, the fixed slice still reflects the old contract:

- the saved March 30 baseline artifacts report `validation_backend: env`
- the same artifacts do not yet expose `validation_path`
- the same artifacts do not yet expose `escalated_backend`

After replaying the fixed slice with the Phase 18 changes, reviewers should require all of these conditions:

- `validation_backend` remains `llm` so the requested-mode contract stays stable
- `validation_path` begins with `env->docker`
- `escalated_backend` is `docker`

If any of those conditions fails, the Phase 18 backend-path proof contract has not been met for that replay.
