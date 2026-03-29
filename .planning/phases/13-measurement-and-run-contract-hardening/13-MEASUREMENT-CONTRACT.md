# Phase 13 Measurement Contract

## Required Run Contract

Every reviewer-facing Phase 13 artifact must carry the canonical run contract fields introduced in Plans 13-01 and 13-02. The minimum contract is:

- `run_contract_version`
- `tool`
- `model_name`
- `base_url`
- `validation_backend`
- `run_intent`
- `execution_mode`
- `cache_state`
- `host_architecture`
- `apdr_binary_architecture`
- `python_architecture`
- `llm_context_window`
- `inference_policy`
- `build_profile`

Artifacts should preserve these fields inside a nested `run_contract` object and also surface the comparison-critical subset at top level for easy diffing and table generation.

## Stage Timings

Phase 13 stage timings must be explicit and numeric at both top-level totals and per-sample scope:

- `solve_duration_ms`
- `validation_duration_ms`
- `llm_duration_ms`
- `env_create_duration_ms`
- `install_duration_ms`
- `docker_startup_duration_ms`
- `smoke_duration_ms`

`docker_startup_duration_ms` is distinct from `smoke_duration_ms`; Docker launch cost must not be hidden inside the smoke bucket.

## Evidence Labels

Reviewer-facing evidence must label execution mode and cache state directly instead of asking reviewers to infer them from commands or notes.

- `execution_mode`
  - `env-fast` means native environment validation is the primary path
  - `docker-proof` means Docker is the proof path
  - `llm-hybrid` means the LLM validation path is active
- `cache_state`
  - `warm` means at least one relevant cache surface was reused
  - `cold` means no relevant cache surface was reused
  - `mixed` means the capture combines multiple cache states
  - `unknown` is only acceptable as a temporary fallback during artifact construction, not for milestone proof

## Comparison Metadata

The fields below are required so later benchmark comparisons can attribute gains correctly:

- `model_name`
- `base_url`
- `run_intent`
- `execution_mode`
- `cache_state`
- `llm_context_window`
- `inference_policy`
- `build_profile`

When reviewers compare `env-fast` versus `docker-proof`, warm versus cold, or one model/build profile against another, the artifact must show these values directly in the JSON or Markdown output. No later phase should need to reconstruct them from free-form logs.
