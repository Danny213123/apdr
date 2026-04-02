# Phase 24 Comparison Runbook

This runbook explains how to regenerate the Phase 24 paired-policy artifacts. If these files drift or are deleted, rerun [`$gsd-execute-phase 24`](/Users/dannyguan/.codex/skills/gsd-execute-phase/SKILL.md) or follow the commands below directly.

## Probe-Only Extraction

This section is the `probe-only extraction` path for fast deterministic validation.

Use the frozen fixture summaries when you want a fast deterministic check of the harness contract.

### 1. Generate the env-first artifact

```bash
python3 scripts/run_phase24_policy_comparison.py \
  --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json \
  --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-env-first-fixture-summary.json \
  --output-json /tmp/phase24-env-artifact.json \
  --mode env-first \
  --llm-validation-policy env-first \
  --probe-only
```

### 2. Generate the docker-first artifact

```bash
python3 scripts/run_phase24_policy_comparison.py \
  --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json \
  --summary-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-docker-first-fixture-summary.json \
  --output-json /tmp/phase24-docker-artifact.json \
  --mode docker-first \
  --llm-validation-policy docker-first \
  --probe-only
```

### 3. Check contract parity and deltas

```bash
python3 scripts/check_phase24_policy_comparison.py \
  --env-artifact /tmp/phase24-env-artifact.json \
  --docker-artifact /tmp/phase24-docker-artifact.json \
  --status-json /tmp/phase24-comparison-status.json \
  --probe-only
```

This probe-only extraction path should pass without launching APDR.

## Paired Live Replay

This section is the `paired live replay` path for real benchmark evidence on a supported host.

Use this when you want real live evidence on a supported host.

### Contract rules

Keep these values identical across both replays:

- the same `--slice-json`
- the same `model` and `base_url`
- the same `build_profile`
- the same `cache_state`
- `validation_backend=llm`

The only intentional change between the two runs is `--llm-validation-policy`.

### 1. Replay env-first

```bash
python3 scripts/run_phase24_policy_comparison.py \
  --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json \
  --output-json /tmp/phase24-live-env.json \
  --mode env-first \
  --llm-validation-policy env-first \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --build-profile standard \
  --cache-state unknown \
  --execute-live
```

### 2. Replay docker-first

```bash
python3 scripts/run_phase24_policy_comparison.py \
  --slice-json .planning/phases/24-env-first-vs-docker-first-comparison-harness/24-comparison-slice.json \
  --output-json /tmp/phase24-live-docker.json \
  --mode docker-first \
  --llm-validation-policy docker-first \
  --model-name qwen3.5:9b \
  --base-url http://localhost:11434 \
  --build-profile standard \
  --cache-state unknown \
  --execute-live
```

### 3. Compare the live pair

```bash
python3 scripts/check_phase24_policy_comparison.py \
  --env-artifact /tmp/phase24-live-env.json \
  --docker-artifact /tmp/phase24-live-docker.json \
  --status-json /tmp/phase24-live-status.json \
  --probe-only
```

## Contract Parity Checks

The checker must confirm all of the following:

- same `slice_id`
- same ordered case set
- same `validation_backend=llm`
- same model and base URL
- same build profile and cache state in the run contract
- same `resultOrigin` coverage
- non-zero `pass delta`
- at least one dominant-bucket delta
- at least one timing delta

## Known Caveats

- The current fixed artifacts are deterministic sample artifacts, not the final live evidence pack.
- Phase 23 human verification is still open in [23-HUMAN-UAT.md](/Users/dannyguan/Documents/fse-aiware-python-dependencies/.planning/phases/23-policy-truth-and-failure-semantics/23-HUMAN-UAT.md). Clear that debt before treating Phase 24 plus Phase 25 as fully signed off.
- This runbook proves the comparison harness, not the final keep/optional/reject recommendation.
