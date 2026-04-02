# APDR

APDR is the agentic dependency resolver in this repository. It now runs the full planned workflow for a snippet:

1. Parse imports and adjacent config files
2. Detect a likely Python version window
3. Resolve packages through seeded cache, heuristics, and optional Ollama fallback
4. Expand known transitive dependencies
5. Generate `requirements.txt` and a resolution report
6. Validate the environment in local Python interpreters with retries and error-driven recovery
7. Persist learned mappings, version data, lockfiles, build artifacts, and recovery patterns in the APDR cache root

## Build

```bash
cd /path/to/apdr-repo/tools/apdr
./build.sh
```

`./build.sh` now sends Cargo output to `APDR_TARGET_DIR` when set, or to `$HOME/.cache/apdr/target` by default so normal builds do not repopulate `tools/apdr/target` inside the repo.

On Windows 11, build with Cargo directly:

```powershell
cd C:\path\to\apdr-repo\tools\apdr
cargo build --release
```

## CLI

Resolve a snippet on disk:

```bash
cargo run -- resolve tests/fixtures/sample_snippet.py --output target/manual-run --no-validate
```

Resolve from stdin:

```bash
cat tests/fixtures/sample_snippet.py | cargo run -- resolve --stdin --output target/stdin-run --no-validate
```

Validate with local interpreters and optional Ollama fallback:

```bash
cargo run -- resolve /path/to/snippet.py \
  --output target/validated-run \
  --range 1 \
  --max-retries 5 \
  --docker-timeout 300 \
  --allow-llm \
  --llm-provider ollama \
  --llm-model gemma3:4b \
  --llm-base-url http://localhost:11434
```

Useful extras:

```bash
cargo run -- classify-log path/to/build.log
cargo run -- cache stats
cargo run -- cache prune
cargo run -- cache warm --top-packages 5000
cargo run -- cache warm --high-centrality 50
```

## Benchmark Wrapper

The benchmark-compatible entrypoint is still `test_executor.py`:

```bash
python3 test_executor.py -f tests/fixtures/sample_snippet.py -v --no-validate
```

It accepts the common benchmark flags (`-m`, `-b`, `-l`, `-r`, `-ra`) and forwards them to the Rust CLI. When validation is enabled, the wrapper now returns a non-zero exit code if APDR validation fails.

## Modernization guardrails

Run these from the repo root when touching the Rust modernization phases:

```bash
cargo fmt --manifest-path tools/apdr/Cargo.toml --all --check
```

Verifies formatting before review or benchmark comparisons.

```bash
cargo clippy --manifest-path tools/apdr/Cargo.toml --all-targets -- -D warnings
```

Keeps touched Rust code aligned with lint expectations before phase commits land.

```bash
cargo test --manifest-path tools/apdr/Cargo.toml
```

Runs the Rust regression suite before and after hotspot refactors.

```bash
python scripts/measure_apdr_baseline.py --fixtures-root tools/apdr/tests/fixtures --limit 3 --validation-backend env --output-json .planning/phases/01-baseline-and-guardrails/01-baseline.json --output-md .planning/phases/01-baseline-and-guardrails/01-BASELINE.md
```

Refreshes the bounded timing and pass-rate baseline used by the modernization milestone.

```bash
python scripts/profile_apdr_memory.py --snippet tools/apdr/tests/fixtures/sample_snippet.py --validation-backend env --output-json .planning/phases/01-baseline-and-guardrails/01-memory-profile.json
```

Captures the representative `peak_rss_bytes` snapshot used in hotspot ranking.

```bash
python scripts/check_apdr_regression.py --baseline .planning/phases/01-baseline-and-guardrails/01-baseline.json --candidate <candidate-json>
```

Compares a candidate run against the committed Phase 1 baseline and fails when pass rate or timing regress beyond explicit thresholds.

## Output Files

APDR writes:

- `requirements.txt`
- `resolution-report.txt`
- `output_data_<python-version>.yml`

The report includes cache hits, heuristic hits, LLM calls, retries, unresolved imports, validation attempts, selected Python version, lockfile key, and validation artifact metadata.

## Runtime Requirements

- Matching local Python interpreters for any versions APDR needs to validate
- Optional Python managers for auto-install support: `uv`, `mise`, `pyenv`, `asdf`, `winget`, `scoop`, or Homebrew
- Optional `ollama` on `PATH` if `--allow-llm` is used
- Network access to PyPI for uncached version discovery

## Cache Layout

APDR uses `APDR_CACHE_DIR` when set. Otherwise it prefers a per-user cache directory outside the repo tree when the platform exposes one, and only falls back to `tools/apdr/.apdr-cache` when no external cache root is available.

The APDR cache root stores:

- dynamic import-to-package mappings
- version constraints
- resolved lockfiles
- build artifact tags
- learned failure patterns with success-rate tracking
- cached PyPI version indexes
- a bounded `validated-envs/` cache for recent successful local environments
- a pip `wheelhouse/` cache for repeated installs

`cargo run -- cache stats` now reports disk usage for the heavy cache directories. `cargo run -- cache prune` removes legacy `pip-cache/`, removes the opt-out `package-repository/` cache when it is disabled, and trims `validated-envs/` down to the configured retention limits.

By default APDR keeps up to 24 validated envs and up to 8 GiB of validated-env cache data. You can override those limits with `APDR_VALIDATED_ENV_CACHE_MAX_ENTRIES` and `APDR_VALIDATED_ENV_CACHE_MAX_GB`. Set `APDR_ENABLE_PACKAGE_REPOSITORY_CACHE=1` only if you explicitly want the much larger package-repository cache back.

To inspect or reclaim local APDR footprint safely:

```bash
bash scripts/cleanup-apdr-footprint.sh --dry-run
bash scripts/cleanup-apdr-footprint.sh --apply
```

The cleanup helper honors `APDR_CACHE_DIR`, `APDR_TARGET_DIR`, `--cache-path`, and `--target-dir`, reports cache plus local target sizes before deleting anything, and requires `--apply` for destructive cleanup.

## Notes

The current implementation completes the planned end-to-end workflow, but it uses lightweight file-backed cache and subprocess orchestration in place of the heavier RocksDB / bollard stack described in the research-oriented design document.
