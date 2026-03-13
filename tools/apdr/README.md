# APDR

APDR is the agentic dependency resolver in this repository. It now runs the full planned workflow for a snippet:

1. Parse imports and adjacent config files
2. Detect a likely Python version window
3. Resolve packages through seeded cache, heuristics, and optional Ollama fallback
4. Expand known transitive dependencies
5. Generate `requirements.txt` and a resolution report
6. Validate the environment in Docker with retries and error-driven recovery
7. Persist learned mappings, version data, lockfiles, build artifacts, and recovery patterns in `.apdr-cache/`

## Build

```bash
cd /Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr
./build.sh
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

Validate with Docker and optional Ollama fallback:

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
cargo run -- cache warm --top-packages 5000
cargo run -- cache warm --high-centrality 50
```

## Benchmark Wrapper

The benchmark-compatible entrypoint is still `test_executor.py`:

```bash
python3 test_executor.py -f tests/fixtures/sample_snippet.py -v --no-validate
```

It accepts the common benchmark flags (`-m`, `-b`, `-l`, `-r`, `-ra`) and forwards them to the Rust CLI. When validation is enabled, the wrapper now returns a non-zero exit code if Docker validation fails.

## Output Files

APDR writes:

- `requirements.txt`
- `resolution-report.txt`
- `output_data_<python-version>.yml`

The report includes cache hits, heuristic hits, LLM calls, retries, unresolved imports, validation attempts, selected Python version, lockfile key, and Docker image id.

## Runtime Requirements

- `docker` on `PATH` if validation is enabled
- Optional `ollama` on `PATH` if `--allow-llm` is used
- Network access to PyPI for uncached version discovery

## Cache Layout

`.apdr-cache/` stores:

- dynamic import-to-package mappings
- version constraints
- resolved lockfiles
- build artifact tags
- learned failure patterns with success-rate tracking
- cached PyPI version indexes

## Notes

The current implementation completes the planned end-to-end workflow, but it uses lightweight file-backed cache and subprocess orchestration in place of the heavier RocksDB / bollard stack described in the research-oriented design document.
