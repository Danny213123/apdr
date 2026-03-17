# Changelog

## 0.2.12 - 2026-03-17

- Reduced APDR pre-solve overhead by deduplicating direct packages with a set, avoiding unnecessary constraint-string clones during propagation, and streamlining lockfile rendering so version-domain checks allocate less while solving.
- Tightened APDR's cache and resolver hot paths with cheaper dotted-import/local-helper checks, single-lock knowledge-cache hydration during bulk KGraph prefetch, and lower-allocation requirement/operator parsing in the PyPI client.
- Optimized heuristic package matching by switching Levenshtein distance and wildcard/compatible-release checks to byte-based paths, cutting extra temporary allocations for the short ASCII package names APDR compares most often.

## 0.2.11 - 2026-03-17

- Added configurable benchmark worker counts across the terminal/web control plane and taught the runner to execute cases in parallel with safer summary/context-log writes, so larger APDR runs can use available CPU more effectively while still supporting sequential mode when needed.
- Expanded APDR's recovery and skip intelligence with persistent unsolvable-module learning, broader host/runtime skip detection, stronger local-helper heuristics, LLM-guided package replacement on failed validations, and safer Python 2 package-version fallback for legacy snippets.
- Improved APDR performance and validation hygiene with mimalloc-enabled release builds, higher-throughput immutable KGraph SQLite reads, more system dependency hints, atomic validated-env archive writes, temporary venv cleanup after validation, and refreshed alias seed data such as `imagekit -> django-imagekit`.

## 0.2.10 - 2026-03-16

- Added selectable APDR validation backends across the web UI, benchmark runner, saved loadouts, and Doctor flow, so runs can now target local env validation, Docker validation, or the new LLM resolver mode from the same control plane.
- Reintroduced Docker-based APDR validation with Rust-side backend routing, Dockerfile generation improvements, smarter apt package inference/retry handling, and richer per-attempt metadata so container validation is easier to diagnose and recover.
- Added the new `tools/apdr/docker_agent` LangGraph-style fallback pipeline plus shared system-dependency heuristics, allowing APDR's `llm` backend to try local env validation first and then escalate to an agent-guided repair loop when deterministic validation fails.

## 0.2.9 - 2026-03-15

- Fixed APDR's Windows 11 environment tooling by teaching interpreter discovery to resolve Windows launcher-managed installs through `py -<version>`, so launcher-managed Python runtimes can be selected for validation and environment creation instead of being reported missing.

## 0.2.8 - 2026-03-15

- Added APDR cache lifecycle controls, including disk-usage reporting in `cache stats`, a new `cache prune` command, compressed `validated-envs` archives, and retention limits for validated env and wheelhouse cache data.
- Improved APDR validation resilience by enforcing a total validation budget, pre-installing known build-time prerequisites for brittle packages, reusing cached validated environments more reliably, and making the package-repository cache opt-in instead of default-on.
- Expanded legacy dependency recovery with better Python 2 detection, new PIL/Pillow and Keras/TensorFlow family handling, refreshed seeded alias/version data, and regression coverage for mixed archive/directory cache entries plus legacy import cases.

## 0.2.7 - 2026-03-14

- Reduced APDR solve overhead by running the LLM solvability assessment only when tier1/tier2 still leave unresolved imports, which avoids several seconds of Ollama latency on already-resolved cases.
- Refactored the APDR dependency-resolution flow so the fast cache and heuristic tiers run before the optional solvability/LLM tier, while preserving skip behavior for genuinely unsolvable snippets.
- Fixed stdlib/module normalization in the AST parser by lowercasing loaded module names, which improves matching consistency against parser import output and seeded stdlib data.

## 0.2.6 - 2026-03-14

- Fixed APDR phase-timing propagation so top-level case outputs now preserve `solve`, `validation`, `install`, and `smoke` timings from the real validation attempts instead of dropping install/smoke totals during retry aggregation.
- Added explicit `env_create_duration_ms` reporting across APDR outputs, saved run summaries, and the benchmark runner contract so local-environment setup time is visible alongside install and smoke timing.
- Updated the Benchmark View to show `Env avg` in the live metrics header and `Env create` in expanded case details, giving live and historical runs a complete validation-phase breakdown for new APDR results.

## 0.2.5 - 2026-03-13

- Updated the Benchmark View completed-cases table to compare each case against the published PLLM, PYEGO, and READPY baselines, showing `MATCH`/`DIFF` badges plus detailed baseline summaries in the expanded case view instead of placeholder resolver markers.
- Adjusted PLLM comparison scoring so APDR `SKIP` outcomes count as a table match whenever the PLLM baseline did not pass that case, which makes host-runtime and intentionally skipped cases line up with the published baseline more honestly.
- Improved APDR runtime provisioning on macOS and Linux by adding APDR-managed Miniforge fallback for missing Python `3.7` and `3.8` interpreters, alongside broader resolver/runtime refinements for Python-version detection, pre-solve metadata handling, and richer LLM-assisted diagnostics.

## 0.2.4 - 2026-03-13

- Improved APDR's TensorFlow handling by removing the hardcoded modern `tensorflow==2.18.0` default, adding a legacy TensorFlow/Keras family bundle, and steering old standalone `keras` + `tensorflow` snippets toward coherent pins like `tensorflow==1.15.5`, `keras==2.3.1`, `numpy==1.16.6`, and `gym==0.17.3`.
- Expanded APDR's Python runtime support to include `3.7` and `3.8` across candidate-version selection, interpreter discovery, auto-install hints, and benchmark UI Doctor reporting so legacy ML stacks can validate against realistic runtimes instead of jumping straight from `2.7` to `3.9+`.
- Refreshed APDR's seeded version index and regression coverage for legacy TensorFlow-family cases, so SMT pre-solve falls back more honestly on incomplete metadata instead of failing early on the wrong TensorFlow assignment.

## 0.2.3 - 2026-03-13

- Added the new terminal CLI/TUI launcher for the benchmark suite, so the project now ships both the web interface and a keyboard-driven command center through `python -m benchmark_ui --cli`.
- Tightened APDR's legacy Python/runtime handling by capping Python `2.7` fallback expansion, improving missing-interpreter guidance, and removing stale Docker-era validation wording so fresh runs reflect the local-environment backend accurately.
- Improved APDR host-runtime and family-aware recovery logic, including deterministic skips for macOS Objective-C framework snippets and a curated legacy PyMC3 companion bundle that prevents generic recovery from drifting into impossible pins like `pandas==2.x` on legacy cases.

## 0.2.2 - 2026-03-13

- Added Windows 11 support across the web benchmark launcher and APDR runtime, including Windows interpreter discovery, `pyenv-win`/`uv` lookup, `apdr.exe` detection, and Windows-safe benchmark process management.
- Extended APDR Python auto-install on Windows with `winget` and `scoop`, and updated Doctor output so missing interpreter guidance reflects Windows launcher-managed installs and Windows package managers.
- Removed remaining hardcoded `python3` assumptions from APDR metadata and package-repository helper paths, and refreshed APDR docs/status messaging to reflect local-environment validation instead of Docker-specific wording.

## 0.2.1 - 2026-03-13

- Added APDR Python runtime auto-install support across [`tools/apdr/`](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr) and the web Doctor flow, with installer attempts through `uv`, `mise`, `pyenv`, `asdf`, and Homebrew when compatible versions are missing locally.
- Expanded APDR interpreter discovery to include managed installs from common framework locations plus `uv`, `pyenv`, `asdf`, `mise`, and Homebrew so new runtimes are picked up automatically after install.
- Improved APDR interpreter failure reporting so validation output shows which Python versions were missing, which installer paths were attempted, and why provisioning still failed when the host environment blocks installation.

## 0.2.0 - 2026-03-13

- Replaced the standalone benchmark desktop UI with the web app in [`web/`](/Users/dannyguan/Documents/fse-aiware-python-dependencies/web) and [`benchmark_ui/`](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui), including separate benchmark-view routing, custom dropdowns on macOS, saved-run load/resume support, doctor auto-fix flows, and live `sec/case` pacing updates.
- Built out APDR in [`tools/apdr/`](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr) with seeded alias coverage, family-aware resolution, solvability checks, detailed debug artifacts, clearer validation statuses, and local-Python validation instead of Docker-first execution.
- Added SMTpip-informed metadata reuse and smartPip-style package-repository reuse for APDR validation, along with per-run benchmark context logs and richer failure classification.
- Improved benchmark scoring and historical-run loading so stale outputs, artifact pollution, and false positive passes are handled more accurately across APDR and PLLM.
- Updated docs and runtime defaults, including the web benchmark launcher, default APDR search range `5`, and the new release version markers.
