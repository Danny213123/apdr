# Changelog

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
