# Changelog

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
