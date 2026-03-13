# Changelog

## 0.2.0 - 2026-03-13

- Replaced the standalone benchmark desktop UI with the web app in [`web/`](/Users/dannyguan/Documents/fse-aiware-python-dependencies/web) and [`benchmark_ui/`](/Users/dannyguan/Documents/fse-aiware-python-dependencies/benchmark_ui), including separate benchmark-view routing, custom dropdowns on macOS, saved-run load/resume support, doctor auto-fix flows, and live `sec/case` pacing updates.
- Built out APDR in [`tools/apdr/`](/Users/dannyguan/Documents/fse-aiware-python-dependencies/tools/apdr) with seeded alias coverage, family-aware resolution, solvability checks, detailed debug artifacts, clearer validation statuses, and local-Python validation instead of Docker-first execution.
- Added SMTpip-informed metadata reuse and smartPip-style package-repository reuse for APDR validation, along with per-run benchmark context logs and richer failure classification.
- Improved benchmark scoring and historical-run loading so stale outputs, artifact pollution, and false positive passes are handled more accurately across APDR and PLLM.
- Updated docs and runtime defaults, including the web benchmark launcher, default APDR search range `5`, and the new release version markers.
