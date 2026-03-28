# Phase 9 Targeted Recovery

## Target Scope

Phase 9 improves recovery accuracy on the bounded canonical slice that Phase 7 locked and Phase 8 carried forward. The work targets `version-not-found` and `dependency-conflict` failure buckets with bounded compatibility policies, plus `module-not-found` with targeted provider rules and stop reasons.

The canonical surface remains unchanged:

- 70 canonical tier3 cases in scope (from `07-tier3-parity-manifest.json`)
- 17 watchlist cases outside the canonical contract
- 87 APDR-failed but pllm-passing overlap cases

Phase 9 does not redefine or widen this surface.

## Module Recovery Coverage

Phase 9 adds targeted module-provider and stop-reason rules in `tools/apdr/data/recovery/module_rules.json`. These deterministic rules fire before generic mapping-failure fallbacks in the retry loop.

| Rule Type | ID | Target | Behavior |
|---|---|---|---|
| Provider | `mod-pkg-resources` | `pkg_resources` | Maps to `setuptools` |
| Provider | `mod-pil-image` | `Image` | Maps to `Pillow` |
| Provider | `mod-django-rest-framework` | `rest_framework` | Maps to `djangorestframework` |
| Stop reason | `stop-removed-runtime` | `imp`, `numpy.distutils`, `elementtree` | Stops retry with "removed-runtime" classification |
| Stop reason | `stop-project-local` | `api`, `taggit_autocomplete`, `pizzanuvola_teaser`, etc. | Stops retry with "project-local" classification |
| Stop reason | `stop-internal-extension` | `_distance_wrap`, `gisutils`, `clips` | Stops retry with "internal-extension" classification |

These rules are backed by `phase9_targeted_module_` regression tests in `tools/apdr/tests/test_resolver.rs`.

## Compatibility Recovery Coverage

Phase 9 adds targeted compatibility clusters, companion rules, and Python ceiling rules in `tools/apdr/data/recovery/compatibility_rules.json`. These fire before broad contradictory-pin exhaustion and strip-all fallbacks.

| Cluster ID | Anchor Packages | Preferred Versions | Family Ref |
|---|---|---|---|
| `compat-torch` | torch, torchvision | torch==2.1.0, torchvision==0.16.0 | (none) |
| `compat-tensorflow` | tensorflow, keras, tensorboard | tensorflow==2.15.0, keras==2.15.0, tensorboard==2.15.0 | `legacy-tensorflow` |
| `compat-scikit-learn` | scikit-learn | scikit-learn==1.3.2 | (none) |
| `compat-pymc` | pymc, pymc3 | pymc==5.10.0, numpy==1.26.4 | (none) |
| `compat-mitmproxy` | mitmproxy | mitmproxy==10.1.6 | (none) |
| `compat-odfpy` | odfpy | odfpy==1.4.1 | (none) |
| `compat-setuptools` | setuptools | setuptools==69.5.1 | (none) |
| `compat-numpy` | numpy | numpy==1.26.4 | (none) |

The `compat-tensorflow` cluster explicitly references the Phase 8 curated family runtime (`legacy-tensorflow`) rather than introducing a separate hardcoded bundle path.

### Companion Rules

| Rule ID | Trigger Package | Companion | Notes |
|---|---|---|---|
| `companion-pyjwt` | PyJWT | cryptography | PyJWT>=2.0.0 often needs cryptography for RS256 |
| `companion-python-dateutil` | python-dateutil | six | python-dateutil<2.0,>=2.1 depends on six |

### Python Ceiling Rules

| Rule ID | Trigger Package | Max Python | Notes |
|---|---|---|---|
| `ceiling-pymc3` | pymc3 | 3.11 | Legacy PyMC3 stack incompatible with Python 3.12+ |
| `ceiling-numpy-legacy` | numpy | 3.12 | numpy<1.24 does not support Python 3.12+ |

### Transitive Specifier Normalization

Phase 9 adds `normalize_requirement_spec` in `recovery_diagnostics.rs` to parse requirement strings like `PyJWT>=2.0.0` and `python-dateutil<2.0,>=2.1` into a package key plus constraint. This gives the retry loop a clean package name for companion and cluster lookups that the standard `extract_package_and_version` (which only handles `==` pins) would miss.

These rules are backed by `phase9_targeted_compatibility_` regression tests in `tools/apdr/tests/test_resolver.rs`.

## Diagnostics

### Recoverable vs Non-Recoverable Cases

The Phase 9 policy layer classifies cases into three categories:

1. **Recoverable by targeted policy**: Cases where a module-provider rule, compatibility cluster, or companion rule can deterministically fix the failure before generic fallbacks fire.

2. **Stopped with explicit reason**: Cases where a stop-reason rule identifies the module as removed-runtime, project-local, or internal-extension. These cases skip further LLM recovery and report an inspectable stop reason.

3. **Deferred to generic recovery**: Cases not covered by the targeted policy layer. These fall through to the existing generic retry, LLM recovery, and strip-all fallback mechanisms unchanged.

### Diagnostics Contract

- The targeted recovery policy is loaded via `init_targeted_recovery_policy(tool_root)` at resolver entry and validated against the Phase 7 parity manifest.
- Invalid policy data (duplicate IDs, unknown case IDs, empty trigger sets) produces actionable initialization errors rather than silent fallback.
- The compatibility cluster lookup (`compatibility_cluster_for_log`) uses trigger substrings from the policy file, not hardcoded log patterns.
- The companion rule lookup (`companion_rule_for_package`) uses normalized package keys.
- The transitive specifier parser (`normalize_requirement_spec`) handles `==`, `>=`, `<=`, `!=`, `~=`, `>`, and `<` operators.

### Validation Commands

- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_policy_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_module_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase9_targeted_compatibility_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
- `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
- `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`
- `python scripts/check_phase9_targeted_recovery.py --parity-manifest .planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json --phase8-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md --phase9-md .planning/phases/09-targeted-tier3-recovery-accuracy/09-TARGETED-RECOVERY.md --module-rules tools/apdr/data/recovery/module_rules.json --compatibility-rules tools/apdr/data/recovery/compatibility_rules.json`

## Phase 10 Handoff

Phase 10 should rerun the benchmark against the locked Phase 7 parity slice and measure these exact bounded changes rather than redefining the slice. The expected measurement targets are:

1. **Targeted compatibility recovery**: Do the torch, tensorflow, scikit-learn, and other canonical clusters now recover instead of ending on contradictory-pin exhaustion?

2. **Transitive specifier normalization**: Do PyJWT, python-dateutil, and similar transitive-specifier cases now have clean package keys for downstream recovery?

3. **Module stop reasons**: Do removed-runtime, project-local, and internal-extension cases now stop with inspectable reasons instead of generic mapping failure?

4. **Phase 8 boundary preservation**: Does the Phase 8 family-runtime checker still pass after Phase 9 changes land?

The operating rule for Phase 10 is: measure the Phase 9 policy changes against the locked baseline, do not reopen the Phase 7 or Phase 8 boundaries, and do not redefine the canonical 70-case slice.
