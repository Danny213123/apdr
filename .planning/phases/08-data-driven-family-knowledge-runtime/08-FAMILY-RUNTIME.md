# Phase 8 Family Runtime

## Data Files

- `tools/apdr/data/family_knowledge/touched_families.json` is the bounded touched-runtime registry for Phase 8. It owns the curated registry families `setuptools`, `pil`, and `sklearn`, the bundle families `legacy-pymc3`, `legacy-tensorflow`, and `legacy-ggplot`, plus the explicit namespace mappings that stay inside the touched migration boundary.
- `tools/apdr/data/family_knowledge/touched_recovery_rules.json` is the matching touched-runtime recovery source. It owns the `pkg-resources`, `legacy-pillow`, `legacy-pymc3`, `legacy-tensorflow`, `keras-backend`, and `legacy-ggplot` rules that drive retry, bundle, and note behavior.
- The required explicit namespace mappings for the deterministic closeout check are `pkg_resources -> setuptools`, `Image -> Pillow`, and `sklearn -> scikit-learn`. The broader touched PIL aliases remain in the same curated file so the runtime note and the resolver share one source of truth.

## Touched Runtime Coverage

Phase 8 moves the current family-owned behavior for those `17 snapshot cases` into validated data files while keeping the canonical `70-case` slice and the `17 overlap cases` outside the Phase 7 contract stable for later comparison work.

| Surface | Curated owner | Protected Phase 7 case IDs | Preserved runtime outcome |
| --- | --- | --- | --- |
| `pkg_resources` / setuptools | family `setuptools`, rule `pkg-resources` | `1e2600ed62d5e76b21ee`, `263113`, `3a6e4d618afc344aab81` | missing-module recovery still adds `setuptools` for `pkg_resources` |
| PIL / Pillow | family `pil`, rule `legacy-pillow` | `2e3b989e0343f0884388ed7ed82eb3b0`, `33e2172bafbb5dd794ab`, `3682135` | Python 2 PIL-era fixtures still pin `Pillow==6.2.2` |
| sklearn shim | family `sklearn` | `28bf77e9a95ae6b70b14141feacb1f84` | `sklearn` stays mapped to `scikit-learn` |
| legacy PyMC3 stack | family `legacy-pymc3`, rule `legacy-pymc3` | `2de2e9a156fe619dbdad762fe1cf84e1`, `4882342eba2b57376ed1` | legacy PyMC3 fixtures keep the coherent bundle anchored by `pymc3==3.11.5` and `Theano-PyMC==1.1.2` |
| legacy TensorFlow / standalone keras | family `legacy-tensorflow`, rules `legacy-tensorflow` and `keras-backend` | `0830affa1f7f19fd47b06d4cf89ed44d`, `0a3d4fae965bdbec1f9d`, `0bdd7059a08cbcd00898`, `187895beb89f0a1b3a54`, `1d878d0401b28b281eb75016ed29f2ee`, `31eee50b9aaebf387b380f70054575c5`, `3a2a081e4f3089920fd8aecefecbe280`, `3fdd80a08808bd275142d46863e92d68` | standalone keras fixtures still gain the TensorFlow backend companion and the TensorFlow family bundle stays data-driven |
| legacy ggplot | family `legacy-ggplot`, rule `legacy-ggplot` | `1e2600ed62d5e76b21ee`, `3a6e4d618afc344aab81` | ggplot compatibility pins remain owned by the touched curated bundle instead of hardcoded branches |

## Diagnostics

- Curated loader failures are expected to stop the touched runtime before new data takes effect. The targeted resolver tests already assert deterministic errors for duplicate family names, duplicate explicit namespace mappings, unknown rule families, empty trigger sets, and invalid bundle-member references.
- The runtime now initializes curated family knowledge at resolver entry, so invalid touched data fails as an actionable initialization error instead of silently falling back to stale touched behavior.
- The bounded verification commands for this phase are:
  - `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver data_driven_family_ -- --nocapture`
  - `cargo test --manifest-path tools/apdr/Cargo.toml --test test_resolver phase7_family_ -- --nocapture`
  - `python scripts/check_phase8_family_runtime.py --family-manifest .planning/phases/07-failure-baseline-parity-slice/07-family-snapshot-manifest.json --families-json tools/apdr/data/family_knowledge/touched_families.json --recovery-json tools/apdr/data/family_knowledge/touched_recovery_rules.json --baseline-md .planning/phases/08-data-driven-family-knowledge-runtime/08-FAMILY-RUNTIME.md`

## Phase 9 Handoff

Phase 9 should build on the stabilized touched runtime and the deterministic Phase 8 checker. Accuracy work can refine recovery quality, bundle selection, and benchmark coverage, but it should not reopen the migration boundary for the `17 snapshot cases`, the canonical `70-case` slice, or the `17 overlap cases` watchlist without an explicit roadmap change.

The operating rule for the next phase is simple: use the curated files and the Phase 7 snapshot corpus as the source of truth, and do not reopen the migration boundary just to re-litigate behavior that Phase 8 already locked behind tests and the closeout checker.
