# Phase 9 Targeted Recovery Policy Data

**Scope:** Phase 9 targeted parity-slice scope only.

This directory contains deterministic recovery policy files for the bounded
Phase 9 accuracy improvement surface. These rules must stay anchored to:

1. **Phase 7 canonical parity manifest** (`07-tier3-parity-manifest.json`) --
   the 70-case canonical slice plus the 17-case watchlist that defines the
   improvement boundary.

2. **Phase 8 curated family runtime** (`data/family_knowledge/`) -- the
   touched-family data that Phase 8 stabilized. Compatibility rules that
   involve touched families (e.g., TensorFlow/Keras) should build on the
   curated family runtime instead of reintroducing hardcoded bundle logic.

## Files

- `module_rules.json` -- Module-provider rules (deterministic import-to-package
  mappings) and stop-reason rules (imports that should not trigger repeated
  recovery retries).
- `compatibility_rules.json` -- Compatibility clusters, companion package
  rules, and Python-ceiling rules for the canonical version-conflict and
  dependency-conflict cases.

## Adding Rules

When adding or modifying rules:

- Every `canonical_case_ids` entry must appear in the Phase 7 parity manifest.
- The loader validates duplicate IDs, duplicate aliases, duplicate anchor
  packages, empty trigger sets, and unknown case IDs at startup.
- Keep rules scoped to the canonical Phase 9 recovery surface. Do not expand
  rules to cover cases outside the parity slice without explicit phase
  transition.
