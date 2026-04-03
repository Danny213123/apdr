# Phase 28 Recovery Truth Proof

Phase 28 proves two separate but connected truths:

- APDR now records bounded, machine-readable recovery attempts for `llm` and `llm-only` cases in `recovery-attempts.json`.
- Final case truth now distinguishes `llm-no-output`, `provider-tooling-failure`, `docker-infrastructure-failure`, and `dependency-runtime-failure` without removing the older coarse `failure_family`.

The applied sample freezes the optimistic path: the recovery attempt preserves authored-plan and Docker-artifact pointers together with the actual package change that APDR accepted.

The failure-truth sample freezes the non-pass path: the case keeps its coarse `failure_family`, but it also records a more precise `failure_truth_class` and `failure_truth_detail` so reviewers can see that a provider timeout is not the same thing as a dependency miss.

What Phase 28 does not claim:

- it does not claim a benchmark pass-rate gain yet
- it does not prove that `llm` and `llm-only` are faster or cheaper
- it does not replace live benchmark comparison evidence

Those claims are deferred to **Phase 29**, where the new recovery and truth surfaces become the fixed contract for before/after benchmark comparison.
