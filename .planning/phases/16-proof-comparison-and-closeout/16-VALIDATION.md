---
phase: 16
slug: proof-comparison-and-closeout
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-03-29
---

# Phase 16 - Validation Strategy

> Validation contract for milestone closeout evidence intake, reviewer-facing comparison notes, and honest requirement reconciliation.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python proof scripts, structural `rg` checks, and the carried-forward Phase 14 and Phase 15 checkers |
| **Config file** | `tools/apdr/Cargo.toml` |
| **Quick run command** | `python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json /tmp/phase16-closeout-status.json` |
| **Full suite command** | `python3 -m py_compile scripts/check_phase16_closeout.py && python3 scripts/check_phase14_macos_replay.py --macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --macos-md .planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md --windows-md .planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md && python3 scripts/check_phase15_agent_quality.py --baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json && python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json /tmp/phase16-closeout-status.json` |
| **Estimated runtime** | ~3-10 minutes for sample-contract validation; longer if live macOS, Windows, and tier3 artifacts are captured during execution |

---

## Sampling Rate

- **After every task commit:** Run the task-specific structural or checker command listed below
- **After every plan wave:** Run the full suite command
- **Before `$gsd-verify-work`:** The Phase 14 checker, Phase 15 checker, and the new Phase 16 closeout checker must all be green for the selected evidence mode
- **Max feedback latency:** keep task-level checks under 5 minutes by preferring `py_compile`, `rg`, and sample-artifact proof commands before any live host capture

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 16-01-01 | 01 | 1 | EVD-04, EVD-06 | closeout checker compile | `python3 -m py_compile scripts/check_phase16_closeout.py` | no | pending |
| 16-01-02 | 01 | 1 | EVD-04, EVD-06 | sample evidence-status generation | `python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json --evidence-md .planning/phases/16-proof-comparison-and-closeout/16-CLOSEOUT-EVIDENCE.md` | no | pending |
| 16-02-01 | 02 | 2 | EVD-04, EVD-06 | macOS and Windows proof-note package | `python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json --macos-md .planning/phases/16-proof-comparison-and-closeout/16-MACOS-COMPARISON.md --windows-md .planning/phases/16-proof-comparison-and-closeout/16-WINDOWS-NONREGRESSION.md` | no | pending |
| 16-02-02 | 02 | 2 | EVD-04, EVD-06 | LLM-quality delta note | `python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json --llm-md .planning/phases/16-proof-comparison-and-closeout/16-LLM-QUALITY-DELTA.md` | no | pending |
| 16-03-01 | 03 | 3 | EVD-04, EVD-06 | carried-forward proof suite | `python3 scripts/check_phase14_macos_replay.py --macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --macos-md .planning/phases/14-macos-execution-path-optimization/14-MACOS-REPLAY.md --windows-md .planning/phases/14-macos-execution-path-optimization/14-WINDOWS-GUARDRAIL.md && python3 scripts/check_phase15_agent_quality.py --baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json && python3 scripts/check_phase16_closeout.py --phase14-macos-before .planning/phases/14-macos-execution-path-optimization/14-macos-before-sample.json --phase14-macos-after .planning/phases/14-macos-execution-path-optimization/14-macos-after-sample.json --phase14-windows-before .planning/phases/14-macos-execution-path-optimization/14-windows-before-sample.json --phase14-windows-after .planning/phases/14-macos-execution-path-optimization/14-windows-after-sample.json --phase15-baseline .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-baseline-sample.json --phase15-candidate .planning/phases/15-langchain-langgraph-tier3-intelligence-improvements/15-tier3-candidate-sample.json --status-json .planning/phases/16-proof-comparison-and-closeout/16-closeout-evidence-status.json --macos-md .planning/phases/16-proof-comparison-and-closeout/16-MACOS-COMPARISON.md --windows-md .planning/phases/16-proof-comparison-and-closeout/16-WINDOWS-NONREGRESSION.md --llm-md .planning/phases/16-proof-comparison-and-closeout/16-LLM-QUALITY-DELTA.md` | yes | pending |
| 16-03-02 | 03 | 3 | EVD-04, EVD-06 | milestone closeout and requirement truth | `rg -n 'EVD-04|EVD-06|sample|live|Final Signoff|milestone closeout' .planning/phases/16-proof-comparison-and-closeout/16-MILESTONE-CLOSEOUT.md .planning/REQUIREMENTS.md .planning/ROADMAP.md .planning/STATE.md` | no | pending |

---

## Wave 0 Requirements

- Existing infrastructure covers the phase.
- Phase 16 adds one new closeout checker and one new machine-readable evidence-status artifact, but it should reuse the Phase 14 and Phase 15 checkers rather than replacing them.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The milestone closeout note does not blur sample-contract proof with live benchmark evidence | EVD-04, EVD-06 | Automation can verify headings and fields, but a reviewer still needs to judge whether the narrative is honest about what was and was not measured live | Read `16-MILESTONE-CLOSEOUT.md`, then confirm it names the evidence mode explicitly in both the requirement verdicts and the final signoff |
| The macOS, Windows, and LLM-quality notes point to prior proof artifacts instead of duplicating tables inconsistently | EVD-04, EVD-06 | A checker can verify links and headings, but a reviewer still needs to confirm the split package stays concise and non-duplicative | Read `16-MACOS-COMPARISON.md`, `16-WINDOWS-NONREGRESSION.md`, and `16-LLM-QUALITY-DELTA.md`, then confirm each one cites the relevant Phase 14 or Phase 15 proof note rather than re-inventing a second data source |

---

## Validation Sign-Off

- [x] All planned tasks have an automated verify step or explicit manual-only review instruction
- [x] Sampling continuity keeps the Phase 14 and Phase 15 proof checkers in the final loop
- [x] Existing infrastructure covers the phase
- [x] No watch-mode commands are required
- [x] Feedback latency remains bounded by `py_compile`, `rg`, and sample-artifact validation before any live host capture
- [x] `nyquist_compliant: true` is set in frontmatter

**Approval:** planned 2026-03-29
