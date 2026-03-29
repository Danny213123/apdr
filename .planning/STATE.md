---
gsd_state_version: 1.0
milestone: v2.2
milestone_name: Improve LLM Performance and Benchmark Performance on macOS
status: planning
last_updated: "2026-03-28T23:59:00Z"
last_activity: 2026-03-28
---

# Project State: APDR

**Last Updated:** 2026-03-28
**Status:** Planning milestone requirements
**Progress:** [░░░░░░░░░░] 0%
**Last Activity:** 2026-03-28
**Last Activity Description:** Milestone v2.2 started
**Resume File:** None

---

## Project Reference

**Core Value:** APDR must stay correct under benchmark pressure while the Rust core remains fast enough and clear enough to evolve without fighting the codebase.

**Current Focus:** Define v2.2 requirements and roadmap for LLM-agent quality plus macOS benchmark performance

---

## Current Position

Phase: Not started (defining requirements)
Plan: —
Status: Planning milestone requirements
Last activity: 2026-03-28 — Milestone v2.2 started

---

## Performance Metrics

- Milestone reset in progress — roadmap phases and plan metrics will be populated after v2.2 is defined.

---

## Accumulated Context

### Decisions

- v2.0 closed the Rust modernization track and remains the last fully shipped milestone.
- v2.1 delivered data-driven family knowledge and targeted recovery changes, but its live-proof closeout was left unfinished when the project moved on.
- v2.2 starts with an explicit constraint: improve LLM-agent behavior without depending on more hardcoded deterministic recovery fixes.

### Pending Todos

- Decide which unfinished v2.1 live-proof artifacts should be retired, reused, or explicitly carried as historical debt during v2.2.
- Keep benchmark evidence comparable while changing agent behavior and macOS execution paths.

### Blockers/Concerns

- The repo still contains unfinished v2.1 recovery-proof debt, so v2.2 docs must avoid implying those claims were closed.
- macOS benchmark measurement needs to be strong enough to compare both quality and runtime deltas.

---

## Session Continuity

Last session: 2026-03-28
Stopped at: Started milestone v2.2 planning
Resume file: None

---

*State updated after starting milestone v2.2 on 2026-03-28*
