//! Tier 3 LLM facade for late-stage recovery and package hints.
//!
//! This facade owns the public Tier 3 LLM entrypoints that assess solvability,
//! request recovery candidates, and feed late-stage package hints back into the
//! resolver. The detailed implementation lives in sibling modules: `process`
//! owns Python subprocess management, `context` owns request-context assembly,
//! and `failure_memory` owns persisted failure history.
mod context;
mod core;
mod failure_memory;
mod process;
pub use core::*;
