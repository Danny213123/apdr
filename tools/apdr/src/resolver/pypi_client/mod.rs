//! PyPI client facade for package metadata and compatibility lookup.
//!
//! This facade owns the public PyPI client entrypoints used by the resolver to
//! fetch versions, choose compatible releases, and load dependency metadata.
//! The detailed implementation lives in sibling modules: `smartpip` owns the
//! SmartPip and KGraph orchestration, `version_matching` owns version parsing
//! and constraint checks, and `host_python` owns host-Python helper commands.
mod core;
mod host_python;
mod smartpip;
mod version_matching;
pub use core::*;
pub use version_matching::version_satisfies;
