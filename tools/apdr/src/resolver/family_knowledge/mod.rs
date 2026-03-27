//! Family knowledge facade for package-family recovery rules.
//!
//! This facade owns the public family knowledge entrypoints that the resolver
//! uses to apply or recover package-family guidance. The detailed implementation
//! lives in sibling modules: `legacy_bundles` owns curated bundle definitions
//! and compatibility pins, `learned` owns learned-family persistence, and
//! `detection` owns family detection and namespace helpers.
mod core;
mod detection;
mod learned;
mod legacy_bundles;
pub use core::*;
pub use detection::{namespace_mapping_allowed, normalize};
pub use learned::{
    LearnedFamily, LearnedFamilyMember, add_learned_family, check_learned_family,
    get_learned_alternatives, init_learned_families, learned_families_path,
    load_learned_families, save_learned_families,
};
