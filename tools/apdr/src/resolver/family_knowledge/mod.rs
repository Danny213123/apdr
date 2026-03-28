//! Family knowledge facade for package-family recovery rules.
//!
//! This facade owns the public family knowledge entrypoints that the resolver
//! uses to apply or recover package-family guidance. The detailed implementation
//! lives in sibling modules: `legacy_bundles` owns curated bundle definitions
//! and compatibility pins, `learned` owns learned-family persistence, and
//! `detection` owns family detection and namespace helpers.
mod data;
mod core;
mod detection;
mod learned;
mod legacy_bundles;
pub use data::{
    curated_family_knowledge_snapshot, init_curated_family_knowledge, load_curated_family_knowledge,
    touched_families_path, touched_recovery_rules_path, with_curated_family_knowledge,
    CuratedBundleMember, CuratedBundleVariant, CuratedConflictKind,
    CuratedExplicitNamespaceMapping, CuratedFamilyKnowledge, CuratedFamilyMember,
    CuratedMemberStatus, CuratedPackageFamily, CuratedPythonPreferenceOrder,
    CuratedRecoveryRule, CuratedRecoveryRuleKind, CuratedRuntimeScope,
};
pub use core::*;
pub use detection::{namespace_mapping_allowed, normalize};
pub use learned::{
    add_learned_family, check_learned_family, get_learned_alternatives, init_learned_families,
    learned_families_path, load_learned_families, save_learned_families, LearnedFamily,
    LearnedFamilyMember,
};
