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
