use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn key_for(requirements: &str, python_version: &str) -> String {
    let mut hasher = DefaultHasher::new();
    requirements.hash(&mut hasher);
    python_version.hash(&mut hasher);
    format!("build-{:x}", hasher.finish())
}
