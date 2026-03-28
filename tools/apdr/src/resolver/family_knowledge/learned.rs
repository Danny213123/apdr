use super::normalize;

// =============================================================================
// Learned Family Knowledge System
// =============================================================================
// This system allows APDR to learn new package families from LLM discoveries
// and persist them for future use. Learned families are stored in JSON and
// merged with the static family knowledge at runtime.

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearnedFamilyMember {
    pub package: String,
    pub modules: Vec<String>,
    pub status: String, // "active", "deprecated", "unmaintained"
    pub preferred: bool,
    pub learned_from_case: Option<String>, // Track which case taught us this
    pub confidence_score: f64,             // Track how confident we are in this mapping
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LearnedFamily {
    pub name: String,
    pub modules: Vec<String>,
    pub conflict_kind: String, // "namespace", "fork", "variant", "replacement", "migration"
    pub members: Vec<LearnedFamilyMember>,
    pub notes: String,
    pub learned_at: String,              // Timestamp
    pub learned_from_cases: Vec<String>, // Track all cases that contributed
}

static LEARNED_FAMILIES: Lazy<Mutex<Vec<LearnedFamily>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// Get the path to the learned families JSON file
pub fn learned_families_path() -> PathBuf {
    if let Ok(cache_dir) = std::env::var("APDR_CACHE_DIR") {
        PathBuf::from(cache_dir).join("learned_families.json")
    } else {
        PathBuf::from(".apdr-cache").join("learned_families.json")
    }
}

/// Load learned families from disk
pub fn load_learned_families() -> Result<Vec<LearnedFamily>, String> {
    let path = learned_families_path();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content =
        fs::read_to_string(&path).map_err(|e| format!("Failed to read learned families: {}", e))?;

    let families: Vec<LearnedFamily> = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse learned families: {}", e))?;

    Ok(families)
}

/// Save learned families to disk
pub fn save_learned_families(families: &[LearnedFamily]) -> Result<(), String> {
    let path = learned_families_path();

    // Ensure directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create cache directory: {}", e))?;
    }

    let json = serde_json::to_string_pretty(families)
        .map_err(|e| format!("Failed to serialize families: {}", e))?;

    fs::write(&path, json).map_err(|e| format!("Failed to write learned families: {}", e))?;

    Ok(())
}

/// Initialize the learned families system
pub fn init_learned_families() {
    if let Ok(families) = load_learned_families() {
        if let Ok(mut learned) = LEARNED_FAMILIES.lock() {
            *learned = families;
        }
    }
}

/// Add a new learned family or update existing one
pub fn add_learned_family(
    family_name: &str,
    module_names: &[&str],
    package_name: &str,
    alternative_packages: &[(&str, bool)], // (package, is_preferred)
    conflict_kind: &str,
    case_id: &str,
    confidence: f64,
) -> Result<(), String> {
    let mut learned = LEARNED_FAMILIES
        .lock()
        .map_err(|_| "Failed to lock learned families")?;

    let now = current_timestamp_string();

    // Check if family already exists
    if let Some(existing) = learned.iter_mut().find(|f| f.name == family_name) {
        // Update existing family
        existing.learned_from_cases.push(case_id.to_string());

        // Add modules if not present
        for module in module_names {
            if !existing.modules.contains(&module.to_string()) {
                existing.modules.push(module.to_string());
            }
        }

        // Add members if not present
        let mut member_packages: Vec<String> = existing
            .members
            .iter()
            .map(|member| member.package.clone())
            .collect();

        if !member_packages
            .iter()
            .any(|package| package == package_name)
        {
            existing.members.push(LearnedFamilyMember {
                package: package_name.to_string(),
                modules: module_names.iter().map(|s| s.to_string()).collect(),
                status: "active".to_string(),
                preferred: false,
                learned_from_case: Some(case_id.to_string()),
                confidence_score: confidence,
            });
            member_packages.push(package_name.to_string());
        }

        for (alt_pkg, is_pref) in alternative_packages {
            if !member_packages.iter().any(|package| package == alt_pkg) {
                existing.members.push(LearnedFamilyMember {
                    package: alt_pkg.to_string(),
                    modules: module_names.iter().map(|s| s.to_string()).collect(),
                    status: "active".to_string(),
                    preferred: *is_pref,
                    learned_from_case: Some(case_id.to_string()),
                    confidence_score: confidence,
                });
                member_packages.push(alt_pkg.to_string());
            }
        }
    } else {
        // Create new family
        let mut members = vec![LearnedFamilyMember {
            package: package_name.to_string(),
            modules: module_names.iter().map(|s| s.to_string()).collect(),
            status: "active".to_string(),
            preferred: false,
            learned_from_case: Some(case_id.to_string()),
            confidence_score: confidence,
        }];

        for (alt_pkg, is_pref) in alternative_packages {
            members.push(LearnedFamilyMember {
                package: alt_pkg.to_string(),
                modules: module_names.iter().map(|s| s.to_string()).collect(),
                status: "active".to_string(),
                preferred: *is_pref,
                learned_from_case: Some(case_id.to_string()),
                confidence_score: confidence,
            });
        }

        learned.push(LearnedFamily {
            name: family_name.to_string(),
            modules: module_names.iter().map(|s| s.to_string()).collect(),
            conflict_kind: conflict_kind.to_string(),
            members,
            notes: format!("Learned from LLM recovery in case {}", case_id),
            learned_at: now,
            learned_from_cases: vec![case_id.to_string()],
        });
    }

    // Save to disk
    save_learned_families(&learned)?;

    Ok(())
}

/// Check if a package/module combination exists in learned families
pub fn check_learned_family(module_name: &str, package_name: &str) -> Option<LearnedFamily> {
    let learned = LEARNED_FAMILIES.lock().ok()?;

    learned
        .iter()
        .find(|f| {
            f.modules
                .iter()
                .any(|m| normalize(m) == normalize(module_name))
                && f.members
                    .iter()
                    .any(|mem| normalize(&mem.package) == normalize(package_name))
        })
        .cloned()
}

/// Get all learned alternatives for a module
pub fn get_learned_alternatives(module_name: &str) -> Vec<String> {
    let Some(learned) = LEARNED_FAMILIES.lock().ok() else {
        return Vec::new();
    };
    let normalized_module = normalize(module_name);

    let mut alternatives = Vec::new();
    for family in learned.iter() {
        if family
            .modules
            .iter()
            .any(|m| normalize(m) == normalized_module)
        {
            for member in &family.members {
                if !alternatives.contains(&member.package) {
                    alternatives.push(member.package.clone());
                }
            }
        }
    }

    alternatives
}

fn current_timestamp_string() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}

