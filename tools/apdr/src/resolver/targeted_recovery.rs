//! Bounded Phase 9 targeted-recovery policy layer.
//!
//! This module defines serde-backed policy structs for the canonical parity-slice
//! recovery surface and provides a deterministic loader and validator. The policy
//! files live under `data/recovery/` and must stay anchored to the canonical
//! Phase 7 parity manifest (70-case slice + 17-case watchlist).
//!
//! The policy layer is intentionally separate from the Phase 8 curated family
//! knowledge in `data/family_knowledge/`, though rules may reference family IDs
//! or curated data where appropriate.

use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const MODULE_RULES_RELATIVE: &str = "data/recovery/module_rules.json";
const COMPATIBILITY_RULES_RELATIVE: &str = "data/recovery/compatibility_rules.json";
const PARITY_MANIFEST_RELATIVE: &str =
    ".planning/phases/07-failure-baseline-parity-slice/07-tier3-parity-manifest.json";

static TARGETED_RECOVERY_POLICY: OnceCell<Mutex<Option<TargetedRecoveryPolicy>>> = OnceCell::new();

// ---------------------------------------------------------------------------
// Public policy snapshot
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TargetedRecoveryPolicy {
    module_rules: Vec<ModuleProviderRule>,
    stop_reason_rules: Vec<StopReasonRule>,
    compatibility_clusters: Vec<CompatibilityCluster>,
    companion_rules: Vec<CompanionPackageRule>,
    python_ceiling_rules: Vec<PythonCeilingRule>,

    // lookup indexes
    module_rule_by_id: BTreeMap<String, usize>,
    module_rule_by_alias: BTreeMap<String, usize>,
    stop_reason_rule_by_id: BTreeMap<String, usize>,
    compatibility_cluster_by_id: BTreeMap<String, usize>,
    companion_rule_by_id: BTreeMap<String, usize>,
    python_ceiling_rule_by_id: BTreeMap<String, usize>,
}

// ---------------------------------------------------------------------------
// Serde structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ModuleProviderRule {
    pub id: String,
    pub module_aliases: Vec<String>,
    pub provider_package: String,
    #[serde(default)]
    pub canonical_case_ids: Vec<String>,
    #[serde(default)]
    pub notes: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StopReasonRule {
    pub id: String,
    pub module_patterns: Vec<String>,
    pub reason: String,
    #[serde(default)]
    pub canonical_case_ids: Vec<String>,
    #[serde(default)]
    pub notes: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompatibilityCluster {
    pub id: String,
    pub anchor_packages: Vec<String>,
    #[serde(default)]
    pub trigger_substrings: Vec<String>,
    #[serde(default)]
    pub replacement_packages: BTreeMap<String, String>,
    #[serde(default)]
    pub preferred_versions: BTreeMap<String, String>,
    #[serde(default)]
    pub companions: Vec<CompanionEntry>,
    #[serde(default)]
    pub python_floor: Option<String>,
    #[serde(default)]
    pub python_ceiling: Option<String>,
    #[serde(default)]
    pub canonical_case_ids: Vec<String>,
    #[serde(default)]
    pub family_ref: Option<String>,
    #[serde(default)]
    pub notes: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompanionEntry {
    pub package: String,
    #[serde(default)]
    pub version_constraint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompanionPackageRule {
    pub id: String,
    pub trigger_package: String,
    pub companion_package: String,
    #[serde(default)]
    pub version_constraint: Option<String>,
    #[serde(default)]
    pub canonical_case_ids: Vec<String>,
    #[serde(default)]
    pub notes: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PythonCeilingRule {
    pub id: String,
    pub trigger_package: String,
    pub max_python: String,
    #[serde(default)]
    pub canonical_case_ids: Vec<String>,
    #[serde(default)]
    pub notes: String,
}

// ---------------------------------------------------------------------------
// File-level serde wrappers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ModuleRulesFile {
    #[serde(default)]
    canonical_case_count: Option<usize>,
    #[serde(default)]
    module_provider_rules: Vec<ModuleProviderRule>,
    #[serde(default)]
    stop_reason_rules: Vec<StopReasonRule>,
}

#[derive(Debug, Deserialize)]
struct CompatibilityRulesFile {
    #[serde(default)]
    canonical_case_count: Option<usize>,
    #[serde(default)]
    compatibility_clusters: Vec<CompatibilityCluster>,
    #[serde(default)]
    companion_rules: Vec<CompanionPackageRule>,
    #[serde(default)]
    python_ceiling_rules: Vec<PythonCeilingRule>,
}

#[derive(Debug, Deserialize)]
struct ParityManifest {
    canonical_case_ids: Vec<String>,
    #[serde(default)]
    tier1_watchlist_case_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Read-only accessors on the policy snapshot
// ---------------------------------------------------------------------------

impl TargetedRecoveryPolicy {
    pub fn module_rules(&self) -> &[ModuleProviderRule] {
        &self.module_rules
    }

    pub fn stop_reason_rules(&self) -> &[StopReasonRule] {
        &self.stop_reason_rules
    }

    pub fn compatibility_clusters(&self) -> &[CompatibilityCluster] {
        &self.compatibility_clusters
    }

    pub fn companion_rules(&self) -> &[CompanionPackageRule] {
        &self.companion_rules
    }

    pub fn python_ceiling_rules(&self) -> &[PythonCeilingRule] {
        &self.python_ceiling_rules
    }

    pub fn module_rule_for_alias(&self, alias: &str) -> Option<&ModuleProviderRule> {
        self.module_rule_by_alias
            .get(&normalize_key(alias))
            .map(|idx| &self.module_rules[*idx])
    }

    pub fn stop_reason_for_module(&self, module: &str) -> Option<&StopReasonRule> {
        let key = normalize_key(module);
        for rule in &self.stop_reason_rules {
            for pattern in &rule.module_patterns {
                if normalize_key(pattern) == key || key.starts_with(&normalize_key(pattern)) {
                    return Some(rule);
                }
            }
        }
        None
    }

    pub fn compatibility_cluster_for_package(
        &self,
        package: &str,
    ) -> Option<&CompatibilityCluster> {
        let key = normalize_key(package);
        for cluster in &self.compatibility_clusters {
            for anchor in &cluster.anchor_packages {
                if normalize_key(anchor) == key {
                    return Some(cluster);
                }
            }
        }
        None
    }

    /// Find a compatibility cluster whose `trigger_substrings` match the log.
    pub fn compatibility_cluster_for_log(&self, log: &str) -> Option<&CompatibilityCluster> {
        let lower = log.to_ascii_lowercase();
        for cluster in &self.compatibility_clusters {
            for trigger in &cluster.trigger_substrings {
                if lower.contains(&trigger.to_ascii_lowercase()) {
                    return Some(cluster);
                }
            }
        }
        None
    }

    pub fn compatibility_cluster_for_resolved(
        &self,
        resolved: &[crate::ResolvedDependency],
    ) -> Option<&CompatibilityCluster> {
        self.compatibility_clusters.iter().find(|cluster| {
            cluster.anchor_packages.iter().any(|anchor| {
                let normalized_anchor = normalize_key(anchor);
                resolved
                    .iter()
                    .any(|dep| normalize_key(&dep.package_name) == normalized_anchor)
            })
        })
    }

    /// Find a companion-package rule for a given trigger package.
    pub fn companion_rule_for_package(&self, package: &str) -> Option<&CompanionPackageRule> {
        let key = normalize_key(package);
        self.companion_rules
            .iter()
            .find(|r| normalize_key(&r.trigger_package) == key)
    }

    /// Find a Python ceiling rule for a given trigger package.
    pub fn python_ceiling_for_package(&self, package: &str) -> Option<&PythonCeilingRule> {
        let key = normalize_key(package);
        self.python_ceiling_rules
            .iter()
            .find(|r| normalize_key(&r.trigger_package) == key)
    }
}

pub fn unsolvable_status_for_reason(reason: &str) -> &'static str {
    let category = reason
        .split_once(':')
        .map(|(prefix, _)| prefix)
        .unwrap_or(reason)
        .trim()
        .to_ascii_lowercase();
    match category.as_str() {
        "host-runtime" | "platform-specific" | "system-dependency" | "system-runtime" => {
            "skipped-host-runtime"
        }
        _ => "skipped-unsolvable",
    }
}

pub fn apply_compatibility_cluster(
    cluster: &CompatibilityCluster,
    resolved: &mut Vec<crate::ResolvedDependency>,
    strategy: &str,
) -> Vec<String> {
    let mut notes = Vec::new();

    for (existing_package, replacement_spec) in &cluster.replacement_packages {
        let existing_key = normalize_key(existing_package);
        if let Some(dep_index) = resolved
            .iter()
            .position(|dep| normalize_key(&dep.package_name) == existing_key)
        {
            let Some((replacement_package, replacement_version)) =
                parse_replacement_spec(replacement_spec)
            else {
                continue;
            };
            let dep = &mut resolved[dep_index];
            let old_package = dep.package_name.clone();
            let old_version = dep.version.clone();
            let changed = normalize_key(&old_package) != normalize_key(&replacement_package)
                || old_version != replacement_version;
            if changed {
                dep.package_name = replacement_package.clone();
                dep.version = replacement_version.clone();
                dep.strategy = strategy.to_string();
                dep.confidence = 0.76;
                notes.push(format!(
                    "cluster {}: replaced `{}` with `{}{}`.",
                    cluster.id,
                    old_package,
                    replacement_package,
                    replacement_version
                        .as_deref()
                        .map(|version| format!("=={version}"))
                        .unwrap_or_default()
                ));
            }
        }
    }

    for (package, preferred_version) in &cluster.preferred_versions {
        let package_key = normalize_key(package);
        if let Some(dep_index) = resolved
            .iter()
            .position(|dep| normalize_key(&dep.package_name) == package_key)
        {
            let dep = &mut resolved[dep_index];
            if dep.version.as_deref() != Some(preferred_version.as_str()) {
                let previous = dep.version.clone().unwrap_or_else(|| "(none)".to_string());
                dep.version = Some(preferred_version.clone());
                dep.strategy = strategy.to_string();
                dep.confidence = 0.75;
                notes.push(format!(
                    "cluster {}: pinned `{}` from {} to {}.",
                    cluster.id, dep.package_name, previous, preferred_version
                ));
            }
        }
    }

    for companion in &cluster.companions {
        let package_key = normalize_key(&companion.package);
        if resolved
            .iter()
            .all(|dep| normalize_key(&dep.package_name) != package_key)
        {
            resolved.push(crate::ResolvedDependency {
                import_name: companion.package.clone(),
                package_name: companion.package.clone(),
                version: None,
                strategy: strategy.to_string(),
                confidence: 0.69,
            });
            notes.push(format!(
                "cluster {}: added companion `{}`.",
                cluster.id, companion.package
            ));
        }
    }

    notes
}

pub fn filter_candidate_versions_for_resolved(
    policy: &TargetedRecoveryPolicy,
    resolved: &[crate::ResolvedDependency],
    versions: Vec<String>,
) -> Vec<String> {
    let original_versions = versions.clone();
    let mut floor: Option<(u32, u32)> = None;
    let mut ceiling: Option<(u32, u32)> = None;

    for dep in resolved {
        if let Some(cluster) = policy.compatibility_cluster_for_package(&dep.package_name) {
            if let Some(cluster_floor) = cluster.python_floor.as_deref().map(version_tuple_safe) {
                floor = Some(match floor {
                    Some(current) => current.max(cluster_floor),
                    None => cluster_floor,
                });
            }
            if let Some(cluster_ceiling) = cluster.python_ceiling.as_deref().map(version_tuple_safe)
            {
                ceiling = Some(match ceiling {
                    Some(current) => current.min(cluster_ceiling),
                    None => cluster_ceiling,
                });
            }
        }
        if let Some(rule) = policy.python_ceiling_for_package(&dep.package_name) {
            let rule_ceiling = version_tuple_safe(&rule.max_python);
            ceiling = Some(match ceiling {
                Some(current) => current.min(rule_ceiling),
                None => rule_ceiling,
            });
        }
    }

    let filtered = versions
        .into_iter()
        .filter(|version| {
            let tuple = version_tuple_safe(version);
            floor.map(|min| tuple >= min).unwrap_or(true)
                && ceiling.map(|max| tuple <= max).unwrap_or(true)
        })
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        original_versions
    } else {
        filtered
    }
}

fn parse_replacement_spec(spec: &str) -> Option<(String, Option<String>)> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some((package, version)) = trimmed.split_once("==") {
        return Some((package.trim().to_string(), Some(version.trim().to_string())));
    }
    Some((trimmed.to_string(), None))
}

fn version_tuple_safe(value: &str) -> (u32, u32) {
    crate::parser::version_detect::version_tuple(value)
}

// ---------------------------------------------------------------------------
// Initialization and loading
// ---------------------------------------------------------------------------

fn targeted_recovery_policy_cache() -> &'static Mutex<Option<TargetedRecoveryPolicy>> {
    TARGETED_RECOVERY_POLICY.get_or_init(|| Mutex::new(None))
}

/// Path to the module rules JSON file relative to the tool root.
pub fn module_rules_path(tool_root: &Path) -> PathBuf {
    tool_root.join(MODULE_RULES_RELATIVE)
}

/// Path to the compatibility rules JSON file relative to the tool root.
pub fn compatibility_rules_path(tool_root: &Path) -> PathBuf {
    tool_root.join(COMPATIBILITY_RULES_RELATIVE)
}

/// Initialize the targeted recovery policy from disk, validating against the
/// Phase 7 parity manifest. Errors are deterministic and include the
/// conflicting rule ID, alias, package, or case ID in the message.
pub fn init_targeted_recovery_policy(tool_root: &Path) -> Result<(), String> {
    let policy = load_targeted_recovery_policy(tool_root)?;
    let mut cached = targeted_recovery_policy_cache()
        .lock()
        .map_err(|_| "failed to lock targeted recovery policy cache".to_string())?;
    *cached = Some(policy);
    Ok(())
}

/// Read the current policy snapshot. Returns `None` if not yet initialized.
pub fn get_targeted_recovery_policy() -> Option<TargetedRecoveryPolicy> {
    let cache = targeted_recovery_policy_cache().lock().ok()?;
    cache.clone()
}

/// Load and validate the targeted recovery policy from disk without caching.
/// This is the primary entrypoint for tests that want validation behavior.
pub fn load_targeted_recovery_policy(tool_root: &Path) -> Result<TargetedRecoveryPolicy, String> {
    let mod_path = module_rules_path(tool_root);
    let compat_path = compatibility_rules_path(tool_root);

    let mod_json = fs::read_to_string(&mod_path).map_err(|err| {
        format!(
            "Failed to read module rules from `{}`: {err}",
            mod_path.display()
        )
    })?;
    let compat_json = fs::read_to_string(&compat_path).map_err(|err| {
        format!(
            "Failed to read compatibility rules from `{}`: {err}",
            compat_path.display()
        )
    })?;

    let mod_file: ModuleRulesFile = serde_json::from_str(&mod_json).map_err(|err| {
        format!(
            "Failed to parse module rules `{}`: {err}",
            mod_path.display()
        )
    })?;
    let compat_file: CompatibilityRulesFile =
        serde_json::from_str(&compat_json).map_err(|err| {
            format!(
                "Failed to parse compatibility rules `{}`: {err}",
                compat_path.display()
            )
        })?;

    // Load canonical case IDs from the Phase 7 parity manifest.
    let manifest_path = tool_root
        .parent()
        .and_then(|p| p.parent())
        .map(|repo_root| repo_root.join(PARITY_MANIFEST_RELATIVE))
        .ok_or_else(|| {
            "Cannot determine repo root from tool root for parity manifest lookup".to_string()
        })?;

    let canonical_case_ids = if manifest_path.exists() {
        let manifest_json = fs::read_to_string(&manifest_path).map_err(|err| {
            format!(
                "Failed to read parity manifest from `{}`: {err}",
                manifest_path.display()
            )
        })?;
        let manifest: ParityManifest = serde_json::from_str(&manifest_json).map_err(|err| {
            format!(
                "Failed to parse parity manifest `{}`: {err}",
                manifest_path.display()
            )
        })?;
        let mut ids: BTreeSet<String> = manifest.canonical_case_ids.into_iter().collect();
        ids.extend(manifest.tier1_watchlist_case_ids);
        ids
    } else {
        // In test environments the manifest may not be present; skip case-ID
        // validation but still validate structural integrity.
        BTreeSet::new()
    };

    validate_and_build(mod_file, compat_file, &canonical_case_ids)
}

// ---------------------------------------------------------------------------
// Validation and index building
// ---------------------------------------------------------------------------

fn validate_and_build(
    mod_file: ModuleRulesFile,
    compat_file: CompatibilityRulesFile,
    canonical_case_ids: &BTreeSet<String>,
) -> Result<TargetedRecoveryPolicy, String> {
    let mut module_rule_by_id: BTreeMap<String, usize> = BTreeMap::new();
    let mut module_rule_by_alias: BTreeMap<String, usize> = BTreeMap::new();

    // --- Module provider rules ---
    for (index, rule) in mod_file.module_provider_rules.iter().enumerate() {
        let id_key = normalize_key(&rule.id);
        if let Some(existing_idx) = module_rule_by_id.insert(id_key, index) {
            return Err(format!(
                "duplicate module provider rule ID `{}` (conflicts with `{}`)",
                rule.id, mod_file.module_provider_rules[existing_idx].id
            ));
        }

        if rule.module_aliases.is_empty() {
            return Err(format!(
                "module provider rule `{}` has an empty module_aliases set",
                rule.id
            ));
        }

        for alias in &rule.module_aliases {
            let alias_key = normalize_key(alias);
            if let Some(existing_idx) = module_rule_by_alias.insert(alias_key, index) {
                return Err(format!(
                    "duplicate module alias `{}` in rule `{}` (already claimed by rule `{}`)",
                    alias, rule.id, mod_file.module_provider_rules[existing_idx].id
                ));
            }
        }

        validate_case_ids(&rule.canonical_case_ids, canonical_case_ids, &rule.id)?;
    }

    // --- Stop reason rules ---
    let mut stop_reason_rule_by_id: BTreeMap<String, usize> = BTreeMap::new();
    for (index, rule) in mod_file.stop_reason_rules.iter().enumerate() {
        let id_key = normalize_key(&rule.id);
        if let Some(existing_idx) = stop_reason_rule_by_id.insert(id_key, index) {
            return Err(format!(
                "duplicate stop reason rule ID `{}` (conflicts with `{}`)",
                rule.id, mod_file.stop_reason_rules[existing_idx].id
            ));
        }

        if rule.module_patterns.is_empty() {
            return Err(format!(
                "stop reason rule `{}` has an empty module_patterns set",
                rule.id
            ));
        }

        validate_case_ids(&rule.canonical_case_ids, canonical_case_ids, &rule.id)?;
    }

    // --- Compatibility clusters ---
    let mut compatibility_cluster_by_id: BTreeMap<String, usize> = BTreeMap::new();
    let mut seen_anchor_packages: BTreeMap<String, String> = BTreeMap::new();

    for (index, cluster) in compat_file.compatibility_clusters.iter().enumerate() {
        let id_key = normalize_key(&cluster.id);
        if let Some(existing_idx) = compatibility_cluster_by_id.insert(id_key, index) {
            return Err(format!(
                "duplicate compatibility cluster ID `{}` (conflicts with `{}`)",
                cluster.id, compat_file.compatibility_clusters[existing_idx].id
            ));
        }

        if cluster.anchor_packages.is_empty() {
            return Err(format!(
                "compatibility cluster `{}` has an empty anchor_packages set",
                cluster.id
            ));
        }

        for anchor in &cluster.anchor_packages {
            let anchor_key = normalize_key(anchor);
            if let Some(existing_cluster_id) = seen_anchor_packages.get(&anchor_key) {
                return Err(format!(
                    "duplicate anchor package `{}` in cluster `{}` (already in cluster `{}`)",
                    anchor, cluster.id, existing_cluster_id
                ));
            }
            seen_anchor_packages.insert(anchor_key, cluster.id.clone());
        }

        if cluster.trigger_substrings.is_empty()
            && cluster.replacement_packages.is_empty()
            && cluster.preferred_versions.is_empty()
            && cluster.companions.is_empty()
            && cluster.python_floor.is_none()
            && cluster.python_ceiling.is_none()
        {
            return Err(format!(
                "compatibility cluster `{}` has no trigger substrings, replacement packages, preferred versions, companions, python floor, or python ceiling",
                cluster.id
            ));
        }

        validate_case_ids(&cluster.canonical_case_ids, canonical_case_ids, &cluster.id)?;
    }

    // --- Companion rules ---
    let mut companion_rule_by_id: BTreeMap<String, usize> = BTreeMap::new();
    for (index, rule) in compat_file.companion_rules.iter().enumerate() {
        let id_key = normalize_key(&rule.id);
        if let Some(existing_idx) = companion_rule_by_id.insert(id_key, index) {
            return Err(format!(
                "duplicate companion rule ID `{}` (conflicts with `{}`)",
                rule.id, compat_file.companion_rules[existing_idx].id
            ));
        }

        validate_case_ids(&rule.canonical_case_ids, canonical_case_ids, &rule.id)?;
    }

    // --- Python ceiling rules ---
    let mut python_ceiling_rule_by_id: BTreeMap<String, usize> = BTreeMap::new();
    for (index, rule) in compat_file.python_ceiling_rules.iter().enumerate() {
        let id_key = normalize_key(&rule.id);
        if let Some(existing_idx) = python_ceiling_rule_by_id.insert(id_key, index) {
            return Err(format!(
                "duplicate python ceiling rule ID `{}` (conflicts with `{}`)",
                rule.id, compat_file.python_ceiling_rules[existing_idx].id
            ));
        }

        validate_case_ids(&rule.canonical_case_ids, canonical_case_ids, &rule.id)?;
    }

    Ok(TargetedRecoveryPolicy {
        module_rules: mod_file.module_provider_rules,
        stop_reason_rules: mod_file.stop_reason_rules,
        compatibility_clusters: compat_file.compatibility_clusters,
        companion_rules: compat_file.companion_rules,
        python_ceiling_rules: compat_file.python_ceiling_rules,
        module_rule_by_id,
        module_rule_by_alias,
        stop_reason_rule_by_id,
        compatibility_cluster_by_id,
        companion_rule_by_id,
        python_ceiling_rule_by_id,
    })
}

fn validate_case_ids(
    rule_case_ids: &[String],
    canonical_case_ids: &BTreeSet<String>,
    rule_id: &str,
) -> Result<(), String> {
    if canonical_case_ids.is_empty() {
        // Skip case-ID validation when no manifest is loaded (test environments).
        return Ok(());
    }
    for case_id in rule_case_ids {
        if !canonical_case_ids.contains(case_id) {
            return Err(format!(
                "rule `{rule_id}` references unknown canonical case ID `{case_id}`"
            ));
        }
    }
    Ok(())
}

fn normalize_key(s: &str) -> String {
    s.to_lowercase().replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_key_lowercases_and_replaces_hyphens() {
        assert_eq!(normalize_key("Foo-Bar"), "foo_bar");
        assert_eq!(normalize_key("pkg_resources"), "pkg_resources");
    }
}
