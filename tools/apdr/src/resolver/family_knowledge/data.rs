use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const TOUCHED_FAMILIES_PATH: &str = "data/family_knowledge/touched_families.json";
const TOUCHED_RECOVERY_RULES_PATH: &str = "data/family_knowledge/touched_recovery_rules.json";

static CURATED_FAMILY_KNOWLEDGE: OnceCell<Mutex<Option<CuratedFamilyKnowledge>>> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct CuratedFamilyKnowledge {
    families: Vec<CuratedPackageFamily>,
    explicit_namespace_mappings: Vec<CuratedExplicitNamespaceMapping>,
    recovery_rules: Vec<CuratedRecoveryRule>,
    family_by_name: BTreeMap<String, usize>,
    registry_family_by_package: BTreeMap<String, usize>,
    registry_family_by_module: BTreeMap<String, Vec<usize>>,
    explicit_namespace_mapping_by_import: BTreeMap<String, usize>,
    recovery_rule_by_id: BTreeMap<String, usize>,
}

impl CuratedFamilyKnowledge {
    fn build(
        families: Vec<CuratedPackageFamily>,
        explicit_namespace_mappings: Vec<CuratedExplicitNamespaceMapping>,
        recovery_rules: Vec<CuratedRecoveryRule>,
    ) -> Result<Self, String> {
        let mut family_by_name = BTreeMap::new();
        let mut registry_family_by_package = BTreeMap::new();
        let mut registry_family_by_module: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
        let mut family_member_packages: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        for (index, family) in families.iter().enumerate() {
            let family_key = normalize_key(&family.name);
            if family_by_name.insert(family_key.clone(), index).is_some() {
                return Err(format!("duplicate family `{}`", family.name));
            }

            let preferred_members = family
                .members
                .iter()
                .filter(|member| member.preferred)
                .map(|member| member.package.as_str())
                .collect::<Vec<_>>();
            if preferred_members.len() > 1 {
                return Err(format!(
                    "family `{}` has more than one preferred member: {}",
                    family.name,
                    preferred_members
                        .into_iter()
                        .map(|package| format!("`{package}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }

            let package_keys = family
                .members
                .iter()
                .map(|member| normalize_key(&member.package))
                .collect::<BTreeSet<_>>();
            family_member_packages.insert(family_key, package_keys);

            if family.has_scope(CuratedRuntimeScope::Registry) {
                for member in &family.members {
                    let package_key = normalize_key(&member.package);
                    if let Some(existing_index) =
                        registry_family_by_package.insert(package_key, index)
                    {
                        if existing_index != index {
                            return Err(format!(
                                "registry package `{}` appears in both family `{}` and family `{}`",
                                member.package, families[existing_index].name, family.name
                            ));
                        }
                    }
                    for module in &member.modules {
                        registry_family_by_module
                            .entry(normalize_key(module))
                            .or_default()
                            .insert(index);
                    }
                }
                for module in &family.modules {
                    registry_family_by_module
                        .entry(normalize_key(module))
                        .or_default()
                        .insert(index);
                }
            }
        }

        let mut explicit_namespace_mapping_by_import = BTreeMap::new();
        for (index, mapping) in explicit_namespace_mappings.iter().enumerate() {
            let alias_key = normalize_key(&mapping.import_alias);
            if let Some(existing_index) =
                explicit_namespace_mapping_by_import.insert(alias_key, index)
            {
                let existing = &explicit_namespace_mappings[existing_index];
                return Err(format!(
                    "duplicate explicit namespace mapping for alias `{}`: `{}` conflicts with `{}`",
                    mapping.import_alias, existing.package_name, mapping.package_name
                ));
            }
        }

        let mut recovery_rule_by_id = BTreeMap::new();
        for (index, rule) in recovery_rules.iter().enumerate() {
            let rule_key = normalize_key(&rule.id);
            if recovery_rule_by_id.insert(rule_key, index).is_some() {
                return Err(format!("duplicate recovery rule `{}`", rule.id));
            }

            let family_key = normalize_key(&rule.family);
            let Some(family_index) = family_by_name.get(&family_key).copied() else {
                return Err(format!(
                    "recovery rule `{}` references unknown family `{}`",
                    rule.id, rule.family
                ));
            };

            if rule.trigger_substrings.is_empty() && rule.anchor_ids.is_empty() {
                return Err(format!(
                    "recovery rule `{}` has no trigger substrings or anchor ids",
                    rule.id
                ));
            }

            let known_members = family_member_packages
                .get(&family_key)
                .cloned()
                .unwrap_or_default();
            for variant in &rule.bundle_variants {
                for member in &variant.members {
                    if !known_members.contains(&normalize_key(&member.package_name)) {
                        return Err(format!(
                            "recovery rule `{}` references bundle member `{}` that is not present in family `{}`",
                            rule.id, member.package_name, families[family_index].name
                        ));
                    }
                }
            }
        }

        Ok(Self {
            families,
            explicit_namespace_mappings,
            recovery_rules,
            family_by_name,
            registry_family_by_package,
            registry_family_by_module: registry_family_by_module
                .into_iter()
                .map(|(module, families)| (module, families.into_iter().collect()))
                .collect(),
            explicit_namespace_mapping_by_import,
            recovery_rule_by_id,
        })
    }

    pub fn families(&self) -> &[CuratedPackageFamily] {
        &self.families
    }

    pub fn explicit_namespace_mappings(&self) -> &[CuratedExplicitNamespaceMapping] {
        &self.explicit_namespace_mappings
    }

    pub fn recovery_rules(&self) -> &[CuratedRecoveryRule] {
        &self.recovery_rules
    }

    pub fn family_named(&self, family_name: &str) -> Option<&CuratedPackageFamily> {
        self.family_by_name
            .get(&normalize_key(family_name))
            .map(|index| &self.families[*index])
    }

    pub fn registry_family_for_package(&self, package_name: &str) -> Option<&CuratedPackageFamily> {
        self.registry_family_by_package
            .get(&normalize_key(package_name))
            .map(|index| &self.families[*index])
    }

    pub fn registry_families_for_module(&self, module_name: &str) -> Vec<&CuratedPackageFamily> {
        self.registry_family_by_module
            .get(&normalize_key(module_name))
            .map(|indices| indices.iter().map(|index| &self.families[*index]).collect())
            .unwrap_or_default()
    }

    pub fn explicit_namespace_mapping(
        &self,
        import_alias: &str,
    ) -> Option<&CuratedExplicitNamespaceMapping> {
        self.explicit_namespace_mapping_by_import
            .get(&normalize_key(import_alias))
            .map(|index| &self.explicit_namespace_mappings[*index])
    }

    pub fn recovery_rule(&self, rule_id: &str) -> Option<&CuratedRecoveryRule> {
        self.recovery_rule_by_id
            .get(&normalize_key(rule_id))
            .map(|index| &self.recovery_rules[*index])
    }

    pub fn recovery_rules_for_family(&self, family_name: &str) -> Vec<&CuratedRecoveryRule> {
        let family_key = normalize_key(family_name);
        self.recovery_rules
            .iter()
            .filter(|rule| normalize_key(&rule.family) == family_key)
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CuratedPackageFamily {
    pub name: String,
    #[serde(default)]
    pub runtime_scopes: Vec<CuratedRuntimeScope>,
    #[serde(default)]
    pub modules: Vec<String>,
    pub conflict_kind: CuratedConflictKind,
    #[serde(default)]
    pub members: Vec<CuratedFamilyMember>,
    pub notes: String,
}

impl CuratedPackageFamily {
    pub fn preferred(&self) -> Option<&CuratedFamilyMember> {
        self.members
            .iter()
            .find(|member| member.preferred)
            .or_else(|| {
                self.members
                    .iter()
                    .find(|member| member.status == CuratedMemberStatus::Active)
            })
    }

    pub fn has_scope(&self, scope: CuratedRuntimeScope) -> bool {
        self.runtime_scopes.contains(&scope)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CuratedFamilyMember {
    pub package: String,
    #[serde(default)]
    pub modules: Vec<String>,
    pub status: CuratedMemberStatus,
    #[serde(default)]
    pub preferred: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CuratedExplicitNamespaceMapping {
    pub import_alias: String,
    pub package_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CuratedRecoveryRule {
    pub id: String,
    pub family: String,
    pub kind: CuratedRecoveryRuleKind,
    pub strategy: String,
    #[serde(default)]
    pub anchor_ids: Vec<String>,
    #[serde(default)]
    pub trigger_substrings: Vec<String>,
    #[serde(default)]
    pub bundle_variants: Vec<CuratedBundleVariant>,
    #[serde(default)]
    pub preferred_python_order: Option<CuratedPythonPreferenceOrder>,
    #[serde(default)]
    pub apply_note_template: Option<String>,
    #[serde(default)]
    pub recovery_note_prefix: Option<String>,
    #[serde(default)]
    pub recovery_locked_note_template: Option<String>,
    #[serde(default)]
    pub recovery_unpinned_note_template: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CuratedBundleVariant {
    #[serde(default)]
    pub python_selectors: Vec<String>,
    #[serde(default)]
    pub members: Vec<CuratedBundleMember>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CuratedBundleMember {
    pub import_name: String,
    pub package_name: String,
    #[serde(default)]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CuratedPythonPreferenceOrder {
    #[serde(default)]
    pub execute_snippet: Vec<String>,
    #[serde(default, rename = "default")]
    pub default_order: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CuratedConflictKind {
    Namespace,
    Fork,
    Variant,
    Replacement,
    Migration,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CuratedMemberStatus {
    Active,
    Deprecated,
    Unmaintained,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CuratedRuntimeScope {
    Registry,
    Bundle,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CuratedRecoveryRuleKind {
    Bundle,
    Companion,
    Pin,
    RetryModuleProvider,
}

#[derive(Debug, Deserialize)]
struct CuratedFamiliesFile {
    #[serde(default)]
    families: Vec<CuratedPackageFamily>,
    #[serde(default)]
    explicit_namespace_mappings: Vec<CuratedExplicitNamespaceMapping>,
}

#[derive(Debug, Deserialize)]
struct CuratedRecoveryRulesFile {
    #[serde(default)]
    rules: Vec<CuratedRecoveryRule>,
}

pub fn init_curated_family_knowledge(tool_root: &Path) -> Result<(), String> {
    let curated = load_curated_family_knowledge(tool_root)?;
    let mut cached = curated_family_knowledge_cache()
        .lock()
        .map_err(|_| "failed to lock curated family knowledge cache".to_string())?;
    *cached = Some(curated);
    Ok(())
}

pub fn load_curated_family_knowledge(tool_root: &Path) -> Result<CuratedFamilyKnowledge, String> {
    let families_path = touched_families_path(tool_root);
    let recovery_rules_path = touched_recovery_rules_path(tool_root);

    let families_json = fs::read_to_string(&families_path).map_err(|err| {
        format!(
            "Failed to read curated families from `{}`: {err}",
            families_path.display()
        )
    })?;
    let recovery_rules_json = fs::read_to_string(&recovery_rules_path).map_err(|err| {
        format!(
            "Failed to read curated recovery rules from `{}`: {err}",
            recovery_rules_path.display()
        )
    })?;

    let families_file: CuratedFamiliesFile =
        serde_json::from_str(&families_json).map_err(|err| {
            format!(
                "Failed to parse curated families `{}`: {err}",
                families_path.display()
            )
        })?;
    let recovery_rules_file: CuratedRecoveryRulesFile = serde_json::from_str(&recovery_rules_json)
        .map_err(|err| {
            format!(
                "Failed to parse curated recovery rules `{}`: {err}",
                recovery_rules_path.display()
            )
        })?;

    CuratedFamilyKnowledge::build(
        families_file.families,
        families_file.explicit_namespace_mappings,
        recovery_rules_file.rules,
    )
}

pub fn curated_family_knowledge_snapshot() -> Option<CuratedFamilyKnowledge> {
    curated_family_knowledge_cache()
        .lock()
        .ok()
        .and_then(|cached| cached.clone())
}

pub fn with_curated_family_knowledge<T, F>(callback: F) -> Result<T, String>
where
    F: FnOnce(&CuratedFamilyKnowledge) -> T,
{
    let cached = curated_family_knowledge_cache()
        .lock()
        .map_err(|_| "failed to lock curated family knowledge cache".to_string())?;
    let curated = cached
        .as_ref()
        .ok_or_else(|| "curated family knowledge is not initialized".to_string())?;
    Ok(callback(curated))
}

pub fn touched_families_path(tool_root: &Path) -> PathBuf {
    tool_root.join(TOUCHED_FAMILIES_PATH)
}

pub fn touched_recovery_rules_path(tool_root: &Path) -> PathBuf {
    tool_root.join(TOUCHED_RECOVERY_RULES_PATH)
}

fn curated_family_knowledge_cache() -> &'static Mutex<Option<CuratedFamilyKnowledge>> {
    CURATED_FAMILY_KNOWLEDGE.get_or_init(|| Mutex::new(None))
}

fn normalize_key(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['-', '.'], "_")
}
