use std::collections::BTreeSet;

use crate::cache::store::CacheStore;
use crate::resolver::pypi_client;
use crate::{ParseResult, ResolvedDependency};

pub struct StageResult {
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub cache_hits: usize,
}

pub fn resolve(parse_result: &ParseResult, store: &mut CacheStore, python_version: &str) -> StageResult {
    let mut resolved = Vec::new();
    let mut unresolved = Vec::new();
    let mut seen = BTreeSet::new();
    let mut cache_hits = 0;

    for dependency in &parse_result.config_deps {
        let package_name = dependency.package.replace('_', "-");
        if seen.insert(package_name.clone()) {
            resolved.push(ResolvedDependency {
                import_name: dependency.package.clone(),
                package_name,
                version: dependency
                    .constraint
                    .clone()
                    .and_then(|value| value.strip_prefix("==").map(|item| item.to_string())),
                strategy: "config-scan".to_string(),
                confidence: 0.88,
            });
        }
    }

    for import_name in candidate_imports(parse_result, store) {
        if looks_like_local_helper_import(parse_result, &import_name) {
            unresolved.push(import_name);
            continue;
        }
        if let Some((record, strategy)) = lookup_import_record(parse_result, &import_name, store) {
            if record.source == "heuristic:fuzzy" {
                unresolved.push(import_name);
                continue;
            }
            let trusted_mapping = matches!(record.source.as_str(), "seed" | "discrepancy");
            if !trusted_mapping
                && !pypi_client::package_exists(store, &record.package_name, python_version)
            {
                unresolved.push(import_name);
                continue;
            }
            if seen.insert(record.package_name.clone()) {
                resolved.push(ResolvedDependency {
                    import_name: import_name.clone(),
                    package_name: record.package_name.clone(),
                    version: record.default_version.clone(),
                    strategy,
                    confidence: 0.97,
                });
                cache_hits += 1;
            }
        } else {
            unresolved.push(import_name);
        }
    }

    StageResult {
        resolved,
        unresolved,
        cache_hits,
    }
}

fn candidate_imports(parse_result: &ParseResult, store: &CacheStore) -> Vec<String> {
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    let mut covered_roots = BTreeSet::new();

    for import_path in &parse_result.import_paths {
        if let Some(prefix) = dotted_prefixes(import_path)
            .into_iter()
            .find(|prefix| prefix != top_level(prefix) && store.import_lookup(prefix).is_some())
        {
            if seen.insert(prefix.clone()) {
                covered_roots.insert(top_level(&prefix).to_string());
                candidates.push(prefix);
            }
        }
    }

    for import_name in &parse_result.imports {
        if covered_roots.contains(import_name) {
            continue;
        }
        if seen.insert(import_name.clone()) {
            candidates.push(import_name.clone());
        }
    }

    candidates
}

fn lookup_import_record(
    parse_result: &ParseResult,
    import_name: &str,
    store: &CacheStore,
) -> Option<(crate::cache::store::PackageRecord, String)> {
    if let Some(record) = store.import_lookup(import_name).cloned() {
        return Some((record.clone(), format!("cache:{}", record.source)));
    }

    for import_path in &parse_result.import_paths {
        if import_path != import_name && !import_path.starts_with(&format!("{import_name}.")) {
            continue;
        }
        for prefix in dotted_prefixes(import_path) {
            let Some(record) = store.import_lookup(&prefix).cloned() else {
                continue;
            };
            return Some((record.clone(), format!("cache:path-prefix:{}", record.source)));
        }
    }

    None
}

fn dotted_prefixes(import_path: &str) -> Vec<String> {
    let parts = import_path.split('.').collect::<Vec<_>>();
    let mut prefixes = Vec::new();
    for end in (1..=parts.len()).rev() {
        prefixes.push(parts[..end].join("."));
    }
    prefixes
}

fn top_level(import_path: &str) -> &str {
    import_path.split('.').next().unwrap_or(import_path)
}

fn looks_like_local_helper_import(parse_result: &ParseResult, import_name: &str) -> bool {
    let normalized = crate::cache::store::normalize(import_name);
    if normalized == "input-data" {
        return true;
    }
    let generic_helper = matches!(
        normalized.as_str(),
        "util" | "utils" | "helper" | "helpers" | "common" | "shared"
    );
    generic_helper
        && parse_result
            .import_paths
            .iter()
            .any(|path| crate::cache::store::normalize(path).starts_with(&format!("{normalized}-")))
}
