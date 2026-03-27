use std::collections::BTreeSet;

use crate::cache::store::CacheStore;
use crate::resolver::pypi_client;
use crate::{ParseResult, ResolvedDependency};

pub struct StageResult {
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub cache_hits: usize,
}

pub fn resolve(
    parse_result: &ParseResult,
    store: &mut CacheStore,
    python_version: &str,
) -> StageResult {
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
            let record = apply_version_aware_seed_overrides(
                parse_result,
                &import_name,
                &record,
                python_version,
            );
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
                    version: pypi_client::compatible_default_version(
                        store,
                        &record.package_name,
                        record.default_version.as_deref(),
                        python_version,
                    ),
                    strategy,
                    confidence: source_confidence(&record.source),
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

fn apply_version_aware_seed_overrides(
    parse_result: &ParseResult,
    import_name: &str,
    record: &crate::cache::store::PackageRecord,
    python_version: &str,
) -> crate::cache::store::PackageRecord {
    let import_norm = crate::cache::store::normalize(import_name);
    let package_norm = crate::cache::store::normalize(&record.package_name);

    if record.source == "seed" && import_norm == "johnny" && package_norm == "johnny" {
        let uses_johnny_cache_api = parse_result.import_paths.iter().any(|path| {
            let normalized = crate::cache::store::normalize(path);
            normalized == "johnny-cache" || normalized.starts_with("johnny-cache-")
        });
        if uses_johnny_cache_api {
            let mut overridden = record.clone();
            overridden.package_name = "johnny-cache".to_string();
            return overridden;
        }
    }

    if record.source == "seed"
        && import_norm == "mecab"
        && package_norm == "mecab-python3"
        && python_version.starts_with("2.")
    {
        let mut overridden = record.clone();
        overridden.package_name = "mecab-python".to_string();
        return overridden;
    }

    record.clone()
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
        if import_path != import_name && !is_dotted_child(import_name, import_path) {
            continue;
        }
        for prefix in dotted_prefixes(import_path) {
            let Some(record) = store.import_lookup(&prefix).cloned() else {
                continue;
            };
            return Some((
                record.clone(),
                format!("cache:path-prefix:{}", record.source),
            ));
        }
    }

    None
}

/// Check if `child` starts with `parent.` (without allocating a format string).
fn is_dotted_child(parent: &str, child: &str) -> bool {
    child.len() > parent.len()
        && child.starts_with(parent)
        && child.as_bytes()[parent.len()] == b'.'
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

/// Confidence score based on the data source of the import mapping.
fn source_confidence(source: &str) -> f64 {
    match source {
        "discrepancy" => 0.97,
        "seed" => 0.97,
        "harvest" => 0.92,
        "pipreqs" => 0.85,
        "llm" => 0.73,
        "recovery:cache" | "recovery:llm" => 0.70,
        "heuristic:pypi-exact" | "recovery:heuristic" => 0.80,
        "heuristic:fuzzy" | "heuristic:trigram-jaccard" => 0.70,
        _ => 0.60,
    }
}

fn looks_like_local_helper_import(parse_result: &ParseResult, import_name: &str) -> bool {
    let normalized = crate::cache::store::normalize(import_name);
    // Unconditionally local: names that are never a correct PyPI import.
    // `settings` is almost always a Django project-local settings module.
    // `config`/`conf` are project-local configuration modules.
    if matches!(
        normalized.as_str(),
        "input-data"
            | "settings"
            | "config"
            | "conf"
            | "constants"
            | "urls"
            | "api"
            | "app"
            | "apps"
            | "views"
            | "models"
            | "forms"
            | "admin"
            | "tests"
            | "manage"
    ) {
        return true;
    }
    let generic_helper = matches!(
        normalized.as_str(),
        "util" | "utils" | "helper" | "helpers" | "common" | "shared"
    );
    generic_helper
        && parse_result.import_paths.iter().any(|path| {
            let np = crate::cache::store::normalize(path);
            np.len() > normalized.len()
                && np.starts_with(normalized.as_str())
                && np.as_bytes()[normalized.len()] == b'-'
        })
}
