use std::collections::BTreeSet;

use crate::cache::store::{normalize, CacheStore};
use crate::resolver::pypi_client;
use crate::resolver::version_sampler;
use crate::{ParseResult, ResolvedDependency};

pub struct StageResult {
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub heuristic_hits: usize,
}

pub fn resolve(
    unresolved_imports: &[String],
    parse_result: &ParseResult,
    store: &mut CacheStore,
    python_version: &str,
) -> StageResult {
    let config_packages = parse_result
        .config_deps
        .iter()
        .map(|dependency| normalize(&dependency.package))
        .collect::<BTreeSet<_>>();
    let known_names = pypi_client::cached_package_names(store);

    let mut resolved = Vec::new();
    let mut unresolved = Vec::new();
    let mut heuristic_hits = 0;

    for import_name in unresolved_imports {
        if looks_like_local_helper_import(parse_result, import_name) {
            unresolved.push(import_name.clone());
            continue;
        }
        let normalized = normalize(import_name);

        if config_packages.contains(&normalized) {
            let version = pypi_client::compatible_versions(store, &normalized, python_version)
                .last()
                .cloned();
            resolved.push(ResolvedDependency {
                import_name: import_name.clone(),
                package_name: normalized.clone(),
                version,
                strategy: "heuristic:config-package".to_string(),
                confidence: 0.72,
            });
            heuristic_hits += 1;
            continue;
        }

        if pypi_client::package_exists(store, &normalized, python_version) {
            let versions = pypi_client::compatible_versions(store, &normalized, python_version);
            let version = version_sampler::equally_distanced_sample(&versions, &[]);
            let _ = store.save_import_mapping(
                import_name,
                &normalized,
                version.as_deref(),
                "heuristic:pypi-exact",
            );
            resolved.push(ResolvedDependency {
                import_name: import_name.clone(),
                package_name: normalized.clone(),
                version,
                strategy: "heuristic:pypi-exact".to_string(),
                confidence: 0.84,
            });
            heuristic_hits += 1;
            continue;
        }

        let best_match = known_names
            .iter()
            .filter_map(|candidate| {
                let distance = levenshtein(&normalized, candidate);
                let is_short = normalized.chars().count() <= 4;
                let substring_match = !is_short
                    && (candidate.contains(&normalized) || normalized.contains(candidate));
                let allowed_distance = if is_short { 1 } else { 2 };
                if distance <= allowed_distance || substring_match {
                    Some((candidate.clone(), distance))
                } else {
                    None
                }
            })
            .min_by_key(|(_, distance)| *distance);

        if let Some((candidate, _distance)) = best_match {
            let versions = pypi_client::compatible_versions(store, &candidate, python_version);
            let version = version_sampler::equally_distanced_sample(&versions, &[]);
            resolved.push(ResolvedDependency {
                import_name: import_name.clone(),
                package_name: candidate,
                version,
                strategy: "heuristic:fuzzy".to_string(),
                confidence: 0.66,
            });
            heuristic_hits += 1;
        } else {
            unresolved.push(import_name.clone());
        }
    }

    StageResult {
        resolved,
        unresolved,
        heuristic_hits,
    }
}

fn levenshtein(left: &str, right: &str) -> usize {
    if left == right {
        return 0;
    }
    if left.is_empty() {
        return right.chars().count();
    }
    if right.is_empty() {
        return left.chars().count();
    }

    let left_chars = left.chars().collect::<Vec<_>>();
    let right_chars = right.chars().collect::<Vec<_>>();
    let mut costs = (0..=right_chars.len()).collect::<Vec<_>>();

    for (left_index, left_char) in left_chars.iter().enumerate() {
        let mut corner = costs[0];
        costs[0] = left_index + 1;
        for (right_index, right_char) in right_chars.iter().enumerate() {
            let upper = costs[right_index + 1];
            let substitution = if left_char == right_char {
                corner
            } else {
                corner + 1
            };
            costs[right_index + 1] = std::cmp::min(
                std::cmp::min(costs[right_index] + 1, upper + 1),
                substitution,
            );
            corner = upper;
        }
    }

    *costs.last().unwrap_or(&usize::MAX)
}

fn looks_like_local_helper_import(parse_result: &ParseResult, import_name: &str) -> bool {
    let normalized = normalize(import_name);
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
            .any(|path| normalize(path).starts_with(&format!("{normalized}-")))
}
