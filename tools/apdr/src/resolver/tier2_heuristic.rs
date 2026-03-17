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
                // Substring match requires the shorter name to be at least
                // 50% the length of the longer — prevents common words like
                // "settings" from matching "pydantic-settings".
                let min_len = normalized.len().min(candidate.len());
                let max_len = normalized.len().max(candidate.len());
                let length_ratio_ok = max_len == 0 || min_len * 2 >= max_len;
                let substring_match = !is_short
                    && length_ratio_ok
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
    // Package names are ASCII — use bytes directly (no Vec<char> allocation).
    let lb = left.as_bytes();
    let rb = right.as_bytes();
    if lb.is_empty() {
        return rb.len();
    }
    if rb.is_empty() {
        return lb.len();
    }
    // Stack-allocated cost array — package names are always short (<128 bytes).
    let mut costs = [0usize; 129];
    for (i, c) in costs.iter_mut().enumerate().take(rb.len() + 1) {
        *c = i;
    }
    for (li, &lc) in lb.iter().enumerate() {
        let mut corner = costs[0];
        costs[0] = li + 1;
        for (ri, &rc) in rb.iter().enumerate() {
            let upper = costs[ri + 1];
            let sub = if lc == rc { corner } else { corner + 1 };
            costs[ri + 1] = sub.min(costs[ri] + 1).min(upper + 1);
            corner = upper;
        }
    }
    costs[rb.len()]
}

fn looks_like_local_helper_import(parse_result: &ParseResult, import_name: &str) -> bool {
    let normalized = normalize(import_name);
    if normalized == "input-data" {
        return true;
    }
    // Django/Flask project-local modules: when the framework is imported, treat
    // `settings`, `urls`, `wsgi`, `asgi`, `apps` as local project modules.
    if matches!(
        normalized.as_str(),
        "settings" | "urls" | "wsgi" | "asgi" | "apps" | "conf"
    ) {
        let has_framework = parse_result.imports.iter().any(|i| {
            let n = normalize(i);
            n == "django" || n.starts_with("django-") || n == "flask"
        }) || parse_result.import_paths.iter().any(|p| {
            let n = p.to_ascii_lowercase();
            n.starts_with("django.") || n.starts_with("flask.")
        });
        if has_framework {
            return true;
        }
    }
    let generic_helper = matches!(
        normalized.as_str(),
        "util" | "utils" | "helper" | "helpers" | "common" | "shared"
    );
    generic_helper
        && parse_result.import_paths.iter().any(|path| {
            let np = normalize(path);
            np.len() > normalized.len()
                && np.starts_with(normalized.as_str())
                && np.as_bytes()[normalized.len()] == b'-'
        })
}
