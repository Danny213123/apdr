use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::cache::store::CacheStore;
use crate::docker;
use crate::resolver::{family_knowledge, pypi_client};
use crate::{ConfigDep, ParseResult, ResolveConfig, ResolvedDependency};

#[derive(Clone, Debug, Default)]
pub struct PreSolveResult {
    pub attempted: bool,
    pub satisfiable: bool,
    pub hard_unsat: bool,
    pub selected_python_version: String,
    pub lockfile_requirements: String,
    pub assigned_versions: BTreeMap<String, String>,
    pub direct_packages: Vec<String>,
    pub transitive_packages: Vec<String>,
    pub notes: Vec<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct SolveState {
    constraints: BTreeMap<String, String>,
    selected: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct SolveOutcome {
    python_version: String,
    selected: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
enum SolveError {
    Hard(String),
    Incomplete(String),
}

pub fn solve_dependency_graph(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
    selected_python: &str,
    store: &mut CacheStore,
    config: &ResolveConfig,
) -> PreSolveResult {
    let mut result = PreSolveResult {
        selected_python_version: selected_python.to_string(),
        lockfile_requirements: render_direct_requirements(resolved),
        ..Default::default()
    };

    if resolved.is_empty() && parse_result.config_deps.is_empty() {
        result.notes.push("Skipped SMT pre-solve because there were no resolved packages or config dependencies.".to_string());
        return result;
    }

    let python_candidates = solver_candidate_versions(
        parse_result,
        resolved,
        selected_python,
        config,
    );
    result.notes.push(format!(
        "SMT pre-solve candidate Python versions: {}.",
        python_candidates.join(", ")
    ));

    let (state, direct_packages) = initial_state(resolved, &parse_result.config_deps);
    result.direct_packages = direct_packages.clone();
    if state.constraints.is_empty() {
        result.notes.push("Skipped SMT pre-solve because no package constraints were available.".to_string());
        return result;
    }

    // Bulk pre-fetch direct packages from KGraph in one subprocess call.
    // This replaces many sequential subprocess invocations during solving.
    // Note: Transitive prefetch was tested but caused excessive overhead for large graphs.
    let prefetch_packages: Vec<String> = state.constraints.keys().cloned().collect();
    pypi_client::bulk_prefetch_from_kgraph(store, &prefetch_packages);

    result.attempted = true;

    // Optimization: for single Python version, use sequential solving to avoid thread overhead
    if python_candidates.len() == 1 {
        let python_version = &python_candidates[0];
        let mut budget = 12_000usize;
        match solve_for_python(store, &state, python_version, &mut budget) {
            Ok(outcome) => {
                let (requirements, transitive_packages) =
                    render_lockfile(&outcome.selected, &direct_packages);
                result.satisfiable = true;
                result.selected_python_version = outcome.python_version;
                result.lockfile_requirements = requirements;
                result.transitive_packages = transitive_packages;
                result.assigned_versions = outcome.selected;
                result.notes.push(format!(
                    "SMT pre-solve pinned {} packages for Python {} ({} direct, {} transitive).",
                    result.assigned_versions.len(),
                    result.selected_python_version,
                    result.direct_packages.len(),
                    result.transitive_packages.len()
                ));
                return result;
            }
            Err(SolveError::Hard(reason)) => {
                result.hard_unsat = true;
                result.reason = Some(format!(
                    "SMT pre-solve could not find a compatible dependency assignment. {python_version}: {reason}"
                ));
                result.notes.push(result.reason.clone().unwrap_or_default());
                return result;
            }
            Err(SolveError::Incomplete(reason)) => {
                result.reason = Some(format!(
                    "SMT pre-solve fell back because dependency metadata was incomplete. {python_version}: {reason}"
                ));
                result.notes.push(result.reason.clone().unwrap_or_default());
                return result;
            }
        }
    }

    // For multiple Python versions, use parallel solving for faster resolution.
    // Each thread gets a cloned CacheStore (cheap since all data is already prefetched).
    let success = Arc::new(Mutex::new(None::<SolveOutcome>));
    let hard_failures = Arc::new(Mutex::new(Vec::new()));
    let incomplete_failures = Arc::new(Mutex::new(Vec::new()));

    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for python_version in python_candidates {
            let state_clone = state.clone();
            let mut store_clone = store.clone();
            let success_ref = Arc::clone(&success);
            let hard_ref = Arc::clone(&hard_failures);
            let incomplete_ref = Arc::clone(&incomplete_failures);

            let handle = scope.spawn(move || {
                // Check if another thread already succeeded
                if success_ref.lock().unwrap().is_some() {
                    return;
                }

                let mut budget = 12_000usize;
                match solve_for_python(&mut store_clone, &state_clone, &python_version, &mut budget) {
                    Ok(outcome) => {
                        let mut success_guard = success_ref.lock().unwrap();
                        if success_guard.is_none() {
                            *success_guard = Some(outcome);
                        }
                    }
                    Err(SolveError::Hard(reason)) => {
                        hard_ref.lock().unwrap().push(format!("{python_version}: {reason}"));
                    }
                    Err(SolveError::Incomplete(reason)) => {
                        incomplete_ref.lock().unwrap().push(format!("{python_version}: {reason}"));
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
    });

    if let Some(outcome) = Arc::try_unwrap(success).unwrap().into_inner().unwrap() {
        let (requirements, transitive_packages) =
            render_lockfile(&outcome.selected, &direct_packages);
        result.satisfiable = true;
        result.selected_python_version = outcome.python_version;
        result.lockfile_requirements = requirements;
        result.transitive_packages = transitive_packages;
        result.assigned_versions = outcome.selected;
        result.notes.push(format!(
            "SMT pre-solve pinned {} packages for Python {} ({} direct, {} transitive).",
            result.assigned_versions.len(),
            result.selected_python_version,
            result.direct_packages.len(),
            result.transitive_packages.len()
        ));
        return result;
    }

    let hard_failures = Arc::try_unwrap(hard_failures).unwrap().into_inner().unwrap();
    let incomplete_failures = Arc::try_unwrap(incomplete_failures).unwrap().into_inner().unwrap();

    if !hard_failures.is_empty() && incomplete_failures.is_empty() {
        result.hard_unsat = true;
        result.reason = Some(format!(
            "SMT pre-solve could not find a compatible dependency assignment. {}",
            hard_failures.join(" | ")
        ));
        result.notes.push(result.reason.clone().unwrap_or_default());
    } else if !incomplete_failures.is_empty() {
        result.reason = Some(format!(
            "SMT pre-solve fell back because dependency metadata was incomplete. {}",
            incomplete_failures.join(" | ")
        ));
        result.notes.push(result.reason.clone().unwrap_or_default());
    }
    result
}

fn solver_candidate_versions(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
    selected_python: &str,
    config: &ResolveConfig,
) -> Vec<String> {
    let mut versions = if config.parallel_versions {
        family_knowledge::validation_candidate_versions(
            parse_result,
            resolved,
            selected_python,
            config.python_version_range,
            config.execute_snippet,
        )
        .unwrap_or_else(|| docker::parallel::candidate_versions(selected_python, config.python_version_range))
    } else {
        vec![selected_python.to_string()]
    };
    if versions.is_empty() {
        versions.push(selected_python.to_string());
    }
    dedupe_strings(versions)
}

fn initial_state(
    resolved: &[ResolvedDependency],
    config_deps: &[ConfigDep],
) -> (SolveState, Vec<String>) {
    let mut state = SolveState::default();
    let mut direct_packages = Vec::new();

    for dependency in resolved {
        let package = pypi_client::requirement_name(&dependency.package_name);
        if package.is_empty() {
            continue;
        }
        if !direct_packages.iter().any(|item| item == &package) {
            direct_packages.push(package.clone());
        }
        let constraint = dependency
            .version
            .as_ref()
            .map(|value| format!("=={value}"))
            .unwrap_or_default();
        merge_constraint(&mut state.constraints, &package, &constraint);
    }

    for dependency in config_deps {
        let package = pypi_client::requirement_name(&dependency.package);
        if package.is_empty() {
            continue;
        }
        if !direct_packages.iter().any(|item| item == &package) {
            direct_packages.push(package.clone());
        }
        merge_constraint(
            &mut state.constraints,
            &package,
            dependency.constraint.as_deref().unwrap_or(""),
        );
    }

    (state, direct_packages)
}

fn solve_for_python(
    store: &mut CacheStore,
    state: &SolveState,
    python_version: &str,
    budget: &mut usize,
) -> Result<SolveOutcome, SolveError> {
    let selected = solve_recursive(store, state.clone(), python_version, budget)?;
    Ok(SolveOutcome {
        python_version: python_version.to_string(),
        selected,
    })
}

fn solve_recursive(
    store: &mut CacheStore,
    state: SolveState,
    python_version: &str,
    budget: &mut usize,
) -> Result<BTreeMap<String, String>, SolveError> {
    if *budget == 0 {
        return Err(SolveError::Incomplete(
            "solver budget exhausted before finding a compatible assignment".to_string(),
        ));
    }
    *budget -= 1;

    // Unit propagation: eagerly assign packages with exactly 1 candidate.
    // This avoids branching on forced choices and cascades constraints early.
    let state = propagate_forced(store, state, python_version, budget)?;

    let Some(package) = next_unsolved_package(store, &state, python_version)? else {
        return Ok(state.selected);
    };

    let constraint = state.constraints.get(&package).cloned().unwrap_or_default();
    let candidates = compatible_versions_for_constraint(store, &package, &constraint, python_version)?;
    if candidates.is_empty() {
        return Err(SolveError::Hard(format!(
            "package `{package}` has no versions satisfying `{}`",
            if constraint.is_empty() { "*" } else { constraint.as_str() }
        )));
    }

    let mut last_failure: Option<SolveError> = None;
    for version in candidates.into_iter().rev() {
        let mut next_state = state.clone();
        next_state.selected.insert(package.clone(), version.clone());
        match apply_dependency_specs(store, &mut next_state, &package, &version, python_version) {
            Ok(()) => match solve_recursive(store, next_state, python_version, budget) {
                Ok(solution) => return Ok(solution),
                Err(reason) => last_failure = Some(reason),
            },
            Err(reason) => last_failure = Some(reason),
        }
    }

    if let Some(reason) = last_failure {
        Err(reason)
    } else {
        Err(SolveError::Hard(format!(
            "no compatible version could be selected for `{package}`"
        )))
    }
}

/// Iteratively assign packages that have exactly one compatible version.
/// This is analogous to unit propagation in SAT/SMT solvers: forced assignments
/// are made without branching, and their transitive dependencies are propagated
/// until no more forced assignments remain.
fn propagate_forced(
    store: &mut CacheStore,
    mut state: SolveState,
    python_version: &str,
    budget: &mut usize,
) -> Result<SolveState, SolveError> {
    loop {
        let mut progress = false;
        let packages: Vec<String> = state
            .constraints
            .keys()
            .filter(|pkg| !state.selected.contains_key(*pkg))
            .cloned()
            .collect();

        for package in packages {
            if state.selected.contains_key(&package) {
                continue;
            }
            let constraint = state.constraints.get(&package).cloned().unwrap_or_default();
            let candidates =
                compatible_versions_for_constraint(store, &package, &constraint, python_version)?;
            if candidates.is_empty() {
                return Err(SolveError::Hard(format!(
                    "package `{package}` has no versions satisfying `{}`",
                    if constraint.is_empty() {
                        "*"
                    } else {
                        constraint.as_str()
                    }
                )));
            }
            if candidates.len() == 1 {
                let version = candidates.into_iter().next().unwrap();
                state.selected.insert(package.clone(), version.clone());
                apply_dependency_specs(store, &mut state, &package, &version, python_version)?;
                progress = true;
                if *budget == 0 {
                    return Err(SolveError::Incomplete(
                        "solver budget exhausted during unit propagation".to_string(),
                    ));
                }
                *budget -= 1;
            }
        }

        if !progress {
            break;
        }
    }
    Ok(state)
}

fn next_unsolved_package(
    store: &mut CacheStore,
    state: &SolveState,
    python_version: &str,
) -> Result<Option<String>, SolveError> {
    let mut best: Option<(String, usize)> = None;
    for (package, constraint) in &state.constraints {
        if state.selected.contains_key(package) {
            continue;
        }
        let candidates = compatible_versions_for_constraint(store, package, constraint, python_version)?;
        if candidates.is_empty() {
            return Err(SolveError::Hard(format!(
                "package `{package}` has no versions satisfying `{}`",
                if constraint.is_empty() { "*" } else { constraint.as_str() }
            )));
        }
        let count = candidates.len();
        match &best {
            Some((_current, current_count)) if *current_count <= count => {}
            _ => best = Some((package.clone(), count)),
        }
    }
    Ok(best.map(|(package, _)| package))
}

fn apply_dependency_specs(
    store: &mut CacheStore,
    state: &mut SolveState,
    package: &str,
    version: &str,
    python_version: &str,
) -> Result<(), SolveError> {
    for spec in pypi_client::dependency_specs(store, package, version) {
        let dep_package = pypi_client::requirement_name(&spec);
        if dep_package.is_empty() {
            continue;
        }
        let dep_constraint = dependency_constraint(&spec);
        merge_constraint(&mut state.constraints, &dep_package, &dep_constraint);
        if let Some(selected_version) = state.selected.get(&dep_package) {
            let merged = state.constraints.get(&dep_package).cloned().unwrap_or_default();
            if !merged.is_empty()
                && !pypi_client::version_satisfies(selected_version, &merged)
            {
                return Err(SolveError::Hard(format!(
                    "selected `{dep_package}=={selected_version}` violates merged constraint `{merged}`"
                )));
            }
            continue;
        }
        let merged = state.constraints.get(&dep_package).cloned().unwrap_or_default();
        if compatible_versions_for_constraint(store, &dep_package, &merged, python_version)?.is_empty() {
            return Err(SolveError::Hard(format!(
                "dependency `{dep_package}` introduced by `{package}=={version}` has no versions satisfying `{}`",
                if merged.is_empty() { "*" } else { merged.as_str() }
            )));
        }
    }
    Ok(())
}

fn compatible_versions_for_constraint(
    store: &mut CacheStore,
    package: &str,
    constraint: &str,
    python_version: &str,
) -> Result<Vec<String>, SolveError> {
    let all_versions = pypi_client::compatible_versions(store, package, python_version);
    if all_versions.is_empty() {
        return Err(SolveError::Incomplete(format!(
            "package `{package}` has no cached or KGraph version metadata"
        )));
    }
    Ok(all_versions
        .into_iter()
        .filter(|version| constraint.is_empty() || pypi_client::version_satisfies(version, constraint))
        .collect())
}

fn merge_constraint(
    constraints: &mut BTreeMap<String, String>,
    package: &str,
    incoming: &str,
) {
    let normalized_package = pypi_client::requirement_name(package);
    if normalized_package.is_empty() {
        return;
    }
    let cleaned = incoming.trim().trim_start_matches(',').trim();
    if cleaned.is_empty() {
        constraints.entry(normalized_package).or_default();
        return;
    }

    let entry = constraints.entry(normalized_package).or_default();
    if entry.is_empty() {
        *entry = cleaned.to_string();
        return;
    }

    let mut existing_parts = entry
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    for fragment in cleaned.split(',').map(str::trim).filter(|item| !item.is_empty()) {
        if !existing_parts.iter().any(|item| item == fragment) {
            existing_parts.push(fragment.to_string());
        }
    }
    *entry = existing_parts.join(",");
}

fn dependency_constraint(spec: &str) -> String {
    let trimmed = spec.split(';').next().unwrap_or(spec).trim();
    let Some(index) = trimmed.find(|ch: char| "<>!=~".contains(ch)) else {
        return String::new();
    };
    trimmed[index..].trim().to_string()
}

fn render_direct_requirements(resolved: &[ResolvedDependency]) -> String {
    resolved
        .iter()
        .map(|dependency| match &dependency.version {
            Some(version) => format!("{}=={}", dependency.package_name, version),
            None => dependency.package_name.clone(),
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn render_lockfile(
    selected: &BTreeMap<String, String>,
    direct_packages: &[String],
) -> (String, Vec<String>) {
    let direct_set = direct_packages
        .iter()
        .map(|item| pypi_client::requirement_name(item))
        .collect::<BTreeSet<_>>();

    let mut lines = Vec::new();
    for package in direct_packages {
        let normalized = pypi_client::requirement_name(package);
        if let Some(version) = selected.get(&normalized) {
            lines.push(format!("{normalized}=={version}"));
        }
    }

    let mut transitive = selected
        .iter()
        .filter(|(package, _)| !direct_set.contains(*package))
        .map(|(package, version)| format!("{package}=={version}"))
        .collect::<Vec<_>>();
    transitive.sort();
    let transitive_packages = transitive
        .iter()
        .filter_map(|item| item.split_once("==").map(|(package, _)| package.to_string()))
        .collect::<Vec<_>>();
    lines.extend(transitive);
    let lockfile = if lines.is_empty() {
        String::new()
    } else {
        lines.join("\n") + "\n"
    };
    (lockfile, transitive_packages)
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            deduped.push(value);
        }
    }
    deduped
}

