//! PubGrub-based pre-solver with CDCL conflict learning.
//!
//! Wraps APDR's version data behind the `DependencyProvider` trait so we get
//! context-aware conflict learning automatically.  This is tried before the
//! existing backtracking solver and MVS fallback.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;

use pubgrub::{Dependencies, DependencyProvider, Map, PackageResolutionStatistics};
use version_ranges::Ranges;

use crate::cache::store::{normalize, CacheStore};
use crate::resolver::{pypi_client, version_sampler};

// ---------------------------------------------------------------------------
// Custom version type (wraps our string-based PEP 440 comparison)
// ---------------------------------------------------------------------------

/// A Python package version. Delegates ordering to APDR's `version_sort_key`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PyVersion(pub String);

impl fmt::Display for PyVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Ord for PyVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        super::kgraph_db::version_sort_key(&self.0)
            .cmp(&super::kgraph_db::version_sort_key(&other.0))
    }
}

impl PartialOrd for PyVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// ---------------------------------------------------------------------------
// DependencyProvider
// ---------------------------------------------------------------------------

/// Adapter exposing APDR's package data to PubGrub.
pub struct ApdrProvider<'a> {
    store: RefCell<&'a mut CacheStore>,
    python_version: String,
    budget: RefCell<usize>,
    /// Root package constraints injected at creation time.
    root_deps: Map<String, Ranges<PyVersion>>,
}

#[derive(Debug)]
pub enum ApdrError {
    BudgetExhausted,
}

impl fmt::Display for ApdrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BudgetExhausted => write!(f, "PubGrub budget exhausted"),
        }
    }
}

impl std::error::Error for ApdrError {}

const ROOT_PACKAGE: &str = "__apdr_root__";

impl<'a> DependencyProvider for ApdrProvider<'a> {
    type P = String;
    type V = PyVersion;
    type VS = Ranges<PyVersion>;
    type Priority = std::cmp::Reverse<usize>;
    type Err = ApdrError;
    type M = String;

    fn prioritize(
        &self,
        package: &Self::P,
        range: &Self::VS,
        _stats: &PackageResolutionStatistics,
    ) -> Self::Priority {
        if package == ROOT_PACKAGE {
            return std::cmp::Reverse(0); // resolve root first
        }
        // MRV: fewer matching versions → higher priority
        let mut store = self.store.borrow_mut();
        let versions = pypi_client::compatible_versions(&mut store, package, &self.python_version);
        let count = versions
            .iter()
            .filter(|v| range.contains(&PyVersion(v.to_string())))
            .count();
        std::cmp::Reverse(count)
    }

    fn choose_version(
        &self,
        package: &Self::P,
        range: &Self::VS,
    ) -> Result<Option<Self::V>, Self::Err> {
        if package == ROOT_PACKAGE {
            let v = PyVersion("0.0.0".to_string());
            return Ok(if range.contains(&v) { Some(v) } else { None });
        }
        let mut store = self.store.borrow_mut();
        let versions = pypi_client::compatible_versions(&mut store, package, &self.python_version);
        // Prefer highest compatible version
        let best = versions
            .iter()
            .rev()
            .map(|v| PyVersion(v.to_string()))
            .find(|v| range.contains(v));
        Ok(best)
    }

    fn get_dependencies(
        &self,
        package: &Self::P,
        _version: &Self::V,
    ) -> Result<Dependencies<Self::P, Self::VS, Self::M>, Self::Err> {
        if package == ROOT_PACKAGE {
            return Ok(Dependencies::Available(self.root_deps.clone()));
        }
        let mut store = self.store.borrow_mut();
        let specs = pypi_client::dependency_specs(&mut store, package, &_version.0);
        let mut deps = Map::default();
        for spec_str in &specs {
            let (dep_name, constraint) = split_requirement(spec_str);
            if dep_name.is_empty() {
                continue;
            }
            let range = constraint_to_range(&constraint);
            deps.insert(dep_name, range);
        }
        Ok(Dependencies::Available(deps))
    }

    fn should_cancel(&self) -> Result<(), Self::Err> {
        let mut budget = self.budget.borrow_mut();
        if *budget == 0 {
            return Err(ApdrError::BudgetExhausted);
        }
        *budget -= 1;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Requirement parsing
// ---------------------------------------------------------------------------

/// Split a PEP 508 requirement string into (normalized_name, constraint).
/// e.g. "requests>=2.0,<3" → ("requests", ">=2.0,<3")
fn split_requirement(req: &str) -> (String, String) {
    let trimmed = req.trim();
    // Find first operator character
    let split_pos = trimmed
        .find(['<', '>', '!', '=', '~'])
        .unwrap_or(trimmed.len());
    let name_part = &trimmed[..split_pos];
    let constraint_part = &trimmed[split_pos..];
    // Strip extras (e.g. "package[extra]>=1.0")
    let name = name_part.split('[').next().unwrap_or(name_part);
    (normalize(name), constraint_part.to_string())
}

// ---------------------------------------------------------------------------
// Constraint → Range conversion
// ---------------------------------------------------------------------------

/// Convert a PEP 440 constraint string (e.g. ">=1.0,<2.0") into a `Ranges<PyVersion>`.
fn constraint_to_range(constraint: &str) -> Ranges<PyVersion> {
    if constraint.is_empty() {
        return Ranges::full();
    }
    let mut result = Ranges::full();
    for part in constraint.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let r = parse_single_constraint(part);
        result = result.intersection(&r);
    }
    result
}

fn parse_single_constraint(spec: &str) -> Ranges<PyVersion> {
    let spec = spec.trim();
    if let Some(v) = spec.strip_prefix(">=") {
        Ranges::higher_than(PyVersion(v.trim().to_string()))
    } else if let Some(v) = spec.strip_prefix('>') {
        Ranges::strictly_higher_than(PyVersion(v.trim().to_string()))
    } else if let Some(v) = spec.strip_prefix("<=") {
        Ranges::strictly_higher_than(PyVersion(v.trim().to_string())).complement()
    } else if let Some(v) = spec.strip_prefix('<') {
        Ranges::strictly_lower_than(PyVersion(v.trim().to_string()))
    } else if let Some(v) = spec.strip_prefix("==") {
        Ranges::singleton(PyVersion(v.trim().to_string()))
    } else if let Some(v) = spec.strip_prefix("!=") {
        Ranges::singleton(PyVersion(v.trim().to_string())).complement()
    } else if let Some(v) = spec.strip_prefix("~=") {
        // Compatible release: ~=X.Y means >=X.Y, <(X+1).0
        let v = v.trim();
        let lower = PyVersion(v.to_string());
        if let Some(dot) = v.rfind('.') {
            if let Ok(major) = v[..dot].parse::<u64>() {
                let upper = PyVersion(format!("{}.0", major + 1));
                return Ranges::higher_than(lower)
                    .intersection(&Ranges::strictly_lower_than(upper));
            }
        }
        Ranges::higher_than(lower)
    } else {
        // Bare version — treat as exact pin
        Ranges::singleton(PyVersion(spec.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Attempt to solve the dependency constraints using PubGrub's CDCL solver.
///
/// Returns `Ok(selected)` on success or `Err(reason)` on failure.
pub fn solve_with_pubgrub(
    store: &mut CacheStore,
    constraints: &BTreeMap<String, String>,
    python_version: &str,
) -> Result<BTreeMap<String, String>, String> {
    if constraints.is_empty() {
        return Ok(BTreeMap::new());
    }

    // Build root dependency map
    let mut root_deps = Map::default();
    for (package, constraint) in constraints {
        let versions = pypi_client::compatible_versions(store, package, python_version);
        if versions.is_empty() {
            return Err(format!(
                "PubGrub: package `{package}` has no compatible versions"
            ));
        }
        let range = constraint_to_range(constraint);
        root_deps.insert(package.clone(), range);
    }

    let provider = ApdrProvider {
        store: RefCell::new(store),
        python_version: python_version.to_string(),
        budget: RefCell::new(12_000),
        root_deps,
    };

    match pubgrub::resolve(
        &provider,
        ROOT_PACKAGE.to_string(),
        PyVersion("0.0.0".to_string()),
    ) {
        Ok(solution) => {
            let mut selected = BTreeMap::new();
            for (pkg, version) in solution {
                if pkg == ROOT_PACKAGE {
                    continue;
                }
                // Only include direct dependencies (from constraints), not transitives
                if constraints.contains_key(&pkg) {
                    selected.insert(pkg, version.0);
                }
            }
            // Ensure all constrained packages are present (PubGrub may have
            // resolved transitive deps that shadow a direct constraint name)
            for pkg in constraints.keys() {
                if !selected.contains_key(pkg) {
                    // Use version sampler as fallback
                    let versions = pypi_client::compatible_versions(
                        &mut provider.store.borrow_mut(),
                        pkg,
                        python_version,
                    );
                    if let Some(v) = version_sampler::equally_distanced_sample(&versions, &[]) {
                        selected.insert(pkg.clone(), v);
                    }
                }
            }
            Ok(selected)
        }
        Err(pubgrub::PubGrubError::NoSolution(_)) => {
            Err("PubGrub: no solution found (conflicting constraints)".to_string())
        }
        Err(pubgrub::PubGrubError::ErrorInShouldCancel(ApdrError::BudgetExhausted)) => {
            Err("PubGrub: budget exhausted (12,000 iterations)".to_string())
        }
        Err(e) => Err(format!("PubGrub error: {e}")),
    }
}
