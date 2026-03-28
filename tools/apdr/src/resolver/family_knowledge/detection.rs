use super::core::EXPLICIT_NAMESPACE_MAPPINGS;
use super::{curated_family_knowledge_snapshot, FamilyRegistry, RuntimeFamily};
use crate::{ParseResult, ResolvedDependency};
use std::collections::BTreeSet;
pub fn normalize(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace(['-', '.'], "_")
}

pub fn namespace_mapping_allowed(import_name: &str, package_name: &str) -> bool {
    let import_norm = normalize(import_name);
    let package_norm = normalize(package_name);
    if import_norm.is_empty() || package_norm.is_empty() {
        return false;
    }
    if import_norm == package_norm {
        return true;
    }

    let registry = FamilyRegistry::new();
    if let Some(family) = registry.runtime_family_for_package(package_name) {
        if family_member_provides_import(family, &package_norm, import_name) {
            return true;
        }
    }

    if let Some(mapping) = curated_family_knowledge_snapshot()
        .and_then(|curated| curated.explicit_namespace_mapping(import_name).cloned())
    {
        return normalize(&mapping.package_name) == package_norm;
    }

    EXPLICIT_NAMESPACE_MAPPINGS
        .iter()
        .any(|(import_alias, package_alias)| {
            normalize(import_alias) == import_norm && normalize(package_alias) == package_norm
        })
}

pub(super) fn uses_legacy_pymc3_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    let imports = parse_result
        .imports
        .iter()
        .chain(parse_result.import_paths.iter())
        .map(|item| item.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let packages = resolved
        .iter()
        .map(|dependency| normalize(&dependency.package_name))
        .collect::<BTreeSet<_>>();

    imports.contains("pymc3")
        || imports.contains("theano")
        || packages.contains("pymc3")
        || packages.contains("theano_pymc")
        || packages.contains("theano")
}

pub(super) fn uses_legacy_tensorflow_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    let imports = parse_result
        .imports
        .iter()
        .chain(parse_result.import_paths.iter())
        .map(|item| item.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let packages = resolved
        .iter()
        .map(|dependency| normalize(&dependency.package_name))
        .collect::<BTreeSet<_>>();

    let has_tensorflow = imports.contains("tensorflow")
        || imports.iter().any(|item| item.starts_with("tensorflow."))
        || resolved.iter().any(|dep| {
            normalize(&dep.package_name) == "tensorflow" && dep.strategy != "family:keras-backend"
        });
    let has_standalone_keras = imports.contains("keras")
        || imports.iter().any(|item| item.starts_with("keras."))
        || packages.contains("keras");
    let py2_target = parse_result.python_version_min.starts_with("2.")
        || parse_result
            .python_version_max
            .as_deref()
            .map(|value| value.starts_with("2."))
            .unwrap_or(false);

    has_tensorflow && (has_standalone_keras || py2_target)
}

pub(super) fn uses_legacy_flask_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    let (imports, packages) = collect_markers(parse_result, resolved);
    let legacy_markers = [
        "flask_security",
        "flask_principal",
        "flask_admin",
        "flask_sqlalchemy",
        "mongoengine",
        "jinja2",
        "markupsafe",
        "werkzeug",
        "itsdangerous",
    ];
    let has_flask = imports.contains("flask")
        || imports.iter().any(|item| item.starts_with("flask."))
        || packages.contains("flask");
    let has_legacy_marker = legacy_markers.iter().any(|item| {
        imports.contains(*item)
            || imports
                .iter()
                .any(|value| value.starts_with(&format!("{item}.")))
            || packages.contains(*item)
    });
    has_flask && has_legacy_marker
}

pub(super) fn uses_cfscrape_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    let (imports, packages) = collect_markers(parse_result, resolved);
    imports.contains("cfscrape") || packages.contains("cfscrape")
}

pub(super) fn uses_legacy_ggplot_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    let (imports, packages) = collect_markers(parse_result, resolved);
    imports.contains("ggplot") || packages.contains("ggplot")
}

pub(super) fn uses_simplecv_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    let (imports, packages) = collect_markers(parse_result, resolved);
    imports.contains("simplecv") || packages.contains("simplecv")
}

fn collect_markers(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> (BTreeSet<String>, BTreeSet<String>) {
    let imports = parse_result
        .imports
        .iter()
        .chain(parse_result.import_paths.iter())
        .map(|item| item.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let packages = resolved
        .iter()
        .map(|dependency| normalize(&dependency.package_name))
        .collect::<BTreeSet<_>>();
    (imports, packages)
}

fn family_member_provides_import(
    family: RuntimeFamily,
    package_norm: &str,
    import_name: &str,
) -> bool {
    family.member_provides_import(package_norm, import_name)
}

pub(super) fn uses_legacy_johnny_cache_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    let (imports, packages) = collect_markers(parse_result, resolved);
    imports.contains("johnny")
        || imports.iter().any(|item| item.starts_with("johnny."))
        || packages.contains("johnny_cache")
}

pub(super) fn uses_legacy_scrapy_stack(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
) -> bool {
    resolved
        .iter()
        .any(|dep| normalize(&dep.package_name) == "scrapy")
        || parse_result
            .imports
            .iter()
            .any(|import_name| normalize(import_name) == "scrapy")
}
