use crate::docker;
use crate::{ParseResult, ResolvedDependency};
use super::detection::{
    normalize, uses_cfscrape_stack, uses_legacy_flask_stack, uses_legacy_ggplot_stack,
    uses_legacy_johnny_cache_stack, uses_legacy_pymc3_stack, uses_legacy_scrapy_stack,
    uses_legacy_tensorflow_stack, uses_simplecv_stack,
};
pub(super) fn apply_legacy_pymc3_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> Option<String> {
    if !uses_legacy_pymc3_stack(parse_result, resolved) {
        return None;
    }

    let bundle_python =
        preferred_legacy_pymc3_python(selected_python, python_range, execute_snippet);
    let mut changes = Vec::new();

    for (import_name, package_name, version) in legacy_pymc3_bundle(&bundle_python) {
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:legacy-pymc3",
            0.97,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        return None;
    }

    Some(format!(
        "Family knowledge targeted the legacy PyMC3 stack at Python {bundle_python} and pinned a coherent bundle: {}.",
        changes.join(", ")
    ))
}

pub(super) fn apply_legacy_flask_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
) -> Option<String> {
    if !uses_legacy_flask_stack(parse_result, resolved) {
        return None;
    }

    let mut changes = Vec::new();
    let needs_core = resolved.iter().any(|dep| {
        matches!(
            normalize(&dep.package_name).as_str(),
            "flask_security"
                | "flask_principal"
                | "flask_admin"
                | "flask_sqlalchemy"
                | "mongoengine"
                | "flask"
        )
    }) || parse_result.imports.iter().any(|item| {
        matches!(
            normalize(item).as_str(),
            "flask_security" | "flask_principal" | "flask_admin" | "flask_sqlalchemy" | "flask"
        )
    });

    for (import_name, package_name, version, essential) in legacy_flask_bundle(selected_python) {
        let already_present = resolved.iter().any(|dep| {
            dep.import_name.eq_ignore_ascii_case(import_name)
                || normalize(&dep.package_name) == normalize(package_name)
        });
        if !essential && !already_present {
            continue;
        }
        if !needs_core && *essential {
            continue;
        }
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:legacy-flask",
            0.95,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(format!(
            "Family knowledge pinned a legacy Flask compatibility bundle: {}.",
            changes.join(", ")
        ))
    }
}

pub(super) fn apply_legacy_johnny_cache_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
) -> Option<String> {
    if !selected_python.starts_with("2.") || !uses_legacy_johnny_cache_stack(parse_result, resolved)
    {
        return None;
    }

    let mut changes = Vec::new();
    for (import_name, package_name, version, essential) in
        legacy_johnny_cache_bundle(selected_python)
    {
        let already_present = resolved.iter().any(|dep| {
            dep.import_name.eq_ignore_ascii_case(import_name)
                || normalize(&dep.package_name) == normalize(package_name)
        });
        if !essential && !already_present {
            continue;
        }
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:legacy-johnny-cache",
            0.95,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(format!(
            "Family knowledge pinned a legacy johnny-cache/Django bundle: {}.",
            changes.join(", ")
        ))
    }
}

pub(super) fn apply_legacy_scrapy_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
) -> Option<String> {
    if !selected_python.starts_with("2.") || !uses_legacy_scrapy_stack(parse_result, resolved) {
        return None;
    }

    let mut changes = Vec::new();
    for (import_name, package_name, version, essential) in legacy_scrapy_bundle(selected_python) {
        let already_present = resolved.iter().any(|dep| {
            dep.import_name.eq_ignore_ascii_case(import_name)
                || normalize(&dep.package_name) == normalize(package_name)
        });
        if !essential && !already_present {
            continue;
        }
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:legacy-scrapy",
            0.94,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(format!(
            "Family knowledge pinned a legacy Scrapy compatibility bundle: {}.",
            changes.join(", ")
        ))
    }
}

pub(super) fn apply_cfscrape_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
) -> Option<String> {
    if !uses_cfscrape_stack(parse_result, resolved) {
        return None;
    }

    let mut changes = Vec::new();
    for (import_name, package_name, version) in [
        ("cfscrape", "cfscrape", "1.2.1"),
        ("urllib3", "urllib3", "1.26.18"),
    ] {
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:cfscrape",
            0.94,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(format!(
            "Family knowledge pinned the cfscrape compatibility bundle: {}.",
            changes.join(", ")
        ))
    }
}

pub(super) fn apply_legacy_ggplot_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
) -> Option<String> {
    if !uses_legacy_ggplot_stack(parse_result, resolved)
        || !(selected_python.starts_with("2.")
            || selected_python.starts_with("3.7")
            || selected_python.starts_with("3.8"))
    {
        return None;
    }

    let mut changes = Vec::new();
    for (import_name, package_name, version) in [
        ("ggplot", "ggplot", "0.11.5"),
        ("pandas", "pandas", "0.24.2"),
        ("matplotlib", "matplotlib", "2.2.5"),
        ("numpy", "numpy", "1.16.6"),
    ] {
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:legacy-ggplot",
            0.93,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(format!(
            "Family knowledge pinned the legacy ggplot/pandas bundle: {}.",
            changes.join(", ")
        ))
    }
}

pub(super) fn apply_simplecv_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
) -> Option<String> {
    if !uses_simplecv_stack(parse_result, resolved) {
        return None;
    }

    let mut changes = Vec::new();
    for (import_name, package_name, version) in [
        ("SimpleCV", "SimpleCV", "1.3"),
        ("cv2", "opencv-python-headless", "4.5.5.64"),
    ] {
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:simplecv",
            0.92,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(format!(
            "Family knowledge pinned the SimpleCV/OpenCV compatibility bundle: {}.",
            changes.join(", ")
        ))
    }
}

pub(super) fn apply_legacy_tensorflow_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> Option<String> {
    if !uses_legacy_tensorflow_stack(parse_result, resolved) {
        return None;
    }

    let bundle_python =
        preferred_legacy_tensorflow_python(selected_python, python_range, execute_snippet);
    let mut changes = Vec::new();

    for (import_name, package_name, version) in legacy_tensorflow_bundle(&bundle_python) {
        // Only pin bundle packages that already appear in the resolved list
        // (i.e. the snippet actually imports them). Don't add unrelated packages
        // like gym or keras when the snippet only uses tensorflow.
        // Exception: protobuf is a critical transitive dep of TF that must be
        // pinned to avoid descriptor breakage — always include it.
        let is_transitive_essential = *package_name == "protobuf";
        let already_resolved = resolved.iter().any(|dep| {
            dep.import_name.eq_ignore_ascii_case(import_name)
                || normalize(&dep.package_name) == normalize(package_name)
        });
        if !already_resolved && !is_transitive_essential {
            continue;
        }
        // Empty version means "let pip resolve freely" (Python 3.8+ path
        // where TF 1.x has no wheels).
        let version_opt = if version.is_empty() {
            None
        } else {
            Some(*version)
        };
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            version_opt,
            "family:legacy-tensorflow",
            0.96,
        ) {
            if version.is_empty() {
                changes.push(format!("{package_name} (unpinned)"));
            } else {
                changes.push(format!("{package_name}=={version}"));
            }
        }
    }

    if changes.is_empty() {
        return None;
    }

    Some(format!(
        "Family knowledge targeted the legacy TensorFlow/Keras stack at Python {bundle_python} and pinned a coherent bundle: {}.",
        changes.join(", ")
    ))
}

/// When standalone `keras` is resolved without any deep-learning backend
/// (tensorflow, jax, torch), add `tensorflow` as a companion dependency.
/// Modern keras 3.x requires a backend framework to function; without one,
/// `import keras` fails at runtime.
pub(super) fn ensure_keras_backend(
    resolved: &mut Vec<ResolvedDependency>,
    python_version: &str,
) -> Option<String> {
    // On Python 2, old keras 1.x/2.x used Theano as the default backend.
    // Adding tensorflow would cause install failures (no Python 2 support).
    if python_version.starts_with("2.") {
        return None;
    }

    let has_keras = resolved
        .iter()
        .any(|d| normalize(&d.package_name) == "keras");
    if !has_keras {
        return None;
    }

    let has_backend = resolved.iter().any(|d| {
        let pkg = normalize(&d.package_name);
        pkg == "tensorflow" || pkg == "torch" || pkg == "jax" || pkg.starts_with("tensorflow-")
    });
    if has_backend {
        return None;
    }

    resolved.push(ResolvedDependency {
        import_name: "tensorflow".to_string(),
        package_name: "tensorflow".to_string(),
        version: None,
        strategy: "family:keras-backend".to_string(),
        confidence: 0.92,
    });

    Some(
        "Family knowledge added tensorflow as the default backend for standalone keras."
            .to_string(),
    )
}

pub(super) fn apply_legacy_pillow_pin(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
) -> Option<String> {
    if !selected_python.starts_with("2.") {
        return None;
    }

    let pil_markers = [
        "pil",
        "image",
        "imagedraw",
        "imagefont",
        "imagefilter",
        "imagechops",
        "imageops",
        "imageenhance",
        "imagegrab",
    ];
    let references_pillow = parse_result
        .imports
        .iter()
        .map(|item| normalize(item))
        .any(|item| pil_markers.contains(&item.as_str()))
        || resolved
            .iter()
            .any(|dependency| normalize(&dependency.package_name) == "pillow");
    if !references_pillow {
        return None;
    }

    let mut changed = false;
    for dependency in resolved.iter_mut() {
        if normalize(&dependency.package_name) == "pillow" {
            let target_version = Some("6.2.2".to_string());
            let row_changed = dependency.package_name != "Pillow"
                || dependency.version != target_version
                || dependency.strategy != "family:legacy-pillow";
            dependency.package_name = "Pillow".to_string();
            dependency.version = Some("6.2.2".to_string());
            dependency.strategy = "family:legacy-pillow".to_string();
            dependency.confidence = 0.95;
            changed |= row_changed;
        }
    }

    if !resolved
        .iter()
        .any(|dependency| normalize(&dependency.package_name) == "pillow")
    {
        pin_dependency(
            resolved,
            "PIL",
            "Pillow",
            Some("6.2.2"),
            "family:legacy-pillow",
            0.95,
        );
        changed = true;
    }

    if changed {
        Some(
            "Family knowledge pinned Pillow to 6.2.2 for Python 2.7 PIL-era compatibility."
                .to_string(),
        )
    } else {
        None
    }
}

pub(super) fn preferred_legacy_pymc3_python(
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> String {
    if execute_snippet {
        return if selected_python.starts_with("3.10") || selected_python.starts_with("3.9") {
            selected_python.to_string()
        } else {
            "2.7".to_string()
        };
    }

    let candidates =
        docker::parallel::candidate_versions(selected_python, python_range, None, None);
    if candidates.iter().any(|value| value == "3.10") {
        "3.10".to_string()
    } else if candidates.iter().any(|value| value == "3.9") {
        "3.9".to_string()
    } else if candidates.iter().any(|value| value == "2.7") {
        "2.7".to_string()
    } else {
        selected_python.to_string()
    }
}

pub(super) fn preferred_legacy_tensorflow_python(
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> String {
    // Check which Python versions are actually available (installed).
    let base_candidates =
        docker::parallel::candidate_versions(selected_python, python_range, None, None);

    if execute_snippet {
        if selected_python.starts_with("2.") {
            return "2.7".to_string();
        }
        // Only prefer 3.7 if it's actually available.
        if base_candidates.iter().any(|v| v == "3.7") {
            return "3.7".to_string();
        }
        // Fall through to the general logic below.
    }

    // Prefer 3.7 (TF 1.15.0 has cp37 wheels) but only if available.
    if base_candidates.iter().any(|v| v == "3.7") {
        "3.7".to_string()
    } else if base_candidates.iter().any(|v| v == "2.7") {
        "2.7".to_string()
    } else if base_candidates.iter().any(|v| v == "3.8") {
        // 3.8+: bundle will use unpinned TF (TF 2.x)
        "3.8".to_string()
    } else if base_candidates.iter().any(|v| v == "3.9") {
        "3.9".to_string()
    } else if base_candidates.iter().any(|v| v == "3.10") {
        "3.10".to_string()
    } else {
        selected_python.to_string()
    }
}

pub(super) fn legacy_tensorflow_candidate_versions(selected_python: &str, python_range: usize) -> Vec<String> {
    let mut candidates =
        docker::parallel::candidate_versions(selected_python, python_range, None, None);
    // Force 2.7 if not already present (TF 1.15.0 has cp27 wheels).
    // Do NOT force 3.7: it's EOL and unavailable on ARM64 / Apple Silicon,
    // so forcing it just wastes a validation attempt.
    if !candidates.iter().any(|item| item == "2.7") {
        candidates.push("2.7".to_string());
    }
    candidates
}

pub(super) fn legacy_pymc3_bundle(
    bundle_python: &str,
) -> &'static [(&'static str, &'static str, &'static str)] {
    if bundle_python.starts_with("2.") {
        &[
            ("numpy", "numpy", "1.16.6"),
            ("pandas", "pandas", "0.24.2"),
            ("pymc3", "pymc3", "3.5"),
            ("scipy", "scipy", "1.2.3"),
            ("setuptools", "setuptools", "44.1.1"),
            ("theano", "Theano", "1.0.5"),
        ]
    } else {
        &[
            ("arviz", "arviz", "0.12.1"),
            ("numpy", "numpy", "1.21.6"),
            ("pandas", "pandas", "1.5.3"),
            ("pymc3", "pymc3", "3.11.5"),
            ("scipy", "scipy", "1.7.3"),
            ("setuptools", "setuptools", "69.5.1"),
            ("theano", "Theano-PyMC", "1.1.2"),
            ("xarray", "xarray", "2022.9.0"),
            ("xarray_einstats", "xarray-einstats", "0.6.0"),
        ]
    }
}

pub(super) fn legacy_tensorflow_bundle(
    bundle_python: &str,
) -> &'static [(&'static str, &'static str, &'static str)] {
    if bundle_python.starts_with("2.") {
        // gym 0.17+ dropped Python 2 support; protobuf must be <4 for TF 1.x
        &[
            ("gym", "gym", "0.16.0"),
            ("keras", "keras", "2.3.1"),
            ("numpy", "numpy", "1.16.6"),
            ("protobuf", "protobuf", "3.20.3"),
            ("tensorflow", "tensorflow", "1.15.0"),
        ]
    } else if bundle_python.starts_with("3.7") {
        // protobuf >=4 breaks TF 1.x descriptor generation
        &[
            ("gym", "gym", "0.17.3"),
            ("keras", "keras", "2.3.1"),
            ("numpy", "numpy", "1.16.6"),
            ("protobuf", "protobuf", "3.20.3"),
            ("tensorflow", "tensorflow", "1.15.0"),
        ]
    } else {
        // Python 3.8+: TF 1.15.0 has no wheels.  Leave versions empty so
        // pip can resolve freely — the latest compatible TF 2.x, keras, etc.
        // will be installed.  Empty-string version is treated as None by the
        // caller (apply_legacy_tensorflow_bundle).
        &[
            ("gym", "gym", ""),
            ("keras", "keras", ""),
            ("numpy", "numpy", ""),
            ("tensorflow", "tensorflow", ""),
        ]
    }
}

fn legacy_flask_bundle(
    selected_python: &str,
) -> &'static [(&'static str, &'static str, &'static str, bool)] {
    if selected_python.starts_with("2.") {
        &[
            ("flask", "Flask", "1.1.4", true),
            ("jinja2", "Jinja2", "2.11.3", true),
            ("markupsafe", "MarkupSafe", "1.1.1", true),
            ("werkzeug", "Werkzeug", "1.0.1", true),
            ("itsdangerous", "itsdangerous", "1.1.0", true),
            ("flask_sqlalchemy", "Flask-SQLAlchemy", "2.5.1", false),
            ("flask_security", "Flask-Security", "3.0.0", false),
            ("flask_principal", "Flask-Principal", "0.4.0", false),
            ("flask_admin", "Flask-Admin", "1.6.1", false),
            ("mongoengine", "mongoengine", "0.24.2", false),
        ]
    } else {
        &[
            ("flask", "Flask", "1.1.4", true),
            ("jinja2", "Jinja2", "2.11.3", true),
            ("markupsafe", "MarkupSafe", "1.1.1", true),
            ("werkzeug", "Werkzeug", "1.0.1", true),
            ("itsdangerous", "itsdangerous", "1.1.0", true),
            ("flask_sqlalchemy", "Flask-SQLAlchemy", "2.5.1", false),
            ("flask_security", "Flask-Security", "3.0.0", false),
            ("flask_principal", "Flask-Principal", "0.4.0", false),
            ("flask_admin", "Flask-Admin", "1.6.1", false),
            ("mongoengine", "mongoengine", "0.29.1", false),
        ]
    }
}

fn legacy_scrapy_bundle(
    selected_python: &str,
) -> &'static [(&'static str, &'static str, &'static str, bool)] {
    if selected_python.starts_with("2.") {
        &[
            ("scrapy", "scrapy", "1.8.3", true),
            ("lxml", "lxml", "4.6.5", true),
        ]
    } else {
        &[]
    }
}

fn legacy_johnny_cache_bundle(
    selected_python: &str,
) -> &'static [(&'static str, &'static str, &'static str, bool)] {
    if selected_python.starts_with("2.") {
        &[
            ("django", "Django", "1.8.19", true),
            ("johnny", "johnny-cache", "1.4", true),
        ]
    } else {
        &[]
    }
}

fn pin_dependency(
    resolved: &mut Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<&str>,
    strategy: &str,
    confidence: f64,
) -> bool {
    let target_version = version.map(str::to_string);
    for dependency in resolved.iter_mut() {
        let import_match = dependency.import_name.eq_ignore_ascii_case(import_name);
        let package_match = normalize(&dependency.package_name) == normalize(package_name);
        if import_match || package_match {
            let changed =
                dependency.package_name != package_name || dependency.version != target_version;
            dependency.import_name = import_name.to_string();
            dependency.package_name = package_name.to_string();
            dependency.version = target_version.clone();
            dependency.strategy = strategy.to_string();
            dependency.confidence = confidence;
            return changed;
        }
    }

    resolved.push(ResolvedDependency {
        import_name: import_name.to_string(),
        package_name: package_name.to_string(),
        version: target_version,
        strategy: strategy.to_string(),
        confidence,
    });
    true
}


