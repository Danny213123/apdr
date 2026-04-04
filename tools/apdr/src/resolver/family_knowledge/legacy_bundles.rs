use super::detection::{
    normalize, uses_cfscrape_stack, uses_legacy_flask_stack, uses_legacy_ggplot_stack,
    uses_legacy_johnny_cache_stack, uses_legacy_pymc3_stack, uses_legacy_scrapy_stack,
    uses_legacy_tensorflow_stack, uses_simplecv_stack,
};
use super::{curated_family_knowledge_snapshot, CuratedBundleMember, CuratedRecoveryRule};
use crate::docker;
use crate::{ParseResult, ResolvedDependency};

fn curated_rule(rule_id: &str) -> Option<CuratedRecoveryRule> {
    curated_family_knowledge_snapshot().and_then(|curated| curated.recovery_rule(rule_id).cloned())
}

fn python_version_tuple(python_version: &str) -> Option<(u32, u32)> {
    let mut parts = python_version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor))
}

fn python_selector_matches(selector: &str, bundle_python: &str) -> bool {
    match selector {
        "any" => true,
        "py2" => bundle_python.starts_with("2."),
        "py3" => !bundle_python.starts_with("2."),
        "py37" => bundle_python.starts_with("3.7"),
        "py38" => bundle_python.starts_with("3.8"),
        "py38-plus" => python_version_tuple(bundle_python)
            .is_some_and(|(major, minor)| major > 3 || (major == 3 && minor >= 8)),
        _ => false,
    }
}

fn bundle_members_for_rule(rule_id: &str, bundle_python: &str) -> Option<Vec<CuratedBundleMember>> {
    let rule = curated_rule(rule_id)?;
    rule.bundle_variants
        .iter()
        .find(|variant| {
            variant.python_selectors.is_empty()
                || variant
                    .python_selectors
                    .iter()
                    .any(|selector| python_selector_matches(selector, bundle_python))
        })
        .map(|variant| variant.members.clone())
}

fn render_template(template: &str, bundle_python: &str, changes: &[String]) -> String {
    template
        .replace("{bundle_python}", bundle_python)
        .replace("{changes}", &changes.join(", "))
}

pub(super) fn render_rule_recovery_prefix(rule_id: &str, fallback: &str) -> String {
    curated_rule(rule_id)
        .and_then(|rule| rule.recovery_note_prefix)
        .unwrap_or_else(|| fallback.to_string())
}

pub(super) fn render_rule_locked_note(
    rule_id: &str,
    bundle_python: &str,
    fallback: &str,
) -> String {
    curated_rule(rule_id)
        .and_then(|rule| rule.recovery_locked_note_template)
        .map(|template| render_template(&template, bundle_python, &[]))
        .unwrap_or_else(|| fallback.to_string())
}

pub(super) fn render_rule_unpinned_note(rule_id: &str, fallback: &str) -> String {
    curated_rule(rule_id)
        .and_then(|rule| rule.recovery_unpinned_note_template)
        .unwrap_or_else(|| fallback.to_string())
}

pub(super) fn rule_log_matches_triggers(rule_id: &str, lowercase_log: &str) -> bool {
    if let Some(rule) = curated_rule(rule_id) {
        return rule
            .trigger_substrings
            .iter()
            .any(|pattern| lowercase_log.contains(&pattern.to_ascii_lowercase()));
    }

    match rule_id {
        "legacy-pymc3" => {
            lowercase_log.contains("pymc3 3.11.5 depends on scipy<1.8.0")
                || lowercase_log.contains("pymc3 3.11.5 depends on numpy<1.22.2")
                || lowercase_log
                    .contains("could not find a version that satisfies the requirement pandas==")
                || lowercase_log.contains("no matching distribution found for pandas==")
                || lowercase_log
                    .contains("could not find a version that satisfies the requirement numpy==")
                || lowercase_log.contains("no matching distribution found for numpy==")
                || lowercase_log.contains("modulenotfounderror: no module named 'pkg_resources'")
                || lowercase_log
                    .contains("typeerror: 'numpy._dtypemeta' object is not subscriptable")
                || lowercase_log.contains("requires a different python version")
                || lowercase_log.contains("cannot import 'setuptools.build_meta'")
                || lowercase_log.contains("resolutionimpossible")
        }
        "legacy-tensorflow" => {
            lowercase_log.contains("requires a different python version")
                || lowercase_log.contains(
                    "could not find a version that satisfies the requirement tensorflow==",
                )
                || lowercase_log.contains("no matching distribution found for tensorflow==")
                || lowercase_log
                    .contains("could not find a version that satisfies the requirement keras==")
                || lowercase_log.contains("no matching distribution found for keras==")
                || lowercase_log.contains("resolutionimpossible")
        }
        _ => false,
    }
}

pub(super) fn preferred_rule_python_order(
    rule_id: &str,
    execute_snippet: bool,
    fallback_default_order: &[&str],
) -> Vec<String> {
    if let Some(rule) = curated_rule(rule_id) {
        if let Some(order) = rule.preferred_python_order {
            let selected = if execute_snippet && !order.execute_snippet.is_empty() {
                order.execute_snippet
            } else if !order.default_order.is_empty() {
                order.default_order
            } else {
                Vec::new()
            };
            if !selected.is_empty() {
                return selected;
            }
        }
    }

    match (rule_id, execute_snippet) {
        ("legacy-pymc3", true) => vec!["2.7".to_string(), "3.10".to_string(), "3.9".to_string()],
        ("legacy-tensorflow", true) => vec![
            "2.7".to_string(),
            "3.7".to_string(),
            "3.8".to_string(),
            "3.9".to_string(),
            "3.10".to_string(),
        ],
        _ => fallback_default_order
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    }
}
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
    let rule = curated_rule("legacy-pymc3");
    let strategy = rule
        .as_ref()
        .map(|rule| rule.strategy.as_str())
        .unwrap_or("family:legacy-pymc3");

    for (import_name, package_name, version) in legacy_pymc3_bundle(&bundle_python) {
        if pin_dependency(
            resolved,
            &import_name,
            &package_name,
            version.as_deref(),
            strategy,
            0.97,
        ) {
            if let Some(version) = version {
                changes.push(format!("{package_name}=={version}"));
            }
        }
    }

    if changes.is_empty() {
        return None;
    }

    Some(
        rule.and_then(|rule| rule.apply_note_template)
            .map(|template| render_template(&template, &bundle_python, &changes))
            .unwrap_or_else(|| {
                format!(
                    "Family knowledge targeted the legacy PyMC3 stack at Python {bundle_python} and pinned a coherent bundle: {}.",
                    changes.join(", ")
                )
            }),
    )
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
    let rule = curated_rule("legacy-ggplot");
    let strategy = rule
        .as_ref()
        .map(|rule| rule.strategy.as_str())
        .unwrap_or("family:legacy-ggplot");
    let members = bundle_members_for_rule("legacy-ggplot", selected_python).unwrap_or_else(|| {
        vec![
            CuratedBundleMember {
                import_name: "ggplot".to_string(),
                package_name: "ggplot".to_string(),
                version: Some("0.11.5".to_string()),
            },
            CuratedBundleMember {
                import_name: "pandas".to_string(),
                package_name: "pandas".to_string(),
                version: Some("0.24.2".to_string()),
            },
            CuratedBundleMember {
                import_name: "matplotlib".to_string(),
                package_name: "matplotlib".to_string(),
                version: Some("2.2.5".to_string()),
            },
            CuratedBundleMember {
                import_name: "numpy".to_string(),
                package_name: "numpy".to_string(),
                version: Some("1.16.6".to_string()),
            },
        ]
    });
    for member in members {
        if pin_dependency(
            resolved,
            &member.import_name,
            &member.package_name,
            member.version.as_deref(),
            strategy,
            0.93,
        ) {
            if let Some(version) = member.version {
                changes.push(format!("{}=={version}", member.package_name));
            }
        }
    }

    if changes.is_empty() {
        None
    } else {
        Some(
            rule.and_then(|rule| rule.apply_note_template)
                .map(|template| render_template(&template, selected_python, &changes))
                .unwrap_or_else(|| {
                    format!(
                        "Family knowledge pinned the legacy ggplot/pandas bundle: {}.",
                        changes.join(", ")
                    )
                }),
        )
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
    let rule = curated_rule("legacy-tensorflow");
    let strategy = rule
        .as_ref()
        .map(|rule| rule.strategy.as_str())
        .unwrap_or("family:legacy-tensorflow");

    for (import_name, package_name, version) in legacy_tensorflow_bundle(&bundle_python) {
        // Only pin bundle packages that already appear in the resolved list
        // (i.e. the snippet actually imports them). Don't add unrelated packages
        // like gym or keras when the snippet only uses tensorflow.
        // Exception: protobuf is a critical transitive dep of TF that must be
        // pinned to avoid descriptor breakage — always include it.
        let is_transitive_essential = package_name == "protobuf";
        let already_resolved = resolved.iter().any(|dep| {
            dep.import_name.eq_ignore_ascii_case(&import_name)
                || normalize(&dep.package_name) == normalize(&package_name)
        });
        if !already_resolved && !is_transitive_essential {
            continue;
        }
        if pin_dependency(
            resolved,
            &import_name,
            &package_name,
            version.as_deref(),
            strategy,
            0.96,
        ) {
            if version.is_none() {
                changes.push(format!("{package_name} (unpinned)"));
            } else {
                changes.push(format!(
                    "{package_name}=={}",
                    version.as_deref().unwrap_or_default()
                ));
            }
        }
    }

    if changes.is_empty() {
        return None;
    }

    Some(
        rule.and_then(|rule| rule.apply_note_template)
            .map(|template| render_template(&template, &bundle_python, &changes))
            .unwrap_or_else(|| {
                format!(
                    "Family knowledge targeted the legacy TensorFlow/Keras stack at Python {bundle_python} and pinned a coherent bundle: {}.",
                    changes.join(", ")
                )
            }),
    )
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

    let rule = curated_rule("keras-backend");
    let strategy = rule
        .as_ref()
        .map(|rule| rule.strategy.as_str())
        .unwrap_or("family:keras-backend");
    let member = bundle_members_for_rule("keras-backend", python_version)
        .and_then(|members| members.into_iter().next())
        .unwrap_or(CuratedBundleMember {
            import_name: "tensorflow".to_string(),
            package_name: "tensorflow".to_string(),
            version: None,
        });
    resolved.push(ResolvedDependency {
        import_name: member.import_name,
        package_name: member.package_name,
        version: member.version,
        strategy: strategy.to_string(),
        confidence: 0.92,
    });

    Some(
        rule.and_then(|rule| rule.apply_note_template)
            .unwrap_or_else(|| {
                "Family knowledge added tensorflow as the default backend for standalone keras."
                    .to_string()
            }),
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

    let rule = curated_rule("legacy-pillow");
    let pil_markers = rule
        .as_ref()
        .map(|rule| {
            rule.anchor_ids
                .iter()
                .map(|item| normalize(item))
                .collect::<Vec<_>>()
        })
        .filter(|markers| !markers.is_empty())
        .unwrap_or_else(|| {
            vec![
                "pil".to_string(),
                "image".to_string(),
                "imagedraw".to_string(),
                "imagefont".to_string(),
                "imagefilter".to_string(),
                "imagechops".to_string(),
                "imageops".to_string(),
                "imageenhance".to_string(),
                "imagegrab".to_string(),
            ]
        });
    let member = bundle_members_for_rule("legacy-pillow", selected_python)
        .and_then(|members| members.into_iter().next())
        .unwrap_or(CuratedBundleMember {
            import_name: "PIL".to_string(),
            package_name: "Pillow".to_string(),
            version: Some("6.2.2".to_string()),
        });
    let strategy = rule
        .as_ref()
        .map(|rule| rule.strategy.as_str())
        .unwrap_or("family:legacy-pillow");
    let references_pillow = parse_result
        .imports
        .iter()
        .map(|item| normalize(item))
        .any(|item| pil_markers.iter().any(|marker| marker == &item))
        || resolved.iter().any(|dependency| {
            normalize(&dependency.package_name) == normalize(&member.package_name)
        });
    if !references_pillow {
        return None;
    }

    let mut changed = false;
    for dependency in resolved.iter_mut() {
        if normalize(&dependency.package_name) == normalize(&member.package_name) {
            let target_version = member.version.clone();
            let row_changed = dependency.package_name != member.package_name
                || dependency.version != target_version
                || dependency.strategy != strategy;
            dependency.package_name = member.package_name.clone();
            dependency.version = target_version;
            dependency.strategy = strategy.to_string();
            dependency.confidence = 0.95;
            changed |= row_changed;
        }
    }

    if !resolved
        .iter()
        .any(|dependency| normalize(&dependency.package_name) == normalize(&member.package_name))
    {
        pin_dependency(
            resolved,
            &member.import_name,
            &member.package_name,
            member.version.as_deref(),
            strategy,
            0.95,
        );
        changed = true;
    }

    if changed {
        Some(
            rule.and_then(|rule| rule.apply_note_template)
                .unwrap_or_else(|| {
                    "Family knowledge pinned Pillow to 6.2.2 for Python 2.7 PIL-era compatibility."
                        .to_string()
                }),
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
    let candidates =
        docker::parallel::candidate_versions(selected_python, python_range, None, None);
    let preferred =
        preferred_rule_python_order("legacy-pymc3", execute_snippet, &["3.10", "3.9", "2.7"]);
    preferred
        .into_iter()
        .find(|version| candidates.iter().any(|candidate| candidate == version))
        .unwrap_or_else(|| selected_python.to_string())
}

pub(super) fn preferred_legacy_tensorflow_python(
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> String {
    // Check which Python versions are actually available (installed).
    let base_candidates =
        docker::parallel::candidate_versions(selected_python, python_range, None, None);

    let preferred = preferred_rule_python_order(
        "legacy-tensorflow",
        execute_snippet,
        &["3.7", "2.7", "3.8", "3.9", "3.10"],
    );
    preferred
        .into_iter()
        .find(|version| base_candidates.iter().any(|candidate| candidate == version))
        .unwrap_or_else(|| selected_python.to_string())
}

pub(super) fn legacy_tensorflow_candidate_versions(
    selected_python: &str,
    python_range: usize,
) -> Vec<String> {
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

pub(super) fn legacy_pymc3_bundle(bundle_python: &str) -> Vec<(String, String, Option<String>)> {
    if let Some(members) = bundle_members_for_rule("legacy-pymc3", bundle_python) {
        return members
            .into_iter()
            .map(|member| (member.import_name, member.package_name, member.version))
            .collect();
    }

    if bundle_python.starts_with("2.") {
        vec![
            (
                "numpy".to_string(),
                "numpy".to_string(),
                Some("1.16.6".to_string()),
            ),
            (
                "pandas".to_string(),
                "pandas".to_string(),
                Some("0.24.2".to_string()),
            ),
            (
                "pymc3".to_string(),
                "pymc3".to_string(),
                Some("3.5".to_string()),
            ),
            (
                "scipy".to_string(),
                "scipy".to_string(),
                Some("1.2.3".to_string()),
            ),
            (
                "setuptools".to_string(),
                "setuptools".to_string(),
                Some("44.1.1".to_string()),
            ),
            (
                "theano".to_string(),
                "Theano".to_string(),
                Some("1.0.5".to_string()),
            ),
        ]
    } else {
        vec![
            (
                "arviz".to_string(),
                "arviz".to_string(),
                Some("0.12.1".to_string()),
            ),
            (
                "numpy".to_string(),
                "numpy".to_string(),
                Some("1.21.6".to_string()),
            ),
            (
                "pandas".to_string(),
                "pandas".to_string(),
                Some("1.5.3".to_string()),
            ),
            (
                "pymc3".to_string(),
                "pymc3".to_string(),
                Some("3.11.5".to_string()),
            ),
            (
                "scipy".to_string(),
                "scipy".to_string(),
                Some("1.7.3".to_string()),
            ),
            (
                "setuptools".to_string(),
                "setuptools".to_string(),
                Some("69.5.1".to_string()),
            ),
            (
                "theano".to_string(),
                "Theano-PyMC".to_string(),
                Some("1.1.2".to_string()),
            ),
            (
                "xarray".to_string(),
                "xarray".to_string(),
                Some("2022.9.0".to_string()),
            ),
            (
                "xarray_einstats".to_string(),
                "xarray-einstats".to_string(),
                Some("0.6.0".to_string()),
            ),
        ]
    }
}

pub(super) fn legacy_tensorflow_bundle(
    bundle_python: &str,
) -> Vec<(String, String, Option<String>)> {
    if let Some(members) = bundle_members_for_rule("legacy-tensorflow", bundle_python) {
        return members
            .into_iter()
            .map(|member| (member.import_name, member.package_name, member.version))
            .collect();
    }

    if bundle_python.starts_with("2.") {
        // gym 0.17+ dropped Python 2 support; protobuf must be <4 for TF 1.x
        vec![
            (
                "gym".to_string(),
                "gym".to_string(),
                Some("0.16.0".to_string()),
            ),
            (
                "keras".to_string(),
                "keras".to_string(),
                Some("2.3.1".to_string()),
            ),
            (
                "numpy".to_string(),
                "numpy".to_string(),
                Some("1.16.6".to_string()),
            ),
            (
                "protobuf".to_string(),
                "protobuf".to_string(),
                Some("3.20.3".to_string()),
            ),
            (
                "tensorflow".to_string(),
                "tensorflow".to_string(),
                Some("1.15.0".to_string()),
            ),
        ]
    } else if bundle_python.starts_with("3.7") {
        // protobuf >=4 breaks TF 1.x descriptor generation
        vec![
            (
                "gym".to_string(),
                "gym".to_string(),
                Some("0.17.3".to_string()),
            ),
            (
                "keras".to_string(),
                "keras".to_string(),
                Some("2.3.1".to_string()),
            ),
            (
                "numpy".to_string(),
                "numpy".to_string(),
                Some("1.16.6".to_string()),
            ),
            (
                "protobuf".to_string(),
                "protobuf".to_string(),
                Some("3.20.3".to_string()),
            ),
            (
                "tensorflow".to_string(),
                "tensorflow".to_string(),
                Some("1.15.0".to_string()),
            ),
        ]
    } else {
        // Python 3.8+: TF 1.15.0 has no wheels.  Leave versions empty so
        // pip can resolve freely — the latest compatible TF 2.x, keras, etc.
        // will be installed.  Empty-string version is treated as None by the
        // caller (apply_legacy_tensorflow_bundle).
        vec![
            ("gym".to_string(), "gym".to_string(), None),
            ("keras".to_string(), "keras".to_string(), None),
            ("numpy".to_string(), "numpy".to_string(), None),
            ("tensorflow".to_string(), "tensorflow".to_string(), None),
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
