use super::retry_loop::{
    dependency_index_by_import, dependency_index_by_package, upsert_dependency,
};
use super::*;
use std::collections::BTreeSet;

pub(super) fn normalize_package_key(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['_', '.'], "-")
}

pub(super) fn unsolvable_status_for_category(category: &str) -> &'static str {
    match category.trim().to_ascii_lowercase().as_str() {
        "host-runtime" | "platform-specific" | "system-dependency" => "skipped-host-runtime",
        _ => "skipped-unsolvable",
    }
}

pub(super) fn set_repair_strategy(validation: &mut ValidationSummary, note: &str) {
    validation.repair_strategy_applied = Some(note.to_string());
}

pub(super) fn remember_failed_import_mapping(
    failed_pairs: &mut BTreeSet<(String, String)>,
    resolved: &[ResolvedDependency],
    module_name: &str,
) {
    let module_norm = normalize_package_key(module_name);
    if let Some(index) = dependency_index_by_import(resolved, &module_norm) {
        let dep = &resolved[index];
        failed_pairs.insert((module_norm, normalize_package_key(&dep.package_name)));
        return;
    }
    let top_level = module_name.split('.').next().unwrap_or(module_name);
    let top_norm = normalize_package_key(top_level);
    if let Some(index) = dependency_index_by_import(resolved, &top_norm) {
        let dep = &resolved[index];
        failed_pairs.insert((top_norm, normalize_package_key(&dep.package_name)));
    }
}

pub(super) fn mapping_is_banned(
    failed_pairs: &BTreeSet<(String, String)>,
    import_name: &str,
    package_name: &str,
) -> bool {
    let import_norm = normalize_package_key(import_name);
    let package_norm = normalize_package_key(package_name);
    failed_pairs.contains(&(import_norm.clone(), package_norm.clone()))
        || failed_pairs.contains(&(
            normalize_package_key(import_name.split('.').next().unwrap_or(import_name)),
            package_norm,
        ))
}

pub(super) fn failure_signature(classified: &crate::ClassifierResult, log: &str) -> String {
    let missing = extract_missing_module(log).unwrap_or_default();
    let package = extract_package_and_version(log)
        .map(|(name, _)| name)
        .unwrap_or_default();
    format!(
        "{}|{}|{}|{}",
        classified.error_type,
        classified.conflict_class,
        normalize_package_key(&missing),
        normalize_package_key(&package)
    )
}

pub(super) fn apply_llm_recovery_hint(
    hint: &tier3_llm::RecoveryHint,
    resolved: &mut Vec<ResolvedDependency>,
    store: &mut CacheStore,
    failed_pairs: &BTreeSet<(String, String)>,
    llm_removed_imports: &mut BTreeSet<String>,
    label: &str,
    reason: &str,
) -> (bool, Vec<String>) {
    let mut applied = false;
    let mut notes = Vec::new();

    let norm_wrong = normalize_package_key(&hint.wrong_pkg);
    if !norm_wrong.is_empty() {
        if let Some(dep_index) = dependency_index_by_package(resolved, &norm_wrong) {
            let dep = &resolved[dep_index];
            let target_pkg = hint.correct_pkg.trim();
            let target_version = hint.version.clone();
            if target_pkg.is_empty() {
                notes.push(format!(
                    "{label}: ignored empty replacement for `{}`.",
                    dep.package_name
                ));
            } else if dep.package_name == target_pkg && dep.version == target_version {
                notes.push(format!(
                    "{label}: discarded no-op repair for `{}`.",
                    dep.package_name
                ));
            } else if !family_knowledge::namespace_mapping_allowed(&dep.import_name, target_pkg) {
                notes.push(format!(
                    "{label}: rejected namespace-incompatible repair for import `{}` -> `{target_pkg}`.",
                    dep.import_name
                ));
            } else if mapping_is_banned(failed_pairs, &dep.import_name, target_pkg) {
                notes.push(format!(
                    "{label}: skipped previously failed mapping `{}` -> `{target_pkg}`.",
                    dep.import_name
                ));
            } else if dep.package_name != target_pkg
                && dependency_index_by_package(resolved, target_pkg)
                    .is_some_and(|index| index != dep_index)
            {
                notes.push(format!(
                    "{label}: discarded duplicate replacement `{}` -> `{target_pkg}`.",
                    dep.package_name
                ));
            } else {
                let old_pkg = dep.package_name.clone();
                let import_name = dep.import_name.clone();
                let dep = &mut resolved[dep_index];
                dep.package_name = target_pkg.to_string();
                dep.version = target_version.clone();
                dep.strategy = label.to_ascii_lowercase().replace(' ', "-");
                dep.confidence = 0.65;
                if old_pkg != target_pkg || target_version.is_some() {
                    let _ = store.save_import_mapping(
                        &import_name,
                        target_pkg,
                        target_version.as_deref(),
                        &dep.strategy,
                    );
                }
                notes.push(if old_pkg == target_pkg {
                    format!(
                        "{label}: pinned `{old_pkg}` to version {} after {reason}.",
                        target_version.as_deref().unwrap_or("(latest)")
                    )
                } else {
                    format!("{label}: replaced `{old_pkg}` with `{target_pkg}` after {reason}.")
                });
                applied = true;
            }
        }
    }

    if let Some((add_name, add_ver)) = &hint.add_package {
        if mapping_is_banned(failed_pairs, add_name, add_name) {
            notes.push(format!(
                "{label}: skipped previously failed package add `{add_name}`."
            ));
        } else if upsert_dependency(
            resolved,
            add_name,
            add_name,
            add_ver.clone(),
            &label.to_ascii_lowercase().replace(' ', "-"),
        ) {
            notes.push(format!(
                "{label}: added transitive dep `{add_name}{}` after {reason}.",
                add_ver
                    .as_deref()
                    .map(|v| format!("=={v}"))
                    .unwrap_or_default()
            ));
            applied = true;
        } else {
            notes.push(format!(
                "{label}: discarded duplicate add for `{add_name}`."
            ));
        }
    }

    if let Some(remove_name) = hint.remove_pkg.as_deref() {
        let norm_remove = normalize_package_key(remove_name);
        if let Some(pos) = dependency_index_by_package(resolved, &norm_remove) {
            let removed = resolved.remove(pos);
            llm_removed_imports.insert(normalize_package_key(&removed.import_name));
            notes.push(format!(
                "{label}: removed `{}` (import `{}`) because it should not be installed from PyPI.",
                removed.package_name, removed.import_name
            ));
            applied = true;
        }
    }

    (applied, notes)
}

/// Check if a missing module matches a Phase 9 targeted stop-reason rule.
/// Returns the stop reason string (e.g. "removed-runtime: ...") if a match
/// is found, or `None` if no stop-reason rule applies.  The caller should
/// use this to skip further LLM recovery attempts for the module.
pub(super) fn targeted_stop_reason_for_module(module_name: &str) -> Option<String> {
    let policy = super::targeted_recovery::get_targeted_recovery_policy()?;
    let rule = policy.stop_reason_for_module(module_name)?;
    Some(rule.reason.clone())
}

pub(super) fn update_failure_metadata(
    validation: &mut ValidationSummary,
    config: &ResolveConfig,
    resolved: &[ResolvedDependency],
    repeat_failure_signature: Option<String>,
) {
    if validation.failure_bucket.is_empty() {
        validation.failure_bucket = validation.status.clone();
    }
    if validation.root_cause.is_none() {
        validation.root_cause = validation.reason.clone();
    }
    let last_log = validation
        .attempts
        .last()
        .map(|attempt| attempt.log_excerpt.as_str())
        .unwrap_or("");
    if validation.missing_module.is_none() {
        validation.missing_module = extract_missing_module(last_log);
    }
    if validation.failing_package.is_none() {
        validation.failing_package =
            extract_package_and_version(last_log).map(|(package, _)| package);
    }
    if validation.repair_strategy_applied.is_none() {
        validation.repair_strategy_applied = validation
            .attempts
            .iter()
            .rev()
            .find_map(|attempt| attempt.fix_applied.clone());
    }
    validation.skip_candidate = validation.skip_candidate
        || validation.status.starts_with("skipped")
        || validation
            .reason
            .as_deref()
            .map(|reason| {
                reason.contains("host-application")
                    || reason.contains("host runtime")
                    || reason.contains("cannot validate this snippet without")
            })
            .unwrap_or(false);
    let first_backend = validation
        .attempts
        .first()
        .map(|attempt| attempt.validation_backend.as_str())
        .unwrap_or(config.validation_backend());
    let final_backend = if validation.validation_backend.is_empty() {
        config.validation_backend()
    } else {
        validation.validation_backend.as_str()
    };
    if first_backend != final_backend {
        validation.escalated_backend = Some(final_backend.to_string());
    }
    if validation.repeat_failure_signature.is_none() {
        validation.repeat_failure_signature = repeat_failure_signature;
    }
    if validation.failing_package.is_none() {
        validation.failing_package = resolved.iter().last().map(|dep| dep.package_name.clone());
    }
}

/// Returns the last known Python 2.7-compatible version for popular packages.
/// Used to cap version sampling during recovery for Python 2.7 snippets.
pub(super) fn last_python2_version(package_name: &str) -> Option<&'static str> {
    let normalized = package_name.to_ascii_lowercase().replace(['_', '.'], "-");
    match normalized.as_str() {
        "numpy" => Some("1.16.6"),
        "scipy" => Some("1.2.3"),
        "pandas" => Some("0.25.3"),
        "scikit-learn" | "sklearn" => Some("0.20.4"),
        "matplotlib" => Some("2.2.5"),
        "pillow" | "pil" => Some("6.2.2"),
        "django" => Some("1.11.29"),
        "flask" => Some("1.1.4"),
        "requests" => Some("2.27.1"),
        "setuptools" => Some("44.1.1"),
        "pip" => Some("20.3.4"),
        "wheel" => Some("0.37.1"),
        "six" => Some("1.16.0"),
        "cryptography" => Some("3.3.2"),
        "ipython" => Some("5.10.0"),
        "pytest" => Some("4.6.11"),
        "coverage" => Some("5.5"),
        "virtualenv" => Some("20.15.1"),
        "typing-extensions" => Some("3.10.0.2"),
        "importlib-metadata" => Some("2.1.3"),
        "more-itertools" => Some("5.0.0"),
        "attrs" => Some("21.4.0"),
        "jinja2" => Some("2.11.3"),
        "markupsafe" => Some("1.1.1"),
        "werkzeug" => Some("1.0.1"),
        "itsdangerous" => Some("1.1.0"),
        "click" => Some("7.1.2"),
        "twisted" => Some("20.3.0"),
        "pyyaml" | "yaml" => Some("5.4.1"),
        "lxml" => Some("4.6.5"),
        "beautifulsoup4" | "bs4" => Some("4.9.3"),
        "boto3" => Some("1.17.112"),
        "botocore" => Some("1.20.112"),
        "paramiko" => Some("2.11.0"),
        "pyopenssl" => Some("21.0.0"),
        "psycopg2" | "psycopg2-binary" => Some("2.8.6"),
        "sqlalchemy" => Some("1.4.46"),
        "celery" => Some("4.4.7"),
        "kombu" => Some("4.6.11"),
        "redis" => Some("3.5.3"),
        "pymongo" => Some("3.12.3"),
        "h5py" => Some("2.10.0"),
        "cython" => Some("0.29.36"),
        "numba" => Some("0.48.0"),
        "theano" => Some("1.0.5"),
        "keras" => Some("2.3.1"),
        "tensorflow" => Some("1.15.0"),
        "torch" | "pytorch" => Some("1.4.0"),
        "scikit-image" | "skimage" => Some("0.14.2"),
        "opencv-python" | "opencv-python-headless" => Some("4.2.0.32"),
        "biopython" | "bio" => Some("1.76"),
        "word2vec" => Some("0.11.1"),
        "scrapy" => Some("1.8.3"),
        "mecab-python" => Some("0.996"),
        "gensim" => Some("3.8.3"),
        "apscheduler" => Some("2.1.2"),
        "python-daemon" => Some("2.3.2"),
        "gevent" => Some("21.1.2"),
        "greenlet" => Some("1.1.3"),
        "python-memcached" => Some("1.59"),
        _ => None,
    }
}

pub(super) fn extract_package_and_version(log: &str) -> Option<(String, Option<String>)> {
    for line in log.lines() {
        if let Some(index) = line.find("requirement ") {
            let candidate = line[index + "requirement ".len()..]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches(',')
                .trim();
            if let Some((package, version)) = candidate.split_once("==") {
                return Some((package.trim().to_string(), Some(version.trim().to_string())));
            }
        }
        if let Some(index) = line.find("pip install ") {
            let candidate = line[index + "pip install ".len()..]
                .split_whitespace()
                .last()
                .unwrap_or("")
                .trim_matches('"')
                .trim_matches('\'')
                .trim_matches(',')
                .trim();
            if let Some((package, version)) = candidate.split_once("==") {
                return Some((package.trim().to_string(), Some(version.trim().to_string())));
            }
        }
    }
    // Second pass: "Failed building wheel for X" or "Could not build wheels for X".
    // This covers BuildFailure errors for transitive dependencies.
    for line in log.lines() {
        let lower = line.to_ascii_lowercase();
        for marker in [
            "failed building wheel for ",
            "could not build wheels for ",
            "failed to build ",
        ] {
            if let Some(index) = lower.find(marker) {
                let after = &line[index + marker.len()..];
                let candidate = after
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_matches(',')
                    .trim_matches('.')
                    .trim();
                if !candidate.is_empty() {
                    if let Some((pkg, ver)) = candidate.split_once("==") {
                        return Some((pkg.trim().to_string(), Some(ver.trim().to_string())));
                    }
                    return Some((candidate.to_string(), None));
                }
            }
        }
    }
    // Third pass: "No matching distribution found for X" (no version pin).
    // This covers VersionNotFound errors for packages that don't exist on PyPI at all.
    for line in log.lines() {
        if let Some(index) = line.find("No matching distribution found for ") {
            let candidate = line[index + "No matching distribution found for ".len()..]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim();
            if !candidate.is_empty() {
                if let Some((pkg, ver)) = candidate.split_once("==") {
                    return Some((pkg.trim().to_string(), Some(ver.trim().to_string())));
                }
                return Some((candidate.to_string(), None));
            }
        }
    }
    None
}

/// Normalize a PEP 508-style requirement specifier into a (package_key, constraint)
/// pair. This handles range specifiers like `PyJWT>=2.0.0`, `python-dateutil<2.0,>=2.1`,
/// and plain `==` pins. The package key is lowercased and dash-normalized.
///
/// Returns `None` if the input does not look like a requirement spec (no comparator
/// operator found).
pub(super) fn normalize_requirement_spec(spec: &str) -> Option<(String, String)> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Find the first comparator operator: ==, !=, >=, <=, ~=, >, <
    let operators = ["==", "!=", ">=", "<=", "~=", ">", "<"];
    let mut split_pos: Option<usize> = None;
    for op in &operators {
        if let Some(pos) = trimmed.find(op) {
            split_pos = Some(match split_pos {
                Some(existing) if existing <= pos => existing,
                _ => pos,
            });
        }
    }
    let pos = split_pos?;
    if pos == 0 {
        return None;
    }
    let package_raw = trimmed[..pos].trim_end();
    let constraint = trimmed[pos..].trim().to_string();
    if package_raw.is_empty() || constraint.is_empty() {
        return None;
    }
    Some((normalize_package_key(package_raw), constraint))
}

/// Extract the package name from "error in {package} setup command" log lines.
/// e.g. "error in plac setup command: use_2to3 is invalid." â†’ Some("plac")
pub(super) fn extract_setup_error_package(log: &str) -> Option<String> {
    for line in log.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(idx) = lower.find("error in ") {
            let after = &line[idx + "error in ".len()..];
            if let Some(pkg_end) = after.find(" setup command") {
                let pkg = after[..pkg_end].trim();
                if !pkg.is_empty()
                    && pkg
                        .chars()
                        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
                {
                    return Some(pkg.to_string());
                }
            }
        }
    }
    None
}

/// Extract the most informative error line(s) from a build/runtime log.
/// Returns a compact string (â‰¤200 chars) suitable for embedding in the
/// resolution-report "notes" section.
pub(super) fn extract_key_error_lines(log: &str) -> String {
    let markers = [
        "ModuleNotFoundError:",
        "ImportError:",
        "AttributeError:",
        "TypeError:",
        "SyntaxError:",
        "RuntimeError:",
        "OSError:",
        "FileNotFoundError:",
        "Double requirement given:",
        "ERROR: Cannot install",
        "ERROR: Could not find",
        "No matching distribution found",
        "error: subprocess-exited-with-error",
        "failed building wheel",
        "pkg-config",
        "fatal error:",
    ];
    for line in log.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        for marker in &markers {
            if trimmed.contains(marker) {
                let excerpt = if trimmed.len() > 200 {
                    format!("{}...", &trimmed[..197])
                } else {
                    trimmed.to_string()
                };
                return excerpt;
            }
        }
    }
    // Fallback: last non-empty line
    log.lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .map(|l| {
            let t = l.trim();
            if t.len() > 200 {
                format!("{}...", &t[..197])
            } else {
                t.to_string()
            }
        })
        .unwrap_or_default()
}

pub(super) fn extract_missing_module(log: &str) -> Option<String> {
    for marker in [
        "No module named ",
        "ModuleNotFoundError: No module named ",
        "ImportError: No module named ",
    ] {
        if let Some(index) = log.find(marker) {
            let fragment = &log[index + marker.len()..];
            let module = fragment
                .trim_matches('"')
                .trim_matches('\'')
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(':')
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
            if !module.is_empty() {
                return Some(module);
            }
        }
    }
    None
}

/// Check if a module import is guarded by a try/except block in the snippet source.
/// Guarded imports are optional â€” if they fail at runtime, the program has a fallback.
/// This covers patterns like:
///   try:
///       import foo
///   except ImportError:
///       ...
pub(super) fn is_guarded_import(snippet_source: &str, module_name: &str) -> bool {
    let lines: Vec<&str> = snippet_source.lines().collect();
    // Look for the import line and check if it's preceded by a try: block
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        // Check if this line imports the module
        let imports_module = if let Some(rest) = trimmed.strip_prefix("import ") {
            rest.split(',').any(|part| {
                let name = part.split(" as ").next().unwrap_or("").trim();
                name == module_name || name.starts_with(&format!("{module_name}."))
            })
        } else if let Some(rest) = trimmed.strip_prefix("from ") {
            rest.split_once(" import ")
                .map(|(mod_path, _)| {
                    let mp = mod_path.trim();
                    mp == module_name || mp.starts_with(&format!("{module_name}."))
                })
                .unwrap_or(false)
        } else {
            false
        };

        if !imports_module {
            continue;
        }

        // Walk backwards from import line to find the enclosing block
        let import_indent = line.len() - line.trim_start().len();
        for j in (0..i).rev() {
            let prev = lines[j];
            let prev_trimmed = prev.trim();
            if prev_trimmed.is_empty() || prev_trimmed.starts_with('#') {
                continue;
            }
            let prev_indent = prev.len() - prev.trim_start().len();
            if prev_indent < import_indent {
                // Found an enclosing block â€” check if it's try:
                if prev_trimmed == "try:" || prev_trimmed.starts_with("try:") {
                    return true;
                }
                break;
            }
        }
    }
    false
}

/// Extract the top-level module name from a SyntaxError traceback when the
/// error is inside an installed package (`site-packages/`), not the snippet.
///
/// Returns `Some("memcache")` for a traceback like:
///   File ".../site-packages/memcache.py", line 374
///       def quit_all(self) -> None:
///   SyntaxError: invalid syntax
pub(super) fn extract_syntax_error_package(log: &str) -> Option<String> {
    let lower = log.to_lowercase();
    if !lower.contains("syntaxerror") {
        return None;
    }
    // Find the last "site-packages/" file reference before the SyntaxError.
    let mut candidate: Option<String> = None;
    for line in log.lines() {
        if line.contains("site-packages/") {
            if let Some(idx) = line.find("site-packages/") {
                let rest = &line[idx + "site-packages/".len()..];
                // Truncate at closing quote (traceback lines are like:
                //   File ".../site-packages/memcache.py", line 374)
                let path = rest.split('"').next().unwrap_or(rest);
                // Take the first path component: "memcache.py" or "foo/bar.py"
                let first = path
                    .split('/')
                    .next()
                    .unwrap_or("")
                    .trim_end_matches(".py")
                    .trim();
                if !first.is_empty() {
                    candidate = Some(first.to_string());
                }
            }
        }
    }
    candidate
}

/// Extract a missing dependency name from a build/runtime error log.
///
/// This is broader than `extract_missing_module` â€” it also catches
/// setup.py messages like "Numerical Python (NumPy) is not installed"
/// and "You must install X" that don't use the standard Python
/// `ImportError` / `ModuleNotFoundError` format.
pub(super) fn extract_build_dependency(log: &str) -> Option<String> {
    // First, try the standard module-not-found patterns.
    if let Some(module) = extract_missing_module(log) {
        return Some(module);
    }

    // Names that refer to the interpreter/toolchain, not a pip-installable package.
    const REJECT: &[&str] = &["python", "python2", "python3", "pip", "pip3"];

    let lower = log.to_lowercase();

    // Pattern: "(NAME) is not installed" â€” e.g. "Numerical Python (NumPy) is not installed".
    // Extract the parenthesized name closest to "is not installed".
    if let Some(idx) = lower.find("is not installed") {
        let before = &log[..idx];
        if let Some(open) = before.rfind('(') {
            if let Some(close) = before[open..].find(')') {
                let name = before[open + 1..open + close].trim();
                if !name.is_empty() && name.len() < 40 {
                    let n = name.to_string();
                    if !REJECT.contains(&n.to_lowercase().as_str()) {
                        return Some(n);
                    }
                }
            }
        }
        // Fallback: word immediately before "is not installed".
        let word = before
            .split_whitespace()
            .next_back()
            .unwrap_or("")
            .trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_');
        if !word.is_empty()
            && word.len() < 40
            && word
                .chars()
                .next()
                .map(|c| c.is_alphabetic())
                .unwrap_or(false)
            && !REJECT.contains(&word.to_lowercase().as_str())
        {
            return Some(word.to_string());
        }
    }

    // Pattern: "please install X before" or "install X before".
    for marker in ["please install ", "need to install "] {
        if let Some(idx) = lower.find(marker) {
            let rest = &log[idx + marker.len()..];
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_');
            if !name.is_empty()
                && name.len() < 40
                && !REJECT.contains(&name.to_lowercase().as_str())
            {
                return Some(name.to_string());
            }
        }
    }

    // Pattern: "requires X" or "requires package X" â€” common in setup.py
    for marker in ["requires ", "requires package "] {
        if let Some(idx) = lower.find(marker) {
            let rest = &log[idx + marker.len()..];
            let name = rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_');
            if !name.is_empty()
                && name.len() < 40
                && name.chars().next().is_some_and(|c| c.is_alphabetic())
                && !REJECT.contains(&name.to_lowercase().as_str())
            {
                return Some(name.to_string());
            }
        }
    }

    // Pattern: "missing required dependency X" or "Missing dependency: X"
    for marker in [
        "missing required dependency ",
        "missing dependency: ",
        "missing dependency ",
    ] {
        if let Some(idx) = lower.find(marker) {
            let rest = &log[idx + marker.len()..];
            let name = rest
                .split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_' && c != '.')
                .next()
                .unwrap_or("")
                .trim();
            if !name.is_empty()
                && name.len() < 40
                && !REJECT.contains(&name.to_lowercase().as_str())
            {
                return Some(name.to_string());
            }
        }
    }

    // Pattern: "Could not import X" or "Cannot import X"
    for marker in ["could not import ", "cannot import "] {
        if let Some(idx) = lower.find(marker) {
            let rest = &log[idx + marker.len()..];
            let name = rest
                .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '.')
                .next()
                .unwrap_or("")
                .trim_matches('\'')
                .trim_matches('"')
                .trim();
            if !name.is_empty()
                && name.len() < 40
                && !REJECT.contains(&name.to_lowercase().as_str())
            {
                return Some(name.to_string());
            }
        }
    }

    None
}

pub(super) fn learned_pattern_key(classified: &crate::ClassifierResult, log: &str) -> String {
    if classified.matched_pattern != "no-known-pattern" {
        return classified.matched_pattern.clone();
    }

    log.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.chars().take(120).collect::<String>())
        .unwrap_or_else(|| classified.error_type.clone())
}

pub(super) fn environment_specific_note(
    classified: &crate::ClassifierResult,
    log: &str,
    parse_result: &crate::ParseResult,
) -> Option<String> {
    let lower = log.to_lowercase();

    // System-level dependencies that cannot be installed via pip.
    // These apply regardless of the classified error type.
    if lower.contains("you must install java")
        || lower.contains("unable to locate a java runtime")
        || lower.contains("no java runtime present")
        || lower.contains("unable to find java_home")
        || lower.contains("java_home")
    {
        return Some(
            "Detected system dependency (Java Runtime). APDR cannot validate this snippet without a JDK/JRE installation.".to_string(),
        );
    }
    if lower.contains("cuda driver version is insufficient")
        || lower.contains("cuda_error_no_device")
        || lower.contains("no cuda gpus are available")
        || lower.contains("cudnn library not found")
    {
        return Some(
            "Detected hardware dependency (CUDA/cuDNN). APDR cannot validate this snippet without an NVIDIA GPU with CUDA drivers.".to_string(),
        );
    }

    // Build failures caused by missing system C libraries / headers that pip cannot provide.
    if matches!(
        classified.error_type.as_str(),
        "BuildFailure" | "SystemDependency" | "Unknown"
    ) {
        // GTK / GObject / GStreamer desktop stack
        if lower.contains("pkg-config") && (lower.contains("pygtk") || lower.contains("gtk+-"))
            || lower.contains("no package 'gtk+-")
            || lower.contains("no package 'pygtk")
        {
            return Some("Detected system dependency (GTK development headers). APDR cannot validate this snippet without libgtk2.0-dev / gtk+-2.0.".to_string());
        }
        if lower.contains("no package 'gstreamer")
            || lower.contains("gst-python") && lower.contains("pkg-config")
        {
            return Some("Detected system dependency (GStreamer). APDR cannot validate this snippet without GStreamer development headers.".to_string());
        }
        if lower.contains("granite")
            || lower.contains("libgranite")
            || lower.contains("xcb/xcb.h")
            || lower.contains("libx11-xcb")
        {
            return Some("Detected desktop runtime dependency (Granite/XCB). APDR cannot validate this snippet without the corresponding GTK desktop libraries.".to_string());
        }
        // Qt / PySide
        if lower.contains("could not find qt")
            || lower.contains("qmake") && lower.contains("not found")
            || lower.contains("pyside")
                && (lower.contains("cmake") || lower.contains("could not find"))
        {
            return Some("Detected system dependency (Qt). APDR cannot validate this snippet without Qt development libraries.".to_string());
        }
        // D-Bus
        if lower.contains("dbus/dbus.h")
            || lower.contains("no package 'dbus-")
            || lower.contains("dbus-1.pc")
        {
            return Some("Detected system dependency (D-Bus). APDR cannot validate this snippet without libdbus-1-dev.".to_string());
        }
        // MPI
        if lower.contains("mpi.h") || lower.contains("mpicc") && lower.contains("not found") {
            return Some("Detected system dependency (MPI). APDR cannot validate this snippet without an MPI implementation (OpenMPI/MPICH).".to_string());
        }
        // Mapnik
        if lower.contains("mapnik-config")
            || lower.contains("mapnik/") && lower.contains("no such file")
        {
            return Some("Detected system dependency (Mapnik). APDR cannot validate this snippet without libmapnik-dev.".to_string());
        }
        if lower.contains("geos_c.dll")
            || lower.contains("lib geos_c")
            || lower.contains("gdal-config")
            || lower.contains("gdal api version")
        {
            return Some("Detected geospatial native dependency (GEOS/GDAL). APDR cannot validate this snippet in the local env without the corresponding native libraries.".to_string());
        }
        if lower.contains("libmemcached") || lower.contains("memcached.h") {
            return Some("Detected system dependency (libmemcached). APDR cannot validate this snippet without libmemcached development libraries.".to_string());
        }
        if lower.contains("m2crypto")
            || lower.contains("swig")
            || lower.contains("openssl/crypto.h")
        {
            return Some("Detected native crypto dependency (M2Crypto/OpenSSL). APDR cannot validate this snippet without OpenSSL headers and SWIG.".to_string());
        }
        if lower.contains("r_home")
            || lower.contains("unable to determine r home")
            || lower.contains("rpy2") && lower.contains("r was not found")
        {
            return Some("Detected external runtime dependency (R). APDR cannot validate this snippet without an R installation.".to_string());
        }
        // Linux-only evdev / uinput
        if lower.contains("linux/input.h") || lower.contains("linux/uinput.h") {
            return Some("Detected platform dependency (Linux kernel headers). APDR cannot validate this snippet on macOS.".to_string());
        }
        // BlueZ / Bluetooth
        if lower.contains("bluetooth/bluetooth.h") || lower.contains("no package 'bluez") {
            return Some("Detected system dependency (BlueZ). APDR cannot validate this snippet without libbluetooth-dev.".to_string());
        }
        // liberasurecode (swift/PyECLib)
        if lower.contains("liberasurecode") {
            return Some("Detected system dependency (liberasurecode). APDR cannot validate this snippet without liberasurecode-dev.".to_string());
        }
    }

    // The remaining checks require a missing module name.
    if classified.error_type != "ModuleNotFound" {
        return None;
    }
    let missing = extract_missing_module(log)?.to_lowercase();
    let source_markers = parse_result
        .imports
        .iter()
        .map(|item| item.to_lowercase())
        .collect::<BTreeSet<_>>();

    if missing == "pyqt4"
        || missing == "maya"
        || source_markers.contains("maya")
        || source_markers.contains("pyqt4")
    {
        return Some(
            "Detected host-application dependency (Maya/PyQt4). APDR cannot validate this snippet without the Autodesk Maya desktop runtime.".to_string(),
        );
    }
    if matches!(
        missing.as_str(),
        "arcpy"
            | "bpy"
            | "binaryninja"
            | "rhinoscriptsyntax"
            | "hou"
            | "unreal"
            | "nuke"
            | "clr"
            | "win32com"
            | "c4d"
            | "odbaccess"
            | "pyfbsdk"
            | "microbit"
    ) {
        return Some(format!(
            "Detected host-application dependency ({missing}). APDR cannot validate this snippet without the corresponding application runtime."
        ));
    }
    if matches!(missing.as_str(), "opendirectory" | "systemconfiguration") {
        return Some(
            "Detected macOS framework dependency (OpenDirectory/SystemConfiguration). APDR cannot validate this snippet without the macOS host framework runtime.".to_string(),
        );
    }
    if matches!(missing.as_str(), "clipboard" | "camera") {
        return Some(
            "Detected Pythonista iOS runtime dependency. APDR cannot validate this snippet without the Pythonista iOS app.".to_string(),
        );
    }
    if matches!(missing.as_str(), "xcb" | "granite") {
        return Some(
            "Detected GTK desktop-runtime dependency (XCB/Granite). APDR cannot validate this snippet without the corresponding desktop libraries.".to_string(),
        );
    }
    if missing == "rpi" || missing == "rpi.gpio" || source_markers.contains("rpi") {
        return Some(
            "Detected hardware/runtime dependency (RPi.GPIO). APDR cannot validate this snippet without Raspberry Pi GPIO access.".to_string(),
        );
    }
    // Note: Unix-only stdlib (pwd, grp, fcntl, etc.) is handled earlier in the
    // recovery loop as a pass-through, not here (which would mark as skipped-host-runtime).
    let py2_stdlib = [
        "urllib2",
        "urlparse",
        "_winreg",
        "configparser",
        "cpickle",
        "cstringio",
        "queue",
        "htmlparser",
        "httplib",
        "cookielib",
        "robotparser",
    ];
    if py2_stdlib.contains(&missing.as_str()) {
        return Some(format!(
            "Runtime import failed: `{missing}` is a Python 2 standard library module \
             that does not exist in Python 3. The snippet requires Python 2.7."
        ));
    }
    None
}

pub(super) fn infer_validation_status(validation: &ValidationSummary) -> String {
    let Some(attempt) = validation.attempts.last() else {
        return "failed".to_string();
    };
    let log = attempt.log_excerpt.to_lowercase();
    if let Some(error_type) = attempt.error_type.as_deref() {
        match error_type {
            "DependencyConflict" => return "dependency-conflict".to_string(),
            "PythonVersionMismatch" => return "python-version-incompatible".to_string(),
            "BuildBackendUnavailable" => return "build-backend-unavailable".to_string(),
            "PythonInterpreterUnavailable" => return "python-interpreter-unavailable".to_string(),
            "NetworkUnavailable" => return "network-unavailable".to_string(),
            "DiskFull" => return "disk-full".to_string(),
            "DockerPermissionDenied" => return "docker-permission-denied".to_string(),
            "DockerDaemonUnavailable" => return "docker-daemon-unavailable".to_string(),
            _ => {}
        }
    }
    if log.contains("permission denied while trying to connect to the docker api") {
        return "docker-permission-denied".to_string();
    }
    if log.contains("cannot connect to the docker daemon")
        || log.contains("is the docker daemon running")
    {
        return "docker-daemon-unavailable".to_string();
    }
    if log.contains("no matching distribution found")
        || log.contains("could not find a version that satisfies")
    {
        return "version-not-found".to_string();
    }
    if log.contains("modulenotfounderror") || log.contains("no module named ") {
        return "module-not-found".to_string();
    }
    if log.contains("importerror") {
        return "import-error".to_string();
    }
    if log.contains("attributeerror") {
        return "attribute-error".to_string();
    }
    if log.contains("syntaxerror") {
        return "syntax-error".to_string();
    }
    match attempt.status.as_str() {
        "build-timeout" => "environment-build-timeout".to_string(),
        "runtime-timeout" => "environment-runtime-timeout".to_string(),
        "build-failed" => "environment-build-failed".to_string(),
        "runtime-failed" => "environment-runtime-failed".to_string(),
        other if !other.is_empty() => other.to_string(),
        _ => "failed".to_string(),
    }
}

pub(super) fn infer_validation_reason(
    validation: &ValidationSummary,
    report: &ResolutionReport,
) -> Option<String> {
    let attempt = validation.attempts.last()?;
    let log = attempt.log_excerpt.as_str();
    let lowercase = log.to_lowercase();
    if let Some(error_type) = attempt.error_type.as_deref() {
        match error_type {
            "DependencyConflict" => {
                if let Some(explanation) = extract_dependency_conflict_reason(log) {
                    return Some(explanation);
                }
                return Some(
                    "Pinned package versions conflict with each other for the attempted validation environment."
                        .to_string(),
                );
            }
            "PythonVersionMismatch" => {
                if let Some(explanation) = extract_python_version_mismatch_reason(log) {
                    return Some(explanation);
                }
                return Some(
                    "The attempted package versions are incompatible with the Python version used for validation."
                        .to_string(),
                );
            }
            "BuildBackendUnavailable" => {
                return Some(
                    "Package build backend `setuptools.build_meta` was unavailable in the local validation environment during source build."
                        .to_string(),
                );
            }
            "PythonInterpreterUnavailable" => {
                if !log.trim().is_empty() {
                    return Some(log.trim().to_string());
                }
                return Some(
                    "APDR could not find a matching local Python interpreter for one of the candidate versions."
                        .to_string(),
                );
            }
            "NetworkUnavailable" => {
                return Some(
                    "APDR could not reach the Python package index while preparing the local validation environment."
                        .to_string(),
                );
            }
            "DiskFull" => {
                return Some(
                    "APDR ran out of local disk space while creating or seeding the validation environment."
                        .to_string(),
                );
            }
            _ => {}
        }
    }

    if lowercase.contains("permission denied while trying to connect to the docker api") {
        return Some(
            "Historical Docker backend error: permission denied while opening the Docker API socket. New APDR runs validate with local Python environments instead."
                .to_string(),
        );
    }
    if lowercase.contains("cannot connect to the docker daemon")
        || lowercase.contains("is the docker daemon running")
    {
        return Some(
            "Historical Docker backend error: Docker daemon was unavailable. New APDR runs validate with local Python environments instead."
                .to_string(),
        );
    }
    if let Some(module_name) = extract_missing_module(log) {
        let lowered = module_name.to_lowercase();
        if validation
            .attempts
            .last()
            .map(|attempt| attempt.status.as_str() == "build-failed")
            .unwrap_or(false)
            && lowered == "typing"
        {
            return Some(
                "Build-time dependency import failed because Python 2.7 is missing the `typing` backport."
                    .to_string(),
            );
        }
        if matches!(
            lowered.as_str(),
            "util" | "utils" | "helper" | "helpers" | "common" | "shared" | "input_data"
        ) {
            return Some(format!(
                "Snippet depends on local helper module `{module_name}`, which was not bundled as an installable package in this case."
            ));
        }
        if lowered == "c4d" {
            return Some(
                "Detected host-application dependency (`c4d`). APDR cannot validate this snippet without the Cinema 4D runtime.".to_string(),
            );
        }
        if lowered == "rpi" || lowered == "rpi.gpio" {
            return Some(
                "Detected hardware/runtime dependency (`RPi.GPIO`). APDR cannot validate this snippet without Raspberry Pi GPIO access.".to_string(),
            );
        }
        return Some(format!(
            "Runtime import failed: missing module `{module_name}`."
        ));
    }
    if lowercase.contains("cannot import name ") {
        if let Some(fragment) = log
            .lines()
            .find(|line| line.to_lowercase().contains("cannot import name "))
        {
            return Some(format!("Runtime import failed: {}.", fragment.trim()));
        }
    }
    if let Some((package_name, Some(version))) = extract_package_and_version(log) {
        if lowercase.contains("no matching distribution found")
            || lowercase.contains("could not find a version that satisfies")
        {
            return Some(format!(
                "Package `{package_name}=={version}` is unavailable for the selected Python version."
            ));
        }
    }
    if lowercase.contains("could not build wheels") {
        return Some(
            "Package build failed while preparing the local validation environment. Missing system headers or compiler toolchain are likely required.".to_string(),
        );
    }
    if lowercase.contains("libxml2 and libxslt development packages are installed") {
        return Some(
            "Package build failed because libxml2/libxslt development headers are missing in the local validation environment."
                .to_string(),
        );
    }
    if lowercase.contains("python.h: no such file or directory") {
        return Some(
            "Package build failed because Python development headers are missing in the local validation environment."
                .to_string(),
        );
    }
    if attempt.status == "build-timeout" {
        return Some(
            "Local package-environment build timed out during APDR validation.".to_string(),
        );
    }
    if attempt.status == "runtime-timeout" {
        return Some("Local APDR smoke test timed out during validation.".to_string());
    }
    report.notes.last().cloned().filter(|note| !note.is_empty())
}

pub(super) fn extract_dependency_conflict_reason(log: &str) -> Option<String> {
    let mut capture = false;
    let mut lines = Vec::new();
    for line in log.lines() {
        let trimmed = line.trim();
        if trimmed.contains("The conflict is caused by:") {
            capture = true;
            continue;
        }
        if capture {
            if trimmed.is_empty()
                || trimmed.starts_with("To fix this")
                || trimmed.starts_with("Additionally,")
                || trimmed.starts_with("ERROR:")
            {
                break;
            }
            lines.push(trimmed.to_string());
        }
    }
    if lines.is_empty() {
        None
    } else {
        Some(format!(
            "Dependency solver reported an incompatible version bundle: {}.",
            lines.join(" ")
        ))
    }
}

pub(super) fn extract_python_version_mismatch_reason(log: &str) -> Option<String> {
    for line in log.lines() {
        let trimmed = line.trim();
        if trimmed.contains("Requires-Python") {
            return Some(format!(
                "Pinned package version is incompatible with the attempted Python runtime: {}.",
                trimmed
            ));
        }
    }
    None
}

/// Check the persistent unsolvable-modules cache for any import that was
/// previously identified as unsolvable.  Only returns a hit when confidence
/// is very high (>= 0.95) to avoid false positives that block solvable
/// packages like django.  In practice this means only curated seed entries
/// (host-runtime / platform-specific APIs with confidence 1.00) trigger the
/// early exit.
pub(super) fn check_unsolvable_cache(
    parse_result: &crate::ParseResult,
    store: &CacheStore,
) -> Option<(String, UnsolvableModuleRecord)> {
    use crate::cache::store::normalize;
    for import in &parse_result.imports {
        let key = normalize(import);
        if let Some(record) = store.unsolvable_modules.get(&key) {
            if record.confidence >= 0.95 {
                return Some((key, record.clone()));
            }
        }
    }
    for path in &parse_result.import_paths {
        let top = path.split('.').next().unwrap_or(path);
        let key = normalize(top);
        if let Some(record) = store.unsolvable_modules.get(&key) {
            if record.confidence >= 0.95 {
                return Some((key, record.clone()));
            }
        }
    }
    None
}
