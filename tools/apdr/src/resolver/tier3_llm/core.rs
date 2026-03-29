use super::context::{
    assemble_batch_context, assemble_context_for_import, build_base_request,
    looks_like_local_helper_import,
};
use super::failure_memory::{
    format_failure_context, has_failed, load_failure_memory, persist_trace,
};
use super::process::call_python;
use crate::cache::store::CacheStore;
use crate::context;
use crate::resolver::{kgraph_db, pypi_client, version_sampler};
use crate::{ParseResult, ResolveConfig, ResolvedDependency, SolvabilityAssessment};
use std::collections::HashMap;
use std::time::Instant;
pub struct StageResult {
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub notes: Vec<String>,
    pub prompts_issued: usize,
    pub llm_duration_ms: u128,
}
// ---------------------------------------------------------------------------
// Solvability Assessment
// ---------------------------------------------------------------------------

pub fn assess_solvability(
    snippet_source: &str,
    parse_result: &ParseResult,
    config: &ResolveConfig,
) -> Option<SolvabilityAssessment> {
    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    let mut request = build_base_request(config);
    request["action"] = "solvability".into();
    request["imports"] = serde_json::json!(parse_result.imports);
    request["snippet_source"] = snippet_source.into();
    request["benchmark_context"] = benchmark_context.into();

    persist_trace(config, "solvability-assessment", &request, None);

    let response = call_python(&request)?;
    persist_trace(config, "solvability-assessment", &request, Some(&response));

    if let Some(error) = response.get("error").and_then(|e| e.as_str()) {
        if !error.is_empty() {
            return None;
        }
    }

    let decision = response
        .get("decision")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_lowercase();
    if decision != "solve" && decision != "skip" {
        return None;
    }

    let confidence = response
        .get("confidence")
        .and_then(|v| v.as_f64())
        .map(|v| v.clamp(0.0, 1.0))
        .unwrap_or(if decision == "skip" { 0.2 } else { 0.6 });

    let reason = response
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("LLM solvability assessment.")
        .to_string();

    let unsolvable_modules = response
        .get("unsolvable_modules")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty() && s != "none")
                .collect()
        })
        .unwrap_or_default();

    Some(SolvabilityAssessment {
        decision,
        confidence,
        reason,
        source: "llm-preflight".to_string(),
        unsolvable_modules,
    })
}

// ---------------------------------------------------------------------------
// Main Package Resolution
// ---------------------------------------------------------------------------

pub fn resolve(
    unresolved_imports: &[String],
    parse_result: &ParseResult,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
) -> StageResult {
    let llm_started = Instant::now();
    let mut llm_candidates = Vec::new();
    let mut preserved_unresolved = Vec::new();
    for import_name in unresolved_imports {
        if looks_like_local_helper_import(parse_result, import_name) {
            preserved_unresolved.push(import_name.clone());
        } else {
            llm_candidates.push(import_name.clone());
        }
    }
    if llm_candidates.is_empty() {
        return StageResult {
            resolved: Vec::new(),
            unresolved: preserved_unresolved,
            notes: vec!["Skipped LLM resolution for likely local helper imports.".to_string()],
            prompts_issued: 0,
            llm_duration_ms: llm_started.elapsed().as_millis(),
        };
    }

    // Load failure memory and assemble context (Rust-side)
    let failures = load_failure_memory(&config.cache_path);
    let failure_ctx = format_failure_context(&failures, &llm_candidates);
    let mut context = assemble_batch_context(store, &llm_candidates, &failure_ctx);
    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    // Build attribute_usage as JSON
    let attr_usage: serde_json::Value = serde_json::json!(parse_result
        .attribute_usage
        .iter()
        .map(|(k, v)| { (k.clone(), v.iter().cloned().collect::<Vec<_>>()) })
        .collect::<std::collections::BTreeMap<_, _>>());

    // Build tier2 candidates for each import (#3: KGraph-grounded)
    let tier2_candidates = build_tier2_candidates(store, &llm_candidates, python_version);

    // Inject "Known package: X" for each tier2 candidate so Python PyPI checker
    // can preload them (avoids N individual HEAD requests to pypi.org)
    if let Some(cands_map) = tier2_candidates.as_object() {
        for (_, cands) in cands_map {
            if let Some(arr) = cands.as_array() {
                for c in arr {
                    if let Some(pkg_name) = c.as_str() {
                        context.push(format!("Known package: {}", pkg_name));
                    }
                }
            }
        }
    }

    let mut request = build_base_request(config);
    request["action"] = "resolve".into();
    request["imports"] = serde_json::json!(llm_candidates);
    request["python_version"] = python_version.into();
    request["context"] = serde_json::json!(context);
    request["benchmark_context"] = serde_json::Value::String(benchmark_context.clone());
    request["attribute_usage"] = attr_usage;
    request["tier2_candidates"] = tier2_candidates;

    persist_trace(config, "package-resolution", &request, None);

    let response = match call_python(&request) {
        Some(r) => r,
        None => {
            return StageResult {
                resolved: Vec::new(),
                unresolved: unresolved_imports.to_vec(),
                notes: vec!["LLM package-resolution call returned no output.".to_string()],
                prompts_issued: 1,
                llm_duration_ms: llm_started.elapsed().as_millis(),
            };
        }
    };

    persist_trace(config, "package-resolution", &request, Some(&response));

    let prompts_issued = response
        .get("prompts_issued")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;

    let mut notes: Vec<String> = response
        .get("notes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    if let Some(agent_mode) = response.get("agent_mode").and_then(|v| v.as_str()) {
        if !agent_mode.trim().is_empty() {
            let tool_profile = response
                .get("tool_profile")
                .and_then(|v| v.as_str())
                .unwrap_or("full");
            let retrieval_profile = response
                .get("retrieval_profile")
                .and_then(|v| v.as_str())
                .unwrap_or("none");
            let policy_label = response
                .get("policy_label")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            notes.push(format!(
                "tier3 agent config: agent_mode={}, tool_profile={}, retrieval_profile={}, policy_label={}",
                agent_mode, tool_profile, retrieval_profile, policy_label
            ));
        }
    }
    if let Some(reason) = response.get("abstain_reason").and_then(|v| v.as_str()) {
        if !reason.trim().is_empty() {
            notes.push(format!("tier3 agent abstained: {}", reason.trim()));
        }
    }
    if let Some(reason) = response.get("failure_reason").and_then(|v| v.as_str()) {
        if !reason.trim().is_empty() {
            notes.push(format!("tier3 agent failure: {}", reason.trim()));
        }
    }

    // Parse mappings from Python response
    let mappings: Vec<(String, String)> = response
        .get("mappings")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let imp = m.get("import_name")?.as_str()?;
                    let pkg = m.get("package_name")?.as_str()?;
                    Some((imp.to_string(), pkg.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();

    // #8: Read confidence from Python semantic entropy / voting
    let llm_confidence = response
        .get("confidence")
        .and_then(|v| v.as_f64())
        .map(|v| v.clamp(0.0, 1.0))
        .unwrap_or(0.73);

    // Post-process: version selection + store integration (stays in Rust)
    let mut resolved = Vec::new();
    let mut still_unresolved = preserved_unresolved;

    // #12: Collect all packages needing version selection, then batch them
    struct PendingMapping {
        import_name: String,
        package_name: String,
        versions: Vec<String>,
    }
    let mut pending: Vec<PendingMapping> = Vec::new();

    for (import_name, mapped) in &mappings {
        let versions = pypi_client::compatible_versions(store, mapped, python_version);
        if versions.is_empty() && mapped == import_name {
            notes.push(format!(
                "Skipped LLM mapping {import_name} -> {mapped}: package not found on PyPI (likely a local module)."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }
        if has_failed(&failures, import_name, mapped) {
            notes.push(format!(
                "Skipped LLM mapping {import_name} -> {mapped}: failed in previous runs (Reflexion)."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }
        pending.push(PendingMapping {
            import_name: import_name.clone(),
            package_name: mapped.clone(),
            versions,
        });
    }

    // Batch version selection: one LLM call for all packages
    let packages_needing_versions: Vec<(String, Vec<String>)> = pending
        .iter()
        .filter(|p| !p.versions.is_empty())
        .map(|p| (p.package_name.clone(), p.versions.clone()))
        .collect();

    let batch_versions = if !packages_needing_versions.is_empty() {
        batch_pick_versions(
            config,
            &packages_needing_versions,
            python_version,
            &benchmark_context,
        )
    } else {
        HashMap::new()
    };

    for entry in &pending {
        let version = if entry.versions.is_empty() {
            None
        } else {
            batch_versions
                .get(&entry.package_name)
                .cloned()
                .or_else(|| version_sampler::equally_distanced_sample(&entry.versions, &[]))
        };

        let _ = store.save_import_mapping(
            &entry.import_name,
            &entry.package_name,
            version.as_deref(),
            "llm",
        );
        resolved.push(ResolvedDependency {
            import_name: entry.import_name.clone(),
            package_name: entry.package_name.clone(),
            version,
            strategy: "llm".to_string(),
            confidence: llm_confidence,
        });
        notes.push(format!(
            "LLM resolved {} -> {}.",
            entry.import_name, entry.package_name
        ));
    }

    StageResult {
        resolved,
        unresolved: still_unresolved,
        notes,
        prompts_issued: prompts_issued + mappings.len(),
        llm_duration_ms: llm_started.elapsed().as_millis(),
    }
}

// ---------------------------------------------------------------------------
// Resolve with additional context (retry path)
// ---------------------------------------------------------------------------

pub fn resolve_with_context(
    unresolved_imports: &[String],
    snippet_source: &str,
    parse_result: &ParseResult,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
    additional_context: Option<String>,
) -> StageResult {
    let llm_started = Instant::now();
    let mut llm_candidates = Vec::new();
    let mut preserved_unresolved = Vec::new();
    for import_name in unresolved_imports {
        if looks_like_local_helper_import(parse_result, import_name) {
            preserved_unresolved.push(import_name.clone());
        } else {
            llm_candidates.push(import_name.clone());
        }
    }
    if llm_candidates.is_empty() {
        return StageResult {
            resolved: Vec::new(),
            unresolved: preserved_unresolved,
            notes: vec!["Skipped LLM resolution for likely local helper imports.".to_string()],
            prompts_issued: 0,
            llm_duration_ms: llm_started.elapsed().as_millis(),
        };
    }

    let failures = load_failure_memory(&config.cache_path);
    let failure_ctx = format_failure_context(&failures, &llm_candidates);
    let mut context = assemble_batch_context(store, &llm_candidates, &failure_ctx);

    if let Some(extra) = additional_context {
        context.insert(0, format!("IMPORTANT: {}", extra));
    }
    context.push(format!(
        "Code snippet showing import usage:\n```python\n{}\n```",
        snippet_source
            .lines()
            .take(50)
            .collect::<Vec<_>>()
            .join("\n")
    ));

    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    let attr_usage: serde_json::Value = serde_json::json!(parse_result
        .attribute_usage
        .iter()
        .map(|(k, v)| { (k.clone(), v.iter().cloned().collect::<Vec<_>>()) })
        .collect::<std::collections::BTreeMap<_, _>>());

    let tier2_candidates = build_tier2_candidates(store, &llm_candidates, python_version);

    let mut request = build_base_request(config);
    request["action"] = "resolve".into();
    request["imports"] = serde_json::json!(llm_candidates);
    request["python_version"] = python_version.into();
    request["context"] = serde_json::json!(context);
    request["benchmark_context"] = serde_json::Value::String(benchmark_context.clone());
    request["attribute_usage"] = attr_usage;
    request["snippet_source"] = snippet_source.into();
    request["tier2_candidates"] = tier2_candidates;

    persist_trace(config, "package-resolution-with-context", &request, None);

    let response = match call_python(&request) {
        Some(r) => r,
        None => {
            return StageResult {
                resolved: Vec::new(),
                unresolved: unresolved_imports.to_vec(),
                notes: vec!["LLM package-resolution call returned no output.".to_string()],
                prompts_issued: 1,
                llm_duration_ms: llm_started.elapsed().as_millis(),
            };
        }
    };

    persist_trace(
        config,
        "package-resolution-with-context",
        &request,
        Some(&response),
    );

    let mappings: Vec<(String, String)> = response
        .get("mappings")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let imp = m.get("import_name")?.as_str()?;
                    let pkg = m.get("package_name")?.as_str()?;
                    Some((imp.to_string(), pkg.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();

    let mut resolved = Vec::new();
    let mut still_unresolved = Vec::new();
    let mut notes: Vec<String> = response
        .get("notes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    for import_name in &llm_candidates {
        let mapped = mappings
            .iter()
            .find(|(imp, _)| imp == import_name)
            .map(|(_, pkg)| pkg.clone())
            .unwrap_or_else(|| import_name.clone());

        let versions = pypi_client::compatible_versions(store, &mapped, python_version);
        if versions.is_empty() && mapped == *import_name {
            notes.push(format!(
                "Skipped LLM retry mapping {import_name} -> {mapped}: not found on PyPI."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }
        if has_failed(&failures, import_name, &mapped) {
            notes.push(format!(
                "Skipped LLM retry {import_name} -> {mapped}: failed in previous runs."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }

        let version = if !versions.is_empty() {
            let llm_v = pick_version_via_python(
                config,
                &mapped,
                &versions,
                python_version,
                &benchmark_context,
            );
            llm_v.or_else(|| version_sampler::equally_distanced_sample(&versions, &[]))
        } else {
            None
        };

        let _ = store.save_import_mapping(import_name, &mapped, version.as_deref(), "llm-retry");
        resolved.push(ResolvedDependency {
            import_name: import_name.clone(),
            package_name: mapped.clone(),
            version,
            strategy: "llm-retry".to_string(),
            confidence: 0.73,
        });
        notes.push(format!("LLM retry resolved {import_name} -> {mapped}."));
    }

    still_unresolved.extend(preserved_unresolved);

    StageResult {
        resolved,
        unresolved: still_unresolved,
        notes,
        prompts_issued: 1 + llm_candidates.len(),
        llm_duration_ms: llm_started.elapsed().as_millis(),
    }
}

// ---------------------------------------------------------------------------
// Single Package Hint
// ---------------------------------------------------------------------------

pub fn single_package_hint(
    import_name: &str,
    parse_result: &ParseResult,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
) -> Option<(String, Option<String>)> {
    if looks_like_local_helper_import(parse_result, import_name) {
        return None;
    }

    let context = assemble_context_for_import(store, import_name);
    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    let attr_usage: serde_json::Value = serde_json::json!(parse_result
        .attribute_usage
        .iter()
        .map(|(k, v)| { (k.clone(), v.iter().cloned().collect::<Vec<_>>()) })
        .collect::<std::collections::BTreeMap<_, _>>());

    let tier2_candidates =
        build_tier2_candidates(store, &[import_name.to_string()], python_version);

    let mut request = build_base_request(config);
    request["action"] = "single".into();
    request["imports"] = serde_json::json!([import_name]);
    request["python_version"] = python_version.into();
    request["context"] = serde_json::json!(context);
    request["benchmark_context"] = serde_json::Value::String(benchmark_context.clone());
    request["attribute_usage"] = attr_usage;
    request["tier2_candidates"] = tier2_candidates;

    persist_trace(
        config,
        &format!("single-package-{import_name}"),
        &request,
        None,
    );

    let response = call_python(&request)?;
    persist_trace(
        config,
        &format!("single-package-{import_name}"),
        &request,
        Some(&response),
    );

    let mapped = response
        .get("mappings")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|m| m.get("package_name"))
        .and_then(|v| v.as_str())
        .unwrap_or(import_name)
        .to_string();

    let versions = pypi_client::compatible_versions(store, &mapped, python_version);
    let version = if versions.is_empty() {
        None
    } else {
        let llm_v = pick_version_via_python(
            config,
            &mapped,
            &versions,
            python_version,
            &benchmark_context,
        );
        llm_v.or_else(|| version_sampler::equally_distanced_sample(&versions, &[]))
    };

    if parse_result.imports.iter().any(|item| item == import_name) || !mapped.is_empty() {
        Some((mapped, version))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Recovery Package Hint
// ---------------------------------------------------------------------------

/// Recovery hint from LLM: swap a package and/or add a new transitive dep.
pub struct RecoveryHint {
    pub wrong_pkg: String,
    pub correct_pkg: String,
    pub version: Option<String>,
    /// Optional new dependency to add (package_name, version).
    pub add_package: Option<(String, Option<String>)>,
    /// Optional package to remove entirely (local module / wrong package).
    pub remove_pkg: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub fn recovery_package_hint(
    resolved: &[ResolvedDependency],
    error_log: &str,
    snippet_source: &str,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
    error_type: &str,
    previous_attempts: &[(String, String, String)],
) -> Option<RecoveryHint> {
    // Phase 9: skip LLM recovery when the module already has a deterministic
    // targeted_recovery stop-reason classification (removed-runtime, project-local,
    // internal-extension).  Burning LLM calls cannot fix these cases.
    if matches!(error_type, "ModuleNotFound" | "ImportError") {
        if let Some(policy) = crate::resolver::targeted_recovery::get_targeted_recovery_policy() {
            if let Some(module) = extract_module_from_error_log(error_log) {
                if policy.stop_reason_for_module(&module).is_some() {
                    return None;
                }
            }
        }
    }

    let resolved_desc: Vec<String> = resolved
        .iter()
        .map(|d| {
            if let Some(v) = &d.version {
                format!("{}=={} (import: {})", d.package_name, v, d.import_name)
            } else {
                format!("{} (import: {})", d.package_name, d.import_name)
            }
        })
        .collect();

    let attempts_json: Vec<Vec<String>> = previous_attempts
        .iter()
        .map(|(a, b, c)| vec![a.clone(), b.clone(), c.clone()])
        .collect();

    let mut request = build_base_request(config);
    request["action"] = "recovery".into();
    request["resolved_packages"] = serde_json::json!(resolved_desc);
    request["error_log"] = error_log.into();
    request["snippet_source"] = snippet_source.into();
    request["python_version"] = python_version.into();
    request["error_type"] = error_type.into();
    request["previous_attempts"] = serde_json::json!(attempts_json);

    persist_trace(config, "recovery-fix", &request, None);

    let response = call_python(&request)?;
    persist_trace(config, "recovery-fix", &request, Some(&response));

    let fix_possible = response
        .get("fix_possible")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !fix_possible {
        return None;
    }

    let wrong_pkg = response
        .get("wrong_package")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let correct_pkg = response
        .get("correct_package")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Parse optional version field (LLM-suggested version pin)
    let llm_version = response
        .get("version")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string());

    // Parse optional add_package field (e.g. "protobuf==3.20.3")
    let add_package = response
        .get("add_package")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| {
            if let Some((name, ver)) = s.split_once("==") {
                (name.trim().to_string(), Some(ver.trim().to_string()))
            } else {
                (s.trim().to_string(), None)
            }
        });

    // Parse optional remove_package field (package to remove entirely)
    let remove_pkg = response
        .get("remove_package")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string());

    let has_swap = !wrong_pkg.is_empty() && !correct_pkg.is_empty() && wrong_pkg != correct_pkg;
    let has_version_pin = !wrong_pkg.is_empty()
        && !correct_pkg.is_empty()
        && wrong_pkg == correct_pkg
        && llm_version.is_some();

    if !has_swap && !has_version_pin && add_package.is_none() && remove_pkg.is_none() {
        return None;
    }

    let version = if has_swap {
        // For package swaps, verify the new package exists on PyPI and pick a version
        let versions = pypi_client::compatible_versions(store, &correct_pkg, python_version);
        if versions.is_empty() && add_package.is_none() {
            return None;
        }
        if !versions.is_empty() {
            version_sampler::equally_distanced_sample(&versions, &[])
        } else {
            None
        }
    } else if has_version_pin {
        // LLM suggested pinning the same package to a specific version
        llm_version
    } else {
        None
    };

    Some(RecoveryHint {
        wrong_pkg,
        correct_pkg,
        version,
        add_package,
        remove_pkg,
    })
}

// ---------------------------------------------------------------------------
// Version selection via Python LLM
// ---------------------------------------------------------------------------

fn pick_version_via_python(
    config: &ResolveConfig,
    package_name: &str,
    versions: &[String],
    python_version: &str,
    benchmark_context: &str,
) -> Option<String> {
    let mut request = build_base_request(config);
    request["action"] = "version".into();
    request["package_name"] = package_name.into();
    request["versions"] = serde_json::json!(versions);
    request["python_version"] = python_version.into();
    request["benchmark_context"] = benchmark_context.into();

    let response = call_python(&request)?;

    let version = response
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if version.is_empty() || version.eq_ignore_ascii_case("NONE") {
        return None;
    }
    if versions.iter().any(|v| v == &version) {
        Some(version)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// #12: Batch version selection — one LLM call for N packages
// ---------------------------------------------------------------------------

fn batch_pick_versions(
    config: &ResolveConfig,
    packages: &[(String, Vec<String>)],
    python_version: &str,
    benchmark_context: &str,
) -> HashMap<String, String> {
    if packages.is_empty() {
        return HashMap::new();
    }
    // For single package, fall back to single-package call
    if packages.len() == 1 {
        let (pkg, versions) = &packages[0];
        if let Some(v) =
            pick_version_via_python(config, pkg, versions, python_version, benchmark_context)
        {
            let mut map = HashMap::new();
            map.insert(pkg.clone(), v);
            return map;
        }
        return HashMap::new();
    }

    let mut batch_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (pkg, versions) in packages {
        batch_map.insert(pkg.clone(), serde_json::json!(versions));
    }

    let mut request = build_base_request(config);
    request["action"] = "batch_version".into();
    request["python_version"] = python_version.into();
    request["benchmark_context"] = benchmark_context.into();
    request["batch_packages"] = serde_json::Value::Object(batch_map);

    let response = match call_python(&request) {
        Some(r) => r,
        None => return HashMap::new(),
    };

    let mut result = HashMap::new();
    if let Some(batch_versions) = response.get("batch_versions").and_then(|v| v.as_object()) {
        for (pkg, version_val) in batch_versions {
            if let Some(version) = version_val.as_str() {
                if !version.is_empty() && !version.eq_ignore_ascii_case("NONE") {
                    // Validate version is in the allowed list
                    if let Some((_, versions)) = packages.iter().find(|(p, _)| p == pkg) {
                        if versions.iter().any(|v| v == version) {
                            result.insert(pkg.clone(), version.to_string());
                        }
                    }
                }
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tier2 candidate lookup for prompt injection
// ---------------------------------------------------------------------------

fn build_tier2_candidates(
    store: &CacheStore,
    imports: &[String],
    _python_version: &str,
) -> serde_json::Value {
    let mut candidates: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for import_name in imports {
        let mut cands = Vec::new();
        let normalized = crate::cache::store::normalize(import_name);

        // Source 1: Check known packages from pypi_index for close matches
        for pkg_name in store.pypi_index.keys() {
            let norm_pkg = crate::cache::store::normalize(pkg_name);
            if norm_pkg == normalized {
                continue; // Skip exact match (identity)
            }
            if (norm_pkg.contains(normalized.as_str()) || normalized.contains(norm_pkg.as_str()))
                && !cands.contains(pkg_name)
            {
                cands.push(pkg_name.clone());
            }
            if cands.len() >= 10 {
                break;
            }
        }

        // Source 2 (#3): Query KGraph SQLite DB for candidate packages
        // Finds packages whose name matches patterns like "python-{import}",
        // "{import}-python", "py{import}", or LIKE %{import}%
        if cands.len() < 10 {
            let kgraph_path = store.cache_path.join("smtpip-kgraph.sqlite3");
            let kgraph_cands = kgraph_db::kgraph_candidate_packages(&kgraph_path, import_name, 10);
            for kc in kgraph_cands {
                if cands.len() >= 10 {
                    break;
                }
                if !cands.iter().any(|c| c.eq_ignore_ascii_case(&kc)) {
                    cands.push(kc);
                }
            }
        }

        if !cands.is_empty() {
            candidates.insert(import_name.clone(), cands);
        }
    }
    serde_json::json!(candidates)
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

pub fn fallback_notes(
    unresolved_imports: &[String],
    _parse_result: &ParseResult,
    llm_enabled: bool,
) -> Vec<String> {
    if unresolved_imports.is_empty() {
        return Vec::new();
    }
    let mut notes = Vec::new();
    if llm_enabled {
        notes.push(format!(
            "LLM fallback requested for {} unresolved imports, but no provider was available.",
            unresolved_imports.len()
        ));
    } else {
        notes.push(format!(
            "LLM fallback skipped for {} unresolved imports because `--allow-llm` was not set.",
            unresolved_imports.len()
        ));
    }
    notes.push(format!(
        "Unresolved imports: {}",
        unresolved_imports.join(", ")
    ));
    notes
}

/// Lightweight extraction of a module name from an error log for the purpose
/// of targeted_recovery stop-reason gating.  Does not need to cover every
/// log format — only the standard ModuleNotFoundError / ImportError patterns.
fn extract_module_from_error_log(log: &str) -> Option<String> {
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
                .split(|ch: char| ch.is_whitespace() || ch == ';' || ch == '\n')
                .next()
                .unwrap_or("")
                .trim_matches('\'')
                .trim_matches('"');
            if !module.is_empty() {
                return Some(module.to_string());
            }
        }
    }
    None
}
