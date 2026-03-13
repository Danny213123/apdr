use crate::cache::store::CacheStore;
use crate::context;
use crate::llm::client::LlmClient;
use crate::llm::{prompts, rag};
use crate::resolver::{pypi_client, version_sampler};
use crate::{ParseResult, ResolveConfig, ResolvedDependency, SolvabilityAssessment};

pub struct StageResult {
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub notes: Vec<String>,
    pub prompts_issued: usize,
}

pub fn assess_solvability(
    snippet_source: &str,
    parse_result: &ParseResult,
    config: &ResolveConfig,
) -> Option<SolvabilityAssessment> {
    let client = LlmClient::new(
        &config.llm_provider,
        &config.llm_model,
        &config.llm_base_url,
    );
    if !client.is_available() {
        return None;
    }

    let benchmark_context = context::read_context_tail(
        config.benchmark_context_log.as_deref(),
        24_000,
    )
    .unwrap_or_default();
    let prompt = prompts::solvability_assessment_prompt(
        snippet_source,
        parse_result,
        &benchmark_context,
    );
    let _ = persist_llm_trace(
        config,
        "solvability-assessment",
        &prompt,
        None,
        &benchmark_context,
        &[],
    );
    let _ = context::append_context_log(
        config.benchmark_context_log.as_deref(),
        "apdr-llm-prompt",
        &prompt,
    );
    let response = client.complete(&prompt)?;
    let _ = context::append_context_log(
        config.benchmark_context_log.as_deref(),
        "apdr-llm-response",
        &response,
    );
    let _ = persist_llm_trace(
        config,
        "solvability-assessment",
        &prompt,
        Some(&response),
        &benchmark_context,
        &[],
    );
    parse_solvability_assessment(&response)
}

pub fn resolve(
    unresolved_imports: &[String],
    parse_result: &ParseResult,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
) -> StageResult {
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
        };
    }

    let client = LlmClient::new(
        &config.llm_provider,
        &config.llm_model,
        &config.llm_base_url,
    );
    if !client.is_available() {
        return StageResult {
            resolved: Vec::new(),
            unresolved: unresolved_imports.to_vec(),
            notes: fallback_notes(unresolved_imports, parse_result, false),
            prompts_issued: 0,
        };
    }

    let context = llm_candidates
        .iter()
        .flat_map(|import_name| rag::assemble_context(store, import_name))
        .collect::<Vec<_>>();
    let benchmark_context = context::read_context_tail(
        config.benchmark_context_log.as_deref(),
        48_000,
    )
    .unwrap_or_default();
    let prompt = prompts::package_resolution_prompt(
        &llm_candidates,
        python_version,
        &context,
        &benchmark_context,
    );
    let _ = persist_llm_trace(
        config,
        "package-resolution",
        &prompt,
        None,
        &benchmark_context,
        &context,
    );
    let _ = context::append_context_log(
        config.benchmark_context_log.as_deref(),
        "apdr-llm-prompt",
        &prompt,
    );
    let response = client.complete(&prompt);
    let Some(response) = response else {
        return StageResult {
            resolved: Vec::new(),
            unresolved: unresolved_imports.to_vec(),
            notes: vec!["LLM package-resolution call returned no output.".to_string()],
            prompts_issued: 1,
        };
    };
    let _ = context::append_context_log(
        config.benchmark_context_log.as_deref(),
        "apdr-llm-response",
        &response,
    );
    let _ = persist_llm_trace(
        config,
        "package-resolution",
        &prompt,
        Some(&response),
        &benchmark_context,
        &context,
    );

    let mut resolved = Vec::new();
    let mut still_unresolved = preserved_unresolved;
    let mut notes = Vec::new();

    for import_name in &llm_candidates {
        let mapped =
            parse_import_mapping(&response, import_name).unwrap_or_else(|| import_name.clone());
        let versions = pypi_client::compatible_versions(store, &mapped, python_version);
        let version = if versions.is_empty() {
            None
        } else {
            let version_prompt =
                prompts::version_inference_prompt(&mapped, &versions, python_version, &benchmark_context);
            let _ = persist_llm_trace(
                config,
                &format!("version-selection-{mapped}"),
                &version_prompt,
                None,
                &benchmark_context,
                &versions,
            );
            let _ = context::append_context_log(
                config.benchmark_context_log.as_deref(),
                "apdr-llm-prompt",
                &version_prompt,
            );
            let picked = client
                .complete(&version_prompt)
                .and_then(|reply| {
                    let _ = context::append_context_log(
                        config.benchmark_context_log.as_deref(),
                        "apdr-llm-response",
                        &reply,
                    );
                    let _ = persist_llm_trace(
                        config,
                        &format!("version-selection-{mapped}"),
                        &version_prompt,
                        Some(&reply),
                        &benchmark_context,
                        &versions,
                    );
                    parse_version_line(&reply, &versions)
                });
            picked.or_else(|| version_sampler::equally_distanced_sample(&versions, &[]))
        };
        if pypi_client::package_exists(store, &mapped, python_version) {
            let _ = store.save_import_mapping(import_name, &mapped, version.as_deref(), "llm");
            resolved.push(ResolvedDependency {
                import_name: import_name.clone(),
                package_name: mapped.clone(),
                version,
                strategy: "llm".to_string(),
                confidence: 0.73,
            });
            notes.push(format!("LLM resolved {import_name} -> {mapped}."));
        } else {
            still_unresolved.push(import_name.clone());
        }
    }

    StageResult {
        resolved,
        unresolved: still_unresolved,
        notes,
        prompts_issued: 1 + llm_candidates.len(),
    }
}

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
    let client = LlmClient::new(
        &config.llm_provider,
        &config.llm_model,
        &config.llm_base_url,
    );
    if !client.is_available() {
        return None;
    }
    let context = rag::assemble_context(store, import_name);
    let benchmark_context = context::read_context_tail(
        config.benchmark_context_log.as_deref(),
        48_000,
    )
    .unwrap_or_default();
    let prompt = prompts::package_resolution_prompt(
        &[import_name.to_string()],
        python_version,
        &context,
        &benchmark_context,
    );
    let _ = persist_llm_trace(
        config,
        &format!("single-package-{import_name}"),
        &prompt,
        None,
        &benchmark_context,
        &context,
    );
    let _ = context::append_context_log(
        config.benchmark_context_log.as_deref(),
        "apdr-llm-prompt",
        &prompt,
    );
    let mapped = client
        .complete(&prompt)
        .and_then(|reply| {
            let _ = context::append_context_log(
                config.benchmark_context_log.as_deref(),
                "apdr-llm-response",
                &reply,
            );
            let _ = persist_llm_trace(
                config,
                &format!("single-package-{import_name}"),
                &prompt,
                Some(&reply),
                &benchmark_context,
                &context,
            );
            parse_import_mapping(&reply, import_name)
        })
        .unwrap_or_else(|| import_name.to_string());
    let versions = pypi_client::compatible_versions(store, &mapped, python_version);
    let version = if versions.is_empty() {
        None
    } else {
        let prompt =
            prompts::version_inference_prompt(&mapped, &versions, python_version, &benchmark_context);
        let _ = persist_llm_trace(
            config,
            &format!("single-version-{mapped}"),
            &prompt,
            None,
            &benchmark_context,
            &versions,
        );
        let _ = context::append_context_log(
            config.benchmark_context_log.as_deref(),
            "apdr-llm-prompt",
            &prompt,
        );
        client
            .complete(&prompt)
            .and_then(|reply| {
                let _ = context::append_context_log(
                    config.benchmark_context_log.as_deref(),
                    "apdr-llm-response",
                    &reply,
                );
                let _ = persist_llm_trace(
                    config,
                    &format!("single-version-{mapped}"),
                    &prompt,
                    Some(&reply),
                    &benchmark_context,
                    &versions,
                );
                parse_version_line(&reply, &versions)
            })
            .or_else(|| version_sampler::equally_distanced_sample(&versions, &[]))
    };
    if parse_result.imports.iter().any(|item| item == import_name) || !mapped.is_empty() {
        Some((mapped, version))
    } else {
        None
    }
}

pub fn fallback_notes(
    unresolved_imports: &[String],
    parse_result: &ParseResult,
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
    notes.push(prompts::package_resolution_prompt(
        unresolved_imports,
        &parse_result.python_version_min,
        &[],
        "",
    ));
    notes
}

fn parse_import_mapping(response: &str, import_name: &str) -> Option<String> {
    for line in response.lines() {
        if let Some((left, right)) = line.split_once('=') {
            if left.trim() == import_name {
                let value = right.trim().to_string();
                if !value.is_empty() {
                    return Some(value);
                }
            }
        }
    }
    None
}

fn parse_version_line(response: &str, versions: &[String]) -> Option<String> {
    for line in response.lines() {
        if let Some((left, right)) = line.split_once('=') {
            if left.trim() == "version" {
                let candidate = right.trim();
                if candidate.eq_ignore_ascii_case("none") {
                    return None;
                }
                if versions.iter().any(|version| version == candidate) {
                    return Some(candidate.to_string());
                }
            }
        }
    }
    None
}

fn looks_like_local_helper_import(parse_result: &ParseResult, import_name: &str) -> bool {
    let normalized = crate::cache::store::normalize(import_name);
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
            .any(|path| crate::cache::store::normalize(path).starts_with(&format!("{normalized}-")))
}

fn parse_solvability_assessment(response: &str) -> Option<SolvabilityAssessment> {
    let mut decision = String::new();
    let mut confidence = None;
    let mut reason = String::new();

    for line in response.lines() {
        if let Some((left, right)) = line.split_once('=') {
            match left.trim() {
                "decision" => decision = right.trim().to_lowercase(),
                "confidence" => {
                    confidence = right.trim().parse::<f64>().ok().map(|value| value.clamp(0.0, 1.0))
                }
                "reason" => reason = right.trim().to_string(),
                _ => {}
            }
        }
    }

    if decision.is_empty() {
        let lowered = response.to_lowercase();
        if lowered.contains("decision=skip") || lowered.contains(" skip ") {
            decision = "skip".to_string();
        } else if lowered.contains("decision=solve") || lowered.contains(" solve ") {
            decision = "solve".to_string();
        }
    }
    if decision != "solve" && decision != "skip" {
        return None;
    }
    let confidence = confidence.unwrap_or(if decision == "skip" { 0.2 } else { 0.6 });
    if reason.is_empty() {
        reason = "LLM solvability assessment did not provide a reason.".to_string();
    }

    Some(SolvabilityAssessment {
        decision,
        confidence,
        reason,
        source: "llm-preflight".to_string(),
    })
}

fn persist_llm_trace(
    config: &ResolveConfig,
    label: &str,
    prompt: &str,
    response: Option<&str>,
    benchmark_context: &str,
    supplemental_context: &[String],
) -> std::io::Result<()> {
    let trace_dir = context::create_llm_trace_dir(&config.output_dir, label)?;
    let supplemental = if supplemental_context.is_empty() {
        "- none".to_string()
    } else {
        supplemental_context.join("\n")
    };
    context::write_text(&trace_dir.join("prompt.txt"), prompt)?;
    context::write_text(
        &trace_dir.join("response.txt"),
        response.unwrap_or("(response pending)"),
    )?;
    context::write_text(
        &trace_dir.join("benchmark-context-tail.txt"),
        if benchmark_context.trim().is_empty() {
            "- none"
        } else {
            benchmark_context
        },
    )?;
    context::write_text(&trace_dir.join("supplemental-context.txt"), &supplemental)?;
    context::write_text(
        &trace_dir.join("metadata.txt"),
        &format!(
            "provider: {}\nmodel: {}\nbase_url: {}\nlabel: {}\n",
            config.llm_provider, config.llm_model, config.llm_base_url, label
        ),
    )?;
    Ok(())
}
