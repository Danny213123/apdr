pub mod family_knowledge;
pub mod pre_solve;
pub mod pypi_client;
pub mod tier1_cache;
pub mod tier2_heuristic;
pub mod tier3_llm;
pub mod version_sampler;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::Path;
use std::time::Instant;

use crate::cache::lockfile_cache;
use crate::cache::store::CacheStore;
use crate::context;
use crate::docker;
use crate::parser;
use crate::recovery::classifier;
use crate::{
    ResolutionReport, ResolveConfig, ResolveResult, ResolvedDependency, ValidationSummary,
};

pub fn resolve_path(
    tool_root: &Path,
    snippet_path: &Path,
    config: &ResolveConfig,
) -> io::Result<ResolveResult> {
    let started = Instant::now();
    context::ensure_debug_layout(&config.output_dir)?;
    let snippet_source = fs::read_to_string(snippet_path)?;
    let data_root = tool_root.join("data");
    let parse_result = parser::parse_snippet(snippet_path, &data_root, config.scan_config_files)?;
    let mut store = CacheStore::load(tool_root, config.cache_path.clone())?;

    let mut selected_python = selected_python_version(&parse_result, config);
    let mut report = ResolutionReport::default();
    write_parse_artifacts(&config.output_dir, snippet_path, &parse_result, &selected_python)?;
    let solvability = if config.allow_llm {
        tier3_llm::assess_solvability(&snippet_source, &parse_result, config)
    } else {
        None
    };
    if let Some(assessment) = solvability.as_ref() {
        report.notes.push(format!(
            "LLM solvability assessment: decision={} confidence={:.2} reason={}",
            assessment.decision, assessment.confidence, assessment.reason
        ));
    }
    if should_skip_from_assessment(solvability.as_ref()) {
        let reason = solvability
            .as_ref()
            .map(|item| format!("LLM skipped snippet at confidence {:.2}: {}", item.confidence, item.reason))
            .unwrap_or_else(|| "LLM skipped snippet as unsolvable.".to_string());
        let validation = skipped_validation_summary(
            "skipped-unsolvable",
            &reason,
            &selected_python,
            &config.output_dir,
            config,
            &render_requirements(&[]),
        );
        report.unresolved = parse_result.imports.clone();
        report.duration = started.elapsed();
        write_state_artifacts(&config.output_dir, "requirements-final.txt", "")?;
        write_state_artifacts(
            &config.output_dir,
            "resolved-final.txt",
            &format_dependency_state(&[], &parse_result.imports),
        )?;
        return Ok(ResolveResult {
            snippet_path: snippet_path.to_path_buf(),
            python_version: selected_python.clone(),
            parse_result,
            solvability,
            resolved: Vec::new(),
            unresolved: report.unresolved.clone(),
            requirements_txt: String::new(),
            lockfile: Some(String::new()),
            docker_image_id: None,
            validation,
            resolution_report: report,
        });
    }

    let (mut resolved, unresolved) = resolve_dependencies(
        &parse_result,
        &selected_python,
        &mut store,
        config,
        &mut report,
    );

    dedupe_dependencies(&mut resolved);
    for note in apply_compatibility_overrides(&parse_result, &mut resolved, &selected_python, config) {
        report.notes.push(note);
    }
    write_state_artifacts(
        &config.output_dir,
        "resolved-before-validation.txt",
        &format_dependency_state(&resolved, &unresolved),
    )?;

    let mut pre_solve = if unresolved.is_empty() {
        Some(pre_solve::solve_dependency_graph(
            &parse_result,
            &resolved,
            &selected_python,
            &mut store,
            config,
        ))
    } else {
        None
    };
    if let Some(result) = pre_solve.as_ref() {
        report.notes.extend(result.notes.clone());
        write_solver_artifacts(&config.output_dir, result)?;
        if result.satisfiable && !result.lockfile_requirements.trim().is_empty() {
            selected_python = result.selected_python_version.clone();
        }
    }

    // If pre-solve failed due to missing KGraph metadata (not hard_unsat), retry with LLM
    if let Some(result) = pre_solve.as_ref() {
        if result.attempted && !result.satisfiable && !result.hard_unsat && config.allow_llm {
            if let Some(packages_without_metadata) = extract_packages_without_metadata(result) {
                let (updated_resolved, updated_unresolved) = retry_with_llm_for_missing_packages(
                    &parse_result,
                    &snippet_source,
                    &resolved,
                    &packages_without_metadata,
                    &selected_python,
                    &mut store,
                    config,
                    &mut report,
                );
                resolved = updated_resolved;

                // Re-run pre-solve with updated dependencies if all imports were resolved
                if updated_unresolved.is_empty() {
                    pre_solve = Some(pre_solve::solve_dependency_graph(
                        &parse_result,
                        &resolved,
                        &selected_python,
                        &mut store,
                        config,
                    ));
                    if let Some(result) = pre_solve.as_ref() {
                        report.notes.push("Re-ran SMT pre-solve after LLM re-resolution of packages with missing metadata.".to_string());
                        report.notes.extend(result.notes.clone());
                        write_solver_artifacts(&config.output_dir, result)?;
                        if result.satisfiable && !result.lockfile_requirements.trim().is_empty() {
                            selected_python = result.selected_python_version.clone();
                        }
                    }
                } else {
                    pre_solve = None;
                }
            }
        }
    }

    let mut requirements_txt = pre_solve
        .as_ref()
        .filter(|result| result.satisfiable && !result.lockfile_requirements.trim().is_empty())
        .map(|result| result.lockfile_requirements.clone())
        .unwrap_or_else(|| render_requirements(&resolved));
    context::write_text(
        &context::debug_root(&config.output_dir).join("requirements-before-validation.txt"),
        &requirements_txt,
    )?;
    let skip_reason = detect_skip_reason(&parse_result, &resolved, &unresolved);
    let validation = if config.validate_with_docker {
        if let Some((status, note)) = skip_reason {
            report.notes.push(note.clone());
            skipped_validation_summary(
                status,
                &note,
                &selected_python,
                &config.output_dir,
                config,
                &requirements_txt,
            )
        } else if let Some(pre_solve) = pre_solve
            .as_ref()
            .filter(|result| result.attempted && !result.satisfiable && result.hard_unsat)
        {
            ValidationSummary {
                succeeded: false,
                status: "unsatisfiable".to_string(),
                reason: pre_solve.reason.clone(),
                selected_python_version: Some(pre_solve.selected_python_version.clone()),
                lockfile_key: Some(lockfile_cache::key_for(&requirements_txt, &selected_python)),
                build_cache_key: Some(lockfile_cache::key_for(&requirements_txt, &selected_python)),
                ..Default::default()
            }
        } else {
            validate_with_retries(
                snippet_path,
                &parse_result,
                &selected_python,
                &mut resolved,
                &mut requirements_txt,
                &mut store,
                config,
                &mut report,
            )?
        }
    } else {
        let unsat_reason = pre_solve
            .as_ref()
            .filter(|result| result.attempted && !result.satisfiable && result.hard_unsat)
            .and_then(|result| result.reason.clone());
        ValidationSummary {
            succeeded: unresolved.is_empty() && unsat_reason.is_none(),
            status: if unresolved.is_empty() && unsat_reason.is_none() {
                "passed".to_string()
            } else if unsat_reason.is_some() {
                "unsatisfiable".to_string()
            } else {
                "unresolved".to_string()
            },
            reason: if let Some(reason) = unsat_reason {
                Some(reason)
            } else if unresolved.is_empty() {
                None
            } else {
                Some(format!(
                    "Skipped local environment validation with {} unresolved imports.",
                    unresolved.len()
                ))
            },
            selected_python_version: Some(selected_python.clone()),
            lockfile_key: Some(lockfile_cache::key_for(&requirements_txt, &selected_python)),
            ..Default::default()
        }
    };

    if validation.succeeded {
        let lockfile_key = lockfile_cache::key_for(&requirements_txt, &selected_python);
        let _ = store.save_lockfile(&lockfile_key, &requirements_txt);
        if let Some(image_tag) = validation.docker_image_id.as_deref() {
            let build_key = lockfile_cache::key_for(&requirements_txt, &selected_python);
            let _ = store.save_build_artifact(&build_key, image_tag);
        }
    }

    report.unresolved = unresolved.clone();
    report.duration = started.elapsed();
    write_state_artifacts(
        &config.output_dir,
        "requirements-final.txt",
        &requirements_txt,
    )?;
    write_state_artifacts(
        &config.output_dir,
        "resolved-final.txt",
        &format_dependency_state(&resolved, &unresolved),
    )?;
    let mut validation = validation;
    validation.debug_dir = Some(context::debug_root(&config.output_dir).display().to_string());
    validation.attempts_dir = Some(context::attempts_root(&config.output_dir).display().to_string());
    validation.llm_trace_dir = Some(context::llm_root(&config.output_dir).display().to_string());
    validation.iterations_dir = Some(context::iterations_root(&config.output_dir).display().to_string());
    validation.context_log_path = config
        .benchmark_context_log
        .as_ref()
        .map(|path| path.display().to_string());

    Ok(ResolveResult {
        snippet_path: snippet_path.to_path_buf(),
        python_version: validation
            .selected_python_version
            .clone()
            .unwrap_or_else(|| selected_python.clone()),
        parse_result,
        solvability,
        resolved,
        unresolved,
        requirements_txt: requirements_txt.clone(),
        lockfile: Some(requirements_txt),
        docker_image_id: validation.docker_image_id.clone(),
        validation,
        resolution_report: report,
    })
}

fn should_skip_from_assessment(assessment: Option<&crate::SolvabilityAssessment>) -> bool {
    let Some(assessment) = assessment else {
        return false;
    };
    assessment.decision == "skip" || assessment.confidence < 0.40
}

fn skipped_validation_summary(
    status: &str,
    reason: &str,
    selected_python: &str,
    output_dir: &Path,
    config: &ResolveConfig,
    requirements_txt: &str,
) -> ValidationSummary {
    let lockfile_key = lockfile_cache::key_for(requirements_txt, selected_python);
    ValidationSummary {
        succeeded: false,
        status: status.to_string(),
        reason: Some(reason.to_string()),
        selected_python_version: Some(selected_python.to_string()),
        lockfile_key: Some(lockfile_key.clone()),
        build_cache_key: Some(lockfile_key),
        debug_dir: Some(context::debug_root(output_dir).display().to_string()),
        attempts_dir: Some(context::attempts_root(output_dir).display().to_string()),
        llm_trace_dir: Some(context::llm_root(output_dir).display().to_string()),
        context_log_path: config
            .benchmark_context_log
            .as_ref()
            .map(|path| path.display().to_string()),
        iterations_dir: Some(context::iterations_root(output_dir).display().to_string()),
        iteration_history: vec![reason.to_string()],
        ..Default::default()
    }
}

fn resolve_dependencies(
    parse_result: &crate::ParseResult,
    python_version: &str,
    store: &mut CacheStore,
    config: &ResolveConfig,
    report: &mut ResolutionReport,
) -> (Vec<ResolvedDependency>, Vec<String>) {
    let mut stage1 = tier1_cache::resolve(parse_result, store, python_version);
    report.cache_hits += stage1.cache_hits;

    let mut stage2 =
        tier2_heuristic::resolve(&stage1.unresolved, parse_result, store, python_version);
    report.heuristic_hits += stage2.heuristic_hits;

    let mut resolved = Vec::new();
    resolved.append(&mut stage1.resolved);
    resolved.append(&mut stage2.resolved);

    let mut unresolved = stage2.unresolved;
    if !unresolved.is_empty() && config.allow_llm {
        let mut stage3 =
            tier3_llm::resolve(&unresolved, parse_result, store, config, python_version);
        report.llm_calls += stage3.prompts_issued;
        report.notes.append(&mut stage3.notes);
        resolved.append(&mut stage3.resolved);
        unresolved = stage3.unresolved;
    } else if !unresolved.is_empty() {
        report.notes.extend(tier3_llm::fallback_notes(
            &unresolved,
            parse_result,
            config.allow_llm,
        ));
    }

    (resolved, unresolved)
}

fn validate_with_retries(
    snippet_path: &Path,
    parse_result: &crate::ParseResult,
    selected_python: &str,
    resolved: &mut Vec<ResolvedDependency>,
    requirements_txt: &mut String,
    store: &mut CacheStore,
    config: &ResolveConfig,
    report: &mut ResolutionReport,
) -> io::Result<ValidationSummary> {
    let mut validation = ValidationSummary::default();
    let mut seen_requirements = BTreeSet::new();
    let mut attempted_versions: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut pending_pattern_learning: Option<(String, String, String, String)> = None;

    for attempt_index in 0..=config.max_retries {
        *requirements_txt = render_requirements(resolved);
        let lockfile_key = lockfile_cache::key_for(requirements_txt, selected_python);
        validation.lockfile_key = Some(lockfile_key.clone());
        validation.build_cache_key = Some(lockfile_key.clone());
        write_iteration_snapshot(
            &config.output_dir,
            attempt_index + 1,
            "requirements-before.txt",
            requirements_txt,
        )?;
        write_iteration_snapshot(
            &config.output_dir,
            attempt_index + 1,
            "resolved-before.txt",
            &format_dependency_state(resolved, &[]),
        )?;
        if let Ok(tail) = context::read_context_tail(config.benchmark_context_log.as_deref(), 48_000) {
            write_iteration_snapshot(
                &config.output_dir,
                attempt_index + 1,
                "benchmark-context-before.txt",
                &tail,
            )?;
        }

        if !seen_requirements.insert(requirements_txt.clone()) {
            report
                .notes
                .push("Stopped validation because requirements began oscillating.".to_string());
            break;
        }

        let versions = if config.parallel_versions {
            family_knowledge::validation_candidate_versions(
                parse_result,
                resolved,
                selected_python,
                config.python_version_range,
                config.execute_snippet,
            )
            .unwrap_or_else(|| {
                docker::parallel::candidate_versions(selected_python, config.python_version_range)
            })
        } else {
            vec![selected_python.to_string()]
        };
        write_iteration_snapshot(
            &config.output_dir,
            attempt_index + 1,
            "candidate-versions.txt",
            &versions.join("\n"),
        )?;

        let attempt_result = docker::builder::validate_requirements(
            snippet_path,
            requirements_txt,
            &parse_result.imports,
            &versions,
            validation.attempts.len(),
            config,
            store,
        )?;
        report.docker_builds += attempt_result.attempts.len();
        validation.lockfile_key = attempt_result
            .lockfile_key
            .clone()
            .or(validation.lockfile_key.clone());
        validation.build_cache_key = attempt_result
            .build_cache_key
            .clone()
            .or(validation.build_cache_key.clone());
        validation.attempts.extend(attempt_result.attempts.clone());

        if let Some((pattern, error_type, conflict_class, fix)) = pending_pattern_learning.take() {
            let _ = store.record_failure_pattern_outcome(
                &pattern,
                &error_type,
                &conflict_class,
                &fix,
                attempt_result.succeeded,
            );
        }

        if attempt_result.succeeded {
            validation.succeeded = true;
            validation.status = "passed".to_string();
            validation.reason = None;
            validation.selected_python_version = attempt_result.selected_python_version.clone();
            validation.docker_image_id = attempt_result.docker_image_id.clone();
            return Ok(validation);
        }

        let last_log = validation
            .attempts
            .last()
            .map(|attempt| attempt.log_excerpt.clone())
            .unwrap_or_default();
        write_iteration_snapshot(
            &config.output_dir,
            attempt_index + 1,
            "last-log.txt",
            &last_log,
        )?;
        if last_log.is_empty() {
            break;
        }

        let classified = classifier::classify_log(&last_log, store);
        write_iteration_snapshot(
            &config.output_dir,
            attempt_index + 1,
            "classifier.txt",
            &format_classifier(&classified),
        )?;
        if let Some(last_attempt) = validation.attempts.last_mut() {
            last_attempt.error_type = Some(classified.error_type.clone());
            last_attempt.conflict_class = Some(classified.conflict_class.clone());
        }
        *report
            .error_types
            .entry(classified.error_type.clone())
            .or_insert(0) += 1;
        *report
            .conflict_classes
            .entry(classified.conflict_class.clone())
            .or_insert(0) += 1;

        if let Some(note) = apply_recovery_fix(
            &classified,
            &last_log,
            resolved,
            parse_result,
            selected_python,
            store,
            &mut attempted_versions,
            config,
        ) {
            report.retries += 1;
            report.notes.push(note.clone());
            validation.iteration_history.push(note.clone());
            pending_pattern_learning = Some((
                learned_pattern_key(&classified, &last_log),
                classified.error_type.clone(),
                classified.conflict_class.clone(),
                note.clone(),
            ));
            if let Some(last_attempt) = validation.attempts.last_mut() {
                last_attempt.fix_applied = Some(note);
            }
            write_iteration_snapshot(
                &config.output_dir,
                attempt_index + 1,
                "recovery.txt",
                report.notes.last().map(String::as_str).unwrap_or(""),
            )?;
            write_iteration_snapshot(
                &config.output_dir,
                attempt_index + 1,
                "requirements-after-recovery.txt",
                &render_requirements(resolved),
            )?;
            continue;
        }

        if let Some(note) = environment_specific_note(&classified, &last_log, parse_result) {
            report.notes.push(note.clone());
            validation.iteration_history.push(note.clone());
            validation.status = "skipped-host-runtime".to_string();
            validation.reason = Some(note.clone());
            if let Some(last_attempt) = validation.attempts.last_mut() {
                last_attempt.fix_applied = Some(note.clone());
            }
            write_iteration_snapshot(
                &config.output_dir,
                attempt_index + 1,
                "recovery.txt",
                &note,
            )?;
            break;
        }

        report.notes.push(format!(
            "No automatic recovery fix found for {}.",
            classified.error_type
        ));
        write_iteration_snapshot(
            &config.output_dir,
            attempt_index + 1,
            "recovery.txt",
            report.notes.last().map(String::as_str).unwrap_or(""),
        )?;
        break;
    }

    if validation.status.is_empty() {
        validation.status = infer_validation_status(&validation);
    }
    if validation.reason.is_none() {
        validation.reason = infer_validation_reason(&validation, report);
    }

    Ok(validation)
}

fn apply_recovery_fix(
    classified: &crate::ClassifierResult,
    log: &str,
    resolved: &mut Vec<ResolvedDependency>,
    parse_result: &crate::ParseResult,
    python_version: &str,
    store: &mut CacheStore,
    attempted_versions: &mut BTreeMap<String, Vec<String>>,
    config: &ResolveConfig,
) -> Option<String> {
    if let Some(note) = family_knowledge::recover_family_knowledge(
        parse_result,
        resolved,
        python_version,
        config.python_version_range,
        config.execute_snippet,
        log,
    ) {
        return Some(note);
    }

    match classified.error_type.as_str() {
        "VersionNotFound" | "DependencyConflict" | "InvalidVersion" | "NonZeroCode" => {
            let (package_name, current_version) = extract_package_and_version(log)?;
            if family_knowledge::protects_family_version(
                parse_result,
                resolved,
                python_version,
                config.python_version_range,
                config.execute_snippet,
                &package_name,
            ) {
                if let Some(note) = family_knowledge::recover_family_knowledge(
                    parse_result,
                    resolved,
                    python_version,
                    config.python_version_range,
                    config.execute_snippet,
                    log,
                ) {
                    return Some(note);
                }
                return Some(format!(
                    "Kept family-managed package `{package_name}` pinned after {} to avoid breaking a curated compatibility bundle.",
                    classified.error_type
                ));
            }
            let known_versions =
                pypi_client::compatible_versions(store, &package_name, python_version);
            if known_versions.is_empty() {
                return None;
            }
            let previous = attempted_versions.entry(package_name.clone()).or_default();
            if let Some(current) = current_version.clone() {
                previous.push(current);
            }
            let next_version =
                version_sampler::equally_distanced_sample(&known_versions, previous)?;
            previous.push(next_version.clone());
            if update_package_version(resolved, &package_name, Some(next_version.clone())) {
                return Some(format!(
                    "Adjusted {package_name} to {next_version} after {}.",
                    classified.error_type
                ));
            }
            None
        }
        "ModuleNotFound" | "ImportError" | "AttributeError" => {
            let module_name = extract_missing_module(log)?;
            if let Some(package_name) = python_backport_package(&module_name, python_version) {
                if upsert_dependency(
                    resolved,
                    &module_name,
                    package_name,
                    None,
                    "recovery:python-backport",
                ) {
                    let _ = store.save_import_mapping(
                        &module_name,
                        package_name,
                        None,
                        "recovery:python-backport",
                    );
                    return Some(format!(
                        "Added Python {} backport package `{}` for missing module `{}`.",
                        python_version, package_name, module_name
                    ));
                }
            }
            if let Some(record) = store.import_lookup(&module_name).cloned() {
                if pypi_client::package_exists(store, &record.package_name, python_version)
                    && upsert_dependency(
                        resolved,
                        &module_name,
                        &record.package_name,
                        record.default_version.clone(),
                        "recovery:cache",
                    )
                {
                    let override_notes =
                        apply_compatibility_overrides(parse_result, resolved, python_version, config);
                    return Some(if override_notes.is_empty() {
                        format!("Remapped {} to {} from cache.", module_name, record.package_name)
                    } else {
                        format!(
                            "Remapped {} to {} from cache. {}",
                            module_name,
                            record.package_name,
                            override_notes.join(" ")
                        )
                    });
                }
            }
            let versions = pypi_client::compatible_versions(store, &module_name, python_version);
            if versions.is_empty() {
                return None;
            }
            let version = version_sampler::equally_distanced_sample(&versions, &[]);
            if upsert_dependency(
                resolved,
                &module_name,
                &module_name,
                version.clone(),
                "recovery:heuristic",
            ) {
                let _ = store.save_import_mapping(
                    &module_name,
                    &module_name,
                    version.as_deref(),
                    "recovery:heuristic",
                );
                let override_notes =
                    apply_compatibility_overrides(parse_result, resolved, python_version, config);
                return Some(if override_notes.is_empty() {
                    format!(
                        "Remapped {module_name} to its exact package after {}.",
                        classified.error_type
                    )
                } else {
                    format!(
                        "Remapped {module_name} to its exact package after {}. {}",
                        classified.error_type,
                        override_notes.join(" ")
                    )
                });
            }
            if config.allow_llm {
                let hint = tier3_llm::single_package_hint(
                    &module_name,
                    parse_result,
                    store,
                    config,
                    python_version,
                );
                if let Some((package_name, version)) = hint {
                    if upsert_dependency(
                        resolved,
                        &module_name,
                        &package_name,
                        version.clone(),
                        "recovery:llm",
                    ) {
                        let _ = store.save_import_mapping(
                            &module_name,
                            &package_name,
                            version.as_deref(),
                            "recovery:llm",
                        );
                        let override_notes =
                            apply_compatibility_overrides(parse_result, resolved, python_version, config);
                        return Some(if override_notes.is_empty() {
                            format!("LLM remapped {module_name} to {package_name}.")
                        } else {
                            format!(
                                "LLM remapped {module_name} to {package_name}. {}",
                                override_notes.join(" ")
                            )
                        });
                    }
                }
            }
            None
        }
        "SyntaxError" => {
            Some("Validation exhausted adjacent Python versions after SyntaxError.".to_string())
        }
        _ => None,
    }
}

fn python_backport_package<'a>(module_name: &str, python_version: &str) -> Option<&'a str> {
    if !python_version.starts_with("2.") {
        return None;
    }
    match module_name.to_lowercase().as_str() {
        "typing" => Some("typing"),
        "pathlib" => Some("pathlib2"),
        "configparser" => Some("configparser"),
        "concurrent.futures" => Some("futures"),
        "ipaddress" => Some("ipaddress"),
        "enum" => Some("enum34"),
        "functools32" => Some("functools32"),
        "singledispatch" => Some("singledispatch"),
        "ordereddict" => Some("ordereddict"),
        "mock" => Some("mock"),
        _ => None,
    }
}

fn selected_python_version(parse_result: &crate::ParseResult, config: &ResolveConfig) -> String {
    if let Some(value) = &config.python_version {
        return value.clone();
    }
    if let Some(value) = &parse_result.python_version_max {
        if value.starts_with("2.") {
            return value.clone();
        }
    }
    parse_result.python_version_min.clone()
}

fn render_requirements(resolved: &[ResolvedDependency]) -> String {
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

fn dedupe_dependencies(resolved: &mut Vec<ResolvedDependency>) {
    let mut seen = BTreeSet::new();
    resolved.retain(|dependency| seen.insert(dependency.package_name.clone()));
}

fn update_package_version(
    resolved: &mut [ResolvedDependency],
    package_name: &str,
    version: Option<String>,
) -> bool {
    for dependency in resolved.iter_mut() {
        if dependency.package_name == package_name {
            dependency.version = version;
            dependency.strategy = "recovery:version-adjustment".to_string();
            dependency.confidence = 0.74;
            return true;
        }
    }
    false
}

fn ensure_dependency(
    resolved: &mut Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<String>,
    strategy: &str,
) -> bool {
    if resolved
        .iter()
        .any(|dependency| dependency.package_name == package_name)
    {
        return false;
    }
    resolved.push(ResolvedDependency {
        import_name: import_name.to_string(),
        package_name: package_name.to_string(),
        version,
        strategy: strategy.to_string(),
        confidence: 0.69,
    });
    true
}

fn upsert_dependency(
    resolved: &mut Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<String>,
    strategy: &str,
) -> bool {
    for dependency in resolved.iter_mut() {
        if dependency.import_name == import_name {
            let changed = dependency.package_name != package_name || dependency.version != version;
            dependency.package_name = package_name.to_string();
            dependency.version = version.clone();
            dependency.strategy = strategy.to_string();
            dependency.confidence = 0.78;
            return changed;
        }
        if dependency.package_name == package_name {
            return false;
        }
    }
    ensure_dependency(resolved, import_name, package_name, version, strategy)
}

fn apply_compatibility_overrides(
    parse_result: &crate::ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
    config: &ResolveConfig,
) -> Vec<String> {
    family_knowledge::apply_family_knowledge(
        parse_result,
        resolved,
        selected_python,
        config.python_version_range,
        config.execute_snippet,
    )
}

fn extract_package_and_version(log: &str) -> Option<(String, Option<String>)> {
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
    None
}

fn extract_missing_module(log: &str) -> Option<String> {
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

fn learned_pattern_key(classified: &crate::ClassifierResult, log: &str) -> String {
    if classified.matched_pattern != "no-known-pattern" {
        return classified.matched_pattern.clone();
    }

    log.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.chars().take(120).collect::<String>())
        .unwrap_or_else(|| classified.error_type.clone())
}

fn write_parse_artifacts(
    output_dir: &Path,
    snippet_path: &Path,
    parse_result: &crate::ParseResult,
    selected_python: &str,
) -> io::Result<()> {
    let imports = if parse_result.imports.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .imports
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let import_paths = if parse_result.import_paths.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .import_paths
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let configs = if parse_result.config_deps.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .config_deps
            .iter()
            .map(|dep| {
                format!(
                    "- {}{} ({})",
                    dep.package,
                    dep.constraint
                        .as_ref()
                        .map(|value| format!(" {value}"))
                        .unwrap_or_default(),
                    dep.source_file
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let scanned = if parse_result.scanned_files.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .scanned_files
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    write_state_artifacts(
        output_dir,
        "parse-summary.txt",
        &format!(
            "snippet: {}\nselected_python: {}\npython_version_min: {}\npython_version_max: {}\nconfidence: {:.2}\n\nimports:\n{}\n\nimport_paths:\n{}\n\nconfig_dependencies:\n{}\n\nscanned_files:\n{}\n",
            snippet_path.display(),
            selected_python,
            parse_result.python_version_min,
            parse_result.python_version_max.as_deref().unwrap_or("--"),
            parse_result.confidence,
            imports,
            import_paths,
            configs,
            scanned,
        ),
    )
}

fn write_solver_artifacts(
    output_dir: &Path,
    result: &pre_solve::PreSolveResult,
) -> io::Result<()> {
    let assignments = if result.assigned_versions.is_empty() {
        "- none".to_string()
    } else {
        result
            .assigned_versions
            .iter()
            .map(|(package, version)| format!("- {package}=={version}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    write_state_artifacts(
        output_dir,
        "solver-summary.txt",
        &format!(
            "attempted: {}\nsatisfiable: {}\nselected_python: {}\ndirect_packages: {}\ntransitive_packages: {}\nreason: {}\n\nassignments:\n{}\n",
            result.attempted,
            result.satisfiable,
            result.selected_python_version,
            if result.direct_packages.is_empty() {
                "--".to_string()
            } else {
                result.direct_packages.join(", ")
            },
            if result.transitive_packages.is_empty() {
                "--".to_string()
            } else {
                result.transitive_packages.join(", ")
            },
            result.reason.as_deref().unwrap_or("--"),
            assignments,
        ),
    )?;
    write_state_artifacts(
        output_dir,
        "solver-lockfile.txt",
        &result.lockfile_requirements,
    )
}

fn write_state_artifacts(output_dir: &Path, name: &str, contents: &str) -> io::Result<()> {
    context::write_text(&context::debug_root(output_dir).join(name), contents)
}

fn write_iteration_snapshot(
    output_dir: &Path,
    iteration_index: usize,
    name: &str,
    contents: &str,
) -> io::Result<()> {
    let directory = context::iteration_dir(output_dir, iteration_index);
    fs::create_dir_all(&directory)?;
    context::write_text(&directory.join(name), contents)
}

fn format_dependency_state(
    resolved: &[ResolvedDependency],
    unresolved: &[String],
) -> String {
    let resolved_rows = if resolved.is_empty() {
        "- none".to_string()
    } else {
        resolved
            .iter()
            .map(|dependency| {
                format!(
                    "- import={} package={} version={} strategy={} confidence={:.2}",
                    dependency.import_name,
                    dependency.package_name,
                    dependency.version.as_deref().unwrap_or("--"),
                    dependency.strategy,
                    dependency.confidence,
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let unresolved_rows = if unresolved.is_empty() {
        "- none".to_string()
    } else {
        unresolved
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    format!("resolved:\n{resolved_rows}\n\nunresolved:\n{unresolved_rows}\n")
}

fn format_classifier(classified: &crate::ClassifierResult) -> String {
    format!(
        "error_type: {}\nconflict_class: {}\nmatched_pattern: {}\nrecommended_fix: {}\n",
        classified.error_type,
        classified.conflict_class,
        classified.matched_pattern,
        classified.recommended_fix,
    )
}

fn environment_specific_note(
    classified: &crate::ClassifierResult,
    log: &str,
    parse_result: &crate::ParseResult,
) -> Option<String> {
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
            | "rhinoscriptsyntax"
            | "hou"
            | "unreal"
            | "nuke"
            | "clr"
            | "win32com"
            | "c4d"
            | "odbaccess"
    ) {
        return Some(format!(
            "Detected host-application dependency ({missing}). APDR cannot validate this snippet without the corresponding application runtime."
        ));
    }
    if missing == "rpi" || missing == "rpi.gpio" || source_markers.contains("rpi") {
        return Some(
            "Detected hardware/runtime dependency (RPi.GPIO). APDR cannot validate this snippet without Raspberry Pi GPIO access.".to_string(),
        );
    }
    let py2_stdlib = [
        "urllib2", "urlparse", "_winreg", "configparser", "cpickle",
        "cstringio", "queue", "htmlparser", "httplib", "cookielib",
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

fn infer_validation_status(validation: &ValidationSummary) -> String {
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

fn infer_validation_reason(
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
        return Some(format!("Runtime import failed: missing module `{module_name}`."));
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
        return Some("Local package-environment build timed out during APDR validation.".to_string());
    }
    if attempt.status == "runtime-timeout" {
        return Some("Local APDR smoke test timed out during validation.".to_string());
    }
    report.notes.last().cloned().filter(|note| !note.is_empty())
}

fn extract_dependency_conflict_reason(log: &str) -> Option<String> {
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

fn extract_python_version_mismatch_reason(log: &str) -> Option<String> {
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

fn detect_skip_reason(
    parse_result: &crate::ParseResult,
    resolved: &[ResolvedDependency],
    unresolved: &[String],
) -> Option<(&'static str, String)> {
    let mut markers = BTreeSet::new();
    for item in &parse_result.imports {
        markers.insert(item.to_lowercase());
    }
    for item in &parse_result.import_paths {
        markers.insert(item.to_lowercase());
    }
    for item in unresolved {
        markers.insert(item.to_lowercase());
    }
    for dependency in resolved {
        markers.insert(dependency.import_name.to_lowercase());
        markers.insert(dependency.package_name.to_lowercase());
    }

    if markers.iter().any(|item| item == "pyqt4" || item.starts_with("pyqt4."))
        || markers.iter().any(|item| item == "maya" || item.starts_with("maya."))
    {
        return Some((
            "skipped-host-runtime",
            "Detected host-application dependency (Maya/PyQt4). APDR cannot validate this snippet without the Autodesk Maya desktop runtime.".to_string(),
        ));
    }

    for marker in [
        "arcpy",
        "bpy",
        "c4d",
        "rhinoscriptsyntax",
        "hou",
        "unreal",
        "nuke",
        "clr",
        "win32com",
        "odbaccess",
    ] {
        if markers.iter().any(|item| item == marker || item.starts_with(&format!("{marker}."))) {
            return Some((
                "skipped-host-runtime",
                format!(
                    "Detected host-application dependency ({marker}). APDR cannot validate this snippet without the corresponding application runtime."
                ),
            ));
        }
    }

    let apple_framework_markers = [
        "foundation",
        "appkit",
        "quartz",
        "systemconfiguration",
        "corefoundation",
        "cfnetwork",
        "security",
        "coreservices",
        "launchservices",
        "pyobjc",
        "pyobjc-core",
        "pyobjc-framework-cocoa",
        "pyobjc-framework-systemconfiguration",
        "pyobjc-framework-quartz",
        "pyobjc-framework-security",
        "pyobjc-framework-coreservices",
    ];
    let has_apple_bridge = markers.iter().any(|item| item == "objc" || item.starts_with("objc."));
    let has_apple_framework = apple_framework_markers.iter().any(|marker| {
        markers
            .iter()
            .any(|item| item == marker || item.starts_with(&format!("{marker}.")))
    });
    if has_apple_bridge && has_apple_framework {
        return Some((
            "skipped-host-runtime",
            "Detected macOS Objective-C framework dependency (PyObjC/Foundation/SystemConfiguration). APDR cannot validate this snippet without the macOS host framework runtime."
                .to_string(),
        ));
    }

    if markers.iter().any(|item| item == "rpi" || item == "rpi.gpio") {
        return Some((
            "skipped-host-runtime",
            "Detected hardware/runtime dependency (RPi.GPIO). APDR cannot validate this snippet without Raspberry Pi GPIO access.".to_string(),
        ));
    }

    if markers.iter().any(|item| item == "input_data")
        || markers
            .iter()
            .any(|item| item == "util" || item.starts_with("util."))
    {
        return Some((
            "skipped-local-helper",
            "Snippet depends on local helper modules (`input_data`/`util`) that are not bundled as installable packages in this case.".to_string(),
        ));
    }

    None
}

/// Extract package names from pre-solve error message indicating missing KGraph metadata
fn extract_packages_without_metadata(result: &pre_solve::PreSolveResult) -> Option<Vec<String>> {
    let reason = result.reason.as_ref()?;
    if !reason.contains("has no cached or KGraph version metadata") {
        return None;
    }

    let mut packages = Vec::new();
    // Parse error messages like: "package `swift` has no cached or KGraph version metadata"
    for fragment in reason.split('|') {
        let trimmed = fragment.trim();
        if let Some(start_idx) = trimmed.find("package `") {
            if let Some(end_idx) = trimmed[start_idx + 9..].find('`') {
                let package = &trimmed[start_idx + 9..start_idx + 9 + end_idx];
                if !packages.contains(&package.to_string()) {
                    packages.push(package.to_string());
                }
            }
        }
    }

    if packages.is_empty() {
        None
    } else {
        Some(packages)
    }
}

/// Retry resolution with LLM for packages that have no KGraph metadata
fn retry_with_llm_for_missing_packages(
    parse_result: &crate::ParseResult,
    snippet_source: &str,
    resolved: &[ResolvedDependency],
    packages_without_metadata: &[String],
    python_version: &str,
    store: &mut CacheStore,
    config: &ResolveConfig,
    report: &mut crate::ResolutionReport,
) -> (Vec<ResolvedDependency>, Vec<String>) {
    let packages_set: BTreeSet<String> = packages_without_metadata
        .iter()
        .map(|pkg| pypi_client::requirement_name(pkg))
        .collect();

    // Partition resolved dependencies into those to keep and those to retry
    let mut kept_resolved = Vec::new();
    let mut imports_to_retry = Vec::new();

    for dep in resolved {
        let normalized_package = pypi_client::requirement_name(&dep.package_name);
        if packages_set.contains(&normalized_package) {
            // This dependency maps to a package with no metadata - retry it
            imports_to_retry.push(dep.import_name.clone());
            report.notes.push(format!(
                "Package `{}` has no KGraph metadata. Retrying import `{}` with tier3_llm.",
                dep.package_name, dep.import_name
            ));
        } else {
            // Keep this dependency
            kept_resolved.push(dep.clone());
        }
    }

    if imports_to_retry.is_empty() {
        return (resolved.to_vec(), Vec::new());
    }

    // Call tier3_llm with additional context about the missing metadata
    let llm_result = tier3_llm::resolve_with_context(
        &imports_to_retry,
        snippet_source,
        parse_result,
        store,
        config,
        python_version,
        Some(format!(
            "Previous resolution failed because these packages have no version metadata in the package index: {}. Please suggest alternative package names that might provide these imports.",
            packages_without_metadata.join(", ")
        )),
    );

    report.llm_calls += llm_result.prompts_issued;
    report.notes.append(&mut llm_result.notes.clone());

    // Merge LLM resolutions with kept dependencies
    let mut final_resolved = kept_resolved;
    final_resolved.extend(llm_result.resolved);

    (final_resolved, llm_result.unresolved)
}
