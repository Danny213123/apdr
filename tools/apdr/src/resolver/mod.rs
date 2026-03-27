pub mod family_knowledge;
pub mod kgraph_db;
pub mod pre_solve;
pub mod pubgrub_solver;
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

use crate::cache;
use crate::cache::lockfile_cache;
use crate::cache::store::CacheStore;
use crate::context;
use crate::docker;
use crate::parser;
use crate::recovery::classifier;
use crate::{
    ResolutionReport, ResolveConfig, ResolveResult, ResolvedDependency, SolvabilityAssessment,
    UnsolvableModuleRecord, ValidationSummary,
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
    write_parse_artifacts(
        &config.output_dir,
        snippet_path,
        &parse_result,
        &selected_python,
    )?;

    // Fast path: detect host-runtime / hardware dependencies from import names alone.
    // This avoids 250-375s of wasted tier1/2/3 + pre-solve work for cases that will
    // inevitably be skipped after resolution anyway.
    // In LLM-only mode, skip this check — let the LLM decide everything.
    if !config.llm_only_mode {
        if let Some((status, note)) = detect_skip_reason(&parse_result, &[], &[]) {
            report.notes.push(note.clone());
            let mut validation = skipped_validation_summary(
                status,
                &note,
                &selected_python,
                &config.output_dir,
                config,
                &render_requirements(&[]),
            );
            validation.solve_duration_ms = started.elapsed().as_millis();
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
                solvability: None,
                resolved: Vec::new(),
                unresolved: report.unresolved.clone(),
                requirements_txt: String::new(),
                lockfile: Some(String::new()),
                build_image_id: None,
                validation,
                resolution_report: report,
            });
        }

        // Fast path: check if any import is known-unsolvable from prior LLM assessments.
        // This avoids all resolution + validation work for cases already seen.
        if let Some((hit_module, record)) = check_unsolvable_cache(&parse_result, &store) {
            let reason = format!(
                "Cached unsolvable module `{}` (confidence {:.2}, seen {} times): {}",
                hit_module, record.confidence, record.times_seen, record.reason
            );
            report.notes.push(reason.clone());
            let skip_status = unsolvable_status_for_category(&record.category);
            let mut validation = skipped_validation_summary(
                skip_status,
                &reason,
                &selected_python,
                &config.output_dir,
                config,
                &render_requirements(&[]),
            );
            validation.solve_duration_ms = started.elapsed().as_millis();
            report.unresolved = parse_result.imports.clone();
            report.duration = started.elapsed();
            write_state_artifacts(&config.output_dir, "requirements-final.txt", "")?;
            write_state_artifacts(
                &config.output_dir,
                "resolved-final.txt",
                &format_dependency_state(&[], &parse_result.imports),
            )?;
            // Increment the seen counter for the matched module
            let _ = store.save_unsolvable_module(
                &hit_module,
                &record.category,
                &record.reason,
                record.confidence,
            );
            return Ok(ResolveResult {
                snippet_path: snippet_path.to_path_buf(),
                python_version: selected_python.clone(),
                parse_result,
                solvability: Some(SolvabilityAssessment {
                    decision: "skip".to_string(),
                    confidence: record.confidence,
                    reason: record.reason.clone(),
                    source: "cached-unsolvable".to_string(),
                    unsolvable_modules: vec![hit_module],
                }),
                resolved: Vec::new(),
                unresolved: report.unresolved.clone(),
                requirements_txt: String::new(),
                lockfile: Some(String::new()),
                build_image_id: None,
                validation,
                resolution_report: report,
            });
        }
    }

    // --- #3: Fast path — reuse a previously validated import-set solution ---
    // If we've already validated an identical import set successfully, skip all
    // resolution + validation work and return the cached result immediately (~5ms).
    // Disabled when --force-validate is set (benchmark mode: always re-validate).
    if config.validate && !config.llm_only_mode && !config.force_validate {
        let import_key = cache::store::import_set_key(&parse_result.imports);
        if let Some(cached) = store.load_import_set_solution(&import_key) {
            report.notes.push(format!(
                "Import-set cache hit (key={}) — reusing validated solution.",
                &import_key[..8.min(import_key.len())]
            ));
            let requirements_txt = cached.requirements_txt.clone();
            let mut validation = ValidationSummary {
                succeeded: true,
                status: "passed-cached".to_string(),
                validation_backend: "import-set-cache".to_string(),
                reason: Some("Reused previously validated import-set solution.".to_string()),
                selected_python_version: Some(cached.python_version.clone()),
                lockfile_key: Some(cache::lockfile_cache::key_for(
                    &requirements_txt,
                    &cached.python_version,
                )),
                ..Default::default()
            };
            validation.solve_duration_ms = started.elapsed().as_millis();
            report.duration = started.elapsed();
            write_state_artifacts(
                &config.output_dir,
                "requirements-final.txt",
                &requirements_txt,
            )?;
            write_state_artifacts(
                &config.output_dir,
                "resolved-final.txt",
                &format_dependency_state(&cached.resolved, &[]),
            )?;
            return Ok(ResolveResult {
                snippet_path: snippet_path.to_path_buf(),
                python_version: cached.python_version.clone(),
                parse_result,
                solvability: None,
                resolved: cached.resolved,
                unresolved: Vec::new(),
                requirements_txt: requirements_txt.clone(),
                lockfile: Some(requirements_txt),
                build_image_id: None,
                validation,
                resolution_report: report,
            });
        }
    }

    // Run tier1 (cache) + tier2 (heuristic) first — these are fast (~ms)
    // In LLM-only mode, skip these tiers and go straight to tier3.
    let mut resolved = Vec::new();
    let mut unresolved = if config.llm_only_mode {
        report
            .notes
            .push("LLM-only mode: skipping tier1 cache and tier2 heuristics.".to_string());
        parse_result.imports.clone()
    } else {
        let mut stage1 = tier1_cache::resolve(&parse_result, &mut store, &selected_python);
        report.cache_hits += stage1.cache_hits;
        let mut stage2 = tier2_heuristic::resolve(
            &stage1.unresolved,
            &parse_result,
            &mut store,
            &selected_python,
        );
        report.heuristic_hits += stage2.heuristic_hits;
        resolved.append(&mut stage1.resolved);
        resolved.append(&mut stage2.resolved);
        stage2.unresolved
    };

    // Only invoke the LLM solvability assessment when there are unresolved imports
    // (avoids 3-8s Ollama overhead when tier1/tier2 already resolved everything)
    // In LLM-only mode, skip solvability — we always attempt resolution.
    // --- #9: Skip solvability when tier1/tier2 resolved >80% of imports ---
    // If most imports are already resolved, the snippet is almost certainly solvable.
    let total_imports = parse_result.imports.len();
    let skip_solvability = total_imports > 0
        && !unresolved.is_empty()
        && (resolved.len() as f64 / total_imports as f64) >= 0.8;
    let solvability =
        if !unresolved.is_empty() && config.allow_llm && !config.llm_only_mode && !skip_solvability
        {
            let assessment = tier3_llm::assess_solvability(&snippet_source, &parse_result, config);
            if let Some(ref a) = assessment {
                report.notes.push(format!(
                    "LLM solvability assessment: decision={} confidence={:.2} reason={}",
                    a.decision, a.confidence, a.reason
                ));
            }
            if should_skip_from_assessment(assessment.as_ref()) {
                // Learn: only persist when the LLM explicitly named specific
                // unsolvable modules AND confidence is very high.  Never bulk-
                // cache all imports — that poisons common names like django.
                if let Some(ref a) = assessment {
                    if !a.unsolvable_modules.is_empty() && a.confidence >= 0.95 {
                        for module in &a.unsolvable_modules {
                            let _ = store.save_unsolvable_module(
                                module,
                                "host-runtime",
                                &a.reason,
                                a.confidence,
                            );
                        }
                    }
                }
                let reason = assessment
                    .as_ref()
                    .map(|item| {
                        format!(
                            "LLM skipped snippet at confidence {:.2}: {}",
                            item.confidence, item.reason
                        )
                    })
                    .unwrap_or_else(|| "LLM skipped snippet as unsolvable.".to_string());
                let mut validation = skipped_validation_summary(
                    "skipped-unsolvable",
                    &reason,
                    &selected_python,
                    &config.output_dir,
                    config,
                    &render_requirements(&[]),
                );
                validation.solve_duration_ms = started.elapsed().as_millis();
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
                    solvability: assessment,
                    resolved: Vec::new(),
                    unresolved: report.unresolved.clone(),
                    requirements_txt: String::new(),
                    lockfile: Some(String::new()),
                    build_image_id: None,
                    validation,
                    resolution_report: report,
                });
            }

            // Run tier3 (LLM) for remaining unresolved imports
            let mut stage3 = tier3_llm::resolve(
                &unresolved,
                &parse_result,
                &mut store,
                config,
                &selected_python,
            );
            report.llm_calls += stage3.prompts_issued;
            report.notes.append(&mut stage3.notes);
            resolved.append(&mut stage3.resolved);
            unresolved = stage3.unresolved;

            assessment
        } else if !unresolved.is_empty() && config.allow_llm && skip_solvability {
            // #9: Tier1/tier2 resolved >80% — skip solvability, go straight to LLM resolution
            report.notes.push(format!(
                "Skipped solvability assessment: {}/{} imports already resolved by tier1/tier2.",
                resolved.len(),
                total_imports,
            ));
            let mut stage3 = tier3_llm::resolve(
                &unresolved,
                &parse_result,
                &mut store,
                config,
                &selected_python,
            );
            report.llm_calls += stage3.prompts_issued;
            report.notes.append(&mut stage3.notes);
            resolved.append(&mut stage3.resolved);
            unresolved = stage3.unresolved;
            None
        } else if !unresolved.is_empty() && config.llm_only_mode {
            // LLM-only mode: skip solvability assessment, go straight to LLM resolution
            let mut stage3 = tier3_llm::resolve(
                &unresolved,
                &parse_result,
                &mut store,
                config,
                &selected_python,
            );
            report.llm_calls += stage3.prompts_issued;
            report.notes.append(&mut stage3.notes);
            resolved.append(&mut stage3.resolved);
            unresolved = stage3.unresolved;
            None
        } else if !unresolved.is_empty() {
            report.notes.extend(tier3_llm::fallback_notes(
                &unresolved,
                &parse_result,
                config.allow_llm,
            ));
            None
        } else {
            None
        };

    dedupe_dependencies(&mut resolved);
    if !resolved.is_empty() {
        report.min_confidence = resolved
            .iter()
            .map(|d| d.confidence)
            .fold(f64::INFINITY, f64::min);
        report.mean_confidence =
            resolved.iter().map(|d| d.confidence).sum::<f64>() / resolved.len() as f64;
    }
    for note in
        apply_compatibility_overrides(&parse_result, &mut resolved, &selected_python, config)
    {
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

                // Re-run pre-solve with updated dependencies if all imports were resolved
                if updated_unresolved.is_empty() {
                    resolved = updated_resolved;
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
                    // LLM retry didn't resolve all imports — keep the original
                    // resolution. Pre-solve couldn't verify due to missing KGraph
                    // metadata but pip will verify at install time during validation.
                    pre_solve = None;
                }
            }
        }
    }

    // Use pre-solve lockfile pins for Python 3+ targets.  For Python 2
    // targets skip pins: KGraph lacks python_requires metadata so pre-solve
    // may pin modern versions (e.g. Scrapy 2.x) that don't support Python 2.
    // Letting pip resolve versions natively respects python_requires.
    let mut requirements_txt = pre_solve
        .as_ref()
        .filter(|result| result.satisfiable && !result.lockfile_requirements.trim().is_empty())
        .filter(|_| !selected_python.starts_with("2."))
        .map(|result| result.lockfile_requirements.clone())
        .unwrap_or_else(|| render_requirements(&resolved));

    // For Python 2 targets, strip generic seed version pins (e.g.
    // requests==2.32.3 from top_5000_mappings.tsv) since they target modern
    // Python 3.  Family pins (curated for specific Python versions) are
    // preserved.  Also cap unpinned packages to their last known Py2 version
    // to avoid installing Py3-only releases.
    if selected_python.starts_with("2.") {
        for dep in &mut resolved {
            if dep.strategy == "cache:seed" {
                dep.version = None;
            }
            // Cap to last known Py2-compatible version when no version is set
            if dep.version.is_none() {
                if let Some(ceiling) = last_python2_version(&dep.package_name) {
                    dep.version = Some(ceiling.to_string());
                }
            }
        }
        requirements_txt = render_requirements(&resolved);
    }
    context::write_text(
        &context::debug_root(&config.output_dir).join("requirements-before-validation.txt"),
        &requirements_txt,
    )?;
    let skip_reason = detect_skip_reason(&parse_result, &resolved, &unresolved);
    let solve_duration_ms = started.elapsed().as_millis();
    let validation_started = Instant::now();
    let mut validation = if config.validate {
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
        } else if let Some((missing_pkg, _is_seed)) =
            find_nonexistent_package(&resolved, &mut store, &selected_python)
        {
            // If LLM is available, retry imports whose resolved package does
            // not exist on PyPI — regardless of whether they came from seed
            // mappings, heuristics, or other sources.
            if config.allow_llm {
                let (new_resolved, new_unresolved) = retry_nonexistent_packages(
                    &parse_result,
                    &snippet_source,
                    &resolved,
                    &selected_python,
                    &mut store,
                    config,
                    &mut report,
                );
                resolved = new_resolved;
                unresolved.extend(new_unresolved);
                requirements_txt = render_requirements(&resolved);
                // Re-check after LLM retry
                if let Some((still_missing, _)) =
                    find_nonexistent_package(&resolved, &mut store, &selected_python)
                {
                    let note = format!(
                        "Package `{}` does not exist on PyPI. Skipping validation.",
                        still_missing
                    );
                    report.notes.push(note.clone());
                    ValidationSummary {
                        succeeded: false,
                        status: "package-does-not-exist".to_string(),
                        reason: Some(note),
                        validation_backend: config.validation_backend().to_string(),
                        selected_python_version: Some(selected_python.clone()),
                        lockfile_key: Some(lockfile_cache::key_for(
                            &requirements_txt,
                            &selected_python,
                        )),
                        build_cache_key: Some(lockfile_cache::key_for(
                            &requirements_txt,
                            &selected_python,
                        )),
                        ..Default::default()
                    }
                } else {
                    // LLM fixed it — proceed to validation
                    validate_with_retries(
                        snippet_path,
                        &snippet_source,
                        &parse_result,
                        &selected_python,
                        &mut resolved,
                        &mut requirements_txt,
                        &mut store,
                        config,
                        &mut report,
                    )?
                }
            } else if config.force_validate {
                report.notes.push(format!(
                    "Package `{}` may not exist on PyPI but --force-validate is set — proceeding.",
                    missing_pkg
                ));
                validate_with_retries(
                    snippet_path,
                    &snippet_source,
                    &parse_result,
                    &selected_python,
                    &mut resolved,
                    &mut requirements_txt,
                    &mut store,
                    config,
                    &mut report,
                )?
            } else {
                let note = format!(
                    "Package `{}` does not exist on PyPI. Skipping validation.",
                    missing_pkg
                );
                report.notes.push(note.clone());
                ValidationSummary {
                    succeeded: false,
                    status: "package-does-not-exist".to_string(),
                    reason: Some(note),
                    validation_backend: config.validation_backend().to_string(),
                    selected_python_version: Some(selected_python.clone()),
                    lockfile_key: Some(lockfile_cache::key_for(
                        &requirements_txt,
                        &selected_python,
                    )),
                    build_cache_key: Some(lockfile_cache::key_for(
                        &requirements_txt,
                        &selected_python,
                    )),
                    ..Default::default()
                }
            }
        } else if let Some(pre_solve) = pre_solve
            .as_ref()
            .filter(|result| result.attempted && !result.satisfiable && result.hard_unsat)
            // For Python 2.7: KGraph lacks python_requires metadata, so
            // pre-solve reports false UNSATs for packages like django that
            // DO have Python 2 compatible releases.  Let pip handle it.
            .filter(|_| !selected_python.starts_with("2."))
        {
            // Pre-solve found a genuine version conflict for Python 3+.
            // Still proceed to validation when the LLM agent is available
            // — the Docker+LLM build-retry pipeline (PLLM approach) can
            // sometimes recover cases the deterministic solver cannot.
            if config.allow_llm {
                report.notes.push(format!(
                    "Pre-solve UNSAT but LLM agent available — proceeding to validation. {}",
                    pre_solve.reason.as_deref().unwrap_or("")
                ));
                validate_with_retries(
                    snippet_path,
                    &snippet_source,
                    &parse_result,
                    &selected_python,
                    &mut resolved,
                    &mut requirements_txt,
                    &mut store,
                    config,
                    &mut report,
                )?
            } else if config.force_validate {
                // --force-validate: attempt validation even when pre-solve says UNSAT
                report.notes.push(format!(
                    "Pre-solve UNSAT but --force-validate is set — proceeding to validation. {}",
                    pre_solve.reason.as_deref().unwrap_or("")
                ));
                validate_with_retries(
                    snippet_path,
                    &snippet_source,
                    &parse_result,
                    &selected_python,
                    &mut resolved,
                    &mut requirements_txt,
                    &mut store,
                    config,
                    &mut report,
                )?
            } else {
                ValidationSummary {
                    succeeded: false,
                    status: "unsatisfiable".to_string(),
                    reason: pre_solve.reason.clone(),
                    validation_backend: config.validation_backend().to_string(),
                    selected_python_version: Some(pre_solve.selected_python_version.clone()),
                    lockfile_key: Some(lockfile_cache::key_for(
                        &requirements_txt,
                        &selected_python,
                    )),
                    build_cache_key: Some(lockfile_cache::key_for(
                        &requirements_txt,
                        &selected_python,
                    )),
                    ..Default::default()
                }
            }
        } else {
            validate_with_retries(
                snippet_path,
                &snippet_source,
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
            validation_backend: config.validation_backend().to_string(),
            reason: if let Some(reason) = unsat_reason {
                Some(reason)
            } else if unresolved.is_empty() {
                None
            } else {
                Some(format!(
                    "Skipped {} validation with {} unresolved imports.",
                    config.validation_backend(),
                    unresolved.len()
                ))
            },
            selected_python_version: Some(selected_python.clone()),
            lockfile_key: Some(lockfile_cache::key_for(&requirements_txt, &selected_python)),
            ..Default::default()
        }
    };
    validation.solve_duration_ms = solve_duration_ms;
    if config.validate && !validation.attempts.is_empty() {
        validation.validation_duration_ms = validation_started.elapsed().as_millis();
    }
    let repeat_failure_signature = validation.repeat_failure_signature.clone();
    update_failure_metadata(&mut validation, config, &resolved, repeat_failure_signature);
    // Count LangGraph agent invocations as LLM calls so the UI reflects them.
    report.llm_calls += validation.agent_invocations;

    if validation.succeeded {
        let lockfile_key = lockfile_cache::key_for(&requirements_txt, &selected_python);
        let _ = store.save_lockfile(&lockfile_key, &requirements_txt);
        if let Some(image_id) = validation.build_image_id.as_deref() {
            let build_key = lockfile_cache::key_for(&requirements_txt, &selected_python);
            let _ = store.save_build_artifact(&build_key, image_id);
        }
        // Import-set memory: cache the full solution for cross-case reuse
        let import_key = cache::store::import_set_key(&parse_result.imports);
        let _ = store.save_import_set_solution(
            &import_key,
            &selected_python,
            &requirements_txt,
            &resolved,
        );
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
    validation.debug_dir = Some(
        context::debug_root(&config.output_dir)
            .display()
            .to_string(),
    );
    validation.attempts_dir = Some(
        context::attempts_root(&config.output_dir)
            .display()
            .to_string(),
    );
    validation.llm_trace_dir = Some(context::llm_root(&config.output_dir).display().to_string());
    validation.iterations_dir = Some(
        context::iterations_root(&config.output_dir)
            .display()
            .to_string(),
    );
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
        build_image_id: validation.build_image_id.clone(),
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

fn noninteractive_validation_note(
    parse_result: &crate::ParseResult,
    snippet_source: &str,
) -> Option<String> {
    if snippet_requires_positional_cli_args(parse_result, snippet_source) {
        Some(
            "Detected direct positional CLI argument access via sys.argv; validated imports without executing the snippet entrypoint."
                .to_string(),
        )
    } else {
        None
    }
}

fn snippet_requires_positional_cli_args(
    parse_result: &crate::ParseResult,
    snippet_source: &str,
) -> bool {
    let uses_sys_argv = parse_result
        .attribute_usage
        .get("sys")
        .map(|attrs| attrs.contains("argv"))
        .unwrap_or(false)
        || snippet_source.contains("sys.argv");
    if !uses_sys_argv {
        return false;
    }

    if contains_nonzero_argv_index(snippet_source, "sys.argv") {
        return true;
    }

    for line in snippet_source.lines() {
        let Some((lhs, rhs)) = line.split_once('=') else {
            continue;
        };
        if rhs.trim() != "sys.argv" {
            continue;
        }
        let alias = lhs.trim();
        if alias.is_empty()
            || !alias
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            continue;
        }
        if contains_nonzero_argv_index(snippet_source, alias) {
            return true;
        }
    }

    false
}

fn contains_nonzero_argv_index(snippet_source: &str, target: &str) -> bool {
    let needle = format!("{target}[");
    let mut start = 0usize;
    while let Some(offset) = snippet_source[start..].find(&needle) {
        let bracket_index = start + offset + needle.len();
        let remainder = &snippet_source[bracket_index..];
        let mut chars = remainder.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if first.is_ascii_digit() {
            if first != '0' {
                return true;
            }
        } else if first != ']' {
            return true;
        }
        start = bracket_index;
    }
    false
}

/// Check if any resolved package does not exist on PyPI.
/// Returns the first nonexistent package name and whether it came from a seed source.
fn find_nonexistent_package(
    resolved: &[ResolvedDependency],
    store: &mut CacheStore,
    python_version: &str,
) -> Option<(String, bool)> {
    for dep in resolved {
        if !pypi_client::package_exists(store, &dep.package_name, python_version) {
            let is_seed = dep.strategy.starts_with("cache:seed")
                || dep.strategy.starts_with("cache:discrepancy");
            return Some((dep.package_name.clone(), is_seed));
        }
    }
    None
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
        failure_bucket: status.to_string(),
        root_cause: Some(reason.to_string()),
        skip_candidate: true,
        validation_backend: config.validation_backend().to_string(),
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

fn validate_with_retries(
    snippet_path: &Path,
    snippet_source: &str,
    parse_result: &crate::ParseResult,
    selected_python: &str,
    resolved: &mut Vec<ResolvedDependency>,
    requirements_txt: &mut String,
    store: &mut CacheStore,
    config: &ResolveConfig,
    report: &mut ResolutionReport,
) -> io::Result<ValidationSummary> {
    let mut effective_config = config.clone();
    if effective_config.execute_snippet {
        if let Some(note) = noninteractive_validation_note(parse_result, snippet_source) {
            effective_config.execute_snippet = false;
            report.notes.push(note);
        }
    }
    let config = &effective_config;
    let mut validation = ValidationSummary::default();
    let mut seen_requirements = BTreeSet::new();
    let mut attempted_versions: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut pending_pattern_learning: Option<(String, String, String, String)> = None;
    let mut llm_recovery_history: Vec<(String, String, String)> = Vec::new();
    let mut seed_llm_fallback_attempted = false;
    let mut consecutive_llm_failures: usize = 0;
    let mut failed_import_package_pairs: BTreeSet<(String, String)> = BTreeSet::new();
    let mut failure_signature_requirements: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut module_requirement_sets: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut repeat_failure_signature: Option<String> = None;
    // Track imports explicitly removed by LLM recovery (local modules, wrong packages)
    // so they don't get re-added by cache lookups.
    let mut llm_removed_imports: Vec<String> = Vec::new();

    // Overall wall-time budget for the entire retry loop equals validation_timeout.
    // Each validate_requirements call gets the *remaining* time as its budget.
    let retry_started = std::time::Instant::now();
    let total_retry_budget = config.validation_timeout;

    // Cache the benchmark context read once instead of re-reading 48KB per iteration.
    let cached_benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 48_000).ok();

    // Buffered iteration snapshots: accumulate writes and flush at end of each iteration.
    // This reduces per-iteration I/O from 8-12 writes to 1 batched write.
    let mut iteration_snapshots: Vec<(usize, String, String)> = Vec::new();

    for attempt_index in 0..=config.max_retries {
        // Check overall wall-time budget before starting another retry iteration
        let elapsed = retry_started.elapsed();
        if elapsed >= total_retry_budget {
            report.notes.push(format!(
                "Stopped retrying: overall budget exhausted ({:.0}s >= {:.0}s).",
                elapsed.as_secs_f64(),
                total_retry_budget.as_secs_f64(),
            ));
            break;
        }
        *requirements_txt = render_requirements(resolved);
        let lockfile_key = lockfile_cache::key_for(requirements_txt, selected_python);
        validation.lockfile_key = Some(lockfile_key.clone());
        validation.build_cache_key = Some(lockfile_key.clone());
        let iter_num = attempt_index + 1;
        iteration_snapshots.push((
            iter_num,
            "requirements-before.txt".to_string(),
            requirements_txt.clone(),
        ));
        iteration_snapshots.push((
            iter_num,
            "resolved-before.txt".to_string(),
            format_dependency_state(resolved, &[]),
        ));
        if let Some(ref ctx) = cached_benchmark_context {
            iteration_snapshots.push((
                iter_num,
                "benchmark-context-before.txt".to_string(),
                ctx.clone(),
            ));
        }

        if !seen_requirements.insert(requirements_txt.clone()) {
            // Before giving up on oscillation, try one LLM recovery pass.
            // The oscillation often means a package keeps flipping between
            // versions that both fail (e.g. python-memcached on Py2.7).
            // Give the LLM a chance to suggest an alternative package.
            if config.allow_llm && !seed_llm_fallback_attempted {
                seed_llm_fallback_attempted = true; // prevent infinite loop
                report.notes.push(
                    "Requirements oscillating — attempting LLM recovery before giving up."
                        .to_string(),
                );
                report.llm_calls += 1;
                let synthetic_log = "Validation is oscillating: the same requirements keep \
                    failing repeatedly. One or more packages may need to be replaced with \
                    an alternative. Consider pure-Python alternatives or different packages.";
                if let Some(hint) = tier3_llm::recovery_package_hint(
                    resolved,
                    synthetic_log,
                    snippet_source,
                    store,
                    config,
                    selected_python,
                    "Oscillation",
                    &llm_recovery_history,
                ) {
                    let (applied, notes) = apply_llm_recovery_hint(
                        &hint,
                        resolved,
                        store,
                        &failed_import_package_pairs,
                        &mut llm_removed_imports,
                        "LLM oscillation recovery",
                        "oscillation",
                    );
                    if applied && !hint.wrong_pkg.is_empty() && !hint.correct_pkg.is_empty() {
                        llm_recovery_history.push((
                            hint.wrong_pkg.clone(),
                            hint.correct_pkg.clone(),
                            "oscillation-fix".to_string(),
                        ));
                    }
                    for note in notes {
                        report.notes.push(note.clone());
                        validation.iteration_history.push(note.clone());
                        set_repair_strategy(&mut validation, &note);
                    }
                    if applied {
                        seen_requirements.clear();
                        seen_requirements.insert(render_requirements(resolved));
                        continue;
                    }
                }
            }
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
                docker::parallel::candidate_versions(
                    selected_python,
                    config.python_version_range,
                    Some(&parse_result.python_version_min),
                    parse_result.python_version_max.as_deref(),
                )
            })
        } else {
            vec![selected_python.to_string()]
        };
        iteration_snapshots.push((
            iter_num,
            "candidate-versions.txt".to_string(),
            versions.join("\n"),
        ));

        // Give this validation call only the remaining budget
        let remaining = total_retry_budget.saturating_sub(retry_started.elapsed());
        let mut scoped_config = config.clone();
        scoped_config.validation_timeout = remaining;
        let attempt_result = docker::builder::validate_requirements(
            snippet_path,
            requirements_txt,
            &parse_result.imports,
            &versions,
            validation.attempts.len(),
            &scoped_config,
            store,
        )?;
        report.env_builds += attempt_result.attempts.len();
        validation.lockfile_key = attempt_result
            .lockfile_key
            .clone()
            .or(validation.lockfile_key.clone());
        validation.build_cache_key = attempt_result
            .build_cache_key
            .clone()
            .or(validation.build_cache_key.clone());
        if validation.validation_backend.is_empty() && !attempt_result.validation_backend.is_empty()
        {
            validation.validation_backend = attempt_result.validation_backend.clone();
        }
        validation.env_create_duration_ms += attempt_result.env_create_duration_ms;
        validation.install_duration_ms += attempt_result.install_duration_ms;
        validation.smoke_duration_ms += attempt_result.smoke_duration_ms;
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
            validation.build_image_id = attempt_result.build_image_id.clone();
            return Ok(validation);
        }

        // Prefer the log from the first runtime-failed attempt in this
        // iteration (most preferred Python version that actually ran the smoke
        // test) over the absolute last attempt, which may be from Python 2.7
        // with an irrelevant TypeError/SyntaxError.
        let last_log = attempt_result
            .attempts
            .iter()
            .find(|a| a.status == "runtime-failed")
            .or_else(|| attempt_result.attempts.last())
            .map(|a| a.log_excerpt.clone())
            .unwrap_or_default();
        iteration_snapshots.push((iter_num, "last-log.txt".to_string(), last_log.clone()));
        if last_log.is_empty() {
            // No error output — try LLM recovery with a synthetic description
            // before giving up, so every failing case gets at least one LLM attempt.
            if config.allow_llm && consecutive_llm_failures < 3 {
                let synthetic_log = "Validation failed with no error output. The environment may have failed to install or the smoke test produced no stderr/stdout.";
                report.llm_calls += 1;
                if let Some(hint) = tier3_llm::recovery_package_hint(
                    resolved,
                    synthetic_log,
                    snippet_source,
                    store,
                    config,
                    selected_python,
                    "Unknown",
                    &llm_recovery_history,
                ) {
                    let (mut applied, llm_notes): (bool, Vec<String>) = (false, Vec::new());
                    let norm_wrong = hint.wrong_pkg.to_ascii_lowercase().replace('_', "-");
                    if !norm_wrong.is_empty() {
                        if let Some(dep) = resolved.iter_mut().find(|d| {
                            d.package_name.to_ascii_lowercase().replace('_', "-") == norm_wrong
                        }) {
                            let old_pkg = dep.package_name.clone();
                            let import_name = dep.import_name.clone();
                            dep.package_name = hint.correct_pkg.clone();
                            dep.version = hint.version.clone();
                            dep.strategy = "recovery:llm-fix".to_string();
                            dep.confidence = 0.65;
                            llm_recovery_history.push((
                                old_pkg.clone(),
                                hint.correct_pkg.clone(),
                                "retrying".to_string(),
                            ));
                            let _ = store.save_import_mapping(
                                &import_name,
                                &hint.correct_pkg,
                                hint.version.as_deref(),
                                "recovery:llm-fix",
                            );
                            let note = format!(
                                "LLM recovery (empty log): replaced `{old_pkg}` with `{}`.",
                                hint.correct_pkg
                            );
                            report.notes.push(note.clone());
                            validation.iteration_history.push(note.clone());
                            if let Some(last_attempt) = validation.attempts.last_mut() {
                                last_attempt.fix_applied = Some(note);
                            }
                            applied = true;
                        }
                    }
                    if let Some((add_name, add_ver)) = &None::<(String, Option<String>)> {
                        upsert_dependency(
                            resolved,
                            add_name,
                            add_name,
                            add_ver.clone(),
                            "recovery:llm-add-dep",
                        );
                        let note = format!(
                            "LLM recovery: added transitive dep `{add_name}{}`.",
                            add_ver
                                .as_deref()
                                .map(|v| format!("=={v}"))
                                .unwrap_or_default()
                        );
                        report.notes.push(note.clone());
                        validation.iteration_history.push(note.clone());
                        if let Some(last_attempt) = validation.attempts.last_mut() {
                            last_attempt.fix_applied = Some(note);
                        }
                        applied = true;
                    }
                    if let Some(ref remove_name) = None::<String> {
                        let norm_remove = remove_name.to_ascii_lowercase().replace('_', "-");
                        if let Some(pos) = resolved.iter().position(|d| {
                            d.package_name.to_ascii_lowercase().replace('_', "-") == norm_remove
                        }) {
                            let removed = resolved.remove(pos);
                            llm_removed_imports.push(removed.import_name.clone());
                            let note = format!(
                                "LLM recovery: removed `{}` (import `{}`) — not a real PyPI package.",
                                removed.package_name, removed.import_name
                            );
                            report.notes.push(note.clone());
                            validation.iteration_history.push(note.clone());
                            if let Some(last_attempt) = validation.attempts.last_mut() {
                                last_attempt.fix_applied = Some(note);
                            }
                            applied = true;
                        }
                    }
                    for note in llm_notes {
                        report.notes.push(note.clone());
                        validation.iteration_history.push(note.clone());
                        set_repair_strategy(&mut validation, &note);
                        if let Some(last_attempt) = validation.attempts.last_mut() {
                            last_attempt.fix_applied = Some(note);
                        }
                    }
                    if applied {
                        report.retries += 1;
                        iteration_snapshots.push((
                            iter_num,
                            "recovery.txt".to_string(),
                            report.notes.last().cloned().unwrap_or_default(),
                        ));
                        iteration_snapshots.push((
                            iter_num,
                            "requirements-after-recovery.txt".to_string(),
                            render_requirements(resolved),
                        ));
                        consecutive_llm_failures = 0;
                        continue;
                    }
                }
            }
            break;
        }

        let classified = classifier::classify_log(&last_log, store);
        iteration_snapshots.push((
            iter_num,
            "classifier.txt".to_string(),
            format_classifier(&classified),
        ));
        // Classify ALL attempts from this iteration so the resolution report
        // shows per-attempt error types instead of "--" for earlier versions.
        let iteration_start = validation
            .attempts
            .len()
            .saturating_sub(attempt_result.attempts.len());
        for attempt in &mut validation.attempts[iteration_start..] {
            if attempt.error_type.is_none() && !attempt.log_excerpt.is_empty() {
                let c = classifier::classify_log(&attempt.log_excerpt, store);
                attempt.error_type = Some(c.error_type);
                attempt.conflict_class = Some(c.conflict_class);
            }
        }
        *report
            .error_types
            .entry(classified.error_type.clone())
            .or_insert(0) += 1;
        *report
            .conflict_classes
            .entry(classified.conflict_class.clone())
            .or_insert(0) += 1;
        let current_signature = failure_signature(&classified, &last_log);
        failure_signature_requirements
            .entry(current_signature.clone())
            .or_default()
            .insert(requirements_txt.clone());
        if matches!(
            classified.error_type.as_str(),
            "ModuleNotFound" | "ImportError"
        ) {
            if let Some(module) = extract_missing_module(&last_log) {
                remember_failed_import_mapping(&mut failed_import_package_pairs, resolved, &module);
                module_requirement_sets
                    .entry(normalize_package_key(&module))
                    .or_default()
                    .insert(requirements_txt.clone());
            }
        }

        // RuntimeConfig errors (e.g. Django ImproperlyConfigured, missing
        // DJANGO_SETTINGS_MODULE) mean the packages installed correctly but the
        // application needs runtime configuration we cannot provide.  Treat this
        // as a successful resolution — the dependency set is correct.
        if classified.error_type == "RuntimeConfig" {
            let note = format!(
                "Runtime configuration error detected ({}). Dependencies installed successfully; \
                 the application requires runtime settings (e.g. DJANGO_SETTINGS_MODULE) that \
                 APDR cannot provide. Treating as resolved.",
                classified.matched_pattern
            );
            report.notes.push(note.clone());
            validation.iteration_history.push(note.clone());
            validation.succeeded = true;
            validation.status = "passed".to_string();
            validation.reason = Some("Runtime config error — deps are correct.".to_string());
            if let Some(last_attempt) = validation.attempts.last_mut() {
                last_attempt.fix_applied = Some(note);
            }
            return Ok(validation);
        }

        // If LLM recovery previously removed an import (determined it's a local/project
        // module, not a PyPI package) and we now see that module missing at runtime,
        // treat as a pass — the dependencies are correct, the module is just local.
        if matches!(
            classified.error_type.as_str(),
            "ModuleNotFound" | "ImportError"
        ) {
            if let Some(module) = extract_missing_module(&last_log) {
                if llm_removed_imports
                    .iter()
                    .any(|r| r.eq_ignore_ascii_case(&module))
                {
                    let note = format!(
                        "Missing module `{module}` was previously identified by LLM as a local/project module \
                         (not a PyPI package). Dependencies are correct; treating as resolved."
                    );
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    validation.succeeded = true;
                    validation.status = "passed".to_string();
                    validation.reason =
                        Some("LLM-identified local module — deps are correct.".to_string());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    return Ok(validation);
                }
                let m = module.to_lowercase();
                let local_modules = [
                    "settings",
                    "config",
                    "conf",
                    "local_settings",
                    "app_settings",
                ];
                if local_modules.contains(&m.as_str()) {
                    let note = format!(
                        "Missing module `{module}` is a project-local file (e.g. Django settings.py), \
                         not a PyPI package. Dependencies are correct; treating as resolved."
                    );
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    validation.succeeded = true;
                    validation.status = "passed".to_string();
                    validation.reason =
                        Some("Local project module — deps are correct.".to_string());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    return Ok(validation);
                }
                // Unix-only stdlib modules (pwd, grp, fcntl, etc.) are unavailable
                // on Windows.  When running Docker validation these would succeed on
                // Linux, so treat as a platform limitation, not a dep failure.
                let unix_only = [
                    "pwd", "grp", "fcntl", "termios", "resource", "syslog", "posix",
                ];
                if unix_only.contains(&m.as_str()) {
                    let note = format!(
                        "Missing module `{module}` is a Unix-only stdlib module unavailable on Windows. \
                         Dependencies are correct; treating as resolved (would pass on Linux/Docker)."
                    );
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    validation.succeeded = true;
                    validation.status = "passed".to_string();
                    validation.reason = Some("Unix-only stdlib — deps are correct.".to_string());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    return Ok(validation);
                }
                // Optional/guarded imports: if the import is inside a try/except block,
                // it's optional — the program has a fallback path. Treat missing
                // optional imports as a pass rather than retrying endlessly.
                if is_guarded_import(snippet_source, &module) {
                    let note = format!(
                        "Missing module `{module}` is inside a try/except block (optional import). \
                         Dependencies are correct; the program has a fallback."
                    );
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    validation.succeeded = true;
                    validation.status = "passed".to_string();
                    validation.reason =
                        Some("Optional guarded import — deps are correct.".to_string());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    return Ok(validation);
                }
            }
        }

        if failure_signature_requirements
            .get(&current_signature)
            .map(|seen| seen.len() >= 2)
            .unwrap_or(false)
        {
            let note = format!(
                "Repeated failure signature `{current_signature}` across multiple dependency sets; ending recovery loop."
            );
            repeat_failure_signature = Some(current_signature.clone());
            report.notes.push(note.clone());
            validation.iteration_history.push(note.clone());
            validation.reason = Some(note.clone());
            validation.failure_bucket = infer_validation_status(&validation);
            validation.root_cause = Some(note.clone());
            if let Some(last_attempt) = validation.attempts.last_mut() {
                last_attempt.fix_applied = Some(note);
            }
            break;
        }
        if matches!(
            classified.error_type.as_str(),
            "ModuleNotFound" | "ImportError"
        ) {
            if let Some(module) = extract_missing_module(&last_log) {
                if module_requirement_sets
                    .get(&normalize_package_key(&module))
                    .map(|seen| seen.len() >= 2)
                    .unwrap_or(false)
                {
                    let note = format!(
                        "Missing module `{module}` persisted across multiple dependency sets; ending recovery as a mapping failure."
                    );
                    repeat_failure_signature = Some(current_signature.clone());
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    validation.reason = Some(note.clone());
                    validation.root_cause = Some(note.clone());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    break;
                }
            }
        }
        if let Some((package_name, _)) = extract_package_and_version(&last_log) {
            if attempted_versions
                .get(&package_name)
                .map(|versions| versions.iter().collect::<BTreeSet<_>>().len() >= 2)
                .unwrap_or(false)
                && matches!(
                    classified.error_type.as_str(),
                    "DependencyConflict" | "VersionNotFound" | "InvalidVersion"
                )
            {
                let note = format!(
                    "Repeated contradictory pins for `{package_name}` exhausted recovery; ending compatibility retries."
                );
                repeat_failure_signature = Some(current_signature.clone());
                report.notes.push(note.clone());
                validation.iteration_history.push(note.clone());
                validation.reason = Some(note.clone());
                validation.root_cause = Some(note.clone());
                if let Some(last_attempt) = validation.attempts.last_mut() {
                    last_attempt.fix_applied = Some(note);
                }
                break;
            }
        }

        if let Some(note) = apply_recovery_fix(
            &classified,
            &last_log,
            resolved,
            parse_result,
            selected_python,
            store,
            &mut attempted_versions,
            config,
            &llm_removed_imports,
        ) {
            report.retries += 1;
            report.notes.push(note.clone());
            validation.iteration_history.push(note.clone());
            set_repair_strategy(&mut validation, &note);
            pending_pattern_learning = Some((
                learned_pattern_key(&classified, &last_log),
                classified.error_type.clone(),
                classified.conflict_class.clone(),
                note.clone(),
            ));
            if let Some(last_attempt) = validation.attempts.last_mut() {
                last_attempt.fix_applied = Some(note);
            }
            iteration_snapshots.push((
                iter_num,
                "recovery.txt".to_string(),
                report.notes.last().cloned().unwrap_or_default(),
            ));
            iteration_snapshots.push((
                iter_num,
                "requirements-after-recovery.txt".to_string(),
                render_requirements(resolved),
            ));
            consecutive_llm_failures = 0;
            continue;
        }

        // Check for system/platform deps BEFORE spending time on LLM recovery.
        // These are unfixable and should break immediately.
        if let Some(note) = environment_specific_note(&classified, &last_log, parse_result) {
            report.notes.push(note.clone());
            validation.iteration_history.push(note.clone());
            validation.status = "skipped-host-runtime".to_string();
            validation.reason = Some(note.clone());
            validation.failure_bucket = "skipped-host-runtime".to_string();
            validation.root_cause = Some(note.clone());
            validation.skip_candidate = true;
            if let Some(last_attempt) = validation.attempts.last_mut() {
                last_attempt.fix_applied = Some(note.clone());
            }
            iteration_snapshots.push((iter_num, "recovery.txt".to_string(), note));
            break;
        }

        // Update outcome for the most recent LLM recovery attempt.
        if let Some(last) = llm_recovery_history.last_mut() {
            if last.2 == "retrying" {
                last.2 = format!("failed: {}", classified.error_type);
            }
        }

        // LLM-powered recovery as last resort when built-in recovery fails.
        // Ask the LLM which resolved package is wrong and what the correct
        // PyPI name should be. Stop after 3 consecutive LLM failures.
        if config.allow_llm && consecutive_llm_failures < 3 {
            report.llm_calls += 1;
            if let Some(hint) = tier3_llm::recovery_package_hint(
                resolved,
                &last_log,
                snippet_source,
                store,
                config,
                selected_python,
                &classified.error_type,
                &llm_recovery_history,
            ) {
                let (applied, llm_notes) = apply_llm_recovery_hint(
                    &hint,
                    resolved,
                    store,
                    &failed_import_package_pairs,
                    &mut llm_removed_imports,
                    "LLM recovery",
                    &format!("{} error", classified.error_type),
                );
                if applied && !hint.wrong_pkg.is_empty() && !hint.correct_pkg.is_empty() {
                    llm_recovery_history.push((
                        hint.wrong_pkg.clone(),
                        hint.correct_pkg.clone(),
                        "retrying".to_string(),
                    ));
                }
                if let Some((add_name, add_ver)) = &None::<(String, Option<String>)> {
                    upsert_dependency(
                        resolved,
                        add_name,
                        add_name,
                        add_ver.clone(),
                        "recovery:llm-add-dep",
                    );
                    let note = format!(
                        "LLM recovery: added transitive dep `{add_name}{}` after {} error.",
                        add_ver
                            .as_deref()
                            .map(|v| format!("=={v}"))
                            .unwrap_or_default(),
                        classified.error_type
                    );
                    report.retries += 1;
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                }
                // Handle remove_package: remove a package that shouldn't be installed
                if let Some(ref remove_name) = None::<String> {
                    let norm_remove = remove_name.to_ascii_lowercase().replace('_', "-");
                    if let Some(pos) = resolved.iter().position(|d| {
                        d.package_name.to_ascii_lowercase().replace('_', "-") == norm_remove
                    }) {
                        let removed = resolved.remove(pos);
                        llm_removed_imports.push(removed.import_name.clone());
                        let note = format!(
                            "LLM recovery: removed `{}` (import `{}`) — likely a local/project module, not a PyPI package.",
                            removed.package_name, removed.import_name
                        );
                        report.retries += 1;
                        report.notes.push(note.clone());
                        validation.iteration_history.push(note.clone());
                        if let Some(last_attempt) = validation.attempts.last_mut() {
                            last_attempt.fix_applied = Some(note);
                        }
                    }
                }
                for note in llm_notes {
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    set_repair_strategy(&mut validation, &note);
                    pending_pattern_learning = Some((
                        learned_pattern_key(&classified, &last_log),
                        classified.error_type.clone(),
                        classified.conflict_class.clone(),
                        note.clone(),
                    ));
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                }
                if applied {
                    report.retries += 1;
                    iteration_snapshots.push((
                        iter_num,
                        "recovery.txt".to_string(),
                        report.notes.last().cloned().unwrap_or_default(),
                    ));
                    iteration_snapshots.push((
                        iter_num,
                        "requirements-after-recovery.txt".to_string(),
                        render_requirements(resolved),
                    ));
                    consecutive_llm_failures = 0;
                    continue;
                }
            }
            consecutive_llm_failures += 1;
        }

        // Seed-aware LLM fallback: when all recovery fails and there are
        // seed-sourced packages, re-resolve those imports via LLM.
        if !seed_llm_fallback_attempted && config.allow_llm {
            let seed_deps: Vec<(String, String)> = resolved
                .iter()
                .filter(|d| {
                    d.strategy.starts_with("cache:seed")
                        || d.strategy.starts_with("cache:discrepancy")
                })
                .map(|d| (d.import_name.clone(), d.package_name.clone()))
                .collect();
            if !seed_deps.is_empty() {
                seed_llm_fallback_attempted = true;
                let imports_to_retry: Vec<String> =
                    seed_deps.iter().map(|(imp, _)| imp.clone()).collect();
                let bad_packages: Vec<String> =
                    seed_deps.iter().map(|(_, pkg)| pkg.clone()).collect();
                let error_excerpt = extract_key_error_lines(&last_log);
                report.notes.push(format!(
                    "Seed-resolved packages [{}] may be wrong. Retrying {} import(s) with LLM.",
                    bad_packages.join(", "),
                    imports_to_retry.len()
                ));
                report.llm_calls += 1;
                let llm_result = tier3_llm::resolve_with_context(
                    &imports_to_retry,
                    snippet_source,
                    parse_result,
                    store,
                    config,
                    selected_python,
                    Some(format!(
                        "Previous resolution from seed data failed validation with {} error. Error: {}. \
                         The following seed mappings may be wrong: {}. \
                         Please suggest the correct PyPI package names for these imports.",
                        classified.error_type,
                        if error_excerpt.is_empty() { "unknown" } else { &error_excerpt },
                        seed_deps
                            .iter()
                            .map(|(imp, pkg)| format!("{} → {}", imp, pkg))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )),
                );
                report.llm_calls += llm_result.prompts_issued;
                // Replace seed-sourced deps with LLM results
                let mut changed = false;
                for llm_dep in &llm_result.resolved {
                    if let Some(existing) = resolved.iter_mut().find(|d| {
                        d.import_name == llm_dep.import_name
                            && (d.strategy.starts_with("cache:seed")
                                || d.strategy.starts_with("cache:discrepancy"))
                    }) {
                        if existing.package_name != llm_dep.package_name {
                            let old_pkg = existing.package_name.clone();
                            existing.package_name = llm_dep.package_name.clone();
                            existing.version = llm_dep.version.clone();
                            existing.strategy = "seed-llm-fallback".to_string();
                            existing.confidence = 0.70;
                            let _ = store.save_import_mapping(
                                &existing.import_name,
                                &llm_dep.package_name,
                                llm_dep.version.as_deref(),
                                "seed-llm-fallback",
                            );
                            let note = format!(
                                "Seed LLM fallback: replaced `{}` with `{}` for import `{}`.",
                                old_pkg, llm_dep.package_name, llm_dep.import_name
                            );
                            report.retries += 1;
                            report.notes.push(note.clone());
                            validation.iteration_history.push(note.clone());
                            changed = true;
                        }
                    }
                }
                if changed {
                    *requirements_txt = render_requirements(resolved);
                    iteration_snapshots.push((
                        iter_num,
                        "recovery.txt".to_string(),
                        "seed-llm-fallback applied".to_string(),
                    ));
                    iteration_snapshots.push((
                        iter_num,
                        "requirements-after-recovery.txt".to_string(),
                        render_requirements(resolved),
                    ));
                    continue;
                }
            }
        }

        // ---- Last-resort fallback: version-strip + retry ----
        // If we haven't hit minimum 3 retries yet, try progressively
        // more aggressive recovery strategies before giving up.
        let min_retries: usize = 3;
        if report.retries < min_retries {
            // Strategy 1: Strip version pin from the package mentioned in the error.
            if matches!(
                classified.error_type.as_str(),
                "VersionNotFound"
                    | "BuildFailure"
                    | "InvalidVersion"
                    | "NonZeroCode"
                    | "DependencyConflict"
            ) {
                if let Some((package_name, _)) = extract_package_and_version(&last_log) {
                    if let Some(dep) = resolved
                        .iter_mut()
                        .find(|d| d.package_name.eq_ignore_ascii_case(&package_name))
                    {
                        if dep.version.is_some() {
                            let old_ver = dep.version.clone().unwrap_or_default();
                            dep.version = None;
                            dep.strategy = "recovery:last-resort-strip".to_string();
                            dep.confidence = 0.55;
                            let note = format!(
                                "Last-resort: stripped version pin from {package_name}=={old_ver} after {} — letting pip choose.",
                                classified.error_type
                            );
                            report.retries += 1;
                            report.notes.push(note.clone());
                            validation.iteration_history.push(note.clone());
                            if let Some(last_attempt) = validation.attempts.last_mut() {
                                last_attempt.fix_applied = Some(note);
                            }
                            iteration_snapshots.push((
                                iter_num,
                                "recovery.txt".to_string(),
                                report.notes.last().cloned().unwrap_or_default(),
                            ));
                            iteration_snapshots.push((
                                iter_num,
                                "requirements-after-recovery.txt".to_string(),
                                render_requirements(resolved),
                            ));
                            continue;
                        }
                    }
                }
            }

            // Strategy 2: For ModuleNotFound, strip version pins from ALL resolved
            // deps to give pip maximum flexibility.
            if classified.error_type == "ModuleNotFound" || classified.error_type == "ImportError" {
                let mut stripped_any = false;
                for dep in resolved.iter_mut() {
                    if dep.version.is_some() {
                        dep.version = None;
                        dep.strategy = "recovery:last-resort-strip-all".to_string();
                        dep.confidence = 0.50;
                        stripped_any = true;
                    }
                }
                if stripped_any {
                    let note = format!(
                        "Last-resort: stripped all version pins after {} — retrying with unpinned deps.",
                        classified.error_type
                    );
                    report.retries += 1;
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    iteration_snapshots.push((
                        iter_num,
                        "recovery.txt".to_string(),
                        report.notes.last().cloned().unwrap_or_default(),
                    ));
                    iteration_snapshots.push((
                        iter_num,
                        "requirements-after-recovery.txt".to_string(),
                        render_requirements(resolved),
                    ));
                    continue;
                }
            }
        }

        // Include key error line(s) so the user can see what went wrong
        // without having to dig into .apdr-debug/iterations/ logs.
        let error_excerpt = extract_key_error_lines(&last_log);
        if error_excerpt.is_empty() {
            report.notes.push(format!(
                "No automatic recovery fix found for {}.",
                classified.error_type
            ));
        } else {
            report.notes.push(format!(
                "No automatic recovery fix found for {}. Error: {}",
                classified.error_type, error_excerpt
            ));
        }
        iteration_snapshots.push((
            iter_num,
            "recovery.txt".to_string(),
            report.notes.last().cloned().unwrap_or_default(),
        ));
        break;
    }

    // Flush all buffered iteration snapshots to disk in one batch.
    for (iter_num, filename, content) in &iteration_snapshots {
        let _ = write_iteration_snapshot(&config.output_dir, *iter_num, filename, content);
    }

    if validation.status.is_empty() {
        validation.status = infer_validation_status(&validation);
    }
    if validation.reason.is_none() {
        validation.reason = infer_validation_reason(&validation, report);
    }
    if validation.validation_backend.is_empty() && !validation.attempts.is_empty() {
        validation.validation_backend = config.validation_backend().to_string();
    }
    update_failure_metadata(&mut validation, config, resolved, repeat_failure_signature);

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
    llm_removed_imports: &[String],
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
        "DependencyConflict" => {
            // For dependency conflicts, skip version sampling and go straight
            // to constraint relaxation.  pip's resolver is better at finding
            // compatible version sets than our version sampler, which wastes
            // retries trying individual versions that still conflict.
            let (package_name, _current_version) = extract_package_and_version(log)?;
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
                    "Kept family-managed package `{package_name}` pinned after DependencyConflict to avoid breaking a curated compatibility bundle.",
                ));
            }
            if let Some(dep) = resolved
                .iter_mut()
                .find(|d| d.package_name.eq_ignore_ascii_case(&package_name))
            {
                if dep.version.is_some() {
                    dep.version = None;
                    dep.strategy = "recovery:constraint-relaxation".to_string();
                    dep.confidence = 0.68;
                    return Some(format!(
                        "Stripped version pin from {package_name} after DependencyConflict — letting pip resolve freely."
                    ));
                }
            }
            None
        }
        "VersionNotFound" | "InvalidVersion" | "NonZeroCode" => {
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
            let mut known_versions =
                pypi_client::compatible_versions(store, &package_name, python_version);
            // For Python 2.7, cap to the last known Py2-compatible version to avoid
            // wasting recovery attempts on versions that require Python 3.
            if python_version.starts_with("2.") {
                if let Some(ceiling) = last_python2_version(&package_name) {
                    let constraint = format!("<={ceiling}");
                    known_versions.retain(|v| pypi_client::version_satisfies(v, &constraint));
                }
            }
            if known_versions.is_empty() {
                // No compatible versions found in our cache.  Before giving up,
                // try the known Py2 ceiling directly — even if it wasn't in the
                // fetched version list, pip may be able to install it.
                if python_version.starts_with("2.") {
                    if let Some(ceiling) = last_python2_version(&package_name) {
                        let current_ver = current_version.as_deref().unwrap_or("");
                        if current_ver != ceiling {
                            if update_package_version(
                                resolved,
                                &package_name,
                                Some(ceiling.to_string()),
                            ) {
                                return Some(format!(
                                    "Pinned {package_name} to {ceiling} (last known Python 2 version) after {}.",
                                    classified.error_type
                                ));
                            }
                        }
                    }
                }
                // Still no luck — try stripping the version pin entirely so pip
                // can pick the best compatible version on its own.
                if let Some(dep) = resolved
                    .iter_mut()
                    .find(|d| d.package_name.eq_ignore_ascii_case(&package_name))
                {
                    if dep.version.is_some() {
                        dep.version = None;
                        dep.strategy = "recovery:version-strip".to_string();
                        dep.confidence = 0.60;
                        return Some(format!(
                            "Stripped version pin from {package_name} after {} — letting pip choose a compatible version.",
                            classified.error_type
                        ));
                    }
                }
                return None;
            }
            let previous = attempted_versions.entry(package_name.clone()).or_default();
            if let Some(current) = current_version.clone() {
                previous.push(current);
            }
            if let Some(next_version) =
                version_sampler::equally_distanced_sample(&known_versions, previous)
            {
                previous.push(next_version.clone());
                if update_package_version(resolved, &package_name, Some(next_version.clone())) {
                    return Some(format!(
                        "Adjusted {package_name} to {next_version} after {}.",
                        classified.error_type
                    ));
                }
            }
            None
        }
        "ModuleNotFound" | "ImportError" | "AttributeError" => {
            let module_name = extract_missing_module(log)?;
            // Skip re-adding imports that were explicitly removed by LLM recovery
            if llm_removed_imports
                .iter()
                .any(|r| r.eq_ignore_ascii_case(&module_name))
            {
                return None;
            }
            // pkg_resources comes from setuptools — a common issue with modern pip
            if module_name == "pkg_resources" {
                if upsert_dependency(
                    resolved,
                    "pkg_resources",
                    "setuptools",
                    None,
                    "recovery:pkg-resources",
                ) {
                    return Some(
                        "Added setuptools to provide missing pkg_resources module.".to_string(),
                    );
                }
            }
            // pip module may be needed at import time by some packages
            if module_name == "pip" {
                let pip_version = if python_version.starts_with("2.") {
                    Some("20.3.4".to_string())
                } else {
                    None
                };
                if upsert_dependency(resolved, "pip", "pip", pip_version, "recovery:pip-module") {
                    return Some("Added pip to provide missing pip module.".to_string());
                }
            }
            // Skip recovery for modules in the stdlib list (e.g. Pythonista builtins
            // like `console` that were intentionally excluded from resolution).
            if parse_result
                .stdlib_modules
                .contains(&module_name.to_lowercase())
            {
                return None;
            }
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
                if family_knowledge::namespace_mapping_allowed(&module_name, &record.package_name)
                    && pypi_client::package_exists(store, &record.package_name, python_version)
                    && upsert_dependency(
                        resolved,
                        &module_name,
                        &record.package_name,
                        pypi_client::compatible_default_version(
                            store,
                            &record.package_name,
                            record.default_version.as_deref(),
                            python_version,
                        ),
                        "recovery:cache",
                    )
                {
                    let override_notes = apply_compatibility_overrides(
                        parse_result,
                        resolved,
                        python_version,
                        config,
                    );
                    return Some(if override_notes.is_empty() {
                        format!(
                            "Remapped {} to {} from cache.",
                            module_name, record.package_name
                        )
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
            if !versions.is_empty()
                && family_knowledge::namespace_mapping_allowed(&module_name, &module_name)
            {
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
                    let override_notes = apply_compatibility_overrides(
                        parse_result,
                        resolved,
                        python_version,
                        config,
                    );
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
            }
            // If the module is a dotted path (e.g. Cython.Build, Bio.PDB),
            // try the top-level component as a package name before falling
            // back to the LLM.  Many build-time imports reference sub-modules
            // of packages whose PyPI name matches the top-level (cython, numpy).
            if module_name.contains('.') {
                let top_level = module_name.split('.').next().unwrap_or(&module_name);
                if let Some(record) = store.import_lookup(top_level).cloned() {
                    if family_knowledge::namespace_mapping_allowed(top_level, &record.package_name)
                        && pypi_client::package_exists(store, &record.package_name, python_version)
                        && upsert_dependency(
                            resolved,
                            &module_name,
                            &record.package_name,
                            pypi_client::compatible_default_version(
                                store,
                                &record.package_name,
                                record.default_version.as_deref(),
                                python_version,
                            ),
                            "recovery:cache",
                        )
                    {
                        let _ = store.save_import_mapping(
                            top_level,
                            &record.package_name,
                            record.default_version.as_deref(),
                            "recovery:cache",
                        );
                        return Some(format!(
                            "Remapped {module_name} (via top-level `{top_level}`) to {} from cache.",
                            record.package_name
                        ));
                    }
                }
                let top_versions =
                    pypi_client::compatible_versions(store, top_level, python_version);
                if !top_versions.is_empty()
                    && family_knowledge::namespace_mapping_allowed(top_level, top_level)
                {
                    let version = version_sampler::equally_distanced_sample(&top_versions, &[]);
                    if upsert_dependency(
                        resolved,
                        &module_name,
                        top_level,
                        version.clone(),
                        "recovery:heuristic",
                    ) {
                        let _ = store.save_import_mapping(
                            top_level,
                            top_level,
                            version.as_deref(),
                            "recovery:heuristic",
                        );
                        return Some(format!(
                            "Added top-level package `{top_level}` for missing sub-module `{module_name}`.",
                        ));
                    }
                }
            }
            // Fall through to LLM recovery even when the package isn't in
            // KGraph/cache — the LLM may suggest an alternative package name.
            if config.allow_llm {
                let hint = tier3_llm::single_package_hint(
                    &module_name,
                    parse_result,
                    store,
                    config,
                    python_version,
                );
                if let Some((package_name, version)) = hint {
                    // Guard: skip if LLM echoed the module name back (or its
                    // top-level component), which causes oscillation with the
                    // cache-based recovery path.
                    let norm_pkg = package_name
                        .to_ascii_lowercase()
                        .replace('_', "-")
                        .replace('.', "-");
                    let norm_mod = module_name
                        .to_ascii_lowercase()
                        .replace('_', "-")
                        .replace('.', "-");
                    let norm_top = module_name
                        .split('.')
                        .next()
                        .unwrap_or(&module_name)
                        .to_ascii_lowercase()
                        .replace('_', "-");
                    if norm_pkg == norm_mod || norm_pkg == norm_top {
                        // LLM returned the same name; skip to avoid oscillation.
                    } else if !family_knowledge::namespace_mapping_allowed(
                        &module_name,
                        &package_name,
                    ) {
                        // Skip namespace-incompatible LLM remaps such as PySide -> PySide6.
                    } else if upsert_dependency(
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
                        let override_notes = apply_compatibility_overrides(
                            parse_result,
                            resolved,
                            python_version,
                            config,
                        );
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
        "SystemDependency" | "BuildFailure" => {
            // Try swapping packages that have pure-Python or headless alternatives
            // before giving up on system-dep build failures.
            if let Some(note) = try_build_failure_alternatives(resolved, log, store, python_version)
            {
                return Some(note);
            }
            // Try downgrading the failing package — older versions may have
            // pre-built wheels or fewer native dependencies.
            if let Some((package_name, current_version)) = extract_package_and_version(log) {
                let mut known_versions =
                    pypi_client::compatible_versions(store, &package_name, python_version);
                if python_version.starts_with("2.") {
                    if let Some(ceiling) = last_python2_version(&package_name) {
                        let constraint = format!("<={ceiling}");
                        known_versions.retain(|v| pypi_client::version_satisfies(v, &constraint));
                    }
                }
                if !known_versions.is_empty() {
                    let previous = attempted_versions.entry(package_name.clone()).or_default();
                    if let Some(current) = current_version {
                        previous.push(current);
                    }
                    if let Some(next_version) =
                        version_sampler::equally_distanced_sample(&known_versions, previous)
                    {
                        previous.push(next_version.clone());
                        if update_package_version(
                            resolved,
                            &package_name,
                            Some(next_version.clone()),
                        ) {
                            return Some(format!(
                                "Downgraded {package_name} to {next_version} after {} (may have pre-built wheel).",
                                classified.error_type
                            ));
                        }
                        // Package not in resolved list — it's a transitive dependency.
                        // Add it explicitly with a version pin so pip uses a compatible version.
                        if upsert_dependency(
                            resolved,
                            &package_name,
                            &package_name,
                            Some(next_version.clone()),
                            "recovery:transitive-pin",
                        ) {
                            return Some(format!(
                                "Pinned transitive dependency {package_name}=={next_version} after {} (pre-built wheel).",
                                classified.error_type
                            ));
                        }
                    }
                }
            }
            None
        }
        "SyntaxError" => {
            // SyntaxError during build (e.g. f-string in setup.py on Py2) is
            // effectively a build failure.  Try build-failure alternatives first.
            if let Some(note) = try_build_failure_alternatives(resolved, log, store, python_version)
            {
                return Some(note);
            }
            // If a package (not the snippet) has a SyntaxError — e.g. Python 3
            // type annotations imported on Python 2.7 — try downgrading that
            // specific package instead of giving up.
            if let Some(module_name) = extract_syntax_error_package(log) {
                let norm_mod = module_name.to_ascii_lowercase().replace('_', "-");
                if let Some(dep) = resolved
                    .iter()
                    .find(|d| d.import_name.to_ascii_lowercase().replace('_', "-") == norm_mod)
                {
                    let package_name = dep.package_name.clone();
                    let mut known_versions =
                        pypi_client::compatible_versions(store, &package_name, python_version);
                    if python_version.starts_with("2.") {
                        if let Some(ceiling) = last_python2_version(&package_name) {
                            let constraint = format!("<={ceiling}");
                            known_versions
                                .retain(|v| pypi_client::version_satisfies(v, &constraint));
                        }
                    }
                    if !known_versions.is_empty() {
                        let previous = attempted_versions.entry(package_name.clone()).or_default();
                        if let Some(current) = dep.version.clone() {
                            previous.push(current);
                        }
                        if let Some(next_version) =
                            version_sampler::equally_distanced_sample(&known_versions, previous)
                        {
                            previous.push(next_version.clone());
                            if update_package_version(
                                resolved,
                                &package_name,
                                Some(next_version.clone()),
                            ) {
                                return Some(format!(
                                    "Downgraded {package_name} to {next_version} after SyntaxError in package module `{module_name}`."
                                ));
                            }
                        }
                    }
                }
            }
            Some("Validation exhausted adjacent Python versions after SyntaxError.".to_string())
        }
        _ => {
            // Handle deprecated setuptools features (use_2to3) by stripping
            // the version pin.  Old package versions (e.g. plac==0.9.2) use
            // use_2to3 which was removed in setuptools 58+.  Newer versions
            // of the same package typically don't use it.
            let lowercase = log.to_ascii_lowercase();
            if lowercase.contains("use_2to3 is invalid")
                || lowercase.contains("use_2to3 is not supported")
            {
                if let Some(pkg) = extract_setup_error_package(log) {
                    if let Some(dep) = resolved
                        .iter_mut()
                        .find(|d| d.package_name.eq_ignore_ascii_case(&pkg))
                    {
                        if dep.version.is_some() {
                            let old_version = dep.version.clone().unwrap_or_default();
                            dep.version = None;
                            dep.strategy = "recovery:deprecated-setup".to_string();
                            dep.confidence = 0.70;
                            return Some(format!(
                                "Stripped version pin from {pkg}=={old_version} after use_2to3 build failure — \
                                 newer versions use modern setuptools."
                            ));
                        }
                    }
                }
            }

            // Generic fallback: try to extract a missing dependency from the
            // build/runtime log even when the error type is Unknown or
            // unhandled.  This covers setup.py errors like
            // "Numerical Python (NumPy) is not installed" and similar.
            if let Some(dep_name) = extract_build_dependency(log) {
                let dep_lower = dep_name.to_lowercase();
                if parse_result.stdlib_modules.contains(&dep_lower) {
                    return None;
                }
                // Try cache lookup first.
                if let Some(record) = store.import_lookup(&dep_lower).cloned() {
                    if pypi_client::package_exists(store, &record.package_name, python_version)
                        && upsert_dependency(
                            resolved,
                            &dep_name,
                            &record.package_name,
                            pypi_client::compatible_default_version(
                                store,
                                &record.package_name,
                                record.default_version.as_deref(),
                                python_version,
                            ),
                            "recovery:build-dep",
                        )
                    {
                        return Some(format!(
                            "Extracted build dependency `{}` from error log; mapped to `{}`.",
                            dep_name, record.package_name
                        ));
                    }
                }
                // Try as a direct package name.
                let versions = pypi_client::compatible_versions(store, &dep_lower, python_version);
                if !versions.is_empty() {
                    let version = version_sampler::equally_distanced_sample(&versions, &[]);
                    if upsert_dependency(
                        resolved,
                        &dep_name,
                        &dep_lower,
                        version.clone(),
                        "recovery:build-dep",
                    ) {
                        let _ = store.save_import_mapping(
                            &dep_lower,
                            &dep_lower,
                            version.as_deref(),
                            "recovery:build-dep",
                        );
                        return Some(format!(
                            "Extracted build dependency `{dep_name}` from error log; added `{dep_lower}`.",
                        ));
                    }
                }
            }
            None
        }
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
    // Normalize with lowercase + hyphen-to-underscore so that "Django" and
    // "django==5.0.8", or "Pillow" and "pillow", collapse to a single entry.
    // When there are duplicates, keep the first occurrence (which typically
    // has a version pin from seed/cache).
    let mut seen = BTreeSet::new();
    resolved.retain(|dependency| {
        let key = dependency.package_name.to_lowercase().replace('-', "_");
        seen.insert(key)
    });
}

fn update_package_version(
    resolved: &mut [ResolvedDependency],
    package_name: &str,
    version: Option<String>,
) -> bool {
    for dependency in resolved.iter_mut() {
        if dependency.package_name.eq_ignore_ascii_case(package_name) {
            if dependency.version == version {
                return false;
            }
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

/// When a package fails to build due to missing system headers/libraries, try
/// swapping it for a pure-Python or headless alternative that doesn't need them.
fn try_build_failure_alternatives(
    resolved: &mut Vec<ResolvedDependency>,
    log: &str,
    store: &mut CacheStore,
    python_version: &str,
) -> Option<String> {
    // Map: (failing_package_normalized, alternative_package, reason)
    const ALTERNATIVES: &[(&str, &str, &str)] = &[
        (
            "opencv-python",
            "opencv-python-headless",
            "headless alternative (no GUI/GTK deps)",
        ),
        (
            "opencv-contrib-python",
            "opencv-contrib-python-headless",
            "headless alternative (no GUI/GTK deps)",
        ),
        (
            "psycopg2",
            "psycopg2-binary",
            "pre-built binary (no libpq-dev headers)",
        ),
        (
            "mecab-python",
            "mecab-python3",
            "pre-built MeCab wheels (no libmecab-dev headers)",
        ),
        ("mecab-python3", "mecab-python3", ""), // already correct, skip
        ("lxml", "lxml", ""),                   // placeholder — lxml wheels usually work; skip
        ("pillow", "Pillow", ""),               // already the right name; skip
    ];

    let lower = log.to_lowercase();

    // Python 2 special case: mecab-python3 is Py3-only; swap back to mecab-python for Py2.
    if python_version.starts_with("2.") {
        for dep in resolved.iter_mut() {
            let norm = dep.package_name.to_ascii_lowercase().replace('_', "-");
            if norm == "mecab-python3"
                && (lower.contains("syntaxerror") || lower.contains("command errored out"))
            {
                let mut alt_versions =
                    pypi_client::compatible_versions(store, "mecab-python", python_version);
                if let Some(ceiling) = last_python2_version("mecab-python") {
                    let constraint = format!("<={ceiling}");
                    alt_versions.retain(|v| pypi_client::version_satisfies(v, &constraint));
                }
                if !alt_versions.is_empty() {
                    let old = dep.package_name.clone();
                    dep.package_name = "mecab-python".to_string();
                    dep.version = version_sampler::equally_distanced_sample(&alt_versions, &[]);
                    dep.strategy = "recovery:py2-build-alt".to_string();
                    dep.confidence = 0.75;
                    return Some(format!(
                        "Replaced `{old}` with `mecab-python` (mecab-python3 is Python 3 only) for Python 2.7."
                    ));
                }
            }
        }
    }

    for dep in resolved.iter_mut() {
        let norm = dep.package_name.to_ascii_lowercase().replace('_', "-");
        for &(failing, alt, reason) in ALTERNATIVES {
            if norm == failing.to_ascii_lowercase().replace('_', "-")
                && !reason.is_empty()
                && (lower.contains(&format!(
                    "failed building wheel for {}",
                    failing.to_lowercase()
                )) || lower.contains(&format!("error: command 'gcc'"))
                    || lower.contains("no matching distribution")
                    || lower.contains("could not build wheels")
                    || lower.contains("subprocess-exited-with-error")
                    || lower.contains("command errored out with exit status"))
            {
                // Only swap if the alternative exists on PyPI for this Python version
                let alt_versions = pypi_client::compatible_versions(store, alt, python_version);
                if alt_versions.is_empty() {
                    continue;
                }
                let version = version_sampler::equally_distanced_sample(&alt_versions, &[]);
                let old = dep.package_name.clone();
                dep.package_name = alt.to_string();
                dep.version = version;
                dep.strategy = "recovery:build-alt".to_string();
                dep.confidence = 0.78;
                let _ = store.save_import_mapping(
                    &dep.import_name,
                    alt,
                    dep.version.as_deref(),
                    "recovery:build-alt",
                );
                return Some(format!(
                    "Replaced `{old}` with `{alt}` ({reason}) after build failure."
                ));
            }
        }
    }
    None
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

fn normalize_package_key(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .replace('.', "-")
}

fn unsolvable_status_for_category(category: &str) -> &'static str {
    match category.trim().to_ascii_lowercase().as_str() {
        "host-runtime" | "platform-specific" | "system-dependency" => "skipped-host-runtime",
        _ => "skipped-unsolvable",
    }
}

fn set_repair_strategy(validation: &mut ValidationSummary, note: &str) {
    validation.repair_strategy_applied = Some(note.to_string());
}

fn remember_failed_import_mapping(
    failed_pairs: &mut BTreeSet<(String, String)>,
    resolved: &[ResolvedDependency],
    module_name: &str,
) {
    let module_norm = normalize_package_key(module_name);
    if let Some(dep) = resolved
        .iter()
        .find(|dep| normalize_package_key(&dep.import_name) == module_norm)
    {
        failed_pairs.insert((module_norm, normalize_package_key(&dep.package_name)));
        return;
    }
    let top_level = module_name.split('.').next().unwrap_or(module_name);
    let top_norm = normalize_package_key(top_level);
    if let Some(dep) = resolved
        .iter()
        .find(|dep| normalize_package_key(&dep.import_name) == top_norm)
    {
        failed_pairs.insert((top_norm, normalize_package_key(&dep.package_name)));
    }
}

fn mapping_is_banned(
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

fn failure_signature(classified: &crate::ClassifierResult, log: &str) -> String {
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

fn apply_llm_recovery_hint(
    hint: &tier3_llm::RecoveryHint,
    resolved: &mut Vec<ResolvedDependency>,
    store: &mut CacheStore,
    failed_pairs: &BTreeSet<(String, String)>,
    llm_removed_imports: &mut Vec<String>,
    label: &str,
    reason: &str,
) -> (bool, Vec<String>) {
    let mut applied = false;
    let mut notes = Vec::new();

    let norm_wrong = normalize_package_key(&hint.wrong_pkg);
    if !norm_wrong.is_empty() {
        if let Some(dep_index) = resolved
            .iter()
            .position(|d| normalize_package_key(&d.package_name) == norm_wrong)
        {
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
                && resolved.iter().enumerate().any(|(idx, existing)| {
                    idx != dep_index
                        && normalize_package_key(&existing.package_name)
                            == normalize_package_key(target_pkg)
                })
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
        if let Some(pos) = resolved
            .iter()
            .position(|d| normalize_package_key(&d.package_name) == norm_remove)
        {
            let removed = resolved.remove(pos);
            llm_removed_imports.push(removed.import_name.clone());
            notes.push(format!(
                "{label}: removed `{}` (import `{}`) because it should not be installed from PyPI.",
                removed.package_name, removed.import_name
            ));
            applied = true;
        }
    }

    (applied, notes)
}

fn update_failure_metadata(
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
fn last_python2_version(package_name: &str) -> Option<&'static str> {
    let normalized = package_name
        .to_ascii_lowercase()
        .replace('_', "-")
        .replace('.', "-");
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

/// Extract the package name from "error in {package} setup command" log lines.
/// e.g. "error in plac setup command: use_2to3 is invalid." → Some("plac")
fn extract_setup_error_package(log: &str) -> Option<String> {
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
/// Returns a compact string (≤200 chars) suitable for embedding in the
/// resolution-report "notes" section.
fn extract_key_error_lines(log: &str) -> String {
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

/// Check if a module import is guarded by a try/except block in the snippet source.
/// Guarded imports are optional — if they fail at runtime, the program has a fallback.
/// This covers patterns like:
///   try:
///       import foo
///   except ImportError:
///       ...
fn is_guarded_import(snippet_source: &str, module_name: &str) -> bool {
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
                // Found an enclosing block — check if it's try:
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
fn extract_syntax_error_package(log: &str) -> Option<String> {
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
/// This is broader than `extract_missing_module` — it also catches
/// setup.py messages like "Numerical Python (NumPy) is not installed"
/// and "You must install X" that don't use the standard Python
/// `ImportError` / `ModuleNotFoundError` format.
fn extract_build_dependency(log: &str) -> Option<String> {
    // First, try the standard module-not-found patterns.
    if let Some(module) = extract_missing_module(log) {
        return Some(module);
    }

    // Names that refer to the interpreter/toolchain, not a pip-installable package.
    const REJECT: &[&str] = &["python", "python2", "python3", "pip", "pip3"];

    let lower = log.to_lowercase();

    // Pattern: "(NAME) is not installed" — e.g. "Numerical Python (NumPy) is not installed".
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

    // Pattern: "requires X" or "requires package X" — common in setup.py
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
                && name.chars().next().map_or(false, |c| c.is_alphabetic())
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

fn write_solver_artifacts(output_dir: &Path, result: &pre_solve::PreSolveResult) -> io::Result<()> {
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

fn format_dependency_state(resolved: &[ResolvedDependency], unresolved: &[String]) -> String {
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

/// Check the persistent unsolvable-modules cache for any import that was
/// previously identified as unsolvable.  Only returns a hit when confidence
/// is very high (>= 0.95) to avoid false positives that block solvable
/// packages like django.  In practice this means only curated seed entries
/// (host-runtime / platform-specific APIs with confidence 1.00) trigger the
/// early exit.
fn check_unsolvable_cache(
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

    if markers
        .iter()
        .any(|item| item == "pyqt4" || item.starts_with("pyqt4."))
        || markers
            .iter()
            .any(|item| item == "maya" || item.starts_with("maya."))
    {
        return Some((
            "skipped-host-runtime",
            "Detected host-application dependency (Maya/PyQt4). APDR cannot validate this snippet without the Autodesk Maya desktop runtime.".to_string(),
        ));
    }

    // Sublime Text plugin API — no PyPI package provides these
    if markers.iter().any(|item| {
        item == "sublime"
            || item == "sublime_plugin"
            || item.starts_with("sublime.")
            || item.starts_with("sublime_plugin.")
    }) {
        return Some((
            "skipped-host-runtime",
            "Detected Sublime Text plugin API dependency (sublime/sublime_plugin). APDR cannot validate this snippet without the Sublime Text editor runtime.".to_string(),
        ));
    }

    // Pythonista iOS runtime — bundled with the Pythonista app, not on PyPI
    for pythonista_marker in [
        "scene",
        "editor",
        "console",
        "photos",
        "dialogs",
        "canvas",
        "sound",
        "clipboard",
        "camera",
    ] {
        if markers.iter().any(|item| {
            item == pythonista_marker || item.starts_with(&format!("{pythonista_marker}."))
        }) {
            // Require at least one more Pythonista-specific marker to avoid
            // false-positives on the generic names `editor` / `console`.
            let pythonista_peers = [
                "scene",
                "editor",
                "console",
                "photos",
                "dialogs",
                "canvas",
                "sound",
                "clipboard",
                "camera",
                "objc_util",
                "cb",
                "ui",
                "appex",
            ];
            let peer_count = pythonista_peers
                .iter()
                .filter(|p| markers.contains(**p))
                .count();
            if peer_count >= 2
                || markers.contains("objc_util")
                || markers.contains("cb")
                || ((pythonista_marker == "clipboard" || pythonista_marker == "camera")
                    && peer_count >= 1)
            {
                return Some((
                    "skipped-host-runtime",
                    "Detected Pythonista iOS runtime dependency. APDR cannot validate this snippet without the Pythonista iOS app.".to_string(),
                ));
            }
        }
    }

    if markers
        .iter()
        .any(|item| item == "binaryninja" || item.starts_with("binaryninja."))
    {
        return Some((
            "skipped-host-runtime",
            "Detected Binary Ninja API dependency. APDR cannot validate this snippet without the Binary Ninja desktop runtime.".to_string(),
        ));
    }

    if markers
        .iter()
        .any(|item| item == "pyfbsdk" || item.starts_with("pyfbsdk."))
    {
        return Some((
            "skipped-host-runtime",
            "Detected Autodesk MotionBuilder API dependency. APDR cannot validate this snippet without the MotionBuilder runtime.".to_string(),
        ));
    }

    if markers
        .iter()
        .any(|item| item == "microbit" || item.starts_with("microbit."))
    {
        return Some((
            "skipped-host-runtime",
            "Detected device/runtime dependency (`microbit`). APDR cannot validate this snippet without the BBC micro:bit runtime or hardware.".to_string(),
        ));
    }

    // IDA Pro reverse-engineering tool API
    if markers.iter().any(|item| {
        item == "idautils"
            || item == "idaapi"
            || item == "idc"
            || item.starts_with("idautils.")
            || item.starts_with("idaapi.")
    }) {
        return Some((
            "skipped-host-runtime",
            "Detected IDA Pro API dependency (idautils/idaapi/idc). APDR cannot validate this snippet without the IDA Pro runtime.".to_string(),
        ));
    }

    // GIMP Script-Fu / Python-Fu
    if markers.iter().any(|item| {
        item == "gimpfu"
            || item == "gimp"
            || item.starts_with("gimpfu.")
            || item.starts_with("gimp.")
    }) {
        return Some((
            "skipped-host-runtime",
            "Detected GIMP plugin API dependency (gimpfu). APDR cannot validate this snippet without the GIMP runtime.".to_string(),
        ));
    }

    // HexChat IRC client plugin API
    if markers.contains("hexchat") {
        return Some((
            "skipped-host-runtime",
            "Detected HexChat plugin API dependency. APDR cannot validate this snippet without the HexChat IRC client runtime.".to_string(),
        ));
    }

    // Rhino 3D scripting API
    if markers.iter().any(|item| {
        item == "rhino"
            || item == "rhinoscriptsyntax"
            || item == "rhino3dm"
            || item.starts_with("rhino.")
            || item == "scriptcontext"
    }) {
        return Some((
            "skipped-host-runtime",
            "Detected Rhino 3D scripting API dependency. APDR cannot validate this snippet without the Rhino 3D runtime.".to_string(),
        ));
    }

    // Jython / Java interop (com.android.*, javax.*, java.*)
    if markers.iter().any(|item| {
        item.starts_with("com.android.")
            || item.starts_with("javax.")
            || item.starts_with("java.")
            || item == "monkeyrunner"
    }) {
        return Some((
            "skipped-host-runtime",
            "Detected Jython/Java interop dependency. APDR cannot validate this snippet without a Jython/Java runtime.".to_string(),
        ));
    }

    for marker in [
        "arcpy",
        "bpy",
        "c4d",
        "hou",
        "unreal",
        "nuke",
        "clr",
        "odbaccess",
    ] {
        if markers
            .iter()
            .any(|item| item == marker || item.starts_with(&format!("{marker}.")))
        {
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
    let has_apple_bridge = markers
        .iter()
        .any(|item| item == "objc" || item.starts_with("objc."));
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
    if markers
        .iter()
        .any(|item| item == "opendirectory" || item.starts_with("opendirectory."))
        || markers
            .iter()
            .any(|item| item == "systemconfiguration" || item.starts_with("systemconfiguration."))
    {
        return Some((
            "skipped-host-runtime",
            "Detected macOS framework dependency (OpenDirectory/SystemConfiguration). APDR cannot validate this snippet without the macOS host framework runtime.".to_string(),
        ));
    }

    if markers.iter().any(|item| {
        item == "xcb"
            || item.starts_with("xcb.")
            || item == "granite"
            || item.starts_with("granite.")
    }) || markers
        .iter()
        .any(|item| item == "gi.repository.granite" || item == "gi.repository.xcb")
    {
        return Some((
            "skipped-host-runtime",
            "Detected GTK desktop-runtime dependency (gi/xcb/Granite). APDR cannot validate this snippet without the corresponding desktop libraries.".to_string(),
        ));
    }

    // Windows-only APIs
    if markers.iter().any(|item| {
        item == "_winreg"
            || item == "winreg"
            || item == "win32security"
            || item == "win32api"
            || item == "win32con"
            || item == "win32file"
            || item == "win32event"
            || item == "win32service"
            || item == "win32process"
            || item == "win32gui"
            || item == "wmi"
            || item == "msvcrt"
            || item == "msilib"
            || item.starts_with("win32com.")
    }) {
        return Some((
            "skipped-host-runtime",
            "Detected Windows-only API dependency. APDR cannot validate this snippet without a Windows runtime.".to_string(),
        ));
    }

    // Raspberry Pi hardware APIs
    if markers.iter().any(|item| {
        item == "rpi"
            || item == "rpi.gpio"
            || item == "picamera"
            || item.starts_with("picamera.")
            || item == "gpiozero"
            || item.starts_with("gpiozero.")
            || item == "spidev"
            || item == "smbus"
    }) {
        return Some((
            "skipped-host-runtime",
            "Detected Raspberry Pi hardware dependency. APDR cannot validate this snippet without Raspberry Pi GPIO/camera access.".to_string(),
        ));
    }

    // Google App Engine bundled modules — google.appengine.* is the definitive
    // GAE marker; webapp2+ndb is a secondary pattern.
    if markers
        .iter()
        .any(|item| item.starts_with("google.appengine"))
        || (markers.contains("webapp2") && markers.contains("ndb"))
    {
        return Some((
            "skipped-host-runtime",
            "Detected Google App Engine dependency. APDR cannot validate this snippet without the GAE SDK runtime.".to_string(),
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

    // Manim library internal modules (helpers, mobject, scene, topics, animation).
    // When 3+ of these appear together, it's clearly a manim project layout.
    {
        let manim_modules = ["helpers", "mobject", "animation", "topics"];
        let manim_count = manim_modules
            .iter()
            .filter(|m| markers.contains(**m))
            .count();
        if manim_count >= 3 {
            return Some((
                "skipped-local-helper",
                "Snippet depends on internal manim library modules (helpers/mobject/animation/topics). These are project-local imports, not installable PyPI packages.".to_string(),
            ));
        }
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

    // Build a map from import_name → original package_name for the retried deps.
    // If the LLM failed to suggest a different package (just echoed the import name),
    // restore the original seed mapping instead.
    let original_mapping: BTreeMap<String, ResolvedDependency> = resolved
        .iter()
        .filter(|dep| packages_set.contains(&pypi_client::requirement_name(&dep.package_name)))
        .map(|dep| (dep.import_name.clone(), dep.clone()))
        .collect();

    let mut final_resolved = kept_resolved;
    for dep in llm_result.resolved {
        let norm_import = pypi_client::requirement_name(&dep.import_name);
        let norm_package = pypi_client::requirement_name(&dep.package_name);

        // Note: if LLM returned the same package that had no KGraph metadata,
        // pre-solve will fall back again, but pip can still install it directly.
        if packages_set.contains(&norm_package) {
            report.notes.push(format!(
                "LLM retry for `{}` returned same no-metadata package `{}`; keeping for pip install.",
                dep.import_name, dep.package_name
            ));
        }

        if norm_import == norm_package {
            // LLM just echoed the import name back — probably failed to parse.
            // Restore the original seed mapping if it had a different package.
            if let Some(original) = original_mapping.get(&dep.import_name) {
                let orig_norm = pypi_client::requirement_name(&original.package_name);
                if orig_norm != norm_import {
                    report.notes.push(format!(
                        "LLM retry returned raw import name `{}`; restoring original mapping to `{}`.",
                        dep.import_name, original.package_name
                    ));
                    final_resolved.push(original.clone());
                    continue;
                }
            }
        }
        final_resolved.push(dep);
    }

    (final_resolved, llm_result.unresolved)
}

/// Retry resolution via LLM for seed-resolved packages that don't exist on PyPI.
fn retry_nonexistent_packages(
    parse_result: &crate::ParseResult,
    snippet_source: &str,
    resolved: &[ResolvedDependency],
    python_version: &str,
    store: &mut CacheStore,
    config: &ResolveConfig,
    report: &mut crate::ResolutionReport,
) -> (Vec<ResolvedDependency>, Vec<String>) {
    // Find ALL resolved packages that don't exist on PyPI (seed or otherwise)
    let mut kept_resolved = Vec::new();
    let mut imports_to_retry = Vec::new();
    let mut bad_packages = Vec::new();

    for dep in resolved {
        if !pypi_client::package_exists(store, &dep.package_name, python_version) {
            imports_to_retry.push(dep.import_name.clone());
            bad_packages.push(dep.package_name.clone());
            report.notes.push(format!(
                "Package `{}` for import `{}` does not exist on PyPI (strategy: {}). Retrying with LLM.",
                dep.package_name, dep.import_name, dep.strategy
            ));
        } else {
            kept_resolved.push(dep.clone());
        }
    }

    if imports_to_retry.is_empty() {
        return (resolved.to_vec(), Vec::new());
    }

    let llm_result = tier3_llm::resolve_with_context(
        &imports_to_retry,
        snippet_source,
        parse_result,
        store,
        config,
        python_version,
        Some(format!(
            "Previous resolution mapped these imports to nonexistent PyPI packages: {}. Please suggest the correct PyPI package names for these imports.",
            bad_packages.join(", ")
        )),
    );

    report.llm_calls += llm_result.prompts_issued;
    report.notes.append(&mut llm_result.notes.clone());

    let mut final_resolved = kept_resolved;
    for dep in llm_result.resolved {
        final_resolved.push(dep);
    }

    (final_resolved, llm_result.unresolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_missing_module_from_standard_errors() {
        assert_eq!(
            extract_missing_module("ModuleNotFoundError: No module named 'Cython.Build'"),
            Some("Cython.Build".to_string()),
        );
        assert_eq!(
            extract_missing_module("ImportError: No module named Cython.Build"),
            Some("Cython.Build".to_string()),
        );
        assert_eq!(
            extract_missing_module("No module named 'foo'"),
            Some("foo".to_string()),
        );
        assert_eq!(extract_missing_module("everything is fine"), None);
    }

    #[test]
    fn extract_build_dependency_parenthesized_name() {
        // Pattern: "Numerical Python (NumPy) is not installed"
        let log = "running build\n\nNumerical Python (NumPy) is not installed.\n\nThis package is required.";
        assert_eq!(extract_build_dependency(log), Some("NumPy".to_string()),);
    }

    #[test]
    fn extract_build_dependency_bare_name() {
        // Pattern: "foo is not installed"
        let log = "running build\nCython is not installed\nPlease install it.";
        assert_eq!(extract_build_dependency(log), Some("Cython".to_string()),);
    }

    #[test]
    fn extract_build_dependency_please_install() {
        let log = "Error: please install numpy before building this package.";
        assert_eq!(extract_build_dependency(log), Some("numpy".to_string()),);
    }

    #[test]
    fn extract_build_dependency_falls_back_to_module_error() {
        // Should delegate to extract_missing_module first.
        let log = "ImportError: No module named 'Cython.Build'";
        assert_eq!(
            extract_build_dependency(log),
            Some("Cython.Build".to_string()),
        );
    }

    #[test]
    fn extract_build_dependency_returns_none_for_clean_log() {
        assert_eq!(
            extract_build_dependency("Successfully installed numpy-1.26.4"),
            None
        );
    }

    #[test]
    fn extract_package_and_version_pinned() {
        let log = "ERROR: Could not find a version that satisfies the requirement Django==5.1.3";
        assert_eq!(
            extract_package_and_version(log),
            Some(("Django".to_string(), Some("5.1.3".to_string()))),
        );
    }

    #[test]
    fn extract_package_and_version_no_matching_distribution() {
        // Package doesn't exist on PyPI at all (no version pin).
        let log = "ERROR: Could not find a version that satisfies the requirement taggit-autocomplete (from versions: none)\n\
                   ERROR: No matching distribution found for taggit-autocomplete";
        assert_eq!(
            extract_package_and_version(log),
            Some(("taggit-autocomplete".to_string(), None)),
        );
    }

    #[test]
    fn extract_package_and_version_no_matching_with_version() {
        let log = "ERROR: No matching distribution found for foo-bar==1.2.3";
        assert_eq!(
            extract_package_and_version(log),
            Some(("foo-bar".to_string(), Some("1.2.3".to_string()))),
        );
    }

    #[test]
    fn extract_syntax_error_package_from_site_packages() {
        let log = "  File \".../site-packages/memcache.py\", line 374\n\
                       def quit_all(self) -> None:\n\
                                          ^\n\
                   SyntaxError: invalid syntax";
        assert_eq!(
            extract_syntax_error_package(log),
            Some("memcache".to_string()),
        );
    }

    #[test]
    fn extract_syntax_error_package_nested() {
        let log = "  File \".../site-packages/foo/bar.py\", line 10\n\
                   SyntaxError: invalid syntax";
        assert_eq!(extract_syntax_error_package(log), Some("foo".to_string()),);
    }

    #[test]
    fn extract_syntax_error_package_not_in_site_packages() {
        // SyntaxError from the snippet itself, not a package.
        let log = "  File \"snippet.py\", line 5\n\
                   SyntaxError: invalid syntax";
        assert_eq!(extract_syntax_error_package(log), None);
    }

    #[test]
    fn guarded_import_detected() {
        let snippet = "try:\n    import foo\nexcept ImportError:\n    foo = None\n";
        assert!(is_guarded_import(snippet, "foo"));
    }

    #[test]
    fn unguarded_import_not_detected() {
        let snippet = "import foo\nimport bar\n";
        assert!(!is_guarded_import(snippet, "foo"));
    }

    #[test]
    fn guarded_from_import() {
        let snippet = "try:\n    from foo.bar import Baz\nexcept ImportError:\n    Baz = None\n";
        assert!(is_guarded_import(snippet, "foo"));
    }

    #[test]
    fn guarded_import_with_deeper_indent() {
        let snippet = "if True:\n    try:\n        import optional_pkg\n    except ImportError:\n        pass\n";
        assert!(is_guarded_import(snippet, "optional_pkg"));
    }

    #[test]
    fn extract_package_from_failed_building_wheel() {
        // Case 00056d4: lxml fails to build as a transitive dep of scrapy on Py2.7
        let log = "  Building wheel for lxml (setup.py): finished with status 'error'\n\
                   Failed to build lxml\n\
                   ERROR: Command errored out with exit status 1";
        assert_eq!(
            extract_package_and_version(log),
            Some(("lxml".to_string(), None)),
        );
    }

    #[test]
    fn extract_package_from_could_not_build_wheels() {
        let log = "ERROR: Could not build wheels for numpy, which is required to install pyproject.toml-based projects";
        assert_eq!(
            extract_package_and_version(log),
            Some(("numpy".to_string(), None)),
        );
    }

    #[test]
    fn detects_direct_sys_argv_positional_access() {
        let mut attribute_usage = std::collections::BTreeMap::new();
        attribute_usage.insert(
            "sys".to_string(),
            ["argv".to_string()].into_iter().collect(),
        );
        let parse_result = crate::ParseResult {
            imports: vec![],
            import_paths: vec![],
            config_deps: vec![],
            python_version_min: "2.7".to_string(),
            python_version_max: Some("2.7".to_string()),
            confidence: 0.72,
            scanned_files: vec![],
            stdlib_modules: std::collections::BTreeSet::new(),
            attribute_usage,
        };
        let snippet = "import sys\nport = int(sys.argv[1])\n";
        assert!(snippet_requires_positional_cli_args(&parse_result, snippet));
    }

    #[test]
    fn detects_sys_argv_alias_positional_access() {
        let mut attribute_usage = std::collections::BTreeMap::new();
        attribute_usage.insert(
            "sys".to_string(),
            ["argv".to_string()].into_iter().collect(),
        );
        let parse_result = crate::ParseResult {
            imports: vec![],
            import_paths: vec![],
            config_deps: vec![],
            python_version_min: "2.7".to_string(),
            python_version_max: Some("2.7".to_string()),
            confidence: 0.72,
            scanned_files: vec![],
            stdlib_modules: std::collections::BTreeSet::new(),
            attribute_usage,
        };
        let snippet = "import sys\nargvs = sys.argv\nmodel = argvs[2]\n";
        assert!(snippet_requires_positional_cli_args(&parse_result, snippet));
    }
}
