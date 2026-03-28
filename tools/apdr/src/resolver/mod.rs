//! Resolver facade for snippet-to-requirements resolution.
//!
//! [`resolve_path(...)`] is the reviewer entrypoint for the full resolver flow:
//! it parses the snippet, consults cache and heuristic tiers, coordinates
//! pre-solve and validation, and writes the review artifacts for each attempt.
//! The heavy implementation clusters live in sibling modules so this facade can
//! keep the high-level orchestration readable: `retry_loop` owns dependency
//! mutation and retry control flow, `recovery_diagnostics` owns failure
//! classification and recovery notes, and `artifacts` owns parse, solver, and
//! iteration artifact output.
//!
//! Reviewers should read this file first when tracing the resolver facade.
//! Tier 3 LLM support remains a later recovery path behind the earlier cache
//! and heuristic stages rather than the primary resolution path.
pub mod family_knowledge;
pub mod kgraph_db;
pub mod pre_solve;
pub mod pubgrub_solver;
pub mod pypi_client;
pub mod tier1_cache;
pub mod tier2_heuristic;
pub mod tier3_llm;
pub mod version_sampler;

mod artifacts;
mod recovery_diagnostics;
mod retry_loop;

use self::artifacts::*;
use self::recovery_diagnostics::*;
use self::retry_loop::{
    apply_compatibility_overrides, dedupe_dependencies, render_requirements,
    selected_python_version, validate_with_retries,
};
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
    family_knowledge::init_curated_family_knowledge(tool_root)
        .map_err(|err| io::Error::other(format!("failed to initialize curated family knowledge: {err}")))?;
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
    // In LLM-only mode, skip this check â€” let the LLM decide everything.
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

    // --- #3: Fast path â€” reuse a previously validated import-set solution ---
    // If we've already validated an identical import set successfully, skip all
    // resolution + validation work and return the cached result immediately (~5ms).
    // Disabled when --force-validate is set (benchmark mode: always re-validate).
    if config.validate && !config.llm_only_mode && !config.force_validate {
        let import_key = cache::store::import_set_key(&parse_result.imports);
        if let Some(cached) = store.load_import_set_solution(&import_key) {
            report.notes.push(format!(
                "Import-set cache hit (key={}) â€” reusing validated solution.",
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

    // Run tier1 (cache) + tier2 (heuristic) first â€” these are fast (~ms)
    // In LLM-only mode, skip these tiers and go straight to Tier 3 LLM.
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
    // In LLM-only mode, skip solvability â€” we always attempt resolution.
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
                // cache all imports â€” that poisons common names like django.
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

            // Run Tier 3 LLM for remaining unresolved imports.
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
            // #9: Tier1/tier2 resolved >80% â€” skip solvability, go straight to LLM resolution
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
                    // LLM retry didn't resolve all imports â€” keep the original
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
            // not exist on PyPI â€” regardless of whether they came from seed
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
                    // LLM fixed it â€” proceed to validation
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
                    "Package `{}` may not exist on PyPI but --force-validate is set â€” proceeding.",
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
            // â€” the Docker+LLM build-retry pipeline (PLLM approach) can
            // sometimes recover cases the deterministic solver cannot.
            if config.allow_llm {
                report.notes.push(format!(
                    "Pre-solve UNSAT but LLM agent available â€” proceeding to validation. {}",
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
                    "Pre-solve UNSAT but --force-validate is set â€” proceeding to validation. {}",
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

#[doc(hidden)]
pub fn debug_retry_loop_requirements_trace(
    initial_requirements: String,
    steps: Vec<(Vec<ResolvedDependency>, bool)>,
) -> Vec<String> {
    retry_loop::debug_retry_loop_requirements_trace(initial_requirements, steps)
}

#[doc(hidden)]
pub fn debug_update_package_version(
    resolved: Vec<ResolvedDependency>,
    package_name: &str,
    version: Option<String>,
) -> Vec<ResolvedDependency> {
    retry_loop::debug_update_package_version(resolved, package_name, version)
}

#[doc(hidden)]
pub fn debug_upsert_dependency(
    resolved: Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<String>,
    strategy: &str,
) -> Vec<ResolvedDependency> {
    retry_loop::debug_upsert_dependency(resolved, import_name, package_name, version, strategy)
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

    // Sublime Text plugin API â€” no PyPI package provides these
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

    // Pythonista iOS runtime â€” bundled with the Pythonista app, not on PyPI
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

    // Google App Engine bundled modules â€” google.appengine.* is the definitive
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
#[allow(clippy::too_many_arguments)]
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
                "Package `{}` has no KGraph metadata. Retrying import `{}` with Tier 3 LLM fallback.",
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

    // Call Tier 3 LLM with additional context about the missing metadata.
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

    // Build a map from import_name â†’ original package_name for the retried deps.
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
            // LLM just echoed the import name back â€” probably failed to parse.
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
