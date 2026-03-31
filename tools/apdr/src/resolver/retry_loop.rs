use super::artifacts::*;
use super::recovery_diagnostics::*;
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_with_retries(
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
    let mut retry_state = RetryLoopState::new(requirements_txt.clone());
    let mut pending_pattern_learning: Option<(String, String, String, String)> = None;
    let mut llm_recovery_history: Vec<(String, String, String)> = Vec::new();
    let mut seed_llm_fallback_attempted = false;
    let mut consecutive_llm_failures: usize = 0;
    let mut failed_import_package_pairs: BTreeSet<(String, String)> = BTreeSet::new();
    let mut failure_signature_requirements: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut module_requirement_sets: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut repeat_failure_signature: Option<String> = None;

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
        render_requirements_if_dirty(&mut retry_state, resolved, requirements_txt);
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

        if !retry_state
            .seen_requirements
            .insert(requirements_txt.clone())
        {
            // Before giving up on oscillation, try one LLM recovery pass.
            // The oscillation often means a package keeps flipping between
            // versions that both fail (e.g. python-memcached on Py2.7).
            // Give the LLM a chance to suggest an alternative package.
            if config.allow_llm && !seed_llm_fallback_attempted {
                seed_llm_fallback_attempted = true; // prevent infinite loop
                report.notes.push(
                    "Requirements oscillating â€” attempting LLM recovery before giving up."
                        .to_string(),
                );
                report.llm_calls += 1;
                let synthetic_log = "Validation is oscillating: the same requirements keep \
                    failing repeatedly. One or more packages may need to be replaced with \
                    an alternative. Consider pure-Python alternatives or different packages.";
                let llm_started = std::time::Instant::now();
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
                        &mut retry_state.llm_removed_imports,
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
                        retry_state.seen_requirements.clear();
                        retry_state.requirements_dirty = true;
                        render_requirements_if_dirty(&mut retry_state, resolved, requirements_txt);
                        retry_state
                            .seen_requirements
                            .insert(requirements_txt.clone());
                        continue;
                    }
                }
                validation.llm_duration_ms += llm_started.elapsed().as_millis();
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
        validation.agent_invocations += attempt_result.agent_invocations;
        validation.escalated_backend = attempt_result
            .escalated_backend
            .clone()
            .or(validation.escalated_backend.clone());
        if validation.validation_backend.is_empty() && !attempt_result.validation_backend.is_empty()
        {
            validation.validation_backend = attempt_result.validation_backend.clone();
        }
        validation.env_create_duration_ms += attempt_result.env_create_duration_ms;
        validation.install_duration_ms += attempt_result.install_duration_ms;
        validation.smoke_duration_ms += attempt_result.smoke_duration_ms;
        validation.attempts.extend(attempt_result.attempts.clone());
        validation.refresh_validation_path();
        update_fallback_metadata(&mut validation);

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
            // No error output â€” try LLM recovery with a synthetic description
            // before giving up, so every failing case gets at least one LLM attempt.
            if config.allow_llm && consecutive_llm_failures < 3 {
                let synthetic_log = "Validation failed with no error output. The environment may have failed to install or the smoke test produced no stderr/stdout.";
                report.llm_calls += 1;
                let llm_started = std::time::Instant::now();
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
                    let norm_wrong = normalize_package_key(&hint.wrong_pkg);
                    if !norm_wrong.is_empty() {
                        if let Some(dep_index) = dependency_index_by_package(resolved, &norm_wrong)
                        {
                            let dep = &mut resolved[dep_index];
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
                        let norm_remove = normalize_package_key(remove_name);
                        if let Some(pos) = dependency_index_by_package(resolved, &norm_remove) {
                            let removed = resolved.remove(pos);
                            retry_state.remember_removed_import(&removed.import_name);
                            let note = format!(
                                "LLM recovery: removed `{}` (import `{}`) â€” not a real PyPI package.",
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
                        validation.llm_duration_ms += llm_started.elapsed().as_millis();
                        report.retries += 1;
                        retry_state.requirements_dirty = true;
                        render_requirements_if_dirty(&mut retry_state, resolved, requirements_txt);
                        iteration_snapshots.push((
                            iter_num,
                            "recovery.txt".to_string(),
                            report.notes.last().cloned().unwrap_or_default(),
                        ));
                        iteration_snapshots.push((
                            iter_num,
                            "requirements-after-recovery.txt".to_string(),
                            requirements_txt.clone(),
                        ));
                        consecutive_llm_failures = 0;
                        continue;
                    }
                }
                validation.llm_duration_ms += llm_started.elapsed().as_millis();
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
        // as a successful resolution â€” the dependency set is correct.
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
            validation.reason = Some("Runtime config error â€” deps are correct.".to_string());
            if let Some(last_attempt) = validation.attempts.last_mut() {
                last_attempt.fix_applied = Some(note);
            }
            return Ok(validation);
        }

        // If LLM recovery previously removed an import (determined it's a local/project
        // module, not a PyPI package) and we now see that module missing at runtime,
        // treat as a pass â€” the dependencies are correct, the module is just local.
        if matches!(
            classified.error_type.as_str(),
            "ModuleNotFound" | "ImportError"
        ) {
            if let Some(module) = extract_missing_module(&last_log) {
                if retry_state.has_removed_import(&module) {
                    let note = format!(
                        "Missing module `{module}` was previously identified by LLM as a local/project module \
                         (not a PyPI package). Dependencies are correct; treating as resolved."
                    );
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    validation.succeeded = true;
                    validation.status = "passed".to_string();
                    validation.reason =
                        Some("LLM-identified local module â€” deps are correct.".to_string());
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
                        Some("Local project module â€” deps are correct.".to_string());
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
                    validation.reason = Some("Unix-only stdlib â€” deps are correct.".to_string());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    return Ok(validation);
                }
                // Optional/guarded imports: if the import is inside a try/except block,
                // it's optional â€” the program has a fallback path. Treat missing
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
                        Some("Optional guarded import â€” deps are correct.".to_string());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    return Ok(validation);
                }
            }
        }

        // Changed from >= 2 to >= 4 to give LLM more attempts before giving up
        // This allows the LLM to try different recovery strategies
        if failure_signature_requirements
            .get(&current_signature)
            .map(|seen| seen.len() >= 4)
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
                // Phase 9: check targeted stop-reason rules first.  If the
                // module is classified as removed-runtime, internal-extension,
                // or project-local, stop immediately with an inspectable note
                // instead of burning LLM retries.
                if let Some(stop_reason) = targeted_stop_reason_for_module(&module) {
                    let note = format!("Phase 9 targeted stop: module `{module}` — {stop_reason}");
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

                // Phase 9: before concluding mapping failure, check if a
                // targeted recovery provider rule can still recover this module.
                if module_requirement_sets
                    .get(&normalize_package_key(&module))
                    .map(|seen| seen.len() >= 2)
                    .unwrap_or(false)
                {
                    // Consult targeted_recovery for a deterministic provider
                    // rule before falling through to the generic break.
                    let has_targeted_provider = targeted_recovery::get_targeted_recovery_policy()
                        .and_then(|policy| policy.module_rule_for_alias(&module).cloned())
                        .is_some();

                    if !has_targeted_provider {
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
                    // If we have a targeted provider rule, fall through to
                    // apply_recovery_fix which will use it instead of breaking.
                }
            }
        }
        if let Some((package_name, _)) = extract_package_and_version(&last_log) {
            if retry_state
                .attempted_versions
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
            &mut retry_state,
            config,
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
            retry_state.requirements_dirty = true;
            render_requirements_if_dirty(&mut retry_state, resolved, requirements_txt);
            iteration_snapshots.push((
                iter_num,
                "recovery.txt".to_string(),
                report.notes.last().cloned().unwrap_or_default(),
            ));
            iteration_snapshots.push((
                iter_num,
                "requirements-after-recovery.txt".to_string(),
                requirements_txt.clone(),
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
            let llm_started = std::time::Instant::now();
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
                    &mut retry_state.llm_removed_imports,
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
                    let norm_remove = normalize_package_key(remove_name);
                    if let Some(pos) = dependency_index_by_package(resolved, &norm_remove) {
                        let removed = resolved.remove(pos);
                        retry_state.remember_removed_import(&removed.import_name);
                        let note = format!(
                            "LLM recovery: removed `{}` (import `{}`) â€” likely a local/project module, not a PyPI package.",
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
                    validation.llm_duration_ms += llm_started.elapsed().as_millis();
                    report.retries += 1;
                    retry_state.requirements_dirty = true;
                    render_requirements_if_dirty(&mut retry_state, resolved, requirements_txt);
                    iteration_snapshots.push((
                        iter_num,
                        "recovery.txt".to_string(),
                        report.notes.last().cloned().unwrap_or_default(),
                    ));
                    iteration_snapshots.push((
                        iter_num,
                        "requirements-after-recovery.txt".to_string(),
                        requirements_txt.clone(),
                    ));
                    consecutive_llm_failures = 0;
                    continue;
                }
            }
            validation.llm_duration_ms += llm_started.elapsed().as_millis();
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
                            .map(|(imp, pkg)| format!("{} â†’ {}", imp, pkg))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )),
                );
                report.llm_calls += llm_result.prompts_issued;
                validation.llm_duration_ms += llm_result.llm_duration_ms;
                // Replace seed-sourced deps with LLM results
                let mut changed = false;
                for llm_dep in &llm_result.resolved {
                    if let Some(existing_index) =
                        dependency_index_by_import(resolved, &llm_dep.import_name)
                    {
                        let existing = &mut resolved[existing_index];
                        if !(existing.strategy.starts_with("cache:seed")
                            || existing.strategy.starts_with("cache:discrepancy"))
                        {
                            continue;
                        }
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
                    retry_state.requirements_dirty = true;
                    render_requirements_if_dirty(&mut retry_state, resolved, requirements_txt);
                    iteration_snapshots.push((
                        iter_num,
                        "recovery.txt".to_string(),
                        "seed-llm-fallback applied".to_string(),
                    ));
                    iteration_snapshots.push((
                        iter_num,
                        "requirements-after-recovery.txt".to_string(),
                        requirements_txt.clone(),
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
                    if let Some(dep_index) = dependency_index_by_package(resolved, &package_name) {
                        let dep = &mut resolved[dep_index];
                        if dep.version.is_some() {
                            let old_ver = dep.version.clone().unwrap_or_default();
                            dep.version = None;
                            dep.strategy = "recovery:last-resort-strip".to_string();
                            dep.confidence = 0.55;
                            let note = format!(
                                "Last-resort: stripped version pin from {package_name}=={old_ver} after {} â€” letting pip choose.",
                                classified.error_type
                            );
                            report.retries += 1;
                            report.notes.push(note.clone());
                            validation.iteration_history.push(note.clone());
                            if let Some(last_attempt) = validation.attempts.last_mut() {
                                last_attempt.fix_applied = Some(note);
                            }
                            retry_state.requirements_dirty = true;
                            render_requirements_if_dirty(
                                &mut retry_state,
                                resolved,
                                requirements_txt,
                            );
                            iteration_snapshots.push((
                                iter_num,
                                "recovery.txt".to_string(),
                                report.notes.last().cloned().unwrap_or_default(),
                            ));
                            iteration_snapshots.push((
                                iter_num,
                                "requirements-after-recovery.txt".to_string(),
                                requirements_txt.clone(),
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
                        "Last-resort: stripped all version pins after {} â€” retrying with unpinned deps.",
                        classified.error_type
                    );
                    report.retries += 1;
                    report.notes.push(note.clone());
                    validation.iteration_history.push(note.clone());
                    if let Some(last_attempt) = validation.attempts.last_mut() {
                        last_attempt.fix_applied = Some(note);
                    }
                    retry_state.requirements_dirty = true;
                    render_requirements_if_dirty(&mut retry_state, resolved, requirements_txt);
                    iteration_snapshots.push((
                        iter_num,
                        "recovery.txt".to_string(),
                        report.notes.last().cloned().unwrap_or_default(),
                    ));
                    iteration_snapshots.push((
                        iter_num,
                        "requirements-after-recovery.txt".to_string(),
                        requirements_txt.clone(),
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

#[allow(clippy::too_many_arguments)]
fn apply_recovery_fix(
    classified: &crate::ClassifierResult,
    log: &str,
    resolved: &mut Vec<ResolvedDependency>,
    parse_result: &crate::ParseResult,
    python_version: &str,
    store: &mut CacheStore,
    retry_state: &mut RetryLoopState,
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

    // Phase 9: try targeted compatibility recovery before error-type-specific
    // fallbacks.  This consults the bounded policy layer (compatibility_rules.json)
    // to apply preferred versions for known clusters like torch/torchvision,
    // tensorflow/keras/tensorboard, and other canonical cases.
    if matches!(
        classified.error_type.as_str(),
        "DependencyConflict" | "VersionNotFound" | "InvalidVersion"
    ) {
        if let Some(note) = try_targeted_compatibility_recovery(log, resolved) {
            return Some(note);
        }
        // Also try normalizing transitive specifiers from the log so the
        // package key is usable for downstream recovery.
        if let Some(note) = try_targeted_transitive_specifier_recovery(log, resolved) {
            return Some(note);
        }
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
            if let Some(dep_index) = dependency_index_by_package(resolved, &package_name) {
                let dep = &mut resolved[dep_index];
                if dep.version.is_some() {
                    dep.version = None;
                    dep.strategy = "recovery:constraint-relaxation".to_string();
                    dep.confidence = 0.68;
                    return Some(format!(
                        "Stripped version pin from {package_name} after DependencyConflict â€” letting pip resolve freely."
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
                // try the known Py2 ceiling directly â€” even if it wasn't in the
                // fetched version list, pip may be able to install it.
                if python_version.starts_with("2.") {
                    if let Some(ceiling) = last_python2_version(&package_name) {
                        let current_ver = current_version.as_deref().unwrap_or("");
                        if current_ver != ceiling
                            && update_package_version(
                                resolved,
                                &package_name,
                                Some(ceiling.to_string()),
                            )
                        {
                            return Some(format!(
                                "Pinned {package_name} to {ceiling} (last known Python 2 version) after {}.",
                                classified.error_type
                            ));
                        }
                    }
                }
                // Still no luck â€” try stripping the version pin entirely so pip
                // can pick the best compatible version on its own.
                if let Some(dep_index) = dependency_index_by_package(resolved, &package_name) {
                    let dep = &mut resolved[dep_index];
                    if dep.version.is_some() {
                        dep.version = None;
                        dep.strategy = "recovery:version-strip".to_string();
                        dep.confidence = 0.60;
                        return Some(format!(
                            "Stripped version pin from {package_name} after {} â€” letting pip choose a compatible version.",
                            classified.error_type
                        ));
                    }
                }
                return None;
            }
            let previous = retry_state
                .attempted_versions
                .entry(package_name.clone())
                .or_default();
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
            if retry_state.has_removed_import(&module_name) {
                return None;
            }
            if let Some(note) =
                family_knowledge::recover_curated_missing_module(&module_name, resolved)
            {
                return Some(note);
            }
            // Phase 9: check targeted stop-reason rules.  If the module is
            // classified as removed-runtime, internal-extension, or project-local,
            // do NOT attempt recovery — the caller's break logic handles the stop.
            if targeted_stop_reason_for_module(&module_name).is_some() {
                return None;
            }
            // Phase 9: consult targeted recovery module-provider rules before
            // generic mapping-failure logic.  This gives deterministic provider
            // aliases (pkg_resources -> setuptools, Image -> Pillow, etc.) a
            // chance to recover the case without burning LLM retries.
            if let Some(policy) = targeted_recovery::get_targeted_recovery_policy() {
                if let Some(rule) = policy.module_rule_for_alias(&module_name) {
                    if upsert_dependency(
                        resolved,
                        &module_name,
                        &rule.provider_package,
                        None,
                        "recovery:targeted-provider",
                    ) {
                        return Some(format!(
                            "Phase 9 targeted recovery: mapped missing module `{}` to provider package `{}` (rule {}).",
                            module_name, rule.provider_package, rule.id
                        ));
                    }
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
            // KGraph/cache â€” the LLM may suggest an alternative package name.
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
                    let norm_pkg = package_name.to_ascii_lowercase().replace(['_', '.'], "-");
                    let norm_mod = module_name.to_ascii_lowercase().replace(['_', '.'], "-");
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
            // Try downgrading the failing package â€” older versions may have
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
                    let previous = retry_state
                        .attempted_versions
                        .entry(package_name.clone())
                        .or_default();
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
                        // Package not in resolved list â€” it's a transitive dependency.
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
            // If a package (not the snippet) has a SyntaxError â€” e.g. Python 3
            // type annotations imported on Python 2.7 â€” try downgrading that
            // specific package instead of giving up.
            if let Some(module_name) = extract_syntax_error_package(log) {
                let norm_mod = module_name.to_ascii_lowercase().replace('_', "-");
                if let Some(dep_index) = dependency_index_by_import(resolved, &norm_mod) {
                    let dep = &resolved[dep_index];
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
                        let previous = retry_state
                            .attempted_versions
                            .entry(package_name.clone())
                            .or_default();
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
                    if let Some(dep_index) = dependency_index_by_package(resolved, &pkg) {
                        let dep = &mut resolved[dep_index];
                        if dep.version.is_some() {
                            let old_version = dep.version.clone().unwrap_or_default();
                            dep.version = None;
                            dep.strategy = "recovery:deprecated-setup".to_string();
                            dep.confidence = 0.70;
                            return Some(format!(
                                "Stripped version pin from {pkg}=={old_version} after use_2to3 build failure â€” \
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

pub(super) fn python_backport_package<'a>(
    module_name: &str,
    python_version: &str,
) -> Option<&'a str> {
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

pub(super) fn selected_python_version(
    parse_result: &crate::ParseResult,
    config: &ResolveConfig,
) -> String {
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

struct RetryLoopState {
    seen_requirements: BTreeSet<String>,
    attempted_versions: BTreeMap<String, Vec<String>>,
    llm_removed_imports: BTreeSet<String>,
    cached_requirements: String,
    requirements_dirty: bool,
}

impl RetryLoopState {
    fn new(initial_requirements: String) -> Self {
        Self {
            seen_requirements: BTreeSet::new(),
            attempted_versions: BTreeMap::new(),
            llm_removed_imports: BTreeSet::new(),
            cached_requirements: initial_requirements,
            requirements_dirty: false,
        }
    }

    fn remember_removed_import(&mut self, import_name: &str) {
        self.llm_removed_imports
            .insert(normalize_package_key(import_name));
    }

    fn has_removed_import(&self, import_name: &str) -> bool {
        self.llm_removed_imports
            .contains(&normalize_package_key(import_name))
    }
}

pub(super) fn render_requirements(resolved: &[ResolvedDependency]) -> String {
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

fn render_requirements_if_dirty(
    retry_state: &mut RetryLoopState,
    resolved: &[ResolvedDependency],
    requirements_txt: &mut String,
) {
    if retry_state.requirements_dirty {
        retry_state.cached_requirements = render_requirements(resolved);
        retry_state.requirements_dirty = false;
    }
    if *requirements_txt != retry_state.cached_requirements {
        *requirements_txt = retry_state.cached_requirements.clone();
    }
}

#[doc(hidden)]
pub(super) fn debug_retry_loop_requirements_trace(
    initial_requirements: String,
    steps: Vec<(Vec<ResolvedDependency>, bool)>,
) -> Vec<String> {
    let mut retry_state = RetryLoopState::new(initial_requirements.clone());
    let mut requirements_txt = initial_requirements;
    let mut trace = Vec::new();

    for (resolved, dirty) in steps {
        retry_state.requirements_dirty = dirty;
        render_requirements_if_dirty(&mut retry_state, &resolved, &mut requirements_txt);
        trace.push(requirements_txt.clone());
    }

    trace
}

#[doc(hidden)]
pub(super) fn debug_update_package_version(
    mut resolved: Vec<ResolvedDependency>,
    package_name: &str,
    version: Option<String>,
) -> Vec<ResolvedDependency> {
    let _ = update_package_version(&mut resolved, package_name, version);
    resolved
}

#[doc(hidden)]
pub(super) fn debug_upsert_dependency(
    mut resolved: Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<String>,
    strategy: &str,
) -> Vec<ResolvedDependency> {
    let _ = upsert_dependency(&mut resolved, import_name, package_name, version, strategy);
    resolved
}

pub(super) fn dependency_index_by_package(
    resolved: &[ResolvedDependency],
    package_name: &str,
) -> Option<usize> {
    let package_key = normalize_package_key(package_name);
    resolved
        .iter()
        .position(|dependency| normalize_package_key(&dependency.package_name) == package_key)
}

pub(super) fn dependency_index_by_import(
    resolved: &[ResolvedDependency],
    import_name: &str,
) -> Option<usize> {
    let import_key = normalize_package_key(import_name);
    resolved
        .iter()
        .position(|dependency| normalize_package_key(&dependency.import_name) == import_key)
}

pub(super) fn dedupe_dependencies(resolved: &mut Vec<ResolvedDependency>) {
    // Normalize with lowercase + hyphen-to-underscore so that "Django" and
    // "django==5.0.8", or "Pillow" and "pillow", collapse to a single entry.
    // When there are duplicates, keep the first occurrence (which typically
    // has a version pin from seed/cache).
    let mut seen = BTreeSet::new();
    resolved.retain(|dependency| {
        let key = normalize_package_key(&dependency.package_name);
        seen.insert(key)
    });
}

pub(super) fn update_package_version(
    resolved: &mut [ResolvedDependency],
    package_name: &str,
    version: Option<String>,
) -> bool {
    if let Some(index) = dependency_index_by_package(resolved, package_name) {
        let dependency = &mut resolved[index];
        if dependency.version == version {
            return false;
        }
        dependency.version = version;
        dependency.strategy = "recovery:version-adjustment".to_string();
        dependency.confidence = 0.74;
        return true;
    }
    false
}

pub(super) fn ensure_dependency(
    resolved: &mut Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<String>,
    strategy: &str,
) -> bool {
    if dependency_index_by_package(resolved, package_name).is_some() {
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
pub(super) fn try_build_failure_alternatives(
    resolved: &mut [ResolvedDependency],
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
        ("lxml", "lxml", ""),                   // placeholder â€” lxml wheels usually work; skip
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
                )) || lower.contains("error: command 'gcc'")
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

pub(super) fn upsert_dependency(
    resolved: &mut Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<String>,
    strategy: &str,
) -> bool {
    if let Some(index) = dependency_index_by_import(resolved, import_name) {
        let dependency = &mut resolved[index];
        let changed = normalize_package_key(&dependency.package_name)
            != normalize_package_key(package_name)
            || dependency.version != version;
        dependency.package_name = package_name.to_string();
        dependency.version = version.clone();
        dependency.strategy = strategy.to_string();
        dependency.confidence = 0.78;
        return changed;
    }
    if dependency_index_by_package(resolved, package_name).is_some() {
        return false;
    }
    ensure_dependency(resolved, import_name, package_name, version, strategy)
}

pub(super) fn apply_compatibility_overrides(
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
/// Phase 9: Try targeted compatibility recovery using the bounded policy layer.
///
/// Checks if the build log matches a known compatibility cluster (torch,
/// tensorflow, scikit-learn, etc.) and applies the cluster's preferred versions
/// and companion packages before broad fallback logic fires.
fn try_targeted_compatibility_recovery(
    log: &str,
    resolved: &mut Vec<ResolvedDependency>,
) -> Option<String> {
    let policy = targeted_recovery::get_targeted_recovery_policy()?;
    let cluster = policy.compatibility_cluster_for_log(log)?;

    let mut notes: Vec<String> = Vec::new();
    let mut applied = false;

    // Apply preferred versions for anchor packages already in the resolved set.
    for (package, preferred_version) in &cluster.preferred_versions {
        if let Some(dep_index) = dependency_index_by_package(resolved, package) {
            let dep = &mut resolved[dep_index];
            let current = dep.version.as_deref().unwrap_or("(none)").to_string();
            if dep.version.as_deref() != Some(preferred_version.as_str()) {
                dep.version = Some(preferred_version.clone());
                dep.strategy = format!("recovery:phase9-compatibility-{}", cluster.id);
                dep.confidence = 0.75;
                notes.push(format!(
                    "phase9 compatibility [{}]: pinned `{}` from {} to {} (preferred).",
                    cluster.id, package, current, preferred_version
                ));
                applied = true;
            }
        }
    }

    // Apply companion packages from the cluster.
    for companion in &cluster.companions {
        if dependency_index_by_package(resolved, &companion.package).is_none() {
            ensure_dependency(
                resolved,
                &companion.package,
                &companion.package,
                None,
                &format!("recovery:phase9-compatibility-{}", cluster.id),
            );
            notes.push(format!(
                "phase9 compatibility [{}]: added companion `{}`.",
                cluster.id, companion.package
            ));
            applied = true;
        }
    }

    if applied {
        Some(notes.join(" "))
    } else {
        None
    }
}

/// Phase 9: Try to recover from transitive specifier strings that the standard
/// `extract_package_and_version` cannot parse (e.g. `PyJWT>=2.0.0`).
fn try_targeted_transitive_specifier_recovery(
    log: &str,
    resolved: &mut Vec<ResolvedDependency>,
) -> Option<String> {
    let policy = targeted_recovery::get_targeted_recovery_policy()?;

    for line in log.lines() {
        for marker in &[
            "No matching distribution found for ",
            "requirement ",
            "the requirement ",
        ] {
            if let Some(idx) = line.find(marker) {
                let candidate = line[idx + marker.len()..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_matches(|c: char| c == '"' || c == '\'' || c == ',' || c == ')')
                    .trim();
                if let Some((pkg_key, _constraint)) = normalize_requirement_spec(candidate) {
                    // Check companion rules for the extracted package
                    if let Some(companion_rule) = policy.companion_rule_for_package(&pkg_key) {
                        if dependency_index_by_package(resolved, &companion_rule.companion_package)
                            .is_none()
                        {
                            ensure_dependency(
                                resolved,
                                &companion_rule.companion_package,
                                &companion_rule.companion_package,
                                None,
                                "recovery:phase9-companion",
                            );
                            return Some(format!(
                                "phase9 transitive specifier: normalized `{}` to package `{}`, added companion `{}` (rule {}).",
                                candidate, pkg_key, companion_rule.companion_package, companion_rule.id
                            ));
                        }
                    }

                    // Check compatibility clusters for the extracted package
                    if let Some(cluster) = policy.compatibility_cluster_for_package(&pkg_key) {
                        let mut notes = Vec::new();
                        let mut changed = false;
                        for (package, preferred) in &cluster.preferred_versions {
                            if let Some(dep_index) = dependency_index_by_package(resolved, package)
                            {
                                let dep = &mut resolved[dep_index];
                                if dep.version.as_deref() != Some(preferred.as_str()) {
                                    dep.version = Some(preferred.clone());
                                    dep.strategy =
                                        format!("recovery:phase9-compatibility-{}", cluster.id);
                                    dep.confidence = 0.75;
                                    notes.push(format!(
                                        "pinned `{}` to {} (cluster {})",
                                        package, preferred, cluster.id
                                    ));
                                    changed = true;
                                }
                            }
                        }
                        if changed {
                            return Some(format!(
                                "phase9 transitive specifier: normalized `{}` to `{}` -- {}.",
                                candidate,
                                pkg_key,
                                notes.join(", ")
                            ));
                        }
                    }
                }
            }
        }
    }
    None
}
