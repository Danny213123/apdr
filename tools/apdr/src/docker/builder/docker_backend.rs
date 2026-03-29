use super::agent_backend::attempt_langgraph_agent;
use super::env_backend::attempt_metadata;
use super::process::{
    command_on_path, docker_container_name, docker_image_tag, run_command_with_timeout,
    truncate_log,
};
use crate::cache::build_cache;
use crate::cache::store::CacheStore;
use crate::context;
use crate::docker::{smoke_test, system_deps, templates};
use crate::{
    ResolveConfig, ValidationAttempt, ValidationSummary, VALIDATION_BACKEND_DOCKER,
    VALIDATION_BACKEND_ENV,
};
use std::fs;
use std::io;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub(super) fn validate_requirements_docker(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    store: &mut CacheStore,
) -> io::Result<ValidationSummary> {
    let mut summary = ValidationSummary {
        validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
        ..ValidationSummary::default()
    };
    context::ensure_debug_layout(&config.output_dir)?;

    // Try LangGraph multi-agent pipeline first when LLM is enabled
    if config.allow_llm {
        summary.agent_invocations += 1;
        if let Some(mut agent_summary) = attempt_langgraph_agent(
            snippet_path,
            requirements_txt,
            imports,
            candidate_versions,
            config,
        ) {
            agent_summary.agent_invocations = summary.agent_invocations;
            return Ok(agent_summary);
        }
        // Fall through to deterministic loop if agent is unavailable or failed
        eprintln!("[docker-agent] falling back to deterministic Docker validation");
    }

    if !command_on_path("docker") {
        return docker_backend_unavailable(
            candidate_versions,
            attempt_offset,
            config,
            requirements_txt,
            "Docker CLI is not installed or not on PATH.",
        );
    }

    let validation_started = Instant::now();
    let total_budget = config.validation_timeout;
    // Pre-generate once â€” identical across all Python version / retry attempts.
    let smoke_test_script = smoke_test::generate(imports, config.execute_snippet);

    // Infer system deps deterministically from requirements
    let mut sys_deps = system_deps::infer_system_deps_from_requirements(requirements_txt);

    let max_build_retries: usize = 2;
    let mut global_attempt = 0usize;

    let min_attempt_budget = Duration::from_secs(120);

    for (local_index, python_version) in candidate_versions.iter().enumerate() {
        let remaining = total_budget.saturating_sub(validation_started.elapsed());
        if remaining < min_attempt_budget {
            eprintln!(
                "[validation] budget too low for another attempt ({:.1}s remaining < {:.1}s min), skipping remaining {} version(s)",
                remaining.as_secs_f64(),
                min_attempt_budget.as_secs_f64(),
                candidate_versions.len() - local_index
            );
            break;
        }

        let build_key = build_cache::key_for(requirements_txt, python_version);
        summary.lockfile_key = Some(build_key.clone());
        summary.build_cache_key = Some(build_key.clone());
        let image_tag = docker_image_tag(&build_key, python_version);

        // Inner retry loop: retry Docker build when new system deps are discovered
        for build_retry in 0..=max_build_retries {
            let elapsed = validation_started.elapsed();
            if elapsed >= total_budget {
                break;
            }

            global_attempt += 1;
            let attempt_index = attempt_offset + global_attempt;
            let work_dir = context::attempt_dir(&config.output_dir, attempt_index, python_version);
            fs::create_dir_all(&work_dir)?;

            let dockerfile_path = work_dir.join("Dockerfile");
            let build_log_path = work_dir.join("build.log");
            let run_log_path = work_dir.join("run.log");
            let combined_log_path = work_dir.join("combined.log");
            let metadata_path = work_dir.join("metadata.txt");
            let context_snapshot_path = work_dir.join("benchmark-context-tail.txt");
            let build_command = format!(
                "docker build --progress=plain -t {image_tag} {}",
                work_dir.display()
            );
            let container_name = docker_container_name(&build_key, python_version, attempt_index);
            let run_command = format!(
                "docker create --name {container_name} {image_tag} && docker start -a {container_name}"
            );

            fs::write(work_dir.join("requirements.txt"), requirements_txt)?;
            fs::write(work_dir.join("smoke_test.py"), &smoke_test_script)?;
            fs::copy(snippet_path, work_dir.join("snippet.py"))?;
            // Generate Dockerfile with inferred system deps
            fs::write(
                &dockerfile_path,
                templates::python_slim_template(python_version, &sys_deps),
            )?;
            fs::write(work_dir.join("docker-build.command.txt"), &build_command)?;
            fs::write(work_dir.join("docker-run.command.txt"), &run_command)?;
            fs::write(&run_log_path, "")?;
            fs::write(&combined_log_path, "")?;
            if let Ok(tail) =
                context::read_context_tail(config.benchmark_context_log.as_deref(), 48_000)
            {
                fs::write(&context_snapshot_path, tail)?;
            } else {
                fs::write(&context_snapshot_path, "")?;
            }

            let mut attempt = ValidationAttempt {
                attempt_index,
                python_version: python_version.clone(),
                validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
                env_label: Some(image_tag.clone()),
                used_cached_lockfile: store.lockfile(&build_key).is_some(),
                artifact_dir: Some(work_dir.display().to_string()),
                build_log_path: Some(build_log_path.display().to_string()),
                run_log_path: Some(run_log_path.display().to_string()),
                combined_log_path: Some(combined_log_path.display().to_string()),
                metadata_path: Some(metadata_path.display().to_string()),
                context_snapshot_path: Some(context_snapshot_path.display().to_string()),
                ..Default::default()
            };

            // Docker build with BuildKit enabled
            let build_timeout = total_budget.saturating_sub(validation_started.elapsed());
            let mut build = Command::new("docker");
            build
                .arg("build")
                .arg("--progress=plain")
                .arg("-t")
                .arg(&image_tag)
                .arg(&work_dir)
                .env("DOCKER_BUILDKIT", "1");
            let build_output = run_command_with_timeout(&mut build, build_timeout)?;
            summary.install_duration_ms += build_output.duration_ms;
            fs::write(&build_log_path, &build_output.combined_output)?;
            let _ = context::append_context_log(
                config.benchmark_context_log.as_deref(),
                "apdr-docker-build",
                &build_output.combined_output,
            );

            if build_output.timed_out || !build_output.success {
                // On build failure, try to extract new system deps from the log
                if !build_output.timed_out && build_retry < max_build_retries {
                    let new_deps =
                        system_deps::extract_system_deps_from_log(&build_output.combined_output);
                    let prev_count = sys_deps.len();
                    for dep in new_deps {
                        if !sys_deps.contains(&dep) {
                            sys_deps.push(dep);
                        }
                    }
                    sys_deps.sort();
                    sys_deps.dedup();
                    if sys_deps.len() > prev_count {
                        eprintln!(
                            "[docker] build failed, discovered new system deps: {:?}, retrying",
                            &sys_deps[prev_count..]
                        );
                        // Record this attempt but continue to retry
                        attempt.status = "build-failed".to_string();
                        attempt.log_excerpt = truncate_log(&build_output.combined_output);
                        fs::write(&combined_log_path, &build_output.combined_output)?;
                        fs::write(
                            &metadata_path,
                            attempt_metadata(
                                &attempt,
                                &build_key,
                                &build_command,
                                &run_command,
                                build_output.exit_code,
                                build_output.duration_ms,
                                None,
                                None,
                            ),
                        )?;
                        summary.attempts.push(attempt);
                        continue; // retry inner loop with updated sys_deps
                    }
                }

                // No new deps found or timed out â€” record failure and move to next Python version
                let status = if build_output.timed_out {
                    "build-timeout"
                } else {
                    "build-failed"
                };
                attempt.status = status.to_string();
                attempt.log_excerpt = truncate_log(&build_output.combined_output);
                fs::write(&combined_log_path, &build_output.combined_output)?;
                fs::write(
                    &metadata_path,
                    attempt_metadata(
                        &attempt,
                        &build_key,
                        &build_command,
                        &run_command,
                        build_output.exit_code,
                        build_output.duration_ms,
                        None,
                        None,
                    ),
                )?;
                summary.attempts.push(attempt);
                break; // break inner retry loop, try next Python version
            }

            // Build succeeded — create the container first so startup is measured
            // separately from the smoke-test runtime.
            let build_logs = build_output.combined_output;
            let build_exit_code = build_output.exit_code;
            let build_duration_ms = build_output.duration_ms;

            let create_timeout = total_budget.saturating_sub(validation_started.elapsed());
            let mut create = Command::new("docker");
            create
                .arg("create")
                .arg("--name")
                .arg(&container_name)
                .arg(&image_tag);
            let create_output = run_command_with_timeout(&mut create, create_timeout)?;
            summary.docker_startup_duration_ms += create_output.duration_ms;
            let _ = context::append_context_log(
                config.benchmark_context_log.as_deref(),
                "apdr-docker-create",
                &create_output.combined_output,
            );

            if create_output.timed_out || !create_output.success {
                let combined = if build_logs.is_empty() {
                    create_output.combined_output.clone()
                } else {
                    format!("{build_logs}\n{}", create_output.combined_output)
                };
                attempt.status = if create_output.timed_out {
                    "runtime-timeout".to_string()
                } else {
                    "runtime-failed".to_string()
                };
                attempt.log_excerpt = truncate_log(&combined);
                fs::write(&run_log_path, &create_output.combined_output)?;
                fs::write(&combined_log_path, &combined)?;
                fs::write(
                    &metadata_path,
                    attempt_metadata(
                        &attempt,
                        &build_key,
                        &build_command,
                        &run_command,
                        build_exit_code,
                        build_duration_ms,
                        create_output.exit_code,
                        Some(create_output.duration_ms),
                    ),
                )?;
                summary.attempts.push(attempt);
                cleanup_docker_container(&container_name);
                break; // try next Python version
            }

            let run_timeout = total_budget.saturating_sub(validation_started.elapsed());
            let mut run = Command::new("docker");
            run.arg("start").arg("-a").arg(&container_name);
            let run_output = run_command_with_timeout(&mut run, run_timeout)?;
            summary.smoke_duration_ms += run_output.duration_ms;
            let combined = [
                build_logs.as_str(),
                &create_output.combined_output,
                &run_output.combined_output,
            ]
            .iter()
            .filter(|chunk| !chunk.trim().is_empty())
            .copied()
            .collect::<Vec<_>>()
            .join("\n");
            fs::write(&run_log_path, &run_output.combined_output)?;
            fs::write(&combined_log_path, &combined)?;
            let _ = context::append_context_log(
                config.benchmark_context_log.as_deref(),
                "apdr-docker-run",
                &run_output.combined_output,
            );

            if run_output.timed_out {
                attempt.status = "runtime-timeout".to_string();
                attempt.log_excerpt = truncate_log(&combined);
                fs::write(
                    &metadata_path,
                    attempt_metadata(
                        &attempt,
                        &build_key,
                        &build_command,
                        &run_command,
                        build_exit_code,
                        build_duration_ms,
                        run_output.exit_code,
                        Some(run_output.duration_ms),
                    ),
                )?;
                summary.attempts.push(attempt);
                cleanup_docker_container(&container_name);
                break; // try next Python version
            }

            if run_output.success {
                attempt.status = "passed".to_string();
                attempt.log_excerpt = truncate_log(&combined);
                fs::write(
                    &metadata_path,
                    attempt_metadata(
                        &attempt,
                        &build_key,
                        &build_command,
                        &run_command,
                        build_exit_code,
                        build_duration_ms,
                        run_output.exit_code,
                        Some(run_output.duration_ms),
                    ),
                )?;
                summary.selected_python_version = Some(python_version.clone());
                summary.build_cache_key = Some(build_key.clone());
                summary.build_image_id = Some(image_tag.clone());
                summary.succeeded = true;
                summary.attempts.push(attempt);
                cleanup_docker_container(&container_name);
                return Ok(summary);
            }

            // Runtime failure â€” no system dep retry for runtime failures
            attempt.status = "runtime-failed".to_string();
            attempt.log_excerpt = truncate_log(&combined);
            fs::write(
                &metadata_path,
                attempt_metadata(
                    &attempt,
                    &build_key,
                    &build_command,
                    &run_command,
                    build_exit_code,
                    build_duration_ms,
                    run_output.exit_code,
                    Some(run_output.duration_ms),
                ),
            )?;
            summary.attempts.push(attempt);
            cleanup_docker_container(&container_name);
            break; // try next Python version
        }

        // Clean up the Docker image for this Python version to reclaim disk space.
        // Each image is ~200-800MB; without cleanup a benchmark run of 2000+ cases
        // can exhaust disk and crash Docker Engine.
        cleanup_docker_image(&image_tag);
    }

    // Prune dangling images and build cache left over from failed builds.
    cleanup_docker_dangling();

    Ok(summary)
}

/// Remove a specific Docker image. Errors are silently ignored.
pub(super) fn cleanup_docker_image(image_tag: &str) {
    let result = Command::new("docker")
        .args(["rmi", "-f", image_tag])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if let Ok(status) = result {
        if status.success() {
            eprintln!("[docker] cleaned up image {image_tag}");
        }
    }
}

pub(super) fn cleanup_docker_container(container_name: &str) {
    let _ = Command::new("docker")
        .args(["rm", "-f", container_name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Prune dangling images and build cache. Runs silently.
pub(super) fn cleanup_docker_dangling() {
    let _ = Command::new("docker")
        .args(["image", "prune", "-f"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    // Also trim build cache â€” keep last 2GB to avoid re-downloading base images
    let _ = Command::new("docker")
        .args(["builder", "prune", "-f", "--keep-storage", "2g"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

pub(super) fn docker_backend_unavailable(
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    requirements_txt: &str,
    detail: &str,
) -> io::Result<ValidationSummary> {
    let python_version = candidate_versions
        .first()
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let attempt_index = attempt_offset + 1;
    let build_key = build_cache::key_for(requirements_txt, &python_version);
    let image_tag = docker_image_tag(&build_key, &python_version);
    let work_dir = context::attempt_dir(&config.output_dir, attempt_index, &python_version);
    fs::create_dir_all(&work_dir)?;
    let build_log_path = work_dir.join("build.log");
    let run_log_path = work_dir.join("run.log");
    let combined_log_path = work_dir.join("combined.log");
    let metadata_path = work_dir.join("metadata.txt");
    let context_snapshot_path = work_dir.join("benchmark-context-tail.txt");
    fs::write(&build_log_path, detail)?;
    fs::write(&run_log_path, "")?;
    fs::write(&combined_log_path, detail)?;
    fs::write(&context_snapshot_path, "")?;

    let attempt = ValidationAttempt {
        attempt_index,
        python_version,
        status: "build-failed".to_string(),
        validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
        env_label: Some(image_tag),
        log_excerpt: truncate_log(detail),
        artifact_dir: Some(work_dir.display().to_string()),
        build_log_path: Some(build_log_path.display().to_string()),
        run_log_path: Some(run_log_path.display().to_string()),
        combined_log_path: Some(combined_log_path.display().to_string()),
        metadata_path: Some(metadata_path.display().to_string()),
        context_snapshot_path: Some(context_snapshot_path.display().to_string()),
        ..Default::default()
    };
    fs::write(
        &metadata_path,
        attempt_metadata(
            &attempt,
            &build_key,
            "docker unavailable",
            "--",
            None,
            0,
            None,
            None,
        ),
    )?;

    Ok(ValidationSummary {
        validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
        lockfile_key: Some(build_key.clone()),
        build_cache_key: Some(build_key),
        attempts: vec![attempt],
        ..Default::default()
    })
}

pub(super) fn env_has_system_dep_failure(summary: &ValidationSummary) -> bool {
    for attempt in &summary.attempts {
        if attempt.status != "build-failed" && attempt.status != "runtime-failed" {
            continue;
        }
        let log = &attempt.log_excerpt;
        if !system_deps::extract_system_deps_from_log(log).is_empty()
            || system_deps::requires_external_runtime_from_log(log)
        {
            return true;
        }
    }
    false
}

pub(super) fn env_attempt_requires_backend_escalation(attempt: &ValidationAttempt) -> bool {
    matches!(attempt.status.as_str(), "build-failed" | "runtime-failed")
        && (!system_deps::extract_system_deps_from_log(&attempt.log_excerpt).is_empty()
            || system_deps::requires_external_runtime_from_log(&attempt.log_excerpt))
}

/// Returns true when any env-backend attempt failed because the local Python
/// interpreter was not found or could not be auto-installed.  Docker images
/// ship their own interpreter so this class of failure is always recoverable.
pub(super) fn env_has_interpreter_failure(summary: &ValidationSummary) -> bool {
    if summary.attempts.is_empty() {
        return false;
    }
    summary.attempts.iter().any(|attempt| {
        attempt.status == "build-failed"
            && (attempt
                .log_excerpt
                .contains("No local interpreter found for Python")
                || attempt
                    .log_excerpt
                    .contains("Organization policies are preventing installation")
                || attempt.log_excerpt.contains("python unavailable")
                || attempt
                    .log_excerpt
                    .contains("Installer failed with exit code"))
    })
}

/// Returns true when any env-backend attempt failed due to a build timeout.
/// Docker/Linux typically has pre-built wheels available, avoiding lengthy
/// from-source compilation that causes timeouts on Windows.
pub(super) fn env_has_build_timeout(summary: &ValidationSummary) -> bool {
    summary.attempts.iter().any(|a| a.status == "build-timeout")
}

pub(super) fn should_retry_failed_env_validation_in_docker(
    summary: &ValidationSummary,
    requirements_txt: &str,
) -> bool {
    if summary.succeeded {
        return false;
    }
    if summary.attempts.is_empty() {
        return false;
    }
    let reqs_have_sys_deps =
        !system_deps::infer_system_deps_from_requirements(requirements_txt).is_empty();
    reqs_have_sys_deps
        || env_has_system_dep_failure(summary)
        || env_has_interpreter_failure(summary)
        || env_has_build_timeout(summary)
        || summary
            .attempts
            .iter()
            .any(|attempt| attempt.validation_backend == VALIDATION_BACKEND_ENV)
}

pub(super) fn env_failure_reason_for_docker_retry(
    summary: &ValidationSummary,
    requirements_txt: &str,
) -> &'static str {
    let reqs_have_sys_deps =
        !system_deps::infer_system_deps_from_requirements(requirements_txt).is_empty();
    if env_has_interpreter_failure(summary) {
        "missing local Python interpreter"
    } else if env_has_build_timeout(summary) {
        "build timeout (Docker has pre-built wheels)"
    } else if reqs_have_sys_deps {
        "packages require system libraries"
    } else if env_has_system_dep_failure(summary) {
        "system-dep build errors"
    } else {
        "env validation failed"
    }
}
