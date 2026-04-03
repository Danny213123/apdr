use super::docker_backend::env_attempt_requires_backend_escalation;
use super::process::*;
use super::python_runtime::*;
use super::*;
use crate::cache::build_cache;
use crate::cache::maintenance;
use crate::cache::store::CacheStore;
use crate::context;
use crate::docker::smoke_test;
use crate::{ResolveConfig, ValidationAttempt, ValidationSummary};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ValidatedEnvCacheSource {
    /// Compressed archive (.tar.zst)
    Archive(PathBuf),
    /// Legacy uncompressed directory
    LegacyDir(PathBuf),
    /// No cached environment found
    None,
}

/// Per-attempt workspace paths and metadata for env validation.
#[derive(Debug, Clone)]
pub(super) struct EnvAttemptPaths {
    work_dir: PathBuf,
    env_dir: PathBuf,
    build_log_path: PathBuf,
    run_log_path: PathBuf,
    combined_log_path: PathBuf,
    metadata_path: PathBuf,
    context_snapshot_path: PathBuf,
    install_requirements_path: PathBuf,
    env_label: String,
}

/// Prepare workspace for a single env validation attempt.
/// Writes requirements.txt, smoke_test.py, snippet.py, context snapshot,
/// and detects cached validated-env source (archive, legacy dir, or none).
/// Returns the selected cache source and the build key for this attempt.
pub(super) fn prepare_env_validation_attempt(
    snippet_path: &Path,
    requirements_txt: &str,
    smoke_test_script: &str,
    python_version: &str,
    attempt_index: usize,
    validated_envs_dir: &Path,
    config: &ResolveConfig,
) -> io::Result<(EnvAttemptPaths, ValidatedEnvCacheSource, String)> {
    let build_key = build_cache::key_for(requirements_txt, python_version);
    let env_label = sanitized_env_label(&build_key, python_version);
    let work_dir = context::attempt_dir(&config.output_dir, attempt_index, python_version);
    fs::create_dir_all(&work_dir)?;

    let env_dir = work_dir.join("env");
    // Defensive cleanup: remove any stale env left by a previously killed process.
    if env_dir.exists() {
        let _ = fs::remove_dir_all(&env_dir);
    }

    fs::write(work_dir.join("requirements.txt"), requirements_txt)?;
    fs::write(work_dir.join("smoke_test.py"), smoke_test_script)?;
    fs::copy(snippet_path, work_dir.join("snippet.py"))?;

    let build_log_path = work_dir.join("build.log");
    let run_log_path = work_dir.join("run.log");
    let combined_log_path = work_dir.join("combined.log");
    let metadata_path = work_dir.join("metadata.txt");
    let context_snapshot_path = work_dir.join("benchmark-context-tail.txt");
    let install_requirements_path = work_dir.join("requirements-install.txt");

    fs::write(&install_requirements_path, requirements_txt)?;
    fs::write(&run_log_path, "")?;
    fs::write(&combined_log_path, "")?;

    if let Ok(tail) = context::read_context_tail(config.benchmark_context_log.as_deref(), 48_000) {
        fs::write(&context_snapshot_path, tail)?;
    } else {
        fs::write(&context_snapshot_path, "")?;
    }

    let paths = EnvAttemptPaths {
        work_dir,
        env_dir,
        build_log_path,
        run_log_path,
        combined_log_path,
        metadata_path,
        context_snapshot_path,
        install_requirements_path,
        env_label,
    };

    // Detect validated-env cache source: prefer archive, fall back to legacy dir
    let cached_archive = validated_env_archive_path(validated_envs_dir, &build_key);
    let cached_env_dir = validated_env_cache_path(validated_envs_dir, &build_key);
    let cache_source = if cached_archive.exists() {
        ValidatedEnvCacheSource::Archive(cached_archive)
    } else if cached_env_dir.exists()
        && (cached_env_dir.join("bin").exists() || cached_env_dir.join("Scripts").exists())
    {
        ValidatedEnvCacheSource::LegacyDir(cached_env_dir)
    } else {
        ValidatedEnvCacheSource::None
    };

    Ok((paths, cache_source, build_key))
}

/// Materialize a validated environment for an attempt: restore from cache or build fresh.
/// Returns (build_logs, exit_code, duration_ms).
/// On failure, sets attempt.status and returns early with empty status to signal continuation.
#[allow(clippy::too_many_arguments)]
pub(super) fn materialize_env_for_attempt(
    cache_source: &ValidatedEnvCacheSource,
    paths: &EnvAttemptPaths,
    interpreter: &Path,
    python_version: &str,
    env_python: &Path,
    wheelhouse_dir: &Path,
    build_command: &str,
    run_command: &str,
    build_key: &str,
    timeout: Duration,
    config: &ResolveConfig,
    attempt: &mut ValidationAttempt,
    summary: &mut ValidationSummary,
) -> io::Result<(String, Option<i32>, u128)> {
    match cache_source {
        ValidatedEnvCacheSource::Archive(cached_archive) => {
            // Try CoW clone from .hot sibling first (near-instant on APFS)
            let hot = maintenance::hot_dir_path(cached_archive);
            let restore_result = if hot.exists() {
                match maintenance::try_cow_clone(&hot, &paths.env_dir) {
                    Ok(true) => Ok(()),
                    _ => maintenance::extract_archive_to_env(cached_archive, &paths.env_dir),
                }
            } else {
                maintenance::extract_archive_to_env(cached_archive, &paths.env_dir)
            };
            // Verify the extracted env has a usable Python binary
            let restore_result = restore_result.and_then(|()| {
                let has_bin =
                    paths.env_dir.join("bin").exists() || paths.env_dir.join("Scripts").exists();
                if has_bin {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "Extracted env missing bin/ directory: {}",
                            paths.env_dir.display()
                        ),
                    ))
                }
            });
            match restore_result {
                Ok(()) => {
                    let _ = maintenance::touch_archive_marker(cached_archive);
                    let log = format!(
                        "reused cached validated env from {}",
                        cached_archive.display()
                    );
                    fs::write(&paths.build_log_path, &log)?;
                    Ok((log, None, 0_u128))
                }
                Err(err) => {
                    // Cache restore failed; fall back to cold build
                    let _ = fs::remove_dir_all(&paths.env_dir);
                    attempt.used_cached_env = false;
                    attempt.validated_env_cache_hit = false;
                    let result = create_and_install_env(
                        interpreter,
                        python_version,
                        &paths.env_dir,
                        env_python,
                        wheelhouse_dir,
                        &paths.install_requirements_path,
                        &paths.build_log_path,
                        &paths.combined_log_path,
                        &paths.metadata_path,
                        build_command,
                        run_command,
                        build_key,
                        timeout,
                        config,
                        attempt,
                        summary,
                    )?;
                    let mut log = format!("(cache restore failed: {})\n", err);
                    log.push_str(&result.0);
                    Ok((log, result.1, result.2))
                }
            }
        }
        ValidatedEnvCacheSource::LegacyDir(cached_env_dir) => {
            let restore_result = copy_dir_all(cached_env_dir, &paths.env_dir);
            // Verify the extracted env has a usable Python binary
            let restore_result = restore_result.and_then(|()| {
                let has_bin =
                    paths.env_dir.join("bin").exists() || paths.env_dir.join("Scripts").exists();
                if has_bin {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "Extracted env missing bin/ directory: {}",
                            paths.env_dir.display()
                        ),
                    ))
                }
            });
            match restore_result {
                Ok(()) => {
                    let _ = maintenance::touch_validated_env_cache_entry(cached_env_dir);
                    let log = format!(
                        "reused cached validated env from {}",
                        cached_env_dir.display()
                    );
                    fs::write(&paths.build_log_path, &log)?;
                    Ok((log, None, 0_u128))
                }
                Err(err) => {
                    // Cache restore failed; fall back to cold build
                    let _ = fs::remove_dir_all(&paths.env_dir);
                    attempt.used_cached_env = false;
                    attempt.validated_env_cache_hit = false;
                    let result = create_and_install_env(
                        interpreter,
                        python_version,
                        &paths.env_dir,
                        env_python,
                        wheelhouse_dir,
                        &paths.install_requirements_path,
                        &paths.build_log_path,
                        &paths.combined_log_path,
                        &paths.metadata_path,
                        build_command,
                        run_command,
                        build_key,
                        timeout,
                        config,
                        attempt,
                        summary,
                    )?;
                    let mut log = format!("(cache restore failed: {})\n", err);
                    log.push_str(&result.0);
                    Ok((log, result.1, result.2))
                }
            }
        }
        ValidatedEnvCacheSource::None => {
            // No cache; build fresh
            create_and_install_env(
                interpreter,
                python_version,
                &paths.env_dir,
                env_python,
                wheelhouse_dir,
                &paths.install_requirements_path,
                &paths.build_log_path,
                &paths.combined_log_path,
                &paths.metadata_path,
                build_command,
                run_command,
                build_key,
                timeout,
                config,
                attempt,
                summary,
            )
        }
    }
}

fn latest_env_attempt_requires_backend_escalation(summary: &ValidationSummary) -> bool {
    summary
        .attempts
        .last()
        .map(env_attempt_requires_backend_escalation)
        .unwrap_or(false)
}

pub(super) fn validate_requirements_env(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    store: &mut CacheStore,
) -> io::Result<ValidationSummary> {
    let mut summary = ValidationSummary::default();
    context::ensure_debug_layout(&config.output_dir)?;
    let wheelhouse_dir = config.cache_path.join("wheelhouse");
    fs::create_dir_all(&wheelhouse_dir)?;
    let validated_envs_dir = config.cache_path.join("validated-envs");
    fs::create_dir_all(&validated_envs_dir)?;
    let _ = maintenance::prune_validated_env_cache(
        &validated_envs_dir,
        config.validated_env_cache_max_entries,
        config.validated_env_cache_max_bytes,
    );

    let validation_started = Instant::now();
    let total_budget = config.validation_timeout;
    // Pre-generate the smoke test script once â€” it's identical across Python
    // version attempts since imports and execute_snippet don't change.
    let smoke_test_script = smoke_test::generate(imports, config.execute_snippet);

    // Minimum time each attempt gets â€” enough for env create + pip install of
    // moderate packages.  Prevents later attempts from getting a near-zero budget
    // when earlier attempts consumed most of the total.
    let min_attempt_budget = Duration::from_secs(120);

    'versions: for (local_index, python_version) in candidate_versions.iter().enumerate() {
        // Check total validation budget before starting another attempt.
        // Require at least min_attempt_budget remaining so the attempt has a
        // realistic chance of completing.
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
        let attempt_index = attempt_offset + local_index + 1;

        // Prepare attempt workspace and detect cache source
        let (paths, cache_source, build_key) = prepare_env_validation_attempt(
            snippet_path,
            requirements_txt,
            &smoke_test_script,
            python_version,
            attempt_index,
            &validated_envs_dir,
            config,
        )?;

        summary.lockfile_key = Some(build_key.clone());
        summary.build_cache_key = Some(build_key.clone());

        let interpreter = match ensure_python_interpreter(python_version) {
            Ok(path) => path,
            Err(detail) => {
                fs::write(&paths.build_log_path, &detail)?;
                fs::write(&paths.combined_log_path, &detail)?;
                let attempt = ValidationAttempt {
                    attempt_index,
                    python_version: python_version.clone(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    env_label: Some(paths.env_label.clone()),
                    status: "build-failed".to_string(),
                    log_excerpt: truncate_log(&detail),
                    artifact_dir: Some(paths.work_dir.display().to_string()),
                    build_log_path: Some(paths.build_log_path.display().to_string()),
                    run_log_path: Some(paths.run_log_path.display().to_string()),
                    combined_log_path: Some(paths.combined_log_path.display().to_string()),
                    metadata_path: Some(paths.metadata_path.display().to_string()),
                    context_snapshot_path: Some(paths.context_snapshot_path.display().to_string()),
                    ..Default::default()
                };
                fs::write(
                    &paths.metadata_path,
                    attempt_metadata(
                        &attempt,
                        &build_key,
                        &format!("python{} unavailable", python_version),
                        "--",
                        None,
                        0,
                        None,
                        None,
                    ),
                )?;
                summary.attempts.push(attempt);
                if latest_env_attempt_requires_backend_escalation(&summary) {
                    break 'versions;
                }
                continue;
            }
        };

        let env_python = env_python_path(&paths.env_dir);
        let env_create_command = if python_version.starts_with("2.") {
            format!(
                "{} -m virtualenv {}",
                interpreter.display(),
                paths.env_dir.display()
            )
        } else {
            format!(
                "{} -m venv {}",
                interpreter.display(),
                paths.env_dir.display()
            )
        };
        let env_install_command = format!(
            "{} -m pip install --disable-pip-version-check --default-timeout=100 --cache-dir {} -r {}",
            env_python.display(),
            wheelhouse_dir.display(),
            paths.install_requirements_path.display()
        );
        let build_command = format!("{}\n{}", env_create_command, env_install_command);
        let run_command = format!(
            "{} {}",
            env_python.display(),
            paths.work_dir.join("smoke_test.py").display()
        );
        fs::write(
            paths.work_dir.join("env-create.command.txt"),
            &env_create_command,
        )?;
        fs::write(
            paths.work_dir.join("env-install.command.txt"),
            &env_install_command,
        )?;
        fs::write(paths.work_dir.join("env-run.command.txt"), &run_command)?;

        let mut attempt = ValidationAttempt {
            attempt_index,
            python_version: python_version.clone(),
            validation_backend: VALIDATION_BACKEND_ENV.to_string(),
            env_label: Some(paths.env_label.clone()),
            env_dir: Some(paths.env_dir.display().to_string()),
            used_cached_lockfile: store.lockfile(&build_key).is_some(),
            artifact_dir: Some(paths.work_dir.display().to_string()),
            build_log_path: Some(paths.build_log_path.display().to_string()),
            run_log_path: Some(paths.run_log_path.display().to_string()),
            combined_log_path: Some(paths.combined_log_path.display().to_string()),
            metadata_path: Some(paths.metadata_path.display().to_string()),
            context_snapshot_path: Some(paths.context_snapshot_path.display().to_string()),
            ..Default::default()
        };
        summary.validation_backend = VALIDATION_BACKEND_ENV.to_string();

        // Determine cache hit status from detected cache source
        let cache_hit = !matches!(cache_source, ValidatedEnvCacheSource::None);
        attempt.used_cached_env = cache_hit;
        attempt.validated_env_cache_hit = cache_hit;

        // Materialize env: restore from cache or build fresh
        let attempt_timeout = total_budget.saturating_sub(validation_started.elapsed());
        let (build_logs, build_exit_code, build_duration_ms) = materialize_env_for_attempt(
            &cache_source,
            &paths,
            &interpreter,
            python_version,
            &env_python,
            &wheelhouse_dir,
            &build_command,
            &run_command,
            &build_key,
            attempt_timeout,
            config,
            &mut attempt,
            &mut summary,
        )?;

        // If attempt.status was set during materialization, escalation or failure occurred
        if !attempt.status.is_empty() {
            let _ = fs::remove_dir_all(&paths.env_dir);
            summary.attempts.push(attempt);
            if latest_env_attempt_requires_backend_escalation(&summary) {
                break 'versions;
            }
            continue;
        }

        let mut smoke_command = smoke_test_command(&env_python, &paths.work_dir);
        let smoke_timeout = total_budget.saturating_sub(validation_started.elapsed());
        let run_output = run_command_with_timeout(&mut smoke_command, smoke_timeout)?;
        summary.smoke_duration_ms += run_output.duration_ms;
        let combined = if build_logs.is_empty() {
            run_output.combined_output.clone()
        } else {
            format!("{build_logs}\n{}", run_output.combined_output)
        };
        fs::write(&paths.run_log_path, &run_output.combined_output)?;
        fs::write(&paths.combined_log_path, &combined)?;
        let _ = context::append_context_log(
            config.benchmark_context_log.as_deref(),
            "apdr-env-run",
            &run_output.combined_output,
        );

        if run_output.timed_out {
            attempt.status = "runtime-timeout".to_string();
            attempt.log_excerpt = truncate_log(&combined);
            fs::write(
                &paths.metadata_path,
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
            let _ = fs::remove_dir_all(&paths.env_dir);
            summary.attempts.push(attempt);
            if latest_env_attempt_requires_backend_escalation(&summary) {
                break 'versions;
            }
            continue;
        }

        if run_output.success {
            attempt.status = "passed".to_string();
            attempt.log_excerpt = truncate_log(&combined);
            let site_packages = env_site_packages_dir(&paths.env_dir, python_version);
            if config.package_repository_cache_enabled {
                let _ = catalog_package_repository(store, python_version, &site_packages);
            }
            // Save validated env to cache for future reuse
            if config.validated_env_cache_max_entries > 0 {
                let _ = save_validated_env(&validated_envs_dir, &build_key, &paths.env_dir);
                let _ = maintenance::prune_validated_env_cache(
                    &validated_envs_dir,
                    config.validated_env_cache_max_entries,
                    config.validated_env_cache_max_bytes,
                );
            }
            fs::write(
                &paths.metadata_path,
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
            summary.build_image_id = None;
            summary.succeeded = true;
            summary.attempts.push(attempt);
            // Clean up the venv to reclaim disk space (already saved to validated-env cache above)
            let _ = fs::remove_dir_all(&paths.env_dir);
            return Ok(summary);
        }

        attempt.status = "runtime-failed".to_string();
        attempt.log_excerpt = truncate_log(&combined);
        fs::write(
            &paths.metadata_path,
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
        if latest_env_attempt_requires_backend_escalation(&summary) {
            break 'versions;
        }
        // Clean up the venv to reclaim disk space (logs and metadata are preserved)
        let _ = fs::remove_dir_all(&paths.env_dir);
    }

    Ok(summary)
}
#[allow(clippy::too_many_arguments)]
pub(super) fn create_and_install_env(
    interpreter: &Path,
    python_version: &str,
    env_dir: &Path,
    env_python: &Path,
    wheelhouse_dir: &Path,
    install_requirements_path: &Path,
    build_log_path: &Path,
    combined_log_path: &Path,
    metadata_path: &Path,
    build_command: &str,
    run_command: &str,
    build_key: &str,
    timeout: Duration,
    config: &ResolveConfig,
    attempt: &mut ValidationAttempt,
    summary: &mut ValidationSummary,
) -> io::Result<(String, Option<i32>, u128)> {
    // Create isolated env
    let create_output = create_env(interpreter, env_dir, python_version, timeout)?;
    summary.env_create_duration_ms += create_output.duration_ms;
    attempt.env_create_duration_ms = create_output.duration_ms;
    if !create_output.success {
        let log = format!("env creation failed:\n{}", create_output.combined_output);
        fs::write(build_log_path, &log)?;
        fs::write(combined_log_path, &log)?;
        attempt.status = if create_output.timed_out {
            "build-timeout".to_string()
        } else {
            "build-failed".to_string()
        };
        attempt.log_excerpt = truncate_log(&log);
        fs::write(
            metadata_path,
            attempt_metadata(
                attempt,
                build_key,
                build_command,
                run_command,
                create_output.exit_code,
                create_output.duration_ms,
                None,
                None,
            ),
        )?;
        return Ok((log, create_output.exit_code, create_output.duration_ms));
    }

    // Ensure setuptools + wheel are present in every venv.  Modern pip (23.1+)
    // no longer bundles setuptools, causing `No module named 'setuptools'`
    // failures when packages use setup.py-based builds.
    let mut bootstrap_ms = 0u128;
    {
        let bootstrap_timeout =
            timeout.saturating_sub(Duration::from_millis(create_output.duration_ms as u64));
        let mut bootstrap_cmd = Command::new(env_python);
        bootstrap_cmd
            .arg("-m")
            .arg("pip")
            .arg("install")
            .arg("--disable-pip-version-check")
            .arg("--cache-dir")
            .arg(wheelhouse_dir)
            .arg("setuptools")
            .arg("wheel")
            .env("PYTHONNOUSERSITE", "1");
        if let Ok(result) = run_command_with_timeout(&mut bootstrap_cmd, bootstrap_timeout) {
            bootstrap_ms = result.duration_ms;
        }
    }

    // Pre-install build-time prerequisites for packages with broken setup.py
    // that import their own dependencies during egg_info (e.g., clipboard â†’ pyperclip).
    let requirements_content = fs::read_to_string(install_requirements_path)?;
    let prereqs = build_time_prerequisites(&requirements_content, python_version);
    let mut prereq_ms = 0u128;
    if !prereqs.is_empty() {
        let prereq_timeout = timeout.saturating_sub(Duration::from_millis(
            (create_output.duration_ms + bootstrap_ms) as u64,
        ));
        let mut command = Command::new(env_python);
        command
            .arg("-m")
            .arg("pip")
            .arg("install")
            .arg("--disable-pip-version-check")
            .arg("--cache-dir")
            .arg(wheelhouse_dir);
        for p in &prereqs {
            command.arg(*p);
        }
        command.env("PYTHONNOUSERSITE", "1");
        if let Ok(result) = run_command_with_timeout(&mut command, prereq_timeout) {
            prereq_ms = result.duration_ms;
        }
    }

    // Install requirements into env (use remaining budget after env creation)
    let install_timeout = timeout.saturating_sub(Duration::from_millis(
        (create_output.duration_ms + bootstrap_ms + prereq_ms) as u64,
    ));
    let install_output = run_env_install_requirements(
        env_python,
        wheelhouse_dir,
        install_requirements_path,
        install_timeout,
    )?;
    summary.install_duration_ms += install_output.duration_ms;
    let build_output = format!(
        "--- env creation ---\n{}\n--- pip install ---\n{}",
        create_output.combined_output, install_output.combined_output
    );
    fs::write(build_log_path, &build_output)?;
    let _ = context::append_context_log(
        config.benchmark_context_log.as_deref(),
        "apdr-env-build",
        &build_output,
    );

    if install_output.timed_out {
        attempt.status = "build-timeout".to_string();
        attempt.log_excerpt = truncate_log(&build_output);
        fs::write(combined_log_path, &build_output)?;
        fs::write(
            metadata_path,
            attempt_metadata(
                attempt,
                build_key,
                build_command,
                run_command,
                install_output.exit_code,
                create_output.duration_ms + install_output.duration_ms,
                None,
                None,
            ),
        )?;
        return Ok((
            build_output,
            install_output.exit_code,
            create_output.duration_ms + install_output.duration_ms,
        ));
    }

    if !install_output.success {
        // Retry with --no-build-isolation: some packages need access to
        // already-installed dependencies during their build (e.g. numpy
        // for scipy, Cython for certain C extensions).  The flag lets the
        // build process see the current env's site-packages.
        let remaining = timeout.saturating_sub(Duration::from_millis(
            (create_output.duration_ms + bootstrap_ms + prereq_ms + install_output.duration_ms)
                as u64,
        ));
        if remaining > Duration::from_secs(10) {
            let mut no_iso_cmd = Command::new(env_python);
            no_iso_cmd
                .arg("-m")
                .arg("pip")
                .arg("install")
                .arg("--disable-pip-version-check")
                .arg("--default-timeout=100")
                .arg("--no-build-isolation")
                .arg("--cache-dir")
                .arg(wheelhouse_dir)
                .arg("-r")
                .arg(install_requirements_path)
                .env("PYTHONNOUSERSITE", "1");
            if let Ok(retry_output) = run_command_with_timeout(&mut no_iso_cmd, remaining) {
                if retry_output.success {
                    let retry_build_output = format!(
                        "{}\n--- pip install --no-build-isolation (retry) ---\n{}",
                        build_output, retry_output.combined_output
                    );
                    fs::write(build_log_path, &retry_build_output)?;
                    summary.install_duration_ms += retry_output.duration_ms;
                    return Ok((
                        retry_build_output,
                        retry_output.exit_code,
                        create_output.duration_ms
                            + install_output.duration_ms
                            + retry_output.duration_ms,
                    ));
                }
            }
        }

        attempt.status = "build-failed".to_string();
        attempt.log_excerpt = truncate_log(&build_output);
        fs::write(combined_log_path, &build_output)?;
        fs::write(
            metadata_path,
            attempt_metadata(
                attempt,
                build_key,
                build_command,
                run_command,
                install_output.exit_code,
                create_output.duration_ms + install_output.duration_ms,
                None,
                None,
            ),
        )?;
        return Ok((
            build_output,
            install_output.exit_code,
            create_output.duration_ms + install_output.duration_ms,
        ));
    }

    Ok((
        build_output,
        install_output.exit_code,
        create_output.duration_ms + install_output.duration_ms,
    ))
}

pub(super) fn copy_dir_all(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let src = entry.path();
        let dst = destination.join(entry.file_name());
        if src.is_dir() {
            copy_dir_all(&src, &dst)?;
        } else {
            fs::copy(&src, &dst)?;
        }
    }
    Ok(())
}

pub(super) fn smoke_test_command(env_python: &Path, work_dir: &Path) -> Command {
    // Make env_python absolute without resolving symlinks. canonicalize()
    // would resolve the venv symlink to the system Python, breaking venv
    // site-packages detection and causing ModuleNotFoundError at import time.
    let python = if env_python.is_absolute() {
        env_python.to_path_buf()
    } else {
        work_dir.join(env_python)
    };
    let mut command = Command::new(&python);
    // smoke_test.py is in work_dir; use just the filename since current_dir is work_dir
    command
        .arg("smoke_test.py")
        .current_dir(work_dir)
        .env("PYTHONNOUSERSITE", "1")
        // Force matplotlib to use non-interactive Agg backend.  On macOS the
        // default backend_macosx requires Python to be installed as a framework
        // (Python.app), which venv pythons are not â†’ RuntimeError at import.
        .env("MPLBACKEND", "Agg");
    command
}

/// One-time install of virtualenv under Python 2.7 so we can create isolated
/// environments without relying on the host Python 3's virtualenv (which may
/// have dropped Python 2 support in versions 21+).
pub(super) fn ensure_py2_virtualenv(interpreter: &Path) {
    use std::sync::OnceLock;
    static DONE: OnceLock<bool> = OnceLock::new();
    DONE.get_or_init(|| {
        // Check if virtualenv is already available
        let check = Command::new(interpreter)
            .arg("-m")
            .arg("virtualenv")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if check.map(|s| s.success()).unwrap_or(false) {
            return true;
        }
        eprintln!("[validation] installing virtualenv under Python 2.7â€¦");
        let _ = Command::new(interpreter)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .arg("--disable-pip-version-check")
            .arg("virtualenv")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        true
    });
}

/// Check whether `uv` (Astral's fast Python package installer) is on PATH.
/// Cached via OnceLock â€” probed at most once per process.
pub(super) fn uv_available() -> bool {
    use std::sync::OnceLock;
    static UV: OnceLock<bool> = OnceLock::new();
    *UV.get_or_init(|| {
        Command::new("uv")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    })
}

pub(super) fn create_env(
    interpreter: &Path,
    env_dir: &Path,
    python_version: &str,
    timeout: Duration,
) -> io::Result<CommandResult> {
    if python_version.starts_with("2.") {
        // Python 2.7: use Python 2.7's own virtualenv.  Modern virtualenv
        // (21+) under Python 3 dropped support for creating Python 2 envs,
        // so we run virtualenv from the Python 2.7 interpreter itself.
        // Auto-install virtualenv under Python 2.7 if missing.
        ensure_py2_virtualenv(interpreter);
        let mut cmd = Command::new(interpreter);
        cmd.arg("-m").arg("virtualenv").arg(env_dir);
        return run_command_with_timeout(&mut cmd, timeout);
    }

    // Python 3.x: try uv first (10-100x faster), then stdlib venv, then virtualenv.
    // Pass the version string (e.g. "3.9") instead of the interpreter path â€” uv's
    // managed Pythons fail inspection when given a raw path but work fine with a
    // version request.
    if uv_available() {
        let mut uv_cmd = Command::new("uv");
        uv_cmd
            .arg("venv")
            .arg("--seed") // install pip+setuptools so fallback `python -m pip` works
            .arg("--python")
            .arg(python_version)
            .arg(env_dir);
        let uv_result = run_command_with_timeout(&mut uv_cmd, timeout)?;
        if uv_result.success {
            return Ok(uv_result);
        }
        // uv failed â€” clean up partial env and fall through
        let _ = fs::remove_dir_all(env_dir);
    }

    let mut venv_command = Command::new(interpreter);
    venv_command.arg("-m").arg("venv").arg(env_dir);
    let venv_output = run_command_with_timeout(&mut venv_command, timeout)?;
    if venv_output.success || venv_output.timed_out {
        return Ok(venv_output);
    }

    let remaining = timeout.saturating_sub(Duration::from_millis(venv_output.duration_ms as u64));
    if remaining.is_zero() {
        return Ok(venv_output);
    }

    let Some(host) = host_python_for_metadata() else {
        return Ok(venv_output);
    };
    let _ = fs::remove_dir_all(env_dir);
    let mut fallback = Command::new(host);
    fallback
        .arg("-m")
        .arg("virtualenv")
        .arg("-p")
        .arg(interpreter)
        .arg(env_dir);
    let fallback_output = run_command_with_timeout(&mut fallback, remaining)?;
    let combined = format!(
        "--- python -m venv ---\n{}\n--- python -m virtualenv fallback ---\n{}",
        venv_output.combined_output, fallback_output.combined_output
    );

    Ok(CommandResult {
        success: fallback_output.success,
        combined_output: combined,
        timed_out: venv_output.timed_out || fallback_output.timed_out,
        exit_code: fallback_output.exit_code.or(venv_output.exit_code),
        duration_ms: venv_output.duration_ms + fallback_output.duration_ms,
    })
}

pub(super) fn run_env_install_requirements(
    env_python: &Path,
    cache_dir: &Path,
    requirements_path: &Path,
    timeout: Duration,
) -> io::Result<CommandResult> {
    // Try uv pip install first (10-100x faster than pip).
    // Skip for Python 2 envs (uv doesn't support them).
    if uv_available() {
        let mut uv_cmd = Command::new("uv");
        uv_cmd
            .arg("pip")
            .arg("install")
            .arg("--python")
            .arg(env_python)
            .arg("--cache-dir")
            .arg(cache_dir)
            .arg("-r")
            .arg(requirements_path);
        let uv_result = run_command_with_timeout(&mut uv_cmd, timeout)?;
        if uv_result.success {
            return Ok(uv_result);
        }
        // Fall through to pip on uv failure
    }

    // First pass: try --only-binary :all: to avoid compilation failures.
    // Many build failures come from missing C headers (mysql-dev, libpq-dev, etc.)
    // and pre-built wheels sidestep these entirely.
    {
        let mut binary_cmd = Command::new(env_python);
        binary_cmd
            .arg("-m")
            .arg("pip")
            .arg("install")
            .arg("--disable-pip-version-check")
            .arg("--default-timeout=100")
            .arg("--only-binary")
            .arg(":all:")
            .arg("--cache-dir")
            .arg(cache_dir)
            .arg("-r")
            .arg(requirements_path)
            .env("PYTHONNOUSERSITE", "1");
        let half_timeout = Duration::from_millis((timeout.as_millis() / 3) as u64);
        if let Ok(result) = run_command_with_timeout(&mut binary_cmd, half_timeout) {
            if result.success {
                return Ok(result);
            }
        }
        // Fall through to normal install (allows source builds)
    }

    let mut command = Command::new(env_python);
    command
        .arg("-m")
        .arg("pip")
        .arg("install")
        .arg("--disable-pip-version-check")
        .arg("--default-timeout=100")
        .arg("--cache-dir")
        .arg(cache_dir)
        .arg("-r")
        .arg(requirements_path)
        .env("PYTHONNOUSERSITE", "1");
    let result = run_command_with_timeout(&mut command, timeout)?;
    if result.success {
        return Ok(result);
    }

    // Last resort: try with --pre to allow pre-release versions.
    // Some packages only have pre-release wheels for certain Python versions.
    let remaining = timeout.saturating_sub(Duration::from_millis(result.duration_ms as u64));
    if remaining > Duration::from_secs(5) {
        let mut pre_cmd = Command::new(env_python);
        pre_cmd
            .arg("-m")
            .arg("pip")
            .arg("install")
            .arg("--disable-pip-version-check")
            .arg("--default-timeout=100")
            .arg("--pre")
            .arg("--cache-dir")
            .arg(cache_dir)
            .arg("-r")
            .arg(requirements_path)
            .env("PYTHONNOUSERSITE", "1");
        let pre_result = run_command_with_timeout(&mut pre_cmd, remaining)?;
        if pre_result.success {
            return Ok(pre_result);
        }
    }

    Ok(result)
}

pub(super) fn env_python_path(env_dir: &Path) -> PathBuf {
    if cfg!(windows) {
        env_dir.join("Scripts").join("python.exe")
    } else {
        env_dir.join("bin").join("python")
    }
}

pub(super) fn env_site_packages_dir(env_dir: &Path, python_version: &str) -> PathBuf {
    if cfg!(windows) {
        env_dir.join("Lib").join("site-packages")
    } else {
        env_dir
            .join("lib")
            .join(format!("python{python_version}"))
            .join("site-packages")
    }
}

pub(super) fn validated_env_cache_path(validated_envs_dir: &Path, build_key: &str) -> PathBuf {
    validated_envs_dir.join(build_key.replace(':', "-"))
}

pub(super) fn validated_env_archive_path(validated_envs_dir: &Path, build_key: &str) -> PathBuf {
    validated_envs_dir.join(format!("{}.tar.zst", build_key.replace(':', "-")))
}

pub(super) fn save_validated_env(
    validated_envs_dir: &Path,
    build_key: &str,
    env_dir: &Path,
) -> io::Result<()> {
    let archive_path = validated_env_archive_path(validated_envs_dir, build_key);
    let legacy_dir = validated_env_cache_path(validated_envs_dir, build_key);
    // Already cached (archive or legacy dir)
    if archive_path.exists() {
        return maintenance::touch_archive_marker(&archive_path);
    }
    if legacy_dir.exists() {
        return maintenance::touch_validated_env_cache_entry(&legacy_dir);
    }
    // Keep .hot uncompressed copy for fast CoW clone on next hit (macOS APFS).
    // On macOS the .hot dir is the primary cache; archive is created in background
    // from the .hot copy (the caller deletes the original env_dir immediately).
    if cfg!(target_os = "macos") {
        let hot = maintenance::hot_dir_path(&archive_path);
        if maintenance::try_cow_clone(env_dir, &hot).unwrap_or(false) {
            // Spawn archive compression from the .hot copy on a background thread.
            // The caller will delete the original env_dir, so we must use .hot.
            let archive_path_owned = archive_path.to_path_buf();
            let hot_owned = hot.to_path_buf();
            std::thread::spawn(move || {
                if maintenance::compress_env_to_archive(&hot_owned, &archive_path_owned).is_ok() {
                    let _ = maintenance::touch_archive_marker(&archive_path_owned);
                }
            });
            return Ok(());
        }
    }
    // Non-macOS or CoW clone failed: compress synchronously from env_dir
    // (caller deletes env_dir after we return, so we can't defer).
    if maintenance::compress_env_to_archive(env_dir, &archive_path).is_ok() {
        let _ = maintenance::touch_archive_marker(&archive_path);
    }
    Ok(())
}

/// Packages whose setup.py imports their own dependencies at build time.
/// These must be pre-installed before `pip install -r requirements.txt` so
/// that the egg_info / setup.py phase can succeed.
pub(super) fn build_time_prerequisites<'a>(
    requirements: &str,
    python_version: &str,
) -> Vec<&'a str> {
    let is_py2 = python_version.starts_with("2.");
    let mut prereqs = Vec::new();
    for line in requirements.lines() {
        let pkg = line
            .split(&['=', '>', '<', '!', '[', ' '][..])
            .next()
            .unwrap_or("")
            .trim();
        if pkg.eq_ignore_ascii_case("clipboard") {
            // clipboard's setup.py imports pyperclip at build time.
            // pyperclip >=1.9 requires setuptools>=61, unavailable on Python 2.7.
            if is_py2 {
                prereqs.push("pyperclip>=1.5,<1.9");
            } else {
                prereqs.push("pyperclip");
            }
        }
        if pkg.eq_ignore_ascii_case("editor") && is_py2 {
            // editor's setup.py does `from pathlib import Path` which needs
            // the pathlib backport on Python 2.7.
            prereqs.push("pathlib");
        }
    }
    prereqs.dedup();
    prereqs
}

pub(super) const PACKAGE_REPOSITORY_CATALOG_SCRIPT: &str = r#"
import importlib.metadata as metadata
import os
import shutil
import sys

site_packages = os.path.abspath(sys.argv[1])
repository_root = os.path.abspath(sys.argv[2])
python_version = sys.argv[3]

def normalize(value):
    return value.strip().replace('_', '-').replace('.', '-').lower()

def safe_copy(source, destination):
    if os.path.exists(destination):
        return
    if os.path.isdir(source):
        shutil.copytree(source, destination)
    else:
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copy2(source, destination)

for dist in metadata.distributions(path=[site_packages]):
    name = (dist.metadata.get('Name') or '').strip()
    version = (dist.version or '').strip()
    if not name or not version:
        continue
    roots = set()
    files = list(dist.files or [])
    for item in files:
        parts = getattr(item, 'parts', tuple(str(item).split('/')))
        if not parts:
            continue
        roots.add(parts[0])
    if not roots:
        continue
    artifact_dir = os.path.join(repository_root, python_version, normalize(name), version)
    os.makedirs(artifact_dir, exist_ok=True)
    copied = False
    for root_name in sorted(roots):
        source = os.path.join(site_packages, root_name)
        destination = os.path.join(artifact_dir, root_name)
        if not os.path.exists(source):
            continue
        safe_copy(source, destination)
        copied = True
    if copied:
        print(f"{normalize(name)}\t{version}\t{artifact_dir}")
"#;

#[allow(clippy::too_many_arguments)]
pub(super) fn attempt_metadata(
    attempt: &ValidationAttempt,
    build_key: &str,
    build_command: &str,
    run_command: &str,
    build_exit_code: Option<i32>,
    build_duration_ms: u128,
    run_exit_code: Option<i32>,
    run_duration_ms: Option<u128>,
) -> String {
    format!(
        "attempt_index: {}\npython_version: {}\nvalidation_backend: {}\nstatus: {}\nenv_label: {}\nenv_dir: {}\nenv_create_duration_ms: {}\nbuild_key: {}\nused_cached_env: {}\nvalidated_env_cache_hit: {}\nused_cached_lockfile: {}\nerror_type: {}\nconflict_class: {}\nfix_applied: {}\nbuild_command: {}\nbuild_exit_code: {}\nbuild_duration_ms: {}\nrun_command: {}\nrun_exit_code: {}\nrun_duration_ms: {}\nartifact_dir: {}\nexecuted_dockerfile_path: {}\ndocker_build_command_path: {}\ndocker_run_command_path: {}\nexecuted_image_ref: {}\nimage_handoff_verified: {}\nimage_inspect_path: {}\n",
        attempt.attempt_index,
        attempt.python_version,
        if attempt.validation_backend.is_empty() { "env" } else { &attempt.validation_backend },
        attempt.status,
        attempt.env_label.as_deref().unwrap_or("--"),
        attempt.env_dir.as_deref().unwrap_or("--"),
        attempt.env_create_duration_ms,
        build_key,
        attempt.used_cached_env,
        attempt.validated_env_cache_hit,
        attempt.used_cached_lockfile,
        attempt.error_type.as_deref().unwrap_or("--"),
        attempt.conflict_class.as_deref().unwrap_or("--"),
        attempt.fix_applied.as_deref().unwrap_or("--"),
        build_command,
        build_exit_code
            .map(|value| value.to_string())
            .unwrap_or_else(|| "--".to_string()),
        build_duration_ms,
        run_command,
        run_exit_code
            .map(|value| value.to_string())
            .unwrap_or_else(|| "--".to_string()),
        run_duration_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "--".to_string()),
        attempt.artifact_dir.as_deref().unwrap_or("--"),
        attempt.executed_dockerfile_path.as_deref().unwrap_or("--"),
        attempt.docker_build_command_path.as_deref().unwrap_or("--"),
        attempt.docker_run_command_path.as_deref().unwrap_or("--"),
        attempt.executed_image_ref.as_deref().unwrap_or("--"),
        attempt.image_handoff_verified,
        attempt.image_inspect_path.as_deref().unwrap_or("--"),
    )
}
