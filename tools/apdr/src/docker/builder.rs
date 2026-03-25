use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::build_cache;
use crate::cache::maintenance;
use crate::cache::store::CacheStore;
use crate::context;
use crate::docker::{smoke_test, system_deps, templates};
use crate::{
    ResolveConfig, ValidationAttempt, ValidationSummary, VALIDATION_BACKEND_DOCKER,
    VALIDATION_BACKEND_ENV, VALIDATION_BACKEND_LLM,
};

struct CommandResult {
    success: bool,
    combined_output: String,
    timed_out: bool,
    exit_code: Option<i32>,
    duration_ms: u128,
}

pub fn validate_requirements(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    store: &mut CacheStore,
) -> io::Result<ValidationSummary> {
    match config.validation_backend() {
        VALIDATION_BACKEND_DOCKER => validate_requirements_docker(
            snippet_path,
            requirements_txt,
            imports,
            candidate_versions,
            attempt_offset,
            config,
            store,
        ),
        VALIDATION_BACKEND_LLM => validate_requirements_llm(
            snippet_path,
            requirements_txt,
            imports,
            candidate_versions,
            attempt_offset,
            config,
            store,
        ),
        _ => {
            let mut summary = validate_requirements_env(
                snippet_path,
                requirements_txt,
                imports,
                candidate_versions,
                attempt_offset,
                config,
                store,
            )?;
            if summary.succeeded {
                return Ok(summary);
            }
            // Fall back to Docker when env validation fails due to:
            // 1. Missing local Python interpreter (e.g. Python 3.12 not installed)
            // 2. System-dep build errors (missing C headers / libraries)
            // 3. Build timeout (packages compiling from source on Windows;
            //    Docker/Linux has pre-built wheels)
            // Docker images (python:X.Y-slim) have the right interpreter and
            // can install system deps via apt-get.
            let reqs_have_sys_deps =
                !system_deps::infer_system_deps_from_requirements(requirements_txt).is_empty();
            if command_on_path("docker")
                && (env_has_system_dep_failure(&summary)
                    || env_has_interpreter_failure(&summary)
                    || env_has_build_timeout(&summary)
                    || reqs_have_sys_deps)
            {
                let reason = if env_has_interpreter_failure(&summary) {
                    "missing local Python interpreter"
                } else if env_has_build_timeout(&summary) {
                    "build timeout (Docker has pre-built wheels)"
                } else if reqs_have_sys_deps {
                    "packages require system libraries"
                } else {
                    "system-dep build errors"
                };
                eprintln!(
                    "[validation] env failed with {reason}, retrying with Docker"
                );
                let docker_offset = attempt_offset + summary.attempts.len();
                let mut docker_summary = validate_requirements_docker(
                    snippet_path,
                    requirements_txt,
                    imports,
                    candidate_versions,
                    docker_offset,
                    config,
                    store,
                )?;
                // Merge env attempts into docker summary to keep full history
                let mut combined = std::mem::take(&mut summary.attempts);
                combined.append(&mut docker_summary.attempts);
                docker_summary.attempts = combined;
                summary = docker_summary;
                if summary.succeeded {
                    return Ok(summary);
                }
            }
            Ok(summary)
        }
    }
}

fn validate_requirements_env(
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
    // Pre-generate the smoke test script once — it's identical across Python
    // version attempts since imports and execute_snippet don't change.
    let smoke_test_script = smoke_test::generate(imports, config.execute_snippet);

    // Minimum time each attempt gets — enough for env create + pip install of
    // moderate packages.  Prevents later attempts from getting a near-zero budget
    // when earlier attempts consumed most of the total.
    let min_attempt_budget = Duration::from_secs(120);

    for (local_index, python_version) in candidate_versions.iter().enumerate() {
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
        let build_key = build_cache::key_for(requirements_txt, python_version);
        summary.lockfile_key = Some(build_key.clone());
        summary.build_cache_key = Some(build_key.clone());
        let env_label = sanitized_env_label(&build_key, python_version);
        let work_dir = context::attempt_dir(&config.output_dir, attempt_index, python_version);
        fs::create_dir_all(&work_dir)?;
        let env_dir = work_dir.join("env");
        // Defensive cleanup: remove any stale env left by a previously killed process.
        if env_dir.exists() {
            let _ = fs::remove_dir_all(&env_dir);
        }

        fs::write(work_dir.join("requirements.txt"), requirements_txt)?;
        fs::write(work_dir.join("smoke_test.py"), &smoke_test_script)?;
        fs::copy(snippet_path, work_dir.join("snippet.py"))?;
        let build_log_path = work_dir.join("build.log");
        let run_log_path = work_dir.join("run.log");
        let combined_log_path = work_dir.join("combined.log");
        let metadata_path = work_dir.join("metadata.txt");
        let context_snapshot_path = work_dir.join("benchmark-context-tail.txt");
        let interpreter = match ensure_python_interpreter(python_version) {
            Ok(path) => path,
            Err(detail) => {
                fs::write(&build_log_path, &detail)?;
                fs::write(&run_log_path, "")?;
                fs::write(&combined_log_path, &detail)?;
                let attempt = ValidationAttempt {
                    attempt_index,
                    python_version: python_version.clone(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    env_label: Some(env_label.clone()),
                    status: "build-failed".to_string(),
                    log_excerpt: truncate_log(&detail),
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
                        &format!("python{} unavailable", python_version),
                        "--",
                        None,
                        0,
                        None,
                        None,
                    ),
                )?;
                summary.attempts.push(attempt);
                continue;
            }
        };

        let install_requirements_path = work_dir.join("requirements-install.txt");
        fs::write(&install_requirements_path, requirements_txt)?;

        let env_python = env_python_path(&env_dir);
        let env_create_command = if python_version.starts_with("2.") {
            format!(
                "{} -m virtualenv {}",
                interpreter.display(),
                env_dir.display()
            )
        } else {
            format!("{} -m venv {}", interpreter.display(), env_dir.display())
        };
        let env_install_command = format!(
            "{} -m pip install --disable-pip-version-check --default-timeout=100 --cache-dir {} -r {}",
            env_python.display(),
            wheelhouse_dir.display(),
            install_requirements_path.display()
        );
        let build_command = format!("{}\n{}", env_create_command, env_install_command);
        let run_command = format!(
            "{} {}",
            env_python.display(),
            work_dir.join("smoke_test.py").display()
        );
        fs::write(work_dir.join("env-create.command.txt"), &env_create_command)?;
        fs::write(
            work_dir.join("env-install.command.txt"),
            &env_install_command,
        )?;
        fs::write(work_dir.join("env-run.command.txt"), &run_command)?;
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
            validation_backend: VALIDATION_BACKEND_ENV.to_string(),
            env_label: Some(env_label.clone()),
            env_dir: Some(env_dir.display().to_string()),
            used_cached_lockfile: store.lockfile(&build_key).is_some(),
            artifact_dir: Some(work_dir.display().to_string()),
            build_log_path: Some(build_log_path.display().to_string()),
            run_log_path: Some(run_log_path.display().to_string()),
            combined_log_path: Some(combined_log_path.display().to_string()),
            metadata_path: Some(metadata_path.display().to_string()),
            context_snapshot_path: Some(context_snapshot_path.display().to_string()),
            ..Default::default()
        };
        summary.validation_backend = VALIDATION_BACKEND_ENV.to_string();

        // Check validated-env cache: prefer compressed archive, fall back to legacy dir
        let cached_archive = validated_env_archive_path(&validated_envs_dir, &build_key);
        let cached_env_dir = validated_env_cache_path(&validated_envs_dir, &build_key);
        let (cache_hit, cache_is_archive) = if cached_archive.exists() {
            (true, true)
        } else if cached_env_dir.exists()
            && (cached_env_dir.join("bin").exists() || cached_env_dir.join("Scripts").exists())
        {
            (true, false)
        } else {
            (false, false)
        };
        attempt.used_cached_env = cache_hit;
        attempt.validated_env_cache_hit = cache_hit;

        let (build_logs, build_exit_code, build_duration_ms) = if cache_hit {
            let restore_result = if cache_is_archive {
                // Try CoW clone from .hot sibling first (near-instant on APFS)
                let hot = maintenance::hot_dir_path(&cached_archive);
                if hot.exists() {
                    match maintenance::try_cow_clone(&hot, &env_dir) {
                        Ok(true) => Ok(()),
                        _ => maintenance::extract_archive_to_env(&cached_archive, &env_dir),
                    }
                } else {
                    maintenance::extract_archive_to_env(&cached_archive, &env_dir)
                }
            } else {
                copy_dir_all(&cached_env_dir, &env_dir)
            };
            // Verify the extracted env has a usable Python binary
            let restore_result = restore_result.and_then(|()| {
                let has_bin = env_dir.join("bin").exists() || env_dir.join("Scripts").exists();
                if has_bin {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Extracted env missing bin/ directory: {}", env_dir.display()),
                    ))
                }
            });
            match restore_result {
                Ok(()) => {
                    if cache_is_archive {
                        let _ = maintenance::touch_archive_marker(&cached_archive);
                    } else {
                        let _ = maintenance::touch_validated_env_cache_entry(&cached_env_dir);
                    }
                    let source = if cache_is_archive {
                        cached_archive.display().to_string()
                    } else {
                        cached_env_dir.display().to_string()
                    };
                    let log = format!("reused cached validated env from {}", source);
                    fs::write(&build_log_path, &log)?;
                    (log, None, 0_u128)
                }
                Err(err) => {
                    // Cache restore failed; fall through to fresh env creation
                    let _ = fs::remove_dir_all(&env_dir);
                    attempt.used_cached_env = false;
                    attempt.validated_env_cache_hit = false;
                    let attempt_timeout = total_budget.saturating_sub(validation_started.elapsed());
                    let result = create_and_install_env(
                        &interpreter,
                        python_version,
                        &env_dir,
                        &env_python,
                        &wheelhouse_dir,
                        &install_requirements_path,
                        &build_log_path,
                        &combined_log_path,
                        &metadata_path,
                        &build_command,
                        &run_command,
                        &build_key,
                        attempt_timeout,
                        config,
                        &mut attempt,
                        &mut summary,
                    )?;
                    if !attempt.status.is_empty() {
                        let _ = fs::remove_dir_all(&env_dir);
                        summary.attempts.push(attempt);
                        continue;
                    }
                    let mut log = format!("(cache restore failed: {})\n", err);
                    log.push_str(&result.0);
                    (log, result.1, result.2)
                }
            }
        } else {
            let attempt_timeout = total_budget.saturating_sub(validation_started.elapsed());
            let result = create_and_install_env(
                &interpreter,
                python_version,
                &env_dir,
                &env_python,
                &wheelhouse_dir,
                &install_requirements_path,
                &build_log_path,
                &combined_log_path,
                &metadata_path,
                &build_command,
                &run_command,
                &build_key,
                attempt_timeout,
                config,
                &mut attempt,
                &mut summary,
            )?;
            if !attempt.status.is_empty() {
                let _ = fs::remove_dir_all(&env_dir);
                summary.attempts.push(attempt);
                continue;
            }
            result
        };

        let mut smoke_command = smoke_test_command(&env_python, &work_dir);
        let smoke_timeout = total_budget.saturating_sub(validation_started.elapsed());
        let run_output = run_command_with_timeout(&mut smoke_command, smoke_timeout)?;
        summary.smoke_duration_ms += run_output.duration_ms;
        let combined = if build_logs.is_empty() {
            run_output.combined_output.clone()
        } else {
            format!("{build_logs}\n{}", run_output.combined_output)
        };
        fs::write(&run_log_path, &run_output.combined_output)?;
        fs::write(&combined_log_path, &combined)?;
        let _ = context::append_context_log(
            config.benchmark_context_log.as_deref(),
            "apdr-env-run",
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
            let _ = fs::remove_dir_all(&env_dir);
            summary.attempts.push(attempt);
            continue;
        }

        if run_output.success {
            attempt.status = "passed".to_string();
            attempt.log_excerpt = truncate_log(&combined);
            let site_packages = env_site_packages_dir(&env_dir, python_version);
            if config.package_repository_cache_enabled {
                let _ = catalog_package_repository(store, python_version, &site_packages);
            }
            // Save validated env to cache for future reuse
            if config.validated_env_cache_max_entries > 0 {
                let _ = save_validated_env(&validated_envs_dir, &build_key, &env_dir);
                let _ = maintenance::prune_validated_env_cache(
                    &validated_envs_dir,
                    config.validated_env_cache_max_entries,
                    config.validated_env_cache_max_bytes,
                );
            }
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
            summary.build_image_id = None;
            summary.succeeded = true;
            summary.attempts.push(attempt);
            // Clean up the venv to reclaim disk space (already saved to validated-env cache above)
            let _ = fs::remove_dir_all(&env_dir);
            return Ok(summary);
        }

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
        // Clean up the venv to reclaim disk space (logs and metadata are preserved)
        let _ = fs::remove_dir_all(&env_dir);
    }

    Ok(summary)
}

fn validate_requirements_llm(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    store: &mut CacheStore,
) -> io::Result<ValidationSummary> {
    // Phase 1: Try the traditional env-based validation first
    let mut env_summary = validate_requirements_env(
        snippet_path,
        requirements_txt,
        imports,
        candidate_versions,
        attempt_offset,
        config,
        store,
    )?;

    if env_summary.succeeded {
        return Ok(env_summary);
    }

    // Phase 2: Env validation failed — fall back to the LangGraph multi-agent pipeline
    eprintln!(
        "[llm-resolver] env validation failed ({} attempt(s)), trying LangGraph agent\u{2026}",
        env_summary.attempts.len()
    );
    env_summary.agent_invocations += 1;
    if let Some(mut agent_summary) = attempt_langgraph_agent(
        snippet_path,
        requirements_txt,
        imports,
        candidate_versions,
        config,
    ) {
        agent_summary.agent_invocations = env_summary.agent_invocations;
        let mut combined_attempts = env_summary.attempts;
        combined_attempts.append(&mut agent_summary.attempts);
        agent_summary.attempts = combined_attempts;
        agent_summary.validation_backend = VALIDATION_BACKEND_LLM.to_string();
        return Ok(agent_summary);
    }

    // Agent unavailable or also failed — return the original env result
    eprintln!("[llm-resolver] LangGraph agent unavailable or failed, returning env result");
    Ok(env_summary)
}

fn attempt_langgraph_agent(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    config: &ResolveConfig,
) -> Option<ValidationSummary> {
    // Find the docker_agent Python module relative to this binary's directory.
    // The module lives at tools/apdr/docker_agent/ — we discover it by walking
    // up from the binary's directory or from CARGO_MANIFEST_DIR at test time.
    let agent_parent = find_docker_agent_parent()?;

    // Check that python3 can import the module
    let check = Command::new("python3")
        .args(["-c", "import docker_agent"])
        .env("PYTHONPATH", &agent_parent)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .ok()?;
    if !check.status.success() {
        eprintln!("[docker-agent] python3 cannot import docker_agent, skipping LLM agent");
        return None;
    }

    // Write JSON config for the Python agent
    let agent_output_dir = config.output_dir.join("agent");
    fs::create_dir_all(&agent_output_dir).ok()?;
    let config_path = agent_output_dir.join("agent-config.json");

    let config_json = format!(
        r#"{{
  "snippet_path": "{}",
  "requirements_txt": "{}",
  "imports": [{}],
  "candidate_versions": [{}],
  "llm_provider": "{}",
  "llm_model": "{}",
  "llm_base_url": "{}",
  "cache_path": "{}",
  "output_dir": "{}",
  "validation_timeout_secs": {},
  "max_attempts": 5
}}"#,
        snippet_path.display().to_string().replace('\\', "\\\\").replace('"', "\\\""),
        requirements_txt.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n"),
        imports
            .iter()
            .map(|s| format!("\"{}\"", s.replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(", "),
        candidate_versions
            .iter()
            .map(|s| format!("\"{}\"", s))
            .collect::<Vec<_>>()
            .join(", "),
        config.llm_provider,
        config.llm_model,
        config.llm_base_url,
        config.cache_path.display(),
        agent_output_dir.display(),
        config.validation_timeout.as_secs(),
    );
    fs::write(&config_path, &config_json).ok()?;

    eprintln!("[docker-agent] invoking LangGraph multi-agent pipeline…");
    let mut cmd = Command::new("python3");
    cmd.arg("-m")
        .arg("docker_agent")
        .arg("--config")
        .arg(&config_path)
        .env("PYTHONPATH", &agent_parent)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let agent_output = run_command_with_timeout(&mut cmd, config.validation_timeout).ok()?;

    if !agent_output.success {
        eprintln!(
            "[docker-agent] agent process failed (exit={}): {}",
            agent_output
                .exit_code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            truncate_log(&agent_output.combined_output)
        );
        return None;
    }

    // Parse JSON result from stdout (last line is the JSON)
    let stdout = &agent_output.combined_output;
    let json_line = stdout.lines().last()?;
    parse_agent_result(json_line)
}

fn find_docker_agent_parent() -> Option<PathBuf> {
    // At test/dev time, CARGO_MANIFEST_DIR points to tools/apdr/
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let candidate = PathBuf::from(&manifest);
        if candidate.join("docker_agent").join("__init__.py").exists() {
            return Some(candidate);
        }
    }
    // At runtime, try relative to the binary
    if let Ok(exe) = std::env::current_exe() {
        for ancestor in exe.ancestors().skip(1).take(5) {
            let candidate = ancestor.join("docker_agent").join("__init__.py");
            if candidate.exists() {
                return Some(ancestor.to_path_buf());
            }
        }
    }
    // Fallback: try CWD
    if let Ok(cwd) = std::env::current_dir() {
        if cwd.join("docker_agent").join("__init__.py").exists() {
            return Some(cwd);
        }
    }
    None
}

fn parse_agent_result(json_str: &str) -> Option<ValidationSummary> {
    // Minimal JSON parsing without pulling in serde for this one spot.
    // The agent output is: {"status":"passed","selected_python_version":"3.11",
    //   "final_requirements":"...","confidence":0.85,"attempts":[...],"total_duration_ms":123}
    let trimmed = json_str.trim();
    if !trimmed.starts_with('{') {
        return None;
    }

    let status = extract_json_string(trimmed, "status")?;
    if status != "passed" {
        eprintln!("[docker-agent] agent returned status={status}, falling back to deterministic");
        return None;
    }

    let mut summary = ValidationSummary::default();
    summary.succeeded = true;
    summary.validation_backend = VALIDATION_BACKEND_DOCKER.to_string();
    summary.selected_python_version = extract_json_string(trimmed, "selected_python_version");

    if let Some(dur) = extract_json_number(trimmed, "total_duration_ms") {
        summary.validation_duration_ms = dur as u128;
    }

    // The agent may have modified requirements; we record that info but
    // it does not change the resolved list (the Rust side already resolved).
    Some(summary)
}

fn extract_json_string(json: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\"", key);
    let pos = json.find(&needle)?;
    let after_key = &json[pos + needle.len()..];
    // Skip whitespace and colon
    let after_colon = after_key.trim_start().strip_prefix(':')?;
    let after_ws = after_colon.trim_start();
    if after_ws.starts_with("null") {
        return None;
    }
    let value_start = after_ws.strip_prefix('"')?;
    let end = value_start.find('"')?;
    Some(value_start[..end].to_string())
}

fn extract_json_number(json: &str, key: &str) -> Option<f64> {
    let needle = format!("\"{}\"", key);
    let pos = json.find(&needle)?;
    let after_key = &json[pos + needle.len()..];
    let after_colon = after_key.trim_start().strip_prefix(':')?;
    let after_ws = after_colon.trim_start();
    // Read digits, dots, minus
    let end = after_ws
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-')
        .unwrap_or(after_ws.len());
    after_ws[..end].parse::<f64>().ok()
}

fn validate_requirements_docker(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    store: &mut CacheStore,
) -> io::Result<ValidationSummary> {
    let mut summary = ValidationSummary::default();
    summary.validation_backend = VALIDATION_BACKEND_DOCKER.to_string();
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
    // Pre-generate once — identical across all Python version / retry attempts.
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
            let work_dir =
                context::attempt_dir(&config.output_dir, attempt_index, python_version);
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
            let container_name =
                docker_container_name(&build_key, python_version, attempt_index);
            let run_command =
                format!("docker run --rm --name {container_name} {image_tag}");

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

                // No new deps found or timed out — record failure and move to next Python version
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

            // Build succeeded — run smoke test
            let build_logs = build_output.combined_output;
            let build_exit_code = build_output.exit_code;
            let build_duration_ms = build_output.duration_ms;

            let run_timeout = total_budget.saturating_sub(validation_started.elapsed());
            let mut run = Command::new("docker");
            run.arg("run")
                .arg("--rm")
                .arg("--name")
                .arg(&container_name)
                .arg(&image_tag);
            let run_output = run_command_with_timeout(&mut run, run_timeout)?;
            summary.smoke_duration_ms += run_output.duration_ms;
            let combined = if build_logs.is_empty() {
                run_output.combined_output.clone()
            } else {
                format!("{build_logs}\n{}", run_output.combined_output)
            };
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
                return Ok(summary);
            }

            // Runtime failure — no system dep retry for runtime failures
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
fn cleanup_docker_image(image_tag: &str) {
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

/// Prune dangling images and build cache. Runs silently.
fn cleanup_docker_dangling() {
    let _ = Command::new("docker")
        .args(["image", "prune", "-f"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    // Also trim build cache — keep last 2GB to avoid re-downloading base images
    let _ = Command::new("docker")
        .args(["builder", "prune", "-f", "--keep-storage", "2g"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn docker_backend_unavailable(
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

/// Create env and install requirements. Returns (build_logs, build_exit_code, build_duration_ms).
/// If the build fails, sets attempt.status; caller must check and skip to next version.
#[allow(clippy::too_many_arguments)]
fn create_and_install_env(
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
    // that import their own dependencies during egg_info (e.g., clipboard → pyperclip).
    let requirements_content = fs::read_to_string(install_requirements_path)?;
    let prereqs = build_time_prerequisites(&requirements_content, python_version);
    let mut prereq_ms = 0u128;
    if !prereqs.is_empty() {
        let prereq_timeout =
            timeout.saturating_sub(Duration::from_millis((create_output.duration_ms + bootstrap_ms) as u64));
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
            (create_output.duration_ms + bootstrap_ms + prereq_ms + install_output.duration_ms) as u64,
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
                        create_output.duration_ms + install_output.duration_ms + retry_output.duration_ms,
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

fn catalog_package_repository(
    store: &mut CacheStore,
    python_version: &str,
    site_packages_dir: &Path,
) -> io::Result<()> {
    let repository_dir = store.cache_path.join("package-repository");
    fs::create_dir_all(&repository_dir)?;
    let Some(host_python) = host_python_for_metadata() else {
        return Ok(());
    };
    let output = Command::new(host_python)
        .arg("-c")
        .arg(PACKAGE_REPOSITORY_CATALOG_SCRIPT)
        .arg(site_packages_dir)
        .arg(&repository_dir)
        .arg(python_version)
        .output()?;
    if !output.status.success() {
        return Ok(());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parts = trimmed.split('\t').collect::<Vec<_>>();
        if parts.len() < 3 {
            continue;
        }
        let _ = store.save_package_artifact(python_version, parts[0], parts[1], parts[2]);
    }
    Ok(())
}

fn combined_output(stdout: &[u8], stderr: &[u8]) -> String {
    let mut output = String::from_utf8_lossy(stdout).to_string();
    let stderr = String::from_utf8_lossy(stderr);
    if !stderr.trim().is_empty() {
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&stderr);
    }
    output
}

fn run_command_with_timeout(command: &mut Command, timeout: Duration) -> io::Result<CommandResult> {
    // Redirect stdout+stderr to a temp file instead of piping.
    // On Windows, docker.exe (BuildKit) can deadlock when its output is piped
    // because docker-buildx.exe inherits the pipe handles and keeps them open
    // even after docker.exe is done writing.  File redirection avoids this.
    let tmp_out = tempfile::NamedTempFile::new()?;
    let out_file = fs::File::create(tmp_out.path())?;
    let err_file = out_file.try_clone()?;

    let mut child = command
        .stdout(out_file)
        .stderr(err_file)
        .spawn()?;
    let started = Instant::now();

    // Adaptive polling: start fast (50ms) for short commands, back off
    // exponentially (cap 1000ms) to reduce CPU wake-ups for long installs.
    let mut poll_interval_ms: u64 = 50;
    let (timed_out, status) = loop {
        match child.try_wait()? {
            Some(status) => break (false, status),
            None if started.elapsed() >= timeout => {
                let _ = child.kill();
                let status = child.wait()?;
                break (true, status);
            }
            None => {
                thread::sleep(Duration::from_millis(poll_interval_ms));
                poll_interval_ms = (poll_interval_ms * 3 / 2).min(1000);
            }
        }
    };

    let combined = fs::read_to_string(tmp_out.path()).unwrap_or_default();
    let success = !timed_out && status.success();

    Ok(CommandResult {
        success,
        combined_output: combined,
        timed_out,
        exit_code: status.code(),
        duration_ms: started.elapsed().as_millis(),
    })
}

fn truncate_log(log: &str) -> String {
    let lines = log
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let start = lines.len().saturating_sub(25);
    lines[start..].join("\n")
}

fn sanitized_env_label(build_key: &str, python_version: &str) -> String {
    format!(
        "apdr-env:{}-py{}",
        build_key.replace(':', "-"),
        python_version.replace('.', "_")
    )
}

fn docker_image_tag(build_key: &str, python_version: &str) -> String {
    format!(
        "apdr-validate:py{}-{}",
        python_version.replace('.', "_"),
        build_key
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                    ch.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    )
}

fn docker_container_name(build_key: &str, python_version: &str, attempt_index: usize) -> String {
    format!(
        "apdr-validate-py{}-{}-{}",
        python_version.replace('.', "_"),
        build_key
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
                    ch.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>(),
        attempt_index
    )
}

fn find_python_interpreter(python_version: &str) -> Option<PathBuf> {
    for candidate in python_interpreter_candidates(python_version) {
        if path_matches_python_version(&candidate, python_version) {
            return Some(candidate);
        }
    }
    if let Some(candidate) = windows_launcher_python_path(python_version) {
        if path_matches_python_version(&candidate, python_version) {
            return Some(candidate);
        }
    }
    None
}

fn path_matches_python_version(candidate: &Path, python_version: &str) -> bool {
    let output = Command::new(candidate)
        .arg("-c")
        .arg("import sys; sys.stdout.write('%s.%s' % (sys.version_info[0], sys.version_info[1]))")
        .output();
    let Ok(output) = output else {
        return false;
    };
    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == python_version
}

fn windows_launcher_python_path(python_version: &str) -> Option<PathBuf> {
    if !cfg!(windows) || !command_on_path("py") {
        return None;
    }
    let version_arg = windows_launcher_version_arg(python_version)?;
    let output = Command::new("py")
        .arg(version_arg)
        .arg("-c")
        .arg("import os, sys; sys.stdout.write(os.path.abspath(sys.executable))")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    normalized_command_output_path(&String::from_utf8_lossy(&output.stdout))
}

fn windows_launcher_version_arg(python_version: &str) -> Option<String> {
    let trimmed = python_version.trim();
    if trimmed.is_empty()
        || !trimmed
            .chars()
            .all(|char| char.is_ascii_digit() || char == '.')
    {
        return None;
    }
    Some(format!("-{trimmed}"))
}

fn normalized_command_output_path(output: &str) -> Option<PathBuf> {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

fn ensure_python_interpreter(python_version: &str) -> Result<PathBuf, String> {
    if let Some(path) = find_python_interpreter(python_version) {
        return Ok(path);
    }

    let detail = maybe_auto_install_python_interpreter(python_version);
    if let Some(path) = find_python_interpreter(python_version) {
        return Ok(path);
    }

    Err(detail.unwrap_or_else(|| missing_interpreter_message(python_version, "")))
}

fn maybe_auto_install_python_interpreter(python_version: &str) -> Option<String> {
    static ATTEMPTS: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
    if !auto_install_enabled() {
        return Some(missing_interpreter_message(
            python_version,
            "Auto-install is disabled by APDR_AUTO_INSTALL_PYTHONS=0.",
        ));
    }

    let attempts = ATTEMPTS.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Some(detail) = attempts
        .lock()
        .ok()
        .and_then(|cache| cache.get(python_version).cloned())
    {
        return Some(detail);
    }

    let detail = attempt_python_auto_install(python_version);
    if let Ok(mut cache) = attempts.lock() {
        cache.insert(python_version.to_string(), detail.clone());
    }
    Some(detail)
}

fn auto_install_enabled() -> bool {
    std::env::var("APDR_AUTO_INSTALL_PYTHONS")
        .map(|value| {
            let lowered = value.trim().to_ascii_lowercase();
            !matches!(lowered.as_str(), "0" | "false" | "no" | "off")
        })
        // Default OFF on Windows (org policies often block winget/scoop;
        // uv is fast but the others are slow and noisy).
        // On Unix, auto-install via uv/miniforge is fast and reliable.
        .unwrap_or(!cfg!(windows))
}

fn attempt_python_auto_install(python_version: &str) -> String {
    // Track managers that failed in ANY previous attempt so we don't retry
    // slow/broken managers (e.g. winget blocked by org policy) for every version.
    use std::collections::BTreeSet;
    static FAILED_MANAGERS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    let failed = FAILED_MANAGERS.get_or_init(|| Mutex::new(BTreeSet::new()));
    let is_failed = |name: &str| -> bool {
        failed
            .lock()
            .map(|set| set.contains(name))
            .unwrap_or(false)
    };
    let mark_failed = |name: &str| {
        if let Ok(mut set) = failed.lock() {
            set.insert(name.to_string());
        }
    };

    let mut managers = Vec::new();
    let mut last_output = String::new();

    if !python_version.starts_with("2.") && command_on_path("uv") && !is_failed("uv") {
        managers.push("uv".to_string());
        let (success, output) = run_install_command("uv", &["python", "install", python_version]);
        if success && find_python_interpreter(python_version).is_some() {
            return format!("Installed Python {python_version} with uv.");
        }
        if !success {
            mark_failed("uv");
        }
        last_output = output;
    }

    if command_on_path("mise") && !is_failed("mise") {
        managers.push("mise".to_string());
        let mut mise_ok = false;
        for spec in python_install_specs(python_version) {
            let request = format!("python@{spec}");
            let (success, output) = run_install_command("mise", &["install", &request]);
            if success && find_python_interpreter(python_version).is_some() {
                return format!("Installed Python {python_version} with mise ({spec}).");
            }
            if success {
                mise_ok = true;
            }
            last_output = output;
        }
        if !mise_ok {
            mark_failed("mise");
        }
    }

    if command_on_path("pyenv") && !is_failed("pyenv") {
        managers.push("pyenv".to_string());
        let mut pyenv_ok = false;
        for spec in python_install_specs(python_version) {
            let (success, output) = run_install_command("pyenv", &["install", "-s", &spec]);
            if success && find_python_interpreter(python_version).is_some() {
                return format!("Installed Python {python_version} with pyenv ({spec}).");
            }
            if success {
                pyenv_ok = true;
            }
            last_output = output;
        }
        if !pyenv_ok {
            mark_failed("pyenv");
        }
    }

    if command_on_path("asdf") && !is_failed("asdf") {
        managers.push("asdf".to_string());
        let (_plugin_ok, plugin_output) = run_install_command("asdf", &["plugin", "list"]);
        if !plugin_output
            .split_whitespace()
            .any(|item| item.trim() == "python")
        {
            let _ = run_install_command("asdf", &["plugin", "add", "python"]);
        }
        let mut asdf_ok = false;
        for spec in python_install_specs(python_version) {
            let (success, output) = run_install_command("asdf", &["install", "python", &spec]);
            if success && find_python_interpreter(python_version).is_some() {
                return format!("Installed Python {python_version} with asdf ({spec}).");
            }
            if success {
                asdf_ok = true;
            }
            last_output = output;
        }
        if !asdf_ok {
            mark_failed("asdf");
        }
    }

    if !cfg!(windows) && !python_version.starts_with("2.") && !is_failed("miniforge") {
        managers.push("miniforge".to_string());
        match install_with_miniforge(python_version) {
            Ok(detail) => {
                if find_python_interpreter(python_version).is_some() {
                    return detail;
                }
                last_output = detail;
            }
            Err(detail) => {
                mark_failed("miniforge");
                last_output = detail;
            }
        }
    }

    if cfg!(windows) {
        if let Some(package_id) = windows_winget_python_package(python_version) {
            if command_on_path("winget") && !is_failed("winget") {
                managers.push("winget".to_string());
                let (success, output) = run_install_command(
                    "winget",
                    &[
                        "install",
                        "-e",
                        "--id",
                        package_id,
                        "--accept-package-agreements",
                        "--accept-source-agreements",
                    ],
                );
                if success && find_python_interpreter(python_version).is_some() {
                    return format!(
                        "Installed Python {python_version} with winget ({package_id})."
                    );
                }
                if !success {
                    mark_failed("winget");
                }
                last_output = output;
            }
        }

        if let Some(package_name) = windows_scoop_python_package(python_version) {
            if command_on_path("scoop") && !is_failed("scoop") {
                managers.push("scoop".to_string());
                let (success, output) = run_install_command("scoop", &["install", package_name]);
                if success && find_python_interpreter(python_version).is_some() {
                    return format!(
                        "Installed Python {python_version} with scoop ({package_name})."
                    );
                }
                if !success {
                    mark_failed("scoop");
                }
                last_output = output;
            }
        }
    }

    if !cfg!(windows)
        && !python_version.starts_with("2.")
        && !matches!(python_version, "3.7" | "3.8")
        && command_on_path("brew")
        && !is_failed("brew")
    {
        managers.push("brew".to_string());
        let formula = format!("python@{python_version}");
        let (success, output) = run_install_command("brew", &["install", &formula]);
        if success && find_python_interpreter(python_version).is_some() {
            return format!("Installed Python {python_version} with Homebrew ({formula}).");
        }
        if !success {
            mark_failed("brew");
        }
        last_output = output;
    }

    if managers.is_empty() {
        return missing_interpreter_message(
            python_version,
            if cfg!(windows) {
                "No supported manager was found. APDR can auto-install via uv, mise, pyenv, asdf, winget, or scoop."
            } else {
                "No supported manager was found. APDR can auto-install via uv, mise, pyenv, asdf, Miniforge, or Homebrew."
            },
        );
    }

    if last_output.trim().is_empty() {
        return missing_interpreter_message(
            python_version,
            &format!(
                "Tried {} but no usable interpreter was discovered afterward.",
                managers.join(", ")
            ),
        );
    }

    missing_interpreter_message(
        python_version,
        &format!(
            "Tried {}. Last installer output: {}",
            managers.join(", "),
            summarize_command_output(&last_output)
        ),
    )
}

fn missing_interpreter_message(python_version: &str, extra: &str) -> String {
    let mut message = format!(
        "No local interpreter found for Python {python_version}. APDR auto-scanned PATH, Python framework installs, Windows launcher-managed installs, common pyenv/asdf/mise/uv locations, and APDR-managed Miniforge envs. Install a matching interpreter, set APDR_PYTHON_{}, or narrow the APDR Python search range.",
        python_version.replace('.', "_")
    );
    if python_version.starts_with("2.") {
        message.push_str(" Python 2.7 is treated as a legacy runtime, so APDR will not try modern-only installers like uv or Miniforge for it.");
    }
    if !extra.trim().is_empty() {
        message.push(' ');
        message.push_str(extra.trim());
    }
    message
}

fn python_install_specs(python_version: &str) -> Vec<String> {
    let mut values = vec![python_version.to_string()];
    let extras = match python_version {
        "2.7" => vec!["2.7.18"],
        "3.7" => vec!["3.7.17", "3.7.16"],
        "3.8" => vec!["3.8.20", "3.8.19", "3.8.18"],
        "3.9" => vec!["3.9.21", "3.9.20", "3.9.19"],
        "3.10" => vec!["3.10.16", "3.10.15", "3.10.14"],
        "3.11" => vec!["3.11.11", "3.11.10", "3.11.9"],
        "3.12" => vec!["3.12.9", "3.12.8", "3.12.7"],
        _ => Vec::new(),
    };
    for value in extras {
        if !values.iter().any(|item| item == value) {
            values.push(value.to_string());
        }
    }
    values
}

/// Returns true when any env-backend attempt failed during build with errors
/// that look like missing system C libraries / headers — the kind Docker with
/// `apt-get install` can fix.
fn env_has_system_dep_failure(summary: &ValidationSummary) -> bool {
    for attempt in &summary.attempts {
        if attempt.status != "build-failed" {
            continue;
        }
        let log = &attempt.log_excerpt;
        if !system_deps::extract_system_deps_from_log(log).is_empty() {
            return true;
        }
    }
    false
}

/// Returns true when any env-backend attempt failed because the local Python
/// interpreter was not found or could not be auto-installed.  Docker images
/// ship their own interpreter so this class of failure is always recoverable.
fn env_has_interpreter_failure(summary: &ValidationSummary) -> bool {
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
                || attempt
                    .log_excerpt
                    .contains("python unavailable")
                || attempt
                    .log_excerpt
                    .contains("Installer failed with exit code"))
    })
}

/// Returns true when any env-backend attempt failed due to a build timeout.
/// Docker/Linux typically has pre-built wheels available, avoiding lengthy
/// from-source compilation that causes timeouts on Windows.
fn env_has_build_timeout(summary: &ValidationSummary) -> bool {
    summary
        .attempts
        .iter()
        .any(|a| a.status == "build-timeout")
}

fn command_on_path(command: &str) -> bool {
    std::env::var_os("PATH")
        .map(|value| {
            std::env::split_paths(&value).any(|path| {
                let direct = path.join(command);
                if direct.exists() && direct.is_file() {
                    return true;
                }
                #[cfg(windows)]
                {
                    let has_extension = Path::new(command).extension().is_some();
                    if !has_extension {
                        let extensions = std::env::var("PATHEXT")
                            .unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
                        for ext in extensions.split(';') {
                            let suffix = ext.trim();
                            if suffix.is_empty() {
                                continue;
                            }
                            let candidate = path.join(format!("{command}{suffix}"));
                            if candidate.exists() && candidate.is_file() {
                                return true;
                            }
                        }
                    }
                }
                false
            })
        })
        .unwrap_or(false)
}

fn run_install_command(command: &str, args: &[&str]) -> (bool, String) {
    let output = Command::new(command).args(args).output();
    let Ok(output) = output else {
        return (false, format!("failed to start {command}"));
    };
    (
        output.status.success(),
        combined_output(&output.stdout, &output.stderr),
    )
}

fn summarize_command_output(output: &str) -> String {
    let lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return String::new();
    }
    let start = lines.len().saturating_sub(8);
    lines[start..].join(" | ")
}

fn install_with_miniforge(python_version: &str) -> Result<String, String> {
    let conda = ensure_unix_miniforge()?;
    let Some(root) = unix_miniforge_root() else {
        return Err("Could not determine an APDR Miniforge root directory.".to_string());
    };
    let env_root = root.join("envs").join(format!("python-{python_version}"));
    let env_python = env_root.join("bin").join("python");
    if env_python.exists() && path_matches_python_version(&env_python, python_version) {
        return Ok(format!(
            "Installed Python {python_version} with Miniforge ({python_version})."
        ));
    }

    let mut last_output = String::new();
    for spec in python_install_specs(python_version) {
        let mut command = Command::new(&conda);
        if env_root.exists() {
            command.args([
                "install",
                "-y",
                "-p",
                &env_root.display().to_string(),
                &format!("python={spec}"),
            ]);
        } else {
            command.args([
                "create",
                "-y",
                "-p",
                &env_root.display().to_string(),
                &format!("python={spec}"),
            ]);
        }
        let Ok(output) = command.output() else {
            return Err("Failed to start Miniforge conda.".to_string());
        };
        if output.status.success()
            && env_python.exists()
            && path_matches_python_version(&env_python, python_version)
        {
            return Ok(format!(
                "Installed Python {python_version} with Miniforge ({spec})."
            ));
        }
        last_output = combined_output(&output.stdout, &output.stderr);
    }

    Err(if last_output.trim().is_empty() {
        "Miniforge finished without exposing a usable interpreter.".to_string()
    } else {
        summarize_command_output(&last_output)
    })
}

fn ensure_unix_miniforge() -> Result<PathBuf, String> {
    if cfg!(windows) {
        return Err(
            "Automatic Miniforge bootstrap is currently only implemented for macOS and Linux."
                .to_string(),
        );
    }
    let Some(root) = unix_miniforge_root() else {
        return Err("Could not determine an APDR Miniforge root directory.".to_string());
    };
    let conda = root.join("bin").join("conda");
    if conda.exists() {
        return Ok(conda);
    }

    let Some(url) = unix_miniforge_installer_url() else {
        return Err(format!(
            "APDR does not have a Miniforge bootstrap URL for {}/{}.",
            std::env::consts::OS,
            std::env::consts::ARCH
        ));
    };

    let download_dir = root
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("downloads");
    if fs::create_dir_all(&download_dir).is_err() {
        return Err("Failed to create the APDR Miniforge download directory.".to_string());
    }
    let installer_path =
        download_dir.join(url.rsplit('/').next().unwrap_or("Miniforge3-installer.sh"));
    if !installer_path.exists() {
        download_with_host_python(url, &installer_path)?;
    }

    let Ok(output) = Command::new("bash")
        .args([
            installer_path.as_os_str(),
            "-b".as_ref(),
            "-p".as_ref(),
            root.as_os_str(),
        ])
        .output()
    else {
        return Err("Failed to start the Miniforge installer.".to_string());
    };
    if output.status.success() && conda.exists() {
        return Ok(conda);
    }
    Err(summarize_command_output(&combined_output(
        &output.stdout,
        &output.stderr,
    )))
}

fn download_with_host_python(url: &str, destination: &Path) -> Result<(), String> {
    let Some(python) = host_python_for_metadata() else {
        return Err(
            "APDR could not find a host Python interpreter to download Miniforge.".to_string(),
        );
    };
    if let Some(parent) = destination.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let output = Command::new(python)
        .args([
            "-c",
            "import pathlib, sys, urllib.request; path = pathlib.Path(sys.argv[2]); path.parent.mkdir(parents=True, exist_ok=True); urllib.request.urlretrieve(sys.argv[1], path)",
            url,
            &destination.display().to_string(),
        ])
        .output()
        .map_err(|_| "Failed to start the host Python downloader.".to_string())?;
    if output.status.success() {
        return Ok(());
    }
    Err(summarize_command_output(&combined_output(
        &output.stdout,
        &output.stderr,
    )))
}

fn unix_miniforge_root() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .map(|home| home.join(".apdr").join("miniforge3"))
}

fn unix_miniforge_installer_url() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Some("https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh"),
        ("macos", "x86_64") => Some("https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-x86_64.sh"),
        ("linux", "x86_64") => Some("https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh"),
        ("linux", "aarch64") => Some("https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh"),
        ("linux", "arm64") => Some("https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh"),
        ("linux", "powerpc64") | ("linux", "powerpc64le") => Some("https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-ppc64le.sh"),
        _ => None,
    }
}

fn python_interpreter_candidates(python_version: &str) -> Vec<PathBuf> {
    let normalized = python_version.replace('.', "_");
    let mut candidates = Vec::new();
    if let Ok(value) = std::env::var(format!("APDR_PYTHON_{normalized}")) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            candidates.push(PathBuf::from(trimmed));
        }
    }

    let mut names = vec![format!("python{python_version}")];
    if python_version.starts_with("3.") {
        names.push("python3".to_string());
    } else if python_version.starts_with("2.") {
        names.push("python2".to_string());
    }
    names.push("python".to_string());
    for name in names {
        candidates.push(PathBuf::from(name));
    }

    candidates.extend(known_python_interpreter_paths(python_version));
    dedupe_paths(candidates)
}

fn known_python_interpreter_paths(python_version: &str) -> Vec<PathBuf> {
    let mut paths = vec![
        PathBuf::from(format!(
            "/Library/Frameworks/Python.framework/Versions/{python_version}/bin/python{python_version}"
        )),
        PathBuf::from(format!("/usr/local/bin/python{python_version}")),
        PathBuf::from(format!("/opt/homebrew/bin/python{python_version}")),
        PathBuf::from(format!(
            "/usr/local/opt/python@{python_version}/bin/python{python_version}"
        )),
        PathBuf::from(format!(
            "/opt/homebrew/opt/python@{python_version}/bin/python{python_version}"
        )),
    ];

    // ~/.local/bin/ — common on both Unix and Windows (uv, pipx, etc.)
    if let Some(home) = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
    {
        let home = PathBuf::from(home);
        let local_bin = home.join(".local").join("bin");
        paths.push(local_bin.join(format!("python{python_version}")));
        if cfg!(windows) {
            paths.push(local_bin.join(format!("python{python_version}.exe")));
            paths.push(local_bin.join("python.exe"));
        }
    }

    if cfg!(windows) {
        let compact = python_version.replace('.', "");
        if let Some(local_appdata) = std::env::var_os("LOCALAPPDATA") {
            let local_appdata = PathBuf::from(local_appdata);
            paths.push(
                local_appdata
                    .join("Programs")
                    .join("Python")
                    .join(format!("Python{compact}"))
                    .join("python.exe"),
            );
            paths.push(
                local_appdata
                    .join("Programs")
                    .join("Python")
                    .join(format!("Python{compact}-32"))
                    .join("python.exe"),
            );
        }
        for variable in ["ProgramFiles", "ProgramFiles(x86)"] {
            if let Some(base) = std::env::var_os(variable) {
                let base = PathBuf::from(base);
                paths.push(
                    base.join("Python")
                        .join(format!("Python{compact}"))
                        .join("python.exe"),
                );
                paths.push(base.join(format!("Python{compact}")).join("python.exe"));
            }
        }
    }

    let major = python_version.split('.').next().unwrap_or(python_version);
    for root in managed_python_roots() {
        if !root.exists() {
            continue;
        }
        for child in matching_version_dirs(&root, python_version) {
            paths.push(child.join("bin").join(format!("python{python_version}")));
            paths.push(child.join("bin").join(format!("python{major}")));
            paths.push(child.join("bin").join("python"));
            paths.push(child.join("python.exe"));
            paths.push(child.join(format!("python{major}.exe")));
            paths.push(child.join(format!("python{python_version}.exe")));
            paths.push(child.join("current").join("python.exe"));
            paths.push(child.join("current").join(format!("python{major}.exe")));
            paths.push(
                child
                    .join("current")
                    .join(format!("python{python_version}.exe")),
            );
        }
    }
    paths
}

fn managed_python_roots() -> Vec<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from);
    let mut roots = Vec::new();
    if let Some(home) = home {
        roots.push(home.join(".pyenv/versions"));
        roots.push(home.join(".pyenv/pyenv-win/versions"));
        roots.push(home.join(".asdf/installs/python"));
        roots.push(home.join(".local/share/mise/installs/python"));
        roots.push(home.join(".local/share/uv/python"));
        roots.push(home.join(".apdr/miniforge3/envs"));
        roots.push(home.join("miniforge3/envs"));
        roots.push(home.join("scoop/apps"));
    }
    if let Some(local_appdata) = std::env::var_os("LOCALAPPDATA") {
        let local_appdata = PathBuf::from(local_appdata);
        roots.push(local_appdata.join("uv/python"));
        roots.push(local_appdata.join("Programs/Python"));
    }
    roots
}

fn matching_version_dirs(root: &Path, version: &str) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let compact = version.replace('.', "");
    let prefixes = [
        version.to_string(),
        format!("{version}."),
        format!("{version}-"),
        format!("python-{version}"),
        format!("Python-{version}"),
        format!("cpython-{version}"),
        format!("Python{compact}"),
        format!("python{compact}"),
    ];
    entries
        .filter_map(|entry| entry.ok().map(|item| item.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| {
                    name == version || prefixes.iter().any(|prefix| name.starts_with(prefix))
                })
                .unwrap_or(false)
        })
        .collect()
}

fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = std::collections::BTreeSet::new();
    let mut unique = Vec::new();
    for path in paths {
        let key = path.to_string_lossy().to_string();
        if seen.insert(key) {
            unique.push(path);
        }
    }
    unique
}

fn host_python_for_metadata() -> Option<PathBuf> {
    for version in ["3.12", "3.11", "3.10", "3.9", "3.8", "3.7"] {
        if let Some(path) = find_python_interpreter(version) {
            return Some(path);
        }
    }
    for candidate in ["python3", "python"] {
        let path = PathBuf::from(candidate);
        let Ok(output) = Command::new(&path)
            .arg("-c")
            .arg("import sys; sys.stdout.write('%s' % sys.version_info[0])")
            .output()
        else {
            continue;
        };
        if output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "3" {
            return Some(path);
        }
    }
    None
}

fn windows_winget_python_package(python_version: &str) -> Option<&'static str> {
    match python_version {
        "3.7" => Some("Python.Python.3.7"),
        "3.8" => Some("Python.Python.3.8"),
        "3.9" => Some("Python.Python.3.9"),
        "3.10" => Some("Python.Python.3.10"),
        "3.11" => Some("Python.Python.3.11"),
        "3.12" => Some("Python.Python.3.12"),
        _ => None,
    }
}

fn windows_scoop_python_package(python_version: &str) -> Option<&'static str> {
    match python_version {
        "3.7" => Some("python37"),
        "3.8" => Some("python38"),
        "3.9" => Some("python39"),
        "3.10" => Some("python310"),
        "3.11" => Some("python311"),
        "3.12" => Some("python312"),
        _ => None,
    }
}

fn copy_dir_all(source: &Path, destination: &Path) -> io::Result<()> {
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

fn smoke_test_command(env_python: &Path, work_dir: &Path) -> Command {
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
        // (Python.app), which venv pythons are not → RuntimeError at import.
        .env("MPLBACKEND", "Agg");
    command
}

/// One-time install of virtualenv under Python 2.7 so we can create isolated
/// environments without relying on the host Python 3's virtualenv (which may
/// have dropped Python 2 support in versions 21+).
fn ensure_py2_virtualenv(interpreter: &Path) {
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
        eprintln!("[validation] installing virtualenv under Python 2.7…");
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
/// Cached via OnceLock — probed at most once per process.
fn uv_available() -> bool {
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

fn create_env(
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
    // Pass the version string (e.g. "3.9") instead of the interpreter path — uv's
    // managed Pythons fail inspection when given a raw path but work fine with a
    // version request.
    if uv_available() {
        let mut uv_cmd = Command::new("uv");
        uv_cmd
            .arg("venv")
            .arg("--seed")          // install pip+setuptools so fallback `python -m pip` works
            .arg("--python")
            .arg(python_version)
            .arg(env_dir);
        let uv_result = run_command_with_timeout(&mut uv_cmd, timeout)?;
        if uv_result.success {
            return Ok(uv_result);
        }
        // uv failed — clean up partial env and fall through
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

fn run_env_install_requirements(
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

fn env_python_path(env_dir: &Path) -> PathBuf {
    if cfg!(windows) {
        env_dir.join("Scripts").join("python.exe")
    } else {
        env_dir.join("bin").join("python")
    }
}

fn env_site_packages_dir(env_dir: &Path, python_version: &str) -> PathBuf {
    if cfg!(windows) {
        env_dir.join("Lib").join("site-packages")
    } else {
        env_dir
            .join("lib")
            .join(format!("python{python_version}"))
            .join("site-packages")
    }
}

fn validated_env_cache_path(validated_envs_dir: &Path, build_key: &str) -> PathBuf {
    validated_envs_dir.join(build_key.replace(':', "-"))
}

fn validated_env_archive_path(validated_envs_dir: &Path, build_key: &str) -> PathBuf {
    validated_envs_dir.join(format!("{}.tar.zst", build_key.replace(':', "-")))
}

fn save_validated_env(
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
fn build_time_prerequisites<'a>(requirements: &str, python_version: &str) -> Vec<&'a str> {
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

const PACKAGE_REPOSITORY_CATALOG_SCRIPT: &str = r#"
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

fn attempt_metadata(
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
        "attempt_index: {}\npython_version: {}\nvalidation_backend: {}\nstatus: {}\nenv_label: {}\nenv_dir: {}\nenv_create_duration_ms: {}\nbuild_key: {}\nused_cached_env: {}\nvalidated_env_cache_hit: {}\nused_cached_lockfile: {}\nerror_type: {}\nconflict_class: {}\nfix_applied: {}\nbuild_command: {}\nbuild_exit_code: {}\nbuild_duration_ms: {}\nrun_command: {}\nrun_exit_code: {}\nrun_duration_ms: {}\nartifact_dir: {}\n",
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_launcher_version_arg_formats_requested_minor() {
        assert_eq!(
            windows_launcher_version_arg("3.11").as_deref(),
            Some("-3.11")
        );
        assert_eq!(windows_launcher_version_arg("2.7").as_deref(), Some("-2.7"));
    }

    #[test]
    fn windows_launcher_version_arg_rejects_invalid_values() {
        assert!(windows_launcher_version_arg("").is_none());
        assert!(windows_launcher_version_arg("  ").is_none());
        assert!(windows_launcher_version_arg("3.11 rc1").is_none());
        assert!(windows_launcher_version_arg("python3.11").is_none());
    }

    #[test]
    fn normalized_command_output_path_trims_newlines() {
        assert_eq!(
            normalized_command_output_path("C:\\Python311\\python.exe\r\n"),
            Some(PathBuf::from("C:\\Python311\\python.exe"))
        );
    }

    #[test]
    fn normalized_command_output_path_rejects_empty_output() {
        assert_eq!(normalized_command_output_path(" \r\n\t "), None);
    }
}
