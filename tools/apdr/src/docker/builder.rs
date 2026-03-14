use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::build_cache;
use crate::cache::store::CacheStore;
use crate::context;
use crate::docker::smoke_test;
use crate::{ResolveConfig, ValidationAttempt, ValidationSummary};

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
    let mut summary = ValidationSummary::default();
    context::ensure_debug_layout(&config.output_dir)?;
    let wheelhouse_dir = config.cache_path.join("wheelhouse");
    fs::create_dir_all(&wheelhouse_dir)?;
    let validated_envs_dir = config.cache_path.join("validated-envs");
    fs::create_dir_all(&validated_envs_dir)?;

    for (local_index, python_version) in candidate_versions.iter().enumerate() {
        let attempt_index = attempt_offset + local_index + 1;
        let build_key = build_cache::key_for(requirements_txt, python_version);
        summary.lockfile_key = Some(build_key.clone());
        summary.build_cache_key = Some(build_key.clone());
        let env_label = sanitized_env_label(&build_key, python_version);
        let work_dir = context::attempt_dir(&config.output_dir, attempt_index, python_version);
        fs::create_dir_all(&work_dir)?;
        let env_dir = work_dir.join("env");

        fs::write(work_dir.join("requirements.txt"), requirements_txt)?;
        fs::write(
            work_dir.join("smoke_test.py"),
            smoke_test::generate(imports, config.execute_snippet),
        )?;
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
                    validation_backend: "env".to_string(),
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
            let host = host_python_for_metadata()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "python3".to_string());
            format!(
                "{} -m virtualenv -p {} {}",
                host,
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
        fs::write(work_dir.join("env-install.command.txt"), &env_install_command)?;
        fs::write(work_dir.join("env-run.command.txt"), &run_command)?;
        fs::write(&run_log_path, "")?;
        fs::write(&combined_log_path, "")?;
        if let Ok(tail) = context::read_context_tail(config.benchmark_context_log.as_deref(), 48_000) {
            fs::write(&context_snapshot_path, tail)?;
        } else {
            fs::write(&context_snapshot_path, "")?;
        }

        let mut attempt = ValidationAttempt {
            attempt_index,
            python_version: python_version.clone(),
            validation_backend: "env".to_string(),
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
        summary.validation_backend = "env".to_string();

        // Check validated-env cache
        let cached_env_dir = validated_env_cache_path(&validated_envs_dir, &build_key);
        let cache_hit = cached_env_dir.exists()
            && (cached_env_dir.join("bin").exists() || cached_env_dir.join("Scripts").exists());
        attempt.used_cached_env = cache_hit;
        attempt.validated_env_cache_hit = cache_hit;

        let (build_logs, build_exit_code, build_duration_ms) = if cache_hit {
            match copy_dir_all(&cached_env_dir, &env_dir) {
                Ok(()) => {
                    let log = format!(
                        "reused cached validated env from {}",
                        cached_env_dir.display()
                    );
                    fs::write(&build_log_path, &log)?;
                    (log, None, 0_u128)
                }
                Err(err) => {
                    // Cache copy failed; fall through to fresh env creation
                    let _ = fs::remove_dir_all(&env_dir);
                    attempt.used_cached_env = false;
                    attempt.validated_env_cache_hit = false;
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
                        config,
                        &mut attempt,
                        &mut summary,
                    )?;
                    if !attempt.status.is_empty() {
                        summary.attempts.push(attempt);
                        continue;
                    }
                    let mut log = format!("(cache copy failed: {})\n", err);
                    log.push_str(&result.0);
                    (log, result.1, result.2)
                }
            }
        } else {
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
                config,
                &mut attempt,
                &mut summary,
            )?;
            if !attempt.status.is_empty() {
                summary.attempts.push(attempt);
                continue;
            }
            result
        };

        let mut smoke_command = smoke_test_command(&env_python, &work_dir);
        let run_output = run_command_with_timeout(&mut smoke_command, config.validation_timeout)?;
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
            summary.attempts.push(attempt);
            continue;
        }

        if run_output.success {
            attempt.status = "passed".to_string();
            attempt.log_excerpt = truncate_log(&combined);
            let site_packages = env_site_packages_dir(&env_dir, python_version);
            let _ = catalog_package_repository(store, python_version, &site_packages);
            // Save validated env to cache for future reuse
            let _ = save_validated_env(&validated_envs_dir, &build_key, &env_dir);
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
    }

    Ok(summary)
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
    config: &ResolveConfig,
    attempt: &mut ValidationAttempt,
    summary: &mut ValidationSummary,
) -> io::Result<(String, Option<i32>, u128)> {
    // Create isolated env
    let create_output = create_env(interpreter, env_dir, python_version, config.validation_timeout)?;
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

    // Install requirements into env
    let install_output = run_env_install_requirements(
        env_python,
        wheelhouse_dir,
        install_requirements_path,
        config.validation_timeout,
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
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let started = Instant::now();

    loop {
        if child.try_wait()?.is_some() {
            let output = child.wait_with_output()?;
            let success = output.status.success();
            return Ok(command_result(success, output, false, started.elapsed().as_millis()));
        }

        if started.elapsed() >= timeout {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            return Ok(command_result(false, output, true, started.elapsed().as_millis()));
        }

        thread::sleep(Duration::from_millis(150));
    }
}

fn command_result(success: bool, output: Output, timed_out: bool, duration_ms: u128) -> CommandResult {
    CommandResult {
        success,
        combined_output: combined_output(&output.stdout, &output.stderr),
        timed_out,
        exit_code: output.status.code(),
        duration_ms,
    }
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

fn find_python_interpreter(python_version: &str) -> Option<PathBuf> {
    for candidate in python_interpreter_candidates(python_version) {
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
        .unwrap_or(true)
}

fn attempt_python_auto_install(python_version: &str) -> String {
    let mut managers = Vec::new();
    let mut last_output = String::new();

    if !python_version.starts_with("2.") && command_on_path("uv") {
        managers.push("uv".to_string());
        let (success, output) = run_install_command("uv", &["python", "install", python_version]);
        if success && find_python_interpreter(python_version).is_some() {
            return format!("Installed Python {python_version} with uv.");
        }
        last_output = output;
    }

    if command_on_path("mise") {
        managers.push("mise".to_string());
        for spec in python_install_specs(python_version) {
            let request = format!("python@{spec}");
            let (success, output) = run_install_command("mise", &["install", &request]);
            if success && find_python_interpreter(python_version).is_some() {
                return format!("Installed Python {python_version} with mise ({spec}).");
            }
            last_output = output;
        }
    }

    if command_on_path("pyenv") {
        managers.push("pyenv".to_string());
        for spec in python_install_specs(python_version) {
            let (success, output) = run_install_command("pyenv", &["install", "-s", &spec]);
            if success && find_python_interpreter(python_version).is_some() {
                return format!("Installed Python {python_version} with pyenv ({spec}).");
            }
            last_output = output;
        }
    }

    if command_on_path("asdf") {
        managers.push("asdf".to_string());
        let (_plugin_ok, plugin_output) = run_install_command("asdf", &["plugin", "list"]);
        if !plugin_output
            .split_whitespace()
            .any(|item| item.trim() == "python")
        {
            let _ = run_install_command("asdf", &["plugin", "add", "python"]);
        }
        for spec in python_install_specs(python_version) {
            let (success, output) = run_install_command("asdf", &["install", "python", &spec]);
            if success && find_python_interpreter(python_version).is_some() {
                return format!("Installed Python {python_version} with asdf ({spec}).");
            }
            last_output = output;
        }
    }

    if !cfg!(windows) && !python_version.starts_with("2.") {
        managers.push("miniforge".to_string());
        match install_with_miniforge(python_version) {
            Ok(detail) => {
                if find_python_interpreter(python_version).is_some() {
                    return detail;
                }
                last_output = detail;
            }
            Err(detail) => last_output = detail,
        }
    }

    if cfg!(windows) {
        if let Some(package_id) = windows_winget_python_package(python_version) {
            if command_on_path("winget") {
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
                    return format!("Installed Python {python_version} with winget ({package_id}).");
                }
                last_output = output;
            }
        }

        if let Some(package_name) = windows_scoop_python_package(python_version) {
            if command_on_path("scoop") {
                managers.push("scoop".to_string());
                let (success, output) = run_install_command("scoop", &["install", package_name]);
                if success && find_python_interpreter(python_version).is_some() {
                    return format!("Installed Python {python_version} with scoop ({package_name}).");
                }
                last_output = output;
            }
        }
    }

    if !cfg!(windows)
        && !python_version.starts_with("2.")
        && !matches!(python_version, "3.7" | "3.8")
        && command_on_path("brew")
    {
        managers.push("brew".to_string());
        let formula = format!("python@{python_version}");
        let (success, output) = run_install_command("brew", &["install", &formula]);
        if success && find_python_interpreter(python_version).is_some() {
            return format!("Installed Python {python_version} with Homebrew ({formula}).");
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
    let output = Command::new(command)
        .args(args)
        .output();
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
        return Ok(format!("Installed Python {python_version} with Miniforge ({python_version})."));
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
        if output.status.success() && env_python.exists() && path_matches_python_version(&env_python, python_version) {
            return Ok(format!("Installed Python {python_version} with Miniforge ({spec})."));
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
        return Err("Automatic Miniforge bootstrap is currently only implemented for macOS and Linux.".to_string());
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
    let installer_path = download_dir.join(
        url.rsplit('/')
            .next()
            .unwrap_or("Miniforge3-installer.sh"),
    );
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
        return Err("APDR could not find a host Python interpreter to download Miniforge.".to_string());
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
                paths.push(base.join("Python").join(format!("Python{compact}")).join("python.exe"));
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
            paths.push(child.join("current").join(format!("python{python_version}.exe")));
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
                .map(|name| name == version || prefixes.iter().any(|prefix| name.starts_with(prefix)))
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
    let mut command = Command::new(env_python);
    command
        .arg(work_dir.join("smoke_test.py"))
        .current_dir(work_dir)
        .env("PYTHONNOUSERSITE", "1");
    command
}

fn create_env(
    interpreter: &Path,
    env_dir: &Path,
    python_version: &str,
    timeout: Duration,
) -> io::Result<CommandResult> {
    let mut command = if python_version.starts_with("2.") {
        // Python 2.7: use virtualenv from host Python 3
        let host = host_python_for_metadata().unwrap_or_else(|| PathBuf::from("python3"));
        let mut cmd = Command::new(host);
        cmd.arg("-m")
            .arg("virtualenv")
            .arg("-p")
            .arg(interpreter)
            .arg(env_dir);
        cmd
    } else {
        // Python 3.x: use stdlib venv
        let mut cmd = Command::new(interpreter);
        cmd.arg("-m").arg("venv").arg(env_dir);
        cmd
    };
    run_command_with_timeout(&mut command, timeout)
}

fn run_env_install_requirements(
    env_python: &Path,
    cache_dir: &Path,
    requirements_path: &Path,
    timeout: Duration,
) -> io::Result<CommandResult> {
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
    run_command_with_timeout(&mut command, timeout)
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

fn save_validated_env(validated_envs_dir: &Path, build_key: &str, env_dir: &Path) -> io::Result<()> {
    let dest = validated_env_cache_path(validated_envs_dir, build_key);
    if dest.exists() {
        return Ok(());
    }
    copy_dir_all(env_dir, &dest)
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
