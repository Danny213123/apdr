use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::cache::build_cache;
use crate::cache::store::CacheStore;
use crate::context;
use crate::docker::smoke_test;
use crate::resolver::pypi_client;
use crate::{ResolveConfig, ValidationAttempt, ValidationSummary};

struct CommandResult {
    success: bool,
    combined_output: String,
    timed_out: bool,
    exit_code: Option<i32>,
    duration_ms: u128,
}

#[derive(Clone)]
struct CachedPackageArtifact {
    package_name: String,
    version: String,
    artifact_dir: PathBuf,
}

#[derive(Default)]
struct InstallPlan {
    reused: Vec<CachedPackageArtifact>,
    install_lines: Vec<String>,
    notes: Vec<String>,
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
    let shared_cache_dir = config.cache_path.join("pip-cache");
    fs::create_dir_all(&shared_cache_dir)?;

    for (local_index, python_version) in candidate_versions.iter().enumerate() {
        let attempt_index = attempt_offset + local_index + 1;
        let build_key = build_cache::key_for(requirements_txt, python_version);
        summary.lockfile_key = Some(build_key.clone());
        summary.build_cache_key = Some(build_key.clone());
        let env_label = sanitized_image_tag(&build_key, python_version);
        let work_dir = context::attempt_dir(&config.output_dir, attempt_index, python_version);
        fs::create_dir_all(&work_dir)?;
        let site_packages_dir = work_dir.join("site-packages");
        fs::create_dir_all(&site_packages_dir)?;

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
        let interpreter = match find_python_interpreter(python_version) {
            Some(path) => path,
            None => {
                let missing = format!(
                    "No local interpreter found for Python {python_version}. APDR auto-scanned PATH, Python framework installs, and common pyenv/asdf/mise locations. Install a matching interpreter, set APDR_PYTHON_{}, or narrow the APDR Python search range.",
                    python_version.replace('.', "_")
                );
                fs::write(&build_log_path, &missing)?;
                fs::write(&run_log_path, "")?;
                fs::write(&combined_log_path, &missing)?;
                let attempt = ValidationAttempt {
                    attempt_index,
                    python_version: python_version.clone(),
                    image_tag: Some(env_label.clone()),
                    status: "build-failed".to_string(),
                    log_excerpt: truncate_log(&missing),
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
        let install_plan = build_install_plan(store, requirements_txt, python_version);
        let install_requirements_path = work_dir.join("requirements-install.txt");
        fs::write(
            &install_requirements_path,
            install_plan.install_lines.join("\n")
                + if install_plan.install_lines.is_empty() {
                    ""
                } else {
                    "\n"
                },
        )?;
        let build_command = if install_plan.install_lines.is_empty() {
            format!(
                "reuse package repository only -> {}",
                site_packages_dir.display()
            )
        } else {
            format!(
                "{} -m pip install --cache-dir {} --target {} -r {}",
                interpreter.display(),
                shared_cache_dir.display(),
                site_packages_dir.display(),
                install_requirements_path.display()
            )
        };
        let run_command = format!(
            "APDR_SITE_PACKAGES={} {} {}",
            site_packages_dir.display(),
            interpreter.display(),
            work_dir.join("smoke_test.py").display()
        );
        fs::write(work_dir.join("env-build.command.txt"), &build_command)?;
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
            image_tag: Some(env_label.clone()),
            used_cached_lockfile: store.lockfile(&build_key).is_some(),
            artifact_dir: Some(work_dir.display().to_string()),
            build_log_path: Some(build_log_path.display().to_string()),
            run_log_path: Some(run_log_path.display().to_string()),
            combined_log_path: Some(combined_log_path.display().to_string()),
            metadata_path: Some(metadata_path.display().to_string()),
            context_snapshot_path: Some(context_snapshot_path.display().to_string()),
            ..Default::default()
        };

        let cached_site_dir = store
            .build_artifact(&build_key)
            .map(|path| Path::new(path).to_path_buf())
            .filter(|path| path.exists());
        attempt.used_cached_image = cached_site_dir.is_some();

        let (effective_site_dir, build_logs, build_exit_code, build_duration_ms) =
            if let Some(cached_site_dir) = cached_site_dir {
                let log = format!(
                    "reused cached environment from {}",
                    cached_site_dir.display()
                );
                fs::write(&build_log_path, &log)?;
                (cached_site_dir, log, None, 0)
            } else {
                let reused_paths = materialize_package_repository(
                    &install_plan.reused,
                    &site_packages_dir,
                )?;
                let mut plan_notes = install_plan.notes.clone();
                if !reused_paths.is_empty() {
                    plan_notes.push(format!(
                        "reused package repository artifacts: {}",
                        reused_paths.join(", ")
                    ));
                }
                if install_plan.install_lines.is_empty() {
                    let log = if plan_notes.is_empty() {
                        "reused package repository artifacts; no pip install required".to_string()
                    } else {
                        plan_notes.join("\n")
                    };
                    fs::write(&build_log_path, &log)?;
                    let _ = context::append_context_log(
                        config.benchmark_context_log.as_deref(),
                        "apdr-env-build",
                        &log,
                    );
                    let _ = store.save_build_artifact(&build_key, &site_packages_dir.display().to_string());
                    (
                        site_packages_dir.clone(),
                        log,
                        None,
                        0,
                    )
                } else {
                    let install_output = run_python_install_requirements(
                        &interpreter,
                        &site_packages_dir,
                        &shared_cache_dir,
                        &install_requirements_path,
                        config.docker_timeout,
                    )?;
                    let mut build_output = install_output.combined_output.clone();
                    if !plan_notes.is_empty() {
                        build_output = format!("{}\n{}", plan_notes.join("\n"), build_output);
                    }
                    fs::write(&build_log_path, &build_output)?;
                    if install_output.timed_out {
                        attempt.status = "build-timeout".to_string();
                        attempt.log_excerpt = truncate_log(&build_output);
                        fs::write(&combined_log_path, &build_output)?;
                        fs::write(
                            &metadata_path,
                            attempt_metadata(
                                &attempt,
                                &build_key,
                                &build_command,
                                &run_command,
                                install_output.exit_code,
                                install_output.duration_ms,
                                None,
                                None,
                            ),
                        )?;
                        summary.attempts.push(attempt);
                        continue;
                    }
                    if !install_output.success {
                        let _ = context::append_context_log(
                            config.benchmark_context_log.as_deref(),
                            "apdr-env-build",
                            &build_output,
                        );
                        attempt.status = "build-failed".to_string();
                        attempt.log_excerpt = truncate_log(&build_output);
                        fs::write(&combined_log_path, &build_output)?;
                        fs::write(
                            &metadata_path,
                            attempt_metadata(
                                &attempt,
                                &build_key,
                                &build_command,
                                &run_command,
                                install_output.exit_code,
                                install_output.duration_ms,
                                None,
                                None,
                            ),
                        )?;
                        summary.attempts.push(attempt);
                        continue;
                    }
                    let _ = context::append_context_log(
                        config.benchmark_context_log.as_deref(),
                        "apdr-env-build",
                        &build_output,
                    );
                    let _ = store.save_build_artifact(&build_key, &site_packages_dir.display().to_string());
                    (
                        site_packages_dir.clone(),
                        build_output,
                        install_output.exit_code,
                        install_output.duration_ms,
                    )
                }
            };

        let mut smoke_command = smoke_test_command(&interpreter, &effective_site_dir, &work_dir);
        let run_output = run_command_with_timeout(&mut smoke_command, config.docker_timeout)?;
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
            let _ = catalog_package_repository(store, python_version, &effective_site_dir);
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
            summary.docker_image_id = None;
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

fn build_install_plan(
    store: &mut CacheStore,
    requirements_txt: &str,
    python_version: &str,
) -> InstallPlan {
    let mut plan = InstallPlan::default();
    let mut install_lines = BTreeMap::new();
    let mut reused = BTreeMap::new();
    let mut visiting = BTreeSet::new();

    for requirement in parse_requirements(requirements_txt) {
        if let Some(version) = &requirement.version {
            visit_exact_requirement(
                store,
                python_version,
                &requirement.name,
                version,
                &mut visiting,
                &mut reused,
                &mut install_lines,
                &mut plan.notes,
            );
        } else {
            install_lines.insert(requirement.name.clone(), requirement.raw_line.clone());
        }
    }

    plan.reused = reused.into_values().collect();
    plan.install_lines = install_lines.into_values().collect();
    plan
}

fn visit_exact_requirement(
    store: &mut CacheStore,
    python_version: &str,
    package_name: &str,
    version: &str,
    visiting: &mut BTreeSet<String>,
    reused: &mut BTreeMap<String, CachedPackageArtifact>,
    install_lines: &mut BTreeMap<String, String>,
    notes: &mut Vec<String>,
) {
    let key = format!("{}=={}", pypi_client::requirement_name(package_name), version.trim());
    if !visiting.insert(key.clone()) {
        return;
    }

    if let Some(path) = store.package_artifact(python_version, package_name, version) {
        reused.insert(
            key.clone(),
            CachedPackageArtifact {
                package_name: pypi_client::requirement_name(package_name),
                version: version.trim().to_string(),
                artifact_dir: PathBuf::from(path),
            },
        );
    } else {
        install_lines.insert(
            key.clone(),
            format!("{}=={}", pypi_client::requirement_name(package_name), version.trim()),
        );
    }

    for spec in pypi_client::dependency_specs(store, package_name, version) {
        let dep_name = pypi_client::requirement_name(&spec);
        if dep_name.is_empty() {
            continue;
        }
        let dep_constraint = requirement_constraint(&spec);
        let cached_match = best_cached_artifact_for_constraint(
            store,
            python_version,
            &dep_name,
            &dep_constraint,
        );
        if let Some((cached_version, _path)) = cached_match {
            notes.push(format!(
                "reused cached transitive dependency {}=={} for {}",
                dep_name, cached_version, package_name
            ));
            visit_exact_requirement(
                store,
                python_version,
                &dep_name,
                &cached_version,
                visiting,
                reused,
                install_lines,
                notes,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::cache::store::CacheStore;

    use super::build_install_plan;

    fn unique_cache_dir(tool_root: &PathBuf, label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        tool_root.join("target").join(format!("{label}-{stamp}"))
    }

    #[test]
    fn install_plan_keeps_direct_legacy_pymc3_requirements_without_expanding_modern_transitives() {
        let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let cache_path = unique_cache_dir(&tool_root, "builder-plan-cache");
        let mut store = CacheStore::load(&tool_root, cache_path.clone()).unwrap();

        let requirements = "\
numpy==1.21.6
pandas==1.5.3
pymc3==3.11.5
scipy==1.7.3
Theano-PyMC==1.1.2
";

        let plan = build_install_plan(&mut store, requirements, "3.10");

        assert_eq!(plan.install_lines.len(), 5);
        assert!(plan.install_lines.iter().any(|line| line == "numpy==1.21.6"));
        assert!(plan.install_lines.iter().any(|line| line == "pandas==1.5.3"));
        assert!(plan.install_lines.iter().any(|line| line == "pymc3==3.11.5"));
        assert!(plan.install_lines.iter().any(|line| line == "scipy==1.7.3"));
        assert!(plan
            .install_lines
            .iter()
            .any(|line| line.contains("1.1.2") && line.to_ascii_lowercase().contains("theano")));
        assert!(plan
            .install_lines
            .iter()
            .all(|line| !line.starts_with("arviz==") && !line.starts_with("matplotlib==")));

        fs::remove_dir_all(cache_path).unwrap();
    }
}

fn best_cached_artifact_for_constraint(
    store: &CacheStore,
    python_version: &str,
    package_name: &str,
    constraint: &str,
) -> Option<(String, String)> {
    let mut versions = store.package_artifact_versions(python_version, package_name);
    versions.sort_by(|left, right| {
        pypi_client::version_satisfies(&left.0, constraint)
            .cmp(&pypi_client::version_satisfies(&right.0, constraint))
            .then_with(|| compare_versions_for_sort(&left.0, &right.0))
    });
    versions
        .into_iter()
        .filter(|(version, _)| constraint.is_empty() || pypi_client::version_satisfies(version, constraint))
        .last()
}

fn materialize_package_repository(
    reused: &[CachedPackageArtifact],
    site_packages_dir: &Path,
) -> io::Result<Vec<String>> {
    let mut materialized = Vec::new();
    for artifact in reused {
        if !artifact.artifact_dir.exists() {
            continue;
        }
        for entry in fs::read_dir(&artifact.artifact_dir)? {
            let entry = entry?;
            let target = site_packages_dir.join(entry.file_name());
            if target.exists() {
                continue;
            }
            link_or_copy(&entry.path(), &target)?;
        }
        materialized.push(format!("{}=={}", artifact.package_name, artifact.version));
    }
    Ok(materialized)
}

fn catalog_package_repository(
    store: &mut CacheStore,
    python_version: &str,
    site_packages_dir: &Path,
) -> io::Result<()> {
    let repository_dir = store.cache_path.join("package-repository");
    fs::create_dir_all(&repository_dir)?;
    let output = Command::new("python3")
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

#[derive(Clone)]
struct RequirementLine {
    name: String,
    version: Option<String>,
    raw_line: String,
}

fn parse_requirements(requirements_txt: &str) -> Vec<RequirementLine> {
    requirements_txt
        .lines()
        .filter_map(|line| parse_requirement_line(line))
        .collect()
}

fn parse_requirement_line(line: &str) -> Option<RequirementLine> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('-') {
        return None;
    }
    let base = trimmed.split(';').next().unwrap_or(trimmed).trim();
    let operators = ["==", ">=", "<=", "!=", "~=", ">", "<"];
    for operator in operators {
        if let Some((left, right)) = base.split_once(operator) {
            let name = pypi_client::requirement_name(left);
            if name.is_empty() {
                return None;
            }
            let version = if operator == "==" {
                Some(right.trim().to_string())
            } else {
                None
            };
            return Some(RequirementLine {
                name,
                version,
                raw_line: base.to_string(),
            });
        }
    }
    let name = pypi_client::requirement_name(base);
    if name.is_empty() {
        return None;
    }
    Some(RequirementLine {
        name,
        version: None,
        raw_line: base.to_string(),
    })
}

fn requirement_constraint(requirement: &str) -> String {
    let trimmed = requirement.split(';').next().unwrap_or(requirement).trim();
    let operators = ["==", ">=", "<=", "!=", "~=", ">", "<"];
    for operator in operators {
        if let Some((_left, right)) = trimmed.split_once(operator) {
            return format!("{operator}{}", right.trim());
        }
    }
    String::new()
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

fn sanitized_image_tag(build_key: &str, python_version: &str) -> String {
    format!(
        "apdr-env:{}-py{}",
        build_key.replace(':', "-"),
        python_version.replace('.', "_")
    )
}

fn find_python_interpreter(python_version: &str) -> Option<PathBuf> {
    for candidate in python_interpreter_candidates(python_version) {
        let output = Command::new(&candidate)
            .arg("-c")
            .arg("import sys; sys.stdout.write('%s.%s' % (sys.version_info[0], sys.version_info[1]))")
            .output();
        let Ok(output) = output else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if version == python_version {
            return Some(candidate);
        }
    }
    None
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

    let major = python_version.split('.').next().unwrap_or(python_version);
    for root in managed_python_roots() {
        if !root.exists() {
            continue;
        }
        for child in matching_version_dirs(&root, python_version) {
            paths.push(child.join("bin").join(format!("python{python_version}")));
            paths.push(child.join("bin").join(format!("python{major}")));
            paths.push(child.join("bin").join("python"));
        }
    }
    paths
}

fn managed_python_roots() -> Vec<PathBuf> {
    let Some(home) = std::env::var_os("HOME") else {
        return Vec::new();
    };
    let home = PathBuf::from(home);
    vec![
        home.join(".pyenv/versions"),
        home.join(".asdf/installs/python"),
        home.join(".local/share/mise/installs/python"),
    ]
}

fn matching_version_dirs(root: &Path, version: &str) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let prefixes = [
        version.to_string(),
        format!("{version}."),
        format!("{version}-"),
        format!("Python-{version}"),
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

fn link_or_copy(source: &Path, destination: &Path) -> io::Result<()> {
    if destination.exists() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs as unix_fs;
        if unix_fs::symlink(source, destination).is_ok() {
            return Ok(());
        }
    }
    if source.is_dir() {
        copy_dir_all(source, destination)
    } else {
        fs::copy(source, destination)?;
        Ok(())
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

fn compare_versions_for_sort(left: &str, right: &str) -> Ordering {
    let left_parts = tokenize_version(left);
    let right_parts = tokenize_version(right);
    let max_len = std::cmp::max(left_parts.len(), right_parts.len());
    for index in 0..max_len {
        let left_part = left_parts.get(index).cloned().unwrap_or(VersionToken::Number(0));
        let right_part = right_parts.get(index).cloned().unwrap_or(VersionToken::Number(0));
        let ordering = match (left_part, right_part) {
            (VersionToken::Number(a), VersionToken::Number(b)) => a.cmp(&b),
            (VersionToken::Text(a), VersionToken::Text(b)) => a.cmp(&b),
            (VersionToken::Number(_), VersionToken::Text(_)) => Ordering::Greater,
            (VersionToken::Text(_), VersionToken::Number(_)) => Ordering::Less,
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

#[derive(Clone)]
enum VersionToken {
    Number(u64),
    Text(String),
}

fn tokenize_version(value: &str) -> Vec<VersionToken> {
    let mut parts = Vec::new();
    let mut buffer = String::new();
    let mut numeric = false;
    for ch in value.chars() {
        if ch.is_ascii_digit() {
            if !numeric && !buffer.is_empty() {
                parts.push(VersionToken::Text(buffer.to_lowercase()));
                buffer.clear();
            }
            numeric = true;
            buffer.push(ch);
        } else if ch.is_ascii_alphabetic() {
            if numeric && !buffer.is_empty() {
                let number = buffer.parse::<u64>().unwrap_or(0);
                parts.push(VersionToken::Number(number));
                buffer.clear();
            }
            numeric = false;
            buffer.push(ch);
        } else {
            if !buffer.is_empty() {
                if numeric {
                    let number = buffer.parse::<u64>().unwrap_or(0);
                    parts.push(VersionToken::Number(number));
                } else {
                    parts.push(VersionToken::Text(buffer.to_lowercase()));
                }
                buffer.clear();
            }
            numeric = false;
        }
    }
    if !buffer.is_empty() {
        if numeric {
            let number = buffer.parse::<u64>().unwrap_or(0);
            parts.push(VersionToken::Number(number));
        } else {
            parts.push(VersionToken::Text(buffer.to_lowercase()));
        }
    }
    parts
}

fn smoke_test_command(interpreter: &Path, site_packages_dir: &Path, work_dir: &Path) -> Command {
    let mut command = Command::new(interpreter);
    command
        .arg(work_dir.join("smoke_test.py"))
        .current_dir(work_dir)
        .env("APDR_SITE_PACKAGES", site_packages_dir)
        .env("PYTHONPATH", site_packages_dir)
        .env("PYTHONNOUSERSITE", "1");
    command
}

fn run_python_install_requirements(
    interpreter: &Path,
    site_packages_dir: &Path,
    cache_dir: &Path,
    requirements_path: &Path,
    timeout: Duration,
) -> io::Result<CommandResult> {
    let mut command = Command::new(interpreter);
    command
        .arg("-m")
        .arg("pip")
        .arg("install")
        .arg("--disable-pip-version-check")
        .arg("--default-timeout=100")
        .arg("--cache-dir")
        .arg(cache_dir)
        .arg("--target")
        .arg(site_packages_dir)
        .arg("--no-build-isolation")
        .arg("-r")
        .arg(requirements_path)
        .env("PYTHONPATH", site_packages_dir)
        .env("PYTHONNOUSERSITE", "1");
    run_command_with_timeout(&mut command, timeout)
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
        "attempt_index: {}\npython_version: {}\nstatus: {}\nimage_tag: {}\nbuild_key: {}\nused_cached_image: {}\nused_cached_lockfile: {}\nerror_type: {}\nconflict_class: {}\nfix_applied: {}\nbuild_command: {}\nbuild_exit_code: {}\nbuild_duration_ms: {}\nrun_command: {}\nrun_exit_code: {}\nrun_duration_ms: {}\nartifact_dir: {}\n",
        attempt.attempt_index,
        attempt.python_version,
        attempt.status,
        attempt.image_tag.as_deref().unwrap_or("--"),
        build_key,
        attempt.used_cached_image,
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
