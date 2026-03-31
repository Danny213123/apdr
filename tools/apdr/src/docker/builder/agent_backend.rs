use super::docker_backend::{
    env_has_build_timeout, env_has_interpreter_failure, validate_requirements_docker_deterministic,
};
use super::process::{run_command_with_timeout, truncate_log};
use super::*;
use crate::docker::system_deps;
use crate::{
    ResolveConfig, ValidationAttempt, ValidationSummary, VALIDATION_BACKEND_DOCKER,
    VALIDATION_BACKEND_LLM,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

static DOCKER_AGENT_IMPORTABLE: OnceLock<bool> = OnceLock::new();

pub(super) fn validate_requirements_llm(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    store: &mut CacheStore,
) -> io::Result<ValidationSummary> {
    // Phase 1: Try the traditional env-based validation first
    let mut summary = validate_requirements_env(
        snippet_path,
        requirements_txt,
        imports,
        candidate_versions,
        attempt_offset,
        config,
        store,
    )?;
    summary.validation_backend = VALIDATION_BACKEND_LLM.to_string();

    if summary.succeeded {
        return Ok(summary);
    }

    // Phase 2: Route backend-recoverable env failures through a deterministic
    // Docker stage before falling back to the LangGraph multi-agent pipeline.
    let docker_escalated = llm_env_failure_requires_docker_escalation(&summary, requirements_txt);
    if docker_escalated {
        let reason = llm_env_failure_reason_for_docker_escalation(&summary, requirements_txt);
        eprintln!(
            "[llm-resolver] env validation failed ({} attempt(s), {}), retrying with Docker before LangGraph agent...",
            summary.attempts.len(),
            reason
        );
        let docker_offset = attempt_offset + summary.attempts.len();
        let mut docker_summary = validate_requirements_docker_deterministic(
            snippet_path,
            requirements_txt,
            imports,
            candidate_versions,
            docker_offset,
            config,
            store,
        )?;
        merge_backend_retry_history(&mut summary, &mut docker_summary);
        docker_summary.validation_backend = VALIDATION_BACKEND_LLM.to_string();
        docker_summary.escalated_backend = Some(VALIDATION_BACKEND_DOCKER.to_string());
        if docker_summary.succeeded {
            return Ok(docker_summary);
        }
        summary = docker_summary;
    }

    // Phase 3: Prior validation failed — fall back to the LangGraph multi-agent pipeline
    eprintln!(
        "[llm-resolver] validation failed ({} attempt(s)), trying LangGraph agent...",
        summary.attempts.len()
    );
    summary.agent_invocations += 1;
    if let Some(mut agent_summary) = attempt_langgraph_agent(
        snippet_path,
        requirements_txt,
        imports,
        candidate_versions,
        config,
    ) {
        inherit_prior_validation_context(&summary, &mut agent_summary);
        if docker_escalated && agent_summary.escalated_backend.is_none() {
            agent_summary.escalated_backend = Some(VALIDATION_BACKEND_DOCKER.to_string());
        }
        merge_llm_retry_history(&mut summary, &mut agent_summary);
        agent_summary.validation_backend = VALIDATION_BACKEND_LLM.to_string();
        return Ok(agent_summary);
    }

    // Agent unavailable or also failed — return the pre-agent result
    eprintln!("[llm-resolver] LangGraph agent unavailable or failed, returning pre-agent result");
    Ok(summary)
}

pub(super) fn merge_llm_retry_history(
    env_summary: &mut ValidationSummary,
    agent_summary: &mut ValidationSummary,
) {
    let next_attempt_index = env_summary
        .attempts
        .last()
        .map(|attempt| attempt.attempt_index + 1)
        .unwrap_or(1);

    for (offset, attempt) in agent_summary.attempts.iter_mut().enumerate() {
        if attempt.attempt_index == 0 {
            attempt.attempt_index = next_attempt_index + offset;
        }
    }

    let mut combined_attempts = std::mem::take(&mut env_summary.attempts);
    combined_attempts.append(&mut agent_summary.attempts);
    agent_summary.attempts = combined_attempts;
}

pub(super) fn llm_env_failure_requires_docker_escalation(
    summary: &ValidationSummary,
    requirements_txt: &str,
) -> bool {
    if summary.succeeded || summary.attempts.is_empty() || env_summary_is_host_runtime(summary) {
        return false;
    }

    let reqs_have_sys_deps =
        !system_deps::infer_system_deps_from_requirements(requirements_txt).is_empty();

    reqs_have_sys_deps
        || env_has_build_system_dep_failure(summary)
        || env_has_interpreter_failure(summary)
        || env_has_build_timeout(summary)
        || env_has_version_not_found(summary)
        || summary.failure_bucket == "environment-build-failed"
        || summary.failure_bucket == "version-not-found"
}

fn llm_env_failure_reason_for_docker_escalation(
    summary: &ValidationSummary,
    requirements_txt: &str,
) -> &'static str {
    let reqs_have_sys_deps =
        !system_deps::infer_system_deps_from_requirements(requirements_txt).is_empty();
    if env_has_interpreter_failure(summary) {
        "missing local Python interpreter"
    } else if env_has_build_timeout(summary) {
        "build timeout"
    } else if reqs_have_sys_deps {
        "packages require system libraries"
    } else if env_has_build_system_dep_failure(summary) {
        "system dependency build errors"
    } else if env_has_version_not_found(summary) || summary.failure_bucket == "version-not-found" {
        "version not found"
    } else {
        "backend-recoverable env failure"
    }
}

fn env_has_build_system_dep_failure(summary: &ValidationSummary) -> bool {
    summary.attempts.iter().any(|attempt| {
        matches!(attempt.status.as_str(), "build-failed" | "runtime-failed")
            && !system_deps::extract_system_deps_from_log(&attempt.log_excerpt).is_empty()
    })
}

fn env_has_version_not_found(summary: &ValidationSummary) -> bool {
    summary.attempts.iter().any(|attempt| {
        let lower = attempt.log_excerpt.to_ascii_lowercase();
        lower.contains("no matching distribution found")
            || lower.contains("could not find a version that satisfies the requirement")
            || lower.contains("versionnotfound")
    })
}

fn env_summary_is_host_runtime(summary: &ValidationSummary) -> bool {
    if summary.status == "skipped-host-runtime" || summary.failure_bucket == "skipped-host-runtime"
    {
        return true;
    }

    let host_runtime_markers = [
        "host runtime",
        "host framework runtime",
        "framework dependency",
        "pyobjc",
        "systemconfiguration",
        "opendirectory",
    ];
    summary
        .reason
        .iter()
        .chain(summary.root_cause.iter())
        .any(|text| {
            let lower = text.to_ascii_lowercase();
            host_runtime_markers
                .iter()
                .any(|marker| lower.contains(marker))
        })
}

fn inherit_prior_validation_context(
    prior_summary: &ValidationSummary,
    agent_summary: &mut ValidationSummary,
) {
    agent_summary.agent_invocations = prior_summary.agent_invocations;
    agent_summary.validation_duration_ms += prior_summary.validation_duration_ms;
    agent_summary.llm_duration_ms += prior_summary.llm_duration_ms;
    agent_summary.env_create_duration_ms += prior_summary.env_create_duration_ms;
    agent_summary.install_duration_ms += prior_summary.install_duration_ms;
    agent_summary.docker_startup_duration_ms += prior_summary.docker_startup_duration_ms;
    agent_summary.smoke_duration_ms += prior_summary.smoke_duration_ms;
    if agent_summary.failure_bucket.is_empty() {
        agent_summary.failure_bucket = prior_summary.failure_bucket.clone();
    }
    if agent_summary.root_cause.is_none() {
        agent_summary.root_cause = prior_summary.root_cause.clone();
    }
    if agent_summary.missing_module.is_none() {
        agent_summary.missing_module = prior_summary.missing_module.clone();
    }
    if agent_summary.failing_package.is_none() {
        agent_summary.failing_package = prior_summary.failing_package.clone();
    }
    if agent_summary.repair_strategy_applied.is_none() {
        agent_summary.repair_strategy_applied = prior_summary.repair_strategy_applied.clone();
    }
    if agent_summary.repeat_failure_signature.is_none() {
        agent_summary.repeat_failure_signature = prior_summary.repeat_failure_signature.clone();
    }
    if agent_summary.selected_python_version.is_none() {
        agent_summary.selected_python_version = prior_summary.selected_python_version.clone();
    }
    if agent_summary.lockfile_key.is_none() {
        agent_summary.lockfile_key = prior_summary.lockfile_key.clone();
    }
    if agent_summary.build_cache_key.is_none() {
        agent_summary.build_cache_key = prior_summary.build_cache_key.clone();
    }
    if agent_summary.reason.is_none() {
        agent_summary.reason = prior_summary.reason.clone();
    }
}

pub(super) fn attempt_langgraph_agent(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    config: &ResolveConfig,
) -> Option<ValidationSummary> {
    // Find the docker_agent Python module relative to this binary's directory.
    // The module lives at tools/apdr/docker_agent/ â€” we discover it by walking
    // up from the binary's directory or from CARGO_MANIFEST_DIR at test time.
    let agent_parent = find_docker_agent_parent()?;

    if !docker_agent_importable(&agent_parent) {
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
        snippet_path
            .display()
            .to_string()
            .replace('\\', "\\\\")
            .replace('"', "\\\""),
        requirements_txt
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n"),
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

    eprintln!("[docker-agent] invoking LangGraph multi-agent pipelineâ€¦");
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
    parse_agent_result(json_line, &agent_output_dir)
}

pub(super) fn docker_agent_importable(agent_parent: &Path) -> bool {
    docker_agent_importable_with_probe(&DOCKER_AGENT_IMPORTABLE, agent_parent, |path| {
        run_docker_agent_import_probe(path)
    })
}

pub(super) fn docker_agent_importable_with_probe<F>(
    cache: &OnceLock<bool>,
    agent_parent: &Path,
    probe: F,
) -> bool
where
    F: FnOnce(&Path) -> bool,
{
    *cache.get_or_init(|| probe(agent_parent))
}

pub(super) fn run_docker_agent_import_probe(agent_parent: &Path) -> bool {
    Command::new("python3")
        .args(["-c", "import docker_agent"])
        .env("PYTHONPATH", agent_parent)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

pub(super) fn find_docker_agent_parent() -> Option<PathBuf> {
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

pub(super) fn parse_agent_result(
    json_str: &str,
    agent_output_dir: &Path,
) -> Option<ValidationSummary> {
    let value = serde_json::from_str::<serde_json::Value>(json_str).ok()?;
    let status = value.get("status")?.as_str()?;
    let supported_status = status == "passed" || status == "abstained" || status == "failed";
    if !supported_status {
        eprintln!("[docker-agent] agent returned unsupported status={status}");
        return None;
    }

    let reason = value
        .get("error_detail")
        .and_then(|item| item.as_str())
        .filter(|item| !item.trim().is_empty())
        .or_else(|| {
            value
                .get("confidence_reason")
                .and_then(|item| item.as_str())
                .filter(|item| !item.trim().is_empty())
        })
        .map(|item| item.to_string())
        .unwrap_or_else(|| format!("LangGraph agent returned status={status}"));

    let mut attempts = Vec::with_capacity(1);
    attempts.push(ValidationAttempt {
        status: status.to_string(),
        validation_backend: VALIDATION_BACKEND_LLM.to_string(),
        log_excerpt: reason.clone(),
        artifact_dir: Some(agent_output_dir.display().to_string()),
        ..ValidationAttempt::default()
    });

    let mut summary = ValidationSummary {
        succeeded: status == "passed",
        status: status.to_string(),
        reason: Some(reason),
        validation_backend: VALIDATION_BACKEND_LLM.to_string(),
        selected_python_version: value
            .get("selected_python_version")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string()),
        llm_trace_dir: Some(agent_output_dir.display().to_string()),
        attempts,
        ..ValidationSummary::default()
    };

    if let Some(dur) = value.get("total_duration_ms").and_then(json_value_as_u128) {
        summary.validation_duration_ms = dur;
        summary.llm_duration_ms = dur;
    }

    Some(summary)
}

pub(super) fn json_value_as_u128(value: &serde_json::Value) -> Option<u128> {
    if let Some(number) = value.as_u64() {
        return Some(number as u128);
    }
    value
        .as_f64()
        .filter(|number| *number >= 0.0)
        .map(|number| number as u128)
}
