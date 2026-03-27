use super::process::{run_command_with_timeout, truncate_log};
use super::*;
use crate::{ResolveConfig, ValidationSummary, VALIDATION_BACKEND_LLM};
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

    // Phase 2: Env validation failed â€” fall back to the LangGraph multi-agent pipeline
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

    // Agent unavailable or also failed â€” return the original env result
    eprintln!("[llm-resolver] LangGraph agent unavailable or failed, returning env result");
    Ok(env_summary)
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
    parse_agent_result(json_line)
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

pub(super) fn parse_agent_result(json_str: &str) -> Option<ValidationSummary> {
    let value = serde_json::from_str::<serde_json::Value>(json_str).ok()?;
    let status = value.get("status")?.as_str()?;
    if status != "passed" {
        eprintln!("[docker-agent] agent returned status={status}, falling back to deterministic");
        return None;
    }

    let mut summary = ValidationSummary {
        succeeded: true,
        validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
        selected_python_version: value
            .get("selected_python_version")
            .and_then(|item| item.as_str())
            .map(|item| item.to_string()),
        ..ValidationSummary::default()
    };

    if let Some(dur) = value.get("total_duration_ms").and_then(json_value_as_u128) {
        summary.validation_duration_ms = dur;
    }

    // The agent may have modified requirements; we record that info but
    // it does not change the resolved list (the Rust side already resolved).
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

