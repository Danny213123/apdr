use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

struct LlmProcess {
    stdin: ChildStdin,
    stdout: Arc<Mutex<BufReader<ChildStdout>>>,
    child: Child,
}

static LLM_PROCESS: OnceLock<Mutex<Option<LlmProcess>>> = OnceLock::new();

const DEFAULT_LLM_PY_STARTUP_TIMEOUT_SECS: u64 = 10;
const DEFAULT_LLM_PY_REQUEST_TIMEOUT_SECS: u64 = 90;

enum ReadLineOutcome {
    Line(String),
    Failure(String),
    Timeout,
}

pub(super) fn find_python() -> String {
    if let Ok(py) = std::env::var("APDR_PYTHON") {
        if !py.is_empty() {
            return py;
        }
    }
    let mut candidates: Vec<String> = Vec::new();
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let sep = if cfg!(windows) { "\\" } else { "/" };
        if cfg!(windows) {
            candidates.push(format!("{prefix}{sep}python.exe"));
        } else {
            candidates.push(format!("{prefix}{sep}bin{sep}python"));
        }
    }
    candidates.extend(["python3".to_string(), "python".to_string()]);
    if cfg!(windows) {
        for ver in &["3.12", "3.11", "3.10", "3.9"] {
            candidates.push(format!("py -{ver}"));
        }
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            for ver in &["312", "311", "310", "39"] {
                candidates.push(format!(
                    "{local}\\Programs\\Python\\Python{ver}\\python.exe"
                ));
            }
        }
    }
    for cmd in &candidates {
        let parts: Vec<&str> = cmd.split_whitespace().collect();
        let (program, extra_args) = (parts[0], &parts[1..]);
        if Command::new(program)
            .args(extra_args)
            .args(["-c", "import pydantic"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return cmd.to_string();
        }
    }
    "python3".to_string()
}

fn llm_py_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("APDR_LLM_PY_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(exe) = std::env::current_exe() {
        let mut p = exe.as_path();
        for _ in 0..6 {
            if let Some(parent) = p.parent() {
                let candidate = parent.join("llm_py");
                if candidate.join("__main__.py").exists() {
                    return candidate;
                }
                let candidate2 = parent.join("tools").join("apdr").join("llm_py");
                if candidate2.join("__main__.py").exists() {
                    return candidate2;
                }
                p = parent;
            }
        }
    }
    PathBuf::from("tools/apdr/llm_py")
}

fn llm_process_slot() -> &'static Mutex<Option<LlmProcess>> {
    LLM_PROCESS.get_or_init(|| Mutex::new(None))
}

fn timeout_from_env(name: &str, default_secs: u64) -> Duration {
    let seconds = std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_secs);
    Duration::from_secs(seconds)
}

fn llm_py_startup_timeout() -> Duration {
    timeout_from_env(
        "APDR_LLM_PY_STARTUP_TIMEOUT_SECS",
        DEFAULT_LLM_PY_STARTUP_TIMEOUT_SECS,
    )
}

fn llm_py_request_timeout(action: &str) -> Duration {
    let action_key = action
        .trim()
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' => ch.to_ascii_uppercase(),
            _ => '_',
        })
        .collect::<String>();
    let scoped_env = format!("APDR_LLM_PY_TIMEOUT_{}_SECS", action_key);
    std::env::var(&scoped_env)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| {
            timeout_from_env(
                "APDR_LLM_PY_TIMEOUT_SECS",
                DEFAULT_LLM_PY_REQUEST_TIMEOUT_SECS,
            )
        })
}

fn read_line_with_timeout(
    stdout: Arc<Mutex<BufReader<ChildStdout>>>,
    timeout: Duration,
) -> ReadLineOutcome {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let outcome = match stdout.lock() {
            Ok(mut reader) => {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(_) => ReadLineOutcome::Line(line),
                    Err(err) => ReadLineOutcome::Failure(format!("failed to read stdout: {err}")),
                }
            }
            Err(_) => ReadLineOutcome::Failure("failed to lock llm_py stdout".to_string()),
        };
        let _ = sender.send(outcome);
    });
    receiver
        .recv_timeout(timeout)
        .unwrap_or(ReadLineOutcome::Timeout)
}

fn reset_llm_process(slot: &mut Option<LlmProcess>, reason: &str) {
    if let Some(mut process) = slot.take() {
        let pid = process.child.id();
        eprintln!("[Tier 3 LLM] resetting Python service (pid {pid}) after {reason}");
        let _ = process.child.kill();
        let _ = process.child.wait();
    }
}

fn spawn_python_process() -> Option<LlmProcess> {
    let python = find_python();
    let py_dir = llm_py_dir();
    let parent = py_dir.parent().unwrap_or_else(|| Path::new("."));

    let parts: Vec<&str> = python.split_whitespace().collect();
    let (program, extra_args) = (parts[0], &parts[1..]);
    let mut child = Command::new(program)
        .args(extra_args)
        .arg("-m")
        .arg("llm_py")
        .current_dir(parent)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| {
            eprintln!(
                "[Tier 3 LLM] Python service unavailable: failed to spawn `{python} -m llm_py` from {}: {err}",
                parent.display()
            );
            err
        })
        .ok()?;

    let Some(stdin) = child.stdin.take() else {
        eprintln!("[Tier 3 LLM] Python service unavailable: failed to capture stdin");
        let _ = child.kill();
        let _ = child.wait();
        return None;
    };
    let Some(stdout) = child.stdout.take() else {
        eprintln!("[Tier 3 LLM] Python service unavailable: failed to capture stdout");
        let _ = child.kill();
        let _ = child.wait();
        return None;
    };
    let stdout = Arc::new(Mutex::new(BufReader::new(stdout)));

    match read_line_with_timeout(stdout.clone(), llm_py_startup_timeout()) {
        ReadLineOutcome::Line(ready_line) => {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&ready_line) {
                if json.get("ready").and_then(|v| v.as_bool()) != Some(true) {
                    eprintln!("[Tier 3 LLM] Python service did not send a ready signal");
                }
            }
        }
        ReadLineOutcome::Failure(reason) => {
            eprintln!("[Tier 3 LLM] Python service unavailable: {reason}");
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        ReadLineOutcome::Timeout => {
            eprintln!(
                "[Tier 3 LLM] Python service unavailable: startup timed out after {:.1}s",
                llm_py_startup_timeout().as_secs_f64()
            );
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
    }

    Some(LlmProcess {
        stdin,
        stdout,
        child,
    })
}

pub(super) fn call_python(request: &serde_json::Value) -> Option<serde_json::Value> {
    let action = request
        .get("action")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let timeout = llm_py_request_timeout(action);
    let slot = llm_process_slot();
    let mut guard = slot.lock().ok()?;
    if guard.is_none() {
        *guard = spawn_python_process();
    }
    let (stdout, pid) = {
        let process = guard.as_mut()?;
        let request_str = serde_json::to_string(request).ok()?;
        if writeln!(process.stdin, "{}", request_str).is_err() || process.stdin.flush().is_err() {
            reset_llm_process(
                &mut guard,
                &format!("failed to send `{action}` request to llm_py"),
            );
            return None;
        }
        (process.stdout.clone(), process.child.id())
    };

    let line = match read_line_with_timeout(stdout, timeout) {
        ReadLineOutcome::Line(line) => line,
        ReadLineOutcome::Failure(reason) => {
            reset_llm_process(
                &mut guard,
                &format!("`{action}` response read failed for llm_py pid {pid}: {reason}"),
            );
            return None;
        }
        ReadLineOutcome::Timeout => {
            reset_llm_process(
                &mut guard,
                &format!(
                    "`{action}` timed out after {:.1}s for llm_py pid {pid}",
                    timeout.as_secs_f64()
                ),
            );
            return None;
        }
    };
    if line.trim().is_empty() {
        reset_llm_process(
            &mut guard,
            &format!("`{action}` returned an empty response from llm_py pid {pid}"),
        );
        return None;
    }
    serde_json::from_str(line.trim()).ok()
}

#[cfg(test)]
fn reset_llm_process_for_tests() {
    if let Ok(mut guard) = llm_process_slot().lock() {
        reset_llm_process(&mut guard, "test reset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Instant;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn set_env(name: &str, value: &str) -> Option<String> {
        let previous = std::env::var(name).ok();
        std::env::set_var(name, value);
        previous
    }

    fn restore_env(name: &str, previous: Option<String>) {
        if let Some(value) = previous {
            std::env::set_var(name, value);
        } else {
            std::env::remove_var(name);
        }
    }

    #[test]
    fn llm_process_timeout_resets_stuck_child() {
        let _env_guard = env_lock().lock().unwrap();
        reset_llm_process_for_tests();

        let temp = tempfile::TempDir::new().unwrap();
        let llm_dir = temp.path().join("llm_py");
        fs::create_dir_all(&llm_dir).unwrap();
        let marker = temp.path().join("first-request.marker");
        let script = [
            "import os, sys, time".to_string(),
            format!("marker = {:?}", marker.to_string_lossy()),
            "sys.stdout.write('{\"ready\": true}\\n')".to_string(),
            "sys.stdout.flush()".to_string(),
            "for _line in sys.stdin:".to_string(),
            "    if not os.path.exists(marker):".to_string(),
            "        open(marker, 'w').close()".to_string(),
            "        time.sleep(2.0)".to_string(),
            "    sys.stdout.write('{\"status\": \"ok\"}\\n')".to_string(),
            "    sys.stdout.flush()".to_string(),
            String::new(),
        ]
        .join("\n");
        fs::write(llm_dir.join("__main__.py"), script).unwrap();

        let previous_python = set_env("APDR_PYTHON", &find_python());
        let previous_dir = set_env("APDR_LLM_PY_DIR", &llm_dir.display().to_string());
        let previous_timeout = set_env("APDR_LLM_PY_TIMEOUT_SECS", "1");
        let previous_startup_timeout = set_env("APDR_LLM_PY_STARTUP_TIMEOUT_SECS", "5");

        let first_started = Instant::now();
        let first = call_python(&serde_json::json!({ "action": "docker_plan" }));
        assert!(first.is_none());
        assert!(first_started.elapsed() < Duration::from_secs(2));

        let second = call_python(&serde_json::json!({ "action": "docker_plan" }))
            .expect("second call should respawn llm_py");
        assert_eq!(
            second.get("status").and_then(|value| value.as_str()),
            Some("ok")
        );

        restore_env("APDR_LLM_PY_STARTUP_TIMEOUT_SECS", previous_startup_timeout);
        restore_env("APDR_LLM_PY_TIMEOUT_SECS", previous_timeout);
        restore_env("APDR_LLM_PY_DIR", previous_dir);
        restore_env("APDR_PYTHON", previous_python);
        reset_llm_process_for_tests();
    }
}
