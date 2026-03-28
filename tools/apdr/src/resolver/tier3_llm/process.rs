use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Mutex, OnceLock};

struct LlmProcess {
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    _child: Child,
}

static LLM_PROCESS: OnceLock<Option<Mutex<LlmProcess>>> = OnceLock::new();

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

fn spawn_python_process() -> Option<Mutex<LlmProcess>> {
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
                "[tier3-llm] Python LLM service unavailable: failed to spawn `{python} -m llm_py` from {}: {err}",
                parent.display()
            );
            err
        })
        .ok()?;

    let Some(stdin) = child.stdin.take() else {
        eprintln!("[tier3-llm] Python LLM service unavailable: failed to capture stdin");
        let _ = child.kill();
        let _ = child.wait();
        return None;
    };
    let Some(stdout) = child.stdout.take() else {
        eprintln!("[tier3-llm] Python LLM service unavailable: failed to capture stdout");
        let _ = child.kill();
        let _ = child.wait();
        return None;
    };
    let mut reader = BufReader::new(stdout);

    let mut ready_line = String::new();
    if reader.read_line(&mut ready_line).is_ok() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&ready_line) {
            if json.get("ready").and_then(|v| v.as_bool()) != Some(true) {
                eprintln!("Warning: Python LLM service did not send ready signal");
            }
        }
    }

    Some(Mutex::new(LlmProcess {
        stdin,
        stdout: reader,
        _child: child,
    }))
}

pub(super) fn call_python(request: &serde_json::Value) -> Option<serde_json::Value> {
    let process = LLM_PROCESS.get_or_init(spawn_python_process).as_ref()?;
    let mut guard = process.lock().ok()?;

    let request_str = serde_json::to_string(request).ok()?;
    writeln!(guard.stdin, "{}", request_str).ok()?;
    guard.stdin.flush().ok()?;

    let mut line = String::new();
    guard.stdout.read_line(&mut line).ok()?;
    if line.trim().is_empty() {
        return None;
    }
    serde_json::from_str(line.trim()).ok()
}
