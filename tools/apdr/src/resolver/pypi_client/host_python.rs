use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub(super) fn run_host_python(args: &[&str]) -> Option<std::process::Output> {
    let python = host_python_command()?;
    run_command_with_timeout(&python, args, host_python_timeout())
}

pub(super) fn host_python_command() -> Option<PathBuf> {
    let mut candidates = vec![PathBuf::from("python3"), PathBuf::from("python")];
    if cfg!(windows) {
        for version in ["312", "311", "310", "39"] {
            if let Some(local_appdata) = std::env::var_os("LOCALAPPDATA") {
                candidates.push(
                    PathBuf::from(&local_appdata)
                        .join("Programs")
                        .join("Python")
                        .join(format!("Python{version}"))
                        .join("python.exe"),
                );
            }
            for variable in ["ProgramFiles", "ProgramFiles(x86)"] {
                if let Some(base) = std::env::var_os(variable) {
                    candidates.push(
                        PathBuf::from(&base)
                            .join("Python")
                            .join(format!("Python{version}"))
                            .join("python.exe"),
                    );
                }
            }
        }
    }
    dedupe_paths(candidates)
        .into_iter()
        .find(|candidate| is_python3(candidate))
}

fn run_command_with_timeout(
    command: &Path,
    args: &[&str],
    timeout: Duration,
) -> Option<std::process::Output> {
    let mut child = Command::new(command)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                if let Some(handle) = child.stdout.as_mut() {
                    let _ = handle.read_to_end(&mut stdout);
                }
                if let Some(handle) = child.stderr.as_mut() {
                    let _ = handle.read_to_end(&mut stderr);
                }
                return Some(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            Ok(None) if started.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => thread::sleep(Duration::from_millis(25)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

fn host_python_timeout() -> Duration {
    let seconds = std::env::var("APDR_HOST_PYTHON_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(20);
    Duration::from_secs(seconds)
}
fn is_python3(candidate: &Path) -> bool {
    let Ok(output) = Command::new(candidate)
        .arg("-c")
        .arg("import sys; sys.stdout.write('%s' % sys.version_info[0])")
        .output()
    else {
        return false;
    };
    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "3"
}
pub(super) fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_out_host_python_process_returns_none() {
        let python = host_python_command().expect("host python should be available in tests");
        let output = run_command_with_timeout(
            &python,
            &["-c", "import time; time.sleep(1)"],
            Duration::from_millis(50),
        );
        assert!(output.is_none());
    }
}
