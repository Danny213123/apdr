use std::path::{Path, PathBuf};
use std::process::Command;
pub(super) fn run_host_python(args: &[&str]) -> Option<std::process::Output> {
    let python = host_python_command()?;
    Command::new(python).args(args).output().ok()
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
