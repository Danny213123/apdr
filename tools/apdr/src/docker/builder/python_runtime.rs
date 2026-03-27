use super::process::{
    command_on_path, install_with_miniforge, run_install_command, summarize_command_output,
};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

pub(super) fn find_python_interpreter(python_version: &str) -> Option<PathBuf> {
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

pub(super) fn path_matches_python_version(candidate: &Path, python_version: &str) -> bool {
    let output = Command::new(candidate)
        .arg("-c")
        .arg("import sys; sys.stdout.write('%s.%s' % (sys.version_info[0], sys.version_info[1]))")
        .output();
    let Ok(output) = output else {
        return false;
    };
    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == python_version
}

pub(super) fn windows_launcher_python_path(python_version: &str) -> Option<PathBuf> {
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

pub(super) fn windows_launcher_version_arg(python_version: &str) -> Option<String> {
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

pub(super) fn normalized_command_output_path(output: &str) -> Option<PathBuf> {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

pub(super) fn ensure_python_interpreter(python_version: &str) -> Result<PathBuf, String> {
    if let Some(path) = find_python_interpreter(python_version) {
        return Ok(path);
    }

    let detail = maybe_auto_install_python_interpreter(python_version);
    if let Some(path) = find_python_interpreter(python_version) {
        return Ok(path);
    }

    Err(detail.unwrap_or_else(|| missing_interpreter_message(python_version, "")))
}

pub(super) fn maybe_auto_install_python_interpreter(python_version: &str) -> Option<String> {
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

pub(super) fn auto_install_enabled() -> bool {
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

pub(super) fn attempt_python_auto_install(python_version: &str) -> String {
    // Track managers that failed in ANY previous attempt so we don't retry
    // slow/broken managers (e.g. winget blocked by org policy) for every version.
    use std::collections::BTreeSet;
    static FAILED_MANAGERS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    let failed = FAILED_MANAGERS.get_or_init(|| Mutex::new(BTreeSet::new()));
    let is_failed =
        |name: &str| -> bool { failed.lock().map(|set| set.contains(name)).unwrap_or(false) };
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

pub(super) fn missing_interpreter_message(python_version: &str, extra: &str) -> String {
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

pub(super) fn python_install_specs(python_version: &str) -> Vec<String> {
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

pub(super) fn python_interpreter_candidates(python_version: &str) -> Vec<PathBuf> {
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

pub(super) fn known_python_interpreter_paths(python_version: &str) -> Vec<PathBuf> {
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
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
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

pub(super) fn managed_python_roots() -> Vec<PathBuf> {
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

pub(super) fn matching_version_dirs(root: &Path, version: &str) -> Vec<PathBuf> {
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

pub(super) fn host_python_for_metadata() -> Option<PathBuf> {
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

pub(super) fn windows_winget_python_package(python_version: &str) -> Option<&'static str> {
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

pub(super) fn windows_scoop_python_package(python_version: &str) -> Option<&'static str> {
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

