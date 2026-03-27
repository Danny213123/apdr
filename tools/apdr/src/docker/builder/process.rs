use super::*;
use super::env_backend::PACKAGE_REPOSITORY_CATALOG_SCRIPT;
use super::python_runtime::{
    host_python_for_metadata, path_matches_python_version, python_install_specs,
};
use crate::cache::store::CacheStore;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

pub(super) fn catalog_package_repository(
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

pub(super) fn combined_output(stdout: &[u8], stderr: &[u8]) -> String {
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

pub(super) fn run_command_with_timeout(command: &mut Command, timeout: Duration) -> io::Result<CommandResult> {
    // Redirect stdout+stderr to a temp file instead of piping.
    // On Windows, docker.exe (BuildKit) can deadlock when its output is piped
    // because docker-buildx.exe inherits the pipe handles and keeps them open
    // even after docker.exe is done writing.  File redirection avoids this.
    let tmp_out = tempfile::NamedTempFile::new()?;
    let out_file = fs::File::create(tmp_out.path())?;
    let err_file = out_file.try_clone()?;

    let mut child = command.stdout(out_file).stderr(err_file).spawn()?;
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

pub(super) fn truncate_log(log: &str) -> String {
    let lines = log
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let start = lines.len().saturating_sub(25);
    lines[start..].join("\n")
}

pub(super) fn sanitized_env_label(build_key: &str, python_version: &str) -> String {
    format!(
        "apdr-env:{}-py{}",
        build_key.replace(':', "-"),
        python_version.replace('.', "_")
    )
}

pub(super) fn docker_image_tag(build_key: &str, python_version: &str) -> String {
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

pub(super) fn docker_container_name(build_key: &str, python_version: &str, attempt_index: usize) -> String {
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


pub(super) fn command_on_path(command: &str) -> bool {
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

pub(super) fn run_install_command(command: &str, args: &[&str]) -> (bool, String) {
    let output = Command::new(command).args(args).output();
    let Ok(output) = output else {
        return (false, format!("failed to start {command}"));
    };
    (
        output.status.success(),
        combined_output(&output.stdout, &output.stderr),
    )
}

pub(super) fn summarize_command_output(output: &str) -> String {
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

pub(super) fn install_with_miniforge(python_version: &str) -> Result<String, String> {
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

pub(super) fn ensure_unix_miniforge() -> Result<PathBuf, String> {
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

pub(super) fn download_with_host_python(url: &str, destination: &Path) -> Result<(), String> {
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

pub(super) fn unix_miniforge_root() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .map(|home| home.join(".apdr").join("miniforge3"))
}

pub(super) fn unix_miniforge_installer_url() -> Option<&'static str> {
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

