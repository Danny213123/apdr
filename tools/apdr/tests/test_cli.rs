use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_output_dir(tool_root: &Path, label: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    tool_root.join("target").join(format!("{label}-{stamp}"))
}

#[test]
fn cli_resolves_from_stdin_without_validation() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = unique_output_dir(&tool_root, "stdin-output");
    let binary = env!("CARGO_BIN_EXE_apdr");
    let snippet = "import requests\nfrom bs4 import BeautifulSoup\n";

    let output = Command::new(binary)
        .arg("resolve")
        .arg("--stdin")
        .arg("--output")
        .arg(&output_dir)
        .arg("--no-validate")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;

            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(snippet.as_bytes())?;
            child.wait_with_output()
        })
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("SOLVE_DURATION_MS="));
    assert!(stdout.contains("VALIDATION_DURATION_MS="));
    assert!(stdout.contains("VALIDATION_SUCCEEDED=true"));
    assert!(stdout.contains("DEBUG_DIR="));
    assert!(stdout.contains("CONTEXT_LOG="));
    assert!(stdout.contains("ENV_CREATE_DURATION_MS=0"));
    assert!(stdout.contains("INSTALL_DURATION_MS=0"));
    assert!(stdout.contains("SMOKE_DURATION_MS=0"));

    let requirements = fs::read_to_string(output_dir.join("requirements.txt")).unwrap();
    let report = fs::read_to_string(output_dir.join("resolution-report.txt")).unwrap();
    assert!(requirements.contains("requests==2.32.3"));
    assert!(requirements.contains("beautifulsoup4==4.12.3"));
    assert!(report.contains("env_create_duration_ms: 0"));
    assert!(report.contains("validation_duration_ms: 0"));
    assert!(report.contains("install_duration_ms: 0"));
    assert!(report.contains("smoke_duration_ms: 0"));
    assert!(output_dir
        .join(".apdr-debug")
        .join("parse-summary.txt")
        .exists());
    assert!(output_dir
        .join(".apdr-debug")
        .join("benchmark-context.log")
        .exists());

    fs::remove_dir_all(output_dir).unwrap();
}

#[test]
fn cli_prunes_cache_at_custom_path() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_output_dir(&tool_root, "cache-prune-cli");
    let envs_dir = cache_path.join("validated-envs");
    fs::create_dir_all(envs_dir.join("build-old")).unwrap();
    fs::create_dir_all(envs_dir.join("build-new")).unwrap();
    fs::write(
        envs_dir.join("build-old").join("payload.bin"),
        vec![0u8; 40],
    )
    .unwrap();
    fs::write(
        envs_dir.join("build-new").join("payload.bin"),
        vec![0u8; 50],
    )
    .unwrap();
    fs::write(envs_dir.join("build-old").join(".apdr-last-used"), "100").unwrap();
    fs::write(envs_dir.join("build-new").join(".apdr-last-used"), "200").unwrap();
    fs::create_dir_all(cache_path.join("package-repository")).unwrap();
    fs::write(
        cache_path.join("package-repository").join("pkg.bin"),
        vec![0u8; 31],
    )
    .unwrap();
    fs::create_dir_all(cache_path.join("pip-cache")).unwrap();
    fs::write(cache_path.join("pip-cache").join("pip.bin"), vec![0u8; 29]).unwrap();

    let binary = env!("CARGO_BIN_EXE_apdr");
    let output = Command::new(binary)
        .arg("cache")
        .arg("--cache-path")
        .arg(&cache_path)
        .arg("prune")
        .arg("--max-validated-envs")
        .arg("1")
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("REMOVED_VALIDATED_ENVS=1"));
    assert!(stdout.contains("REMOVED_PACKAGE_REPOSITORY=true"));
    assert!(stdout.contains("REMOVED_LEGACY_PIP_CACHE=true"));
    assert!(!cache_path.join("package-repository").exists());
    assert!(!cache_path.join("pip-cache").exists());
    assert!(!cache_path.join("validated-envs").join("build-old").exists());
    assert!(cache_path.join("validated-envs").join("build-new").exists());

    fs::remove_dir_all(cache_path).unwrap();
}
