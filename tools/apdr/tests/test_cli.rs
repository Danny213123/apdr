use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_output_dir(tool_root: &PathBuf, label: &str) -> PathBuf {
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
    assert!(stdout.contains("VALIDATION_SUCCEEDED=true"));
    assert!(stdout.contains("DEBUG_DIR="));
    assert!(stdout.contains("CONTEXT_LOG="));
    assert!(stdout.contains("ENV_CREATE_DURATION_MS=0"));

    let requirements = fs::read_to_string(output_dir.join("requirements.txt")).unwrap();
    let report = fs::read_to_string(output_dir.join("resolution-report.txt")).unwrap();
    assert!(requirements.contains("requests==2.32.3"));
    assert!(requirements.contains("beautifulsoup4==4.12.3"));
    assert!(report.contains("env_create_duration_ms: 0"));
    assert!(report.contains("validation_duration_ms: 0"));
    assert!(report.contains("install_duration_ms: 0"));
    assert!(report.contains("smoke_duration_ms: 0"));
    assert!(output_dir.join(".apdr-debug").join("parse-summary.txt").exists());
    assert!(output_dir.join(".apdr-debug").join("benchmark-context.log").exists());

    fs::remove_dir_all(output_dir).unwrap();
}
