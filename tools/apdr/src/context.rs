use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn debug_root(output_dir: &Path) -> PathBuf {
    output_dir.join(".apdr-debug")
}

pub fn attempts_root(output_dir: &Path) -> PathBuf {
    debug_root(output_dir).join("attempts")
}

pub fn llm_root(output_dir: &Path) -> PathBuf {
    debug_root(output_dir).join("llm")
}

pub fn iterations_root(output_dir: &Path) -> PathBuf {
    debug_root(output_dir).join("iterations")
}

pub fn ensure_debug_layout(output_dir: &Path) -> io::Result<()> {
    fs::create_dir_all(debug_root(output_dir))?;
    fs::create_dir_all(attempts_root(output_dir))?;
    fs::create_dir_all(llm_root(output_dir))?;
    fs::create_dir_all(iterations_root(output_dir))?;
    Ok(())
}

pub fn append_context_log(
    path: Option<&Path>,
    kind: &str,
    message: &str,
) -> io::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let block = format!(
        "===== {timestamp} kind={kind} =====\n{}\n\n",
        message.trim_end()
    );
    let mut handle = OpenOptions::new().create(true).append(true).open(path)?;
    handle.write_all(block.as_bytes())?;
    handle.flush()?;
    Ok(())
}

pub fn read_context_tail(path: Option<&Path>, max_bytes: usize) -> io::Result<String> {
    let Some(path) = path else {
        return Ok(String::new());
    };
    if !path.exists() {
        return Ok(String::new());
    }

    let mut handle = OpenOptions::new().read(true).open(path)?;
    let size = handle.metadata()?.len();
    let start = size.saturating_sub(max_bytes as u64);
    handle.seek(SeekFrom::Start(start))?;
    let mut buffer = Vec::new();
    handle.read_to_end(&mut buffer)?;
    let text = String::from_utf8_lossy(&buffer).trim().to_string();
    if text.is_empty() {
        return Ok(String::new());
    }
    if start > 0 {
        return Ok(format!("[older benchmark context omitted]\n{text}"));
    }
    Ok(text)
}

pub fn write_text(path: &Path, contents: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)
}

pub fn relative_path(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .unwrap_or(path)
        .display()
        .to_string()
}

pub fn attempt_dir(output_dir: &Path, attempt_index: usize, python_version: &str) -> PathBuf {
    attempts_root(output_dir).join(format!(
        "attempt-{attempt_index:03}-py-{}",
        sanitize_segment(python_version)
    ))
}

pub fn iteration_dir(output_dir: &Path, iteration_index: usize) -> PathBuf {
    iterations_root(output_dir).join(format!("iteration-{iteration_index:03}"))
}

pub fn create_llm_trace_dir(output_dir: &Path, label: &str) -> io::Result<PathBuf> {
    let root = llm_root(output_dir);
    fs::create_dir_all(&root)?;
    let next_index = next_sequence_index(&root)?;
    let dir = root.join(format!("call-{next_index:03}-{}", sanitize_segment(label)));
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn next_sequence_index(root: &Path) -> io::Result<usize> {
    if !root.exists() {
        return Ok(1);
    }
    let mut max_value = 0usize;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(value) = name
            .split('-')
            .nth(1)
            .and_then(|part| part.parse::<usize>().ok())
        {
            max_value = max_value.max(value);
        }
    }
    Ok(max_value + 1)
}

fn sanitize_segment(value: &str) -> String {
    value
        .chars()
        .map(|char| match char {
            'a'..='z' | 'A'..='Z' | '0'..='9' => char,
            _ => '_',
        })
        .collect::<String>()
        .trim_matches('_')
        .to_lowercase()
}
