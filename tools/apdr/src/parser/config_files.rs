use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::ConfigDep;

pub struct ConfigScan {
    pub dependencies: Vec<ConfigDep>,
    pub scanned_files: Vec<String>,
}

pub fn scan(snippet_path: &Path) -> io::Result<ConfigScan> {
    let root = snippet_path.parent().unwrap_or_else(|| Path::new("."));
    let candidates = collect_candidates(root, 2)?;
    let mut dependencies = Vec::new();
    let mut scanned_files = Vec::new();
    let mut seen = BTreeSet::new();

    for path in candidates {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let lowercase_name = file_name.to_lowercase();
        if !is_supported_config(&lowercase_name) {
            continue;
        }
        if is_generated_benchmark_config(&path, snippet_path, &lowercase_name) {
            continue;
        }

        let contents = fs::read_to_string(&path)?;
        scanned_files.push(path.display().to_string());

        for dependency in parse_dependencies(&lowercase_name, &contents) {
            let key = format!("{}::{:?}", dependency.package, dependency.constraint);
            if seen.insert(key) {
                dependencies.push(ConfigDep {
                    package: dependency.package,
                    constraint: dependency.constraint,
                    source_file: path.display().to_string(),
                });
            }
        }
    }

    Ok(ConfigScan {
        dependencies,
        scanned_files,
    })
}

#[derive(Clone)]
struct ParsedDependency {
    package: String,
    constraint: Option<String>,
}

fn collect_candidates(root: &Path, max_depth: usize) -> io::Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    collect_recursive(root, max_depth, &mut output)?;
    Ok(output)
}

fn collect_recursive(root: &Path, depth: usize, output: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file() {
            output.push(path);
        } else if depth > 0 {
            if should_skip_directory(&path) {
                continue;
            }
            collect_recursive(&path, depth - 1, output)?;
        }
    }
    Ok(())
}

fn should_skip_directory(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    name.starts_with('.')
        || matches!(
            name,
            "__pycache__" | "node_modules" | "venv" | ".venv" | "__MACOSX"
        )
}

fn is_supported_config(file_name: &str) -> bool {
    file_name == "setup.py"
        || file_name == "setup.cfg"
        || file_name == "pyproject.toml"
        || file_name == "pipfile"
        || file_name == "tox.ini"
        || file_name.ends_with("requirements.txt")
        || file_name.starts_with("requirements-")
        || file_name == "environment.yml"
        || file_name == "environment.yaml"
}

fn is_generated_benchmark_config(path: &Path, snippet_path: &Path, file_name: &str) -> bool {
    if !(file_name.ends_with("requirements.txt") || file_name.starts_with("requirements-")) {
        return false;
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let has_benchmark_markers = parent.join("resolution-report.txt").exists()
        || parent.join(".apdr-docker").exists()
        || parent
            .read_dir()
            .ok()
            .into_iter()
            .flat_map(|entries| entries.filter_map(Result::ok))
            .any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.starts_with("output_data_") && name.ends_with(".yml"))
                    .unwrap_or(false)
            });
    if !has_benchmark_markers {
        return false;
    }

    let requirement_mtime = path
        .metadata()
        .and_then(|meta| meta.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let snippet_mtime = snippet_path
        .metadata()
        .and_then(|meta| meta.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    requirement_mtime >= snippet_mtime
}

fn parse_dependencies(file_name: &str, contents: &str) -> Vec<ParsedDependency> {
    if file_name.ends_with("requirements.txt") || file_name.starts_with("requirements-") {
        return parse_requirements_like(contents);
    }
    if file_name == "pipfile" {
        return parse_pipfile(contents);
    }
    if file_name == "environment.yml" || file_name == "environment.yaml" {
        return parse_environment_yml(contents);
    }
    if file_name == "pyproject.toml" {
        return parse_toml_dependencies(contents);
    }
    parse_generic_strings(contents)
}

fn parse_requirements_like(contents: &str) -> Vec<ParsedDependency> {
    contents
        .lines()
        .filter_map(|line| {
            let trimmed = line.split('#').next().unwrap_or("").trim();
            if trimmed.is_empty() || trimmed.starts_with('-') {
                return None;
            }
            Some(split_requirement(trimmed))
        })
        .collect()
}

fn parse_pipfile(contents: &str) -> Vec<ParsedDependency> {
    let mut in_packages = false;
    let mut output = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_packages = trimmed == "[packages]" || trimmed == "[dev-packages]";
            continue;
        }
        if !in_packages || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((name, value)) = trimmed.split_once('=') {
            let dependency = ParsedDependency {
                package: name.trim().to_lowercase(),
                constraint: Some(
                    value
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_string(),
                ),
            };
            output.push(dependency);
        }
    }
    output
}

fn parse_environment_yml(contents: &str) -> Vec<ParsedDependency> {
    contents
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if !trimmed.starts_with("- ") {
                return None;
            }
            let candidate = trimmed.trim_start_matches("- ").trim();
            if candidate.eq_ignore_ascii_case("pip:") {
                return None;
            }
            Some(split_requirement(candidate))
        })
        .collect()
}

fn parse_toml_dependencies(contents: &str) -> Vec<ParsedDependency> {
    let mut output = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if !(trimmed.starts_with('"') || trimmed.starts_with('\'')) {
            continue;
        }
        let quote = trimmed.chars().next().unwrap_or('"');
        let without_quote = trimmed.trim_matches(',').trim_matches(quote);
        if without_quote.contains(' ')
            || without_quote.contains('=')
            || without_quote.contains('<')
            || without_quote.contains('>')
        {
            output.push(split_requirement(without_quote));
        }
    }
    output
}

fn parse_generic_strings(contents: &str) -> Vec<ParsedDependency> {
    let mut output = Vec::new();
    for token in contents.split(|ch: char| {
        ch == '"' || ch == '\'' || ch == '\n' || ch == ',' || ch == '[' || ch == ']'
    }) {
        let trimmed = token.trim();
        if looks_like_requirement(trimmed) {
            output.push(split_requirement(trimmed));
        }
    }
    output
}

fn looks_like_requirement(token: &str) -> bool {
    if token.len() < 2 {
        return false;
    }
    token.chars().all(|ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '<' | '>' | '=' | '!' | '~')
    }) && token.chars().any(|ch| ch.is_ascii_alphabetic())
}

fn split_requirement(value: &str) -> ParsedDependency {
    let separators = ["==", ">=", "<=", "!=", "~=", ">", "<"];
    for separator in separators {
        if let Some((package, constraint)) = value.split_once(separator) {
            return ParsedDependency {
                package: package.trim().to_lowercase(),
                constraint: Some(format!("{separator}{}", constraint.trim())),
            };
        }
    }
    ParsedDependency {
        package: value.trim().to_lowercase(),
        constraint: None,
    }
}
