use std::collections::BTreeSet;

pub struct ImportScan {
    pub top_levels: Vec<String>,
    pub full_paths: Vec<String>,
}

pub fn scan_imports(source: &str) -> ImportScan {
    let mut top_levels = BTreeSet::new();
    let mut full_paths = BTreeSet::new();

    for raw_line in source.lines() {
        let trimmed = raw_line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        let trimmed = strip_comment(trimmed);

        // Split on semicolons to handle multiple statements per line
        // e.g. "import sys; from PIL import Image; import numpy"
        for statement in trimmed.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }

            if let Some(rest) = stmt.strip_prefix("import ") {
                for part in rest.split(',') {
                    let import_path = normalize_import(part);
                    if import_path.is_empty() {
                        continue;
                    }
                    top_levels.insert(top_level(&import_path).to_string());
                    full_paths.insert(import_path);
                }
                continue;
            }

            if let Some(rest) = stmt.strip_prefix("from ") {
                if let Some((module, import_part)) = rest.split_once(" import ") {
                    let module_path = normalize_import(module);
                    if module_path.is_empty() {
                        continue;
                    }
                    top_levels.insert(top_level(&module_path).to_string());
                    full_paths.insert(module_path.clone());

                    for name in import_part.split(',') {
                        let imported_name = normalize_member(name);
                        if imported_name.is_empty() || imported_name == "*" {
                            continue;
                        }
                        full_paths.insert(format!("{module_path}.{imported_name}"));
                    }
                }
            }
        }
    }

    ImportScan {
        top_levels: top_levels.into_iter().collect(),
        full_paths: full_paths.into_iter().collect(),
    }
}

fn normalize_import(value: &str) -> String {
    let without_alias = value.split(" as ").next().unwrap_or("").trim();
    if without_alias.starts_with('.') {
        return String::new();
    }
    without_alias
        .trim_matches('(')
        .trim_matches(')')
        .trim()
        .to_string()
}

fn normalize_member(value: &str) -> String {
    value
        .split(" as ")
        .next()
        .unwrap_or("")
        .trim()
        .trim_matches('(')
        .trim_matches(')')
        .trim()
        .to_string()
}

fn top_level(path: &str) -> &str {
    path.split('.').next().unwrap_or(path)
}

fn strip_comment(line: &str) -> &str {
    if !line.contains('#') {
        return line;
    }
    let bytes = line.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    for (index, byte) in bytes.iter().enumerate() {
        match byte {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b'#' if !in_single && !in_double => return &line[..index],
            _ => {}
        }
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_tracks_top_levels_and_full_paths() {
        let scan = scan_imports(
            "import requests\nfrom bs4 import BeautifulSoup\nfrom google.cloud.storage import Client as StorageClient\n",
        );

        assert!(scan.top_levels.contains(&"requests".to_string()));
        assert!(scan.top_levels.contains(&"bs4".to_string()));
        assert!(scan.top_levels.contains(&"google".to_string()));
        assert!(scan.full_paths.contains(&"bs4.BeautifulSoup".to_string()));
        assert!(scan.full_paths.contains(&"google.cloud.storage".to_string()));
        assert!(scan
            .full_paths
            .contains(&"google.cloud.storage.Client".to_string()));
    }

    #[test]
    fn scan_preserves_case_for_nonstandard_modules() {
        let scan = scan_imports("import RPi.GPIO as GPIO\n");

        assert!(scan.top_levels.contains(&"RPi".to_string()));
        assert!(scan.full_paths.contains(&"RPi.GPIO".to_string()));
    }

    #[test]
    fn scan_handles_semicolon_separated_statements() {
        let scan = scan_imports("import sys; from PIL import Image; import numpy\n");
        assert!(scan.top_levels.contains(&"sys".to_string()));
        assert!(scan.top_levels.contains(&"PIL".to_string()));
        assert!(scan.top_levels.contains(&"numpy".to_string()));
        assert!(scan.full_paths.contains(&"PIL.Image".to_string()));
    }

    #[test]
    fn scan_skips_relative_imports() {
        let scan = scan_imports("from . import local\nfrom ..pkg import util\n");
        assert!(scan.top_levels.is_empty());
        assert!(scan.full_paths.is_empty());
    }
}
