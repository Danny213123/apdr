use std::collections::{BTreeMap, BTreeSet};

pub struct ImportScan {
    pub top_levels: Vec<String>,
    pub full_paths: Vec<String>,
    /// Maps imported module name → set of attributes accessed on it in code.
    /// E.g. `import cv2; cv2.imread(...)` → {"cv2": {"imread"}}
    pub attribute_usage: BTreeMap<String, BTreeSet<String>>,
}

pub fn scan_imports(source: &str) -> ImportScan {
    let mut top_levels = BTreeSet::new();
    let mut full_paths = BTreeSet::new();
    let mut attribute_usage: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    // Collect non-import lines for attribute scanning in a single pass.
    let mut code_lines: Vec<&str> = Vec::new();

    // Strip multi-line string literals (triple-quoted) so we don't extract
    // imports from docstrings or string constants.
    let cleaned = strip_triple_quoted_strings(source);

    for raw_line in cleaned.lines() {
        let trimmed = raw_line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        let trimmed = strip_comment(trimmed);
        let mut is_import_line = false;

        // Split on semicolons to handle multiple statements per line
        // e.g. "import sys; from PIL import Image; import numpy"
        for statement in trimmed.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }

            if let Some(rest) = stmt.strip_prefix("import ") {
                is_import_line = true;
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
                is_import_line = true;
                if let Some((module, import_part)) = rest.split_once(" import ") {
                    let module_path = normalize_import(module);
                    if module_path.is_empty() {
                        continue;
                    }

                    // flask.ext.X → flask_X (Flask extension convention)
                    // e.g. `from flask.ext.sqlalchemy import SQLAlchemy` → flask_sqlalchemy
                    if let Some(ext) = module_path.strip_prefix("flask.ext.") {
                        if !ext.is_empty() {
                            let flask_pkg = format!("flask_{ext}");
                            top_levels.insert(flask_pkg.clone());
                            full_paths.insert(module_path.clone());
                            full_paths.insert(flask_pkg);
                            // Also keep flask as a dependency
                            top_levels.insert("flask".to_string());
                            for name in import_part.split(',') {
                                let imported_name = normalize_member(name);
                                if !imported_name.is_empty() && imported_name != "*" {
                                    full_paths.insert(format!("{module_path}.{imported_name}"));
                                }
                            }
                            continue;
                        }
                    }

                    top_levels.insert(top_level(&module_path).to_string());
                    full_paths.insert(module_path.clone());

                    for name in import_part.split(',') {
                        let imported_name = normalize_member(name);
                        if imported_name.is_empty() {
                            continue;
                        }
                        if imported_name == "*" {
                            // `from X import *` — we can't know what's imported
                            // but the module itself is definitely needed.
                            // Mark it explicitly so resolution covers it.
                            full_paths.insert(format!("{module_path}.*"));
                            continue;
                        }
                        full_paths.insert(format!("{module_path}.{imported_name}"));
                    }
                }
            }
        }

        if !is_import_line {
            code_lines.push(trimmed);
        }
    }

    // Scan collected code lines for MODULE.ATTRIBUTE usage patterns.
    // This helps the LLM disambiguate imports (e.g. crypto.AES → pycryptodome).
    let top_levels_set: BTreeSet<&str> = top_levels.iter().map(String::as_str).collect();
    for trimmed in &code_lines {
        let bytes = trimmed.as_bytes();
        let mut pos = 0;
        while pos < bytes.len() {
            if !bytes[pos].is_ascii_alphanumeric() && bytes[pos] != b'_' {
                pos += 1;
                continue;
            }
            let start = pos;
            while pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                pos += 1;
            }
            let word = &trimmed[start..pos];
            if pos < bytes.len() && bytes[pos] == b'.' && top_levels_set.contains(word) {
                pos += 1;
                let attr_start = pos;
                while pos < bytes.len()
                    && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_')
                {
                    pos += 1;
                }
                if pos > attr_start {
                    let attr = &trimmed[attr_start..pos];
                    if attr.chars().next().map_or(false, |c| c.is_alphabetic()) {
                        attribute_usage
                            .entry(word.to_string())
                            .or_default()
                            .insert(attr.to_string());
                    }
                }
            }
        }
    }

    ImportScan {
        top_levels: top_levels.into_iter().collect(),
        full_paths: full_paths.into_iter().collect(),
        attribute_usage,
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

/// Remove content inside triple-quoted strings (""" or ''') so the import
/// scanner doesn't pick up import statements from docstrings or string literals.
fn strip_triple_quoted_strings(source: &str) -> String {
    let mut result = String::with_capacity(source.len());
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        // Check for triple quote start
        if i + 2 < len
            && ((bytes[i] == b'"' && bytes[i + 1] == b'"' && bytes[i + 2] == b'"')
                || (bytes[i] == b'\'' && bytes[i + 1] == b'\'' && bytes[i + 2] == b'\''))
        {
            let quote = bytes[i];
            i += 3;
            // Skip until matching closing triple quote
            while i + 2 < len {
                if bytes[i] == quote && bytes[i + 1] == quote && bytes[i + 2] == quote {
                    i += 3;
                    break;
                }
                // Preserve newlines so line numbers stay correct
                if bytes[i] == b'\n' {
                    result.push('\n');
                }
                i += 1;
            }
            continue;
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
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
        assert!(scan
            .full_paths
            .contains(&"google.cloud.storage".to_string()));
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

    #[test]
    fn scan_skips_imports_inside_docstrings() {
        let source = r#""""
Example usage:
    from flopsy import Connection, Consumer
    consumer = Consumer(connection=Connection())
"""

from django.core.management.base import BaseCommand
import daemon
"#;
        let scan = scan_imports(source);
        assert!(
            !scan.top_levels.contains(&"flopsy".to_string()),
            "flopsy should not be extracted from a docstring"
        );
        assert!(scan.top_levels.contains(&"django".to_string()));
        assert!(scan.top_levels.contains(&"daemon".to_string()));
    }

    #[test]
    fn scan_skips_imports_inside_single_quoted_docstrings() {
        let source = "'''\nfrom fake_module import Foo\n'''\nimport real_module\n";
        let scan = scan_imports(source);
        assert!(!scan.top_levels.contains(&"fake_module".to_string()));
        assert!(scan.top_levels.contains(&"real_module".to_string()));
    }
}
