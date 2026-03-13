pub mod ast;
pub mod config_files;
pub mod imports;
pub mod version_detect;

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;

use crate::ParseResult;

pub fn parse_snippet(
    snippet_path: &Path,
    data_root: &Path,
    scan_config_files: bool,
) -> io::Result<ParseResult> {
    let source = fs::read_to_string(snippet_path)?;
    let python_version_min = version_detect::detect_minimum_python(&source);
    let python_version_max = version_detect::detect_maximum_python(&source);
    let stdlib_version = python_version_max
        .as_ref()
        .filter(|value| value.starts_with("2."))
        .cloned()
        .unwrap_or_else(|| python_version_min.clone());
    let stdlib_modules = ast::load_stdlib_modules(data_root, &stdlib_version)?;
    let import_scan = imports::scan_imports(&source);
    let imports = import_scan
        .top_levels
        .into_iter()
        .filter(|value| !stdlib_modules.contains(&value.to_lowercase()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let import_paths = import_scan
        .full_paths
        .into_iter()
        .filter(|value| {
            let top_level = value.split('.').next().unwrap_or(value);
            !stdlib_modules.contains(&top_level.to_lowercase())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut scanned_files = vec![snippet_path.display().to_string()];
    let config_deps = if scan_config_files {
        let scan = config_files::scan(snippet_path)?;
        scanned_files.extend(scan.scanned_files);
        scan.dependencies
    } else {
        Vec::new()
    };

    Ok(ParseResult {
        imports,
        import_paths,
        config_deps,
        python_version_min,
        python_version_max,
        confidence: 0.72,
        scanned_files,
    })
}
