use super::*;
use std::fs;
use std::io;
use std::path::Path;

pub(super) fn write_parse_artifacts(
    output_dir: &Path,
    snippet_path: &Path,
    parse_result: &crate::ParseResult,
    selected_python: &str,
) -> io::Result<()> {
    let imports = if parse_result.imports.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .imports
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let import_paths = if parse_result.import_paths.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .import_paths
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let configs = if parse_result.config_deps.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .config_deps
            .iter()
            .map(|dep| {
                format!(
                    "- {}{} ({})",
                    dep.package,
                    dep.constraint
                        .as_ref()
                        .map(|value| format!(" {value}"))
                        .unwrap_or_default(),
                    dep.source_file
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let scanned = if parse_result.scanned_files.is_empty() {
        "- none".to_string()
    } else {
        parse_result
            .scanned_files
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    write_state_artifacts(
        output_dir,
        "parse-summary.txt",
        &format!(
            "snippet: {}\nselected_python: {}\npython_version_min: {}\npython_version_max: {}\nconfidence: {:.2}\n\nimports:\n{}\n\nimport_paths:\n{}\n\nconfig_dependencies:\n{}\n\nscanned_files:\n{}\n",
            snippet_path.display(),
            selected_python,
            parse_result.python_version_min,
            parse_result.python_version_max.as_deref().unwrap_or("--"),
            parse_result.confidence,
            imports,
            import_paths,
            configs,
            scanned,
        ),
    )
}

pub(super) fn write_solver_artifacts(output_dir: &Path, result: &pre_solve::PreSolveResult) -> io::Result<()> {
    let assignments = if result.assigned_versions.is_empty() {
        "- none".to_string()
    } else {
        result
            .assigned_versions
            .iter()
            .map(|(package, version)| format!("- {package}=={version}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    write_state_artifacts(
        output_dir,
        "solver-summary.txt",
        &format!(
            "attempted: {}\nsatisfiable: {}\nselected_python: {}\ndirect_packages: {}\ntransitive_packages: {}\nreason: {}\n\nassignments:\n{}\n",
            result.attempted,
            result.satisfiable,
            result.selected_python_version,
            if result.direct_packages.is_empty() {
                "--".to_string()
            } else {
                result.direct_packages.join(", ")
            },
            if result.transitive_packages.is_empty() {
                "--".to_string()
            } else {
                result.transitive_packages.join(", ")
            },
            result.reason.as_deref().unwrap_or("--"),
            assignments,
        ),
    )?;
    write_state_artifacts(
        output_dir,
        "solver-lockfile.txt",
        &result.lockfile_requirements,
    )
}

pub(super) fn write_state_artifacts(output_dir: &Path, name: &str, contents: &str) -> io::Result<()> {
    context::write_text(&context::debug_root(output_dir).join(name), contents)
}

pub(super) fn write_iteration_snapshot(
    output_dir: &Path,
    iteration_index: usize,
    name: &str,
    contents: &str,
) -> io::Result<()> {
    let directory = context::iteration_dir(output_dir, iteration_index);
    fs::create_dir_all(&directory)?;
    context::write_text(&directory.join(name), contents)
}

pub(super) fn format_dependency_state(resolved: &[ResolvedDependency], unresolved: &[String]) -> String {
    let resolved_rows = if resolved.is_empty() {
        "- none".to_string()
    } else {
        resolved
            .iter()
            .map(|dependency| {
                format!(
                    "- import={} package={} version={} strategy={} confidence={:.2}",
                    dependency.import_name,
                    dependency.package_name,
                    dependency.version.as_deref().unwrap_or("--"),
                    dependency.strategy,
                    dependency.confidence,
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let unresolved_rows = if unresolved.is_empty() {
        "- none".to_string()
    } else {
        unresolved
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    format!("resolved:\n{resolved_rows}\n\nunresolved:\n{unresolved_rows}\n")
}

pub(super) fn format_classifier(classified: &crate::ClassifierResult) -> String {
    format!(
        "error_type: {}\nconflict_class: {}\nmatched_pattern: {}\nrecommended_fix: {}\n",
        classified.error_type,
        classified.conflict_class,
        classified.matched_pattern,
        classified.recommended_fix,
    )
}

