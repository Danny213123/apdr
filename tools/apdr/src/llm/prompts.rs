use crate::ParseResult;

pub fn solvability_assessment_prompt(
    source: &str,
    parse_result: &ParseResult,
    benchmark_context: &str,
) -> String {
    let imports = if parse_result.imports.is_empty() {
        "- none".to_string()
    } else {
        parse_result.imports.join(", ")
    };
    let import_paths = if parse_result.import_paths.is_empty() {
        "- none".to_string()
    } else {
        parse_result.import_paths.join(", ")
    };
    format!(
        "You are triaging whether a Python snippet is solvable in a generic Docker + PyPI environment.\n\
Decide whether APDR should try dependency resolution or skip the snippet.\n\
Treat host-application runtimes as NOT solvable in generic Docker, for example Maya, Blender, ArcGIS, Houdini, Rhino, Unreal, Nuke, Sublime, Autodesk, COM-only Windows APIs, or snippets that require a local project module not present here.\n\
Return exactly three lines:\n\
decision=solve OR decision=skip\n\
confidence=0.00 to 1.00\n\
reason=short explanation\n\
Imports: {imports}\n\
Import paths: {import_paths}\n\
Benchmark trace context:\n{}\n\
Snippet:\n```python\n{}\n```",
        if benchmark_context.trim().is_empty() {
            "- none".to_string()
        } else {
            benchmark_context.to_string()
        },
        source
    )
}

pub fn package_resolution_prompt(
    unresolved_imports: &[String],
    python_version: &str,
    context: &[String],
    benchmark_context: &str,
) -> String {
    format!(
        "You are resolving Python imports to PyPI package names.\n\
Target Python version: {python_version}\n\
Context:\n{}\n\
Benchmark trace context:\n{}\n\
Return one mapping per line in the exact format import=package.\n\
If unknown, repeat the import name as the package name.\n\
Imports:\n{}",
        if context.is_empty() {
            "- none".to_string()
        } else {
            context.join("\n")
        },
        if benchmark_context.trim().is_empty() {
            "- none".to_string()
        } else {
            benchmark_context.to_string()
        },
        unresolved_imports.join("\n")
    )
}

pub fn version_inference_prompt(
    package_name: &str,
    versions: &[String],
    python_version: &str,
    benchmark_context: &str,
) -> String {
    format!(
        "Choose one installable version for the Python package '{package_name}'.\n\
Target Python version: {python_version}\n\
Allowed versions (oldest to newest): {}\n\
Benchmark trace context:\n{}\n\
Return only one line in the exact format version=x.y.z. If none look viable, return version=NONE.",
        versions.join(", "),
        if benchmark_context.trim().is_empty() {
            "- none".to_string()
        } else {
            benchmark_context.to_string()
        }
    )
}
