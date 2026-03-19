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
Treat these as NOT solvable in generic Docker:\n\
- Host-application runtimes: Maya, Blender, ArcGIS, Houdini, Rhino, Unreal, Nuke, Sublime Text, GIMP, IDA Pro, Cinema4D, HexChat\n\
- Platform-specific APIs: COM/Win32 Windows APIs, macOS Objective-C frameworks (Foundation, CoreFoundation, AppKit, SystemConfiguration, OpenDirectory), Raspberry Pi GPIO/camera\n\
- Java/Jython interop: javax.*, java.*, com.android.*\n\
- Local project modules not available on PyPI\n\
Return exactly four lines:\n\
decision=solve OR decision=skip\n\
confidence=0.00 to 1.00\n\
reason=short explanation\n\
unsolvable_modules=comma,separated,import,names OR unsolvable_modules=none\n\
The unsolvable_modules line must list the specific import names from the snippet that cannot be resolved from PyPI. Use the exact import names as they appear. If decision=solve, set unsolvable_modules=none.\n\
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
IMPORTANT: Only return actual PyPI package names that can be installed with pip.\n\
Do NOT return local project module names, helper scripts, or internal imports.\n\
If an import appears to be a local/project-specific module (not a known PyPI package), \
skip it entirely — do not include it in the output.\n\
Return one mapping per line in the exact format import=package.\n\
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

pub fn recovery_resolution_prompt(
    resolved_packages: &[String],
    error_log: &str,
    snippet_source: &str,
    python_version: &str,
    error_type: &str,
) -> String {
    format!(
        "You are fixing a Python dependency installation failure.\n\
Target Python version: {python_version}\n\
Error type: {error_type}\n\n\
Currently resolved packages:\n{}\n\n\
Installation/import error:\n```\n{}\n```\n\n\
Python snippet being resolved:\n```python\n{}\n```\n\n\
One of the resolved packages above is incorrect. Common problems:\n\
- Package does not exist on PyPI (wrong name, needs prefix like `django-` or `python-`)\n\
- Package needs system C libraries and a pure-Python alternative exists (e.g., mysqlclient -> PyMySQL)\n\
- Wrong package was installed (same name on PyPI but unrelated project)\n\
Return exactly one line in the format: wrong_package=correct_package\n\
Use the exact current package name from the resolved list as wrong_package.\n\
Use the correct PyPI package name as correct_package.\n\
If no fix is possible, return: fix=NONE",
        resolved_packages.join("\n"),
        error_log,
        snippet_source
            .lines()
            .take(50)
            .collect::<Vec<_>>()
            .join("\n"),
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
