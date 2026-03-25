use crate::cache::store::CacheStore;
use crate::context;
use crate::resolver::{pypi_client, version_sampler};
use crate::{ParseResult, ResolveConfig, ResolvedDependency, SolvabilityAssessment};

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Mutex, OnceLock};

pub struct StageResult {
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub notes: Vec<String>,
    pub prompts_issued: usize,
}

// ---------------------------------------------------------------------------
// Python subprocess IPC
// ---------------------------------------------------------------------------

struct LlmProcess {
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    _child: Child,
}

static LLM_PROCESS: OnceLock<Mutex<LlmProcess>> = OnceLock::new();

fn find_python() -> String {
    // Honour explicit override first.
    if let Ok(py) = std::env::var("APDR_PYTHON") {
        if !py.is_empty() {
            return py;
        }
    }
    // Build a candidate list: prefer interpreters that are likely to have
    // pydantic/instructor installed (conda, venv, then generic python3).
    let mut candidates: Vec<String> = Vec::new();
    // Conda/mamba Python (usually has scientific packages pre-installed)
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let sep = if cfg!(windows) { "\\" } else { "/" };
        if cfg!(windows) {
            candidates.push(format!("{prefix}{sep}python.exe"));
        } else {
            candidates.push(format!("{prefix}{sep}bin{sep}python"));
        }
    }
    candidates.extend(["python3".to_string(), "python".to_string()]);
    // On Windows, also try the `py` launcher with descending version flags
    // and common install paths, since `python` often resolves to the oldest.
    if cfg!(windows) {
        for ver in &["3.12", "3.11", "3.10", "3.9"] {
            candidates.push(format!("py -{ver}"));
        }
        // Common Windows install locations (newest first)
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            for ver in &["312", "311", "310", "39"] {
                candidates.push(format!("{local}\\Programs\\Python\\Python{ver}\\python.exe"));
            }
        }
    }
    for cmd in &candidates {
        // Check the interpreter can import pydantic (required by llm_py).
        // Some candidates (e.g. "py -3.11") have space-separated args.
        let parts: Vec<&str> = cmd.split_whitespace().collect();
        let (program, extra_args) = (parts[0], &parts[1..]);
        if Command::new(program)
            .args(extra_args)
            .args(&["-c", "import pydantic"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return cmd.to_string();
        }
    }
    // Last resort — hope that python3 is adequate.
    "python3".to_string()
}

fn llm_py_dir() -> std::path::PathBuf {
    // The llm_py package lives next to the apdr binary's source tree.
    // At runtime, the binary is in target/release/ or target/debug/,
    // but the llm_py dir is at tools/apdr/llm_py/.
    // We locate it relative to the executable or via APDR_LLM_PY_DIR env var.
    if let Ok(dir) = std::env::var("APDR_LLM_PY_DIR") {
        return std::path::PathBuf::from(dir);
    }
    // Walk up from executable to find tools/apdr/llm_py/
    if let Ok(exe) = std::env::current_exe() {
        let mut p = exe.as_path();
        for _ in 0..6 {
            if let Some(parent) = p.parent() {
                let candidate = parent.join("llm_py");
                if candidate.join("__main__.py").exists() {
                    return candidate;
                }
                let candidate2 = parent.join("tools").join("apdr").join("llm_py");
                if candidate2.join("__main__.py").exists() {
                    return candidate2;
                }
                p = parent;
            }
        }
    }
    // Fallback: assume CWD-relative
    std::path::PathBuf::from("tools/apdr/llm_py")
}

fn spawn_python_process() -> Mutex<LlmProcess> {
    let python = find_python();
    let py_dir = llm_py_dir();
    let parent = py_dir.parent().unwrap_or_else(|| Path::new("."));

    let parts: Vec<&str> = python.split_whitespace().collect();
    let (program, extra_args) = (parts[0], &parts[1..]);
    let mut child = Command::new(program)
        .args(extra_args)
        .arg("-m")
        .arg("llm_py")
        .current_dir(parent)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn Python LLM service: {e}"));

    let stdin = child.stdin.take().expect("Failed to open stdin");
    let stdout = child.stdout.take().expect("Failed to open stdout");
    let mut reader = BufReader::new(stdout);

    // Wait for ready signal
    let mut ready_line = String::new();
    if reader.read_line(&mut ready_line).is_ok() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&ready_line) {
            if json.get("ready").and_then(|v| v.as_bool()) != Some(true) {
                eprintln!("Warning: Python LLM service did not send ready signal");
            }
        }
    }

    Mutex::new(LlmProcess {
        stdin,
        stdout: reader,
        _child: child,
    })
}

fn call_python(request: &serde_json::Value) -> Option<serde_json::Value> {
    let process = LLM_PROCESS.get_or_init(spawn_python_process);
    let mut guard = process.lock().ok()?;

    let request_str = serde_json::to_string(request).ok()?;
    writeln!(guard.stdin, "{}", request_str).ok()?;
    guard.stdin.flush().ok()?;

    let mut line = String::new();
    guard.stdout.read_line(&mut line).ok()?;
    if line.trim().is_empty() {
        return None;
    }
    serde_json::from_str(line.trim()).ok()
}

fn build_base_request(config: &ResolveConfig) -> serde_json::Value {
    serde_json::json!({
        "provider": config.llm_provider,
        "model": config.llm_model,
        "base_url": config.llm_base_url,
        "cache_path": config.cache_path.to_string_lossy(),
        "output_dir": config.output_dir.to_string_lossy(),
    })
}

// ---------------------------------------------------------------------------
// Inlined RAG context assembly (from llm/rag.rs)
// ---------------------------------------------------------------------------

fn assemble_context_for_import(store: &CacheStore, name: &str) -> Vec<String> {
    let mut context = Vec::new();
    if let Some(record) = store.import_lookup(name) {
        context.push(format!(
            "known import mapping: {} -> {}",
            record.import_name, record.package_name
        ));
    }
    if let Some(versions) = store.pypi_index.get(name) {
        context.push(format!("known versions: {}", versions.join(", ")));
    }
    if let Some(deps) = store.dependency_graph.get(name) {
        context.push(format!("known transitive deps: {}", deps.join(", ")));
    }
    context
}

fn assemble_batch_context(
    store: &CacheStore,
    import_names: &[String],
    failure_context: &str,
) -> Vec<String> {
    let mut context: Vec<String> = import_names
        .iter()
        .flat_map(|name| assemble_context_for_import(store, name))
        .collect();
    if !failure_context.is_empty() {
        context.push(failure_context.to_string());
    }
    context
}

// ---------------------------------------------------------------------------
// Inlined failure memory (from llm/failure_memory.rs)
// ---------------------------------------------------------------------------

struct FailureEntry {
    package_tried: String,
    #[allow(dead_code)]
    error_reason: String,
}

fn load_failure_memory(cache_path: &Path) -> HashMap<String, Vec<FailureEntry>> {
    let path = cache_path.join("llm_failure_memory.tsv");
    let mut failures: HashMap<String, Vec<FailureEntry>> = HashMap::new();
    if let Ok(content) = std::fs::read_to_string(&path) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.splitn(5, '\t').collect();
            if parts.len() >= 4 {
                let import_name = parts[0].to_string();
                let entry = FailureEntry {
                    package_tried: parts[1].to_string(),
                    error_reason: parts[2].to_string(),
                };
                failures.entry(import_name).or_default().push(entry);
            }
        }
    }
    failures
}

fn format_failure_context(
    failures: &HashMap<String, Vec<FailureEntry>>,
    import_names: &[String],
) -> String {
    let mut lines = Vec::new();
    for name in import_names {
        if let Some(records) = failures.get(name) {
            for r in records {
                lines.push(format!(
                    "PREVIOUS FAILURE: import `{}` mapped to `{}` failed ({}). DO NOT suggest `{}` again.",
                    name, r.package_tried, r.error_reason, r.package_tried
                ));
            }
        }
    }
    lines.join("\n")
}

fn has_failed(
    failures: &HashMap<String, Vec<FailureEntry>>,
    import_name: &str,
    package_name: &str,
) -> bool {
    failures
        .get(import_name)
        .map(|records| {
            records
                .iter()
                .any(|r| r.package_tried.eq_ignore_ascii_case(package_name))
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Trace persistence (simplified — logs JSON request/response)
// ---------------------------------------------------------------------------

fn persist_trace(
    config: &ResolveConfig,
    label: &str,
    request: &serde_json::Value,
    response: Option<&serde_json::Value>,
) {
    let _ = (|| -> std::io::Result<()> {
        let trace_dir = context::create_llm_trace_dir(&config.output_dir, label)?;
        context::write_text(
            &trace_dir.join("request.json"),
            &serde_json::to_string_pretty(request).unwrap_or_default(),
        )?;
        if let Some(resp) = response {
            context::write_text(
                &trace_dir.join("response.json"),
                &serde_json::to_string_pretty(resp).unwrap_or_default(),
            )?;
        }
        Ok(())
    })();
}

// ---------------------------------------------------------------------------
// Solvability Assessment
// ---------------------------------------------------------------------------

pub fn assess_solvability(
    snippet_source: &str,
    parse_result: &ParseResult,
    config: &ResolveConfig,
) -> Option<SolvabilityAssessment> {
    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    let mut request = build_base_request(config);
    request["action"] = "solvability".into();
    request["imports"] = serde_json::json!(parse_result.imports);
    request["snippet_source"] = snippet_source.into();
    request["benchmark_context"] = benchmark_context.into();

    persist_trace(config, "solvability-assessment", &request, None);

    let response = call_python(&request)?;
    persist_trace(config, "solvability-assessment", &request, Some(&response));

    if let Some(error) = response.get("error").and_then(|e| e.as_str()) {
        if !error.is_empty() {
            return None;
        }
    }

    let decision = response
        .get("decision")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_lowercase();
    if decision != "solve" && decision != "skip" {
        return None;
    }

    let confidence = response
        .get("confidence")
        .and_then(|v| v.as_f64())
        .map(|v| v.clamp(0.0, 1.0))
        .unwrap_or(if decision == "skip" { 0.2 } else { 0.6 });

    let reason = response
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("LLM solvability assessment.")
        .to_string();

    let unsolvable_modules = response
        .get("unsolvable_modules")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty() && s != "none")
                .collect()
        })
        .unwrap_or_default();

    Some(SolvabilityAssessment {
        decision,
        confidence,
        reason,
        source: "llm-preflight".to_string(),
        unsolvable_modules,
    })
}

// ---------------------------------------------------------------------------
// Main Package Resolution
// ---------------------------------------------------------------------------

pub fn resolve(
    unresolved_imports: &[String],
    parse_result: &ParseResult,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
) -> StageResult {
    let mut llm_candidates = Vec::new();
    let mut preserved_unresolved = Vec::new();
    for import_name in unresolved_imports {
        if looks_like_local_helper_import(parse_result, import_name) {
            preserved_unresolved.push(import_name.clone());
        } else {
            llm_candidates.push(import_name.clone());
        }
    }
    if llm_candidates.is_empty() {
        return StageResult {
            resolved: Vec::new(),
            unresolved: preserved_unresolved,
            notes: vec!["Skipped LLM resolution for likely local helper imports.".to_string()],
            prompts_issued: 0,
        };
    }

    // Load failure memory and assemble context (Rust-side)
    let failures = load_failure_memory(&config.cache_path);
    let failure_ctx = format_failure_context(&failures, &llm_candidates);
    let mut context = assemble_batch_context(store, &llm_candidates, &failure_ctx);
    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    // Build attribute_usage as JSON
    let attr_usage: serde_json::Value = serde_json::json!(
        parse_result.attribute_usage.iter().map(|(k, v)| {
            (k.clone(), v.iter().cloned().collect::<Vec<_>>())
        }).collect::<std::collections::BTreeMap<_, _>>()
    );

    // Build tier2 candidates for each import (#3: KGraph-grounded)
    let tier2_candidates = build_tier2_candidates(store, &llm_candidates, python_version);

    // Inject "Known package: X" for each tier2 candidate so Python PyPI checker
    // can preload them (avoids N individual HEAD requests to pypi.org)
    if let Some(cands_map) = tier2_candidates.as_object() {
        for (_, cands) in cands_map {
            if let Some(arr) = cands.as_array() {
                for c in arr {
                    if let Some(pkg_name) = c.as_str() {
                        context.push(format!("Known package: {}", pkg_name));
                    }
                }
            }
        }
    }

    let mut request = build_base_request(config);
    request["action"] = "resolve".into();
    request["imports"] = serde_json::json!(llm_candidates);
    request["python_version"] = python_version.into();
    request["context"] = serde_json::json!(context);
    request["benchmark_context"] = serde_json::Value::String(benchmark_context.clone());
    request["attribute_usage"] = attr_usage;
    request["tier2_candidates"] = tier2_candidates;

    persist_trace(config, "package-resolution", &request, None);

    let response = match call_python(&request) {
        Some(r) => r,
        None => {
            return StageResult {
                resolved: Vec::new(),
                unresolved: unresolved_imports.to_vec(),
                notes: vec!["LLM package-resolution call returned no output.".to_string()],
                prompts_issued: 1,
            };
        }
    };

    persist_trace(config, "package-resolution", &request, Some(&response));

    let prompts_issued = response
        .get("prompts_issued")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;

    let mut notes: Vec<String> = response
        .get("notes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    // Parse mappings from Python response
    let mappings: Vec<(String, String)> = response
        .get("mappings")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let imp = m.get("import_name")?.as_str()?;
                    let pkg = m.get("package_name")?.as_str()?;
                    Some((imp.to_string(), pkg.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();

    // #8: Read confidence from Python semantic entropy / voting
    let llm_confidence = response
        .get("confidence")
        .and_then(|v| v.as_f64())
        .map(|v| v.clamp(0.0, 1.0))
        .unwrap_or(0.73);

    // Post-process: version selection + store integration (stays in Rust)
    let mut resolved = Vec::new();
    let mut still_unresolved = preserved_unresolved;

    // #12: Collect all packages needing version selection, then batch them
    struct PendingMapping {
        import_name: String,
        package_name: String,
        versions: Vec<String>,
    }
    let mut pending: Vec<PendingMapping> = Vec::new();

    for (import_name, mapped) in &mappings {
        let versions = pypi_client::compatible_versions(store, mapped, python_version);
        if versions.is_empty() && mapped == import_name {
            notes.push(format!(
                "Skipped LLM mapping {import_name} -> {mapped}: package not found on PyPI (likely a local module)."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }
        if has_failed(&failures, import_name, mapped) {
            notes.push(format!(
                "Skipped LLM mapping {import_name} -> {mapped}: failed in previous runs (Reflexion)."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }
        pending.push(PendingMapping {
            import_name: import_name.clone(),
            package_name: mapped.clone(),
            versions,
        });
    }

    // Batch version selection: one LLM call for all packages
    let packages_needing_versions: Vec<(String, Vec<String>)> = pending
        .iter()
        .filter(|p| !p.versions.is_empty())
        .map(|p| (p.package_name.clone(), p.versions.clone()))
        .collect();

    let batch_versions = if !packages_needing_versions.is_empty() {
        batch_pick_versions(config, &packages_needing_versions, python_version, &benchmark_context)
    } else {
        HashMap::new()
    };

    for entry in &pending {
        let version = if entry.versions.is_empty() {
            None
        } else {
            batch_versions
                .get(&entry.package_name)
                .cloned()
                .or_else(|| version_sampler::equally_distanced_sample(&entry.versions, &[]))
        };

        let _ = store.save_import_mapping(&entry.import_name, &entry.package_name, version.as_deref(), "llm");
        resolved.push(ResolvedDependency {
            import_name: entry.import_name.clone(),
            package_name: entry.package_name.clone(),
            version,
            strategy: "llm".to_string(),
            confidence: llm_confidence,
        });
        notes.push(format!("LLM resolved {} -> {}.", entry.import_name, entry.package_name));
    }

    StageResult {
        resolved,
        unresolved: still_unresolved,
        notes,
        prompts_issued: prompts_issued + mappings.len(),
    }
}

// ---------------------------------------------------------------------------
// Resolve with additional context (retry path)
// ---------------------------------------------------------------------------

pub fn resolve_with_context(
    unresolved_imports: &[String],
    snippet_source: &str,
    parse_result: &ParseResult,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
    additional_context: Option<String>,
) -> StageResult {
    let mut llm_candidates = Vec::new();
    let mut preserved_unresolved = Vec::new();
    for import_name in unresolved_imports {
        if looks_like_local_helper_import(parse_result, import_name) {
            preserved_unresolved.push(import_name.clone());
        } else {
            llm_candidates.push(import_name.clone());
        }
    }
    if llm_candidates.is_empty() {
        return StageResult {
            resolved: Vec::new(),
            unresolved: preserved_unresolved,
            notes: vec!["Skipped LLM resolution for likely local helper imports.".to_string()],
            prompts_issued: 0,
        };
    }

    let failures = load_failure_memory(&config.cache_path);
    let failure_ctx = format_failure_context(&failures, &llm_candidates);
    let mut context = assemble_batch_context(store, &llm_candidates, &failure_ctx);

    if let Some(extra) = additional_context {
        context.insert(0, format!("IMPORTANT: {}", extra));
    }
    context.push(format!(
        "Code snippet showing import usage:\n```python\n{}\n```",
        snippet_source
            .lines()
            .take(50)
            .collect::<Vec<_>>()
            .join("\n")
    ));

    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    let attr_usage: serde_json::Value = serde_json::json!(
        parse_result.attribute_usage.iter().map(|(k, v)| {
            (k.clone(), v.iter().cloned().collect::<Vec<_>>())
        }).collect::<std::collections::BTreeMap<_, _>>()
    );

    let tier2_candidates = build_tier2_candidates(store, &llm_candidates, python_version);

    let mut request = build_base_request(config);
    request["action"] = "resolve".into();
    request["imports"] = serde_json::json!(llm_candidates);
    request["python_version"] = python_version.into();
    request["context"] = serde_json::json!(context);
    request["benchmark_context"] = serde_json::Value::String(benchmark_context.clone());
    request["attribute_usage"] = attr_usage;
    request["snippet_source"] = snippet_source.into();
    request["tier2_candidates"] = tier2_candidates;

    persist_trace(config, "package-resolution-with-context", &request, None);

    let response = match call_python(&request) {
        Some(r) => r,
        None => {
            return StageResult {
                resolved: Vec::new(),
                unresolved: unresolved_imports.to_vec(),
                notes: vec!["LLM package-resolution call returned no output.".to_string()],
                prompts_issued: 1,
            };
        }
    };

    persist_trace(
        config,
        "package-resolution-with-context",
        &request,
        Some(&response),
    );

    let mappings: Vec<(String, String)> = response
        .get("mappings")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let imp = m.get("import_name")?.as_str()?;
                    let pkg = m.get("package_name")?.as_str()?;
                    Some((imp.to_string(), pkg.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();

    let mut resolved = Vec::new();
    let mut still_unresolved = Vec::new();
    let mut notes: Vec<String> = response
        .get("notes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    for import_name in &llm_candidates {
        let mapped = mappings
            .iter()
            .find(|(imp, _)| imp == import_name)
            .map(|(_, pkg)| pkg.clone())
            .unwrap_or_else(|| import_name.clone());

        let versions = pypi_client::compatible_versions(store, &mapped, python_version);
        if versions.is_empty() && mapped == *import_name {
            notes.push(format!(
                "Skipped LLM retry mapping {import_name} -> {mapped}: not found on PyPI."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }
        if has_failed(&failures, import_name, &mapped) {
            notes.push(format!(
                "Skipped LLM retry {import_name} -> {mapped}: failed in previous runs."
            ));
            still_unresolved.push(import_name.clone());
            continue;
        }

        let version = if !versions.is_empty() {
            let llm_v = pick_version_via_python(
                config,
                &mapped,
                &versions,
                python_version,
                &benchmark_context,
            );
            llm_v.or_else(|| version_sampler::equally_distanced_sample(&versions, &[]))
        } else {
            None
        };

        let _ = store.save_import_mapping(import_name, &mapped, version.as_deref(), "llm-retry");
        resolved.push(ResolvedDependency {
            import_name: import_name.clone(),
            package_name: mapped.clone(),
            version,
            strategy: "llm-retry".to_string(),
            confidence: 0.73,
        });
        notes.push(format!("LLM retry resolved {import_name} -> {mapped}."));
    }

    still_unresolved.extend(preserved_unresolved);

    StageResult {
        resolved,
        unresolved: still_unresolved,
        notes,
        prompts_issued: 1 + llm_candidates.len(),
    }
}

// ---------------------------------------------------------------------------
// Single Package Hint
// ---------------------------------------------------------------------------

pub fn single_package_hint(
    import_name: &str,
    parse_result: &ParseResult,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
) -> Option<(String, Option<String>)> {
    if looks_like_local_helper_import(parse_result, import_name) {
        return None;
    }

    let context = assemble_context_for_import(store, import_name);
    let benchmark_context =
        context::read_context_tail(config.benchmark_context_log.as_deref(), 96_000)
            .unwrap_or_default();

    let attr_usage: serde_json::Value = serde_json::json!(
        parse_result.attribute_usage.iter().map(|(k, v)| {
            (k.clone(), v.iter().cloned().collect::<Vec<_>>())
        }).collect::<std::collections::BTreeMap<_, _>>()
    );

    let tier2_candidates = build_tier2_candidates(store, &[import_name.to_string()], python_version);

    let mut request = build_base_request(config);
    request["action"] = "single".into();
    request["imports"] = serde_json::json!([import_name]);
    request["python_version"] = python_version.into();
    request["context"] = serde_json::json!(context);
    request["benchmark_context"] = serde_json::Value::String(benchmark_context.clone());
    request["attribute_usage"] = attr_usage;
    request["tier2_candidates"] = tier2_candidates;

    persist_trace(config, &format!("single-package-{import_name}"), &request, None);

    let response = call_python(&request)?;
    persist_trace(
        config,
        &format!("single-package-{import_name}"),
        &request,
        Some(&response),
    );

    let mapped = response
        .get("mappings")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|m| m.get("package_name"))
        .and_then(|v| v.as_str())
        .unwrap_or(import_name)
        .to_string();

    let versions = pypi_client::compatible_versions(store, &mapped, python_version);
    let version = if versions.is_empty() {
        None
    } else {
        let llm_v = pick_version_via_python(config, &mapped, &versions, python_version, &benchmark_context);
        llm_v.or_else(|| version_sampler::equally_distanced_sample(&versions, &[]))
    };

    if parse_result.imports.iter().any(|item| item == import_name) || !mapped.is_empty() {
        Some((mapped, version))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Recovery Package Hint
// ---------------------------------------------------------------------------

/// Recovery hint from LLM: swap a package and/or add a new transitive dep.
pub struct RecoveryHint {
    pub wrong_pkg: String,
    pub correct_pkg: String,
    pub version: Option<String>,
    /// Optional new dependency to add (package_name, version).
    pub add_package: Option<(String, Option<String>)>,
    /// Optional package to remove entirely (local module / wrong package).
    pub remove_pkg: Option<String>,
}

pub fn recovery_package_hint(
    resolved: &[ResolvedDependency],
    error_log: &str,
    snippet_source: &str,
    store: &mut CacheStore,
    config: &ResolveConfig,
    python_version: &str,
    error_type: &str,
    previous_attempts: &[(String, String, String)],
) -> Option<RecoveryHint> {
    let resolved_desc: Vec<String> = resolved
        .iter()
        .map(|d| {
            if let Some(v) = &d.version {
                format!("{}=={} (import: {})", d.package_name, v, d.import_name)
            } else {
                format!("{} (import: {})", d.package_name, d.import_name)
            }
        })
        .collect();

    let attempts_json: Vec<Vec<String>> = previous_attempts
        .iter()
        .map(|(a, b, c)| vec![a.clone(), b.clone(), c.clone()])
        .collect();

    let mut request = build_base_request(config);
    request["action"] = "recovery".into();
    request["resolved_packages"] = serde_json::json!(resolved_desc);
    request["error_log"] = error_log.into();
    request["snippet_source"] = snippet_source.into();
    request["python_version"] = python_version.into();
    request["error_type"] = error_type.into();
    request["previous_attempts"] = serde_json::json!(attempts_json);

    persist_trace(config, "recovery-fix", &request, None);

    let response = call_python(&request)?;
    persist_trace(config, "recovery-fix", &request, Some(&response));

    let fix_possible = response
        .get("fix_possible")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !fix_possible {
        return None;
    }

    let wrong_pkg = response
        .get("wrong_package")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let correct_pkg = response
        .get("correct_package")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Parse optional version field (LLM-suggested version pin)
    let llm_version = response
        .get("version")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string());

    // Parse optional add_package field (e.g. "protobuf==3.20.3")
    let add_package = response
        .get("add_package")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| {
            if let Some((name, ver)) = s.split_once("==") {
                (name.trim().to_string(), Some(ver.trim().to_string()))
            } else {
                (s.trim().to_string(), None)
            }
        });

    // Parse optional remove_package field (package to remove entirely)
    let remove_pkg = response
        .get("remove_package")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string());

    let has_swap = !wrong_pkg.is_empty() && !correct_pkg.is_empty() && wrong_pkg != correct_pkg;
    let has_version_pin = !wrong_pkg.is_empty()
        && !correct_pkg.is_empty()
        && wrong_pkg == correct_pkg
        && llm_version.is_some();

    if !has_swap && !has_version_pin && add_package.is_none() && remove_pkg.is_none() {
        return None;
    }

    let version = if has_swap {
        // For package swaps, verify the new package exists on PyPI and pick a version
        let versions = pypi_client::compatible_versions(store, &correct_pkg, python_version);
        if versions.is_empty() && add_package.is_none() {
            return None;
        }
        if !versions.is_empty() {
            version_sampler::equally_distanced_sample(&versions, &[])
        } else {
            None
        }
    } else if has_version_pin {
        // LLM suggested pinning the same package to a specific version
        llm_version
    } else {
        None
    };

    Some(RecoveryHint {
        wrong_pkg,
        correct_pkg,
        version,
        add_package,
        remove_pkg,
    })
}

// ---------------------------------------------------------------------------
// Version selection via Python LLM
// ---------------------------------------------------------------------------

fn pick_version_via_python(
    config: &ResolveConfig,
    package_name: &str,
    versions: &[String],
    python_version: &str,
    benchmark_context: &str,
) -> Option<String> {
    let mut request = build_base_request(config);
    request["action"] = "version".into();
    request["package_name"] = package_name.into();
    request["versions"] = serde_json::json!(versions);
    request["python_version"] = python_version.into();
    request["benchmark_context"] = benchmark_context.into();

    let response = call_python(&request)?;

    let version = response
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if version.is_empty() || version.eq_ignore_ascii_case("NONE") {
        return None;
    }
    if versions.iter().any(|v| v == &version) {
        Some(version)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// #12: Batch version selection — one LLM call for N packages
// ---------------------------------------------------------------------------

fn batch_pick_versions(
    config: &ResolveConfig,
    packages: &[(String, Vec<String>)],
    python_version: &str,
    benchmark_context: &str,
) -> HashMap<String, String> {
    if packages.is_empty() {
        return HashMap::new();
    }
    // For single package, fall back to single-package call
    if packages.len() == 1 {
        let (pkg, versions) = &packages[0];
        if let Some(v) = pick_version_via_python(config, pkg, versions, python_version, benchmark_context) {
            let mut map = HashMap::new();
            map.insert(pkg.clone(), v);
            return map;
        }
        return HashMap::new();
    }

    let mut batch_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (pkg, versions) in packages {
        batch_map.insert(pkg.clone(), serde_json::json!(versions));
    }

    let mut request = build_base_request(config);
    request["action"] = "batch_version".into();
    request["python_version"] = python_version.into();
    request["benchmark_context"] = benchmark_context.into();
    request["batch_packages"] = serde_json::Value::Object(batch_map);

    let response = match call_python(&request) {
        Some(r) => r,
        None => return HashMap::new(),
    };

    let mut result = HashMap::new();
    if let Some(batch_versions) = response.get("batch_versions").and_then(|v| v.as_object()) {
        for (pkg, version_val) in batch_versions {
            if let Some(version) = version_val.as_str() {
                if !version.is_empty() && !version.eq_ignore_ascii_case("NONE") {
                    // Validate version is in the allowed list
                    if let Some((_, versions)) = packages.iter().find(|(p, _)| p == pkg) {
                        if versions.iter().any(|v| v == version) {
                            result.insert(pkg.clone(), version.to_string());
                        }
                    }
                }
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tier2 candidate lookup for prompt injection
// ---------------------------------------------------------------------------

fn build_tier2_candidates(
    store: &CacheStore,
    imports: &[String],
    _python_version: &str,
) -> serde_json::Value {
    let mut candidates: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for import_name in imports {
        let mut cands = Vec::new();
        let normalized = crate::cache::store::normalize(import_name);

        // Source 1: Check known packages from pypi_index for close matches
        for pkg_name in store.pypi_index.keys() {
            let norm_pkg = crate::cache::store::normalize(pkg_name);
            if norm_pkg == normalized {
                continue; // Skip exact match (identity)
            }
            if norm_pkg.contains(normalized.as_str()) || normalized.contains(norm_pkg.as_str()) {
                if !cands.contains(pkg_name) {
                    cands.push(pkg_name.clone());
                }
            }
            if cands.len() >= 10 {
                break;
            }
        }

        // Source 2 (#3): Query KGraph SQLite DB for candidate packages
        // Finds packages whose name matches patterns like "python-{import}",
        // "{import}-python", "py{import}", or LIKE %{import}%
        if cands.len() < 10 {
            let kgraph_path = store.cache_path.join("smtpip-kgraph.sqlite3");
            let kgraph_cands =
                super::kgraph_db::kgraph_candidate_packages(&kgraph_path, import_name, 10);
            for kc in kgraph_cands {
                if cands.len() >= 10 {
                    break;
                }
                if !cands.iter().any(|c| c.eq_ignore_ascii_case(&kc)) {
                    cands.push(kc);
                }
            }
        }

        if !cands.is_empty() {
            candidates.insert(import_name.clone(), cands);
        }
    }
    serde_json::json!(candidates)
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

pub fn fallback_notes(
    unresolved_imports: &[String],
    _parse_result: &ParseResult,
    llm_enabled: bool,
) -> Vec<String> {
    if unresolved_imports.is_empty() {
        return Vec::new();
    }
    let mut notes = Vec::new();
    if llm_enabled {
        notes.push(format!(
            "LLM fallback requested for {} unresolved imports, but no provider was available.",
            unresolved_imports.len()
        ));
    } else {
        notes.push(format!(
            "LLM fallback skipped for {} unresolved imports because `--allow-llm` was not set.",
            unresolved_imports.len()
        ));
    }
    notes.push(format!(
        "Unresolved imports: {}",
        unresolved_imports.join(", ")
    ));
    notes
}

fn looks_like_local_helper_import(parse_result: &ParseResult, import_name: &str) -> bool {
    let normalized = crate::cache::store::normalize(import_name);
    // Expanded local module list matching Python-side local_detector.py (#4)
    if matches!(
        normalized.as_str(),
        "input-data" | "settings" | "config" | "conf" | "constants" | "urls" | "api" | "app" | "apps"
        | "views" | "models" | "forms" | "admin" | "tests" | "manage" | "wsgi" | "asgi"
        | "conftest" | "tasks" | "celery-tasks"
        | "util" | "utils" | "helper" | "helpers" | "common" | "shared" | "base" | "core"
        | "main" | "run" | "setup" | "version"
        | "db" | "database" | "middleware" | "serializers" | "permissions" | "signals"
        | "routers" | "schemas" | "exceptions" | "mixins" | "decorators"
        | "context-processors" | "templatetags" | "management" | "fixtures" | "migrations"
        | "factory" | "factories" | "mocks" | "stubs" | "testutils" | "test-helpers"
        | "local-settings" | "production-settings" | "development-settings"
        | "celeryconfig" | "gunicorn-config" | "uwsgi"
        | "input" | "output" | "data" | "result" | "results"
        | "solution" | "answer" | "submission" | "benchmark" | "train" | "test"
        | "evaluate" | "predict" | "preprocess" | "postprocess"
    ) {
        return true;
    }
    // Also check the non-expanded generic helper heuristic
    let generic_helper = matches!(
        normalized.as_str(),
        "util" | "utils" | "helper" | "helpers" | "common" | "shared"
    );
    generic_helper
        && parse_result
            .import_paths
            .iter()
            .any(|path| crate::cache::store::normalize(path).starts_with(&format!("{normalized}-")))
}
