use crate::context;
use crate::ResolveConfig;
use std::collections::HashMap;
use std::path::Path;
pub(super) struct FailureEntry {
    package_tried: String,
    #[allow(dead_code)]
    error_reason: String,
}
pub(super) fn load_failure_memory(cache_path: &Path) -> HashMap<String, Vec<FailureEntry>> {
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
pub(super) fn format_failure_context(
    failures: &HashMap<String, Vec<FailureEntry>>,
    import_names: &[String],
) -> String {
    let mut lines = Vec::new();
    for name in import_names {
        if let Some(records) = failures.get(name) {
            for r in records {
                lines.push(format!(
                    "PREVIOUS FAILURE: import {} mapped to {} failed ({}). DO NOT suggest {} again.",
                    name, r.package_tried, r.error_reason, r.package_tried
                ));
            }
        }
    }
    lines.join("\n")
}
pub(super) fn has_failed(
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
pub(super) fn persist_trace(
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
