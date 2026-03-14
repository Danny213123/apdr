pub mod cache;
pub mod context;
pub mod docker;
pub mod knowledge_cache;
pub mod llm;
pub mod parser;
pub mod recovery;
pub mod resolver;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ConfigDep {
    pub package: String,
    pub constraint: Option<String>,
    pub source_file: String,
}

#[derive(Clone, Debug)]
pub struct ParseResult {
    pub imports: Vec<String>,
    pub import_paths: Vec<String>,
    pub config_deps: Vec<ConfigDep>,
    pub python_version_min: String,
    pub python_version_max: Option<String>,
    pub confidence: f64,
    pub scanned_files: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ResolveConfig {
    pub python_version: Option<String>,
    pub python_version_range: usize,
    pub max_retries: usize,
    pub cache_path: PathBuf,
    pub output_dir: PathBuf,
    pub validation_timeout: Duration,
    pub parallel_versions: bool,
    pub scan_config_files: bool,
    pub allow_llm: bool,
    pub llm_provider: String,
    pub llm_model: String,
    pub llm_base_url: String,
    pub benchmark_context_log: Option<PathBuf>,
    pub validate: bool,
    pub execute_snippet: bool,
}

#[derive(Clone, Debug)]
pub struct ResolvedDependency {
    pub import_name: String,
    pub package_name: String,
    pub version: Option<String>,
    pub strategy: String,
    pub confidence: f64,
}

#[derive(Clone, Debug, Default)]
pub struct SolvabilityAssessment {
    pub decision: String,
    pub confidence: f64,
    pub reason: String,
    pub source: String,
}

#[derive(Clone, Debug, Default)]
pub struct ResolutionReport {
    pub cache_hits: usize,
    pub heuristic_hits: usize,
    pub llm_calls: usize,
    pub env_builds: usize,
    pub retries: usize,
    pub unresolved: Vec<String>,
    pub conflict_classes: BTreeMap<String, usize>,
    pub error_types: BTreeMap<String, usize>,
    pub notes: Vec<String>,
    pub duration: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct ValidationAttempt {
    pub attempt_index: usize,
    pub python_version: String,
    pub status: String,
    pub validation_backend: String,
    pub env_label: Option<String>,
    pub env_dir: Option<String>,
    pub env_create_duration_ms: u128,
    pub used_cached_env: bool,
    pub validated_env_cache_hit: bool,
    pub used_cached_lockfile: bool,
    pub error_type: Option<String>,
    pub conflict_class: Option<String>,
    pub fix_applied: Option<String>,
    pub log_excerpt: String,
    pub artifact_dir: Option<String>,
    pub build_log_path: Option<String>,
    pub run_log_path: Option<String>,
    pub combined_log_path: Option<String>,
    pub metadata_path: Option<String>,
    pub context_snapshot_path: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ValidationSummary {
    pub succeeded: bool,
    pub status: String,
    pub reason: Option<String>,
    pub validation_backend: String,
    pub solve_duration_ms: u128,
    pub validation_duration_ms: u128,
    pub env_create_duration_ms: u128,
    pub install_duration_ms: u128,
    pub smoke_duration_ms: u128,
    pub selected_python_version: Option<String>,
    pub build_image_id: Option<String>,
    pub lockfile_key: Option<String>,
    pub build_cache_key: Option<String>,
    pub attempts: Vec<ValidationAttempt>,
    pub iteration_history: Vec<String>,
    pub debug_dir: Option<String>,
    pub attempts_dir: Option<String>,
    pub llm_trace_dir: Option<String>,
    pub context_log_path: Option<String>,
    pub iterations_dir: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResolveResult {
    pub snippet_path: PathBuf,
    pub python_version: String,
    pub parse_result: ParseResult,
    pub solvability: Option<SolvabilityAssessment>,
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub requirements_txt: String,
    pub lockfile: Option<String>,
    pub build_image_id: Option<String>,
    pub validation: ValidationSummary,
    pub resolution_report: ResolutionReport,
}

#[derive(Clone, Debug, Default)]
pub struct CacheStats {
    pub import_mappings: usize,
    pub failure_patterns: usize,
    pub version_constraints: usize,
    pub lockfile_entries: usize,
    pub build_artifacts: usize,
    pub pypi_index_entries: usize,
    pub dependency_graph_entries: usize,
}

#[derive(Clone, Debug)]
pub struct FailurePattern {
    pub pattern: String,
    pub error_type: String,
    pub conflict_class: String,
    pub fix: String,
    pub success_rate: f64,
    pub times_applied: u32,
}

#[derive(Clone, Debug)]
pub struct ClassifierResult {
    pub error_type: String,
    pub conflict_class: String,
    pub matched_pattern: String,
    pub recommended_fix: String,
}

impl ResolveConfig {
    pub fn for_tool_root(tool_root: &Path) -> Self {
        Self {
            python_version: None,
            python_version_range: 1,
            max_retries: 10,
            cache_path: tool_root.join(".apdr-cache"),
            output_dir: tool_root.join("out"),
            validation_timeout: Duration::from_secs(300),
            parallel_versions: true,
            scan_config_files: true,
            allow_llm: false,
            llm_provider: "ollama".to_string(),
            llm_model: "gemma3:4b".to_string(),
            llm_base_url: "http://localhost:11434".to_string(),
            benchmark_context_log: None,
            validate: true,
            execute_snippet: true,
        }
    }
}

impl ResolveResult {
    pub fn write_outputs(&self, output_dir: &Path) -> io::Result<(PathBuf, PathBuf)> {
        fs::create_dir_all(output_dir)?;
        let requirements_path = output_dir.join("requirements.txt");
        let report_path = output_dir.join("resolution-report.txt");
        fs::write(&requirements_path, &self.requirements_txt)?;
        fs::write(&report_path, self.report_text())?;
        Ok((requirements_path, report_path))
    }

    pub fn report_text(&self) -> String {
        let resolved_rows = self
            .resolved
            .iter()
            .map(|dependency| {
                format!(
                    "- {} -> {}{} [{} | confidence {:.2}]",
                    dependency.import_name,
                    dependency.package_name,
                    dependency
                        .version
                        .as_ref()
                        .map(|value| format!("=={value}"))
                        .unwrap_or_default(),
                    dependency.strategy,
                    dependency.confidence
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let config_rows = self
            .parse_result
            .config_deps
            .iter()
            .map(|dependency| {
                format!(
                    "- {}{} ({})",
                    dependency.package,
                    dependency
                        .constraint
                        .as_ref()
                        .map(|value| format!(" {value}"))
                        .unwrap_or_default(),
                    dependency.source_file
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let notes = if self.resolution_report.notes.is_empty() {
            "- none".to_string()
        } else {
            self.resolution_report
                .notes
                .iter()
                .map(|note| format!("- {note}"))
                .collect::<Vec<_>>()
                .join("\n")
        };

        let unresolved = if self.unresolved.is_empty() {
            "- none".to_string()
        } else {
            self.unresolved
                .iter()
                .map(|item| format!("- {item}"))
                .collect::<Vec<_>>()
                .join("\n")
        };

        format!(
            "snippet: {}\npython_version: {}\nsolvability_decision: {}\nsolvability_confidence: {:.2}\nsolvability_reason: {}\nsolvability_source: {}\ncache_hits: {}\nheuristic_hits: {}\nllm_calls: {}\nenv_builds: {}\nretries: {}\nduration_ms: {}\nsolve_duration_ms: {}\nvalidation_duration_ms: {}\nenv_create_duration_ms: {}\ninstall_duration_ms: {}\nsmoke_duration_ms: {}\nvalidation_backend: {}\nvalidation_succeeded: {}\nvalidation_status: {}\nvalidation_reason: {}\nvalidation_python: {}\nbuild_image_id: {}\nlockfile_key: {}\ndebug_dir: {}\nattempts_dir: {}\nllm_trace_dir: {}\ncontext_log: {}\niterations_dir: {}\n\nresolved_dependencies:\n{}\n\nconfig_dependencies:\n{}\n\nunresolved:\n{}\n\nnotes:\n{}\n\nvalidation_attempts:\n{}\n",
            self.snippet_path.display(),
            self.python_version,
            self.solvability
                .as_ref()
                .map(|item| item.decision.as_str())
                .unwrap_or("--"),
            self.solvability
                .as_ref()
                .map(|item| item.confidence)
                .unwrap_or(0.0),
            self.solvability
                .as_ref()
                .map(|item| item.reason.as_str())
                .unwrap_or("--"),
            self.solvability
                .as_ref()
                .map(|item| item.source.as_str())
                .unwrap_or("--"),
            self.resolution_report.cache_hits,
            self.resolution_report.heuristic_hits,
            self.resolution_report.llm_calls,
            self.resolution_report.env_builds,
            self.resolution_report.retries,
            self.resolution_report.duration.as_millis(),
            self.validation.solve_duration_ms,
            self.validation.validation_duration_ms,
            self.validation.env_create_duration_ms,
            self.validation.install_duration_ms,
            self.validation.smoke_duration_ms,
            if self.validation.validation_backend.is_empty() { "env" } else { &self.validation.validation_backend },
            self.validation.succeeded,
            if self.validation.status.is_empty() {
                if self.validation.succeeded {
                    "passed"
                } else {
                    "failed"
                }
            } else {
                &self.validation.status
            },
            self.validation.reason.as_deref().unwrap_or("--"),
            self.validation.selected_python_version.as_deref().unwrap_or("--"),
            self.build_image_id.as_deref().unwrap_or("--"),
            self.validation.lockfile_key.as_deref().unwrap_or("--"),
            self.validation.debug_dir.as_deref().unwrap_or("--"),
            self.validation.attempts_dir.as_deref().unwrap_or("--"),
            self.validation.llm_trace_dir.as_deref().unwrap_or("--"),
            self.validation.context_log_path.as_deref().unwrap_or("--"),
            self.validation.iterations_dir.as_deref().unwrap_or("--"),
            if resolved_rows.is_empty() {
                "- none".to_string()
            } else {
                resolved_rows
            },
            if config_rows.is_empty() {
                "- none".to_string()
            } else {
                config_rows
            },
            unresolved,
            notes,
            if self.validation.attempts.is_empty() {
                "- none".to_string()
            } else {
                self.validation
                    .attempts
                    .iter()
                    .map(|attempt| {
                        format!(
                            "- attempt={} py={} backend={} status={} error_type={} conflict_class={} fix={} env_label={} env_dir={} env_create_ms={} cached_env={} env_cache_hit={} cached_lockfile={} artifact_dir={} build_log={} run_log={} combined_log={} metadata={} context_snapshot={}",
                            attempt.attempt_index,
                            attempt.python_version,
                            if attempt.validation_backend.is_empty() { "env" } else { &attempt.validation_backend },
                            attempt.status,
                            attempt.error_type.as_deref().unwrap_or("--"),
                            attempt.conflict_class.as_deref().unwrap_or("--"),
                            attempt.fix_applied.as_deref().unwrap_or("--"),
                            attempt.env_label.as_deref().unwrap_or("--"),
                            attempt.env_dir.as_deref().unwrap_or("--"),
                            attempt.env_create_duration_ms,
                            attempt.used_cached_env,
                            attempt.validated_env_cache_hit,
                            attempt.used_cached_lockfile,
                            attempt.artifact_dir.as_deref().unwrap_or("--"),
                            attempt.build_log_path.as_deref().unwrap_or("--"),
                            attempt.run_log_path.as_deref().unwrap_or("--"),
                            attempt.combined_log_path.as_deref().unwrap_or("--"),
                            attempt.metadata_path.as_deref().unwrap_or("--"),
                            attempt.context_snapshot_path.as_deref().unwrap_or("--")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        )
    }

    pub fn summary_lines(&self, requirements_path: &Path, report_path: &Path) -> String {
        format!(
            "PYTHON_VERSION={}\nREQUIREMENTS_PATH={}\nREPORT_PATH={}\nRESOLVED_COUNT={}\nUNRESOLVED_COUNT={}\nSOLVABILITY_DECISION={}\nSOLVABILITY_CONFIDENCE={:.2}\nSOLVABILITY_REASON={}\nSOLVABILITY_SOURCE={}\nSOLVE_DURATION_MS={}\nVALIDATION_DURATION_MS={}\nENV_CREATE_DURATION_MS={}\nINSTALL_DURATION_MS={}\nSMOKE_DURATION_MS={}\nVALIDATION_BACKEND={}\nVALIDATION_SUCCEEDED={}\nVALIDATION_STATUS={}\nVALIDATION_REASON={}\nVALIDATION_PYTHON={}\nBUILD_IMAGE_ID={}\nLOCKFILE_KEY={}\nDEBUG_DIR={}\nATTEMPTS_DIR={}\nLLM_TRACE_DIR={}\nCONTEXT_LOG={}\nITERATIONS_DIR={}\n",
            self.python_version,
            requirements_path.display(),
            report_path.display(),
            self.resolved.len(),
            self.unresolved.len(),
            self.solvability
                .as_ref()
                .map(|item| item.decision.as_str())
                .unwrap_or(""),
            self.solvability
                .as_ref()
                .map(|item| item.confidence)
                .unwrap_or(0.0),
            self.solvability
                .as_ref()
                .map(|item| item.reason.as_str())
                .unwrap_or(""),
            self.solvability
                .as_ref()
                .map(|item| item.source.as_str())
                .unwrap_or(""),
            self.validation.solve_duration_ms,
            self.validation.validation_duration_ms,
            self.validation.env_create_duration_ms,
            self.validation.install_duration_ms,
            self.validation.smoke_duration_ms,
            if self.validation.validation_backend.is_empty() { "env" } else { &self.validation.validation_backend },
            self.validation.succeeded,
            if self.validation.status.is_empty() {
                if self.validation.succeeded {
                    "passed"
                } else {
                    "failed"
                }
            } else {
                &self.validation.status
            },
            self.validation.reason.as_deref().unwrap_or(""),
            self.validation.selected_python_version.as_deref().unwrap_or(""),
            self.build_image_id.as_deref().unwrap_or(""),
            self.validation.lockfile_key.as_deref().unwrap_or(""),
            self.validation.debug_dir.as_deref().unwrap_or(""),
            self.validation.attempts_dir.as_deref().unwrap_or(""),
            self.validation.llm_trace_dir.as_deref().unwrap_or(""),
            self.validation.context_log_path.as_deref().unwrap_or(""),
            self.validation.iterations_dir.as_deref().unwrap_or("")
        )
    }
}
