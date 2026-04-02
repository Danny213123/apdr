pub mod cache;
pub mod context;
pub mod docker;
pub mod knowledge_cache;
pub mod parser;
pub mod recovery;
pub mod resolver;

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const VALIDATION_BACKEND_ENV: &str = "env";
pub const VALIDATION_BACKEND_DOCKER: &str = "docker";
pub const VALIDATION_BACKEND_LLM: &str = "llm";
pub const RUN_CONTRACT_VERSION: &str = "1";

pub fn default_apdr_cache_path(tool_root: &Path) -> PathBuf {
    if let Ok(cache_dir) = env::var("APDR_CACHE_DIR") {
        let trimmed = cache_dir.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    if let Some(cache_root) = dirs::cache_dir() {
        return cache_root.join("apdr");
    }
    tool_root.join(".apdr-cache")
}

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
    pub stdlib_modules: std::collections::BTreeSet<String>,
    /// Maps module name → set of attributes accessed (e.g. {"cv2": {"imread"}}).
    pub attribute_usage: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
}

#[derive(Clone, Debug)]
pub struct ResolveConfig {
    pub python_version: Option<String>,
    pub python_version_range: usize,
    pub max_retries: usize,
    pub cache_path: PathBuf,
    pub output_dir: PathBuf,
    pub pre_solve_timeout: Duration,
    pub validation_timeout: Duration,
    pub validated_env_cache_max_entries: usize,
    pub validated_env_cache_max_bytes: Option<u64>,
    pub package_repository_cache_enabled: bool,
    pub parallel_versions: bool,
    pub scan_config_files: bool,
    pub allow_llm: bool,
    pub llm_only_mode: bool,
    pub llm_provider: String,
    pub llm_model: String,
    pub llm_base_url: String,
    pub agent_mode: String,
    pub tool_profile: String,
    pub retrieval_profile: String,
    pub policy_label: String,
    pub benchmark_context_log: Option<PathBuf>,
    pub validate: bool,
    pub validation_backend: String,
    pub execute_snippet: bool,
    pub force_validate: bool,
    pub run_contract: RunContractMetadata,
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
    /// Specific imports the LLM identified as unsolvable (for persistent learning).
    pub unsolvable_modules: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct UnsolvableModuleRecord {
    pub module_name: String,
    pub category: String,
    pub reason: String,
    pub confidence: f64,
    pub times_seen: u32,
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
    pub min_confidence: f64,
    pub mean_confidence: f64,
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
    pub fallback_invoked: bool,
    pub fallback_outcome: Option<String>,
    pub fallback_reason: Option<String>,
    pub failure_bucket: String,
    pub failure_family: Option<String>,
    pub root_cause: Option<String>,
    pub missing_module: Option<String>,
    pub failing_package: Option<String>,
    pub repair_strategy_applied: Option<String>,
    pub skip_candidate: bool,
    pub escalated_backend: Option<String>,
    pub validation_path: Option<String>,
    pub repeat_failure_signature: Option<String>,
    pub validation_backend: String,
    pub solve_duration_ms: u128,
    pub validation_duration_ms: u128,
    pub llm_duration_ms: u128,
    pub env_create_duration_ms: u128,
    pub install_duration_ms: u128,
    pub docker_startup_duration_ms: u128,
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
    /// Number of times the LangGraph multi-agent pipeline was invoked.
    pub agent_invocations: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RunContractMetadata {
    pub run_contract_version: String,
    pub model_name: String,
    pub base_url: String,
    pub run_intent: String,
    pub execution_mode: String,
    pub cache_state: String,
    pub host_architecture: String,
    pub apdr_binary_architecture: String,
    pub python_architecture: String,
    pub llm_context_window: String,
    pub inference_policy: String,
    pub build_profile: String,
}

#[derive(Clone, Debug)]
pub struct ResolveResult {
    pub snippet_path: PathBuf,
    pub python_version: String,
    pub parse_result: ParseResult,
    pub run_contract: RunContractMetadata,
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

impl ValidationSummary {
    pub fn effective_validation_path(&self) -> Option<String> {
        self.validation_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| derive_validation_path_from_attempts(&self.attempts, &self.validation_backend))
    }

    pub fn refresh_validation_path(&mut self) {
        self.validation_path = self.effective_validation_path();
    }
}

fn derive_validation_path_from_attempts(
    attempts: &[ValidationAttempt],
    fallback_backend: &str,
) -> Option<String> {
    let mut segments: Vec<String> = Vec::new();
    for attempt in attempts {
        let Some(segment) = validation_path_segment(&attempt.validation_backend) else {
            continue;
        };
        if segments.last() != Some(&segment) {
            segments.push(segment);
        }
    }
    if segments.is_empty() {
        validation_path_segment(fallback_backend)
    } else {
        Some(segments.join("->"))
    }
}

fn validation_path_segment(backend: &str) -> Option<String> {
    let trimmed = backend.trim();
    if trimmed.is_empty() {
        return None;
    }
    match trimmed {
        VALIDATION_BACKEND_LLM => Some("llm-agent".to_string()),
        _ => Some(trimmed.to_string()),
    }
}

impl ResolveConfig {
    pub fn for_tool_root(tool_root: &Path) -> Self {
        let mut config = Self {
            python_version: None,
            python_version_range: 1,
            max_retries: 7, // Increased from 5 to give LLM more opportunities to learn and recover
            cache_path: default_apdr_cache_path(tool_root),
            output_dir: tool_root.join("out"),
            pre_solve_timeout: Duration::from_secs(
                env_usize("APDR_PRE_SOLVE_TIMEOUT_SECS", 10) as u64
            ),
            validation_timeout: Duration::from_secs(
                env_usize("APDR_VALIDATION_TIMEOUT_SECS", 900) as u64
            ),
            validated_env_cache_max_entries: env_usize(
                "APDR_VALIDATED_ENV_CACHE_MAX_ENTRIES",
                crate::cache::maintenance::DEFAULT_MAX_VALIDATED_ENVS,
            ),
            validated_env_cache_max_bytes: env_optional_gib(
                "APDR_VALIDATED_ENV_CACHE_MAX_GB",
                Some(crate::cache::maintenance::DEFAULT_MAX_VALIDATED_ENV_BYTES),
            ),
            package_repository_cache_enabled: env_flag(
                "APDR_ENABLE_PACKAGE_REPOSITORY_CACHE",
                false,
            ),
            parallel_versions: true,
            scan_config_files: true,
            allow_llm: false,
            llm_only_mode: false,
            llm_provider: "ollama".to_string(),
            llm_model: "qwen3.5:9b".to_string(),
            llm_base_url: "http://localhost:11434".to_string(),
            agent_mode: env_string("APDR_TIER3_AGENT_MODE", "direct"),
            tool_profile: env_string("APDR_TIER3_TOOL_PROFILE", "full"),
            retrieval_profile: env_string("APDR_TIER3_RETRIEVAL_PROFILE", "none"),
            policy_label: env_string("APDR_TIER3_POLICY_LABEL", ""),
            benchmark_context_log: None,
            validate: true,
            validation_backend: VALIDATION_BACKEND_ENV.to_string(),
            execute_snippet: true,
            force_validate: false,
            run_contract: RunContractMetadata::default(),
        };
        config.run_contract = RunContractMetadata::from_runtime_defaults(&config);
        config
    }

    pub fn validation_backend(&self) -> &str {
        normalize_validation_backend(&self.validation_backend)
    }
}

pub fn normalize_validation_backend(value: &str) -> &str {
    match value.trim().to_ascii_lowercase().as_str() {
        VALIDATION_BACKEND_DOCKER => VALIDATION_BACKEND_DOCKER,
        VALIDATION_BACKEND_LLM => VALIDATION_BACKEND_LLM,
        _ => VALIDATION_BACKEND_ENV,
    }
}

fn normalize_machine_architecture(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "aarch64" | "arm64e" => "arm64".to_string(),
        "amd64" | "x64" | "x86-64" => "x86_64".to_string(),
        other if !other.is_empty() => other.to_string(),
        _ => "unknown".to_string(),
    }
}

fn default_execution_mode(validation_backend: &str) -> String {
    match normalize_validation_backend(validation_backend) {
        VALIDATION_BACKEND_DOCKER => "docker-proof".to_string(),
        VALIDATION_BACKEND_LLM => "llm-hybrid".to_string(),
        _ => "env-fast".to_string(),
    }
}

fn default_context_window() -> String {
    env::var("APDR_NUM_CTX")
        .ok()
        .or_else(|| env::var("OLLAMA_CONTEXT_LENGTH").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "16384".to_string())
}

impl RunContractMetadata {
    pub fn from_runtime_defaults(config: &ResolveConfig) -> Self {
        let host_architecture = normalize_machine_architecture(env::consts::ARCH);
        Self {
            run_contract_version: RUN_CONTRACT_VERSION.to_string(),
            model_name: config.llm_model.trim().to_string(),
            base_url: config.llm_base_url.trim().to_string(),
            run_intent: "baseline".to_string(),
            execution_mode: default_execution_mode(config.validation_backend()),
            cache_state: "unknown".to_string(),
            host_architecture: host_architecture.clone(),
            apdr_binary_architecture: host_architecture.clone(),
            python_architecture: host_architecture,
            llm_context_window: default_context_window(),
            inference_policy: "temperature=inherited".to_string(),
            build_profile: "standard".to_string(),
        }
    }

    pub fn from_json_path(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|error| format!("failed to read run contract {}: {error}", path.display()))?;
        let value = serde_json::from_str::<serde_json::Value>(&raw)
            .map_err(|error| format!("failed to parse run contract {}: {error}", path.display()))?;
        let contract = Self {
            run_contract_version: json_string(&value, "run_contract_version"),
            model_name: json_string(&value, "model_name"),
            base_url: json_string(&value, "base_url"),
            run_intent: json_string(&value, "run_intent"),
            execution_mode: json_string(&value, "execution_mode"),
            cache_state: json_string(&value, "cache_state"),
            host_architecture: json_string(&value, "host_architecture"),
            apdr_binary_architecture: json_string(&value, "apdr_binary_architecture"),
            python_architecture: json_string(&value, "python_architecture"),
            llm_context_window: json_string(&value, "llm_context_window"),
            inference_policy: json_string(&value, "inference_policy"),
            build_profile: json_string(&value, "build_profile"),
        };
        let missing = contract.missing_required_keys();
        if missing.is_empty() {
            Ok(contract)
        } else {
            Err(format!(
                "run contract {} is missing required keys: {}",
                path.display(),
                missing.join(", ")
            ))
        }
    }

    pub fn with_runtime_fallbacks(mut self, config: &ResolveConfig) -> Self {
        let defaults = Self::from_runtime_defaults(config);
        if self.run_contract_version.trim().is_empty() {
            self.run_contract_version = defaults.run_contract_version;
        }
        if self.model_name.trim().is_empty() {
            self.model_name = defaults.model_name;
        }
        if self.base_url.trim().is_empty() {
            self.base_url = defaults.base_url;
        }
        if self.run_intent.trim().is_empty() {
            self.run_intent = defaults.run_intent;
        }
        if self.execution_mode.trim().is_empty() {
            self.execution_mode = defaults.execution_mode;
        }
        if self.cache_state.trim().is_empty() {
            self.cache_state = defaults.cache_state;
        }
        if self.host_architecture.trim().is_empty() {
            self.host_architecture = defaults.host_architecture;
        }
        if self.apdr_binary_architecture.trim().is_empty() {
            self.apdr_binary_architecture = defaults.apdr_binary_architecture;
        }
        if self.python_architecture.trim().is_empty() {
            self.python_architecture = defaults.python_architecture;
        }
        if self.llm_context_window.trim().is_empty() {
            self.llm_context_window = defaults.llm_context_window;
        }
        if self.inference_policy.trim().is_empty() {
            self.inference_policy = defaults.inference_policy;
        }
        if self.build_profile.trim().is_empty() {
            self.build_profile = defaults.build_profile;
        }
        self
    }

    pub fn missing_required_keys(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if self.run_contract_version.trim().is_empty() {
            missing.push("run_contract_version");
        }
        if self.model_name.trim().is_empty() {
            missing.push("model_name");
        }
        if self.base_url.trim().is_empty() {
            missing.push("base_url");
        }
        if self.run_intent.trim().is_empty() {
            missing.push("run_intent");
        }
        if self.execution_mode.trim().is_empty() {
            missing.push("execution_mode");
        }
        if self.cache_state.trim().is_empty() {
            missing.push("cache_state");
        }
        if self.host_architecture.trim().is_empty() {
            missing.push("host_architecture");
        }
        if self.apdr_binary_architecture.trim().is_empty() {
            missing.push("apdr_binary_architecture");
        }
        if self.python_architecture.trim().is_empty() {
            missing.push("python_architecture");
        }
        if self.llm_context_window.trim().is_empty() {
            missing.push("llm_context_window");
        }
        if self.inference_policy.trim().is_empty() {
            missing.push("inference_policy");
        }
        if self.build_profile.trim().is_empty() {
            missing.push("build_profile");
        }
        missing
    }
}

fn json_string(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|item| item.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn env_flag(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_optional_gib(name: &str, default_bytes: Option<u64>) -> Option<u64> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value.trim().parse::<u64>().ok()?;
            if parsed == 0 {
                None
            } else {
                Some(parsed.saturating_mul(1024 * 1024 * 1024))
            }
        }
        Err(_) => default_bytes,
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
        let validation_path = self.validation.effective_validation_path();
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
            "snippet: {}\npython_version: {}\nsolvability_decision: {}\nsolvability_confidence: {:.2}\nsolvability_reason: {}\nsolvability_source: {}\ncache_hits: {}\nheuristic_hits: {}\nllm_calls: {}\nenv_builds: {}\nretries: {}\nmin_confidence: {:.2}\nmean_confidence: {:.2}\nduration_ms: {}\nsolve_duration_ms: {}\nvalidation_duration_ms: {}\nllm_duration_ms: {}\nenv_create_duration_ms: {}\ninstall_duration_ms: {}\ndocker_startup_duration_ms: {}\nsmoke_duration_ms: {}\nrun_contract_version: {}\nmodel_name: {}\nbase_url: {}\nrun_intent: {}\nexecution_mode: {}\ncache_state: {}\nhost_architecture: {}\napdr_binary_architecture: {}\npython_architecture: {}\nllm_context_window: {}\ninference_policy: {}\nbuild_profile: {}\nvalidation_backend: {}\nvalidation_path: {}\nvalidation_succeeded: {}\nvalidation_status: {}\nvalidation_reason: {}\nfallback_invoked: {}\nfallback_outcome: {}\nfallback_reason: {}\nfailure_bucket: {}\nfailure_family: {}\nroot_cause: {}\nmissing_module: {}\nfailing_package: {}\nrepair_strategy_applied: {}\nskip_candidate: {}\nescalated_backend: {}\nrepeat_failure_signature: {}\nvalidation_python: {}\nbuild_image_id: {}\nlockfile_key: {}\ndebug_dir: {}\nattempts_dir: {}\nllm_trace_dir: {}\ncontext_log: {}\niterations_dir: {}\n\nresolved_dependencies:\n{}\n\nconfig_dependencies:\n{}\n\nunresolved:\n{}\n\nnotes:\n{}\n\nvalidation_attempts:\n{}\n",
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
            self.resolution_report.min_confidence,
            self.resolution_report.mean_confidence,
            self.resolution_report.duration.as_millis(),
            self.validation.solve_duration_ms,
            self.validation.validation_duration_ms,
            self.validation.llm_duration_ms,
            self.validation.env_create_duration_ms,
            self.validation.install_duration_ms,
            self.validation.docker_startup_duration_ms,
            self.validation.smoke_duration_ms,
            self.run_contract.run_contract_version,
            self.run_contract.model_name,
            self.run_contract.base_url,
            self.run_contract.run_intent,
            self.run_contract.execution_mode,
            self.run_contract.cache_state,
            self.run_contract.host_architecture,
            self.run_contract.apdr_binary_architecture,
            self.run_contract.python_architecture,
            self.run_contract.llm_context_window,
            self.run_contract.inference_policy,
            self.run_contract.build_profile,
            if self.validation.validation_backend.is_empty() { "env" } else { &self.validation.validation_backend },
            validation_path.as_deref().unwrap_or("--"),
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
            self.validation.fallback_invoked,
            self.validation
                .fallback_outcome
                .as_deref()
                .unwrap_or("--"),
            self.validation
                .fallback_reason
                .as_deref()
                .unwrap_or("--"),
            if self.validation.failure_bucket.is_empty() {
                "--"
            } else {
                &self.validation.failure_bucket
            },
            self.validation.failure_family.as_deref().unwrap_or("--"),
            self.validation.root_cause.as_deref().unwrap_or("--"),
            self.validation.missing_module.as_deref().unwrap_or("--"),
            self.validation.failing_package.as_deref().unwrap_or("--"),
            self.validation
                .repair_strategy_applied
                .as_deref()
                .unwrap_or("--"),
            self.validation.skip_candidate,
            self.validation.escalated_backend.as_deref().unwrap_or("--"),
            self.validation
                .repeat_failure_signature
                .as_deref()
                .unwrap_or("--"),
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
                        // Extract a short error hint from the log_excerpt for
                        // quick scanning without opening combined.log.
                        let error_hint = if attempt.status == "passed" || attempt.log_excerpt.is_empty() {
                            String::new()
                        } else {
                            extract_error_hint(&attempt.log_excerpt)
                        };
                        format!(
                            "- attempt={} py={} backend={} status={} error_type={} conflict_class={} fix={}{} cached_env={} env_cache_hit={} cached_lockfile={} combined_log={} metadata={}",
                            attempt.attempt_index,
                            attempt.python_version,
                            if attempt.validation_backend.is_empty() { "env" } else { &attempt.validation_backend },
                            attempt.status,
                            attempt.error_type.as_deref().unwrap_or("--"),
                            attempt.conflict_class.as_deref().unwrap_or("--"),
                            attempt.fix_applied.as_deref().unwrap_or("--"),
                            if error_hint.is_empty() { String::new() } else { format!(" error_hint=\"{}\"", error_hint) },
                            attempt.used_cached_env,
                            attempt.validated_env_cache_hit,
                            attempt.used_cached_lockfile,
                            attempt.combined_log_path.as_deref().unwrap_or("--"),
                            attempt.metadata_path.as_deref().unwrap_or("--")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }
        )
    }
}

/// Extract a short error hint (≤120 chars) from a log excerpt.
/// Used in the resolution report to show at a glance what went wrong.
fn extract_error_hint(log: &str) -> String {
    let markers = [
        "ModuleNotFoundError:",
        "ImportError:",
        "AttributeError:",
        "TypeError:",
        "SyntaxError:",
        "RuntimeError:",
        "Double requirement given:",
        "ERROR: Cannot install",
        "No matching distribution found",
        "error: subprocess-exited-with-error",
        "pkg-config",
        "fatal error:",
    ];
    for line in log.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        for marker in &markers {
            if trimmed.contains(marker) {
                let clean = trimmed.replace('"', "'");
                return if clean.len() > 120 {
                    format!("{}...", &clean[..117])
                } else {
                    clean
                };
            }
        }
    }
    String::new()
}

impl ResolveResult {
    pub fn summary_lines(&self, requirements_path: &Path, report_path: &Path) -> String {
        let validation_path = self.validation.effective_validation_path();
        format!(
            "PYTHON_VERSION={}\nREQUIREMENTS_PATH={}\nREPORT_PATH={}\nRESOLVED_COUNT={}\nUNRESOLVED_COUNT={}\nSOLVABILITY_DECISION={}\nSOLVABILITY_CONFIDENCE={:.2}\nSOLVABILITY_REASON={}\nSOLVABILITY_SOURCE={}\nLLM_CALLS={}\nENV_BUILDS={}\nRETRIES={}\nSOLVE_DURATION_MS={}\nVALIDATION_DURATION_MS={}\nLLM_DURATION_MS={}\nENV_CREATE_DURATION_MS={}\nINSTALL_DURATION_MS={}\nDOCKER_STARTUP_DURATION_MS={}\nSMOKE_DURATION_MS={}\nRUN_CONTRACT_VERSION={}\nMODEL_NAME={}\nBASE_URL={}\nRUN_INTENT={}\nEXECUTION_MODE={}\nCACHE_STATE={}\nHOST_ARCHITECTURE={}\nAPDR_BINARY_ARCHITECTURE={}\nPYTHON_ARCHITECTURE={}\nLLM_CONTEXT_WINDOW={}\nINFERENCE_POLICY={}\nBUILD_PROFILE={}\nVALIDATION_BACKEND={}\nVALIDATION_PATH={}\nVALIDATION_SUCCEEDED={}\nVALIDATION_STATUS={}\nVALIDATION_REASON={}\nfallback_invoked={}\nfallback_outcome={}\nfallback_reason={}\nFAILURE_BUCKET={}\nFAILURE_FAMILY={}\nROOT_CAUSE={}\nMISSING_MODULE={}\nFAILING_PACKAGE={}\nREPAIR_STRATEGY_APPLIED={}\nSKIP_CANDIDATE={}\nESCALATED_BACKEND={}\nREPEAT_FAILURE_SIGNATURE={}\nVALIDATION_PYTHON={}\nBUILD_IMAGE_ID={}\nLOCKFILE_KEY={}\nDEBUG_DIR={}\nATTEMPTS_DIR={}\nLLM_TRACE_DIR={}\nCONTEXT_LOG={}\nITERATIONS_DIR={}\n",
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
            self.resolution_report.llm_calls,
            self.resolution_report.env_builds,
            self.resolution_report.retries,
            self.validation.solve_duration_ms,
            self.validation.validation_duration_ms,
            self.validation.llm_duration_ms,
            self.validation.env_create_duration_ms,
            self.validation.install_duration_ms,
            self.validation.docker_startup_duration_ms,
            self.validation.smoke_duration_ms,
            self.run_contract.run_contract_version,
            self.run_contract.model_name,
            self.run_contract.base_url,
            self.run_contract.run_intent,
            self.run_contract.execution_mode,
            self.run_contract.cache_state,
            self.run_contract.host_architecture,
            self.run_contract.apdr_binary_architecture,
            self.run_contract.python_architecture,
            self.run_contract.llm_context_window,
            self.run_contract.inference_policy,
            self.run_contract.build_profile,
            if self.validation.validation_backend.is_empty() { "env" } else { &self.validation.validation_backend },
            validation_path.as_deref().unwrap_or(""),
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
            self.validation.fallback_invoked,
            self.validation
                .fallback_outcome
                .as_deref()
                .unwrap_or(""),
            self.validation
                .fallback_reason
                .as_deref()
                .unwrap_or(""),
            self.validation.failure_bucket.as_str(),
            self.validation.failure_family.as_deref().unwrap_or(""),
            self.validation.root_cause.as_deref().unwrap_or(""),
            self.validation.missing_module.as_deref().unwrap_or(""),
            self.validation.failing_package.as_deref().unwrap_or(""),
            self.validation
                .repair_strategy_applied
                .as_deref()
                .unwrap_or(""),
            self.validation.skip_candidate,
            self.validation.escalated_backend.as_deref().unwrap_or(""),
            self.validation
                .repeat_failure_signature
                .as_deref()
                .unwrap_or(""),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn phase17_llm_fixture_result() -> ResolveResult {
        ResolveResult {
            snippet_path: PathBuf::from("snippet.py"),
            python_version: "3.11".to_string(),
            parse_result: ParseResult {
                imports: vec!["requests".to_string()],
                import_paths: Vec::new(),
                config_deps: Vec::new(),
                python_version_min: "3.11".to_string(),
                python_version_max: None,
                confidence: 0.9,
                scanned_files: vec!["snippet.py".to_string()],
                stdlib_modules: std::collections::BTreeSet::new(),
                attribute_usage: BTreeMap::new(),
            },
            run_contract: RunContractMetadata {
                run_contract_version: "1".to_string(),
                model_name: "qwen".to_string(),
                base_url: "http://localhost:11434".to_string(),
                run_intent: "benchmark".to_string(),
                execution_mode: "llm-hybrid".to_string(),
                cache_state: "warm".to_string(),
                host_architecture: "arm64".to_string(),
                apdr_binary_architecture: "arm64".to_string(),
                python_architecture: "arm64".to_string(),
                llm_context_window: "8192".to_string(),
                inference_policy: "phase17".to_string(),
                build_profile: "debug".to_string(),
            },
            solvability: None,
            resolved: vec![ResolvedDependency {
                import_name: "requests".to_string(),
                package_name: "requests".to_string(),
                version: Some("2.32.0".to_string()),
                strategy: "heuristic".to_string(),
                confidence: 0.91,
            }],
            unresolved: Vec::new(),
            requirements_txt: "requests==2.32.0\n".to_string(),
            lockfile: None,
            build_image_id: None,
            validation: ValidationSummary {
                succeeded: false,
                status: "environment-build-failed".to_string(),
                reason: Some("env build failed".to_string()),
                fallback_invoked: true,
                fallback_outcome: Some("failed".to_string()),
                fallback_reason: Some("state key crash".to_string()),
                validation_backend: VALIDATION_BACKEND_LLM.to_string(),
                ..ValidationSummary::default()
            },
            resolution_report: ResolutionReport::default(),
        }
    }

    #[test]
    fn phase17_llm_report_text_includes_fallback_fields() {
        let result = phase17_llm_fixture_result();
        let report = result.report_text();

        assert!(report.contains("fallback_invoked: true"));
        assert!(report.contains("fallback_outcome: failed"));
        assert!(report.contains("fallback_reason: state key crash"));
    }

    #[test]
    fn phase17_llm_summary_lines_include_fallback_fields() {
        let result = phase17_llm_fixture_result();
        let summary = result.summary_lines(Path::new("requirements.txt"), Path::new("report.txt"));

        assert!(summary.contains("fallback_invoked=true"));
        assert!(summary.contains("fallback_outcome=failed"));
        assert!(summary.contains("fallback_reason=state key crash"));
    }

    fn phase18_backend_path_fixture_result() -> ResolveResult {
        let mut result = phase17_llm_fixture_result();
        result.validation.validation_backend = VALIDATION_BACKEND_LLM.to_string();
        result.validation.escalated_backend = Some(VALIDATION_BACKEND_DOCKER.to_string());
        result.validation.attempts = vec![
            ValidationAttempt {
                attempt_index: 1,
                python_version: "3.11".to_string(),
                validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                status: "build-failed".to_string(),
                ..Default::default()
            },
            ValidationAttempt {
                attempt_index: 2,
                python_version: "3.11".to_string(),
                validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
                status: "build-failed".to_string(),
                ..Default::default()
            },
        ];
        result
    }

    #[test]
    fn phase18_backend_report_text_includes_validation_path() {
        let result = phase18_backend_path_fixture_result();
        let report = result.report_text();

        assert!(report.contains("validation_backend: llm"));
        assert!(report.contains("validation_path: env->docker"));
        assert!(report.contains("escalated_backend: docker"));
    }

    #[test]
    fn phase18_backend_summary_lines_include_validation_path() {
        let result = phase18_backend_path_fixture_result();
        let summary = result.summary_lines(Path::new("requirements.txt"), Path::new("report.txt"));

        assert!(summary.contains("VALIDATION_BACKEND=llm"));
        assert!(summary.contains("VALIDATION_PATH=env->docker"));
        assert!(summary.contains("ESCALATED_BACKEND=docker"));
    }

    fn phase19_classification_fixture_result() -> ResolveResult {
        let mut result = phase17_llm_fixture_result();
        result.validation.status = "module-not-found".to_string();
        result.validation.reason = Some(
            "Missing module `numpy` persisted across multiple dependency sets.".to_string(),
        );
        result.validation.failure_bucket = "module-not-found".to_string();
        result.validation.failure_family = Some("dependency-resolution".to_string());
        result.validation.root_cause = result.validation.reason.clone();
        result.validation.missing_module = Some("numpy".to_string());
        result.validation.failing_package = Some("numpy".to_string());
        result.validation.fallback_invoked = false;
        result.validation.fallback_outcome = None;
        result.validation.fallback_reason = None;
        result
    }

    #[test]
    fn phase19_classification_report_text_includes_failure_family() {
        let result = phase19_classification_fixture_result();
        let report = result.report_text();

        assert!(report.contains("failure_bucket: module-not-found"));
        assert!(report.contains("failure_family: dependency-resolution"));
    }

    #[test]
    fn phase19_classification_summary_lines_include_failure_family() {
        let result = phase19_classification_fixture_result();
        let summary = result.summary_lines(Path::new("requirements.txt"), Path::new("report.txt"));

        assert!(summary.contains("FAILURE_BUCKET=module-not-found"));
        assert!(summary.contains("FAILURE_FAMILY=dependency-resolution"));
    }
}
