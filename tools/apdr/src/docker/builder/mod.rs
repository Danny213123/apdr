//! Validation builder facade for requirement verification.
//!
//! [`validate_requirements(...)`] is the reviewer entrypoint for the validation
//! builder. The facade selects the active backend, runs env validation first in
//! the default path, and records the attempt history that reviewers use to
//! understand how validation progressed.
//!
//! Backend-specific work lives in sibling modules: `env_backend` owns local env
//! creation and smoke tests, `docker_backend` owns container validation and the
//! env-to-Docker escalation path, `agent_backend` owns the validation-agent
//! path, and the `python_runtime` and `process` modules hold shared runtime and
//! command helpers. Reviewers should start here before drilling into a specific
//! backend module.
mod agent_backend;
mod docker_backend;
mod env_backend;
mod process;
mod python_runtime;

use self::agent_backend::validate_requirements_llm;
use self::docker_backend::{
    env_failure_reason_for_docker_retry, should_retry_failed_env_validation_in_docker,
    validate_requirements_docker,
};
use self::env_backend::validate_requirements_env;
use crate::cache::store::CacheStore;
use crate::{
    ResolveConfig, ValidationSummary, VALIDATION_BACKEND_DOCKER, VALIDATION_BACKEND_ENV,
    VALIDATION_BACKEND_LLM,
};
use std::io;
use std::path::Path;

pub(super) struct CommandResult {
    success: bool,
    combined_output: String,
    timed_out: bool,
    exit_code: Option<i32>,
    duration_ms: u128,
}

pub fn validate_requirements(
    snippet_path: &Path,
    requirements_txt: &str,
    imports: &[String],
    candidate_versions: &[String],
    attempt_offset: usize,
    config: &ResolveConfig,
    store: &mut CacheStore,
) -> io::Result<ValidationSummary> {
    match config.validation_backend() {
        VALIDATION_BACKEND_DOCKER => validate_requirements_docker(
            snippet_path,
            requirements_txt,
            imports,
            candidate_versions,
            attempt_offset,
            config,
            store,
        ),
        VALIDATION_BACKEND_LLM => validate_requirements_llm(
            snippet_path,
            requirements_txt,
            imports,
            candidate_versions,
            attempt_offset,
            config,
            store,
        ),
        _ => {
            let mut summary = validate_requirements_env(
                snippet_path,
                requirements_txt,
                imports,
                candidate_versions,
                attempt_offset,
                config,
                store,
            )?;
            if summary.succeeded {
                return Ok(summary);
            }
            if should_retry_failed_env_validation_in_docker(&summary, requirements_txt) {
                let reason = env_failure_reason_for_docker_retry(&summary, requirements_txt);
                eprintln!("[validation] env failed with {reason}, retrying with Docker");
                let docker_offset = attempt_offset + summary.attempts.len();
                let mut docker_summary = validate_requirements_docker(
                    snippet_path,
                    requirements_txt,
                    imports,
                    candidate_versions,
                    docker_offset,
                    config,
                    store,
                )?;
                merge_backend_retry_history(&mut summary, &mut docker_summary);
                summary = docker_summary;
                if summary.succeeded {
                    return Ok(summary);
                }
            }
            Ok(summary)
        }
    }
}

pub(super) fn merge_backend_retry_history(
    env_summary: &mut ValidationSummary,
    docker_summary: &mut ValidationSummary,
) {
    let mut combined = std::mem::take(&mut env_summary.attempts);
    combined.append(&mut docker_summary.attempts);
    docker_summary.attempts = combined;
}

#[cfg(test)]
use self::agent_backend::docker_agent_importable_with_probe;
#[cfg(test)]
use self::agent_backend::{
    apply_llm_route_metadata, llm_case_requires_host_runtime,
    llm_env_failure_requires_docker_escalation, llm_validation_route, merge_llm_retry_history,
    parse_agent_result, LlmValidationRoute,
};
#[cfg(test)]
use self::docker_backend::{
    candidate_image_refs_for_verification, should_attempt_docker_agent_fallback,
};
#[cfg(test)]
use self::env_backend::prepare_env_validation_attempt;
#[cfg(test)]
use self::env_backend::validated_env_archive_path;
#[cfg(test)]
use self::env_backend::validated_env_cache_path;
#[cfg(test)]
use self::env_backend::ValidatedEnvCacheSource;
#[cfg(test)]
use self::process::{DockerUnavailabilityReason, DockerValidationAvailability};
#[cfg(test)]
use self::python_runtime::{normalized_command_output_path, windows_launcher_version_arg};
#[cfg(test)]
use crate::cache::build_cache;
#[cfg(test)]
use crate::ValidationAttempt;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::PathBuf;
#[cfg(test)]
use std::sync::OnceLock;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_launcher_version_arg_formats_requested_minor() {
        assert_eq!(
            windows_launcher_version_arg("3.11").as_deref(),
            Some("-3.11")
        );
        assert_eq!(windows_launcher_version_arg("2.7").as_deref(), Some("-2.7"));
    }

    #[test]
    fn windows_launcher_version_arg_rejects_invalid_values() {
        assert!(windows_launcher_version_arg("").is_none());
        assert!(windows_launcher_version_arg("  ").is_none());
        assert!(windows_launcher_version_arg("3.11 rc1").is_none());
        assert!(windows_launcher_version_arg("python3.11").is_none());
    }

    #[test]
    fn normalized_command_output_path_trims_newlines() {
        assert_eq!(
            normalized_command_output_path("C:\\Python311\\python.exe\r\n"),
            Some(PathBuf::from("C:\\Python311\\python.exe"))
        );
    }

    #[test]
    fn normalized_command_output_path_rejects_empty_output() {
        assert_eq!(normalized_command_output_path(" \r\n\t "), None);
    }

    #[test]
    fn retries_failed_env_validation_in_docker_for_generic_env_failure() {
        let summary = ValidationSummary {
            attempts: vec![ValidationAttempt {
                status: "build-failed".to_string(),
                validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                log_excerpt: "some pip failure".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(should_retry_failed_env_validation_in_docker(
            &summary,
            "scrapy==1.8.3"
        ));
        assert_eq!(
            env_failure_reason_for_docker_retry(&summary, "scrapy==1.8.3"),
            "env validation failed"
        );
    }

    #[test]
    fn does_not_retry_successful_env_validation_in_docker() {
        let summary = ValidationSummary {
            succeeded: true,
            attempts: vec![ValidationAttempt {
                status: "passed".to_string(),
                validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!should_retry_failed_env_validation_in_docker(
            &summary,
            "scrapy==1.8.3"
        ));
    }

    #[test]
    fn validation_pipeline_detects_archive_cache_source() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let validated_envs_dir = temp.path().join("validated-envs");
        fs::create_dir_all(&validated_envs_dir).unwrap();

        let requirements = "os==0.0.0";
        let build_key = build_cache::key_for(requirements, "3.11");
        let archive_path = validated_env_archive_path(&validated_envs_dir, &build_key);

        // Create an archive file
        fs::write(&archive_path, b"fake archive").unwrap();

        let snippet_path = temp.path().join("snippet.py");
        fs::write(&snippet_path, "import os").unwrap();

        let mut config = ResolveConfig::for_tool_root(temp.path());
        config.output_dir = temp.path().join("debug");
        config.cache_path = temp.path().join("cache");
        config.benchmark_context_log = None;
        fs::create_dir_all(&config.output_dir).unwrap();

        let (_, cache_source, _) = prepare_env_validation_attempt(
            &snippet_path,
            requirements,
            "# smoke test",
            "3.11",
            1,
            &validated_envs_dir,
            &config,
        )
        .unwrap();

        assert!(
            matches!(cache_source, ValidatedEnvCacheSource::Archive(_)),
            "Expected Archive cache source, got {:?}",
            cache_source
        );
    }

    #[test]
    fn validation_pipeline_detects_legacy_env_cache_source() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let validated_envs_dir = temp.path().join("validated-envs");
        fs::create_dir_all(&validated_envs_dir).unwrap();

        let requirements = "os==0.0.0";
        let build_key = build_cache::key_for(requirements, "3.11");
        let legacy_env_dir = validated_env_cache_path(&validated_envs_dir, &build_key);

        // Create a legacy env directory with bin/ subdirectory
        fs::create_dir_all(legacy_env_dir.join("bin")).unwrap();

        let snippet_path = temp.path().join("snippet.py");
        fs::write(&snippet_path, "import os").unwrap();

        let mut config = ResolveConfig::for_tool_root(temp.path());
        config.output_dir = temp.path().join("debug");
        config.cache_path = temp.path().join("cache");
        config.benchmark_context_log = None;
        fs::create_dir_all(&config.output_dir).unwrap();

        let (_, cache_source, _) = prepare_env_validation_attempt(
            &snippet_path,
            requirements,
            "# smoke test",
            "3.11",
            1,
            &validated_envs_dir,
            &config,
        )
        .unwrap();

        assert!(
            matches!(cache_source, ValidatedEnvCacheSource::LegacyDir(_)),
            "Expected LegacyDir cache source, got {:?}",
            cache_source
        );
    }

    #[test]
    fn validation_pipeline_merges_env_history_before_docker_attempts() {
        let mut env_summary = ValidationSummary {
            attempts: vec![
                ValidationAttempt {
                    attempt_index: 1,
                    python_version: "3.11".to_string(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    status: "build-failed".to_string(),
                    ..Default::default()
                },
                ValidationAttempt {
                    attempt_index: 2,
                    python_version: "3.10".to_string(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    status: "runtime-failed".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let mut docker_summary = ValidationSummary {
            attempts: vec![ValidationAttempt {
                attempt_index: 3,
                python_version: "3.11".to_string(),
                validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
                status: "passed".to_string(),
                ..Default::default()
            }],
            succeeded: true,
            ..Default::default()
        };

        merge_backend_retry_history(&mut env_summary, &mut docker_summary);

        assert_eq!(docker_summary.attempts.len(), 3);
        assert_eq!(docker_summary.attempts[0].attempt_index, 1);
        assert_eq!(
            docker_summary.attempts[0].validation_backend,
            VALIDATION_BACKEND_ENV
        );
        assert_eq!(docker_summary.attempts[1].attempt_index, 2);
        assert_eq!(
            docker_summary.attempts[1].validation_backend,
            VALIDATION_BACKEND_ENV
        );
        assert_eq!(docker_summary.attempts[2].attempt_index, 3);
        assert_eq!(
            docker_summary.attempts[2].validation_backend,
            VALIDATION_BACKEND_DOCKER
        );
        assert!(docker_summary.succeeded);
    }

    #[test]
    fn phase17_llm_parse_agent_result_accepts_failed_status() {
        let summary = parse_agent_result(
            r#"{"status":"failed","confidence_reason":"state key crash","attempts":[]}"#,
            Path::new("apdr-agent-output"),
        )
        .expect("failed agent result should remain structured");

        assert!(!summary.succeeded);
        assert_eq!(summary.status, "failed");
        assert_eq!(summary.reason.as_deref(), Some("state key crash"));
        assert_eq!(summary.attempts.len(), 1);
        assert_eq!(summary.attempts[0].status, "failed");
        assert_eq!(
            summary.attempts[0].validation_backend,
            VALIDATION_BACKEND_LLM
        );
    }

    #[test]
    fn phase17_llm_parse_agent_result_accepts_abstained_status() {
        let summary = parse_agent_result(
            r#"{"status":"abstained","confidence_reason":"low confidence","attempts":[]}"#,
            Path::new("apdr-agent-output"),
        )
        .expect("abstained agent result should remain structured");

        assert!(!summary.succeeded);
        assert_eq!(summary.status, "abstained");
        assert_eq!(summary.reason.as_deref(), Some("low confidence"));
        assert_eq!(summary.attempts.len(), 1);
        assert_eq!(summary.attempts[0].status, "abstained");
        assert_eq!(
            summary.attempts[0].validation_backend,
            VALIDATION_BACKEND_LLM
        );
    }

    #[test]
    fn phase17_llm_merge_preserves_env_attempts_before_agent_attempt() {
        let mut env_summary = ValidationSummary {
            attempts: vec![
                ValidationAttempt {
                    attempt_index: 1,
                    python_version: "3.11".to_string(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    status: "build-failed".to_string(),
                    ..Default::default()
                },
                ValidationAttempt {
                    attempt_index: 2,
                    python_version: "3.10".to_string(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    status: "runtime-failed".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let mut agent_summary = parse_agent_result(
            r#"{"status":"failed","confidence_reason":"state key crash","attempts":[]}"#,
            Path::new("apdr-agent-output"),
        )
        .expect("failed agent result should parse");

        merge_llm_retry_history(&mut env_summary, &mut agent_summary);

        assert_eq!(agent_summary.attempts.len(), 3);
        assert_eq!(
            agent_summary.attempts[0].validation_backend,
            VALIDATION_BACKEND_ENV
        );
        assert_eq!(
            agent_summary.attempts[1].validation_backend,
            VALIDATION_BACKEND_ENV
        );
        assert_eq!(
            agent_summary.attempts[2].validation_backend,
            VALIDATION_BACKEND_LLM
        );
        assert_eq!(agent_summary.attempts[2].attempt_index, 3);
    }

    #[test]
    fn validation_pipeline_caches_docker_agent_probe() {
        use std::sync::Mutex;

        let probe_cache = OnceLock::new();
        let probe_calls = Mutex::new(0usize);
        let agent_parent = PathBuf::from("D:/apdr/tools/apdr");

        assert!(docker_agent_importable_with_probe(
            &probe_cache,
            &agent_parent,
            |_| {
                *probe_calls.lock().unwrap() += 1;
                true
            }
        ));
        assert!(docker_agent_importable_with_probe(
            &probe_cache,
            &agent_parent,
            |_| {
                *probe_calls.lock().unwrap() += 1;
                false
            }
        ));
        assert_eq!(*probe_calls.lock().unwrap(), 1);
    }

    #[test]
    fn phase18_backend_interpreter_failure_is_docker_eligible() {
        let summary = ValidationSummary {
            attempts: vec![ValidationAttempt {
                attempt_index: 1,
                python_version: "3.11".to_string(),
                validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                status: "build-failed".to_string(),
                log_excerpt:
                    "No local interpreter found for Python 3.11. Install a matching interpreter."
                        .to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(llm_env_failure_requires_docker_escalation(
            &summary,
            "requests==2.31.0"
        ));
    }

    #[test]
    fn phase18_backend_build_timeout_is_docker_eligible() {
        let summary = ValidationSummary {
            attempts: vec![ValidationAttempt {
                attempt_index: 1,
                python_version: "3.11".to_string(),
                validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                status: "build-timeout".to_string(),
                log_excerpt: "pip install timed out while building wheels".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(llm_env_failure_requires_docker_escalation(
            &summary,
            "scipy==1.10.1"
        ));
    }

    #[test]
    fn phase18_backend_host_runtime_failure_is_not_docker_eligible() {
        let summary = ValidationSummary {
            status: "skipped-host-runtime".to_string(),
            failure_bucket: "skipped-host-runtime".to_string(),
            reason: Some(
                "Detected macOS framework dependency (OpenDirectory/SystemConfiguration). APDR cannot validate this snippet without the macOS host framework runtime."
                    .to_string(),
            ),
            attempts: vec![ValidationAttempt {
                attempt_index: 1,
                python_version: "3.11".to_string(),
                validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                status: "runtime-failed".to_string(),
                log_excerpt: "requires the macOS host framework runtime".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!llm_env_failure_requires_docker_escalation(
            &summary,
            "pyobjc-framework-SystemConfiguration==10.0"
        ));
    }

    #[test]
    fn phase18_backend_llm_path_tries_docker_before_agent() {
        let mut env_summary = ValidationSummary {
            attempts: vec![
                ValidationAttempt {
                    attempt_index: 1,
                    python_version: "3.11".to_string(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    status: "build-failed".to_string(),
                    ..Default::default()
                },
                ValidationAttempt {
                    attempt_index: 2,
                    python_version: "3.10".to_string(),
                    validation_backend: VALIDATION_BACKEND_ENV.to_string(),
                    status: "build-timeout".to_string(),
                    ..Default::default()
                },
            ],
            validation_backend: VALIDATION_BACKEND_LLM.to_string(),
            ..Default::default()
        };

        let mut docker_summary = ValidationSummary {
            attempts: vec![ValidationAttempt {
                attempt_index: 3,
                python_version: "3.11".to_string(),
                validation_backend: VALIDATION_BACKEND_DOCKER.to_string(),
                status: "build-failed".to_string(),
                ..Default::default()
            }],
            validation_backend: VALIDATION_BACKEND_LLM.to_string(),
            escalated_backend: Some(VALIDATION_BACKEND_DOCKER.to_string()),
            ..Default::default()
        };

        merge_backend_retry_history(&mut env_summary, &mut docker_summary);

        let mut agent_summary = parse_agent_result(
            r#"{"status":"failed","confidence_reason":"still unresolved","attempts":[]}"#,
            Path::new("apdr-agent-output"),
        )
        .expect("failed agent result should parse");

        merge_llm_retry_history(&mut docker_summary, &mut agent_summary);

        assert_eq!(agent_summary.attempts.len(), 4);
        assert_eq!(
            agent_summary.attempts[0].validation_backend,
            VALIDATION_BACKEND_ENV
        );
        assert_eq!(
            agent_summary.attempts[1].validation_backend,
            VALIDATION_BACKEND_ENV
        );
        assert_eq!(
            agent_summary.attempts[2].validation_backend,
            VALIDATION_BACKEND_DOCKER
        );
        assert_eq!(
            agent_summary.attempts[3].validation_backend,
            VALIDATION_BACKEND_LLM
        );
    }

    #[test]
    fn phase22_policy_llm_defaults_to_env_first() {
        let config = ResolveConfig::for_tool_root(Path::new("."));

        assert_eq!(
            llm_validation_route(
                &config,
                &[],
                "requests==2.31.0",
                DockerValidationAvailability::Available
            ),
            LlmValidationRoute::EnvFirst
        );
    }

    #[test]
    fn phase22_policy_explicit_docker_first_route_uses_docker_first() {
        let mut config = ResolveConfig::for_tool_root(Path::new("."));
        config.llm_validation_policy = "docker-first".to_string();

        assert_eq!(
            llm_validation_route(
                &config,
                &[],
                "requests==2.31.0",
                DockerValidationAvailability::Available
            ),
            LlmValidationRoute::DockerFirst
        );
    }

    #[test]
    fn phase22_policy_host_runtime_skips_before_docker() {
        let config = ResolveConfig::for_tool_root(Path::new("."));
        let imports = vec!["objc".to_string(), "SystemConfiguration".to_string()];

        assert!(llm_case_requires_host_runtime(
            &imports,
            "pyobjc-framework-SystemConfiguration==10.0"
        ));
        assert_eq!(
            llm_validation_route(
                &config,
                &imports,
                "pyobjc-framework-SystemConfiguration==10.0",
                DockerValidationAvailability::Available
            ),
            LlmValidationRoute::EnvFirstHostRuntime
        );
    }

    #[test]
    fn phase23_truth_host_runtime_route_metadata_marks_preskip() {
        use tempfile::TempDir;

        let temp = TempDir::new().expect("tempdir");
        let mut config = ResolveConfig::for_tool_root(temp.path());
        config.output_dir = temp.path().join("out");
        let imports = vec!["objc".to_string(), "SystemConfiguration".to_string()];
        let route = llm_validation_route(
            &config,
            &imports,
            "pyobjc-framework-SystemConfiguration==10.0",
            DockerValidationAvailability::Available,
        );
        let mut summary = ValidationSummary::default();

        apply_llm_route_metadata(&mut summary, &config, route).expect("route metadata");

        assert_eq!(route, LlmValidationRoute::EnvFirstHostRuntime);
        assert_eq!(
            summary.requested_llm_validation_policy.as_deref(),
            Some("env-first")
        );
        assert_eq!(
            summary.llm_validation_route.as_deref(),
            Some("env-first-host-runtime")
        );
        assert_eq!(
            summary.docker_bypass_reason.as_deref(),
            Some("host-runtime pre-skip")
        );
    }

    #[test]
    fn phase22_policy_docker_bypass_falls_back_to_env() {
        let config = ResolveConfig::for_tool_root(Path::new("."));

        assert_eq!(
            llm_validation_route(
                &config,
                &[],
                "requests==2.31.0",
                DockerValidationAvailability::Unavailable(
                    DockerUnavailabilityReason::CliUnavailable
                )
            ),
            LlmValidationRoute::EnvFirstDockerBypass(DockerUnavailabilityReason::CliUnavailable)
        );
    }

    #[test]
    fn phase22_policy_daemon_unavailable_falls_back_to_env() {
        let config = ResolveConfig::for_tool_root(Path::new("."));

        assert_eq!(
            llm_validation_route(
                &config,
                &[],
                "requests==2.31.0",
                DockerValidationAvailability::Unavailable(
                    DockerUnavailabilityReason::DaemonUnavailable
                )
            ),
            LlmValidationRoute::EnvFirstDockerBypass(DockerUnavailabilityReason::DaemonUnavailable)
        );
    }

    #[test]
    fn phase23_truth_daemon_unavailable_route_writes_bypass_metadata() {
        use tempfile::TempDir;

        let temp = TempDir::new().expect("tempdir");
        let mut config = ResolveConfig::for_tool_root(temp.path());
        config.output_dir = temp.path().join("out");
        let mut summary = ValidationSummary::default();

        apply_llm_route_metadata(
            &mut summary,
            &config,
            LlmValidationRoute::EnvFirstDockerBypass(DockerUnavailabilityReason::DaemonUnavailable),
        )
        .expect("route metadata");

        assert_eq!(
            summary.requested_llm_validation_policy.as_deref(),
            Some("env-first")
        );
        assert_eq!(
            summary.llm_validation_route.as_deref(),
            Some("env-first-docker-bypass")
        );
        assert_eq!(
            summary.docker_bypass_reason.as_deref(),
            Some("docker daemon unavailable")
        );
    }

    #[test]
    fn phase22_policy_non_docker_routes_write_bypass_note() {
        use tempfile::TempDir;

        let temp = TempDir::new().expect("tempdir");
        let mut config = ResolveConfig::for_tool_root(temp.path());
        config.output_dir = temp.path().join("out");
        let mut summary = ValidationSummary::default();

        apply_llm_route_metadata(
            &mut summary,
            &config,
            LlmValidationRoute::EnvFirstDockerBypass(DockerUnavailabilityReason::CliUnavailable),
        )
        .expect("route metadata");

        let note_path = temp
            .path()
            .join("out")
            .join(".apdr-debug")
            .join("docker-bypass.txt");
        let note_path_string = note_path.display().to_string();
        let note = fs::read_to_string(&note_path).expect("docker bypass note");
        assert_eq!(
            summary.requested_llm_validation_policy.as_deref(),
            Some("env-first")
        );
        assert_eq!(
            summary.llm_validation_route.as_deref(),
            Some("env-first-docker-bypass")
        );
        assert_eq!(
            summary.docker_bypass_reason.as_deref(),
            Some("docker cli unavailable")
        );
        assert_eq!(
            summary.docker_bypass_note_path.as_deref(),
            Some(note_path_string.as_str())
        );
        assert!(note.contains("note_type: docker-bypass"));
        assert!(note.contains("requested_llm_validation_policy: env-first"));
        assert!(note.contains("llm_validation_route: env-first-docker-bypass"));
        assert!(note.contains("docker_bypass_reason: docker cli unavailable"));
    }

    #[test]
    fn phase22_policy_daemon_unavailable_writes_bypass_note() {
        use tempfile::TempDir;

        let temp = TempDir::new().expect("tempdir");
        let mut config = ResolveConfig::for_tool_root(temp.path());
        config.output_dir = temp.path().join("out");
        let mut summary = ValidationSummary::default();

        apply_llm_route_metadata(
            &mut summary,
            &config,
            LlmValidationRoute::EnvFirstDockerBypass(DockerUnavailabilityReason::DaemonUnavailable),
        )
        .expect("route metadata");

        let note_path = temp
            .path()
            .join("out")
            .join(".apdr-debug")
            .join("docker-bypass.txt");
        let note = fs::read_to_string(&note_path).expect("docker bypass note");
        assert_eq!(
            summary.docker_bypass_reason.as_deref(),
            Some("docker daemon unavailable")
        );
        assert!(note.contains("docker_bypass_reason: docker daemon unavailable"));
    }

    #[test]
    fn phase22_policy_docker_first_route_skips_bypass_note() {
        use tempfile::TempDir;

        let temp = TempDir::new().expect("tempdir");
        let mut config = ResolveConfig::for_tool_root(temp.path());
        config.llm_validation_policy = "docker-first".to_string();
        config.output_dir = temp.path().join("out");
        let mut summary = ValidationSummary::default();

        apply_llm_route_metadata(&mut summary, &config, LlmValidationRoute::DockerFirst)
            .expect("route metadata");

        assert_eq!(
            summary.requested_llm_validation_policy.as_deref(),
            Some("docker-first")
        );
        assert_eq!(
            summary.llm_validation_route.as_deref(),
            Some("docker-first")
        );
        assert!(summary.docker_bypass_reason.is_none());
        assert!(summary.docker_bypass_note_path.is_none());
        assert!(!temp
            .path()
            .join("out")
            .join(".apdr-debug")
            .join("docker-bypass.txt")
            .exists());
    }

    #[test]
    fn llm_only_docker_validation_skips_agent_first_hop() {
        let mut config = ResolveConfig::for_tool_root(Path::new("."));
        config.validation_backend = VALIDATION_BACKEND_DOCKER.to_string();
        config.allow_llm = true;
        config.llm_only_mode = true;

        assert!(!should_attempt_docker_agent_fallback(&config));
    }

    #[test]
    fn regular_docker_validation_can_still_attempt_agent_fallback() {
        let mut config = ResolveConfig::for_tool_root(Path::new("."));
        config.validation_backend = VALIDATION_BACKEND_DOCKER.to_string();
        config.allow_llm = true;
        config.llm_only_mode = false;

        assert!(should_attempt_docker_agent_fallback(&config));
    }

    #[test]
    fn phase27_handoff_verification_candidates_include_iid_and_registry_variant() {
        let candidates = candidate_image_refs_for_verification(
            "apdr-validate:py3_11-deadbeef",
            Some("sha256:1234abcd"),
        );

        assert_eq!(candidates[0], "apdr-validate:py3_11-deadbeef");
        assert!(candidates
            .iter()
            .any(|item| item == "docker.io/library/apdr-validate:py3_11-deadbeef"));
        assert!(candidates.iter().any(|item| item == "sha256:1234abcd"));
    }
}
