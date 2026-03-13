use crate::FailurePattern;

pub fn built_in_patterns() -> Vec<FailurePattern> {
    vec![
        FailurePattern {
            pattern: "No matching distribution found".to_string(),
            error_type: "VersionNotFound".to_string(),
            conflict_class: "TPL-TPL".to_string(),
            fix: "Try an adjacent compatible version from the cached version index.".to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "Cannot install".to_string(),
            error_type: "DependencyConflict".to_string(),
            conflict_class: "TPL-TPL".to_string(),
            fix: "Relax or realign version constraints across the conflicting packages."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "ResolutionImpossible".to_string(),
            error_type: "DependencyConflict".to_string(),
            conflict_class: "TPL-TPL".to_string(),
            fix: "Rebuild the requirement set around a coherent package family/version bundle."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "requires a different python version".to_string(),
            error_type: "PythonVersionMismatch".to_string(),
            conflict_class: "TPL-Python".to_string(),
            fix: "Choose a package version family that matches the attempted Python version."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "ModuleNotFoundError".to_string(),
            error_type: "ModuleNotFound".to_string(),
            conflict_class: "TPL-TPL".to_string(),
            fix: "Add the missing package and re-check import-to-package mappings.".to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "ImportError".to_string(),
            error_type: "ImportError".to_string(),
            conflict_class: "TPL-TPL".to_string(),
            fix: "Pin the dependency to an API-compatible version.".to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "SyntaxError".to_string(),
            error_type: "SyntaxError".to_string(),
            conflict_class: "TPL-Python".to_string(),
            fix: "Retry on an adjacent Python version.".to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "permission denied while trying to connect to the docker api".to_string(),
            error_type: "DockerPermissionDenied".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Start Docker Desktop or grant the current user access to the Docker socket."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "cannot connect to the docker daemon".to_string(),
            error_type: "DockerDaemonUnavailable".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Start Docker Desktop before running validation.".to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "could not build wheels".to_string(),
            error_type: "BuildFailure".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Add the missing system headers/toolchain or choose a version with prebuilt wheels."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "libxml2 and libxslt development packages are installed".to_string(),
            error_type: "BuildFailure".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Install libxml2-dev and libxslt1-dev before building lxml-backed packages such as scrapy."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "python.h: no such file or directory".to_string(),
            error_type: "BuildFailure".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Install Python development headers in the validation image.".to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "Cannot import 'setuptools.build_meta'".to_string(),
            error_type: "BuildBackendUnavailable".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Preinstall setuptools and wheel in the validation image before resolving source builds."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "No local interpreter found for Python".to_string(),
            error_type: "PythonInterpreterUnavailable".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Install a matching local Python interpreter or limit APDR to locally available versions."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
        FailurePattern {
            pattern: "Failed to establish a new connection".to_string(),
            error_type: "NetworkUnavailable".to_string(),
            conflict_class: "TPL-OS".to_string(),
            fix: "Check network access to the Python package index or prewarm the local pip cache."
                .to_string(),
            success_rate: 1.0,
            times_applied: 1,
        },
    ]
}
