pub fn classify_from_error_type(error_type: &str) -> String {
    match error_type {
        "SyntaxError" => "TPL-Python".to_string(),
        "VersionNotFound" | "DependencyConflict" | "ImportError" | "ModuleNotFound"
        | "AttributeError" | "InvalidVersion" => "TPL-TPL".to_string(),
        "DockerPermissionDenied" | "DockerDaemonUnavailable" | "BuildFailure" => "TPL-OS".to_string(),
        "CudaError" => "TPL-CUDA".to_string(),
        "CpuError" => "TPL-CPU".to_string(),
        "CompilerError" => "TPL-GCC".to_string(),
        _ => "TPL-OS".to_string(),
    }
}
