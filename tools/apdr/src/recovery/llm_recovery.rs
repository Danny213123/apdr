pub fn prompt_for_error(error_type: &str, log_excerpt: &str, requirements_txt: &str) -> String {
    format!(
        "System: A build error occurred.\nError type: {error_type}\nBuild log: {log_excerpt}\nCurrent requirements: {requirements_txt}\nRespond with a single JSON fix suggestion."
    )
}
