use std::cmp::Ordering;

use crate::cache::store::CacheStore;
use crate::recovery::conflict_taxonomy;
use crate::recovery::patterns;
use crate::ClassifierResult;

pub fn classify_log(log: &str, store: &CacheStore) -> ClassifierResult {
    let mut known_patterns = patterns::built_in_patterns();
    known_patterns.extend(store.failure_patterns.clone());
    known_patterns.sort_by(|left, right| {
        right
            .success_rate
            .partial_cmp(&left.success_rate)
            .unwrap_or(Ordering::Equal)
            .then(right.times_applied.cmp(&left.times_applied))
            .then(right.pattern.len().cmp(&left.pattern.len()))
    });
    let lowercase = log.to_lowercase();

    for pattern in known_patterns {
        if lowercase.contains(&pattern.pattern.to_lowercase()) {
            return ClassifierResult {
                error_type: pattern.error_type,
                conflict_class: pattern.conflict_class,
                matched_pattern: pattern.pattern,
                recommended_fix: pattern.fix,
            };
        }
    }

    let inferred_type = if lowercase.contains("double requirement given") {
        "DependencyConflict"
    } else if lowercase.contains("attributeerror") {
        "AttributeError"
    } else if lowercase.contains("invalid version") {
        "InvalidVersion"
    } else if lowercase.contains("non-zero exit status") {
        "NonZeroCode"
    } else if lowercase.contains("command not found")
        || lowercase.contains("cannot find -l")
        || lowercase.contains("no such file or directory")
            && (lowercase.contains(".so")
                || lowercase.contains(".dylib")
                || lowercase.contains(".h"))
        || lowercase.contains("pkg-config")
    {
        "SystemDependency"
    } else if lowercase.contains("failed building wheel")
        || lowercase.contains("error: subprocess-exited-with-error")
        || lowercase.contains("command errored out")
    {
        "BuildFailure"
    } else if lowercase.contains("improperlyconfigured")
        || lowercase.contains("django_settings_module")
    {
        "RuntimeConfig"
    } else {
        "Unknown"
    };

    ClassifierResult {
        error_type: inferred_type.to_string(),
        conflict_class: conflict_taxonomy::classify_from_error_type(inferred_type),
        matched_pattern: "no-known-pattern".to_string(),
        recommended_fix: "Escalate to isolated per-error recovery logic.".to_string(),
    }
}
