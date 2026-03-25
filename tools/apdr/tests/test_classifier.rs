use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn classifier_recognizes_module_not_found() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let store =
        apdr::cache::store::CacheStore::load(&tool_root, tool_root.join(".apdr-cache")).unwrap();
    let log = "ModuleNotFoundError: No module named 'requests'";
    let result = apdr::recovery::classifier::classify_log(log, &store);

    assert_eq!(result.error_type, "ModuleNotFound");
    assert_eq!(result.conflict_class, "TPL-TPL");
}

#[test]
fn classifier_prefers_high_success_dynamic_patterns() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let cache_path = tool_root
        .join("target")
        .join(format!("classifier-cache-{stamp}"));
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .record_failure_pattern_outcome(
            "No matching distribution found",
            "VersionNotFound",
            "TPL-TPL",
            "Use the cached Python-2-compatible release window.",
            true,
        )
        .unwrap();
    store
        .record_failure_pattern_outcome(
            "No matching distribution found",
            "VersionNotFound",
            "TPL-TPL",
            "Use the cached Python-2-compatible release window.",
            true,
        )
        .unwrap();

    let result = apdr::recovery::classifier::classify_log(
        "ERROR: No matching distribution found for scrapy==2.11.0",
        &store,
    );

    assert_eq!(
        result.recommended_fix,
        "Use the cached Python-2-compatible release window."
    );
    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn classifier_recognizes_build_backend_unavailable() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let store =
        apdr::cache::store::CacheStore::load(&tool_root, tool_root.join(".apdr-cache")).unwrap();
    let log = "pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'setuptools.build_meta'";
    let result = apdr::recovery::classifier::classify_log(log, &store);

    assert_eq!(result.error_type, "BuildBackendUnavailable");
    assert_eq!(result.conflict_class, "TPL-OS");
}

#[test]
fn classifier_prefers_specific_network_pattern_over_generic_version_not_found() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let cache_path = tool_root
        .join("target")
        .join(format!("classifier-net-cache-{stamp}"));
    let store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    let log = "WARNING: Failed to establish a new connection\nERROR: No matching distribution found for opencv-python==4.10.0.84";
    let result = apdr::recovery::classifier::classify_log(log, &store);

    assert_eq!(result.error_type, "NetworkUnavailable");
    assert_eq!(result.conflict_class, "TPL-OS");
    let _ = std::fs::remove_dir_all(cache_path);
}

#[test]
fn classifier_recognizes_disk_full() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let store =
        apdr::cache::store::CacheStore::load(&tool_root, tool_root.join(".apdr-cache")).unwrap();
    let log = "OSError: [Errno 28] No space left on device";
    let result = apdr::recovery::classifier::classify_log(log, &store);

    assert_eq!(result.error_type, "DiskFull");
    assert_eq!(result.conflict_class, "TPL-OS");
}

#[test]
fn classifier_recognizes_msvc_build_failure() {
    // Case 00056d4: lxml build failure due to missing MSVC on Py2.7 Windows
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let store =
        apdr::cache::store::CacheStore::load(&tool_root, tool_root.join(".apdr-cache")).unwrap();
    let log = "building 'lxml.etree' extension\n\
               error: Microsoft Visual C++ 9.0 is required.\n\
               Failed to build lxml\n\
               ERROR: Command errored out with exit status 1";
    let result = apdr::recovery::classifier::classify_log(log, &store);

    assert_eq!(result.error_type, "BuildFailure");
}
