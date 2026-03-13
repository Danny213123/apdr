use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_cache_dir(tool_root: &PathBuf, label: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    tool_root.join("target").join(format!("{label}-{stamp}"))
}

#[test]
fn cache_persists_dynamic_imports_and_failure_pattern_stats() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "cache-persist");

    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_import_mapping("custom_pkg", "custom-package", Some("1.2.3"), "test")
        .unwrap();
    store
        .record_failure_pattern_outcome(
            "No matching distribution found",
            "VersionNotFound",
            "TPL-TPL",
            "Pin to the newest compatible version.",
            true,
        )
        .unwrap();
    store
        .record_failure_pattern_outcome(
            "No matching distribution found",
            "VersionNotFound",
            "TPL-TPL",
            "Pin to the newest compatible version.",
            false,
        )
        .unwrap();

    let reloaded = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    let record = reloaded.import_lookup("custom_pkg").unwrap();
    assert_eq!(record.package_name, "custom-package");
    assert_eq!(record.default_version.as_deref(), Some("1.2.3"));

    let learned = reloaded
        .failure_patterns
        .iter()
        .find(|pattern| pattern.fix == "Pin to the newest compatible version.")
        .unwrap();
    assert_eq!(learned.times_applied, 2);
    assert!((learned.success_rate - 0.5).abs() < 0.01);

    fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn weak_fuzzy_mapping_does_not_override_seed_mapping() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "cache-precedence");

    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_import_mapping("scrapy", "scipy", None, "heuristic:fuzzy")
        .unwrap();

    let record = store.import_lookup("scrapy").unwrap();
    assert_eq!(record.package_name, "scrapy");
    assert_eq!(record.source, "seed");

    fs::remove_dir_all(cache_path).unwrap();
}
