use std::fs;
#[cfg(unix)]
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use filetime::{set_file_mtime, FileTime};

fn unique_cache_dir(tool_root: &Path, label: &str) -> PathBuf {
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
fn disk_usage_reports_heavy_cache_directories() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "cache-disk-usage");

    fs::create_dir_all(cache_path.join("validated-envs").join("entry-a")).unwrap();
    fs::create_dir_all(cache_path.join("wheelhouse")).unwrap();
    fs::create_dir_all(cache_path.join("pip-cache")).unwrap();
    fs::create_dir_all(cache_path.join("package-repository")).unwrap();
    fs::create_dir_all(cache_path.join("lockfiles")).unwrap();
    fs::write(
        cache_path
            .join("validated-envs")
            .join("entry-a")
            .join("payload.bin"),
        vec![0u8; 17],
    )
    .unwrap();
    fs::write(
        cache_path.join("wheelhouse").join("wheel.bin"),
        vec![0u8; 23],
    )
    .unwrap();
    fs::write(cache_path.join("pip-cache").join("http.bin"), vec![0u8; 29]).unwrap();
    fs::write(
        cache_path.join("package-repository").join("pkg.bin"),
        vec![0u8; 31],
    )
    .unwrap();
    fs::write(
        cache_path.join("lockfiles").join("sample.txt"),
        vec![0u8; 7],
    )
    .unwrap();
    fs::write(cache_path.join("smtpip-kgraph.sqlite3"), vec![0u8; 11]).unwrap();
    fs::write(cache_path.join("dynamic_imports.tsv"), vec![0u8; 5]).unwrap();

    let usage = apdr::cache::maintenance::disk_usage(&cache_path).unwrap();
    assert_eq!(usage.validated_env_entries, 1);
    assert_eq!(usage.validated_envs_bytes, 17);
    assert_eq!(usage.wheelhouse_bytes, 23);
    assert_eq!(usage.legacy_pip_cache_bytes, 29);
    assert_eq!(usage.package_repository_bytes, 31);
    assert_eq!(usage.lockfiles_bytes, 7);
    assert_eq!(usage.sqlite_bytes, 11);
    assert_eq!(usage.other_bytes, 5);
    assert_eq!(usage.total_bytes, 123);

    fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn prune_cache_removes_legacy_dirs_and_old_validated_envs() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "cache-prune");
    let envs_dir = cache_path.join("validated-envs");
    fs::create_dir_all(&envs_dir).unwrap();

    let old_env = envs_dir.join("build-old");
    let new_env = envs_dir.join("build-new");
    fs::create_dir_all(&old_env).unwrap();
    fs::create_dir_all(&new_env).unwrap();
    fs::write(old_env.join("payload.bin"), vec![0u8; 40]).unwrap();
    fs::write(new_env.join("payload.bin"), vec![0u8; 50]).unwrap();
    fs::write(
        old_env.join(apdr::cache::maintenance::VALIDATED_ENV_LAST_USED_MARKER),
        "100",
    )
    .unwrap();
    fs::write(
        new_env.join(apdr::cache::maintenance::VALIDATED_ENV_LAST_USED_MARKER),
        "200",
    )
    .unwrap();
    fs::create_dir_all(cache_path.join("package-repository")).unwrap();
    fs::write(
        cache_path.join("package-repository").join("pkg.bin"),
        vec![0u8; 31],
    )
    .unwrap();
    fs::create_dir_all(cache_path.join("pip-cache")).unwrap();
    fs::write(cache_path.join("pip-cache").join("pip.bin"), vec![0u8; 29]).unwrap();

    let summary = apdr::cache::maintenance::prune_cache(
        &cache_path,
        &apdr::cache::maintenance::CachePruneOptions {
            max_validated_envs: 1,
            max_validated_env_bytes: Some(60),
            max_wheelhouse_bytes: None,
            remove_package_repository: true,
            remove_legacy_pip_cache: true,
        },
    )
    .unwrap();

    assert_eq!(summary.removed_validated_envs, 1);
    assert!(summary.removed_package_repository);
    assert!(summary.removed_legacy_pip_cache);
    assert!(!old_env.exists());
    assert!(new_env.exists());
    assert!(!cache_path.join("package-repository").exists());
    assert!(!cache_path.join("pip-cache").exists());

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

#[test]
fn version_index_load_merges_seed_and_dynamic_entries() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "cache-version-merge");
    fs::create_dir_all(&cache_path).unwrap();
    fs::write(
        cache_path.join("dynamic_pypi_index.tsv"),
        "pillow\t10.4.0,11.0.0\n",
    )
    .unwrap();

    let store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    let versions = store.pypi_index.get("pillow").unwrap();

    assert!(versions.iter().any(|item| item == "6.2.2"));
    assert!(versions.iter().any(|item| item == "10.4.0"));
    assert!(versions.iter().any(|item| item == "11.0.0"));

    fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn compress_and_extract_roundtrip() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let base = unique_cache_dir(&tool_root, "compress-roundtrip");
    let src = base.join("src-env");
    let archive = base.join("env.tar.zst");
    let dst = base.join("dst-env");

    // Create a venv-like directory structure
    fs::create_dir_all(src.join("bin")).unwrap();
    fs::write(src.join("bin").join("python"), b"#!/usr/bin/env python\n").unwrap();
    fs::create_dir_all(src.join("lib").join("python3.10").join("site-packages")).unwrap();
    fs::write(
        src.join("lib")
            .join("python3.10")
            .join("site-packages")
            .join("hello.py"),
        b"print('hello')\n",
    )
    .unwrap();
    #[cfg(unix)]
    unix_fs::symlink("python", src.join("bin").join("python3")).unwrap();

    // Compress
    let size = apdr::cache::maintenance::compress_env_to_archive(&src, &archive).unwrap();
    assert!(size > 0);
    assert!(archive.exists());

    // Extract
    apdr::cache::maintenance::extract_archive_to_env(&archive, &dst).unwrap();

    // Verify files
    assert!(dst.join("bin").join("python").exists());
    let content = fs::read_to_string(
        dst.join("lib")
            .join("python3.10")
            .join("site-packages")
            .join("hello.py"),
    )
    .unwrap();
    assert_eq!(content, "print('hello')\n");

    // Verify symlink preserved (Unix only)
    #[cfg(unix)]
    {
        let link = dst.join("bin").join("python3");
        assert!(link.exists());
        let target = fs::read_link(&link).unwrap();
        assert_eq!(target.to_str().unwrap(), "python");
    }

    fs::remove_dir_all(base).unwrap();
}

#[test]
fn prune_handles_mixed_archive_and_dir_entries() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "prune-mixed");
    let envs_dir = cache_path.join("validated-envs");
    fs::create_dir_all(&envs_dir).unwrap();

    // Create a legacy directory entry (old, last_used=100)
    let old_dir = envs_dir.join("build-old-dir");
    fs::create_dir_all(&old_dir).unwrap();
    fs::write(old_dir.join("payload.bin"), vec![0u8; 40]).unwrap();
    fs::write(
        old_dir.join(apdr::cache::maintenance::VALIDATED_ENV_LAST_USED_MARKER),
        "100",
    )
    .unwrap();

    // Create a compressed archive entry (newer, last_used=200)
    let archive_src = cache_path.join("tmp-env");
    fs::create_dir_all(archive_src.join("bin")).unwrap();
    fs::write(archive_src.join("bin").join("python"), b"python").unwrap();
    let archive_path = envs_dir.join("build-new-archive.tar.zst");
    apdr::cache::maintenance::compress_env_to_archive(&archive_src, &archive_path).unwrap();
    fs::write(envs_dir.join("build-new-archive.tar.zst.last-used"), "200").unwrap();

    // Prune to max 1 entry
    let summary = apdr::cache::maintenance::prune_validated_env_cache(&envs_dir, 1, None).unwrap();
    assert_eq!(summary.removed_validated_envs, 1);
    // Old directory entry should be removed
    assert!(!old_dir.exists());
    // Newer archive entry should be kept
    assert!(archive_path.exists());

    fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn wheelhouse_prune_removes_oldest_files() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "wheelhouse-prune");
    let wheelhouse = cache_path.join("wheelhouse");
    fs::create_dir_all(&wheelhouse).unwrap();

    let old_wheel = wheelhouse.join("old.whl");
    let new_wheel = wheelhouse.join("new.whl");

    // Create files with different sizes
    fs::write(&old_wheel, vec![0u8; 100]).unwrap();
    fs::write(&new_wheel, vec![0u8; 50]).unwrap();

    // Force a real oldest/newest split so the test checks prune order rather
    // than depending on whatever timestamp resolution the host filesystem uses.
    set_file_mtime(&old_wheel, FileTime::from_unix_time(1_700_000_000, 0)).unwrap();
    set_file_mtime(&new_wheel, FileTime::from_unix_time(1_700_000_100, 0)).unwrap();

    // Prune to 60 bytes max (should remove old.whl to get under limit)
    let removed = apdr::cache::maintenance::prune_wheelhouse(&wheelhouse, Some(60)).unwrap();
    assert_eq!(removed, 100);
    assert!(!old_wheel.exists());
    assert!(new_wheel.exists());

    fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn disk_usage_counts_archive_entries() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = unique_cache_dir(&tool_root, "archive-disk-usage");
    let envs_dir = cache_path.join("validated-envs");
    fs::create_dir_all(&envs_dir).unwrap();

    // Create one legacy dir entry
    let dir_entry = envs_dir.join("entry-dir");
    fs::create_dir_all(&dir_entry).unwrap();
    fs::write(dir_entry.join("data.bin"), vec![0u8; 10]).unwrap();

    // Create one archive entry
    let archive_src = cache_path.join("tmp-env");
    fs::create_dir_all(archive_src.join("bin")).unwrap();
    fs::write(archive_src.join("bin").join("python"), b"py").unwrap();
    let archive_path = envs_dir.join("entry-archive.tar.zst");
    apdr::cache::maintenance::compress_env_to_archive(&archive_src, &archive_path).unwrap();

    let usage = apdr::cache::maintenance::disk_usage(&cache_path).unwrap();
    assert_eq!(usage.validated_env_entries, 2);

    fs::remove_dir_all(cache_path).unwrap();
}
