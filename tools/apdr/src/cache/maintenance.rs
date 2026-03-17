use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_MAX_VALIDATED_ENVS: usize = 15;
pub const DEFAULT_MAX_VALIDATED_ENV_BYTES: u64 = 1536 * 1024 * 1024; // 1.5 GiB
pub const DEFAULT_MAX_WHEELHOUSE_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB
pub const VALIDATED_ENV_LAST_USED_MARKER: &str = ".apdr-last-used";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheDiskUsage {
    pub total_bytes: u64,
    pub validated_envs_bytes: u64,
    pub validated_env_entries: usize,
    pub wheelhouse_bytes: u64,
    pub legacy_pip_cache_bytes: u64,
    pub package_repository_bytes: u64,
    pub lockfiles_bytes: u64,
    pub sqlite_bytes: u64,
    pub other_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CachePruneOptions {
    pub max_validated_envs: usize,
    pub max_validated_env_bytes: Option<u64>,
    pub max_wheelhouse_bytes: Option<u64>,
    pub remove_package_repository: bool,
    pub remove_legacy_pip_cache: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CachePruneSummary {
    pub removed_bytes: u64,
    pub removed_validated_envs: usize,
    pub removed_wheelhouse_bytes: u64,
    pub removed_package_repository: bool,
    pub removed_legacy_pip_cache: bool,
}

#[derive(Clone, Debug)]
struct ValidatedEnvEntry {
    path: PathBuf,
    bytes: u64,
    last_used: u128,
    is_archive: bool,
}

// ---------------------------------------------------------------------------
// Archive compression / extraction
// ---------------------------------------------------------------------------

/// Compress a directory into a tar.zst archive.
/// Uses atomic write (temp file + rename) for safe concurrent access.
pub fn compress_env_to_archive(env_dir: &Path, archive_path: &Path) -> io::Result<u64> {
    let tmp_path = archive_path.with_extension("tar.zst.tmp");
    let file = fs::File::create(&tmp_path)?;
    let encoder = zstd::Encoder::new(file, 6)?; // level 6: ~30% better ratio than 3, <1s extra
    let mut tar_builder = tar::Builder::new(encoder.auto_finish());
    tar_builder.follow_symlinks(false);
    tar_builder.append_dir_all(".", env_dir)?;
    tar_builder.finish()?;
    drop(tar_builder);
    let size = fs::metadata(&tmp_path)?.len();
    fs::rename(&tmp_path, archive_path)?; // atomic on same filesystem
    Ok(size)
}

/// Extract a tar.zst archive to a destination directory.
pub fn extract_archive_to_env(archive_path: &Path, dest_dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dest_dir)?;
    let file = fs::File::open(archive_path)?;
    let decoder = zstd::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    archive.set_preserve_permissions(true);
    archive.unpack(dest_dir)?;
    Ok(())
}

/// Touch the LRU marker for a compressed archive (sibling .last-used file).
pub fn touch_archive_marker(archive_path: &Path) -> io::Result<()> {
    if !archive_path.exists() {
        return Ok(());
    }
    let marker = archive_marker_path(archive_path);
    fs::write(marker, now_millis().to_string())
}

fn archive_marker_path(archive_path: &Path) -> PathBuf {
    let mut name = archive_path
        .file_name()
        .unwrap_or_default()
        .to_os_string();
    name.push(".last-used");
    archive_path.with_file_name(name)
}

fn read_archive_last_used(archive_path: &Path) -> u128 {
    let marker = archive_marker_path(archive_path);
    if let Ok(contents) = fs::read_to_string(&marker) {
        if let Ok(stamp) = contents.trim().parse::<u128>() {
            return stamp;
        }
    }
    fs::metadata(archive_path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(system_time_to_millis)
        .unwrap_or(0)
}

fn is_env_archive(path: &Path) -> bool {
    path.to_string_lossy().ends_with(".tar.zst")
}

// ---------------------------------------------------------------------------
// Disk usage
// ---------------------------------------------------------------------------

pub fn disk_usage(cache_path: &Path) -> io::Result<CacheDiskUsage> {
    let mut usage = CacheDiskUsage::default();
    if !cache_path.exists() {
        return Ok(usage);
    }

    for entry in fs::read_dir(cache_path)? {
        let entry = entry?;
        let path = entry.path();
        let bytes = path_size(&path)?;
        usage.total_bytes += bytes;

        match entry.file_name().to_string_lossy().as_ref() {
            "validated-envs" => {
                usage.validated_envs_bytes = bytes;
                usage.validated_env_entries = validated_env_entries(&path)?;
            }
            "wheelhouse" => usage.wheelhouse_bytes = bytes,
            "pip-cache" => usage.legacy_pip_cache_bytes = bytes,
            "package-repository" => usage.package_repository_bytes = bytes,
            "lockfiles" => usage.lockfiles_bytes = bytes,
            "smtpip-kgraph.sqlite3" => usage.sqlite_bytes = bytes,
            _ => usage.other_bytes += bytes,
        }
    }

    Ok(usage)
}

// ---------------------------------------------------------------------------
// Pruning
// ---------------------------------------------------------------------------

pub fn prune_cache(
    cache_path: &Path,
    options: &CachePruneOptions,
) -> io::Result<CachePruneSummary> {
    let mut summary = CachePruneSummary::default();

    if options.remove_package_repository {
        summary.removed_bytes += remove_path_if_exists(&cache_path.join("package-repository"))?;
        summary.removed_package_repository = true;
    }

    if options.remove_legacy_pip_cache {
        summary.removed_bytes += remove_path_if_exists(&cache_path.join("pip-cache"))?;
        summary.removed_legacy_pip_cache = true;
    }

    let envs_dir = cache_path.join("validated-envs");
    let env_summary = prune_validated_env_cache(
        &envs_dir,
        options.max_validated_envs,
        options.max_validated_env_bytes,
    )?;
    summary.removed_bytes += env_summary.removed_bytes;
    summary.removed_validated_envs = env_summary.removed_validated_envs;

    let wheelhouse_removed = prune_wheelhouse(
        &cache_path.join("wheelhouse"),
        options.max_wheelhouse_bytes,
    )?;
    summary.removed_wheelhouse_bytes = wheelhouse_removed;
    summary.removed_bytes += wheelhouse_removed;

    Ok(summary)
}

pub fn prune_validated_env_cache(
    validated_envs_dir: &Path,
    max_entries: usize,
    max_bytes: Option<u64>,
) -> io::Result<CachePruneSummary> {
    let mut summary = CachePruneSummary::default();
    if !validated_envs_dir.exists() {
        return Ok(summary);
    }

    let mut entries = validated_env_cache_entries(validated_envs_dir)?;
    if entries.is_empty() {
        return Ok(summary);
    }

    let mut total_bytes = entries.iter().map(|entry| entry.bytes).sum::<u64>();
    entries.sort_by(|left, right| right.last_used.cmp(&left.last_used));
    let byte_limit = max_bytes.unwrap_or(u64::MAX);
    let mut keep = entries.len();

    while keep > max_entries || total_bytes > byte_limit {
        let Some(entry) = entries.get(keep.saturating_sub(1)) else {
            break;
        };
        total_bytes = total_bytes.saturating_sub(entry.bytes);
        keep = keep.saturating_sub(1);
    }

    for entry in entries.into_iter().skip(keep) {
        if entry.is_archive {
            let _ = fs::remove_file(archive_marker_path(&entry.path));
            fs::remove_file(&entry.path)?;
        } else {
            fs::remove_dir_all(&entry.path)?;
        }
        summary.removed_validated_envs += 1;
        summary.removed_bytes += entry.bytes;
    }

    Ok(summary)
}

pub fn prune_wheelhouse(
    wheelhouse_dir: &Path,
    max_bytes: Option<u64>,
) -> io::Result<u64> {
    let Some(max_bytes) = max_bytes else {
        return Ok(0);
    };
    if !wheelhouse_dir.exists() {
        return Ok(0);
    }

    let mut files: Vec<(PathBuf, u64, u128)> = Vec::new();
    collect_files_recursive(wheelhouse_dir, &mut files)?;
    let total_bytes: u64 = files.iter().map(|(_, size, _)| *size).sum();
    if total_bytes <= max_bytes {
        return Ok(0);
    }

    // Sort oldest-modified first
    files.sort_by_key(|(_, _, mtime)| *mtime);

    let mut removed = 0u64;
    let mut current_total = total_bytes;
    for (path, size, _) in &files {
        if current_total <= max_bytes {
            break;
        }
        if fs::remove_file(path).is_ok() {
            removed += *size;
            current_total -= *size;
        }
    }
    Ok(removed)
}

fn collect_files_recursive(dir: &Path, files: &mut Vec<(PathBuf, u64, u128)>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, files)?;
        } else if path.is_file() {
            let metadata = fs::metadata(&path)?;
            let mtime = metadata
                .modified()
                .ok()
                .and_then(system_time_to_millis)
                .unwrap_or(0);
            files.push((path, metadata.len(), mtime));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// LRU markers
// ---------------------------------------------------------------------------

pub fn touch_validated_env_cache_entry(entry_dir: &Path) -> io::Result<()> {
    if !entry_dir.exists() {
        return Ok(());
    }
    fs::write(
        entry_dir.join(VALIDATED_ENV_LAST_USED_MARKER),
        now_millis().to_string(),
    )
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes}B");
    }

    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1}{}", UNITS[unit])
}

fn validated_env_entries(validated_envs_dir: &Path) -> io::Result<usize> {
    let mut count = 0usize;
    for entry in fs::read_dir(validated_envs_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            count += 1;
        } else if is_env_archive(&path) {
            count += 1;
        }
    }
    Ok(count)
}

fn validated_env_cache_entries(validated_envs_dir: &Path) -> io::Result<Vec<ValidatedEnvEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(validated_envs_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            entries.push(ValidatedEnvEntry {
                bytes: path_size(&path)?,
                last_used: read_last_used(&path),
                path,
                is_archive: false,
            });
        } else if is_env_archive(&path) {
            entries.push(ValidatedEnvEntry {
                bytes: fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
                last_used: read_archive_last_used(&path),
                path,
                is_archive: true,
            });
        }
    }
    Ok(entries)
}

fn read_last_used(entry_dir: &Path) -> u128 {
    let marker_path = entry_dir.join(VALIDATED_ENV_LAST_USED_MARKER);
    if let Ok(contents) = fs::read_to_string(&marker_path) {
        if let Ok(stamp) = contents.trim().parse::<u128>() {
            return stamp;
        }
    }

    fs::metadata(entry_dir)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(system_time_to_millis)
        .unwrap_or(0)
}

fn path_size(path: &Path) -> io::Result<u64> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };

    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(metadata.len());
    }

    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(path_size(&entry.path())?);
    }
    Ok(total)
}

fn remove_path_if_exists(path: &Path) -> io::Result<u64> {
    let bytes = path_size(path)?;
    if bytes == 0 && !path.exists() {
        return Ok(0);
    }
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(bytes)
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn system_time_to_millis(value: SystemTime) -> Option<u128> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis())
}
