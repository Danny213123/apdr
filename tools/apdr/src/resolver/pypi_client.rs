use std::cmp::Ordering;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use once_cell::sync::OnceCell;

use crate::cache::pypi_index;
use crate::cache::store::{normalize, CacheStore};
use crate::knowledge_cache::KnowledgeCache;
use crate::resolver::kgraph_db;

// Lazy-initialized in-process knowledge cache (fastest lookup path)
// Wrapped in Mutex to allow learning/updates as we discover new packages
static KNOWLEDGE_CACHE: OnceCell<Mutex<KnowledgeCache>> = OnceCell::new();

// Lazy-initialized TCP connection to smartPip server (port 8888)
static SMARTPIP_CONNECTION: Mutex<Option<TcpStream>> = Mutex::new(None);
static SMARTPIP_SERVER_LAUNCHING: AtomicBool = AtomicBool::new(false);
static SMARTPIP_SERVER_UNAVAILABLE: AtomicBool = AtomicBool::new(false);

/// Get or initialize the knowledge cache (starts empty, populated on-demand from smartPip)
fn get_knowledge_cache() -> &'static Mutex<KnowledgeCache> {
    KNOWLEDGE_CACHE.get_or_init(|| {
        // Start with empty cache - will be populated on-demand from smartPip Z3 queries
        // This avoids the 70s startup delay from loading .shrink files
        Mutex::new(KnowledgeCache::new_empty())
    })
}

/// Save the knowledge cache back to disk (persists learned knowledge)
pub fn save_knowledge_cache() -> std::io::Result<()> {
    let cache_mutex = get_knowledge_cache();
    let cache = cache_mutex.lock().ok().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "Failed to lock knowledge cache")
    })?;

    let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/knowledge");
    cache.save_to_directory(&data_dir)
}

pub fn latest_known_version(store: &CacheStore, package_name: &str) -> Option<String> {
    pypi_index::compatible_versions(store, package_name)
        .and_then(|versions| versions.last().cloned())
}

fn dependency_names_from_specs(specs: &[String]) -> Vec<String> {
    specs
        .iter()
        .map(|spec| requirement_name(spec))
        .filter(|name| !name.is_empty())
        .collect()
}

fn persist_versions_with_cache(
    store: &mut CacheStore,
    package_name: &str,
    versions: &[String],
    cache: Option<&mut KnowledgeCache>,
) {
    if versions.is_empty() {
        return;
    }

    let _ = store.save_pypi_versions(package_name, versions);
    if let Some(cache) = cache {
        for version in versions {
            cache.add_package_version(package_name, version);
        }
        return;
    }

    if let Ok(mut cache) = get_knowledge_cache().lock() {
        for version in versions {
            cache.add_package_version(package_name, version);
        }
    }
}

fn persist_versions(store: &mut CacheStore, package_name: &str, versions: &[String]) {
    persist_versions_with_cache(store, package_name, versions, None);
}

fn persist_dependency_specs_with_cache(
    store: &mut CacheStore,
    package_name: &str,
    version: &str,
    specs: &[String],
    cache: Option<&mut KnowledgeCache>,
) {
    if specs.is_empty() {
        return;
    }

    let _ = store.save_version_dependency_specs(package_name, version, specs);
    let dependency_names = dependency_names_from_specs(specs);
    if !dependency_names.is_empty() {
        let _ = store.save_dependency_graph_entry(package_name, &dependency_names);
    }

    if let Some(cache) = cache {
        cache.add_dependencies(package_name, version, specs);
        return;
    }

    if let Ok(mut cache) = get_knowledge_cache().lock() {
        cache.add_dependencies(package_name, version, specs);
    }
}

fn persist_dependency_specs(
    store: &mut CacheStore,
    package_name: &str,
    version: &str,
    specs: &[String],
) {
    persist_dependency_specs_with_cache(store, package_name, version, specs, None);
}

pub fn fetch_versions(
    store: &mut CacheStore,
    package_name: &str,
    python_version: &str,
) -> Vec<String> {
    // 1. Try local cache first (includes session data and test data)
    if let Some(versions) = pypi_index::compatible_versions(store, package_name) {
        if !versions.is_empty() {
            return versions.clone();
        }
    }

    // 2. Try in-process knowledge cache (on-demand from smartPip, no file loading)
    {
        let cache_mutex = get_knowledge_cache();
        if let Ok(cache) = cache_mutex.lock() {
            if let Some(versions) = cache.get_versions(package_name) {
                if !versions.is_empty() {
                    persist_versions(store, package_name, &versions);
                    return versions;
                }
            }
        }
    }

    // 3. Try native KGraph SQLite (fast path: ~1ms indexed query, no IPC)
    let db_path = smtpip_db_path(store);
    let versions = kgraph_db::kgraph_versions(&db_path, package_name);
    if !versions.is_empty() {
        persist_versions(store, package_name, &versions);
        return versions;
    }

    // 4. Try smartPip Z3 solver (TCP or subprocess fallback)
    let versions = fetch_versions_from_smtpip(store, package_name);
    if !versions.is_empty() {
        persist_versions(store, package_name, &versions);
        return versions;
    }

    // 4.5. PEP 658 / PyPI Simple API (JSON, no subprocess overhead)
    let versions = fetch_versions_from_pypi_simple(package_name);
    if !versions.is_empty() {
        persist_versions(store, package_name, &versions);
        return versions;
    }

    // 5. Fallback to PyPI API (subprocess)
    let Some(output) = run_host_python(&["-c", PYPI_VERSION_SCRIPT, package_name, python_version])
    else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let versions = stdout
        .trim()
        .split(',')
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !versions.is_empty() {
        persist_versions(store, package_name, &versions);
    }
    versions
}

pub fn package_exists(store: &mut CacheStore, package_name: &str, python_version: &str) -> bool {
    let versions = fetch_versions(store, package_name, python_version);
    // A package with only version "0" is treated as nonexistent (phantom cache entry).
    versions.iter().any(|v| v != "0")
}

pub fn compatible_versions(
    store: &mut CacheStore,
    package_name: &str,
    python_version: &str,
) -> Vec<String> {
    let mut versions = fetch_versions(store, package_name, python_version);
    // Filter phantom version "0" that contaminates caches.
    versions.retain(|v| v != "0");
    versions
}

pub fn best_matching_version(
    store: &mut CacheStore,
    package_name: &str,
    constraint: &str,
    python_version: &str,
) -> Option<String> {
    compatible_versions(store, package_name, python_version)
        .into_iter()
        .filter(|version| version_satisfies(version, constraint))
        .last()
}

pub fn compatible_default_version(
    store: &mut CacheStore,
    package_name: &str,
    preferred_version: Option<&str>,
    python_version: &str,
) -> Option<String> {
    let preferred = preferred_version
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let versions = compatible_versions(store, package_name, python_version);
    if versions.is_empty() {
        return None;
    }
    if versions.iter().any(|item| item == preferred) {
        return Some(preferred.to_string());
    }
    versions.last().cloned()
}

pub fn dependency_specs(store: &mut CacheStore, package_name: &str, version: &str) -> Vec<String> {
    // 1. Try local cache first
    if let Some(specs) = store.version_dependency_specs(package_name, version) {
        return specs.clone();
    }

    // 2. Try in-process knowledge cache (on-demand from smartPip, no file loading)
    {
        let cache_mutex = get_knowledge_cache();
        if let Ok(cache) = cache_mutex.lock() {
            if let Some(specs) = cache.get_dependencies(package_name, version) {
                if !specs.is_empty() {
                    persist_dependency_specs(store, package_name, version, &specs);
                    return specs;
                }
            }
        }
    }

    // 3. Try native KGraph SQLite (fast path: ~1ms indexed query, no IPC)
    {
        let db_path = smtpip_db_path(store);
        let specs = kgraph_db::kgraph_dependency_specs(&db_path, package_name, version);
        if !specs.is_empty() {
            persist_dependency_specs(store, package_name, version, &specs);
            return specs;
        }
    }

    // 4. Try smartPip Z3 solver via TCP (fast path if server is running)
    if let Some(specs) = try_smartpip_tcp_deps(store, package_name, version) {
        if !specs.is_empty() {
            persist_dependency_specs(store, package_name, version, &specs);
            return specs;
        }
    }

    // 5. Fallback to subprocess (slowest path)
    let Some(kgraph_path) = smtpip_kgraph_path(store) else {
        return Vec::new();
    };
    let kgraph_path_text = kgraph_path.display().to_string();
    let db_path_text = smtpip_db_path(store).display().to_string();
    let Some(output) = run_host_python(&[
        "-c",
        SMTPIP_KGRAPH_SCRIPT,
        "deps",
        kgraph_path_text.as_str(),
        db_path_text.as_str(),
        package_name,
        version,
    ]) else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let specs = stdout
        .trim()
        .split('\n')
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !specs.is_empty() {
        persist_dependency_specs(store, package_name, version, &specs);
    }
    specs
}

/// Bulk pre-fetch versions and dependency specs from the KGraph for a set of
/// packages. This replaces many sequential subprocess calls with a single one,
/// dramatically reducing startup time for the pre-solve phase.
pub fn bulk_prefetch_from_kgraph(store: &mut CacheStore, packages: &[String]) {
    let missing: Vec<&String> = packages
        .iter()
        .filter(|pkg| pypi_index::compatible_versions(store, pkg).is_none())
        .collect();
    if missing.is_empty() {
        return;
    }

    // Try native SQLite bulk prefetch first (~50ms for 30 packages vs ~2-5s subprocess)
    let db_path = smtpip_db_path(store);
    if kgraph_db::db_available(&db_path) {
        let missing_owned: Vec<String> = missing.iter().map(|p| (*p).clone()).collect();
        let results = kgraph_db::kgraph_bulk_prefetch(&db_path, &missing_owned);
        if !results.is_empty() {
            if let Ok(mut cache) = get_knowledge_cache().lock() {
                for (pkg, (versions, deps_by_version)) in &results {
                    persist_versions_with_cache(store, pkg, versions, Some(&mut cache));
                    for (version, specs) in deps_by_version {
                        persist_dependency_specs_with_cache(
                            store,
                            pkg,
                            version,
                            specs,
                            Some(&mut cache),
                        );
                    }
                }
            } else {
                for (pkg, (versions, deps_by_version)) in &results {
                    persist_versions(store, pkg, versions);
                    for (version, specs) in deps_by_version {
                        persist_dependency_specs(store, pkg, version, specs);
                    }
                }
            }
            return;
        }
    }

    // Fallback to Python subprocess for bulk prefetch (also builds DB if missing)
    let Some(kgraph_path) = smtpip_kgraph_path(store) else {
        return;
    };
    let kgraph_path_text = kgraph_path.display().to_string();
    let db_path_text = db_path.display().to_string();
    let package_list = missing
        .iter()
        .map(|p| normalize(p))
        .collect::<Vec<_>>()
        .join(",");
    let Some(output) = run_host_python(&[
        "-c",
        SMTPIP_BULK_SCRIPT,
        kgraph_path_text.as_str(),
        db_path_text.as_str(),
        &package_list,
    ]) else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        match parts.first().copied() {
            Some("V") if parts.len() >= 3 => {
                let pkg = parts[1];
                let versions: Vec<String> = parts[2]
                    .split(',')
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect();
                if !versions.is_empty() {
                    persist_versions(store, pkg, &versions);
                }
            }
            Some("D") if parts.len() >= 4 => {
                let pkg = parts[1];
                let version = parts[2];
                let specs: Vec<String> = parts[3]
                    .split('|')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !specs.is_empty() {
                    persist_dependency_specs(store, pkg, version, &specs);
                }
            }
            _ => {}
        }
    }
}

pub fn requirement_name(requirement: &str) -> String {
    use std::cell::RefCell;
    use std::collections::HashMap;

    thread_local! {
        static CACHE: RefCell<HashMap<String, String>> = RefCell::new(HashMap::with_capacity(256));
    }
    const MAX_ENTRIES: usize = 8192;

    let trimmed = requirement.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(cached) = cache.get(trimmed) {
            return cached.clone();
        }
        // Find first operator character position (single scan instead of 7 split_once calls)
        let base = match trimmed.find(|ch: char| matches!(ch, '<' | '>' | '!' | '=' | '~')) {
            Some(pos) => &trimmed[..pos],
            None => trimmed,
        };
        let without_extras = base.split('[').next().unwrap_or(base);
        let result = normalize(without_extras);
        if cache.len() < MAX_ENTRIES {
            cache.insert(trimmed.to_string(), result.clone());
        }
        result
    })
}

pub fn cached_package_names(store: &CacheStore) -> Vec<String> {
    // Pre-allocate with estimated capacity to avoid re-allocation (#6)
    let estimated = store.import_map.len() + store.pypi_index.len();
    let mut names = Vec::with_capacity(estimated);
    names.extend(
        store
            .import_records()
            .into_iter()
            .map(|record| normalize(&record.package_name)),
    );
    names.extend(store.pypi_index.keys().cloned());
    names.sort_unstable();
    names.dedup();
    names
}

pub fn version_satisfies(version: &str, constraint: &str) -> bool {
    let trimmed = constraint.trim();
    if trimmed.is_empty() {
        return true;
    }
    trimmed
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .all(|item| satisfies_single_constraint(version, item))
}

fn fetch_versions_from_smtpip(store: &mut CacheStore, package_name: &str) -> Vec<String> {
    // Try TCP connection to smartPip server first (fast path)
    if let Some(versions) = try_smartpip_tcp_versions(store, package_name) {
        if !versions.is_empty() {
            let _ = store.save_pypi_versions(package_name, &versions);
            return versions;
        }
    }

    // Fallback to subprocess (slow path)
    let Some(kgraph_path) = smtpip_kgraph_path(store) else {
        return Vec::new();
    };
    let kgraph_path_text = kgraph_path.display().to_string();
    let db_path_text = smtpip_db_path(store).display().to_string();
    let Some(output) = run_host_python(&[
        "-c",
        SMTPIP_KGRAPH_SCRIPT,
        "versions",
        kgraph_path_text.as_str(),
        db_path_text.as_str(),
        package_name,
    ]) else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let versions = stdout
        .trim()
        .split(',')
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !versions.is_empty() {
        let _ = store.save_pypi_versions(package_name, &versions);
    }
    versions
}

/// Fetch available versions for a package from the PyPI Simple API (PEP 658).
/// Uses the JSON variant (`application/vnd.pypi.simple.v1+json`) to avoid HTML parsing.
/// Returns an empty Vec on any error or timeout (10s).
fn fetch_versions_from_pypi_simple(package_name: &str) -> Vec<String> {
    let url = format!("https://pypi.org/simple/{}/", normalize(package_name));
    let agent = ureq::Agent::new_with_config(
        ureq::config::Config::builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .build(),
    );
    let response = agent
        .get(&url)
        .header("Accept", "application/vnd.pypi.simple.v1+json")
        .call();
    let Ok(mut response) = response else {
        return Vec::new();
    };
    let Ok(body) = response.body_mut().read_to_string() else {
        return Vec::new();
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) else {
        return Vec::new();
    };
    // Extract versions from the "versions" key (PEP 700 / Simple API v1)
    // PyPI returns "versions" as either:
    //   - A JSON array of version strings (current PyPI behavior)
    //   - A JSON object with version keys (PEP 700 draft spec)
    if let Some(versions_arr) = json.get("versions").and_then(|v| v.as_array()) {
        let mut versions: Vec<String> = versions_arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        if !versions.is_empty() {
            versions.sort_by(|a, b| {
                super::kgraph_db::version_sort_key(a).cmp(&super::kgraph_db::version_sort_key(b))
            });
            return versions;
        }
    }
    if let Some(versions_obj) = json.get("versions").and_then(|v| v.as_object()) {
        let mut versions: Vec<String> = versions_obj.keys().cloned().collect();
        versions.sort_by(|a, b| {
            super::kgraph_db::version_sort_key(a).cmp(&super::kgraph_db::version_sort_key(b))
        });
        return versions;
    }
    // Fallback: parse version from filenames in "files" array
    if let Some(files) = json.get("files").and_then(|v| v.as_array()) {
        let mut version_set = std::collections::BTreeSet::new();
        for file in files {
            if let Some(filename) = file.get("filename").and_then(|v| v.as_str()) {
                if let Some(version) = extract_version_from_filename(filename, package_name) {
                    version_set.insert(version);
                }
            }
        }
        let mut versions: Vec<String> = version_set.into_iter().collect();
        versions.sort_by(|a, b| {
            super::kgraph_db::version_sort_key(a).cmp(&super::kgraph_db::version_sort_key(b))
        });
        return versions;
    }
    Vec::new()
}

/// Extract version string from a PyPI filename (sdist or wheel).
fn extract_version_from_filename(filename: &str, package_name: &str) -> Option<String> {
    let normalized = normalize(package_name);
    let prefix = format!("{}-", normalized);
    let lower = filename.to_lowercase().replace('-', "_");
    let norm_prefix = prefix.to_lowercase().replace('-', "_");
    if !lower.starts_with(&norm_prefix) {
        return None;
    }
    let rest = &filename[prefix.len()..].replace(&normalized, "");
    // For wheel: name-version-py-abi-platform.whl
    // For sdist: name-version.tar.gz or name-version.zip
    let version_end = rest
        .find(|c: char| c == '-' || c == '.')
        .filter(|&pos| pos > 0)
        .unwrap_or(rest.len());
    let candidate = &rest[..version_end];
    if candidate
        .chars()
        .next()
        .map_or(false, |c| c.is_ascii_digit())
    {
        Some(candidate.to_string())
    } else {
        None
    }
}

fn smtpip_kgraph_path(store: &CacheStore) -> Option<PathBuf> {
    let candidates = [
        store.tool_root.join("../../SMTpip/KGraph.zip"),
        store.tool_root.join("../../SMTpip/KGraph.json"),
        store.tool_root.join("../SMTpip/KGraph.zip"),
        store.tool_root.join("../SMTpip/KGraph.json"),
    ];
    candidates
        .into_iter()
        .map(|path| path.canonicalize().unwrap_or(path))
        .find(|path| path.exists())
}

/// Try to query smartPip TCP server for package versions.
/// Returns None if TCP connection fails, allowing fallback to subprocess.
fn try_smartpip_tcp_versions(store: &CacheStore, package_name: &str) -> Option<Vec<String>> {
    let mut conn_guard = SMARTPIP_CONNECTION.lock().ok()?;

    // Establish connection if not already connected
    if conn_guard.is_none() {
        match connect_smartpip_stream() {
            Ok(stream) => {
                *conn_guard = Some(stream);
            }
            Err(_) => {
                if !SMARTPIP_SERVER_UNAVAILABLE.load(AtomicOrdering::SeqCst) {
                    ensure_smartpip_tcp_server(store);
                }
                match connect_smartpip_stream() {
                    Ok(stream) => {
                        *conn_guard = Some(stream);
                    }
                    Err(_) => return None,
                }
            }
        }
    }

    let stream = conn_guard.as_mut()?;

    // Send request: "VERSIONS package_name\n"
    let request = format!("VERSIONS {}\n", normalize(package_name));
    if stream.write_all(request.as_bytes()).is_err() {
        *conn_guard = None; // Connection failed, reset
        return None;
    }

    // Read response: "version1,version2,version3\n"
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut response = String::new();
    if reader.read_line(&mut response).is_err() {
        *conn_guard = None;
        return None;
    }

    let versions = response
        .trim()
        .split(',')
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();

    if versions.is_empty() {
        None
    } else {
        Some(versions)
    }
}

/// Try to query smartPip TCP server for dependency specs.
/// Returns None if TCP connection fails, allowing fallback to subprocess.
fn try_smartpip_tcp_deps(
    store: &CacheStore,
    package_name: &str,
    version: &str,
) -> Option<Vec<String>> {
    let mut conn_guard = SMARTPIP_CONNECTION.lock().ok()?;

    if conn_guard.is_none() {
        match connect_smartpip_stream() {
            Ok(stream) => {
                *conn_guard = Some(stream);
            }
            Err(_) => {
                if !SMARTPIP_SERVER_UNAVAILABLE.load(AtomicOrdering::SeqCst) {
                    ensure_smartpip_tcp_server(store);
                }
                match connect_smartpip_stream() {
                    Ok(stream) => {
                        *conn_guard = Some(stream);
                    }
                    Err(_) => return None,
                }
            }
        }
    }

    let stream = conn_guard.as_mut()?;

    // Send request: "DEPS package_name version\n"
    let request = format!("DEPS {} {}\n", normalize(package_name), version);
    if stream.write_all(request.as_bytes()).is_err() {
        *conn_guard = None;
        return None;
    }

    // Read response: "spec1|spec2|spec3\n"
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut response = String::new();
    if reader.read_line(&mut response).is_err() {
        *conn_guard = None;
        return None;
    }

    let specs = response
        .trim()
        .split('|')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    Some(specs)
}

fn smtpip_db_path(store: &CacheStore) -> PathBuf {
    store.cache_path.join("smtpip-kgraph.sqlite3")
}

fn smartpip_server_script_path(store: &CacheStore) -> Option<PathBuf> {
    let path = store.tool_root.join("smartpip_kgraph_server.py");
    if path.exists() {
        return Some(path);
    }
    None
}

fn smartpip_server_log_path(store: &CacheStore) -> PathBuf {
    store.cache_path.join("smartpip-kgraph-server.log")
}

fn ensure_smartpip_tcp_server(store: &CacheStore) {
    if smartpip_server_available() {
        SMARTPIP_SERVER_UNAVAILABLE.store(false, AtomicOrdering::SeqCst);
        return;
    }
    if SMARTPIP_SERVER_LAUNCHING.swap(true, AtomicOrdering::SeqCst) {
        wait_for_smartpip_server();
        return;
    }

    let Some(python) = host_python_command() else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
        SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
        return;
    };
    let Some(graph_path) = smtpip_kgraph_path(store) else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
        SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
        return;
    };
    let Some(script_path) = smartpip_server_script_path(store) else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
        SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
        return;
    };
    let db_path = smtpip_db_path(store);
    let log_path = smartpip_server_log_path(store);

    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .ok();
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .ok();

    let mut command = Command::new(python);
    command
        .arg(script_path)
        .arg(graph_path)
        .arg(db_path)
        .arg("8888");

    if let Some(handle) = stdout {
        command.stdout(handle);
    }
    if let Some(handle) = stderr {
        command.stderr(handle);
    }

    let _ = command.spawn();
    if smartpip_server_available() || wait_for_smartpip_server() {
        SMARTPIP_SERVER_UNAVAILABLE.store(false, AtomicOrdering::SeqCst);
    } else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
    }
    SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
}

fn smartpip_server_available() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:8888".parse().unwrap(),
        Duration::from_millis(250),
    )
    .is_ok()
}

fn wait_for_smartpip_server() -> bool {
    for _ in 0..40 {
        if smartpip_server_available() {
            return true;
        }
        thread::sleep(Duration::from_millis(250));
    }
    false
}

fn connect_smartpip_stream() -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(
        &"127.0.0.1:8888".parse().unwrap(),
        Duration::from_millis(500),
    )?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    Ok(stream)
}

fn run_host_python(args: &[&str]) -> Option<std::process::Output> {
    let python = host_python_command()?;
    Command::new(python).args(args).output().ok()
}

fn host_python_command() -> Option<PathBuf> {
    let mut candidates = vec![PathBuf::from("python3"), PathBuf::from("python")];
    if cfg!(windows) {
        for version in ["312", "311", "310", "39"] {
            if let Some(local_appdata) = std::env::var_os("LOCALAPPDATA") {
                candidates.push(
                    PathBuf::from(&local_appdata)
                        .join("Programs")
                        .join("Python")
                        .join(format!("Python{version}"))
                        .join("python.exe"),
                );
            }
            for variable in ["ProgramFiles", "ProgramFiles(x86)"] {
                if let Some(base) = std::env::var_os(variable) {
                    candidates.push(
                        PathBuf::from(&base)
                            .join("Python")
                            .join(format!("Python{version}"))
                            .join("python.exe"),
                    );
                }
            }
        }
    }
    dedupe_paths(candidates)
        .into_iter()
        .find(|candidate| is_python3(candidate))
}

fn is_python3(candidate: &Path) -> bool {
    let Ok(output) = Command::new(candidate)
        .arg("-c")
        .arg("import sys; sys.stdout.write('%s' % sys.version_info[0])")
        .output()
    else {
        return false;
    };
    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "3"
}

fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = std::collections::BTreeSet::new();
    let mut unique = Vec::new();
    for path in paths {
        let key = path.to_string_lossy().to_string();
        if seen.insert(key) {
            unique.push(path);
        }
    }
    unique
}

fn satisfies_single_constraint(version: &str, constraint: &str) -> bool {
    let bytes = constraint.as_bytes();
    // Two-character operators (checked first)
    if bytes.len() >= 2 {
        match (bytes[0], bytes[1]) {
            (b'=', b'=') => return wildcard_match(version, constraint[2..].trim()),
            (b'>', b'=') => {
                return compare_versions(version, constraint[2..].trim()) != Ordering::Less
            }
            (b'<', b'=') => {
                return compare_versions(version, constraint[2..].trim()) != Ordering::Greater
            }
            (b'!', b'=') => return !wildcard_match(version, constraint[2..].trim()),
            (b'~', b'=') => return compatible_release(version, constraint[2..].trim()),
            _ => {}
        }
    }
    // Single-character operators
    if !bytes.is_empty() {
        match bytes[0] {
            b'>' => return compare_versions(version, constraint[1..].trim()) == Ordering::Greater,
            b'<' => return compare_versions(version, constraint[1..].trim()) == Ordering::Less,
            _ => {}
        }
    }
    wildcard_match(version, constraint)
}

fn wildcard_match(version: &str, target: &str) -> bool {
    let target = target.trim();
    if !target.contains('*') {
        return compare_versions(version, target) == Ordering::Equal;
    }
    let prefix = target.trim_end_matches('*').trim_end_matches('.');
    version == prefix
        || (version.len() > prefix.len()
            && version.starts_with(prefix)
            && version.as_bytes()[prefix.len()] == b'.')
}

fn compatible_release(version: &str, base: &str) -> bool {
    if compare_versions(version, base) == Ordering::Less {
        return false;
    }
    let parts: Vec<&str> = base.split('.').collect();
    if parts.len() <= 1 {
        return true;
    }
    // Build upper bound: take all but last segment, increment second-to-last, append ".0"
    let inc_index = parts.len() - 2;
    let mut upper = String::with_capacity(base.len() + 4);
    for (i, part) in parts[..parts.len() - 1].iter().enumerate() {
        if i > 0 {
            upper.push('.');
        }
        if i == inc_index {
            upper.push_str(&increment_numeric(part));
        } else {
            upper.push_str(part);
        }
    }
    upper.push_str(".0");
    compare_versions(version, &upper) == Ordering::Less
}

fn increment_numeric(value: &str) -> String {
    value
        .parse::<u64>()
        .map(|number| (number + 1).to_string())
        .unwrap_or_else(|_| format!("{value}1"))
}

/// Maximum number of parsed segments in a version string.
/// Real-world Python versions rarely exceed 6 segments (e.g. "1.0.0.dev201502270022" = 5 parts).
const MAX_VERSION_PARTS: usize = 10;

/// Stack-allocated version segment — no heap allocation.
#[derive(Clone, Copy)]
enum VersionPart {
    Number(u64),
    /// Lowercased text segment stored inline. Max 8 bytes covers all known
    /// PEP 440 suffixes (a, b, rc, dev, post, alpha, beta, pre, rev, final).
    Text([u8; 8], u8),
}

/// Parse a version string into a fixed-size array of parts.
/// Semantics match the previous Vec-based `tokenize_version` exactly:
///   - Digits accumulate into Number parts
///   - Letters accumulate into Text parts (lowercased)
///   - All other characters (`.`, `-`, `_`) are separators
fn tokenize_version(value: &str) -> ([VersionPart; MAX_VERSION_PARTS], usize) {
    let mut parts = [VersionPart::Number(0); MAX_VERSION_PARTS];
    let mut len = 0usize;
    let mut text_buf = [0u8; 8];
    let mut text_len: u8 = 0;
    let mut num_acc: u64 = 0;
    let mut in_number = false;
    let mut buf_active = false;

    for ch in value.bytes() {
        if ch.is_ascii_digit() {
            if !in_number && buf_active {
                if len < MAX_VERSION_PARTS {
                    parts[len] = VersionPart::Text(text_buf, text_len);
                    len += 1;
                }
                text_buf = [0u8; 8];
                text_len = 0;
            }
            in_number = true;
            buf_active = true;
            num_acc = num_acc.wrapping_mul(10).wrapping_add((ch - b'0') as u64);
        } else if ch.is_ascii_alphabetic() {
            if in_number && buf_active {
                if len < MAX_VERSION_PARTS {
                    parts[len] = VersionPart::Number(num_acc);
                    len += 1;
                }
                num_acc = 0;
            }
            in_number = false;
            buf_active = true;
            if text_len < 8 {
                text_buf[text_len as usize] = ch.to_ascii_lowercase();
                text_len += 1;
            }
        } else {
            if buf_active {
                if in_number {
                    if len < MAX_VERSION_PARTS {
                        parts[len] = VersionPart::Number(num_acc);
                        len += 1;
                    }
                    num_acc = 0;
                } else if len < MAX_VERSION_PARTS {
                    parts[len] = VersionPart::Text(text_buf, text_len);
                    len += 1;
                    text_buf = [0u8; 8];
                    text_len = 0;
                }
                buf_active = false;
            }
            in_number = false;
        }
    }

    if buf_active {
        if in_number {
            if len < MAX_VERSION_PARTS {
                parts[len] = VersionPart::Number(num_acc);
                len += 1;
            }
        } else if len < MAX_VERSION_PARTS {
            parts[len] = VersionPart::Text(text_buf, text_len);
            len += 1;
        }
    }

    (parts, len)
}

/// Thread-local cache for tokenized version strings. Avoids re-tokenizing
/// the same version (e.g. "3.11", "1.0.0") thousands of times in the solver loop.
fn tokenize_cached(version: &str) -> ([VersionPart; MAX_VERSION_PARTS], usize) {
    use std::cell::RefCell;
    use std::collections::HashMap;

    thread_local! {
        static CACHE: RefCell<HashMap<String, ([VersionPart; MAX_VERSION_PARTS], usize)>> =
            RefCell::new(HashMap::with_capacity(256));
    }
    const MAX_ENTRIES: usize = 4096;

    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(cached) = cache.get(version) {
            return *cached;
        }
        let result = tokenize_version(version);
        if cache.len() < MAX_ENTRIES {
            cache.insert(version.to_string(), result);
        }
        result
    })
}

fn compare_versions(left: &str, right: &str) -> Ordering {
    let (lp, ll) = tokenize_cached(left);
    let (rp, rl) = tokenize_cached(right);
    let max_len = std::cmp::max(ll, rl);
    for i in 0..max_len {
        let left_part = if i < ll {
            lp[i]
        } else {
            VersionPart::Number(0)
        };
        let right_part = if i < rl {
            rp[i]
        } else {
            VersionPart::Number(0)
        };
        let ordering = match (left_part, right_part) {
            (VersionPart::Number(a), VersionPart::Number(b)) => a.cmp(&b),
            (VersionPart::Text(a, al), VersionPart::Text(b, bl)) => {
                a[..al as usize].cmp(&b[..bl as usize])
            }
            (VersionPart::Number(_), VersionPart::Text(..)) => Ordering::Greater,
            (VersionPart::Text(..), VersionPart::Number(_)) => Ordering::Less,
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

const PYPI_VERSION_SCRIPT: &str = r#"
import json
import sys
import urllib.request

package = sys.argv[1]
python_version = sys.argv[2]
target_major = python_version.split('.')[0]
target_minor = python_version.split('.')[1] if '.' in python_version else '0'

def tag_supports(tag, major, minor):
    tag = (tag or '').lower()
    if not tag or tag in {'source', 'any', 'py2.py3', 'py3.py2'}:
        return True
    if major == '2':
        return ('py2' in tag) or ('cp27' in tag) or tag.startswith('2.')
    if major == '3':
        return ('py3' in tag) or (f'cp{major}{minor}' in tag) or tag.startswith(f'{major}.')
    return False

def version_key(value):
    parts = []
    current = ''
    for ch in value:
        if ch.isdigit():
            current += ch
        else:
            if current:
                parts.append(int(current))
                current = ''
            parts.append(ch)
    if current:
        parts.append(int(current))
    return parts

try:
    with urllib.request.urlopen(f'https://pypi.org/pypi/{package}/json', timeout=8) as response:
        payload = json.load(response)
except Exception:
    print('')
    raise SystemExit(0)

releases = payload.get('releases', {}) or {}
versions = []
for version, files in releases.items():
    if not files:
        continue
    if any(tag_supports(item.get('python_version'), target_major, target_minor) for item in files):
        versions.append(version)

versions = sorted(set(versions), key=version_key)
print(','.join(versions))
"#;

const SMTPIP_KGRAPH_SCRIPT: &str = r#"
import json
import os
import sqlite3
import sys
import zipfile
from pathlib import Path

mode = sys.argv[1]
graph_path = Path(sys.argv[2])
db_path = Path(sys.argv[3])
package = sys.argv[4]
version = sys.argv[5] if len(sys.argv) > 5 else ""

def normalize(name):
    return name.strip().replace('_', '-').replace('.', '-').lower()

def version_key(value):
    parts = []
    current = ''
    for ch in value:
        if ch.isdigit():
            current += ch
        else:
            if current:
                parts.append(int(current))
                current = ''
            parts.append(ch)
    if current:
        parts.append(int(current))
    return parts

def load_graph(path):
    if path.suffix == '.zip':
        with zipfile.ZipFile(path) as zf:
            with zf.open('KGraph.json') as fh:
                return json.load(fh)
    with path.open('r', encoding='utf-8') as fh:
        return json.load(fh)

def ensure_db(graph_path, db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    should_rebuild = (
        (not db_path.exists())
        or db_path.stat().st_mtime < graph_path.stat().st_mtime
    )
    conn = sqlite3.connect(db_path)
    if not should_rebuild:
        return conn
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS versions")
    cur.execute("DROP TABLE IF EXISTS deps")
    cur.execute("CREATE TABLE versions(package TEXT NOT NULL, version TEXT NOT NULL)")
    cur.execute("CREATE TABLE deps(package TEXT NOT NULL, version TEXT NOT NULL, spec TEXT NOT NULL)")
    cur.execute("CREATE INDEX idx_versions_package ON versions(package)")
    cur.execute("CREATE INDEX idx_deps_package_version ON deps(package, version)")
    graph = load_graph(graph_path)
    projects = graph.get('projects', {})
    version_rows = []
    dep_rows = []
    for raw_name, payload in projects.items():
        package_name = normalize(raw_name)
        for raw_version, meta in (payload or {}).items():
            version_rows.append((package_name, str(raw_version).strip()))
            dependency_packages = ((meta or {}).get('dependency_packages') or [])
            for spec in dependency_packages:
                spec_text = str(spec).strip()
                if spec_text:
                    dep_rows.append((package_name, str(raw_version).strip(), spec_text))
    cur.executemany("INSERT INTO versions(package, version) VALUES (?, ?)", version_rows)
    cur.executemany("INSERT INTO deps(package, version, spec) VALUES (?, ?, ?)", dep_rows)
    conn.commit()
    return conn

try:
    conn = ensure_db(graph_path, db_path)
except Exception:
    raise SystemExit(0)

if mode == 'versions':
    rows = conn.execute(
        "SELECT version FROM versions WHERE package = ?",
        (normalize(package),),
    ).fetchall()
    versions = sorted({row[0] for row in rows}, key=version_key)
    print(','.join(versions))
elif mode == 'deps':
    rows = conn.execute(
        "SELECT spec FROM deps WHERE package = ? AND version = ?",
        (normalize(package), version),
    ).fetchall()
    dependencies = [row[0] for row in rows]
    for item in dependencies:
        print(str(item).strip())
conn.close()
"#;

const SMTPIP_BULK_SCRIPT: &str = r#"
import json
import os
import sqlite3
import sys
import zipfile
from pathlib import Path

graph_path = Path(sys.argv[1])
db_path = Path(sys.argv[2])
packages = [p.strip() for p in sys.argv[3].split(',') if p.strip()]

def normalize(name):
    return name.strip().replace('_', '-').replace('.', '-').lower()

def version_key(value):
    parts = []
    current = ''
    for ch in value:
        if ch.isdigit():
            current += ch
        else:
            if current:
                parts.append(int(current))
                current = ''
            parts.append(ch)
    if current:
        parts.append(int(current))
    return parts

def load_graph(path):
    if path.suffix == '.zip':
        with zipfile.ZipFile(path) as zf:
            with zf.open('KGraph.json') as fh:
                return json.load(fh)
    with path.open('r', encoding='utf-8') as fh:
        return json.load(fh)

def ensure_db(graph_path, db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    should_rebuild = (
        (not db_path.exists())
        or db_path.stat().st_mtime < graph_path.stat().st_mtime
    )
    conn = sqlite3.connect(db_path)
    if not should_rebuild:
        return conn
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS versions")
    cur.execute("DROP TABLE IF EXISTS deps")
    cur.execute("CREATE TABLE versions(package TEXT NOT NULL, version TEXT NOT NULL)")
    cur.execute("CREATE TABLE deps(package TEXT NOT NULL, version TEXT NOT NULL, spec TEXT NOT NULL)")
    cur.execute("CREATE INDEX idx_versions_package ON versions(package)")
    cur.execute("CREATE INDEX idx_deps_package_version ON deps(package, version)")
    graph = load_graph(graph_path)
    projects = graph.get('projects', {})
    version_rows = []
    dep_rows = []
    for raw_name, payload in projects.items():
        package_name = normalize(raw_name)
        for raw_version, meta in (payload or {}).items():
            version_rows.append((package_name, str(raw_version).strip()))
            dependency_packages = ((meta or {}).get('dependency_packages') or [])
            for spec in dependency_packages:
                spec_text = str(spec).strip()
                if spec_text:
                    dep_rows.append((package_name, str(raw_version).strip(), spec_text))
    cur.executemany("INSERT INTO versions(package, version) VALUES (?, ?)", version_rows)
    cur.executemany("INSERT INTO deps(package, version, spec) VALUES (?, ?, ?)", dep_rows)
    conn.commit()
    return conn

try:
    conn = ensure_db(graph_path, db_path)
except Exception:
    raise SystemExit(0)

normalized = [normalize(p) for p in packages]
for pkg in normalized:
    rows = conn.execute(
        "SELECT version FROM versions WHERE package = ?", (pkg,)
    ).fetchall()
    versions = sorted({row[0] for row in rows}, key=version_key)
    if versions:
        print(f"V\t{pkg}\t{','.join(versions)}")
    for ver in versions:
        dep_rows = conn.execute(
            "SELECT spec FROM deps WHERE package = ? AND version = ?",
            (pkg, ver),
        ).fetchall()
        specs = [row[0] for row in dep_rows]
        if specs:
            print(f"D\t{pkg}\t{ver}\t{'|'.join(specs)}")
conn.close()
"#;

#[cfg(test)]
mod tests {
    use super::{best_matching_version, requirement_name, version_satisfies};
    use crate::cache::store::CacheStore;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn requirement_name_normalizes_extras_and_constraints() {
        assert_eq!(requirement_name("requests[socks]>=2.22"), "requests");
        assert_eq!(
            requirement_name("google.cloud.storage"),
            "google-cloud-storage"
        );
    }

    #[test]
    fn version_satisfies_common_constraints() {
        assert!(version_satisfies("1.7.3", ">=1.7,<1.8"));
        assert!(!version_satisfies("1.8.0", ">=1.7,<1.8"));
        assert!(version_satisfies("1.1.2", "==1.1.2"));
    }

    #[test]
    fn best_matching_version_prefers_highest_compatible() {
        let tool_root = PathBuf::from(".");
        let cache_path =
            std::env::temp_dir().join(format!("apdr-pypi-client-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&cache_path);
        let mut store =
            CacheStore::load(tool_root.as_path(), cache_path.clone()).expect("cache should load");
        let _ = store.save_pypi_versions(
            "demo-package",
            &["1.0.0".into(), "1.5.0".into(), "2.0.0".into()],
        );
        assert_eq!(
            best_matching_version(&mut store, "demo-package", ">=1.0,<2.0", "3.11").as_deref(),
            Some("1.5.0")
        );
        let _ = fs::remove_dir_all(cache_path);
    }
}
