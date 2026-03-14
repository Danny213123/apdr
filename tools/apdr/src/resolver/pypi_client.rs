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
                    // Cache hit! Save to local store for this session too
                    let _ = store.save_pypi_versions(package_name, &versions);
                    return versions;
                }
            }
        }
    }

    // 3. Try native KGraph SQLite (fast path: ~1ms indexed query, no IPC)
    let db_path = smtpip_db_path(store);
    let versions = kgraph_db::kgraph_versions(&db_path, package_name);
    if !versions.is_empty() {
        let _ = store.save_pypi_versions(package_name, &versions);
        let cache_mutex = get_knowledge_cache();
        if let Ok(mut cache) = cache_mutex.lock() {
            for version in &versions {
                cache.add_package_version(package_name, version);
            }
        }
        return versions;
    }

    // 4. Try smartPip Z3 solver (TCP or subprocess fallback)
    let versions = fetch_versions_from_smtpip(store, package_name);
    if !versions.is_empty() {
        // Populate knowledge cache from smartPip result
        let cache_mutex = get_knowledge_cache();
        if let Ok(mut cache) = cache_mutex.lock() {
            for version in &versions {
                cache.add_package_version(package_name, version);
            }
        }
        return versions;
    }

    // 5. Fallback to PyPI API
    let Some(output) = run_host_python(&[
        "-c",
        PYPI_VERSION_SCRIPT,
        package_name,
        python_version,
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

        // Populate knowledge cache from PyPI result
        let cache_mutex = get_knowledge_cache();
        if let Ok(mut cache) = cache_mutex.lock() {
            for version in &versions {
                cache.add_package_version(package_name, version);
            }
        }
    }
    versions
}

pub fn package_exists(store: &mut CacheStore, package_name: &str, python_version: &str) -> bool {
    !fetch_versions(store, package_name, python_version).is_empty()
}

pub fn compatible_versions(
    store: &mut CacheStore,
    package_name: &str,
    python_version: &str,
) -> Vec<String> {
    fetch_versions(store, package_name, python_version)
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

pub fn dependency_specs(
    store: &mut CacheStore,
    package_name: &str,
    version: &str,
) -> Vec<String> {
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
                    // Cache hit! Save to local store for this session too
                    let _ = store.save_version_dependency_specs(package_name, version, &specs);
                    let dep_names: Vec<String> = specs
                        .iter()
                        .map(|s| requirement_name(s))
                        .filter(|n| !n.is_empty())
                        .collect();
                    if !dep_names.is_empty() {
                        let _ = store.save_dependency_graph_entry(package_name, &dep_names);
                    }
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
            let _ = store.save_version_dependency_specs(package_name, version, &specs);
            let dep_names: Vec<String> = specs
                .iter()
                .map(|s| requirement_name(s))
                .filter(|n| !n.is_empty())
                .collect();
            if !dep_names.is_empty() {
                let _ = store.save_dependency_graph_entry(package_name, &dep_names);
            }
            let cache_mutex = get_knowledge_cache();
            if let Ok(mut cache) = cache_mutex.lock() {
                cache.add_dependencies(package_name, version, &specs);
            }
            return specs;
        }
    }

    // 4. Try smartPip Z3 solver via TCP (fast path if server is running)
    if let Some(specs) = try_smartpip_tcp_deps(store, package_name, version) {
        if !specs.is_empty() {
            let _ = store.save_version_dependency_specs(package_name, version, &specs);
            let dep_names: Vec<String> = specs
                .iter()
                .map(|s| requirement_name(s))
                .filter(|n| !n.is_empty())
                .collect();
            if !dep_names.is_empty() {
                let _ = store.save_dependency_graph_entry(package_name, &dep_names);
            }

            // Populate knowledge cache from smartPip TCP result
            let cache_mutex = get_knowledge_cache();
            if let Ok(mut cache) = cache_mutex.lock() {
                cache.add_dependencies(package_name, version, &specs);
            }

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
        let _ = store.save_version_dependency_specs(package_name, version, &specs);
        let dependency_names = specs
            .iter()
            .map(|spec| requirement_name(spec))
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        if !dependency_names.is_empty() {
            let _ = store.save_dependency_graph_entry(package_name, &dependency_names);
        }

        // Populate knowledge cache from smartPip subprocess result
        let cache_mutex = get_knowledge_cache();
        if let Ok(mut cache) = cache_mutex.lock() {
            cache.add_dependencies(package_name, version, &specs);
        }
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
            let cache_mutex = get_knowledge_cache();
            for (pkg, (versions, deps_by_version)) in &results {
                let _ = store.save_pypi_versions(pkg, versions);
                if let Ok(mut cache) = cache_mutex.lock() {
                    for version in versions {
                        cache.add_package_version(pkg, version);
                    }
                }
                for (version, specs) in deps_by_version {
                    let _ = store.save_version_dependency_specs(pkg, version, specs);
                    let dep_names: Vec<String> = specs
                        .iter()
                        .map(|s| requirement_name(s))
                        .filter(|n| !n.is_empty())
                        .collect();
                    if !dep_names.is_empty() {
                        let _ = store.save_dependency_graph_entry(pkg, &dep_names);
                    }
                    if let Ok(mut cache) = cache_mutex.lock() {
                        cache.add_dependencies(pkg, version, specs);
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
    let package_list = missing.iter().map(|p| normalize(p)).collect::<Vec<_>>().join(",");
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
                    let _ = store.save_pypi_versions(pkg, &versions);
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
                    let _ = store.save_version_dependency_specs(pkg, version, &specs);
                    let dep_names: Vec<String> = specs
                        .iter()
                        .map(|s| requirement_name(s))
                        .filter(|n| !n.is_empty())
                        .collect();
                    if !dep_names.is_empty() {
                        let _ = store.save_dependency_graph_entry(pkg, &dep_names);
                    }
                }
            }
            _ => {}
        }
    }
}

pub fn requirement_name(requirement: &str) -> String {
    let trimmed = requirement.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let operators = ["==", ">=", "<=", "!=", "~=", ">", "<"];
    let mut base = trimmed;
    for operator in operators {
        if let Some((left, _right)) = trimmed.split_once(operator) {
            base = left;
            break;
        }
    }
    let without_extras = base.split('[').next().unwrap_or(base);
    normalize(without_extras)
}

pub fn cached_package_names(store: &CacheStore) -> Vec<String> {
    let mut names = store
        .import_records()
        .into_iter()
        .map(|record| normalize(&record.package_name))
        .collect::<Vec<_>>();
    names.extend(store.pypi_index.keys().cloned());
    names.sort();
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
fn try_smartpip_tcp_deps(store: &CacheStore, package_name: &str, version: &str) -> Option<Vec<String>> {
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
    TcpStream::connect_timeout(&"127.0.0.1:8888".parse().unwrap(), Duration::from_millis(250)).is_ok()
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
    let stream = TcpStream::connect_timeout(&"127.0.0.1:8888".parse().unwrap(), Duration::from_millis(500))?;
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
    for operator in ["==", ">=", "<=", "!=", "~=", ">", "<"] {
        if let Some(target) = constraint.strip_prefix(operator) {
            return match operator {
                "==" => wildcard_match(version, target.trim()),
                "!=" => !wildcard_match(version, target.trim()),
                ">=" => compare_versions(version, target.trim()) != Ordering::Less,
                "<=" => compare_versions(version, target.trim()) != Ordering::Greater,
                ">" => compare_versions(version, target.trim()) == Ordering::Greater,
                "<" => compare_versions(version, target.trim()) == Ordering::Less,
                "~=" => compatible_release(version, target.trim()),
                _ => true,
            };
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
    version == prefix || version.starts_with(&format!("{prefix}."))
}

fn compatible_release(version: &str, base: &str) -> bool {
    if compare_versions(version, base) == Ordering::Less {
        return false;
    }
    let parts = base.split('.').collect::<Vec<_>>();
    if parts.len() <= 1 {
        return true;
    }
    let upper = if parts.len() == 2 {
        format!("{}.0", increment_numeric(parts[0]))
    } else {
        let mut prefix = parts[..parts.len() - 1]
            .iter()
            .map(|item| (*item).to_string())
            .collect::<Vec<_>>();
        let index = prefix.len().saturating_sub(1);
        prefix[index] = increment_numeric(&prefix[index]);
        prefix.truncate(index + 1);
        format!("{}.0", prefix.join("."))
    };
    compare_versions(version, &upper) == Ordering::Less
}

fn increment_numeric(value: &str) -> String {
    value
        .parse::<u64>()
        .map(|number| (number + 1).to_string())
        .unwrap_or_else(|_| format!("{value}1"))
}

fn compare_versions(left: &str, right: &str) -> Ordering {
    let left_parts = tokenize_version(left);
    let right_parts = tokenize_version(right);
    let max_len = std::cmp::max(left_parts.len(), right_parts.len());
    for index in 0..max_len {
        let left_part = left_parts.get(index).cloned().unwrap_or(VersionPart::Number(0));
        let right_part = right_parts.get(index).cloned().unwrap_or(VersionPart::Number(0));
        let ordering = match (left_part, right_part) {
            (VersionPart::Number(a), VersionPart::Number(b)) => a.cmp(&b),
            (VersionPart::Text(a), VersionPart::Text(b)) => a.cmp(&b),
            (VersionPart::Number(_), VersionPart::Text(_)) => Ordering::Greater,
            (VersionPart::Text(_), VersionPart::Number(_)) => Ordering::Less,
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

#[derive(Clone)]
enum VersionPart {
    Number(u64),
    Text(String),
}

fn tokenize_version(value: &str) -> Vec<VersionPart> {
    let mut parts = Vec::new();
    let mut buffer = String::new();
    let mut numeric = false;
    for ch in value.chars() {
        if ch.is_ascii_digit() {
            if !numeric && !buffer.is_empty() {
                parts.push(VersionPart::Text(buffer.to_lowercase()));
                buffer.clear();
            }
            numeric = true;
            buffer.push(ch);
        } else if ch.is_ascii_alphabetic() {
            if numeric && !buffer.is_empty() {
                let number = buffer.parse::<u64>().unwrap_or(0);
                parts.push(VersionPart::Number(number));
                buffer.clear();
            }
            numeric = false;
            buffer.push(ch);
        } else {
            if !buffer.is_empty() {
                if numeric {
                    let number = buffer.parse::<u64>().unwrap_or(0);
                    parts.push(VersionPart::Number(number));
                } else {
                    parts.push(VersionPart::Text(buffer.to_lowercase()));
                }
                buffer.clear();
            }
            numeric = false;
        }
    }
    if !buffer.is_empty() {
        if numeric {
            let number = buffer.parse::<u64>().unwrap_or(0);
            parts.push(VersionPart::Number(number));
        } else {
            parts.push(VersionPart::Text(buffer.to_lowercase()));
        }
    }
    parts
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
        assert_eq!(requirement_name("google.cloud.storage"), "google-cloud-storage");
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
        let cache_path = std::env::temp_dir().join(format!(
            "apdr-pypi-client-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&cache_path);
        let mut store =
            CacheStore::load(tool_root.as_path(), cache_path.clone()).expect("cache should load");
        let _ = store.save_pypi_versions("demo-package", &["1.0.0".into(), "1.5.0".into(), "2.0.0".into()]);
        assert_eq!(
            best_matching_version(&mut store, "demo-package", ">=1.0,<2.0", "3.11").as_deref(),
            Some("1.5.0")
        );
        let _ = fs::remove_dir_all(cache_path);
    }
}
