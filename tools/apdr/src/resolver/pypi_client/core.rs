use super::host_python::run_host_python;
use super::smartpip::{
    fetch_versions_from_smtpip, smtpip_db_path, smtpip_kgraph_path, try_smartpip_tcp_deps,
};
use super::version_matching::version_satisfies;
use crate::cache::pypi_index;
use crate::cache::store::{normalize, CacheStore};
use crate::knowledge_cache::KnowledgeCache;
use crate::resolver::kgraph_db;
use once_cell::sync::OnceCell;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

// Lazy-initialized in-process knowledge cache (fastest lookup path)
// Wrapped in Mutex to allow learning/updates as we discover new packages
static KNOWLEDGE_CACHE: OnceCell<Mutex<KnowledgeCache>> = OnceCell::new();

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
    let cache = cache_mutex
        .lock()
        .ok()
        .ok_or_else(|| std::io::Error::other("Failed to lock knowledge cache"))?;

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
        .rfind(|version| version_satisfies(version, constraint))
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
        let base = match trimmed.find(['<', '>', '!', '=', '~']) {
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
    if let Some(versions_arr) = json.get("versions").and_then(|v| v.as_array()) {
        let mut versions: Vec<String> = versions_arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        if !versions.is_empty() {
            versions.sort_by_key(|a| kgraph_db::version_sort_key(a));
            return versions;
        }
    }
    if let Some(versions_obj) = json.get("versions").and_then(|v| v.as_object()) {
        let mut versions: Vec<String> = versions_obj.keys().cloned().collect();
        versions.sort_by_key(|a| kgraph_db::version_sort_key(a));
        return versions;
    }
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
        versions.sort_by_key(|a| kgraph_db::version_sort_key(a));
        return versions;
    }
    Vec::new()
}

fn extract_version_from_filename(filename: &str, package_name: &str) -> Option<String> {
    let normalized = normalize(package_name);
    let prefix = format!("{}-", normalized);
    let lower = filename.to_lowercase().replace('-', "_");
    let norm_prefix = prefix.to_lowercase().replace('-', "_");
    if !lower.starts_with(&norm_prefix) {
        return None;
    }
    let rest = &filename[prefix.len()..].replace(&normalized, "");
    let version_end = rest
        .find(['-', '.'])
        .filter(|&pos| pos > 0)
        .unwrap_or(rest.len());
    let candidate = &rest[..version_end];
    if candidate.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        Some(candidate.to_string())
    } else {
        None
    }
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
