use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use rusqlite::Connection;

/// Cached connection to the KGraph SQLite database.
/// Opened once on first access and reused for the lifetime of the process.
static KGRAPH_CONNECTION: OnceLock<Mutex<Option<Connection>>> = OnceLock::new();

/// Path that was used to open the cached connection.
static KGRAPH_DB_PATH: OnceLock<PathBuf> = OnceLock::new();

fn get_connection(db_path: &Path) -> &'static Mutex<Option<Connection>> {
    KGRAPH_CONNECTION.get_or_init(|| {
        let conn = if db_path.exists() {
            Connection::open(db_path).ok()
        } else {
            None
        };
        if conn.is_some() {
            let _ = KGRAPH_DB_PATH.set(db_path.to_path_buf());
        }
        Mutex::new(conn)
    })
}

fn normalize(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .replace('.', "-")
}

/// Fetch all versions for a package from the KGraph SQLite DB.
/// Returns an empty Vec if the DB is unavailable or the package is not found.
pub fn kgraph_versions(db_path: &Path, package: &str) -> Vec<String> {
    let guard = match get_connection(db_path).lock() {
        Ok(g) => g,
        Err(_) => return Vec::new(),
    };
    let Some(conn) = guard.as_ref() else {
        return Vec::new();
    };
    let normalized = normalize(package);
    let mut stmt = match conn.prepare_cached("SELECT version FROM versions WHERE package = ?1") {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let rows = stmt.query_map([&normalized], |row| row.get::<_, String>(0));
    let Ok(rows) = rows else {
        return Vec::new();
    };
    let mut versions: Vec<String> = rows.filter_map(|r| r.ok()).collect();
    versions.sort_unstable();
    versions.dedup();
    versions.sort_by(|a, b| compare_version_keys(a, b));
    versions
}

/// Fetch dependency specs for a specific package version from the KGraph SQLite DB.
pub fn kgraph_dependency_specs(db_path: &Path, package: &str, version: &str) -> Vec<String> {
    let guard = match get_connection(db_path).lock() {
        Ok(g) => g,
        Err(_) => return Vec::new(),
    };
    let Some(conn) = guard.as_ref() else {
        return Vec::new();
    };
    let normalized = normalize(package);
    let mut stmt = match conn.prepare_cached(
        "SELECT spec FROM deps WHERE package = ?1 AND version = ?2",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let rows = stmt.query_map(rusqlite::params![&normalized, version], |row| {
        row.get::<_, String>(0)
    });
    let Ok(rows) = rows else {
        return Vec::new();
    };
    rows.filter_map(|r| r.ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Bulk-prefetch versions and dependency specs for a set of packages.
/// Returns a map of package_name -> (versions, deps_by_version).
pub fn kgraph_bulk_prefetch(
    db_path: &Path,
    packages: &[String],
) -> BTreeMap<String, (Vec<String>, BTreeMap<String, Vec<String>>)> {
    let mut results = BTreeMap::new();
    let guard = match get_connection(db_path).lock() {
        Ok(g) => g,
        Err(_) => return results,
    };
    let Some(conn) = guard.as_ref() else {
        return results;
    };

    let mut ver_stmt = match conn.prepare_cached("SELECT version FROM versions WHERE package = ?1")
    {
        Ok(s) => s,
        Err(_) => return results,
    };
    let mut dep_stmt = match conn
        .prepare_cached("SELECT spec FROM deps WHERE package = ?1 AND version = ?2")
    {
        Ok(s) => s,
        Err(_) => return results,
    };

    for package in packages {
        let normalized = normalize(package);
        let rows = ver_stmt.query_map([&normalized], |row| row.get::<_, String>(0));
        let Ok(rows) = rows else { continue };
        let mut versions: Vec<String> = rows.filter_map(|r| r.ok()).collect();
        versions.sort_unstable();
        versions.dedup();
        versions.sort_by(|a, b| compare_version_keys(a, b));
        if versions.is_empty() {
            continue;
        }

        let mut deps_by_version = BTreeMap::new();
        for version in &versions {
            let dep_rows =
                dep_stmt.query_map(rusqlite::params![&normalized, version], |row| {
                    row.get::<_, String>(0)
                });
            let Ok(dep_rows) = dep_rows else { continue };
            let specs: Vec<String> = dep_rows
                .filter_map(|r| r.ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !specs.is_empty() {
                deps_by_version.insert(version.clone(), specs);
            }
        }

        results.insert(normalized, (versions, deps_by_version));
    }
    results
}

/// Check if the KGraph DB file exists and can be opened.
pub fn db_available(db_path: &Path) -> bool {
    if !db_path.exists() {
        return false;
    }
    let guard = match get_connection(db_path).lock() {
        Ok(g) => g,
        Err(_) => return false,
    };
    guard.is_some()
}

// ---------------------------------------------------------------------------
// Version sorting (replicates Python's version_key for KGraph compatibility)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
enum VersionToken {
    Num(u64),
    Str(String),
}

impl Ord for VersionToken {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (VersionToken::Num(a), VersionToken::Num(b)) => a.cmp(b),
            (VersionToken::Str(a), VersionToken::Str(b)) => a.cmp(b),
            // Numbers sort before strings (matching Python behavior where int < str)
            (VersionToken::Num(_), VersionToken::Str(_)) => Ordering::Less,
            (VersionToken::Str(_), VersionToken::Num(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for VersionToken {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn version_sort_key(version: &str) -> Vec<VersionToken> {
    let mut tokens = Vec::new();
    let mut current_digits = String::new();
    for ch in version.chars() {
        if ch.is_ascii_digit() {
            current_digits.push(ch);
        } else {
            if !current_digits.is_empty() {
                tokens.push(VersionToken::Num(
                    current_digits.parse::<u64>().unwrap_or(0),
                ));
                current_digits.clear();
            }
            tokens.push(VersionToken::Str(ch.to_string()));
        }
    }
    if !current_digits.is_empty() {
        tokens.push(VersionToken::Num(
            current_digits.parse::<u64>().unwrap_or(0),
        ));
    }
    tokens
}

fn compare_version_keys(a: &str, b: &str) -> Ordering {
    let key_a = version_sort_key(a);
    let key_b = version_sort_key(b);
    key_a.cmp(&key_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_sort_key_orders_numerically() {
        let mut versions = vec![
            "1.0".to_string(),
            "1.10".to_string(),
            "1.2".to_string(),
            "1.1".to_string(),
            "2.0".to_string(),
        ];
        versions.sort_by(|a, b| compare_version_keys(a, b));
        assert_eq!(versions, vec!["1.0", "1.1", "1.2", "1.10", "2.0"]);
    }

    #[test]
    fn version_sort_key_handles_prerelease() {
        let mut versions = vec![
            "1.0a1".to_string(),
            "1.0".to_string(),
            "1.0b2".to_string(),
        ];
        versions.sort_by(|a, b| compare_version_keys(a, b));
        // 'a' < 'b' < numeric-only, so a1 < b2 < bare 1.0
        // Actually: 1.0a1 = [1,'.','0','a',1], 1.0 = [1,'.','0], 1.0b2 = [1,'.','0','b',2]
        // Since Num < Str: [1,.,0] < [1,.,0,a,1] because at index 3, nothing vs 'a'
        // Actually with Vec comparison: shorter vec is less if prefix matches
        assert_eq!(versions, vec!["1.0", "1.0a1", "1.0b2"]);
    }
}
