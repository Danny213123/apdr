use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use rusqlite::Connection;

/// Connection pool for the KGraph SQLite database.
/// Allows multiple concurrent readers without mutex contention.
struct ConnectionPool {
    connections: Mutex<Vec<Connection>>,
    db_path: PathBuf,
    max_size: usize,
    available: bool,
}

/// RAII guard that returns the connection to the pool on drop.
struct PooledConnection<'a> {
    conn: Option<Connection>,
    pool: &'a ConnectionPool,
}

impl std::ops::Deref for PooledConnection<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if let Ok(mut conns) = self.pool.connections.lock() {
                if conns.len() < self.pool.max_size {
                    conns.push(conn);
                }
            }
        }
    }
}

impl ConnectionPool {
    fn new(db_path: &Path, max_size: usize) -> Self {
        ConnectionPool {
            available: db_path.exists(),
            connections: Mutex::new(Vec::with_capacity(max_size)),
            db_path: db_path.to_path_buf(),
            max_size,
        }
    }

    fn get(&self) -> Option<PooledConnection<'_>> {
        if !self.available {
            return None;
        }
        // Try to reuse an existing connection from the pool.
        if let Ok(mut conns) = self.connections.lock() {
            if let Some(conn) = conns.pop() {
                return Some(PooledConnection {
                    conn: Some(conn),
                    pool: self,
                });
            }
        }
        // Open a new read-only connection with immutable flag for optimal read performance.
        let uri = format!("file:{}?immutable=1", self.db_path.display());
        let conn = Connection::open_with_flags(
            &uri,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .ok()?;
        conn.execute_batch(
            "PRAGMA mmap_size = 268435456; PRAGMA cache_size = -65536; PRAGMA temp_store = MEMORY;",
        )
        .ok()?;
        Some(PooledConnection {
            conn: Some(conn),
            pool: self,
        })
    }
}

static KGRAPH_POOL: OnceLock<ConnectionPool> = OnceLock::new();

fn get_pool(db_path: &Path) -> &'static ConnectionPool {
    KGRAPH_POOL.get_or_init(|| ConnectionPool::new(db_path, 16))
}

fn normalize(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace(['_', '.'], "-")
}

type BulkPrefetchEntry = (Vec<String>, BTreeMap<String, Vec<String>>);

/// Fetch all versions for a package from the KGraph SQLite DB.
/// Returns an empty Vec if the DB is unavailable or the package is not found.
pub fn kgraph_versions(db_path: &Path, package: &str) -> Vec<String> {
    let Some(conn) = get_pool(db_path).get() else {
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
    let Some(conn) = get_pool(db_path).get() else {
        return Vec::new();
    };
    let normalized = normalize(package);
    let mut stmt =
        match conn.prepare_cached("SELECT spec FROM deps WHERE package = ?1 AND version = ?2") {
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
/// Uses SQL IN clause for batch queries instead of N individual queries (#10).
/// Returns a map of package_name -> (versions, deps_by_version).
pub fn kgraph_bulk_prefetch(
    db_path: &Path,
    packages: &[String],
) -> BTreeMap<String, BulkPrefetchEntry> {
    let mut results = BTreeMap::new();
    if packages.is_empty() {
        return results;
    }
    let Some(conn) = get_pool(db_path).get() else {
        return results;
    };

    let normalized: Vec<String> = packages.iter().map(|p| normalize(p)).collect();

    // Single LEFT JOIN query: fetch versions and their deps in one round-trip
    // instead of two separate queries. Rows with no deps have spec = NULL.
    let placeholders: String = normalized.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT v.package, v.version, d.spec \
         FROM versions v \
         LEFT JOIN deps d ON v.package = d.package AND v.version = d.version \
         WHERE v.package IN ({})",
        placeholders
    );
    if let Ok(mut stmt) = conn.prepare(&sql) {
        let params: Vec<&dyn rusqlite::types::ToSql> = normalized
            .iter()
            .map(|s| s as &dyn rusqlite::types::ToSql)
            .collect();
        if let Ok(rows) = stmt.query_map(params.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        }) {
            for row in rows.flatten() {
                let (pkg, version, spec) = row;
                let entry = results
                    .entry(pkg)
                    .or_insert_with(|| (Vec::new(), BTreeMap::new()));
                entry.0.push(version.clone());
                if let Some(spec) = spec {
                    let spec = spec.trim().to_string();
                    if !spec.is_empty() {
                        entry.1.entry(version).or_insert_with(Vec::new).push(spec);
                    }
                }
            }
        }
    }

    // Sort and dedup versions for each package (JOIN produces duplicates
    // when a version has multiple deps).
    for (_, (versions, _)) in results.iter_mut() {
        versions.sort_unstable();
        versions.dedup();
        versions.sort_by(|a, b| compare_version_keys(a, b));
    }

    // Remove packages with no versions
    results.retain(|_, (versions, _)| !versions.is_empty());

    results
}

/// Check if the KGraph DB file exists and can be opened.
pub fn db_available(db_path: &Path) -> bool {
    if !db_path.exists() {
        return false;
    }
    get_pool(db_path).get().is_some()
}

/// #3: Find KGraph packages whose normalized name contains or matches a pattern.
/// Used to build tier2 candidates for LLM prompt injection.
/// Returns up to `limit` package names that are similar to the import name.
pub fn kgraph_candidate_packages(db_path: &Path, import_name: &str, limit: usize) -> Vec<String> {
    let Some(conn) = get_pool(db_path).get() else {
        return Vec::new();
    };
    let norm = normalize(import_name);
    if norm.is_empty() {
        return Vec::new();
    }

    let mut candidates = Vec::new();

    // Strategy 1: Exact match (import_name == package_name after normalization)
    if let Ok(mut stmt) =
        conn.prepare_cached("SELECT DISTINCT package FROM versions WHERE package = ?1 LIMIT 1")
    {
        if let Ok(rows) = stmt.query_map([&norm], |row| row.get::<_, String>(0)) {
            for row in rows.flatten() {
                if !candidates.contains(&row) {
                    candidates.push(row);
                }
            }
        }
    }

    // Strategy 2: Common prefix/suffix patterns (e.g. cv2 -> opencv-python, PIL -> pillow)
    // Use LIKE patterns: "python-{name}", "{name}-python", "py{name}", "{name}"
    let patterns = vec![
        format!("python-{}", norm),
        format!("{}-python", norm),
        format!("py{}", norm),
        format!("{}py", norm),
    ];
    for pattern in &patterns {
        if candidates.len() >= limit {
            break;
        }
        if let Ok(mut stmt) =
            conn.prepare_cached("SELECT DISTINCT package FROM versions WHERE package = ?1 LIMIT 1")
        {
            if let Ok(rows) = stmt.query_map([pattern], |row| row.get::<_, String>(0)) {
                for row in rows.flatten() {
                    if !candidates.contains(&row) {
                        candidates.push(row);
                    }
                }
            }
        }
    }

    // Strategy 3: LIKE containment search (e.g. "cv2" -> "opencv-python-headless")
    if candidates.len() < limit {
        let like_pattern = format!("%{}%", norm);
        if let Ok(mut stmt) = conn
            .prepare_cached("SELECT DISTINCT package FROM versions WHERE package LIKE ?1 LIMIT ?2")
        {
            let remaining = (limit - candidates.len()) as i64;
            if let Ok(rows) = stmt
                .query_map(rusqlite::params![&like_pattern, remaining + 5], |row| {
                    row.get::<_, String>(0)
                })
            {
                for row in rows.flatten() {
                    if candidates.len() >= limit {
                        break;
                    }
                    if !candidates.contains(&row) && row != norm {
                        candidates.push(row);
                    }
                }
            }
        }
    }

    candidates.truncate(limit);
    candidates
}

// ---------------------------------------------------------------------------
// Version sorting (replicates Python's version_key for KGraph compatibility)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VersionToken {
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

pub(crate) fn version_sort_key(version: &str) -> Vec<VersionToken> {
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
        let mut versions = vec!["1.0a1".to_string(), "1.0".to_string(), "1.0b2".to_string()];
        versions.sort_by(|a, b| compare_version_keys(a, b));
        // 'a' < 'b' < numeric-only, so a1 < b2 < bare 1.0
        // Actually: 1.0a1 = [1,'.','0','a',1], 1.0 = [1,'.','0], 1.0b2 = [1,'.','0','b',2]
        // Since Num < Str: [1,.,0] < [1,.,0,a,1] because at index 3, nothing vs 'a'
        // Actually with Vec comparison: shorter vec is less if prefix matches
        assert_eq!(versions, vec!["1.0", "1.0a1", "1.0b2"]);
    }
}
