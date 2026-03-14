use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use flate2::read::GzDecoder;
use serde_json::Value;

/// In-memory knowledge cache for fast package version and dependency lookups.
/// Loads compressed package metadata from pre-indexed data files.
pub struct KnowledgeCache {
    /// Maps package name -> version -> numeric ID
    labels: HashMap<String, HashMap<String, u32>>,
    /// Maps numeric ID -> "package-version" string
    labels_by_id: HashMap<u32, String>,
    /// Maps package ID -> dependency map (dep_package -> list of compatible dep IDs)
    dependencies: HashMap<u32, HashMap<String, Vec<u32>>>,
    /// Maps package ID -> list of all dependency IDs
    dependency_sets: HashMap<u32, Vec<u32>>,
}

impl KnowledgeCache {
    /// Create an empty knowledge cache (used as fallback when data files are unavailable)
    pub fn new_empty() -> Self {
        Self {
            labels: HashMap::new(),
            labels_by_id: HashMap::new(),
            dependencies: HashMap::new(),
            dependency_sets: HashMap::new(),
        }
    }

    /// Load knowledge cache from compressed data files in the specified directory
    pub fn load_from_directory(data_dir: &Path) -> io::Result<Self> {
        let labels_path = data_dir.join("label.shrink");
        let dependencies_path = data_dir.join("dependency.shrink");
        let dep_sets_path = data_dir.join("dep_set.shrink");

        let labels_json = read_gzip_json(&labels_path)?;
        let dependencies_json = read_gzip_json(&dependencies_path)?;
        let dep_sets_json = read_gzip_json(&dep_sets_path)?;

        let labels = parse_labels(&labels_json);
        let labels_by_id = build_labels_by_id(&labels);
        let dependencies = parse_dependencies(&dependencies_json);
        let dependency_sets = parse_dependency_sets(&dep_sets_json);

        Ok(Self {
            labels,
            labels_by_id,
            dependencies,
            dependency_sets,
        })
    }

    /// Get all available versions for a package
    pub fn get_versions(&self, package_name: &str) -> Option<Vec<String>> {
        self.labels.get(package_name).map(|versions| {
            let mut version_list: Vec<String> = versions.keys().cloned().collect();
            version_list.sort();
            version_list
        })
    }

    /// Get dependencies for a specific package version
    pub fn get_dependencies(&self, package_name: &str, version: &str) -> Option<Vec<String>> {
        // Get package ID
        let package_id = self.labels.get(package_name)?.get(version)?;

        // Get dependency IDs
        let dep_ids = self.dependency_sets.get(package_id)?;

        // Convert IDs to package-version strings
        let deps: Vec<String> = dep_ids
            .iter()
            .filter_map(|id| self.labels_by_id.get(id).cloned())
            .collect();

        Some(deps)
    }

    /// Get package name and version from numeric ID
    pub fn get_package_by_id(&self, id: u32) -> Option<&str> {
        self.labels_by_id.get(&id).map(|s| s.as_str())
    }

    /// Check if a package exists in the cache
    pub fn has_package(&self, package_name: &str) -> bool {
        self.labels.contains_key(package_name)
    }

    /// Add a package version to the cache (learning mode)
    pub fn add_package_version(&mut self, package_name: &str, version: &str) {
        // Check if version already exists
        if let Some(versions) = self.labels.get(package_name) {
            if versions.contains_key(version) {
                return; // Already exists
            }
        }

        // Generate new ID before mutable borrow
        let new_id = self.next_available_id();

        // Now insert
        let versions = self.labels.entry(package_name.to_string()).or_insert_with(HashMap::new);
        versions.insert(version.to_string(), new_id);
        self.labels_by_id.insert(new_id, format!("{}-{}", package_name, version));
    }

    /// Add dependency information for a package version (learning mode)
    pub fn add_dependencies(&mut self, package_name: &str, version: &str, dep_specs: &[String]) {
        // Ensure the package version exists
        self.add_package_version(package_name, version);

        // Get the package ID
        if let Some(package_id) = self.labels.get(package_name).and_then(|v| v.get(version)) {
            let package_id = *package_id;

            // Parse dependency specs to get package names and versions
            let dep_ids: Vec<u32> = dep_specs
                .iter()
                .filter_map(|spec| {
                    // Extract package name from spec (before any operator)
                    let dep_name = extract_package_name(spec);
                    if dep_name.is_empty() {
                        return None;
                    }

                    // For now, just track that this dependency exists
                    // We'll get its specific version when we resolve it
                    self.labels.get(&dep_name).and_then(|versions| {
                        versions.values().next().copied()
                    })
                })
                .collect();

            // Update dependency sets
            self.dependency_sets.insert(package_id, dep_ids);
        }
    }

    /// Save the knowledge cache back to disk (persists learned knowledge)
    pub fn save_to_directory(&self, data_dir: &Path) -> io::Result<()> {
        use std::fs::create_dir_all;
        use flate2::write::GzEncoder;
        use flate2::Compression;

        create_dir_all(data_dir)?;

        // Save labels
        let labels_json = serde_json::to_value(&self.labels)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let labels_path = data_dir.join("label.shrink");
        let file = std::fs::File::create(labels_path)?;
        let mut encoder = GzEncoder::new(file, Compression::fast());
        serde_json::to_writer(&mut encoder, &labels_json)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        encoder.finish()?;

        // Save dependency sets
        let dep_sets_json = serde_json::to_value(&self.dependency_sets)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let dep_sets_path = data_dir.join("dep_set.shrink");
        let file = std::fs::File::create(dep_sets_path)?;
        let mut encoder = GzEncoder::new(file, Compression::fast());
        serde_json::to_writer(&mut encoder, &dep_sets_json)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        encoder.finish()?;

        // Save dependencies (keep empty structure for compatibility)
        let dependencies_json = serde_json::to_value(&self.dependencies)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let dependencies_path = data_dir.join("dependency.shrink");
        let file = std::fs::File::create(dependencies_path)?;
        let mut encoder = GzEncoder::new(file, Compression::fast());
        serde_json::to_writer(&mut encoder, &dependencies_json)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        encoder.finish()?;

        Ok(())
    }

    /// Get the next available ID for new entries
    fn next_available_id(&self) -> u32 {
        self.labels_by_id.keys().max().map(|id| id + 1).unwrap_or(1)
    }

    /// Get statistics about the cache
    pub fn stats(&self) -> CacheStats {
        let total_packages = self.labels.len();
        let total_versions: usize = self.labels.values().map(|v| v.len()).sum();
        let total_dependencies = self.dependency_sets.len();

        CacheStats {
            packages: total_packages,
            versions: total_versions,
            dependencies: total_dependencies,
        }
    }
}

/// Statistics about the knowledge cache
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub packages: usize,
    pub versions: usize,
    pub dependencies: usize,
}

/// Extract package name from a dependency spec (e.g., "requests>=2.0" -> "requests")
fn extract_package_name(spec: &str) -> String {
    let trimmed = spec.trim();
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
    without_extras.trim().to_string()
}

/// Read and decompress a gzipped JSON file
fn read_gzip_json(path: &Path) -> io::Result<Value> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let decoder = GzDecoder::new(reader);
    let value: Value = serde_json::from_reader(decoder)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(value)
}

/// Parse labels JSON into internal format
/// Input format: {"package": {"version": id, "*": wildcard_id}}
fn parse_labels(json: &Value) -> HashMap<String, HashMap<String, u32>> {
    let mut labels = HashMap::new();

    if let Some(obj) = json.as_object() {
        for (package, versions_obj) in obj {
            if let Some(versions_map) = versions_obj.as_object() {
                let mut version_ids = HashMap::new();
                for (version, id) in versions_map {
                    if version != "*" {
                        // Skip wildcard entries
                        if let Some(id_num) = id.as_u64() {
                            version_ids.insert(version.clone(), id_num as u32);
                        }
                    }
                }
                if !version_ids.is_empty() {
                    labels.insert(package.clone(), version_ids);
                }
            }
        }
    }

    labels
}

/// Build reverse mapping from ID to "package-version"
fn build_labels_by_id(labels: &HashMap<String, HashMap<String, u32>>) -> HashMap<u32, String> {
    let mut by_id = HashMap::new();

    for (package, versions) in labels {
        for (version, id) in versions {
            by_id.insert(*id, format!("{}-{}", package, version));
        }
    }

    by_id
}

/// Parse dependencies JSON
/// Input format: {"id": {"dep_package": [dep_id1, dep_id2, ...]}}
fn parse_dependencies(json: &Value) -> HashMap<u32, HashMap<String, Vec<u32>>> {
    let mut dependencies = HashMap::new();

    if let Some(obj) = json.as_object() {
        for (id_str, deps_obj) in obj {
            if let Ok(id) = id_str.parse::<u32>() {
                if let Some(deps_map) = deps_obj.as_object() {
                    let mut dep_map = HashMap::new();
                    for (dep_package, dep_ids) in deps_map {
                        if let Some(ids_array) = dep_ids.as_array() {
                            let ids: Vec<u32> = ids_array
                                .iter()
                                .filter_map(|v| v.as_u64().map(|n| n as u32))
                                .collect();
                            dep_map.insert(dep_package.clone(), ids);
                        }
                    }
                    dependencies.insert(id, dep_map);
                }
            }
        }
    }

    dependencies
}

/// Parse dependency sets JSON
/// Input format: {"id": [dep_id1, dep_id2, ...]}
fn parse_dependency_sets(json: &Value) -> HashMap<u32, Vec<u32>> {
    let mut dep_sets = HashMap::new();

    if let Some(obj) = json.as_object() {
        for (id_str, ids_array) in obj {
            if let Ok(id) = id_str.parse::<u32>() {
                if let Some(array) = ids_array.as_array() {
                    let ids: Vec<u32> = array
                        .iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u32))
                        .collect();
                    dep_sets.insert(id, ids);
                }
            }
        }
    }

    dep_sets
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_knowledge_cache_loads() {
        let data_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/knowledge");
        if data_dir.exists() {
            let cache = KnowledgeCache::load_from_directory(&data_dir);
            assert!(cache.is_ok());

            if let Ok(cache) = cache {
                // Test that we can query for common packages
                assert!(cache.has_package("requests") || cache.labels.len() > 0);
            }
        }
    }
}
