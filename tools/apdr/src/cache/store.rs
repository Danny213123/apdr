use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::{CacheStats, FailurePattern};

#[derive(Clone, Debug)]
pub struct PackageRecord {
    pub import_name: String,
    pub package_name: String,
    pub default_version: Option<String>,
    pub source: String,
}

#[derive(Clone, Debug, Default)]
pub struct CacheStore {
    pub tool_root: PathBuf,
    pub cache_path: PathBuf,
    pub import_map: BTreeMap<String, PackageRecord>,
    pub version_constraints: BTreeMap<String, String>,
    pub resolved_lockfiles: BTreeMap<String, String>,
    pub build_artifacts: BTreeMap<String, String>,
    pub package_artifacts: BTreeMap<String, String>,
    pub failure_patterns: Vec<FailurePattern>,
    pub pypi_index: BTreeMap<String, Vec<String>>,
    pub dependency_graph: BTreeMap<String, Vec<String>>,
    pub version_dependency_specs: BTreeMap<String, Vec<String>>,
}

impl CacheStore {
    pub fn load(tool_root: &Path, cache_path: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&cache_path)?;
        fs::create_dir_all(cache_path.join("lockfiles"))?;

        let mut store = CacheStore {
            tool_root: tool_root.to_path_buf(),
            cache_path,
            ..Default::default()
        };
        store.load_seed_imports()?;
        store.load_dynamic_imports()?;
        store.load_version_constraints()?;
        store.load_failure_patterns()?;
        store.load_dynamic_failure_patterns()?;
        store.load_version_index()?;
        store.load_dynamic_version_index()?;
        store.load_dependency_graph()?;
        store.load_dynamic_dependency_graph()?;
        store.load_version_dependency_specs()?;
        store.load_lockfiles()?;
        store.load_build_artifacts()?;
        store.load_package_artifacts()?;
        Ok(store)
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            import_mappings: self.import_map.len(),
            failure_patterns: self.failure_patterns.len(),
            version_constraints: self.version_constraints.len(),
            lockfile_entries: self.resolved_lockfiles.len(),
            build_artifacts: self.build_artifacts.len(),
            pypi_index_entries: self.pypi_index.len(),
            dependency_graph_entries: self.dependency_graph.len(),
        }
    }

    pub fn import_lookup(&self, import_name: &str) -> Option<&PackageRecord> {
        self.import_map.get(&normalize(import_name))
    }

    pub fn lockfile(&self, key: &str) -> Option<&String> {
        self.resolved_lockfiles.get(key)
    }

    pub fn build_artifact(&self, key: &str) -> Option<&String> {
        self.build_artifacts.get(key)
    }

    pub fn package_artifact(
        &self,
        python_version: &str,
        package_name: &str,
        version: &str,
    ) -> Option<&String> {
        self.package_artifacts
            .get(&package_artifact_key(python_version, package_name, version))
    }

    pub fn package_artifact_versions(
        &self,
        python_version: &str,
        package_name: &str,
    ) -> Vec<(String, String)> {
        let prefix = format!("{}\t{}\t", python_version.trim(), normalize(package_name));
        self.package_artifacts
            .iter()
            .filter_map(|(key, value)| {
                if !key.starts_with(&prefix) {
                    return None;
                }
                let version = key.rsplit('\t').next()?.trim().to_string();
                Some((version, value.clone()))
            })
            .collect()
    }

    pub fn save_import_mapping(
        &mut self,
        import_name: &str,
        package_name: &str,
        default_version: Option<&str>,
        source: &str,
    ) -> io::Result<()> {
        let record = PackageRecord {
            import_name: normalize(import_name),
            package_name: package_name.trim().to_string(),
            default_version: default_version
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            source: source.trim().to_string(),
        };
        if let Some(existing) = self.import_map.get(&record.import_name) {
            if !should_replace_import_record(existing, &record) {
                return Ok(());
            }
        }
        self.import_map
            .insert(record.import_name.clone(), record.clone());
        let path = self.cache_path.join("dynamic_imports.tsv");
        let mut rows = self
            .import_map
            .values()
            .filter(|item| item.source != "seed" && item.source != "discrepancy")
            .map(|item| {
                format!(
                    "{}\t{}\t{}\t{}",
                    item.import_name,
                    item.package_name,
                    item.default_version.clone().unwrap_or_default(),
                    item.source
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        fs::write(
            path,
            rows.join("\n") + if rows.is_empty() { "" } else { "\n" },
        )?;
        Ok(())
    }

    pub fn save_version_constraint(
        &mut self,
        api_usage: &str,
        min_version: &str,
    ) -> io::Result<()> {
        self.version_constraints
            .insert(api_usage.trim().to_string(), min_version.trim().to_string());
        let rows = self
            .version_constraints
            .iter()
            .map(|(key, value)| format!("{key}\t{value}"))
            .collect::<Vec<_>>();
        fs::write(
            self.cache_path.join("version_constraints.tsv"),
            rows.join("\n") + "\n",
        )?;
        Ok(())
    }

    pub fn save_lockfile(&mut self, key: &str, requirements_txt: &str) -> io::Result<()> {
        let normalized = key.trim().to_string();
        self.resolved_lockfiles
            .insert(normalized.clone(), requirements_txt.to_string());
        fs::write(
            self.cache_path
                .join("lockfiles")
                .join(format!("{normalized}.txt")),
            requirements_txt,
        )?;
        Ok(())
    }

    pub fn save_build_artifact(&mut self, key: &str, image_tag: &str) -> io::Result<()> {
        self.build_artifacts
            .insert(key.trim().to_string(), image_tag.trim().to_string());
        let rows = self
            .build_artifacts
            .iter()
            .map(|(artifact_key, tag)| format!("{artifact_key}\t{tag}"))
            .collect::<Vec<_>>();
        fs::write(
            self.cache_path.join("build_artifacts.tsv"),
            rows.join("\n") + "\n",
        )?;
        Ok(())
    }

    pub fn save_dependency_graph_entry(
        &mut self,
        package_name: &str,
        dependencies: &[String],
    ) -> io::Result<()> {
        let normalized = normalize(package_name);
        self.dependency_graph
            .insert(normalized.clone(), dependencies.to_vec());
        let mut rows = self
            .dependency_graph
            .iter()
            .map(|(name, deps)| format!("{name}\t{}", deps.join(",")))
            .collect::<Vec<_>>();
        rows.sort();
        fs::write(
            self.cache_path.join("dynamic_dependency_graph.tsv"),
            rows.join("\n") + if rows.is_empty() { "" } else { "\n" },
        )?;
        Ok(())
    }

    pub fn save_version_dependency_specs(
        &mut self,
        package_name: &str,
        version: &str,
        specs: &[String],
    ) -> io::Result<()> {
        let key = version_dependency_key(package_name, version);
        self.version_dependency_specs.insert(key, specs.to_vec());
        let mut rows = self
            .version_dependency_specs
            .iter()
            .map(|(dep_key, values)| format!("{dep_key}\t{}", values.join(",")))
            .collect::<Vec<_>>();
        rows.sort();
        fs::write(
            self.cache_path.join("dynamic_dependency_specs.tsv"),
            rows.join("\n") + if rows.is_empty() { "" } else { "\n" },
        )?;
        Ok(())
    }

    pub fn version_dependency_specs(
        &self,
        package_name: &str,
        version: &str,
    ) -> Option<&Vec<String>> {
        self.version_dependency_specs
            .get(&version_dependency_key(package_name, version))
    }

    pub fn save_package_artifact(
        &mut self,
        python_version: &str,
        package_name: &str,
        version: &str,
        artifact_dir: &str,
    ) -> io::Result<()> {
        self.package_artifacts.insert(
            package_artifact_key(python_version, package_name, version),
            artifact_dir.trim().to_string(),
        );
        let mut rows = self
            .package_artifacts
            .iter()
            .map(|(key, value)| format!("{key}\t{value}"))
            .collect::<Vec<_>>();
        rows.sort();
        fs::write(
            self.cache_path.join("package_artifacts.tsv"),
            rows.join("\n") + if rows.is_empty() { "" } else { "\n" },
        )?;
        Ok(())
    }

    pub fn save_failure_pattern(&mut self, pattern: FailurePattern) -> io::Result<()> {
        self.record_failure_pattern_outcome(
            &pattern.pattern,
            &pattern.error_type,
            &pattern.conflict_class,
            &pattern.fix,
            pattern.success_rate >= 0.5,
        )
    }

    pub fn record_failure_pattern_outcome(
        &mut self,
        pattern: &str,
        error_type: &str,
        conflict_class: &str,
        fix: &str,
        succeeded: bool,
    ) -> io::Result<()> {
        let normalized_pattern = pattern.trim().to_string();
        let normalized_error_type = error_type.trim().to_string();
        let normalized_fix = fix.trim().to_string();
        let normalized_conflict_class = conflict_class.trim().to_string();

        if let Some(existing) = self.failure_patterns.iter_mut().find(|item| {
            item.pattern.eq_ignore_ascii_case(&normalized_pattern)
                && item.error_type.eq_ignore_ascii_case(&normalized_error_type)
                && item.fix.eq_ignore_ascii_case(&normalized_fix)
        }) {
            let total = existing.times_applied.max(1);
            let successes = existing.success_rate * f64::from(total);
            let updated_total = total + 1;
            let updated_successes = successes + if succeeded { 1.0 } else { 0.0 };
            existing.conflict_class = normalized_conflict_class;
            existing.times_applied = updated_total;
            existing.success_rate = updated_successes / f64::from(updated_total);
        } else {
            self.failure_patterns.push(FailurePattern {
                pattern: normalized_pattern,
                error_type: normalized_error_type,
                conflict_class: normalized_conflict_class,
                fix: normalized_fix,
                success_rate: if succeeded { 1.0 } else { 0.0 },
                times_applied: 1,
            });
        }

        self.persist_failure_patterns()
    }

    pub fn save_pypi_versions(
        &mut self,
        package_name: &str,
        versions: &[String],
    ) -> io::Result<()> {
        self.pypi_index
            .insert(normalize(package_name), versions.to_vec());
        let rows = self
            .pypi_index
            .iter()
            .map(|(name, values)| format!("{name}\t{}", values.join(",")))
            .collect::<Vec<_>>();
        fs::write(
            self.cache_path.join("dynamic_pypi_index.tsv"),
            rows.join("\n") + "\n",
        )?;
        Ok(())
    }

    pub fn import_records(&self) -> Vec<PackageRecord> {
        self.import_map.values().cloned().collect()
    }

    fn load_seed_imports(&mut self) -> io::Result<()> {
        let paths = [
            self.tool_root.join("data/seed/top_5000_mappings.tsv"),
            self.tool_root.join("data/seed/name_discrepancies.tsv"),
            self.tool_root.join("data/seed/reference_aliases.tsv"),
        ];

        for path in paths {
            self.load_import_file(&path)?;
        }
        Ok(())
    }

    fn load_dynamic_imports(&mut self) -> io::Result<()> {
        self.load_import_file(&self.cache_path.join("dynamic_imports.tsv"))
    }

    fn load_import_file(&mut self, path: &Path) -> io::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts = trimmed.split('\t').collect::<Vec<_>>();
            if parts.len() < 2 {
                continue;
            }
            let record = PackageRecord {
                import_name: normalize(parts[0]),
                package_name: parts[1].trim().to_string(),
                default_version: parts
                    .get(2)
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty()),
                source: parts.get(3).copied().unwrap_or("seed").to_string(),
            };
            let should_replace = self
                .import_map
                .get(&record.import_name)
                .map(|existing| should_replace_import_record(existing, &record))
                .unwrap_or(true);
            if should_replace {
                self.import_map.insert(record.import_name.clone(), record);
            }
        }
        Ok(())
    }

    fn load_version_constraints(&mut self) -> io::Result<()> {
        let path = self.cache_path.join("version_constraints.tsv");
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = trimmed.split_once('\t') {
                self.version_constraints
                    .insert(key.trim().to_string(), value.trim().to_string());
            }
        }
        Ok(())
    }

    fn load_failure_patterns(&mut self) -> io::Result<()> {
        self.load_failure_pattern_file(
            &self.tool_root.join("data/seed/common_failure_patterns.tsv"),
        )
    }

    fn load_dynamic_failure_patterns(&mut self) -> io::Result<()> {
        self.load_failure_pattern_file(&self.cache_path.join("dynamic_failure_patterns.tsv"))
    }

    fn load_failure_pattern_file(&mut self, path: &Path) -> io::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts = trimmed.split('\t').collect::<Vec<_>>();
            if parts.len() < 4 {
                continue;
            }
            self.failure_patterns.push(FailurePattern {
                pattern: parts[0].trim().to_string(),
                error_type: parts[1].trim().to_string(),
                conflict_class: parts[2].trim().to_string(),
                fix: parts[3].trim().to_string(),
                success_rate: parts
                    .get(4)
                    .and_then(|value| value.trim().parse::<f64>().ok())
                    .unwrap_or(1.0),
                times_applied: parts
                    .get(5)
                    .and_then(|value| value.trim().parse::<u32>().ok())
                    .unwrap_or(1),
            });
        }
        Ok(())
    }

    fn load_version_index(&mut self) -> io::Result<()> {
        self.load_version_index_file(&self.tool_root.join("data/seed/pypi_version_index.tsv"))
    }

    fn load_dynamic_version_index(&mut self) -> io::Result<()> {
        self.load_version_index_file(&self.cache_path.join("dynamic_pypi_index.tsv"))
    }

    fn load_version_index_file(&mut self, path: &Path) -> io::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts = trimmed.split('\t').collect::<Vec<_>>();
            if parts.len() < 2 {
                continue;
            }
            let versions = parts[1]
                .split(',')
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            self.pypi_index.insert(normalize(parts[0]), versions);
        }
        Ok(())
    }

    fn load_dependency_graph(&mut self) -> io::Result<()> {
        let path = self.tool_root.join("data/seed/dependency_graph.tsv");
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts = trimmed.split('\t').collect::<Vec<_>>();
            if parts.len() < 2 {
                continue;
            }
            let deps = parts[1]
                .split(',')
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            self.dependency_graph.insert(normalize(parts[0]), deps);
        }
        Ok(())
    }

    fn load_dynamic_dependency_graph(&mut self) -> io::Result<()> {
        let path = self.cache_path.join("dynamic_dependency_graph.tsv");
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts = trimmed.split('\t').collect::<Vec<_>>();
            if parts.len() < 2 {
                continue;
            }
            let deps = parts[1]
                .split(',')
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            self.dependency_graph.insert(normalize(parts[0]), deps);
        }
        Ok(())
    }

    fn load_version_dependency_specs(&mut self) -> io::Result<()> {
        let path = self.cache_path.join("dynamic_dependency_specs.tsv");
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts = trimmed.split('\t').collect::<Vec<_>>();
            if parts.len() < 2 {
                continue;
            }
            let specs = parts[1]
                .split(',')
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            self.version_dependency_specs
                .insert(parts[0].trim().to_string(), specs);
        }
        Ok(())
    }

    fn load_lockfiles(&mut self) -> io::Result<()> {
        let directory = self.cache_path.join("lockfiles");
        if !directory.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let entry_path = entry.path();
            let Some(stem) = entry_path
                .file_stem()
                .and_then(|value| value.to_str())
                .map(|value| value.to_string())
            else {
                continue;
            };
            let contents = fs::read_to_string(&entry_path)?;
            self.resolved_lockfiles.insert(stem, contents);
        }
        Ok(())
    }

    fn load_build_artifacts(&mut self) -> io::Result<()> {
        let path = self.cache_path.join("build_artifacts.tsv");
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = trimmed.split_once('\t') {
                self.build_artifacts
                    .insert(key.trim().to_string(), value.trim().to_string());
            }
        }
        Ok(())
    }

    fn load_package_artifacts(&mut self) -> io::Result<()> {
        let path = self.cache_path.join("package_artifacts.tsv");
        if !path.exists() {
            return Ok(());
        }
        let contents = fs::read_to_string(path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = trimmed.split_once('\t') {
                self.package_artifacts
                    .insert(key.trim().to_string(), value.trim().to_string());
            }
        }
        Ok(())
    }

    fn persist_failure_patterns(&self) -> io::Result<()> {
        let mut rows = self
            .failure_patterns
            .iter()
            .map(|item| {
                format!(
                    "{}\t{}\t{}\t{}\t{:.4}\t{}",
                    item.pattern,
                    item.error_type,
                    item.conflict_class,
                    item.fix,
                    item.success_rate,
                    item.times_applied
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        fs::write(
            self.cache_path.join("dynamic_failure_patterns.tsv"),
            rows.join("\n") + if rows.is_empty() { "" } else { "\n" },
        )?;
        Ok(())
    }
}

pub fn normalize(value: &str) -> String {
    value
        .trim()
        .replace(['_', '.'], "-")
        .to_lowercase()
}

fn version_dependency_key(package_name: &str, version: &str) -> String {
    format!("{}\t{}", normalize(package_name), version.trim())
}

fn package_artifact_key(python_version: &str, package_name: &str, version: &str) -> String {
    format!(
        "{}\t{}\t{}",
        python_version.trim(),
        normalize(package_name),
        version.trim()
    )
}

fn should_replace_import_record(existing: &PackageRecord, candidate: &PackageRecord) -> bool {
    let existing_rank = source_rank(&existing.source);
    let candidate_rank = source_rank(&candidate.source);
    candidate_rank > existing_rank
        || (candidate_rank == existing_rank
            && !candidate
                .default_version
                .as_deref()
                .unwrap_or("")
                .is_empty()
            && existing.default_version.as_deref().unwrap_or("").is_empty())
}

fn source_rank(source: &str) -> usize {
    match source {
        "discrepancy" => 6,
        "seed" => 5,
        "llm" => 4,
        "recovery:cache" | "recovery:llm" => 3,
        "heuristic:pypi-exact" | "recovery:heuristic" => 2,
        "heuristic:fuzzy" => 1,
        _ => 0,
    }
}
