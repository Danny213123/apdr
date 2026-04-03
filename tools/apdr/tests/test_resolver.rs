use std::path::PathBuf;

fn resolve_fixture(snippet_name: &str, output_name: &str) -> apdr::ResolveResult {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures").join(snippet_name);
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target").join(output_name);
    config.validate = false;
    apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap()
}

#[test]
fn resolver_maps_seeded_imports_to_packages() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/sample_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-output");
    config.validate = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert!(result.requirements_txt.contains("requests==2.32.3"));
    assert!(result.requirements_txt.contains("beautifulsoup4==4.12.3"));
    assert!(result.validation.succeeded);
}

#[test]
fn resolver_pins_legacy_pillow_for_python2_pil_snippets() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/python2_pil_stringio_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-legacy-pillow-output");
    config.validate = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "2.7");
    assert!(result.requirements_txt.contains("Pillow==6.2.2"));
    assert!(!result.requirements_txt.to_lowercase().contains("stringio"));
    assert!(!result.unresolved.iter().any(|item| item == "StringIO"));
}

#[test]
fn tier1_ignores_poisoned_fuzzy_cache_entries() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-fuzzy-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_import_mapping("scrapy", "scipy", None, "heuristic:fuzzy")
        .unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["scrapy".to_string()],
        import_paths: vec!["scrapy".to_string()],
        config_deps: Vec::new(),
        python_version_min: "2.7".to_string(),
        python_version_max: Some("2.7".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };

    let stage = apdr::resolver::tier1_cache::resolve(&parse_result, &mut store, "2.7");
    assert_eq!(stage.resolved.len(), 1);
    assert_eq!(stage.resolved[0].package_name, "scrapy");
    assert_eq!(stage.resolved[0].strategy, "cache:seed");
    assert!(stage.unresolved.is_empty());

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn tier2_does_not_fuzzy_match_short_imports_to_unrelated_packages() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-short-fuzzy-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["sip".to_string()],
        import_paths: vec!["sip".to_string()],
        config_deps: Vec::new(),
        python_version_min: "3.9".to_string(),
        python_version_max: Some("3.9".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };

    let stage = apdr::resolver::tier2_heuristic::resolve(
        &["sip".to_string()],
        &parse_result,
        &mut store,
        "3.9",
    );
    assert!(stage
        .resolved
        .iter()
        .all(|item| item.package_name != "scipy"));

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn tier1_resolves_specific_namespace_aliases_from_import_paths() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-namespace-alias-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["google".to_string()],
        import_paths: vec!["google.cloud.storage.client".to_string()],
        config_deps: Vec::new(),
        python_version_min: "3.9".to_string(),
        python_version_max: Some("3.9".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };

    let stage = apdr::resolver::tier1_cache::resolve(&parse_result, &mut store, "3.9");
    assert_eq!(stage.resolved.len(), 1);
    assert_eq!(stage.resolved[0].package_name, "google-cloud-storage");
    assert_eq!(stage.resolved[0].strategy, "cache:seed");

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn tier1_resolves_reference_alias_seed_entries() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-reference-alias-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["ldap".to_string()],
        import_paths: vec!["ldap".to_string()],
        config_deps: Vec::new(),
        python_version_min: "3.9".to_string(),
        python_version_max: Some("3.9".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };

    let stage = apdr::resolver::tier1_cache::resolve(&parse_result, &mut store, "3.9");
    assert_eq!(stage.resolved.len(), 1);
    assert_eq!(stage.resolved[0].package_name, "python-ldap");
    assert_eq!(stage.resolved[0].strategy, "cache:seed");

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn tier1_discrepancy_versions_fall_back_to_latest_python_compatible_release() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-discrepancy-version-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    // PyYAML is in name_discrepancies.tsv pinned at 6.0.2.  Populate the
    // pypi_index with older versions only so the pinned version is absent,
    // forcing the fallback-to-latest logic.  Key must be normalized (lowercase,
    // hyphens) because pypi_index lookups go through normalize().
    store.pypi_index.insert(
        "pyyaml".to_string(),
        vec!["5.4.1".to_string(), "6.0".to_string()],
    );

    let parse_result = apdr::ParseResult {
        imports: vec!["yaml".to_string()],
        import_paths: vec!["yaml".to_string()],
        config_deps: Vec::new(),
        python_version_min: "2.7".to_string(),
        python_version_max: Some("2.7".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };

    let stage = apdr::resolver::tier1_cache::resolve(&parse_result, &mut store, "2.7");
    assert_eq!(stage.resolved.len(), 1);
    assert_eq!(stage.resolved[0].package_name, "PyYAML");
    assert_eq!(stage.resolved[0].version.as_deref(), Some("6.0"));

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn tier1_resolves_libxmp_to_python_xmp_toolkit() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-libxmp-alias-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["libxmp".to_string()],
        import_paths: vec!["libxmp.utils".to_string()],
        config_deps: Vec::new(),
        python_version_min: "3.9".to_string(),
        python_version_max: Some("3.9".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };

    let stage = apdr::resolver::tier1_cache::resolve(&parse_result, &mut store, "3.9");
    assert_eq!(stage.resolved.len(), 1);
    assert_eq!(stage.resolved[0].package_name, "python-xmp-toolkit");

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn tier1_skips_generic_local_helper_imports() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-local-helper-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["util".to_string()],
        import_paths: vec!["util.tile_raster_images".to_string()],
        config_deps: Vec::new(),
        python_version_min: "2.7".to_string(),
        python_version_max: Some("2.7".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };

    let stage = apdr::resolver::tier1_cache::resolve(&parse_result, &mut store, "2.7");
    assert!(stage.resolved.is_empty());
    assert_eq!(stage.unresolved, vec!["util".to_string()]);

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn resolver_normalizes_legacy_pymc3_stack_to_compatible_versions() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/legacy_pymc3_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-legacy-pymc3-output");
    config.validate = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert!(result.requirements_txt.contains("pymc3==3.11.5"));
    assert!(result.requirements_txt.contains("theano-pymc==1.1.2"));
    assert!(result.requirements_txt.contains("arviz==0.12.1"));
    assert!(result.requirements_txt.contains("numpy==1.21.6"));
    assert!(result.requirements_txt.contains("pandas==1.5.3"));
    assert!(result.requirements_txt.contains("scipy==1.7.3"));
    assert!(result.requirements_txt.contains("setuptools==69.5.1"));
    assert!(result.requirements_txt.contains("xarray==2022.9.0"));
    assert!(result.requirements_txt.contains("xarray-einstats==0.6.0"));
}

#[test]
fn resolver_uses_family_bundle_for_py2_style_pymc3_benchmarks() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/legacy_pymc3_py2_style_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-legacy-pymc3-py2-style-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "3.10");
    assert!(result.requirements_txt.contains("pymc3==3.11.5"));
    assert!(result.requirements_txt.contains("theano-pymc==1.1.2"));
    assert!(result.requirements_txt.contains("arviz==0.12.1"));
    assert!(result.requirements_txt.contains("numpy==1.21.6"));
    assert!(result.requirements_txt.contains("pandas==1.5.3"));
    assert!(result.requirements_txt.contains("scipy==1.7.3"));
    assert!(result.requirements_txt.contains("setuptools==69.5.1"));
    assert!(result.requirements_txt.contains("xarray==2022.9.0"));
    assert!(result.requirements_txt.contains("xarray-einstats==0.6.0"));
    assert!(result.resolution_report.notes.iter().any(
        |note| note.contains("Family knowledge targeted the legacy PyMC3 stack at Python 3.10")
    ));
}

#[test]
fn legacy_pymc3_validation_prefers_supported_runtime_order() {
    let parse_result = apdr::ParseResult {
        imports: vec!["pymc3".to_string(), "theano".to_string()],
        import_paths: vec!["pymc3".to_string(), "theano.tensor".to_string()],
        config_deps: Vec::new(),
        python_version_min: "2.7".to_string(),
        python_version_max: Some("2.7".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let resolved = vec![
        apdr::ResolvedDependency {
            import_name: "pymc3".to_string(),
            package_name: "pymc3".to_string(),
            version: Some("3.11.5".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "theano".to_string(),
            package_name: "Theano-PyMC".to_string(),
            version: Some("1.1.2".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
    ];

    let versions = apdr::resolver::family_knowledge::validation_candidate_versions(
        &parse_result,
        &resolved,
        "2.7",
        5,
        false,
    )
    .unwrap();

    assert_eq!(versions, vec!["3.10", "3.9", "2.7"]);
}

#[test]
fn legacy_pymc3_family_recovery_keeps_curated_bundle_pins() {
    let parse_result = apdr::ParseResult {
        imports: vec!["pymc3".to_string(), "theano".to_string()],
        import_paths: vec!["pymc3".to_string(), "theano.tensor".to_string()],
        config_deps: Vec::new(),
        python_version_min: "2.7".to_string(),
        python_version_max: Some("2.7".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let mut resolved = vec![
        apdr::ResolvedDependency {
            import_name: "arviz".to_string(),
            package_name: "arviz".to_string(),
            version: Some("0.12.1".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "numpy".to_string(),
            package_name: "numpy".to_string(),
            version: Some("1.21.6".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "pandas".to_string(),
            package_name: "pandas".to_string(),
            version: Some("1.5.3".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "pymc3".to_string(),
            package_name: "pymc3".to_string(),
            version: Some("3.11.5".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "scipy".to_string(),
            package_name: "scipy".to_string(),
            version: Some("1.7.3".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "setuptools".to_string(),
            package_name: "setuptools".to_string(),
            version: Some("69.5.1".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "theano".to_string(),
            package_name: "Theano-PyMC".to_string(),
            version: Some("1.1.2".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "xarray".to_string(),
            package_name: "xarray".to_string(),
            version: Some("2022.9.0".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
        apdr::ResolvedDependency {
            import_name: "xarray_einstats".to_string(),
            package_name: "xarray-einstats".to_string(),
            version: Some("0.6.0".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.97,
        },
    ];

    let note = apdr::resolver::family_knowledge::recover_family_knowledge(
        &parse_result,
        &mut resolved,
        "2.7",
        5,
        false,
        "ERROR: Could not find a version that satisfies the requirement pandas==2.1.4",
    )
    .unwrap();

    assert!(
        note.contains("Family-aware recovery reapplied the legacy PyMC3 stack")
            || note.contains("Family-aware recovery kept the legacy PyMC3 stack pinned")
    );
    assert!(resolved
        .iter()
        .any(|item| item.package_name == "pandas" && item.version.as_deref() == Some("1.5.3")));
    assert!(resolved
        .iter()
        .any(|item| item.package_name == "xarray" && item.version.as_deref() == Some("2022.9.0")));
}

#[test]
fn resolver_uses_family_bundle_for_legacy_tensorflow_stack() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/legacy_tensorflow_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-legacy-tensorflow-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert!(result.requirements_txt.contains("tensorflow==1.15.0"));
    assert!(result.requirements_txt.contains("keras==2.3.1"));
    assert!(result.requirements_txt.contains("numpy==1.16.6"));
    assert!(result.requirements_txt.contains("gym==0.17.3"));
    assert!(result.resolution_report.notes.iter().any(|note| note
        .contains("Family knowledge targeted the legacy TensorFlow/Keras stack at Python 3.7")));
}

#[test]
fn legacy_tensorflow_validation_prefers_py37_before_py27() {
    let parse_result = apdr::ParseResult {
        imports: vec![
            "tensorflow".to_string(),
            "keras".to_string(),
            "gym".to_string(),
        ],
        import_paths: vec![
            "tensorflow".to_string(),
            "keras.layers".to_string(),
            "gym".to_string(),
        ],
        config_deps: Vec::new(),
        python_version_min: "3.9".to_string(),
        python_version_max: Some("3.9".to_string()),
        confidence: 0.8,
        scanned_files: Vec::new(),
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let resolved = vec![
        apdr::ResolvedDependency {
            import_name: "tensorflow".to_string(),
            package_name: "tensorflow".to_string(),
            version: Some("1.15.0".to_string()),
            strategy: "family:legacy-tensorflow".to_string(),
            confidence: 0.96,
        },
        apdr::ResolvedDependency {
            import_name: "keras".to_string(),
            package_name: "keras".to_string(),
            version: Some("2.3.1".to_string()),
            strategy: "family:legacy-tensorflow".to_string(),
            confidence: 0.96,
        },
    ];

    let versions = apdr::resolver::family_knowledge::validation_candidate_versions(
        &parse_result,
        &resolved,
        "3.9",
        5,
        false,
    )
    .unwrap();

    // 3.7 and 2.7 get TF 1.15.0 bundle; 3.8+ get unpinned TF (pip resolves freely)
    assert_eq!(versions, vec!["3.7", "2.7", "3.8", "3.9", "3.10"]);
}

#[test]
fn resolver_skips_macos_pyobjc_private_framework_cases() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/apple_private_framework_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-apple-private-framework-output");
    config.allow_llm = false;
    config.execute_snippet = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.validation.status, "skipped-host-runtime");
    assert!(result
        .validation
        .reason
        .as_deref()
        .unwrap_or("")
        .contains("macOS Objective-C framework dependency"));
}

#[test]
fn pre_solver_pins_compatible_versions_before_validation() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-pre-solver-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_pypi_versions("click", &["6.6".into(), "8.1.0".into()])
        .unwrap();
    store
        .save_pypi_versions("pip-tools", &["4.4.0".into(), "7.4.1".into()])
        .unwrap();
    store
        .save_version_dependency_specs("pip-tools", "4.4.0", &["click==6.6".into()])
        .unwrap();
    store
        .save_version_dependency_specs("pip-tools", "7.4.1", &["click>=8.0".into()])
        .unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["click".to_string()],
        import_paths: vec!["click".to_string()],
        config_deps: vec![apdr::ConfigDep {
            package: "pip-tools".to_string(),
            constraint: Some(">=4.0.0".to_string()),
            source_file: "requirements.txt".to_string(),
        }],
        python_version_min: "3.11".to_string(),
        python_version_max: Some("3.11".to_string()),
        confidence: 0.9,
        scanned_files: vec!["requirements.txt".to_string()],
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let resolved = vec![apdr::ResolvedDependency {
        import_name: "click".to_string(),
        package_name: "click".to_string(),
        version: Some("6.6".to_string()),
        strategy: "test".to_string(),
        confidence: 1.0,
    }];
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.parallel_versions = false;

    let result = apdr::resolver::pre_solve::solve_dependency_graph(
        &parse_result,
        &resolved,
        "3.11",
        &mut store,
        &config,
    );

    assert!(result.attempted);
    assert!(result.satisfiable);
    assert_eq!(result.selected_python_version, "3.11");
    assert_eq!(
        result.assigned_versions.get("click").map(String::as_str),
        Some("6.6")
    );
    assert_eq!(
        result
            .assigned_versions
            .get("pip-tools")
            .map(String::as_str),
        Some("4.4.0")
    );
    assert!(result.lockfile_requirements.contains("click==6.6"));
    assert!(result.lockfile_requirements.contains("pip-tools==4.4.0"));

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn pre_solver_reports_unsat_without_validation_attempts() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-pre-solver-unsat-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_pypi_versions("click", &["6.6".into(), "8.1.0".into()])
        .unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["click".to_string()],
        import_paths: vec!["click".to_string()],
        config_deps: vec![apdr::ConfigDep {
            package: "click".to_string(),
            constraint: Some(">=8.0".to_string()),
            source_file: "requirements.txt".to_string(),
        }],
        python_version_min: "3.11".to_string(),
        python_version_max: Some("3.11".to_string()),
        confidence: 0.9,
        scanned_files: vec!["requirements.txt".to_string()],
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let resolved = vec![apdr::ResolvedDependency {
        import_name: "click".to_string(),
        package_name: "click".to_string(),
        version: Some("6.6".to_string()),
        strategy: "test".to_string(),
        confidence: 1.0,
    }];
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.parallel_versions = false;

    let result = apdr::resolver::pre_solve::solve_dependency_graph(
        &parse_result,
        &resolved,
        "3.11",
        &mut store,
        &config,
    );

    assert!(result.attempted);
    assert!(!result.satisfiable);
    assert!(result.hard_unsat);
    assert!(result.reason.is_some());

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn pre_solver_treats_missing_exact_pin_as_incomplete_metadata() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-pre-solver-exact-pin-metadata-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_pypi_versions("django", &["1.11.29".into()])
        .unwrap();
    store
        .save_pypi_versions("johnny-cache", &["1.4".into()])
        .unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["django".to_string(), "johnny".to_string()],
        import_paths: vec![
            "django.core.management.base".to_string(),
            "johnny.cache".to_string(),
        ],
        config_deps: vec![],
        python_version_min: "2.7".to_string(),
        python_version_max: Some("2.7".to_string()),
        confidence: 0.9,
        scanned_files: vec!["snippet.py".to_string()],
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let resolved = vec![
        apdr::ResolvedDependency {
            import_name: "django".to_string(),
            package_name: "Django".to_string(),
            version: Some("1.8.19".to_string()),
            strategy: "family:legacy-johnny-cache".to_string(),
            confidence: 1.0,
        },
        apdr::ResolvedDependency {
            import_name: "johnny".to_string(),
            package_name: "johnny-cache".to_string(),
            version: Some("1.4".to_string()),
            strategy: "family:legacy-johnny-cache".to_string(),
            confidence: 1.0,
        },
    ];
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.parallel_versions = false;

    let result = apdr::resolver::pre_solve::solve_dependency_graph(
        &parse_result,
        &resolved,
        "2.7",
        &mut store,
        &config,
    );

    assert!(result.attempted);
    assert!(!result.satisfiable);
    assert!(
        !result.hard_unsat,
        "exact pin missing from solver metadata should not be treated as hard UNSAT"
    );
    assert!(
        result
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("exact pin `1.8.19`")
            || result
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("exact pin `django==1.8.19`")
    );

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn pre_solver_preserves_candidate_order_without_mutex_aggregation() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-pre-solver-candidate-order-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_pypi_versions("click", &["6.6".into(), "8.1.0".into()])
        .unwrap();
    store
        .save_pypi_versions("pip-tools", &["4.4.0".into(), "7.4.1".into()])
        .unwrap();
    store
        .save_version_dependency_specs("pip-tools", "4.4.0", &["click==6.6".into()])
        .unwrap();
    store
        .save_version_dependency_specs("pip-tools", "7.4.1", &["click>=8.0".into()])
        .unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["click".to_string()],
        import_paths: vec!["click".to_string()],
        config_deps: vec![apdr::ConfigDep {
            package: "pip-tools".to_string(),
            constraint: Some(">=4.0.0".to_string()),
            source_file: "requirements.txt".to_string(),
        }],
        python_version_min: "3.10".to_string(),
        python_version_max: Some("3.11".to_string()),
        confidence: 0.9,
        scanned_files: vec!["requirements.txt".to_string()],
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let resolved = vec![apdr::ResolvedDependency {
        import_name: "click".to_string(),
        package_name: "click".to_string(),
        version: Some("6.6".to_string()),
        strategy: "test".to_string(),
        confidence: 1.0,
    }];
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.parallel_versions = true;
    config.python_version_range = 1;
    config.execute_snippet = false;

    let result = apdr::resolver::pre_solve::solve_dependency_graph(
        &parse_result,
        &resolved,
        "3.10",
        &mut store,
        &config,
    );

    assert!(result.attempted);
    assert!(result.satisfiable);
    assert_eq!(
        result.selected_python_version, "3.10",
        "the first satisfiable candidate in candidate order should win"
    );
    assert_eq!(
        result
            .assigned_versions
            .get("pip-tools")
            .map(String::as_str),
        Some("4.4.0")
    );

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn pre_solver_keeps_incomplete_metadata_separate_from_hard_failures() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-pre-solver-metadata-fallback-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    store
        .save_pypi_versions("django", &["1.11.29".into()])
        .unwrap();
    store
        .save_pypi_versions("johnny-cache", &["1.4".into()])
        .unwrap();

    let parse_result = apdr::ParseResult {
        imports: vec!["django".to_string(), "johnny".to_string()],
        import_paths: vec![
            "django.core.management.base".to_string(),
            "johnny.cache".to_string(),
        ],
        config_deps: vec![],
        python_version_min: "2.7".to_string(),
        python_version_max: Some("3.7".to_string()),
        confidence: 0.9,
        scanned_files: vec!["snippet.py".to_string()],
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let resolved = vec![
        apdr::ResolvedDependency {
            import_name: "django".to_string(),
            package_name: "Django".to_string(),
            version: Some("1.8.19".to_string()),
            strategy: "family:legacy-johnny-cache".to_string(),
            confidence: 1.0,
        },
        apdr::ResolvedDependency {
            import_name: "johnny".to_string(),
            package_name: "johnny-cache".to_string(),
            version: Some("1.4".to_string()),
            strategy: "family:legacy-johnny-cache".to_string(),
            confidence: 1.0,
        },
    ];
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.parallel_versions = true;
    config.python_version_range = 1;
    config.execute_snippet = false;

    let result = apdr::resolver::pre_solve::solve_dependency_graph(
        &parse_result,
        &resolved,
        "2.7",
        &mut store,
        &config,
    );

    assert!(result.attempted);
    assert!(!result.satisfiable);
    assert!(
        !result.hard_unsat,
        "incomplete metadata should still fall back instead of becoming hard UNSAT"
    );
    assert!(result
        .reason
        .as_deref()
        .unwrap_or("")
        .contains("dependency metadata was incomplete"));

    std::fs::remove_dir_all(cache_path).unwrap();
}

// ---------------------------------------------------------------------------
// New fixture-based tests for recent bugfix scenarios
// ---------------------------------------------------------------------------

#[test]
fn py2_memcache_maps_to_python_memcached_with_version_cap() {
    // Case 005bbad: `import memcache` must map to python-memcached,
    // and on Py 2.7 the version must be capped to <=1.59.
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/py2_memcache_redis_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-py2-memcache-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 1;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "2.7");
    // memcache import must resolve to python-memcached (not left unresolved)
    assert!(
        result
            .requirements_txt
            .to_lowercase()
            .contains("python-memcached"),
        "Expected python-memcached in requirements, got: {}",
        result.requirements_txt,
    );
    // Version must be capped for Py2 (<=1.59)
    let has_capped_version = result.requirements_txt.contains("python-memcached==1.59")
        || result.requirements_txt.contains("python-memcached==1.58")
        || result.requirements_txt.contains("python-memcached==1.57");
    assert!(
        has_capped_version,
        "Expected python-memcached with Py2-compatible version cap, got: {}",
        result.requirements_txt,
    );
    // redis should also be capped
    assert!(
        result.requirements_txt.contains("redis=="),
        "Expected pinned redis version for Py2, got: {}",
        result.requirements_txt,
    );
}

#[test]
fn scrapy_peewee_resolves_both_packages() {
    // Case 00056d4: scrapy + peewee snippet (Py 2.7 due to iteritems)
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/scrapy_peewee_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-scrapy-peewee-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 1;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "2.7");
    assert!(
        result.requirements_txt.to_lowercase().contains("scrapy"),
        "Expected scrapy in requirements, got: {}",
        result.requirements_txt,
    );
    // scrapy should have a Py2-compatible version
    assert!(
        result.requirements_txt.contains("scrapy=="),
        "Expected pinned scrapy version for Py2, got: {}",
        result.requirements_txt,
    );
    assert!(
        result.requirements_txt.contains("lxml==4.6.5"),
        "Expected Py2-compatible lxml pin for scrapy bundle, got: {}",
        result.requirements_txt,
    );
    assert!(
        result.unresolved.is_empty(),
        "Unexpected unresolved: {:?}",
        result.unresolved
    );
}

#[test]
fn protobuf_tensorflow_includes_protobuf_dep() {
    // Case 00e9638: TF + protobuf descriptor import. The protobuf package
    // must appear in resolved deps (either from explicit import or as a
    // transitive essential in the TF family bundle).
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/protobuf_tensorflow_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-protobuf-tf-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert!(
        result
            .requirements_txt
            .to_lowercase()
            .contains("tensorflow"),
        "Expected tensorflow in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        result.requirements_txt.to_lowercase().contains("protobuf"),
        "Expected protobuf in requirements (explicit import or TF transitive), got: {}",
        result.requirements_txt,
    );
    assert!(
        result.requirements_txt.to_lowercase().contains("numpy"),
        "Expected numpy in requirements, got: {}",
        result.requirements_txt,
    );
}

#[test]
fn flask_extensions_resolve_with_flask_prefix() {
    // Flask extensions use the Flask-* PyPI naming convention
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/flask_extensions_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-flask-extensions-output");
    config.validate = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("flask-sqlalchemy") || req_lower.contains("flask_sqlalchemy"),
        "Expected Flask-SQLAlchemy, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("flask-login") || req_lower.contains("flask_login"),
        "Expected Flask-Login, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("flask-cors") || req_lower.contains("flask_cors"),
        "Expected Flask-Cors, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("redis"),
        "Expected redis, got: {}",
        result.requirements_txt,
    );
    assert!(
        result.unresolved.is_empty(),
        "Unexpected unresolved: {:?}",
        result.unresolved
    );
}

#[test]
fn cv2_serial_maps_to_correct_pypi_packages() {
    // C-extension wrappers: cv2 -> opencv-python, serial -> pyserial,
    // PIL -> Pillow (Pattern A and E)
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/cv2_serial_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-cv2-serial-output");
    config.validate = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("opencv-python") || req_lower.contains("opencv_python"),
        "Expected opencv-python for cv2, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("pyserial"),
        "Expected pyserial for serial, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("pillow"),
        "Expected Pillow for PIL, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("numpy"),
        "Expected numpy, got: {}",
        result.requirements_txt,
    );
    assert!(
        result.unresolved.is_empty(),
        "Unexpected unresolved: {:?}",
        result.unresolved
    );
}

#[test]
fn freetype_import_maps_to_freetype_py() {
    // Pattern F: freetype -> freetype-py (completely different name)
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/freetype_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-freetype-output");
    config.validate = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("freetype-py"),
        "Expected freetype-py for freetype import, got: {}",
        result.requirements_txt,
    );
}

#[test]
fn domain_is_treated_as_local_module() {
    // 'domain' is a project-local module, not a PyPI package.
    // Only jsonpickle should be resolved.
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/domain_jsonpickle_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-domain-output");
    config.validate = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("jsonpickle"),
        "Expected jsonpickle, got: {}",
        result.requirements_txt,
    );
    // 'domain' should either be unresolved or not appear in requirements
    // (it's a local project module, not a PyPI package)
    assert!(
        !req_lower.contains("\ndomain==") && !req_lower.contains("\ndomain\n"),
        "domain should NOT be in requirements (it's a local module), got: {}",
        result.requirements_txt,
    );
}

// ==========================================================================
// Regression tests for PLLM-passing cases (cases 1329319, 125559, 1254809, 1202066)
// ==========================================================================

#[test]
fn pylibmc_redis_resolves_both_packages() {
    // Case 1329319: redis + pylibmc (Py 2.7 due to `print` statement)
    // PLLM solves with: redis, pylibmc
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/pylibmc_redis_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-pylibmc-redis-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 1;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "2.7");
    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("redis"),
        "Expected redis in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("pylibmc"),
        "Expected pylibmc in requirements, got: {}",
        result.requirements_txt,
    );
}

#[test]
fn daemon_django_resolves_without_docstring_imports() {
    // Case 125559: django + python-daemon (Py 2.7)
    // The snippet has `from daemonextension import DaemonCommand` and
    // `from flopsy import Connection` in docstrings — these should NOT
    // be resolved as real dependencies.
    // PLLM solves with: django, python-daemon
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/daemon_django_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-daemon-django-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 1;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("django"),
        "Expected django in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("python-daemon"),
        "Expected python-daemon in requirements, got: {}",
        result.requirements_txt,
    );
    // daemonextension and flopsy are from docstrings — should NOT be resolved
    assert!(
        !req_lower.contains("extensionclass"),
        "extensionclass should NOT be in requirements (daemonextension is from a docstring), got: {}",
        result.requirements_txt,
    );
    assert!(
        !req_lower.contains("flopsy"),
        "flopsy should NOT be in requirements (it's from a docstring example), got: {}",
        result.requirements_txt,
    );
}

#[test]
fn levenshtein_resolves_to_python_levenshtein() {
    // Case 1254809: Levenshtein (Py 2.7 due to `print` statement)
    // PLLM solves with: levenshtein
    // READPY solves with: python-levenshtein
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/levenshtein_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-levenshtein-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 1;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "2.7");
    let req_lower = result.requirements_txt.to_lowercase();
    // Either python-levenshtein or levenshtein is acceptable
    assert!(
        req_lower.contains("levenshtein"),
        "Expected Levenshtein package in requirements, got: {}",
        result.requirements_txt,
    );
}

#[test]
fn redis_webpy_resolves_to_web_py() {
    // Case 1202066: redis + web.py (Py 2.7)
    // PLLM solves with: redis, web-py (i.e. web.py framework)
    // The snippet imports `web` which maps to the `web.py` package.
    // `rediswebpy` in the docstring is the snippet's own module, not a dep.
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/redis_webpy_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-redis-webpy-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 1;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("redis"),
        "Expected redis in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("web.py") || req_lower.contains("web-py"),
        "Expected web.py in requirements (not web3), got: {}",
        result.requirements_txt,
    );
    // web3 is the WRONG package — it's the Ethereum library
    assert!(
        !req_lower.contains("web3"),
        "web3 should NOT be in requirements (web.py is the correct package), got: {}",
        result.requirements_txt,
    );
    // rediswebpy from docstring should not be in requirements
    assert!(
        !req_lower.contains("rediswebpy"),
        "rediswebpy should NOT be in requirements (it's the snippet's own module), got: {}",
        result.requirements_txt,
    );
}

#[test]
fn geospatial_fiona_resolves_key_packages() {
    // Case 0306734d: KMZ/KML parser using pandas, numpy, shapely, fiona, geopandas
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/geospatial_fiona_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-geospatial-fiona-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("pandas"),
        "Expected pandas in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("numpy"),
        "Expected numpy in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("shapely"),
        "Expected shapely in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("fiona"),
        "Expected fiona in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("geopandas"),
        "Expected geopandas in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        result.unresolved.is_empty(),
        "Unexpected unresolved: {:?}",
        result.unresolved
    );
}

#[test]
fn resolver_marks_opendirectory_as_host_runtime_skip() {
    let result = resolve_fixture(
        "skip_opendirectory_snippet.py",
        "test-skip-opendirectory-output",
    );
    assert_eq!(result.validation.status, "skipped-host-runtime");
}

#[test]
fn resolver_marks_microbit_as_host_runtime_skip() {
    let result = resolve_fixture("skip_microbit_snippet.py", "test-skip-microbit-output");
    assert_eq!(result.validation.status, "skipped-host-runtime");
}

#[test]
fn resolver_marks_binaryninja_as_host_runtime_skip() {
    let result = resolve_fixture(
        "skip_binaryninja_snippet.py",
        "test-skip-binaryninja-output",
    );
    assert_eq!(result.validation.status, "skipped-host-runtime");
}

#[test]
fn resolver_marks_pythonista_clipboard_camera_as_host_runtime_skip() {
    let result = resolve_fixture(
        "skip_pythonista_clipboard_camera_snippet.py",
        "test-skip-pythonista-output",
    );
    assert_eq!(result.validation.status, "skipped-host-runtime");
}

#[test]
fn resolver_marks_maya_pyqt_llm_only_as_host_runtime_skip() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/skip_maya_pyqt_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-skip-maya-pyqt-llm-only-output");
    config.allow_llm = true;
    config.llm_only_mode = true;
    config.execute_snippet = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.validation.status, "skipped-host-runtime");
    assert!(result
        .validation
        .reason
        .as_deref()
        .unwrap_or("")
        .contains("Autodesk Maya desktop runtime"));
}

#[test]
fn namespace_validation_rejects_invalid_swaps() {
    assert!(!apdr::resolver::family_knowledge::namespace_mapping_allowed("PySide", "PySide6",));
    assert!(
        !apdr::resolver::family_knowledge::namespace_mapping_allowed("mosquitto", "paho-mqtt",)
    );
    assert!(!apdr::resolver::family_knowledge::namespace_mapping_allowed("ldap", "ldap3",));
    assert!(!apdr::resolver::family_knowledge::namespace_mapping_allowed("oaipmh", "a",));
    assert!(
        !apdr::resolver::family_knowledge::namespace_mapping_allowed("Levenshtein", "fuzzywuzzy",)
    );
}

#[test]
fn resolver_applies_legacy_flask_bundle() {
    let result = resolve_fixture(
        "legacy_flask_stack_snippet.py",
        "test-legacy-flask-bundle-output",
    );
    assert!(result.requirements_txt.contains("Flask==1.1.4"));
    assert!(result.requirements_txt.contains("Jinja2==2.11.3"));
    assert!(result.requirements_txt.contains("MarkupSafe==1.1.1"));
    assert!(result.requirements_txt.contains("Werkzeug==1.0.1"));
}

#[test]
fn resolver_applies_cfscrape_bundle() {
    let result = resolve_fixture("cfscrape_snippet.py", "test-cfscrape-bundle-output");
    assert!(result.requirements_txt.contains("cfscrape==1.2.1"));
    assert!(result.requirements_txt.contains("urllib3==1.26.18"));
}

#[test]
fn resolver_applies_legacy_ggplot_bundle() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/legacy_ggplot_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-ggplot-bundle-output");
    config.validate = false;
    config.python_version = Some("3.8".to_string());
    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert!(result.requirements_txt.contains("ggplot==0.11.5"));
    assert!(result.requirements_txt.contains("pandas==0.24.2"));
}

#[test]
fn resolver_applies_simplecv_bundle() {
    let result = resolve_fixture("simplecv_snippet.py", "test-simplecv-bundle-output");
    let req_lower = result.requirements_txt.to_lowercase();
    assert!(req_lower.contains("simplecv==1.3"));
    assert!(req_lower.contains("opencv-python-headless==4.5.5.64"));
}

#[test]
fn resolver_skips_vendor_caffe_case() {
    let result = resolve_fixture("vendor_caffe_snippet.py", "test-vendor-caffe-output");
    assert_eq!(result.validation.status, "skipped-unsolvable");
}

#[test]
fn resolver_maps_johnny_cache_imports_to_johnny_cache_package() {
    let result = resolve_fixture("johnny_cache_snippet.py", "test-johnny-cache-output");
    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("johnny-cache"),
        "Expected johnny-cache in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        !req_lower.contains("\njohnny\n") && !req_lower.starts_with("johnny\n"),
        "Did not expect bare johnny package in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("django==1.8.19"),
        "Expected legacy Django pin for johnny-cache snippet, got: {}",
        result.requirements_txt,
    );
}

#[test]
fn resolver_dependency_updates_use_normalized_package_identity() {
    let resolved = vec![apdr::ResolvedDependency {
        import_name: "django".to_string(),
        package_name: "Django".to_string(),
        version: Some("5.1.3".to_string()),
        strategy: "cache:seed".to_string(),
        confidence: 0.82,
    }];
    let resolved = apdr::resolver::debug_update_package_version(
        resolved,
        "django",
        Some("1.8.19".to_string()),
    );
    let resolved = apdr::resolver::debug_upsert_dependency(
        resolved,
        "django",
        "django",
        Some("1.8.19".to_string()),
        "recovery:cache",
    );

    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].package_name.to_ascii_lowercase(), "django");
    assert_eq!(resolved[0].version.as_deref(), Some("1.8.19"));
}

#[test]
fn resolver_retry_loop_reuses_dirty_requirements_buffer() {
    let clean_dependency = apdr::ResolvedDependency {
        import_name: "requests".to_string(),
        package_name: "requests".to_string(),
        version: Some("2.32.3".to_string()),
        strategy: "cache:seed".to_string(),
        confidence: 0.9,
    };
    let downgraded_dependency = apdr::ResolvedDependency {
        version: Some("2.31.0".to_string()),
        strategy: "recovery:version-adjustment".to_string(),
        ..clean_dependency.clone()
    };

    let trace = apdr::resolver::debug_retry_loop_requirements_trace(
        "requests==2.32.3\n".to_string(),
        vec![
            (vec![clean_dependency.clone()], false),
            (vec![clean_dependency], false),
            (vec![downgraded_dependency.clone()], true),
            (vec![downgraded_dependency], false),
        ],
    );

    assert_eq!(trace.len(), 4);
    assert_eq!(trace[0], "requests==2.32.3\n");
    assert_eq!(trace[0], trace[1]);
    assert_eq!(trace[2], "requests==2.31.0\n");
    assert_eq!(trace[2], trace[3]);
}

#[test]
fn resolver_prefers_mecab_python_for_python2_snippets() {
    let result = resolve_fixture("python2_mecab_snippet.py", "test-python2-mecab-output");
    let req_lower = result.requirements_txt.to_lowercase();
    assert!(
        req_lower.contains("mecab-python"),
        "Expected mecab-python in requirements, got: {}",
        result.requirements_txt,
    );
    assert!(
        !req_lower.contains("mecab-python3"),
        "Did not expect mecab-python3 for Python 2 snippet, got: {}",
        result.requirements_txt,
    );
}

fn write_curated_family_test_files(
    tool_root: &std::path::Path,
    families_json: &str,
    recovery_rules_json: &str,
) {
    let family_dir = tool_root.join("data").join("family_knowledge");
    std::fs::create_dir_all(&family_dir).unwrap();
    std::fs::write(family_dir.join("touched_families.json"), families_json).unwrap();
    std::fs::write(
        family_dir.join("touched_recovery_rules.json"),
        recovery_rules_json,
    )
    .unwrap();
}

#[test]
fn data_driven_family_loader_reads_touched_seed() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let curated = apdr::resolver::family_knowledge::load_curated_family_knowledge(&tool_root)
        .expect("expected touched family seed to load");

    assert_eq!(curated.families().len(), 6);
    assert_eq!(curated.recovery_rules().len(), 6);
    assert!(curated.family_named("legacy-pymc3").is_some());
    assert_eq!(
        curated
            .explicit_namespace_mapping("pkg_resources")
            .map(|mapping| mapping.package_name.as_str()),
        Some("setuptools")
    );
    assert_eq!(
        curated
            .explicit_namespace_mapping("Image")
            .map(|mapping| mapping.package_name.as_str()),
        Some("Pillow")
    );
    assert_eq!(
        curated
            .recovery_rule("pkg-resources")
            .map(|rule| rule.family.as_str()),
        Some("setuptools")
    );
}

#[test]
fn data_driven_family_loader_rejects_duplicate_namespace_mappings() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_curated_family_test_files(
        temp_dir.path(),
        r#"{
  "families": [
    {
      "name": "pil",
      "runtime_scopes": ["registry"],
      "modules": ["PIL", "Image"],
      "conflict_kind": "replacement",
      "members": [
        {
          "package": "Pillow",
          "modules": ["PIL", "Image"],
          "status": "active",
          "preferred": true
        }
      ],
      "notes": "Pillow is the maintained fork of PIL."
    }
  ],
  "explicit_namespace_mappings": [
    {
      "import_alias": "Image",
      "package_name": "Pillow"
    },
    {
      "import_alias": "Image",
      "package_name": "PIL"
    }
  ]
}"#,
        r#"{
  "rules": [
    {
      "id": "legacy-pillow",
      "family": "pil",
      "kind": "pin",
      "strategy": "family:legacy-pillow",
      "anchor_ids": ["Image"],
      "bundle_variants": [
        {
          "python_selectors": ["py2"],
          "members": [
            {
              "import_name": "PIL",
              "package_name": "Pillow",
              "version": "6.2.2"
            }
          ]
        }
      ]
    }
  ]
}"#,
    );

    let err = apdr::resolver::family_knowledge::load_curated_family_knowledge(temp_dir.path())
        .expect_err("expected duplicate namespace mapping to fail");

    assert_eq!(
        err,
        "duplicate explicit namespace mapping for alias `Image`: `Pillow` conflicts with `PIL`"
    );
}

#[test]
fn data_driven_family_loader_rejects_unknown_rule_family() {
    let temp_dir = tempfile::tempdir().unwrap();
    write_curated_family_test_files(
        temp_dir.path(),
        r#"{
  "families": [
    {
      "name": "setuptools",
      "runtime_scopes": ["registry"],
      "modules": ["setuptools", "pkg_resources"],
      "conflict_kind": "replacement",
      "members": [
        {
          "package": "setuptools",
          "modules": ["setuptools", "pkg_resources"],
          "status": "active",
          "preferred": true
        }
      ],
      "notes": "distribute was merged back into setuptools."
    }
  ],
  "explicit_namespace_mappings": [
    {
      "import_alias": "pkg_resources",
      "package_name": "setuptools"
    }
  ]
}"#,
        r#"{
  "rules": [
    {
      "id": "pkg-resources",
      "family": "missing-family",
      "kind": "retry-module-provider",
      "strategy": "recovery:missing-module",
      "anchor_ids": ["pkg_resources"],
      "trigger_substrings": [
        "modulenotfounderror: no module named 'pkg_resources'"
      ],
      "bundle_variants": [
        {
          "python_selectors": ["any"],
          "members": [
            {
              "import_name": "pkg_resources",
              "package_name": "setuptools",
              "version": null
            }
          ]
        }
      ]
    }
  ]
}"#,
    );

    let err = apdr::resolver::family_knowledge::load_curated_family_knowledge(temp_dir.path())
        .expect_err("expected unknown rule family to fail");

    assert_eq!(
        err,
        "recovery rule `pkg-resources` references unknown family `missing-family`"
    );
}

fn family_runtime_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

fn resolve_inline_snippet(snippet_source: &str, output_name: &str) -> apdr::ResolveResult {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet_dir = tool_root.join("target").join(output_name);
    std::fs::create_dir_all(&snippet_dir).unwrap();
    let snippet_path = snippet_dir.join("snippet.py");
    std::fs::write(&snippet_path, snippet_source).unwrap();

    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = snippet_dir.join("artifacts");
    config.validate = false;

    apdr::resolver::resolve_path(&tool_root, &snippet_path, &config).unwrap()
}

fn resolve_phase7_family_fixture(case_id: &str, output_name: &str) -> apdr::ResolveResult {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root
        .join("tests/phase7_family_fixtures")
        .join(case_id)
        .join("snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target").join(output_name);
    config.validate = false;

    apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap()
}

fn resolve_phase7_family_bundle_fixture(case_id: &str, output_name: &str) -> apdr::ResolveResult {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root
        .join("tests/phase7_family_fixtures")
        .join(case_id)
        .join("snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target").join(output_name);
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap()
}

struct CuratedFamilyOverride {
    original_tool_root: PathBuf,
    _temp_dir: tempfile::TempDir,
}

impl CuratedFamilyOverride {
    fn from_modified_families<F>(transform: F) -> Self
    where
        F: FnOnce(String) -> String,
    {
        let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let temp_dir = tempfile::tempdir().unwrap();
        let families_json =
            std::fs::read_to_string(apdr::resolver::family_knowledge::touched_families_path(
                &tool_root,
            ))
            .unwrap();
        let recovery_rules_json = std::fs::read_to_string(
            apdr::resolver::family_knowledge::touched_recovery_rules_path(&tool_root),
        )
        .unwrap();
        write_curated_family_test_files(
            temp_dir.path(),
            &transform(families_json),
            &recovery_rules_json,
        );
        apdr::resolver::family_knowledge::init_curated_family_knowledge(temp_dir.path()).unwrap();

        Self {
            original_tool_root: tool_root,
            _temp_dir: temp_dir,
        }
    }
}

impl Drop for CuratedFamilyOverride {
    fn drop(&mut self) {
        let _ = apdr::resolver::family_knowledge::init_curated_family_knowledge(
            &self.original_tool_root,
        );
    }
}

#[test]
fn data_driven_family_runtime_registry_prefers_curated_touch_points() {
    let _lock = family_runtime_test_lock();
    let _override = CuratedFamilyOverride::from_modified_families(|json| {
        json.replace("\"package\": \"scikit-learn\"", "\"package\": \"scikit-learn-modern\"")
            .replace(
                "\"package_name\": \"scikit-learn\"",
                "\"package_name\": \"scikit-learn-modern\"",
            )
    });

    assert_eq!(
        apdr::resolver::family_knowledge::debug_family_registry_entry_for_package(
            "scikit-learn-modern",
        ),
        Some(("curated".to_string(), "sklearn".to_string()))
    );
    assert!(apdr::resolver::family_knowledge::namespace_mapping_allowed(
        "sklearn",
        "scikit-learn-modern",
    ));
    assert!(!apdr::resolver::family_knowledge::namespace_mapping_allowed(
        "sklearn",
        "scikit-learn",
    ));
}

#[test]
fn data_driven_family_runtime_registry_keeps_untouched_static_families() {
    let _lock = family_runtime_test_lock();
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    apdr::resolver::family_knowledge::init_curated_family_knowledge(&tool_root).unwrap();

    assert_eq!(
        apdr::resolver::family_knowledge::debug_family_registry_entry_for_package("pyserial"),
        Some(("static".to_string(), "serial".to_string()))
    );
    assert!(apdr::resolver::family_knowledge::namespace_mapping_allowed(
        "serial",
        "pyserial",
    ));
}

#[test]
fn data_driven_family_runtime_behavior_preserves_legacy_pillow_pin() {
    let _lock = family_runtime_test_lock();
    let result = resolve_fixture(
        "python2_pil_stringio_snippet.py",
        "test-data-driven-legacy-pillow-output",
    );

    assert!(result.requirements_txt.contains("Pillow==6.2.2"));
    assert!(result.resolution_report.notes.iter().any(|note| note
        .contains("Family knowledge pinned Pillow to 6.2.2 for Python 2.7 PIL-era compatibility.")));
}

#[test]
fn data_driven_family_runtime_behavior_preserves_legacy_pymc3_bundle() {
    let _lock = family_runtime_test_lock();
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/legacy_pymc3_py2_style_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root
        .join("target")
        .join("test-data-driven-legacy-pymc3-output");
    config.validate = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    let req_lower = result.requirements_txt.to_lowercase();

    assert!(req_lower.contains("pymc3==3.11.5"));
    assert!(req_lower.contains("theano-pymc==1.1.2"));
    assert!(result
        .resolution_report
        .notes
        .iter()
        .any(|note| note.contains("Family knowledge targeted the legacy PyMC3 stack")));
}

#[test]
fn data_driven_family_runtime_behavior_preserves_keras_backend_rule() {
    let _lock = family_runtime_test_lock();
    let result = resolve_inline_snippet("import keras\nmodel = keras.Sequential()\n", "test-data-driven-keras-backend-output");
    let req_lower = result.requirements_txt.to_lowercase();

    assert!(req_lower.contains("keras"));
    assert!(req_lower.contains("tensorflow"));
    assert!(result.resolution_report.notes.iter().any(|note| note
        .contains("Family knowledge added tensorflow as the default backend for standalone keras.")));
}

#[test]
fn data_driven_family_runtime_behavior_routes_pkg_resources_through_curated_mapping() {
    let _lock = family_runtime_test_lock();
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    apdr::resolver::family_knowledge::init_curated_family_knowledge(&tool_root).unwrap();

    let (resolved, note) = apdr::resolver::family_knowledge::debug_curated_missing_module_recovery(
        "pkg_resources",
        Vec::new(),
    );

    assert_eq!(
        note.as_deref(),
        Some("Added setuptools to provide missing pkg_resources module.")
    );
    assert!(resolved.iter().any(|dependency| {
        dependency.import_name == "pkg_resources"
            && dependency.package_name == "setuptools"
            && dependency.strategy == "recovery:missing-module"
    }));
}

#[test]
fn phase7_family_pillow_fixtures_still_pin_pillow() {
    let _lock = family_runtime_test_lock();
    for case_id in ["2e3b989e0343f0884388ed7ed82eb3b0", "33e2172bafbb5dd794ab"] {
        let result = resolve_phase7_family_fixture(
            case_id,
            &format!("phase7-family-pillow-{case_id}"),
        );

        assert!(
            result.requirements_txt.contains("Pillow==6.2.2"),
            "{case_id} should keep the legacy Pillow pin"
        );
        assert!(
            result.resolution_report.notes.iter().any(|note| note.contains(
                "Family knowledge pinned Pillow to 6.2.2 for Python 2.7 PIL-era compatibility."
            )),
            "{case_id} should keep the legacy Pillow note"
        );
    }
}

#[test]
fn phase7_family_pymc3_fixture_keeps_coherent_bundle() {
    let _lock = family_runtime_test_lock();
    let result = resolve_phase7_family_bundle_fixture(
        "2de2e9a156fe619dbdad762fe1cf84e1",
        "phase7-family-pymc3",
    );
    let req_lower = result.requirements_txt.to_lowercase();

    assert_eq!(result.python_version, "3.10");
    assert!(req_lower.contains("pymc3==3.11.5"));
    assert!(req_lower.contains("theano-pymc==1.1.2"));
    assert!(req_lower.contains("arviz==0.12.1"));
    assert!(req_lower.contains("xarray-einstats==0.6.0"));
    assert!(result.resolved.iter().any(|dependency| {
        dependency.package_name == "Theano-PyMC" && dependency.strategy == "family:legacy-pymc3"
    }));
    assert!(result
        .resolution_report
        .notes
        .iter()
        .any(|note| note.contains("Family knowledge targeted the legacy PyMC3 stack")));
}

#[test]
fn phase7_family_keras_fixtures_add_tensorflow_backend() {
    let _lock = family_runtime_test_lock();
    for case_id in [
        "0830affa1f7f19fd47b06d4cf89ed44d",
        "3a2a081e4f3089920fd8aecefecbe280",
        "3fdd80a08808bd275142d46863e92d68",
    ] {
        let result = resolve_phase7_family_bundle_fixture(
            case_id,
            &format!("phase7-family-keras-{case_id}"),
        );
        let req_lower = result.requirements_txt.to_lowercase();

        assert!(
            req_lower.contains("tensorflow"),
            "{case_id} should keep tensorflow in the resolved requirements"
        );
        assert!(result.resolved.iter().any(|dependency| {
            dependency.package_name == "tensorflow"
                && dependency.strategy == "family:keras-backend"
        }));
        assert!(
            result.resolution_report.notes.iter().any(|note| note.contains(
                "Family knowledge added tensorflow as the default backend for standalone keras."
            )),
            "{case_id} should keep the keras backend note"
        );
    }
}

#[test]
fn phase7_family_pkg_resources_fixtures_add_setuptools() {
    let _lock = family_runtime_test_lock();
    for case_id in ["1e2600ed62d5e76b21ee", "263113", "3a6e4d618afc344aab81"] {
        let result = resolve_phase7_family_fixture(
            case_id,
            &format!("phase7-family-pkg-resources-{case_id}"),
        );
        let (resolved, note) =
            apdr::resolver::family_knowledge::debug_curated_missing_module_recovery(
                "pkg_resources",
                result.resolved.clone(),
            );

        assert_eq!(
            note.as_deref(),
            Some("Added setuptools to provide missing pkg_resources module."),
            "{case_id} should keep the pkg_resources recovery note"
        );
        assert!(resolved.iter().any(|dependency| {
            dependency.import_name == "pkg_resources"
                && dependency.package_name == "setuptools"
                && dependency.strategy == "recovery:missing-module"
        }));
    }
}

#[test]
fn phase7_family_sklearn_fixture_resolves_to_scikit_learn() {
    let _lock = family_runtime_test_lock();
    let result = resolve_phase7_family_fixture(
        "28bf77e9a95ae6b70b14141feacb1f84",
        "phase7-family-sklearn",
    );
    let req_lower = result.requirements_txt.to_lowercase();

    assert!(req_lower.contains("scikit-learn"));
    assert!(result.resolved.iter().any(|dependency| {
        dependency.import_name == "sklearn" && dependency.package_name == "scikit-learn"
    }));
    assert!(!result.resolved.iter().any(|dependency| {
        dependency.import_name == "sklearn" && dependency.package_name == "sklearn"
    }));
}

// ---------------------------------------------------------------------------
// Phase 9 targeted recovery policy tests
// ---------------------------------------------------------------------------

#[test]
fn phase9_targeted_policy_reads_seed_rules() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    // Module provider rules should include pkg_resources and Image.
    assert!(
        policy.module_rules().iter().any(|r| r.id == "mod-pkg-resources"),
        "expected mod-pkg-resources rule in module_rules"
    );
    assert!(
        policy.module_rules().iter().any(|r| r.id == "mod-pil-image"),
        "expected mod-pil-image rule in module_rules"
    );

    // Stop reason rules should include imp and numpy.distutils.
    assert!(
        policy.stop_reason_rules().iter().any(|r| r.id == "stop-imp-removed"),
        "expected stop-imp-removed rule in stop_reason_rules"
    );
    assert!(
        policy.stop_reason_rules().iter().any(|r| r.id == "stop-numpy-distutils"),
        "expected stop-numpy-distutils rule in stop_reason_rules"
    );

    // Compatibility clusters should include torch and tensorflow.
    assert!(
        policy.compatibility_clusters().iter().any(|c| c.id == "compat-torch"),
        "expected compat-torch cluster in compatibility_clusters"
    );
    assert!(
        policy.compatibility_clusters().iter().any(|c| c.id == "compat-tensorflow"),
        "expected compat-tensorflow cluster in compatibility_clusters"
    );

    // Read-only accessors should work.
    let pkg_rule = policy.module_rule_for_alias("pkg_resources");
    assert!(pkg_rule.is_some(), "module_rule_for_alias should find pkg_resources");
    assert_eq!(pkg_rule.unwrap().provider_package, "setuptools");

    let torch_cluster = policy.compatibility_cluster_for_package("torch");
    assert!(torch_cluster.is_some(), "compatibility_cluster_for_package should find torch");
    assert_eq!(torch_cluster.unwrap().id, "compat-torch");
}

#[test]
fn phase9_targeted_policy_rejects_duplicate_module_aliases() {
    use std::io::Write;

    let tmp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let recovery_dir = tmp_dir.path().join("data").join("recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();

    // Write module_rules.json with duplicate alias "pkg_resources" in two rules.
    let module_rules = r#"{
        "module_provider_rules": [
            {
                "id": "rule-a",
                "module_aliases": ["pkg_resources"],
                "provider_package": "setuptools"
            },
            {
                "id": "rule-b",
                "module_aliases": ["pkg_resources"],
                "provider_package": "pkg-resources"
            }
        ],
        "stop_reason_rules": []
    }"#;
    let mut f = std::fs::File::create(recovery_dir.join("module_rules.json")).unwrap();
    f.write_all(module_rules.as_bytes()).unwrap();

    // Write valid empty compatibility_rules.json.
    let compat_rules = r#"{
        "compatibility_clusters": [],
        "companion_rules": [],
        "python_ceiling_rules": []
    }"#;
    let mut f2 = std::fs::File::create(recovery_dir.join("compatibility_rules.json")).unwrap();
    f2.write_all(compat_rules.as_bytes()).unwrap();

    let err = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(tmp_dir.path())
        .expect_err("should reject duplicate module aliases");

    assert!(
        err.contains("duplicate module alias `pkg_resources`"),
        "error should mention the duplicate alias, got: {err}"
    );
    assert!(
        err.contains("rule-a"),
        "error should mention the conflicting rule ID, got: {err}"
    );
}

#[test]
fn phase9_targeted_policy_rejects_unknown_case_ids() {
    use std::io::Write;

    let tmp_dir = tempfile::tempdir().expect("failed to create temp dir");

    // Create a minimal parity manifest with known case IDs.
    let planning_dir = tmp_dir
        .path()
        .join(".planning")
        .join("phases")
        .join("07-failure-baseline-parity-slice");
    std::fs::create_dir_all(&planning_dir).unwrap();
    let manifest = r#"{
        "canonical_case_ids": ["case-aaa", "case-bbb"],
        "tier1_watchlist_case_ids": ["case-ccc"]
    }"#;
    let mut mf = std::fs::File::create(planning_dir.join("07-tier3-parity-manifest.json")).unwrap();
    mf.write_all(manifest.as_bytes()).unwrap();

    // Create a fake tool root two levels down so the manifest resolves.
    let tool_root = tmp_dir.path().join("tools").join("apdr");
    let recovery_dir = tool_root.join("data").join("recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();

    // Write module_rules.json with an unknown case ID.
    let module_rules = r#"{
        "module_provider_rules": [
            {
                "id": "rule-x",
                "module_aliases": ["fake_mod"],
                "provider_package": "fake-pkg",
                "canonical_case_ids": ["case-aaa", "case-UNKNOWN"]
            }
        ],
        "stop_reason_rules": []
    }"#;
    let mut f = std::fs::File::create(recovery_dir.join("module_rules.json")).unwrap();
    f.write_all(module_rules.as_bytes()).unwrap();

    let compat_rules = r#"{
        "compatibility_clusters": [],
        "companion_rules": [],
        "python_ceiling_rules": []
    }"#;
    let mut f2 = std::fs::File::create(recovery_dir.join("compatibility_rules.json")).unwrap();
    f2.write_all(compat_rules.as_bytes()).unwrap();

    let err = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect_err("should reject unknown case IDs");

    assert!(
        err.contains("unknown canonical case ID `case-UNKNOWN`"),
        "error should mention the unknown case ID, got: {err}"
    );
    assert!(
        err.contains("rule-x"),
        "error should mention the rule ID, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Phase 9 targeted module recovery tests (plan 09-02)
// ---------------------------------------------------------------------------

#[test]
fn phase9_targeted_module_recovers_pkg_resources_provider() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    let rule = policy
        .module_rule_for_alias("pkg_resources")
        .expect("should find a provider rule for pkg_resources");
    assert_eq!(
        rule.provider_package, "setuptools",
        "pkg_resources should be provided by setuptools"
    );
    assert_eq!(rule.id, "mod-pkg-resources");
    assert!(
        !rule.canonical_case_ids.is_empty(),
        "pkg_resources provider rule should reference canonical case IDs"
    );
}

#[test]
fn phase9_targeted_module_recovers_image_provider() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    let rule = policy
        .module_rule_for_alias("Image")
        .expect("should find a provider rule for Image");
    assert_eq!(
        rule.provider_package, "Pillow",
        "Image should be provided by Pillow"
    );
    assert_eq!(rule.id, "mod-pil-image");
}

#[test]
fn phase9_targeted_module_recovers_rest_framework_provider() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    let rule = policy
        .module_rule_for_alias("rest_framework")
        .expect("should find a provider rule for rest_framework");
    assert_eq!(
        rule.provider_package, "djangorestframework",
        "rest_framework should be provided by djangorestframework"
    );
    assert_eq!(rule.id, "mod-django-rest-framework");
}

#[test]
fn phase9_targeted_module_marks_removed_runtime_case() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    // imp is a removed stdlib module (Python 3.12+).
    let stop = policy
        .stop_reason_for_module("imp")
        .expect("should find a stop-reason rule for imp");
    assert!(
        stop.reason.contains("removed-runtime"),
        "imp stop reason should contain 'removed-runtime', got: {}",
        stop.reason
    );

    // numpy.distutils was removed in NumPy 2.0.
    let stop_np = policy
        .stop_reason_for_module("numpy.distutils")
        .expect("should find a stop-reason rule for numpy.distutils");
    assert!(
        stop_np.reason.contains("removed-runtime"),
        "numpy.distutils stop reason should contain 'removed-runtime', got: {}",
        stop_np.reason
    );

    // elementtree is a removed standalone package.
    let stop_et = policy
        .stop_reason_for_module("elementtree")
        .expect("should find a stop-reason rule for elementtree");
    assert!(
        stop_et.reason.contains("removed-runtime"),
        "elementtree stop reason should contain 'removed-runtime', got: {}",
        stop_et.reason
    );
}

#[test]
fn phase9_targeted_module_marks_project_local_case() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    // api is typically a project-local module.
    let stop_api = policy
        .stop_reason_for_module("api")
        .expect("should find a stop-reason rule for api");
    assert!(
        stop_api.reason.contains("project-local"),
        "api stop reason should contain 'project-local', got: {}",
        stop_api.reason
    );

    // taggit_autocomplete is a project-local Django app.
    let stop_taggit = policy
        .stop_reason_for_module("taggit_autocomplete")
        .expect("should find a stop-reason rule for taggit_autocomplete");
    assert!(
        stop_taggit.reason.contains("project-local"),
        "taggit_autocomplete stop reason should contain 'project-local', got: {}",
        stop_taggit.reason
    );

    // pizzanuvola_teaser is a project-specific Plone module.
    let stop_pizza = policy
        .stop_reason_for_module("pizzanuvola_teaser")
        .expect("should find a stop-reason rule for pizzanuvola_teaser");
    assert!(
        stop_pizza.reason.contains("project-local"),
        "pizzanuvola_teaser stop reason should contain 'project-local', got: {}",
        stop_pizza.reason
    );

    // _distance_wrap is an internal C extension.
    let stop_dw = policy
        .stop_reason_for_module("_distance_wrap")
        .expect("should find a stop-reason rule for _distance_wrap");
    assert!(
        stop_dw.reason.contains("internal-extension"),
        "_distance_wrap stop reason should contain 'internal-extension', got: {}",
        stop_dw.reason
    );
}

#[test]
fn phase20_module_request_alias_recovers_requests() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    let rule = policy
        .module_rule_for_alias("request")
        .expect("should find a provider rule for request");
    assert_eq!(rule.id, "mod-request-requests");
    assert_eq!(rule.provider_package, "requests");
}

#[test]
fn phase20_module_simplegui_leaves_module_not_found_bucket() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    let stop = policy
        .stop_reason_for_module("simplegui")
        .expect("should find a stop-reason rule for simplegui");
    assert_eq!(stop.id, "stop-simplegui-runtime");
    assert!(
        stop.reason.contains("host-runtime"),
        "simplegui stop reason should contain 'host-runtime', got: {}",
        stop.reason
    );
    assert_eq!(
        apdr::resolver::targeted_recovery::unsolvable_status_for_reason(&stop.reason),
        "skipped-host-runtime"
    );

    let mosquitto = policy
        .stop_reason_for_module("mosquitto")
        .expect("should find a stop-reason rule for mosquitto");
    assert_eq!(mosquitto.id, "stop-mosquitto-runtime");
    assert_eq!(
        apdr::resolver::targeted_recovery::unsolvable_status_for_reason(&mosquitto.reason),
        "skipped-host-runtime"
    );
}

#[test]
fn phase20_module_removed_runtime_becomes_skipped_unsolvable() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    let stop = policy
        .stop_reason_for_module("elementtree")
        .expect("should find a stop-reason rule for elementtree");
    assert!(
        stop.reason.contains("removed-runtime"),
        "elementtree stop reason should contain 'removed-runtime', got: {}",
        stop.reason
    );
    assert_eq!(
        apdr::resolver::targeted_recovery::unsolvable_status_for_reason(&stop.reason),
        "skipped-unsolvable"
    );
}

#[test]
fn phase26_intake_preserves_authored_plan() {
    let response = serde_json::json!({
        "authored_plan": {
            "plan_version": "1",
            "extracted_imports": ["sklearn"],
            "package_mappings": [
                {
                    "import_name": "sklearn",
                    "package_name": "scikit-learn",
                    "source": "llm",
                    "confidence": 0.91
                }
            ],
            "unresolved_imports": [],
            "system_dependency_hints": [],
            "runtime_assumptions": ["python_version=3.11"],
            "smoke_strategy": {
                "mode": "import",
                "import_targets": ["sklearn"],
                "commands": [],
                "rationale": "Verify imports."
            },
            "section_confidence": {
                "imports": 1.0,
                "package_mappings": 0.91
            },
            "authorship": "llm-authored",
            "deterministic_fallback_sections": []
        }
    });

    let authored_plan = apdr::resolver::tier3_llm::parse_authored_plan_response(&response)
        .expect("expected authored plan");

    assert_eq!(authored_plan.extracted_imports, vec!["sklearn"]);
    assert_eq!(authored_plan.package_mappings.len(), 1);
    assert_eq!(authored_plan.package_mappings[0].package_name, "scikit-learn");
    assert_eq!(authored_plan.smoke_strategy.mode, "import");
}

#[test]
fn phase26_intake_preserves_failure_class() {
    let response = serde_json::json!({
        "intake_failure": {
            "failure_class": "schema-validation-failure",
            "reason": "LLM package-resolution call returned no output.",
            "diagnostic_preview": "schema failure while validating authored plan",
            "raw_response_preview": "",
            "authored_plan_status": "unusable",
            "llm_only_behavior": "fail"
        }
    });

    let intake_failure =
        apdr::resolver::tier3_llm::parse_intake_failure_response(&response)
            .expect("expected intake failure");

    assert_eq!(intake_failure.failure_class, "schema-validation-failure");
    assert_eq!(intake_failure.authored_plan_status, "unusable");
    assert_eq!(intake_failure.llm_only_behavior, "fail");
}

#[test]
fn phase26_zero_dependency_intake_returns_deterministic_authored_plan() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-zero-dependency-intake-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    let parse_result = apdr::ParseResult {
        imports: Vec::new(),
        import_paths: Vec::new(),
        config_deps: Vec::new(),
        python_version_min: "3.9".to_string(),
        python_version_max: Some("3.9".to_string()),
        confidence: 0.72,
        scanned_files: vec!["snippet.py".to_string()],
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/phase26-zero-dependency-intake");
    config.allow_llm = true;
    config.llm_only_mode = true;
    config.validate = false;

    let stage = apdr::resolver::tier3_llm::resolve(&[], &parse_result, &mut store, &config, "3.9");

    assert!(stage.authored_plan.is_some());
    assert_eq!(stage.authored_plan_status, "available");
    assert!(stage.intake_failure.is_none());
    assert_eq!(stage.prompts_issued, 0);
    assert!(stage.unresolved.is_empty());
    let plan = stage.authored_plan.unwrap();
    assert!(plan.extracted_imports.is_empty());
    assert!(plan.package_mappings.is_empty());
    assert!(
        plan.runtime_assumptions
            .iter()
            .any(|item| item.contains("no third-party imports were detected"))
    );

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn phase26_zero_dependency_retry_intake_returns_deterministic_authored_plan() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cache_path = tool_root.join("target/test-zero-dependency-retry-intake-cache");
    let mut store = apdr::cache::store::CacheStore::load(&tool_root, cache_path.clone()).unwrap();
    let parse_result = apdr::ParseResult {
        imports: Vec::new(),
        import_paths: Vec::new(),
        config_deps: Vec::new(),
        python_version_min: "3.9".to_string(),
        python_version_max: Some("3.9".to_string()),
        confidence: 0.72,
        scanned_files: vec!["snippet.py".to_string()],
        stdlib_modules: std::collections::BTreeSet::new(),
        attribute_usage: std::collections::BTreeMap::new(),
    };
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/phase26-zero-dependency-retry-intake");
    config.allow_llm = true;
    config.llm_only_mode = true;
    config.validate = false;

    let stage = apdr::resolver::tier3_llm::resolve_with_context(
        &[],
        "import array",
        &parse_result,
        &mut store,
        &config,
        "3.9",
        Some("Retry path context".to_string()),
    );

    assert!(stage.authored_plan.is_some());
    assert_eq!(stage.authored_plan_status, "available");
    assert!(stage.intake_failure.is_none());
    assert_eq!(stage.prompts_issued, 0);
    assert!(stage.unresolved.is_empty());
    let plan = stage.authored_plan.unwrap();
    assert!(plan.extracted_imports.is_empty());
    assert!(plan.package_mappings.is_empty());
    assert!(
        plan.runtime_assumptions
            .iter()
            .any(|item| item.contains("no third-party imports were detected"))
    );

    std::fs::remove_dir_all(cache_path).unwrap();
}

#[test]
fn phase26_llm_only_resolve_path_keeps_zero_dependency_cases_out_of_intake_failure() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/stdlib_only_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/phase26-zero-dependency-resolve-path");
    config.allow_llm = true;
    config.llm_only_mode = true;
    config.validate = false;
    config.validation_backend = apdr::VALIDATION_BACKEND_DOCKER.to_string();

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_ne!(result.validation.status, "llm-intake-failed");
    assert!(result.intake_failure.is_none());
    assert!(result.authored_plan.is_some());
    assert_eq!(result.authored_plan_status, "deterministic-fallback");
    assert!(result.requirements_txt.trim().is_empty());

    std::fs::remove_dir_all(tool_root.join("target/phase26-zero-dependency-resolve-path")).ok();
}

#[test]
fn phase27_author_preserves_authored_docker_plan() {
    let response = serde_json::json!({
        "docker_plan": {
            "plan_version": "1",
            "base_image": "python:3.11-slim",
            "system_packages": ["libxml2-dev", "libxslt1-dev"],
            "environment_variables": ["PIP_DEFAULT_TIMEOUT=100"],
            "working_directory": "/app",
            "command": ["python", "/app/smoke_test.py"],
            "smoke_strategy": {
                "mode": "import",
                "import_targets": ["scrapy"],
                "commands": [],
                "rationale": "Verify imports."
            },
            "rationale": "Install build deps for lxml-backed validation.",
            "section_confidence": {
                "base_image": 1.0,
                "system_packages": 0.84
            },
            "authorship": "llm-authored",
            "deterministic_fallback_sections": []
        }
    });

    let docker_plan =
        apdr::resolver::tier3_llm::parse_authored_docker_plan_response(&response)
            .expect("expected authored docker plan");

    assert_eq!(docker_plan.base_image, "python:3.11-slim");
    assert_eq!(docker_plan.system_packages.len(), 2);
    assert_eq!(docker_plan.command, vec!["python", "/app/smoke_test.py"]);
    assert_eq!(docker_plan.smoke_strategy.import_targets, vec!["scrapy"]);
}

fn phase26_truth_fixture_result(output_name: &str) -> apdr::ResolveResult {
    apdr::ResolveResult {
        snippet_path: PathBuf::from("snippet.py"),
        python_version: "3.11".to_string(),
        parse_result: apdr::ParseResult {
            imports: vec!["sklearn".to_string()],
            import_paths: Vec::new(),
            config_deps: Vec::new(),
            python_version_min: "3.11".to_string(),
            python_version_max: None,
            confidence: 0.9,
            scanned_files: vec!["snippet.py".to_string()],
            stdlib_modules: std::collections::BTreeSet::new(),
            attribute_usage: std::collections::BTreeMap::new(),
        },
        run_contract: apdr::RunContractMetadata {
            run_contract_version: "1".to_string(),
            model_name: "qwen3.5:9b".to_string(),
            base_url: "http://localhost:11434".to_string(),
            run_intent: "phase26-test".to_string(),
            execution_mode: "llm".to_string(),
            cache_state: "warm".to_string(),
            host_architecture: "arm64".to_string(),
            apdr_binary_architecture: "arm64".to_string(),
            python_architecture: "arm64".to_string(),
            llm_context_window: "16384".to_string(),
            inference_policy: "phase26".to_string(),
            build_profile: "debug".to_string(),
        },
        solvability: None,
        resolved: vec![apdr::ResolvedDependency {
            import_name: "sklearn".to_string(),
            package_name: "scikit-learn".to_string(),
            version: Some("1.5.0".to_string()),
            strategy: "llm".to_string(),
            confidence: 0.91,
        }],
        unresolved: Vec::new(),
        requirements_txt: "scikit-learn==1.5.0\n".to_string(),
        lockfile: Some("scikit-learn==1.5.0\n".to_string()),
        build_image_id: None,
        authored_plan: Some(apdr::AuthoredCasePlan {
            plan_version: "1".to_string(),
            extracted_imports: vec!["sklearn".to_string()],
            package_mappings: vec![apdr::AuthoredPlanPackageMapping {
                import_name: "sklearn".to_string(),
                package_name: "scikit-learn".to_string(),
                source: "llm".to_string(),
                confidence: 0.91,
            }],
            unresolved_imports: Vec::new(),
            system_dependency_hints: Vec::new(),
            runtime_assumptions: vec!["python_version=3.11".to_string()],
            smoke_strategy: apdr::SmokeStrategy {
                mode: "import".to_string(),
                import_targets: vec!["sklearn".to_string()],
                commands: Vec::new(),
                rationale: "Verify imports.".to_string(),
            },
            section_confidence: std::collections::BTreeMap::from([
                ("imports".to_string(), 1.0),
                ("package_mappings".to_string(), 0.91),
            ]),
            authorship: "llm-authored".to_string(),
            deterministic_fallback_sections: Vec::new(),
        }),
        authored_plan_status: "available".to_string(),
        docker_plan: Some(apdr::AuthoredDockerPlan {
            plan_version: "1".to_string(),
            base_image: "python:3.11-slim".to_string(),
            system_packages: vec!["libxml2-dev".to_string()],
            environment_variables: vec!["PIP_DEFAULT_TIMEOUT=100".to_string()],
            working_directory: "/app".to_string(),
            command: vec!["python".to_string(), "/app/smoke_test.py".to_string()],
            smoke_strategy: apdr::SmokeStrategy {
                mode: "import".to_string(),
                import_targets: vec!["sklearn".to_string()],
                commands: Vec::new(),
                rationale: "Verify imports.".to_string(),
            },
            rationale: "Install build helpers before validation.".to_string(),
            section_confidence: std::collections::BTreeMap::from([
                ("base_image".to_string(), 1.0),
                ("system_packages".to_string(), 0.8),
            ]),
            authorship: "llm-authored".to_string(),
            deterministic_fallback_sections: Vec::new(),
        }),
        docker_plan_status: "available".to_string(),
        intake_failure: Some(apdr::IntakeFailureRecord {
            failure_class: "schema-validation-failure".to_string(),
            reason: "LLM package-resolution call returned no output.".to_string(),
            diagnostic_preview: "schema failure while validating authored plan".to_string(),
            raw_response_preview: String::new(),
            authored_plan_status: "unusable".to_string(),
            llm_only_behavior: "fail".to_string(),
        }),
        validation: apdr::ValidationSummary {
            succeeded: false,
            status: "llm-intake-failed".to_string(),
            reason: Some("LLM intake failed".to_string()),
            validation_backend: "docker".to_string(),
            debug_dir: Some(format!("target/{output_name}/.apdr-debug")),
            ..Default::default()
        },
        resolution_report: apdr::ResolutionReport::default(),
    }
}

#[test]
fn phase26_truth_write_outputs_include_case_plan_and_intake_failure_artifacts() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = tool_root.join("target/phase26-truth-artifacts");
    let result = phase26_truth_fixture_result("phase26-truth-artifacts");

    if output_dir.exists() {
        std::fs::remove_dir_all(&output_dir).unwrap();
    }

    let (_requirements_path, _report_path) = result.write_outputs(&output_dir).unwrap();

    let case_plan = std::fs::read_to_string(output_dir.join("case-plan.json")).unwrap();
    let intake_failure =
        std::fs::read_to_string(output_dir.join("intake-failure.json")).unwrap();

    assert!(case_plan.contains("\"smoke_strategy\""));
    assert!(case_plan.contains("\"authorship\": \"llm-authored\""));
    assert!(intake_failure.contains("\"failure_class\": \"schema-validation-failure\""));

    std::fs::remove_dir_all(output_dir).unwrap();
}

#[test]
fn phase27_artifact_write_outputs_include_docker_plan_artifact() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = tool_root.join("target/phase27-truth-artifacts");
    let result = phase26_truth_fixture_result("phase27-truth-artifacts");

    if output_dir.exists() {
        std::fs::remove_dir_all(&output_dir).unwrap();
    }

    let (_requirements_path, _report_path) = result.write_outputs(&output_dir).unwrap();

    let docker_plan = std::fs::read_to_string(output_dir.join("docker-plan.json")).unwrap();

    assert!(docker_plan.contains("\"base_image\": \"python:3.11-slim\""));
    assert!(docker_plan.contains("\"authorship\": \"llm-authored\""));

    std::fs::remove_dir_all(output_dir).unwrap();
}

#[test]
fn phase26_truth_summary_lines_include_authorship_metadata() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = tool_root.join("target/phase26-truth-summary");
    let result = phase26_truth_fixture_result("phase26-truth-summary");
    let requirements_path = output_dir.join("requirements.txt");
    let report_path = output_dir.join("resolution-report.txt");
    let summary = result.summary_lines(&requirements_path, &report_path);

    assert!(summary.contains("AUTHORED_PLAN_STATUS=available"));
    assert!(summary.contains("AUTHORED_PLAN_PATH="));
    assert!(summary.contains("AUTHORED_PLAN_AUTHORSHIP=llm-authored"));
    assert!(summary.contains("INTAKE_FAILURE_CLASS=schema-validation-failure"));
    assert!(summary.contains("INTAKE_FAILURE_PATH="));
}

#[test]
fn phase27_artifact_summary_lines_include_docker_plan_metadata() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = tool_root.join("target/phase27-truth-summary");
    let result = phase26_truth_fixture_result("phase27-truth-summary");
    let requirements_path = output_dir.join("requirements.txt");
    let report_path = output_dir.join("report.txt");
    let summary = result.summary_lines(&requirements_path, &report_path);

    assert!(summary.contains("DOCKER_PLAN_STATUS=available"));
    assert!(summary.contains("DOCKER_PLAN_PATH="));
    assert!(summary.contains("DOCKER_PLAN_AUTHORSHIP=llm-authored"));
    assert!(summary.contains("DOCKER_PLAN_FALLBACK_SECTIONS="));
}

#[test]
fn phase27_handoff_summary_lines_include_executed_docker_truth() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = tool_root.join("target/phase27-handoff-summary");
    let mut result = phase26_truth_fixture_result("phase27-handoff-summary");
    result.validation.executed_dockerfile_path = Some(
        output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/Dockerfile.executed")
            .display()
            .to_string(),
    );
    result.validation.docker_build_command_path = Some(
        output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/docker-build.command.txt")
            .display()
            .to_string(),
    );
    result.validation.docker_run_command_path = Some(
        output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/docker-run.command.txt")
            .display()
            .to_string(),
    );
    result.validation.executed_image_ref = Some("sha256:deadbeef".to_string());
    result.validation.image_handoff_verified = true;
    result.validation.image_inspect_path = Some(
        output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/docker-image.inspect.txt")
            .display()
            .to_string(),
    );
    let requirements_path = output_dir.join("requirements.txt");
    let report_path = output_dir.join("report.txt");
    let summary = result.summary_lines(&requirements_path, &report_path);

    assert!(summary.contains("AUTHORED_DOCKERFILE_PATH="));
    assert!(summary.contains("EXECUTED_DOCKERFILE_PATH="));
    assert!(summary.contains("DOCKER_BUILD_COMMAND_PATH="));
    assert!(summary.contains("DOCKER_RUN_COMMAND_PATH="));
    assert!(summary.contains("EXECUTED_IMAGE_REF=sha256:deadbeef"));
    assert!(summary.contains("IMAGE_HANDOFF_VERIFIED=true"));
    assert!(summary.contains("IMAGE_INSPECT_PATH="));
}

#[test]
fn phase28_recovery_write_outputs_include_recovery_attempts_artifact() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = tool_root.join("target/phase28-recovery-artifacts");
    let mut result = phase26_truth_fixture_result("phase28-recovery-artifacts");
    result.validation.recovery_outcome = Some("no-output".to_string());
    result.validation.recovery_attempts = vec![apdr::RecoveryAttemptRecord {
        attempt_index: 1,
        recovery_outcome: "no-output".to_string(),
        failure_class: "invalid-json".to_string(),
        diagnostic_preview: "attempt 1: provider returned malformed JSON".to_string(),
        authored_plan_path: output_dir.join("case-plan.json").display().to_string(),
        docker_plan_path: output_dir.join("docker-plan.json").display().to_string(),
        intake_failure_path: String::new(),
        combined_log_path: output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/combined.log")
            .display()
            .to_string(),
        executed_dockerfile_path: output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/Dockerfile.executed")
            .display()
            .to_string(),
        docker_build_command_path: output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/docker-build.command.txt")
            .display()
            .to_string(),
        docker_run_command_path: output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/docker-run.command.txt")
            .display()
            .to_string(),
        image_inspect_path: output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/docker-image.inspect.txt")
            .display()
            .to_string(),
        executed_image_ref: "sha256:phase28".to_string(),
        wrong_package: "scrapy".to_string(),
        correct_package: String::new(),
        version: String::new(),
        add_package: String::new(),
        remove_package: String::new(),
        notes: vec!["LLM recovery diagnostics captured.".to_string()],
    }];
    result.validation.failure_truth_class = Some("llm-no-output".to_string());
    result.validation.failure_truth_detail =
        Some("invalid-json: attempt 1: provider returned malformed JSON".to_string());

    if output_dir.exists() {
        std::fs::remove_dir_all(&output_dir).unwrap();
    }

    let (_requirements_path, _report_path) = result.write_outputs(&output_dir).unwrap();

    let recovery_attempts =
        std::fs::read_to_string(output_dir.join("recovery-attempts.json")).unwrap();
    let report = std::fs::read_to_string(output_dir.join("resolution-report.txt")).unwrap();

    assert!(recovery_attempts.contains("\"recovery_outcome\": \"no-output\""));
    assert!(recovery_attempts.contains("\"failure_class\": \"invalid-json\""));
    assert!(report.contains("recovery_attempts_path: recovery-attempts.json"));
    assert!(report.contains("failure_truth_class: llm-no-output"));

    std::fs::remove_dir_all(output_dir).unwrap();
}

#[test]
fn phase28_recovery_summary_lines_include_recovery_truth_fields() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output_dir = tool_root.join("target/phase28-recovery-summary");
    let mut result = phase26_truth_fixture_result("phase28-recovery-summary");
    result.validation.recovery_outcome = Some("provider-failure".to_string());
    result.validation.failure_truth_class =
        Some("provider-tooling-failure".to_string());
    result.validation.failure_truth_detail =
        Some("timeout: attempt 1 timed out waiting for structured JSON".to_string());
    result.validation.recovery_attempts = vec![apdr::RecoveryAttemptRecord {
        attempt_index: 1,
        recovery_outcome: "provider-failure".to_string(),
        failure_class: "timeout".to_string(),
        diagnostic_preview: "attempt 1 timed out waiting for structured JSON".to_string(),
        authored_plan_path: output_dir.join("case-plan.json").display().to_string(),
        docker_plan_path: output_dir.join("docker-plan.json").display().to_string(),
        intake_failure_path: String::new(),
        combined_log_path: output_dir
            .join(".apdr-debug/attempts/attempt-001-py-3_11/combined.log")
            .display()
            .to_string(),
        executed_dockerfile_path: String::new(),
        docker_build_command_path: String::new(),
        docker_run_command_path: String::new(),
        image_inspect_path: String::new(),
        executed_image_ref: String::new(),
        wrong_package: String::new(),
        correct_package: String::new(),
        version: String::new(),
        add_package: String::new(),
        remove_package: String::new(),
        notes: Vec::new(),
    }];
    let requirements_path = output_dir.join("requirements.txt");
    let report_path = output_dir.join("report.txt");
    let summary = result.summary_lines(&requirements_path, &report_path);

    assert!(summary.contains("RECOVERY_ATTEMPTS_PATH="));
    assert!(summary.contains("RECOVERY_OUTCOME=provider-failure"));
    assert!(summary.contains("FAILURE_TRUTH_CLASS=provider-tooling-failure"));
    assert!(summary.contains("FAILURE_TRUTH_DETAIL=timeout: attempt 1 timed out waiting for structured JSON"));
}

#[test]
fn phase26_truth_llm_only_no_output_becomes_intake_failure() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/sample_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/phase26-llm-only-intake-failed");
    config.validate = false;
    config.allow_llm = true;
    config.llm_only_mode = true;

    let prior_python = std::env::var("APDR_PYTHON").ok();
    std::env::set_var("APDR_PYTHON", "python-that-does-not-exist");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    if let Some(previous) = prior_python {
        std::env::set_var("APDR_PYTHON", previous);
    } else {
        std::env::remove_var("APDR_PYTHON");
    }

    assert!(result.authored_plan.is_none());
    assert_eq!(result.authored_plan_status, "unusable");
    assert_eq!(result.validation.status, "llm-intake-failed");
    assert_eq!(
        result
            .intake_failure
            .as_ref()
            .map(|failure| failure.failure_class.as_str()),
        Some("empty-output")
    );
}

// ---------------------------------------------------------------------------
// Phase 9 targeted compatibility recovery tests
// ---------------------------------------------------------------------------

#[test]
fn phase9_targeted_compatibility_recovers_torch_cluster() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    // The torch compatibility cluster should be found by package lookup.
    let cluster = policy
        .compatibility_cluster_for_package("torch")
        .expect("should find a compatibility cluster for torch");
    assert_eq!(cluster.id, "compat-torch");
    assert!(
        cluster.preferred_versions.contains_key("torch"),
        "compat-torch should have a preferred version for torch"
    );
    assert!(
        cluster.preferred_versions.contains_key("torchvision"),
        "compat-torch should have a preferred version for torchvision"
    );

    // The cluster should also be found by log trigger substring.
    let log = "ERROR: contradictory pins for torch - cannot install torch==1.9.0 and torch==2.1.0";
    let cluster_from_log = policy
        .compatibility_cluster_for_log(log)
        .expect("should find a compatibility cluster from torch log");
    assert_eq!(
        cluster_from_log.id, "compat-torch",
        "log-triggered cluster should be compat-torch"
    );

    // Also check torchvision anchor.
    let tv_cluster = policy
        .compatibility_cluster_for_package("torchvision")
        .expect("should find a compatibility cluster for torchvision");
    assert_eq!(tv_cluster.id, "compat-torch");

    // Verify the cluster notes reference the canonical recovery behavior.
    assert!(
        cluster.notes.contains("torch") && cluster.notes.contains("torchvision"),
        "compat-torch notes should reference both torch and torchvision, got: {}",
        cluster.notes
    );
}

#[test]
fn phase9_targeted_compatibility_recovers_tensorflow_cluster() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    // The tensorflow compatibility cluster should be found by package lookup.
    let cluster = policy
        .compatibility_cluster_for_package("tensorflow")
        .expect("should find a compatibility cluster for tensorflow");
    assert_eq!(cluster.id, "compat-tensorflow");
    assert!(
        cluster.preferred_versions.contains_key("tensorflow"),
        "compat-tensorflow should have a preferred version for tensorflow"
    );
    assert!(
        cluster.preferred_versions.contains_key("keras"),
        "compat-tensorflow should have a preferred version for keras"
    );
    assert!(
        cluster.preferred_versions.contains_key("tensorboard"),
        "compat-tensorflow should have a preferred version for tensorboard"
    );

    // keras and tensorboard should also resolve to the same cluster.
    let keras_cluster = policy
        .compatibility_cluster_for_package("keras")
        .expect("should find a compatibility cluster for keras");
    assert_eq!(keras_cluster.id, "compat-tensorflow");

    let tb_cluster = policy
        .compatibility_cluster_for_package("tensorboard")
        .expect("should find a compatibility cluster for tensorboard");
    assert_eq!(tb_cluster.id, "compat-tensorflow");

    // The cluster should be found by a keras log trigger.
    let log = "ERROR: Cannot install keras==3.0.0 and tensorflow==2.18.0";
    let cluster_from_log = policy
        .compatibility_cluster_for_log(log)
        .expect("should find a compatibility cluster from keras log");
    assert_eq!(cluster_from_log.id, "compat-tensorflow");

    // The cluster should reference the Phase 8 family runtime.
    assert!(
        cluster.family_ref.as_deref() == Some("legacy-tensorflow"),
        "compat-tensorflow should reference family_ref legacy-tensorflow, got: {:?}",
        cluster.family_ref
    );

    // Verify companions include keras and tensorboard.
    let companion_packages: Vec<&str> = cluster
        .companions
        .iter()
        .map(|c| c.package.as_str())
        .collect();
    assert!(
        companion_packages.contains(&"keras"),
        "compat-tensorflow companions should include keras"
    );
    assert!(
        companion_packages.contains(&"tensorboard"),
        "compat-tensorflow companions should include tensorboard"
    );
}

#[test]
fn phase9_targeted_compatibility_normalizes_transitive_specifier() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    // PyJWT>=2.0.0 should be parseable, and the companion rule should fire.
    let pyjwt_companion = policy
        .companion_rule_for_package("pyjwt")
        .expect("should find a companion rule for PyJWT");
    assert_eq!(
        pyjwt_companion.companion_package, "cryptography",
        "PyJWT companion should be cryptography"
    );

    // python-dateutil should also have a companion rule.
    let dateutil_companion = policy
        .companion_rule_for_package("python-dateutil")
        .expect("should find a companion rule for python-dateutil");
    assert_eq!(
        dateutil_companion.companion_package, "six",
        "python-dateutil companion should be six"
    );

    // Verify that the cluster lookup confirms the normalization pipeline works.
    let numpy_cluster = policy
        .compatibility_cluster_for_package("numpy")
        .expect("should find a compatibility cluster for numpy");
    assert_eq!(numpy_cluster.id, "compat-numpy");
    assert!(
        numpy_cluster.preferred_versions.contains_key("numpy"),
        "compat-numpy should have a preferred version for numpy"
    );

    // Also verify the pymc ceiling rule works.
    let pymc_ceiling = policy
        .python_ceiling_for_package("pymc3")
        .expect("should find a python ceiling rule for pymc3");
    assert_eq!(
        pymc_ceiling.max_python, "3.11",
        "pymc3 ceiling should be 3.11"
    );
}

#[test]
fn phase20_compat_beautifulsoup_rewrites_to_bs4() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");
    let cluster = policy
        .compatibility_cluster_for_package("BeautifulSoup")
        .expect("should find a compatibility cluster for BeautifulSoup");

    let mut resolved = vec![apdr::ResolvedDependency {
        import_name: "BeautifulSoup".to_string(),
        package_name: "BeautifulSoup".to_string(),
        version: Some("3.2.2".to_string()),
        strategy: "cache:seed".to_string(),
        confidence: 0.7,
    }];

    let notes = apdr::resolver::targeted_recovery::apply_compatibility_cluster(
        cluster,
        &mut resolved,
        "phase20-test",
    );

    assert_eq!(cluster.id, "compat-beautifulsoup-rename");
    assert!(!notes.is_empty(), "expected replacement notes for BeautifulSoup");
    assert_eq!(resolved[0].package_name, "beautifulsoup4");
    assert_eq!(resolved[0].version.as_deref(), Some("4.12.3"));
}

#[test]
fn phase20_compat_pymc3_keeps_python_floor() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");

    let resolved = vec![apdr::ResolvedDependency {
        import_name: "pymc3".to_string(),
        package_name: "pymc3".to_string(),
        version: Some("3.11.5".to_string()),
        strategy: "family:legacy-pymc3".to_string(),
        confidence: 0.9,
    }];

    let filtered = apdr::resolver::targeted_recovery::filter_candidate_versions_for_resolved(
        &policy,
        &resolved,
        vec![
            "3.12".to_string(),
            "3.11".to_string(),
            "3.10".to_string(),
            "3.9".to_string(),
            "2.7".to_string(),
        ],
    );

    assert_eq!(filtered, vec!["3.11", "3.10", "3.9"]);
}

#[test]
fn phase20_compat_opencv_cluster_converges() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let policy = apdr::resolver::targeted_recovery::load_targeted_recovery_policy(&tool_root)
        .expect("seed policy files should load and validate");
    let cluster = policy
        .compatibility_cluster_for_package("opencv-python")
        .expect("should find a compatibility cluster for opencv-python");

    let mut resolved = vec![
        apdr::ResolvedDependency {
            import_name: "cv2".to_string(),
            package_name: "opencv-python".to_string(),
            version: Some("4.1.0.25".to_string()),
            strategy: "heuristic:cv2".to_string(),
            confidence: 0.7,
        },
        apdr::ResolvedDependency {
            import_name: "numpy".to_string(),
            package_name: "numpy".to_string(),
            version: Some("1.21.6".to_string()),
            strategy: "family:legacy-pymc3".to_string(),
            confidence: 0.8,
        },
    ];

    let notes = apdr::resolver::targeted_recovery::apply_compatibility_cluster(
        cluster,
        &mut resolved,
        "phase20-test",
    );

    let opencv = resolved
        .iter()
        .find(|dep| dep.import_name == "cv2")
        .expect("expected cv2 dependency");
    let numpy = resolved
        .iter()
        .find(|dep| dep.package_name == "numpy")
        .expect("expected numpy dependency");

    assert_eq!(cluster.id, "compat-opencv-headless-legacy");
    assert!(!notes.is_empty(), "expected convergence notes for opencv cluster");
    assert_eq!(opencv.package_name, "opencv-python-headless");
    assert_eq!(opencv.version.as_deref(), Some("3.4.11.43"));
    assert_eq!(numpy.version.as_deref(), Some("1.24.4"));
}
