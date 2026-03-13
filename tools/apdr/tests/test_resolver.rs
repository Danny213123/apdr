use std::path::PathBuf;

#[test]
fn resolver_maps_seeded_imports_to_packages() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/sample_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-output");
    config.validate_with_docker = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert!(result.requirements_txt.contains("requests==2.32.3"));
    assert!(result.requirements_txt.contains("beautifulsoup4==4.12.3"));
    assert!(result.validation.succeeded);
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
    };

    let stage = apdr::resolver::tier2_heuristic::resolve(&["sip".to_string()], &parse_result, &mut store, "3.9");
    assert!(stage.resolved.iter().all(|item| item.package_name != "scipy"));

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
    };

    let stage = apdr::resolver::tier1_cache::resolve(&parse_result, &mut store, "3.9");
    assert_eq!(stage.resolved.len(), 1);
    assert_eq!(stage.resolved[0].package_name, "python-ldap");
    assert_eq!(stage.resolved[0].strategy, "cache:seed");

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
    config.validate_with_docker = false;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert!(result.requirements_txt.contains("pymc3==3.11.5"));
    assert!(result.requirements_txt.contains("Theano-PyMC==1.1.2"));
    assert!(result.requirements_txt.contains("numpy==1.21.6"));
    assert!(result.requirements_txt.contains("pandas==1.5.3"));
    assert!(result.requirements_txt.contains("scipy==1.7.3"));
}

#[test]
fn resolver_uses_family_bundle_for_py2_style_pymc3_benchmarks() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/legacy_pymc3_py2_style_snippet.py");
    let mut config = apdr::ResolveConfig::for_tool_root(&tool_root);
    config.output_dir = tool_root.join("target/test-legacy-pymc3-py2-style-output");
    config.validate_with_docker = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "2.7");
    assert!(result.requirements_txt.contains("pymc3==3.11.5"));
    assert!(result.requirements_txt.contains("Theano-PyMC==1.1.2"));
    assert!(result.requirements_txt.contains("numpy==1.21.6"));
    assert!(result.requirements_txt.contains("pandas==1.5.3"));
    assert!(result.requirements_txt.contains("scipy==1.7.3"));
    assert!(result
        .resolution_report
        .notes
        .iter()
        .any(|note| note.contains("Family knowledge targeted the legacy PyMC3 stack at Python 3.10")));
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
