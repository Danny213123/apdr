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
    config.validate_with_docker = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert_eq!(result.python_version, "2.7");
    assert!(result.requirements_txt.contains("pymc3==3.11.5"));
    assert!(result.requirements_txt.contains("Theano-PyMC==1.1.2"));
    assert!(result.requirements_txt.contains("arviz==0.12.1"));
    assert!(result.requirements_txt.contains("numpy==1.21.6"));
    assert!(result.requirements_txt.contains("pandas==1.5.3"));
    assert!(result.requirements_txt.contains("scipy==1.7.3"));
    assert!(result.requirements_txt.contains("setuptools==69.5.1"));
    assert!(result.requirements_txt.contains("xarray==2022.9.0"));
    assert!(result.requirements_txt.contains("xarray-einstats==0.6.0"));
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
    config.validate_with_docker = false;
    config.execute_snippet = false;
    config.python_version_range = 5;

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    assert!(result.requirements_txt.contains("tensorflow==1.15.5"));
    assert!(result.requirements_txt.contains("keras==2.3.1"));
    assert!(result.requirements_txt.contains("numpy==1.16.6"));
    assert!(result.requirements_txt.contains("gym==0.17.3"));
    assert!(result
        .resolution_report
        .notes
        .iter()
        .any(|note| note.contains("Family knowledge targeted the legacy TensorFlow/Keras stack at Python 3.7")));
}

#[test]
fn legacy_tensorflow_validation_prefers_py37_before_py27() {
    let parse_result = apdr::ParseResult {
        imports: vec!["tensorflow".to_string(), "keras".to_string(), "gym".to_string()],
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
    };
    let resolved = vec![
        apdr::ResolvedDependency {
            import_name: "tensorflow".to_string(),
            package_name: "tensorflow".to_string(),
            version: Some("1.15.5".to_string()),
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

    assert_eq!(versions, vec!["3.7", "2.7", "3.8"]);
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
    assert_eq!(result.assigned_versions.get("click").map(String::as_str), Some("6.6"));
    assert_eq!(
        result.assigned_versions.get("pip-tools").map(String::as_str),
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
