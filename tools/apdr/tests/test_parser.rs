use std::path::PathBuf;

#[test]
fn parser_extracts_non_stdlib_imports() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/sample_snippet.py");
    let parsed = apdr::parser::parse_snippet(&snippet, &tool_root.join("data"), true).unwrap();

    assert!(parsed.imports.contains(&"requests".to_string()));
    assert!(parsed.imports.contains(&"bs4".to_string()));
    assert!(parsed.import_paths.contains(&"bs4.BeautifulSoup".to_string()));
    assert!(!parsed.imports.contains(&"json".to_string()));
}

#[test]
fn parser_detects_python27_and_uses_python27_stdlib() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/python2_snippet.py");
    let parsed = apdr::parser::parse_snippet(&snippet, &tool_root.join("data"), true).unwrap();

    assert_eq!(parsed.python_version_min, "2.7");
    assert_eq!(parsed.python_version_max.as_deref(), Some("2.7"));
    assert!(parsed.imports.contains(&"scrapy".to_string()));
    assert!(!parsed.imports.contains(&"traceback".to_string()));
}

#[test]
fn parser_ignores_generated_benchmark_requirements() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/generated_config_case/snippet.py");
    let parsed = apdr::parser::parse_snippet(&snippet, &tool_root.join("data"), true).unwrap();

    assert!(parsed.imports.contains(&"requests".to_string()));
    assert!(parsed.config_deps.is_empty());
}
