//! Integration tests that prompt the live LLM (Ollama).
//!
//! These tests require:
//!   - Ollama running locally on port 11434
//!   - A model loaded (default: gemma3:12b)
//!   - Python 3.9+ with pydantic installed
//!
//! Run with: cargo test --test test_llm_integration -- --ignored --nocapture

use std::path::PathBuf;

fn llm_config(tool_root: &std::path::Path, output_name: &str) -> apdr::ResolveConfig {
    let mut config = apdr::ResolveConfig::for_tool_root(tool_root);
    config.output_dir = tool_root.join(format!("target/test-llm-{output_name}"));
    config.allow_llm = true;
    config.llm_only_mode = true; // bypass tier1/tier2, force LLM for all imports
    config.validate = false;
    config.execute_snippet = false;
    config.parallel_versions = false;
    config
}

/// Verify the LLM was actually called and contributed to resolution.
fn assert_llm_was_used(result: &apdr::ResolveResult) {
    assert!(
        result.resolution_report.llm_calls > 0,
        "Expected LLM calls > 0, got {}. Notes: {:?}",
        result.resolution_report.llm_calls,
        result.resolution_report.notes,
    );
}

// ---------------------------------------------------------------------------
// Test 1: cv2 → opencv-python, PIL → Pillow (Pattern A: C-extension wrappers)
// Expected: PASS
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_cv2_pil_to_correct_packages() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_cv2_pil_snippet.py");
    let config = llm_config(&tool_root, "cv2-pil");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    // cv2 must map to opencv-python (not opencv, not cv2)
    assert!(
        req_lower.contains("opencv-python") || req_lower.contains("opencv_python"),
        "LLM should map cv2 → opencv-python, got:\n{}",
        result.requirements_txt,
    );

    // PIL must map to Pillow (not PIL, not python-pil)
    assert!(
        req_lower.contains("pillow"),
        "LLM should map PIL → Pillow, got:\n{}",
        result.requirements_txt,
    );

    // numpy should be resolved
    assert!(
        req_lower.contains("numpy"),
        "LLM should resolve numpy, got:\n{}",
        result.requirements_txt,
    );

    assert!(
        result.unresolved.is_empty(),
        "All imports should be resolved, unresolved: {:?}",
        result.unresolved,
    );
}

// ---------------------------------------------------------------------------
// Test 2: flask_cors → Flask-Cors, redis → redis (Pattern D: Flask prefix)
// Expected: PASS
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_flask_extensions_and_redis() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_flask_redis_snippet.py");
    let config = llm_config(&tool_root, "flask-redis");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    // flask should be resolved
    assert!(
        req_lower.contains("flask"),
        "LLM should resolve flask, got:\n{}",
        result.requirements_txt,
    );

    // flask_cors must map to Flask-Cors (not flask-cors-decorator, etc.)
    assert!(
        req_lower.contains("flask-cors") || req_lower.contains("flask_cors"),
        "LLM should map flask_cors → Flask-Cors, got:\n{}",
        result.requirements_txt,
    );

    // redis should be resolved
    assert!(
        req_lower.contains("redis"),
        "LLM should resolve redis, got:\n{}",
        result.requirements_txt,
    );

    assert!(
        result.unresolved.is_empty(),
        "All imports should be resolved, unresolved: {:?}",
        result.unresolved,
    );
}

// ---------------------------------------------------------------------------
// Test 3: serial → pyserial, yaml → PyYAML (Pattern E + different name)
// Expected: PASS
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_serial_and_yaml_to_correct_packages() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_serial_yaml_snippet.py");
    let config = llm_config(&tool_root, "serial-yaml");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    // serial must map to pyserial (not serial, not serialport)
    assert!(
        req_lower.contains("pyserial"),
        "LLM should map serial → pyserial, got:\n{}",
        result.requirements_txt,
    );

    // yaml must map to PyYAML (not yaml, not pyyml)
    assert!(
        req_lower.contains("pyyaml"),
        "LLM should map yaml → PyYAML, got:\n{}",
        result.requirements_txt,
    );

    assert!(
        result.unresolved.is_empty(),
        "All imports should be resolved, unresolved: {:?}",
        result.unresolved,
    );
}

// ---------------------------------------------------------------------------
// Test 4: sklearn → scikit-learn, pandas → pandas (Pattern F: different name)
// Expected: PASS
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_sklearn_to_scikit_learn() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_sklearn_pandas_snippet.py");
    let config = llm_config(&tool_root, "sklearn-pandas");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    // sklearn must map to scikit-learn (not sklearn, not scikit_learn)
    assert!(
        req_lower.contains("scikit-learn") || req_lower.contains("scikit_learn"),
        "LLM should map sklearn → scikit-learn, got:\n{}",
        result.requirements_txt,
    );

    // pandas should be resolved as-is
    assert!(
        req_lower.contains("pandas"),
        "LLM should resolve pandas, got:\n{}",
        result.requirements_txt,
    );

    assert!(
        result.unresolved.is_empty(),
        "All imports should be resolved, unresolved: {:?}",
        result.unresolved,
    );
}

// ---------------------------------------------------------------------------
// Test 5: maya.cmds, maya.mel, pymel — host-app runtime, unsolvable
// Expected: FAIL (skipped as host-runtime)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_maya_host_runtime_snippet() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_maya_unsolvable_snippet.py");
    let config = llm_config(&tool_root, "maya-unsolvable");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    // The solvability assessment or skip-detection should flag this as
    // a host-application runtime that can't be resolved from PyPI.
    let status = &result.validation.status;
    let reason = result.validation.reason.as_deref().unwrap_or("");
    let notes_joined = result.resolution_report.notes.join(" ");

    let is_skipped = status.contains("skipped")
        || reason.to_lowercase().contains("maya")
        || reason.to_lowercase().contains("host")
        || notes_joined.to_lowercase().contains("maya")
        || notes_joined.to_lowercase().contains("unsolvable")
        || notes_joined.to_lowercase().contains("skip");

    assert!(
        is_skipped,
        "Maya snippet should be flagged as unsolvable host-runtime.\n\
         Status: {status}\n\
         Reason: {reason}\n\
         Notes: {:?}",
        result.resolution_report.notes,
    );
}

// ---------------------------------------------------------------------------
// Test 6: fiona, shapely, geopandas — geospatial stack (Pattern: system-dep packages)
// Expected: PASS — LLM should resolve all geospatial imports correctly
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_geospatial_fiona_stack() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/geospatial_fiona_snippet.py");
    let config = llm_config(&tool_root, "geospatial-fiona");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert!(
        req_lower.contains("pandas"),
        "LLM should resolve pandas, got:\n{}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("numpy"),
        "LLM should resolve numpy, got:\n{}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("shapely"),
        "LLM should resolve shapely, got:\n{}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("fiona"),
        "LLM should resolve fiona, got:\n{}",
        result.requirements_txt,
    );
    assert!(
        req_lower.contains("geopandas"),
        "LLM should resolve geopandas, got:\n{}",
        result.requirements_txt,
    );
    assert!(
        result.unresolved.is_empty(),
        "All imports should be resolved, unresolved: {:?}",
        result.unresolved,
    );
}

// ---------------------------------------------------------------------------
// Test 7: scrapy — Py2 snippet with scrapy Item/Field (Pattern: version-sensitive)
// Expected: PASS — LLM should resolve scrapy and pin Py2-compatible version
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_scrapy_peewee_for_py2() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/scrapy_peewee_snippet.py");
    let mut config = llm_config(&tool_root, "scrapy-peewee");
    config.python_version_range = 1; // narrow to Py2

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert!(
        req_lower.contains("scrapy"),
        "LLM should resolve scrapy, got:\n{}",
        result.requirements_txt,
    );
    // scrapy should have a pinned version for Py2
    assert!(
        req_lower.contains("scrapy=="),
        "LLM should pin scrapy version for Py2 compat, got:\n{}",
        result.requirements_txt,
    );
    assert!(
        result.unresolved.is_empty(),
        "All imports should be resolved, unresolved: {:?}",
        result.unresolved,
    );
}
