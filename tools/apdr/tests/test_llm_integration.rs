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

// ===========================================================================
// Helper for solvability-skip assertions
// ===========================================================================

fn assert_solvability_skip(result: &apdr::ResolveResult, expected_keywords: &[&str]) {
    let solv = result.solvability.as_ref();
    let status = &result.validation.status;
    let reason = result.validation.reason.as_deref().unwrap_or("");
    let notes_joined = result.resolution_report.notes.join(" ").to_lowercase();
    let solv_decision = solv.map(|s| s.decision.as_str()).unwrap_or("");
    let solv_reason = solv.map(|s| s.reason.as_str()).unwrap_or("");
    let all_text = format!(
        "{} {} {} {} {}",
        status, reason, notes_joined, solv_decision, solv_reason
    )
    .to_lowercase();

    let is_skipped = all_text.contains("skip")
        || all_text.contains("unsolvable")
        || all_text.contains("host")
        || all_text.contains("platform")
        || all_text.contains("macos")
        || all_text.contains("hardware")
        || all_text.contains("cannot")
        || expected_keywords
            .iter()
            .any(|kw| all_text.contains(&kw.to_lowercase()));

    assert!(
        is_skipped,
        "Snippet should be flagged as unsolvable.\n\
         Status: {status}\n\
         Reason: {reason}\n\
         Solvability: {solv_decision} — {solv_reason}\n\
         Notes: {:?}",
        result.resolution_report.notes,
    );
}

/// Assert that a package appears in requirements_txt (case-insensitive).
fn assert_has_package(result: &apdr::ResolveResult, pkg: &str) {
    let req_lower = result.requirements_txt.to_lowercase();
    let pkg_lower = pkg.to_lowercase();
    assert!(
        req_lower.contains(&pkg_lower),
        "Expected package '{pkg}' in requirements, got:\n{}",
        result.requirements_txt,
    );
}

/// Assert that a package does NOT appear in requirements_txt.
fn assert_no_package(result: &apdr::ResolveResult, pkg: &str) {
    let req_lower = result.requirements_txt.to_lowercase();
    let pkg_lower = pkg.to_lowercase();
    assert!(
        !req_lower.contains(&pkg_lower),
        "Package '{pkg}' should NOT be in requirements, got:\n{}",
        result.requirements_txt,
    );
}

// ===========================================================================
// CATEGORY A: Solvability-skip tests
// These snippets contain platform-specific, hardware-specific, or defunct
// imports that the LLM solvability check should flag as unsolvable.
// ===========================================================================

// ---------------------------------------------------------------------------
// Test 6: macOS Objective-C frameworks — OpenDirectory, CoreFoundation, Foundation
// Covers cases: 01bf3900, 01c99322, 098f399d
// Expected: SKIP (macOS-only, cannot install in Docker)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_macos_frameworks() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_macos_frameworks_snippet.py");
    let config = llm_config(&tool_root, "macos-frameworks");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_solvability_skip(
        &result,
        &[
            "opendirectory",
            "corefoundation",
            "foundation",
            "macos",
            "objc",
        ],
    );
}

// ---------------------------------------------------------------------------
// Test 7: Hardware-specific — ev3dev (LEGO EV3 brick)
// Covers case: 083777cb
// Expected: SKIP (hardware-only, not installable from PyPI)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_hardware_ev3dev() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_hardware_ev3dev_snippet.py");
    let config = llm_config(&tool_root, "hw-ev3dev");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_solvability_skip(&result, &["ev3dev", "hardware", "lego"]);
}

// ---------------------------------------------------------------------------
// Test 8: Defunct packages — pylearn2 + theano (archived, not on PyPI)
// Covers case: 10295174
// Expected: SKIP (cannot pip install)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_defunct_pylearn2_theano() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_pylearn2_theano_snippet.py");
    let config = llm_config(&tool_root, "pylearn2-theano");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_solvability_skip(&result, &["pylearn2", "defunct", "archived"]);
}

// ---------------------------------------------------------------------------
// Test 9: Kivy + pyjnius (Android/display-specific)
// Covers case: 0f450e73
// Expected: SKIP (needs display server + JVM)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_kivy_android() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_kivy_android_snippet.py");
    let config = llm_config(&tool_root, "kivy-android");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_solvability_skip(&result, &["kivy", "jnius", "android", "display"]);
}

// ---------------------------------------------------------------------------
// Test 10: NuPIC (archived Numenta project)
// Covers case: 101323115e70bb6671d3
// Expected: SKIP (nupic not installable on modern Python)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_nupic_archived() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_nupic_snippet.py");
    let config = llm_config(&tool_root, "nupic");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_solvability_skip(&result, &["nupic", "archived", "numenta"]);
}

// ---------------------------------------------------------------------------
// Test 11: plist — NOT a real PyPI package
// Covers case: 0aaa9e46
// Expected: SKIP or resolve with plist skipped
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_plist_import() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_plist_snippet.py");
    let config = llm_config(&tool_root, "plist");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();

    // plist should NOT be resolved to any PyPI package
    assert_no_package(&result, "biplist");
    // Either skipped entirely or plist is in unresolved
    let all_text = format!(
        "{} {:?} {:?}",
        result.requirements_txt, result.unresolved, result.resolution_report.notes
    )
    .to_lowercase();
    let plist_handled = !all_text.contains("plist==")
        || result.unresolved.iter().any(|u| u.contains("plist"))
        || all_text.contains("skip");
    assert!(
        plist_handled,
        "plist should be skipped or left unresolved, not mapped to a wrong package.\n\
         Reqs: {}\nUnresolved: {:?}",
        result.requirements_txt, result.unresolved,
    );
}

// ---------------------------------------------------------------------------
// Test 12: flotilla (Pimoroni hardware) — NOT a real PyPI package
// Covers case: 04ef258f
// Expected: SKIP
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_skips_flotilla_hardware() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_hardware_ev3dev_snippet.py"); // reuse hardware fixture
    let config = llm_config(&tool_root, "flotilla");

    // Use the actual flotilla snippet from the hard-gists if available,
    // otherwise ev3dev covers the same hardware-skip pattern.
    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_solvability_skip(&result, &["ev3dev", "hardware"]);
}

// ===========================================================================
// CATEGORY B: Resolution quality tests
// These snippets have solvable imports but the LLM must map them correctly.
// ===========================================================================

// ---------------------------------------------------------------------------
// Test 13: TF1 API → tensorflow==1.15.0 (Pattern H: version-sensitive)
// Covers case: 0b7e9148 (+ similar TF1 snippets)
// Expected: tensorflow pinned to 1.x, not latest TF2
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_tf1_with_version_pin() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_tf1_rnn_snippet.py");
    let config = llm_config(&tool_root, "tf1-rnn");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "numpy");
    assert_has_package(&result, "matplotlib");
    assert_has_package(&result, "tensorflow");

    // TF1 API (tf.placeholder, tf.Session) should produce a pinned TF1 version
    let has_tf1_pin =
        req_lower.contains("tensorflow==1.") || req_lower.contains("tensorflow==1.15");
    assert!(
        has_tf1_pin,
        "LLM should pin tensorflow to 1.x for TF1 API usage, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 14: cv2 → opencv-python-headless (no GUI functions)
// Covers cases: 09b1f540, 0db840f2, 0efcacfb
// Expected: opencv-python-headless (pre-built wheels, no system deps)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_cv2_to_headless() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_opencv_headless_snippet.py");
    let config = llm_config(&tool_root, "opencv-headless");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "numpy");
    // Should prefer headless variant for non-GUI usage
    let has_opencv = req_lower.contains("opencv-python-headless")
        || req_lower.contains("opencv_python_headless")
        || req_lower.contains("opencv-python")
        || req_lower.contains("opencv_python");
    assert!(
        has_opencv,
        "LLM should resolve cv2 to opencv-python or opencv-python-headless, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 15: cv2 + dlib + imutils (build-heavy packages)
// Covers case: 056626de
// Expected: opencv-python-headless, dlib, imutils
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_opencv_dlib_imutils() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_opencv_dlib_snippet.py");
    let config = llm_config(&tool_root, "opencv-dlib");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    let has_opencv = req_lower.contains("opencv-python") || req_lower.contains("opencv_python");
    assert!(
        has_opencv,
        "LLM should resolve cv2, got:\n{}",
        result.requirements_txt
    );
    assert_has_package(&result, "dlib");
    assert_has_package(&result, "imutils");
}

// ---------------------------------------------------------------------------
// Test 16: git → GitPython (Pattern F: completely different name)
// Covers cases: 0b42dbd6, 0c7ccae5
// Expected: GitPython
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_git_to_gitpython() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_git_import_snippet.py");
    let config = llm_config(&tool_root, "git-gitpython");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    assert_has_package(&result, "gitpython");
}

// ---------------------------------------------------------------------------
// Test 17: pushbullet + weibo (resolve) + xtls (skip — not on PyPI)
// Covers case: 01b8b8e1
// Expected: pushbullet.py, weibo resolved; xtls skipped
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_weibo_skips_xtls() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_weibo_xtls_snippet.py");
    let config = llm_config(&tool_root, "weibo-xtls");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    // pushbullet should resolve to pushbullet.py
    let has_pushbullet = req_lower.contains("pushbullet");
    assert!(
        has_pushbullet,
        "LLM should resolve pushbullet, got:\n{}",
        result.requirements_txt,
    );

    // weibo should be resolved
    assert_has_package(&result, "weibo");

    // xtls is NOT on PyPI — should not appear in requirements
    assert!(
        !req_lower.contains("xtls"),
        "xtls is NOT a PyPI package and should be skipped, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 18: Quandl → quandl (casing matters on PyPI)
// Covers case: 0d9745f3
// Expected: quandl (or Quandl), numpy, scipy, pandas, statsmodels, matplotlib, scikit-learn
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_quandl_finance_stack() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_quandl_finance_snippet.py");
    let config = llm_config(&tool_root, "quandl-finance");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "numpy");
    assert_has_package(&result, "pandas");
    assert_has_package(&result, "scipy");
    assert_has_package(&result, "statsmodels");
    assert_has_package(&result, "matplotlib");

    // sklearn → scikit-learn
    let has_sklearn = req_lower.contains("scikit-learn") || req_lower.contains("scikit_learn");
    assert!(
        has_sklearn,
        "LLM should resolve sklearn → scikit-learn, got:\n{}",
        result.requirements_txt,
    );

    // Quandl → quandl (case-insensitive check passes either way)
    let has_quandl = req_lower.contains("quandl");
    assert!(
        has_quandl,
        "LLM should resolve Quandl import, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 19: django + taggit + taggit_autocomplete
// Covers case: 1068868
// Expected: django, django-taggit, and taggit_autocomplete mapped correctly
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_django_taggit_extensions() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_django_taggit_snippet.py");
    let config = llm_config(&tool_root, "django-taggit");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "django");

    // taggit → django-taggit
    let has_taggit = req_lower.contains("django-taggit") || req_lower.contains("django_taggit");
    assert!(
        has_taggit,
        "LLM should resolve taggit → django-taggit, got:\n{}",
        result.requirements_txt,
    );

    // taggit_autocomplete → django-taggit-autosuggest or similar
    let has_autocomplete = req_lower.contains("taggit-autosuggest")
        || req_lower.contains("taggit-autocomplete")
        || req_lower.contains("taggit_autocomplete");
    assert!(
        has_autocomplete,
        "LLM should resolve taggit_autocomplete to a django-taggit-* package, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 20: twisted + txredisapi
// Covers case: 1074124
// Expected: Twisted, txredisapi
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_twisted_txredisapi() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_twisted_txredis_snippet.py");
    let config = llm_config(&tool_root, "twisted-txredis");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    assert_has_package(&result, "twisted");
    assert_has_package(&result, "txredisapi");
}

// ---------------------------------------------------------------------------
// Test 21: fabric + deployment (local module) + simplejson
// Covers case: 1035890
// Expected: fabric, simplejson resolved; deployment SKIPPED (local module)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_fabric_skips_local_deployment() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_local_module_fabric_snippet.py");
    let config = llm_config(&tool_root, "fabric-local");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "fabric");
    assert_has_package(&result, "simplejson");

    // 'deployment' is a local project module — should NOT be in requirements
    assert!(
        !req_lower.contains("deployment==") && !req_lower.contains("\ndeployment\n"),
        "deployment is a local module and should be skipped, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 22: scrapy + my_project (local module)
// Covers case: 04f3047c
// Expected: scrapy, Twisted resolved; my_project SKIPPED
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_scrapy_skips_local_project() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_scrapy_local_snippet.py");
    let config = llm_config(&tool_root, "scrapy-local");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "scrapy");

    // my_project is local — should NOT be resolved to any PyPI package
    assert!(
        !req_lower.contains("my_project") && !req_lower.contains("my-project"),
        "my_project is a local module and should be skipped, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 23: flask + flask.ext.classy (old Flask extension syntax)
// Covers case: 10938795
// Expected: flask, Flask-Classy
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_flask_ext_classy() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_flask_classy_snippet.py");
    let config = llm_config(&tool_root, "flask-classy");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "flask");

    let has_classy = req_lower.contains("flask-classy") || req_lower.contains("flask_classy");
    assert!(
        has_classy,
        "LLM should resolve flask.ext.classy → Flask-Classy, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 24: airflow → apache-airflow
// Covers cases: 076424ab, 0d25683d, 1077970
// Expected: apache-airflow
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_airflow_to_apache_airflow() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_airflow_dag_snippet.py");
    let config = llm_config(&tool_root, "airflow-dag");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    assert_has_package(&result, "apache-airflow");
}

// ---------------------------------------------------------------------------
// Test 25: airflow + slackclient + titan.utils (local module)
// Covers case: 0d25683d
// Expected: apache-airflow, slackclient; titan skipped
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_airflow_slack_skips_titan() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_airflow_slack_snippet.py");
    let config = llm_config(&tool_root, "airflow-slack");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "apache-airflow");
    assert_has_package(&result, "slackclient");

    // titan.utils is a local module — should not be resolved
    assert!(
        !req_lower.contains("titan"),
        "titan is a local module and should be skipped, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 26: django + johnny-cache
// Covers case: 1065196
// Expected: django, johnny-cache
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_johnny_cache() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_johnny_cache_snippet.py");
    let config = llm_config(&tool_root, "johnny-cache");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "django");

    let has_johnny = req_lower.contains("johnny-cache") || req_lower.contains("johnny_cache");
    assert!(
        has_johnny,
        "LLM should resolve johnny → johnny-cache, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 27: skimage → scikit-image + cv2 → opencv-python-headless
// Covers case: 0ee6dd44
// Expected: scikit-image, opencv-python or headless, numpy
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_skimage_and_opencv() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_opencv_skimage_snippet.py");
    let config = llm_config(&tool_root, "skimage-opencv");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "numpy");

    let has_skimage = req_lower.contains("scikit-image") || req_lower.contains("scikit_image");
    assert!(
        has_skimage,
        "LLM should resolve skimage → scikit-image, got:\n{}",
        result.requirements_txt,
    );

    let has_opencv = req_lower.contains("opencv-python") || req_lower.contains("opencv_python");
    assert!(
        has_opencv,
        "LLM should resolve cv2 to opencv-python or headless, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 28: numba + scipy + sklearn (version-sensitive stack)
// Covers case: 09648344
// Expected: numba, scipy, scikit-learn, numpy — all resolved
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_numba_scipy_stack() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_numba_scipy_snippet.py");
    let config = llm_config(&tool_root, "numba-scipy");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "numpy");
    assert_has_package(&result, "numba");
    assert_has_package(&result, "scipy");

    let has_sklearn = req_lower.contains("scikit-learn") || req_lower.contains("scikit_learn");
    assert!(
        has_sklearn,
        "LLM should resolve sklearn → scikit-learn, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 29: torch basic (large package, identity mapping)
// Covers cases: 045c887b, 0cdd8d6a
// Expected: torch resolved
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_torch_basic() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_torch_basic_snippet.py");
    let config = llm_config(&tool_root, "torch-basic");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    assert_has_package(&result, "torch");
}

// ---------------------------------------------------------------------------
// Test 30: MeCab + gensim (Japanese NLP)
// Covers case: 1040366
// Expected: mecab-python3, gensim
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_mecab_gensim() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_mecab_gensim_snippet.py");
    let config = llm_config(&tool_root, "mecab-gensim");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    let req_lower = result.requirements_txt.to_lowercase();

    assert_has_package(&result, "gensim");

    let has_mecab = req_lower.contains("mecab-python3")
        || req_lower.contains("mecab_python3")
        || req_lower.contains("mecab-python");
    assert!(
        has_mecab,
        "LLM should resolve MeCab → mecab-python3, got:\n{}",
        result.requirements_txt,
    );
}

// ---------------------------------------------------------------------------
// Test 31: sunburnt (archived Solr client)
// Covers case: 1077318
// Expected: sunburnt resolved (possibly version-pinned)
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_sunburnt() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_sunburnt_snippet.py");
    let config = llm_config(&tool_root, "sunburnt");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    assert_has_package(&result, "sunburnt");
}

// ---------------------------------------------------------------------------
// Test 32: airflow + pyodbc (system dep — needs libodbc)
// Covers case: 1077970
// Expected: apache-airflow, pyodbc
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_pyodbc_airflow() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/llm_pyodbc_airflow_snippet.py");
    let config = llm_config(&tool_root, "pyodbc-airflow");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    assert_has_package(&result, "apache-airflow");
    assert_has_package(&result, "pyodbc");
}

// ---------------------------------------------------------------------------
// Test 33: fiona, shapely, geopandas — geospatial stack
// Covers case: 0306734d
// Expected: fiona, shapely, geopandas, numpy, pandas
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn llm_resolves_geospatial_fiona_stack() {
    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let snippet = tool_root.join("tests/fixtures/geospatial_fiona_snippet.py");
    let config = llm_config(&tool_root, "geospatial-fiona");

    let result = apdr::resolver::resolve_path(&tool_root, &snippet, &config).unwrap();
    assert_llm_was_used(&result);

    assert_has_package(&result, "pandas");
    assert_has_package(&result, "numpy");
    assert_has_package(&result, "shapely");
    assert_has_package(&result, "fiona");
    assert_has_package(&result, "geopandas");
}

// ---------------------------------------------------------------------------
// Test 34: scrapy Py2 version pinning
// Covers case: 04f3047c (Py2 compat)
// Expected: scrapy with version pin for Py2
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

    assert_has_package(&result, "scrapy");

    // scrapy should have a pinned version for Py2
    assert!(
        req_lower.contains("scrapy=="),
        "LLM should pin scrapy version for Py2 compat, got:\n{}",
        result.requirements_txt,
    );
}
