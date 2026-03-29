use crate::cache::store::CacheStore;
use crate::{ParseResult, ResolveConfig};
pub(super) fn build_base_request(config: &ResolveConfig) -> serde_json::Value {
    serde_json::json!({
        "provider": config.llm_provider,
        "model": config.llm_model,
        "base_url": config.llm_base_url,
        "agent_mode": config.agent_mode,
        "tool_profile": config.tool_profile,
        "retrieval_profile": config.retrieval_profile,
        "policy_label": config.policy_label,
        "cache_path": config.cache_path.to_string_lossy(),
        "output_dir": config.output_dir.to_string_lossy(),
    })
}
pub(super) fn assemble_context_for_import(store: &CacheStore, name: &str) -> Vec<String> {
    let mut context = Vec::new();
    if let Some(record) = store.import_lookup(name) {
        context.push(format!(
            "known import mapping: {} -> {}",
            record.import_name, record.package_name
        ));
    }
    if let Some(versions) = store.pypi_index.get(name) {
        context.push(format!("known versions: {}", versions.join(", ")));
    }
    if let Some(deps) = store.dependency_graph.get(name) {
        context.push(format!("known transitive deps: {}", deps.join(", ")));
    }
    context
}
pub(super) fn assemble_batch_context(
    store: &CacheStore,
    import_names: &[String],
    failure_context: &str,
) -> Vec<String> {
    let mut context: Vec<String> = import_names
        .iter()
        .flat_map(|name| assemble_context_for_import(store, name))
        .collect();
    if !failure_context.is_empty() {
        context.push(failure_context.to_string());
    }
    context
}
pub(super) fn looks_like_local_helper_import(
    parse_result: &ParseResult,
    import_name: &str,
) -> bool {
    let normalized = crate::cache::store::normalize(import_name);
    if matches!(
        normalized.as_str(),
        "input-data"
            | "settings"
            | "config"
            | "conf"
            | "constants"
            | "urls"
            | "api"
            | "app"
            | "apps"
            | "views"
            | "models"
            | "forms"
            | "admin"
            | "tests"
            | "manage"
            | "wsgi"
            | "asgi"
            | "conftest"
            | "tasks"
            | "celery-tasks"
            | "util"
            | "utils"
            | "helper"
            | "helpers"
            | "common"
            | "shared"
            | "base"
            | "core"
            | "main"
            | "run"
            | "setup"
            | "version"
            | "db"
            | "database"
            | "middleware"
            | "serializers"
            | "permissions"
            | "signals"
            | "routers"
            | "schemas"
            | "exceptions"
            | "mixins"
            | "decorators"
            | "context-processors"
            | "templatetags"
            | "management"
            | "fixtures"
            | "migrations"
            | "factory"
            | "factories"
            | "mocks"
            | "stubs"
            | "testutils"
            | "test-helpers"
            | "local-settings"
            | "production-settings"
            | "development-settings"
            | "celeryconfig"
            | "gunicorn-config"
            | "uwsgi"
            | "input"
            | "output"
            | "data"
            | "result"
            | "results"
            | "solution"
            | "answer"
            | "submission"
            | "benchmark"
            | "train"
            | "test"
            | "evaluate"
            | "predict"
            | "preprocess"
            | "postprocess"
    ) {
        return true;
    }
    let generic_helper = matches!(
        normalized.as_str(),
        "util" | "utils" | "helper" | "helpers" | "common" | "shared"
    );
    generic_helper
        && parse_result
            .import_paths
            .iter()
            .any(|path| crate::cache::store::normalize(path).starts_with(&format!("{normalized}-")))
}
