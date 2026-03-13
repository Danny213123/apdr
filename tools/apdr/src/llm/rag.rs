use crate::cache::store::CacheStore;

pub fn assemble_context(store: &CacheStore, package_name: &str) -> Vec<String> {
    let mut context = Vec::new();
    if let Some(record) = store.import_lookup(package_name) {
        context.push(format!(
            "known import mapping: {} -> {}",
            record.import_name, record.package_name
        ));
    }
    if let Some(versions) = store.pypi_index.get(package_name) {
        context.push(format!("known versions: {}", versions.join(", ")));
    }
    if let Some(deps) = store.dependency_graph.get(package_name) {
        context.push(format!("known transitive deps: {}", deps.join(", ")));
    }
    context
}
