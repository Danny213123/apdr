use crate::cache::store::{normalize, CacheStore};

pub fn dependencies<'a>(store: &'a CacheStore, package_name: &str) -> Option<&'a Vec<String>> {
    store.dependency_graph.get(&normalize(package_name))
}
