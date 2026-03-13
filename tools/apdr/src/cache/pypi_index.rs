use crate::cache::store::{normalize, CacheStore};

pub fn compatible_versions<'a>(
    store: &'a CacheStore,
    package_name: &str,
) -> Option<&'a Vec<String>> {
    store.pypi_index.get(&normalize(package_name))
}
