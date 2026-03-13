use crate::cache::store::{normalize, CacheStore, PackageRecord};

pub fn lookup<'a>(store: &'a CacheStore, import_name: &str) -> Option<&'a PackageRecord> {
    store.import_map.get(&normalize(import_name))
}
