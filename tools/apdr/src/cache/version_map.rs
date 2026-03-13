use crate::cache::store::CacheStore;

pub fn get_constraint<'a>(store: &'a CacheStore, api_usage: &str) -> Option<&'a String> {
    store.version_constraints.get(api_usage)
}
