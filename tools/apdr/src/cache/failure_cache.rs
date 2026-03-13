use crate::cache::store::CacheStore;
use crate::FailurePattern;

pub fn patterns(store: &CacheStore) -> &[FailurePattern] {
    &store.failure_patterns
}
