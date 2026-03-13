use std::collections::BTreeSet;

pub fn equally_distanced_sample(
    versions: &[String],
    previous_versions: &[String],
) -> Option<String> {
    if versions.is_empty() {
        return None;
    }

    let previous = previous_versions.iter().cloned().collect::<BTreeSet<_>>();
    let candidates = versions
        .iter()
        .filter(|version| !previous.contains(*version))
        .cloned()
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return versions.last().cloned();
    }

    candidates.get(candidates.len() / 2).cloned()
}
