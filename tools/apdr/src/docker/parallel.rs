pub fn candidate_versions(base_version: &str, range: usize) -> Vec<String> {
    let known = [
        "2.7".to_string(),
        "3.9".to_string(),
        "3.10".to_string(),
        "3.11".to_string(),
        "3.12".to_string(),
    ];
    let Some(index) = known.iter().position(|value| value == base_version) else {
        return vec![base_version.to_string()];
    };

    let start = index.saturating_sub(range);
    let end = std::cmp::min(index + range + 1, known.len());
    known[start..end].to_vec()
}
