pub fn candidate_versions(base_version: &str, range: usize) -> Vec<String> {
    let known = if base_version == "2.7" {
        vec![
            "2.7".to_string(),
            "3.7".to_string(),
            "3.8".to_string(),
            "3.9".to_string(),
            "3.10".to_string(),
        ]
    } else {
        vec![
            "2.7".to_string(),
            "3.7".to_string(),
            "3.8".to_string(),
            "3.9".to_string(),
            "3.10".to_string(),
            "3.11".to_string(),
            "3.12".to_string(),
        ]
    };
    let Some(index) = known.iter().position(|value| value == base_version) else {
        return vec![base_version.to_string()];
    };

    let start = index.saturating_sub(range);
    let end = std::cmp::min(index + range + 1, known.len());
    known[start..end].to_vec()
}

#[cfg(test)]
mod tests {
    use super::candidate_versions;

    #[test]
    fn py27_expansion_stops_at_py310() {
        assert_eq!(
            candidate_versions("2.7", 5),
            vec![
                "2.7".to_string(),
                "3.7".to_string(),
                "3.8".to_string(),
                "3.9".to_string(),
                "3.10".to_string()
            ]
        );
    }

    #[test]
    fn py39_expansion_keeps_modern_versions() {
        assert_eq!(
            candidate_versions("3.9", 5),
            vec![
                "2.7".to_string(),
                "3.7".to_string(),
                "3.8".to_string(),
                "3.9".to_string(),
                "3.10".to_string(),
                "3.11".to_string(),
                "3.12".to_string()
            ]
        );
    }
}
