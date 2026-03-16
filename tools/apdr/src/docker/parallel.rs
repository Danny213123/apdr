use crate::parser::version_detect::version_tuple;

pub fn candidate_versions(
    base_version: &str,
    range: usize,
    min_version: Option<&str>,
    max_version: Option<&str>,
) -> Vec<String> {
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
    let windowed: Vec<String> = known[start..end].to_vec();

    // Apply parser-bound pruning
    let min_tuple = min_version.map(version_tuple);
    let max_tuple = max_version.map(version_tuple);
    let pruned: Vec<String> = windowed
        .into_iter()
        .filter(|v| {
            let vt = version_tuple(v);
            if let Some(min) = min_tuple {
                if vt < min {
                    return false;
                }
            }
            if let Some(max) = max_tuple {
                if vt > max {
                    return false;
                }
            }
            true
        })
        .collect();

    // Order nearest-first around base_version, with forward bias (prefer higher at equal distance)
    let base_tuple = version_tuple(base_version);
    let mut with_distance: Vec<(usize, usize, String)> = pruned
        .into_iter()
        .enumerate()
        .map(|(original_idx, v)| {
            let vt = version_tuple(&v);
            let distance = ((vt.0 as i64 - base_tuple.0 as i64).unsigned_abs() * 100
                + (vt.1 as i64 - base_tuple.1 as i64).unsigned_abs())
                as usize;
            // Forward bias: at equal distance, prefer versions >= base
            let below = if vt < base_tuple { 1usize } else { 0usize };
            (distance * 2 + below, original_idx, v)
        })
        .collect();
    with_distance.sort_by_key(|(key, idx, _)| (*key, *idx));

    let result: Vec<String> = with_distance.into_iter().map(|(_, _, v)| v).collect();
    if result.is_empty() {
        vec![base_version.to_string()]
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::candidate_versions;

    #[test]
    fn py27_expansion_stops_at_py310() {
        assert_eq!(
            candidate_versions("2.7", 5, None, None),
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
    fn py39_expansion_nearest_first() {
        assert_eq!(
            candidate_versions("3.9", 5, None, None),
            vec![
                "3.9".to_string(),
                "3.10".to_string(),
                "3.8".to_string(),
                "3.11".to_string(),
                "3.7".to_string(),
                "3.12".to_string(),
                "2.7".to_string(),
            ]
        );
    }

    #[test]
    fn py39_with_min_bound_prunes_old_versions() {
        assert_eq!(
            candidate_versions("3.9", 5, Some("3.9"), None),
            vec![
                "3.9".to_string(),
                "3.10".to_string(),
                "3.11".to_string(),
                "3.12".to_string(),
            ]
        );
    }

    #[test]
    fn py27_with_max_bound_prunes_modern() {
        assert_eq!(
            candidate_versions("2.7", 5, None, Some("2.7")),
            vec!["2.7".to_string()]
        );
    }

    #[test]
    fn py39_range1_nearest_first() {
        assert_eq!(
            candidate_versions("3.9", 1, None, None),
            vec!["3.9".to_string(), "3.10".to_string(), "3.8".to_string(),]
        );
    }
}
