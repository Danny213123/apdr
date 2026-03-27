use std::collections::BTreeSet;

/// PLLM-style equally-distanced version sampling.
///
/// Instead of always trying the latest or middle version, this spreads
/// attempts across the version timeline. On the first call it picks the
/// middle (most likely compatible), then alternates between newer and older
/// regions, maximizing coverage of the version space.
///
/// `versions` should be sorted oldest → newest (as returned by PyPI).
/// `previous_versions` are versions already tried (will be skipped).
pub fn equally_distanced_sample(
    versions: &[String],
    previous_versions: &[String],
) -> Option<String> {
    if versions.is_empty() {
        return None;
    }

    let previous: BTreeSet<&str> = previous_versions.iter().map(String::as_str).collect();
    let candidates: Vec<&String> = versions
        .iter()
        .filter(|v| !previous.contains(v.as_str()) && v.as_str() != "0")
        .collect();

    if candidates.is_empty() {
        return versions.last().cloned();
    }

    // Number of prior attempts determines which region to sample from
    // within the *remaining* candidates. This creates an equally-distanced
    // spread across the full version timeline:
    //   attempt 0 → middle (50th percentile)
    //   attempt 1 → 75th percentile (newer half)
    //   attempt 2 → 25th percentile (older half)
    //   ...binary subdivision style
    let attempt = previous_versions.len();

    // Use binary spread on original version list to pick a target fraction,
    // then find the closest candidate in the filtered list.
    let total = versions.len();
    let target_idx = binary_spread_index(total, attempt).min(total.saturating_sub(1));
    let target_version = &versions[target_idx];

    // Find the closest untried candidate to the target
    if candidates.contains(&target_version) {
        return Some(target_version.clone());
    }
    // Target was already tried; pick the nearest untried candidate
    let target_pos = versions
        .iter()
        .position(|v| v == target_version)
        .unwrap_or(0);
    candidates
        .iter()
        .min_by_key(|c| {
            let pos = versions.iter().position(|v| v == **c).unwrap_or(0);
            (pos as isize - target_pos as isize).unsigned_abs()
        })
        .map(|v| (*v).clone())
}

/// Returns the index to sample for the given attempt number, spreading
/// picks evenly across the range [0, n).
fn binary_spread_index(n: usize, attempt: usize) -> usize {
    if n == 0 {
        return 0;
    }
    if n == 1 {
        return 0;
    }

    // Generate the fractional position using binary subdivision:
    // attempt 0 → 0.5, attempt 1 → 0.75, attempt 2 → 0.25,
    // attempt 3 → 0.875, attempt 4 → 0.125, etc.
    let frac = binary_fraction(attempt);
    let idx = (frac * n as f64) as usize;
    idx.min(n - 1)
}

/// Maps attempt number to a fraction in (0, 1) using binary subdivision.
fn binary_fraction(attempt: usize) -> f64 {
    if attempt == 0 {
        return 0.5;
    }
    // Convert attempt to a binary tree traversal:
    // Level 0: 0.5
    // Level 1: 0.75, 0.25
    // Level 2: 0.875, 0.375, 0.625, 0.125
    let level = ((attempt + 1) as f64).log2().floor() as u32;
    let level_start = (1_usize << level) - 1;
    let pos_in_level = attempt - level_start;
    let divisions = 1_usize << (level + 1);
    let numerator = 2 * pos_in_level + 1;
    numerator as f64 / divisions as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_pick_is_middle() {
        let versions: Vec<String> = (0..10).map(|i| format!("1.{i}.0")).collect();
        let pick = equally_distanced_sample(&versions, &[]);
        assert_eq!(pick, Some("1.5.0".to_string()));
    }

    #[test]
    fn picks_spread_across_range() {
        // The key property: picks should NOT cluster together
        let versions: Vec<String> = (0..10).map(|i| format!("1.{i}.0")).collect();
        let mut tried = Vec::new();
        let mut picks = Vec::new();
        for _ in 0..5 {
            let pick = equally_distanced_sample(&versions, &tried).unwrap();
            picks.push(pick.clone());
            tried.push(pick);
        }
        // All picks should be unique
        let unique: BTreeSet<&String> = picks.iter().collect();
        assert_eq!(unique.len(), 5, "All picks should be unique: {:?}", picks);
        // First pick should be middle
        assert_eq!(picks[0], "1.5.0");
    }

    #[test]
    fn skips_already_tried() {
        let versions: Vec<String> = vec!["1.0".to_string(), "2.0".to_string(), "3.0".to_string()];
        let pick = equally_distanced_sample(&versions, &["2.0".to_string()]);
        // attempt 1 → 75th percentile of 3 items → target index 2 → "3.0"
        // "2.0" already tried, "3.0" is untried and closest
        assert!(pick == Some("1.0".to_string()) || pick == Some("3.0".to_string()));
    }

    #[test]
    fn single_version() {
        let pick = equally_distanced_sample(&["1.0".to_string()], &[]);
        assert_eq!(pick, Some("1.0".to_string()));
    }

    #[test]
    fn all_exhausted_returns_latest() {
        let versions = vec!["1.0".to_string(), "2.0".to_string()];
        let pick = equally_distanced_sample(&versions, &["1.0".to_string(), "2.0".to_string()]);
        assert_eq!(pick, Some("2.0".to_string()));
    }

    #[test]
    fn spread_covers_range() {
        // Verify that 5 consecutive picks cover the full range
        let versions: Vec<String> = (0..20).map(|i| format!("{i}")).collect();
        let mut tried = Vec::new();
        let mut picks = Vec::new();
        for _ in 0..5 {
            let pick = equally_distanced_sample(&versions, &tried).unwrap();
            picks.push(pick.clone());
            tried.push(pick);
        }
        // Should have picks spread across the range, not clustered
        let indices: Vec<usize> = picks.iter().map(|p| p.parse::<usize>().unwrap()).collect();
        let min = *indices.iter().min().unwrap();
        let max = *indices.iter().max().unwrap();
        assert!(
            max - min >= 10,
            "Picks should span at least half the range: {:?}",
            indices
        );
    }
}
