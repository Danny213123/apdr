use std::cmp::Ordering;

pub fn version_satisfies(version: &str, constraint: &str) -> bool {
    let trimmed = constraint.trim();
    if trimmed.is_empty() {
        return true;
    }
    trimmed
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .all(|item| satisfies_single_constraint(version, item))
}

fn satisfies_single_constraint(version: &str, constraint: &str) -> bool {
    let bytes = constraint.as_bytes();
    if bytes.len() >= 2 {
        match (bytes[0], bytes[1]) {
            (b'=', b'=') => return wildcard_match(version, constraint[2..].trim()),
            (b'>', b'=') => return compare_versions(version, constraint[2..].trim()) != Ordering::Less,
            (b'<', b'=') => return compare_versions(version, constraint[2..].trim()) != Ordering::Greater,
            (b'!', b'=') => return !wildcard_match(version, constraint[2..].trim()),
            (b'~', b'=') => return compatible_release(version, constraint[2..].trim()),
            _ => {}
        }
    }
    if !bytes.is_empty() {
        match bytes[0] {
            b'>' => return compare_versions(version, constraint[1..].trim()) == Ordering::Greater,
            b'<' => return compare_versions(version, constraint[1..].trim()) == Ordering::Less,
            _ => {}
        }
    }
    wildcard_match(version, constraint)
}

fn wildcard_match(version: &str, target: &str) -> bool {
    let target = target.trim();
    if !target.contains('*') {
        return compare_versions(version, target) == Ordering::Equal;
    }
    let prefix = target.trim_end_matches('*').trim_end_matches('.');
    version == prefix
        || (version.len() > prefix.len()
            && version.starts_with(prefix)
            && version.as_bytes()[prefix.len()] == b'.')
}

fn compatible_release(version: &str, base: &str) -> bool {
    if compare_versions(version, base) == Ordering::Less {
        return false;
    }
    let parts: Vec<&str> = base.split('.').collect();
    if parts.len() <= 1 {
        return true;
    }
    let inc_index = parts.len() - 2;
    let mut upper = String::with_capacity(base.len() + 4);
    for (i, part) in parts[..parts.len() - 1].iter().enumerate() {
        if i > 0 {
            upper.push('.');
        }
        if i == inc_index {
            upper.push_str(&increment_numeric(part));
        } else {
            upper.push_str(part);
        }
    }
    upper.push_str(".0");
    compare_versions(version, &upper) == Ordering::Less
}

fn increment_numeric(value: &str) -> String {
    value
        .parse::<u64>()
        .map(|number| (number + 1).to_string())
        .unwrap_or_else(|_| format!("{value}1"))
}

const MAX_VERSION_PARTS: usize = 10;

#[derive(Clone, Copy)]
enum VersionPart {
    Number(u64),
    Text([u8; 8], u8),
}

fn tokenize_version(value: &str) -> ([VersionPart; MAX_VERSION_PARTS], usize) {
    let mut parts = [VersionPart::Number(0); MAX_VERSION_PARTS];
    let mut len = 0usize;
    let mut text_buf = [0u8; 8];
    let mut text_len: u8 = 0;
    let mut num_acc: u64 = 0;
    let mut in_number = false;
    let mut buf_active = false;

    for ch in value.bytes() {
        if ch.is_ascii_digit() {
            if !in_number && buf_active {
                if len < MAX_VERSION_PARTS {
                    parts[len] = VersionPart::Text(text_buf, text_len);
                    len += 1;
                }
                text_buf = [0u8; 8];
                text_len = 0;
            }
            in_number = true;
            buf_active = true;
            num_acc = num_acc.wrapping_mul(10).wrapping_add((ch - b'0') as u64);
        } else if ch.is_ascii_alphabetic() {
            if in_number && buf_active {
                if len < MAX_VERSION_PARTS {
                    parts[len] = VersionPart::Number(num_acc);
                    len += 1;
                }
                num_acc = 0;
            }
            in_number = false;
            buf_active = true;
            if text_len < 8 {
                text_buf[text_len as usize] = ch.to_ascii_lowercase();
                text_len += 1;
            }
        } else {
            if buf_active {
                if in_number {
                    if len < MAX_VERSION_PARTS {
                        parts[len] = VersionPart::Number(num_acc);
                        len += 1;
                    }
                    num_acc = 0;
                } else if len < MAX_VERSION_PARTS {
                    parts[len] = VersionPart::Text(text_buf, text_len);
                    len += 1;
                    text_buf = [0u8; 8];
                    text_len = 0;
                }
                buf_active = false;
            }
            in_number = false;
        }
    }

    if buf_active {
        if in_number {
            if len < MAX_VERSION_PARTS {
                parts[len] = VersionPart::Number(num_acc);
                len += 1;
            }
        } else if len < MAX_VERSION_PARTS {
            parts[len] = VersionPart::Text(text_buf, text_len);
            len += 1;
        }
    }

    (parts, len)
}

fn tokenize_cached(version: &str) -> ([VersionPart; MAX_VERSION_PARTS], usize) {
    use std::cell::RefCell;
    use std::collections::HashMap;

    thread_local! {
        static CACHE: RefCell<HashMap<String, ([VersionPart; MAX_VERSION_PARTS], usize)>> =
            RefCell::new(HashMap::with_capacity(256));
    }
    const MAX_ENTRIES: usize = 4096;

    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(cached) = cache.get(version) {
            return *cached;
        }
        let result = tokenize_version(version);
        if cache.len() < MAX_ENTRIES {
            cache.insert(version.to_string(), result);
        }
        result
    })
}

pub(super) fn compare_versions(left: &str, right: &str) -> Ordering {
    let (lp, ll) = tokenize_cached(left);
    let (rp, rl) = tokenize_cached(right);
    let max_len = std::cmp::max(ll, rl);
    for i in 0..max_len {
        let left_part = if i < ll { lp[i] } else { VersionPart::Number(0) };
        let right_part = if i < rl { rp[i] } else { VersionPart::Number(0) };
        let ordering = match (left_part, right_part) {
            (VersionPart::Number(a), VersionPart::Number(b)) => a.cmp(&b),
            (VersionPart::Text(a, al), VersionPart::Text(b, bl)) => a[..al as usize].cmp(&b[..bl as usize]),
            (VersionPart::Number(_), VersionPart::Text(..)) => Ordering::Greater,
            (VersionPart::Text(..), VersionPart::Number(_)) => Ordering::Less,
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}
