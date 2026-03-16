pub fn detect_minimum_python(source: &str) -> String {
    if looks_like_python_27(source) {
        return "2.7".to_string();
    }

    let mut version = "3.9".to_string();
    if source.contains(":=") {
        version = max_version(&version, "3.8");
    }
    if source.contains("match ") && source.contains("case ") {
        version = max_version(&version, "3.10");
    }
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("type ") && trimmed.contains('=') {
            version = max_version(&version, "3.12");
        }
        if trimmed.starts_with("async def ") || trimmed.contains(" await ") {
            version = max_version(&version, "3.7");
        }
        if trimmed.contains("f\"") || trimmed.contains("f'") {
            version = max_version(&version, "3.6");
        }
    }
    max_version(&version, "3.9")
}

pub fn detect_maximum_python(source: &str) -> Option<String> {
    if looks_like_python_27(source) {
        return Some("2.7".to_string());
    }
    None
}

fn looks_like_python_27(source: &str) -> bool {
    // Check for Python 2-only stdlib imports (line-level to avoid false positives).
    // e.g. "from html.parser import HTMLParser" contains the substring "import HTMLParser"
    // but is Python 3, not Python 2. Checking at line level prevents this.
    let py2_stdlib = [
        "urllib2",
        "urlparse",
        "_winreg",
        "ConfigParser",
        "cPickle",
        "cStringIO",
        "StringIO",
        "Queue",
        "HTMLParser",
        "httplib",
        "cookielib",
        "robotparser",
    ];

    for line in source.lines() {
        let trimmed = line.trim();

        for name in &py2_stdlib {
            // Match: import <name>, import <name> as ..., import <name>, ...
            let import_stmt = format!("import {name}");
            if let Some(rest) = trimmed.strip_prefix(&import_stmt) {
                if rest.is_empty() || !rest.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
                    return true;
                }
            }
            // Match: from <name> import ..., from <name>.<sub> import ...
            if trimmed.starts_with(&format!("from {name} "))
                || trimmed.starts_with(&format!("from {name}."))
            {
                return true;
            }
        }

        if trimmed.starts_with("print ") && !trimmed.starts_with("print(") {
            return true;
        }
        if trimmed.contains("xrange(")
            || trimmed.contains("raw_input(")
            || trimmed.contains("urllib.urlopen(")
            || trimmed.contains("urllib.urlencode(")
            || trimmed.contains("unicode(")
            || trimmed.contains("long(")
            || trimmed.contains("basestring")
            || trimmed.contains("iteritems(")
            || trimmed.contains("itervalues(")
            || trimmed.contains("iterkeys(")
        {
            return true;
        }
        let lowered = trimmed.to_lowercase();
        if lowered.contains("pyqt4") || lowered.contains("sip.wrapinstance(long(") {
            return true;
        }
        if trimmed.starts_with("except ") && trimmed.contains(',') && trimmed.ends_with(':') {
            return true;
        }
    }
    false
}

fn max_version(current: &str, candidate: &str) -> String {
    if version_tuple(candidate) > version_tuple(current) {
        candidate.to_string()
    } else {
        current.to_string()
    }
}

pub fn version_tuple(value: &str) -> (u32, u32) {
    let mut parts = value.split('.');
    let major = parts.next().unwrap_or("0").parse::<u32>().unwrap_or(0);
    let minor = parts.next().unwrap_or("0").parse::<u32>().unwrap_or(0);
    (major, minor)
}
