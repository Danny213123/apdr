use std::collections::BTreeSet;

use crate::cache::store::{normalize, CacheStore};
use crate::resolver::pypi_client;
use crate::resolver::version_sampler;
use crate::{ParseResult, ResolvedDependency};

pub struct StageResult {
    pub resolved: Vec<ResolvedDependency>,
    pub unresolved: Vec<String>,
    pub heuristic_hits: usize,
}

/// Well-known namespace package prefix → PyPI package name mappings.
/// When an import path matches a prefix, we can directly resolve it.
const NAMESPACE_PACKAGES: &[(&str, &str)] = &[
    ("google.cloud.storage", "google-cloud-storage"),
    ("google.cloud.bigquery", "google-cloud-bigquery"),
    ("google.cloud.pubsub", "google-cloud-pubsub"),
    ("google.cloud.datastore", "google-cloud-datastore"),
    ("google.cloud.firestore", "google-cloud-firestore"),
    ("google.cloud.logging", "google-cloud-logging"),
    ("google.cloud.spanner", "google-cloud-spanner"),
    ("google.cloud.vision", "google-cloud-vision"),
    ("google.cloud.translate", "google-cloud-translate"),
    ("google.cloud.language", "google-cloud-language"),
    ("google.cloud.speech", "google-cloud-speech"),
    ("google.cloud.texttospeech", "google-cloud-texttospeech"),
    ("google.cloud.bigtable", "google-cloud-bigtable"),
    ("google.cloud.kms", "google-cloud-kms"),
    ("google.cloud.tasks", "google-cloud-tasks"),
    ("google.cloud.secret_manager", "google-cloud-secret-manager"),
    ("google.cloud.monitoring", "google-cloud-monitoring"),
    ("google.cloud.container", "google-cloud-container"),
    ("google.cloud.dns", "google-cloud-dns"),
    ("google.cloud.redis", "google-cloud-redis"),
    ("google.cloud.ndb", "google-cloud-ndb"),
    ("google.cloud.memcache", "google-cloud-memcache"),
    ("google.auth", "google-auth"),
    ("google.oauth2", "google-auth"),
    ("google.api_core", "google-api-core"),
    ("google.protobuf", "protobuf"),
    ("azure.storage.blob", "azure-storage-blob"),
    ("azure.storage.queue", "azure-storage-queue"),
    ("azure.storage.file", "azure-storage-file-share"),
    ("azure.cosmos", "azure-cosmos"),
    ("azure.identity", "azure-identity"),
    ("azure.keyvault", "azure-keyvault"),
    ("azure.servicebus", "azure-servicebus"),
    ("azure.eventhub", "azure-eventhub"),
    ("azure.cognitiveservices", "azure-cognitiveservices-vision-computervision"),
    ("azure.mgmt", "azure-mgmt-core"),
    ("azure.core", "azure-core"),
    ("zope.interface", "zope.interface"),
    ("zope.component", "zope.component"),
    ("zope.schema", "zope.schema"),
    ("zope.event", "zope.event"),
    ("zope.security", "zope.security"),
    ("zope.sqlalchemy", "zope.sqlalchemy"),
    ("twisted.internet", "twisted"),
    ("twisted.web", "twisted"),
    ("twisted.protocols", "twisted"),
    ("twisted.conch", "twisted"),
    ("twisted.names", "twisted"),
    ("twisted.mail", "twisted"),
    ("pkg_resources", "setuptools"),
    ("setuptools", "setuptools"),
    ("OpenSSL", "pyOpenSSL"),
    ("jwt", "PyJWT"),
    ("yaml", "PyYAML"),
    ("cv2", "opencv-python"),
    ("PIL", "Pillow"),
    ("Image", "Pillow"),
    ("gi.repository", "PyGObject"),
    ("Crypto", "pycryptodome"),
    ("Cryptodome", "pycryptodome"),
    ("serial", "pyserial"),
    ("usb", "pyusb"),
    ("dateutil", "python-dateutil"),
    ("dotenv", "python-dotenv"),
    ("magic", "python-magic"),
    ("attr", "attrs"),
    ("skimage", "scikit-image"),
    ("sklearn", "scikit-learn"),
    ("bs4", "beautifulsoup4"),
    // --- #15: Expanded common import-to-package mappings ---
    // Pattern B: python- prefix
    ("ldap", "python-ldap"),
    ("daemon", "python-daemon"),
    ("memcache", "python-memcached"),
    ("memcached", "python-memcached"),
    ("xlib", "python-xlib"),
    ("Levenshtein", "python-Levenshtein"),
    // Pattern C: django- prefix
    ("taggit", "django-taggit"),
    ("storages", "django-storages"),
    ("compressor", "django-compressor"),
    ("crispy_forms", "django-crispy-forms"),
    ("ckeditor", "django-ckeditor"),
    ("rest_framework", "djangorestframework"),
    ("mptt", "django-mptt"),
    ("allauth", "django-allauth"),
    ("cors_headers", "django-cors-headers"),
    ("filter", "django-filter"),
    ("guardian", "django-guardian"),
    ("extensions", "django-extensions"),
    // Pattern D: Flask- prefix
    ("flask_cors", "Flask-Cors"),
    ("flask_login", "Flask-Login"),
    ("flask_wtf", "Flask-WTF"),
    ("flask_sqlalchemy", "Flask-SQLAlchemy"),
    ("flask_mail", "Flask-Mail"),
    ("flask_restful", "flask-restful"),
    ("flask_migrate", "Flask-Migrate"),
    ("flask_caching", "Flask-Caching"),
    // Pattern E: Py prefix
    ("enchant", "pyenchant"),
    ("cups", "pycups"),
    ("audio", "pyaudio"),
    ("modbus", "pymodbus"),
    ("nmap", "python-nmap"),
    // Pattern F: completely different names
    ("git", "GitPython"),
    ("dns", "dnspython"),
    ("cassandra", "cassandra-driver"),
    ("wx", "wxPython"),
    ("MySQLdb", "mysqlclient"),
    ("lxml", "lxml"),
    ("docx", "python-docx"),
    ("bson", "pymongo"),
    ("pymongo", "pymongo"),
    ("psycopg2", "psycopg2-binary"),
    ("nacl", "PyNaCl"),
    ("socks", "PySocks"),
    ("zmq", "pyzmq"),
    ("multidict", "multidict"),
    ("aiohttp", "aiohttp"),
    ("websocket", "websocket-client"),
    ("websockets", "websockets"),
    // Common data/ML libraries
    ("scipy", "scipy"),
    ("matplotlib", "matplotlib"),
    ("seaborn", "seaborn"),
    ("plotly", "plotly"),
    ("sympy", "sympy"),
    ("networkx", "networkx"),
    ("nltk", "nltk"),
    ("spacy", "spacy"),
    ("gensim", "gensim"),
    ("xgboost", "xgboost"),
    ("lightgbm", "lightgbm"),
    ("transformers", "transformers"),
    ("tqdm", "tqdm"),
    ("click", "click"),
    ("colorama", "colorama"),
    ("rich", "rich"),
    ("pydantic", "pydantic"),
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
    ("starlette", "starlette"),
    ("httpx", "httpx"),
    ("paramiko", "paramiko"),
    ("fabric", "fabric"),
    ("invoke", "invoke"),
    ("celery", "celery"),
    ("kombu", "kombu"),
    ("boto3", "boto3"),
    ("botocore", "botocore"),
    ("sqlalchemy", "SQLAlchemy"),
    ("alembic", "alembic"),
    ("marshmallow", "marshmallow"),
    ("jinja2", "Jinja2"),
    ("markupsafe", "MarkupSafe"),
    ("werkzeug", "Werkzeug"),
    ("itsdangerous", "itsdangerous"),
    ("babel", "Babel"),
    ("pytz", "pytz"),
    ("arrow", "arrow"),
    ("pendulum", "pendulum"),
    ("msgpack", "msgpack"),
    ("toml", "toml"),
    ("tomli", "tomli"),
    ("ruamel.yaml", "ruamel.yaml"),
    ("pygments", "Pygments"),
    ("tabulate", "tabulate"),
    ("prettytable", "prettytable"),
    ("xmltodict", "xmltodict"),
    ("defusedxml", "defusedxml"),
    ("certifi", "certifi"),
    ("chardet", "chardet"),
    ("charset_normalizer", "charset-normalizer"),
    ("urllib3", "urllib3"),
    ("idna", "idna"),
    ("six", "six"),
    ("typing_extensions", "typing_extensions"),
    ("wrapt", "wrapt"),
    ("decorator", "decorator"),
    ("more_itertools", "more-itertools"),
    ("regex", "regex"),
    ("fuzzywuzzy", "fuzzywuzzy"),
    ("Levenshtein", "python-Levenshtein"),
    ("cachetools", "cachetools"),
    ("filelock", "filelock"),
    ("watchdog", "watchdog"),
    ("apscheduler", "APScheduler"),
    ("freetype", "freetype-py"),
    ("schedule", "schedule"),
    ("scrapy", "Scrapy"),
    ("tweepy", "tweepy"),
    ("praw", "praw"),
    ("telegram", "python-telegram-bot"),
    ("slack_sdk", "slack-sdk"),
    ("stripe", "stripe"),
    ("twilio", "twilio"),
];

pub fn resolve(
    unresolved_imports: &[String],
    parse_result: &ParseResult,
    store: &mut CacheStore,
    python_version: &str,
) -> StageResult {
    let config_packages = parse_result
        .config_deps
        .iter()
        .map(|dependency| normalize(&dependency.package))
        .collect::<BTreeSet<_>>();
    let known_names = pypi_client::cached_package_names(store);

    // Pre-compute trigram sets for all known package names once (avoids
    // re-computing O(K) trigrams per import during the Jaccard scan).
    let known_trigrams: Vec<(&str, Vec<[u8; 3]>)> = known_names
        .iter()
        .map(|name| (name.as_str(), trigrams(name)))
        .collect();

    let mut resolved = Vec::new();
    let mut unresolved = Vec::new();
    let mut heuristic_hits = 0;

    for import_name in unresolved_imports {
        if looks_like_local_helper_import(parse_result, import_name) {
            unresolved.push(import_name.clone());
            continue;
        }
        let normalized = normalize(import_name);

        if config_packages.contains(&normalized) {
            let version = pypi_client::compatible_versions(store, &normalized, python_version)
                .last()
                .cloned();
            resolved.push(ResolvedDependency {
                import_name: import_name.clone(),
                package_name: normalized.clone(),
                version,
                strategy: "heuristic:config-package".to_string(),
                confidence: 0.72,
            });
            heuristic_hits += 1;
            continue;
        }

        // Namespace package prefix lookup — resolves dotted imports like
        // google.cloud.storage → google-cloud-storage, PIL → Pillow, etc.
        // Also handles well-known import-name-differs-from-package-name mappings.
        {
            let mut ns_found = false;
            // Check full import paths from parse_result for dotted namespace matches
            let import_lower = import_name.to_lowercase();
            for &(prefix, package) in NAMESPACE_PACKAGES {
                let prefix_lower = prefix.to_lowercase();
                if import_lower == prefix_lower
                    || import_lower.starts_with(&format!("{prefix_lower}."))
                    || parse_result.import_paths.iter().any(|p| {
                        let p_lower = p.to_lowercase();
                        p_lower == prefix_lower || p_lower.starts_with(&format!("{prefix_lower}."))
                    })
                {
                    let pkg_norm = normalize(package);
                    // For known import-to-package mappings where the names differ,
                    // trust the mapping without requiring PyPI cache validation.
                    // This prevents falling through to exact-match which would
                    // find the wrong package (e.g., `freetype` on PyPI != `freetype-py`).
                    let is_different_name = normalize(prefix) != pkg_norm;
                    if is_different_name || pypi_client::package_exists(store, &pkg_norm, python_version) {
                        let versions =
                            pypi_client::compatible_versions(store, &pkg_norm, python_version);
                        let version = version_sampler::equally_distanced_sample(&versions, &[]);
                        let _ = store.save_import_mapping(
                            import_name,
                            &pkg_norm,
                            version.as_deref(),
                            "heuristic:namespace-prefix",
                        );
                        resolved.push(ResolvedDependency {
                            import_name: import_name.clone(),
                            package_name: pkg_norm,
                            version,
                            strategy: "heuristic:namespace-prefix".to_string(),
                            confidence: 0.90,
                        });
                        heuristic_hits += 1;
                        ns_found = true;
                        break;
                    }
                }
            }
            if ns_found {
                continue;
            }
        }

        if pypi_client::package_exists(store, &normalized, python_version) {
            let versions = pypi_client::compatible_versions(store, &normalized, python_version);
            let version = version_sampler::equally_distanced_sample(&versions, &[]);
            let _ = store.save_import_mapping(
                import_name,
                &normalized,
                version.as_deref(),
                "heuristic:pypi-exact",
            );
            resolved.push(ResolvedDependency {
                import_name: import_name.clone(),
                package_name: normalized.clone(),
                version,
                strategy: "heuristic:pypi-exact".to_string(),
                confidence: 0.84,
            });
            heuristic_hits += 1;
            continue;
        }

        // Tier 2.5: Trigram Jaccard similarity — catches underscore/hyphen
        // variations (e.g. google_cloud_storage → google-cloud-storage).
        let import_tg = trigrams(&normalized);
        if !import_tg.is_empty() {
            let best_trigram = known_trigrams
                .iter()
                .filter_map(|(candidate, candidate_tg)| {
                    if candidate_tg.is_empty() {
                        return None;
                    }
                    // Length pre-filter: very different lengths can't have high Jaccard
                    let len_diff = normalized.len().abs_diff(candidate.len());
                    if len_diff > normalized.len().max(candidate.len()) / 2 {
                        return None;
                    }
                    let sim = trigram_jaccard(&import_tg, candidate_tg);
                    if sim >= 0.35 {
                        Some((candidate.to_string(), sim))
                    } else {
                        None
                    }
                })
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            if let Some((candidate, _sim)) = best_trigram {
                if pypi_client::package_exists(store, &candidate, python_version) {
                    let versions =
                        pypi_client::compatible_versions(store, &candidate, python_version);
                    let version = version_sampler::equally_distanced_sample(&versions, &[]);
                    resolved.push(ResolvedDependency {
                        import_name: import_name.clone(),
                        package_name: candidate,
                        version,
                        strategy: "heuristic:trigram-jaccard".to_string(),
                        confidence: 0.70,
                    });
                    heuristic_hits += 1;
                    continue;
                }
            }
        }

        // Hoist loop-invariant values outside the closure
        let is_short = normalized.len() <= 4;
        let allowed_distance: usize = if is_short { 1 } else { 2 };

        let best_match = known_names
            .iter()
            .filter_map(|candidate| {
                let min_len = normalized.len().min(candidate.len());
                let max_len = normalized.len().max(candidate.len());
                let len_diff = max_len - min_len;

                // Length pre-filter: edit distance >= length difference,
                // so skip expensive Levenshtein when impossible to match.
                let length_ratio_ok = max_len == 0 || min_len * 2 >= max_len;
                let substring_match = !is_short
                    && length_ratio_ok
                    && (candidate.contains(&normalized) || normalized.contains(candidate));

                if len_diff > allowed_distance && !substring_match {
                    return None;
                }

                let distance = levenshtein(&normalized, candidate);
                if distance <= allowed_distance || substring_match {
                    Some((candidate.clone(), distance))
                } else {
                    None
                }
            })
            .min_by(|(a, dist_a), (b, dist_b)| {
                dist_a.cmp(dist_b).then_with(|| {
                    let rank_a = store.popularity.get(a).copied().unwrap_or(usize::MAX);
                    let rank_b = store.popularity.get(b).copied().unwrap_or(usize::MAX);
                    rank_a.cmp(&rank_b)
                })
            });

        if let Some((candidate, _distance)) = best_match {
            let versions = pypi_client::compatible_versions(store, &candidate, python_version);
            let version = version_sampler::equally_distanced_sample(&versions, &[]);
            resolved.push(ResolvedDependency {
                import_name: import_name.clone(),
                package_name: candidate,
                version,
                strategy: "heuristic:fuzzy".to_string(),
                confidence: 0.66,
            });
            heuristic_hits += 1;
        } else {
            unresolved.push(import_name.clone());
        }
    }

    StageResult {
        resolved,
        unresolved,
        heuristic_hits,
    }
}

fn levenshtein(left: &str, right: &str) -> usize {
    if left == right {
        return 0;
    }
    // Package names are ASCII — use bytes directly (no Vec<char> allocation).
    let lb = left.as_bytes();
    let rb = right.as_bytes();
    if lb.is_empty() {
        return rb.len();
    }
    if rb.is_empty() {
        return lb.len();
    }
    // Stack-allocated cost array — package names are always short (<128 bytes).
    let mut costs = [0usize; 129];
    for (i, c) in costs.iter_mut().enumerate().take(rb.len() + 1) {
        *c = i;
    }
    for (li, &lc) in lb.iter().enumerate() {
        let mut corner = costs[0];
        costs[0] = li + 1;
        for (ri, &rc) in rb.iter().enumerate() {
            let upper = costs[ri + 1];
            let sub = if lc == rc { corner } else { corner + 1 };
            costs[ri + 1] = sub.min(costs[ri] + 1).min(upper + 1);
            corner = upper;
        }
    }
    costs[rb.len()]
}

/// Compute character trigrams for a string. Returns sorted, deduplicated byte triples.
fn trigrams(s: &str) -> Vec<[u8; 3]> {
    let bytes = s.as_bytes();
    if bytes.len() < 3 {
        return Vec::new();
    }
    let mut result: Vec<[u8; 3]> = Vec::with_capacity(bytes.len() - 2);
    for window in bytes.windows(3) {
        result.push([window[0], window[1], window[2]]);
    }
    result.sort();
    result.dedup();
    result
}

/// Jaccard similarity between two sorted trigram sets. Range: 0.0–1.0.
fn trigram_jaccard(a: &[[u8; 3]], b: &[[u8; 3]]) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let (mut i, mut j) = (0, 0);
    let (mut intersection, mut union) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                intersection += 1;
                union += 1;
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => {
                union += 1;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                union += 1;
                j += 1;
            }
        }
    }
    union += (a.len() - i) + (b.len() - j);
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn looks_like_local_helper_import(parse_result: &ParseResult, import_name: &str) -> bool {
    let normalized = normalize(import_name);
    // Unconditionally local: names that are never a correct PyPI import.
    // These are standard Django/Flask project structure names and generic
    // project-local module names.
    if matches!(
        normalized.as_str(),
        "input-data" | "settings" | "config" | "conf" | "constants" | "urls"
            | "api" | "app" | "apps" | "views" | "models" | "forms" | "admin"
            | "tests" | "manage" | "wsgi" | "asgi"
    ) {
        return true;
    }
    let generic_helper = matches!(
        normalized.as_str(),
        "util" | "utils" | "helper" | "helpers" | "common" | "shared"
    );
    generic_helper
        && parse_result.import_paths.iter().any(|path| {
            let np = normalize(path);
            np.len() > normalized.len()
                && np.starts_with(normalized.as_str())
                && np.as_bytes()[normalized.len()] == b'-'
        })
}
