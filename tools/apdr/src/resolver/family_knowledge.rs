use std::collections::{BTreeMap, BTreeSet};

use crate::docker;
use crate::{ParseResult, ResolvedDependency};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictKind {
    Namespace,
    Fork,
    Variant,
    Replacement,
    Migration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberStatus {
    Active,
    Deprecated,
    Unmaintained,
}

#[derive(Clone, Debug)]
pub struct FamilyMember {
    pub package: &'static str,
    pub modules: &'static [&'static str],
    pub status: MemberStatus,
    pub preferred: bool,
}

#[derive(Clone, Debug)]
pub struct PackageFamily {
    pub name: &'static str,
    pub modules: &'static [&'static str],
    pub conflict_kind: ConflictKind,
    pub members: &'static [FamilyMember],
    pub notes: &'static str,
}

impl PackageFamily {
    pub fn preferred(&self) -> Option<&FamilyMember> {
        self.members
            .iter()
            .find(|member| member.preferred)
            .or_else(|| self.members.iter().find(|member| member.status == MemberStatus::Active))
    }
}

macro_rules! member {
    ($pkg:expr, $mods:expr, preferred) => {
        FamilyMember {
            package: $pkg,
            modules: $mods,
            status: MemberStatus::Active,
            preferred: true,
        }
    };
    ($pkg:expr, $mods:expr, $status:ident) => {
        FamilyMember {
            package: $pkg,
            modules: $mods,
            status: MemberStatus::$status,
            preferred: false,
        }
    };
}

pub fn normalize(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace('.', "_")
}

pub static FAMILIES: &[PackageFamily] = &[
    PackageFamily {
        name: "opencv",
        modules: &["cv2"],
        conflict_kind: ConflictKind::Variant,
        notes: "All OpenCV wheels install into the cv2 namespace.",
        members: &[
            member!("opencv-python", &["cv2"], Active),
            member!("opencv-python-headless", &["cv2"], preferred),
            member!("opencv-contrib-python", &["cv2"], Active),
            member!("opencv-contrib-python-headless", &["cv2"], Active),
        ],
    },
    PackageFamily {
        name: "pycrypto",
        modules: &["Crypto"],
        conflict_kind: ConflictKind::Fork,
        notes: "pycryptodome is the maintained drop-in replacement for pycrypto.",
        members: &[
            member!("pycrypto", &["Crypto"], Unmaintained),
            member!("pycryptodome", &["Crypto"], preferred),
        ],
    },
    PackageFamily {
        name: "theano",
        modules: &["theano"],
        conflict_kind: ConflictKind::Fork,
        notes: "Theano and Theano-PyMC share the same namespace.",
        members: &[
            member!("Theano", &["theano"], Unmaintained),
            member!("Theano-PyMC", &["theano"], preferred),
            member!("theano-pymc", &["theano"], Deprecated),
        ],
    },
    PackageFamily {
        name: "pil",
        modules: &["PIL", "Image", "ImageDraw", "ImageFont"],
        conflict_kind: ConflictKind::Replacement,
        notes: "Pillow is the maintained fork of PIL.",
        members: &[
            member!("PIL", &["PIL", "Image"], Unmaintained),
            member!("Pillow", &["PIL", "Image", "ImageDraw", "ImageFont"], preferred),
        ],
    },
    PackageFamily {
        name: "yaml",
        modules: &["yaml"],
        conflict_kind: ConflictKind::Namespace,
        notes: "PyYAML owns the yaml namespace.",
        members: &[
            member!("PyYAML", &["yaml"], preferred),
            member!("yaml", &["yaml"], Deprecated),
        ],
    },
    PackageFamily {
        name: "sklearn",
        modules: &["sklearn"],
        conflict_kind: ConflictKind::Namespace,
        notes: "The sklearn package is a deprecated shim.",
        members: &[
            member!("scikit-learn", &["sklearn"], preferred),
            member!("sklearn", &["sklearn"], Deprecated),
        ],
    },
    PackageFamily {
        name: "beautifulsoup",
        modules: &["BeautifulSoup", "bs4"],
        conflict_kind: ConflictKind::Migration,
        notes: "BeautifulSoup 3 migrated to beautifulsoup4.",
        members: &[
            member!("BeautifulSoup", &["BeautifulSoup"], Unmaintained),
            member!("beautifulsoup4", &["bs4"], preferred),
        ],
    },
    PackageFamily {
        name: "dateutil",
        modules: &["dateutil"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-dateutil is the maintained package name.",
        members: &[
            member!("python-dateutil", &["dateutil"], preferred),
            member!("dateutil", &["dateutil"], Deprecated),
        ],
    },
    PackageFamily {
        name: "dns",
        modules: &["dns"],
        conflict_kind: ConflictKind::Namespace,
        notes: "dnspython is the maintained dns provider.",
        members: &[
            member!("dnspython", &["dns"], preferred),
            member!("pydns", &["dns"], Unmaintained),
            member!("py3dns", &["dns"], Deprecated),
        ],
    },
    PackageFamily {
        name: "magic",
        modules: &["magic"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-magic and filemagic expose incompatible magic APIs.",
        members: &[
            member!("python-magic", &["magic"], preferred),
            member!("filemagic", &["magic"], Active),
        ],
    },
    PackageFamily {
        name: "jwt",
        modules: &["jwt"],
        conflict_kind: ConflictKind::Namespace,
        notes: "PyJWT is the maintained jwt package.",
        members: &[
            member!("PyJWT", &["jwt"], preferred),
            member!("jwt", &["jwt"], Deprecated),
        ],
    },
    PackageFamily {
        name: "zmq",
        modules: &["zmq"],
        conflict_kind: ConflictKind::Namespace,
        notes: "pyzmq is the canonical zmq binding.",
        members: &[
            member!("pyzmq", &["zmq"], preferred),
            member!("zmq", &["zmq"], Deprecated),
        ],
    },
    PackageFamily {
        name: "soundfile",
        modules: &["soundfile"],
        conflict_kind: ConflictKind::Migration,
        notes: "pysoundfile was renamed to SoundFile.",
        members: &[
            member!("SoundFile", &["soundfile"], preferred),
            member!("pysoundfile", &["soundfile"], Deprecated),
        ],
    },
    PackageFamily {
        name: "slack",
        modules: &["slack_sdk", "slackclient"],
        conflict_kind: ConflictKind::Migration,
        notes: "slackclient was renamed to slack-sdk.",
        members: &[
            member!("slack-sdk", &["slack_sdk"], preferred),
            member!("slackclient", &["slackclient"], Deprecated),
        ],
    },
    PackageFamily {
        name: "setuptools",
        modules: &["setuptools", "pkg_resources"],
        conflict_kind: ConflictKind::Replacement,
        notes: "distribute was merged back into setuptools.",
        members: &[
            member!("setuptools", &["setuptools", "pkg_resources"], preferred),
            member!("distribute", &["setuptools"], Unmaintained),
        ],
    },
    PackageFamily {
        name: "protobuf",
        modules: &["google.protobuf"],
        conflict_kind: ConflictKind::Variant,
        notes: "protobuf3 is a deprecated alternate packaging of protobuf.",
        members: &[
            member!("protobuf", &["google.protobuf"], preferred),
            member!("protobuf3", &["google.protobuf"], Deprecated),
        ],
    },
    PackageFamily {
        name: "drf",
        modules: &["rest_framework"],
        conflict_kind: ConflictKind::Namespace,
        notes: "djangorestframework is the canonical package name.",
        members: &[
            member!("djangorestframework", &["rest_framework"], preferred),
            member!("drf", &["rest_framework"], Deprecated),
        ],
    },
    PackageFamily {
        name: "haystack",
        modules: &["haystack"],
        conflict_kind: ConflictKind::Namespace,
        notes: "django-haystack and haystack are different projects sharing a namespace.",
        members: &[
            member!("django-haystack", &["haystack"], preferred),
            member!("haystack", &["haystack"], Active),
        ],
    },
    PackageFamily {
        name: "igraph",
        modules: &["igraph"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-igraph is the maintained package name.",
        members: &[
            member!("python-igraph", &["igraph"], preferred),
            member!("igraph", &["igraph"], Deprecated),
        ],
    },
    PackageFamily {
        name: "pdfminer",
        modules: &["pdfminer"],
        conflict_kind: ConflictKind::Fork,
        notes: "pdfminer.six is the maintained Python 3 fork.",
        members: &[
            member!("pdfminer.six", &["pdfminer"], preferred),
            member!("pdfminer", &["pdfminer"], Unmaintained),
        ],
    },
    PackageFamily {
        name: "aesara-pytensor",
        modules: &["aesara", "pytensor"],
        conflict_kind: ConflictKind::Migration,
        notes: "aesara was renamed to pytensor.",
        members: &[
            member!("pytensor", &["pytensor"], preferred),
            member!("aesara", &["aesara"], Deprecated),
        ],
    },
];

pub struct FamilyRegistry {
    by_package: BTreeMap<String, usize>,
    by_module: BTreeMap<String, Vec<usize>>,
}

impl FamilyRegistry {
    pub fn new() -> Self {
        let mut by_package = BTreeMap::new();
        let mut by_module: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (index, family) in FAMILIES.iter().enumerate() {
            for member in family.members {
                by_package.insert(normalize(member.package), index);
                for module in member.modules {
                    by_module
                        .entry(module.to_ascii_lowercase())
                        .or_default()
                        .push(index);
                }
            }
            for module in family.modules {
                by_module
                    .entry(module.to_ascii_lowercase())
                    .or_default()
                    .push(index);
            }
        }
        Self { by_package, by_module }
    }

    pub fn family_for_package(&self, package: &str) -> Option<&'static PackageFamily> {
        self.by_package
            .get(&normalize(package))
            .map(|index| &FAMILIES[*index])
    }

    pub fn families_for_module(&self, module: &str) -> Vec<&'static PackageFamily> {
        self.by_module
            .get(&module.to_ascii_lowercase())
            .map(|indices| indices.iter().map(|index| &FAMILIES[*index]).collect())
            .unwrap_or_default()
    }
}

pub fn apply_family_knowledge(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> Vec<String> {
    let mut notes = prune_family_conflicts(resolved);
    if let Some(note) = apply_legacy_pymc3_bundle(
        parse_result,
        resolved,
        selected_python,
        python_range,
        execute_snippet,
    ) {
        notes.push(note);
    }
    notes
}

pub fn recover_family_knowledge(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
    log: &str,
) -> Option<String> {
    let lowercase = log.to_ascii_lowercase();
    if uses_legacy_pymc3_stack(parse_result, resolved)
        && (lowercase.contains("pymc3 3.11.5 depends on scipy<1.8.0")
            || lowercase.contains("pymc3 3.11.5 depends on numpy<1.22.2")
            || lowercase.contains("requires a different python version")
            || lowercase.contains("cannot import 'setuptools.build_meta'")
            || lowercase.contains("resolutionimpossible"))
    {
        return apply_legacy_pymc3_bundle(
            parse_result,
            resolved,
            selected_python,
            python_range,
            execute_snippet,
        )
        .map(|note| format!("Family-aware recovery reapplied the legacy PyMC3 stack. {note}"));
    }
    None
}

pub fn validation_candidate_versions(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> Option<Vec<String>> {
    if uses_legacy_pymc3_stack(parse_result, resolved) {
        let candidates = docker::parallel::candidate_versions(selected_python, python_range);
        let preferred = if execute_snippet {
            vec!["2.7", "3.10", "3.9"]
        } else {
            vec!["3.10", "3.9", "2.7"]
        };
        let ordered = preferred
            .into_iter()
            .filter(|version| candidates.iter().any(|candidate| candidate == version))
            .map(str::to_string)
            .collect::<Vec<_>>();
        return Some(if ordered.is_empty() { candidates } else { ordered });
    }

    None
}

fn prune_family_conflicts(resolved: &mut Vec<ResolvedDependency>) -> Vec<String> {
    let registry = FamilyRegistry::new();
    let mut by_family: BTreeMap<&'static str, Vec<usize>> = BTreeMap::new();
    for (index, dependency) in resolved.iter().enumerate() {
        if let Some(family) = registry.family_for_package(&dependency.package_name) {
            by_family.entry(family.name).or_default().push(index);
        }
    }

    let mut keep = vec![true; resolved.len()];
    let mut notes = Vec::new();
    for (family_name, indices) in by_family {
        if indices.len() < 2 {
            continue;
        }
        let Some(family) = registry.family_for_package(&resolved[indices[0]].package_name) else {
            continue;
        };
        let preferred = family.preferred().map(|member| normalize(member.package));
        let mut chosen_index = indices[0];
        if let Some(preferred_name) = preferred {
            if let Some(index) = indices
                .iter()
                .copied()
                .find(|index| normalize(&resolved[*index].package_name) == preferred_name)
            {
                chosen_index = index;
            }
        }

        let packages = indices
            .iter()
            .map(|index| resolved[*index].package_name.clone())
            .collect::<Vec<_>>();
        for index in indices {
            if index != chosen_index {
                keep[index] = false;
            }
        }
        notes.push(format!(
            "Family knowledge pruned the {} {:?} conflict: kept `{}` and removed {}. {}",
            family_name,
            family.conflict_kind,
            resolved[chosen_index].package_name,
            packages
                .into_iter()
                .filter(|package| package != &resolved[chosen_index].package_name)
                .map(|package| format!("`{package}`"))
                .collect::<Vec<_>>()
                .join(", "),
            family.notes
        ));
    }

    let mut index = 0usize;
    resolved.retain(|_| {
        let keep_row = keep[index];
        index += 1;
        keep_row
    });
    notes
}

fn apply_legacy_pymc3_bundle(
    parse_result: &ParseResult,
    resolved: &mut Vec<ResolvedDependency>,
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> Option<String> {
    if !uses_legacy_pymc3_stack(parse_result, resolved) {
        return None;
    }

    let bundle_python =
        preferred_legacy_pymc3_python(selected_python, python_range, execute_snippet);
    let mut changes = Vec::new();

    for (import_name, package_name, version) in legacy_pymc3_bundle(&bundle_python) {
        if pin_dependency(
            resolved,
            import_name,
            package_name,
            Some(version),
            "family:legacy-pymc3",
            0.97,
        ) {
            changes.push(format!("{package_name}=={version}"));
        }
    }

    if changes.is_empty() {
        return None;
    }

    Some(format!(
        "Family knowledge targeted the legacy PyMC3 stack at Python {bundle_python} and pinned a coherent bundle: {}.",
        changes.join(", ")
    ))
}

fn preferred_legacy_pymc3_python(
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> String {
    if execute_snippet {
        return if selected_python.starts_with("3.10") || selected_python.starts_with("3.9") {
            selected_python.to_string()
        } else {
            "2.7".to_string()
        };
    }

    let candidates = docker::parallel::candidate_versions(selected_python, python_range);
    if candidates.iter().any(|value| value == "3.10") {
        "3.10".to_string()
    } else if candidates.iter().any(|value| value == "3.9") {
        "3.9".to_string()
    } else if candidates.iter().any(|value| value == "2.7") {
        "2.7".to_string()
    } else {
        selected_python.to_string()
    }
}

fn legacy_pymc3_bundle(bundle_python: &str) -> &'static [(&'static str, &'static str, &'static str)] {
    if bundle_python.starts_with("2.") {
        &[
            ("numpy", "numpy", "1.16.6"),
            ("pandas", "pandas", "0.24.2"),
            ("pymc3", "pymc3", "3.5"),
            ("scipy", "scipy", "1.2.3"),
            ("theano", "Theano", "1.0.5"),
        ]
    } else {
        &[
            ("numpy", "numpy", "1.21.6"),
            ("pandas", "pandas", "1.5.3"),
            ("pymc3", "pymc3", "3.11.5"),
            ("scipy", "scipy", "1.7.3"),
            ("theano", "Theano-PyMC", "1.1.2"),
        ]
    }
}

fn uses_legacy_pymc3_stack(parse_result: &ParseResult, resolved: &[ResolvedDependency]) -> bool {
    let imports = parse_result
        .imports
        .iter()
        .chain(parse_result.import_paths.iter())
        .map(|item| item.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let packages = resolved
        .iter()
        .map(|dependency| normalize(&dependency.package_name))
        .collect::<BTreeSet<_>>();

    imports.contains("pymc3")
        || imports.contains("theano")
        || packages.contains("pymc3")
        || packages.contains("theano_pymc")
        || packages.contains("theano")
}

fn pin_dependency(
    resolved: &mut Vec<ResolvedDependency>,
    import_name: &str,
    package_name: &str,
    version: Option<&str>,
    strategy: &str,
    confidence: f64,
) -> bool {
    let target_version = version.map(str::to_string);
    for dependency in resolved.iter_mut() {
        let import_match = dependency.import_name.eq_ignore_ascii_case(import_name);
        let package_match = normalize(&dependency.package_name) == normalize(package_name);
        if import_match || package_match {
            let changed = dependency.package_name != package_name || dependency.version != target_version;
            dependency.import_name = import_name.to_string();
            dependency.package_name = package_name.to_string();
            dependency.version = target_version.clone();
            dependency.strategy = strategy.to_string();
            dependency.confidence = confidence;
            return changed;
        }
    }

    resolved.push(ResolvedDependency {
        import_name: import_name.to_string(),
        package_name: package_name.to_string(),
        version: target_version,
        strategy: strategy.to_string(),
        confidence,
    });
    true
}
