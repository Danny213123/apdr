use super::detection::{
    normalize, uses_cfscrape_stack, uses_legacy_flask_stack, uses_legacy_ggplot_stack,
    uses_legacy_pymc3_stack, uses_legacy_tensorflow_stack, uses_simplecv_stack,
};
use super::legacy_bundles::{
    apply_cfscrape_bundle, apply_legacy_flask_bundle, apply_legacy_ggplot_bundle,
    apply_legacy_johnny_cache_bundle, apply_legacy_pillow_pin, apply_legacy_pymc3_bundle,
    apply_legacy_scrapy_bundle, apply_legacy_tensorflow_bundle, apply_simplecv_bundle,
    ensure_keras_backend, legacy_pymc3_bundle, legacy_tensorflow_bundle,
    legacy_tensorflow_candidate_versions, preferred_legacy_pymc3_python,
    preferred_legacy_tensorflow_python,
};
use std::collections::BTreeMap;

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
            .or_else(|| {
                self.members
                    .iter()
                    .find(|member| member.status == MemberStatus::Active)
            })
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
            member!(
                "Pillow",
                &["PIL", "Image", "ImageDraw", "ImageFont"],
                preferred
            ),
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
    PackageFamily {
        name: "graphql",
        modules: &["graphql"],
        conflict_kind: ConflictKind::Namespace,
        notes: "graphql-core is the maintained reference implementation.",
        members: &[
            member!("graphql-core", &["graphql"], preferred),
            member!("graphql-py", &["graphql"], Unmaintained),
        ],
    },
    PackageFamily {
        name: "serial",
        modules: &["serial"],
        conflict_kind: ConflictKind::Namespace,
        notes: "pyserial is the maintained serial port library.",
        members: &[
            member!("pyserial", &["serial"], preferred),
            member!("serial", &["serial"], Deprecated),
        ],
    },
    PackageFamily {
        name: "attr",
        modules: &["attr", "attrs"],
        conflict_kind: ConflictKind::Namespace,
        notes: "attrs is the maintained package providing the attr namespace.",
        members: &[
            member!("attrs", &["attr", "attrs"], preferred),
            member!("attr", &["attr"], Deprecated),
        ],
    },
    PackageFamily {
        name: "mysql",
        modules: &["MySQLdb", "pymysql"],
        conflict_kind: ConflictKind::Variant,
        notes: "mysqlclient (C ext) and PyMySQL (pure Python) both provide MySQL access.",
        members: &[
            member!("mysqlclient", &["MySQLdb"], preferred),
            member!("PyMySQL", &["pymysql"], Active),
            member!("mysql-connector-python", &["mysql"], Active),
        ],
    },
    PackageFamily {
        name: "postgres",
        modules: &["psycopg2", "asyncpg"],
        conflict_kind: ConflictKind::Variant,
        notes: "psycopg2-binary is the most common PostgreSQL adapter.",
        members: &[
            member!("psycopg2-binary", &["psycopg2"], preferred),
            member!("psycopg2", &["psycopg2"], Active),
            member!("psycopg", &["psycopg"], Active),
        ],
    },
    PackageFamily {
        name: "ldap",
        modules: &["ldap"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-ldap is the maintained LDAP library.",
        members: &[
            member!("python-ldap", &["ldap"], preferred),
            member!("ldap", &["ldap"], Deprecated),
            member!("ldap3", &["ldap3"], Active),
        ],
    },
    PackageFamily {
        name: "git",
        modules: &["git"],
        conflict_kind: ConflictKind::Namespace,
        notes: "GitPython is the maintained git binding.",
        members: &[
            member!("GitPython", &["git"], preferred),
            member!("pygit2", &["pygit2"], Active),
        ],
    },
    PackageFamily {
        name: "telegram",
        modules: &["telegram", "telethon", "pyrogram"],
        conflict_kind: ConflictKind::Variant,
        notes: "Multiple Telegram bot libraries share overlapping functionality.",
        members: &[
            member!("python-telegram-bot", &["telegram"], preferred),
            member!("Telethon", &["telethon"], Active),
            member!("Pyrogram", &["pyrogram"], Active),
            member!("aiogram", &["aiogram"], Active),
        ],
    },
    PackageFamily {
        name: "discord",
        modules: &["discord"],
        conflict_kind: ConflictKind::Fork,
        notes: "discord.py is the original; py-cord and nextcord are maintained forks.",
        members: &[
            member!("discord.py", &["discord"], preferred),
            member!("py-cord", &["discord"], Active),
            member!("nextcord", &["nextcord"], Active),
        ],
    },
    PackageFamily {
        name: "docx",
        modules: &["docx"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-docx is the maintained Word document library.",
        members: &[
            member!("python-docx", &["docx"], preferred),
            member!("docx", &["docx"], Unmaintained),
        ],
    },
    PackageFamily {
        name: "pptx",
        modules: &["pptx"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-pptx is the maintained PowerPoint library.",
        members: &[member!("python-pptx", &["pptx"], preferred)],
    },
    PackageFamily {
        name: "pdf-reader",
        modules: &["PyPDF2", "pypdf"],
        conflict_kind: ConflictKind::Migration,
        notes: "PyPDF2 was merged back into pypdf.",
        members: &[
            member!("pypdf", &["pypdf"], preferred),
            member!("PyPDF2", &["PyPDF2"], Deprecated),
            member!("pyPdf", &["pyPdf"], Unmaintained),
        ],
    },
    PackageFamily {
        name: "fitz",
        modules: &["fitz"],
        conflict_kind: ConflictKind::Namespace,
        notes: "PyMuPDF provides the fitz module.",
        members: &[
            member!("PyMuPDF", &["fitz"], preferred),
            member!("fitz", &["fitz"], Deprecated),
        ],
    },
    PackageFamily {
        name: "opengl",
        modules: &["OpenGL"],
        conflict_kind: ConflictKind::Variant,
        notes: "PyOpenGL and PyOpenGL-accelerate share the OpenGL namespace.",
        members: &[
            member!("PyOpenGL", &["OpenGL"], preferred),
            member!("PyOpenGL-accelerate", &["OpenGL"], Active),
        ],
    },
    PackageFamily {
        name: "pyobjc",
        modules: &["objc", "AppKit", "Foundation", "Quartz", "CoreFoundation"],
        conflict_kind: ConflictKind::Variant,
        notes: "pyobjc is the umbrella; framework packages install into sub-namespaces.",
        members: &[
            member!("pyobjc", &["objc", "PyObjCTools"], preferred),
            member!("pyobjc-framework-Cocoa", &["AppKit", "Foundation"], Active),
            member!(
                "pyobjc-framework-Quartz",
                &["Quartz", "CoreGraphics"],
                Active
            ),
            member!(
                "pyobjc-framework-CoreFoundation",
                &["CoreFoundation"],
                Active
            ),
            member!("pyobjc-framework-CoreServices", &["LaunchServices"], Active),
            member!(
                "pyobjc-framework-SystemConfiguration",
                &["SystemConfiguration"],
                Active
            ),
        ],
    },
    PackageFamily {
        name: "pygobject",
        modules: &["gi"],
        conflict_kind: ConflictKind::Namespace,
        notes: "PyGObject provides the gi namespace for GTK/GObject bindings.",
        members: &[member!("PyGObject", &["gi"], preferred)],
    },
    PackageFamily {
        name: "pyside",
        modules: &["PySide", "PySide2", "PySide6"],
        conflict_kind: ConflictKind::Variant,
        notes: "Qt bindings use version-specific top-level namespaces and are not drop-in interchangeable.",
        members: &[
            member!("PySide", &["PySide"], Active),
            member!("PySide2", &["PySide2"], Active),
            member!("PySide6", &["PySide6"], preferred),
        ],
    },
    PackageFamily {
        name: "pywin32",
        modules: &["win32api", "win32con", "win32com", "win32gui", "pywintypes"],
        conflict_kind: ConflictKind::Namespace,
        notes: "pywin32 provides all win32 modules.",
        members: &[member!(
            "pywin32",
            &["win32api", "win32con", "win32com", "win32gui", "pywintypes"],
            preferred
        )],
    },
    PackageFamily {
        name: "levenshtein",
        modules: &["Levenshtein"],
        conflict_kind: ConflictKind::Migration,
        notes: "python-Levenshtein now wraps rapidfuzz internally.",
        members: &[
            member!("python-Levenshtein", &["Levenshtein"], Active),
            member!("rapidfuzz", &["rapidfuzz"], preferred),
            member!("thefuzz", &["thefuzz"], Active),
            member!("fuzzywuzzy", &["fuzzywuzzy"], Deprecated),
        ],
    },
    PackageFamily {
        name: "socks-proxy",
        modules: &["socks"],
        conflict_kind: ConflictKind::Namespace,
        notes: "PySocks is the maintained SOCKS proxy library.",
        members: &[
            member!("PySocks", &["socks"], preferred),
            member!("SocksiPy", &["socks"], Unmaintained),
        ],
    },
    PackageFamily {
        name: "faiss",
        modules: &["faiss"],
        conflict_kind: ConflictKind::Variant,
        notes: "faiss-cpu and faiss-gpu are mutually exclusive variants.",
        members: &[
            member!("faiss-cpu", &["faiss"], preferred),
            member!("faiss-gpu", &["faiss"], Active),
        ],
    },
    PackageFamily {
        name: "paddle",
        modules: &["paddle"],
        conflict_kind: ConflictKind::Variant,
        notes: "paddlepaddle and paddlepaddle-gpu share the paddle namespace.",
        members: &[
            member!("paddlepaddle", &["paddle"], preferred),
            member!("paddlepaddle-gpu", &["paddle"], Active),
        ],
    },
    PackageFamily {
        name: "gymnasium",
        modules: &["gym", "gymnasium"],
        conflict_kind: ConflictKind::Migration,
        notes: "gym was renamed to gymnasium by the Farama Foundation.",
        members: &[
            member!("gymnasium", &["gymnasium"], preferred),
            member!("gym", &["gym"], Deprecated),
        ],
    },
    PackageFamily {
        name: "gdal",
        modules: &["osgeo"],
        conflict_kind: ConflictKind::Namespace,
        notes: "GDAL is the canonical package providing osgeo.",
        members: &[
            member!("GDAL", &["osgeo"], preferred),
            member!("pygdal", &["osgeo"], Active),
        ],
    },
    PackageFamily {
        name: "decouple",
        modules: &["decouple"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-decouple is the maintained config library.",
        members: &[
            member!("python-decouple", &["decouple"], preferred),
            member!("decouple", &["decouple"], Deprecated),
        ],
    },
    PackageFamily {
        name: "dotenv",
        modules: &["dotenv"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-dotenv is the maintained .env file loader.",
        members: &[
            member!("python-dotenv", &["dotenv"], preferred),
            member!("dotenv", &["dotenv"], Deprecated),
        ],
    },
    PackageFamily {
        name: "grpc",
        modules: &["grpc"],
        conflict_kind: ConflictKind::Variant,
        notes: "grpcio is the core package; grpcio-tools adds protoc compilation.",
        members: &[
            member!("grpcio", &["grpc"], preferred),
            member!("grpcio-tools", &["grpc_tools"], Active),
            member!("grpcio-status", &["grpc_status"], Active),
            member!("grpcio-health-checking", &["grpc_health"], Active),
        ],
    },
    PackageFamily {
        name: "whisper",
        modules: &["whisper"],
        conflict_kind: ConflictKind::Namespace,
        notes: "openai-whisper is OpenAI's speech recognition; whisper is a Graphite tool.",
        members: &[
            member!("openai-whisper", &["whisper"], preferred),
            member!("whisper", &["whisper"], Active),
        ],
    },
    PackageFamily {
        name: "slugify",
        modules: &["slugify"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-slugify and awesome-slugify share the slugify namespace.",
        members: &[
            member!("python-slugify", &["slugify"], preferred),
            member!("awesome-slugify", &["slugify"], Active),
        ],
    },
    PackageFamily {
        name: "multipart",
        modules: &["multipart"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-multipart is the maintained multipart form library.",
        members: &[
            member!("python-multipart", &["multipart"], preferred),
            member!("multipart", &["multipart"], Deprecated),
        ],
    },
    PackageFamily {
        name: "jose",
        modules: &["jose"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-jose is the maintained JOSE implementation.",
        members: &[
            member!("python-jose", &["jose"], preferred),
            member!("jose", &["jose"], Deprecated),
        ],
    },
    PackageFamily {
        name: "nmap",
        modules: &["nmap"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-nmap is the maintained nmap wrapper.",
        members: &[
            member!("python-nmap", &["nmap"], preferred),
            member!("nmap", &["nmap"], Deprecated),
        ],
    },
    PackageFamily {
        name: "snap7",
        modules: &["snap7"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-snap7 is the maintained Snap7 binding.",
        members: &[
            member!("python-snap7", &["snap7"], preferred),
            member!("snap7", &["snap7"], Deprecated),
        ],
    },
    PackageFamily {
        name: "cups",
        modules: &["cups"],
        conflict_kind: ConflictKind::Namespace,
        notes: "pycups is the maintained CUPS binding.",
        members: &[member!("pycups", &["cups"], preferred)],
    },
    PackageFamily {
        name: "slack-migration",
        modules: &["slack_sdk", "slack_bolt", "slackclient", "slacker"],
        conflict_kind: ConflictKind::Migration,
        notes: "slackclient and slacker migrated to slack-sdk and slack-bolt.",
        members: &[
            member!("slack-sdk", &["slack_sdk"], preferred),
            member!("slack-bolt", &["slack_bolt"], Active),
            member!("slackclient", &["slackclient"], Deprecated),
            member!("slacker", &["slacker"], Unmaintained),
        ],
    },
    PackageFamily {
        name: "xlib",
        modules: &["Xlib"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-xlib is the maintained X11 binding.",
        members: &[member!("python-xlib", &["Xlib"], preferred)],
    },
    PackageFamily {
        name: "usb",
        modules: &["usb"],
        conflict_kind: ConflictKind::Namespace,
        notes: "pyusb is the maintained USB library.",
        members: &[
            member!("pyusb", &["usb"], preferred),
            member!("usb", &["usb"], Deprecated),
        ],
    },
    PackageFamily {
        name: "blinka",
        modules: &["board", "busio", "digitalio", "analogio", "neopixel"],
        conflict_kind: ConflictKind::Namespace,
        notes: "adafruit-blinka provides CircuitPython APIs on desktop.",
        members: &[member!(
            "adafruit-blinka",
            &["board", "busio", "digitalio", "analogio"],
            preferred
        )],
    },
    PackageFamily {
        name: "rapidjson",
        modules: &["rapidjson"],
        conflict_kind: ConflictKind::Namespace,
        notes: "python-rapidjson is the maintained rapidjson binding.",
        members: &[member!("python-rapidjson", &["rapidjson"], preferred)],
    },
];

pub(super) const EXPLICIT_NAMESPACE_MAPPINGS: &[(&str, &str)] = &[
    ("pkg_resources", "setuptools"),
    ("PIL", "Pillow"),
    ("Image", "Pillow"),
    ("ImageDraw", "Pillow"),
    ("ImageFont", "Pillow"),
    ("ImageEnhance", "Pillow"),
    ("ImageGrab", "Pillow"),
    ("yaml", "PyYAML"),
    ("cv2", "opencv-python"),
    ("gi", "PyGObject"),
    ("gi.repository", "PyGObject"),
    ("rest_framework", "djangorestframework"),
    ("sklearn", "scikit-learn"),
    ("bs4", "beautifulsoup4"),
    ("serial", "pyserial"),
    ("usb", "pyusb"),
    ("psycopg2", "psycopg2-binary"),
    (
        "SystemConfiguration",
        "pyobjc-framework-SystemConfiguration",
    ),
];

pub struct FamilyRegistry {
    by_package: BTreeMap<String, usize>,
    by_module: BTreeMap<String, Vec<usize>>,
}

impl Default for FamilyRegistry {
    fn default() -> Self {
        Self::new()
    }
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
        Self {
            by_package,
            by_module,
        }
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
    if let Some(note) = apply_legacy_tensorflow_bundle(
        parse_result,
        resolved,
        selected_python,
        python_range,
        execute_snippet,
    ) {
        notes.push(note);
    } else if let Some(note) = ensure_keras_backend(resolved, selected_python) {
        // Only add backend companion if the legacy TF bundle was NOT applied.
        notes.push(note);
    }
    if let Some(note) = apply_legacy_pillow_pin(parse_result, resolved, selected_python) {
        notes.push(note);
    }
    if let Some(note) = apply_legacy_flask_bundle(parse_result, resolved, selected_python) {
        notes.push(note);
    }
    if let Some(note) = apply_legacy_johnny_cache_bundle(parse_result, resolved, selected_python) {
        notes.push(note);
    }
    if let Some(note) = apply_legacy_scrapy_bundle(parse_result, resolved, selected_python) {
        notes.push(note);
    }
    if let Some(note) = apply_cfscrape_bundle(parse_result, resolved) {
        notes.push(note);
    }
    if let Some(note) = apply_legacy_ggplot_bundle(parse_result, resolved, selected_python) {
        notes.push(note);
    }
    if let Some(note) = apply_simplecv_bundle(parse_result, resolved) {
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
            || lowercase
                .contains("could not find a version that satisfies the requirement pandas==")
            || lowercase.contains("no matching distribution found for pandas==")
            || lowercase
                .contains("could not find a version that satisfies the requirement numpy==")
            || lowercase.contains("no matching distribution found for numpy==")
            || lowercase.contains("modulenotfounderror: no module named 'pkg_resources'")
            || lowercase.contains("typeerror: 'numpy._dtypemeta' object is not subscriptable")
            || lowercase.contains("requires a different python version")
            || lowercase.contains("cannot import 'setuptools.build_meta'")
            || lowercase.contains("resolutionimpossible"))
    {
        if let Some(note) = apply_legacy_pymc3_bundle(
            parse_result,
            resolved,
            selected_python,
            python_range,
            execute_snippet,
        ) {
            return Some(format!(
                "Family-aware recovery reapplied the legacy PyMC3 stack. {note}"
            ));
        }
        let bundle_python =
            preferred_legacy_pymc3_python(selected_python, python_range, execute_snippet);
        return Some(format!(
            "Family-aware recovery kept the legacy PyMC3 stack pinned at the curated Python {bundle_python} bundle."
        ));
    }

    if uses_legacy_tensorflow_stack(parse_result, resolved)
        && (lowercase.contains("requires a different python version")
            || lowercase
                .contains("could not find a version that satisfies the requirement tensorflow==")
            || lowercase.contains("no matching distribution found for tensorflow==")
            || lowercase
                .contains("could not find a version that satisfies the requirement keras==")
            || lowercase.contains("no matching distribution found for keras==")
            || lowercase.contains("resolutionimpossible"))
    {
        if let Some(note) = apply_legacy_tensorflow_bundle(
            parse_result,
            resolved,
            selected_python,
            python_range,
            execute_snippet,
        ) {
            return Some(format!(
                "Family-aware recovery reapplied the legacy TensorFlow/Keras stack. {note}"
            ));
        }

        // Bundle re-application changed nothing — the pinned versions are
        // unavailable for the target Python.  Strip ALL version pins from the
        // legacy TF bundle so pip can resolve freely (e.g. TF 2.x on 3.8+).
        let mut stripped = false;
        for dep in resolved.iter_mut() {
            if dep.strategy.starts_with("family:legacy-tensorflow") && dep.version.is_some() {
                dep.version = None;
                dep.strategy = "family:legacy-tensorflow-unpinned".to_string();
                stripped = true;
            }
        }
        if stripped {
            return Some(
                "Family-aware recovery unpinned legacy TensorFlow bundle versions \
                 (pinned versions unavailable for target Python — letting pip resolve freely)."
                    .to_string(),
            );
        }

        let bundle_python =
            preferred_legacy_tensorflow_python(selected_python, python_range, execute_snippet);
        return Some(format!(
            "Family-aware recovery kept the legacy TensorFlow/Keras stack pinned at the curated Python {bundle_python} bundle."
        ));
    }

    if uses_legacy_flask_stack(parse_result, resolved)
        && (lowercase.contains("soft_unicode")
            || lowercase.contains("markupsafe")
            || lowercase.contains("werkzeug")
            || lowercase.contains("jinja2")
            || lowercase.contains("itsdangerous")
            || lowercase.contains("flask-security")
            || lowercase.contains("flask_principal")
            || lowercase.contains("mongoengine")
            || lowercase.contains("cannot import name 'url_quote'")
            || lowercase.contains("cannot import name 'escape'"))
    {
        return apply_legacy_flask_bundle(parse_result, resolved, selected_python)
            .map(|note| format!("Family-aware recovery applied the legacy Flask bundle. {note}"));
    }

    if uses_cfscrape_stack(parse_result, resolved)
        && (lowercase.contains("urllib3")
            || lowercase.contains("cipher suite")
            || lowercase.contains("cannot import name 'appengine'")
            || lowercase.contains("requests"))
    {
        return apply_cfscrape_bundle(parse_result, resolved).map(|note| {
            format!("Family-aware recovery applied the cfscrape/urllib3 bundle. {note}")
        });
    }

    if uses_legacy_ggplot_stack(parse_result, resolved)
        && (lowercase.contains("pandas")
            || lowercase.contains("ggplot")
            || lowercase.contains("cannot import name")
            || lowercase.contains("attributeerror"))
    {
        return apply_legacy_ggplot_bundle(parse_result, resolved, selected_python)
            .map(|note| format!("Family-aware recovery applied the legacy ggplot bundle. {note}"));
    }

    if uses_simplecv_stack(parse_result, resolved)
        && (lowercase.contains("cv2")
            || lowercase.contains("opencv")
            || lowercase.contains("simplecv"))
    {
        return apply_simplecv_bundle(parse_result, resolved).map(|note| {
            format!("Family-aware recovery applied the SimpleCV/OpenCV bundle. {note}")
        });
    }
    None
}

pub fn protects_family_version(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
    package_name: &str,
) -> bool {
    let normalized = normalize(package_name);

    if uses_legacy_pymc3_stack(parse_result, resolved) {
        let bundle_python =
            preferred_legacy_pymc3_python(selected_python, python_range, execute_snippet);
        if legacy_pymc3_bundle(&bundle_python)
            .iter()
            .any(|(_, candidate, _)| normalize(candidate) == normalized)
        {
            return true;
        }
    }

    if uses_legacy_tensorflow_stack(parse_result, resolved) {
        let bundle_python =
            preferred_legacy_tensorflow_python(selected_python, python_range, execute_snippet);
        // Only protect packages that have a concrete version pin in the bundle.
        // For Python 3.8+, the bundle leaves versions empty (pip resolves
        // freely), so there's nothing to protect.
        if legacy_tensorflow_bundle(&bundle_python)
            .iter()
            .any(|(_, candidate, version)| {
                normalize(candidate) == normalized && !version.is_empty()
            })
        {
            return true;
        }
    }

    false
}

pub fn validation_candidate_versions(
    parse_result: &ParseResult,
    resolved: &[ResolvedDependency],
    selected_python: &str,
    python_range: usize,
    execute_snippet: bool,
) -> Option<Vec<String>> {
    if uses_legacy_pymc3_stack(parse_result, resolved) {
        let candidates =
            docker::parallel::candidate_versions(selected_python, python_range, None, None);
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
        return Some(if ordered.is_empty() {
            candidates
        } else {
            ordered
        });
    }

    if uses_legacy_tensorflow_stack(parse_result, resolved) {
        let candidates = legacy_tensorflow_candidate_versions(selected_python, python_range);
        // Prefer 3.7 if available (TF 1.15.0 has cp37 wheels), then 2.7,
        // then 3.8+ (will use unpinned TF 2.x).
        let preferred = if execute_snippet {
            vec!["2.7", "3.7", "3.8", "3.9", "3.10"]
        } else {
            vec!["3.7", "2.7", "3.8", "3.9", "3.10"]
        };
        let ordered = preferred
            .into_iter()
            .filter(|version| candidates.iter().any(|candidate| candidate == version))
            .map(str::to_string)
            .collect::<Vec<_>>();
        return Some(if ordered.is_empty() {
            candidates
        } else {
            ordered
        });
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
