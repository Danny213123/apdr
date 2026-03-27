use std::collections::BTreeSet;

/// Returns apt packages needed by a given PyPI package for building from source
/// on Debian-based containers (python:*-slim).
/// Returns an empty slice if the package needs no known system deps.
pub fn system_packages_for_pypi(package_name: &str) -> &'static [&'static str] {
    let normalized = package_name.to_ascii_lowercase().replace(['_', '.'], "-");
    match normalized.as_str() {
        "lxml" => &["libxml2-dev", "libxslt1-dev"],
        "pillow" | "pil" => &["libjpeg-dev", "zlib1g-dev", "libfreetype6-dev"],
        "freetype-py" | "freetype" => &["libfreetype6-dev"],
        "psycopg2" | "psycopg2-binary" => &["libpq-dev"],
        "mysqlclient" | "mysql-python" => &["default-libmysqlclient-dev"],
        "cryptography" => &["libssl-dev", "libffi-dev"],
        "numpy" | "scipy" => &["gfortran", "libopenblas-dev", "liblapack-dev"],
        "pandas" => &["gfortran", "libopenblas-dev"],
        "cffi" => &["libffi-dev"],
        "bcrypt" => &["libffi-dev"],
        "pynacl" => &["libsodium-dev"],
        "pyaudio" => &["portaudio19-dev"],
        "pygraphviz" => &["graphviz", "libgraphviz-dev"],
        "python-ldap" | "ldap" => &["libldap2-dev", "libsasl2-dev"],
        "pyyaml" | "yaml" => &["libyaml-dev"],
        "pyzmq" | "zmq" => &["libzmq3-dev"],
        "h5py" => &["libhdf5-dev"],
        "shapely" | "fiona" => &["libgeos-dev", "libgdal-dev"],
        "geopandas" => &["libgeos-dev", "libgdal-dev"],
        "opencv-python" | "opencv-python-headless" | "cv2" => &["libgl1-mesa-glx", "libglib2.0-0"],
        "greenlet" | "gevent" => &["libevent-dev"],
        "uwsgi" => &["libpcre3-dev"],
        "grpcio" => &["libssl-dev"],
        "kerberos" | "pykerberos" => &["libkrb5-dev"],
        "confluent-kafka" => &["librdkafka-dev"],
        "xmlsec" | "python-xmlsec" => &["libxmlsec1-dev", "libxmlsec1-openssl", "pkg-config"],
        "cairocffi" | "pycairo" => &["libcairo2-dev", "pkg-config"],
        "weasyprint" => &[
            "libcairo2-dev",
            "libpango1.0-dev",
            "libgdk-pixbuf2.0-dev",
            "libffi-dev",
        ],
        "pygobject" => &[
            "libgirepository1.0-dev",
            "gir1.2-gtk-3.0",
            "pkg-config",
            "libcairo2-dev",
        ],
        "pyjnius" | "jnius" => &["default-jdk", "pkg-config"],
        "pygtk" => &["python-gtk2-dev", "libgtk2.0-dev"],
        "dbus-python" => &["libdbus-1-dev", "libdbus-glib-1-dev", "pkg-config"],
        "dlib" => &["cmake", "libopenblas-dev"],
        "pylibmc" => &["libmemcached-dev"],
        "mecab" | "mecab-python" | "mecab-python3" => {
            &["mecab", "libmecab-dev", "mecab-ipadic-utf8"]
        }
        "m2crypto" => &["libssl-dev", "swig"],
        "rpy2" => &["r-base-dev"],
        "evdev" => &["linux-headers-generic"],
        "imposm" => &[
            "libgeos-dev",
            "libprotobuf-dev",
            "protobuf-compiler",
            "libtokyocabinet-dev",
        ],
        "mapnik" | "mapnik2" => &["libmapnik-dev", "mapnik-utils"],
        "pyqt5" | "pyqtgraph" => &["qt5-default", "libqt5-dev"],
        "cython" => &[],
        _ => &[],
    }
}

/// Scan a requirements.txt and return deduplicated, sorted apt packages needed.
pub fn infer_system_deps_from_requirements(requirements_txt: &str) -> Vec<String> {
    let mut deps = BTreeSet::new();
    for line in requirements_txt.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let pkg = trimmed
            .split(&['=', '>', '<', '!', '[', ' ', ';'][..])
            .next()
            .unwrap_or("")
            .trim();
        if pkg.is_empty() {
            continue;
        }
        for dep in system_packages_for_pypi(pkg) {
            deps.insert(dep.to_string());
        }
    }
    deps.into_iter().collect()
}

/// Extract system dependency names from Docker build failure logs by matching
/// common header-file and library errors.
pub fn extract_system_deps_from_log(log: &str) -> Vec<String> {
    let lower = log.to_lowercase();
    let mut deps = BTreeSet::new();

    if lower.contains("python.h: no such file or directory")
        || lower.contains("python.h: no such file")
    {
        deps.insert("python3-dev".to_string());
    }
    if lower.contains("libxml/xmlversion.h") || lower.contains("libxml/parser.h") {
        deps.insert("libxml2-dev".to_string());
    }
    if lower.contains("libxslt/xslt.h") || lower.contains("libxslt/extensions.h") {
        deps.insert("libxslt1-dev".to_string());
    }
    if lower.contains("ffi.h: no such file") || lower.contains("ffi.h:") {
        deps.insert("libffi-dev".to_string());
    }
    if lower.contains("openssl/ssl.h")
        || lower.contains("openssl/evp.h")
        || lower.contains("openssl/crypto.h")
    {
        deps.insert("libssl-dev".to_string());
    }
    if lower.contains("libpq-fe.h") || lower.contains("pg_config executable not found") {
        deps.insert("libpq-dev".to_string());
    }
    if lower.contains("mysql.h") || lower.contains("mysql_config") {
        deps.insert("default-libmysqlclient-dev".to_string());
    }
    if lower.contains("jpeglib.h") {
        deps.insert("libjpeg-dev".to_string());
    }
    if lower.contains("zlib.h: no such file") {
        deps.insert("zlib1g-dev".to_string());
    }
    if lower.contains("yaml.h: no such file") || lower.contains("yaml.h:") {
        deps.insert("libyaml-dev".to_string());
    }
    if lower.contains("hdf5.h") {
        deps.insert("libhdf5-dev".to_string());
    }
    if lower.contains("geos_c.h") {
        deps.insert("libgeos-dev".to_string());
    }
    if lower.contains("geos_c.dll") || lower.contains("lib geos_c") {
        deps.insert("libgeos-dev".to_string());
    }
    if lower.contains("cairo.h") || lower.contains("cairo/cairo.h") {
        deps.insert("libcairo2-dev".to_string());
    }
    if lower.contains("sodium.h") {
        deps.insert("libsodium-dev".to_string());
    }
    if lower.contains("zmq.h") {
        deps.insert("libzmq3-dev".to_string());
    }
    if lower.contains("portaudio.h") {
        deps.insert("portaudio19-dev".to_string());
    }
    if lower.contains("pkg-config") && lower.contains("not found") {
        deps.insert("pkg-config".to_string());
    }
    if lower.contains("dbus/dbus.h") || lower.contains("dbus-1.0") {
        deps.insert("libdbus-1-dev".to_string());
    }
    if lower.contains("girepository.h") || lower.contains("girepository-1.0") {
        deps.insert("libgirepository1.0-dev".to_string());
    }
    if lower.contains("gtk+-3.0") || lower.contains("granite") || lower.contains("libgranite") {
        deps.insert("gir1.2-gtk-3.0".to_string());
    }
    if lower.contains("glib.h") || lower.contains("glib-2.0") && lower.contains("not found") {
        deps.insert("libglib2.0-dev".to_string());
    }
    if lower.contains("mecab.h") {
        deps.insert("libmecab-dev".to_string());
    }
    if lower.contains("memcached.h") || lower.contains("libmemcached") {
        deps.insert("libmemcached-dev".to_string());
    }
    if lower.contains("java_home")
        || lower.contains("unable to find java_home")
        || lower.contains("jni.h")
    {
        deps.insert("default-jdk".to_string());
    }
    if lower.contains("r_home") || lower.contains("unable to determine r home") {
        deps.insert("r-base-dev".to_string());
    }
    if lower.contains("swig") && lower.contains("m2crypto") {
        deps.insert("swig".to_string());
        deps.insert("libssl-dev".to_string());
    }
    if lower.contains("cmake") && (lower.contains("not found") || lower.contains("is required")) {
        deps.insert("cmake".to_string());
    }
    if lower.contains("gdal-config")
        || lower.contains("gdal_config")
        || lower.contains("gdal api version")
    {
        deps.insert("libgdal-dev".to_string());
    }
    if lower.contains("geos-config") || lower.contains("geos_config") || lower.contains("geos_c") {
        deps.insert("libgeos-dev".to_string());
    }

    deps.into_iter().collect()
}

pub fn requires_external_runtime_from_log(log: &str) -> bool {
    let lower = log.to_lowercase();
    lower.contains("java_home")
        || lower.contains("unable to find java_home")
        || lower.contains("no java runtime present")
        || lower.contains("r_home")
        || lower.contains("unable to determine r home")
        || lower.contains("r was not found")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_system_deps_for_lxml() {
        let reqs = "lxml==4.9.3\nrequests==2.31.0\n";
        let deps = infer_system_deps_from_requirements(reqs);
        assert!(deps.contains(&"libxml2-dev".to_string()));
        assert!(deps.contains(&"libxslt1-dev".to_string()));
    }

    #[test]
    fn infer_system_deps_deduplicates() {
        let reqs = "numpy==1.24.0\nscipy==1.11.0\n";
        let deps = infer_system_deps_from_requirements(reqs);
        assert_eq!(deps.iter().filter(|d| d.as_str() == "gfortran").count(), 1);
        assert_eq!(
            deps.iter()
                .filter(|d| d.as_str() == "libopenblas-dev")
                .count(),
            1
        );
    }

    #[test]
    fn extract_system_deps_from_python_h_error() {
        let log = "gcc -pthread -c foo.c -I/usr/include/python3.11\n\
                   fatal error: Python.h: No such file or directory\n\
                   compilation terminated.";
        let deps = extract_system_deps_from_log(log);
        assert!(deps.contains(&"python3-dev".to_string()));
    }

    #[test]
    fn extract_system_deps_from_libpq_error() {
        let log = "Error: pg_config executable not found.\n\
                   libpq-fe.h: No such file or directory";
        let deps = extract_system_deps_from_log(log);
        assert!(deps.contains(&"libpq-dev".to_string()));
    }

    #[test]
    fn empty_requirements_yield_no_deps() {
        assert!(infer_system_deps_from_requirements("").is_empty());
    }

    #[test]
    fn unknown_package_yields_no_deps() {
        assert!(system_packages_for_pypi("some-unknown-pkg-12345").is_empty());
    }

    #[test]
    fn comments_and_blanks_skipped() {
        let reqs = "# a comment\n\nlxml>=4.0\n";
        let deps = infer_system_deps_from_requirements(reqs);
        assert!(deps.contains(&"libxml2-dev".to_string()));
    }

    #[test]
    fn extract_openssl_error() {
        let log = "fatal error: openssl/ssl.h: No such file or directory";
        let deps = extract_system_deps_from_log(log);
        assert!(deps.contains(&"libssl-dev".to_string()));
    }

    #[test]
    fn extract_gdal_config_error() {
        let log = "A GDAL API version must be specified. Provide a path to gdal-config using a GDAL_CONFIG environment variable";
        let deps = extract_system_deps_from_log(log);
        assert!(deps.contains(&"libgdal-dev".to_string()));
    }

    #[test]
    fn extract_geos_config_error() {
        let log = "OSError: Could not find lib geos_c or load any of its variants ['libgeos_c.so']";
        let deps = extract_system_deps_from_log(log);
        assert!(deps.contains(&"libgeos-dev".to_string()));
    }

    #[test]
    fn infer_system_deps_for_geospatial() {
        let reqs = "Fiona==1.8.22\ngeopandas==0.12.2\nshapely==2.0.1\n";
        let deps = infer_system_deps_from_requirements(reqs);
        assert!(deps.contains(&"libgdal-dev".to_string()));
        assert!(deps.contains(&"libgeos-dev".to_string()));
    }

    #[test]
    fn extract_java_home_error() {
        let log = "Exception: Unable to find JAVA_HOME";
        let deps = extract_system_deps_from_log(log);
        assert!(deps.contains(&"default-jdk".to_string()));
        assert!(requires_external_runtime_from_log(log));
    }

    #[test]
    fn infer_system_deps_for_pyjnius_and_rpy2() {
        let reqs = "pyjnius==1.6.1\nrpy2==3.5.17\n";
        let deps = infer_system_deps_from_requirements(reqs);
        assert!(deps.contains(&"default-jdk".to_string()));
        assert!(deps.contains(&"r-base-dev".to_string()));
    }
}
