"""Deterministic PyPI package → Debian apt package mapping.

Python port of tools/apdr/src/docker/system_deps.rs.
"""

from __future__ import annotations

import re

_PYPI_TO_APT: dict[str, tuple[str, ...]] = {
    "lxml": ("libxml2-dev", "libxslt1-dev"),
    "pillow": ("libjpeg-dev", "zlib1g-dev", "libfreetype6-dev"),
    "pil": ("libjpeg-dev", "zlib1g-dev", "libfreetype6-dev"),
    "psycopg2": ("libpq-dev",),
    "psycopg2-binary": ("libpq-dev",),
    "mysqlclient": ("default-libmysqlclient-dev",),
    "mysql-python": ("default-libmysqlclient-dev",),
    "cryptography": ("libssl-dev", "libffi-dev"),
    "numpy": ("gfortran", "libopenblas-dev", "liblapack-dev"),
    "scipy": ("gfortran", "libopenblas-dev", "liblapack-dev"),
    "pandas": ("gfortran", "libopenblas-dev"),
    "cffi": ("libffi-dev",),
    "bcrypt": ("libffi-dev",),
    "pynacl": ("libsodium-dev",),
    "pyaudio": ("portaudio19-dev",),
    "pygraphviz": ("graphviz", "libgraphviz-dev"),
    "python-ldap": ("libldap2-dev", "libsasl2-dev"),
    "ldap": ("libldap2-dev", "libsasl2-dev"),
    "pyyaml": ("libyaml-dev",),
    "yaml": ("libyaml-dev",),
    "pyzmq": ("libzmq3-dev",),
    "zmq": ("libzmq3-dev",),
    "h5py": ("libhdf5-dev",),
    "shapely": ("libgeos-dev", "libgdal-dev"),
    "fiona": ("libgeos-dev", "libgdal-dev"),
    "geopandas": ("libgeos-dev", "libgdal-dev"),
    "opencv-python": ("libgl1-mesa-glx", "libglib2.0-0"),
    "opencv-python-headless": ("libgl1-mesa-glx", "libglib2.0-0"),
    "cv2": ("libgl1-mesa-glx", "libglib2.0-0"),
    "greenlet": ("libevent-dev",),
    "gevent": ("libevent-dev",),
    "uwsgi": ("libpcre3-dev",),
    "grpcio": ("libssl-dev",),
    "kerberos": ("libkrb5-dev",),
    "pykerberos": ("libkrb5-dev",),
    "confluent-kafka": ("librdkafka-dev",),
    "xmlsec": ("libxmlsec1-dev", "libxmlsec1-openssl", "pkg-config"),
    "python-xmlsec": ("libxmlsec1-dev", "libxmlsec1-openssl", "pkg-config"),
    "cairocffi": ("libcairo2-dev", "pkg-config"),
    "pycairo": ("libcairo2-dev", "pkg-config"),
    "weasyprint": ("libcairo2-dev", "libpango1.0-dev", "libgdk-pixbuf2.0-dev", "libffi-dev"),
}

_HEADER_TO_APT: list[tuple[str, str]] = [
    ("python.h: no such file", "python3-dev"),
    ("libxml/xmlversion.h", "libxml2-dev"),
    ("libxml/parser.h", "libxml2-dev"),
    ("libxslt/xslt.h", "libxslt1-dev"),
    ("libxslt/extensions.h", "libxslt1-dev"),
    ("ffi.h: no such file", "libffi-dev"),
    ("ffi.h:", "libffi-dev"),
    ("openssl/ssl.h", "libssl-dev"),
    ("openssl/evp.h", "libssl-dev"),
    ("openssl/crypto.h", "libssl-dev"),
    ("libpq-fe.h", "libpq-dev"),
    ("pg_config executable not found", "libpq-dev"),
    ("mysql.h", "default-libmysqlclient-dev"),
    ("mysql_config", "default-libmysqlclient-dev"),
    ("jpeglib.h", "libjpeg-dev"),
    ("zlib.h: no such file", "zlib1g-dev"),
    ("yaml.h: no such file", "libyaml-dev"),
    ("yaml.h:", "libyaml-dev"),
    ("hdf5.h", "libhdf5-dev"),
    ("geos_c.h", "libgeos-dev"),
    ("cairo.h", "libcairo2-dev"),
    ("cairo/cairo.h", "libcairo2-dev"),
    ("sodium.h", "libsodium-dev"),
    ("zmq.h", "libzmq3-dev"),
    ("portaudio.h", "portaudio19-dev"),
]


def _normalize(name: str) -> str:
    return name.lower().replace("_", "-").replace(".", "-")


def system_packages_for_pypi(package_name: str) -> tuple[str, ...]:
    return _PYPI_TO_APT.get(_normalize(package_name), ())


def infer_system_deps(requirements_txt: str) -> list[str]:
    deps: set[str] = set()
    for line in requirements_txt.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pkg = re.split(r"[=><!\[; ]", line, maxsplit=1)[0].strip()
        if not pkg:
            continue
        for dep in system_packages_for_pypi(pkg):
            deps.add(dep)
    return sorted(deps)


def extract_system_deps_from_log(log: str) -> list[str]:
    lower = log.lower()
    deps: set[str] = set()
    for pattern, apt_pkg in _HEADER_TO_APT:
        if pattern in lower:
            deps.add(apt_pkg)
    if "pkg-config" in lower and "not found" in lower:
        deps.add("pkg-config")
    return sorted(deps)
