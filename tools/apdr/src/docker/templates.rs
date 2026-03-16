pub fn python_slim_template(python_version: &str, system_deps: &[String]) -> String {
    let mut packages = vec!["build-essential".to_string()];
    if python_version.starts_with("2.") {
        packages.push("python-dev".to_string());
    } else {
        packages.push("python3-dev".to_string());
    }
    packages.push("pkg-config".to_string());
    for dep in system_deps {
        if !packages.contains(dep) {
            packages.push(dep.clone());
        }
    }
    let packages_str = packages.join(" ");

    let apt_install = if python_version.starts_with("2.7") {
        format!(
            "RUN sed -i 's|deb.debian.org/debian|archive.debian.org/debian|g' /etc/apt/sources.list \
&& sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list \
&& sed -i '/buster-updates/d' /etc/apt/sources.list \
&& apt-get -o Acquire::Check-Valid-Until=false update \
&& apt-get install -y --no-install-recommends {packages_str} \
&& rm -rf /var/lib/apt/lists/*"
        )
    } else {
        format!(
            "RUN apt-get update && apt-get install -y --no-install-recommends {packages_str} && rm -rf /var/lib/apt/lists/*"
        )
    };

    format!(
        "# syntax=docker/dockerfile:1\n\
FROM python:{python_version}-slim\n\
WORKDIR /app\n\
{apt_install}\n\
COPY requirements.txt /app/requirements.txt\n\
RUN --mount=type=cache,target=/root/.cache/pip \
python -m pip install --upgrade pip setuptools wheel && \
pip install --default-timeout=100 -r /app/requirements.txt\n\
COPY smoke_test.py /app/smoke_test.py\n\
COPY snippet.py /app/snippet.py\n\
CMD [\"python\", \"/app/smoke_test.py\"]\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_includes_system_deps() {
        let dockerfile = python_slim_template(
            "3.11",
            &["libxml2-dev".to_string(), "libxslt1-dev".to_string()],
        );
        assert!(dockerfile.contains("libxml2-dev"));
        assert!(dockerfile.contains("libxslt1-dev"));
        assert!(dockerfile.contains("build-essential"));
    }

    #[test]
    fn template_includes_buildkit_cache_mount() {
        let dockerfile = python_slim_template("3.11", &[]);
        assert!(dockerfile.contains("--mount=type=cache"));
        assert!(dockerfile.contains("# syntax=docker/dockerfile:1"));
    }

    #[test]
    fn template_includes_python_dev_by_default() {
        let dockerfile = python_slim_template("3.11", &[]);
        assert!(dockerfile.contains("python3-dev"));
        assert!(dockerfile.contains("pkg-config"));
    }

    #[test]
    fn template_python2_uses_python_dev() {
        let dockerfile = python_slim_template("2.7", &[]);
        assert!(dockerfile.contains("python-dev"));
        assert!(dockerfile.contains("archive.debian.org"));
    }

    #[test]
    fn template_no_duplicate_deps() {
        let dockerfile = python_slim_template(
            "3.11",
            &["python3-dev".to_string(), "build-essential".to_string()],
        );
        let count = dockerfile.matches("python3-dev").count();
        assert_eq!(count, 1, "python3-dev should appear exactly once");
    }
}
