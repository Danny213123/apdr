use crate::AuthoredDockerPlan;

fn base_system_packages(python_version: &str) -> Vec<String> {
    let mut packages = vec!["build-essential".to_string()];
    if python_version.starts_with("2.") {
        packages.push("python-dev".to_string());
    } else {
        packages.push("python3-dev".to_string());
    }
    packages.push("pkg-config".to_string());
    packages
}

fn dedupe_packages(mut packages: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::new();
    for package in packages.drain(..) {
        if !deduped.contains(&package) {
            deduped.push(package);
        }
    }
    deduped
}

fn render_template(
    base_image: &str,
    python_version: &str,
    system_deps: &[String],
    environment_variables: &[String],
    working_directory: &str,
    command: &[String],
) -> String {
    let mut packages = base_system_packages(python_version);
    for dep in system_deps {
        if !packages.contains(dep) {
            packages.push(dep.clone());
        }
    }
    let packages = dedupe_packages(packages);
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
    let env_block = if environment_variables.is_empty() {
        String::new()
    } else {
        environment_variables
            .iter()
            .map(|value| format!("ENV {value}\n"))
            .collect::<Vec<_>>()
            .join("")
    };
    let workdir = if working_directory.trim().is_empty() {
        "/app"
    } else {
        working_directory.trim()
    };
    let default_command = vec!["python".to_string(), "/app/smoke_test.py".to_string()];
    let effective_command = if command.is_empty() {
        default_command.as_slice()
    } else {
        command
    };
    let command_json = serde_json::to_string(effective_command)
        .unwrap_or_else(|_| "[\"python\",\"/app/smoke_test.py\"]".to_string());

    format!(
        "# syntax=docker/dockerfile:1\n\
FROM {base_image}\n\
WORKDIR {workdir}\n\
{apt_install}\n\
{env_block}\
COPY requirements.txt /app/requirements.txt\n\
RUN --mount=type=cache,target=/root/.cache/pip \
python -m pip install --upgrade pip setuptools wheel && \
pip install --default-timeout=100 -r /app/requirements.txt\n\
COPY smoke_test.py /app/smoke_test.py\n\
COPY snippet.py /app/snippet.py\n\
CMD {command_json}\n"
    )
}

pub fn python_slim_template(python_version: &str, system_deps: &[String]) -> String {
    render_template(
        &format!("python:{python_version}-slim"),
        python_version,
        system_deps,
        &[],
        "/app",
        &[],
    )
}

pub fn authored_docker_template(
    plan: &AuthoredDockerPlan,
    python_version: &str,
    supplemental_system_deps: &[String],
) -> String {
    let mut merged_system_packages = plan.system_packages.clone();
    for item in supplemental_system_deps {
        if !merged_system_packages.contains(item) {
            merged_system_packages.push(item.clone());
        }
    }
    let default_base_image = format!("python:{python_version}-slim");
    let base_image = if plan.base_image.trim().is_empty() {
        default_base_image.as_str()
    } else {
        plan.base_image.trim()
    };
    render_template(
        base_image,
        python_version,
        &merged_system_packages,
        &plan.environment_variables,
        &plan.working_directory,
        &plan.command,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthoredDockerPlan, SmokeStrategy};

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

    #[test]
    fn phase27_template_authored_plan_applies_env_and_command() {
        let dockerfile = authored_docker_template(
            &AuthoredDockerPlan {
                plan_version: "1".to_string(),
                base_image: "python:3.11-slim".to_string(),
                system_packages: vec!["libxml2-dev".to_string()],
                environment_variables: vec!["PIP_DEFAULT_TIMEOUT=100".to_string()],
                working_directory: "/workspace".to_string(),
                command: vec!["python".to_string(), "/app/custom_smoke.py".to_string()],
                smoke_strategy: SmokeStrategy::default(),
                rationale: String::new(),
                section_confidence: std::collections::BTreeMap::new(),
                authorship: "llm-authored".to_string(),
                deterministic_fallback_sections: Vec::new(),
            },
            "3.11",
            &["libxslt1-dev".to_string()],
        );

        assert!(dockerfile.contains("FROM python:3.11-slim"));
        assert!(dockerfile.contains("WORKDIR /workspace"));
        assert!(dockerfile.contains("ENV PIP_DEFAULT_TIMEOUT=100"));
        assert!(dockerfile.contains("libxml2-dev"));
        assert!(dockerfile.contains("libxslt1-dev"));
        assert!(dockerfile.contains("CMD [\"python\",\"/app/custom_smoke.py\"]"));
    }
}
