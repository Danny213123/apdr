pub fn python_slim_template(python_version: &str, system_deps: &[String]) -> String {
    let packages = if system_deps.is_empty() {
        "build-essential".to_string()
    } else {
        format!("build-essential {}", system_deps.join(" "))
    };
    let apt_install = if python_version.starts_with("2.7") {
        format!(
            "RUN sed -i 's|deb.debian.org/debian|archive.debian.org/debian|g' /etc/apt/sources.list \
&& sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list \
&& sed -i '/buster-updates/d' /etc/apt/sources.list \
&& apt-get -o Acquire::Check-Valid-Until=false update \
&& apt-get install -y --no-install-recommends {packages} \
&& rm -rf /var/lib/apt/lists/*"
        )
    } else {
        format!(
            "RUN apt-get update && apt-get install -y --no-install-recommends {packages} && rm -rf /var/lib/apt/lists/*"
        )
    };

    format!(
        "FROM python:{python_version}-slim\n\
WORKDIR /app\n\
{apt_install}\n\
COPY requirements.txt /app/requirements.txt\n\
RUN python -m pip install --upgrade pip setuptools wheel && pip install --default-timeout=100 -r /app/requirements.txt\n\
COPY smoke_test.py /app/smoke_test.py\n\
COPY snippet.py /app/snippet.py\n\
CMD [\"python\", \"/app/smoke_test.py\"]\n"
    )
}
