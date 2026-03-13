use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use apdr::cache::store::CacheStore;
use apdr::context;
use apdr::recovery::classifier;
use apdr::resolver;
use apdr::ResolveConfig;

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_help();
        return Ok(());
    }

    let tool_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match args[1].as_str() {
        "resolve" => resolve_command(&tool_root, &args[2..]),
        "classify-log" => classify_log_command(&tool_root, &args[2..]),
        "cache" => cache_command(&tool_root, &args[2..]),
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        unknown => Err(format!("unknown subcommand: {unknown}")),
    }
}

fn resolve_command(tool_root: &Path, args: &[String]) -> Result<(), String> {
    let mut config = ResolveConfig::for_tool_root(tool_root);
    let mut snippet_path: Option<PathBuf> = None;
    let mut read_from_stdin = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--stdin" => {
                read_from_stdin = true;
            }
            "--output" => {
                index += 1;
                let value = args.get(index).ok_or("--output expects a value")?;
                config.output_dir = PathBuf::from(value);
            }
            "--python" => {
                index += 1;
                let value = args.get(index).ok_or("--python expects a value")?;
                config.python_version = Some(value.to_string());
            }
            "--range" => {
                index += 1;
                let value = args.get(index).ok_or("--range expects a value")?;
                config.python_version_range = value
                    .parse::<usize>()
                    .map_err(|_| "--range must be an integer".to_string())?;
            }
            "--max-retries" => {
                index += 1;
                let value = args.get(index).ok_or("--max-retries expects a value")?;
                config.max_retries = value
                    .parse::<usize>()
                    .map_err(|_| "--max-retries must be an integer".to_string())?;
            }
            "--docker-timeout" => {
                index += 1;
                let value = args.get(index).ok_or("--docker-timeout expects a value")?;
                let seconds = value.parse::<u64>().map_err(|_| {
                    "--docker-timeout must be an integer number of seconds".to_string()
                })?;
                config.docker_timeout = std::time::Duration::from_secs(seconds);
            }
            "--cache-path" => {
                index += 1;
                let value = args.get(index).ok_or("--cache-path expects a value")?;
                config.cache_path = PathBuf::from(value);
            }
            "--llm-provider" => {
                index += 1;
                let value = args.get(index).ok_or("--llm-provider expects a value")?;
                config.llm_provider = value.to_string();
            }
            "--llm-model" => {
                index += 1;
                let value = args.get(index).ok_or("--llm-model expects a value")?;
                config.llm_model = value.to_string();
            }
            "--llm-base-url" => {
                index += 1;
                let value = args.get(index).ok_or("--llm-base-url expects a value")?;
                config.llm_base_url = value.to_string();
            }
            "--benchmark-context-log" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or("--benchmark-context-log expects a value")?;
                config.benchmark_context_log = Some(PathBuf::from(value));
            }
            "--allow-llm" => {
                config.allow_llm = true;
            }
            "--no-config-scan" => {
                config.scan_config_files = false;
            }
            "--no-validate" => {
                config.validate_with_docker = false;
            }
            "--no-execute-snippet" => {
                config.execute_snippet = false;
            }
            "--no-parallel-versions" => {
                config.parallel_versions = false;
            }
            value if !value.starts_with("--") && snippet_path.is_none() => {
                snippet_path = Some(PathBuf::from(value));
            }
            flag => return Err(format!("unknown resolve flag: {flag}")),
        }
        index += 1;
    }

    if read_from_stdin && snippet_path.is_some() {
        return Err("resolve accepts either a snippet path or --stdin, not both".to_string());
    }

    if config.benchmark_context_log.is_none() {
        config.benchmark_context_log = Some(context::debug_root(&config.output_dir).join("benchmark-context.log"));
    }
    context::ensure_debug_layout(&config.output_dir).map_err(|error| error.to_string())?;

    let temporary_snippet = if read_from_stdin {
        Some(write_stdin_snippet(&config.output_dir).map_err(|error| error.to_string())?)
    } else {
        None
    };
    let snippet_path = temporary_snippet
        .clone()
        .or(snippet_path)
        .ok_or("resolve expects a snippet path or --stdin".to_string())?;
    let _ = context::append_context_log(
        config.benchmark_context_log.as_deref(),
        "apdr-resolve-command",
        &format!(
            "snippet={}\noutput_dir={}\nallow_llm={}\nvalidate_with_docker={}\npython_override={}\nrange={}\nmax_retries={}",
            snippet_path.display(),
            config.output_dir.display(),
            config.allow_llm,
            config.validate_with_docker,
            config.python_version.as_deref().unwrap_or(""),
            config.python_version_range,
            config.max_retries
        ),
    );

    let resolution = (|| -> Result<_, String> {
        let result = resolver::resolve_path(tool_root, &snippet_path, &config)
            .map_err(|error| error.to_string())?;
        let (requirements_path, report_path) = result
            .write_outputs(&config.output_dir)
            .map_err(|error| error.to_string())?;
        Ok((result, requirements_path, report_path))
    })();
    if let Some(path) = temporary_snippet {
        let _ = fs::remove_file(path);
    }
    let (result, requirements_path, report_path) = resolution?;
    print!("{}", result.summary_lines(&requirements_path, &report_path));
    if !result.validation.succeeded {
        if result.validation.status.starts_with("skipped") {
            return Ok(());
        }
        let status = if result.validation.status.is_empty() {
            "validation failed".to_string()
        } else {
            result.validation.status.clone()
        };
        if let Some(reason) = result.validation.reason.as_ref().filter(|value| !value.trim().is_empty()) {
            return Err(format!("{status}: {reason}"));
        }
        return Err(status);
    }
    Ok(())
}

fn classify_log_command(tool_root: &Path, args: &[String]) -> Result<(), String> {
    let log_path = args
        .first()
        .ok_or("classify-log expects a path to a log file")?;
    let contents = fs::read_to_string(log_path).map_err(|error| error.to_string())?;
    let store = CacheStore::load(tool_root, tool_root.join(".apdr-cache"))
        .map_err(|error| error.to_string())?;
    let result = classifier::classify_log(&contents, &store);
    println!("ERROR_TYPE={}", result.error_type);
    println!("CONFLICT_CLASS={}", result.conflict_class);
    println!("MATCHED_PATTERN={}", result.matched_pattern);
    println!("RECOMMENDED_FIX={}", result.recommended_fix);
    Ok(())
}

fn cache_command(tool_root: &Path, args: &[String]) -> Result<(), String> {
    let subcommand = args.first().map(|value| value.as_str()).unwrap_or("stats");
    match subcommand {
        "stats" => {
            let store = CacheStore::load(tool_root, tool_root.join(".apdr-cache"))
                .map_err(|error| error.to_string())?;
            let stats = store.stats();
            println!("IMPORT_MAPPINGS={}", stats.import_mappings);
            println!("FAILURE_PATTERNS={}", stats.failure_patterns);
            println!("VERSION_CONSTRAINTS={}", stats.version_constraints);
            println!("LOCKFILE_ENTRIES={}", stats.lockfile_entries);
            println!("BUILD_ARTIFACTS={}", stats.build_artifacts);
            println!("PYPI_INDEX_ENTRIES={}", stats.pypi_index_entries);
            println!(
                "DEPENDENCY_GRAPH_ENTRIES={}",
                stats.dependency_graph_entries
            );
            Ok(())
        }
        "warm" => cache_warm_command(tool_root, &args[1..]),
        _ => Err("cache supports `stats` and `warm`".to_string()),
    }
}

fn cache_warm_command(tool_root: &Path, args: &[String]) -> Result<(), String> {
    let mut top_packages = 0usize;
    let mut high_centrality = 0usize;
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--top-packages" => {
                index += 1;
                let value = args.get(index).ok_or("--top-packages expects a value")?;
                top_packages = value
                    .parse::<usize>()
                    .map_err(|_| "--top-packages must be an integer".to_string())?;
            }
            "--high-centrality" => {
                index += 1;
                let value = args.get(index).ok_or("--high-centrality expects a value")?;
                high_centrality = value
                    .parse::<usize>()
                    .map_err(|_| "--high-centrality must be an integer".to_string())?;
            }
            flag => return Err(format!("unknown cache warm flag: {flag}")),
        }
        index += 1;
    }

    let mut store = CacheStore::load(tool_root, tool_root.join(".apdr-cache"))
        .map_err(|error| error.to_string())?;
    let mut warmed = 0usize;
    let mut warmed_packages = BTreeSet::new();

    if top_packages > 0 {
        for package in
            top_ranked_packages(tool_root, top_packages).map_err(|error| error.to_string())?
        {
            if !warmed_packages.insert(package.clone()) {
                continue;
            }
            let versions = resolver::pypi_client::compatible_versions(&mut store, &package, "3.11");
            if !versions.is_empty() {
                warmed += 1;
            }
        }
    }

    if high_centrality > 0 {
        for package in high_centrality_packages(tool_root, &store, high_centrality)
            .map_err(|error| error.to_string())?
        {
            if !warmed_packages.insert(package.clone()) {
                continue;
            }
            let versions = resolver::pypi_client::compatible_versions(&mut store, &package, "3.11");
            if !versions.is_empty() {
                warmed += 1;
            }
        }
    }

    println!("WARMED_ENTRIES={warmed}");
    Ok(())
}

fn print_help() {
    println!("APDR");
    println!();
    println!("Usage:");
    println!("  apdr resolve <snippet.py>|--stdin [--output DIR] [--python 3.11] [--range 1] [--max-retries 10]");
    println!("              [--cache-path DIR] [--allow-llm --llm-provider ollama --llm-model gemma3:4b]");
    println!("              [--llm-base-url http://localhost:11434] [--benchmark-context-log trace.log]");
    println!("              [--docker-timeout 300] [--no-validate]");
    println!("              [--no-execute-snippet]");
    println!("              [--no-parallel-versions] [--no-config-scan]");
    println!("  apdr classify-log <build.log>");
    println!("  apdr cache stats");
    println!("  apdr cache warm --top-packages 5000");
    println!("  apdr cache warm --high-centrality 50");
}

fn write_stdin_snippet(output_dir: &Path) -> Result<PathBuf, std::io::Error> {
    fs::create_dir_all(output_dir)?;
    let unique = format!(
        "stdin-{}-{}.py",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let path = output_dir.join(unique);
    let mut buffer = String::new();
    std::io::stdin().read_to_string(&mut buffer)?;
    fs::write(&path, buffer)?;
    Ok(path)
}

fn top_ranked_packages(tool_root: &Path, limit: usize) -> Result<Vec<String>, std::io::Error> {
    read_seed_packages(&tool_root.join("data/seed/top_5000_mappings.tsv"), 1, limit)
}

fn high_centrality_packages(
    tool_root: &Path,
    store: &CacheStore,
    limit: usize,
) -> Result<Vec<String>, std::io::Error> {
    let seed_path = tool_root.join("data/seed/high_centrality_packages.tsv");
    if seed_path.exists() {
        let seeded = read_seed_packages(&seed_path, 0, limit)?;
        if !seeded.is_empty() {
            return Ok(seeded);
        }
    }

    let mut packages = store
        .dependency_graph
        .iter()
        .map(|(package, deps)| (package.clone(), deps.len()))
        .collect::<Vec<_>>();
    packages.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    Ok(packages
        .into_iter()
        .take(limit)
        .map(|(package, _)| package)
        .collect())
}

fn read_seed_packages(
    path: &Path,
    column: usize,
    limit: usize,
) -> Result<Vec<String>, std::io::Error> {
    let contents = fs::read_to_string(path)?;
    let mut seen = BTreeSet::new();
    let mut packages = Vec::new();

    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let parts = trimmed.split('\t').collect::<Vec<_>>();
        let Some(value) = parts.get(column) else {
            continue;
        };
        let package = value.trim().to_lowercase();
        if package.is_empty() || !seen.insert(package.clone()) {
            continue;
        }
        packages.push(package);
        if packages.len() >= limit {
            break;
        }
    }

    Ok(packages)
}
