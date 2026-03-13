use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;

pub fn load_stdlib_modules(data_root: &Path, python_version: &str) -> io::Result<BTreeSet<String>> {
    let directory = data_root.join("stdlib_modules");
    if !directory.exists() {
        return Ok(BTreeSet::new());
    }

    let preferred = directory.join(format!("{python_version}.txt"));
    if preferred.exists() {
        return load_file(&preferred);
    }

    let major_minor = python_version
        .split('.')
        .take(2)
        .collect::<Vec<_>>()
        .join(".");
    let fallback = directory.join(format!("{major_minor}.txt"));
    if fallback.exists() {
        return load_file(&fallback);
    }

    let mut modules = BTreeSet::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        modules.extend(load_file(&entry.path())?);
    }
    Ok(modules)
}

fn load_file(path: &Path) -> io::Result<BTreeSet<String>> {
    let mut modules = BTreeSet::new();
    let contents = fs::read_to_string(path)?;
    for line in contents.lines() {
        let item = line.trim();
        if item.is_empty() || item.starts_with('#') {
            continue;
        }
        modules.insert(item.to_string());
    }
    Ok(modules)
}
