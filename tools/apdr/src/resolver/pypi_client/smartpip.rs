use super::host_python::{host_python_command, run_host_python};
use crate::cache::store::{normalize, CacheStore};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
static SMARTPIP_CONNECTION: Mutex<Option<TcpStream>> = Mutex::new(None);
static SMARTPIP_SERVER_LAUNCHING: AtomicBool = AtomicBool::new(false);
static SMARTPIP_SERVER_UNAVAILABLE: AtomicBool = AtomicBool::new(false);
pub(super) fn fetch_versions_from_smtpip(store: &mut CacheStore, package_name: &str) -> Vec<String> {
    if let Some(versions) = try_smartpip_tcp_versions(store, package_name) {
        if !versions.is_empty() {
            let _ = store.save_pypi_versions(package_name, &versions);
            return versions;
        }
    }
    let Some(kgraph_path) = smtpip_kgraph_path(store) else {
        return Vec::new();
    };
    let kgraph_path_text = kgraph_path.display().to_string();
    let db_path_text = smtpip_db_path(store).display().to_string();
    let Some(output) = run_host_python(&[
        "-c",
        SMTPIP_KGRAPH_SCRIPT,
        "versions",
        kgraph_path_text.as_str(),
        db_path_text.as_str(),
        package_name,
    ]) else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let versions = stdout
        .trim()
        .split(',')
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if !versions.is_empty() {
        let _ = store.save_pypi_versions(package_name, &versions);
    }
    versions
}
pub(super) fn smtpip_kgraph_path(store: &CacheStore) -> Option<PathBuf> {
    let candidates = [
        store.tool_root.join("../../SMTpip/KGraph.zip"),
        store.tool_root.join("../../SMTpip/KGraph.json"),
        store.tool_root.join("../SMTpip/KGraph.zip"),
        store.tool_root.join("../SMTpip/KGraph.json"),
    ];
    candidates
        .into_iter()
        .map(|path| path.canonicalize().unwrap_or(path))
        .find(|path| path.exists())
}
fn try_smartpip_tcp_versions(store: &CacheStore, package_name: &str) -> Option<Vec<String>> {
    let mut conn_guard = SMARTPIP_CONNECTION.lock().ok()?;
    if conn_guard.is_none() {
        match connect_smartpip_stream() {
            Ok(stream) => {
                *conn_guard = Some(stream);
            }
            Err(_) => {
                if !SMARTPIP_SERVER_UNAVAILABLE.load(AtomicOrdering::SeqCst) {
                    ensure_smartpip_tcp_server(store);
                }
                match connect_smartpip_stream() {
                    Ok(stream) => {
                        *conn_guard = Some(stream);
                    }
                    Err(_) => return None,
                }
            }
        }
    }
    let stream = conn_guard.as_mut()?;
    let request = format!("VERSIONS {}\n", normalize(package_name));
    if stream.write_all(request.as_bytes()).is_err() {
        *conn_guard = None;
        return None;
    }
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut response = String::new();
    if reader.read_line(&mut response).is_err() {
        *conn_guard = None;
        return None;
    }
    let versions = response
        .trim()
        .split(',')
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();
    if versions.is_empty() { None } else { Some(versions) }
}
pub(super) fn try_smartpip_tcp_deps(
    store: &CacheStore,
    package_name: &str,
    version: &str,
) -> Option<Vec<String>> {
    let mut conn_guard = SMARTPIP_CONNECTION.lock().ok()?;
    if conn_guard.is_none() {
        match connect_smartpip_stream() {
            Ok(stream) => {
                *conn_guard = Some(stream);
            }
            Err(_) => {
                if !SMARTPIP_SERVER_UNAVAILABLE.load(AtomicOrdering::SeqCst) {
                    ensure_smartpip_tcp_server(store);
                }
                match connect_smartpip_stream() {
                    Ok(stream) => {
                        *conn_guard = Some(stream);
                    }
                    Err(_) => return None,
                }
            }
        }
    }
    let stream = conn_guard.as_mut()?;
    let request = format!("DEPS {} {}\n", normalize(package_name), version);
    if stream.write_all(request.as_bytes()).is_err() {
        *conn_guard = None;
        return None;
    }
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut response = String::new();
    if reader.read_line(&mut response).is_err() {
        *conn_guard = None;
        return None;
    }
    Some(
        response
            .trim()
            .split('|')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>(),
    )
}
pub(super) fn smtpip_db_path(store: &CacheStore) -> PathBuf {
    store.cache_path.join("smtpip-kgraph.sqlite3")
}
fn smartpip_server_script_path(store: &CacheStore) -> Option<PathBuf> {
    let path = store.tool_root.join("smartpip_kgraph_server.py");
    if path.exists() {
        return Some(path);
    }
    None
}
fn smartpip_server_log_path(store: &CacheStore) -> PathBuf {
    store.cache_path.join("smartpip-kgraph-server.log")
}
pub(super) fn ensure_smartpip_tcp_server(store: &CacheStore) {
    if smartpip_server_available() {
        SMARTPIP_SERVER_UNAVAILABLE.store(false, AtomicOrdering::SeqCst);
        return;
    }
    if SMARTPIP_SERVER_LAUNCHING.swap(true, AtomicOrdering::SeqCst) {
        wait_for_smartpip_server();
        return;
    }
    let Some(python) = host_python_command() else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
        SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
        return;
    };
    let Some(graph_path) = smtpip_kgraph_path(store) else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
        SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
        return;
    };
    let Some(script_path) = smartpip_server_script_path(store) else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
        SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
        return;
    };
    let db_path = smtpip_db_path(store);
    let log_path = smartpip_server_log_path(store);
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .ok();
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .ok();
    let mut command = Command::new(python);
    command
        .arg(script_path)
        .arg(graph_path)
        .arg(db_path)
        .arg("8888");
    if let Some(handle) = stdout {
        command.stdout(handle);
    }
    if let Some(handle) = stderr {
        command.stderr(handle);
    }
    let _ = command.spawn();
    if smartpip_server_available() || wait_for_smartpip_server() {
        SMARTPIP_SERVER_UNAVAILABLE.store(false, AtomicOrdering::SeqCst);
    } else {
        SMARTPIP_SERVER_UNAVAILABLE.store(true, AtomicOrdering::SeqCst);
    }
    SMARTPIP_SERVER_LAUNCHING.store(false, AtomicOrdering::SeqCst);
}
fn smartpip_server_available() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:8888".parse().unwrap(),
        Duration::from_millis(250),
    )
    .is_ok()
}
fn wait_for_smartpip_server() -> bool {
    for _ in 0..40 {
        if smartpip_server_available() {
            return true;
        }
        thread::sleep(Duration::from_millis(250));
    }
    false
}
fn connect_smartpip_stream() -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(
        &"127.0.0.1:8888".parse().unwrap(),
        Duration::from_millis(500),
    )?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    Ok(stream)
}
const SMTPIP_KGRAPH_SCRIPT: &str = r#"
import json
import sqlite3
import sys
import zipfile
from pathlib import Path
mode = sys.argv[1]
kgraph_path = Path(sys.argv[2])
db_path = Path(sys.argv[3])
conn = sqlite3.connect(str(db_path))
cur = conn.cursor()
if mode == 'versions':
    package = sys.argv[4].replace('_', '-').lower()
    cur.execute(
        'SELECT version FROM versions WHERE normalized_name = ? ORDER BY sort_key ASC',
        (package,)
    )
    print(','.join(row[0] for row in cur.fetchall()))
elif mode == 'deps':
    package = sys.argv[4].replace('_', '-').lower()
    version = sys.argv[5]
    cur.execute(
        'SELECT dependencies_json FROM dependencies WHERE normalized_name = ? AND version = ?',
        (package, version)
    )
    row = cur.fetchone()
    if row and row[0]:
        try:
            deps = json.loads(row[0])
            if isinstance(deps, list):
                print('\n'.join(str(dep).strip() for dep in deps if str(dep).strip()))
        except Exception:
            pass
"#;
