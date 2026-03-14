#!/usr/bin/env python3
from __future__ import annotations

import json
import socketserver
import sqlite3
import sys
import zipfile
from pathlib import Path


def normalize(name: str) -> str:
    return name.strip().replace("_", "-").replace(".", "-").lower()


def version_key(value: str) -> list[object]:
    parts: list[object] = []
    current = ""
    for ch in value:
        if ch.isdigit():
            current += ch
            continue
        if current:
            parts.append(int(current))
            current = ""
        parts.append(ch)
    if current:
        parts.append(int(current))
    return parts


def load_graph(path: Path) -> dict[str, object]:
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            with archive.open("KGraph.json") as handle:
                return json.load(handle)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_db(graph_path: Path, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    should_rebuild = (not db_path.exists()) or db_path.stat().st_mtime < graph_path.stat().st_mtime
    conn = sqlite3.connect(db_path)
    try:
        if not should_rebuild:
            return
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS versions")
        cur.execute("DROP TABLE IF EXISTS deps")
        cur.execute("CREATE TABLE versions(package TEXT NOT NULL, version TEXT NOT NULL)")
        cur.execute("CREATE TABLE deps(package TEXT NOT NULL, version TEXT NOT NULL, spec TEXT NOT NULL)")
        cur.execute("CREATE INDEX idx_versions_package ON versions(package)")
        cur.execute("CREATE INDEX idx_deps_package_version ON deps(package, version)")
        graph = load_graph(graph_path)
        version_rows: list[tuple[str, str]] = []
        dep_rows: list[tuple[str, str, str]] = []
        for raw_name, payload in (graph.get("projects", {}) or {}).items():
            package_name = normalize(str(raw_name))
            for raw_version, meta in (payload or {}).items():
                version_text = str(raw_version).strip()
                version_rows.append((package_name, version_text))
                for spec in ((meta or {}).get("dependency_packages") or []):
                    spec_text = str(spec).strip()
                    if spec_text:
                        dep_rows.append((package_name, version_text, spec_text))
        cur.executemany("INSERT INTO versions(package, version) VALUES (?, ?)", version_rows)
        cur.executemany("INSERT INTO deps(package, version, spec) VALUES (?, ?, ?)", dep_rows)
        conn.commit()
    finally:
        conn.close()


class KGraphTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class KGraphHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        line = self.rfile.readline().decode("utf-8", errors="replace").strip()
        if not line:
            return
        parts = line.split()
        command = parts[0].upper()
        if command == "VERSIONS" and len(parts) >= 2:
            self._handle_versions(parts[1])
            return
        if command == "DEPS" and len(parts) >= 3:
            self._handle_deps(parts[1], parts[2])
            return
        self.wfile.write(b"\n")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.server.db_path)

    def _handle_versions(self, package_name: str) -> None:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT version FROM versions WHERE package = ?",
                (normalize(package_name),),
            ).fetchall()
        finally:
            conn.close()
        versions = sorted({row[0] for row in rows}, key=version_key)
        self.wfile.write((",".join(versions) + "\n").encode("utf-8"))

    def _handle_deps(self, package_name: str, version: str) -> None:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT spec FROM deps WHERE package = ? AND version = ?",
                (normalize(package_name), version),
            ).fetchall()
        finally:
            conn.close()
        specs = [str(row[0]).strip() for row in rows if str(row[0]).strip()]
        self.wfile.write(("|".join(specs) + "\n").encode("utf-8"))


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: smartpip_kgraph_server.py <kgraph-path> <sqlite-db-path> [port]", file=sys.stderr)
        return 2
    graph_path = Path(sys.argv[1]).expanduser().resolve()
    db_path = Path(sys.argv[2]).expanduser().resolve()
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 8888
    ensure_db(graph_path, db_path)
    with KGraphTCPServer(("127.0.0.1", port), KGraphHandler) as server:
        server.db_path = str(db_path)
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
