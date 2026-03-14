from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
import json
import os
import platform
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request

from . import APP_NAME, APP_VERSION


DEFAULT_BASE_URL = "http://localhost:11434"
DEFAULT_MODEL = "phi3:medium"
DEFAULT_TEMPERATURE = 0.7
APDR_PYTHON_VERSIONS = ["2.7", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]
APDR_PYTHON_INSTALL_CANDIDATES: dict[str, list[str]] = {
    "2.7": ["2.7.18"],
    "3.7": ["3.7.17", "3.7.16"],
    "3.8": ["3.8.20", "3.8.19", "3.8.18"],
    "3.9": ["3.9.21", "3.9.20", "3.9.19"],
    "3.10": ["3.10.16", "3.10.15", "3.10.14"],
    "3.11": ["3.11.11", "3.11.10", "3.11.9"],
    "3.12": ["3.12.9", "3.12.8", "3.12.7"],
}


@dataclass
class ModelConfig:
    tool: str
    model: str = DEFAULT_MODEL
    base_url: str = DEFAULT_BASE_URL
    temperature: float = DEFAULT_TEMPERATURE
    updated_at: str = ""


class AppState:
    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = (repo_root or Path(__file__).resolve().parents[1]).resolve()
        self.tools_dir = self.repo_root / "tools"
        self.models_dir = self.repo_root / "models"
        self.loadouts_dir = self.repo_root / "loadouts"
        self.runs_dir = self.repo_root / "runs"
        self.default_dataset_tar = self.repo_root / "hard-gists.tar.gz"
        self.ensure_directories()

    def ensure_directories(self) -> None:
        for directory in (self.models_dir, self.loadouts_dir, self.runs_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def now_iso(self) -> str:
        return datetime.now().replace(microsecond=0).isoformat()

    def version_display(self) -> str:
        revision = self.git_revision()
        if revision:
            return f"{APP_NAME} {APP_VERSION} ({revision})"
        return f"{APP_NAME} {APP_VERSION}"

    def git_revision(self) -> str:
        code, output = self._run_command(["git", "rev-parse", "--short", "HEAD"], cwd=self.repo_root, timeout=5)
        if code == 0 and output:
            return output.splitlines()[0].strip()
        return ""

    def discover_tools(self) -> list[str]:
        if not self.tools_dir.exists():
            return []
        tools: list[str] = []
        for child in sorted(self.tools_dir.iterdir()):
            if child.is_dir() and (child / "test_executor.py").exists():
                tools.append(child.name)
        return tools

    def tool_dir(self, tool: str) -> Path:
        return self.tools_dir / tool

    def model_config_path(self, tool: str) -> Path:
        return self.models_dir / f"{tool}.json"

    def load_model_config(self, tool: str) -> ModelConfig:
        data = self.read_json(self.model_config_path(tool))
        if not data:
            cached_models = self.get_cached_models(DEFAULT_BASE_URL)
            fallback_model = cached_models[0] if cached_models else DEFAULT_MODEL
            return ModelConfig(tool=tool, model=fallback_model)
        base_url = self.normalize_base_url(str(data.get("base_url") or DEFAULT_BASE_URL))
        cached_models = self.get_cached_models(base_url)
        fallback_model = cached_models[0] if cached_models else DEFAULT_MODEL
        return ModelConfig(
            tool=tool,
            model=str(data.get("model") or fallback_model),
            base_url=base_url,
            temperature=self._safe_float(data.get("temperature"), DEFAULT_TEMPERATURE),
            updated_at=str(data.get("updated_at") or ""),
        )

    def save_model_config(self, config: ModelConfig) -> Path:
        payload = asdict(config)
        payload["base_url"] = self.normalize_base_url(payload["base_url"])
        payload["updated_at"] = self.now_iso()
        path = self.model_config_path(config.tool)
        self.write_json(path, payload)
        return path

    def load_all_model_configs(self) -> dict[str, ModelConfig]:
        return {tool: self.load_model_config(tool) for tool in self.discover_tools()}

    def catalog_path(self) -> Path:
        return self.models_dir / "catalog.json"

    def load_model_catalog(self) -> dict[str, Any]:
        data = self.read_json(self.catalog_path())
        if isinstance(data, dict) and isinstance(data.get("entries"), dict):
            return data
        return {"entries": {}}

    def store_model_catalog(self, base_url: str, models: list[str], source: str) -> None:
        catalog = self.load_model_catalog()
        normalized = self.normalize_base_url(base_url)
        catalog["entries"][normalized] = {
            "models": sorted(dict.fromkeys(models)),
            "source": source,
            "updated_at": self.now_iso(),
        }
        self.write_json(self.catalog_path(), catalog)

    def get_cached_models(self, base_url: str) -> list[str]:
        catalog = self.load_model_catalog()
        normalized = self.normalize_base_url(base_url)
        entry = catalog["entries"].get(normalized, {})
        models = entry.get("models", [])
        if isinstance(models, list):
            return [str(model) for model in models if str(model).strip()]
        return []

    def discover_ollama_models(self, base_url: str) -> tuple[list[str], str, str]:
        normalized = self.normalize_base_url(base_url)
        api_error = ""
        try:
            with urllib.request.urlopen(f"{normalized}/api/tags", timeout=4) as response:
                payload = json.loads(response.read().decode("utf-8"))
            models = sorted(
                {
                    str(item.get("name")).strip()
                    for item in payload.get("models", [])
                    if item.get("name")
                }
            )
            if models:
                self.store_model_catalog(normalized, models, "api")
                return models, "api", ""
            api_error = "Ollama API responded without any models."
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            api_error = str(exc)

        if self._is_local_base_url(normalized) and shutil.which("ollama"):
            code, output = self._run_command(["ollama", "list"], cwd=self.repo_root, timeout=6)
            if code == 0 and output:
                models = self._parse_ollama_list(output)
                if models:
                    self.store_model_catalog(normalized, models, "cli")
                    return models, "cli", ""
            if not api_error:
                api_error = output or "Unable to read local Ollama models."

        cached = self.get_cached_models(normalized)
        if cached:
            return cached, "cache", api_error or "Using cached Ollama model list."
        return [], "", api_error or "Unable to reach Ollama."

    def default_run_config(self, preferred_tool: str | None = None) -> dict[str, Any]:
        tools = self.discover_tools()
        tool = preferred_tool or (tools[0] if tools else "")
        return {
            "tool": tool,
            "dataset_tar": str(self.default_dataset_tar),
            "loop_count": 5,
            "search_range": 5,
            "rag": True,
            "verbose": False,
            "snippet_limit": "",
            "python_command": "",
        }

    def load_loadouts(self) -> list[dict[str, Any]]:
        loadouts: list[dict[str, Any]] = []
        for path in sorted(self.loadouts_dir.glob("*.json")):
            data = self.read_json(path)
            if not isinstance(data, dict):
                continue
            data.setdefault("name", path.stem)
            data.setdefault("updated_at", "")
            data["slug"] = path.stem
            loadouts.append(data)
        return sorted(loadouts, key=lambda item: item["name"].lower())

    def save_loadout(self, name: str, data: dict[str, Any]) -> Path:
        slug = self.slugify(name)
        payload = self.default_run_config(preferred_tool=str(data.get("tool") or ""))
        payload.update(data)
        payload["name"] = name.strip()
        payload["updated_at"] = self.now_iso()
        path = self.loadouts_dir / f"{slug}.json"
        self.write_json(path, payload)
        return path

    def load_loadout(self, slug: str) -> dict[str, Any] | None:
        path = self.loadouts_dir / f"{slug}.json"
        data = self.read_json(path)
        if isinstance(data, dict):
            data.setdefault("name", slug)
            data.setdefault("slug", slug)
            return data
        return None

    def delete_loadout(self, slug: str) -> bool:
        path = self.loadouts_dir / f"{slug}.json"
        if path.exists():
            path.unlink()
            return True
        return False

    def run_summary_path(self, run_id: str) -> Path:
        return self.runs_dir / run_id / "summary.json"

    def load_run_summary(self, run_id: str) -> dict[str, Any] | None:
        data = self.read_json(self.run_summary_path(run_id))
        return data if isinstance(data, dict) else None

    def list_run_summaries(self) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        for directory in sorted(self.runs_dir.iterdir(), key=lambda item: item.name, reverse=True):
            if not directory.is_dir():
                continue
            summary_path = directory / "summary.json"
            data = self.read_json(summary_path)
            if not isinstance(data, dict):
                continue
            runs.append(
                {
                    "run_id": directory.name,
                    "run_dir": directory,
                    "summary_path": summary_path,
                    "summary": data,
                }
            )
        return runs

    def slugify(self, text: str) -> str:
        cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in text.strip())
        collapsed = "-".join(part for part in cleaned.split("-") if part)
        return collapsed or "loadout"

    def normalize_base_url(self, base_url: str) -> str:
        cleaned = (base_url or DEFAULT_BASE_URL).strip()
        if not cleaned:
            return DEFAULT_BASE_URL
        if "://" not in cleaned:
            cleaned = f"http://{cleaned}"
        return cleaned.rstrip("/")

    def choose_runner(self, tool: str, python_command: str = "") -> list[str]:
        if python_command.strip():
            return shlex.split(python_command.strip())
        tool_dir = self.tool_dir(tool)
        venv_candidates = [
            tool_dir / ".venv" / "bin" / "python",
            tool_dir / "venv" / "bin" / "python",
            tool_dir / ".venv" / "Scripts" / "python.exe",
            tool_dir / "venv" / "Scripts" / "python.exe",
        ]
        for candidate in venv_candidates:
            if candidate.exists():
                return [str(candidate)]
        if tool == "apdr":
            return [sys.executable]
        pipenv_command = self.find_pipenv_command()
        if pipenv_command and (tool_dir / "Pipfile").exists():
            return pipenv_command + ["run", "python"]
        return [sys.executable]

    def find_pipenv_command(self) -> list[str]:
        if shutil.which("pipenv"):
            return ["pipenv"]
        code, _output = self._run_command([sys.executable, "-m", "pipenv", "--version"], cwd=self.repo_root, timeout=10)
        if code == 0:
            return [sys.executable, "-m", "pipenv"]
        candidates = [
            Path.home() / ".local" / "bin" / "pipenv",
            Path.home() / "Library" / "Python" / f"{sys.version_info.major}.{sys.version_info.minor}" / "bin" / "pipenv",
            Path(sys.executable).resolve().parent / "Scripts" / "pipenv.exe",
            Path.home()
            / "AppData"
            / "Roaming"
            / "Python"
            / f"Python{sys.version_info.major}{sys.version_info.minor}"
            / "Scripts"
            / "pipenv.exe",
        ]
        for candidate in candidates:
            if candidate.exists():
                return [str(candidate)]
        return []

    def tool_runtime_imports(self, tool: str) -> list[str]:
        if tool == "pllm":
            return [
                "docker",
                "ollama",
                "requests",
                "yaml",
                "langchain_community",
                "langchain_openai",
                "transformers",
                "jq",
                "jsonschema",
            ]
        if tool == "apdr":
            return []
        return ["docker", "requests"]

    def validate_tool_runtime(self, tool: str, python_command: str = "") -> tuple[bool, str, list[str]]:
        runner = self.choose_runner(tool, python_command)
        tool_dir = self.tool_dir(tool)
        if tool == "apdr":
            binary_candidates = [
                tool_dir / "target" / "release" / "apdr",
                tool_dir / "target" / "debug" / "apdr",
            ]
            built_binary = next((candidate for candidate in binary_candidates if candidate.exists()), None)
            code, output = self._run_command(
                runner + ["-c", "import argparse, subprocess, sys; print('runtime-ok')"],
                cwd=tool_dir,
                timeout=20,
            )
            if code != 0:
                detail = output or "Unable to run the Python wrapper for APDR."
                return False, detail, runner
            available, missing = self.apdr_local_interpreters()
            interpreter_detail = self.format_apdr_interpreter_detail(available, missing)
            if built_binary:
                if not available:
                    return False, f"APDR binary is ready at {self.relative_path(built_binary)}, but no local validation interpreters were found. {interpreter_detail}", runner
                return True, f"APDR binary ready at {self.relative_path(built_binary)}. {interpreter_detail}", runner
            if shutil.which("cargo"):
                if not available:
                    return False, f"APDR wrapper is usable and Cargo is available to build/run the Rust CLI, but no local validation interpreters were found. {interpreter_detail}", runner
                return True, f"APDR wrapper is usable and Cargo is available to build/run the Rust CLI. {interpreter_detail}", runner
            return (
                False,
                "APDR needs either a built binary in tools/apdr/target or Cargo on PATH. Build it with `cargo build --release` in tools/apdr.",
                runner,
            )

        imports = self.tool_runtime_imports(tool)
        import_statement = ", ".join(imports)
        code, output = self._run_command(
            runner + ["-c", f"import {import_statement}; print('runtime-ok')"],
            cwd=tool_dir,
            timeout=20,
        )
        detail = output or "runtime-ok"
        if code != 0:
            missing_module = self._extract_missing_module_name(detail)
            if tool == "pllm":
                if not python_command.strip() and not self._has_tool_env(tool):
                    detail = (
                        f"{self.format_command(runner)} is being used because no local virtualenv or pipenv was found. "
                        f"Missing Python package: {missing_module or 'unknown'}. "
                        "Create the PLLM environment with `pipenv install` in tools/pllm, "
                        "or set Python command override in the UI."
                    )
                elif missing_module:
                    detail = (
                        f"{self.format_command(runner)} is missing Python package `{missing_module}`. "
                        "Install the PLLM dependencies into that interpreter."
                    )
        return code == 0, detail, runner

    def format_command(self, args: list[str]) -> str:
        return shlex.join(args)

    def dataset_root_from_archive(self, archive_path: str | Path) -> Path:
        archive = Path(archive_path)
        with tarfile.open(archive, "r:gz") as handle:
            for member in handle.getmembers():
                name = member.name.strip("/")
                if name:
                    top_level = name.split("/", 1)[0]
                    if self._is_metadata_archive_path(top_level):
                        continue
                    return self.repo_root / top_level
        return self.repo_root / archive.stem.replace(".tar", "")

    def ensure_dataset_extracted(self, archive_path: str | Path) -> Path:
        archive = Path(archive_path)
        if not archive.exists():
            raise FileNotFoundError(f"Dataset archive not found: {archive}")
        dataset_root = self.dataset_root_from_archive(archive)
        if dataset_root.exists() and self.count_snippets(dataset_root) > 0:
            return dataset_root

        with tarfile.open(archive, "r:gz") as handle:
            self._safe_extract(handle, self.repo_root)
        return dataset_root

    def count_snippets(self, dataset_dir: str | Path) -> int:
        path = Path(dataset_dir)
        if not path.exists():
            return 0
        return len(self.snippet_files(path))

    def snippet_files(self, dataset_dir: str | Path) -> list[Path]:
        path = Path(dataset_dir)
        if not path.exists():
            return []
        return sorted(
            snippet
            for snippet in path.rglob("snippet.py")
            if not self._is_ignored_snippet_path(snippet)
        )

    def _is_ignored_snippet_path(self, path: Path) -> bool:
        ignored_dirs = {
            "__pycache__",
            "__MACOSX",
            ".apdr-docker",
            ".git",
            ".hg",
            ".svn",
            ".venv",
            "venv",
            "node_modules",
        }
        for part in path.parts[:-1]:
            if part in ignored_dirs or part.startswith("."):
                return True
        return False

    def relative_path(self, path: str | Path) -> str:
        candidate = Path(path).resolve()
        try:
            return str(candidate.relative_to(self.repo_root))
        except ValueError:
            return str(candidate)

    def doctor_checks(self, selected_tool: str = "", base_url: str = "", python_command: str = "") -> list[dict[str, str]]:
        checks: list[dict[str, str]] = []
        tools = self.discover_tools()
        resolved_tool = selected_tool or (tools[0] if tools else "")

        checks.append(self._doctor_row("PASS", "Repository", str(self.repo_root)))
        checks.append(
            self._doctor_row(
                "PASS" if tools else "FAIL",
                "Tool entry points",
                ", ".join(tools) if tools else "No `test_executor.py` files found under tools/.",
            )
        )

        if self.default_dataset_tar.exists():
            checks.append(self._doctor_row("PASS", "Dataset archive", self.relative_path(self.default_dataset_tar)))
        else:
            checks.append(self._doctor_row("FAIL", "Dataset archive", "Missing hard-gists.tar.gz at the repo root."))

        dataset_root = self.repo_root / "hard-gists"
        snippet_count = self.count_snippets(dataset_root)
        if snippet_count:
            detail = f"{snippet_count} snippets ready in {self.relative_path(dataset_root)}"
            checks.append(self._doctor_row("PASS", "Extracted dataset", detail))
        else:
            checks.append(self._doctor_row("WARN", "Extracted dataset", "Dataset will be extracted on first benchmark run."))

        configured_base_url = self.load_model_config(resolved_tool).base_url if resolved_tool else DEFAULT_BASE_URL
        normalized_base = self.normalize_base_url(base_url or configured_base_url)
        models, source, error = self.discover_ollama_models(normalized_base)
        if models:
            detail = f"{len(models)} models discovered from {normalized_base} via {source}."
            checks.append(self._doctor_row("PASS", "Ollama models", detail))
        else:
            checks.append(self._doctor_row("WARN", "Ollama models", error or f"No models available from {normalized_base}."))

        if shutil.which("ollama"):
            code, output = self._run_command(["ollama", "--version"], cwd=self.repo_root, timeout=5)
            status = "PASS" if code == 0 else "WARN"
            checks.append(self._doctor_row(status, "Ollama CLI", output or "Unable to read ollama version."))
        else:
            checks.append(self._doctor_row("WARN", "Ollama CLI", "The `ollama` command is not on PATH."))

        docker_optional = resolved_tool == "apdr"
        if shutil.which("docker"):
            code, output = self._run_command(["docker", "--version"], cwd=self.repo_root, timeout=5)
            docker_cli_label = "Docker CLI (PLLM only)" if docker_optional else "Docker CLI"
            docker_cli_status = "PASS" if code == 0 else ("WARN" if docker_optional else "FAIL")
            docker_cli_detail = output or (
                "Docker is installed, but APDR does not require it."
                if docker_optional
                else "Unable to read docker version."
            )
            checks.append(self._doctor_row(docker_cli_status, docker_cli_label, docker_cli_detail))
            code, output = self._run_command(["docker", "info", "--format", "{{.ServerVersion}}"], cwd=self.repo_root, timeout=8)
            detail = output or "Unable to talk to the Docker daemon."
            if code != 0 and not docker_optional:
                detail = f"{detail} Start Docker Desktop or another local Docker daemon, then rerun Doctor."
            elif code != 0 and docker_optional:
                detail = f"{detail} APDR does not require Docker, but PLLM still does."
            checks.append(
                self._doctor_row(
                    "PASS" if code == 0 else ("WARN" if docker_optional else "FAIL"),
                    "Docker daemon (PLLM only)" if docker_optional else "Docker daemon",
                    detail,
                )
            )
        else:
            if docker_optional:
                checks.append(
                    self._doctor_row(
                        "WARN",
                        "Docker (PLLM only)",
                        "Docker is not installed. APDR no longer requires Docker, but PLLM still does.",
                    )
                )
            else:
                checks.append(self._doctor_row("FAIL", "Docker CLI", "The `docker` command is not on PATH."))
                checks.append(self._doctor_row("FAIL", "Docker daemon", "Skipped because the Docker CLI is missing."))

        for tool in tools:
            is_valid, output, runner = self.validate_tool_runtime(
                tool, python_command if tool == resolved_tool else ""
            )
            detail = f"{self.format_command(runner)} -> {output}"
            checks.append(self._doctor_row("PASS" if is_valid else "WARN", f"{tool} runtime", detail))
            if tool == "apdr":
                available, missing = self.apdr_local_interpreters()
                interpreter_status = "FAIL" if not available else ("WARN" if missing else "PASS")
                checks.append(
                    self._doctor_row(
                        interpreter_status,
                        "apdr interpreters",
                        self.format_apdr_interpreter_detail(available, missing),
                    )
                )
                venv_ok, venv_detail = self.apdr_env_tooling_available(available)
                checks.append(
                    self._doctor_row(
                        "PASS" if venv_ok else "WARN",
                        "apdr env tooling",
                        venv_detail,
                    )
                )
                kgraph_server_up = self.apdr_kgraph_server_available()
                checks.append(
                    self._doctor_row(
                        "PASS" if kgraph_server_up else "WARN",
                        "apdr KGraph server",
                        (
                            "smartPip-compatible KGraph server is listening on 127.0.0.1:8888."
                            if kgraph_server_up
                            else "KGraph server is not running yet. APDR can auto-start it on port 8888 from SMTpip/KGraph.zip."
                        ),
                    )
                )

        return checks

    def auto_fix_doctor_issues(
        self,
        selected_tool: str = "",
        python_command: str = "",
        logger: Any | None = None,
    ) -> list[dict[str, str]]:
        log = logger or (lambda _message: None)
        tools = self.discover_tools()
        log("Starting automatic setup checks.")
        resolved_tool = selected_tool or (tools[0] if tools else "")
        if resolved_tool != "apdr":
            self._auto_start_docker_if_needed(log)
        else:
            log("Skipping Docker auto-start because APDR now validates with local Python interpreters.")

        if "apdr" in tools:
            self._auto_fix_apdr(log)
            started, detail = self.ensure_apdr_kgraph_server()
            if started:
                log(f"APDR KGraph server: {detail}")
            else:
                log(f"APDR KGraph server: {detail}")
            available, missing = self.apdr_local_interpreters()
            log(f"APDR interpreter availability: {self.format_apdr_interpreter_detail(available, missing)}")

        if "pllm" in tools and resolved_tool != "apdr":
            self._auto_fix_pllm(selected_tool, python_command, log)

        base_url = self.load_model_config(resolved_tool).base_url if resolved_tool else DEFAULT_BASE_URL
        log("Re-running Doctor after setup changes.")
        return self.doctor_checks(selected_tool, base_url, python_command)

    def read_json(self, path: Path) -> Any:
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, json.JSONDecodeError):
            return None

    def write_json(self, path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def _doctor_row(self, status: str, label: str, detail: str) -> dict[str, str]:
        return {"status": status, "label": label, "detail": detail}

    def _parse_ollama_list(self, output: str) -> list[str]:
        models: list[str] = []
        for line in output.splitlines():
            line = line.strip()
            if not line or line.startswith("NAME "):
                continue
            model = line.split()[0]
            if model:
                models.append(model)
        return sorted(dict.fromkeys(models))

    def _is_local_base_url(self, base_url: str) -> bool:
        parsed = urllib.parse.urlparse(base_url)
        return parsed.hostname in {"localhost", "127.0.0.1", None}

    def _safe_extract(self, archive: tarfile.TarFile, destination: Path) -> None:
        root = destination.resolve()
        safe_members = []
        for member in archive.getmembers():
            if self._is_metadata_archive_path(member.name):
                continue
            member_path = (destination / member.name).resolve()
            try:
                member_path.relative_to(root)
            except ValueError:
                raise ValueError(f"Unsafe archive member: {member.name}")
            safe_members.append(member)
        archive.extractall(destination, members=safe_members)

    def _run_command(self, args: list[str], cwd: Path, timeout: int) -> tuple[int, str]:
        try:
            completed = subprocess.run(
                args,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
            return 1, str(exc)
        output = (completed.stdout or completed.stderr or "").strip()
        return completed.returncode, output

    def _safe_float(self, value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _has_tool_env(self, tool: str) -> bool:
        tool_dir = self.tool_dir(tool)
        env_candidates = [
            tool_dir / ".venv" / "bin" / "python",
            tool_dir / "venv" / "bin" / "python",
            tool_dir / ".venv" / "Scripts" / "python.exe",
            tool_dir / "venv" / "Scripts" / "python.exe",
        ]
        return any(candidate.exists() for candidate in env_candidates) or (
            bool(self.find_pipenv_command()) and (tool_dir / "Pipfile").exists()
        )

    def _extract_missing_module_name(self, detail: str) -> str:
        pattern = "No module named '"
        if pattern not in detail:
            return ""
        return detail.split(pattern, 1)[1].split("'", 1)[0]

    def apdr_kgraph_server_available(self) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", 8888), timeout=0.3):
                return True
        except OSError:
            return False

    def _apdr_kgraph_path(self) -> Path | None:
        candidates = [
            self.repo_root / "SMTpip" / "KGraph.zip",
            self.repo_root / "SMTpip" / "KGraph.json",
        ]
        for path in candidates:
            if path.exists():
                return path
        return None

    def _apdr_kgraph_server_script(self) -> Path:
        return self.repo_root / "tools" / "apdr" / "smartpip_kgraph_server.py"

    def _apdr_kgraph_db_path(self) -> Path:
        return self.repo_root / "tools" / "apdr" / ".apdr-cache" / "smtpip-kgraph.sqlite3"

    def _apdr_kgraph_log_path(self) -> Path:
        return self.repo_root / "tools" / "apdr" / ".apdr-cache" / "smartpip-kgraph-server.log"

    def _python3_command(self) -> list[str]:
        candidates = [Path(sys.executable)]
        for name in ("python3", "python"):
            resolved = shutil.which(name)
            if resolved:
                candidates.append(Path(resolved))
        seen: set[str] = set()
        for candidate in candidates:
            key = str(candidate)
            if not key or key in seen:
                continue
            seen.add(key)
            code, output = self._run_command(
                [str(candidate), "-c", "import sys; print(sys.version_info[0])"],
                cwd=self.repo_root,
                timeout=10,
            )
            if code == 0 and output.strip() == "3":
                return [str(candidate)]
        return []

    def ensure_apdr_kgraph_server(self) -> tuple[bool, str]:
        if self.apdr_kgraph_server_available():
            return True, "smartPip-compatible KGraph server is already listening on 127.0.0.1:8888."
        graph_path = self._apdr_kgraph_path()
        if not graph_path:
            return False, "Missing SMTpip/KGraph.zip (or KGraph.json), so the APDR KGraph server cannot start."
        script_path = self._apdr_kgraph_server_script()
        if not script_path.exists():
            return False, f"Missing APDR KGraph server launcher at {self.relative_path(script_path)}."
        python = self._python3_command()
        if not python:
            return False, "No Python 3 interpreter is available to launch the APDR KGraph server."
        log_path = self._apdr_kgraph_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            try:
                subprocess.Popen(
                    python + [str(script_path), str(graph_path), str(self._apdr_kgraph_db_path()), "8888"],
                    cwd=self.repo_root,
                    stdout=handle,
                    stderr=handle,
                    start_new_session=not self._is_windows(),
                )
            except OSError as exc:
                return False, f"Failed to start APDR KGraph server: {exc}"
        deadline = time.time() + 12.0
        while time.time() < deadline:
            if self.apdr_kgraph_server_available():
                return True, "Started smartPip-compatible KGraph server on 127.0.0.1:8888."
            time.sleep(0.25)
        return False, f"KGraph server did not become ready on port 8888. Check {self.relative_path(log_path)}."

    def apdr_local_interpreters(self) -> tuple[dict[str, str], list[str]]:
        available: dict[str, str] = {}
        missing: list[str] = []
        for version in APDR_PYTHON_VERSIONS:
            command = self._find_python_interpreter_command(version)
            if command:
                available[version] = self.format_command(command)
            else:
                missing.append(version)
        return available, missing

    def format_apdr_interpreter_detail(self, available: dict[str, str], missing: list[str]) -> str:
        parts: list[str] = []
        if available:
            ordered = [f"{version} via {available[version]}" for version in APDR_PYTHON_VERSIONS if version in available]
            parts.append("Available: " + ", ".join(ordered))
        else:
            parts.append("Available: none")
        if missing:
            parts.append("Missing: " + ", ".join(missing))
        parts.append(
            "APDR auto-scans PATH, Python framework installs, Windows launcher-managed installs, common pyenv/asdf/mise/uv locations, and APDR-managed Miniforge envs, and the Doctor can auto-install missing interpreters with supported managers."
        )
        return " ".join(parts)

    def apdr_env_tooling_available(self, available_interpreters: dict[str, str]) -> tuple[bool, str]:
        """Check that venv (3.x) and virtualenv (2.7) are available for APDR env-based validation."""
        issues: list[str] = []
        # Check venv for each available 3.x interpreter
        for version, cmd_path in available_interpreters.items():
            if version.startswith("3."):
                interpreter = cmd_path.split()[-1] if cmd_path else f"python{version}"
                code, _ = self._run_command([interpreter, "-m", "venv", "--help"], cwd=self.repo_root, timeout=5)
                if code != 0:
                    issues.append(f"venv unavailable for Python {version}")
                break  # Only need to verify one 3.x interpreter
        # Check virtualenv for Python 2.7 support
        has_27 = "2.7" in available_interpreters
        if has_27:
            host_python = shutil.which("python3") or "python3"
            code, _ = self._run_command([host_python, "-m", "virtualenv", "--version"], cwd=self.repo_root, timeout=5)
            if code != 0:
                issues.append("virtualenv not installed for host Python 3 (needed for Python 2.7 env creation)")
        if issues:
            return False, "Issues: " + "; ".join(issues) + ". Run auto-fix to install missing tooling."
        parts = ["Validation backend: isolated local envs (venv for 3.x"]
        if has_27:
            parts.append(", virtualenv for 2.7")
        parts.append(").")
        return True, "".join(parts)

    def _apdr_python_install_specs(self, version: str) -> list[str]:
        specs = [version]
        for candidate in APDR_PYTHON_INSTALL_CANDIDATES.get(version, []):
            if candidate not in specs:
                specs.append(candidate)
        return specs

    def _apdr_miniforge_root(self) -> Path:
        return Path.home() / ".apdr" / "miniforge3"

    def _apdr_miniforge_conda(self) -> Path:
        root = self._apdr_miniforge_root()
        if self._is_windows():
            return root / "Scripts" / "conda.exe"
        return root / "bin" / "conda"

    def _apdr_miniforge_env_python(self, version: str) -> Path:
        env_root = self._apdr_miniforge_root() / "envs" / f"python-{version}"
        if self._is_windows():
            return env_root / "python.exe"
        return env_root / "bin" / "python"

    def _unix_miniforge_installer_url(self) -> str:
        machine = platform.machine().lower()
        if sys.platform == "darwin":
            suffix_map = {
                "arm64": "MacOSX-arm64",
                "aarch64": "MacOSX-arm64",
                "x86_64": "MacOSX-x86_64",
                "amd64": "MacOSX-x86_64",
            }
        elif sys.platform.startswith("linux"):
            suffix_map = {
                "x86_64": "Linux-x86_64",
                "amd64": "Linux-x86_64",
                "aarch64": "Linux-aarch64",
                "arm64": "Linux-aarch64",
                "ppc64le": "Linux-ppc64le",
            }
        else:
            suffix_map = {}
        suffix = suffix_map.get(machine, "")
        if not suffix:
            return ""
        return f"https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-{suffix}.sh"

    def _ensure_apdr_miniforge(self) -> tuple[bool, str]:
        conda_path = self._apdr_miniforge_conda()
        if conda_path.exists():
            return True, "Miniforge is already available."
        if self._is_windows():
            return False, "Automatic Miniforge bootstrap is currently only implemented for macOS and Linux."
        url = self._unix_miniforge_installer_url()
        if not url:
            return False, f"APDR does not have a Miniforge bootstrap URL for {sys.platform}/{platform.machine()}."
        download_dir = Path.home() / ".apdr" / "downloads"
        download_dir.mkdir(parents=True, exist_ok=True)
        installer_path = download_dir / Path(url).name

        # Try downloading with SSL verification first, then fall back to unverified if needed
        download_error = None
        try:
            urllib.request.urlretrieve(url, installer_path)
        except urllib.error.URLError as exc:
            download_error = exc
            # Check if it's an SSL certificate error
            if "SSL" in str(exc) or "CERTIFICATE" in str(exc):
                try:
                    # Retry with unverified SSL context (Miniforge is a trusted source)
                    ssl_context = ssl._create_unverified_context()
                    with urllib.request.urlopen(url, context=ssl_context) as response:
                        installer_path.write_bytes(response.read())
                    download_error = None  # Download succeeded with unverified context
                except Exception as retry_exc:
                    return False, f"Failed to download Miniforge installer even with unverified SSL: {retry_exc}"
            else:
                return False, f"Failed to download Miniforge installer: {exc}"
        except Exception as exc:
            return False, f"Failed to download Miniforge installer: {exc}"

        if download_error:
            return False, f"Failed to download Miniforge installer: {download_error}"

        self._apdr_miniforge_root().parent.mkdir(parents=True, exist_ok=True)
        code, output = self._run_command(
            ["bash", str(installer_path), "-b", "-p", str(self._apdr_miniforge_root())],
            cwd=self.repo_root,
            timeout=7200,
        )
        if code == 0 and conda_path.exists():
            return True, "Installed Miniforge."
        return False, self._summarize_output(output) or "Miniforge installer did not expose a usable conda executable."

    def _auto_install_apdr_python_with_miniforge(self, version: str) -> tuple[bool, str]:
        ready, detail = self._ensure_apdr_miniforge()
        if not ready:
            return False, detail

        conda_path = self._apdr_miniforge_conda()
        env_root = self._apdr_miniforge_root() / "envs" / f"python-{version}"
        env_python = self._apdr_miniforge_env_python(version)
        if env_python.exists() and self._command_matches_python_version([str(env_python)], version):
            return True, f"Installed with Miniforge ({version})."

        for spec in self._apdr_python_install_specs(version):
            if env_root.exists():
                args = [str(conda_path), "install", "-y", "-p", str(env_root), f"python={spec}"]
            else:
                args = [str(conda_path), "create", "-y", "-p", str(env_root), f"python={spec}"]
            code, output = self._run_command(args, cwd=self.repo_root, timeout=7200)
            if code == 0 and env_python.exists() and self._command_matches_python_version([str(env_python)], version):
                return True, f"Installed with Miniforge ({spec})."
            detail = self._summarize_output(output)
        return False, detail or "Miniforge finished without exposing a usable interpreter."

    def _auto_install_apdr_python(self, version: str) -> tuple[bool, str]:
        supported_managers: list[str] = []

        def already_available() -> bool:
            return bool(self._find_python_interpreter_command(version))

        if already_available():
            return True, f"Python {version} is already available."

        if not version.startswith("2.") and shutil.which("uv"):
            supported_managers.append("uv")
            code, output = self._run_command(["uv", "python", "install", version], cwd=self.repo_root, timeout=7200)
            if code == 0 and already_available():
                return True, "Installed with uv."
            if code == 0:
                return False, "uv reported success, but APDR still could not discover the interpreter."
            last_output = self._summarize_output(output)
        else:
            last_output = ""

        if shutil.which("mise"):
            supported_managers.append("mise")
            for spec in self._apdr_python_install_specs(version):
                code, output = self._run_command(["mise", "install", f"python@{spec}"], cwd=self.repo_root, timeout=7200)
                if code == 0 and already_available():
                    return True, f"Installed with mise ({spec})."
                last_output = self._summarize_output(output)

        if shutil.which("pyenv"):
            supported_managers.append("pyenv")
            for spec in self._apdr_python_install_specs(version):
                code, output = self._run_command(["pyenv", "install", "-s", spec], cwd=self.repo_root, timeout=7200)
                if code == 0 and already_available():
                    return True, f"Installed with pyenv ({spec})."
                last_output = self._summarize_output(output)

        if shutil.which("asdf"):
            supported_managers.append("asdf")
            plugin_code, plugin_output = self._run_command(["asdf", "plugin", "list"], cwd=self.repo_root, timeout=60)
            if plugin_code == 0 and "python" not in plugin_output.split():
                self._run_command(["asdf", "plugin", "add", "python"], cwd=self.repo_root, timeout=300)
            for spec in self._apdr_python_install_specs(version):
                code, output = self._run_command(["asdf", "install", "python", spec], cwd=self.repo_root, timeout=7200)
                if code == 0 and already_available():
                    return True, f"Installed with asdf ({spec})."
                last_output = self._summarize_output(output)

        if self._is_windows():
            package_id = self._windows_winget_python_package(version)
            if package_id and shutil.which("winget"):
                supported_managers.append("winget")
                code, output = self._run_command(
                    [
                        "winget",
                        "install",
                        "-e",
                        "--id",
                        package_id,
                        "--accept-package-agreements",
                        "--accept-source-agreements",
                    ],
                    cwd=self.repo_root,
                    timeout=7200,
                )
                if code == 0 and already_available():
                    return True, f"Installed with winget ({package_id})."
                last_output = self._summarize_output(output)

            scoop_package = self._windows_scoop_python_package(version)
            if scoop_package and shutil.which("scoop"):
                supported_managers.append("scoop")
                code, output = self._run_command(["scoop", "install", scoop_package], cwd=self.repo_root, timeout=7200)
                if code == 0 and already_available():
                    return True, f"Installed with scoop ({scoop_package})."
                last_output = self._summarize_output(output)

        if not self._is_windows() and not version.startswith("2."):
            supported_managers.append("miniforge")
            success, detail = self._auto_install_apdr_python_with_miniforge(version)
            if success and already_available():
                return True, detail
            last_output = self._summarize_output(detail)

        if (
            not self._is_windows()
            and not version.startswith("2.")
            and version not in {"3.7", "3.8"}
            and shutil.which("brew")
        ):
            supported_managers.append("brew")
            code, output = self._run_command(["brew", "install", f"python@{version}"], cwd=self.repo_root, timeout=7200)
            if code == 0 and already_available():
                return True, f"Installed with Homebrew (python@{version})."
            last_output = self._summarize_output(output)

        if already_available():
            return True, f"Python {version} became available during setup."

        if supported_managers:
            detail = last_output or "installer finished without exposing a usable interpreter on the current machine."
            return False, f"Tried {', '.join(supported_managers)}. Last output: {detail}"
        if version.startswith("2."):
            if self._is_windows():
                return False, "No supported legacy Python 2.7 manager was found. APDR will not ask uv, winget, or scoop for 2.7; use mise, pyenv, or asdf."
            return False, "No supported legacy Python 2.7 manager was found. APDR will not ask uv, Homebrew, or Miniforge for 2.7; use mise, pyenv, or asdf."
        if self._is_windows():
            return False, "No supported Python manager was found. APDR currently auto-installs via uv, mise, pyenv, asdf, winget, or scoop."
        return False, "No supported Python manager was found. APDR currently auto-installs via uv, mise, pyenv, asdf, Miniforge, or Homebrew."

    def _find_python_interpreter_command(self, version: str) -> list[str]:
        env_name = f"APDR_PYTHON_{version.replace('.', '_')}"
        env_value = (os.environ.get(env_name) or "").strip()
        if env_value:
            args = shlex.split(env_value)
            if args and self._command_matches_python_version(args, version):
                return args

        for args in self._candidate_python_interpreter_commands(version):
            if self._command_matches_python_version(args, version):
                return args
        return []

    def _candidate_python_interpreter_commands(self, version: str) -> list[list[str]]:
        candidates: list[list[str]] = []
        seen: set[str] = set()

        def add(args: list[str]) -> None:
            if not args:
                return
            key = self.format_command(args)
            if key in seen:
                return
            seen.add(key)
            candidates.append(args)

        names = [f"python{version}"]
        if version.startswith("3."):
            names.append("python3")
        elif version.startswith("2."):
            names.append("python2")
        names.append("python")
        if self._is_windows() and shutil.which("py"):
            add(["py", f"-{version}"])

        for name in names:
            resolved = shutil.which(name)
            if resolved:
                add([resolved])

        for path in self._known_python_interpreter_paths(version):
            if path.exists():
                add([str(path)])

        return candidates

    def _known_python_interpreter_paths(self, version: str) -> list[Path]:
        home = Path.home()
        minor = version
        major = version.split(".", 1)[0]
        paths: list[Path] = [
            Path("/Library/Frameworks/Python.framework/Versions") / version / "bin" / f"python{version}",
            Path("/usr/local/bin") / f"python{version}",
            Path("/opt/homebrew/bin") / f"python{version}",
            Path("/usr/local/opt") / f"python@{version}" / "bin" / f"python{version}",
            Path("/opt/homebrew/opt") / f"python@{version}" / "bin" / f"python{version}",
        ]
        if self._is_windows():
            compact = version.replace(".", "")
            windows_roots: list[Path] = []
            local_appdata_value = os.environ.get("LOCALAPPDATA", "").strip()
            if local_appdata_value:
                local_appdata = Path(local_appdata_value)
                windows_roots.extend(
                    [
                        local_appdata / "Programs" / "Python" / f"Python{compact}",
                        local_appdata / "Programs" / "Python" / f"Python{compact}-32",
                    ]
                )
            for env_name in ("ProgramFiles", "ProgramFiles(x86)"):
                base_value = os.environ.get(env_name, "").strip()
                if not base_value:
                    continue
                base = Path(base_value)
                windows_roots.extend(
                    [
                        base / "Python" / f"Python{compact}",
                        base / f"Python{compact}",
                    ]
                )
            for root in windows_roots:
                paths.append(root / "python.exe")

        manager_roots = [
            home / ".pyenv" / "versions",
            home / ".pyenv" / "pyenv-win" / "versions",
            home / ".asdf" / "installs" / "python",
            home / ".local" / "share" / "mise" / "installs" / "python",
            home / ".local" / "share" / "uv" / "python",
            home / ".apdr" / "miniforge3" / "envs",
            home / "miniforge3" / "envs",
        ]
        if self._is_windows():
            local_appdata_value = os.environ.get("LOCALAPPDATA", "").strip()
            if local_appdata_value:
                local_appdata = Path(local_appdata_value)
                manager_roots.extend(
                    [
                        local_appdata / "uv" / "python",
                        local_appdata / "Programs" / "Python",
                    ]
                )
            manager_roots.append(home / "scoop" / "apps")
        for root in manager_roots:
            if not root.exists():
                continue
            for child in self._matching_version_dirs(root, version):
                paths.append(child / "bin" / f"python{minor}")
                paths.append(child / "bin" / f"python{major}")
                paths.append(child / "bin" / "python")
                paths.append(child / "python.exe")
                paths.append(child / f"python{major}.exe")
                paths.append(child / f"python{minor}.exe")
                current = child / "current"
                paths.append(current / "python.exe")
                paths.append(current / f"python{major}.exe")
                paths.append(current / f"python{minor}.exe")

        return paths

    def _matching_version_dirs(self, root: Path, version: str) -> list[Path]:
        if not root.exists():
            return []
        matches: list[Path] = []
        try:
            children = sorted(root.iterdir())
        except OSError:
            return []
        compact = version.replace(".", "")
        prefixes = {
            version,
            f"{version}.",
            f"{version}-",
            f"python-{version}",
            f"Python-{version}",
            f"cpython-{version}",
            f"Python{compact}",
            f"python{compact}",
        }
        for child in children:
            name = child.name
            if name == version or any(name.startswith(prefix) for prefix in prefixes):
                matches.append(child)
        return matches

    def _command_matches_python_version(self, args: list[str], version: str) -> bool:
        code, output = self._run_command(
            args + ["-c", "import sys; sys.stdout.write('%s.%s' % (sys.version_info[0], sys.version_info[1]))"],
            cwd=self.repo_root,
            timeout=8,
        )
        return code == 0 and output.strip() == version

    def _is_metadata_archive_path(self, value: str) -> bool:
        cleaned = value.strip("/")
        if not cleaned:
            return True
        return any(
            part == "__MACOSX" or part.startswith("._")
            for part in cleaned.split("/")
            if part
        )

    def _auto_start_docker_if_needed(self, log: Any) -> None:
        if not shutil.which("docker"):
            log("Docker CLI is not installed, so the UI cannot auto-start the daemon.")
            return
        code, output = self._run_command(["docker", "info", "--format", "{{.ServerVersion}}"], cwd=self.repo_root, timeout=8)
        if code == 0:
            log(f"Docker daemon is already available (server {output}).")
            return
        if sys.platform != "darwin":
            if self._is_windows():
                docker_exes: list[Path] = [Path.home() / "AppData" / "Local" / "Docker" / "Docker Desktop.exe"]
                program_files_value = os.environ.get("ProgramFiles", "").strip()
                if program_files_value:
                    docker_exes.insert(0, Path(program_files_value) / "Docker" / "Docker" / "Docker Desktop.exe")
                docker_exe = next((path for path in docker_exes if path.exists()), None)
                if not docker_exe:
                    log("Docker Desktop.exe was not found in the usual Windows install locations.")
                    return
                log("Attempting to start Docker Desktop on Windows.")
                try:
                    subprocess.Popen([str(docker_exe)], cwd=self.repo_root)
                except OSError as exc:
                    log(f"Failed to launch Docker Desktop: {exc}")
                    return
                for attempt in range(30):
                    time.sleep(2)
                    code, output = self._run_command(["docker", "info", "--format", "{{.ServerVersion}}"], cwd=self.repo_root, timeout=8)
                    if code == 0:
                        log(f"Docker daemon is ready (server {output}).")
                        return
                    if attempt in {0, 4, 9, 19, 29}:
                        log("Waiting for Docker Desktop to finish starting...")
                log("Docker Desktop was launched, but the daemon is still unavailable.")
                return
            log("Docker daemon is not available. Automatic startup is only implemented for macOS and Windows right now.")
            return

        docker_apps = [
            Path("/Applications/Docker.app"),
            Path.home() / "Applications" / "Docker.app",
        ]
        docker_app = next((path for path in docker_apps if path.exists()), None)
        if not docker_app:
            log("Docker Desktop.app was not found in /Applications or ~/Applications.")
            return

        log("Attempting to start Docker Desktop.")
        open_output = self._run_command(["open", "-a", "Docker"], cwd=self.repo_root, timeout=15)[1]
        if open_output:
            log(self._summarize_output(open_output))

        for attempt in range(30):
            time.sleep(2)
            code, output = self._run_command(["docker", "info", "--format", "{{.ServerVersion}}"], cwd=self.repo_root, timeout=8)
            if code == 0:
                log(f"Docker daemon is ready (server {output}).")
                return
            if attempt in {0, 4, 9, 19, 29}:
                log("Waiting for Docker Desktop to finish starting...")
        log("Docker Desktop was launched, but the daemon is still unavailable.")

    def _auto_fix_apdr(self, log: Any) -> None:
        tool_dir = self.tool_dir("apdr")
        available, missing = self.apdr_local_interpreters()
        if missing:
            log(f"Attempting to install missing APDR Python interpreters: {', '.join(missing)}")
            for version in missing:
                success, detail = self._auto_install_apdr_python(version)
                if success:
                    log(f"Python {version}: {detail}")
                else:
                    log(f"Python {version}: {detail}")
        else:
            log("APDR Python interpreters are already available.")

        # Ensure virtualenv is available for Python 2.7 env creation
        if "2.7" in available:
            host_python = shutil.which("python3") or "python3"
            code, _ = self._run_command(
                [host_python, "-m", "virtualenv", "--version"], cwd=self.repo_root, timeout=5
            )
            if code != 0:
                log("Installing virtualenv for Python 2.7 env creation.")
                code, output = self._run_command(
                    [host_python, "-m", "pip", "install", "--user", "virtualenv"],
                    cwd=self.repo_root,
                    timeout=120,
                )
                if code == 0:
                    log("virtualenv installed successfully.")
                else:
                    log(f"virtualenv installation failed: {self._summarize_output(output)}")
            else:
                log("virtualenv is already available for Python 2.7 env creation.")

        binary_candidates = [
            tool_dir / "target" / "release" / "apdr",
            tool_dir / "target" / "debug" / "apdr",
        ]
        if any(candidate.exists() for candidate in binary_candidates):
            log("APDR binary is already available.")
            return
        if not shutil.which("cargo"):
            log("Cargo is not installed, so APDR cannot be built automatically.")
            return
        build_script = tool_dir / "build.sh"
        if build_script.exists() and not self._is_windows():
            log("Building APDR.")
            code, output = self._run_command([str(build_script)], cwd=tool_dir, timeout=1800)
        else:
            log("Building APDR with cargo.")
            code, output = self._run_command(["cargo", "build", "--release"], cwd=tool_dir, timeout=1800)
        if code == 0:
            log("APDR build completed.")
        else:
            log(f"APDR build failed: {self._summarize_output(output)}")

    def _auto_fix_pllm(self, selected_tool: str, python_command: str, log: Any) -> None:
        if selected_tool == "pllm" and python_command.strip():
            log("Python command override is set for PLLM, so the UI will not modify that interpreter automatically.")
            return
        runtime_ok, detail, _runner = self.validate_tool_runtime("pllm", "")
        if runtime_ok:
            log("PLLM runtime is already ready.")
            return
        log(f"PLLM runtime needs setup: {self._summarize_output(detail)}")

        pipenv_command = self.find_pipenv_command()
        if not pipenv_command:
            log("Installing pipenv with the current Python interpreter.")
            code, output = self._run_command(
                [sys.executable, "-m", "pip", "install", "--user", "pipenv"],
                cwd=self.repo_root,
                timeout=1800,
            )
            if code != 0:
                log(f"Pipenv installation failed: {self._summarize_output(output)}")
                return
            pipenv_command = self.find_pipenv_command()
        if not pipenv_command:
            log("Pipenv still was not found after installation.")
            return

        log("Installing PLLM dependencies with pipenv.")
        code, output = self._run_command(pipenv_command + ["install"], cwd=self.tool_dir("pllm"), timeout=3600)
        if code == 0:
            log("PLLM environment installation completed.")
        else:
            log(f"PLLM environment installation failed: {self._summarize_output(output)}")

    def _summarize_output(self, output: str, max_lines: int = 8) -> str:
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        if not lines:
            return ""
        if len(lines) <= max_lines:
            return " | ".join(lines)
        head = " | ".join(lines[:max_lines])
        return f"{head} | ..."

    def _is_windows(self) -> bool:
        return sys.platform.startswith("win")

    def _windows_winget_python_package(self, version: str) -> str:
        if version == "2.7":
            return ""
        return f"Python.Python.{version}"

    def _windows_scoop_python_package(self, version: str) -> str:
        return {
            "3.7": "python37",
            "3.8": "python38",
            "3.9": "python39",
            "3.10": "python310",
            "3.11": "python311",
            "3.12": "python312",
        }.get(version, "")
