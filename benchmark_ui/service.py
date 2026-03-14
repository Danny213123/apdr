from __future__ import annotations

import csv
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from typing import Any
import os
import platform
import shutil
import socket
import threading
import time

from . import APP_NAME, APP_VERSION
from .runner import BenchmarkWorker
from .state import APDR_PYTHON_VERSIONS, AppState, ModelConfig


class BenchmarkService:
    def __init__(self, state: AppState | None = None) -> None:
        self.state = state or AppState()
        self.queue: Queue[dict[str, Any]] = Queue()
        self.worker: BenchmarkWorker | None = None
        self._lock = threading.RLock()
        self._doctor_thread: threading.Thread | None = None
        self._run_started_at = 0.0
        self._run_elapsed_offset = 0.0
        default_config = self._normalize_run_config(self.state.default_run_config())
        self._server_info = {
            "scope": "web app",
            "localUrl": "",
            "networkUrl": "",
            "host": "127.0.0.1",
            "port": 4173,
        }
        self._doctor_state = {
            "busy": False,
            "mode": "idle",
            "summary": "Doctor has not been run yet.",
            "results": [],
            "logs": [],
            "updatedAt": "",
        }
        self._baseline_indexes = self._load_baseline_indexes()
        self._current_run = self._make_idle_run(default_config)

    def set_server_context(self, host: str, port: int, api_only: bool = False) -> None:
        local_host = "localhost" if host in {"0.0.0.0", "::"} else host
        network_host = self._network_host()
        self._server_info = {
            "scope": "api only" if api_only else "web app",
            "localUrl": f"http://{local_host}:{port}",
            "networkUrl": f"http://{network_host}:{port}" if network_host else "",
            "host": host,
            "port": port,
        }

    def bootstrap(self) -> dict[str, Any]:
        self._drain_messages()
        default_config = self._normalize_run_config(self.state.default_run_config())
        return {
            "app": self._app_payload(),
            "defaultConfig": default_config,
            "homePreview": self.preview(default_config),
            "modelConfigs": self._model_configs_payload(),
            "loadouts": self.state.load_loadouts(),
            "runs": self.runs(),
            "doctor": self._doctor_snapshot(),
            "currentRun": self._run_snapshot(),
        }

    def status(self) -> dict[str, Any]:
        self._drain_messages()
        return {
            "currentRun": self._run_snapshot(),
            "doctor": self._doctor_snapshot(),
        }

    def preview(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        config = self._normalize_run_config(payload or self.state.default_run_config())
        return {
            "config": config,
            "resolvedModel": self._resolved_model_label(config),
            "infoFields": self._info_fields(config, self._current_run),
        }

    def model_configs(self) -> dict[str, Any]:
        return self._model_configs_payload()

    def refresh_models(self, tool: str, base_url: str = "") -> dict[str, Any]:
        current = self.state.load_model_config(tool)
        models, source, error = self.state.discover_ollama_models(base_url or current.base_url)
        if models and current.model not in models:
            current.model = models[0]
            current.base_url = self.state.normalize_base_url(base_url or current.base_url)
            self.state.save_model_config(current)
        return {
            "tool": tool,
            "models": models,
            "source": source,
            "error": error,
            "config": self._model_configs_payload().get(tool, {}),
            "allConfigs": self._model_configs_payload(),
        }

    def save_model_configs(self, payload: dict[str, Any]) -> dict[str, Any]:
        configs = payload.get("configs") or []
        for item in configs:
            config = ModelConfig(
                tool=str(item.get("tool") or "").strip(),
                model=str(item.get("model") or "").strip(),
                base_url=str(item.get("base_url") or item.get("baseUrl") or "").strip(),
                temperature=float(item.get("temperature") or 0.7),
            )
            if config.tool:
                self.state.save_model_config(config)
        return {"modelConfigs": self._model_configs_payload()}

    def loadouts(self) -> list[dict[str, Any]]:
        return self.state.load_loadouts()

    def save_loadout(self, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name") or "").strip()
        config = self._normalize_run_config(payload.get("config") or payload)
        path = self.state.save_loadout(name or config.get("tool") or "benchmark", config)
        return {
            "saved": {"slug": path.stem, "name": name or path.stem},
            "loadouts": self.state.load_loadouts(),
        }

    def delete_loadout(self, slug: str) -> dict[str, Any]:
        self.state.delete_loadout(slug)
        return {"loadouts": self.state.load_loadouts()}

    def runs(self) -> list[dict[str, Any]]:
        return [self._run_descriptor(entry["run_id"], entry["summary"], entry["run_dir"]) for entry in self.state.list_run_summaries()]

    def load_run(self, run_id: str) -> dict[str, Any]:
        summary = self.state.load_run_summary(run_id)
        if not summary:
            raise ValueError(f"Saved run not found: {run_id}")
        run_dir = self.state.runs_dir / run_id
        return {
            "run": self._historical_run_snapshot(run_id, summary, run_dir),
            "formConfig": self._run_form_config_from_summary(summary),
            "runs": self.runs(),
        }

    def resume_run(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            self._drain_messages()
            if self.worker and self.worker.is_alive():
                raise RuntimeError("A benchmark is already running.")
            summary = self.state.load_run_summary(run_id)
            if not summary:
                raise ValueError(f"Saved run not found: {run_id}")
            run_dir = self.state.runs_dir / run_id
            historical_run = self._historical_run_snapshot(run_id, summary, run_dir)
            if not historical_run.get("resumeAvailable"):
                raise ValueError("This saved run has no remaining snippets to resume.")

            config = self._run_config_from_summary(summary)
            config["_resume_from_run_id"] = run_id
            config["_resume_results"] = self._summary_results(summary)

            self._current_run = historical_run
            self._current_run["status"] = "booting"
            self._current_run["title"] = "Resuming benchmark run"
            self._current_run["subtitle"] = (
                f"warning: resuming {run_id} with {historical_run['completed']}/{historical_run['total']} cases already recorded."
            )
            self._current_run["statusText"] = (
                f"Preparing to resume {historical_run['remaining']} remaining snippets from {self.state.relative_path(run_dir)}."
            )
            self._current_run["activeCase"] = "preparing dataset archive"
            self._append_activity(f"Loaded saved run {run_id} for resume.")
            self._append_activity(self._current_run["statusText"])
            self._run_elapsed_offset = float(historical_run.get("elapsedSeconds") or 0.0)
            self._run_started_at = time.time()
            self.worker = BenchmarkWorker(self.state, config, self.queue)
            self.worker.start()
            return {"currentRun": self._run_snapshot(), "runs": self.runs()}

    def start_benchmark(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._drain_messages()
            if self.worker and self.worker.is_alive():
                raise RuntimeError("A benchmark is already running.")
            config = self._hydrate_run_config(self._normalize_run_config(payload, validate=True))
            self._current_run = self._make_idle_run(config)
            self._current_run["status"] = "booting"
            self._current_run["title"] = "PyRAG benchmark in progress"
            self._current_run["subtitle"] = (
                f"warning: preparing resolver {config['tool']} against "
                f"{self._strip_archive_suffix(config['dataset_tar'])}; live activity will stream below."
            )
            self._current_run["statusText"] = "Preparing benchmark run..."
            self._current_run["activeCase"] = "preparing dataset archive"
            self._current_run["recentActivity"].append(
                f"Starting benchmark with {config['tool']} against {config['dataset_tar']}"
            )
            self._run_elapsed_offset = 0.0
            self._run_started_at = time.time()
            self.worker = BenchmarkWorker(self.state, config, self.queue)
            self.worker.start()
            return {"currentRun": self._run_snapshot(), "runs": self.runs()}

    def stop_benchmark(self) -> dict[str, Any]:
        with self._lock:
            if self.worker and self.worker.is_alive():
                self.worker.stop()
                self._current_run["status"] = "stopping"
                self._current_run["statusText"] = "Stopping the active benchmark..."
                self._append_activity("Stopping the active benchmark...")
            return {"currentRun": self._run_snapshot(), "runs": self.runs()}

    def start_doctor(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._doctor_state["busy"]:
                return {"doctor": self._doctor_snapshot()}
            tool = str(payload.get("tool") or self.state.default_run_config()["tool"]).strip()
            python_command = str(payload.get("python_command") or payload.get("pythonCommand") or "").strip()
            self._doctor_state = {
                "busy": True,
                "mode": "doctor",
                "summary": self._doctor_intro_summary(tool),
                "results": [],
                "logs": [],
                "updatedAt": self.state.now_iso(),
            }
            self._doctor_thread = threading.Thread(
                target=self._doctor_worker,
                args=(tool, python_command),
                daemon=True,
            )
            self._doctor_thread.start()
            return {"doctor": self._doctor_snapshot()}

    def start_doctor_fix(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._doctor_state["busy"]:
                return {"doctor": self._doctor_snapshot()}
            tool = str(payload.get("tool") or self.state.default_run_config()["tool"]).strip()
            python_command = str(payload.get("python_command") or payload.get("pythonCommand") or "").strip()
            self._doctor_state = {
                "busy": True,
                "mode": "fix",
                "summary": "Doctor is fixing issues automatically.",
                "results": [],
                "logs": ["Starting automatic setup."],
                "updatedAt": self.state.now_iso(),
            }
            self._doctor_thread = threading.Thread(
                target=self._doctor_fix_worker,
                args=(tool, python_command),
                daemon=True,
            )
            self._doctor_thread.start()
            return {"doctor": self._doctor_snapshot()}

    def _doctor_worker(self, tool: str, python_command: str) -> None:
        try:
            base_url = self.state.load_model_config(tool).base_url if tool else ""
            results = self.state.doctor_checks(tool, base_url, python_command)
            with self._lock:
                self._doctor_state["results"] = results
                self._doctor_state["summary"] = self._doctor_summary(results)
                self._doctor_state["updatedAt"] = self.state.now_iso()
        finally:
            with self._lock:
                self._doctor_state["busy"] = False

    def _doctor_fix_worker(self, tool: str, python_command: str) -> None:
        def log(message: str) -> None:
            with self._lock:
                self._doctor_state["logs"].append(message)
                self._doctor_state["logs"] = self._doctor_state["logs"][-250:]

        try:
            results = self.state.auto_fix_doctor_issues(tool, python_command, logger=log)
            with self._lock:
                self._doctor_state["results"] = results
                self._doctor_state["logs"].append("Automatic setup finished. Refreshing Doctor results.")
                self._doctor_state["logs"] = self._doctor_state["logs"][-250:]
                self._doctor_state["summary"] = self._doctor_summary(results)
                self._doctor_state["updatedAt"] = self.state.now_iso()
        except Exception as exc:
            with self._lock:
                self._doctor_state["logs"].append(str(exc))
                self._doctor_state["summary"] = str(exc)
                self._doctor_state["updatedAt"] = self.state.now_iso()
        finally:
            with self._lock:
                self._doctor_state["busy"] = False

    def _doctor_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self._doctor_state)

    def _run_snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_live_run_metrics_locked()
            snapshot = deepcopy(self._current_run)
        snapshot.pop("_recentActivityLimit", None)
        snapshot.pop("_completedCasesLimit", None)
        return snapshot

    def _app_payload(self) -> dict[str, Any]:
        tools = self.state.discover_tools()
        return {
            "name": APP_NAME,
            "version": APP_VERSION,
            "versionDisplay": self.state.version_display(),
            "repoRoot": str(self.state.repo_root),
            "tools": tools,
            "defaultDatasetTar": str(self.state.default_dataset_tar),
            "defaultDatasetLabel": self._display_path(str(self.state.default_dataset_tar)),
            "systemInfo": {
                "os": f"{platform.system()} {platform.release()}",
                "cpu": self._cpu_label(),
                "gpu": self._gpu_label(),
                "memory": self._memory_label(),
            },
            "server": deepcopy(self._server_info),
        }

    def _model_configs_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for tool in self.state.discover_tools():
            config = self.state.load_model_config(tool)
            payload[tool] = {
                "tool": tool,
                "model": config.model,
                "base_url": config.base_url,
                "temperature": config.temperature,
                "updated_at": config.updated_at,
                "cached_models": self.state.get_cached_models(config.base_url),
            }
        return payload

    def _normalize_run_config(self, payload: dict[str, Any] | None, validate: bool = False) -> dict[str, Any]:
        payload = payload or {}
        preferred_tool = str(payload.get("tool") or "").strip() or None
        defaults = self.state.default_run_config(preferred_tool=preferred_tool)
        tool = str(payload.get("tool") or defaults["tool"] or "").strip()
        dataset_tar = str(payload.get("dataset_tar") or payload.get("datasetTar") or defaults["dataset_tar"]).strip()
        loop_count = int(payload.get("loop_count") or payload.get("loopCount") or defaults["loop_count"])
        search_range = int(payload.get("search_range") or payload.get("searchRange") or defaults["search_range"])
        snippet_limit = str(payload.get("snippet_limit") or payload.get("snippetLimit") or defaults["snippet_limit"]).strip()
        config = {
            "tool": tool,
            "dataset_tar": dataset_tar,
            "loop_count": loop_count,
            "search_range": search_range,
            "rag": self._as_bool(payload.get("rag", defaults["rag"])),
            "verbose": self._as_bool(payload.get("verbose", defaults["verbose"])),
            "snippet_limit": snippet_limit,
            "python_command": str(payload.get("python_command") or payload.get("pythonCommand") or defaults["python_command"]).strip(),
            "loadout_name": str(payload.get("loadout_name") or payload.get("loadoutName") or "").strip(),
            "model": str(payload.get("model") or "").strip(),
            "base_url": str(payload.get("base_url") or payload.get("baseUrl") or "").strip(),
            "temperature": self._optional_float(payload.get("temperature") or payload.get("temp")),
        }
        if validate:
            if not config["tool"]:
                raise ValueError("Choose a tool from tools/ before starting a benchmark.")
            if not config["dataset_tar"]:
                raise ValueError("Choose the benchmark archive to run.")
            if config["loop_count"] < 1:
                raise ValueError("Loop count must be at least 1.")
            if config["search_range"] < 0:
                raise ValueError("Search range cannot be negative.")
            if config["snippet_limit"]:
                if int(config["snippet_limit"]) < 0:
                    raise ValueError("Snippet limit cannot be negative.")
        return config

    def _make_idle_run(self, config: dict[str, Any]) -> dict[str, Any]:
        return {
            "status": "idle",
            "title": "PyRAG benchmark ready",
            "subtitle": (
                f"warning: terminal dashboard armed for {config.get('tool') or 'tool selection'}; "
                "configure the run, verify Doctor, then start the benchmark."
            ),
            "statusText": "warning: benchmark is idle; configure your run and press start.",
            "activeCase": "waiting for benchmark start",
            "progressBar": self._format_progress_bar(0, 0),
            "progressPercent": 0.0,
            "runId": "",
            "runDir": "",
            "config": deepcopy(config),
            "infoFields": self._info_fields(config, {}),
            "resolvedModel": self._resolved_model_label(config),
            "completed": 0,
            "total": 0,
            "successes": 0,
            "failures": 0,
            "skipped": 0,
            "elapsedSeconds": 0.0,
            "elapsedLabel": "0m 00s",
            "passRate": "0.0%",
            "speed": "--",
            "eta": "--",
            "recentActivity": [],
            "completedCases": [],
            "_recentActivityLimit": 350,
            "_completedCasesLimit": 500,
        }

    def _drain_messages(self) -> None:
        try:
            while True:
                message = self.queue.get_nowait()
                self._handle_worker_message(message)
        except Empty:
            pass

    def _handle_worker_message(self, message: dict[str, Any]) -> None:
        kind = message["kind"]
        with self._lock:
            if kind == "status":
                text = str(message["text"])
                self._current_run["statusText"] = text
                if text.startswith("Running "):
                    self._current_run["activeCase"] = text.split(" (", 1)[0][len("Running ") :]
                elif text.startswith("Preparing dataset"):
                    self._current_run["activeCase"] = "preparing dataset archive"
                self._append_activity(text)
            elif kind == "plan":
                self._run_started_at = time.time()
                self._current_run["status"] = "running"
                self._current_run["runDir"] = str(message["run_dir"])
                self._current_run["runId"] = os.path.basename(str(message["run_dir"]))
                resumed_completed = int(message.get("resumed_completed") or 0)
                resumed_successes = int(message.get("resumed_successes") or 0)
                resumed_failures = int(message.get("resumed_failures") or 0)
                resumed_skips = int(message.get("resumed_skips") or 0)
                resumed_run_id = str(message.get("resumed_run_id") or "")
                self._current_run["completed"] = resumed_completed
                self._current_run["successes"] = resumed_successes
                self._current_run["failures"] = resumed_failures
                self._current_run["skipped"] = resumed_skips
                self._current_run["total"] = int(message["total"])
                self._current_run["progressPercent"] = (
                    round(self._current_run["completed"] / self._current_run["total"] * 100, 1)
                    if self._current_run["total"]
                    else 0.0
                )
                self._current_run["passRate"] = self._format_pass_rate(
                    self._current_run["successes"],
                    self._current_run["failures"],
                )
                self._current_run["progressBar"] = self._format_progress_bar(
                    self._current_run["completed"],
                    self._current_run["total"],
                )
                if resumed_run_id:
                    self._current_run["statusText"] = (
                        f"Resuming {resumed_run_id} into {self.state.relative_path(message['run_dir'])} | "
                        f"{resumed_completed}/{message['total']} cases already recorded"
                    )
                    self._current_run["title"] = "PyRAG benchmark resumed"
                    self._current_run["subtitle"] = (
                        "warning: historical results were restored; only the remaining cases will execute below."
                    )
                else:
                    self._current_run["statusText"] = (
                        f"Run directory: {self.state.relative_path(message['run_dir'])} | Total snippets: {message['total']}"
                    )
                    self._current_run["title"] = "PyRAG benchmark in progress"
                    self._current_run["subtitle"] = (
                        "warning: benchmark telemetry is live; monitor active cases, logs, and completed rows below."
                    )
                self._refresh_live_run_metrics_locked(force=True)
                self._refresh_run_fields()
                self._append_activity(self._current_run["statusText"])
            elif kind == "command":
                self._append_activity(f"$ {message['text']}")
            elif kind == "log":
                self._append_activity(str(message["line"]))
            elif kind == "progress":
                result = dict(message.get("result") or {})
                if not result:
                    result = {
                        "snippet": message["snippet"],
                        "returncode": message["returncode"],
                        "duration_seconds": message["duration"],
                        "output_files": [],
                        "log_tail": [],
                    }
                case_succeeded = self._result_succeeded(result)
                case_skipped = self._result_skipped(result)
                self._current_run["completed"] = int(message["completed"])
                self._current_run["total"] = int(message["total"])
                if case_succeeded:
                    self._current_run["successes"] += 1
                elif case_skipped:
                    self._current_run["skipped"] += 1
                else:
                    self._current_run["failures"] += 1
                self._current_run["passRate"] = self._format_pass_rate(
                    self._current_run["successes"],
                    self._current_run["failures"],
                )
                self._current_run["progressPercent"] = (
                    round(self._current_run["completed"] / self._current_run["total"] * 100, 1)
                    if self._current_run["total"]
                    else 0.0
                )
                self._current_run["progressBar"] = self._format_progress_bar(
                    self._current_run["completed"],
                    self._current_run["total"],
                )
                status = self._result_status_label(result)
                self._current_run["statusText"] = (
                    f"{message['completed']}/{message['total']} complete | {message['snippet']} | "
                    f"{status} | {float(message['duration']):.2f}s"
                )
                self._current_run["activeCase"] = (
                    f"completed {self._extract_case_id(str(message['snippet']))}; awaiting next case"
                )
                self._refresh_live_run_metrics_locked(force=True)
                self._current_run["subtitle"] = (
                    f"warning: {self._current_run['successes']} passes, "
                    f"{self._current_run['failures']} failures, "
                    f"{self._current_run['skipped']} skips, {self._current_run['speed']} pace."
                )
                self._current_run["completedCases"].insert(0, self._build_case_row(result))
                self._current_run["completedCases"] = self._current_run["completedCases"][
                    : self._current_run["_completedCasesLimit"]
                ]
                self._refresh_run_fields()
            elif kind == "done":
                self._refresh_live_run_metrics_locked(force=True)
                self._current_run["status"] = "completed" if message["status"] == "completed" else "stopped"
                self._current_run["statusText"] = (
                    f"{message['status'].capitalize()} benchmark. "
                    f"Summary saved to {self.state.relative_path(message['run_dir'])}."
                )
                self._current_run["activeCase"] = "benchmark finished"
                self._current_run["title"] = (
                    "PyRAG benchmark complete" if message["status"] == "completed" else "PyRAG benchmark stopped"
                )
                self._current_run["subtitle"] = (
                    f"warning: artifacts written to {self.state.relative_path(message['run_dir'])}; "
                    "review the completed cases table for per-snippet outcomes."
                )
                self._refresh_run_fields()
                self._append_activity(self._current_run["statusText"])
                self.worker = None
            elif kind == "error":
                self._refresh_live_run_metrics_locked(force=True)
                self._current_run["status"] = "failed"
                self._current_run["statusText"] = str(message["message"])
                self._current_run["activeCase"] = "run aborted"
                self._current_run["title"] = "PyRAG benchmark failed"
                self._current_run["subtitle"] = (
                    "warning: benchmark execution aborted; inspect the recent activity panel and doctor checks."
                )
                self._refresh_run_fields()
                self._append_activity(str(message["message"]))
                self.worker = None

    def _refresh_run_fields(self) -> None:
        self._refresh_run_fields_for(self._current_run)

    def _refresh_live_run_metrics_locked(self, force: bool = False) -> None:
        status = str(self._current_run.get("status") or "")
        if not self._run_started_at and not self._run_elapsed_offset:
            return
        if not force and status not in {"booting", "running", "stopping"}:
            return
        elapsed = self._run_elapsed_offset
        if self._run_started_at:
            elapsed += max(time.time() - self._run_started_at, 0.0)
        completed = int(self._current_run.get("completed") or 0)
        total = int(self._current_run.get("total") or 0)
        case_pace = (elapsed / completed) if completed > 0 and elapsed > 0 else None
        remaining = max(total - completed, 0)
        eta_seconds = (remaining * case_pace) if case_pace is not None else None
        self._current_run["elapsedSeconds"] = round(elapsed, 2)
        self._current_run["elapsedLabel"] = self._format_duration(elapsed)
        self._current_run["speed"] = self._format_case_pace(case_pace)
        self._current_run["eta"] = self._format_eta(eta_seconds)

    def _build_case_row(self, result: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
        snippet = str(result.get("snippet", ""))
        status = self._display_status(result)
        run_config = config or self._current_run["config"]
        case_id = self._extract_case_id(snippet)
        comparisons = self._baseline_comparisons(case_id, status)
        return {
            "status": status,
            "caseId": case_id,
            "python": self._extract_python_version(result.get("output_files", [])),
            "tries": str(run_config.get("loop_count", 0)),
            "seconds": f"{float(result.get('duration_seconds', 0.0)):.2f}",
            "pllm": comparisons["pllm"]["label"],
            "legacy": comparisons["legacy"]["label"],
            "readpy": comparisons["readpy"]["label"],
            "pllmSummary": comparisons["pllm"]["summary"],
            "legacySummary": comparisons["legacy"]["summary"],
            "readpySummary": comparisons["readpy"]["summary"],
            "result": self._summarize_result(result),
            "dependencies": self._dependency_summary(snippet, result),
            "snippet": snippet,
            "outputFiles": [str(item) for item in result.get("output_files", []) if item],
            "logTail": [str(line) for line in result.get("log_tail", []) if str(line).strip()],
        }

    def _append_activity(self, text: str) -> None:
        self._current_run["recentActivity"].append(text)
        self._current_run["recentActivity"] = self._current_run["recentActivity"][
            -self._current_run["_recentActivityLimit"] :
        ]

    def _run_form_config_from_summary(self, summary: dict[str, Any]) -> dict[str, Any]:
        config = self._normalize_run_config(summary)
        return {
            "tool": config["tool"],
            "dataset_tar": config["dataset_tar"],
            "loop_count": config["loop_count"],
            "search_range": config["search_range"],
            "rag": config["rag"],
            "verbose": config["verbose"],
            "snippet_limit": config["snippet_limit"],
            "python_command": config["python_command"],
            "loadout_name": config["loadout_name"],
        }

    def _run_config_from_summary(self, summary: dict[str, Any]) -> dict[str, Any]:
        return self._hydrate_run_config(self._normalize_run_config(summary))

    def _hydrate_run_config(self, config: dict[str, Any]) -> dict[str, Any]:
        hydrated = deepcopy(config)
        tool = str(hydrated.get("tool") or "").strip()
        if not tool:
            return hydrated
        selected = self.state.load_model_config(tool)
        hydrated["model"] = str(hydrated.get("model") or selected.model)
        hydrated["base_url"] = str(hydrated.get("base_url") or selected.base_url)
        if hydrated.get("temperature") is None:
            hydrated["temperature"] = selected.temperature
        return hydrated

    def _historical_run_snapshot(self, run_id: str, summary: dict[str, Any], run_dir: Path) -> dict[str, Any]:
        config = self._run_config_from_summary(summary)
        results = self._summary_results(summary)
        completed = len(results)
        total = self._estimate_total_from_summary(summary, config, completed)
        successes = sum(1 for item in results if self._result_succeeded(item))
        skipped = sum(1 for item in results if self._result_skipped(item))
        failures = completed - successes - skipped
        elapsed = self._summary_elapsed_seconds(summary)
        case_pace = (elapsed / completed) if completed > 0 and elapsed > 0 else None
        remaining = max(total - completed, 0)
        eta_seconds = (
            remaining * case_pace
            if case_pace is not None and remaining > 0
            else (0.0 if remaining == 0 and total else None)
        )
        pass_rate = self._pass_rate_value(successes, failures)
        status = str(summary.get("status") or "completed")
        resume_available = status != "completed" and remaining > 0

        run = self._make_idle_run(config)
        run.update(
            {
                "status": status,
                "title": self._historical_title(status),
                "subtitle": (
                    f"warning: viewing saved run {run_id} from {self.state.relative_path(run_dir)}."
                    if not resume_available
                    else f"warning: viewing saved run {run_id}; {remaining} cases remain and can be resumed."
                ),
                "statusText": (
                    f"Loaded saved run {run_id} with {completed}/{total} completed cases."
                ),
                "activeCase": "historical run snapshot",
                "progressBar": self._format_progress_bar(completed, total),
                "progressPercent": round(completed / total * 100, 1) if total else 0.0,
                "runId": run_id,
                "runDir": str(run_dir),
                "completed": completed,
                "total": total,
                "successes": successes,
                "failures": failures,
                "skipped": skipped,
                "elapsedSeconds": round(elapsed, 2),
                "elapsedLabel": self._format_duration(elapsed) if elapsed > 0 else "0m 00s",
                "passRate": f"{pass_rate:0.1f}%",
                "speed": self._format_case_pace(case_pace),
                "eta": self._format_eta(eta_seconds),
                "recentActivity": self._historical_activity(run_id, summary, run_dir, completed, total, remaining),
                "completedCases": [
                    self._build_case_row(item, config)
                    for item in reversed(results[-run["_completedCasesLimit"] :])
                ],
                "resumeAvailable": resume_available,
                "remaining": remaining,
            }
        )
        self._refresh_run_fields_for(run)
        return run

    def _historical_activity(
        self,
        run_id: str,
        summary: dict[str, Any],
        run_dir: Path,
        completed: int,
        total: int,
        remaining: int,
    ) -> list[str]:
        lines = [
            f"Loaded saved run {run_id} from {self.state.relative_path(run_dir)}.",
            f"Original status: {summary.get('status') or 'unknown'}.",
            f"Completed {completed}/{total} cases; {remaining} remaining.",
        ]
        if summary.get("started_at"):
            lines.append(f"Started at {summary['started_at']}.")
        if summary.get("finished_at"):
            lines.append(f"Finished at {summary['finished_at']}.")
        historical_results = self._summary_results(summary)
        last_result = historical_results[-1] if historical_results else None
        if last_result:
            lines.append(f"Last case: {last_result.get('snippet') or '--'}")
            for line in [str(item).strip() for item in last_result.get("log_tail", []) if str(item).strip()][-3:]:
                lines.append(line)
        return lines[:12]

    def _run_descriptor(self, run_id: str, summary: dict[str, Any], run_dir: Path) -> dict[str, Any]:
        config = self._run_config_from_summary(summary)
        results = self._summary_results(summary)
        completed = len(results)
        total = self._estimate_total_from_summary(summary, config, completed)
        successes = sum(1 for item in results if self._result_succeeded(item))
        skipped = sum(1 for item in results if self._result_skipped(item))
        failures = completed - successes - skipped
        remaining = max(total - completed, 0)
        status = str(summary.get("status") or "completed")
        return {
            "runId": run_id,
            "status": status,
            "tool": str(summary.get("tool") or config.get("tool") or "--"),
            "completed": completed,
            "total": total,
            "successes": successes,
            "failures": failures,
            "skipped": skipped,
            "remaining": remaining,
            "resumable": status != "completed" and remaining > 0,
            "startedAt": str(summary.get("started_at") or ""),
            "finishedAt": str(summary.get("finished_at") or ""),
            "runDir": self.state.relative_path(run_dir),
            "label": (
                f"{run_id} | {status.upper()} | {summary.get('tool') or config.get('tool') or '--'} | "
                f"{completed}/{total}"
            ),
        }

    def _refresh_run_fields_for(self, run: dict[str, Any]) -> None:
        run["resolvedModel"] = self._resolved_model_label(run["config"])
        run["infoFields"] = self._info_fields(run["config"], run)

    def _estimate_total_from_summary(self, summary: dict[str, Any], config: dict[str, Any], completed: int) -> int:
        dataset_dir = summary.get("dataset_dir")
        snippet_limit = self._optional_int(summary.get("snippet_limit") or config.get("snippet_limit"))
        snippet_count = self.state.count_snippets(dataset_dir) if dataset_dir else 0
        if snippet_limit:
            total = min(snippet_count, snippet_limit) if snippet_count else snippet_limit
        else:
            total = snippet_count or completed
        return max(total, completed)

    def _summary_elapsed_seconds(self, summary: dict[str, Any]) -> float:
        started_at = self._parse_timestamp(summary.get("started_at"))
        finished_at = self._parse_timestamp(summary.get("finished_at"))
        if started_at and finished_at:
            return max((finished_at - started_at).total_seconds(), 0.0)
        return sum(float(item.get("duration_seconds", 0.0)) for item in summary.get("results", []) if isinstance(item, dict))

    def _historical_title(self, status: str) -> str:
        if status == "completed":
            return "Historical benchmark complete"
        if status == "stopped":
            return "Historical benchmark stopped"
        if status == "failed":
            return "Historical benchmark failed"
        return "Historical benchmark snapshot"

    def _parse_timestamp(self, value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _resolved_model_label(self, config: dict[str, Any]) -> str:
        tool = config.get("tool") or ""
        if not tool:
            return "Choose a tool to resolve the active model."
        selected = self.state.load_model_config(tool)
        model_name = str(config.get("model") or selected.model)
        base_url = str(config.get("base_url") or selected.base_url)
        temperature = config.get("temperature")
        if temperature is None:
            temperature = selected.temperature
        return f"{model_name} from {base_url} with temperature {temperature}"

    def _info_fields(self, config: dict[str, Any], run: dict[str, Any]) -> list[dict[str, str]]:
        tool = config.get("tool") or ""
        selected = self.state.load_model_config(tool) if tool else None
        model_name = str(config.get("model") or (selected.model if selected else "not selected"))
        base_url = str(config.get("base_url") or (selected.base_url if selected else "--"))
        dataset_tar = str(config.get("dataset_tar") or self.state.default_dataset_tar)
        source_path = self._display_path(dataset_tar)
        target_label = self._strip_archive_suffix(dataset_tar)
        effective = (
            self.state.format_command(self.state.choose_runner(tool, str(config.get("python_command") or "")))
            if tool
            else "--"
        )
        jobs = run.get("total") or str(config.get("snippet_limit") or "all")
        artifacts = self.state.relative_path(run["runDir"]) if run.get("runDir") else "runs/pending"
        fields = [
            {"label": "Run ID", "value": run.get("runId") or "standby"},
            {"label": "Version", "value": self.state.version_display()},
            {"label": "OS", "value": f"{platform.system()} {platform.release()}"},
            {"label": "CPU", "value": self._cpu_label()},
            {"label": "GPU", "value": self._gpu_label()},
            {"label": "Memory", "value": self._memory_label()},
            {"label": "Target", "value": target_label or "--"},
            {"label": "Resolver", "value": tool or "--"},
            {"label": "Preset", "value": str(config.get("loadout_name") or "manual")},
            {"label": "Research", "value": "enabled" if config.get("rag") else "disabled"},
            {
                "label": "Prompt",
                "value": (
                    f"loop={config.get('loop_count', 0)} range={config.get('search_range', 0)} "
                    f"verbose={'on' if config.get('verbose') else 'off'}"
                ),
            },
            {"label": "Source", "value": source_path},
            {"label": "Models", "value": model_name},
            {"label": "Effective", "value": effective},
            {"label": "LLM", "value": f"{base_url} [{model_name}]" if tool else "--"},
            {"label": "Jobs", "value": str(jobs)},
            {"label": "Artifacts", "value": artifacts},
        ]
        if tool == "apdr":
            available, missing = self.state.apdr_local_interpreters()
            fields.insert(14, {"label": "Validation", "value": "local Python environments"})
            fields.insert(15, {"label": "Py envs", "value": self._compact_apdr_interpreter_label(available, missing)})
        elif tool == "pllm":
            fields.insert(14, {"label": "Validation", "value": "Docker build + run"})
        return fields

    def _display_path(self, value: str) -> str:
        candidate = self._repo_relative_path(value)
        return self.state.relative_path(candidate)

    def _repo_relative_path(self, value: str) -> Path:
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = self.state.repo_root / candidate
        return candidate.resolve()

    def _strip_archive_suffix(self, value: str) -> str:
        name = os.path.basename(value)
        for suffix in (".tar.gz", ".tgz", ".tar"):
            if name.endswith(suffix):
                return name[: -len(suffix)]
        return name

    def _cpu_label(self) -> str:
        cores = os.cpu_count() or 0
        processor = platform.processor().strip() or platform.machine() or "unknown"
        return f"{processor} x{cores}" if cores else processor

    def _gpu_label(self) -> str:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            return "Apple Silicon integrated"
        if shutil.which("nvidia-smi"):
            return "NVIDIA detected"
        if platform.system() == "Darwin":
            return "macOS integrated"
        return "not reported"

    def _memory_label(self) -> str:
        try:
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
            page_count = int(os.sysconf("SC_PHYS_PAGES"))
            return self._format_bytes(page_size * page_count)
        except (AttributeError, OSError, ValueError):
            return "unknown"

    def _format_bytes(self, total_bytes: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        value = float(total_bytes)
        for unit in units:
            if value < 1024 or unit == units[-1]:
                return f"{value:.1f} {unit}"
            value /= 1024
        return f"{value:.1f} TB"

    def _format_duration(self, total_seconds: float) -> str:
        seconds = max(int(total_seconds), 0)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        if hours:
            return f"{hours}h {minutes:02d}m {seconds:02d}s"
        return f"{minutes}m {seconds:02d}s"

    def _format_eta(self, total_seconds: float | None) -> str:
        if total_seconds is None:
            return "--"
        return self._format_duration(total_seconds)

    def _format_case_pace(self, seconds_per_case: float | None) -> str:
        if seconds_per_case is None:
            return "--"
        return f"{seconds_per_case:0.2f} sec/case"

    def _format_progress_bar(self, completed: int, total: int, width: int = 40) -> str:
        if total <= 0:
            return f"Progress {completed}/0 (  0.0%) [{'-' * width}]"
        ratio = min(max(completed / total, 0.0), 1.0)
        filled = min(width, int(round(ratio * width)))
        bar = "#" * filled + "-" * (width - filled)
        return f"Progress {completed}/{total} ({ratio * 100:5.1f}%) [{bar}]"

    def _extract_case_id(self, snippet_path: str) -> str:
        parts = os.path.normpath(snippet_path).split(os.sep)
        if len(parts) >= 2:
            return parts[-2]
        return "--"

    def _extract_python_version(self, output_files: list[str]) -> str:
        for item in output_files:
            name = os.path.basename(str(item))
            version = self._extract_python_version_from_name(name)
            if version:
                return version
        version_info = platform.python_version_tuple()
        return f"{version_info[0]}.{version_info[1]}"

    def _extract_python_version_from_name(self, name: str) -> str:
        if name.startswith("output_data_") and name.endswith(".yml"):
            return name[len("output_data_") : -4]
        return ""

    def _load_baseline_indexes(self) -> dict[str, dict[str, dict[str, str]]]:
        return {
            "pllm": self._load_pllm_baseline(self.state.repo_root / "pllm_results" / "csv" / "summary-all-runs.csv"),
            "legacy": self._load_simple_baseline(self.state.repo_root / "pyego-results" / "pyego_results.csv", tool_label="PYEGO"),
            "readpy": self._load_simple_baseline(self.state.repo_root / "readpy-results" / "readpy_results_total.csv", tool_label="READPY"),
        }

    def _load_pllm_baseline(self, path: Path) -> dict[str, dict[str, str]]:
        index: dict[str, dict[str, str]] = {}
        try:
            with path.open(encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    case_id = str(row.get("name") or "").strip()
                    if not case_id:
                        continue
                    pass_count = self._safe_int(row.get("passed"))
                    status = "PASS" if pass_count > 0 else "FAIL"
                    summary_parts = [f"PLLM {status} ({pass_count}/10)"]
                    python_version = self._extract_python_version_from_name(str(row.get("file") or ""))
                    result_label = str(row.get("result") or "").strip()
                    modules = self._format_baseline_modules(str(row.get("python_modules") or ""))
                    if python_version:
                        summary_parts.append(f"py {python_version}")
                    if result_label:
                        summary_parts.append(result_label)
                    if modules:
                        summary_parts.append(f"deps {modules}")
                    index[case_id] = {
                        "status": status,
                        "summary": " | ".join(summary_parts),
                    }
        except OSError:
            return {}
        return index

    def _load_simple_baseline(self, path: Path, tool_label: str) -> dict[str, dict[str, str]]:
        index: dict[str, dict[str, str]] = {}
        try:
            with path.open(encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    case_id = str(row.get("name") or "").strip()
                    if not case_id:
                        continue
                    passed = self._as_bool(row.get("passed"))
                    status = "PASS" if passed else "FAIL"
                    result_label = str(row.get("result") or "").strip()
                    modules = self._format_baseline_modules(str(row.get("python_modules") or ""))
                    summary_parts = [f"{tool_label} {status}"]
                    if result_label:
                        summary_parts.append(result_label)
                    if modules:
                        summary_parts.append(f"deps {modules}")
                    index[case_id] = {
                        "status": status,
                        "summary": " | ".join(summary_parts),
                    }
        except OSError:
            return {}
        return index

    def _baseline_comparisons(self, case_id: str, status: str) -> dict[str, dict[str, str]]:
        return {
            "pllm": self._comparison_entry("pllm", "PLLM", case_id, status),
            "legacy": self._comparison_entry("legacy", "PYEGO", case_id, status),
            "readpy": self._comparison_entry("readpy", "READPY", case_id, status),
        }

    def _comparison_entry(self, key: str, label: str, case_id: str, status: str) -> dict[str, str]:
        baseline = self._baseline_indexes.get(key, {}).get(case_id)
        if not baseline:
            return {"label": "--", "summary": f"{label} baseline unavailable."}
        baseline_status = str(baseline.get("status") or "").strip().upper() or "FAIL"
        match = status == baseline_status
        if key == "pllm" and status == "SKIP" and baseline_status != "PASS":
            match = True
        comparison = "MATCH" if match else "DIFF"
        return {
            "label": comparison,
            "summary": f"{comparison}: current {status} vs {baseline.get('summary') or f'{label} {baseline_status}'}",
        }

    def _format_baseline_modules(self, raw: str) -> str:
        modules = [item.strip() for item in raw.split(";") if item.strip()]
        if not modules:
            return ""
        preview = ", ".join(modules[:3])
        if len(modules) > 3:
            preview = f"{preview} +{len(modules) - 3}"
        return preview[:96]

    def _safe_int(self, value: Any) -> int:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return 0

    def _result_succeeded(self, result: dict[str, Any]) -> bool:
        if self._result_skipped(result):
            return False
        if int(result.get("returncode", 1)) != 0:
            return False
        if self._result_has_failure_markers(result):
            return False
        explicit = result.get("succeeded")
        if explicit is not None:
            return bool(explicit)
        if self._result_requirements(str(result.get("snippet", "")), result):
            return True
        output_files = [str(item) for item in result.get("output_files", []) if str(item).strip()]
        return bool(output_files) and int(result.get("returncode", 1)) == 0

    def _result_skipped(self, result: dict[str, Any]) -> bool:
        explicit = result.get("skipped")
        if explicit is not None:
            return bool(explicit)
        validation_status = self._result_validation_status(result)
        return validation_status.startswith("skipped") or validation_status == "host-runtime-required"

    def _display_status(self, result: dict[str, Any]) -> str:
        if self._result_succeeded(result):
            return "PASS"
        if self._result_skipped(result):
            return "SKIP"
        return "FAIL"

    def _result_status_label(self, result: dict[str, Any]) -> str:
        display_status = self._display_status(result)
        if display_status != "FAIL":
            return display_status
        return f"FAIL ({result.get('returncode', 1)})" if int(result.get("returncode", 1)) != 0 else "FAIL"

    def _result_has_failure_markers(self, result: dict[str, Any]) -> bool:
        terms = (
            "traceback",
            "import error",
            "importerror",
            "error:",
            "error ",
            "failed",
            "could not find a version",
            "no matching distribution",
            "client error",
            "non-zero code",
        )
        for raw in result.get("log_tail", []) or []:
            lowered = str(raw).lower()
            if any(term in lowered for term in terms):
                return True
        return False

    def _summarize_result(self, result: dict[str, Any]) -> str:
        if self._result_succeeded(result):
            return "ok"
        validation_reason = self._result_validation_reason(result)
        if validation_reason:
            return validation_reason[:90]
        tail = [str(line).strip() for line in result.get("log_tail", []) if str(line).strip()]
        meaningful = self._meaningful_failure_line(tail)
        returncode = int(result.get("returncode", 1))
        if meaningful and (returncode == 0 or self._result_has_failure_markers(result)):
            return meaningful[:90]
        if returncode != 0:
            return f"exit {returncode}"
        if meaningful:
            return meaningful[:90]
        if tail:
            return tail[-1][:90]
        return "no output generated"

    def _result_validation_status(self, result: dict[str, Any]) -> str:
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("validation_status") or "").strip().lower()

    def _result_validation_reason(self, result: dict[str, Any]) -> str:
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("validation_reason") or "").strip()

    def _meaningful_failure_line(self, tail: list[str]) -> str:
        priority_terms = ("error", "fail", "exception", "traceback", "importerror", "no matching distribution")
        for line in reversed(tail):
            cleaned = line.strip()
            lowered = cleaned.lower()
            if not cleaned or self._is_ignorable_tail_line(lowered):
                continue
            if any(term in lowered for term in priority_terms):
                return cleaned
        for line in reversed(tail):
            cleaned = line.strip()
            lowered = cleaned.lower()
            if cleaned and not self._is_ignorable_tail_line(lowered):
                return cleaned
        return ""

    def _is_ignorable_tail_line(self, lowered: str) -> bool:
        if not lowered:
            return True
        if lowered.isdigit():
            return True
        if lowered.startswith("found \"import\""):
            return True
        if lowered.startswith("['") and lowered.endswith("']"):
            return True
        if lowered.startswith('["') and lowered.endswith('"]'):
            return True
        if lowered.startswith("{'python_version'") or lowered.startswith('{"python_version"'):
            return True
        if lowered.startswith("{'module'") or lowered.startswith('{"module"'):
            return True
        if lowered.startswith("{'properties'") or lowered.startswith('{"properties"'):
            return True
        return lowered in {
            "done",
            "created",
            "processing completed without the timeout",
            "processing completed without timeout",
            "no previous this time!",
        }

    def _dependency_summary(self, snippet: str, result: dict[str, Any]) -> str:
        dependencies = self._result_requirements(snippet, result)
        if dependencies and self._result_succeeded(result):
            preview = ", ".join(dependencies[:3])
            if len(dependencies) > 3:
                preview = f"{preview} +{len(dependencies) - 3}"
            return preview[:110]
        if not self._result_succeeded(result):
            return "--"
        output_files = [os.path.basename(str(item)) for item in result.get("output_files", []) if item]
        if output_files:
            return ", ".join(output_files[:2])[:110]
        return "--"

    def _result_requirements(self, snippet: str, result: dict[str, Any]) -> list[str]:
        inline = [str(item).strip() for item in result.get("requirements", []) if str(item).strip()]
        if inline:
            return inline
        if not snippet:
            return []
        snippet_path = self._repo_relative_path(snippet)
        requirements_path = snippet_path.parent / "requirements.txt"
        if not requirements_path.exists():
            return []
        started_at = self._parse_timestamp(result.get("started_at"))
        finished_at = self._parse_timestamp(result.get("finished_at"))
        if started_at and finished_at:
            modified_at = datetime.fromtimestamp(requirements_path.stat().st_mtime)
            if modified_at < (started_at - timedelta(seconds=1)) or modified_at > (finished_at + timedelta(seconds=1)):
                return []
        try:
            return [
                line.strip()
                for line in requirements_path.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
        except OSError:
            return []

    def _summary_results(self, summary: dict[str, Any]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for item in summary.get("results", []):
            if not isinstance(item, dict):
                continue
            result = dict(item)
            snippet = str(result.get("snippet") or "").strip()
            if self._is_artifact_snippet(snippet):
                continue
            result["succeeded"] = self._result_succeeded(result)
            result["skipped"] = self._result_skipped(result)
            results.append(result)
        return results

    def _pass_rate_value(self, successes: int, failures: int) -> float:
        scored = max(successes + failures, 0)
        if not scored:
            return 0.0
        return successes / scored * 100

    def _format_pass_rate(self, successes: int, failures: int) -> str:
        return f"{self._pass_rate_value(successes, failures):0.1f}%"

    def _is_artifact_snippet(self, snippet: str) -> bool:
        normalized = snippet.replace("\\", "/")
        return "/.apdr-docker/" in normalized or normalized.startswith(".apdr-docker/")

    def _doctor_summary(self, results: list[dict[str, str]]) -> str:
        failing = sum(1 for row in results if row["status"] == "FAIL")
        warnings = sum(1 for row in results if row["status"] == "WARN")
        return f"Doctor finished with {failing} failures and {warnings} warnings."

    def _doctor_intro_summary(self, tool: str) -> str:
        if tool == "apdr":
            return "Doctor is checking local Python interpreters, Ollama, dataset readiness, and each tool runtime."
        if tool == "pllm":
            return "Doctor is checking Docker, Ollama, dataset readiness, and each tool runtime."
        return "Doctor is checking dataset readiness, model access, and each tool runtime."

    def _compact_apdr_interpreter_label(self, available: dict[str, str], missing: list[str]) -> str:
        if not available:
            return "none installed"
        installed = [version for version in APDR_PYTHON_VERSIONS if version in available]
        detail = ", ".join(installed)
        if missing:
            return f"installed: {detail} | missing: {', '.join(missing)}"
        return f"installed: {detail}"

    def _network_host(self) -> str:
        try:
            hostname = socket.gethostname()
            address = socket.gethostbyname(hostname)
        except OSError:
            return ""
        if address.startswith("127."):
            return ""
        return address

    def _as_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _optional_float(self, value: Any) -> float | None:
        if value in ("", None):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _optional_int(self, value: Any) -> int:
        if value in ("", None):
            return 0
        try:
            return max(int(value), 0)
        except (TypeError, ValueError):
            return 0
