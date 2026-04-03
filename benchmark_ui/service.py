from __future__ import annotations

import csv
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Generator
import os
import platform
import shutil
import socket
import threading
import time

from . import APP_NAME, APP_VERSION
from .runner import (
    BenchmarkWorker,
    collect_replay_preflight_warnings,
    determine_effective_worker_count,
)
from .run_contract import (
    contract_from_sources,
    determine_execution_mode,
    normalize_build_profile,
    normalize_cache_state,
    normalize_context_window,
    normalize_inference_policy,
    normalize_llm_validation_policy,
    normalize_run_intent,
)
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
            # Saved-run history can be large on Windows; keep bootstrap fast and
            # let the UI fetch the run list after first paint.
            "runs": [],
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
        run_id = self.state._sanitize_path_component(run_id)
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
        run_id = self.state._sanitize_path_component(run_id)
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
            # Initialize event queue for SSE streaming before worker starts
            self._current_run["_event_queue"] = Queue()
            self.worker = BenchmarkWorker(self.state, config, self.queue)
            # Attach event queue reference to worker for SSE streaming
            self.worker._current_run_event_queue = self._current_run["_event_queue"]
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
            self._current_run["title"] = "APDR benchmark in progress"
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
            # Initialize event queue for SSE streaming before worker starts
            self._current_run["_event_queue"] = Queue()
            self.worker = BenchmarkWorker(self.state, config, self.queue)
            # Attach event queue reference to worker for SSE streaming
            self.worker._current_run_event_queue = self._current_run["_event_queue"]
            self.worker.start()
            return {"currentRun": self._run_snapshot(), "runs": self.runs()}

    def stop_benchmark(self, join_timeout: float = 20) -> dict[str, Any]:
        worker = None
        with self._lock:
            if self.worker and self.worker.is_alive():
                self.worker.stop()
                worker = self.worker
                self._current_run["status"] = "stopping"
                self._current_run["statusText"] = "Stopping the active benchmark..."
                self._append_activity("Stopping the active benchmark...")
        # Join outside the lock so the worker can still emit final messages
        if worker is not None:
            worker.join(timeout=join_timeout)
        with self._lock:
            return {"currentRun": self._run_snapshot(), "runs": self.runs()}

    def _calculate_tier_stats(self, run_state: dict[str, Any]) -> dict[str, Any]:
        """Calculate tier1/tier2/tier3 breakdown from results.

        Returns dict with tier counts and percentages.
        """
        results = run_state.get("results", [])
        tier1_count = sum(1 for r in results if r.get("tier") == "tier1")
        tier2_count = sum(1 for r in results if r.get("tier") == "tier2")
        tier3_count = sum(1 for r in results if r.get("tier") == "tier3")
        total = len(results)

        return {
            "tier1": {
                "count": tier1_count,
                "percent": round(tier1_count / total * 100, 1) if total > 0 else 0.0
            },
            "tier2": {
                "count": tier2_count,
                "percent": round(tier2_count / total * 100, 1) if total > 0 else 0.0
            },
            "tier3": {
                "count": tier3_count,
                "percent": round(tier3_count / total * 100, 1) if total > 0 else 0.0
            },
            "total": total
        }

    def stream_benchmark_progress(self, run_id: str) -> Generator[dict[str, Any], None, None]:
        """Stream Server-Sent Events for real-time benchmark progress.

        Yields event dicts with heartbeat every 15 seconds to prevent proxy buffering.
        Event types: init, status_update, case_complete, progress, heartbeat, complete, tier_stats.
        """
        run_id = self.state._sanitize_path_component(run_id)

        # Validate run exists
        with self._lock:
            if self._current_run.get("runId") == run_id:
                run_state = self._current_run
                is_current = True
            else:
                summary = self.state.load_run_summary(run_id)
                if not summary:
                    raise ValueError(f"Run not found: {run_id}")
                run_dir = self.state.runs_dir / run_id
                run_state = self._historical_run_snapshot(run_id, summary, run_dir)
                is_current = False

        # Initialize event queue if not present on current run
        if is_current:
            with self._lock:
                if "_event_queue" not in self._current_run:
                    self._current_run["_event_queue"] = Queue()
                event_queue = self._current_run["_event_queue"]
        else:
            event_queue = None

        # Yield initial state event
        yield {
            "type": "init",
            "progress": {
                "completed": run_state.get("completed", 0),
                "total": run_state.get("total", 0),
                "percent": run_state.get("progressPercent", 0.0),
            },
            "timestamp": datetime.now().isoformat(),
        }

        # Stream events with heartbeat
        if is_current and event_queue is not None:
            while True:
                try:
                    event = event_queue.get(timeout=15)
                    yield event

                    # Check if run is complete
                    with self._lock:
                        status = self._current_run.get("status")
                    if status in ("completed", "stopped", "failed"):
                        yield {
                            "type": "complete",
                            "status": status,
                            "timestamp": datetime.now().isoformat(),
                        }
                        break
                except Empty:
                    # Timeout - send heartbeat
                    yield {
                        "type": "heartbeat",
                        "timestamp": datetime.now().isoformat(),
                    }

                    # Check if run stopped while we were waiting
                    with self._lock:
                        status = self._current_run.get("status")
                    if status in ("completed", "stopped", "failed"):
                        yield {
                            "type": "complete",
                            "status": status,
                            "timestamp": datetime.now().isoformat(),
                        }
                        break
        else:
            # Historical run - just send complete event
            yield {
                "type": "complete",
                "status": run_state.get("status", "completed"),
                "timestamp": datetime.now().isoformat(),
            }

    def start_doctor(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._doctor_state["busy"]:
                return {"doctor": self._doctor_snapshot()}
            tool = str(payload.get("tool") or self.state.default_run_config()["tool"]).strip()
            python_command = str(payload.get("python_command") or payload.get("pythonCommand") or "").strip()
            validation_backend = str(
                payload.get("validation_backend") or payload.get("validationBackend") or ""
            ).strip()
            self._doctor_state = {
                "busy": True,
                "mode": "doctor",
                "summary": self._doctor_intro_summary(tool, validation_backend),
                "results": [],
                "logs": [],
                "updatedAt": self.state.now_iso(),
            }
            self._doctor_thread = threading.Thread(
                target=self._doctor_worker,
                args=(tool, python_command, validation_backend),
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
            validation_backend = str(
                payload.get("validation_backend") or payload.get("validationBackend") or ""
            ).strip()
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
                args=(tool, python_command, validation_backend),
                daemon=True,
            )
            self._doctor_thread.start()
            return {"doctor": self._doctor_snapshot()}

    def _doctor_worker(self, tool: str, python_command: str, validation_backend: str) -> None:
        try:
            base_url = self.state.load_model_config(tool).base_url if tool else ""
            results = self.state.doctor_checks(tool, base_url, python_command, validation_backend)
            with self._lock:
                self._doctor_state["results"] = results
                self._doctor_state["summary"] = self._doctor_summary(results)
                self._doctor_state["updatedAt"] = self.state.now_iso()
        finally:
            with self._lock:
                self._doctor_state["busy"] = False

    def _doctor_fix_worker(self, tool: str, python_command: str, validation_backend: str) -> None:
        def log(message: str) -> None:
            with self._lock:
                self._doctor_state["logs"].append(message)
                self._doctor_state["logs"] = self._doctor_state["logs"][-250:]

        try:
            results = self.state.auto_fix_doctor_issues(
                tool, python_command, validation_backend, logger=log
            )
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
            # Temporarily remove unpicklable objects before deepcopy
            event_queue = self._current_run.pop("_event_queue", None)
            snapshot = deepcopy(self._current_run)
            # Restore event queue to original dict
            if event_queue is not None:
                self._current_run["_event_queue"] = event_queue
        snapshot.pop("_recentActivityLimit", None)
        snapshot.pop("_solveSecondsTotal", None)
        snapshot.pop("_validationSecondsTotal", None)
        snapshot.pop("_envCreateSecondsTotal", None)
        snapshot.pop("_installSecondsTotal", None)
        snapshot.pop("_smokeSecondsTotal", None)
        snapshot.pop("_solveSamples", None)
        snapshot.pop("_validationSamples", None)
        snapshot.pop("_envCreateSamples", None)
        snapshot.pop("_installSamples", None)
        snapshot.pop("_smokeSamples", None)
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
        run_contract = contract_from_sources(payload)
        preferred_tool = str(payload.get("tool") or "").strip() or None
        defaults = self.state.default_run_config(preferred_tool=preferred_tool)
        tool = str(payload.get("tool") or defaults["tool"] or "").strip()
        dataset_tar = str(payload.get("dataset_tar") or payload.get("datasetTar") or defaults["dataset_tar"]).strip()
        loop_count = int(payload.get("loop_count") or payload.get("loopCount") or defaults["loop_count"])
        search_range = int(payload.get("search_range") or payload.get("searchRange") or defaults["search_range"])
        snippet_limit = str(payload.get("snippet_limit") or payload.get("snippetLimit") or defaults["snippet_limit"]).strip()
        build_profile_value = (
            payload.get("build_profile")
            or payload.get("buildProfile")
            or run_contract.get("build_profile")
        )
        config = {
            "tool": tool,
            "dataset_tar": dataset_tar,
            "loop_count": loop_count,
            "search_range": search_range,
            "rag": self._as_bool(payload.get("rag", defaults["rag"])),
            "verbose": self._as_bool(payload.get("verbose", defaults["verbose"])),
            "snippet_limit": snippet_limit,
            "python_command": str(payload.get("python_command") or payload.get("pythonCommand") or defaults["python_command"]).strip(),
            "llm_only_mode": self._as_bool(
                payload.get("llm_only_mode", payload.get("llmOnlyMode", defaults["llm_only_mode"]))
            ),
            "validation_backend": self.state.normalize_validation_backend(
                tool,
                str(payload.get("validation_backend") or payload.get("validationBackend") or defaults["validation_backend"]).strip(),
            ),
            "llm_validation_policy": normalize_llm_validation_policy(
                payload.get("llm_validation_policy")
                or payload.get("llmValidationPolicy")
                or run_contract.get("llm_validation_policy")
                or defaults.get("llm_validation_policy")
            ),
            "loadout_name": str(payload.get("loadout_name") or payload.get("loadoutName") or "").strip(),
            "run_intent": normalize_run_intent(
                payload.get("run_intent")
                or payload.get("runIntent")
                or run_contract.get("run_intent")
                or defaults["run_intent"]
            ),
            "cache_state": normalize_cache_state(
                payload.get("cache_state")
                or payload.get("cacheState")
                or run_contract.get("cache_state")
                or defaults["cache_state"]
            ),
            "llm_context_window": normalize_context_window(
                payload.get("llm_context_window")
                or payload.get("llmContextWindow")
                or run_contract.get("llm_context_window")
                or defaults["llm_context_window"]
            ),
            "inference_policy": normalize_inference_policy(
                payload.get("inference_policy")
                or payload.get("inferencePolicy")
                or run_contract.get("inference_policy")
                or defaults["inference_policy"]
            ),
            "build_profile": normalize_build_profile(
                build_profile_value
                or defaults["build_profile"]
            ),
            "model": str(payload.get("model") or run_contract.get("model_name") or "").strip(),
            "base_url": str(payload.get("base_url") or payload.get("baseUrl") or run_contract.get("base_url") or "").strip(),
            "temperature": self._optional_float(payload.get("temperature") or payload.get("temp")),
            "workers": int(payload.get("workers", 0) or 0),
            "replay_manifest": str(
                payload.get("replay_manifest")
                or payload.get("replayManifest")
                or defaults.get("replay_manifest")
                or ""
            ).strip(),
            "run_contract": run_contract if run_contract else {},
        }
        if config["run_intent"] == "macos-replay" and not str(build_profile_value or "").strip():
            config["build_profile"] = "release"
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
        effective_workers, _ = determine_effective_worker_count(config)
        return {
            "status": "idle",
            "title": "APDR benchmark ready",
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
            "preflightWarnings": [],
            "effectiveWorkers": effective_workers,
            "completed": 0,
            "total": 0,
            "successes": 0,
            "failures": 0,
            "skipped": 0,
            "regularSuccesses": 0,
            "regularFailures": 0,
            "regularSkipped": 0,
            "llmSuccesses": 0,
            "llmFailures": 0,
            "llmSkipped": 0,
            "elapsedSeconds": 0.0,
            "elapsedLabel": "0m 00s",
            "passRate": "0.0%",
            "speed": "--",
            "solveAverage": "--",
            "validationAverage": "--",
            "envCreateAverage": "--",
            "installAverage": "--",
            "smokeAverage": "--",
            "eta": "--",
            "recentActivity": [],
            "completedCases": [],
            "llmCases": [],
            "_recentActivityLimit": 350,
            "_solveSecondsTotal": 0.0,
            "_validationSecondsTotal": 0.0,
            "_envCreateSecondsTotal": 0.0,
            "_installSecondsTotal": 0.0,
            "_smokeSecondsTotal": 0.0,
            "_solveSamples": 0,
            "_validationSamples": 0,
            "_envCreateSamples": 0,
            "_installSamples": 0,
            "_smokeSamples": 0,
            "totalLlmCalls": 0,
            "totalEnvBuilds": 0,
            "totalRetries": 0,
            "casesWithLlmRetries": 0,
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
                self._current_run["preflightWarnings"] = [
                    str(item)
                    for item in (message.get("preflight_warnings") or [])
                    if str(item).strip()
                ]
                self._current_run["effectiveWorkers"] = int(message.get("effective_workers") or 0)
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
                    self._current_run["title"] = "APDR benchmark resumed"
                    self._current_run["subtitle"] = (
                        "warning: historical results were restored; only the remaining cases will execute below."
                    )
                else:
                    self._current_run["statusText"] = (
                        f"Run directory: {self.state.relative_path(message['run_dir'])} | Total snippets: {message['total']}"
                    )
                    self._current_run["title"] = "APDR benchmark in progress"
                    self._current_run["subtitle"] = (
                        "warning: benchmark telemetry is live; monitor active cases, logs, and completed rows below."
                    )
                for warning in self._current_run["preflightWarnings"]:
                    self._append_activity(f"warning: replay preflight - {warning}")
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
                self._accumulate_phase_metrics(self._current_run, result)
                case_succeeded = self._result_succeeded(result)
                case_skipped = self._result_skipped(result)
                # Use tier instead of llm_calls to match frontend categorization
                is_llm_case = result.get("tier") == "tier3"
                self._current_run["completed"] = int(message["completed"])
                self._current_run["total"] = int(message["total"])
                if case_succeeded:
                    self._current_run["successes"] += 1
                    if is_llm_case:
                        self._current_run["llmSuccesses"] += 1
                    else:
                        self._current_run["regularSuccesses"] += 1
                elif case_skipped:
                    self._current_run["skipped"] += 1
                    if is_llm_case:
                        self._current_run["llmSkipped"] += 1
                    else:
                        self._current_run["regularSkipped"] += 1
                else:
                    self._current_run["failures"] += 1
                    if is_llm_case:
                        self._current_run["llmFailures"] += 1
                    else:
                        self._current_run["regularFailures"] += 1
                # Ensure completed stays consistent with actual counter totals
                actual_counted = (
                    self._current_run["successes"]
                    + self._current_run["failures"]
                    + self._current_run["skipped"]
                )
                if self._current_run["completed"] != actual_counted:
                    self._current_run["completed"] = actual_counted
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
                    f"warning: regular {self._current_run['regularSuccesses']}P/{self._current_run['regularFailures']}F/{self._current_run['regularSkipped']}S "
                    f"| llm {self._current_run['llmSuccesses']}P/{self._current_run['llmFailures']}F/{self._current_run['llmSkipped']}S "
                    f"| total {self._current_run['successes']}P/{self._current_run['failures']}F/{self._current_run['skipped']}S "
                    f"| {self._current_run['speed']} pace."
                )
                case_row = self._build_case_row(result)
                self._current_run["completedCases"].insert(0, case_row)
                self._record_llm_case(self._current_run, case_row)
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
                    "APDR benchmark complete" if message["status"] == "completed" else "APDR benchmark stopped"
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
                self._current_run["title"] = "APDR benchmark failed"
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
        self._current_run["solveAverage"] = self._format_phase_average(
            self._phase_average(
                float(self._current_run.get("_solveSecondsTotal") or 0.0),
                int(self._current_run.get("_solveSamples") or 0),
            )
        )
        self._current_run["validationAverage"] = self._format_phase_average(
            self._phase_average(
                float(self._current_run.get("_validationSecondsTotal") or 0.0),
                int(self._current_run.get("_validationSamples") or 0),
            )
        )
        self._current_run["envCreateAverage"] = self._format_phase_average(
            self._phase_average(
                float(self._current_run.get("_envCreateSecondsTotal") or 0.0),
                int(self._current_run.get("_envCreateSamples") or 0),
            )
        )
        self._current_run["installAverage"] = self._format_phase_average(
            self._phase_average(
                float(self._current_run.get("_installSecondsTotal") or 0.0),
                int(self._current_run.get("_installSamples") or 0),
            )
        )
        self._current_run["smokeAverage"] = self._format_phase_average(
            self._phase_average(
                float(self._current_run.get("_smokeSecondsTotal") or 0.0),
                int(self._current_run.get("_smokeSamples") or 0),
            )
        )
        self._current_run["eta"] = self._format_eta(eta_seconds)

    def _build_case_row(self, result: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
        snippet = str(result.get("snippet", ""))
        status = self._display_status(result)
        run_config = config or self._current_run["config"]
        case_id = self._extract_case_id(snippet)
        comparisons = self._baseline_comparisons(case_id, status)
        solve_seconds = self._result_phase_seconds(result, "solve")
        validation_seconds = self._result_phase_seconds(result, "validation")
        env_create_seconds = self._result_phase_seconds(result, "env_create")
        install_seconds = self._result_phase_seconds(result, "install")
        smoke_seconds = self._result_phase_seconds(result, "smoke")
        llm_calls = self._result_int_metric(result, "llm_calls")
        env_builds = self._result_int_metric(result, "env_builds")
        retries = self._result_int_metric(result, "retries")
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
            "solve": self._format_phase_average(solve_seconds),
            "validation": self._format_phase_average(validation_seconds),
            "envCreate": self._format_phase_average(env_create_seconds),
            "install": self._format_phase_average(install_seconds),
            "smoke": self._format_phase_average(smoke_seconds),
            "llmCalls": str(llm_calls),
            "envBuilds": str(env_builds),
            "retries": str(retries),
            "hadLlmRetry": retries > 0,
            "tier": result.get("tier", "unknown"),
            "confidence": result.get("confidence"),
            "cached": result.get("cached", False),
            "fallbackInvoked": self._result_fallback_invoked(result),
            "fallbackOutcome": self._result_fallback_outcome(result),
            "fallbackReason": self._result_fallback_reason(result),
            "validationBackend": self._result_validation_backend(result),
            "validationPath": self._result_validation_path(result),
            "requestedLlmValidationPolicy": self._result_requested_llm_validation_policy(result),
            "llmValidationRoute": self._result_llm_validation_route(result),
            "dockerStatus": self._result_docker_status(result),
            "dockerBypassReason": self._result_docker_bypass_reason(result),
            "dockerBypassNote": self._result_docker_bypass_note(result),
            "debugDir": self._result_debug_dir(result),
            "dockerPlanStatus": self._result_docker_plan_status(result),
            "dockerPlanPath": self._result_docker_plan_path(result),
            "dockerPlanAuthorship": self._result_docker_plan_authorship(result),
            "dockerPlanFallbackSections": self._result_docker_plan_fallback_sections(result),
            "authoredDockerfilePath": self._result_authored_dockerfile_path(result),
            "executedDockerfilePath": self._result_executed_dockerfile_path(result),
            "dockerBuildCommandPath": self._result_docker_build_command_path(result),
            "dockerRunCommandPath": self._result_docker_run_command_path(result),
            "executedImageRef": self._result_executed_image_ref(result),
            "imageHandoffVerified": self._result_image_handoff_verified(result),
            "imageInspectPath": self._result_image_inspect_path(result),
            "authoredPlanStatus": self._result_authored_plan_status(result),
            "authoredPlanPath": self._result_authored_plan_path(result),
            "authoredPlanAuthorship": self._result_authored_plan_authorship(result),
            "authoredPlanFallbackSections": self._result_authored_plan_fallback_sections(result),
            "intakeFailureClass": self._result_intake_failure_class(result),
            "intakeFailurePath": self._result_intake_failure_path(result),
            "escalatedBackend": self._result_escalated_backend(result),
            "failureFamily": self._result_failure_family(result),
            "failureBucket": self._result_failure_bucket(result),
            "skipCandidate": self._result_skip_candidate(result),
            "resultOrigin": self._result_origin(result),
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
            "validation_backend": "llm-only" if config.get("llm_only_mode") else config["validation_backend"],
            "llm_validation_policy": config["llm_validation_policy"],
            "llm_only_mode": config.get("llm_only_mode", False),
            "loadout_name": config["loadout_name"],
            "run_intent": config["run_intent"],
            "cache_state": config["cache_state"],
            "llm_context_window": config["llm_context_window"],
            "inference_policy": config["inference_policy"],
            "build_profile": config["build_profile"],
            "workers": config["workers"],
            "replay_manifest": config.get("replay_manifest", ""),
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
        run_contract = contract_from_sources(summary, config)
        results = self._summary_results(summary)
        historical_results = self._historical_summary_results(summary)
        live_results = self._live_summary_results(summary)
        completed = len(results)
        total = self._estimate_total_from_summary(summary, config, completed)
        successes = sum(1 for item in results if self._result_succeeded(item))
        skipped = sum(1 for item in results if self._result_skipped(item))
        failures = completed - successes - skipped
        live_completed = len(live_results)
        historical_completed = len(historical_results)
        live_successes = sum(1 for item in live_results if self._result_succeeded(item))
        live_skipped = sum(1 for item in live_results if self._result_skipped(item))
        live_failures = live_completed - live_successes - live_skipped
        # Split by deterministic (tier1/tier2) vs LLM (tier3) cases
        # Use tier instead of llm_calls to match frontend categorization logic
        llm_results = [r for r in results if r.get("tier") == "tier3"]
        regular_results = [r for r in results if r.get("tier") in ("tier1", "tier2")]
        regular_successes = sum(1 for r in regular_results if self._result_succeeded(r))
        regular_skipped = sum(1 for r in regular_results if self._result_skipped(r))
        regular_failures = len(regular_results) - regular_successes - regular_skipped
        llm_successes = sum(1 for r in llm_results if self._result_succeeded(r))
        llm_skipped = sum(1 for r in llm_results if self._result_skipped(r))
        llm_failures = len(llm_results) - llm_successes - llm_skipped
        phase_totals = self._phase_totals(results)
        llm_val_totals = self._llm_validation_totals(results)
        llm_cases = self._llm_case_rows(results, config)
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
                "runContract": run_contract,
                "preflightWarnings": [
                    str(item)
                    for item in (summary.get("preflight_warnings") or [])
                    if str(item).strip()
                ],
                "effectiveWorkers": int(
                    summary.get("effective_workers")
                    or determine_effective_worker_count(config)[0]
                ),
                "completed": completed,
                "total": total,
                "successes": successes,
                "failures": failures,
                "skipped": skipped,
                "historicalCompleted": historical_completed,
                "liveCompleted": live_completed,
                "liveSuccesses": live_successes,
                "liveFailures": live_failures,
                "liveSkipped": live_skipped,
                "liveOnlyAvailable": self._summary_has_separated_history(summary)
                or not bool(summary.get("resume_from_run_id")),
                "regularSuccesses": regular_successes,
                "regularFailures": regular_failures,
                "regularSkipped": regular_skipped,
                "llmSuccesses": llm_successes,
                "llmFailures": llm_failures,
                "llmSkipped": llm_skipped,
                "elapsedSeconds": round(elapsed, 2),
                "elapsedLabel": self._format_duration(elapsed) if elapsed > 0 else "0m 00s",
                "passRate": f"{pass_rate:0.1f}%",
                "speed": self._format_case_pace(case_pace),
                "solveAverage": self._format_phase_average(self._phase_average(*phase_totals["solve"])),
                "validationAverage": self._format_phase_average(self._phase_average(*phase_totals["validation"])),
                "envCreateAverage": self._format_phase_average(self._phase_average(*phase_totals["env_create"])),
                "installAverage": self._format_phase_average(self._phase_average(*phase_totals["install"])),
                "smokeAverage": self._format_phase_average(self._phase_average(*phase_totals["smoke"])),
                "eta": self._format_eta(eta_seconds),
                "recentActivity": self._historical_activity(run_id, summary, run_dir, completed, total, remaining),
                "completedCases": [self._build_case_row(item, config) for item in reversed(results)],
                "llmCases": llm_cases,
                "resumeAvailable": resume_available,
                "remaining": remaining,
                "_solveSecondsTotal": phase_totals["solve"][0],
                "_validationSecondsTotal": phase_totals["validation"][0],
                "_envCreateSecondsTotal": phase_totals["env_create"][0],
                "_installSecondsTotal": phase_totals["install"][0],
                "_smokeSecondsTotal": phase_totals["smoke"][0],
                "_solveSamples": phase_totals["solve"][1],
                "_validationSamples": phase_totals["validation"][1],
                "_envCreateSamples": phase_totals["env_create"][1],
                "_installSamples": phase_totals["install"][1],
                "_smokeSamples": phase_totals["smoke"][1],
                "totalLlmCalls": llm_val_totals["totalLlmCalls"],
                "totalEnvBuilds": llm_val_totals["totalEnvBuilds"],
                "totalRetries": llm_val_totals["totalRetries"],
                "casesWithLlmRetries": llm_val_totals["casesWithLlmRetries"],
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
        return sum(
            float(item.get("duration_seconds", 0.0))
            for item in self._summary_results(summary)
            if isinstance(item, dict)
        )

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
        llm_only_mode = self._as_bool(config.get("llm_only_mode"))
        validation_backend = self.state.normalize_validation_backend(
            tool, str(config.get("validation_backend") or "")
        )
        selected = self.state.load_model_config(tool) if tool else None
        run_contract = contract_from_sources(run, config)
        model_name = str(
            run_contract.get("model_name")
            or config.get("model")
            or (selected.model if selected else "not selected")
        )
        base_url = str(
            run_contract.get("base_url")
            or config.get("base_url")
            or (selected.base_url if selected else "--")
        )
        temperature = config.get("temperature")
        if temperature is None and selected is not None:
            temperature = selected.temperature
        contract_view = {
            "run_intent": str(run_contract.get("run_intent") or config.get("run_intent") or "baseline"),
            "execution_mode": str(run_contract.get("execution_mode") or determine_execution_mode(tool, validation_backend)),
            "cache_state": str(run_contract.get("cache_state") or config.get("cache_state") or "unknown"),
            "llm_validation_policy": str(
                run_contract.get("llm_validation_policy")
                or config.get("llm_validation_policy")
                or normalize_llm_validation_policy("")
            ),
            "llm_context_window": str(
                run_contract.get("llm_context_window") or config.get("llm_context_window") or "--"
            ),
            "inference_policy": str(
                run_contract.get("inference_policy")
                or config.get("inference_policy")
                or normalize_inference_policy("", temperature)
            ),
            "build_profile": str(run_contract.get("build_profile") or config.get("build_profile") or "standard"),
        }
        dataset_tar = str(config.get("dataset_tar") or self.state.default_dataset_tar)
        source_path = self._display_path(dataset_tar)
        target_label = self._strip_archive_suffix(dataset_tar)
        effective = (
            self.state.format_command(self.state.choose_runner(tool, str(config.get("python_command") or "")))
            if tool
            else "--"
        )
        effective_workers = int(
            run.get("effectiveWorkers")
            or determine_effective_worker_count(config)[0]
        )
        preflight_warnings = [
            str(item)
            for item in (
                run.get("preflightWarnings")
                or run.get("preflight_warnings")
                or (
                    collect_replay_preflight_warnings(config, self.state.tool_dir(tool))
                    if tool == "apdr"
                    else []
                )
            )
            if str(item).strip()
        ]
        jobs = run.get("total") or str(config.get("snippet_limit") or "all")
        artifacts = self.state.relative_path(run["runDir"]) if run.get("runDir") else "runs/pending"
        llm_validation_label = (
            "LLM resolver (legacy env-first control + Docker follow-up + agent fallback)"
            if contract_view["llm_validation_policy"] == "env-first"
            else "LLM resolver (docker-first required + env fallback + agent fallback)"
        )
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
            {"label": "Model", "value": model_name},
            {"label": "Effective", "value": effective},
            {"label": "LLM", "value": f"{base_url} [{model_name}]" if tool else "--"},
            {"label": "Run intent", "value": contract_view["run_intent"]},
            {"label": "Execution mode", "value": contract_view["execution_mode"]},
            {"label": "Cache state", "value": contract_view["cache_state"]},
            {"label": "Ctx window", "value": contract_view["llm_context_window"]},
            {"label": "Inference", "value": contract_view["inference_policy"]},
            {"label": "Build profile", "value": contract_view["build_profile"]},
            {"label": "Workers", "value": str(effective_workers)},
            {"label": "Jobs", "value": str(jobs)},
            {"label": "Artifacts", "value": artifacts},
        ]
        if contract_view["run_intent"] == "macos-replay" and preflight_warnings:
            fields.insert(
                21,
                {
                    "label": "Replay warnings",
                    "value": " | ".join(preflight_warnings),
                },
            )
        if tool == "apdr":
            validation_label = (
                "LLM-only resolver + Docker validation"
                if llm_only_mode and validation_backend == "docker"
                else "LLM-only resolver + local Python env validation"
                if llm_only_mode
                else "Docker build + run"
                if validation_backend == "docker"
                else llm_validation_label
                if validation_backend == "llm"
                else "local Python environments"
            )
            fields.insert(
                14,
                {
                    "label": "Validation",
                    "value": validation_label,
                },
            )
            if llm_only_mode:
                fields.insert(
                    15,
                    {
                        "label": "LLM mode",
                        "value": "llm-only",
                    },
                )
            elif validation_backend == "llm":
                fields.insert(
                    15,
                    {
                        "label": "LLM policy",
                        "value": contract_view["llm_validation_policy"],
                    },
                )
            if validation_backend in ("env", "llm") or (llm_only_mode and validation_backend == "env"):
                available, missing = self.state.apdr_local_interpreters()
                fields.insert(
                    16 if validation_backend == "llm" or (llm_only_mode and validation_backend == "env") else 15,
                    {
                        "label": "Py envs",
                        "value": self._compact_apdr_interpreter_label(available, missing),
                    },
                )
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

    def _format_phase_average(self, seconds: float | None) -> str:
        if seconds is None:
            return "--"
        return f"{seconds:0.2f}s"

    def _phase_average(self, total_seconds: float, samples: int) -> float | None:
        if samples <= 0:
            return None
        return total_seconds / samples

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

    def _safe_float(self, value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return float(text)
        except (TypeError, ValueError):
            return None

    def _result_int_metric(self, result: dict[str, Any], key: str) -> int:
        direct = result.get(key)
        if direct is not None:
            try:
                return max(0, int(direct))
            except (TypeError, ValueError):
                pass
        metadata = result.get("output_metadata")
        if isinstance(metadata, dict):
            text = str(metadata.get(key) or "").strip()
            if text:
                try:
                    return max(0, int(text))
                except (TypeError, ValueError):
                    pass
        return 0

    def _result_phase_seconds(self, result: dict[str, Any], phase: str) -> float | None:
        direct = self._safe_float(result.get(f"{phase}_duration_seconds"))
        if direct is not None:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return None
        millis = self._safe_float(metadata.get(f"{phase}_duration_ms"))
        if millis is None or millis < 0:
            return None
        return round(millis / 1000.0, 2)

    def _phase_totals(self, results: list[dict[str, Any]]) -> dict[str, tuple[float, int]]:
        totals: dict[str, tuple[float, int]] = {
            "solve": (0.0, 0),
            "validation": (0.0, 0),
            "env_create": (0.0, 0),
            "install": (0.0, 0),
            "smoke": (0.0, 0),
        }
        for result in results:
            for phase in ("solve", "validation", "env_create", "install", "smoke"):
                seconds = self._result_phase_seconds(result, phase)
                if seconds is None:
                    continue
                total, samples = totals[phase]
                totals[phase] = (total + seconds, samples + 1)
        return totals

    def _llm_validation_totals(self, results: list[dict[str, Any]]) -> dict[str, int]:
        total_llm_calls = 0
        total_env_builds = 0
        total_retries = 0
        cases_with_retries = 0
        for result in results:
            total_llm_calls += self._result_int_metric(result, "llm_calls")
            total_env_builds += self._result_int_metric(result, "env_builds")
            retries = self._result_int_metric(result, "retries")
            total_retries += retries
            if retries > 0:
                cases_with_retries += 1
        return {
            "totalLlmCalls": total_llm_calls,
            "totalEnvBuilds": total_env_builds,
            "totalRetries": total_retries,
            "casesWithLlmRetries": cases_with_retries,
        }

    def _llm_case_rows(self, results: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
        llm_cases: list[dict[str, Any]] = []
        for result in reversed(results):
            # Use tier to match frontend categorization
            if result.get("tier") != "tier3":
                continue
            llm_cases.append(self._build_case_row(result, config))
        return llm_cases

    def _record_llm_case(self, run: dict[str, Any], case_row: dict[str, Any]) -> None:
        # Use tier to match frontend categorization
        if case_row.get("tier") != "tier3":
            return
        llm_cases = [
            item
            for item in run.get("llmCases", [])
            if item.get("snippet") != case_row.get("snippet")
        ]
        llm_cases.insert(0, deepcopy(case_row))
        run["llmCases"] = llm_cases

    def _accumulate_phase_metrics(self, run: dict[str, Any], result: dict[str, Any]) -> None:
        for phase in ("solve", "validation", "env_create", "install", "smoke"):
            seconds = self._result_phase_seconds(result, phase)
            if seconds is None:
                continue
            total_key = f"_{phase}SecondsTotal"
            sample_key = f"_{phase}Samples"
            run[total_key] = float(run.get(total_key) or 0.0) + seconds
            run[sample_key] = int(run.get(sample_key) or 0) + 1
        llm_calls = self._result_int_metric(result, "llm_calls")
        env_builds = self._result_int_metric(result, "env_builds")
        retries = self._result_int_metric(result, "retries")
        run["totalLlmCalls"] = int(run.get("totalLlmCalls") or 0) + llm_calls
        run["totalEnvBuilds"] = int(run.get("totalEnvBuilds") or 0) + env_builds
        run["totalRetries"] = int(run.get("totalRetries") or 0) + retries
        if retries > 0:
            run["casesWithLlmRetries"] = int(run.get("casesWithLlmRetries") or 0) + 1

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
        validation_status = self._result_validation_status(result)
        if validation_status.startswith("skipped") or validation_status == "host-runtime-required":
            return True
        explicit = result.get("skipped")
        if explicit is not None:
            return bool(explicit)
        return False

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

    def _result_failure_family(self, result: dict[str, Any]) -> str:
        direct = str(result.get("failureFamily") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("failure_family") or "").strip()

    def _result_failure_bucket(self, result: dict[str, Any]) -> str:
        direct = str(result.get("failureBucket") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("failure_bucket") or "").strip()

    def _result_skip_candidate(self, result: dict[str, Any]) -> bool:
        direct = result.get("skipCandidate")
        if direct is not None:
            return self._as_bool(direct)
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return False
        return self._as_bool(metadata.get("skip_candidate"))

    def _result_origin(self, result: dict[str, Any]) -> str:
        direct = str(result.get("resultOrigin") or "").strip()
        if direct:
            return direct
        return "live"

    def _result_fallback_invoked(self, result: dict[str, Any]) -> bool:
        direct = result.get("fallbackInvoked")
        if direct is not None:
            return self._as_bool(direct)
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return False
        return self._as_bool(metadata.get("fallback_invoked"))

    def _result_fallback_outcome(self, result: dict[str, Any]) -> str:
        direct = str(result.get("fallbackOutcome") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("fallback_outcome") or "").strip()

    def _result_fallback_reason(self, result: dict[str, Any]) -> str:
        direct = str(result.get("fallbackReason") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("fallback_reason") or "").strip()

    def _result_validation_backend(self, result: dict[str, Any]) -> str:
        direct = str(result.get("validationBackend") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("validation_backend") or "").strip()

    def _result_validation_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("validationPath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("validation_path") or "").strip()

    def _result_requested_llm_validation_policy(self, result: dict[str, Any]) -> str:
        direct = str(result.get("requestedLlmValidationPolicy") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("requested_llm_validation_policy") or "").strip()

    def _result_llm_validation_route(self, result: dict[str, Any]) -> str:
        direct = str(result.get("llmValidationRoute") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("llm_validation_route") or "").strip()

    def _result_docker_status(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerStatus") or "").strip()
        if direct:
            return direct

        route = self._result_llm_validation_route(result)
        bypass_reason = self._result_docker_bypass_reason(result)
        validation_path = self._result_validation_path(result)
        escalated_backend = self._result_escalated_backend(result)

        if route == "env-first-control" or bypass_reason == "explicit env-first control policy":
            return "env-first control"
        if route == "env-first-host-runtime" or bypass_reason == "host-runtime pre-skip":
            return "host-runtime pre-skip"
        if route == "env-first-docker-bypass" or bypass_reason in (
            "docker cli unavailable",
            "docker daemon unavailable",
        ):
            return "bypassed"

        path_hops = {part.strip() for part in validation_path.split("->") if part.strip()}
        if route == "docker-first" or "docker" in path_hops or escalated_backend == "docker":
            return "attempted"
        return ""

    def _result_docker_bypass_reason(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerBypassReason") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("docker_bypass_reason") or "").strip()

    def _result_docker_bypass_note(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerBypassNote") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("docker_bypass_note") or metadata.get("docker_bypass_note_path") or "").strip()

    def _result_debug_dir(self, result: dict[str, Any]) -> str:
        direct = str(result.get("debugDir") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("debug_dir") or "").strip()

    def _result_docker_plan_status(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerPlanStatus") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("docker_plan_status") or "").strip()

    def _result_docker_plan_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerPlanPath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("docker_plan_path") or "").strip()

    def _result_docker_plan_authorship(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerPlanAuthorship") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("docker_plan_authorship") or "").strip()

    def _result_docker_plan_fallback_sections(self, result: dict[str, Any]) -> list[str]:
        direct = result.get("dockerPlanFallbackSections")
        if isinstance(direct, list):
            return [str(item).strip() for item in direct if str(item).strip()]
        if isinstance(direct, str) and direct.strip():
            return [item.strip() for item in direct.split(",") if item.strip()]
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return []
        raw = str(metadata.get("docker_plan_fallback_sections") or "").strip()
        if not raw:
            return []
        return [item.strip() for item in raw.split(",") if item.strip()]

    def _result_authored_dockerfile_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("authoredDockerfilePath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("authored_dockerfile_path") or "").strip()

    def _result_executed_dockerfile_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("executedDockerfilePath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("executed_dockerfile_path") or "").strip()

    def _result_docker_build_command_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerBuildCommandPath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("docker_build_command_path") or "").strip()

    def _result_docker_run_command_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("dockerRunCommandPath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("docker_run_command_path") or "").strip()

    def _result_executed_image_ref(self, result: dict[str, Any]) -> str:
        direct = str(result.get("executedImageRef") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("executed_image_ref") or "").strip()

    def _result_image_handoff_verified(self, result: dict[str, Any]) -> bool:
        direct = result.get("imageHandoffVerified")
        if direct is not None:
            return self._as_bool(direct)
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return False
        return self._as_bool(metadata.get("image_handoff_verified"))

    def _result_image_inspect_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("imageInspectPath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("image_inspect_path") or "").strip()

    def _result_authored_plan_status(self, result: dict[str, Any]) -> str:
        direct = str(result.get("authoredPlanStatus") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("authored_plan_status") or "").strip()

    def _result_authored_plan_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("authoredPlanPath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("authored_plan_path") or "").strip()

    def _result_authored_plan_authorship(self, result: dict[str, Any]) -> str:
        direct = str(result.get("authoredPlanAuthorship") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("authored_plan_authorship") or "").strip()

    def _result_authored_plan_fallback_sections(self, result: dict[str, Any]) -> list[str]:
        direct = result.get("authoredPlanFallbackSections")
        if isinstance(direct, list):
            return [str(item).strip() for item in direct if str(item).strip()]
        if isinstance(direct, str) and direct.strip():
            return [item.strip() for item in direct.split(",") if item.strip()]
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return []
        raw = str(metadata.get("authored_plan_fallback_sections") or "").strip()
        if not raw:
            return []
        return [item.strip() for item in raw.split(",") if item.strip()]

    def _result_intake_failure_class(self, result: dict[str, Any]) -> str:
        direct = str(result.get("intakeFailureClass") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("intake_failure_class") or "").strip()

    def _result_intake_failure_path(self, result: dict[str, Any]) -> str:
        direct = str(result.get("intakeFailurePath") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("intake_failure_path") or "").strip()

    def _result_escalated_backend(self, result: dict[str, Any]) -> str:
        direct = str(result.get("escalatedBackend") or "").strip()
        if direct:
            return direct
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return ""
        return str(metadata.get("escalated_backend") or "").strip()

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
        # Case succeeded but no requirements — show "(no deps)" instead of
        # falling back to the output filename which is confusing.
        return "(no deps)"

    def _result_requirements(self, snippet: str, result: dict[str, Any]) -> list[str]:
        inline = [str(item).strip() for item in result.get("requirements", []) if str(item).strip()]
        if inline:
            return inline
        if not snippet:
            return []
        # Prefer artifact_dir (APDR --output-dir) over snippet parent directory
        artifact_dir = result.get("artifact_dir")
        if artifact_dir:
            artifact_path = self._repo_relative_path(str(artifact_dir))
            req_path = artifact_path / "requirements.txt"
            if req_path.exists():
                try:
                    return [
                        line.strip()
                        for line in req_path.read_text(encoding="utf-8").splitlines()
                        if line.strip() and not line.lstrip().startswith("#")
                    ]
                except OSError:
                    pass
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

    def _summary_has_separated_history(self, summary: dict[str, Any]) -> bool:
        return isinstance(summary.get("historical_results"), list)

    def _normalize_summary_result_rows(
        self,
        items: Any,
        default_origin: str,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            result = dict(item)
            snippet = str(result.get("snippet") or "").strip()
            if self._is_artifact_snippet(snippet):
                continue
            result["resultOrigin"] = str(result.get("resultOrigin") or default_origin).strip() or default_origin
            result["succeeded"] = self._result_succeeded(result)
            result["skipped"] = self._result_skipped(result)
            results.append(result)
        return results

    def _historical_summary_results(self, summary: dict[str, Any]) -> list[dict[str, Any]]:
        if not self._summary_has_separated_history(summary):
            return []
        return self._normalize_summary_result_rows(summary.get("historical_results", []), "historical")

    def _live_summary_results(self, summary: dict[str, Any]) -> list[dict[str, Any]]:
        if summary.get("resume_from_run_id") and not self._summary_has_separated_history(summary):
            return []
        return self._normalize_summary_result_rows(summary.get("results", []), "live")

    def _summary_results(self, summary: dict[str, Any]) -> list[dict[str, Any]]:
        results = self._historical_summary_results(summary)
        live_rows = self._live_summary_results(summary)
        if live_rows:
            results.extend(live_rows)
            return results
        if self._summary_has_separated_history(summary):
            return results
        default_origin = "unknown" if summary.get("resume_from_run_id") else "live"
        return self._normalize_summary_result_rows(summary.get("results", []), default_origin)

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

    def _doctor_intro_summary(self, tool: str, validation_backend: str = "") -> str:
        if tool == "apdr":
            resolved_backend = self.state.normalize_validation_backend(tool, validation_backend)
            if resolved_backend == "docker":
                return "Doctor is checking Docker, Ollama, dataset readiness, and each tool runtime."
            if resolved_backend == "llm":
                return (
                    "Doctor is checking Docker-first llm readiness, local Python env fallback "
                    "readiness, the LLM agent, Ollama, dataset readiness, and each tool runtime."
                )
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
