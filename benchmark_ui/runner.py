from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Any
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

from .state import AppState
from .run_contract import (
    build_run_contract,
    missing_required_keys,
    normalize_build_profile,
    normalize_cache_state,
    normalize_llm_validation_policy,
    normalize_run_intent,
)


def load_replay_manifest(manifest_path: str | Path) -> dict[str, Any]:
    """Load and validate a replay-slice manifest JSON file.

    Returns the parsed manifest dict which must contain at minimum
    ``slice_id`` (str) and ``cases`` (list of dicts with ``relative_path``).
    Raises ``ValueError`` if the manifest is structurally invalid or
    ``FileNotFoundError`` if the path does not exist.
    """
    path = Path(manifest_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Replay manifest not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Replay manifest must be a JSON object: {path}")
    if not data.get("slice_id"):
        raise ValueError(f"Replay manifest is missing 'slice_id': {path}")
    cases = data.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"Replay manifest must contain a non-empty 'cases' array: {path}")
    for idx, entry in enumerate(cases):
        if not isinstance(entry, dict) or not entry.get("relative_path"):
            raise ValueError(
                f"Replay manifest case {idx} must have a 'relative_path' field: {path}"
            )
    return data


def filter_snippets_by_manifest(
    snippets: list[Path],
    manifest: dict[str, Any],
    dataset_dir: Path,
) -> list[Path]:
    """Filter and reorder *snippets* to match the manifest case list.

    Returns snippets in manifest order.  Raises ``ValueError`` if any
    manifest-referenced snippet is missing from the discovered set.
    """
    # Build a lookup from normalized relative path -> absolute snippet path.
    lookup: dict[str, Path] = {}
    dataset_root_name = dataset_dir.name.strip()
    for snippet in snippets:
        try:
            rel = str(snippet.relative_to(dataset_dir)).replace("\\", "/")
        except ValueError:
            rel = str(snippet).replace("\\", "/")
        lookup[rel] = snippet
        if dataset_root_name:
            lookup.setdefault(f"{dataset_root_name}/{rel}", snippet)
        # Also index by parent-dir/snippet.py for flat layouts.
        parts = rel.split("/")
        if len(parts) >= 2:
            short_key = "/".join(parts[-2:])
            lookup.setdefault(short_key, snippet)
            if dataset_root_name:
                lookup.setdefault(f"{dataset_root_name}/{short_key}", snippet)

    ordered: list[Path] = []
    missing: list[str] = []
    for entry in manifest["cases"]:
        rel_path = entry["relative_path"]
        # Try exact match first, then suffix match.
        match = lookup.get(rel_path)
        if match is None:
            # Try with snippet.py appended if the manifest lists just the dir.
            if not rel_path.endswith(".py"):
                match = lookup.get(rel_path.rstrip("/") + "/snippet.py")
        if match is None:
            missing.append(rel_path)
        else:
            ordered.append(match)
    if missing:
        raise ValueError(
            f"Replay manifest references {len(missing)} snippets not found in "
            f"{dataset_dir}: {', '.join(missing[:10])}"
        )
    return ordered

# WSL mount prefix pattern: /mnt/<drive>/...
_WSL_MNT_RE = re.compile(r"^/mnt/([a-zA-Z])(/.*)?$")
LLM_ONLY_MAX_WORKERS = 2
MACOS_REPLAY_MAX_WORKERS = 4


def _normalize_path_for_native(p: Path) -> Path:
    """Translate WSL /mnt/X/... paths to Windows X:\\ paths when running on Windows.

    When the benchmark UI is launched from WSL but the APDR binary is a native
    Windows executable, dataset paths like /mnt/d/apdr/hard-gists need to become
    D:\\apdr\\hard-gists so the Windows binary can find them.
    """
    if os.name != "nt":
        return p
    s = str(p)
    # Also handle forward-slash paths that sneak through on Windows
    m = _WSL_MNT_RE.match(s) or _WSL_MNT_RE.match(s.replace("\\", "/"))
    if m:
        drive = m.group(1).upper()
        rest = (m.group(2) or "").replace("/", "\\")
        return Path(f"{drive}:{rest}")
    return p


def detect_rosetta_translation() -> bool:
    """Return True when the current macOS process is translated by Rosetta 2."""
    if sys.platform != "darwin":
        return False
    try:
        completed = subprocess.run(
            ["sysctl", "-n", "sysctl.proc_translated"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0 and completed.stdout.strip() == "1"


def detect_requested_apdr_binary(tool_dir: Path, build_profile: str) -> tuple[Path | None, list[str]]:
    """Find the APDR binary matching the requested build profile."""
    profile = normalize_build_profile(build_profile)
    release_binary = tool_dir / "target" / "release" / "apdr"
    debug_binary = tool_dir / "target" / "debug" / "apdr"
    release_binary_windows = tool_dir / "target" / "release" / "apdr.exe"
    debug_binary_windows = tool_dir / "target" / "debug" / "apdr.exe"

    if profile in {"release", "pgo"}:
        preferred = [release_binary, release_binary_windows]
        fallback = [debug_binary, debug_binary_windows]
    elif profile == "debug":
        preferred = [debug_binary, debug_binary_windows]
        fallback = [release_binary, release_binary_windows]
    else:
        preferred = [release_binary, release_binary_windows, debug_binary, debug_binary_windows]
        fallback = []

    for candidate in preferred:
        if candidate.exists():
            return candidate, []

    for candidate in fallback:
        if candidate.exists():
            return (
                candidate,
                [
                    f"Requested build_profile={profile} but only found "
                    f"{candidate.relative_to(tool_dir)}. Replay evidence may mix "
                    "optimization levels."
                ],
            )

    return (
        None,
        [
            f"No prebuilt APDR binary found for build_profile={profile}. "
            "Replay evidence would fall back to cargo run and include build overhead."
        ],
    )


def determine_effective_worker_count(
    run_config: dict[str, Any],
    cpu_count: int | None = None,
) -> tuple[int, list[str]]:
    requested = int(run_config.get("workers", 0) or 0)
    resolved_cpu_count = max(1, int(cpu_count or os.cpu_count() or 4))
    run_intent = normalize_run_intent(run_config.get("run_intent"))
    llm_only_mode = bool(run_config.get("llm_only_mode"))
    if llm_only_mode:
        auto_workers = min(LLM_ONLY_MAX_WORKERS, max(1, resolved_cpu_count - 2))
        if requested <= 0:
            return auto_workers, []
        if requested > LLM_ONLY_MAX_WORKERS:
            return (
                LLM_ONLY_MAX_WORKERS,
                [
                    f"llm-only capped requested workers={requested} to "
                    f"{LLM_ONLY_MAX_WORKERS} to bound local LLM concurrency."
                ],
            )
        return requested, []
    if run_intent == "macos-replay":
        if requested <= 0:
            return 1, []
        if requested > MACOS_REPLAY_MAX_WORKERS:
            return (
                MACOS_REPLAY_MAX_WORKERS,
                [
                    f"macos-replay capped requested workers={requested} to "
                    f"{MACOS_REPLAY_MAX_WORKERS} to avoid native env-cache and disk thrash."
                ],
            )
        return requested, []
    if requested <= 0:
        return max(1, resolved_cpu_count - 2), []
    return requested, []


def collect_replay_preflight_warnings(run_config: dict[str, Any], tool_dir: Path) -> list[str]:
    run_intent = normalize_run_intent(run_config.get("run_intent"))
    if run_intent != "macos-replay":
        return []

    warnings: list[str] = []
    if sys.platform != "darwin":
        warnings.append(
            f"run_intent=macos-replay is executing on {sys.platform}, not macOS. "
            "Treat this capture as non-comparable replay evidence."
        )
    elif detect_rosetta_translation():
        warnings.append(
            "Running under Rosetta 2 translation. Timings will not reflect native macOS ARM64 replay performance."
        )

    validation_backend = str(run_config.get("validation_backend") or "").strip().lower()
    if validation_backend != "env":
        warnings.append(
            f"macos-replay is using validation_backend={validation_backend}. "
            "Comparable replay evidence should use the native env backend."
        )

    cache_state = normalize_cache_state(run_config.get("cache_state"))
    if cache_state not in {"warm", "cold"}:
        warnings.append(
            f"macos-replay reported cache_state={cache_state}. "
            "Use explicit warm or cold cache state before treating the capture as comparable."
        )

    build_profile = normalize_build_profile(run_config.get("build_profile"))
    if build_profile == "standard":
        warnings.append(
            "macos-replay did not pin an explicit build_profile. Use release or pgo for comparable replay evidence."
        )
    _, binary_warnings = detect_requested_apdr_binary(tool_dir, build_profile)
    warnings.extend(binary_warnings)

    _, worker_warnings = determine_effective_worker_count(run_config)
    warnings.extend(worker_warnings)
    return warnings


class BenchmarkWorker(threading.Thread):
    def __init__(self, state: AppState, run_config: dict[str, Any], message_queue: Any) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.run_config = run_config
        self.message_queue = message_queue
        self.stop_requested = threading.Event()
        self._active_processes: set[subprocess.Popen[str]] = set()
        self._active_lock = threading.Lock()
        self._summary_lock = threading.Lock()
        self.run_dir: Path | None = None

    # Keep old attribute for backward compat
    @property
    def current_process(self) -> subprocess.Popen[str] | None:
        with self._active_lock:
            return next(iter(self._active_processes), None)

    @current_process.setter
    def current_process(self, value: subprocess.Popen[str] | None) -> None:
        pass  # no-op; managed via _active_processes

    def stop(self, timeout: float = 15) -> None:
        """Signal all active processes to stop and wait until they're dead.

        Sets the stop flag first so the worker loop won't spawn new work,
        then kills every active subprocess tree in parallel.
        """
        self.stop_requested.set()
        with self._active_lock:
            procs = [p for p in self._active_processes if p.poll() is None]
        # Kill in parallel threads so we don't wait sequentially per process
        kill_threads = []
        for proc in procs:
            t = threading.Thread(target=self._terminate_process, args=(proc,), daemon=True)
            t.start()
            kill_threads.append(t)
        deadline = time.monotonic() + timeout
        for t in kill_threads:
            remaining = max(0, deadline - time.monotonic())
            t.join(timeout=remaining)
        # Kill orphaned processes that escape the process tree.
        # docker-buildx.exe is spawned by Docker Desktop, not as a child of
        # docker.exe, so taskkill /T doesn't reach it.
        self._kill_orphaned_processes()

    def run(self) -> None:
        summary: dict[str, Any] = {}
        try:
            tool = str(self.run_config["tool"])
            if not tool:
                raise ValueError("Choose a tool before starting a benchmark.")

            tool_dir = self.state.tool_dir(tool)
            if not tool_dir.exists():
                raise FileNotFoundError(f"Tool directory not found: {tool_dir}")

            runner = self.state.choose_runner(tool, str(self.run_config.get("python_command", "")))
            self._emit("status", text="Validating tool runtime...")
            runtime_ok, runtime_detail, runtime_runner = self.state.validate_tool_runtime(
                tool,
                str(self.run_config.get("python_command", "")),
                str(self.run_config.get("validation_backend", "")),
            )
            if not runtime_ok:
                raise RuntimeError(
                    "The selected tool runtime is missing required Python packages.\n"
                    f"Runner: {self.state.format_command(runtime_runner)}\n"
                    f"Details: {runtime_detail}\n"
                    "Install the tool environment first, or set 'Python command override' in the UI "
                    "to a Python interpreter that already has the tool dependencies installed."
                )

            model_config = self.state.load_model_config(tool)
            selected_model = str(self.run_config.get("model") or model_config.model)
            selected_base_url = str(self.run_config.get("base_url") or model_config.base_url)
            selected_temperature = float(self.run_config.get("temperature") or model_config.temperature)
            dataset_tar = _normalize_path_for_native(
                Path(str(self.run_config["dataset_tar"])).expanduser().resolve()
            )
            self._emit("status", text=f"Preparing dataset from {self.state.relative_path(dataset_tar)}")
            dataset_dir = _normalize_path_for_native(
                self.state.ensure_dataset_extracted(dataset_tar)
            )
            self._emit("status", text="Discovering snippets...")
            snippets = self.state.snippet_files(dataset_dir)

            # Apply replay manifest if specified — overrides snippet_limit.
            replay_manifest_path = str(self.run_config.get("replay_manifest") or "").strip()
            replay_manifest: dict[str, Any] | None = None
            replay_slice_id = ""
            if replay_manifest_path:
                replay_manifest = load_replay_manifest(replay_manifest_path)
                replay_slice_id = str(replay_manifest.get("slice_id", ""))
                snippets = filter_snippets_by_manifest(snippets, replay_manifest, dataset_dir)
            else:
                snippet_limit = self._parse_limit(self.run_config.get("snippet_limit", ""))
                if snippet_limit:
                    snippets = snippets[:snippet_limit]

            resume_results = [
                {**dict(item), "resultOrigin": str(dict(item).get("resultOrigin") or "historical")}
                for item in (self.run_config.get("_resume_results") or [])
            ]
            resume_lookup = {
                str(item.get("snippet")).strip()
                for item in resume_results
                if str(item.get("snippet") or "").strip()
            }
            if resume_lookup:
                snippets = [snippet for snippet in snippets if self.state.relative_path(snippet) not in resume_lookup]
            resumed_completed = len(resume_results)
            resumed_successes = sum(1 for item in resume_results if self._result_succeeded(item))
            resumed_skips = sum(1 for item in resume_results if self._result_skipped(item))
            resumed_failures = resumed_completed - resumed_successes - resumed_skips
            total_snippets = resumed_completed + len(snippets)

            if not snippets:
                if resumed_completed:
                    raise ValueError("Selected run has no remaining snippets to resume.")
                raise ValueError(f"No snippet.py files found in {dataset_dir}")

            self.run_dir = self._create_run_dir(tool)
            context_log = self.run_dir / "benchmark-context.log"
            context_log.touch(exist_ok=True)
            run_contract = build_run_contract(
                repo_root=self.state.repo_root,
                tool=tool,
                model_name=selected_model,
                base_url=selected_base_url,
                temperature=selected_temperature,
                validation_backend=str(self.run_config.get("validation_backend", "")),
                run_config=self.run_config,
                runner_command=runner,
            )
            missing_contract_keys = missing_required_keys(run_contract)
            if missing_contract_keys:
                raise RuntimeError(
                    f"Incomplete benchmark run contract: {', '.join(missing_contract_keys)}"
                )
            effective_workers, _ = determine_effective_worker_count(self.run_config)
            preflight_warnings = collect_replay_preflight_warnings(self.run_config, tool_dir)

            summary: dict[str, Any] = {
                "tool": tool,
                "model": selected_model,
                "base_url": selected_base_url,
                "temperature": selected_temperature,
                "dataset_tar": str(dataset_tar),
                "dataset_dir": str(dataset_dir),
                "loop_count": int(self.run_config["loop_count"]),
                "search_range": int(self.run_config["search_range"]),
                "rag": bool(self.run_config["rag"]),
                "verbose": bool(self.run_config["verbose"]),
                "snippet_limit": (snippet_limit if not replay_manifest_path else "") or "",
                "python_command": str(self.run_config.get("python_command", "")),
                "validation_backend": str(self.run_config.get("validation_backend", "")),
                "llm_only_mode": bool(self.run_config.get("llm_only_mode")),
                "llm_validation_policy": normalize_llm_validation_policy(
                    self.run_config.get("llm_validation_policy")
                ),
                "run_intent": run_contract["run_intent"],
                "cache_state": run_contract["cache_state"],
                "build_profile": run_contract["build_profile"],
                "workers": int(self.run_config.get("workers", 0) or 0),
                "effective_workers": effective_workers,
                "preflight_warnings": list(preflight_warnings),
                "started_at": self.state.now_iso(),
                "status": "running",
                "historical_results": resume_results,
                "results": [],
                "benchmark_context_log": self.state.relative_path(context_log),
                "run_contract": run_contract,
            }
            if replay_manifest_path:
                summary["replay_manifest"] = replay_manifest_path
                summary["replay_slice_id"] = replay_slice_id
            if self.run_config.get("_resume_from_run_id"):
                summary["resume_from_run_id"] = str(self.run_config["_resume_from_run_id"])
                summary["resumed_results"] = resumed_completed
            self._persist_run_contract(summary, run_contract)
            self._write_summary(summary)
            run_contract_path = self.run_dir / "run_contract.json"
            self._emit(
                "plan",
                total=total_snippets,
                run_dir=str(self.run_dir),
                resumed_completed=resumed_completed,
                resumed_successes=resumed_successes,
                resumed_failures=resumed_failures,
                resumed_skips=resumed_skips,
                resumed_run_id=str(self.run_config.get("_resume_from_run_id") or ""),
                run_contract=run_contract,
                effective_workers=effective_workers,
                preflight_warnings=preflight_warnings,
            )
            self._append_context_log(
                context_log,
                "benchmark-start",
                "\n".join(
                    [
                        f"tool={tool}",
                        f"model={selected_model}",
                        f"base_url={selected_base_url}",
                        f"dataset={self.state.relative_path(dataset_tar)}",
                        f"total_snippets={total_snippets}",
                        f"resumed_completed={resumed_completed}",
                        f"effective_workers={effective_workers}",
                        f"preflight_warnings={json.dumps(preflight_warnings)}",
                    ]
                ),
            )

            case_artifacts_root = self.run_dir / "cases" if self.run_dir else None
            if case_artifacts_root is not None:
                case_artifacts_root.mkdir(parents=True, exist_ok=True)

            workers = effective_workers

            # Pre-build per-case command + metadata
            case_tasks: list[dict[str, Any]] = []
            for index, snippet in enumerate(snippets, start=1):
                overall_index = resumed_completed + index
                snippet_label = self.state.relative_path(snippet)
                artifact_dir = None
                build_profile = str(run_contract.get("build_profile") or "standard")
                command = list(runner) + [
                    "test_executor.py",
                    "-f",
                    str(snippet),
                    "-m",
                    selected_model,
                    "-b",
                    selected_base_url,
                    "-t",
                    str(selected_temperature),
                    "-l",
                    str(int(self.run_config["loop_count"])),
                    "-r",
                    str(int(self.run_config["search_range"])),
                    "-ra",
                    "true" if self.run_config["rag"] else "false",
                ]
                if tool == "apdr":
                    vb = str(self.run_config.get("validation_backend") or "env")
                    is_llm_only = vb == "llm-only" or self.run_config.get("llm_only_mode")
                    command.extend(
                        [
                            "--validation-backend",
                            "docker" if is_llm_only else vb,
                        ]
                    )
                    if vb == "llm":
                        command.extend(
                            [
                                "--llm-validation-policy",
                                normalize_llm_validation_policy(
                                    self.run_config.get("llm_validation_policy")
                                ),
                            ]
                        )
                    command.extend(["--build-profile", build_profile])
                    if is_llm_only:
                        command.append("--llm-only")
                    vt = self.run_config.get("validation_timeout")
                    if vt and int(vt) > 0:
                        command.extend(["--validation-timeout", str(int(vt))])
                if self.run_config["verbose"]:
                    command.append("-v")
                command.extend(["--benchmark-context-log", str(context_log)])

                if tool == "apdr" and case_artifacts_root is not None:
                    artifact_dir = case_artifacts_root / self._case_id_from_snippet(snippet)
                    artifact_dir.mkdir(parents=True, exist_ok=True)
                    command.extend(["--output-dir", str(artifact_dir)])
                    command.extend(["--run-contract-json", str(run_contract_path)])
                    command.append("--no-execute-snippet")

                case_tasks.append({
                    "command": command,
                    "snippet": snippet,
                    "snippet_label": snippet_label,
                    "overall_index": overall_index,
                    "artifact_dir": artifact_dir,
                })

            # Track completion count for progress reporting
            completed_count = [resumed_completed]
            completed_lock = threading.Lock()

            if workers == 1:
                # Sequential mode: same behavior as before
                for task in case_tasks:
                    if self.stop_requested.is_set():
                        break
                    self._emit("status", text=f"Running {task['snippet_label']} ({task['overall_index']}/{total_snippets})")
                    self._emit("command", text=self.state.format_command(task["command"]))
                    self._append_context_log(
                        context_log,
                        "case-start",
                        f"index={task['overall_index']}/{total_snippets}\nsnippet={task['snippet_label']}\ncommand={self.state.format_command(task['command'])}",
                    )
                    result = self._run_single(tool, tool_dir, task["command"], task["snippet"], task["overall_index"], total_snippets, task["artifact_dir"])
                    with self._summary_lock:
                        summary["results"].append(result)
                        self._write_summary(summary)
                        # Emit tier_stats event after case completion
                        self._emit_tier_stats_event([
                            *summary.get("historical_results", []),
                            *summary["results"],
                        ])
                    self._append_context_log(
                        context_log,
                        "case-finished",
                        json.dumps(result, indent=2, sort_keys=True),
                    )
                    self._emit(
                        "progress",
                        completed=task["overall_index"],
                        total=total_snippets,
                        snippet=task["snippet_label"],
                        returncode=result["returncode"],
                        duration=result["duration_seconds"],
                        result=result,
                    )
            else:
                # Parallel mode: use ThreadPoolExecutor
                self._emit("status", text=f"Running {len(case_tasks)} cases with {workers} parallel workers")
                pool = ThreadPoolExecutor(max_workers=workers)
                try:
                    future_to_task: dict[Future[dict[str, Any]], dict[str, Any]] = {}
                    for task in case_tasks:
                        if self.stop_requested.is_set():
                            break
                        future = pool.submit(
                            self._run_single,
                            tool, tool_dir, task["command"], task["snippet"],
                            task["overall_index"], total_snippets, task["artifact_dir"],
                        )
                        future_to_task[future] = task

                    for future in as_completed(future_to_task):
                        if self.stop_requested.is_set():
                            # Cancel pending futures
                            for f in future_to_task:
                                f.cancel()
                            break
                        task = future_to_task[future]
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = {
                                "snippet": task["snippet_label"],
                                "started_at": self.state.now_iso(),
                                "finished_at": self.state.now_iso(),
                                "duration_seconds": 0.0,
                                "returncode": -1,
                                "succeeded": False,
                                "skipped": False,
                                "requirements": [],
                                "output_metadata": {},
                                "log_lines_streamed": 0,
                                "log_tail": [str(exc)],
                                "output_files": [],
                                "solve_duration_seconds": None,
                                "validation_duration_seconds": None,
                                "env_create_duration_seconds": None,
                                "install_duration_seconds": None,
                                "smoke_duration_seconds": None,
                                "llm_calls": 0,
                                "env_builds": 0,
                                "retries": 0,
                            }

                        with self._summary_lock:
                            summary["results"].append(result)
                            self._write_summary(summary)
                            # Emit tier_stats event after case completion
                            self._emit_tier_stats_event([
                                *summary.get("historical_results", []),
                                *summary["results"],
                            ])

                        with completed_lock:
                            completed_count[0] += 1
                            current_completed = completed_count[0]

                        self._append_context_log(
                            context_log,
                            "case-finished",
                            json.dumps(result, indent=2, sort_keys=True),
                        )
                        self._emit(
                            "progress",
                            completed=current_completed,
                            total=total_snippets,
                            snippet=task["snippet_label"],
                            returncode=result["returncode"],
                            duration=result["duration_seconds"],
                            result=result,
                        )
                finally:
                    # cancel_futures=True (Python 3.9+) prevents queued work
                    # from starting; running workers finish after _terminate_process
                    # kills their subprocesses via the stop_requested check.
                    pool.shutdown(wait=True, cancel_futures=True)

            if self.stop_requested.is_set():
                summary["status"] = "stopped"
            else:
                summary["status"] = "completed"
            summary["finished_at"] = self.state.now_iso()
            self._write_summary(summary)
            self._append_context_log(
                context_log,
                "benchmark-finished",
                json.dumps(
                    {
                        "status": summary["status"],
                        "finished_at": summary["finished_at"],
                        "completed": len(summary.get("historical_results", [])) + len(summary["results"]),
                    },
                    indent=2,
                    sort_keys=True,
                ),
            )
            self._emit("done", status=summary["status"], run_dir=str(self.run_dir), total=total_snippets)
        except Exception as exc:
            if summary:
                summary["status"] = "failed"
                summary["finished_at"] = self.state.now_iso()
                summary["error"] = str(exc)
                self._write_summary(summary)
            self._emit("error", message=str(exc), trace=traceback.format_exc())

    def _run_single(
        self,
        tool: str,
        tool_dir: Path,
        command: list[str],
        snippet: Path,
        index: int,
        total: int,
        artifact_dir: Path | None = None,
    ) -> dict[str, Any]:
        # Get event queue from current run if available
        event_queue: Queue[dict[str, Any]] | None = None
        if hasattr(self, '_current_run_event_queue'):
            event_queue = self._current_run_event_queue

        def emit_event(event_type: str, **kwargs: Any) -> None:
            """Emit progress event to SSE queue if available."""
            if event_queue is None:
                return
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            try:
                event_queue.put_nowait(event)
            except Exception:
                # Best-effort streaming - don't block runner on queue errors
                pass

        started_at = time.time()
        started_iso = self.state.now_iso()
        output_root = artifact_dir if artifact_dir is not None else snippet.parent

        # Extract case_id for event emission
        case_id = self._case_id_from_snippet(snippet)

        # Emit status_update: case starting
        emit_event("status_update", caseId=case_id, status="running")
        existing_outputs = {path.resolve() for path in output_root.glob("output_data_*.yml")}
        requirements_path = output_root / "requirements.txt"
        existing_requirements_mtime = requirements_path.stat().st_mtime if requirements_path.exists() else None
        popen_kwargs: dict[str, Any] = {
            "cwd": tool_dir,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
            "bufsize": 1,
        }
        if os.name == "nt":
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        else:
            popen_kwargs["start_new_session"] = True
        process = subprocess.Popen(command, **popen_kwargs)
        with self._active_lock:
            self._active_processes.add(process)
        streamed_lines = 0
        captured_tail: list[str] = []

        try:
            if process.stdout is not None:
                for raw_line in process.stdout:
                    if self.stop_requested.is_set():
                        self._terminate_process(process)
                        break
                    line = raw_line.rstrip()
                    if not line:
                        continue
                    streamed_lines += 1
                    captured_tail.append(line)
                    captured_tail = captured_tail[-25:]
                    self._emit("log", line=f"[{index}/{total}] {line}")
            returncode = process.wait()
        finally:
            with self._active_lock:
                self._active_processes.discard(process)

        finished_at = time.time()
        output_paths: list[Path] = []
        for path in sorted(output_root.glob("output_data_*.yml"), key=lambda item: item.stat().st_mtime, reverse=True):
            resolved = path.resolve()
            if resolved not in existing_outputs or path.stat().st_mtime >= started_at - 1:
                output_paths.append(path)
        outputs = [self.state.relative_path(path) for path in output_paths]
        requirements = self._read_requirements_if_updated(requirements_path, existing_requirements_mtime, started_at)
        output_metadata = self._read_output_metadata(output_paths[0]) if output_paths else {}
        skipped = self._output_metadata_skipped(output_metadata)
        succeeded = not skipped and returncode == 0 and not self._has_failure_markers(captured_tail) and (
            bool(requirements) or bool(outputs)
        )
        solve_duration_seconds = self._metadata_millis_to_seconds(output_metadata.get("solve_duration_ms"))
        validation_duration_seconds = self._metadata_millis_to_seconds(output_metadata.get("validation_duration_ms"))
        env_create_duration_seconds = self._metadata_millis_to_seconds(output_metadata.get("env_create_duration_ms"))
        install_duration_seconds = self._metadata_millis_to_seconds(output_metadata.get("install_duration_ms"))
        smoke_duration_seconds = self._metadata_millis_to_seconds(output_metadata.get("smoke_duration_ms"))

        llm_calls = self._metadata_int(output_metadata.get("llm_calls"))
        env_builds = self._metadata_int(output_metadata.get("env_builds"))
        retries = self._metadata_int(output_metadata.get("retries"))
        fallback_invoked = self._metadata_bool(output_metadata.get("fallback_invoked"))
        fallback_outcome = self._metadata_text(output_metadata.get("fallback_outcome"))
        fallback_reason = self._metadata_text(output_metadata.get("fallback_reason"))
        validation_backend = self._metadata_text(output_metadata.get("validation_backend"))
        validation_path = self._metadata_text(output_metadata.get("validation_path"))
        requested_llm_validation_policy = self._metadata_text(
            output_metadata.get("requested_llm_validation_policy")
        )
        llm_validation_route = self._metadata_text(output_metadata.get("llm_validation_route"))
        docker_bypass_reason = self._metadata_text(output_metadata.get("docker_bypass_reason"))
        docker_bypass_note = self._metadata_text(
            output_metadata.get("docker_bypass_note")
            or output_metadata.get("docker_bypass_note_path")
        )
        debug_dir = self._metadata_text(output_metadata.get("debug_dir"))
        docker_plan_status = self._metadata_text(output_metadata.get("docker_plan_status"))
        docker_plan_path = self._metadata_text(output_metadata.get("docker_plan_path"))
        docker_plan_authorship = self._metadata_text(output_metadata.get("docker_plan_authorship"))
        docker_plan_fallback_sections = self._metadata_text(
            output_metadata.get("docker_plan_fallback_sections")
        )
        authored_dockerfile_path = self._metadata_text(
            output_metadata.get("authored_dockerfile_path")
        )
        executed_dockerfile_path = self._metadata_text(
            output_metadata.get("executed_dockerfile_path")
        )
        docker_build_command_path = self._metadata_text(
            output_metadata.get("docker_build_command_path")
        )
        docker_run_command_path = self._metadata_text(
            output_metadata.get("docker_run_command_path")
        )
        executed_image_ref = self._metadata_text(output_metadata.get("executed_image_ref"))
        image_handoff_verified = self._metadata_bool(
            output_metadata.get("image_handoff_verified")
        )
        image_inspect_path = self._metadata_text(output_metadata.get("image_inspect_path"))
        recovery_attempts_path = self._metadata_text(output_metadata.get("recovery_attempts_path"))
        recovery_outcome = self._metadata_text(output_metadata.get("recovery_outcome"))
        escalated_backend = self._metadata_text(output_metadata.get("escalated_backend"))
        failure_family = self._metadata_text(output_metadata.get("failure_family"))
        failure_bucket = self._metadata_text(output_metadata.get("failure_bucket"))
        failure_truth_class = self._metadata_text(output_metadata.get("failure_truth_class"))
        failure_truth_detail = self._metadata_text(output_metadata.get("failure_truth_detail"))
        skip_candidate = self._metadata_bool(output_metadata.get("skip_candidate"))

        result = {
            "snippet": self.state.relative_path(snippet),
            "started_at": started_iso,
            "finished_at": self.state.now_iso(),
            "duration_seconds": round(finished_at - started_at, 2),
            "returncode": returncode,
            "succeeded": succeeded,
            "skipped": skipped,
            "requirements": requirements,
            "output_metadata": output_metadata,
            "log_lines_streamed": streamed_lines,
            "log_tail": captured_tail,
            "output_files": outputs[:5],
            "solve_duration_seconds": solve_duration_seconds,
            "validation_duration_seconds": validation_duration_seconds,
            "env_create_duration_seconds": env_create_duration_seconds,
            "install_duration_seconds": install_duration_seconds,
            "smoke_duration_seconds": smoke_duration_seconds,
            "llm_calls": llm_calls,
            "env_builds": env_builds,
            "retries": retries,
            "fallbackInvoked": fallback_invoked,
            "fallbackOutcome": fallback_outcome,
            "fallbackReason": fallback_reason,
            "validationBackend": validation_backend,
            "validationPath": validation_path,
            "requestedLlmValidationPolicy": requested_llm_validation_policy,
            "llmValidationRoute": llm_validation_route,
            "dockerBypassReason": docker_bypass_reason,
            "dockerBypassNote": docker_bypass_note,
            "debugDir": debug_dir,
            "dockerPlanStatus": docker_plan_status,
            "dockerPlanPath": docker_plan_path,
            "dockerPlanAuthorship": docker_plan_authorship,
            "dockerPlanFallbackSections": docker_plan_fallback_sections,
            "authoredDockerfilePath": authored_dockerfile_path,
            "executedDockerfilePath": executed_dockerfile_path,
            "dockerBuildCommandPath": docker_build_command_path,
            "dockerRunCommandPath": docker_run_command_path,
            "executedImageRef": executed_image_ref,
            "imageHandoffVerified": image_handoff_verified,
            "imageInspectPath": image_inspect_path,
            "recoveryAttemptsPath": recovery_attempts_path,
            "recoveryOutcome": recovery_outcome,
            "escalatedBackend": escalated_backend,
            "failureFamily": failure_family,
            "failureBucket": failure_bucket,
            "failureTruthClass": failure_truth_class,
            "failureTruthDetail": failure_truth_detail,
            "skipCandidate": skip_candidate,
            "resultOrigin": "live",
        }
        if artifact_dir is not None:
            result["artifact_dir"] = self.state.relative_path(artifact_dir)

        # Extract tier metadata from output for categorization
        tier = self._extract_tier(output_metadata, captured_tail, llm_calls)
        confidence = self._extract_confidence(output_metadata, captured_tail) if tier == "tier3" else None
        cached = self._extract_cached_status(output_metadata, captured_tail) if tier == "tier3" else False

        # Determine result status for event emission
        if succeeded:
            result_status = "pass"
        elif skipped:
            result_status = "skip"
        else:
            result_status = "fail"

        # Emit case_complete event with tier metadata
        event_data = {"caseId": case_id, "status": result_status, "tier": tier}
        if tier == "tier3":
            if confidence is not None:
                event_data["confidence"] = confidence
            event_data["cached"] = cached
        event_data["fallbackInvoked"] = fallback_invoked
        if fallback_outcome:
            event_data["fallbackOutcome"] = fallback_outcome
        if fallback_reason:
            event_data["fallbackReason"] = fallback_reason
        if validation_path:
            event_data["validationPath"] = validation_path
        if requested_llm_validation_policy:
            event_data["requestedLlmValidationPolicy"] = requested_llm_validation_policy
        if llm_validation_route:
            event_data["llmValidationRoute"] = llm_validation_route
        if docker_bypass_reason:
            event_data["dockerBypassReason"] = docker_bypass_reason
        if docker_bypass_note:
            event_data["dockerBypassNote"] = docker_bypass_note
        if debug_dir:
            event_data["debugDir"] = debug_dir
        if docker_plan_status:
            event_data["dockerPlanStatus"] = docker_plan_status
        if docker_plan_path:
            event_data["dockerPlanPath"] = docker_plan_path
        if authored_dockerfile_path:
            event_data["authoredDockerfilePath"] = authored_dockerfile_path
        if executed_dockerfile_path:
            event_data["executedDockerfilePath"] = executed_dockerfile_path
        if docker_build_command_path:
            event_data["dockerBuildCommandPath"] = docker_build_command_path
        if docker_run_command_path:
            event_data["dockerRunCommandPath"] = docker_run_command_path
        if executed_image_ref:
            event_data["executedImageRef"] = executed_image_ref
        event_data["imageHandoffVerified"] = image_handoff_verified
        if image_inspect_path:
            event_data["imageInspectPath"] = image_inspect_path
        if recovery_attempts_path:
            event_data["recoveryAttemptsPath"] = recovery_attempts_path
        if recovery_outcome:
            event_data["recoveryOutcome"] = recovery_outcome
        if escalated_backend:
            event_data["escalatedBackend"] = escalated_backend
        if failure_family:
            event_data["failureFamily"] = failure_family
        if failure_truth_class:
            event_data["failureTruthClass"] = failure_truth_class
        if failure_truth_detail:
            event_data["failureTruthDetail"] = failure_truth_detail
        event_data["resultOrigin"] = "live"
        emit_event("case_complete", **event_data)

        # Store tier in result for tier_stats calculation
        result["tier"] = tier
        if tier == "tier3":
            if confidence is not None:
                result["confidence"] = confidence
            result["cached"] = cached

        # Emit progress event
        percent = round((index / total * 100), 1) if total > 0 else 0.0
        emit_event(
            "progress",
            progress={
                "completed": index,
                "total": total,
                "percent": percent,
            },
        )

        return result

    def _result_succeeded(self, result: dict[str, Any]) -> bool:
        if self._result_skipped(result):
            return False
        if int(result.get("returncode", 1)) != 0:
            return False
        if self._has_failure_markers(result.get("log_tail", [])):
            return False
        explicit = result.get("succeeded")
        if explicit is not None:
            return bool(explicit)
        requirements = [str(item).strip() for item in result.get("requirements", []) if str(item).strip()]
        if requirements:
            return True
        if self._has_failure_markers(result.get("log_tail", [])):
            return False
        output_files = [str(item) for item in result.get("output_files", []) if str(item).strip()]
        return bool(output_files) and int(result.get("returncode", 1)) == 0

    def _result_skipped(self, result: dict[str, Any]) -> bool:
        explicit = result.get("skipped")
        if explicit is not None:
            return bool(explicit)
        metadata = result.get("output_metadata")
        if not isinstance(metadata, dict):
            return False
        return self._output_metadata_skipped(metadata)

    def _output_metadata_skipped(self, metadata: dict[str, str]) -> bool:
        status = str(metadata.get("validation_status") or "").strip().lower()
        return status.startswith("skipped") or status == "host-runtime-required"

    def _read_requirements_if_updated(
        self,
        requirements_path: Path,
        existing_mtime: float | None,
        started_at: float,
    ) -> list[str]:
        if not requirements_path.exists():
            return []
        current_mtime = requirements_path.stat().st_mtime
        if existing_mtime is not None and current_mtime < started_at - 1:
            return []
        try:
            return [
                line.strip()
                for line in requirements_path.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
        except OSError:
            return []

    def _read_output_metadata(self, path: Path) -> dict[str, str]:
        if not path.exists():
            return {}
        metadata: dict[str, str] = {}
        try:
            for raw_line in path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line == "---" or ":" not in line:
                    continue
                key, value = line.split(":", 1)
                metadata[key.strip()] = value.strip()
        except OSError:
            return {}
        return metadata

    def _metadata_int(self, value: Any) -> int:
        text = str(value or "").strip()
        if not text:
            return 0
        try:
            return max(0, int(text))
        except (TypeError, ValueError):
            return 0

    def _metadata_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}

    def _metadata_text(self, value: Any) -> str:
        text = str(value or "").strip()
        return text

    def _metadata_millis_to_seconds(self, value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            millis = float(text)
        except (TypeError, ValueError):
            return None
        if millis < 0:
            return None
        return round(millis / 1000.0, 2)

    def _has_failure_markers(self, lines: Any) -> bool:
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
        for raw in lines or []:
            lowered = str(raw).lower()
            if any(term in lowered for term in terms):
                return True
        return False

    def _create_run_dir(self, tool: str) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        run_dir = self.state.runs_dir / f"{timestamp}-{tool}"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def _append_context_log(self, path: Path, kind: str, message: str) -> None:
        timestamp = self.state.now_iso()
        block = f"===== {timestamp} kind={kind} =====\n{message.rstrip()}\n\n"
        try:
            with path.open("a", encoding="utf-8") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    handle.write(block)
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass

    def _emit(self, kind: str, **payload: Any) -> None:
        self.message_queue.put({"kind": kind, **payload})

    def _emit_tier_stats_event(self, results: list[dict[str, Any]]) -> None:
        """Emit tier_stats event to SSE queue with tier breakdown."""
        if not hasattr(self, '_current_run_event_queue') or self._current_run_event_queue is None:
            return

        # Calculate tier breakdown
        tier1_count = sum(1 for r in results if r.get("tier") == "tier1")
        tier2_count = sum(1 for r in results if r.get("tier") == "tier2")
        tier3_count = sum(1 for r in results if r.get("tier") == "tier3")
        total = len(results)

        tier_stats = {
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

        event = {
            "type": "tier_stats",
            "stats": tier_stats,
            "timestamp": datetime.now().isoformat()
        }

        try:
            self._current_run_event_queue.put_nowait(event)
        except Exception:
            # Best-effort streaming - don't block runner on queue errors
            pass

    def _write_summary(self, summary: dict[str, Any]) -> None:
        if not self.run_dir:
            return
        path = self.run_dir / "summary.json"
        with path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
            handle.write("\n")

    def _persist_run_contract(self, summary: dict[str, Any], run_contract: dict[str, Any]) -> None:
        summary["run_contract"] = dict(run_contract)
        if not self.run_dir:
            return
        self.state.write_json(self.run_dir / "run_contract.json", run_contract)

    def _parse_limit(self, value: Any) -> int:
        if value in ("", None):
            return 0
        parsed = int(str(value))
        if parsed < 0:
            raise ValueError("Snippet limit must be zero or a positive integer.")
        return parsed

    def _terminate_process(self, process: subprocess.Popen[str]) -> None:
        """Kill a subprocess and all its children. Returns when the process is dead."""
        if process.poll() is not None:
            return
        pid = process.pid
        try:
            if os.name == "nt":
                # taskkill /T /F kills the entire process tree reliably on Windows.
                # CTRL_BREAK_EVENT only reaches the immediate process, not child
                # processes (e.g. the Python LLM service or Docker containers).
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )
            elif hasattr(os, "killpg"):
                os.killpg(pid, signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            # Escalate to SIGKILL / hard kill
            try:
                if os.name == "nt":
                    process.kill()
                elif hasattr(os, "killpg"):
                    os.killpg(pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                pass

    @staticmethod
    def _kill_orphaned_processes() -> None:
        """Kill orphaned apdr/docker processes that survive process-tree kills.

        On Windows, docker-buildx.exe is spawned by Docker Desktop's daemon,
        not as a child of the docker.exe CLI we spawned.  taskkill /T on our
        process tree therefore misses it.  We also kill any leftover apdr.exe
        and stop Docker containers with the apdr-validate prefix.
        """
        for proc_name in ("apdr.exe", "docker-buildx.exe"):
            try:
                subprocess.run(
                    ["taskkill", "/F", "/IM", proc_name],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass
        # Stop any running apdr-validate Docker containers
        try:
            result = subprocess.run(
                ["docker", "ps", "-q", "--filter", "name=apdr-validate"],
                capture_output=True, text=True, timeout=10,
            )
            container_ids = result.stdout.strip().split()
            for cid in container_ids:
                if cid:
                    subprocess.run(
                        ["docker", "rm", "-f", cid],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=10,
                    )
        except (OSError, subprocess.TimeoutExpired):
            pass

    def _case_id_from_snippet(self, snippet: Path) -> str:
        if snippet.parent.name:
            return snippet.parent.name
        return snippet.stem or "case"

    def _extract_tier(self, output_metadata: dict[str, str], log_tail: list[str], llm_calls: int) -> str:
        """Extract resolution tier from output metadata or logs.

        Returns "tier1", "tier2", "tier3", or "unknown".
        """
        # Check metadata first (APDR may write tier to output_data YAML)
        tier_value = str(output_metadata.get("resolution_tier") or "").strip().lower()
        if tier_value in ("tier1", "tier2", "tier3"):
            return tier_value

        # Strong indicator: if llm_calls > 0, it's tier3
        if llm_calls > 0:
            return "tier3"

        # Parse from log tail for tier markers
        for line in log_tail:
            line_lower = line.lower()
            if "tier1" in line_lower or "cache hit" in line_lower and "seed" not in line_lower:
                return "tier1"
            if "tier2" in line_lower or "heuristic" in line_lower:
                return "tier2"
            if "tier3" in line_lower or "llm" in line_lower or "language model" in line_lower:
                return "tier3"

        # Default to tier1 (most cases are cache/seed lookups)
        return "tier1"

    def _extract_confidence(self, output_metadata: dict[str, str], log_tail: list[str]) -> float | None:
        """Extract LLM confidence score from output metadata or logs.

        Returns float 0.0-1.0 or None if not available.
        """
        # Check metadata first
        confidence_str = str(output_metadata.get("confidence") or "").strip()
        if confidence_str:
            try:
                confidence = float(confidence_str)
                if 0.0 <= confidence <= 1.0:
                    return confidence
            except (TypeError, ValueError):
                pass

        # Parse from log tail (look for "confidence: 0.XX" or "confidence=0.XX")
        for line in log_tail:
            line_lower = line.lower()
            if "confidence" in line_lower:
                # Try to extract numeric value after "confidence"
                import re
                match = re.search(r'confidence[:\s=]+([0-9.]+)', line_lower)
                if match:
                    try:
                        confidence = float(match.group(1))
                        if 0.0 <= confidence <= 1.0:
                            return confidence
                    except (TypeError, ValueError):
                        pass

        return None

    def _extract_cached_status(self, output_metadata: dict[str, str], log_tail: list[str]) -> bool:
        """Extract import-set cache hit status from output metadata or logs.

        Returns True if import-set cache hit detected, False otherwise.
        """
        # Check metadata first
        cached_str = str(output_metadata.get("import_set_cached") or "").strip().lower()
        if cached_str in ("true", "1", "yes"):
            return True
        if cached_str in ("false", "0", "no"):
            return False

        # Parse from log tail (look for cache hit patterns)
        for line in log_tail:
            line_lower = line.lower()
            if "import-set cache hit" in line_lower or "import set cache hit" in line_lower:
                return True
            if "cache hit" in line_lower and "import" in line_lower:
                return True

        return False
