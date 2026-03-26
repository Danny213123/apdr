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
import threading
import time
import traceback

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

from .state import AppState

# WSL mount prefix pattern: /mnt/<drive>/...
_WSL_MNT_RE = re.compile(r"^/mnt/([a-zA-Z])(/.*)?$")


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

            snippet_limit = self._parse_limit(self.run_config.get("snippet_limit", ""))
            if snippet_limit:
                snippets = snippets[:snippet_limit]

            resume_results = [dict(item) for item in (self.run_config.get("_resume_results") or [])]
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
            summary = {
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
                "snippet_limit": snippet_limit or "",
                "python_command": str(self.run_config.get("python_command", "")),
                "validation_backend": str(self.run_config.get("validation_backend", "")),
                "started_at": self.state.now_iso(),
                "status": "running",
                "results": resume_results,
                "benchmark_context_log": self.state.relative_path(context_log),
            }
            if self.run_config.get("_resume_from_run_id"):
                summary["resume_from_run_id"] = str(self.run_config["_resume_from_run_id"])
                summary["resumed_results"] = resumed_completed
            self._write_summary(summary)
            self._emit(
                "plan",
                total=total_snippets,
                run_dir=str(self.run_dir),
                resumed_completed=resumed_completed,
                resumed_successes=resumed_successes,
                resumed_failures=resumed_failures,
                resumed_skips=resumed_skips,
                resumed_run_id=str(self.run_config.get("_resume_from_run_id") or ""),
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
                    ]
                ),
            )

            case_artifacts_root = self.run_dir / "cases" if self.run_dir else None
            if case_artifacts_root is not None:
                case_artifacts_root.mkdir(parents=True, exist_ok=True)

            # Determine worker count: 0 = auto, 1 = sequential.
            # Most cases resolve via Tier1/2 without LLM, so CPU-bound env
            # validation benefits from high parallelism.  The few LLM cases
            # serialize on the Ollama GPU anyway.
            workers = int(self.run_config.get("workers", 0))
            if workers <= 0:
                workers = max(1, (os.cpu_count() or 4) - 2)

            # Pre-build per-case command + metadata
            case_tasks: list[dict[str, Any]] = []
            for index, snippet in enumerate(snippets, start=1):
                overall_index = resumed_completed + index
                snippet_label = self.state.relative_path(snippet)
                artifact_dir = None
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
                            "env" if is_llm_only else vb,
                        ]
                    )
                    if is_llm_only:
                        command.append("--llm-only")
                    vt = self.run_config.get("validation_timeout")
                    if vt and int(vt) > 0:
                        command.extend(["--validation-timeout", str(int(vt))])
                    # Always force venv validation for non-LLM cases in benchmark mode
                    if not is_llm_only:
                        command.append("--force-validate")
                if self.run_config["verbose"]:
                    command.append("-v")
                command.extend(["--benchmark-context-log", str(context_log)])

                if tool == "apdr" and case_artifacts_root is not None:
                    artifact_dir = case_artifacts_root / self._case_id_from_snippet(snippet)
                    artifact_dir.mkdir(parents=True, exist_ok=True)
                    command.extend(["--output-dir", str(artifact_dir)])
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
                        "completed": len(summary["results"]),
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
        # Host-runtime skips with valid requirements count as passes —
        # the dependencies were correctly resolved but can't validate on this host.
        if skipped and bool(requirements) and returncode == 0:
            skipped = False
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
        }
        if artifact_dir is not None:
            result["artifact_dir"] = self.state.relative_path(artifact_dir)

        # Determine result status for event emission
        if succeeded:
            result_status = "pass"
        elif skipped:
            result_status = "skip"
        else:
            result_status = "fail"

        # Emit case_complete event
        emit_event("case_complete", caseId=case_id, status=result_status)

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

    def _write_summary(self, summary: dict[str, Any]) -> None:
        if not self.run_dir:
            return
        path = self.run_dir / "summary.json"
        with path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
            handle.write("\n")

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
