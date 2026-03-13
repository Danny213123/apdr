from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
import json
import os
import signal
import subprocess
import threading
import time
import traceback

from .state import AppState


class BenchmarkWorker(threading.Thread):
    def __init__(self, state: AppState, run_config: dict[str, Any], message_queue: Any) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.run_config = run_config
        self.message_queue = message_queue
        self.stop_requested = threading.Event()
        self.current_process: subprocess.Popen[str] | None = None
        self.run_dir: Path | None = None

    def stop(self) -> None:
        self.stop_requested.set()
        if self.current_process and self.current_process.poll() is None:
            self._terminate_process(self.current_process)

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
            runtime_ok, runtime_detail, runtime_runner = self.state.validate_tool_runtime(
                tool, str(self.run_config.get("python_command", ""))
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
            dataset_tar = Path(str(self.run_config["dataset_tar"])).expanduser().resolve()
            self._emit("status", text=f"Preparing dataset from {self.state.relative_path(dataset_tar)}")
            dataset_dir = self.state.ensure_dataset_extracted(dataset_tar)
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

            for index, snippet in enumerate(snippets, start=1):
                if self.stop_requested.is_set():
                    break

                snippet_label = self.state.relative_path(snippet)
                overall_index = resumed_completed + index
                self._emit("status", text=f"Running {snippet_label} ({overall_index}/{total_snippets})")

                command = runner + [
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
                if self.run_config["verbose"]:
                    command.append("-v")
                command.extend(["--benchmark-context-log", str(context_log)])

                artifact_dir = None
                if tool == "apdr" and case_artifacts_root is not None:
                    artifact_dir = case_artifacts_root / self._case_id_from_snippet(snippet)
                    artifact_dir.mkdir(parents=True, exist_ok=True)
                    command.extend(["--output-dir", str(artifact_dir)])
                    # Benchmark parity: validate resolved dependencies via install/import smoke tests
                    # without executing the whole snippet body.
                    command.append("--no-execute-snippet")

                self._emit("command", text=self.state.format_command(command))
                self._append_context_log(
                    context_log,
                    "case-start",
                    f"index={overall_index}/{total_snippets}\nsnippet={snippet_label}\ncommand={self.state.format_command(command)}",
                )
                result = self._run_single(tool, tool_dir, command, snippet, overall_index, total_snippets, artifact_dir)
                summary["results"].append(result)
                self._write_summary(summary)
                self._append_context_log(
                    context_log,
                    "case-finished",
                    json.dumps(result, indent=2, sort_keys=True),
                )
                self._emit(
                    "progress",
                    completed=overall_index,
                    total=total_snippets,
                    snippet=snippet_label,
                    returncode=result["returncode"],
                    duration=result["duration_seconds"],
                    result=result,
                )

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
        started_at = time.time()
        started_iso = self.state.now_iso()
        output_root = artifact_dir if artifact_dir is not None else snippet.parent
        existing_outputs = {path.resolve() for path in output_root.glob("output_data_*.yml")}
        requirements_path = output_root / "requirements.txt"
        existing_requirements_mtime = requirements_path.stat().st_mtime if requirements_path.exists() else None
        process = subprocess.Popen(
            command,
            cwd=tool_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        self.current_process = process
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
            self.current_process = None

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
        }
        if artifact_dir is not None:
            result["artifact_dir"] = self.state.relative_path(artifact_dir)
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
        with path.open("a", encoding="utf-8") as handle:
            handle.write(block)

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
        if process.poll() is not None:
            return
        try:
            if hasattr(os, "killpg"):
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                if hasattr(os, "killpg"):
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except OSError:
                pass

    def _case_id_from_snippet(self, snippet: Path) -> str:
        if snippet.parent.name:
            return snippet.parent.name
        return snippet.stem or "case"
