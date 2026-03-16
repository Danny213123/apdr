"""Builder Agent — generates Dockerfile, runs docker build + docker run.

Tools: system dep lookup, Dockerfile generator, Docker CLI
LLM: No (purely deterministic)
"""

from __future__ import annotations

import time

from ..state import AgentState
from ..tools import docker_ops, system_deps


def builder_node(state: AgentState) -> dict:
    """Generate Dockerfile with system deps and build Docker image."""
    config = state.get("config", {})
    output_dir = config.get("output_dir", "/tmp/apdr-agent")
    attempt = state.get("attempt", 0) + 1
    current_idx = state.get("current_python_idx", 0)
    candidate_versions = state.get("candidate_versions", ["3.11"])

    if current_idx >= len(candidate_versions):
        return {"status": "failed", "error_type": "no_more_python_versions"}

    python_version = candidate_versions[current_idx]

    # Tool 1: Infer system deps from requirements (deterministic)
    inferred_deps = system_deps.infer_system_deps(state.get("requirements_txt", ""))

    # Merge with previously discovered deps and recovery-suggested deps
    all_deps = sorted(set(
        inferred_deps
        + state.get("system_deps", [])
        + state.get("fix_system_deps", [])
    ))

    # Tool 2: Setup work directory with Dockerfile and build files
    work_dir, image_tag, container_name = docker_ops.setup_work_dir(
        output_dir=output_dir,
        attempt_index=attempt,
        python_version=python_version,
        requirements_txt=state.get("requirements_txt", ""),
        snippet_path=state.get("snippet_path", ""),
        imports=state.get("imports", []),
        system_deps=all_deps,
    )

    timeout_secs = config.get("validation_timeout_secs", 300)

    # Tool 3: Docker build
    build_result = docker_ops.docker_build(work_dir, image_tag, timeout=timeout_secs)

    attempt_record = {
        "attempt_index": attempt,
        "python_version": python_version,
        "system_deps": all_deps,
        "duration_ms": build_result["duration_ms"],
    }

    if not build_result["success"]:
        attempt_record["status"] = "build-timeout" if build_result["timed_out"] else "build-failed"
        attempt_record["error_type"] = "build_failed"

        attempts_log = list(state.get("attempts_log", []))
        attempts_log.append(attempt_record)

        return {
            "build_log": build_result["log"],
            "run_log": "",
            "system_deps": all_deps,
            "python_version": python_version,
            "image_tag": image_tag,
            "work_dir": work_dir,
            "error_type": "build_failed",
            "attempt": attempt,
            "attempts_log": attempts_log,
        }

    # Tool 4: Docker run (smoke test)
    remaining = max(30, timeout_secs - build_result["duration_ms"] // 1000)
    run_result = docker_ops.docker_run(image_tag, container_name, timeout=remaining)

    attempt_record["duration_ms"] += run_result["duration_ms"]

    if run_result["success"]:
        attempt_record["status"] = "passed"
        attempts_log = list(state.get("attempts_log", []))
        attempts_log.append(attempt_record)

        return {
            "build_log": build_result["log"],
            "run_log": run_result["log"],
            "system_deps": all_deps,
            "python_version": python_version,
            "image_tag": image_tag,
            "work_dir": work_dir,
            "status": "passed",
            "selected_python_version": python_version,
            "final_requirements": state.get("requirements_txt", ""),
            "attempt": attempt,
            "attempts_log": attempts_log,
        }

    # Runtime failure
    if run_result["timed_out"]:
        attempt_record["status"] = "runtime-timeout"
        attempt_record["error_type"] = "runtime_timeout"
    else:
        attempt_record["status"] = "runtime-failed"
        attempt_record["error_type"] = "runtime_failed"

    attempts_log = list(state.get("attempts_log", []))
    attempts_log.append(attempt_record)

    return {
        "build_log": build_result["log"],
        "run_log": run_result["log"],
        "system_deps": all_deps,
        "python_version": python_version,
        "image_tag": image_tag,
        "work_dir": work_dir,
        "error_type": attempt_record["error_type"],
        "attempt": attempt,
        "attempts_log": attempts_log,
    }
