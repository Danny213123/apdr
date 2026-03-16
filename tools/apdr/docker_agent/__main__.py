"""CLI entry point: python -m docker_agent --config <path>"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time


def main() -> None:
    parser = argparse.ArgumentParser(description="APDR Docker Agent — LangGraph multi-agent build pipeline")
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    # Read snippet source
    snippet_path = config["snippet_path"]
    with open(snippet_path, encoding="utf-8", errors="replace") as f:
        snippet_source = f.read()

    candidate_versions = config.get("candidate_versions", ["3.11"])

    # Build initial state
    initial_state = {
        "snippet_path": snippet_path,
        "snippet_source": snippet_source,
        "requirements_txt": config.get("requirements_txt", ""),
        "imports": config.get("imports", []),
        "python_version": candidate_versions[0] if candidate_versions else "3.11",
        "candidate_versions": candidate_versions,
        "config": config,
        "confidence": 0.0,
        "confidence_reason": "",
        "system_deps": [],
        "dockerfile_content": "",
        "image_tag": "",
        "work_dir": "",
        "attempt": 0,
        "max_attempts": config.get("max_attempts", 5),
        "current_python_idx": 0,
        "attempts_log": [],
        "build_log": "",
        "run_log": "",
        "error_type": "",
        "error_package": "",
        "error_detail": "",
        "fix_action": "",
        "fix_package": "",
        "fix_version": None,
        "fix_system_deps": [],
        "fix_reason": "",
        "tried_versions": {},
        "tried_packages": [],
        "status": "pending",
        "selected_python_version": None,
        "final_requirements": None,
    }

    # Build and run the graph
    from .graph import build_graph

    start_time = time.monotonic()
    graph = build_graph()

    try:
        final_state = graph.invoke(initial_state)
    except Exception as exc:
        final_state = dict(initial_state)
        final_state["status"] = "failed"
        final_state["error_detail"] = str(exc)

    total_ms = int((time.monotonic() - start_time) * 1000)

    # Write result JSON
    result = {
        "status": final_state.get("status", "failed"),
        "selected_python_version": final_state.get("selected_python_version"),
        "final_requirements": final_state.get("requirements_txt"),
        "confidence": final_state.get("confidence", 0.0),
        "confidence_reason": final_state.get("confidence_reason", ""),
        "attempts": final_state.get("attempts_log", []),
        "total_duration_ms": total_ms,
    }

    output_dir = config.get("output_dir", "/tmp/apdr-agent")
    os.makedirs(output_dir, exist_ok=True)
    result_path = os.path.join(output_dir, "agent-result.json")

    with open(result_path, "w") as f:
        json.dump(result, f, indent=2)

    # Print summary to stdout for Rust to capture
    print(json.dumps(result))


if __name__ == "__main__":
    main()
