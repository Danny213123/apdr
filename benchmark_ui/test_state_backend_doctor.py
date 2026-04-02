from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .service import BenchmarkService
from .state import AppState, ModelConfig


class _FakeDoctorState(AppState):
    def __init__(self, repo_root: Path) -> None:
        super().__init__(repo_root)
        (self.tools_dir / "apdr").mkdir(parents=True, exist_ok=True)
        self.default_dataset_tar.write_text("stub", encoding="utf-8")
        dataset_root = self.repo_root / "hard-gists"
        dataset_root.mkdir(parents=True, exist_ok=True)
        (dataset_root / "snippet.py").write_text("print('ok')\n", encoding="utf-8")

    def discover_tools(self) -> list[str]:
        return ["apdr"]

    def load_model_config(self, tool: str) -> ModelConfig:
        return ModelConfig(tool=tool, model="qwen3.5:9b", base_url="http://localhost:11434")

    def discover_ollama_models(self, base_url: str) -> tuple[list[str], str, str]:
        return (["qwen3.5:9b"], "api", "")

    def count_snippets(self, dataset_root: Path) -> int:
        return 1

    def validate_tool_runtime(
        self, tool: str, python_command: str = "", validation_backend: str = ""
    ) -> tuple[bool, str, list[str]]:
        return True, f"runtime ok for {self.apdr_backend_description(validation_backend)}", [sys.executable]

    def apdr_local_interpreters(self) -> tuple[dict[str, str], list[str]]:
        return ({"3.11": "/usr/bin/python3.11"}, ["2.7"])

    def apdr_env_tooling_available(self, available_interpreters: dict[str, str]) -> tuple[bool, str]:
        return True, "virtualenv tooling ready"

    def apdr_kgraph_server_available(self) -> bool:
        return True

    def _run_command(self, command: list[str], cwd: Path | None = None, timeout: int = 30) -> tuple[int, str]:
        if command[:2] == ["docker", "--version"]:
            return 1, "docker missing"
        if command[:3] == ["docker", "info", "--format"]:
            return 1, "daemon unavailable"
        if command[:2] == ["ollama", "--version"]:
            return 0, "ollama version 0.6.0"
        return 0, "ok"


class TestStateBackendDoctor(unittest.TestCase):
    def _docker_aware_which(self, command: str) -> str | None:
        if command == "docker":
            return None
        if command == "ollama":
            return "/usr/bin/ollama"
        if command == "cargo":
            return "/usr/bin/cargo"
        return "/usr/bin/python3"

    def test_llm_backend_warns_when_docker_is_missing_for_docker_first_degradation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state = _FakeDoctorState(Path(temp_dir))
            with patch("benchmark_ui.state.shutil.which", side_effect=self._docker_aware_which):
                rows = state.doctor_checks(selected_tool="apdr", validation_backend="llm")

            docker_row = next(
                row for row in rows if row["label"] == "Docker (preferred for APDR llm docker-first)"
            )
            self.assertEqual(docker_row["status"], "WARN")
            self.assertIn("requested docker-first validation", docker_row["detail"])
            self.assertIn("degrade to env validation", docker_row["detail"])

    def test_llm_backend_still_checks_local_env_tooling(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state = _FakeDoctorState(Path(temp_dir))
            with patch("benchmark_ui.state.shutil.which", side_effect=self._docker_aware_which):
                rows = state.doctor_checks(selected_tool="apdr", validation_backend="llm")

            backend_row = next(row for row in rows if row["label"] == "apdr validation backend")
            tooling_row = next(row for row in rows if row["label"] == "apdr env tooling")
            self.assertIn("Docker-first validation with safe env fallback", backend_row["detail"])
            self.assertEqual(tooling_row["status"], "PASS")
            self.assertEqual(tooling_row["detail"], "virtualenv tooling ready")

    def test_pure_docker_backend_still_fails_without_docker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state = _FakeDoctorState(Path(temp_dir))
            with patch("benchmark_ui.state.shutil.which", side_effect=self._docker_aware_which):
                rows = state.doctor_checks(selected_tool="apdr", validation_backend="docker")

            docker_cli_row = next(row for row in rows if row["label"] == "Docker CLI")
            docker_daemon_row = next(row for row in rows if row["label"] == "Docker daemon")
            self.assertEqual(docker_cli_row["status"], "FAIL")
            self.assertEqual(docker_daemon_row["status"], "FAIL")

    def test_service_doctor_copy_mentions_targeted_docker_escalation_for_llm(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(_FakeDoctorState(Path(temp_dir)))
            summary = service._doctor_intro_summary("apdr", "llm")
            self.assertIn("Docker-first llm readiness", summary)
            self.assertIn("env fallback", summary)


if __name__ == "__main__":
    unittest.main()
