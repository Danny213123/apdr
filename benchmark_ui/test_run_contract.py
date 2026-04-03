from __future__ import annotations

import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from queue import Queue

from .run_contract import (
    REQUIRED_RUN_CONTRACT_KEYS,
    build_run_contract,
    missing_required_keys,
)
from .runner import BenchmarkWorker
from .service import BenchmarkService
from .state import AppState, ModelConfig


class _FakeBenchmarkState(AppState):
    def __init__(self, repo_root: Path) -> None:
        super().__init__(repo_root)
        tool_dir = self.tools_dir / "apdr"
        tool_dir.mkdir(parents=True, exist_ok=True)
        (tool_dir / "test_executor.py").write_text("print('stub')\n", encoding="utf-8")

    def load_model_config(self, tool: str) -> ModelConfig:
        return ModelConfig(tool=tool, model="qwen3.5:9b", base_url="http://localhost:11434")

    def validate_tool_runtime(
        self, tool: str, python_command: str = "", validation_backend: str = ""
    ) -> tuple[bool, str, list[str]]:
        return True, "runtime ok", [sys.executable]

    def discover_ollama_models(self, base_url: str) -> tuple[list[str], str, str]:
        return (["qwen3.5:9b"], "api", "")


class _CommandCapturingWorker(BenchmarkWorker):
    def __init__(self, state: AppState, run_config: dict[str, object], message_queue: Queue[dict[str, object]]) -> None:
        super().__init__(state, run_config, message_queue)
        self.commands: list[list[str]] = []

    def _run_single(
        self,
        tool: str,
        tool_dir: Path,
        command: list[str],
        snippet: Path,
        overall_index: int,
        total_snippets: int,
        artifact_dir: Path | None,
    ) -> dict[str, object]:
        self.commands.append(command)
        return {
            "snippet": str(snippet),
            "returncode": 0,
            "succeeded": True,
            "skipped": False,
            "requirements": [],
            "output_files": [],
            "log_tail": [],
            "duration_seconds": 0.1,
            "output_metadata": {
                "validation_backend": "llm",
                "validation_path": "docker",
                "llm_calls": "1",
            },
        }


class TestRunContract(unittest.TestCase):
    def test_service_defaults_macos_replay_to_release_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(AppState(Path(temp_dir)))
            config = service._normalize_run_config(
                {
                    "tool": "apdr",
                    "dataset_tar": str(Path(temp_dir) / "hard-gists.tar.gz"),
                    "run_intent": "macos-replay",
                }
            )
            self.assertEqual(config["run_intent"], "macos-replay")
            self.assertEqual(config["build_profile"], "release")

    def test_service_defaults_llm_policy_to_docker_first(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(AppState(Path(temp_dir)))
            config = service._normalize_run_config(
                {
                    "tool": "apdr",
                    "dataset_tar": str(Path(temp_dir) / "hard-gists.tar.gz"),
                    "validation_backend": "llm",
                }
            )
            self.assertEqual(config["llm_validation_policy"], "docker-first")

    def test_service_coerces_legacy_env_first_policy_to_docker_first(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(AppState(Path(temp_dir)))
            config = service._normalize_run_config(
                {
                    "tool": "apdr",
                    "dataset_tar": str(Path(temp_dir) / "hard-gists.tar.gz"),
                    "validation_backend": "llm",
                    "llm_validation_policy": "env-first",
                }
            )
            self.assertEqual(config["llm_validation_policy"], "docker-first")

    def test_service_preserves_llm_only_mode_from_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(AppState(Path(temp_dir)))
            config = service._normalize_run_config(
                {
                    "tool": "apdr",
                    "dataset_tar": str(Path(temp_dir) / "hard-gists.tar.gz"),
                    "validation_backend": "docker",
                    "llm_only_mode": True,
                }
            )
            self.assertEqual(config["validation_backend"], "docker")
            self.assertTrue(config["llm_only_mode"])

    def test_build_run_contract_includes_required_keys(self) -> None:
        contract = build_run_contract(
            repo_root=Path.cwd(),
            tool="apdr",
            model_name="qwen3.5:9b",
            base_url="http://localhost:11434",
            temperature=0.7,
            validation_backend="env",
            run_config={
                "llm_validation_policy": "env-first",
                "run_intent": "comparison",
                "cache_state": "warm",
                "llm_context_window": "32768",
                "inference_policy": "temperature=0.2; mode=compare",
                "build_profile": "pgo",
            },
            runner_command=[sys.executable],
            host_architecture="arm64",
            python_architecture="arm64-64",
            apdr_binary_architecture="arm64",
        )

        self.assertEqual(missing_required_keys(contract), [])
        self.assertEqual(set(REQUIRED_RUN_CONTRACT_KEYS), set(contract.keys()))
        self.assertEqual(contract["model_name"], "qwen3.5:9b")
        self.assertEqual(contract["llm_validation_policy"], "docker-first")
        self.assertEqual(contract["run_intent"], "comparison")
        self.assertEqual(contract["execution_mode"], "env-fast")
        self.assertEqual(contract["cache_state"], "warm")
        self.assertEqual(contract["llm_context_window"], "32768")
        self.assertEqual(contract["build_profile"], "pgo")

    def test_runner_persists_run_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = AppState(repo_root)
            worker = BenchmarkWorker(
                state,
                {
                    "tool": "apdr",
                    "dataset_tar": "test.tar.gz",
                    "loop_count": 1,
                    "search_range": 1,
                    "rag": False,
                    "verbose": False,
                    "snippet_limit": "",
                    "python_command": "",
                    "validation_backend": "env",
                },
                Queue(),
            )
            worker.run_dir = repo_root / "runs" / "20260329-000000-apdr"
            worker.run_dir.mkdir(parents=True, exist_ok=True)

            contract = {
                "run_contract_version": "1",
                "tool": "apdr",
                "model_name": "qwen3.5:9b",
                "base_url": "http://localhost:11434",
                "validation_backend": "env",
                "llm_validation_policy": "docker-first",
                "run_intent": "baseline",
                "execution_mode": "env-fast",
                "cache_state": "unknown",
                "host_architecture": "arm64",
                "apdr_binary_architecture": "arm64",
                "python_architecture": "arm64-64",
                "llm_context_window": "16384",
                "inference_policy": "temperature=0.7",
                "build_profile": "standard",
            }
            summary: dict[str, object] = {"tool": "apdr"}

            worker._persist_run_contract(summary, contract)

            self.assertEqual(summary["run_contract"], contract)
            persisted = json.loads((worker.run_dir / "run_contract.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted, contract)

    def test_historical_run_uses_saved_run_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = AppState(repo_root)
            run_dir = repo_root / "runs" / "20260329-000000-apdr"
            run_dir.mkdir(parents=True, exist_ok=True)

            state.write_json(
                run_dir / "summary.json",
                {
                    "tool": "apdr",
                    "model": "",
                    "base_url": "",
                    "dataset_tar": str(repo_root / "hard-gists.tar.gz"),
                    "dataset_dir": str(repo_root / "hard-gists"),
                    "loop_count": 1,
                    "search_range": 1,
                    "rag": False,
                    "verbose": False,
                    "snippet_limit": "",
                    "python_command": "",
                    "validation_backend": "docker",
                    "started_at": "2026-03-28T10:00:00",
                    "finished_at": "2026-03-28T10:00:01",
                    "status": "completed",
                    "results": [],
                    "run_contract": {
                        "run_contract_version": "1",
                        "tool": "apdr",
                        "model_name": "qwen3.5:9b",
                        "base_url": "http://localhost:11434",
                        "validation_backend": "docker",
                        "llm_validation_policy": "docker-first",
                        "run_intent": "comparison",
                        "execution_mode": "docker-proof",
                        "cache_state": "warm",
                        "host_architecture": "arm64",
                        "apdr_binary_architecture": "arm64",
                        "python_architecture": "arm64-64",
                        "llm_context_window": "32768",
                        "inference_policy": "temperature=0.2; mode=compare",
                        "build_profile": "pgo",
                    },
                },
            )

            service = BenchmarkService(state)
            payload = service.load_run("20260329-000000-apdr")
            run = payload["run"]
            info_fields = {item["label"]: item["value"] for item in run["infoFields"]}

            self.assertEqual(info_fields["Model"], "qwen3.5:9b")
            self.assertEqual(info_fields["Run intent"], "comparison")
            self.assertEqual(info_fields["Execution mode"], "docker-proof")
            self.assertEqual(info_fields["Cache state"], "warm")
            self.assertEqual(info_fields["Ctx window"], "32768")
            self.assertEqual(info_fields["Build profile"], "pgo")

    def test_historical_llm_run_keeps_backend_stable_and_surfaces_policy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = AppState(repo_root)
            run_dir = repo_root / "runs" / "20260329-005500-apdr"
            run_dir.mkdir(parents=True, exist_ok=True)

            state.write_json(
                run_dir / "summary.json",
                {
                    "tool": "apdr",
                    "model": "",
                    "base_url": "",
                    "dataset_tar": str(repo_root / "hard-gists.tar.gz"),
                    "dataset_dir": str(repo_root / "hard-gists"),
                    "loop_count": 1,
                    "search_range": 1,
                    "rag": False,
                    "verbose": False,
                    "snippet_limit": "",
                    "python_command": "",
                    "validation_backend": "llm",
                    "llm_validation_policy": "env-first",
                    "started_at": "2026-03-28T10:00:00",
                    "finished_at": "2026-03-28T10:00:01",
                    "status": "completed",
                    "results": [],
                    "run_contract": {
                        "run_contract_version": "1",
                        "tool": "apdr",
                        "model_name": "qwen3.5:9b",
                        "base_url": "http://localhost:11434",
                        "validation_backend": "llm",
                        "llm_validation_policy": "env-first",
                        "run_intent": "comparison",
                        "execution_mode": "llm-hybrid",
                        "cache_state": "warm",
                        "host_architecture": "arm64",
                        "apdr_binary_architecture": "arm64",
                        "python_architecture": "arm64-64",
                        "llm_context_window": "32768",
                        "inference_policy": "temperature=0.2; mode=compare",
                        "build_profile": "pgo",
                    },
                },
            )

            service = BenchmarkService(state)
            payload = service.load_run("20260329-005500-apdr")
            run = payload["run"]
            info_fields = {item["label"]: item["value"] for item in run["infoFields"]}

            self.assertEqual(payload["formConfig"]["validation_backend"], "llm")
            self.assertEqual(payload["formConfig"]["llm_validation_policy"], "docker-first")
            self.assertEqual(info_fields["Validation"], "LLM resolver (legacy env-first control + Docker follow-up + agent fallback)")
            self.assertEqual(info_fields["LLM policy"], "env-first")

    def test_historical_llm_only_run_restores_backend_selection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = AppState(repo_root)
            run_dir = repo_root / "runs" / "20260329-005700-apdr"
            run_dir.mkdir(parents=True, exist_ok=True)

            state.write_json(
                run_dir / "summary.json",
                {
                    "tool": "apdr",
                    "model": "",
                    "base_url": "",
                    "dataset_tar": str(repo_root / "hard-gists.tar.gz"),
                    "dataset_dir": str(repo_root / "hard-gists"),
                    "loop_count": 1,
                    "search_range": 1,
                    "rag": False,
                    "verbose": False,
                    "snippet_limit": "",
                    "python_command": "",
                    "validation_backend": "env",
                    "llm_only_mode": True,
                    "started_at": "2026-03-28T10:00:00",
                    "finished_at": "2026-03-28T10:00:01",
                    "status": "completed",
                    "results": [],
                    "run_contract": {
                        "run_contract_version": "1",
                        "tool": "apdr",
                        "model_name": "qwen3.5:9b",
                        "base_url": "http://localhost:11434",
                        "validation_backend": "env",
                        "llm_validation_policy": "docker-first",
                        "run_intent": "comparison",
                        "execution_mode": "env-fast",
                        "cache_state": "warm",
                        "host_architecture": "arm64",
                        "apdr_binary_architecture": "arm64",
                        "python_architecture": "arm64-64",
                        "llm_context_window": "32768",
                        "inference_policy": "temperature=0.2; mode=compare",
                        "build_profile": "pgo",
                    },
                },
            )

            service = BenchmarkService(state)
            payload = service.load_run("20260329-005700-apdr")
            run = payload["run"]
            info_fields = {item["label"]: item["value"] for item in run["infoFields"]}

            self.assertEqual(payload["formConfig"]["validation_backend"], "llm-only")
            self.assertTrue(payload["formConfig"]["llm_only_mode"])
            self.assertEqual(info_fields["Validation"], "LLM-only resolver + local Python env validation")
            self.assertEqual(info_fields["LLM mode"], "llm-only")

    def test_historical_run_shows_macos_replay_workers_and_warnings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = AppState(repo_root)
            run_dir = repo_root / "runs" / "20260329-010000-apdr"
            run_dir.mkdir(parents=True, exist_ok=True)

            state.write_json(
                run_dir / "summary.json",
                {
                    "tool": "apdr",
                    "model": "",
                    "base_url": "",
                    "dataset_tar": str(repo_root / "hard-gists.tar.gz"),
                    "dataset_dir": str(repo_root / "hard-gists"),
                    "loop_count": 1,
                    "search_range": 1,
                    "rag": False,
                    "verbose": False,
                    "snippet_limit": "",
                    "python_command": "",
                    "validation_backend": "env",
                    "workers": 0,
                    "effective_workers": 1,
                    "preflight_warnings": [
                        "Running under Rosetta 2 translation. Timings will not reflect native macOS ARM64 replay performance."
                    ],
                    "started_at": "2026-03-28T10:00:00",
                    "finished_at": "2026-03-28T10:00:01",
                    "status": "completed",
                    "results": [],
                    "run_contract": {
                        "run_contract_version": "1",
                        "tool": "apdr",
                        "model_name": "qwen3.5:9b",
                        "base_url": "http://localhost:11434",
                        "validation_backend": "env",
                        "llm_validation_policy": "docker-first",
                        "run_intent": "macos-replay",
                        "execution_mode": "env-fast",
                        "cache_state": "cold",
                        "host_architecture": "arm64",
                        "apdr_binary_architecture": "arm64",
                        "python_architecture": "arm64-64",
                        "llm_context_window": "16384",
                        "inference_policy": "temperature=0.7",
                        "build_profile": "release",
                    },
                },
            )

            service = BenchmarkService(state)
            payload = service.load_run("20260329-010000-apdr")
            run = payload["run"]
            info_fields = {item["label"]: item["value"] for item in run["infoFields"]}

            self.assertEqual(info_fields["Run intent"], "macos-replay")
            self.assertEqual(info_fields["Build profile"], "release")
            self.assertEqual(info_fields["Workers"], "1")
            self.assertIn("Rosetta 2", info_fields["Replay warnings"])

    def test_runner_passes_llm_validation_policy_flag_for_llm_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = _FakeBenchmarkState(repo_root)
            dataset_root = repo_root / "hard-gists"
            snippet_dir = dataset_root / "case-001"
            snippet_dir.mkdir(parents=True, exist_ok=True)
            (snippet_dir / "snippet.py").write_text("print('ok')\n", encoding="utf-8")
            dataset_tar = repo_root / "hard-gists.tar.gz"
            with tarfile.open(dataset_tar, "w:gz"):
                pass

            worker = _CommandCapturingWorker(
                state,
                {
                    "tool": "apdr",
                    "dataset_tar": str(dataset_tar),
                    "loop_count": 1,
                    "search_range": 1,
                    "rag": False,
                    "verbose": False,
                    "snippet_limit": "",
                    "python_command": "",
                    "validation_backend": "llm",
                    "llm_validation_policy": "env-first",
                },
                Queue(),
            )

            worker.run()

            self.assertEqual(len(worker.commands), 1)
            command = worker.commands[0]
            self.assertIn("--validation-backend", command)
            self.assertIn("llm", command)
            self.assertIn("--llm-validation-policy", command)
            policy_index = command.index("--llm-validation-policy")
            self.assertEqual(command[policy_index + 1], "docker-first")

    def test_runner_passes_llm_only_flag_for_llm_only_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = _FakeBenchmarkState(repo_root)
            dataset_root = repo_root / "hard-gists"
            snippet_dir = dataset_root / "case-001"
            snippet_dir.mkdir(parents=True, exist_ok=True)
            (snippet_dir / "snippet.py").write_text("print('ok')\n", encoding="utf-8")
            dataset_tar = repo_root / "hard-gists.tar.gz"
            with tarfile.open(dataset_tar, "w:gz"):
                pass

            worker = _CommandCapturingWorker(
                state,
                {
                    "tool": "apdr",
                    "dataset_tar": str(dataset_tar),
                    "loop_count": 1,
                    "search_range": 1,
                    "rag": False,
                    "verbose": False,
                    "snippet_limit": "",
                    "python_command": "",
                    "validation_backend": "env",
                    "llm_only_mode": True,
                },
                Queue(),
            )

            worker.run()

            self.assertEqual(len(worker.commands), 1)
            command = worker.commands[0]
            self.assertIn("--llm-only", command)
            self.assertNotIn("--force-validate", command)
            backend_index = command.index("--validation-backend")
            self.assertEqual(command[backend_index + 1], "docker")

    def test_case_row_keeps_requested_backend_distinct_from_validation_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(AppState(Path(temp_dir)))
            row = service._build_case_row(
                {
                    "snippet": "cases/routed/snippet.py",
                    "returncode": 0,
                    "succeeded": False,
                    "skipped": False,
                    "requirements": [],
                    "output_files": ["output_data_3.11.yml"],
                    "log_tail": [],
                    "duration_seconds": 1.1,
                    "output_metadata": {
                        "validation_backend": "llm",
                        "validation_path": "env->docker",
                        "requested_llm_validation_policy": "docker-first",
                        "llm_validation_route": "env-first-docker-bypass",
                        "docker_plan_status": "available",
                        "docker_plan_path": "cases/routed/docker-plan.json",
                        "docker_plan_authorship": "llm-authored",
                        "docker_plan_fallback_sections": "phase26-case-plan",
                        "authored_dockerfile_path": "cases/routed/Dockerfile.authored",
                        "executed_dockerfile_path": "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/Dockerfile.executed",
                        "docker_build_command_path": "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/docker-build.command.txt",
                        "docker_run_command_path": "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/docker-run.command.txt",
                        "executed_image_ref": "sha256:abc123",
                        "image_handoff_verified": "true",
                        "image_inspect_path": "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/docker-image.inspect.txt",
                        "docker_bypass_reason": "docker cli unavailable",
                        "docker_bypass_note": "cases/routed/.apdr-debug/docker-bypass.txt",
                        "debug_dir": "cases/routed/.apdr-debug",
                        "authored_plan_status": "available",
                        "authored_plan_path": "cases/routed/case-plan.json",
                        "authored_plan_authorship": "llm-authored-with-deterministic-fallback",
                        "authored_plan_fallback_sections": "tier1-cache,tier2-heuristic",
                        "intake_failure_class": "",
                        "intake_failure_path": "",
                        "recovery_attempts_path": "cases/routed/recovery-attempts.json",
                        "recovery_outcome": "provider-failure",
                        "escalated_backend": "docker",
                        "failure_truth_class": "provider-tooling-failure",
                        "failure_truth_detail": "timeout: structured recovery call timed out",
                        "validation_status": "environment-build-failed",
                        "validation_reason": "env build failed",
                        "llm_calls": "1",
                        "env_builds": "2",
                        "retries": "0",
                    },
                },
                {
                    "tool": "apdr",
                    "loop_count": 5,
                },
            )

            self.assertEqual(row["validationBackend"], "llm")
            self.assertEqual(row["validationPath"], "env->docker")
            self.assertEqual(row["requestedLlmValidationPolicy"], "docker-first")
            self.assertEqual(row["llmValidationRoute"], "env-first-docker-bypass")
            self.assertEqual(row["dockerStatus"], "bypassed")
            self.assertEqual(row["dockerPlanStatus"], "available")
            self.assertEqual(row["dockerPlanPath"], "cases/routed/docker-plan.json")
            self.assertEqual(row["dockerPlanAuthorship"], "llm-authored")
            self.assertEqual(row["dockerPlanFallbackSections"], ["phase26-case-plan"])
            self.assertEqual(row["authoredDockerfilePath"], "cases/routed/Dockerfile.authored")
            self.assertEqual(
                row["executedDockerfilePath"],
                "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/Dockerfile.executed",
            )
            self.assertEqual(
                row["dockerBuildCommandPath"],
                "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/docker-build.command.txt",
            )
            self.assertEqual(
                row["dockerRunCommandPath"],
                "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/docker-run.command.txt",
            )
            self.assertEqual(row["executedImageRef"], "sha256:abc123")
            self.assertTrue(row["imageHandoffVerified"])
            self.assertEqual(
                row["imageInspectPath"],
                "cases/routed/.apdr-debug/attempts/attempt-001-py-3_11/docker-image.inspect.txt",
            )
            self.assertEqual(row["dockerBypassReason"], "docker cli unavailable")
            self.assertEqual(
                row["dockerBypassNote"],
                "cases/routed/.apdr-debug/docker-bypass.txt",
            )
            self.assertEqual(row["debugDir"], "cases/routed/.apdr-debug")
            self.assertEqual(row["authoredPlanStatus"], "available")
            self.assertEqual(row["authoredPlanPath"], "cases/routed/case-plan.json")
            self.assertEqual(
                row["authoredPlanAuthorship"],
                "llm-authored-with-deterministic-fallback",
            )
            self.assertEqual(
                row["authoredPlanFallbackSections"],
                ["tier1-cache", "tier2-heuristic"],
            )
            self.assertEqual(row["intakeFailureClass"], "")
            self.assertEqual(
                row["recoveryAttemptsPath"], "cases/routed/recovery-attempts.json"
            )
            self.assertEqual(row["recoveryOutcome"], "provider-failure")
            self.assertEqual(row["escalatedBackend"], "docker")
            self.assertEqual(
                row["failureTruthClass"], "provider-tooling-failure"
            )
            self.assertEqual(
                row["failureTruthDetail"],
                "timeout: structured recovery call timed out",
            )
            self.assertNotEqual(row["requestedLlmValidationPolicy"], row["validationBackend"])
            self.assertNotEqual(row["llmValidationRoute"], row["validationPath"])

    def test_case_row_derives_exact_docker_status_labels(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(AppState(Path(temp_dir)))
            base_result = {
                "snippet": "cases/routed/snippet.py",
                "returncode": 1,
                "succeeded": False,
                "skipped": False,
                "requirements": [],
                "output_files": ["output_data_3.11.yml"],
                "log_tail": [],
                "duration_seconds": 1.1,
            }
            run_config = {"tool": "apdr", "loop_count": 1}

            scenarios = [
                (
                    "docker route",
                    {
                        "validation_backend": "llm",
                        "validation_path": "docker->llm-agent",
                        "llm_validation_route": "docker-first",
                    },
                    "attempted",
                ),
                (
                    "env first control",
                    {
                        "validation_backend": "llm",
                        "validation_path": "env->llm-agent",
                        "llm_validation_route": "env-first-control",
                        "docker_bypass_reason": "explicit env-first control policy",
                    },
                    "env-first control",
                ),
                (
                    "host runtime pre skip",
                    {
                        "validation_backend": "llm",
                        "validation_path": "env",
                        "llm_validation_route": "env-first-host-runtime",
                        "docker_bypass_reason": "host-runtime pre-skip",
                    },
                    "host-runtime pre-skip",
                ),
                (
                    "docker bypass",
                    {
                        "validation_backend": "llm",
                        "validation_path": "env",
                        "llm_validation_route": "env-first-docker-bypass",
                        "docker_bypass_reason": "docker daemon unavailable",
                    },
                    "bypassed",
                ),
                (
                    "historical docker path",
                    {
                        "validation_backend": "llm",
                        "validation_path": "env->docker",
                    },
                    "attempted",
                ),
            ]

            for label, metadata, expected in scenarios:
                with self.subTest(label=label):
                    row = service._build_case_row(
                        {
                            **base_result,
                            "output_metadata": metadata,
                        },
                        run_config,
                    )
                    self.assertEqual(row["dockerStatus"], expected)


if __name__ == "__main__":
    unittest.main()
