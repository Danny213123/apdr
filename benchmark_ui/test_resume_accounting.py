from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from .service import BenchmarkService
from .state import AppState


class TestResumeAccounting(unittest.TestCase):
    def test_host_runtime_skip_with_requirements_stays_skip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = BenchmarkService(AppState(Path(temp_dir)))
            result = {
                "snippet": "hard-gists/host-runtime/snippet.py",
                "returncode": 0,
                "succeeded": True,
                "skipped": False,
                "requirements": ["gpiozero==2.0.1"],
                "output_files": ["output_data_3.11.yml"],
                "log_tail": [],
                "duration_seconds": 1.2,
                "output_metadata": {
                    "validation_status": "skipped-host-runtime",
                    "validation_reason": "Detected Raspberry Pi hardware dependency.",
                    "failure_family": "environment-specific",
                    "failure_bucket": "skipped-host-runtime",
                    "skip_candidate": "true",
                },
            }

            self.assertTrue(service._result_skipped(result))
            self.assertFalse(service._result_succeeded(result))

            row = service._build_case_row(result, {"tool": "apdr", "loop_count": 5})
            self.assertEqual(row["status"], "SKIP")
            self.assertEqual(row["failureFamily"], "environment-specific")
            self.assertEqual(row["failureBucket"], "skipped-host-runtime")
            self.assertTrue(row["skipCandidate"])

    def test_resume_summary_keeps_historical_results_out_of_live_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            state = AppState(repo_root)
            run_dir = repo_root / "runs" / "20260401-010000-apdr"
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
                    "started_at": "2026-04-01T10:00:00",
                    "finished_at": "2026-04-01T10:00:05",
                    "status": "completed",
                    "resume_from_run_id": "20260330-004502-apdr",
                    "historical_results": [
                        {
                            "snippet": "hard-gists/0115e0ce312f26ff59f4fbf4f5821ca2/snippet.py",
                            "returncode": 0,
                            "succeeded": False,
                            "skipped": True,
                            "requirements": [],
                            "output_files": ["output_data_2.7.yml"],
                            "log_tail": [],
                            "duration_seconds": 2.0,
                            "tier": "tier1",
                            "output_metadata": {
                                "validation_status": "skipped-host-runtime",
                                "failure_family": "environment-specific",
                                "failure_bucket": "skipped-host-runtime",
                            },
                        }
                    ],
                    "results": [
                        {
                            "snippet": "hard-gists/00e9638c0efad1adac878522cf172484/snippet.py",
                            "returncode": 1,
                            "succeeded": False,
                            "skipped": False,
                            "requirements": ["gym==0.17.3"],
                            "output_files": ["output_data_3.9.yml"],
                            "log_tail": ["APDR validation failed: No automatic recovery fix found."],
                            "duration_seconds": 4.0,
                            "tier": "tier3",
                            "output_metadata": {
                                "validation_status": "environment-build-failed",
                                "failure_family": "dependency-resolution",
                                "failure_bucket": "environment-build-failed",
                                "validation_backend": "llm",
                                "validation_path": "env->docker->llm-agent",
                            },
                        }
                    ],
                    "run_contract": {
                        "run_contract_version": "1",
                        "tool": "apdr",
                        "model_name": "qwen3.5:9b",
                        "base_url": "http://localhost:11434",
                        "validation_backend": "llm",
                        "run_intent": "baseline",
                        "execution_mode": "llm-hybrid",
                        "cache_state": "unknown",
                        "host_architecture": "arm64",
                        "apdr_binary_architecture": "arm64",
                        "python_architecture": "arm64-64",
                        "llm_context_window": "16384",
                        "inference_policy": "temperature=0.7",
                        "build_profile": "standard",
                    },
                },
            )

            service = BenchmarkService(state)
            payload = service.load_run("20260401-010000-apdr")
            run = payload["run"]

            self.assertEqual(run["completed"], 2)
            self.assertEqual(run["historicalCompleted"], 1)
            self.assertEqual(run["liveCompleted"], 1)
            self.assertEqual(run["liveSuccesses"], 0)
            self.assertEqual(run["liveFailures"], 1)
            self.assertEqual(run["liveSkipped"], 0)
            self.assertTrue(run["liveOnlyAvailable"])
            self.assertEqual(run["completedCases"][0]["resultOrigin"], "live")
            self.assertEqual(run["completedCases"][1]["resultOrigin"], "historical")
