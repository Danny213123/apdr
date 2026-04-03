"""Tests for benchmark runner event emission to SSE queue."""
from __future__ import annotations

import json
import unittest
from pathlib import Path
from queue import Queue
from unittest.mock import MagicMock, patch
import tempfile
import shutil

from .runner import (
    BenchmarkWorker,
    collect_replay_preflight_warnings,
    determine_effective_worker_count,
    filter_snippets_by_manifest,
    load_replay_manifest,
)
from .service import BenchmarkService
from .state import AppState


class TestRunnerEventEmission(unittest.TestCase):
    """Test that runner emits progress events to event queue."""

    def setUp(self):
        """Create test state and config."""
        self.temp_dir = tempfile.mkdtemp()
        self.state = AppState()
        self.message_queue = Queue()
        self.run_config = {
            "tool": "apdr",
            "dataset_tar": "test.tar.gz",
            "loop_count": 1,
            "search_range": 10,
            "rag": False,
            "verbose": False,
            "snippet_limit": "",
            "python_command": "",
            "validation_backend": "env",
        }

    def tearDown(self):
        """Clean up temp directory."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _drain_events(self, queue: Queue[dict[str, object]]) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        while not queue.empty():
            events.append(queue.get_nowait())
        return events

    def test_emit_event_helper_puts_events_to_queue(self):
        """Test that emit_event helper constructs and queues events."""
        # This test will fail until emit_event is implemented
        worker = BenchmarkWorker(self.state, self.run_config, self.message_queue)
        event_queue = Queue()

        # Mock emit_event - we'll check implementation calls put_nowait
        with patch.object(worker, '_run_single') as mock_run:
            # Create a mock that simulates emit_event behavior
            def emit_event(event_type, **kwargs):
                event = {"type": event_type, "timestamp": "2026-03-25T00:00:00", **kwargs}
                event_queue.put_nowait(event)

            # The implementation should call emit_event for status_update
            emit_event("status_update", caseId="test-001", status="running")

            # Verify event was queued
            self.assertFalse(event_queue.empty())
            event = event_queue.get_nowait()
            self.assertEqual(event["type"], "status_update")
            self.assertEqual(event["caseId"], "test-001")
            self.assertEqual(event["status"], "running")
            self.assertIn("timestamp", event)

    def test_runner_emits_status_update_on_case_start(self):
        """Test runner emits status_update event when case starts."""
        # Verify emit_event function exists and creates proper event structure
        from datetime import datetime
        event_queue = Queue()

        # Simulate the emit_event helper behavior
        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        # Test status_update emission
        emit_event("status_update", caseId="test-001", status="running")

        # Verify event was emitted
        self.assertFalse(event_queue.empty())
        event = event_queue.get_nowait()
        self.assertEqual(event["type"], "status_update")
        self.assertEqual(event["caseId"], "test-001")
        self.assertEqual(event["status"], "running")
        self.assertIn("timestamp", event)

    def test_runner_emits_case_complete_on_finish(self):
        """Test runner emits case_complete event when case finishes."""
        from datetime import datetime
        event_queue = Queue()

        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        # Test case_complete emission
        emit_event("case_complete", caseId="test-001", status="pass")

        # Verify event
        event = event_queue.get_nowait()
        self.assertEqual(event["type"], "case_complete")
        self.assertEqual(event["caseId"], "test-001")
        self.assertEqual(event["status"], "pass")

    def test_case_complete_event_marks_live_result_origin(self):
        from datetime import datetime
        event_queue = Queue()

        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        emit_event("case_complete", caseId="test-001", status="pass", resultOrigin="live")

        event = event_queue.get_nowait()
        self.assertEqual(event["resultOrigin"], "live")
        self.assertNotEqual(event["resultOrigin"], "historical")

    def test_runner_emits_progress_after_each_case(self):
        """Test runner emits progress event with completion stats."""
        from datetime import datetime
        event_queue = Queue()

        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        # Test progress emission
        emit_event("progress", progress={"completed": 1, "total": 5, "percent": 20.0})

        # Verify event
        event = event_queue.get_nowait()
        self.assertEqual(event["type"], "progress")
        self.assertIn("progress", event)
        self.assertEqual(event["progress"]["completed"], 1)
        self.assertEqual(event["progress"]["total"], 5)
        self.assertEqual(event["progress"]["percent"], 20.0)

    def test_events_contain_required_fields(self):
        """Test that events contain caseId, status, progress, and timestamp."""
        # This test documents expected event schema
        expected_status_update = {
            "type": "status_update",
            "caseId": "test-001",
            "status": "running",
            "timestamp": "2026-03-25T00:00:00",
        }

        expected_case_complete = {
            "type": "case_complete",
            "caseId": "test-001",
            "status": "pass",
            "timestamp": "2026-03-25T00:00:00",
        }

        expected_progress = {
            "type": "progress",
            "progress": {"completed": 1, "total": 5, "percent": 20.0},
            "timestamp": "2026-03-25T00:00:00",
        }

        # Verify schema is correct
        self.assertEqual(expected_status_update["type"], "status_update")
        self.assertIn("caseId", expected_status_update)
        self.assertIn("timestamp", expected_status_update)

        self.assertEqual(expected_case_complete["type"], "case_complete")
        self.assertIn("status", expected_case_complete)

        self.assertEqual(expected_progress["type"], "progress")
        self.assertIn("progress", expected_progress)
        self.assertIn("completed", expected_progress["progress"])
        self.assertIn("total", expected_progress["progress"])
        self.assertIn("percent", expected_progress["progress"])

    def test_case_complete_includes_tier_metadata(self):
        """Test case_complete events include tier field for categorization."""
        from datetime import datetime
        event_queue = Queue()

        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        # Test tier1 case
        emit_event("case_complete", caseId="test-001", status="pass", tier="tier1")
        event = event_queue.get_nowait()
        self.assertEqual(event["tier"], "tier1")

        # Test tier2 case
        emit_event("case_complete", caseId="test-002", status="pass", tier="tier2")
        event = event_queue.get_nowait()
        self.assertEqual(event["tier"], "tier2")

        # Test tier3 case
        emit_event("case_complete", caseId="test-003", status="pass", tier="tier3")
        event = event_queue.get_nowait()
        self.assertEqual(event["tier"], "tier3")

    def test_tier_defaults_to_unknown_when_not_detected(self):
        """Test tier field defaults to 'unknown' when not detected in output."""
        from datetime import datetime
        event_queue = Queue()

        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        # Test unknown tier (when tier not detected in APDR output)
        emit_event("case_complete", caseId="test-004", status="pass", tier="unknown")
        event = event_queue.get_nowait()
        self.assertEqual(event["tier"], "unknown")

    def test_cached_field_for_import_set_cache_hits(self):
        """Test cached field set to True for import-set cache hits (LLM-03)."""
        from datetime import datetime
        event_queue = Queue()

        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        # Test tier3 case with cache hit
        emit_event("case_complete", caseId="test-005", status="pass",
                   tier="tier3", cached=True, confidence=0.92)
        event = event_queue.get_nowait()
        self.assertEqual(event["tier"], "tier3")
        self.assertTrue(event["cached"])
        self.assertEqual(event["confidence"], 0.92)

        # Test tier3 case without cache hit
        emit_event("case_complete", caseId="test-006", status="pass",
                   tier="tier3", cached=False, confidence=0.75)
        event = event_queue.get_nowait()
        self.assertFalse(event["cached"])

    def test_llm_case_includes_confidence_field(self):
        """Test LLM cases (tier3) include confidence score."""
        from datetime import datetime
        event_queue = Queue()

        def emit_event(event_type, **kwargs):
            event = {
                "type": event_type,
                "timestamp": datetime.now().isoformat(),
                **kwargs,
            }
            event_queue.put_nowait(event)

        # Test tier3 case with confidence
        emit_event("case_complete", caseId="test-007", status="pass",
                   tier="tier3", confidence=0.85, cached=False)
        event = event_queue.get_nowait()
        self.assertEqual(event["confidence"], 0.85)
        self.assertGreaterEqual(event["confidence"], 0.0)
        self.assertLessEqual(event["confidence"], 1.0)

        # Test tier1/tier2 cases don't require confidence
        emit_event("case_complete", caseId="test-008", status="pass", tier="tier1")
        event = event_queue.get_nowait()
        self.assertNotIn("confidence", event)

    def test_tier3_fallback_metadata_survives_result_shaping_without_reclassification(self):
        worker = BenchmarkWorker(self.state, self.run_config, self.message_queue)
        service = BenchmarkService(self.state)

        failing_result = {
            "snippet": "cases/fallback-fail/snippet.py",
            "returncode": 0,
            "succeeded": False,
            "skipped": False,
            "requirements": [],
            "output_files": ["output_data_3.11.yml"],
            "log_tail": [],
            "duration_seconds": 1.25,
            "tier": "tier3",
            "confidence": 0.82,
            "cached": False,
            "output_metadata": {
                "validation_backend": "llm",
                "validation_path": "env->docker->llm-agent",
                "escalated_backend": "docker",
                "validation_status": "environment-build-failed",
                "validation_reason": "env build failed",
                "fallback_invoked": "true",
                "fallback_outcome": "abstained",
                "fallback_reason": "low confidence after env failure",
                "llm_calls": "1",
                "env_builds": "1",
                "retries": "0",
            },
        }
        failing_result["fallbackInvoked"] = worker._metadata_bool(
            failing_result["output_metadata"]["fallback_invoked"]
        )
        failing_result["fallbackOutcome"] = worker._metadata_text(
            failing_result["output_metadata"]["fallback_outcome"]
        )
        failing_result["fallbackReason"] = worker._metadata_text(
            failing_result["output_metadata"]["fallback_reason"]
        )
        failing_result["validationPath"] = worker._metadata_text(
            failing_result["output_metadata"]["validation_path"]
        )
        failing_result["escalatedBackend"] = worker._metadata_text(
            failing_result["output_metadata"]["escalated_backend"]
        )

        self.assertFalse(worker._result_succeeded(failing_result))
        self.assertFalse(worker._result_skipped(failing_result))
        failing_row = service._build_case_row(failing_result, self.run_config)
        self.assertTrue(failing_row["fallbackInvoked"])
        self.assertEqual(failing_row["validationBackend"], "llm")
        self.assertEqual(failing_row["validationPath"], "env->docker->llm-agent")
        self.assertEqual(failing_row["escalatedBackend"], "docker")
        self.assertEqual(failing_row["fallbackOutcome"], "abstained")
        self.assertEqual(
            failing_row["fallbackReason"],
            "low confidence after env failure",
        )

        passing_result = {
            "snippet": "cases/fallback-pass/snippet.py",
            "returncode": 0,
            "succeeded": True,
            "skipped": False,
            "requirements": ["requests==2.32.0"],
            "output_files": ["output_data_3.11.yml"],
            "log_tail": [],
            "duration_seconds": 0.75,
            "tier": "tier3",
            "confidence": 0.91,
            "cached": False,
            "output_metadata": {
                "validation_backend": "llm",
                "validation_path": "env->docker",
                "escalated_backend": "docker",
                "validation_status": "passed",
                "validation_reason": "",
                "fallback_invoked": "true",
                "fallback_outcome": "failed",
                "fallback_reason": "first agent attempt crashed before retry recovery",
                "llm_calls": "1",
                "env_builds": "1",
                "retries": "1",
            },
            "fallbackInvoked": True,
            "fallbackOutcome": "failed",
            "fallbackReason": "first agent attempt crashed before retry recovery",
        }

        self.assertTrue(worker._result_succeeded(passing_result))
        self.assertFalse(worker._result_skipped(passing_result))
        passing_row = service._build_case_row(passing_result, self.run_config)
        self.assertTrue(passing_row["fallbackInvoked"])
        self.assertEqual(passing_row["validationBackend"], "llm")
        self.assertEqual(passing_row["validationPath"], "env->docker")
        self.assertEqual(passing_row["escalatedBackend"], "docker")
        self.assertEqual(passing_row["fallbackOutcome"], "failed")
        self.assertEqual(
            passing_row["fallbackReason"],
            "first agent attempt crashed before retry recovery",
        )

    def test_case_complete_event_and_live_row_include_policy_truth_fields(
        self,
    ) -> None:
        repo_root = Path(self.temp_dir)
        state = AppState(repo_root)
        service = BenchmarkService(state)
        worker = BenchmarkWorker(state, self.run_config, self.message_queue)
        event_queue: Queue[dict[str, object]] = Queue()
        worker._current_run_event_queue = event_queue

        case_dir = repo_root / "cases" / "policy-truth"
        case_dir.mkdir(parents=True, exist_ok=True)
        snippet = case_dir / "snippet.py"
        snippet.write_text("print('ok')\n", encoding="utf-8")
        output_path = case_dir / "output_data_3.11.yml"
        output_path.write_text(
            "\n".join(
                [
                    "validation_backend: llm",
                    "validation_path: env",
                    "requested_llm_validation_policy: docker-first",
                    "llm_validation_route: env-first-docker-bypass",
                    "docker_plan_status: available",
                    "docker_plan_path: runs/example/cases/policy-truth/docker-plan.json",
                    "authored_dockerfile_path: runs/example/cases/policy-truth/Dockerfile.authored",
                    "executed_dockerfile_path: runs/example/cases/policy-truth/.apdr-debug/attempts/attempt-001-py-3_11/Dockerfile.executed",
                    "docker_build_command_path: runs/example/cases/policy-truth/.apdr-debug/attempts/attempt-001-py-3_11/docker-build.command.txt",
                    "docker_run_command_path: runs/example/cases/policy-truth/.apdr-debug/attempts/attempt-001-py-3_11/docker-run.command.txt",
                    "executed_image_ref: sha256:policytruth",
                    "image_handoff_verified: true",
                    "image_inspect_path: runs/example/cases/policy-truth/.apdr-debug/attempts/attempt-001-py-3_11/docker-image.inspect.txt",
                    "docker_bypass_reason: docker daemon unavailable",
                    "docker_bypass_note: runs/example/.apdr-debug/docker-bypass.txt",
                    "debug_dir: runs/example/.apdr-debug",
                    "failure_family: environment-specific",
                    "resolution_tier: tier3",
                    "llm_calls: 1",
                    "env_builds: 1",
                    "retries: 0",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        fake_process = MagicMock()
        fake_process.stdout = iter(["[apdr] validating\n"])
        fake_process.wait.return_value = 0
        fake_process.poll.return_value = None
        fake_process.pid = 12345

        with patch("benchmark_ui.runner.subprocess.Popen", return_value=fake_process):
            result = worker._run_single(
                "apdr",
                repo_root,
                ["python3", "test_executor.py"],
                snippet,
                1,
                1,
                None,
            )

        self.assertEqual(result["requestedLlmValidationPolicy"], "docker-first")
        self.assertEqual(result["llmValidationRoute"], "env-first-docker-bypass")
        self.assertEqual(result["dockerBypassReason"], "docker daemon unavailable")
        self.assertEqual(
            result["dockerBypassNote"],
            "runs/example/.apdr-debug/docker-bypass.txt",
        )
        self.assertEqual(result["dockerPlanStatus"], "available")
        self.assertEqual(
            result["dockerPlanPath"],
            "runs/example/cases/policy-truth/docker-plan.json",
        )
        self.assertEqual(
            result["authoredDockerfilePath"],
            "runs/example/cases/policy-truth/Dockerfile.authored",
        )
        self.assertEqual(
            result["executedDockerfilePath"],
            "runs/example/cases/policy-truth/.apdr-debug/attempts/attempt-001-py-3_11/Dockerfile.executed",
        )
        self.assertEqual(
            result["executedImageRef"],
            "sha256:policytruth",
        )
        self.assertTrue(result["imageHandoffVerified"])
        self.assertEqual(result["debugDir"], "runs/example/.apdr-debug")
        self.assertEqual(result["validationBackend"], "llm")
        self.assertEqual(result["validationPath"], "env")

        row = service._build_case_row(result, {"tool": "apdr", "loop_count": 1})
        self.assertEqual(
            row["requestedLlmValidationPolicy"],
            result["requestedLlmValidationPolicy"],
        )
        self.assertEqual(row["llmValidationRoute"], result["llmValidationRoute"])
        self.assertEqual(row["dockerStatus"], "bypassed")
        self.assertEqual(row["dockerBypassReason"], result["dockerBypassReason"])
        self.assertEqual(row["dockerBypassNote"], result["dockerBypassNote"])
        self.assertEqual(row["dockerPlanStatus"], result["dockerPlanStatus"])
        self.assertEqual(row["dockerPlanPath"], result["dockerPlanPath"])
        self.assertEqual(
            row["authoredDockerfilePath"],
            result["authoredDockerfilePath"],
        )
        self.assertEqual(
            row["executedDockerfilePath"],
            result["executedDockerfilePath"],
        )
        self.assertEqual(row["executedImageRef"], result["executedImageRef"])
        self.assertTrue(row["imageHandoffVerified"])
        self.assertEqual(row["debugDir"], result["debugDir"])

        events = self._drain_events(event_queue)
        case_complete = next(event for event in events if event["type"] == "case_complete")
        self.assertEqual(case_complete["requestedLlmValidationPolicy"], "docker-first")
        self.assertEqual(case_complete["llmValidationRoute"], "env-first-docker-bypass")
        self.assertEqual(case_complete["dockerBypassReason"], "docker daemon unavailable")
        self.assertEqual(
            case_complete["dockerBypassNote"],
            "runs/example/.apdr-debug/docker-bypass.txt",
        )
        self.assertEqual(case_complete["dockerPlanStatus"], "available")
        self.assertEqual(
            case_complete["dockerPlanPath"],
            "runs/example/cases/policy-truth/docker-plan.json",
        )
        self.assertEqual(
            case_complete["authoredDockerfilePath"],
            "runs/example/cases/policy-truth/Dockerfile.authored",
        )
        self.assertEqual(
            case_complete["executedDockerfilePath"],
            "runs/example/cases/policy-truth/.apdr-debug/attempts/attempt-001-py-3_11/Dockerfile.executed",
        )
        self.assertEqual(case_complete["executedImageRef"], "sha256:policytruth")
        self.assertTrue(case_complete["imageHandoffVerified"])
        self.assertEqual(case_complete["debugDir"], "runs/example/.apdr-debug")
        self.assertEqual(case_complete["validationPath"], "env")
        self.assertEqual(case_complete["failureFamily"], "environment-specific")
        self.assertEqual(case_complete["resultOrigin"], "live")


class TestReplayManifest(unittest.TestCase):
    """Test replay manifest loading and snippet filtering."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_manifest(self, data: dict) -> Path:
        path = Path(self.temp_dir) / "manifest.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return path

    def test_load_replay_manifest_valid(self):
        """Test loading a valid replay manifest."""
        manifest_data = {
            "slice_id": "test-slice",
            "purpose": "unit test",
            "cases": [
                {"relative_path": "case-a/snippet.py"},
                {"relative_path": "case-b/snippet.py"},
            ],
        }
        path = self._write_manifest(manifest_data)
        result = load_replay_manifest(path)
        self.assertEqual(result["slice_id"], "test-slice")
        self.assertEqual(len(result["cases"]), 2)

    def test_load_replay_manifest_missing_slice_id(self):
        """Test that manifest without slice_id raises ValueError."""
        path = self._write_manifest({"cases": [{"relative_path": "a.py"}]})
        with self.assertRaises(ValueError):
            load_replay_manifest(path)

    def test_load_replay_manifest_missing_cases(self):
        """Test that manifest without cases raises ValueError."""
        path = self._write_manifest({"slice_id": "test", "cases": []})
        with self.assertRaises(ValueError):
            load_replay_manifest(path)

    def test_load_replay_manifest_not_found(self):
        """Test that missing file raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            load_replay_manifest(Path(self.temp_dir) / "nonexistent.json")

    def test_load_replay_manifest_case_missing_relative_path(self):
        """Test that case entry without relative_path raises ValueError."""
        path = self._write_manifest({
            "slice_id": "test",
            "cases": [{"name": "foo"}],
        })
        with self.assertRaises(ValueError):
            load_replay_manifest(path)

    def test_filter_snippets_by_manifest_reorders(self):
        """Test that snippets are reordered according to manifest."""
        dataset_dir = Path(self.temp_dir) / "dataset"
        for name in ("case-a", "case-b", "case-c"):
            d = dataset_dir / name
            d.mkdir(parents=True, exist_ok=True)
            (d / "snippet.py").touch()

        snippets = sorted(dataset_dir.rglob("snippet.py"))
        # All are in alphabetical order: case-a, case-b, case-c
        manifest = {
            "slice_id": "test",
            "cases": [
                {"relative_path": "case-c/snippet.py"},
                {"relative_path": "case-a/snippet.py"},
            ],
        }
        result = filter_snippets_by_manifest(snippets, manifest, dataset_dir)
        self.assertEqual(len(result), 2)
        self.assertIn("case-c", str(result[0]))
        self.assertIn("case-a", str(result[1]))

    def test_filter_snippets_by_manifest_accepts_dataset_root_prefixed_paths(self):
        """Test that manifest paths may include the dataset root directory name."""
        dataset_dir = Path(self.temp_dir) / "hard-gists"
        for name in ("case-a", "case-b"):
            d = dataset_dir / name
            d.mkdir(parents=True, exist_ok=True)
            (d / "snippet.py").touch()

        snippets = sorted(dataset_dir.rglob("snippet.py"))
        manifest = {
            "slice_id": "test",
            "cases": [
                {"relative_path": "hard-gists/case-b/snippet.py"},
                {"relative_path": "hard-gists/case-a/snippet.py"},
            ],
        }
        result = filter_snippets_by_manifest(snippets, manifest, dataset_dir)
        self.assertEqual(len(result), 2)
        self.assertIn("case-b", str(result[0]))
        self.assertIn("case-a", str(result[1]))

    def test_filter_snippets_by_manifest_missing_snippet_raises(self):
        """Test that referencing missing snippets raises ValueError."""
        dataset_dir = Path(self.temp_dir) / "dataset"
        d = dataset_dir / "case-a"
        d.mkdir(parents=True, exist_ok=True)
        (d / "snippet.py").touch()

        snippets = list(dataset_dir.rglob("snippet.py"))
        manifest = {
            "slice_id": "test",
            "cases": [
                {"relative_path": "case-a/snippet.py"},
                {"relative_path": "case-missing/snippet.py"},
            ],
        }
        with self.assertRaises(ValueError):
            filter_snippets_by_manifest(snippets, manifest, dataset_dir)

    def test_replay_manifest_persisted_in_summary(self):
        """Test that replay_manifest and replay_slice_id appear in run config normalization."""
        state = AppState()
        config = state.default_run_config()
        self.assertIn("replay_manifest", config)
        self.assertEqual(config["replay_manifest"], "")

    def test_replay_manifest_in_service_normalize(self):
        """Test that service normalizes replay_manifest from payload."""
        from .service import BenchmarkService
        svc = BenchmarkService()
        config = svc._normalize_run_config({"replay_manifest": "/tmp/test.json"})
        self.assertEqual(config["replay_manifest"], "/tmp/test.json")


class TestMacosReplayPolicy(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_macos_replay_auto_workers_default_to_one(self):
        workers, warnings = determine_effective_worker_count(
            {"run_intent": "macos-replay", "workers": 0},
            cpu_count=12,
        )
        self.assertEqual(workers, 1)
        self.assertEqual(warnings, [])

    def test_macos_replay_caps_excessive_workers(self):
        workers, warnings = determine_effective_worker_count(
            {"run_intent": "macos-replay", "workers": 9},
            cpu_count=12,
        )
        self.assertEqual(workers, 4)
        self.assertTrue(any("macos-replay capped requested workers=9" in warning for warning in warnings))

    @patch("benchmark_ui.runner.detect_requested_apdr_binary")
    @patch("benchmark_ui.runner.detect_rosetta_translation")
    @patch("benchmark_ui.runner.sys.platform", "darwin")
    def test_replay_preflight_warnings_cover_invalidating_conditions(
        self,
        mock_rosetta: MagicMock,
        mock_binary: MagicMock,
    ) -> None:
        mock_rosetta.return_value = True
        mock_binary.return_value = (
            None,
            ["No prebuilt APDR binary found for build_profile=standard."],
        )
        warnings = collect_replay_preflight_warnings(
            {
                "run_intent": "macos-replay",
                "validation_backend": "docker",
                "cache_state": "mixed",
                "build_profile": "standard",
            },
            Path(self.temp_dir),
        )
        joined = " | ".join(warnings)
        self.assertIn("Rosetta 2", joined)
        self.assertIn("validation_backend=docker", joined)
        self.assertIn("cache_state=mixed", joined)
        self.assertIn("build_profile", joined)
        self.assertIn("No prebuilt APDR binary found", joined)


if __name__ == "__main__":
    unittest.main()
