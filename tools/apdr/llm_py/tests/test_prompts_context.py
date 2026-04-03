from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from llm_py.prompts import compress_benchmark_context


class BenchmarkContextCompressionTests(unittest.TestCase):
    def test_apdr_benchmark_context_keeps_only_stable_run_summary(self) -> None:
        context = """===== 2026-04-03T00:36:25 kind=benchmark-start =====
tool=apdr
model=qwen3.5:9b
base_url=http://localhost:11434
dataset=hard-gists.tar.gz
total_snippets=2891
resumed_completed=0
effective_workers=10
preflight_warnings=[]

===== 2026-04-03T04:36:26.031326+00:00 kind=apdr-command =====
snippet=/tmp/example/snippet.py
artifact_dir=/tmp/example/out
build_profile=standard
command=/tmp/apdr resolve /tmp/example/snippet.py --validation-backend docker

===== 1775190986 kind=apdr-resolve-command =====
snippet=/tmp/example/snippet.py
output_dir=/tmp/example/out
allow_llm=true
validate=true
validation_backend=docker
llm_validation_policy=docker-first
python_override=
range=5
max_retries=5
"""
        compressed = compress_benchmark_context(context, max_chars=4096)

        self.assertIn("APDR benchmark summary:", compressed)
        self.assertIn("dataset=hard-gists.tar.gz", compressed)
        self.assertIn("validation_backend=docker", compressed)
        self.assertIn("llm_validation_policy=docker-first", compressed)
        self.assertIn("shared_case_log=omitted", compressed)
        self.assertNotIn("snippet=/tmp/example/snippet.py", compressed)
        self.assertNotIn("artifact_dir=/tmp/example/out", compressed)
        self.assertNotIn("command=/tmp/apdr resolve", compressed)

    def test_generic_context_still_uses_tail_trimming(self) -> None:
        compressed = compress_benchmark_context("x" * 200, max_chars=32)

        self.assertTrue(compressed.startswith("[earlier context omitted]"))
        self.assertLessEqual(len(compressed), len("[earlier context omitted]\n") + 32)

    def test_partial_apdr_tail_still_collapses_to_summary(self) -> None:
        context = """[older benchmark context omitted]
#11 12.97 Fetched 110 MB in 10s (11.1 MB/s)
build_profile=standard
validation_backend=docker
llm_validation_policy=docker-first
allow_llm=true
validate=true
range=5
max_retries=5
command=/tmp/apdr resolve /tmp/example/snippet.py --validation-backend docker
artifact_dir=/tmp/example/out
"""
        compressed = compress_benchmark_context(context, max_chars=512)

        self.assertIn("APDR benchmark summary:", compressed)
        self.assertIn("build_profile=standard", compressed)
        self.assertIn("validation_backend=docker", compressed)
        self.assertIn("max_retries=5", compressed)
        self.assertIn("shared_case_log=omitted", compressed)
        self.assertNotIn("Fetched 110 MB", compressed)
        self.assertNotIn("artifact_dir=/tmp/example/out", compressed)


if __name__ == "__main__":
    unittest.main()
