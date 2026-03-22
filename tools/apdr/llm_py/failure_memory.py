"""Persistent cross-run failure memory for Reflexion pattern.

Ported from tools/apdr/src/llm/failure_memory.rs.
Tracks which import→package mappings have been tried and failed,
so the LLM can learn from past mistakes.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class FailureRecord:
    package_tried: str
    error_reason: str
    python_version: str
    timestamp: int = 0


@dataclass
class FailureMemory:
    path: Path
    failures: dict[str, list[FailureRecord]] = field(default_factory=dict)

    @classmethod
    def load(cls, cache_path: str) -> FailureMemory:
        p = Path(cache_path) / "llm_failure_memory.tsv"
        mem = cls(path=p)
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t", 4)
                if len(parts) >= 4:
                    import_name = parts[0]
                    record = FailureRecord(
                        package_tried=parts[1],
                        error_reason=parts[2],
                        python_version=parts[3],
                        timestamp=int(parts[4]) if len(parts) > 4 else 0,
                    )
                    mem.failures.setdefault(import_name, []).append(record)
        return mem

    def format_context(self, import_names: list[str]) -> str:
        lines = []
        for name in import_names:
            for r in self.failures.get(name, []):
                lines.append(
                    f"PREVIOUS FAILURE: import `{name}` mapped to `{r.package_tried}` "
                    f"failed ({r.error_reason}). DO NOT suggest `{r.package_tried}` again."
                )
        return "\n".join(lines)

    def has_failed(self, import_name: str, package_name: str) -> bool:
        for r in self.failures.get(import_name, []):
            if r.package_tried.lower() == package_name.lower():
                return True
        return False

    def record_failure(
        self,
        import_name: str,
        package_tried: str,
        error_reason: str,
        python_version: str,
    ) -> None:
        record = FailureRecord(
            package_tried=package_tried,
            error_reason=error_reason,
            python_version=python_version,
            timestamp=int(time.time()),
        )
        self.failures.setdefault(import_name, []).append(record)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# import_name\tpackage_tried\terror_reason\tpython_version\ttimestamp"
        ]
        for import_name, records in self.failures.items():
            for r in records:
                reason = r.error_reason.replace("\t", " ").replace("\n", " ")
                lines.append(
                    f"{import_name}\t{r.package_tried}\t{reason}\t{r.python_version}\t{r.timestamp}"
                )
        self.path.write_text("\n".join(lines))
