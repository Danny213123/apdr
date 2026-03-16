"""Import → package mapping from APDR seed TSV files."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from typing import Optional

_SEED_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "seed")


@dataclass
class ImportMapping:
    import_name: str
    package_name: str
    default_version: str
    source: str  # "seed" or "discrepancy"


_CACHE: Optional[dict[str, ImportMapping]] = None


def _normalize(name: str) -> str:
    return name.lower().replace("-", "_").replace(".", "_")


def _load_tsv(path: str, source: str) -> dict[str, ImportMapping]:
    mappings: dict[str, ImportMapping] = {}
    if not os.path.exists(path):
        return mappings
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) < 2 or row[0].startswith("#"):
                continue
            import_name = row[0].strip()
            package_name = row[1].strip()
            default_version = row[2].strip() if len(row) > 2 else ""
            key = _normalize(import_name)
            mappings[key] = ImportMapping(
                import_name=import_name,
                package_name=package_name,
                default_version=default_version,
                source=source,
            )
    return mappings


def load_seed_mappings() -> dict[str, ImportMapping]:
    """Load all seed TSV files. Discrepancy entries override seed entries."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    mappings: dict[str, ImportMapping] = {}
    # Load in priority order (lower priority first, higher overwrites)
    for filename, source in [
        ("top_5000_mappings.tsv", "seed"),
        ("reference_aliases.tsv", "seed"),
        ("name_discrepancies.tsv", "discrepancy"),
    ]:
        path = os.path.join(_SEED_DIR, filename)
        for key, mapping in _load_tsv(path, source).items():
            mappings[key] = mapping

    _CACHE = mappings
    return mappings


def lookup_import(import_name: str) -> Optional[ImportMapping]:
    """Look up an import name in seed data. Returns None if not found."""
    mappings = load_seed_mappings()
    key = _normalize(import_name)
    return mappings.get(key)
