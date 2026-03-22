"""Reverse index from top_level.txt harvest data + pipreqs + importlib.metadata.

#15: Comprehensive reverse index that maps import names -> packages.

Sources:
1. tools/apdr/data/seed/top_level_harvest.tsv (highest priority)
2. tools/apdr/data/seed/reference_aliases.tsv
3. tools/apdr/data/seed/name_discrepancies.tsv
4. tools/apdr/data/seed/top_5000_mappings.tsv
5. tools/apdr/data/seed/pipreqs_mapping.tsv (pipreqs cross-reference)
6. importlib.metadata from installed packages (runtime scan)
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("apdr_llm")

# import_name (normalized) -> list of package_names
_reverse_index: dict[str, list[str]] = {}
_loaded = False


def _normalize(name: str) -> str:
    return name.strip().lower().replace("-", "_").replace(".", "_")


def _add_entry(import_name: str, package_name: str) -> None:
    """Add an entry to the reverse index (deduplicated)."""
    norm = _normalize(import_name)
    if not norm or not package_name:
        return
    if norm not in _reverse_index:
        _reverse_index[norm] = []
    if package_name not in _reverse_index[norm]:
        _reverse_index[norm].append(package_name)


def _load_tsv(path: Path) -> int:
    """Load a TSV file into the reverse index. Returns count of entries added."""
    if not path.exists():
        return 0
    count = 0
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                _add_entry(parts[0], parts[1].strip())
                count += 1
    except Exception as e:
        logger.warning("Failed to load %s: %s", path, e)
    return count


def _load_importlib_metadata() -> int:
    """Scan installed packages via importlib.metadata for top_level.txt data."""
    count = 0
    try:
        from importlib.metadata import distributions
        for dist in distributions():
            pkg_name = dist.metadata.get("Name", "")
            if not pkg_name:
                continue
            try:
                top_level = dist.read_text("top_level.txt")
                if top_level:
                    for module in top_level.strip().splitlines():
                        module = module.strip()
                        if module:
                            _add_entry(module, pkg_name)
                            count += 1
            except FileNotFoundError:
                pass
    except Exception as e:
        logger.debug("importlib.metadata scan: %s", e)
    return count


def load(tool_root: str | None = None) -> None:
    """Load reverse index from all available sources."""
    global _reverse_index, _loaded
    if _loaded:
        return

    if tool_root is None:
        here = Path(__file__).parent
        tool_root_path = here.parent  # tools/apdr/
    else:
        tool_root_path = Path(tool_root)

    seed_dir = tool_root_path / "data" / "seed"

    tsv_files = [
        seed_dir / "top_level_harvest.tsv",
        seed_dir / "reference_aliases.tsv",
        seed_dir / "name_discrepancies.tsv",
        seed_dir / "top_5000_mappings.tsv",
        seed_dir / "pipreqs_mapping.tsv",
    ]

    total = 0
    for tsv_path in tsv_files:
        n = _load_tsv(tsv_path)
        total += n

    meta_count = _load_importlib_metadata()
    total += meta_count

    _loaded = True
    logger.info(
        "Reverse index loaded: %d unique imports, %d total entries (%d from metadata)",
        len(_reverse_index), total, meta_count,
    )


def lookup(import_name: str) -> list[str]:
    """Look up packages that provide a given import name."""
    if not _loaded:
        load()
    return _reverse_index.get(_normalize(import_name), [])


def has_exact_match(import_name: str) -> bool:
    """Check if the import has an exact match in the reverse index."""
    if not _loaded:
        load()
    return _normalize(import_name) in _reverse_index


def fuzzy_lookup(import_name: str, limit: int = 10) -> list[str]:
    """Fuzzy lookup: find packages whose import names are similar."""
    if not _loaded:
        load()

    norm = _normalize(import_name)
    results: list[str] = []

    exact = _reverse_index.get(norm, [])
    results.extend(exact)

    for key, packages in _reverse_index.items():
        if len(results) >= limit:
            break
        if key == norm:
            continue
        if norm in key or key in norm:
            for pkg in packages:
                if pkg not in results:
                    results.append(pkg)
                    if len(results) >= limit:
                        break

    return results[:limit]


def enrich_context(import_names: list[str]) -> list[str]:
    """Build RAG context lines from reverse index for given imports."""
    context_lines = []
    for imp in import_names:
        packages = lookup(imp)
        if packages:
            context_lines.append(
                f"VERIFIED: import `{imp}` is provided by: {', '.join(packages[:5])}"
            )
        else:
            fuzzy = fuzzy_lookup(imp, limit=5)
            if fuzzy:
                context_lines.append(
                    f"Similar packages for `{imp}`: {', '.join(fuzzy)}"
                )
    return context_lines


def total_entries() -> int:
    """Return the total number of unique import names in the index."""
    if not _loaded:
        load()
    return len(_reverse_index)
