#!/usr/bin/env python3
"""Harvest import-name -> package-name mappings from installed packages.

Requires Python 3.11+ for `importlib.metadata.packages_distributions()`.

Usage:
    # Install many popular packages first, e.g. via:
    #   pip install -r top_packages.txt
    # Then run:
    python harvest_top_level.py > ../data/seed/top_level_harvest.tsv

The output is a 4-column TSV matching the APDR seed format:
    import_name\tpackage_name\t\tharvest
"""

import sys

if sys.version_info < (3, 11):
    print("ERROR: Python 3.11+ required for packages_distributions()", file=sys.stderr)
    sys.exit(1)

from importlib.metadata import packages_distributions  # noqa: E402


def main():
    mapping = packages_distributions()
    seen = set()
    for import_name, dist_names in sorted(mapping.items()):
        # Skip private/internal modules
        if import_name.startswith("_") or "." in import_name:
            continue
        for dist in dist_names:
            key = (import_name.lower(), dist.lower())
            if key in seen:
                continue
            seen.add(key)
            # 4 columns: import_name, package_name, version (empty), source
            print(f"{import_name}\t{dist}\t\tharvest")


if __name__ == "__main__":
    main()
