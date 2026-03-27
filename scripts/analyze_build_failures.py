#!/usr/bin/env python3
"""Analyze build failure patterns from benchmark results."""

import re
from pathlib import Path
from collections import Counter

cases_dir = Path("runs/20260325-010231-apdr/cases")

# Patterns to extract
build_failures = Counter()
version_failures = Counter()
dependency_conflicts = Counter()

for report_file in cases_dir.glob("*/resolution-report.txt"):
    with open(report_file) as f:
        content = f.read()

        # Check validation status
        status_match = re.search(r"validation_status: (.+)", content)
        if not status_match:
            continue

        status = status_match.group(1).strip()

        if status == "environment-build-failed":
            # Look for error hints
            error_match = re.search(r'error_hint="([^"]+)"', content)
            if error_match:
                error = error_match.group(1)[:100]  # First 100 chars
                build_failures[error] += 1

        elif status == "version-not-found":
            # Extract failing package
            package_match = re.search(r"failing_package: (.+)", content)
            if package_match:
                package = package_match.group(1).strip()
                if package and package != "--":
                    version_failures[package] += 1

        elif status == "dependency-conflict":
            # Extract failing package
            package_match = re.search(r"failing_package: (.+)", content)
            if package_match:
                package = package_match.group(1).strip()
                if package and package != "--":
                    dependency_conflicts[package] += 1

print("=== Environment Build Failures (Top 20) ===")
for error, count in build_failures.most_common(20):
    print(f"{count:4d} {error}")

print("\n=== Version Not Found (Top 20) ===")
for package, count in version_failures.most_common(20):
    print(f"{count:4d} {package}")

print("\n=== Dependency Conflicts (Top 20) ===")
for package, count in dependency_conflicts.most_common(20):
    print(f"{count:4d} {package}")

print(f"\nTotal build failures: {sum(build_failures.values())}")
print(f"Total version failures: {sum(version_failures.values())}")
print(f"Total dependency conflicts: {sum(dependency_conflicts.values())}")
