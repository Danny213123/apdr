#!/usr/bin/env python3
"""Analyze missing modules from module-not-found failures."""

import re
from pathlib import Path
from collections import Counter

cases_dir = Path("runs/20260325-010231-apdr/cases")

missing_modules = []

for report_file in cases_dir.glob("*/resolution-report.txt"):
    with open(report_file) as f:
        content = f.read()
        match = re.search(r"validation_reason: Runtime import failed: missing module `([^`]+)`", content)
        if match:
            module_name = match.group(1).split(".")[0]  # Get top-level module
            missing_modules.append(module_name)

# Count occurrences
counts = Counter(missing_modules)

print("=== Missing Modules (Top 50) ===")
for module, count in counts.most_common(50):
    print(f"{count:4d} {module}")

print(f"\nTotal unique modules: {len(counts)}")
print(f"Total failures: {sum(counts.values())}")
