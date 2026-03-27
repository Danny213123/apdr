#!/usr/bin/env python3
"""Find cases with numpy missing module errors."""

import re
from pathlib import Path

cases_dir = Path("runs/20260325-010231-apdr/cases")

numpy_cases = []

for report_file in cases_dir.glob("*/resolution-report.txt"):
    with open(report_file) as f:
        content = f.read()

        # Check for numpy missing
        if "missing module `numpy" in content:
            case_id = report_file.parent.name

            # Extract resolved packages
            resolved = []
            in_resolved = False
            for line in content.split('\n'):
                if line.startswith('resolved_dependencies:'):
                    in_resolved = True
                elif in_resolved:
                    if line.startswith('-'):
                        resolved.append(line.strip('- '))
                    elif line.strip() and not line.startswith(' '):
                        break

            numpy_cases.append({
                'case_id': case_id,
                'resolved': resolved
            })

print(f"=== NumPy Missing Module Cases: {len(numpy_cases)} ===\n")

for case in numpy_cases[:10]:
    print(f"{case['case_id']}:")
    for pkg in case['resolved']:
        print(f"  {pkg}")
    print()
