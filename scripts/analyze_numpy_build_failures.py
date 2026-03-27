#!/usr/bin/env python3
"""Deep dive into NumPy build failures where numpy was resolved but still missing."""

from pathlib import Path
import re

cases_dir = Path("runs/20260325-010231-apdr/cases")

numpy_build_failures = []

for report_file in cases_dir.glob("*/resolution-report.txt"):
    with open(report_file) as f:
        content = f.read()

        # Check for the specific pattern: numpy in resolved but missing at runtime
        has_numpy_resolved = 'numpy' in content and 'resolved_dependencies:' in content
        has_numpy_missing = 'missing module `numpy' in content

        if has_numpy_resolved and has_numpy_missing:
            case_id = report_file.parent.name

            # Extract the actual error
            error_section = ""
            in_error = False
            for line in content.split('\n'):
                if 'error_message:' in line or 'ModuleNotFoundError' in line:
                    in_error = True
                if in_error:
                    error_section += line + '\n'
                    if len(error_section) > 500:
                        break

            numpy_build_failures.append({
                'case_id': case_id,
                'error_snippet': error_section[:500]
            })

print(f"=== NumPy Build Failures (resolved but still missing): {len(numpy_build_failures)} ===\n")

for case in numpy_build_failures:
    print(f"{case['case_id']}:")
    print(case['error_snippet'])
    print("-" * 80)
    print()
