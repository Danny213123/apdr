#!/usr/bin/env python3
"""Sample build failure cases to identify patterns."""

from pathlib import Path
import re

cases_dir = Path("runs/20260325-010231-apdr/cases")

build_fail_cases = []

for report_file in cases_dir.glob("*/resolution-report.txt"):
    try:
        with open(report_file) as f:
            content = f.read()

            if "No automatic recovery fix found for BuildFailure" in content:
                case_id = report_file.parent.name

                # Extract the error snippet
                error_match = re.search(r'validation_reason: (.+)', content)
                if error_match:
                    error = error_match.group(1).strip()
                    build_fail_cases.append({
                        'case_id': case_id,
                        'error': error[:200]
                    })

                if len(build_fail_cases) >= 10:
                    break
    except:
        pass

print(f"=== Sample Build Failures (10/{len(build_fail_cases)}) ===\n")

for case in build_fail_cases:
    print(f"{case['case_id']}:")
    print(f"  {case['error']}")
    print()
