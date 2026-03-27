#!/usr/bin/env python3
"""Analyze top failure categories to prioritize fixes."""

from pathlib import Path
from collections import Counter
import re

cases_dir = Path("runs/20260325-010231-apdr/cases")

failure_reasons = Counter()
failure_examples = {}

for report_file in cases_dir.glob("*/resolution-report.txt"):
    try:
        with open(report_file) as f:
            content = f.read()

            # Extract validation status and reason
            status_match = re.search(r'validation_status: (.+)', content)
            reason_match = re.search(r'validation_reason: (.+)', content)

            if status_match:
                status = status_match.group(1).strip()
                if status != 'passed':
                    case_id = report_file.parent.name

                    if reason_match:
                        reason = reason_match.group(1).strip()[:100]  # First 100 chars
                    else:
                        reason = status

                    failure_reasons[reason] += 1

                    # Store example case IDs
                    if reason not in failure_examples:
                        failure_examples[reason] = []
                    if len(failure_examples[reason]) < 3:
                        failure_examples[reason].append(case_id)
    except:
        pass

print(f"=== Top 30 Failure Patterns ===\n")

for reason, count in failure_reasons.most_common(30):
    print(f"{count:4d}  {reason}")
    examples = failure_examples.get(reason, [])
    if examples:
        print(f"       Examples: {', '.join(examples[:2])}")
    print()
