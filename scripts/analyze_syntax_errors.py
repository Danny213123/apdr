#!/usr/bin/env python3
"""Analyze syntax error cases to identify Python 2/3 patterns."""

import re
from pathlib import Path
from collections import Counter

cases_dir = Path("runs/20260325-010231-apdr/cases")

syntax_errors = []
python_versions = Counter()

for report_file in cases_dir.glob("*/resolution-report.txt"):
    with open(report_file) as f:
        content = f.read()

        # Check if it's a syntax error
        if "validation_status: syntax-error" not in content:
            continue

        case_id = report_file.parent.name

        # Extract Python version
        py_ver_match = re.search(r"python_version: (.+)", content)
        py_ver = py_ver_match.group(1).strip() if py_ver_match else "unknown"

        # Extract error hint
        error_match = re.search(r'error_hint="([^"]+)"', content)
        error = error_match.group(1) if error_match else "no hint"

        # Extract notes for Python 2/3 indicators
        notes = re.findall(r"^- (.+)", content, re.MULTILINE)

        python_versions[py_ver] += 1

        syntax_errors.append({
            "case_id": case_id,
            "python_version": py_ver,
            "error": error,
            "notes": notes
        })

print(f"=== Syntax Error Cases: {len(syntax_errors)} ===\n")

print("=== Python Version Distribution ===")
for ver, count in python_versions.most_common():
    print(f"{count:4d} Python {ver}")

print("\n=== Sample Syntax Errors (First 20) ===")
for i, case in enumerate(syntax_errors[:20], 1):
    print(f"\n[{i}] {case['case_id']}")
    print(f"    Python: {case['python_version']}")
    print(f"    Error: {case['error'][:100]}")

    # Look for Python 2 indicators in notes
    py2_indicators = [n for n in case['notes'] if 'python 2' in n.lower() or 'python 3' in n.lower()]
    if py2_indicators:
        print(f"    Note: {py2_indicators[0][:100]}")

# Identify Python 2 syntax in Python 3 cases
print("\n=== Python 2 Code in Python 3 Environment ===")
py2_in_py3 = [c for c in syntax_errors if c['python_version'] == '3.9' and 'SyntaxError' in c['error']]
print(f"Found {len(py2_in_py3)} cases")
for case in py2_in_py3[:10]:
    print(f"  {case['case_id']}: {case['error'][:80]}")

# Identify print statement errors
print("\n=== Common Syntax Error Patterns ===")
print_errors = sum(1 for c in syntax_errors if 'print' in c['error'].lower())
print(f"Print statement errors: {print_errors}")

invalid_syntax = sum(1 for c in syntax_errors if 'invalid syntax' in c['error'].lower())
print(f"Invalid syntax (generic): {invalid_syntax}")
