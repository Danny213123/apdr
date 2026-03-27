#!/usr/bin/env python3
"""Test error pattern matching against real failure cases."""

import sys
from pathlib import Path

sys.path.insert(0, "tools/apdr/llm_py")

from build_error_patterns import match_error_patterns

# Test cases from analysis
test_errors = [
    ("Print statement", "SyntaxError: Missing parentheses in call to 'print'. Did you mean print('Adding titanOSX User')?"),
    ("Exception syntax", "SyntaxError: multiple exception types must be parenthesized"),
    ("Lambda 3.11", "SyntaxError: Lambda expression parameters cannot be parenthesized"),
    ("NumPy.bool", "AttributeError: module 'numpy' has no attribute 'bool'."),
    ("pandas 0.25", "ERROR: No matching distribution found for pandas==0.25.3 (from -r requirements.txt)"),
    ("dbus-python", "ERROR: No matching distribution found for dbus-python==1.2.2"),
    ("Double req", "ERROR: Double requirement given: ujson (from -r requirements.txt)"),
    ("pkg-config", "Found pkg-config: YES (C:\\msys64\\ucrt64\\bin\\pkg-config.EXE) 2.2.0"),
]

print("=== Testing Error Pattern Matching ===\n")

for name, error_log in test_errors:
    matches = match_error_patterns(error_log)
    print(f"{name}:")
    if matches:
        for m in matches:
            print(f"  + {m.diagnosis}")
            print(f"    Fix: {m.fix_type}")
    else:
        print(f"  - No match")
    print()
