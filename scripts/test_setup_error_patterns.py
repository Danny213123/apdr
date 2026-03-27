#!/usr/bin/env python3
"""Test setup.py error pattern matching."""

import sys
from pathlib import Path

sys.path.insert(0, "tools/apdr/llm_py")

from build_error_patterns import match_error_patterns

# Test cases from actual logs
test_errors = [
    ("FuncDesigner/numpy", """
Getting requirements to build wheel: started
Getting requirements to build wheel: finished with status 'error'
ModuleNotFoundError: No module named 'numpy'
ERROR: Failed to build 'FuncDesigner' when getting requirements to build wheel
"""),
    ("geodesy/Cython", """
Getting requirements to build wheel did not run successfully.
Traceback (most recent call last):
  File "setup.py", line 3
    from Cython.Build import cythonize
ModuleNotFoundError: No module named 'Cython'
"""),
    ("pycrayon/crayon", """
Collecting pycrayon==2016.9.15
  Getting requirements to build wheel: finished with status 'error'
  ModuleNotFoundError: No module named 'crayon'
"""),
    ("matplotlib/pkg_resources", """
Getting requirements to build wheel: started
Getting requirements to build wheel: finished with status 'error'
ModuleNotFoundError: No module named 'pkg_resources'
"""),
]

print("=== Testing Setup.py Error Patterns ===\n")

for name, error_log in test_errors:
    matches = match_error_patterns(error_log)
    print(f"{name}:")
    if matches:
        for m in matches:
            print(f"  + {m.diagnosis}")
            print(f"    Fix: {m.fix_type} - {m.suggested_fix}")
    else:
        print(f"  - No match")
    print()
