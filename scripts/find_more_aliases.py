#!/usr/bin/env python3
"""Find module-not-found cases that might be fixable with aliases."""

from pathlib import Path
from collections import Counter
import re

cases_dir = Path("runs/20260325-010231-apdr/cases")

missing_modules = Counter()

for report_file in cases_dir.glob("*/resolution-report.txt"):
    try:
        with open(report_file) as f:
            content = f.read()

            # Look for module-not-found that aren't in unsolvable list
            if "module-not-found" in content or "missing module" in content:
                # Extract the missing module name
                module_match = re.search(r"missing module `([^`]+)`", content)
                if module_match:
                    module = module_match.group(1).split('.')[0]  # First part of module

                    # Skip known platform/host specific patterns
                    skip_patterns = ['sublime', 'ida', 'gimp', 'bpy', 'win32', 'arcpy',
                                   'android', 'AppKit', 'lldb', 'xbmc', 'nuke']
                    if not any(pat in module for pat in skip_patterns):
                        missing_modules[module] += 1
    except:
        pass

print(f"=== Top 30 Missing Modules (potential aliases) ===\n")

for module, count in missing_modules.most_common(30):
    # Only show if appears multiple times (more likely to be a real alias need)
    if count >= 3:
        print(f"{count:3d}  {module}")
