#!/usr/bin/env python3
"""Find cases where a package's setup.py imports dependencies that aren't installed yet."""

from pathlib import Path
import re

cases_dir = Path("runs/20260325-010231-apdr/cases")

setup_import_cases = []

for combined_log in cases_dir.glob("*/.apdr-debug/attempts/*/combined.log"):
    try:
        with open(combined_log) as f:
            content = f.read()

            # Pattern: ModuleNotFoundError during "Getting requirements to build wheel"
            if "Getting requirements to build wheel" in content and "ModuleNotFoundError" in content:
                case_id = combined_log.parts[-4]

                # Extract the package being built and the missing module
                package_match = re.search(r"Collecting ([^\s]+)", content)
                module_match = re.search(r"ModuleNotFoundError: No module named '([^']+)'", content)

                if package_match and module_match:
                    setup_import_cases.append({
                        'case_id': case_id,
                        'package': package_match.group(1),
                        'missing_module': module_match.group(1)
                    })
    except:
        pass

print(f"=== Setup.py Import Errors: {len(setup_import_cases)} ===\n")

# Group by package
package_issues = {}
for case in setup_import_cases:
    pkg = case['package'].split('==')[0]
    if pkg not in package_issues:
        package_issues[pkg] = []
    package_issues[pkg].append(case['missing_module'])

for pkg, missing in sorted(package_issues.items()):
    unique_missing = list(set(missing))
    print(f"{pkg}: needs {', '.join(unique_missing)} at setup time ({len(missing)} cases)")
