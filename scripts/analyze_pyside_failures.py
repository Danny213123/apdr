#!/usr/bin/env python3
"""Analyze PySide/Qt-related failures."""

from pathlib import Path
import re

cases_dir = Path("runs/20260325-010231-apdr/cases")

pyside_cases = []

for report_file in cases_dir.glob("*/resolution-report.txt"):
    with open(report_file) as f:
        content = f.read()

        # Check for PySide/Qt-related errors
        if any(term in content.lower() for term in ['pyside', 'pyqt', 'qt4', 'qt5', 'qt6']):
            case_id = report_file.parent.name

            # Extract key info
            status = "unknown"
            error_lines = []

            for line in content.split('\n'):
                if line.startswith('final_status:'):
                    status = line.split(':', 1)[1].strip()
                elif any(term in line.lower() for term in ['pyside', 'pyqt', 'qt']):
                    error_lines.append(line.strip())

            pyside_cases.append({
                'case_id': case_id,
                'status': status,
                'errors': error_lines[:5]  # First 5 relevant lines
            })

print(f"=== PySide/Qt Failures: {len(pyside_cases)} ===\n")

for case in pyside_cases[:15]:
    print(f"{case['case_id']} ({case['status']}):")
    for err in case['errors']:
        print(f"  {err}")
    print()
