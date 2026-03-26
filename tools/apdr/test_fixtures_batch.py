#!/usr/bin/env python3
"""Batch test all fixtures and collect results."""

import subprocess
import sys
from pathlib import Path
import json

fixtures_dir = Path("tests/fixtures")
output_base = Path("out/fixture_tests")
output_base.mkdir(parents=True, exist_ok=True)

results = []
fixtures = sorted([f for f in fixtures_dir.rglob("*.py") if "__" not in f.name])

print(f"=== Testing {len(fixtures)} fixtures ===\n")

for i, fixture in enumerate(fixtures, 1):
    fixture_name = fixture.stem
    output_dir = output_base / fixture_name

    print(f"[{i}/{len(fixtures)}] {fixture_name}...", end=" ", flush=True)

    try:
        result = subprocess.run(
            [
                str(Path("target/release/apdr.exe").absolute()),
                "resolve",
                str(fixture),
                "--output",
                str(output_dir),
                "--python",
                "3.9",
                "--allow-llm",
                "--no-validate",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Parse stdout for key metrics
        output = result.stdout
        resolved = unresolved = llm_calls = tier = "?"

        for line in output.split("\n"):
            if "RESOLVED_COUNT=" in line:
                resolved = line.split("=")[1]
            elif "UNRESOLVED_COUNT=" in line:
                unresolved = line.split("=")[1]
            elif "LLM_CALLS=" in line:
                llm_calls = line.split("=")[1]
            elif "VALIDATION_STATUS=" in line:
                status = line.split("=")[1]

        # Determine tier from report
        tier = "tier1" if llm_calls == "0" else "tier3"
        if "heuristic" in output.lower():
            tier = "tier2"

        passed = result.returncode == 0
        print(f"{'PASS' if passed else 'FAIL'} | {resolved}/{int(resolved) + int(unresolved) if resolved.isdigit() and unresolved.isdigit() else '?'} | llm:{llm_calls} | {tier}")

        results.append({
            "name": fixture_name,
            "passed": passed,
            "resolved": resolved,
            "unresolved": unresolved,
            "llm_calls": llm_calls,
            "tier": tier,
        })

    except subprocess.TimeoutExpired:
        print("TIMEOUT")
        results.append({"name": fixture_name, "passed": False, "error": "timeout"})
    except Exception as e:
        print(f"ERROR: {e}")
        results.append({"name": fixture_name, "passed": False, "error": str(e)})

# Summary
pass_count = sum(1 for r in results if r.get("passed"))
fail_count = len(results) - pass_count
tier1 = sum(1 for r in results if r.get("tier") == "tier1")
tier2 = sum(1 for r in results if r.get("tier") == "tier2")
tier3 = sum(1 for r in results if r.get("tier") == "tier3")

print(f"\n=== Summary ===")
print(f"Total: {len(results)}")
print(f"Pass:  {pass_count} ({pass_count/len(results)*100:.1f}%)")
print(f"Fail:  {fail_count}")
print(f"Tier1: {tier1} (cache)")
print(f"Tier2: {tier2} (heuristic)")
print(f"Tier3: {tier3} (LLM)")

# Save results
with open(output_base / "results.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"\nResults saved to: {output_base / 'results.json'}")
