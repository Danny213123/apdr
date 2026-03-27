#!/bin/bash
# Retest cases that should be fixed by new reference aliases

CASES="10005117 11405336 1218932 1720032 1780878 2358089 2392540 2e0cbef7d50e8e7ef2d2 3989161 5794494 6532185 b4ea0034839ccbc2aa03"
APDR="./tools/apdr/target/release/apdr.exe"
OUTPUT_BASE="./retest_results"

mkdir -p "$OUTPUT_BASE"

PASSED=0
FAILED=0

for case_id in $CASES; do
    echo "=== Testing case $case_id ==="

    # Find snippet file in hard-gists
    snippet_file=$(find hard-gists/$case_id -name "snippet.py" 2>/dev/null | head -1)

    if [ -z "$snippet_file" ]; then
        echo "  SKIP: snippet not found"
        continue
    fi

    output_dir="$OUTPUT_BASE/$case_id"

    # Run APDR
    $APDR resolve "$snippet_file" --output "$output_dir" --python 3.9 --allow-llm --no-validate > /dev/null 2>&1

    # Check result
    if grep -q "validation_status: passed" "$output_dir/resolution-report.txt" 2>/dev/null; then
        echo "  PASS"
        PASSED=$((PASSED + 1))
    else
        status=$(grep "validation_status:" "$output_dir/resolution-report.txt" 2>/dev/null | cut -d: -f2 | tr -d ' ')
        echo "  FAIL ($status)"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "=== Summary ==="
echo "Passed: $PASSED"
echo "Failed: $FAILED"
echo "Total: $((PASSED + FAILED))"
