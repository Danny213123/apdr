#!/bin/bash
# Retest a sample of module-not-found failures

BENCHMARK_DIR="runs/20260325-010231-apdr/cases"
APDR="./tools/apdr/target/release/apdr.exe"
OUTPUT_BASE="./retest_sample"
SAMPLE_SIZE=50

mkdir -p "$OUTPUT_BASE"

# Get all module-not-found cases
cd "$BENCHMARK_DIR"
module_not_found_cases=$(grep -l "validation_status: module-not-found" */resolution-report.txt | cut -d/ -f1)
cd ../..

# Take a sample (first N cases)
sample_cases=$(echo "$module_not_found_cases" | head -$SAMPLE_SIZE)

PASSED=0
FAILED=0
TOTAL=0

echo "=== Testing $SAMPLE_SIZE module-not-found cases ==="
echo ""

for case_id in $sample_cases; do
    TOTAL=$((TOTAL + 1))
    printf "[%3d/%3d] %s ... " $TOTAL $SAMPLE_SIZE "$case_id"

    # Find snippet file (try hard-gists first, then benchmark cases)
    snippet_file=$(find hard-gists/$case_id -name "snippet.py" 2>/dev/null | head -1)

    if [ -z "$snippet_file" ]; then
        snippet_file=$(find runs/20260325-010231-apdr/cases/$case_id/.apdr-debug/attempts -name "snippet.py" 2>/dev/null | head -1)
    fi

    if [ -z "$snippet_file" ]; then
        echo "SKIP (no snippet)"
        continue
    fi

    output_dir="$OUTPUT_BASE/$case_id"

    # Run APDR
    $APDR resolve "$snippet_file" --output "$output_dir" --python 3.9 --allow-llm --no-validate > /dev/null 2>&1

    # Check result
    if grep -q "validation_status: passed" "$output_dir/resolution-report.txt" 2>/dev/null; then
        echo "PASS"
        PASSED=$((PASSED + 1))
    else
        status=$(grep "validation_status:" "$output_dir/resolution-report.txt" 2>/dev/null | cut -d: -f2 | tr -d ' ')
        echo "FAIL ($status)"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "=== Summary ==="
echo "Passed: $PASSED / $TOTAL ($((PASSED * 100 / TOTAL))%)"
echo "Failed: $FAILED / $TOTAL"
echo ""
echo "Improvement: $PASSED cases fixed out of $TOTAL sampled"
