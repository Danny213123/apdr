#!/bin/bash
# Batch test all fixtures with Phase 3 features

FIXTURES_DIR="tests/fixtures"
OUTPUT_BASE="out/fixture_tests"
RESULTS_FILE="$OUTPUT_BASE/results.txt"

mkdir -p "$OUTPUT_BASE"
rm -f "$RESULTS_FILE"

echo "=== APDR Fixture Batch Test ===" | tee "$RESULTS_FILE"
echo "Started: $(date)" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

PASS=0
FAIL=0
SKIP=0
TOTAL=0

for fixture in $(find "$FIXTURES_DIR" -name "*.py" -type f ! -name "__*" | sort); do
    TOTAL=$((TOTAL + 1))
    fixture_name=$(basename "$fixture" .py)
    output_dir="$OUTPUT_BASE/$fixture_name"

    echo "[$TOTAL] Testing: $fixture_name" | tee -a "$RESULTS_FILE"

    # Run APDR with --no-validate for speed
    ./target/release/apdr.exe resolve "$fixture" --output "$output_dir" --python 3.9 --allow-llm --no-validate > "$output_dir/output.log" 2>&1
    exit_code=$?

    # Parse result
    if [ $exit_code -eq 0 ]; then
        status="PASS"
        PASS=$((PASS + 1))
    elif grep -q "skipped" "$output_dir/output.log" 2>/dev/null; then
        status="SKIP"
        SKIP=$((SKIP + 1))
    else
        status="FAIL"
        FAIL=$((FAIL + 1))
    fi

    # Extract key metrics
    resolved=$(grep "RESOLVED_COUNT=" "$output_dir/output.log" 2>/dev/null | cut -d= -f2)
    unresolved=$(grep "UNRESOLVED_COUNT=" "$output_dir/output.log" 2>/dev/null | cut -d= -f2)
    llm_calls=$(grep "LLM_CALLS=" "$output_dir/output.log" 2>/dev/null | cut -d= -f2)

    echo "  Status: $status | Resolved: $resolved | Unresolved: $unresolved | LLM: $llm_calls" | tee -a "$RESULTS_FILE"
done

echo "" | tee -a "$RESULTS_FILE"
echo "=== Summary ===" | tee -a "$RESULTS_FILE"
echo "Total:  $TOTAL" | tee -a "$RESULTS_FILE"
echo "Pass:   $PASS" | tee -a "$RESULTS_FILE"
echo "Fail:   $FAIL" | tee -a "$RESULTS_FILE"
echo "Skip:   $SKIP" | tee -a "$RESULTS_FILE"
echo "Ended: $(date)" | tee -a "$RESULTS_FILE"
