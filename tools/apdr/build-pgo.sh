#!/usr/bin/env bash
# Profile-Guided Optimization (PGO) build for APDR (#9)
#
# PGO uses a two-phase build process:
#   1. Instrumented build — collects runtime profiles during representative workloads
#   2. Optimized build   — uses collected profiles to guide code layout, inlining, etc.
#
# Requirements:
#   - Rust nightly toolchain (for -Cprofile-generate / -Cprofile-use)
#   - llvm-profdata (bundled with rustup's llvm-tools component)
#
# Usage:
#   cd tools/apdr
#   ./build-pgo.sh
#
# The final binary is placed at target/release/apdr (same as a normal release build).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROFILE_DIR="$SCRIPT_DIR/target/pgo-profiles"
MERGED_PROFDATA="$PROFILE_DIR/merged.profdata"

# Ensure llvm-profdata is available
if ! rustup component list --installed | grep -q llvm-tools; then
    echo "Installing llvm-tools component..."
    rustup component add llvm-tools
fi

LLVM_PROFDATA=$(find "$(rustup run stable rustc --print sysroot)" -name llvm-profdata -type f 2>/dev/null | head -1)
if [ -z "$LLVM_PROFDATA" ]; then
    LLVM_PROFDATA=$(find "$(rustc --print sysroot)" -name llvm-profdata -type f 2>/dev/null | head -1)
fi
if [ -z "$LLVM_PROFDATA" ]; then
    echo "ERROR: llvm-profdata not found. Run: rustup component add llvm-tools"
    exit 1
fi

echo "=== Phase 1: Instrumented build ==="
rm -rf "$PROFILE_DIR"
mkdir -p "$PROFILE_DIR"

RUSTFLAGS="-Cprofile-generate=$PROFILE_DIR" \
    cargo build --release --target-dir target/pgo-build

echo "=== Phase 1.5: Collecting profiles ==="
# Run the instrumented binary against test suite and representative workloads
INSTRUMENTED="$SCRIPT_DIR/target/pgo-build/release/apdr"

# Run unit tests with the instrumented binary (exercises resolver, parser, etc.)
RUSTFLAGS="-Cprofile-generate=$PROFILE_DIR" \
    cargo test --release --target-dir target/pgo-build 2>/dev/null || true

# Run a quick benchmark if benchmark data exists
BENCHMARK_DIR="$SCRIPT_DIR/../../benchmark_dataset"
if [ -d "$BENCHMARK_DIR" ]; then
    echo "Running benchmark workload for profiling..."
    # Process a subset of cases to generate realistic profiles
    for snippet in $(find "$BENCHMARK_DIR" -name "snippet.py" -type f | head -20); do
        "$INSTRUMENTED" --snippet "$snippet" --timeout 30 2>/dev/null || true
    done
fi

echo "=== Phase 2: Merging profiles ==="
"$LLVM_PROFDATA" merge -o "$MERGED_PROFDATA" "$PROFILE_DIR"/*.profraw 2>/dev/null || \
"$LLVM_PROFDATA" merge -o "$MERGED_PROFDATA" "$PROFILE_DIR" 2>/dev/null || {
    echo "WARNING: No profile data collected. Building without PGO."
    cargo build --release
    exit 0
}

echo "=== Phase 3: Optimized build ==="
RUSTFLAGS="-Cprofile-use=$MERGED_PROFDATA -Cllvm-args=-pgo-warn-missing-function" \
    cargo build --release

echo "=== Done ==="
ls -lh target/release/apdr
echo "PGO-optimized binary: target/release/apdr"
