#!/usr/bin/env bash
# Clean up leftover Python venv (env/) directories from APDR benchmark runs.
# These are left behind when the benchmark process is killed mid-validation.
#
# Usage:
#   ./scripts/cleanup-envs.sh [RUNS_DIR]
#
# Default RUNS_DIR: d:/apr/runs

set -euo pipefail

RUNS_DIR="${1:-d:/apr/runs}"

if [ ! -d "$RUNS_DIR" ]; then
    echo "Runs directory not found: $RUNS_DIR"
    exit 1
fi

echo "Scanning for leftover env/ directories in $RUNS_DIR ..."

count=0
freed=0

while IFS= read -r cfg; do
    env_dir="$(dirname "$cfg")"
    size=$(du -sm "$env_dir" 2>/dev/null | cut -f1)
    echo "  Removing: $env_dir (${size:-?}MB)"
    rm -rf "$env_dir"
    count=$((count + 1))
    freed=$((freed + ${size:-0}))
done < <(find "$RUNS_DIR" -path "*/attempts/*/env/pyvenv.cfg" -type f 2>/dev/null)

echo ""
echo "Cleaned up $count env directories (~${freed}MB freed)"
