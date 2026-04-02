#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export CARGO_TARGET_DIR="${APDR_TARGET_DIR:-$HOME/.cache/apdr/target}"

echo "Building APDR Rust CLI..."
echo "Using Cargo target dir: $CARGO_TARGET_DIR"
cargo build --release
echo "APDR build complete: $CARGO_TARGET_DIR/release/apdr"
