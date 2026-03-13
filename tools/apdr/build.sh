#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building APDR Rust CLI..."
cargo build --release
echo "APDR build complete: $SCRIPT_DIR/target/release/apdr"

