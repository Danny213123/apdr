#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
APDR_DIR="$REPO_ROOT/tools/apdr"
REPO_TARGET_DIR="$APDR_DIR/target"

usage() {
  cat <<'EOF'
Usage: bash scripts/cleanup-apdr-footprint.sh [--dry-run] [--apply] [--cache-path DIR] [--target-dir DIR]

Options:
  --dry-run          Report cache and target usage without deleting anything (default)
  --apply            Reclaim cache and target bytes
  --cache-path DIR   Override the APDR cache path to inspect/prune
  --target-dir DIR   Override the Cargo target dir to clean
  --help             Show this help text
EOF
}

default_cache_path() {
  if [[ -n "${APDR_CACHE_DIR:-}" ]]; then
    printf '%s\n' "$APDR_CACHE_DIR"
    return
  fi
  if [[ -n "${LOCALAPPDATA:-}" ]]; then
    printf '%s\n' "$LOCALAPPDATA/apdr"
    return
  fi
  if [[ "$(uname -s)" == "Darwin" ]]; then
    printf '%s\n' "$HOME/Library/Caches/apdr"
    return
  fi
  printf '%s\n' "${XDG_CACHE_HOME:-$HOME/.cache}/apdr"
}

bytes_for_path() {
  local path="$1"
  if [[ ! -e "$path" ]]; then
    printf '0\n'
    return
  fi
  du -sk "$path" 2>/dev/null | awk '{print $1 * 1024}'
}

MODE="dry-run"
CACHE_PATH=""
TARGET_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      MODE="dry-run"
      ;;
    --apply)
      MODE="apply"
      ;;
    --cache-path)
      shift
      CACHE_PATH="${1:-}"
      if [[ -z "$CACHE_PATH" ]]; then
        echo "--cache-path expects a value" >&2
        exit 1
      fi
      ;;
    --target-dir)
      shift
      TARGET_DIR="${1:-}"
      if [[ -z "$TARGET_DIR" ]]; then
        echo "--target-dir expects a value" >&2
        exit 1
      fi
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown flag: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if [[ -z "$CACHE_PATH" ]]; then
  CACHE_PATH="$(default_cache_path)"
fi

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="${APDR_TARGET_DIR:-$HOME/.cache/apdr/target}"
fi

shopt -s nullglob
EXTRA_TARGET_DIRS=("$APDR_DIR"/target-*)
shopt -u nullglob

echo "Mode: $MODE"
echo "Cache path: $CACHE_PATH"
echo "Target dir: $TARGET_DIR"
echo "Repo target bytes: $(bytes_for_path "$REPO_TARGET_DIR")"
for extra_dir in "${EXTRA_TARGET_DIRS[@]}"; do
  echo "Extra target bytes [$extra_dir]: $(bytes_for_path "$extra_dir")"
done
echo "Cache bytes: $(bytes_for_path "$CACHE_PATH")"

if [[ "$MODE" != "apply" ]]; then
  echo "Dry run only. Re-run with --apply to delete anything."
  exit 0
fi

export CARGO_TARGET_DIR="$TARGET_DIR"

if [[ -d "$TARGET_DIR" ]]; then
  cargo clean --manifest-path tools/apdr/Cargo.toml --target-dir "$TARGET_DIR"
fi

if [[ -d "$REPO_TARGET_DIR" && "$REPO_TARGET_DIR" != "$TARGET_DIR" ]]; then
  cargo clean --manifest-path tools/apdr/Cargo.toml --target-dir "$REPO_TARGET_DIR"
fi

for extra_dir in "${EXTRA_TARGET_DIRS[@]}"; do
  if [[ -d "$extra_dir" ]]; then
    rm -rf "$extra_dir"
  fi
done

cargo run --manifest-path tools/apdr/Cargo.toml -- cache --cache-path "$CACHE_PATH" prune --max-validated-envs 8 --max-validated-env-gb 1 --max-wheelhouse-gb 1
