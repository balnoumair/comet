#!/usr/bin/env bash
# Map PR file changes to workspace packages that need `cargo test`.
# Prints one package name per line, or "all" / "none".
set -euo pipefail

BASE="${1:?usage: ci-affected-crates.sh <base-ref>}"
HEAD="${2:-HEAD}"

changed="$(git diff --name-only "${BASE}...${HEAD}")"
if [ -z "$changed" ]; then
  echo none
  exit 0
fi

# Workspace-wide edits: run the full suite.
if printf '%s\n' "$changed" | grep -qE '^(Cargo\.toml|rust-toolchain\.toml)'; then
  echo all
  exit 0
fi

# CI-only changes: no crate tests needed.
if printf '%s\n' "$changed" | grep -qv '^\.github/'; then
  :
else
  echo none
  exit 0
fi

path_to_pkg() {
  case "$1" in
    crates/proto) echo zeron-proto ;;
    crates/doc) echo zeron-doc ;;
    crates/sync) echo zeron-sync ;;
    crates/harness) echo zeron-harness ;;
    crates/engine) echo zeron-engine ;;
    crates/rpc) echo zeron-rpc ;;
  esac
}

dependents() {
  case "$1" in
    zeron-proto) echo zeron-doc zeron-harness zeron-rpc zeron-engine ;;
    zeron-doc) echo zeron-rpc zeron-engine ;;
    zeron-sync) echo zeron-engine ;;
    zeron-harness) echo zeron-engine ;;
    zeron-rpc) echo zeron-engine ;;
    zeron-engine) ;;
  esac
}

paths=(
  crates/proto
  crates/doc
  crates/sync
  crates/harness
  crates/engine
  crates/rpc
)

touched=()
for path in "${paths[@]}"; do
  if printf '%s\n' "$changed" | grep -q "^${path}/"; then
    pkg="$(path_to_pkg "$path")"
    touched+=("$pkg")
  fi
done

if [ "${#touched[@]}" -eq 0 ]; then
  echo none
  exit 0
fi

affected=()
for pkg in "${touched[@]}"; do
  affected+=("$pkg")
  read -r -a down <<< "$(dependents "$pkg")"
  if ((${#down[@]})); then
    affected+=("${down[@]}")
  fi
done

printf '%s\n' "${affected[@]}" | sort -u
