#!/bin/sh
# A Tollgate slot is seeded by cloning another slot's tree, so its `target/` holds build-script
# outputs that name the other slot's absolute paths (Tauri's permissions, for one). Cargo's
# fingerprints don't notice the move and reuse them, and the build then reads files under a path
# that no longer exists. This cleans every package whose build-script output names a `target/`
# outside this checkout, so Cargo runs its build script again here.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$root"
packages=""
for output in target/*/build/*/output target/*/*/build/*/output; do
  [ -f "$output" ] || continue
  # Absolute paths into some target directory, other than this checkout's.
  if grep -Eo '/[^[:space:]]*/target/' "$output" | grep -qv "^$root/target/"; then
    dir=$(basename "$(dirname "$output")")
    packages="$packages ${dir%-*}"
  fi
done
for package in $(printf '%s\n' $packages | sort -u); do
  echo "clean-relocated-builds: $package's build script output names another checkout; cleaning it"
  cargo clean -p "$package" || echo "clean-relocated-builds: could not clean $package" >&2
done
