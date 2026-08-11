#!/bin/sh
set -eu

profile="${1:-debug}"
case "$profile" in
  debug) cargo_args="" ;;
  release) cargo_args="--release" ;;
  *) echo "usage: $0 [debug|release]" >&2; exit 2 ;;
esac

workspace_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
target_triple=${TOLLGATE_TARGET:-${TAURI_ENV_TARGET_TRIPLE:-$(rustc -vV | sed -n 's/^host: //p')}}
cargo_target_args="--target $target_triple"
if [ -n "$cargo_args" ]; then
  cargo build --manifest-path "$workspace_root/Cargo.toml" -p tollgate-worker -p tg $cargo_target_args "$cargo_args"
else
  cargo build --manifest-path "$workspace_root/Cargo.toml" -p tollgate-worker -p tg $cargo_target_args
fi
mkdir -p "$workspace_root/src-tauri/binaries"
cp "$workspace_root/target/$target_triple/$profile/tollgate-worker" \
  "$workspace_root/src-tauri/binaries/tollgate-worker-$target_triple"
cp "$workspace_root/target/$target_triple/$profile/tg" \
  "$workspace_root/src-tauri/binaries/tg-$target_triple"
