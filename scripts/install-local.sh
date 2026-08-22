#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
install_dir=${TOLLGATE_INSTALL_DIR:-/Applications}
built_app="$project_root/target/release/bundle/macos/Tollgate.app"
installed_app="$install_dir/Tollgate.app"
installed_executable="$installed_app/Contents/MacOS/tollgate-app"
cli_link="$HOME/.local/bin/tg"

if [ "$(uname -s)" != Darwin ]; then
  echo "Tollgate can only be installed on macOS." >&2
  exit 1
fi

for command_name in cargo codesign ditto npm rustc open osascript; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command is unavailable: $command_name" >&2
    exit 1
  fi
done

if [ ! -d "$install_dir" ]; then
  echo "Install directory does not exist: $install_dir" >&2
  exit 1
fi
if [ ! -w "$install_dir" ]; then
  echo "Install directory is not writable: $install_dir" >&2
  echo "Set TOLLGATE_INSTALL_DIR to a writable Applications directory." >&2
  exit 1
fi
if [ -e "$cli_link" ] && [ ! -L "$cli_link" ]; then
  echo "Refusing to replace the non-symlink CLI at $cli_link" >&2
  exit 1
fi

echo "Installing locked UI dependencies..."
npm --prefix "$project_root/ui" ci

echo "Building Tollgate.app, tg, and tollgate-worker..."
npm --prefix "$project_root/ui" run bundle

if [ ! -x "$built_app/Contents/MacOS/tollgate-app" ] ||
   [ ! -x "$built_app/Contents/MacOS/tg" ] ||
   [ ! -x "$built_app/Contents/MacOS/tollgate-worker" ]; then
  echo "The release build did not produce a complete Tollgate.app." >&2
  exit 1
fi
if [ -z "${APPLE_SIGNING_IDENTITY:-}" ]; then
  codesign --force --deep --sign - "$built_app"
fi
codesign --verify --deep --strict "$built_app"

tollgate_pid() {
  ps -axo pid=,comm= | awk 'index($0, "/Tollgate.app/Contents/MacOS/tollgate-app") { print $1; exit }'
}

installed_pid() {
  ps -axo pid=,comm= | awk -v executable="$installed_executable" 'index($0, executable) { print $1; exit }'
}

pid=$(tollgate_pid)
if [ -n "$pid" ]; then
  echo "Stopping the installed Tollgate app gracefully..."
  osascript -e 'tell application id "dev.tollgate.desktop" to quit'
  attempts=0
  while [ -n "$(tollgate_pid)" ] && [ "$attempts" -lt 30 ]; do
    sleep 1
    attempts=$((attempts + 1))
  done
  if [ -n "$(tollgate_pid)" ]; then
    echo "Tollgate did not quit within 30 seconds; the installed app was not changed." >&2
    exit 1
  fi
fi

staging=$(mktemp -d "$install_dir/.tollgate-install.XXXXXX")
staged_app="$staging/Tollgate.app"
previous_app="$staging/Tollgate.previous.app"

cleanup() {
  if [ ! -d "$installed_app" ] && [ -d "$previous_app" ]; then
    mv "$previous_app" "$installed_app"
  fi
  rm -rf "$staging"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Installing $installed_app..."
ditto "$built_app" "$staged_app"
if [ -e "$installed_app" ]; then
  mv "$installed_app" "$previous_app"
fi
mv "$staged_app" "$installed_app"

mkdir -p "$(dirname -- "$cli_link")"
ln -sfn "$installed_app/Contents/MacOS/tg" "$cli_link"

echo "Launching Tollgate..."
open "$installed_app"

attempts=0
until [ -n "$(installed_pid)" ] && "$cli_link" --no-launch doctor >/dev/null 2>&1; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 30 ]; then
    echo "Tollgate was installed, but it did not become healthy within 30 seconds." >&2
    exit 1
  fi
  sleep 1
done

"$cli_link" --no-launch doctor
echo "Installed and running: $installed_app"
