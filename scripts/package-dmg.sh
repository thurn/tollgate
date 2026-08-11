#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
npm --prefix "$project_root/ui" run bundle

app="$project_root/target/release/bundle/macos/Tollgate.app"
if [ ! -d "$app" ]; then
  echo "Tollgate.app was not produced" >&2
  exit 1
fi

staging=$(mktemp -d "${TMPDIR:-/tmp}/tollgate-dmg.XXXXXX")
trap 'rm -rf "$staging"' EXIT INT TERM

if [ -z "${APPLE_SIGNING_IDENTITY:-}" ]; then
  # The linker places an ad-hoc signature on the main executable, but an
  # unsigned bundle has no sealed-resource envelope. Seal the complete local
  # bundle so development DMGs pass the same structural verification as a
  # release-signed build without replacing a release identity when one exists.
  codesign --force --deep --sign - "$app"
else
  if [ -z "${TOLLGATE_NOTARY_PROFILE:-}" ]; then
    echo "TOLLGATE_NOTARY_PROFILE is required for a release-signed DMG" >&2
    exit 1
  fi
  ditto -c -k --keepParent "$app" "$staging/Tollgate-notarization.zip"
  xcrun notarytool submit "$staging/Tollgate-notarization.zip" \
    --keychain-profile "$TOLLGATE_NOTARY_PROFILE" --wait
  xcrun stapler staple "$app"
  xcrun stapler validate "$app"
fi
codesign --verify --deep --strict "$app"

version=$(node -e "const fs=require('fs'); console.log(JSON.parse(fs.readFileSync(process.argv[1])).version)" "$project_root/src-tauri/tauri.conf.json")
case "$(uname -m)" in
  arm64) architecture=aarch64 ;;
  x86_64) architecture=x64 ;;
  *) architecture=$(uname -m) ;;
esac
destination="$project_root/target/release/bundle/dmg/Tollgate_${version}_${architecture}.dmg"
cp -R "$app" "$staging/Tollgate.app"
rm -f "$staging/Tollgate-notarization.zip"
ln -s /Applications "$staging/Applications"
mkdir -p "$(dirname -- "$destination")"
hdiutil create -quiet -ov -volname Tollgate -srcfolder "$staging" -format UDZO "$destination"
if [ -n "${APPLE_SIGNING_IDENTITY:-}" ]; then
  xcrun notarytool submit "$destination" \
    --keychain-profile "$TOLLGATE_NOTARY_PROFILE" --wait
  xcrun stapler staple "$destination"
  xcrun stapler validate "$destination"
fi
hdiutil verify "$destination"
echo "$destination"
