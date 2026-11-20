#!/usr/bin/env bash
# Build a signed and notarized Katamaran release into target/dist/:
#
#   Katamaran-<version>.dmg
#   Katamaran-<version>.dmg.sha256
#   Katamaran.dmg
#   Katamaran-<version>.zip            Sparkle update archive (stapled, EdDSA-signed)
#   sparkle.json                       Appcast metadata for scripts/release/appcast.py
#
#   scripts/release/build-release.sh
#
# Environment:
#   KATAMARAN_SIGN_IDENTITY        "Developer ID Application: ..."
#   KATAMARAN_BUILD_NUMBER         CFBundleVersion (monotonic integer, e.g. commit count)
#   KATAMARAN_FEED_URL             Sparkle appcast URL compiled into Info.plist
#   KATAMARAN_SPARKLE_PUBLIC_KEY   Sparkle EdDSA public key compiled into Info.plist
#   SPARKLE_KEY_FILE               Path to matching Sparkle EdDSA private key file
#   KATAMARAN_DOWNLOAD_BASE        Base URL where release assets are hosted
#   NOTARY_KEY_PATH, NOTARY_KEY_ID, NOTARY_ISSUER_ID   (or NOTARY_PROFILE)
#   KATAMARAN_VERSION              optional override (defaults to Cargo.toml workspace version)
#   KATAMARAN_NOTES_FILE           optional Markdown release notes for sparkle.json
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
die() { echo "error: $*" >&2; exit 1; }
log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

for v in KATAMARAN_SIGN_IDENTITY KATAMARAN_BUILD_NUMBER KATAMARAN_FEED_URL \
    KATAMARAN_SPARKLE_PUBLIC_KEY SPARKLE_KEY_FILE KATAMARAN_DOWNLOAD_BASE; do
    [[ -n "${!v:-}" ]] || die "$v is not set"
done
[[ "$KATAMARAN_SIGN_IDENTITY" != "-" ]] || die "KATAMARAN_SIGN_IDENTITY cannot be ad-hoc (-)"
[[ -f "$SPARKLE_KEY_FILE" ]] || die "SPARKLE_KEY_FILE $SPARKLE_KEY_FILE not found"
[[ "$KATAMARAN_BUILD_NUMBER" =~ ^[0-9]+$ ]] || die "KATAMARAN_BUILD_NUMBER must be an integer"
if ! [[ "$KATAMARAN_FEED_URL" =~ ^https://[^/]+/[^/] && "${KATAMARAN_FEED_URL#https://}" != *//* ]]; then
    die "KATAMARAN_FEED_URL is not a valid https URL: $KATAMARAN_FEED_URL"
fi

VERSION="${KATAMARAN_VERSION:-$(sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT/Cargo.toml" | head -n1)}"
[[ -n "$VERSION" ]] || die "could not determine version from Cargo.toml"

cd "$ROOT"

for f in guest/out/Image guest/out/rootfs.erofs guest/out/data.img; do
    [[ -f "$f" ]] || die "$f is missing; build the guest images first"
done

log "Building signed Katamaran.app ($VERSION, build $KATAMARAN_BUILD_NUMBER)"
cargo xtask app

APP="$ROOT/target/Katamaran.app"
MACOS="$APP/Contents/MacOS"
SPARKLE="$APP/Contents/Frameworks/Sparkle.framework"

log "Verifying signatures, hardened runtime, timestamps, architecture, and entitlements"
for binary in "$APP" "$MACOS/KatamaranApp" "$MACOS/katamaran" "$MACOS/katamaran-vm" \
    "$MACOS/gvproxy" "$MACOS/libgvproxy.dylib" \
    "$SPARKLE" "$SPARKLE/Versions/B/Autoupdate" "$SPARKLE/Versions/B/Updater.app" \
    "$SPARKLE/Versions/B/XPCServices/Installer.xpc" \
    "$SPARKLE/Versions/B/XPCServices/Downloader.xpc"; do
    codesign --verify --strict --verbose=2 "$binary" || die "invalid signature: $binary"
    details="$(codesign -dv --verbose=4 "$binary" 2>&1)"
    grep -q '^Authority=Developer ID Application:' <<<"$details" || die "not signed with Developer ID: $binary"
    grep -q 'flags=.*runtime' <<<"$details" || die "hardened runtime missing: $binary"
    grep -q '^Timestamp=' <<<"$details" || die "secure timestamp missing: $binary"
done
codesign -d --entitlements - "$MACOS/katamaran-vm" 2>&1 \
    | grep -q 'com.apple.security.hypervisor' \
    || die "com.apple.security.hypervisor entitlement missing on katamaran-vm"

for binary in "$MACOS/KatamaranApp" "$MACOS/katamaran" "$MACOS/katamaran-vm" \
    "$MACOS/gvproxy" "$MACOS/libgvproxy.dylib"; do
    lipo "$binary" -verify_arch arm64 || die "$binary is not arm64"
    if otool -L "$binary" | grep -q '/nix/store/'; then
        die "$(basename "$binary") links libraries from /nix/store; build with /usr/bin first on PATH"
    fi
done

log "Notarizing and stapling Katamaran.app"
"$HERE/notarize.sh" "$APP"
spctl --assess --type execute --verbose=2 "$APP"

DIST="$ROOT/target/dist"
rm -rf "$DIST"
mkdir -p "$DIST"

ZIP_NAME="Katamaran-$VERSION.zip"
log "Creating Sparkle update archive ($ZIP_NAME)"
ditto -c -k --sequesterRsrc --keepParent "$APP" "$DIST/$ZIP_NAME"

SIGN_UPDATE="$(find "$ROOT/app/.build/artifacts" -path '*/bin/sign_update' -type f | head -n1)"
[[ -x "$SIGN_UPDATE" ]] || die "sign_update not found under app/.build/artifacts"
SIGNATURE="$("$SIGN_UPDATE" --ed-key-file "$SPARKLE_KEY_FILE" -p "$DIST/$ZIP_NAME")"
[[ -n "$SIGNATURE" ]] || die "sign_update produced no signature"
LENGTH="$(stat -f %z "$DIST/$ZIP_NAME")"
MIN_OS="$(/usr/libexec/PlistBuddy -c 'Print LSMinimumSystemVersion' "$APP/Contents/Info.plist")"

META_VERSION="$KATAMARAN_BUILD_NUMBER" META_SHORT="$VERSION" \
META_COMMIT="$(git -C "$ROOT" rev-parse HEAD)" META_URL="$KATAMARAN_DOWNLOAD_BASE/$ZIP_NAME" \
META_LENGTH="$LENGTH" META_SIGNATURE="$SIGNATURE" META_MIN_OS="$MIN_OS" \
META_NOTES_FILE="${KATAMARAN_NOTES_FILE:-}" \
python3 - "$DIST/sparkle.json" <<'EOF'
import json, os, sys, time
e = os.environ
notes_file = e.get("META_NOTES_FILE", "")
notes = open(notes_file, encoding="utf-8").read() if notes_file and os.path.isfile(notes_file) else ""
with open(sys.argv[1], "w", encoding="utf-8") as f:
    json.dump({
        "channel": "release",
        "version": e["META_VERSION"],
        "short_version": e["META_SHORT"],
        "commit": e["META_COMMIT"],
        "url": e["META_URL"],
        "length": int(e["META_LENGTH"]),
        "ed_signature": e["META_SIGNATURE"],
        "minimum_system_version": e["META_MIN_OS"],
        "published": time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime()),
        "notes": notes,
    }, f, indent=2)
EOF

log "Creating signed and notarized DMG"
WORK="$(mktemp -d "$DIST/dmg.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/stage"
ditto "$APP" "$WORK/stage/Katamaran.app"
ln -s /Applications "$WORK/stage/Applications"

DMG_NAME="Katamaran-$VERSION.dmg"
DMG="$DIST/$DMG_NAME"
hdiutil create -volname Katamaran -fs APFS -format ULMO -srcfolder "$WORK/stage" "$DMG"
codesign --force --timestamp --sign "$KATAMARAN_SIGN_IDENTITY" "$DMG"
codesign --verify --strict "$DMG"

"$HERE/notarize.sh" "$DMG"
spctl --assess --type open --context context:primary-signature --verbose=2 "$DMG"
hdiutil verify "$DMG"

cp "$DMG" "$DIST/Katamaran.dmg"
(cd "$DIST" && shasum -a 256 "$DMG_NAME" > "$DMG_NAME.sha256")
cat "$DIST/$DMG_NAME.sha256"
log "Done: $(ls "$DIST" | tr '\n' ' ')"
