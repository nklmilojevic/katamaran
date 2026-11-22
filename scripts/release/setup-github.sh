#!/usr/bin/env bash
# One-time setup of the release pipeline (.github/workflows/release.yaml) for this repository.
# Stores Developer ID, App Store Connect notary, and Sparkle EdDSA signing material as GitHub
# Actions secrets/variables and enables GitHub Pages for docs + appcast.xml.
# Nothing is written to the repository or printed.
#
#   scripts/release/setup-github.sh \
#       --certificate  op://Private/Katamaran Developer ID/certificate.p12 \
#       --certificate-password op://Private/Katamaran Developer ID/password \
#       --notary-key   op://Private/Katamaran Notary API Key/AuthKey.p8 \
#       --notary-key-id op://Private/Katamaran Notary API Key/key id \
#       --notary-issuer op://Private/Katamaran Notary API Key/issuer id
#
# Each value is a 1Password reference (read with `op read`) or a local file path.
#   certificate          Developer ID Application certificate with its private key, as .p12
#                        (Keychain Access > Export).
#   notary key           App Store Connect API key (.p8, "Developer" access), its key id and
#                        issuer id (App Store Connect > Users and Access > Integrations).
# The Sparkle EdDSA key pair is created in your login keychain by Sparkle's generate_keys (or
# reused when it exists); back up the private key with:
#   generate_keys --account katamaran -x FILE
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
die() { echo "error: $*" >&2; exit 1; }
log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

CERT_REF="" CERT_PASS_REF="" NOTARY_REF="" NOTARY_ID_REF="" NOTARY_ISSUER_REF=""
while [[ $# -gt 0 ]]; do
    [[ $# -gt 1 ]] || die "$1 needs a value"
    case "$1" in
        --certificate) CERT_REF="$2" ;;
        --certificate-password) CERT_PASS_REF="$2" ;;
        --notary-key) NOTARY_REF="$2" ;;
        --notary-key-id) NOTARY_ID_REF="$2" ;;
        --notary-issuer) NOTARY_ISSUER_REF="$2" ;;
        *) die "unknown argument $1 (see the header of $0)" ;;
    esac
    shift 2
done
[[ -n "$CERT_REF" && -n "$CERT_PASS_REF" && -n "$NOTARY_REF" && -n "$NOTARY_ID_REF" && -n "$NOTARY_ISSUER_REF" ]] \
    || die "--certificate, --certificate-password, --notary-key, --notary-key-id and --notary-issuer are required"
command -v gh >/dev/null || die "gh not found"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
chmod 700 "$WORK"

expand_tilde() {
    case "$1" in
        "~"/*) printf '%s/%s' "$HOME" "${1#\~/}" ;;
        *) printf '%s' "$1" ;;
    esac
}

fetch_file() {
    if [[ "$1" == op://* ]]; then
        op read --force --out-file "$2" "$1" >/dev/null
    else
        local path
        path="$(expand_tilde "$1")"
        [[ -f "$path" ]] || die "$1 is not a file or an op:// reference"
        cp "$path" "$2"
    fi
}

fetch_text() {
    if [[ "$1" == op://* ]]; then
        op read --no-newline "$1"
    else
        local path
        path="$(expand_tilde "$1")"
        if [[ -f "$path" ]]; then
            head -n1 "$path" | tr -d '\r\n'
        else
            printf '%s' "$1"
        fi
    fi
}

REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
log "Configuring $REPO"

log "Developer ID certificate"
CERT="$WORK/certificate"
fetch_file "$CERT_REF" "$CERT"
fetch_text "$CERT_PASS_REF" > "$CERT.pass"
if ! IDENTITY="$(MACOS_CERTIFICATE_P12="$(base64 < "$CERT")" \
    MACOS_CERTIFICATE_PASSWORD="$(cat "$CERT.pass")" \
    "$HERE/ci-keychain.sh" create "$WORK/check.keychain-db")"; then
    "$HERE/ci-keychain.sh" delete "$WORK/check.keychain-db"
    die "the certificate does not open with that password (re-export it, or check the password item)"
fi
"$HERE/ci-keychain.sh" delete "$WORK/check.keychain-db"
[[ -n "$IDENTITY" ]] || die "the certificate holds no Developer ID Application identity with its private key"
log "Certificate OK: $IDENTITY"
base64 < "$CERT" | gh secret set MACOS_CERTIFICATE_P12 --repo "$REPO"
gh secret set MACOS_CERTIFICATE_PASSWORD --repo "$REPO" < "$CERT.pass"

log "Notarization API key"
NOTARY="$WORK/notary"
fetch_file "$NOTARY_REF" "$NOTARY"
grep -q "BEGIN PRIVATE KEY" "$NOTARY" || die "the notary key is not an App Store Connect .p8 key"
base64 < "$NOTARY" | gh secret set NOTARY_API_KEY_P8 --repo "$REPO"
fetch_text "$NOTARY_ID_REF" | gh secret set NOTARY_API_KEY_ID --repo "$REPO"
fetch_text "$NOTARY_ISSUER_REF" | gh secret set NOTARY_API_KEY_ISSUER_ID --repo "$REPO"

log "Sparkle update signing key"
(cd "$ROOT/app" && swift package resolve >/dev/null)
GENERATE_KEYS="$(find "$ROOT/app/.build/artifacts" -path '*/bin/generate_keys' -type f | head -n1)"
[[ -x "$GENERATE_KEYS" ]] || die "generate_keys not found under app/.build/artifacts"
"$GENERATE_KEYS" --account katamaran >/dev/null
PUBLIC="$("$GENERATE_KEYS" --account katamaran -p)"
[[ -n "$PUBLIC" ]] || die "generate_keys printed no public key"
gh variable set SPARKLE_PUBLIC_KEY --repo "$REPO" --body "$PUBLIC"
"$GENERATE_KEYS" --account katamaran -x "$WORK/sparkle" >/dev/null
gh secret set SPARKLE_PRIVATE_KEY --repo "$REPO" < "$WORK/sparkle"

log "GitHub Pages (docs and appcast), deployed by pages.yml"
if gh api -X POST "repos/$REPO/pages" -f build_type=workflow >/dev/null 2>&1 \
    || gh api -X PUT "repos/$REPO/pages" -f build_type=workflow >/dev/null 2>&1; then
    gh api -X PUT "repos/$REPO/environments/github-pages" \
        -F 'deployment_branch_policy[protected_branches]=false' \
        -F 'deployment_branch_policy[custom_branch_policies]=true' >/dev/null
    for spec in "main:branch" "v*:tag"; do
        IFS=: read -r name kind <<< "$spec"
        gh api -X POST "repos/$REPO/environments/github-pages/deployment-branch-policies" \
            -f name="$name" -f type="$kind" >/dev/null 2>&1 || true
    done
    URL="$(gh api "repos/$REPO/pages" -q .html_url)"
    log "Done. Docs and appcast: ${URL}appcast.xml (published after the first Release run)"
    echo "Set the SPARKLE_FEED_URL repository variable if you serve appcast.xml from a custom domain."
else
    log "Skipped GitHub Pages (repository has no default branch yet or Pages is unavailable)."
    echo "After pushing main, enable Pages (Source: GitHub Actions) in repo Settings > Pages,"
    echo "or run: gh api -X POST repos/$REPO/pages -f build_type=workflow"
fi
