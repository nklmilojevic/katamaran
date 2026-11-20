#!/usr/bin/env bash
# Build the katamaran data-disk template: guest/out/data.img, a sparse raw image (64 GiB apparent,
# a few MiB allocated) holding a btrfs filesystem on the whole disk (no partition table), label
# katamaran-data, with top-level subvolumes docker, containerd and katamaran. kat-init attaches it as
# /dev/vdb and mounts subvol=docker on /var/lib/docker, subvol=containerd on /var/lib/containerd and
# subvol=katamaran on /var/lib/katamaran. The host APFS-clones the template on first run.
#
# Usage (from anywhere, on the macOS host):
#   guest/disk/build.sh          build guest/out/data.img, check it offline, then check that an APFS
#                                clone (cp -c) stays sparse
#   guest/disk/build.sh verify   only the checks (btrfs check, label, subvolume list, sparseness)
#
# mkfs runs in a Linux container on a Docker engine: the context named by KATAMARAN_BUILD_CONTEXT, or
# the current Docker context if it is unset (no context is ever changed). Containers are named
# `katamaran-kbuild-*` and run with --rm. mkfs.btrfs creates the subvolumes itself (--rootdir +
# --subvol), so no loop mount or privileged container is needed. Nothing is installed on the host.
set -euo pipefail

ALPINE_IMAGE=alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf
SIZE=64G
LABEL=katamaran-data
FS_UUID=5efe76c9-10f8-4ff2-9a86-c88da9df4f23
SUBVOLS="docker containerd katamaran"

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=$(cd "$HERE/../.." && pwd)/guest/out
DOCKER=(docker)
if [[ -n ${KATAMARAN_BUILD_CONTEXT:-} ]]; then
    DOCKER+=(--context "$KATAMARAN_BUILD_CONTEXT")
fi
IMG=$OUT/data.img

run() {
    local name=$1; shift
    "${DOCKER[@]}" run --rm -i --name "katamaran-kbuild-$name-$$" --platform linux/arm64 \
        -v "$OUT:/out" -e SIZE="$SIZE" -e LABEL="$LABEL" -e FS_UUID="$FS_UUID" -e SUBVOLS="$SUBVOLS" \
        "$ALPINE_IMAGE" sh -s
}

sizes() {
    # apparent (st_size) and allocated (st_blocks * 512) bytes
    if [[ "$(uname -s)" == Darwin ]]; then
        stat -f '%z %b' "$1"
    else
        stat -c '%s %b' "$1"
    fi | awk -v f="$2" '{ printf "%s: apparent %.1f GiB, allocated %.1f MiB\n", f, $1/2^30, $2*512/2^20 }'
}

build() {
    run disk <<'EOF'
set -eu
apk add -q --no-progress btrfs-progs
mkdir -p /work/root
sv_args=
for s in $SUBVOLS; do mkdir /work/root/$s; sv_args="$sv_args --subvol rw:$s"; done
rm -f /out/data.img.tmp
truncate -s "$SIZE" /out/data.img.tmp
# -K: no whole-device TRIM (it would only walk the empty sparse file).
# shellcheck disable=SC2086
mkfs.btrfs -q -K -L "$LABEL" -U "$FS_UUID" --rootdir /work/root $sv_args /out/data.img.tmp
mv /out/data.img.tmp /out/data.img
EOF
}

verify() {
    run disk-verify <<'EOF'
set -eu
apk add -q --no-progress btrfs-progs
btrfs check --readonly /out/data.img >/dev/null 2>&1 || { echo "btrfs check: FAILED" >&2; exit 1; }
echo "btrfs check: OK"
btrfs filesystem show /out/data.img | head -1
echo "subvolumes: $(btrfs inspect-internal dump-tree -t root /out/data.img | awk '/ROOT_REF/ { getline; print $NF }' | sort | xargs)"
EOF
    sizes "$IMG" data.img
    if [[ "$(uname -s)" == Darwin ]]; then
        # Same volume as the template, so cp -c is a real APFS clone.
        local clone=$OUT/.data-clone-$$.img
        cp -c "$IMG" "$clone"
        sizes "$clone" "cp -c clone"
        du -h "$IMG" "$clone"
        rm -f "$clone"
    fi
}

case ${1:-build} in
    build)
        mkdir -p "$OUT"
        start=$SECONDS
        build
        echo "built guest/out/data.img in $((SECONDS - start)) s"
        verify
        ;;
    verify) verify ;;
    *) echo "usage: $0 [build|verify]" >&2; exit 2 ;;
esac
