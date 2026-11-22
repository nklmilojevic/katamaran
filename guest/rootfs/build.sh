#!/usr/bin/env bash
# Build the katamaran guest system image: guest/out/rootfs.erofs, a read-only EROFS (lz4hc) image of an
# Alpine aarch64 userland with the Docker engine static binaries. kat-init mounts it from /dev/vda and
# uses it as the lower layer of an overlay with a tmpfs upper.
#
# Usage (from anywhere, on the macOS host):
#   guest/rootfs/build.sh          build guest/out/rootfs.erofs (+ rootfs.packages.txt), then verify it
#   guest/rootfs/build.sh verify   verify an existing image: fsck.erofs --extract, then chroot into the tree
#                                  and run dockerd/containerd/runc/iptables --version and the config checks
#   guest/rootfs/build.sh smoke [storage-driver]
#                                  boot-like end-to-end test in a privileged container: loop-mount
#                                  rootfs.erofs under a tmpfs overlay, mount the subvolumes of a scratch
#                                  copy of guest/out/data.img (guest/disk/build.sh), start containerd and
#                                  dockerd from the image, then `docker run --rm alpine true`
#
# Everything runs in Linux containers on a Docker engine: the context named by KATAMARAN_BUILD_CONTEXT,
# or the current Docker context if it is unset (no context is ever changed). Containers are named
# `katamaran-kbuild-*` and run with --rm. Nothing is installed on the host.
#
# Reproducibility: pinned Alpine image digest and branch, pinned Docker tarball sha256, every file
# timestamp set to SOURCE_DATE_EPOCH, fixed filesystem UUID. Alpine packages come from the pinned
# stable branch, so a branch package update changes the image; rootfs.packages.txt records the exact
# package versions of each build.
set -euo pipefail

ALPINE_VERSION=3.24.4
ALPINE_BRANCH=v3.24
# Multi-arch index digest of alpine:3.24.4 (linux/arm64/v8 manifest: sha256:1829893486be257df5049ab3b7fc95afbd0fc09d89803cbc580bda1ba5386b29).
ALPINE_IMAGE=alpine:${ALPINE_VERSION}@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

DOCKER_VERSION=29.10.0
DOCKER_URL=https://download.docker.com/linux/static/stable/aarch64/docker-${DOCKER_VERSION}.tgz
# download.docker.com publishes no checksums for the static tarballs; this is the sha256 of the
# tarball as first downloaded (2026-11-18).
DOCKER_SHA256=6e5159e8b5615e0c0c2954a2a1a2a12bc65f5d9b603595dba24a10cb367408cd
DOCKER_BINS="dockerd containerd containerd-shim-runc-v2 runc docker-init docker-proxy ctr"

# x86-64 user-mode emulator for amd64 images when Rosetta is not used (kat-init registers it with
# binfmt_misc). Alpine's build is a static PIE, so the F flag works inside every container. Pinned
# by package version and by the sha256 of the installed binary.
QEMU_VERSION=11.0.3-r0
QEMU_SHA256=2af820864493b720fcfed48561678a249b1f5df07cf4c77dc906a874a0b3ee6f

# 2026-11-16 00:00:00 UTC, the Docker ${DOCKER_VERSION} release date.
SOURCE_DATE_EPOCH=1794787200
EROFS_UUID=9cf133a8-f288-41c0-af85-cfd1d561af5f
EROFS_LABEL=katamaran-root

# busybox (+ /bin/sh) is the debug shell; iptables ships ip6tables and defaults to the nft backend;
# mount/umount are util-linux; shadow is for the userns-remap subuid/subgid files later.
# e2fsprogs is left out: nothing in the guest uses ext4.
# Linux machines: lxc runs them, dnsmasq is their DHCP server on the machine bridge, and GNU tar
# unpacks their images with xattrs (file capabilities) intact.
PACKAGES="alpine-baselayout alpine-release busybox busybox-binsh musl-utils ca-certificates
  iptables iproute2 btrfs-progs xz pigz procps-ng mount umount shadow lxc dnsmasq tar"

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=$(cd "$HERE/../.." && pwd)/guest/out
DOCKER=(docker)
if [[ -n ${KATAMARAN_BUILD_CONTEXT:-} ]]; then
    DOCKER+=(--context "$KATAMARAN_BUILD_CONTEXT")
fi

run() {
    local name=$1; shift
    "${DOCKER[@]}" run --rm -i --name "katamaran-kbuild-$name-$$" --platform linux/arm64 \
        -v "$OUT:/out" \
        -e ALPINE_BRANCH="$ALPINE_BRANCH" -e PACKAGES="$PACKAGES" \
        -e DOCKER_VERSION="$DOCKER_VERSION" -e DOCKER_URL="$DOCKER_URL" -e DOCKER_SHA256="$DOCKER_SHA256" \
        -e DOCKER_BINS="$DOCKER_BINS" -e QEMU_VERSION="$QEMU_VERSION" -e QEMU_SHA256="$QEMU_SHA256" \
        -e SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" \
        -e EROFS_UUID="$EROFS_UUID" -e EROFS_LABEL="$EROFS_LABEL" \
        "$@"
}

build() {
    run rootfs "$ALPINE_IMAGE" sh -s <<'EOF'
set -eu
apk add -q --no-progress erofs-utils

R=/work/rootfs
mkdir -p "$R/etc/apk"
cp -a /etc/apk/keys "$R/etc/apk/"
printf 'https://dl-cdn.alpinelinux.org/alpine/%s/%s\n' "$ALPINE_BRANCH" main "$ALPINE_BRANCH" community \
    > "$R/etc/apk/repositories"
# shellcheck disable=SC2086
apk add -q --no-progress --root "$R" --initdb --no-cache $PACKAGES "qemu-x86_64=$QEMU_VERSION"
echo "$QEMU_SHA256  $R/usr/bin/qemu-x86_64" | sha256sum -c -s || { echo "qemu-x86_64 sha256 mismatch" >&2; exit 1; }

mkdir -p /out/cache
tgz=/out/cache/docker-$DOCKER_VERSION.tgz
if ! echo "$DOCKER_SHA256  $tgz" | sha256sum -c -s 2>/dev/null; then
    wget -q -O "$tgz.part" "$DOCKER_URL"
    mv "$tgz.part" "$tgz"
fi
echo "$DOCKER_SHA256  $tgz" | sha256sum -c -s || { echo "docker tarball sha256 mismatch" >&2; exit 1; }
mkdir -p /work/docker
tar -xzf "$tgz" -C /work
for b in $DOCKER_BINS; do install -m 0755 "/work/docker/$b" "$R/usr/bin/$b"; done

# Storage driver: "overlayfs". The data disk is btrfs, mounted at /var/lib/docker and
# /var/lib/containerd (subvolumes). With the containerd image store (Docker 29 default, pinned on
# below) "storage-driver" names a containerd snapshotter and image layers live in containerd's root
# (/var/lib/containerd), not in data-root. Checked with `build.sh smoke` on Docker 29.10.0:
#   - "overlayfs": works on the btrfs subvolume (the overlay2-on-btrfs caveat is about the legacy
#     graph driver).
#   - "overlay2": rejected, `configured driver "overlay2" not available` (graph-driver name, no such
#     snapshotter).
#   - "btrfs": rejected, the static containerd is built without the btrfs snapshotter (only
#     overlayfs, native and erofs load). The legacy btrfs graph driver is deprecated.
mkdir -p "$R/etc/docker"
cat > "$R/etc/docker/daemon.json" <<'JSON'
{
  "data-root": "/var/lib/docker",
  "hosts": ["unix:///run/docker.sock"],
  "containerd": "/run/containerd/containerd.sock",
  "features": {
    "containerd-snapshotter": true
  },
  "storage-driver": "overlayfs",
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
JSON

# containerd is started by kat-init (dockerd connects to it instead of spawning its own).
mkdir -p "$R/etc/containerd"
cat > "$R/etc/containerd/config.toml" <<'TOML'
version = 4
root = "/var/lib/containerd"
state = "/run/containerd"
disabled_plugins = ["io.containerd.grpc.v1.cri", "io.containerd.cri.v1.images", "io.containerd.cri.v1.runtime"]

[grpc]
  address = "/run/containerd/containerd.sock"
TOML

echo katamaran > "$R/etc/hostname"
cat > "$R/etc/hosts" <<'HOSTS'
127.0.0.1	localhost katamaran
::1	localhost katamaran
HOSTS
echo '# placeholder: kat-init writes this file at boot' > "$R/etc/resolv.conf"

for d in proc sys dev run tmp var/lib/docker var/lib/containerd var/lib/katamaran mnt/initramfs; do
    mkdir -p "$R/$d"
done
chmod 1777 "$R/tmp"
# apk.log carries the install wall-clock time; dropping it keeps the image byte-reproducible.
rm -rf "$R/var/cache/apk"/* "$R/lib/apk/db/lock" "$R/var/log/apk.log"

apk --root "$R" info -v 2>/dev/null | sort > /out/rootfs.packages.txt
printf 'docker-static-%s (%s)\n' "$DOCKER_VERSION" "$DOCKER_BINS" >> /out/rootfs.packages.txt

mkfs.erofs --quiet -zlz4hc,level=12 -Eztailpacking -T"$SOURCE_DATE_EPOCH" -U"$EROFS_UUID" -L"$EROFS_LABEL" \
    /out/rootfs.erofs.tmp "$R"
mv /out/rootfs.erofs.tmp /out/rootfs.erofs
EOF
}

verify() {
    run rootfs-verify "$ALPINE_IMAGE" sh -s <<'EOF'
set -eu
apk add -q --no-progress erofs-utils
fsck.erofs /out/rootfs.erofs >/dev/null
X=/work/x; mkdir -p /work
fsck.erofs --preserve --extract="$X" /out/rootfs.erofs >/dev/null
for d in proc sys dev run tmp var/lib/docker var/lib/containerd var/lib/katamaran mnt/initramfs; do
    test -d "$X/$d" || { echo "missing mountpoint /$d" >&2; exit 1; }
done
for f in etc/hostname etc/hosts etc/resolv.conf etc/docker/daemon.json etc/containerd/config.toml; do
    test -f "$X/$f" || { echo "missing /$f" >&2; exit 1; }
done
# no /proc in the chroot: drop the getCPUVariant noise, keep stdout
c() { chroot "$X" "$@" 2>/dev/null; }
c dockerd --version
c containerd --version
c runc --version | head -1
c iptables --version
c ip6tables --version
c qemu-x86_64 --version | head -1
c lxc-start --version
c dnsmasq --version | head -1
c tar --version | head -1
c dockerd --validate --config-file /etc/docker/daemon.json
echo "daemon.json OK"
c containerd --config /etc/containerd/config.toml config dump >/dev/null && echo "containerd config OK"
grep -v '^disabled_plugins' "$X/etc/containerd/config.toml" > "$X/tmp/containerd-cri.toml"
c containerd --config /tmp/containerd-cri.toml config dump >/dev/null && echo "containerd CRI config OK"
rm -f "$X/tmp/containerd-cri.toml"
c sh -c 'echo "busybox sh OK: $(busybox | head -1)"'
EOF
}

smoke() {
    local driver=${1:-}
    [[ -f $OUT/data.img ]] || { echo "guest/out/data.img missing: run guest/disk/build.sh" >&2; exit 1; }
    run smoke --privileged -e DRIVER="$driver" "$ALPINE_IMAGE" sh -s <<'EOF'
set -eu
apk add -q --no-progress util-linux btrfs-progs coreutils jq
R=/m/root
cleanup() {
    [ -f $R/run/docker.pid ] && kill "$(cat $R/run/docker.pid)" 2>/dev/null || true
    sleep 2
    pkill containerd 2>/dev/null || true
    sleep 1
    umount -R $R 2>/dev/null || umount -lR $R 2>/dev/null || true
    umount /m/ro /m/rw 2>/dev/null || true
    for l in $(losetup -nO NAME -j /out/rootfs.erofs) $(losetup -nO NAME -j /work/data.img); do losetup -d "$l"; done
}
trap cleanup EXIT

mkdir -p /m/ro /m/rw $R /work
cp --sparse=always /out/data.img /work/data.img
ro=$(losetup -f --show -r /out/rootfs.erofs)
mount -t erofs -o ro "$ro" /m/ro
mount -t tmpfs tmpfs /m/rw
mkdir /m/rw/upper /m/rw/work
mount -t overlay overlay -o lowerdir=/m/ro,upperdir=/m/rw/upper,workdir=/m/rw/work $R
data=$(losetup -f --show /work/data.img)
for sv in docker containerd katamaran; do mount -o subvol=$sv,discard=async "$data" $R/var/lib/$sv; done
mount -t proc proc $R/proc
mount -t sysfs sysfs $R/sys
mount -t cgroup2 cgroup2 $R/sys/fs/cgroup
mount --rbind /dev $R/dev
mount -t tmpfs tmpfs $R/run
mount -t tmpfs tmpfs $R/tmp
echo "rootfs: $(findmnt -no FSTYPE /m/ro) under overlay; data: $(findmnt -no FSTYPE,OPTIONS $R/var/lib/docker)"

cp /etc/resolv.conf $R/etc/resolv.conf
tar -xzf /out/cache/docker-*.tgz -C /work docker/docker
install -m 0755 /work/docker/docker $R/usr/local/bin/docker
if [ -n "$DRIVER" ]; then
    jq --arg d "$DRIVER" '."storage-driver" = $d' $R/etc/docker/daemon.json > /work/d.json
    cp /work/d.json $R/etc/docker/daemon.json
fi

t0=$(date +%s%N)
chroot $R sh -c 'containerd >/run/containerd.log 2>&1 &'
for _ in $(seq 100); do [ -S $R/run/containerd/containerd.sock ] && break; sleep 0.1; done
chroot $R sh -c 'dockerd --pidfile /run/docker.pid >/run/dockerd.log 2>&1 &'
for _ in $(seq 300); do chroot $R docker info >/dev/null 2>&1 && break; sleep 0.1; done
t1=$(date +%s%N)
chroot $R docker info >/dev/null 2>&1 || { tail -30 $R/run/dockerd.log; exit 1; }
echo "containerd+dockerd ready in $(( (t1 - t0) / 1000000 )) ms"
chroot $R docker info --format 'driver={{.Driver}} {{json .DriverStatus}} version={{.ServerVersion}}'
chroot $R docker run --rm alpine:3.24 sh -c 'echo container OK: $(uname -m) $(cat /etc/alpine-release)'
echo "image store: $(du -sh $R/var/lib/containerd | cut -f1) in /var/lib/containerd, $(du -sh $R/var/lib/docker | cut -f1) in /var/lib/docker"
EOF
}

file_size() {
    if [[ "$(uname -s)" == Darwin ]]; then
        stat -f %z "$1"
    else
        stat -c %s "$1"
    fi
}

file_sha256() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -c1-16
    else
        sha256sum "$1" | cut -c1-16
    fi
}

cmd=${1:-build}
case $cmd in
    build)
        mkdir -p "$OUT"
        start=$SECONDS
        build
        echo "built guest/out/rootfs.erofs in $((SECONDS - start)) s:" \
             "$(file_size "$OUT/rootfs.erofs") bytes ($(du -h "$OUT/rootfs.erofs" | cut -f1)), sha256 $(file_sha256 "$OUT/rootfs.erofs")"
        verify
        ;;
    verify) verify ;;
    smoke) shift; smoke "$@" ;;
    *) echo "usage: $0 [build|verify|smoke [storage-driver]]" >&2; exit 2 ;;
esac
