#!/usr/bin/env bash
# k3s-nft-check.sh [k3s tag] [docker args...]: check that a k3s release starts kube-proxy in
# nftables mode using the guest rootfs's /usr/sbin/nft rather than a bundled nft.
#
# k3s v1.31.0-v1.31.5 bundle k3s-root without `nft`, so kube-proxy's nftables mode (enabled
# by kat-init's `--kube-proxy-arg=proxy-mode=nftables`) depends on the guest rootfs's nftables
# package being on PATH. This runs the release binary in the guest's pinned Alpine image with
# only the rootfs networking packages installed and kat-init's k3s server flags, then checks:
#   - whether the release bundles nft (if not, the rootfs binary is what kube-proxy used),
#   - kube-proxy created its `ip`/`ip6 kube-proxy` nftables tables,
#   - the node reports Ready.
# Examples:
#   scripts/k3s-nft-check.sh                       # oldest supported release, v1.31.0+k3s1
#   scripts/k3s-nft-check.sh v1.37.2+k3s1
#   scripts/k3s-nft-check.sh v1.31.0+k3s1 -H unix://$HOME/.katamaran-cache/run/docker.sock
set -euo pipefail
tag=${1:-v1.31.0+k3s1}
shift || true
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
image=$(sed -n 's/^ALPINE_IMAGE=alpine:\${ALPINE_VERSION}@//p' "$here/../guest/rootfs/build.sh")
version=$(sed -n 's/^ALPINE_VERSION=//p' "$here/../guest/rootfs/build.sh")
image="alpine:${version}@${image}"
work=$(mktemp -d)
# Random suffix so the name cannot collide with an existing container or volume;
# cleanup only removes resources this run actually created.
name="kat-k3s-nft-check-$$-$(od -An -N4 -tx4 /dev/urandom | tr -d ' ')"
created_volume=
created_container=
cleanup() {
    if [ -n "$created_container" ]; then
        docker "$@" rm -f "$name" >/dev/null 2>&1 || true
    fi
    if [ -n "$created_volume" ]; then
        docker "$@" volume rm "$name" >/dev/null 2>&1 || true
    fi
    rm -rf "$work"
}
trap 'cleanup "$@"' EXIT

url_tag=${tag//+/%2B}
curl -fsSL -o "$work/k3s-arm64" "https://github.com/k3s-io/k3s/releases/download/$url_tag/k3s-arm64"
curl -fsSL "https://github.com/k3s-io/k3s/releases/download/$url_tag/sha256sum-arm64.txt" |
    grep ' k3s-arm64$' | (cd "$work" && shasum -a 256 -c -)
chmod +x "$work/k3s-arm64"

if docker "$@" volume inspect "$name" >/dev/null 2>&1 ||
    docker "$@" container inspect "$name" >/dev/null 2>&1; then
    echo "refusing to reuse existing docker resource $name" >&2
    exit 1
fi
docker "$@" volume create "$name" >/dev/null
created_volume=1
# Keep the server flags in sync with `K8s::start_inner` in crates/kat-init/src/linux/k8s.rs.
# The binary is copied in rather than bind-mounted so this works with engines that
# cannot share the host's temp directory.
docker "$@" create --name "$name" --privileged --cgroupns=host \
    -v "$name":/var/lib/rancher/k3s "$image" sh -c '
    apk add --no-cache nftables iptables >/dev/null &&
    exec env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin /opt/k3s server \
        --node-name=katamaran --flannel-backend=host-gw \
        --cluster-cidr=10.42.0.0/16 --service-cidr=10.43.0.0/16 --cluster-dns=10.43.0.10 \
        --https-listen-port=6443 --kubelet-arg=fail-swap-on=false \
        --kube-proxy-arg=proxy-mode=nftables' >/dev/null
created_container=1
docker "$@" cp "$work/k3s-arm64" "$name":/opt/k3s
docker "$@" start "$name" >/dev/null

for _ in $(seq 1 60); do
    if [ "$(docker "$@" inspect -f '{{.State.Running}}' "$name")" != true ]; then
        break
    fi
    if docker "$@" exec "$name" sh -c \
        '/opt/k3s kubectl get node katamaran 2>/dev/null | grep -qw Ready &&
         nft list table ip kube-proxy >/dev/null 2>&1' 2>/dev/null; then
        docker "$@" exec "$name" sh -c '
            set -e
            bin=$(ls -d /var/lib/rancher/k3s/data/*/bin | head -1)
            nft list tables | grep kube-proxy
            /opt/k3s kubectl get node katamaran --no-headers'
        if docker "$@" exec "$name" sh -c \
            'bin=$(ls -d /var/lib/rancher/k3s/data/*/bin | head -1); [ -e "$bin/nft" ] || [ -e "$bin/aux/nft" ]'; then
            echo "OK: $tag runs kube-proxy in nftables mode (with its own bundled nft)"
        else
            echo "OK: $tag bundles no nft and runs kube-proxy in nftables mode with the guest rootfs $(docker "$@" exec "$name" sh -c 'command -v nft')"
        fi
        exit 0
    fi
    sleep 5
done
docker "$@" logs "$name" 2>&1 | tail -40
echo "FAIL: $tag did not reach a Ready node with kube-proxy nftables tables" >&2
exit 1
