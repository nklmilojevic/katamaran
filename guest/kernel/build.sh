#!/usr/bin/env bash
# Build the katamaran guest kernel: an uncompressed arm64 raw `Image` for libkrun
# (Payload::load_external(kernel, KernelFormat::Raw, Some(initramfs), cmdline)).
#
# Usage (from anywhere, on the macOS host):
#   guest/kernel/build.sh            build guest/out/Image and guest/out/kernel.config,
#                                    then run moby's check-config.sh -> guest/out/check-config.txt
#   guest/kernel/build.sh clean      drop the object tree (keeps the source and ccache volumes)
#   guest/kernel/build.sh shell CMD  run CMD in the build container, cwd = prepared source tree
#                                    (guest/out is read-only there; edits to the cached tree are
#                                    kept for later shells, but the next build re-extracts and
#                                    re-patches a pristine tree)
#   guest/kernel/build.sh stamp      print the input stamp (no Docker); a build records it in
#                                    guest/out/Image.stamp so `cargo xtask test-vm` can reject
#                                    an Image built from older patches or config
#
# Method: allnoconfig + KCONFIG_ALLCONFIG=katamaran.config, then olddefconfig, then every
# CONFIG_*=<value> line of the fragment must appear verbatim in .config (else `MISSING: ...`) and
# every `# CONFIG_* is not set` line must not be enabled (else `UNWANTED: ...`). Patches in
# patches/*.patch are applied in name order to a fresh tree whenever the patch set changes.
#
# Everything runs in a Linux container on a Docker engine: the context named by
# KATAMARAN_BUILD_CONTEXT, or the current Docker context if it is unset (no context is ever
# changed). Containers are named `katamaran-kbuild-*` and run with --rm.
# Caches live in named volumes: katamaran-kbuild-src (tarball, patched tree, objects) and
# katamaran-kbuild-ccache. The toolchain image is created once from a pinned Debian base.
# Nothing is installed on the macOS host.
set -euo pipefail

KVER=6.18.64
# From https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc (signed by kernel.org).
KSHA256=020d45510c539972e6f14c5c71566878d3b7ef56fb082b3e09134f2ebaade562
KURL=https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-${KVER}.tar.xz
# Fixed build identity so the Image is reproducible (date = KVER release date).
KBUILD_TIMESTAMP="2026-11-17 00:00:00 UTC"

# moby contrib/check-config.sh, pinned.
MOBY_COMMIT=d217a0d97eb468d4d09260889bf78653ad953945
CHECK_CONFIG_SHA256=07dd6666c6f1edb3c8f6e534aeb0926683967e4dca358c4e7df53582713f7f4d

BASE_IMAGE=debian:trixie@sha256:9c6e381421d0f48d1b8133e573f33c185ee16cbf505eda9c4a8a4791b25ea8a6
PACKAGES="bc bison build-essential ca-certificates ccache cpio curl flex libelf-dev libssl-dev procps xz-utils"

DOCKER=(docker)
if [[ -n ${KATAMARAN_BUILD_CONTEXT:-} ]]; then
	DOCKER+=(--context "$KATAMARAN_BUILD_CONTEXT")
fi
SRC_VOL=katamaran-kbuild-src
CCACHE_VOL=katamaran-kbuild-ccache
sha256() {
	if command -v shasum >/dev/null 2>&1; then
		shasum -a 256
	else
		sha256sum
	fi
}
TOOL_TAG=$(printf '%s\n' "$BASE_IMAGE" "$PACKAGES" | sha256 | cut -c1-12)
TOOL_IMAGE=katamaran-kbuild-toolchain:$TOOL_TAG

HERE=$(cd "$(dirname "$0")" && pwd)

# Identity of everything the Image is built from: kernel version, patch set (in name order),
# config fragment, and this script (pinned toolchain, tarball hash, build steps). Written next to
# the Image as Image.stamp once the whole build, including check-config, has passed.
# Fails (no output) if any input is missing or unreadable, so a stamp never silently covers
# fewer files than the build applies.
image_stamp() {
	(
		export LC_ALL=C
		shopt -s nullglob
		local inputs=("$HERE"/patches/*.patch "$HERE/katamaran.config" "$HERE/build.sh") f data
		for f in "${inputs[@]}"; do
			if [[ ! -f $f || ! -r $f ]]; then
				echo "kernel build input missing or unreadable: $f" >&2
				exit 1
			fi
		done
		data=$({ echo "$KVER"; cat -- "${inputs[@]}"; } | sha256 | cut -c1-64) || exit 1
		[[ ${#data} == 64 ]] || { echo "could not hash kernel build inputs" >&2; exit 1; }
		echo "$data"
	)
}
if [[ ${1:-} == stamp ]]; then
	image_stamp
	exit
fi
IMAGE_STAMP=$(image_stamp)

OUT=$(cd "$HERE/.." && pwd)/out
mkdir -p "$OUT"

if ! "${DOCKER[@]}" info >/dev/null 2>&1; then
	echo "Docker is not reachable${KATAMARAN_BUILD_CONTEXT:+ (context $KATAMARAN_BUILD_CONTEXT)}; start a Docker engine or set KATAMARAN_BUILD_CONTEXT" >&2
	exit 1
fi

ensure_toolchain() {
	"${DOCKER[@]}" image inspect "$TOOL_IMAGE" >/dev/null 2>&1 && return
	local name=katamaran-kbuild-toolchain-$$
	echo ">> creating $TOOL_IMAGE"
	"${DOCKER[@]}" run --name "$name" "$BASE_IMAGE" sh -euc "
		export DEBIAN_FRONTEND=noninteractive
		apt-get update -qq
		apt-get install -y -qq --no-install-recommends $PACKAGES >/dev/null
		rm -rf /var/lib/apt/lists/*"
	"${DOCKER[@]}" commit "$name" "$TOOL_IMAGE" >/dev/null
	"${DOCKER[@]}" rm "$name" >/dev/null
}

# Runs inside the container. Prepares /work/src/linux (cached), then does $MODE.
INNER=$(cat <<'EOF'
set -euo pipefail
cd /work
mkdir -p cache src build
tarball=cache/linux-$KVER.tar.xz
if [ ! -f "$tarball" ] || ! echo "$KSHA256  $tarball" | sha256sum -c --status; then
	echo ">> downloading linux-$KVER"
	curl -fsSL --retry 3 -o "$tarball.part" "$KURL"
	echo "$KSHA256  $tarball.part" | sha256sum -c --quiet
	mv "$tarball.part" "$tarball"
fi

# A fresh tree (and object dir) whenever the version or the patch set changes, and before any
# build after a shell session may have edited the cached tree (src/shell-used), so an installed
# Image is always built from the checked-in patches exactly.
stamp=$( { echo "$KVER"; cat /in/patches/*.patch 2>/dev/null || true; } | sha256sum | cut -c1-64)
tree=src/linux
obj=build/linux
if [ "$(cat src/stamp 2>/dev/null)" != "$stamp" ] ||
	{ [ "$MODE" = build ] && [ -e src/shell-used ]; }; then
	echo ">> extracting linux-$KVER"
	rm -rf "$tree" "$obj" src/stamp src/shell-used
	mkdir -p "$tree"
	tar -xJf "$tarball" -C "$tree" --strip-components=1
	for p in /in/patches/*.patch; do
		[ -e "$p" ] || continue
		echo ">> applying $(basename "$p")"
		patch -d "$tree" -p1 --forward --quiet < "$p"
	done
	echo "$stamp" > src/stamp
fi
mkdir -p "$obj"

export CCACHE_DIR=/ccache CCACHE_MAXSIZE=5G CCACHE_COMPILERCHECK=content \
	CCACHE_BASEDIR=/work CCACHE_NOHASHDIR=1
export KBUILD_BUILD_TIMESTAMP KBUILD_BUILD_USER=katamaran KBUILD_BUILD_HOST=katamaran \
	KBUILD_BUILD_VERSION=1
MAKE=(make -C "/work/$tree" O="/work/$obj" ARCH=arm64 CC="ccache gcc" -j"$(nproc)")

case "$MODE" in
shell)
	# Edits made here persist for later shells, but the next build starts from a pristine tree.
	touch src/shell-used
	cd "/work/$tree"
	exec bash -c "$SHELL_CMD"
	;;
clean)
	rm -rf "/work/$obj"
	echo ">> object tree removed"
	exit 0
	;;
esac

echo ">> configuring"
"${MAKE[@]}" -s KCONFIG_ALLCONFIG=/in/katamaran.config allnoconfig
"${MAKE[@]}" -s olddefconfig
missing=0
while IFS= read -r line; do
	case "$line" in
	CONFIG_*=*)
		if ! grep -qxF "$line" "$obj/.config"; then
			echo "MISSING: $line"
			missing=1
		fi
		;;
	"# CONFIG_"*" is not set")
		sym=${line#\# }
		sym=${sym%% *}
		if grep -q "^$sym=" "$obj/.config"; then
			echo "UNWANTED: $(grep "^$sym=" "$obj/.config")"
			missing=1
		fi
		;;
	esac
done < /in/katamaran.config
true
[ "$missing" = 0 ] || { echo "config check failed" >&2; exit 1; }
echo ">> config check passed"

# Drop the stamp before anything can replace the Image, so an interrupted build never leaves a
# new Image paired with the previous inputs' stamp.
rm -f /out/Image.stamp
echo ">> building Image ($(nproc) jobs)"
ccache -z >/dev/null
start=$(date +%s)
"${MAKE[@]}" -s Image
echo ">> built in $(( $(date +%s) - start ))s; ccache: $(ccache -s | grep -iE 'hits:' | head -1 | tr -s ' ')"
install -m 0644 "$obj/arch/arm64/boot/Image" /out/Image
install -m 0644 "$obj/.config" /out/kernel.config
echo ">> $(cat "$obj/include/config/kernel.release"): Image $(stat -c %s /out/Image) bytes"

checker=cache/check-config-$MOBY_COMMIT.sh
if [ ! -f "$checker" ] || ! echo "$CHECK_CONFIG_SHA256  $checker" | sha256sum -c --status; then
	curl -fsSL --retry 3 -o "$checker.part" \
		"https://raw.githubusercontent.com/moby/moby/$MOBY_COMMIT/contrib/check-config.sh"
	echo "$CHECK_CONFIG_SHA256  $checker.part" | sha256sum -c --quiet
	mv "$checker.part" "$checker"
fi
# The script also probes the running system (cgroup mounts, sysctls, keys limits); those lines
# describe the build container, not this kernel. The "bridge" network driver section consists only
# of the forwarding sysctls, which dockerd itself turns on at startup; the container is started with
# the same values (see the docker run below), so that section reports what the guest will see once
# dockerd runs. The script's exit code is not used; the gate is the lines checked below.
NO_COLOR=1 sh "$checker" /out/kernel.config > /out/check-config.txt 2>&1 || true
# Gate: every CONFIG_* line in "Generally Necessary", plus the overlay storage driver.
bad=$(awk '/^Generally Necessary:/{g=1} /^Optional Features:/{g=0} g && /CONFIG_/ && !/: enabled/' \
	/out/check-config.txt)
bad+=$(awk '/"overlay":/{o=1} /"zfs":/{o=0} o && /CONFIG_OVERLAY_FS/ && !/: enabled/' /out/check-config.txt)
bad+=$(awk '/"bridge":/{b=1} /"overlay":/{b=0} b && !/"bridge":/ && !/sysctl .*: enabled/' /out/check-config.txt)
if [ "$(grep -c 'sysctl .*: enabled' /out/check-config.txt)" -ne 3 ]; then
	bad+=" bridge sysctl checks did not run"
fi
if [ -n "$bad" ]; then
	echo "$bad"
	echo "check-config: required items missing (see guest/out/check-config.txt)" >&2
	exit 1
fi
echo ">> check-config: Generally Necessary, overlay and bridge passed (guest/out/check-config.txt)"
printf '%s\n' "$IMAGE_STAMP" > /out/Image.stamp
EOF
)

MODE=${1:-build}
SHELL_CMD=""
case "$MODE" in
build | clean) ;;
shell)
	shift
	SHELL_CMD=${*:-bash}
	;;
*)
	echo "usage: $0 [build|clean|shell CMD|stamp]" >&2
	exit 2
	;;
esac

ensure_toolchain
"${DOCKER[@]}" volume create "$SRC_VOL" >/dev/null
"${DOCKER[@]}" volume create "$CCACHE_VOL" >/dev/null

tty=()
# A shell can inspect or edit the cached tree but never replace guest/out/Image (or its stamp):
# only a full build installs an Image.
out_mode=
[ "$MODE" = shell ] && out_mode=:ro
[ "$MODE" = shell ] && [ -t 0 ] && [ -t 1 ] && tty=(-it)
"${DOCKER[@]}" run --rm "${tty[@]}" --name "katamaran-kbuild-$$" \
	-v "$SRC_VOL":/work -v "$CCACHE_VOL":/ccache \
	-v "$HERE":/in:ro -v "$OUT":/out"$out_mode" \
	--sysctl net.ipv4.ip_forward=1 --sysctl net.ipv6.conf.all.forwarding=1 \
	--sysctl net.ipv6.conf.default.forwarding=1 \
	-e MODE="$MODE" -e SHELL_CMD="$SHELL_CMD" \
	-e KVER="$KVER" -e KSHA256="$KSHA256" -e KURL="$KURL" \
	-e KBUILD_BUILD_TIMESTAMP="$KBUILD_TIMESTAMP" \
	-e MOBY_COMMIT="$MOBY_COMMIT" -e CHECK_CONFIG_SHA256="$CHECK_CONFIG_SHA256" \
	-e IMAGE_STAMP="$IMAGE_STAMP" \
	"$TOOL_IMAGE" bash -c "$INNER"
