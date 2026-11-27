---
title: Build from source
description: Build the Rust binaries, gvproxy, Linux kernel, Alpine rootfs, btrfs data disk, macOS app bundle, and DMG installer.
---

Every build task runs through `cargo xtask`. Host binaries build directly on macOS, while guest images (kernel, rootfs, and data disk) build reproducibly inside Linux Docker containers.

## Prerequisites

- macOS 15 or later on Apple silicon
- Rust via `rustup` (`rust-toolchain.toml` pins the Rust toolchain and the `aarch64-unknown-linux-musl` target)
- Go (required for `gvproxy` and `libgvproxy.dylib` with cgo)
- Xcode command-line tools (for Swift, `codesign`, and cgo)
- A running Docker engine (for building the Linux kernel, rootfs, and data disk)

## Build host binaries and initramfs

Compile `kat-init` for `aarch64-unknown-linux-musl`, pack `guest/out/initramfs.cpio`, build `gvproxy` and `libgvproxy.dylib` from `vendor/patches/gvproxy/`, compile `katamaran` and `katamaran-vm` in release mode, and ad-hoc sign `katamaran-vm` with the Hypervisor entitlement:

```sh
cargo xtask build
```

To build only `gvproxy` and `libgvproxy.dylib`:

```sh
cargo xtask gvproxy
```

To build only `guest/out/initramfs.cpio`:

```sh
cargo xtask initramfs
```

## Build guest images

Build the Linux kernel (`guest/out/Image`), the read-only Alpine EROFS rootfs (`guest/out/rootfs.erofs`), and the sparse btrfs data disk template (`guest/out/data.img`):

```sh
cargo xtask kernel
cargo xtask rootfs
cargo xtask disk
```

Each task uses the active Docker context by default. Set `KATAMARAN_BUILD_CONTEXT` to target a different Docker context:

```sh
KATAMARAN_BUILD_CONTEXT=desktop-linux cargo xtask kernel
```

`cargo xtask kernel` validates every entry in `guest/kernel/katamaran.config`, applies `guest/kernel/patches/*.patch`, builds `Image`, and gates the build on Moby's `check-config.sh`.

## Build the macOS app and DMG

Build `target/Katamaran.app` (Swift AppKit binary, generated `.icns` icon, `katamaran`, `katamaran-vm`, `libgvproxy.dylib`, and guest images, ad-hoc signed):

```sh
cargo xtask app
```

Build a distributable disk image at `target/dist/Katamaran-<version>.dmg`:

```sh
cargo xtask dmg
```

Run a headless smoke check of the built app against a test daemon:

```sh
cargo xtask app-smoke
```

## Run an isolated development VM

To boot your local build without touching `~/.katamaran` or `~/Katamaran`, set `KATAMARAN_HOME` and `--mount`:

```sh
KATAMARAN_HOME=~/.katamaran-dev ./target/release/katamaran run --cpus 4 --memory-mib 4096 --mount ~/Katamaran-dev
```

## Format and test

Format only the katamaran workspace crates (do not run `cargo fmt --all`, which would reformat `vendor/libkrun`):

```sh
cargo xtask fmt
```

Run fast host unit tests:

```sh
cargo test --lib -p kat-proto -p kat-vmm -p kat-daemon -p xtask
```

The port forwarding tests send payloads through a real `gvproxy` and a `lo0` alias on regular and privileged ports. Without an alias, or without `target/release/gvproxy`, they only test `127.0.0.1` or skip. CI runs them in strict mode, where a missing alias or `gvproxy` fails the run:

```sh
cargo xtask gvproxy
sudo ifconfig lo0 alias 172.16.254.200
KATAMARAN_TEST_LO0_ALIAS=172.16.254.200 KATAMARAN_REQUIRE_LO0_ALIAS=1 \
  KATAMARAN_REQUIRE_GVPROXY=1 cargo test --lib -p kat-daemon
sudo ifconfig lo0 -alias 172.16.254.200
```

Run the full VM integration test suite (boots real VMs; requires `docker` and `kubectl` on `PATH`, plus internet access). `cargo xtask kernel` records the inputs it built from in `guest/out/Image.stamp`; `test-vm` refuses to start when that stamp is missing or no longer matches the kernel patches and config (for example an `Image` built before the quick ACK patch), so run `cargo xtask kernel` first in that case:

```sh
cargo xtask test-vm
```

## Repository layout

| Path | Description |
|---|---|
| `crates/kat-daemon` | `katamaran` CLI and background daemon |
| `crates/kat-vmm` | `katamaran-vm` process wrapping libkrun, in-process gvproxy, and FSEvents cache invalidation |
| `crates/kat-init` | Guest PID 1 supervising containerd, dockerd, NFS, LXC machines, and memory reclaim |
| `crates/kat-nfs` | In-guest NFSv3 server exporting Linux files to macOS |
| `crates/kat-proto` | Wire protocol shared between host and guest |
| `crates/kat-term` | Terminal library wrapping `libghostty-vt` for the macOS app |
| `app` | Swift and AppKit macOS app and icon generator |
| `xtask` | Workspace build commands (`cargo xtask ...`) |
| `guest` | Build scripts, kernel config, and kernel patches |
| `vendor/libkrun` | Patched libkrun tree (patch series in `vendor/patches`) |
| `vendor/patches/gvproxy` | Patch series applied to `gvisor-tap-vsock` |
| `docs` | Astro and Starlight documentation website |
| `bench` | Benchmark tool and saved comparison results |
| `scripts` | Standalone filesystem and network probe scripts |
