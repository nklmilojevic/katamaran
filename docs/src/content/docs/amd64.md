---
title: Run x86-64 images
description: Run linux/amd64 Docker images on Apple silicon using Rosetta for Linux or QEMU user-mode emulation.
---

katamaran runs `linux/amd64` containers and binaries on Apple silicon through Linux `binfmt_misc` registration at boot.

## Choose an emulation mode

The `amd64` setting controls which emulator `kat-init` registers:

| Value | Behavior |
|---|---|
| `auto` | Default. Uses Apple Rosetta for Linux when installed on macOS, and falls back to `qemu-x86_64` otherwise. |
| `rosetta` | Requires Apple Rosetta for Linux. |
| `qemu` | Uses `qemu-x86_64` bundled inside the guest system image. |
| `off` | Disables `linux/amd64` binary execution. |

Install Rosetta on macOS if it is not installed yet:

```sh
softwareupdate --install-rosetta
```

Switch modes with `kat config set` and restart the VM:

```sh
kat config set amd64=rosetta
kat restart
```

## Run an x86-64 container

Pass `--platform linux/amd64` to `docker run` or `docker build`:

```sh
docker run --rm --platform linux/amd64 alpine:3.24 uname -m
```

The command prints `x86_64`.

## How Rosetta works inside katamaran

When Rosetta is enabled, `katamaran-vm` shares Apple's `RosettaLinux` runtime directory read-only into the guest under the virtio-fs tag `rosetta` with `CachePolicy::Always` and 24-hour metadata timeouts.

Because translated x86-64 code expects total store ordering (TSO), the guest kernel supports `prctl(PR_SET_MEM_MODEL, PR_SET_MEM_MODEL_TSO)` per task. On context switch, the kernel invokes a vendor hypercall that toggles `ACTLR_EL1.EnTSO` on that vCPU only while a Rosetta task is running. Native arm64 tasks never run with TSO enabled and never trap for it.
