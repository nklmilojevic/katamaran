---
title: Command-line reference
description: Complete reference for every katamaran and kat subcommand, argument, and option.
---

`kat` is a symlink to `katamaran` created by `kat setup`. Every command below works with either name.

## Lifecycle commands

### `kat start`

Start the VM (starting the daemon first if needed) and wait until Docker answers.

```sh
kat start [--timeout <SECONDS>]
```

| Option | Default | Description |
|---|---|---|
| `--timeout <SECONDS>` | `180` | Maximum seconds to wait for Docker to become ready |

### `kat stop`

Stop the VM cleanly. The background daemon keeps running.

```sh
kat stop
```

### `kat restart`

Stop the VM and start it again so saved configuration changes take effect.

```sh
kat restart [--timeout <SECONDS>]
```

### `kat status`

Print the VM state, CPU and memory limits, Docker status, NFS mount path, and forwarded ports.

```sh
kat status [--json]
```

| Option | Description |
|---|---|
| `--json` | Emit the full status payload as JSON |

### `kat logs`

Print logs from the daemon, the guest console, the VM monitor, or gvproxy.

```sh
kat logs [-f] [-n <LINES>] [--source <SOURCE>] [--console]
```

| Option | Default | Description |
|---|---|---|
| `-f`, `--follow` | off | Stream new log lines as they arrive |
| `-n`, `--lines <LINES>` | `100` | Number of lines to print from the end |
| `--source <SOURCE>` | `daemon` | Log stream (`daemon`, `console`, `vm`, or `gvproxy`) |
| `--console` | off | Shorthand for `--source console` |

### `kat version`

Print the CLI version and the running daemon version.

```sh
kat version
```

## Configuration commands

### `kat config get`

Print every setting in `~/.katamaran/config.toml`, or print one key.

```sh
kat config get [KEY]
```

### `kat config set`

Update one or more settings and validate them against the Mac's hardware limits. Changes apply on the next VM start (except `launch_at_login`, which updates the LaunchAgent immediately).

```sh
kat config set cpus 6
kat config set cpus=6 memory=8G mount=off
```

See [Configuration reference](/configuration/) for every valid key and value format.

## Setup commands

### `kat setup`

Install the LaunchAgent, the Docker context, the SSH config files under `~/.katamaran/ssh/`, and the `~/.local/bin/kat` symlink.

```sh
kat setup [--default] [--suffix <SUFFIX>]
```

| Option | Description |
|---|---|
| `--default` | Set the katamaran Docker context as the active default |
| `--suffix <SUFFIX>` | Suffix for the LaunchAgent label, Docker context, and symlink |

### `kat uninstall`

Remove the LaunchAgent, Docker context, and `kat` symlink installed by `kat setup`. Stored VM data under `~/.katamaran` is preserved.

```sh
kat uninstall [--suffix <SUFFIX>]
```

## Linux machine commands

### `kat machine create`

Download an image if needed, create an LXC machine, and start it.

```sh
kat machine create <NAME> [-d <DISTRO>] [-r <VERSION>]
```

| Option | Default | Description |
|---|---|---|
| `-d`, `--distro <DISTRO>` | `ubuntu` | Distribution (`ubuntu`, `debian`, `alpine`, `fedora`, `arch`, or any image on `images.linuxcontainers.org`) |
| `-r`, `--version <VERSION>` | current release | Specific release name or version (for example `noble` or `3.24`) |

### `kat machine list`

List all Linux machines and their states.

```sh
kat machine list [--json]
```

### `kat machine start`

Start a stopped machine (defaults to the default machine if `NAME` is omitted).

```sh
kat machine start [NAME]
```

### `kat machine stop`

Stop a running machine and disable its autostart on VM boot.

```sh
kat machine stop [NAME]
```

### `kat machine delete`

Stop and permanently delete a machine and its root filesystem.

```sh
kat machine delete <NAME>
```

### `kat machine info`

Display metadata, IP address, and state for one machine.

```sh
kat machine info [NAME]
```

### `kat machine default`

Print the current default machine, or set a new default machine.

```sh
kat machine default [NAME]
```

### `kat shell`

Open an interactive login shell or run a command inside a Linux machine.

```sh
kat shell [-u <USER>] [MACHINE] [-- <CMD>...]
```

| Option | Default | Description |
|---|---|---|
| `-u`, `--user <USER>` | your Mac user | Linux user to run as |

### `kat ssh`

Connect to a Linux machine through katamaran's managed SSH configuration.

```sh
kat ssh [DEST] [-- <CMD>...]
```

`DEST` is `<machine>` or `<user>@<machine>`. Omit `DEST` to connect to the default machine.

## Foreground and diagnostic commands

### `katamaran run`

Run one VM directly in the foreground without the LaunchAgent daemon until interrupted with `Ctrl+C` or `SIGTERM`.

```sh
katamaran run [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--cpus <CPUS>` | `4` | Number of vCPUs |
| `--memory-mib <MIB>` | `16384` | Guest memory ceiling in MiB |
| `--kernel <PATH>` | `guest/out/Image` | Path to uncompressed arm64 kernel image |
| `--initramfs <PATH>` | `guest/out/initramfs.cpio` | Path to initramfs archive |
| `--rootfs <PATH>` | `guest/out/rootfs.erofs` | Path to read-only EROFS system image |
| `--data-template <PATH>` | `guest/out/data.img` | Path to template btrfs data disk |
| `--cmdline <ARGS>` | empty | Extra Linux kernel command-line arguments |
| `--vm-bin <PATH>` | sibling `katamaran-vm` | Explicit path to the `katamaran-vm` binary |
| `--no-share` | off | Do not share `/Users` into the guest |
| `--permissions <MODE>` | `caller` | Host file permission mode (`caller`, `caller-uncached`, `fixed`, or `xattr`) |
| `--mount <PATH>` | `~/Katamaran` | Host mount point for Linux NFS files |
| `--no-mount` | off | Disable the macOS NFS mount |
| `--no-network` | off | Boot without a `virtio-net` device |
| `--gvproxy <PATH>` | in-process `libgvproxy.dylib` | Run an external `gvproxy` binary instead of loading `libgvproxy.dylib` |
| `--mtu <BYTES>` | `65520` | MTU of `eth0` and Docker bridges |
| `--pause-on-sleep` | off | Freeze vCPUs while macOS sleeps |
| `--no-auto-forward` | off | Disable forwarding of unpublished container ports |
| `--amd64 <MODE>` | `auto` | x86-64 emulation (`auto`, `rosetta`, `qemu`, or `off`) |

### `katamaran daemon`

Run the background daemon owned by `launchd`. You do not normally invoke this directly.

### `katamaran debug-boot`

Boot a throwaway VM, print per-stage kernel and init boot timings, and power it off immediately.
