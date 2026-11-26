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

### `kat vm shell`

Open an interactive root shell directly in the Linux VM's own namespaces (unlike `kat shell`, which connects to an LXC Linux machine). Useful for inspecting `containerd`, `dockerd`, `k3s`, sysctls, rlimits, and `/var/log/*`. Starts the VM automatically if it is stopped.

A pseudo-terminal is allocated only if both stdin and stdout are terminals. To request one explicitly, use `kat vm exec -t -- <CMD>`.

```sh
kat vm shell [-w <DIR>] [-e <KEY=VALUE>]... [-- <CMD>...]
```

| Option | Default | Description |
|---|---|---|
| `-w`, `--workdir <DIR>` | `/root` | Working directory inside the VM |
| `-e`, `--env <KEY=VALUE>` | none | Extra environment variable (`KEY=VALUE`, or `KEY` to copy from the host); repeatable |

### `kat vm exec`

Run a command as `root` directly in the Linux VM's own namespaces. Designed for scripting: no pseudo-terminal is allocated by default, keeping `stdout` and `stderr` separate and preserving the command's exit code. Starts the VM automatically if it is stopped.

```sh
kat vm exec [-t] [-w <DIR>] [-e <KEY=VALUE>]... [--] <CMD>...
```

| Option | Default | Description |
|---|---|---|
| `-t`, `--tty` | off | Allocate a pseudo-terminal and combine the command's stdout and stderr on stdout |
| `-w`, `--workdir <DIR>` | `/root` | Working directory inside the VM |
| `-e`, `--env <KEY=VALUE>` | none | Extra environment variable (`KEY=VALUE`, or `KEY` to copy from the host); repeatable |

## Configuration commands

### `kat config get`

Print every setting in `~/.katamaran/config.toml`, or print one key.

```sh
kat config get [KEY]
```

### `kat config set`

Update one or more settings and validate them against the Mac's hardware limits. Changes apply on the next VM start (except `launch_at_login`, which updates the LaunchAgent immediately, and `kubernetes`, which starts or stops k3s immediately when the VM is running).

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

### `kat skill`

Manage the coding-agent skill for Claude Code (`~/.claude/skills/katamaran`) and Codex, OpenCode, and Pi (`~/.agents/skills/katamaran`). Installed agents receive a small stub that runs `kat skill show`, so upgrading katamaran updates the guide automatically.

```sh
kat skill setup [--agent claude|agents]
kat skill show
kat skill status [--agent claude|agents] [--json]
kat skill remove [--agent claude|agents]
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

## Kubernetes commands

`kat k8s` (aliased as `kat kubernetes`) manages the optional single-node Kubernetes (`k3s`) cluster inside the VM. See the [Kubernetes guide](/kubernetes/) for setup, image syncing, Ingress DNS, and networking details.

### `kat k8s status`

Show Kubernetes state, k3s version, API endpoint, kubeconfig context and path, and `*.k8s.kat.local` DNS status.

```sh
kat k8s status [--json]
```

| Option | Description |
|---|---|
| `--json` | Emit the Kubernetes status payload as JSON |

### `kat k8s start`

Enable and start the Kubernetes cluster, downloading the selected `k3s` release (the pinned build by default; see [Choose the k3s version](/kubernetes/#choose-the-k3s-version)) on first use and merging the `katamaran` context into `~/.kube/config`.

```sh
kat k8s start
```

### `kat k8s stop`

Stop Kubernetes and disable auto-start on VM boot. Uses zero resources when off.

```sh
kat k8s stop
```

### `kat k8s reset`

Stop Kubernetes and delete all cluster state, workloads, and persistent volumes.

```sh
kat k8s reset
```

### `kat k8s mark-version`

Record an independently verified `k3s` release tag in `/var/lib/katamaran/k3s/data/.katamaran-version` for an existing unmarked cluster datastore that katamaran cannot accept automatically (only a `server/db/state.db` node record showing `v1.37.2+k3s1`, the build every pre-tracking release pinned, is accepted without a marker). Requires the VM to be running and Kubernetes to be stopped, validates the release tag, and refuses to overwrite an existing marker, to mark a datastore whose version is already known (a `server/db/.katamaran-k3s-version` stamp exists), or to mark a lower minor than `state.db` already records. If the VM is stopped, disable Kubernetes auto-start (`kat config set kubernetes=off`) if enabled and run `kat start` before marking. See [Migrating an unmarked cluster from an older katamaran release](/kubernetes/#migrating-an-unmarked-cluster-from-an-older-katamaran-release).

```sh
kat k8s mark-version <VERSION>
```

### `kat k8s kubeconfig`

Print the cluster kubeconfig YAML, or print the path to `~/.katamaran/run/kubeconfig.yaml`.

```sh
kat k8s kubeconfig [--path]
```

| Option | Description |
|---|---|
| `--path` | Print the path to `<home>/run/kubeconfig.yaml` instead of its contents |

### `kat k8s sync-images`

Mirror locally built Docker images into Kubernetes immediately.

```sh
kat k8s sync-images
```

### `kat k8s dns`

Manage macOS DNS resolution for `*.k8s.kat.local` via `/etc/resolver/k8s.kat.local`.

```sh
kat k8s dns status
kat k8s dns install
kat k8s dns uninstall
```

| Subcommand | Description |
|---|---|
| `status` | Show whether `/etc/resolver/k8s.kat.local` is installed |
| `install` | Install `/etc/resolver/k8s.kat.local` pointing to the local DNS responder (uses `sudo` once; requires Kubernetes to be running) |
| `uninstall` | Remove `/etc/resolver/k8s.kat.local` (uses `sudo` once) |

### `kat k8s forward`

Forward a local TCP port on `127.0.0.1` directly to a Pod IP or Service ClusterIP inside the guest.

```sh
kat k8s forward <TARGET> [--local <PORT>]
```

| Argument / Option | Default | Description |
|---|---|---|
| `<TARGET>` | required | Guest target `<ip>:<port>` (for example `10.43.0.1:443` or `10.42.0.5:8080`) |
| `--local <PORT>` | target port | Local TCP port on `127.0.0.1` |

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
| `--forward-address <IP>` | `127.0.0.1` if omitted | Mac IPv4 address for forwarded TCP ports (`127.0.0.1` or a `lo0` alias); repeat to listen on several addresses. Passing the flag replaces the default, so to keep localhost and add an alias, use `--forward-address 127.0.0.1 --forward-address 172.16.1.200`. |
| `--amd64 <MODE>` | `auto` | x86-64 emulation (`auto`, `rosetta`, `qemu`, or `off`) |

By default (`kat.quickack=1`), the guest kernel shortens ping-pong delayed ACKs on in-VM TCP traffic routed inside container and Kubernetes pod network namespaces or over local bridges/`veth` devices in the guest root namespace (`cni0`, `docker0`, `br-*`, `katbr0`). When userspace reads data that ends in a small or `PSH`-terminated segment, the ACK goes out immediately. When a `PSH` segment stays queued below the reader's `SO_RCVLOWAT`, the kernel compares its payload against an upper bound derived from the guest's advertised MSS (net of negotiated TCP options) capped at half the largest receive window the guest advertised, and uses the peer's advertised MSS (net of options) as a lower hint. A segment strictly below both limits is classified as a definite short push and gets an immediate ACK if no earlier ACK already covers unread data; otherwise—for a second short push in the same unread batch (such as the middle write of a three-write Nagle exchange) or an uncertain push between the peer hint and local upper bound (such as an asymmetric low `advmss` metric on a jumbo-MTU path)—the kernel keeps the stock ACK checks and shortens at most one pending delayed-ACK timer per unread batch to the next timer tick (about 10 ms) instead of 40–50 ms. Subsequent paced pushes in the same unread batch, full-sized segments waiting below `SO_RCVLOWAT`, and sustained bulk transfers keep stock Linux delayed-ACK timing and batching. Where a sender's full-sized segments are smaller than the local upper bound (for example an intermediate hop narrower than the advertised MSS, MTU probing on the sender only, SACK blocks during loss recovery, or MD5/AO signatures), at most one delayed-ACK timer per unread batch is shortened while below `SO_RCVLOWAT`, bounding any extra ACK cost without delaying data; in-VM container and bridge paths normally hit none of these. A route opts out only if it locks the `RTAX_QUICKACK` metric (`RTAX_LOCK`); iproute2 cannot set that lock, so use rtnetlink directly, and note that an unlocked `quickack 0` is indistinguishable from an unset metric and is overridden. Routes that set `quickack 1` keep it, and traffic crossing the `eth0` uplink and all routing tables remain untouched. This avoids 40–50 ms Nagle + delayed-ACK stalls on split-write keep-alive traffic with the 65520-byte MTU. The trade-off is that a small request read on an eligible socket is ACKed on its own rather than piggybacked on the reply, so ping-pong RPC traffic sends roughly one extra pure ACK per message. Eligibility comes from the route the socket itself uses, before any NAT: guest root-namespace clients that reach a pod or container through a NAT'd address (a ClusterIP from a host-network pod, or a published port via `localhost`) route via `eth0` or `lo` and keep stock delayed ACKs. To disable it at boot for a foreground VM, pass `--cmdline kat.quickack=0` to `katamaran run` (the daemon-managed VM started by `kat start` does not expose kernel command-line options).

For a running VM, including the one started by `kat start`, a privileged write to the guest's `/sys/module/kat/parameters/quickack` switches the behavior at runtime: `0` disables it and `1` re-enables it, for subsequent ACK decisions on new and established TCP connections; timers already armed are not rescheduled. For the default daemon-managed VM, run `docker --context katamaran run --rm --privileged alpine sh -c 'echo 0 > /sys/module/kat/parameters/quickack'`. For a foreground VM started with `katamaran run`, set `DOCKER_HOST` to the Unix socket printed on startup instead of using a Docker context.

### `katamaran daemon`

Run the background daemon owned by `launchd`. You do not normally invoke this directly.

### `katamaran debug-boot`

Boot a throwaway VM, print per-stage kernel and init boot timings, and power it off immediately.
