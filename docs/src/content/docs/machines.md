---
title: Linux machines
description: Create, start, stop, shell into, and SSH into full Linux distributions running as LXC containers inside the VM.
---

A Linux machine in katamaran is a full Linux system (an LXC container) running inside the katamaran VM with systemd or OpenRC, its own network address, and your macOS username preconfigured.

## Create a machine

Create and start an Ubuntu machine named `dev`:

```sh
kat machine create dev
```

Specify another distribution with `--distro` (`-d`) and an optional release with `--version` (`-r`):

```sh
kat machine create box --distro alpine
kat machine create fed --distro fedora
kat machine create deb --distro debian --version bookworm
```

`--distro` accepts `ubuntu`, `debian`, `alpine`, `fedora`, `arch`, or any distribution published on `images.linuxcontainers.org`.

## List and manage machines

```sh
kat machine list
kat machine list --json
kat machine info dev
kat machine default dev
kat machine stop dev
kat machine start dev
kat machine delete dev
```

Machines that are running when the VM shuts down start automatically the next time the VM boots. Running `kat machine stop <name>` stops the machine and turns off autostart until you start it again.

## Open a shell or run a command

Open an interactive login shell in the default machine or a named machine:

```sh
kat shell
kat shell dev
```

Run a single command without starting an interactive shell:

```sh
kat shell dev -- uname -a
```

Log in as another user, such as `root`:

```sh
kat shell dev -u root
```

## Connect over SSH

katamaran ships an integrated SSH proxy (`katamaran ssh-proxy`) so standard `ssh`, `scp`, `sftp`, VS Code Remote SSH, and rsync work without managing authorized keys manually.

1. Run `kat setup` if you have not done so yet.
2. Add the `Include` line printed by `kat setup` at the top of `~/.ssh/config`:

```text
Include ~/.katamaran/ssh/config
```

3. Connect with `ssh` or `kat ssh`:

```sh
ssh dev@katamaran
ssh root@dev@katamaran
ssh katamaran
kat ssh dev
kat ssh dev -- ls -la
```

- `ssh <machine>@katamaran` connects to `<machine>` as your Mac user.
- `ssh <user>@<machine>@katamaran` connects as `<user>`.
- `ssh katamaran` connects to the default machine.

katamaran generates and pins a host key under `~/.katamaran/ssh/` on first use.

## Ports and files in machines

- TCP ports that a machine listens on across all addresses (`0.0.0.0` or `::`) are forwarded to `127.0.0.1` on macOS automatically, unless the root namespace already owns that port.
- Each machine's filesystem is visible on macOS at `~/Katamaran/machines/<name>` when NFS mounting is enabled.
