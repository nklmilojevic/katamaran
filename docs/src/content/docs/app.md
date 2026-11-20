---
title: macOS app
description: Manage containers, images, volumes, builds, networks, Linux machines, logs, and settings from the native macOS app.
---

`Katamaran.app` is a native Swift and AppKit application that lives in the macOS menu bar and opens a full management window on demand. It communicates with `katamaran daemon` and the guest Docker socket directly over Unix sockets.

## Menu bar controls

Click the sailboat icon in the macOS menu bar to:

- Check whether the VM and Docker are running, starting, or stopped
- Start, stop, or restart the VM
- Open the main management window
- Open the **Settings** window
- Quit the app

Closing the main window keeps the menu-bar app and the VM running.

## Main window sections

The main window sidebar groups management views into three sections.

### Docker

- **Containers**: List running and stopped containers, start, stop, restart, remove, inspect environment and mounts, stream logs, and open an interactive terminal powered by `libghostty-vt` (`crates/kat-term`).
- **Volumes**: Inspect Docker volumes, open volume data in Finder under `~/Katamaran/docker/volumes/`, and remove unused volumes.
- **Images**: Browse local images in the containerd store, view sizes and tags, and prune unused images.
- **Builds**: Monitor active and recent Docker BuildKit builds with step progress.
- **Networks**: Inspect Docker bridge networks and attached containers.

![Containers list with live logs](../../assets/screenshots/containers.png)

![Container inspection showing ports, mounts, and environment](../../assets/screenshots/containers-info.png)

### Linux

- **Machines**: Create, start, stop, delete, and open interactive terminals into LXC Linux machines.

![Linux machines view](../../assets/screenshots/machines.png)

### General

- **Activity**: View live CPU and memory usage of the VM.
- **Logs**: Stream live logs from the daemon, guest console, VM monitor, and gvproxy.

![Activity view with live VM CPU, memory, and per-container usage](../../assets/screenshots/activity.png)

## Settings window

Open **Settings** from the menu bar or press `Cmd+,` to adjust:

- CPU count (`cpus`)
- Memory limit (`memory_mib`)
- x86-64 emulation mode (`amd64`)
- Automatic port forwarding (`auto_forward`)
- macOS NFS file mount (`mount` and `mount_path`)
- Start at login (`launch_at_login`)
- Pause VM while Mac sleeps (`pause_on_sleep`)

Changes write atomically to `~/.katamaran/config.toml` and indicate when a VM restart is required.
