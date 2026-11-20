---
title: File sharing
description: Share macOS folders into containers with fast metadata caching, caller ownership, inotify events, and Finder access to Linux files.
---

katamaran shares files in both directions: macOS files under `/Users` are mounted into Linux over virtio-fs, and Linux files are mounted on macOS at `~/Katamaran` over NFSv3.

## Share macOS folders with containers

katamaran mounts `/Users` from your Mac into the Linux guest at `/Users` at boot. Any Docker bind mount under `/Users` works without extra configuration:

```sh
docker run --rm -v "$PWD:/app" -w /app alpine:3.24 ls -la
```

### File ownership inside containers

By default, katamaran mounts `/Users` with the `caller` permission mode (`owner_caller` in the guest FUSE driver). Every shared file appears owned by the calling container process's UID and GID, while mode bits pass through to macOS.

This design lets root and non-root container users read and write your Mac files without `chown` errors, while keeping file attributes cacheable across every container.

When running the VM directly with `katamaran run`, you can override `--permissions`:

| Mode | Behavior |
|---|---|
| `caller` | Default. Files appear owned by the calling process, attributes stay cached, and `chown` is ignored. |
| `caller-uncached` | Host substitutes the caller UID and GID per request with attribute caching turned off. |
| `fixed` | Files appear owned by root (`0:0`) with attribute caching enabled. |
| `xattr` | Stores Linux UID, GID, and mode in a host extended attribute (`user.containers.override_stat`). |

### Host changes and inotify

katamaran caches entries, attributes, negative lookups, file pages, and unchanged directory listings inside the Linux guest for up to 3,600 seconds. A macOS FSEvents watcher in `katamaran-vm` tracks changes on the Mac and performs two actions:

1. Drops stale entries, attributes, page caches, and directory listings in the guest by FUSE node ID so containers see host edits immediately.
2. Delivers Linux inotify events (`IN_CREATE`, `IN_DELETE`, `IN_MODIFY`, `IN_MOVED_FROM`, `IN_MOVED_TO`) to watchers inside containers (such as Vite, Webpack, Jest, and `cargo watch`).

You do not need polling watchers (`CHOKIDAR_USEPOLLING`) for projects mounted under `/Users`.

### Case sensitivity note

macOS APFS is case-insensitive by default. When a host file changes, katamaran expires matching cached names in the guest regardless of ASCII case so lookups for different casings stay consistent.

## Browse Linux files on macOS

When `mount` is enabled (the default), katamaran runs an in-guest NFSv3 server (`kat-nfs`) over a kernel-only loopback socket and mounts it at `~/Katamaran`. Open `~/Katamaran` in Finder or in your Mac terminal to inspect and edit Linux files directly:

| Path under `~/Katamaran` | Contents |
|---|---|
| `docker/volumes/<name>` | Data stored in each named Docker volume |
| `docker/containers/<name>` | Filesystem of each running Docker container |
| `machines/<name>` | Root filesystem of each Linux machine |

### Change or disable the macOS mount

Move the mount point to a different absolute path:

```sh
kat config set mount_path=/Users/you/LinuxFiles
kat restart
```

Turn off the macOS mount completely:

```sh
kat config set mount=off
kat restart
```

When `KATAMARAN_HOME` is set, the NFS mount defaults to off unless you pass `--mount` or set `mount_path`, keeping development instances separated from your main `~/Katamaran` folder.
