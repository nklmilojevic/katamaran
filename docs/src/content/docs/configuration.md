---
title: Configuration reference
description: Settings in ~/.katamaran/config.toml and environment variables recognized by katamaran.
---

katamaran stores daemon and VM settings in `~/.katamaran/config.toml` (or `$KATAMARAN_HOME/config.toml` when `KATAMARAN_HOME` is set). The daemon reads `config.toml` every time the VM starts.

## File format

```toml
# katamaran settings; applied at the next VM start.
cpus = 14
memory_mib = 16384
mount = true
launch_at_login = true
pause_on_sleep = false
auto_forward = true
amd64 = "auto"
kubernetes = false
```

Edit settings with `kat config set` or the **Settings** window in the app, then run `kat restart` (changes to `launch_at_login` and `kubernetes` apply immediately; changes to `k8s_version`, `k8s_disable`, and `k8s_args` apply on the next Kubernetes start).

Some settings, such as `nofile`, are not in the app's Settings window. Change them with the CLI or by editing `config.toml`:

```sh
kat config set nofile=65536
kat restart
```

`nofile` is only written to `config.toml` when it differs from the default. Before downgrading to a katamaran version without `nofile`, remove a custom `nofile` line, since older versions reject unknown keys.

## Settings keys

| Key | Type | Default | Valid values |
|---|---|---|---|
| `cpus` | integer | All host cores | `1` to the number of host CPU cores |
| `memory_mib` | integer | Half of host RAM, clamped to `2048` to `16384` | `1024` to `(host RAM - 2048)` MiB. `kat config set` also accepts `memory=8G` or `memory=4096M`. |
| `mount` | boolean | `true` | `true`, `false`, `on`, `off`, `yes`, `no`, `1`, `0` |
| `mount_path` | string (optional) | `~/Katamaran` (unset) | Any absolute path, or empty string to reset to default |
| `launch_at_login` | boolean | `true` | Boolean |
| `pause_on_sleep` | boolean | `false` | Boolean. Freezes vCPUs and adjusts virtual timer offsets across macOS sleep. |
| `auto_forward` | boolean | `true` | Boolean. Forwards unpublished container TCP ports to `127.0.0.1` on macOS. |
| `amd64` | string | `"auto"` | `"auto"`, `"rosetta"`, `"qemu"`, or `"off"` |
| `kubernetes` | boolean | `false` | Boolean. Enables the optional single-node Kubernetes (`k3s`) cluster; applies immediately when the VM is running. |
| `k8s_version` | string (optional) | Pinned build (unset) | A supported k3s release tag, `v1.31` or newer (for example `"v1.32.5+k3s1"`). To reset to the pinned build, remove the key from `config.toml` or run `kat config set k8s_version=default` (or `kat config set k8s_version=`). Applies at the next Kubernetes start. Before rolling back to an older katamaran release without `k8s_version`: (1) if you ran a custom k3s release, stop Kubernetes and disable auto-start (`kat k8s stop`, setting `kubernetes = false`) so the older daemon does not automatically start its pinned k3s build against the retained datastore; (2) clear the key with `kat config set k8s_version=default`, or, if you already rolled back, remove the `k8s_version` line (and set `kubernetes = false` if you have a custom k3s datastore) in `config.toml`, because older releases reject unknown keys and cannot start the VM until `k8s_version` is removed; (3) compare the retained datastore's k3s version against the destination katamaran release's pinned k3s before enabling Kubernetes again: use a supported sequential minor-upgrade path, restore a matching backup, or run `kat k8s reset` rather than skipping minors or downgrading in place. When upgrading an existing cluster from an older release (or after resetting and recreating a cluster on an older release), katamaran accepts it automatically when `server/db/state.db` shows it was last run by `v1.37.2+k3s1` (the only build older releases pinned); otherwise, follow [Migrating an unmarked cluster from an older katamaran release](/kubernetes/#migrating-an-unmarked-cluster-from-an-older-katamaran-release). See [Choose the k3s version](/kubernetes/#choose-the-k3s-version) for sequential minor upgrades, backup/restore, and reset options. |
| `k8s_disable` | string array | `[]` | Bundled `k3s` components to disable (for example `["traefik"]`, or `kat config set k8s_disable=traefik,servicelb`). Passed as `--disable=<name>` on `k3s` start. Omitted from `config.toml` when empty; before rolling back to an older katamaran release without this key, clear it with `kat config set k8s_disable=""` (or remove the `k8s_disable` line from `config.toml`), because older releases reject unknown keys and cannot start the VM until it is removed. |
| `k8s_args` | string array | `[]` | Extra CLI arguments appended to `k3s server` (in `kat config set`, space-separated with shell-style quoting; empty string clears). Applies on `k3s` start. Omitted from `config.toml` when empty; before rolling back to an older katamaran release without this key, clear it with `kat config set k8s_args=""` (or remove the `k8s_args` line from `config.toml`), because older releases reject unknown keys and cannot start the VM until it is removed. |
| `nofile` | integer | `1048576` | `1024` to `1073741824`. Open-files limit (`RLIMIT_NOFILE`, soft and hard) set at guest boot, like `LimitNOFILE` in Docker Desktop. containerd, dockerd, and containers and pods inherit it unless they set their own (for example `docker run --ulimit nofile=...`). |

`memory_mib` is an upper ceiling, not a fixed allocation. macOS allocates pages on demand as the guest touches them, and katamaran returns freed 64 KiB blocks back to macOS.

## State directory layout

All host state lives under `~/.katamaran` (or `$KATAMARAN_HOME`):

| Path | Contents |
|---|---|
| `config.toml` | Saved configuration |
| `k3s-config.yaml` | Optional user `k3s` configuration file synced to `/var/lib/katamaran/k3s/config.yaml` (mode `0600`, passed via `K3S_CONFIG_FILE`) when Kubernetes starts |
| `data/data.img` | Sparse btrfs disk image for Docker, containerd, and LXC machines |
| `run/docker.sock` | Docker Engine Unix socket relayed from the guest |
| `run/daemon.sock` | Control socket used by the CLI and macOS app |
| `run/k8s.sock` | Kubernetes control socket relayed to the guest |
| `run/k8s-api.sock` | Kubernetes API server socket relayed from the guest |
| `run/k8s-proxy.sock` | Plain-HTTP Kubernetes API socket relayed to the guest `kubectl proxy` (used by the macOS app) |
| `run/kubeconfig.yaml` | Generated kubeconfig for the single-node Kubernetes cluster |
| `run/vm.json` | Generated VM launch configuration passed to `katamaran-vm` |
| `cache/k3s/` | Cached and SHA-256 verified `k3s-arm64` binaries downloaded on first Kubernetes start, one directory per version |
| `logs/` | Logs for `daemon`, `console`, `vm`, and `gvproxy` |
| `ssh/` | Managed SSH host key, `known_hosts`, and `config` file |

## Environment variables

| Variable | Effect |
|---|---|
| `KATAMARAN_HOME` | Overrides the state directory (default `~/.katamaran`). Isolates a separate VM instance, Docker socket, and data disk. |
| `KATAMARAN_MOUNT` | Overrides the macOS NFS mount path when `mount_path` is unset. |
| `KATAMARAN_LABEL_SUFFIX` | Default `--suffix` value for `kat setup` and `kat uninstall`. |
| `KATAMARAN_BUILD_CONTEXT` | Docker context used by `cargo xtask kernel`, `rootfs`, and `disk`. |
| `KATAMARAN_FS_CACHE` | Set to `0` to disable long virtio-fs metadata caching and FSEvents invalidation. |
| `KATAMARAN_FS_DIRECT` | Set to `0` to disable `KAT_FS_DIRECT` hypercalls and route all FUSE requests over virtqueues. |
| `KATAMARAN_FS_BULK` | Set to `0` to fall back from `getattrlistbulk` to `readdir` and `lstat`. |
| `KATAMARAN_FS_PINS` | Set to `0` to disable pinned `O_EVTONLY` directory file descriptors. |
| `KATAMARAN_FS_TIMEOUT` | Override virtio-fs entry and attribute cache timeout in seconds. |
| `KATAMARAN_FS_NEGATIVE` | Override virtio-fs negative lookup cache timeout in seconds. |
| `KATAMARAN_BALLOON_MODE` | Set free-page reporting mode (`replace` default, `default`, or `off`). |
