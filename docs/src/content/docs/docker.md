---
title: Use Docker
description: Connect Docker and Compose to katamaran, manage storage, and inspect the containerd image store.
---

katamaran runs `containerd` and `dockerd` inside the Linux guest and relays the Docker Unix socket to macOS over virtio-vsock.

## Select the Docker context

`kat setup` creates a Docker context named `katamaran`. Use it for one command:

```sh
docker --context katamaran ps
```

Switch to it for all commands:

```sh
docker context use katamaran
```

For tools that ignore Docker contexts and read `DOCKER_HOST`, export the socket path:

```sh
export DOCKER_HOST="unix://$HOME/.katamaran/run/docker.sock"
```

When `KATAMARAN_HOME` is set to a custom directory, the socket lives at `$KATAMARAN_HOME/run/docker.sock`.

## Run Docker Compose

`docker compose` inherits the active Docker context or `DOCKER_HOST`. Run it from any project directory under `/Users`:

```sh
docker compose up -d
docker compose ps
docker compose down
```

Relative bind mounts such as `.:/app` resolve against `/Users`, which katamaran shares into the guest at the same path.

## Understand Docker storage inside the VM

The guest uses a read-only EROFS system image as the lower layer of an overlay root filesystem, paired with a sparse btrfs data disk at `~/.katamaran/data/data.img`. The data disk holds three btrfs subvolumes mounted with `noatime` and `discard=async`:

| Subvolume | Guest mount path | Purpose |
|---|---|---|
| `docker` | `/var/lib/docker` | Docker runtime state, networks, and volumes |
| `containerd` | `/var/lib/containerd` | Containerd image store and `overlayfs` snapshots |
| `katamaran` | `/var/lib/katamaran` | Linux machines and persistent guest state |

Docker runs with the containerd image store (`containerd-snapshotter: true`) and the `overlayfs` snapshotter. Periodic `FITRIM` discards unused blocks on the btrfs data disk so the sparse `data.img` file on APFS shrinks as you prune unused images and containers.

Reclaim unused Docker storage inside the guest with standard Docker commands:

```sh
docker system df
docker system prune
```

## Tune Docker daemon resources

Docker uses the vCPUs and memory ceiling configured for the katamaran VM. Change them and restart the VM:

```sh
kat config set cpus=8 memory=12G
kat restart
```

See [Configuration reference](/configuration/) for all VM settings.
