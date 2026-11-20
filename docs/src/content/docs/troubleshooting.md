---
title: Troubleshooting
description: Inspect logs, diagnose startup or mount issues, and recover a stuck katamaran instance.
---

Use the built-in status and logging commands to inspect both the macOS host side and the Linux guest.

## Check overall state

Run `kat status` to verify that the daemon, VM, Docker Engine, NFS mount, and port forwards are healthy:

```sh
kat status
kat status --json
```

## Read logs by component

katamaran separates logs into four streams:

```sh
kat logs --source daemon -n 200
kat logs --source console -n 200
kat logs --source vm -n 200
kat logs --source gvproxy -n 200
```

Add `-f` to follow any stream live. Inside the guest, containerd and dockerd also write to `/var/log/containerd.log` and `/var/log/dockerd.log`.

## Measure boot stages

If the VM starts slowly or fails during boot, run `katamaran debug-boot` to boot a throwaway VM and print per-stage timings:

```sh
katamaran debug-boot
```

## Docker commands cannot connect to the socket

1. Confirm the VM is running with `kat status`.
2. Verify which Docker context is active:

```sh
docker context ls
```

3. Switch to `katamaran` if another context is selected:

```sh
docker context use katamaran
```

4. Check whether `DOCKER_HOST` is overriding the Docker context in your shell:

```sh
echo "$DOCKER_HOST"
```

Unset `DOCKER_HOST` or point it to `unix://$HOME/.katamaran/run/docker.sock`.

## Gatekeeper blocks a local build

Unsigned builds made with `cargo xtask app` or `cargo xtask dmg` are ad-hoc signed. If macOS refuses to open `Katamaran.app`:

1. Open **System Settings**.
2. Select **Privacy & Security**.
3. Click **Open Anyway** next to the **Katamaran** message.

## `~/Katamaran` mount does not appear or is stale

1. Check whether `mount` is enabled:

```sh
kat config get mount
kat config get mount_path
```

2. Restart the VM to remount cleanly:

```sh
kat restart
```

3. If a previous crash left a stale NFS mount point, unmount it manually before starting again:

```sh
umount -f ~/Katamaran
```

## Published or auto-forwarded port is unreachable

1. Check that the service inside the container listens on `0.0.0.0` or `::`, not only `127.0.0.1`. Automatic port forwarding and `-p` only expose wildcard listeners.
2. Check whether another macOS process already holds the same port on `127.0.0.1`:

```sh
lsof -nP -iTCP:<PORT> -sTCP:LISTEN
```

3. Verify the port appears in `kat status` and check `kat logs --source gvproxy`.

## Reset all VM data

If the btrfs data disk becomes corrupted or you want a completely clean Docker and machine state:

1. Stop the VM:

```sh
kat stop
```

2. Remove the data disk (this deletes all Docker images, containers, volumes, and Linux machines):

```sh
rm -f ~/.katamaran/data/data.img
```

3. Start the VM again. katamaran clones a fresh `data.img` on startup:

```sh
kat start
```
