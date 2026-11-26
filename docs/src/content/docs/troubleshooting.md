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

Add `-f` to follow any stream live. Inside the guest, containerd, dockerd, and k3s also write to `/var/log/containerd.log`, `/var/log/dockerd.log`, and `/var/log/k3s.log`.

## Open a root shell in the VM

Unlike `kat shell` (which opens a Linux LXC machine), `kat vm shell` and `kat vm exec` run as `root` directly in the VM's own namespaces, even when Kubernetes is disabled:

```sh
kat vm shell
kat vm exec -- sysctl fs.nr_open
kat vm exec -- sh -c 'pid=$(pgrep -xo -P 1 containerd) || { echo "containerd is not running" >&2; exit 1; }; grep "open files" "/proc/$pid/limits"'
kat vm exec -- tail -n 100 /var/log/containerd.log
```

`kat vm exec` does not allocate a PTY by default, so `stdout`, `stderr`, and exit codes behave cleanly in scripts. Pass `-t` to allocate a PTY for interactive tools such as `top`.

If you upgraded katamaran while the VM was running and the CLI reports that the VM shell service is unavailable, run `kat restart` to start a VM with the new service.

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

3. Check `kat status` for `port err` entries (or `port_errors` in `kat status --json`) and inspect `kat logs --source gvproxy` and `kat logs --source daemon`.

## Kubernetes cluster will not start or `kubectl` cannot connect

1. Check cluster state, API address, and any reported error:

```sh
kat k8s status
```

2. Confirm `kubectl` is targeting the `katamaran` context (or point `KUBECONFIG` at katamaran's kubeconfig if `kubectl` was not installed when k3s started):

```sh
kubectl config current-context
kubectl config use-context katamaran
# Or use the standalone kubeconfig directly:
export KUBECONFIG="$(kat k8s kubeconfig --path)"
```

3. Inspect daemon and guest console logs for `k3s` errors:

```sh
kat logs --source daemon -n 200
kat logs --source console -n 200
```

4. If a local image fails with `ErrImagePull`, run `kat k8s sync-images` and set `imagePullPolicy: IfNotPresent` (or `Never`) on the container spec.

5. To wipe a broken cluster state without touching Docker containers or Linux machines, reset Kubernetes and start it again (`kat k8s reset` keeps cached `k8s.io` images by default; add `--images` to clear them too):

```sh
kat k8s reset
# Or also clear cached k8s.io images:
kat k8s reset --images
kat k8s start
```

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
