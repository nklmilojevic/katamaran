---
title: Kubernetes
description: Run an optional single-node k3s cluster inside katamaran with shared containerd image sync, automatic service port forwarding, and *.k8s.kat.local DNS.
---

katamaran includes an optional single-node Kubernetes (`k3s`) cluster that runs inside the same Linux VM as Docker and your Linux machines. Kubernetes is off by default and uses zero resources when stopped.

## Start and stop the cluster

Enable and start Kubernetes from the command line:

```sh
kat k8s start
```

On the first start, katamaran downloads the selected `k3s-arm64` release (the pinned build by default; see [Choose the k3s version](#choose-the-k3s-version)) into `~/.katamaran/cache/k3s/`, verifies its SHA-256 checksum, transfers it into the VM, enables CRI on the shared `containerd` daemon, and waits for the API server to become ready. Starting Kubernetes also sets `kubernetes = true` in `~/.katamaran/config.toml` so the cluster starts automatically on future VM boots.

You can also toggle Kubernetes immediately while the VM is running with `kat config set` or in the macOS app (**Settings > Kubernetes > Enable Kubernetes**, or the **Turn On** button in the **Pods** and **Services** views):

```sh
kat config set kubernetes=on
kat config set kubernetes=off
```

Check cluster state, version, API endpoint, kubeconfig path, and DNS status:

```sh
kat k8s status
kat k8s status --json
```

Stop Kubernetes and disable auto-start on VM boot:

```sh
kat k8s stop
```

Stopping Kubernetes terminates all pods in containerd's `k8s.io` namespace without affecting your Docker containers (`moby` namespace) and removes the `katamaran` context from `~/.kube/config`.

To wipe all cluster state, workloads, and persistent volumes and start fresh:

```sh
kat k8s reset
```

## Choose the k3s version

Each katamaran release pins a tested `k3s` build. To keep the local cluster on the same minor as production, set `k8s_version` to any [k3s release tag](https://github.com/k3s-io/k3s/releases) from `v1.31` on:

```sh
kat config set k8s_version=v1.32.5+k3s1
kat config set k8s_version=default   # back to the pinned build
```

katamaran downloads that release's `k3s-arm64` into `~/.katamaran/cache/k3s/<version>/` and verifies it against the SHA-256 published in the release's `sha256sum-arm64.txt` (the pinned build is checked against a checksum built into katamaran). The new version is installed at the next Kubernetes start, so a running cluster picks it up after `kat k8s stop && kat k8s start` or `kat restart`.

- **Upgrading while keeping cluster state:** follow the [K3s manual upgrade guide](https://docs.k3s.io/upgrades/manual) — back up your cluster state first and upgrade one minor version at a time without skipping intermediate minors. Clearing `k8s_version` (`kat config set k8s_version=default`) selects the pinned release immediately on the next Kubernetes start rather than stepping through intermediate minors; if your cluster is more than one minor behind the pinned build, upgrade one minor at a time or reset the cluster first.
- **Switching to an older minor (or starting fresh):** Kubernetes does not support minor-version downgrades against an existing datastore, and katamaran refuses to start an older minor against newer cluster data. To switch to an older minor with a fresh cluster:
  1. Back up any cluster state or data you need.
  2. Set the desired release tag (for example `kat config set k8s_version=v1.32.5+k3s1`).
  3. Run `kat k8s reset` **before** the first start of that tag — this stops Kubernetes, deletes all cluster data, workloads, and persistent volumes, and disables auto-start.
  4. Run `kat k8s start` to initialize the cluster on the selected release and re-enable auto-start.

## Connect with `kubectl`

When Kubernetes starts, katamaran relays `127.0.0.1:6443` on macOS (falling back to an available local port if `6443` is already in use) to the guest API server over virtio-vsock (`~/.katamaran/run/k8s-api.sock`).

katamaran writes a private kubeconfig file at `~/.katamaran/run/kubeconfig.yaml` and merges the `katamaran` context into `~/.kube/config` without overwriting your other contexts (preserving symlinks if `~/.kube/config` is symlinked).

Verify the node is ready:

```sh
kubectl --context katamaran get nodes
```

Switch to the `katamaran` context as your default:

```sh
kubectl config use-context katamaran
kubectl get pods -A
```

If you prefer not to use `~/.kube/config` (or if `kubectl` was not installed when Kubernetes started), point `KUBECONFIG` directly at katamaran's generated file:

```sh
export KUBECONFIG="$(kat k8s kubeconfig --path)"
```

You can also print the raw kubeconfig YAML with `kat k8s kubeconfig`.

## Use local Docker images

`k3s` shares katamaran's existing `containerd` daemon (`/run/containerd/containerd.sock`) and content store. Image layers are deduplicated on disk between Docker (`moby` namespace) and Kubernetes (`k8s.io` namespace).

While Kubernetes is running, katamaran watches Docker image events (`tag`, `pull`, `load`, `import`, and `commit`) and automatically mirrors local Docker images into `k8s.io`. You can also trigger an immediate sync manually:

```sh
kat k8s sync-images
```

When referencing a locally built image in a Kubernetes manifest, set `imagePullPolicy: IfNotPresent` (or `Never`) so kubelet uses the mirrored image instead of trying to pull from a remote registry:

```yaml
containers:
  - name: app
    image: my-local-app:dev
    imagePullPolicy: IfNotPresent
```

## Services, Ingress DNS, and port forwarding

### NodePort and LoadBalancer services

katamaran watches Kubernetes services and automatically forwards TCP `NodePort` and `LoadBalancer` ports to `127.0.0.1:<port>` on macOS:

```sh
kubectl create deployment web --image=nginx:alpine
kubectl expose deployment web --type=LoadBalancer --port=8080 --target-port=80
curl http://127.0.0.1:8080/
```

Internal Kubernetes control-plane ports (`6443`, `6444`, `10250`, `10256`, `10257`, `10259`) are excluded from general port forwarding; the API server uses its dedicated relay.

### Wildcard Ingress DNS (`*.k8s.kat.local`)

`k3s` ships with Traefik as its default Ingress controller. While Kubernetes is running, katamaran runs a local UDP DNS responder on `127.0.0.1:15353` (or another free port if `15353` is busy) that resolves `k8s.kat.local` and `*.k8s.kat.local` to `127.0.0.1`.

Install the macOS resolver entry (`/etc/resolver/k8s.kat.local`, prompts for `sudo` once):

```sh
kat k8s dns install
```

Check or remove the resolver at any time:

```sh
kat k8s dns status
kat k8s dns uninstall
```

With the resolver installed, any Ingress rule with a host such as `app.k8s.kat.local` resolves directly from your Mac browser and `curl`.

### Forward a Pod IP or ClusterIP directly

To reach a Pod IP (`10.42.0.0/16`) or Service ClusterIP (`10.43.0.0/16`) from macOS without exposing a NodePort or Ingress, use `kat k8s forward`:

```sh
kat k8s forward 10.43.0.1:443 --local 8443
kat k8s forward 10.42.0.5:8080
```

If `--local` is omitted, katamaran binds the same port number on `127.0.0.1`.

## Storage and host paths

- **Cluster data**: `k3s` stores its state and `local-path` persistent volumes under `/var/lib/katamaran/k3s/data` on the VM's btrfs `data.img` disk (as a btrfs subvolume when supported, making `kat k8s reset` fast).
- **Host directories**: Because katamaran mounts `/Users` into the VM at the same path, Kubernetes `hostPath` volumes pointing to directories under `/Users` access your macOS files directly.

## Cluster defaults

| Setting | Value |
|---|---|
| Node name | `katamaran` |
| Container runtime | Shared `containerd` (`/run/containerd/containerd.sock`) |
| Pod CIDR | `10.42.0.0/16` (Flannel `host-gw`) |
| Service CIDR | `10.43.0.0/16` |
| Cluster DNS | `10.43.0.10` |
| kube-proxy mode | `nftables` |
| API server | `https://127.0.0.1:6443` (or fallback port) |
| Guest log file | `/var/log/k3s.log` |

## Manage in the macOS app

`Katamaran.app` includes **Pods** and **Services** views in the sidebar for inspecting workloads, following container logs, opening `LoadBalancer` and `NodePort` services in your browser, and managing cluster settings without needing `kubectl` installed on macOS. See [macOS app](/app/) for details, or [Command-line reference](/cli/#kubernetes-commands) for every `kat k8s` subcommand.
