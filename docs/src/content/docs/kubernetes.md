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

katamaran downloads that release's `k3s-arm64` into `~/.katamaran/cache/k3s/<version>/` and checks it against the SHA-256 in the same release's `sha256sum-arm64.txt`. That is an integrity check (it catches corrupted or truncated downloads); authenticity rests on HTTPS to GitHub. Only the pinned build is checked against a checksum built into katamaran. The new version is installed at the next Kubernetes start, so a running cluster picks it up after `kat k8s stop && kat k8s start` or `kat restart`.

- **Upgrading while keeping cluster state:** follow the [K3s manual upgrade guide](https://docs.k3s.io/upgrades/manual): back up your cluster state first and upgrade one minor version at a time without skipping intermediate minors. Katamaran refuses to start a release that skips one or more minor versions against an existing datastore. Clearing `k8s_version` (`kat config set k8s_version=default`) selects the pinned release immediately on the next Kubernetes start rather than stepping through intermediate minors; if your cluster is more than one minor behind the pinned build, upgrade one minor at a time or reset the cluster first.
- **Switching to an older minor (fresh cluster):** Kubernetes does not support minor-version downgrades against an existing datastore, and katamaran refuses to start an older minor against newer cluster data. To switch to an older minor with a fresh cluster:
  1. Back up any cluster state or data you need.
  2. Set the desired release tag (for example `kat config set k8s_version=v1.32.5+k3s1`).
  3. Run `kat k8s reset` **before** the first start of that tag. This stops Kubernetes, deletes all cluster data, workloads, and persistent volumes, and disables auto-start.
  4. Run `kat k8s start` to initialize the cluster on the selected release and re-enable auto-start.
- **Backing up and restoring a SQLite datastore for rollback:** to roll back a minor upgrade while keeping pre-upgrade cluster state (see the [K3s rollback guide](https://docs.k3s.io/upgrades/roll-back)), note that a SQLite backup preserves Kubernetes API objects (including PV and PVC records) but **not** persistent-volume file contents. Take datastore, version-marker, and persistent-volume backups from the same stopped point in time:
  1. **Before upgrading**, quiesce workloads and stop Kubernetes (`kat k8s stop`) so nothing writes to SQLite or persistent volumes. Back up:
     - `/var/lib/katamaran/k3s/data/server/` (including `server/db/` and hidden files such as `server/db/.katamaran-k3s-version`) for cluster state and Kubernetes objects.
     - `/var/lib/katamaran/k3s/data/.katamaran-version` for the cluster's `k3s` release tag. On a legacy cluster created before `.katamaran-version` existed, save the exact tag from the guest's `/var/lib/katamaran/k3s/bin/version` (verified to match the katamaran release that created and ran the cluster) alongside the backup so you can restore it as `.katamaran-version`. If both marker files are missing or you are unsure, do not guess: independently verify which `k3s` release created the datastore before relying on the backup for rollback.
     - `/var/lib/katamaran/k3s/data/storage/` (where k3s's default `local-path` StorageClass stores volume files) and any custom persistent-volume or `hostPath` directories used by your workloads.
  2. **To restore**, keep Kubernetes stopped (`kat k8s stop`) and restore from the same backup set:
     - Move the current `/var/lib/katamaran/k3s/data/server/` aside (or delete it), then restore the complete backed-up `server/` directory, including hidden files. Never copy a backup over the live directory: files left by the newer release, such as `state.db-wal`, `state.db-shm`, or a newer `server/db/.katamaran-k3s-version` stamp, would mix with the restored data, and a leftover newer stamp makes katamaran refuse the restored cluster. A backup taken before the stamp existed must not inherit one from the installation being rolled back.
     - Restore the matching `/var/lib/katamaran/k3s/data/.katamaran-version` file (from the backed-up `.katamaran-version`, or from the verified legacy `/var/lib/katamaran/k3s/bin/version` tag). Katamaran never infers a datastore's version from `/var/lib/katamaran/k3s/bin/version`: if `.katamaran-version` is missing on an existing datastore, it accepts the datastore only if `server/db/state.db` shows the node was last run by `v1.37.2+k3s1` (the build every pre-tracking katamaran release pinned), and still refuses minor downgrades or skipped-minor upgrades. Otherwise, record the independently verified tag from macOS with `kat k8s mark-version <version>` while the VM is running and Kubernetes is stopped (if the VM is stopped, disable Kubernetes auto-start with `kat config set kubernetes=off` and run `kat start` first).
     - Restore the matching `/var/lib/katamaran/k3s/data/storage/` contents and any custom PV directories so volume files match the restored PV/PVC records.
  3. Set `k8s_version` to the release tag from that backup (for example `kat config set k8s_version=v1.32.5+k3s1`) and run `kat k8s start`.

  Do not change `/var/lib/katamaran/k3s/data/.katamaran-version` without restoring matching cluster data: mismatched metadata allows an older `k3s` binary to open a newer datastore and can corrupt cluster state.

### Migrating an unmarked cluster from an older katamaran release

Katamaran records the cluster's `k3s` release tag in `/var/lib/katamaran/k3s/data/.katamaran-version` and never falls back to `/var/lib/katamaran/k3s/bin/version`, so an unmarked restored datastore can never inherit an unrelated installed binary's version.

Katamaran decides which `k3s` release last opened an existing datastore from three records, using the newest minor among those available:

- `/var/lib/katamaran/k3s/data/.katamaran-version`, written before every launch.
- `/var/lib/katamaran/k3s/data/server/db/.katamaran-k3s-version`, a copy written beside `state.db` before every launch (and, for a new cluster, as soon as `k3s` creates `server/db/`; startup fails if it cannot be written). It travels with any backup of `server/` or `server/db/`.
- The node's `kubeletVersion` in `server/db/state.db`. A checkpointed database is read in place, read-only; a database with uncheckpointed WAL content is read from a private copy (up to 256 MiB) on the guest's `/run` tmpfs, so the live files are never opened for writing. It is a **best-effort** lower bound: when it can be read, data last run by a release that did not update the other two records still refuses a downgrade. When it cannot be read (for example a WAL copy over the limit), the version files alone decide.

A node record alone is not proof of which control plane last opened the datastore: a newer `k3s` server can open the database and stop before its kubelet reports status. So when neither version file exists, katamaran accepts the datastore only if the node record is `v1.37.2+k3s1`, the only build that katamaran releases before version tracking ever ran. Tracking-enabled releases write `.katamaran-version` before launching `k3s` and the stamp before launching it against an existing `server/db/` (for a new cluster, as soon as `k3s` creates it), so a datastore without either file was last opened by a pre-tracking release, unless it was copied without them. This makes a normal upgrade from an older katamaran release automatic. An empty, non-regular, or malformed version file fails closed. Always back up `.katamaran-version` together with `server/`.

If you return to this release after running the same datastore on an older katamaran release (which does not update either version file), check which `k3s` release that older release ran (its pinned build) before choosing `k8s_version`: select that release or one minor newer, or restore a matching backup before selecting an older minor. Do not rely on the node record alone to catch the downgrade.

If the node record shows any other version, or katamaran cannot read it (for example after an incomplete initialization before the node registered, a corrupt or unreadable `state.db`, more than 256 MiB of database and uncheckpointed WAL, or an unmarked backup without a node record), startup fails closed rather than guessing from `/var/lib/katamaran/k3s/bin/version`. To migrate a datastore whose version you **know** independently (do not use this for an unknown backup restore, and never guess the version from whichever `k3s` binary is currently installed):

1. Make sure the VM is running and Kubernetes is stopped:
   - If the VM is already running (for example after upgrading when Kubernetes auto-start refused the unmarked datastore), run `kat k8s stop` if Kubernetes is still active.
   - If the VM is stopped, disable Kubernetes auto-start first (`kat config set kubernetes=off`) so booting does not attempt to start Kubernetes, then start the VM (`kat start`).
2. Independently verify the exact `k3s` release tag that last opened the datastore, for example from the backup's records or your cluster logs from before stopping.
3. Record that exact verified tag from macOS using `kat k8s mark-version`:

   ```sh
   kat k8s mark-version v1.36.4+k3s1
   ```

   `kat k8s mark-version` validates the release tag, requires the VM to be running with Kubernetes stopped and an existing unmarked datastore to be present, writes `/var/lib/katamaran/k3s/data/.katamaran-version` inside the VM, refuses to overwrite any existing marker, refuses when the version is already known (a `server/db/.katamaran-k3s-version` stamp exists, or the node record is `v1.37.2+k3s1`), refuses a lower minor than `state.db` already records, and never infers the version from `/var/lib/katamaran/k3s/bin/version`.
4. Start Kubernetes (`kat k8s start`) with a compatible `k8s_version` (same minor or one minor newer). This also re-enables Kubernetes auto-start — or run `kat k8s reset` if you prefer to start fresh instead of migrating.

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

### amd64-only images

Docker runs `linux/amd64` images under Rosetta (or qemu), but Kubernetes's image service (containerd's CRI) only accepts images for the node's own platform, `linux/arm64`, with no configuration option to add fallback platforms. An image that only has a `linux/amd64` manifest locally would otherwise be invisible to kubelet, which then tries to pull it and fails with `no match for platform in manifest`.

When x86-64 emulation is on (`amd64 = "auto"`, `"rosetta"`, or `"qemu"`), the image sync handles this automatically: if a Docker image has `linux/amd64` locally and no native `linux/arm64` variant locally (including amd64-only images, `docker build --platform linux/amd64`, or `docker pull --platform linux/amd64`), katamaran mirrors it into `k8s.io` with the image config's `architecture` set to `arm64`, so kubelet accepts it. The layers are not changed, so the binaries stay x86-64 and run under emulation, the same as with `docker run` (`uname -m` in the pod prints `x86_64`):

```sh
docker pull --platform linux/amd64 my-amd64-only-app:dev
kat k8s sync-images
kubectl run app --image=my-amd64-only-app:dev --image-pull-policy=IfNotPresent
```

Direct registry pulls by kubelet without mirroring from Docker still require a `linux/arm64` manifest because containerd's CRI cannot be configured to pull `linux/amd64` on an `arm64` node. Only tagged local Docker images (`repo:tag`) are rewritten: digest-pinned references (`repo@sha256:...`) are never rewritten under their original digest because rewriting changes the manifest digest. To run a digest-pinned amd64 image in a pod, tag it locally first (`docker tag my-amd64-app@sha256:... my-amd64-app:dev`), sync, and reference the local tag in your pod spec with `imagePullPolicy: IfNotPresent` (or `Never`). The mirrored copy has a different image ID from the Docker image because its config changed. Images that have a `linux/arm64` variant locally are mirrored unchanged, and kubelet uses the native variant. With `amd64 = "off"`, amd64-only images are not mirrored and the sync reports each one while keeping any existing Kubernetes copy.

katamaran never deletes an image from Kubernetes on its own. It only ever *replaces* an image it published itself (an emulated mirror of the same Docker tag) with a newer one, and every other case leaves the Kubernetes image in place:

- Turning emulation off, or removing an image from Docker, keeps its Kubernetes copy, so running pods keep working.
- If Kubernetes already has a different image under the same name that katamaran did not publish, the sync leaves it untouched and reports the conflict. Examples: one pulled with `ctr` or by kubelet, a native mirror of an older build, or an amd64 copy mirrored unchanged by an earlier katamaran release. To resolve it, give the Docker image a different local name (`docker tag my-app:dev my-app:dev-amd64 && docker rmi my-app:dev`) and use that name in your pod spec.
- If a Docker tag is moved to an image katamaran cannot mirror (no `linux/arm64` or `linux/amd64` variant), Kubernetes keeps the previous build under that tag and the sync reports it.
- If the emulator fails to register at boot, or its `binfmt_misc` handler is later removed or disabled, amd64 images are not mirrored until the next start, and the sync reports each one; native images keep syncing.

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

## Customize `k3s` server flags and config

You can disable bundled `k3s` components (such as Traefik when installing your own ingress controller), pass extra `k3s server` flags, or supply a `k3s` YAML config file:

### Disable bundled components (`k8s_disable`)

```sh
kat config set k8s_disable=traefik
# Disable multiple components:
kat config set k8s_disable=traefik,servicelb
# Re-enable all bundled components:
kat config set k8s_disable=""
```

Each entry is passed to `k3s server` as `--disable=<name>` and stored in `~/.katamaran/config.toml` as `k8s_disable = ["traefik"]`.

### Pass extra `k3s server` flags (`k8s_args`)

Set `k8s_args` in `~/.katamaran/config.toml`:

```toml
k8s_args = [
  "--disable=traefik",
  "--kube-apiserver-arg=feature-gates=Foo=true",
]
```

Or via `kat config set` (space-separated, supporting single/double quotes):

```sh
kat config set k8s_args="--kube-apiserver-arg=audit-log-path=- --debug"
kat config set k8s_args=""
```

### Use a `k3s` config file (`~/.katamaran/k3s-config.yaml`)

If `~/.katamaran/k3s-config.yaml` (or `$KATAMARAN_HOME/k3s-config.yaml`) exists on your Mac, katamaran copies it to `/etc/rancher/k3s/config.yaml` inside the guest before starting `k3s server` (and removes the guest copy if you later delete the host file):

```yaml
disable:
  - traefik
kube-apiserver-arg:
  - "service-node-port-range=30000-32767"
```

Following standard `k3s` precedence, CLI flags (`k8s_disable`, `k8s_args`, and katamaran's defaults) take precedence over keys in `config.yaml`; use `k3s`'s `+` suffix (such as `kubelet-arg+:`) in `k3s-config.yaml` to append to list flags katamaran already passes.

Changes to `k8s_disable`, `k8s_args`, or `k3s-config.yaml` apply the next time Kubernetes starts (no VM restart is needed). If the cluster is already running, run `kat k8s stop && kat k8s start`, or `kat k8s reset && kat k8s start` if you are disabling a component like Traefik whose CRDs were already installed.

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
