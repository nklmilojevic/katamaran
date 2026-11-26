# Using katamaran

`kat` and `katamaran` are the same binary. Katamaran runs Docker (`containerd` and `dockerd`) and LXC Linux machines inside one lightweight Linux VM on Apple silicon Macs. It also includes an optional single-node Kubernetes (`k3s`) cluster (`kat k8s`) that stays off by default with zero overhead.

## VM lifecycle

```sh
kat status                # VM state, CPU/memory, Docker socket, mount, ports
kat status --json         # Machine-readable status
kat start                 # Start daemon and VM, wait until Docker is ready
kat stop                  # Stop VM (daemon stays running)
kat restart               # Restart VM and apply pending config changes
```

`kat start` and launching `Katamaran.app` set the active Docker context to `katamaran` automatically (for the default `~/.katamaran` home).

## Docker and networking

- Default Docker context: `katamaran` (`unix://$HOME/.katamaran/run/docker.sock`).
- Tools that ignore Docker contexts can set `DOCKER_HOST="unix://$HOME/.katamaran/run/docker.sock"`.
- Both published (`-p`) and unpublished container ports forward to `127.0.0.1` on macOS when `auto_forward` is enabled (the default).
- Containers reach services on macOS via `host.docker.internal`.
- Host directories under `/Users` are bind-mounted at the same path inside containers with FSEvents-driven inotify and cache invalidation.
- `linux/amd64` images run under Apple Rosetta with per-task hardware TSO by default (`amd64 = "auto"`).

## Isolated throwaway VMs

Set `KATAMARAN_HOME` to a temporary directory to boot a completely separate VM without touching `~/.katamaran`, the user's Docker context, or `~/Katamaran`:

```sh
KATAMARAN_HOME=/tmp/kat-test kat start
DOCKER_HOST="unix:///tmp/kat-test/run/docker.sock" docker run --rm alpine true
KATAMARAN_HOME=/tmp/kat-test kat stop
```

## Linux machines (LXC)

Run full Linux distributions inside the same VM (`ubuntu`, `debian`, `alpine`, `fedora`, `arch`, or any image from `images.linuxcontainers.org`):

```sh
kat machine create dev                 # Ubuntu by default
kat machine create box --distro alpine
kat machine list
kat shell dev                          # Interactive shell
kat shell dev -- uname -a              # Run a single command
kat ssh dev                            # Connect over SSH proxy
kat machine stop dev
kat machine delete dev
```

## Kubernetes (optional)

Manage the optional single-node `k3s` cluster inside the VM (off by default, zero overhead when stopped; merges the `katamaran` context into `~/.kube/config`):

```sh
kat k8s start                             # Enable and start k3s (downloads on first use)
kat k8s status [--json]                   # Cluster state, API endpoint, context, DNS
kat k8s sync-images                       # Mirror local Docker images into k3s (amd64-only images require emulation enabled)
kat k8s kubeconfig [--path]               # Print kubeconfig YAML or file path
kat k8s dns status|install|uninstall      # Manage *.k8s.kat.local DNS on macOS
kat k8s forward <ip>:<port> [--local P]   # Forward a local TCP port to a Pod/Service IP
kat k8s stop                              # Stop k3s and disable auto-start
kat k8s reset                             # Stop k3s and wipe cluster state and volumes
kat k8s mark-version <version>            # Record verified k3s tag for an unmarked datastore
```

## Configuration

Settings live in `~/.katamaran/config.toml` and take effect on the next VM start (`kat restart`), except `launch_at_login` and `kubernetes`, which apply immediately, and `k8s_version` / `k8s_disable` / `k8s_args` (plus optional `~/.katamaran/k3s-config.yaml`), which apply when Kubernetes starts.

```sh
kat config get
kat config set cpus=8 memory=12G pause_on_sleep=on
kat config set k8s_disable=traefik
kat restart
```

Keys: `cpus`, `memory_mib` (`memory=8G`), `amd64` (`auto|rosetta|qemu|off`), `auto_forward`, `forward_addresses` (`127.0.0.1,172.16.1.200`; replaces the `127.0.0.1` default for TCP forwards; aliases must be on `lo0`, empty resets), `mount`, `mount_path`, `launch_at_login`, `pause_on_sleep`, `kubernetes`, `k8s_version` (`v1.31+` k3s release tag such as `v1.32.5+k3s1`, `default` or empty via `config set` / omit in `config.toml` for the pinned build; applies at the next Kubernetes start), `k8s_disable`, `k8s_args`, `nofile` (open-files limit for containers, default `1048576`).

## Files in Finder

When `mount` is enabled, `~/Katamaran` exposes Linux files over NFS:

- `~/Katamaran/docker/volumes/<name>` - named Docker volumes
- `~/Katamaran/docker/containers/<name>` - running container filesystems
- `~/Katamaran/machines/<name>` - Linux machine filesystems

## Logs and troubleshooting

```sh
kat logs                  # Daemon log
kat logs -f               # Follow daemon log
kat logs --console        # Linux guest console log
kat logs --source vm      # VMM (katamaran-vm) log
kat logs --source gvproxy # Networking log
```

If `docker` cannot reach katamaran, check `kat status`, `docker context ls`, and whether `DOCKER_HOST` is overriding the active context.
