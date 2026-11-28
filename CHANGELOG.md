# Changelog

## v0.11.1 - 2026-11-28

This release fixes pause and resume of the VM. The `pause_on_sleep` option now works as expected.

### Fixes

- Fix pause and resume of the VM. Before, the VM ignored the pause and resume signals, so `pause_on_sleep` did nothing and the VM kept running during sleep.

## v0.11.0 - 2026-11-27

This release lets you choose the k3s version, run amd64 images in pods, open a shell in the VM, and grow the data disk. It also fixes file limits and slow container traffic.

### New features

- Choose the k3s version with the `k8s_version` config key. The guest refuses unsafe downgrades of existing cluster data. Use `kat k8s mark-version` for a cluster of unknown version.
- Run amd64-only Docker images in pods under emulation. The guest makes a rewritten copy of the image for Kubernetes and never deletes a tag that it does not own.
- Set custom k3s server flags with `k8s_args`, turn off components with `k8s_disable`, and use a `k3s-config.yaml` file. The guest accepts only settings that are safe for the VM.
- Use `forward_addresses` or `--forward-address` to publish ports on macOS `lo0` aliases, such as `172.16.1.200`. The default is `127.0.0.1`.
- Open a root shell or run a command in the Linux VM with `kat vm shell` and `kat vm exec`.
- Grow the data disk with the `disk_gib` config key.
- Add `--images` to `kat k8s reset` to also remove the cached images in the `k8s.io` namespace. The macOS app has a matching checkbox.

### Fixes

- Containerd, Docker, and all containers now get a higher open-file limit. The new `nofile` key sets it and the default is `1048576`. Elasticsearch and similar databases now start.
- Keep-alive traffic between containers and pods no longer stalls for 40 to 50 ms for each request. A guest kernel patch fixes this. Set `kat.quickack=0` on the kernel command line to turn it off.
- Docker ports that you publish on `127.0.0.1`, for example `docker run -p 127.0.0.1:8080:80`, now work from the Mac.
- The file cache now starts the macOS file event stream only when the guest uses a shared folder. This reduces idle CPU use and macOS permission prompts.

## v0.10.0 - 2026-11-25

This release adds Landlock sandbox support to the guest kernel. Container workloads that use Landlock now work in katamaran.

### New features

- The guest kernel now enables the Landlock LSM. Container workloads that use unprivileged Landlock sandboxing, such as `aube` with `jailBuilds: true`, now work inside katamaran.

## v0.9.1 - 2026-11-24

This release fixes a deadlock in the VM network transport. Large Docker streams over `docker.sock` now work without a hang.

### Fixes

- Fix a deadlock in the virtio-vsock Unix proxy. Before, a command such as `docker export <image> | docker run -i ...` could stop the VM. After the fix, `docker ps` and `docker version` stay responsive, and the guest keeps answering watchdog pings.

## v0.9.0 - 2026-11-24

This release lets you publish privileged ports, such as 80 and 443, without root on macOS. It also shows port forwards that fail.

### New features

- Privileged host ports below 1024 now work without root on macOS. For example, `-p 80:80` and `-p 443:443` now work on `127.0.0.1`. This also applies to `auto_forward` and `kat k8s forward`.
- Failed port forwards now show in `kat status` under `port err`. The daemon status API reports them in `port_errors`. `Katamaran.app` shows a warning and an error tooltip in place of a dead `localhost` link.

## v0.8.0 - 2026-11-23

This release adds compressed swap to the guest VM. The VM now handles memory spikes better and is less likely to stop a process because of low memory.

### New features

- The guest VM now uses an LZ4 `zram` swap device that is the same size as the guest RAM. Heavy workloads, such as large asset builds next to running containers, now cause fewer out-of-memory kills.
- Idle file cache trimming now does not swap out the pages of active containers.

### Other changes

- Add a Kubernetes guide to the documentation.
- The `README.md` and the architecture page now describe `zram` swap in the memory reclaim overview.

## v0.7.0 - 2026-11-22

This release adds automatic update checks. The app now tells you when a new version is ready.

### New features

- Release builds check for updates one time each day. A native dialog shows the new version, also when the app runs only in the menu bar.
- The status bar menu and the app menu show `Update Available (x.y.z)…` when a new version exists. You can open the update from there.
- **Settings → General** has a new **Updates** section on release builds. It has a **Check for updates automatically** toggle and a **Check Now** or **View Update…** button.

## v0.6.1 - 2026-11-22

This release makes upgrades safer. The daemon now restarts after an upgrade, so the CLI and the app do not talk to an old daemon.

### Fixes

- The app runs `katamaran setup --default` again when the installed app version changes. Sparkle, Homebrew cask, and DMG upgrades now reload the `dev.katamaran.daemon` LaunchAgent.
- `kat setup` keeps a running VM active when `launch_at_login = false`. The VM does not stop during an upgrade.
- `kat restart` now restarts a daemon that has a different version. `kat status` and `kat version` show an upgrade hint when the daemon and CLI versions differ.

## v0.6.0 - 2026-11-22

This release adds Kubernetes Pods and Services views to the main window. You can now see and manage your cluster without `kubectl` on the Mac.

### New features

- Add a Kubernetes group to the sidebar with Pods and Services views. Pods show status, readiness, restarts, and age, with followed logs for each container. Services show type, ports, selector, and selected pods. You can delete both kinds of resource.
- Open a LoadBalancer or NodePort service on `localhost` from the Services view. The ports are already forwarded, so the link works at once.
- Show or hide system namespaces with a new option in the More menu. The app hides them by default.
- See the state of the VM or cluster in the Kubernetes views. Use Start, Turn On, or Try Again to fix the problem from the same view.

### Other changes

- The View menu shortcuts now stop at ⌘1 to ⌘9. The Logs section no longer has a shortcut.
- The `status` output now includes `k8s.proxy_socket` while the cluster runs. The app uses a private Unix socket to reach the cluster, and `docs/app.md` explains how to move it with `KATAMARAN_HOME`.

## v0.5.0 - 2026-11-22

This release adds optional single-node Kubernetes to the Katamaran VM. It also makes the daemon, the guest init, the NFS server, and the app more secure and more reliable.

### New features

- Add optional single-node Kubernetes (k3s) inside the Katamaran VM. It is off by default and uses no resources when it is off. Use `kat k8s start|stop`, `kat config set kubernetes=on|off`, or Settings to turn it on or off without a VM restart.
- The Kubernetes cluster uses the existing `containerd` and mirrors images from `docker build` into the cluster, so pods run local images without a push. The `kat k8s reset` command wipes the cluster state.
- The host gets the Kubernetes API, a `katamaran` context in `~/.kube/config`, automatic forwards for `NodePort` and `LoadBalancer` services, and DNS for `*.k8s.kat.local`. Use `kat k8s forward` for direct Pod and Service IP access.
- Add a Kubernetes tab to the app Settings. It has an enable switch, the cluster status, Sync Images, and Reset Cluster.

### Fixes

- Harden the NFS server against path escape, symlink races, and incorrect directory listings. This protects the files in your shared folders.
- Make the VM pause and resume signals and the file event cache in `kat-vmm` safer. Lost file events now invalidate the cache correctly.
- Fix memory safety problems in the terminal library `kat-term`.
- Harden the app against shell command injection in machine shell commands. Fix HTTP response handling, reconnect problems, and stuck Start and Restart states in the UI.
- Make the daemon more reliable. SSH proxy channels no longer block, the VM handshake has a deadline, and the NFS mount and unmount steps are safer. Other fixes cover config saves, the setup restart, and image downloads.
- Make the guest init more reliable. Fixes cover limits on connections, retries after accept errors, process ID reuse, stuck pipes, and checks on protocol frame sizes.

### Other changes

- Replace all em-dashes in the app and the documentation with normal dashes.
- Document the Kubernetes feature in the README, the agent skill, and the documentation site.
- Fix the release scripts and the build so they work on Linux runners, and escape special characters in `Info.plist` values.
- Update the `russh` SSH library to 0.65.4. This includes upstream security fixes.

## v0.4.0 - 2026-11-20

This release makes it easier to get Katamaran. It also fixes the `binfmt_misc` mask in `kat-init`.

### New features

- The site `katamaran.rs` now has a **Download for macOS** button on the landing page. The `/download/` page and the install page link directly to the latest `Katamaran.dmg`.

### Fixes

- `kat-init` now matches `EI_DATA` and `EI_VERSION` in the `binfmt_misc` mask. This makes the ELF binary check more exact.

### Other changes

- The documentation site now loads from the root of `katamaran.rs`.
- The `toml` crate is now at version 1.2.1.

## v0.3.0 - 2026-11-20

This release adds a guide for coding agents. An agent can now learn how to use katamaran with the `kat skill` command.

### New features

- Add the `kat skill` command. Use `kat skill show` to print a guide for coding agents. The guide covers VM use, Docker, networking, and more.
- Add `kat skill setup`, `kat skill status`, and `kat skill remove` to manage the skill for Claude Code, Codex, OpenCode, and Pi. The installed skill is a small stub that calls `kat skill show`, so the guide stays current when you upgrade katamaran.

## v0.2.0 - 2026-11-20

This release makes `katamaran` the default Docker context when you start the app. It also updates the documentation site with screenshots and a new design.

### New features

- `Katamaran.app` now sets `katamaran` as the default Docker context on launch. The first launch runs `katamaran setup --default`, and later launches run `katamaran use-context`.
- `kat start` now makes sure the `katamaran` Docker context is active. You do not need to switch the context by hand.

### Fixes

- `katamaran` now finds `docker` in the standard macOS install locations. The Docker context steps now work when you open the app from Finder, the Dock, or Spotlight.

### Other changes

- The README and the documentation site now show screenshots of the macOS app. The install, quickstart, and Docker pages describe the automatic default Docker context.
- The documentation site has a new design. It uses the Allo and JetBrains Mono fonts, a midnight-indigo color palette, and Tokyo Night code highlighting.
- The documentation site moves to the `katamaran.rs` domain. The default Sparkle feed URL is now `https://katamaran.rs/appcast.xml`.

## v0.1.1 - 2026-11-20

This release explains how to install katamaran with Homebrew and removes build warnings from the libkrun code.

### Other changes

- Add instructions for the Homebrew cask install to the README and the docs. Users can now find the install steps in one place.
- Remove the deprecated `libc` `VM_MEMORY_APPLICATION_SPECIFIC_1` warnings in the libkrun `vstate` code. The build output is now clean.

## v0.1.0 - 2026-11-20

This is the first release of katamaran. It gives you the first version of the project and its documentation site.

### New features

- Add the first version of katamaran, a macOS app that runs virtual machines and containers.

### Other changes

- Set up the documentation site to work from the `/katamaran` path on GitHub Pages. The links and files on the site now load correctly.
