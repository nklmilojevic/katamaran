---
title: Architecture
description: How katamaran combines libkrun, direct FUSE hypercalls, FSEvents invalidation, in-process gvproxy, and 64 KiB memory reclaim.
---

katamaran is designed around one constraint: every extra process boundary, socket hop, and thread handoff between a Linux container and macOS shows up directly in developer workloads with tens of thousands of small files or short network round trips.

## Process layout

On macOS, two processes run for an active VM:

1. `katamaran daemon` owns lifecycle management, configuration, the macOS sleep watcher, the NFS mount lifecycle, and the local control API (`~/.katamaran/run/daemon.sock`).
2. `katamaran-vm` links `libkrun` directly on Apple's Hypervisor.framework (`HVF`) and loads `libgvproxy.dylib` in-process. Separating the VM into its own process keeps Hypervisor entitlements isolated and allows clean VM restarts without dropping the daemon or app connection.

Inside the Linux guest, `kat-init` runs as PID 1 from an uncompressed `initramfs.cpio`, mounts the read-only EROFS system image (`/dev/vda`) under a tmpfs overlay, mounts the three btrfs subvolumes of `data.img` (`/dev/vdb`), configures networking statically from `kat.net=`, and supervises `containerd`, `dockerd`, `kat-nfs`, LXC machines, and the memory reclaim controller.

## Direct virtio-fs hypercalls and APFS backend

Standard virtio-fs routes every FUSE request through a virtqueue descriptor ring, wakes a host worker thread, and injects a completion interrupt into the guest vCPU. That round trip dominates small metadata operations (`stat`, `open`, `readdir`, `unlink`).

katamaran changes both the transport and the macOS server:

- **Direct hypercalls (`KAT_FS_DIRECT`)**: Small, bounded foreground FUSE requests pass their guest-physical buffer table directly in an ARM SMCCC vendor call. The vCPU thread handles the request inline and returns the reply length without touching virtqueues or injecting an interrupt. Requests that can block (such as iCloud dataless files, FIFOs, network volumes, or oversized I/O) fall back to the asynchronous worker pool, enforced by `SyncPolicy` and macOS `setiopolicy_np` bounded opens.
- **APFS-tuned operations**: Directory listings use macOS `getattrlistbulk(2)`. Plain `FUSE_READDIR` requests only `ATTR_CMN_NAME`, `ATTR_CMN_OBJTYPE`, and `ATTR_CMN_FILEID`, which APFS serves from inline directory records without reading child inodes. Frequently used parent directories hold `O_EVTONLY` pinned descriptors (`PinPool`) so lookups, opens, and unlinks use `*at()` syscalls instead of `/.vol/<dev>/<ino>` volfs path resolution. `OpenTracker` skips `fstatat` on `unlink` and `rmdir` when no open handle can refer to the target.
- **Long caching with FSEvents invalidation**: Shares cache entries, attributes, negative lookups, unchanged file pages (`FOPEN_KEEP_CACHE`), and unchanged directory listings (`FOPEN_CACHE_DIR | FOPEN_KEEP_CACHE`). A single macOS FSEvents stream watches shared trees and sends targeted FUSE node ID invalidations over virtio-vsock to `/dev/kat_fsnotify`, dropping stale guest caches and emitting Linux inotify events.
- **Caller ownership (`owner_caller`)**: The host reports a fixed owner so attributes remain cacheable, while the guest FUSE driver substitutes `current_fsuid()` and `current_fsgid()` on `getattr`, checks `access(2)` locally against cached mode bits, and skips no-op `chmod` and `chown` `FUSE_SETATTR` calls.

## In-process networking

`katamaran-vm` loads `libgvproxy.dylib` (gvisor-tap-vsock built with `-buildmode=c-shared`) into the VM process.

- Small outbound guest frames (`<= 4 KiB`) are transmitted inline on the vCPU thread through `kat_gvproxy_tx` without waking a virtio-net worker thread.
- Small inbound frames (`<= 2 KiB`) copy headers and payload into a stack buffer and call `inproc_net_rx` directly without `runtime.Pinner` or slice allocations, while large frames (up to the 65,520-byte MTU) stay zero-copy with partial checksum offload (`VIRTIO_NET_F_GUEST_CSUM`).
- TCP connections in the forwarder register persistent gVisor `waiter.Entry` objects and use 8 MiB macOS socket buffers with copy buffers that start at 64 KiB and grow to 1 MiB for bulk transfers.
- DNS queries go through macOS `dns_sd` with answer caching and in-flight deduplication.
- Inside the Linux guest, `kat-init` disables `bridge-nf-call-iptables` so container-to-container L2 bridge packets bypass IP netfilter hooks, and the guest kernel builds its arm64 vDSO with `--hash-style=both` so both glibc and musl containers resolve `clock_gettime` without syscalls.

## Adaptive idle polling and QoS

Waking a halted vCPU from WFI on Hypervisor.framework costs tens of microseconds of host thread wakeup latency. The guest kernel defines `TIF_POLLING_NRFLAG` on arm64 and polls adaptively before halting (growing up to 200 µs during busy request-response traffic and shrinking to 0 µs when idle), keeping cross-vCPU container traffic fast without burning idle CPU. Active vCPU, network, and filesystem worker threads run at `QOS_CLASS_USER_INTERACTIVE` so macOS keeps them on performance cores under background host load.

## 64 KiB memory reclaim

On macOS, `madvise(MADV_FREE)` does not immediately remove dirty hypervisor-backed pages from a process's physical footprint. To give memory back to macOS reliably:

1. At boot, `libkrun` maps guest RAM into separate 64 KiB XNU VM map entries by mapping the untouched region once with Tag 1 and overwriting only the odd 64 KiB blocks with Tag 2. Each block gets its own VM object on first fault.
2. In the guest, `kat-init` sets `page_reporting_order = 4` (64 KiB) on the virtio-balloon driver, configures an LZ4-compressed `/dev/zram0` swap device sized to guest RAM (with page discard enabled so freed swap slots release memory immediately), trims idle file and reclaimable slab caches toward a 128 MiB budget through cgroup v2 `memory.reclaim` (`swappiness=0`), and runs `compact_memory` after fragmentation exceeds 64 MiB.
3. When the balloon reports free 64 KiB blocks, `libkrun` unmaps them from Hypervisor.framework, replaces them with fresh anonymous `mmap(MAP_FIXED)` blocks (immediately freeing the old VM objects and lowering macOS physical footprint), and maps them back into the guest.
