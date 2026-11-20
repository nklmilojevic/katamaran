# Third-party notices

katamaran is licensed under the GNU Affero General Public License, version 3 or later (see [LICENSE](LICENSE)).
It includes and ships parts that have their own licenses.
Those licenses continue to apply to those parts.

## Parts in this repository

| Part | Where | License |
|---|---|---|
| libkrun, with katamaran's patches | `vendor/libkrun`, `vendor/patches` | Apache-2.0 (see `vendor/libkrun/LICENSE`) |
| gvproxy (gvisor-tap-vsock) patches | `vendor/patches/gvproxy` | Apache-2.0 |
| Linux kernel patches | `guest/kernel/patches` | GPL-2.0 |

## Parts that the build downloads and ships

| Part | Version | License | Shipped as |
|---|---|---|---|
| Linux kernel | 6.18.64 (see `guest/kernel/build.sh`) | GPL-2.0 | the guest kernel image |
| gvisor-tap-vsock (gvproxy) | v0.9.0 with katamaran's patches | Apache-2.0 | `gvproxy`, `libgvproxy.dylib` |
| gVisor network stack (used by gvproxy) | see gvproxy's go.mod | Apache-2.0 | in `gvproxy`, `libgvproxy.dylib` |
| libghostty-vt (Ghostty terminal engine), through ghostty-vt-rs-sys 0.3.0 | 0.3.0 | MIT | in the Katamaran app |
| Alpine Linux packages | 3.24.4 (see `guest/rootfs/build.sh`) | each package's license (GPL-2.0, MIT, BSD and others) | the guest system image |
| Docker Engine, containerd, runc, docker-proxy | 29.10.0 static binaries | Apache-2.0 | the guest system image |
| QEMU user mode (qemu-x86_64) | 11.0.3 (Alpine package) | GPL-2.0 | the guest system image |
| LXC | Alpine package | LGPL-2.1 | the guest system image |
| Rust crates | see `Cargo.lock` | each crate's license (mostly MIT or Apache-2.0) | in the katamaran binaries |

katamaran does not ship Rosetta.
When you turn on Rosetta, katamaran uses the copy that macOS installed on your Mac.

## Source code for GPL parts

The guest kernel and some guest packages are licensed under the GPL.
For each release, the source of the kernel (with the patches in `guest/kernel/patches`) is available with the release on GitHub, or from the exact upstream tree named in `guest/kernel/build.sh`.
The source of the Alpine packages is available from the Alpine Linux project for the version named above.
