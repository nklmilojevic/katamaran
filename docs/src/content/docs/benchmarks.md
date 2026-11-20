---
title: Benchmarks
description: Measured performance numbers for katamaran on Apple M3 Max and instructions for running the benchmark suite.
---

These measurements come from run `20261120c` on a Mac15,10 with an Apple M3 Max (14 cores, 10 performance and 4 efficiency, 36 GiB RAM, macOS 27.2.1). The katamaran VM used 14 vCPUs, a 16 GiB memory ceiling, kernel 6.18.64, Docker 29.10.0, and an Alpine 3.24 rootfs. Every number is the median of 5 timed runs unless noted.

## Bind-mount file sharing (macOS to Linux)

| Workload | Median |
|---|---|
| `stat()` on a 91,000-file Linux tree (warm) | 346 ns per file |
| `stat()` on a 91,000-file Linux tree (guest caches dropped) | 6.44 µs per file |
| `open()` and `close()` (warm) | 24.5 µs per pair |
| `find . -type f` over `node_modules` (33,598 files) | 0.113 s |
| `git status` on the Linux tree (warm) | 0.166 s |
| `git status` on the Linux tree (guest caches dropped) | 6.43 s |
| `git status` in a new container right after cold pass | 0.176 s |
| `tar -xf` of `linux-6.18.tar.xz` onto a bind mount | 13.8 s |
| `rm -rf` of the extracted Linux tree on a bind mount | 5.71 s |
| `npm ci` on video.js (warm cache) | 3.23 s |
| `pnpm install` on video.js (warm store) | 2.32 s |
| `cargo build` on ripgrep (debug, offline) | 3.51 s |
| Host file write to container inotify event | 13.6 ms |
| Inotify events lost in a 10,000-file burst | 0 |
| `fio` sequential read, 1 MiB blocks | 4,092 MB/s |
| `fio` sequential write, 1 MiB blocks | 2,006 MB/s |
| `fio` random read, 4 KiB blocks | 1,098 MB/s (267,965 IOPS) |
| `fio` random write, 4 KiB blocks | 747 MB/s (182,430 IOPS) |
| `fio` fsync write, 4 KiB blocks | 26.9 MB/s (6,572 IOPS) |

## Application and database workloads

Measured with a Laravel 12 and Vite application on a bind mount plus PostgreSQL 17 and Redis 7 containers:

| Workload | Median |
|---|---|
| `composer install` (warm cache) | 3.54 s |
| `bun install` (warm cache) | 3.95 s |
| `vite build` | 6.26 s |
| `php artisan migrate:fresh` (Postgres 17) | 0.772 s |
| `php artisan db:seed` total (12,000 inserts and 3,000 row-lock transactions) | 7.59 s |
| Laravel `GET /login`, opcache off | 155 ms per request |
| Laravel `GET /login`, opcache on | 26.3 ms per request |
| 10 parallel unzips of the Composer cache | 0.981 s |
| Container to container Redis PING round trip (`redis-benchmark -c 1 -n 20000`) | 6.15 µs |

## Networking

| Workload | Median |
|---|---|
| Container downloads 1 GiB from macOS (`host.docker.internal`) | 4,440 MiB/s |
| Container uploads 1 GiB to macOS | 5,698 MiB/s |
| macOS downloads 1 GiB from a published port (`-p`) | 5,131 MiB/s |
| HTTP to macOS, keep-alive p50 and p99 | 95.6 µs (p99 151 µs) |
| HTTP to macOS, new connection p50 and p99 | 334 µs (p99 651 µs) |
| DNS `getaddrinfo` for one cached name | 91.5 µs |
| DNS `getaddrinfo` for 8 different hosts | 0.56 ms |

## Linux to macOS files (`~/Katamaran` over NFS)

| Workload | Median |
|---|---|
| Read a 100 MiB file on macOS | 981 MiB/s |
| Write a 100 MiB file from macOS | 1,090 MiB/s |
| Read 10,000 files of 4 KiB (`cat`) | 3.55 s |
| `ls -R` over a 21,000-file tree | 4.08 s |
| `cp -R` of a 21,000-file tree to macOS | 12.8 s |

## x86-64 under Rosetta

| Workload (`linux/amd64`) | Median |
|---|---|
| 200 executions of `/bin/true` | 495 ms |
| Python `sum(i*i)` over 10,000,000 integers | 849 ms |
| Node loop of 300,000,000 iterations | 1,179 ms |
| `gcc -O2` compiling 3,000 functions | 20.2 s |
| 10 executions of `node -e 0` | 3.30 s |

## Lifecycle and memory footprint

Physical footprint is measured from `vmmap -summary` on the VM processes.

| Measurement | Median |
|---|---|
| Cold start to `docker ready` | 668 ms |
| Cold start to first `docker run --rm alpine true` | 967 ms |
| Stop VM (`SIGINT` to process exit) | 120 ms |
| `docker run --rm alpine true` (10 runs) | 302 ms |
| Idle footprint on fresh VM, no containers | 922 MiB |
| Idle footprint after dropping guest caches (fresh VM) | 895 MiB |
| Idle CPU, no containers (5 windows of 30 s) | 1.13 % to 1.47 % of one core |
| Peak footprint with a 4 GiB container running | 5,636 MiB |
| Footprint 10 s after the 4 GiB container exits | 2,769 MiB |
| Footprint 60 s after the 4 GiB container exits | 1,540 MiB |

## Run benchmarks on your Mac

The benchmark tool in `bench/` uses `uv` and Python 3. Prepare the datasets once, then run all or selected cases against your `katamaran` context:

```sh
cd bench
uv run bench prepare --engine katamaran
uv run bench run --engine katamaran --locations bind --runs 5
```

List available cases:

```sh
uv run bench list
```

Measure network throughput and HTTP latency directly:

```sh
bash scripts/netperf.sh katamaran
```
