# katamaran vs OrbStack, 2026-11-20 (run 20261120c)

Run id `20261120c`, 16:50–18:16 CET. Raw data: [`2026-11-20-vs-20261120c-orbstack.json`](2026-11-20-vs-20261120c-orbstack.json), [`2026-11-20-vs-20261120c-katamaran.json`](2026-11-20-vs-20261120c-katamaran.json).

## Machine

| | |
|---|---|
| Mac | Mac15,10, Apple M3 Max, 14 cores (10 P + 4 E), 36 GiB |
| macOS | 27.2.1 (26B131) |
| OrbStack | 2.2.5 (2020500), commit bf842321f95f (v2.2.5); Docker 29.8.2; kernel 7.2.12-orbstack-00394-gbecdd6130068; VM 14 CPUs, 15.66 GiB |
| katamaran | 0.1.0, build of 2026-11-20 from the working tree; Docker 29.10.0; kernel 6.18.64; Alpine 3.24 rootfs; `katamaran run --cpus 14 --memory-mib 16384 --mount ~/Katamaran-bench`, own instance (`KATAMARAN_HOME=~/.katamaran-bench`, context `katamaran-bench`); VM 14 CPUs, 15.68 GiB |
| katamaran SHA-256 | katamaran 590e2bc91c3a…, katamaran-vm 27a5cc854458…, Image 3010b20f2c65…, rootfs.erofs cf944ec8b20f…, initramfs.cpio 4b6b0162e8db…, libgvproxy.dylib ebd0d9b821f2… |

## Method

- Both engines run side by side at the same time; every case runs on both with 0 errors across all 82 records per engine.
- Unless stated otherwise: 1 untimed warm-up, then 5 timed runs per engine, alternating engines run by run. Medians of 5. `docker run --rm alpine true` uses 2 warm-ups and 10 runs. NFS uses 6 rounds and drops round 1. Cold start uses 6 rounds and drops round 0.
- Bind-mount cases (block `fs`) use the bench harness (`uv run bench run --engine <ctx> --locations bind --runs 5`), one call per engine and case, with its own warm-up; the engine order flips from case to case.
- Identical images on both engines (`katbench:f4787c2b1b1e`, `katbench-php:1`, `katbench-amd64:1`, `alpine:3.24.4`, `oven/bun:1.3`, `postgres:17-alpine`, `redis:7-alpine`) and identical harness cache volumes (`bench prepare`).
- "better" marks the lower value, or the higher one for ↑ units. It reads "≈ tie" within ±5 % and "(ranges overlap)" when the min–max ranges overlap. Spreads are min–max (p95) over the timed runs.

### 1. File sharing on bind mounts (Mac → Linux)

| case | unit | OrbStack median | katamaran median | kat/Orb | better | OrbStack spread | katamaran spread |
|---|---|---:|---:|---:|---|---|---|
| stat(), 91k-file tree, warm | ns/op | 451 | 346 | 0.77 | katamaran | 422–454 (p95 454) | 340–356 (p95 356) |
| open()+close(), warm | ns/op | 23,385 | 24,518 | 1.05 | ≈ tie | 23,341–23,449 (p95 23,449) | 23,976–24,912 (p95 24,912) |
| readdir: find node_modules (33,598 files) | s | 0.222 | 0.113 | 0.51 | katamaran | 0.218–0.224 (p95 0.224) | 0.112–0.118 (p95 0.118) |
| git status, Linux tree, warm | s | 0.407 | 0.166 | 0.41 | katamaran | 0.404–0.509 (p95 0.509) | 0.159–0.167 (p95 0.167) |
| untar linux-6.18 (xz) onto the share | s | 13.5 | 13.8 | 1.02 | ≈ tie | 13.3–13.9 (p95 13.9) | 13.3–14.4 (p95 14.4) |
| rm -rf of that tree | s | 5.12 | 5.71 | 1.12 | OrbStack | 4.99–5.22 (p95 5.22) | 5.68–6.35 (p95 6.35) |
| npm ci (video.js, warm cache) | s | 3.79 | 3.23 | 0.85 | katamaran | 3.72–3.89 (p95 3.89) | 3.21–3.29 (p95 3.29) |
| pnpm install (video.js, warm store) | s | 2.43 | 2.32 | 0.96 | ≈ tie | 2.38–2.51 (p95 2.51) | 2.31–2.34 (p95 2.34) |
| cargo build (ripgrep, debug) | s | 3.49 | 3.51 | 1.00 | ≈ tie | 3.36–3.56 (p95 3.56) | 3.50–3.54 (p95 3.54) |
| inotify latency, Mac write → container event | ms | 75.0 | 13.6 | 0.18 | katamaran (ranges overlap) | 69.8–82.9 (p95 81.3) | 2.18–1,931 (p95 1,426) |
| inotify events lost in a 10k-file burst | events | 0.000 | 0.000 | – | – | 0.000–0.000 (p95 0.000) | 0.000–0.000 (p95 0.000) |
| inotify events missed (2 s timeout) | events | 0.000 | 1.00 | – | – | 0.000–0.000 (p95 0.000) | 1.00–1.00 (p95 1.00) |
| fio seq_read_1m_mb_s | MB/s ↑ | 4,029 | 4,092 | 1.02 | ≈ tie | 4,018–4,046 (p95 4,046) | 4,080–4,112 (p95 4,112) |
| fio seq_read_1m_iops | IOPS ↑ | 3,842 | 3,902 | 1.02 | ≈ tie | 3,832–3,858 (p95 3,858) | 3,891–3,922 (p95 3,922) |
| fio seq_write_1m_mb_s | MB/s ↑ | 2,291 | 2,006 | 0.88 | OrbStack (ranges overlap) | 1,337–4,516 (p95 4,516) | 1,248–4,226 (p95 4,226) |
| fio seq_write_1m_iops | IOPS ↑ | 2,185 | 1,914 | 0.88 | OrbStack (ranges overlap) | 1,275–4,307 (p95 4,307) | 1,191–4,030 (p95 4,030) |
| fio rand_read_4k_mb_s | MB/s ↑ | 1,088 | 1,098 | 1.01 | ≈ tie | 1,071–1,093 (p95 1,093) | 1,093–1,106 (p95 1,106) |
| fio rand_read_4k_iops | IOPS ↑ | 265,671 | 267,965 | 1.01 | ≈ tie | 261,519–266,806 (p95 266,806) | 266,802–270,132 (p95 270,132) |
| fio rand_write_4k_mb_s | MB/s ↑ | 775 | 747 | 0.96 | ≈ tie | 759–783 (p95 783) | 741–750 (p95 750) |
| fio rand_write_4k_iops | IOPS ↑ | 189,313 | 182,430 | 0.96 | ≈ tie | 185,219–191,098 (p95 191,098) | 180,951–183,025 (p95 183,025) |
| fio fsync_write_4k_mb_s | MB/s ↑ | 27.4 | 26.9 | 0.98 | ≈ tie | 18.6–27.6 (p95 27.6) | 25.9–27.2 (p95 27.2) |
| fio fsync_write_4k_iops | IOPS ↑ | 6,692 | 6,572 | 0.98 | ≈ tie | 4,535–6,730 (p95 6,730) | 6,321–6,632 (p95 6,632) |
| stat(), 91k files, guest caches dropped | µs/file | 6.75 | 6.44 | 0.95 | ≈ tie | 6.45–7.32 (p95 7.32) | 6.33–6.49 (p95 6.49) |
| stat(), 91k files, second pass | µs/file | 0.633 | 2.65 | 4.19 | OrbStack (ranges overlap) | 0.450–5.83 (p95 5.83) | 0.392–4.85 (p95 4.85) |
| git status, Linux tree, guest caches dropped | ms | 6,294 | 6,432 | 1.02 | ≈ tie | 6,254–6,357 (p95 6,357) | 6,386–6,843 (p95 6,843) |
| git status right after (new container) | ms | 507 | 176 | 0.35 | katamaran | 504–508 (p95 508) | 174–181 (p95 181) |

### 2. Dev workloads (Monica: Laravel 12 + Vite, project on a bind mount, uid 24128)

| case | unit | OrbStack median | katamaran median | kat/Orb | better | OrbStack spread | katamaran spread |
|---|---|---:|---:|---:|---|---|---|
| composer install (warm cache) | ms | 3,677 | 3,544 | 0.96 | ≈ tie | 3,603–3,765 (p95 3,765) | 3,487–3,566 (p95 3,566) |
| bun install (warm cache) | ms | 3,720 | 3,949 | 1.06 | OrbStack (ranges overlap) | 3,471–3,878 (p95 3,878) | 3,670–3,979 (p95 3,979) |
| vite build | ms | 6,293 | 6,260 | 0.99 | ≈ tie | 6,181–6,556 (p95 6,556) | 6,166–6,498 (p95 6,498) |
| artisan migrate:fresh (Postgres 17) | ms | 729 | 772 | 1.06 | OrbStack (ranges overlap) | 648–954 (p95 954) | 767–817 (p95 817) |
| db:seed total | ms | 27,493 | 7,592 | 0.28 | katamaran | 25,166–28,218 (p95 28,218) | 7,351–8,913 (p95 8,913) |
|   12k Eloquent factory inserts | ms | 20,644 | 5,905 | 0.29 | katamaran | 17,909–21,859 (p95 21,859) | 5,594–6,806 (p95 6,806) |
|   3000 row-lock transactions | ms | 6,695 | 1,647 | 0.25 | katamaran | 6,214–7,105 (p95 7,105) | 1,533–1,951 (p95 1,951) |
| GET /login, opcache off (per request) | ms | 157 | 155 | 0.99 | ≈ tie | 157–159 (p95 159) | 155–156 (p95 156) |
| GET /login, opcache on (per request) | ms | 27.8 | 26.3 | 0.95 | katamaran | 27.5–28.4 (p95 28.4) | 26.1–26.6 (p95 26.6) |
| 10 parallel unzips of the composer cache | ms | 1,091 | 981 | 0.90 | katamaran | 1,083–1,111 (p95 1,111) | 974–991 (p95 991) |
| container → container Redis PING round trip | µs | 6.05 | 6.15 | 1.02 | ≈ tie | 6.00–6.15 (p95 6.15) | 6.10–6.60 (p95 6.60) |

### 3. Networking

| case | unit | OrbStack median | katamaran median | kat/Orb | better | OrbStack spread | katamaran spread |
|---|---|---:|---:|---:|---|---|---|
| container downloads from the Mac (1 GiB) | MiB/s ↑ | 4,101 | 4,440 | 1.08 | katamaran (ranges overlap) | 3,862–4,191 (p95 4,191) | 3,913–4,488 (p95 4,488) |
| container uploads to the Mac (1 GiB) | MiB/s ↑ | 4,920 | 5,698 | 1.16 | katamaran | 4,829–5,133 (p95 5,133) | 5,601–6,138 (p95 6,138) |
| Mac downloads from a published port (1 GiB) | MiB/s ↑ | 5,096 | 5,131 | 1.01 | ≈ tie | 4,673–5,381 (p95 5,381) | 5,077–5,213 (p95 5,213) |
| HTTP to the Mac, keep-alive, p50 | µs | 87.6 | 95.6 | 1.09 | OrbStack | 86.6–89.9 (p95 89.9) | 90.7–98.3 (p95 98.3) |
| HTTP to the Mac, keep-alive, p99 | µs | 149 | 151 | 1.02 | ≈ tie | 140–164 (p95 164) | 146–154 (p95 154) |
| HTTP to the Mac, new connection, p50 | µs | 377 | 334 | 0.88 | katamaran | 370–385 (p95 385) | 319–336 (p95 336) |
| HTTP to the Mac, new connection, p99 | µs | 758 | 651 | 0.86 | katamaran | 678–878 (p95 878) | 628–675 (p95 675) |
| DNS getaddrinfo, same name (cached), median | µs | 915 | 91.5 | 0.10 | katamaran | 814–1,039 (p95 1,039) | 88.0–103 (p95 103) |
| DNS getaddrinfo, 8 different hosts | ms | 8.57 | 0.560 | 0.07 | katamaran | 8.06–9.97 (p95 9.97) | 0.500–0.610 (p95 0.610) |

### 4. Linux → Mac files (~/Katamaran-bench vs ~/OrbStack, NFS)

| case | unit | OrbStack median | katamaran median | kat/Orb | better | OrbStack spread | katamaran spread |
|---|---|---:|---:|---:|---|---|---|
| read 100 MiB file on the Mac | MiB/s ↑ | 1,261 | 981 | 0.78 | OrbStack (ranges overlap) | 1,062–1,626 (p95 1,626) | 838–1,712 (p95 1,712) |
| write 100 MiB file from the Mac | MiB/s ↑ | 1,064 | 1,090 | 1.03 | ≈ tie | 902–1,508 (p95 1,508) | 775–1,385 (p95 1,385) |
| read 10k × 4 KiB files (cat) | ms | 4,562 | 3,554 | 0.78 | katamaran | 3,917–4,705 (p95 4,705) | 3,169–3,733 (p95 3,733) |
| ls -R, node_modules-like tree (21k files) | ms | 3,596 | 4,081 | 1.13 | OrbStack (ranges overlap) | 2,628–3,657 (p95 3,657) | 3,407–4,119 (p95 4,119) |
| cp -R that tree to the Mac | ms | 14,291 | 12,840 | 0.90 | katamaran (ranges overlap) | 12,840–14,443 (p95 14,443) | 12,735–12,872 (p95 12,872) |

### 5. amd64 under Rosetta (alpine + gcc/python/node, linux/amd64)

| case | unit | OrbStack median | katamaran median | kat/Orb | better | OrbStack spread | katamaran spread |
|---|---|---:|---:|---:|---|---|---|
| 200 × /bin/true | ms | 521 | 495 | 0.95 | ≈ tie | 515–526 (p95 526) | 490–498 (p95 498) |
| python sum(i*i), 1e7 | ms | 854 | 849 | 0.99 | ≈ tie | 851–867 (p95 867) | 847–851 (p95 851) |
| node loop 3e8 | ms | 1,260 | 1,179 | 0.94 | katamaran | 1,241–1,269 (p95 1,269) | 1,174–1,182 (p95 1,182) |
| gcc -O2, 3000 functions | ms | 20,331 | 20,182 | 0.99 | ≈ tie | 20,281–20,408 (p95 20,408) | 20,167–20,394 (p95 20,394) |
| 10 × node -e 0 | ms | 3,318 | 3,302 | 1.00 | ≈ tie | 3,309–3,334 (p95 3,334) | 3,292–3,306 (p95 3,306) |

### 6. Lifecycle and footprint

| case | unit | OrbStack median | katamaran median | kat/Orb | better | OrbStack spread | katamaran spread |
|---|---|---:|---:|---:|---|---|---|
| docker run --rm alpine true (from the Mac) | ms | 246 | 302 | 1.22 | OrbStack (ranges overlap) | 237–262 (p95 262) | 253–346 (p95 346) |
| VM footprint before a 4 GiB container | MiB | 1,434 | 1,643 | 1.15 | OrbStack | 1,434–1,536 (p95 1,536) | 1,540–1,745 (p95 1,745) |
| VM footprint with 4 GiB allocated | MiB | 5,632 | 5,636 | 1.00 | ≈ tie | 5,530–5,837 (p95 5,837) | 5,636–5,739 (p95 5,739) |
| VM footprint 10 s after it exits | MiB | 5,632 | 2,769 | 0.49 | katamaran | 5,530–5,734 (p95 5,734) | 2,667–3,281 (p95 3,281) |
| VM footprint 60 s after it exits | MiB | 1,434 | 1,540 | 1.07 | OrbStack | 1,331–1,434 (p95 1,434) | 1,540–1,643 (p95 1,643) |
| memory returned to macOS (peak − 60 s after) | MiB ↑ | 4,301 | 4,096 | 0.95 | ≈ tie | 4,096–4,403 (p95 4,403) | 3,994–4,096 (p95 4,096) |
| idle CPU of the VM process(es), no containers (pass 1, 16:06) | % of one core | 2.03 | 1.47 | 0.72 | katamaran (ranges overlap) | 1.77–2.13 (p95 2.13) | 1.03–2.13 (p95 2.13) |
| idle VM footprint, no containers (pass 1, 16:06) | MiB | 1,126 | 922 | 0.82 | katamaran | 1,126–1,126 (p95 1,126) | 880–945 (p95 945) |
| idle VM footprint after dropping guest caches (pass 1, 16:06) | MiB | 917 | 895 | 0.98 | ≈ tie | 878–928 (p95 928) | 889–899 (p95 899) |
| idle CPU of the VM process(es), no containers (pass 2, 16:11) | % of one core | 1.57 | 1.13 | 0.72 | katamaran (ranges overlap) | 1.47–1.73 (p95 1.73) | 1.07–1.63 (p95 1.63) |
| idle VM footprint, no containers (pass 2, 16:11) | MiB | 1,331 | 1,540 | 1.16 | OrbStack | 1,331–1,434 (p95 1,434) | 1,540–1,540 (p95 1,540) |
| idle VM footprint after dropping guest caches (pass 2, 16:11) | MiB | 975 | 1,438 | 1.47 | OrbStack | 974–980 (p95 980) | 1,438–1,438 (p95 1,438) |
| cold start → ready (kat: `docker ready` log line; Orb: `orb start` returns) | ms | 543 | 668 | 1.23 | OrbStack | 537–553 (p95 553) | 634–671 (p95 671) |
| cold start → first `docker run` succeeds | ms | 1,430 | 967 | 0.68 | katamaran | 1,339–1,438 (p95 1,438) | 932–978 (p95 978) |
| stop (kat: SIGINT → exit; Orb: `orb stop`) | ms | 1,952 | 120 | 0.06 | katamaran | 1,853–1,970 (p95 1,970) | 113–130 (p95 130) |

