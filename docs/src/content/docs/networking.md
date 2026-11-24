---
title: Networking
description: Publish container ports, use automatic port forwarding, reach macOS services with host.docker.internal, and resolve DNS through macOS.
---

katamaran runs a patched gvproxy userspace network stack (`libgvproxy.dylib`) inside the `katamaran-vm` process. Frames move between the guest's `virtio-net` device and gVisor's TCP stack by direct function calls without an intermediate socket.

## Reach containers from macOS

### Published ports

Standard Docker `-p` flags bind ports on `127.0.0.1` on your Mac:

```sh
docker run -d --rm -p 8080:80 nginx:alpine
curl http://127.0.0.1:8080/
```

The guest watches wildcard TCP listeners in the root network namespace (such as `docker-proxy`) via netlink `sock_diag` and asks gvproxy to expose those ports on macOS.

Privileged ports below `1024` (such as `80` and `443`) work without administrator privileges on macOS: when `127.0.0.1:<port>` returns `EACCES`, katamaran binds a wildcard socket scoped to `lo0` (`IP_BOUND_IF`) and drops non-loopback endpoints so traffic remains local to your Mac.

If macOS refuses to bind a port (for example because another macOS service already holds it), `kat status` lists it under `port err` (and `port_errors` in `kat status --json`) and the app marks the port as not forwarded.

### Automatic port forwarding

When `auto_forward` is enabled (the default), katamaran also forwards unpublished TCP ports that containers listen on inside their own network namespaces directly to `127.0.0.1:<port>` on your Mac over virtio-vsock.

If you later publish the same port with `-p`, the explicit published port takes priority over the automatic forward.

Inspect active forwarded ports with `kat status`. To turn off automatic port forwarding:

```sh
kat config set auto_forward=off
kat restart
```

## Reach macOS from containers

Containers reach services listening on `127.0.0.1` on your Mac through `host.docker.internal` (or `host.internal`). Both resolve to `192.168.127.254`, which gvproxy maps to `127.0.0.1` on macOS:

```sh
docker run --rm alpine:3.24 wget -qO- http://host.docker.internal:3000/
```

`dockerd` is started with `--host-gateway-ip=192.168.127.254`, so `--add-host myhost:host-gateway` also routes to your Mac.

## DNS resolution

The guest's `/etc/resolv.conf` points to the gvproxy gateway (`192.168.127.1`). gvproxy resolves names through the macOS system resolver (`dns_sd`), caches upstream answers, and coalesces concurrent identical lookups.

Because resolution goes through macOS, VPN split-DNS rules, `/etc/hosts` entries on macOS, and local `.local` or corporate domains work inside containers without manual DNS setup.

## Container to container networking

Traffic between containers on the same Docker bridge stays inside the Linux guest kernel. `kat-init` disables `net.bridge.bridge-nf-call-iptables`, `net.bridge.bridge-nf-call-ip6tables`, and `net.bridge.bridge-nf-call-arptables` at boot so Layer 2 bridged frames skip unnecessary IP netfilter hooks while Layer 3 NAT, published ports, and inter-bridge isolation remain active.

The guest network interface (`eth0`) and Docker bridges default to an MTU of `65520` bytes so bulk traffic between containers and macOS moves in large frames.
