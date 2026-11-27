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

Ports published on a loopback address work the same way from your Mac:

```sh
docker run -d --rm -p 127.0.0.1:8080:80 nginx:alpine
curl http://127.0.0.1:8080/
```

Docker binds such a port on the VM's loopback, which gvproxy cannot reach, so katamaran forwards it over virtio-vsock instead: the Mac's `127.0.0.1:<port>` connects to the guest agent, which dials the container from inside the VM, as `docker-proxy` does. Only loopback sockets that a `docker-proxy` holds for that exact `-host-ip` and `-host-port` are forwarded (`127.0.0.1`, other `127.0.0.0/8` addresses, and `::1`); the VM's own loopback services, such as containerd, k3s, and kubelet, stay private to the VM. The port is never exposed on the VM's network interface, and on the Mac it listens only on loopback, on `127.0.0.1` (and any [`lo0` aliases](#listen-on-lo0-aliases) in `forward_addresses`) like every other forward. These ports are forwarded even when `auto_forward` is off, and tools that publish on `127.0.0.1` by default, such as the `ddev` router (`127.0.0.1:80` and `127.0.0.1:443`), work without `--router-bind-all-interfaces`.

Privileged ports below `1024` (such as `80` and `443`) work without administrator privileges on macOS: when `127.0.0.1:<port>` returns `EACCES`, katamaran binds a wildcard socket scoped to `lo0` (`IP_BOUND_IF`) and drops non-loopback endpoints so traffic remains local to your Mac.

If macOS refuses to bind a port (for example because another macOS service already holds it), `kat status` lists it under `port err` (and `port_errors` in `kat status --json`) and the app marks the port as not forwarded.

### Automatic port forwarding

When `auto_forward` is enabled (the default), katamaran also forwards unpublished TCP ports that containers listen on inside their own network namespaces directly to `127.0.0.1:<port>` (or your configured `forward_addresses`) on your Mac over virtio-vsock.

If you later publish the same port with `-p`, the explicit published port takes priority over the automatic forward.

Inspect active forwarded ports with `kat status`. To turn off automatic port forwarding:

```sh
kat config set auto_forward=off
kat restart
```

### Listen on lo0 aliases

By default, forwarded TCP ports listen only on `127.0.0.1`. Some setups put a local ingress on a loopback alias instead. For example, `*.dev.example` resolves to `172.16.1.200`, and the ingress controller's Service lists that IP in `externalIPs`. To make published and automatically forwarded TCP ports, including Kubernetes `NodePort` and `LoadBalancer` ports, listen on the alias as well, add the alias to `lo0` and list it in `forward_addresses` (UDP forwards continue to use `127.0.0.1`):

```sh
sudo ifconfig lo0 alias 172.16.1.200
kat config set forward_addresses=127.0.0.1,172.16.1.200
kat restart
curl https://app.dev.example/
```

Only addresses assigned to a loopback interface are used, and alias sockets are scoped to `lo0` (`IP_BOUND_IF`) with loopback-peer checks so traffic from other hosts on your network cannot reach forwarded ports. katamaran skips any address not assigned to `lo0` and logs a warning, so a typo cannot expose a port to your network. If one configured address binds and another is already in use, the forward stays active on the addresses that bound; `kat status` still lists the port as forwarded (showing the bound addresses) rather than as a port-level error, and the skipped address is recorded in the daemon logs (`kat logs`). Add the alias before the VM starts, because a port that is already forwarded does not pick up an alias added later. Privileged ports such as `443` also work without root: one `lo0`-scoped wildcard socket serves every listed address. If another process already listens on one of those addresses at that port, connections to that address keep reaching that process, although `kat status` lists every address for the port. macOS removes `lo0` aliases on reboot, so persist yours with a LaunchDaemon if you need it.

With addresses other than the default, katamaran relays published TCP ports itself instead of gvproxy listening on `127.0.0.1`. Those relays share a daemon-wide budget sized from the process's open-file limit (at most 512 concurrent connections): each published port gets up to 128 of them and all published ports together up to 512, while automatically forwarded ports are capped at half the budget so they cannot crowd out published ports. Each port katamaran listens on itself also uses a few descriptors from a shared listener budget sized from the same limit (a few hundred ports with the default limits). A port that does not fit shows a "forward descriptor limit reached" error in `kat status` and is forwarded once another forward closes. A connection over a limit is accepted and closed immediately, and the daemon logs which limit was reached.

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
