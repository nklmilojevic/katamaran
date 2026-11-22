//! Auto-forwarding of container ports that were not published: which ports
//! to forward and to where, from the TCP listeners in each container's
//! network namespace. Platform independent, so it is tested on the Mac; the
//! Linux side (linux/autofwd.rs) gathers the inputs and relays connections.
//!
//! Rules:
//! - A port the VM's root namespace listens on (docker-proxy for `-p`, or a
//!   `--network host` container) is the published forward's, never auto.
//! - A container port that is the target of a `-p` mapping is not
//!   auto-forwarded: the user chose another Mac port for it, and taking the
//!   container port on the Mac too could steal it from a Mac service.
//! - Listeners bound only to loopback stay private, as `-p` would keep them.
//! - Two containers on one port: the one that listened first keeps it; the
//!   other takes over when it goes away.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// A listening socket: its bound address and port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Listener {
    pub addr: IpAddr,
    pub port: u16,
}

/// A container network namespace as the planner sees it.
#[derive(Debug, Clone, Default)]
pub struct Namespace {
    /// Stable identity (the namespace's inode).
    pub key: u64,
    /// The namespace's own addresses, to match `-p` targets.
    pub addrs: Vec<IpAddr>,
    pub listeners: Vec<Listener>,
}

/// Where connections to an auto-forwarded port go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route {
    pub ns: u64,
    pub target: SocketAddr,
}

/// The address to dial inside the namespace for `port`, or `None` if only
/// loopback listens on it. Dialing loopback reaches wildcard listeners
/// without depending on the container's interfaces.
pub fn target(listeners: &[Listener], port: u16) -> Option<SocketAddr> {
    let on_port = || listeners.iter().filter(move |l| l.port == port);
    let pick = if on_port().any(|l| l.addr == IpAddr::V4(Ipv4Addr::UNSPECIFIED)) {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else if on_port().any(|l| l.addr == IpAddr::V6(Ipv6Addr::UNSPECIFIED)) {
        IpAddr::V6(Ipv6Addr::LOCALHOST)
    } else {
        on_port()
            .map(|l| l.addr)
            .filter(|a| !a.is_loopback() && !is_v4_mapped_loopback(a))
            .min_by_key(|a| a.is_ipv6())?
    };
    Some(SocketAddr::new(pick, port))
}

fn is_v4_mapped_loopback(a: &IpAddr) -> bool {
    matches!(a, IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback()))
}

/// The routes for every auto-forwardable port. `namespaces` is in the order
/// they were first seen; `current` keeps a port with its owner while that
/// owner still listens.
pub fn plan(
    root: &BTreeSet<u16>,
    published: &BTreeSet<(IpAddr, u16)>,
    namespaces: &[Namespace],
    current: &BTreeMap<u16, Route>,
) -> BTreeMap<u16, Route> {
    let route = |ns: &Namespace, port: u16| -> Option<Route> {
        if root.contains(&port) {
            return None;
        }
        let mapped = ns.addrs.iter().any(|a| published.contains(&(*a, port)))
            || ns
                .listeners
                .iter()
                .any(|l| l.port == port && published.contains(&(l.addr, port)));
        if mapped {
            return None;
        }
        Some(Route {
            ns: ns.key,
            target: target(&ns.listeners, port)?,
        })
    };
    let mut out = BTreeMap::new();
    for (port, old) in current {
        if let Some(r) = namespaces
            .iter()
            .find(|ns| ns.key == old.ns)
            .and_then(|ns| route(ns, *port))
        {
            out.insert(*port, r);
        }
    }
    for ns in namespaces {
        for l in &ns.listeners {
            if !out.contains_key(&l.port)
                && let Some(r) = route(ns, l.port)
            {
                out.insert(l.port, r);
            }
        }
    }
    out
}

/// Ports that stopped being forwarded and ports that started. A port that
/// changed owner is in neither: the relay looks the owner up per connection.
pub fn diff(before: &BTreeMap<u16, Route>, now: &BTreeMap<u16, Route>) -> (Vec<u16>, Vec<u16>) {
    (
        before
            .keys()
            .filter(|p| !now.contains_key(p))
            .copied()
            .collect(),
        now.keys()
            .filter(|p| !before.contains_key(p))
            .copied()
            .collect(),
    )
}

/// The `-p` target in a docker-proxy command line (NUL-separated, as in
/// /proc/<pid>/cmdline): `(proto, container ip, container port)`.
pub fn docker_proxy_target(cmdline: &[u8]) -> Option<(String, IpAddr, u16)> {
    let args: Vec<&str> = cmdline
        .split(|b| *b == 0)
        .filter_map(|a| std::str::from_utf8(a).ok())
        .collect();
    let flag = |name: &str| {
        args.iter()
            .position(|a| *a == name)
            .and_then(|i| args.get(i + 1).copied())
    };
    Some((
        flag("-proto").unwrap_or("tcp").to_string(),
        flag("-container-ip")?.parse().ok()?,
        flag("-container-port")?.parse().ok()?,
    ))
}

// Netlink sock_diag (linux/inet_diag.h), in native byte order except the
// ports in the socket id, which are big endian.

pub const NLMSG_HDR: usize = 16;
pub const NLMSG_ERROR: u16 = 2;
pub const NLMSG_DONE: u16 = 3;
pub const SOCK_DIAG_BY_FAMILY: u16 = 20;
const NLM_F_REQUEST: u16 = 1;
const NLM_F_DUMP: u16 = 0x300;
pub const AF_INET: u8 = 2;
pub const AF_INET6: u8 = 10;
pub const IPPROTO_TCP: u8 = 6;
pub const TCP_LISTEN: u32 = 10;
/// `struct inet_diag_req_v2`.
const REQ_LEN: usize = 56;
/// `struct inet_diag_msg`.
const MSG_LEN: usize = 72;
pub const REQUEST_LEN: usize = NLMSG_HDR + REQ_LEN;

/// A dump request for `protocol` sockets of `family` in the given states
/// (a bit mask of `1 << TCP_*`).
pub fn diag_request(family: u8, protocol: u8, states: u32, seq: u32) -> [u8; REQUEST_LEN] {
    let mut b = [0u8; REQUEST_LEN];
    b[0..4].copy_from_slice(&(REQUEST_LEN as u32).to_ne_bytes());
    b[4..6].copy_from_slice(&SOCK_DIAG_BY_FAMILY.to_ne_bytes());
    b[6..8].copy_from_slice(&(NLM_F_REQUEST | NLM_F_DUMP).to_ne_bytes());
    b[8..12].copy_from_slice(&seq.to_ne_bytes());
    b[16] = family;
    b[17] = protocol;
    b[20..24].copy_from_slice(&states.to_ne_bytes());
    b
}

#[derive(Debug, PartialEq, Eq)]
pub enum DiagEnd {
    /// More datagrams follow.
    More,
    Done,
    /// The kernel answered with this errno.
    Error(i32),
}

/// Append the sockets in one netlink datagram of a dump to `out`, ignoring
/// messages whose sequence number does not match `expected_seq`.
pub fn parse_diag(buf: &[u8], expected_seq: u32, out: &mut Vec<Listener>) -> DiagEnd {
    let mut rest = buf;
    while rest.len() >= NLMSG_HDR {
        let len = u32::from_ne_bytes(rest[0..4].try_into().unwrap()) as usize;
        let kind = u16::from_ne_bytes(rest[4..6].try_into().unwrap());
        let seq = u32::from_ne_bytes(rest[8..12].try_into().unwrap());
        if len < NLMSG_HDR || len > rest.len() {
            return DiagEnd::Error(EPROTO);
        }
        if seq != expected_seq {
            rest = rest.get((len + 3) & !3..).unwrap_or(&[]);
            continue;
        }
        let body = &rest[NLMSG_HDR..len];
        match kind {
            NLMSG_DONE => return DiagEnd::Done,
            NLMSG_ERROR => {
                let err = body
                    .get(0..4)
                    .map_or(0, |b| i32::from_ne_bytes(b.try_into().unwrap()));
                return if err == 0 {
                    DiagEnd::Done
                } else {
                    DiagEnd::Error(-err)
                };
            }
            SOCK_DIAG_BY_FAMILY if body.len() >= MSG_LEN => {
                let port = u16::from_be_bytes([body[4], body[5]]);
                let src = &body[8..24];
                let addr = match body[0] {
                    AF_INET => Some(IpAddr::from(<[u8; 4]>::try_from(&src[..4]).unwrap())),
                    AF_INET6 => Some(IpAddr::from(<[u8; 16]>::try_from(src).unwrap())),
                    _ => None,
                };
                if let Some(addr) = addr
                    && port != 0
                {
                    out.push(Listener { addr, port });
                }
            }
            _ => {}
        }
        rest = rest.get((len + 3) & !3..).unwrap_or(&[]);
    }
    DiagEnd::More
}

const EPROTO: i32 = 71;

#[cfg(test)]
mod tests {
    use super::*;

    fn l(addr: &str, port: u16) -> Listener {
        Listener {
            addr: addr.parse().unwrap(),
            port,
        }
    }

    fn ns(key: u64, addr: &str, listeners: &[Listener]) -> Namespace {
        Namespace {
            key,
            addrs: vec!["127.0.0.1".parse().unwrap(), addr.parse().unwrap()],
            listeners: listeners.to_vec(),
        }
    }

    fn sa(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn targets_follow_the_bound_address() {
        assert_eq!(target(&[l("0.0.0.0", 80)], 80), Some(sa("127.0.0.1:80")));
        assert_eq!(target(&[l("::", 80)], 80), Some(sa("[::1]:80")));
        assert_eq!(
            target(&[l("::", 80), l("0.0.0.0", 80)], 80),
            Some(sa("127.0.0.1:80"))
        );
        assert_eq!(
            target(&[l("172.17.0.2", 80)], 80),
            Some(sa("172.17.0.2:80"))
        );
        assert_eq!(
            target(&[l("fd00::2", 80), l("172.17.0.2", 80)], 80),
            Some(sa("172.17.0.2:80"))
        );
        assert_eq!(target(&[l("127.0.0.1", 80)], 80), None);
        assert_eq!(target(&[l("::1", 80)], 80), None);
        assert_eq!(target(&[l("::ffff:127.0.0.1", 80)], 80), None);
        assert_eq!(target(&[l("0.0.0.0", 81)], 80), None);
    }

    #[test]
    fn plans_unpublished_container_ports() {
        let a = ns(1, "172.17.0.2", &[l("0.0.0.0", 8000), l("127.0.0.1", 5432)]);
        let b = ns(2, "172.17.0.3", &[l("::", 9000)]);
        let p = plan(
            &BTreeSet::new(),
            &BTreeSet::new(),
            &[a, b],
            &BTreeMap::new(),
        );
        assert_eq!(
            p,
            BTreeMap::from([
                (
                    8000,
                    Route {
                        ns: 1,
                        target: sa("127.0.0.1:8000")
                    }
                ),
                (
                    9000,
                    Route {
                        ns: 2,
                        target: sa("[::1]:9000")
                    }
                ),
            ])
        );
    }

    #[test]
    fn root_namespace_and_published_targets_win() {
        // 8080 is published as 8080:80 (docker-proxy listens in the root
        // namespace); 80 is its target and must not be taken on the Mac too.
        let a = ns(1, "172.17.0.2", &[l("0.0.0.0", 80), l("0.0.0.0", 8080)]);
        let b = ns(2, "172.17.0.3", &[l("0.0.0.0", 3000)]);
        let root = BTreeSet::from([8080]);
        let published = BTreeSet::from([("172.17.0.2".parse().unwrap(), 80)]);
        let p = plan(&root, &published, &[a, b], &BTreeMap::new());
        assert_eq!(p.keys().copied().collect::<Vec<_>>(), [3000]);
    }

    #[test]
    fn published_target_matches_a_specific_bind_too() {
        let mut a = ns(1, "172.18.0.5", &[l("172.18.0.5", 5000)]);
        a.addrs.clear();
        let published = BTreeSet::from([("172.18.0.5".parse().unwrap(), 5000)]);
        assert!(plan(&BTreeSet::new(), &published, &[a], &BTreeMap::new()).is_empty());
    }

    #[test]
    fn first_listener_keeps_a_shared_port_until_it_goes() {
        let a = ns(1, "172.17.0.2", &[l("0.0.0.0", 8000)]);
        let b = ns(2, "172.17.0.3", &[l("0.0.0.0", 8000)]);
        let none = BTreeSet::new();
        let first = plan(
            &none,
            &BTreeSet::new(),
            &[a.clone(), b.clone()],
            &BTreeMap::new(),
        );
        assert_eq!(first[&8000].ns, 1);
        // Order changes (b was seen first after a restart of the scan), but
        // the current owner keeps it.
        let again = plan(&none, &BTreeSet::new(), &[b.clone(), a], &first);
        assert_eq!(again[&8000].ns, 1);
        // a stops: b takes over, and the port never leaves the Mac.
        let after = plan(&none, &BTreeSet::new(), &[b], &again);
        assert_eq!(
            after[&8000],
            Route {
                ns: 2,
                target: sa("127.0.0.1:8000")
            }
        );
        assert_eq!(diff(&again, &after), (vec![], vec![]));
    }

    #[test]
    fn diffs_routes() {
        let r = |ns| Route {
            ns,
            target: sa("127.0.0.1:1"),
        };
        let before = BTreeMap::from([(1, r(1)), (2, r(1))]);
        let now = BTreeMap::from([(2, r(2)), (3, r(1))]);
        assert_eq!(diff(&before, &now), (vec![1], vec![3]));
    }

    fn cmdline(args: &[&str]) -> Vec<u8> {
        args.iter().flat_map(|a| a.bytes().chain([0])).collect()
    }

    #[test]
    fn parses_docker_proxy_command_lines() {
        let cmd = cmdline(&[
            "/usr/bin/docker-proxy",
            "-proto",
            "tcp",
            "-host-ip",
            "0.0.0.0",
            "-host-port",
            "8080",
            "-container-ip",
            "172.17.0.2",
            "-container-port",
            "80",
            "-use-listen-fd",
        ]);
        assert_eq!(
            docker_proxy_target(&cmd),
            Some(("tcp".into(), "172.17.0.2".parse().unwrap(), 80))
        );
        let v6 = cmdline(&[
            "docker-proxy",
            "-proto",
            "udp",
            "-container-ip",
            "fd00::2",
            "-container-port",
            "53",
        ]);
        assert_eq!(
            docker_proxy_target(&v6),
            Some(("udp".into(), "fd00::2".parse().unwrap(), 53))
        );
        assert_eq!(
            docker_proxy_target(&cmdline(&["docker-proxy", "-proto", "tcp"])),
            None
        );
        let bad_ip = cmdline(&["docker-proxy", "-container-ip", "x", "-container-port", "1"]);
        assert_eq!(docker_proxy_target(&bad_ip), None);
    }

    fn nlmsg(kind: u16, seq: u32, body: &[u8]) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&((NLMSG_HDR + body.len()) as u32).to_ne_bytes());
        m.extend_from_slice(&kind.to_ne_bytes());
        m.extend_from_slice(&[0; 2]);
        m.extend_from_slice(&seq.to_ne_bytes());
        m.extend_from_slice(&[0; 4]);
        m.extend_from_slice(body);
        while m.len() % 4 != 0 {
            m.push(0);
        }
        m
    }

    fn diag_msg(family: u8, addr: &[u8], port: u16) -> Vec<u8> {
        let mut b = vec![0u8; MSG_LEN];
        b[0] = family;
        b[1] = TCP_LISTEN as u8;
        b[4..6].copy_from_slice(&port.to_be_bytes());
        b[8..8 + addr.len()].copy_from_slice(addr);
        b
    }

    #[test]
    fn builds_dump_request() {
        let r = diag_request(AF_INET6, IPPROTO_TCP, 1 << TCP_LISTEN, 7);
        assert_eq!(u32::from_ne_bytes(r[0..4].try_into().unwrap()), 72);
        assert_eq!(u16::from_ne_bytes(r[4..6].try_into().unwrap()), 20);
        assert_eq!(u16::from_ne_bytes(r[6..8].try_into().unwrap()), 0x301);
        assert_eq!(u32::from_ne_bytes(r[8..12].try_into().unwrap()), 7);
        assert_eq!((r[16], r[17]), (AF_INET6, IPPROTO_TCP));
        assert_eq!(u32::from_ne_bytes(r[20..24].try_into().unwrap()), 1 << 10);
        assert!(r[24..].iter().all(|b| *b == 0));
    }

    #[test]
    fn parses_diag_dumps() {
        let mut buf = nlmsg(
            SOCK_DIAG_BY_FAMILY,
            7,
            &diag_msg(AF_INET, &[0, 0, 0, 0], 8000),
        );
        buf.extend(nlmsg(
            SOCK_DIAG_BY_FAMILY,
            7,
            &diag_msg(AF_INET, &[172, 17, 0, 2], 443),
        ));
        let mut out = Vec::new();
        assert_eq!(parse_diag(&buf, 7, &mut out), DiagEnd::More);
        let mut v6 = [0u8; 16];
        v6[15] = 1;
        let mut buf = nlmsg(SOCK_DIAG_BY_FAMILY, 7, &diag_msg(AF_INET6, &v6, 9000));
        buf.extend(nlmsg(NLMSG_DONE, 7, &[0; 4]));
        assert_eq!(parse_diag(&buf, 7, &mut out), DiagEnd::Done);
        assert_eq!(
            out,
            [l("0.0.0.0", 8000), l("172.17.0.2", 443), l("::1", 9000)]
        );
    }

    #[test]
    fn diag_errors_and_garbage() {
        let mut out = Vec::new();
        let err = nlmsg(NLMSG_ERROR, 1, &(-2i32).to_ne_bytes());
        assert_eq!(parse_diag(&err, 1, &mut out), DiagEnd::Error(2));
        assert_eq!(parse_diag(&[0; 7], 1, &mut out), DiagEnd::More);
        let mut bad = nlmsg(SOCK_DIAG_BY_FAMILY, 1, &diag_msg(AF_INET, &[0; 4], 1));
        bad[0..4].copy_from_slice(&1000u32.to_ne_bytes());
        assert!(matches!(parse_diag(&bad, 1, &mut out), DiagEnd::Error(_)));
        // Too short to be an inet_diag_msg, and port 0: skipped.
        let mut buf = nlmsg(SOCK_DIAG_BY_FAMILY, 1, &[0; 8]);
        buf.extend(nlmsg(
            SOCK_DIAG_BY_FAMILY,
            1,
            &diag_msg(AF_INET, &[0; 4], 0),
        ));
        assert_eq!(parse_diag(&buf, 1, &mut out), DiagEnd::More);
        assert!(out.is_empty());
    }

    #[test]
    fn parse_diag_ignores_stale_sequence_numbers() {
        let mut buf = nlmsg(
            SOCK_DIAG_BY_FAMILY,
            41,
            &diag_msg(AF_INET, &[0, 0, 0, 0], 1111),
        );
        buf.extend(nlmsg(NLMSG_DONE, 41, &[0; 4]));
        buf.extend(nlmsg(NLMSG_ERROR, 41, &(-5i32).to_ne_bytes()));
        buf.extend(nlmsg(
            SOCK_DIAG_BY_FAMILY,
            42,
            &diag_msg(AF_INET, &[0, 0, 0, 0], 2222),
        ));
        buf.extend(nlmsg(NLMSG_DONE, 42, &[0; 4]));
        let mut out = Vec::new();
        assert_eq!(parse_diag(&buf, 42, &mut out), DiagEnd::Done);
        assert_eq!(out, [l("0.0.0.0", 2222)]);
    }
}
