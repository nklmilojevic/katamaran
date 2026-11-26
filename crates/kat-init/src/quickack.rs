//! `quickack` on container routes (issue #43). Platform independent, so it is
//! tested on the Mac; the Linux side (linux/quickack.rs) finds the container
//! network namespaces and talks rtnetlink in each.
//!
//! Request/response traffic between containers (HTTP keep-alive between pods)
//! can stall for a delayed-ACK timeout per request when the server writes the
//! response header and body separately without `TCP_NODELAY`: Nagle holds the
//! second write back while it is smaller than one MSS and the first write is
//! unacknowledged, while the client, in interactive (pingpong) mode, delays
//! that ACK for 40 ms (`TCP_DELACK_MIN = HZ/25`, rounded up to ~50 ms at our
//! `HZ=100`). Katamaran's 65520-byte MTU (inherited from `eth0` by `docker0`,
//! `cni0` and `katbr0`) raises the MSS to 65468 bytes, so response bodies up
//! to ~64 KiB fall under Nagle's 1-MSS threshold rather than just < 1448 B.
//!
//! The `quickack` route metric (`RTAX_QUICKACK`) makes TCP acknowledge at once
//! on that route while keeping the 65520-byte MTU for bulk throughput. Inside
//! container namespaces and on local container bridges (`cni0`, `docker0`,
//! `br-*`, `katbr0`), traffic only crosses veth pairs inside the VM, so the
//! extra ACKs cost a few cycles and no VM exits.
//!
//! Each unambiguous main-table unicast route without the metric is replaced
//! (`NLM_F_REPLACE`, never `NLM_F_CREATE`, so a route that went away meanwhile
//! is not brought back) with the same keys and attributes plus
//! `RTAX_QUICKACK=1`. Replacing a route flushes the routing cache, so sockets
//! opened before the change pick the metric up too. When multiple sibling
//! routes share the same kernel replacement key (including filtered route
//! types or routes on other interfaces), all of them are skipped because
//! `NLM_F_REPLACE` matches by key and could overwrite a different route.

use std::collections::HashMap;

// rtnetlink (linux/netlink.h, linux/rtnetlink.h), in native byte order.

pub const NLMSG_HDR: usize = 16;
const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const RTM_NEWROUTE: u16 = 24;
const RTM_GETROUTE: u16 = 26;
const NLM_F_REQUEST: u16 = 0x1;
const NLM_F_ACK: u16 = 0x4;
const NLM_F_DUMP_INTR: u16 = 0x10;
const NLM_F_REPLACE: u16 = 0x100;
const NLM_F_DUMP: u16 = 0x300;
pub const AF_INET: u8 = 2;
pub const AF_INET6: u8 = 10;

/// `struct rtmsg`.
const RTMSG_LEN: usize = 12;
const RT_TABLE_MAIN: u8 = 254;
const RTPROT_RA: u8 = 9;
const RTN_UNICAST: u8 = 1;
const RTNH_F_ONLINK: u32 = 4;
const RTM_F_CLONED: u32 = 0x200;
/// Kernel default priority for IPv6 routes when `RTA_PRIORITY` is omitted.
const IP6_RT_PRIO_USER: u32 = 1024;
/// `sizeof(struct rta_cacheinfo)`.
const RTA_CACHEINFO_LEN: usize = 32;

const RTA_DST: u16 = 1;
const RTA_SRC: u16 = 2;
const RTA_OIF: u16 = 4;
const RTA_GATEWAY: u16 = 5;
const RTA_PRIORITY: u16 = 6;
const RTA_PREFSRC: u16 = 7;
const RTA_METRICS: u16 = 8;
const RTA_FLOW: u16 = 11;
const RTA_CACHEINFO: u16 = 12;
const RTA_TABLE: u16 = 15;
const RTA_VIA: u16 = 18;
const RTA_PREF: u16 = 20;
const NLA_F_NESTED: u16 = 0x8000;
const NLA_TYPE_MASK: u16 = 0x3fff;

const RTAX_QUICKACK: u16 = 15;

pub const EINTR: i32 = 4;
pub const EPROTO: i32 = 71;

/// Off with `kat.quickack=0` on the kernel command line.
pub fn enabled(cmdline: &str) -> bool {
    !cmdline.split_whitespace().any(|a| a == "kat.quickack=0")
}

/// Interfaces in the VM's root network namespace that only lead over local
/// veth bridges (never `eth0` to gvproxy).
pub fn is_local_bridge_name(name: &[u8]) -> bool {
    matches!(name, b"cni0" | b"docker0" | b"katbr0") || name.starts_with(b"br-")
}

/// A dump request for the routes of `family`.
pub fn dump_request(family: u8, seq: u32) -> Vec<u8> {
    let mut rtm = [0u8; RTMSG_LEN];
    rtm[0] = family;
    nlmsg(RTM_GETROUTE, NLM_F_REQUEST | NLM_F_DUMP, seq, &rtm)
}

#[derive(Debug, PartialEq, Eq)]
pub enum End {
    /// More datagrams follow.
    More,
    Done,
    /// The kernel answered with this errno.
    Error(i32),
}

/// Key that Linux `NLM_F_REPLACE` matches against in a routing table:
/// - IPv4 (`fib_table_insert`): `(table, dst, dst_len, tos, priority)`
/// - IPv6 (`fib6_add_rt2node`): `(table, dst, dst_len, src, src_len, priority)`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RouteKey {
    family: u8,
    table: u32,
    dst_len: u8,
    src_len: u8,
    tos: u8,
    dst: [u8; 16],
    src: [u8; 16],
    priority: u32,
}

/// Outcome of scanning a route dump (possibly across multiple datagrams and
/// address families).
#[derive(Debug, Default)]
pub struct Dump {
    /// Candidate replacements paired with their route key.
    candidates: Vec<(RouteKey, Vec<u8>)>,
    /// Count of all main-table routes per key (including filtered route types,
    /// complex attributes and routes outside `allow_oif`), so sibling routes
    /// sharing a key are never replaced.
    key_counts: HashMap<RouteKey, usize>,
    /// Set if any matching dump message carried `NLM_F_DUMP_INTR`.
    interrupted: bool,
    /// Number of matching main-table unicast routes seen (including ones that
    /// already carry `quickack` or were skipped).
    pub seen: usize,
}

impl Dump {
    /// Return the route bodies that are unambiguous (unique key in the dump)
    /// and need `RTAX_QUICKACK=1`. Returns empty if the dump was interrupted.
    pub fn into_updates(self) -> Vec<Vec<u8>> {
        if self.interrupted {
            return Vec::new();
        }
        self.candidates
            .into_iter()
            .filter(|(k, _)| self.key_counts.get(k).copied() == Some(1))
            .map(|(_, body)| body)
            .collect()
    }
}

/// Parse one netlink datagram of a route dump, recording candidate route
/// replacements and route-key counts in `out`. Call [`Dump::into_updates`]
/// after the dump finishes to filter out any ambiguous sibling keys.
/// Messages whose sequence number is not `expected_seq` are ignored.
///
/// When `allow_oif` is `Some(pred)`, only routes whose `RTA_OIF` is accepted
/// by `pred` are considered for replacement (used in the root namespace so
/// `eth0` is untouched); sibling key counts still include all main-table
/// routes so `NLM_F_REPLACE` can never clobber another route.
pub fn parse_dump(
    buf: &[u8],
    expected_seq: u32,
    allow_oif: Option<&dyn Fn(u32) -> bool>,
    out: &mut Dump,
) -> End {
    let mut interrupted = out.interrupted;
    let end = each_msg(buf, expected_seq, |kind, flags, body| {
        if flags & NLM_F_DUMP_INTR != 0 {
            interrupted = true;
        }
        if kind == RTM_NEWROUTE
            && let Some(route) = inspect_route(body, allow_oif)
        {
            *out.key_counts.entry(route.key.clone()).or_insert(0) += 1;
            if route.matched_unicast {
                out.seen += 1;
                if let Some(rebuilt) = route.rebuilt {
                    out.candidates.push((route.key, rebuilt));
                }
            }
        }
        None
    });
    out.interrupted = interrupted;
    match end {
        End::Done if interrupted => End::Error(EINTR),
        other => other,
    }
}

/// The outcome of a request sent with `NLM_F_ACK`: `Done` for success.
pub fn parse_ack(buf: &[u8], expected_seq: u32) -> End {
    each_msg(buf, expected_seq, |_, _, _| None)
}

/// The replace request for a route body returned by [`Dump::into_updates`].
pub fn replace_request(route: &[u8], seq: u32) -> Vec<u8> {
    nlmsg(
        RTM_NEWROUTE,
        NLM_F_REQUEST | NLM_F_ACK | NLM_F_REPLACE,
        seq,
        route,
    )
}

fn each_msg(
    buf: &[u8],
    expected_seq: u32,
    mut f: impl FnMut(u16, u16, &[u8]) -> Option<End>,
) -> End {
    let mut rest = buf;
    while rest.len() >= NLMSG_HDR {
        let len = u32::from_ne_bytes(rest[0..4].try_into().unwrap()) as usize;
        let kind = u16::from_ne_bytes(rest[4..6].try_into().unwrap());
        let flags = u16::from_ne_bytes(rest[6..8].try_into().unwrap());
        let seq = u32::from_ne_bytes(rest[8..12].try_into().unwrap());
        if len < NLMSG_HDR || len > rest.len() {
            return End::Error(EPROTO);
        }
        if seq == expected_seq {
            let body = &rest[NLMSG_HDR..len];
            match kind {
                NLMSG_DONE => {
                    let _ = f(kind, flags, body);
                    let err = body
                        .get(0..4)
                        .map_or(0, |b| i32::from_ne_bytes(b.try_into().unwrap()));
                    return if err < 0 { End::Error(-err) } else { End::Done };
                }
                NLMSG_ERROR => {
                    let err = body
                        .get(0..4)
                        .map_or(0, |b| i32::from_ne_bytes(b.try_into().unwrap()));
                    return if err == 0 {
                        End::Done
                    } else {
                        End::Error(-err)
                    };
                }
                _ => {
                    if let Some(end) = f(kind, flags, body) {
                        return end;
                    }
                }
            }
        }
        rest = rest.get(align(len)..).unwrap_or(&[]);
    }
    End::More
}

struct InspectedRoute {
    key: RouteKey,
    /// True if this is a unicast route that passed `allow_oif`.
    matched_unicast: bool,
    /// Rebuilt route body with `RTAX_QUICKACK=1`, if eligible and not set yet.
    rebuilt: Option<Vec<u8>>,
}

/// Inspect a dumped non-cloned main-table route body. Returns `None` if the
/// message is not a non-cloned IPv4/IPv6 main-table route or is malformed.
///
/// Non-unicast routes (blackhole, unreachable, prohibit, ...) and routes with
/// unsupported attributes still return `Some` with `rebuilt: None` so their
/// [`RouteKey`] participates in sibling collision counting.
fn inspect_route(body: &[u8], allow_oif: Option<&dyn Fn(u32) -> bool>) -> Option<InspectedRoute> {
    let rtm: &[u8; RTMSG_LEN] = body.get(..RTMSG_LEN)?.try_into().ok()?;
    let family = rtm[0];
    let dst_len = rtm[1];
    let src_len = rtm[2];
    let tos = rtm[3];
    let protocol = rtm[5];
    let rt_type = rtm[7];
    let flags = u32::from_ne_bytes(rtm[8..12].try_into().unwrap());
    if !matches!(family, AF_INET | AF_INET6) || flags & RTM_F_CLONED != 0 {
        return None;
    }
    let parsed = attrs(&body[RTMSG_LEN..])?;

    let mut table = u32::from(rtm[4]);
    let mut oif: Option<u32> = None;
    let mut dst = [0u8; 16];
    let mut src = [0u8; 16];
    let mut priority = if family == AF_INET6 {
        IP6_RT_PRIO_USER
    } else {
        0
    };
    let mut has_quickack = false;
    let mut skip = rt_type != RTN_UNICAST || (family == AF_INET6 && protocol == RTPROT_RA);
    let mut kept: Vec<(u16, &[u8])> = Vec::new();
    let mut metrics: Vec<(u16, &[u8])> = Vec::new();
    for (kind, data) in parsed {
        match kind {
            RTA_TABLE => {
                table = u32::from_ne_bytes(data.get(..4)?.try_into().ok()?);
            }
            RTA_OIF => {
                oif = Some(u32::from_ne_bytes(data.get(..4)?.try_into().ok()?));
                kept.push((kind, data));
            }
            RTA_DST => {
                let n = if family == AF_INET { 4 } else { 16 };
                dst[..n].copy_from_slice(data.get(..n)?);
                kept.push((kind, data));
            }
            RTA_SRC => {
                let n = if family == AF_INET { 4 } else { 16 };
                src[..n].copy_from_slice(data.get(..n)?);
                kept.push((kind, data));
            }
            RTA_PRIORITY => {
                priority = u32::from_ne_bytes(data.get(..4)?.try_into().ok()?);
                kept.push((kind, data));
            }
            RTA_METRICS => {
                for (m, v) in attrs(data)? {
                    if m == RTAX_QUICKACK {
                        if v.get(..4).is_some_and(|v| v != [0; 4]) {
                            has_quickack = true;
                        }
                    } else {
                        metrics.push((m, v));
                    }
                }
            }
            // Dump-only state; `rta_expires` sits at bytes 8..12 (signed int)
            // and is non-zero on expiring routes (e.g. IPv6 router
            // advertisements), which must not be replaced into permanent routes.
            RTA_CACHEINFO => {
                if data.len() < RTA_CACHEINFO_LEN {
                    skip = true;
                } else {
                    let expires = i32::from_ne_bytes(data[8..12].try_into().unwrap());
                    if expires != 0 {
                        skip = true;
                    }
                }
            }
            RTA_GATEWAY | RTA_PREFSRC | RTA_FLOW | RTA_VIA | RTA_PREF => kept.push((kind, data)),
            // Multipath, encap, nexthop-id, expiring or unrecognized attributes:
            // never strip attributes we do not understand.
            _ => skip = true,
        }
    }
    if table != u32::from(RT_TABLE_MAIN) {
        return None;
    }

    let key = RouteKey {
        family,
        table,
        dst_len,
        src_len: if family == AF_INET6 { src_len } else { 0 },
        tos: if family == AF_INET { tos } else { 0 },
        dst,
        src: if family == AF_INET6 { src } else { [0; 16] },
        priority,
    };
    let matched_unicast = rt_type == RTN_UNICAST
        && match allow_oif {
            Some(pred) => oif.is_some_and(pred),
            None => true,
        };
    // Skip IPv6 link-local (fe80::/10): kernel-managed per interface and not
    // used for pod/container TCP traffic.
    let is_v6_link_local =
        family == AF_INET6 && dst_len >= 10 && dst[0] == 0xfe && (dst[1] & 0xc0) == 0x80;
    if !matched_unicast || has_quickack || skip || is_v6_link_local {
        return Some(InspectedRoute {
            key,
            matched_unicast,
            rebuilt: None,
        });
    }

    let mut out = Vec::with_capacity(body.len() + 12);
    out.extend_from_slice(&rtm[..8]);
    // Ensure rtm_table is RT_TABLE_MAIN even if the dump put 0 there and
    // carried the table only in RTA_TABLE.
    out[4] = RT_TABLE_MAIN;
    // Dumps also report state such as RTNH_F_LINKDOWN, which a request
    // must not carry; onlink is the only flag that is part of the route.
    out.extend_from_slice(&(flags & RTNH_F_ONLINK).to_ne_bytes());
    for (kind, data) in kept {
        if !put_attr(&mut out, kind, data) {
            return Some(InspectedRoute {
                key,
                matched_unicast,
                rebuilt: None,
            });
        }
    }
    let mut nested = Vec::new();
    for (m, v) in metrics {
        if !put_attr(&mut nested, m, v) {
            return Some(InspectedRoute {
                key,
                matched_unicast,
                rebuilt: None,
            });
        }
    }
    if !put_attr(&mut nested, RTAX_QUICKACK, &1u32.to_ne_bytes())
        || !put_attr(&mut out, RTA_METRICS | NLA_F_NESTED, &nested)
    {
        return Some(InspectedRoute {
            key,
            matched_unicast,
            rebuilt: None,
        });
    }
    Some(InspectedRoute {
        key,
        matched_unicast,
        rebuilt: Some(out),
    })
}

/// The attributes in `buf`, or `None` if they are malformed.
fn attrs(mut buf: &[u8]) -> Option<Vec<(u16, &[u8])>> {
    let mut out = Vec::new();
    while buf.len() >= 4 {
        let len = usize::from(u16::from_ne_bytes([buf[0], buf[1]]));
        let kind = u16::from_ne_bytes([buf[2], buf[3]]) & NLA_TYPE_MASK;
        if len < 4 || len > buf.len() {
            return None;
        }
        out.push((kind, &buf[4..len]));
        buf = buf.get(align(len)..).unwrap_or(&[]);
    }
    Some(out)
}

fn put_attr(out: &mut Vec<u8>, kind: u16, data: &[u8]) -> bool {
    let Ok(len) = u16::try_from(4 + data.len()) else {
        return false;
    };
    out.extend_from_slice(&len.to_ne_bytes());
    out.extend_from_slice(&kind.to_ne_bytes());
    out.extend_from_slice(data);
    out.resize(align(out.len()), 0);
    true
}

fn nlmsg(kind: u16, flags: u16, seq: u32, body: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(NLMSG_HDR + body.len());
    m.extend_from_slice(&((NLMSG_HDR + body.len()) as u32).to_ne_bytes());
    m.extend_from_slice(&kind.to_ne_bytes());
    m.extend_from_slice(&flags.to_ne_bytes());
    m.extend_from_slice(&seq.to_ne_bytes());
    m.extend_from_slice(&[0; 4]);
    m.extend_from_slice(body);
    m.resize(align(m.len()), 0);
    m
}

fn align(n: usize) -> usize {
    (n + 3) & !3
}

#[cfg(test)]
mod tests {
    use super::*;

    const RTA_MULTIPATH: u16 = 9;
    const RTA_EXPIRES: u16 = 23;
    const RTA_UNKNOWN: u16 = 99;
    const RTAX_MTU: u16 = 2;
    const RTN_BLACKHOLE: u8 = 6;
    const RTNH_F_LINKDOWN: u32 = 16;

    fn rtmsg(family: u8, dst_len: u8, table: u8, kind: u8, flags: u32) -> Vec<u8> {
        rtmsg_proto(family, dst_len, table, 3, kind, flags)
    }

    fn rtmsg_proto(
        family: u8,
        dst_len: u8,
        table: u8,
        protocol: u8,
        kind: u8,
        flags: u32,
    ) -> Vec<u8> {
        let mut b = vec![family, dst_len, 0, 0, table, protocol, 0, kind];
        b.extend_from_slice(&flags.to_ne_bytes());
        b
    }

    fn route(rtm: Vec<u8>, attrs: &[(u16, Vec<u8>)]) -> Vec<u8> {
        let mut b = rtm;
        for (k, v) in attrs {
            assert!(put_attr(&mut b, *k, v));
        }
        b
    }

    fn cacheinfo(expires: i32) -> Vec<u8> {
        let mut b = vec![0u8; RTA_CACHEINFO_LEN];
        b[8..12].copy_from_slice(&expires.to_ne_bytes());
        b
    }

    fn metrics(list: &[(u16, u32)]) -> Vec<u8> {
        let mut b = Vec::new();
        for (k, v) in list {
            assert!(put_attr(&mut b, *k, &v.to_ne_bytes()));
        }
        b
    }

    fn reply(seq: u32, routes: &[Vec<u8>]) -> Vec<u8> {
        let mut b = Vec::new();
        for r in routes {
            b.extend(nlmsg(RTM_NEWROUTE, 2, seq, r));
        }
        b
    }

    fn dump(buf: &[u8], seq: u32) -> (End, usize, Vec<Vec<u8>>) {
        let mut out = Dump::default();
        let end = parse_dump(buf, seq, None, &mut out);
        let seen = out.seen;
        (end, seen, out.into_updates())
    }

    #[test]
    fn enabled_flag_and_bridge_names() {
        assert!(enabled("console=hvc0 quiet"));
        assert!(!enabled("console=hvc0 kat.quickack=0 quiet"));
        assert!(enabled("kat.quickack=1"));

        assert!(is_local_bridge_name(b"cni0"));
        assert!(is_local_bridge_name(b"docker0"));
        assert!(is_local_bridge_name(b"katbr0"));
        assert!(is_local_bridge_name(b"br-deadbeef1234"));
        assert!(!is_local_bridge_name(b"eth0"));
        assert!(!is_local_bridge_name(b"lo"));
        assert!(!is_local_bridge_name(b"flannel.1"));
    }

    #[test]
    fn builds_dump_request() {
        let r = dump_request(AF_INET6, 7);
        assert_eq!(r.len(), NLMSG_HDR + RTMSG_LEN);
        assert_eq!(u32::from_ne_bytes(r[0..4].try_into().unwrap()), 28);
        assert_eq!(
            u16::from_ne_bytes(r[4..6].try_into().unwrap()),
            RTM_GETROUTE
        );
        assert_eq!(u16::from_ne_bytes(r[6..8].try_into().unwrap()), 0x301);
        assert_eq!(u32::from_ne_bytes(r[8..12].try_into().unwrap()), 7);
        assert_eq!(r[16], AF_INET6);
    }

    #[test]
    fn adds_quickack_and_keeps_the_route() {
        // default via 10.42.0.1 dev eth0, flagged linkdown in the dump, with
        // an MTU metric, rtm_table=0 + RTA_TABLE=254, and dump-only cache info.
        let gw = vec![10, 42, 0, 1];
        let oif = 3u32.to_ne_bytes().to_vec();
        let r = route(
            rtmsg(AF_INET, 0, 0, RTN_UNICAST, RTNH_F_LINKDOWN | RTNH_F_ONLINK),
            &[
                (RTA_TABLE, 254u32.to_ne_bytes().to_vec()),
                (RTA_GATEWAY, gw.clone()),
                (RTA_OIF, oif.clone()),
                (RTA_METRICS, metrics(&[(RTAX_MTU, 1450)])),
                (RTA_CACHEINFO, cacheinfo(0)),
            ],
        );
        let (end, seen, updates) = dump(&reply(9, &[r]), 9);
        assert_eq!(end, End::More);
        assert_eq!(seen, 1);
        let want = route(
            rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, RTNH_F_ONLINK),
            &[
                (RTA_GATEWAY, gw),
                (RTA_OIF, oif),
                (
                    RTA_METRICS | NLA_F_NESTED,
                    metrics(&[(RTAX_MTU, 1450), (RTAX_QUICKACK, 1)]),
                ),
            ],
        );
        assert_eq!(updates, vec![want.clone()]);

        let req = replace_request(&want, 10);
        assert_eq!(
            u16::from_ne_bytes(req[4..6].try_into().unwrap()),
            RTM_NEWROUTE
        );
        // REQUEST | ACK | REPLACE, and no CREATE.
        assert_eq!(u16::from_ne_bytes(req[6..8].try_into().unwrap()), 0x105);
        assert_eq!(&req[NLMSG_HDR..], &want[..]);

        // The replaced route reads back with the metric: seen is 1, updates is empty.
        let again = route(
            rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[(RTA_METRICS, metrics(&[(RTAX_QUICKACK, 1)]))],
        );
        let (_, seen, updates) = dump(&reply(1, &[again]), 1);
        assert_eq!(seen, 1);
        assert!(updates.is_empty());
    }

    #[test]
    fn skips_routes_it_must_not_touch() {
        let mut fe80 = [0u8; 16];
        fe80[0] = 0xfe;
        fe80[1] = 0x80;
        let mut fd00 = [0u8; 16];
        fd00[0] = 0xfd;
        let mut fd01 = [0u8; 16];
        fd01[0] = 0xfd;
        fd01[1] = 0x01;
        let mut fd02 = [0u8; 16];
        fd02[0] = 0xfd;
        fd02[1] = 0x02;
        let mut fd03 = [0u8; 16];
        fd03[0] = 0xfd;
        fd03[1] = 0x03;

        let skipped = [
            // Local table, and a local route.
            route(rtmsg(AF_INET, 32, 255, 2, 0), &[]),
            // Main table by rtm_table, but another table in RTA_TABLE.
            route(
                rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_TABLE, 1000u32.to_ne_bytes().to_vec())],
            ),
            // Cloned (cache) entry.
            route(
                rtmsg(AF_INET6, 128, RT_TABLE_MAIN, RTN_UNICAST, RTM_F_CLONED),
                &[],
            ),
            // Multipath route.
            route(
                rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_MULTIPATH, vec![0; 8])],
            ),
            // Explicit RTA_EXPIRES attribute.
            route(
                rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[
                    (RTA_DST, fd01.to_vec()),
                    (RTA_EXPIRES, 100u32.to_ne_bytes().to_vec()),
                ],
            ),
            // Expiring IPv6 route reported via RTA_CACHEINFO (rta_expires > 0).
            route(
                rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_DST, fd02.to_vec()), (RTA_CACHEINFO, cacheinfo(1800))],
            ),
            // IPv6 router advertisement route even when rta_expires rounds to 0.
            route(
                rtmsg_proto(AF_INET6, 0, RT_TABLE_MAIN, RTPROT_RA, RTN_UNICAST, 0),
                &[(RTA_CACHEINFO, cacheinfo(0))],
            ),
            // Malformed (short) RTA_CACHEINFO.
            route(
                rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_DST, fd03.to_vec()), (RTA_CACHEINFO, vec![0; 16])],
            ),
            // Unknown attribute must not be stripped.
            route(
                rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_UNKNOWN, vec![1, 2, 3, 4])],
            ),
            // IPv6 link-local.
            route(
                rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_DST, fe80.to_vec())],
            ),
            // Not IP.
            route(rtmsg(7, 0, RT_TABLE_MAIN, RTN_UNICAST, 0), &[]),
        ];
        let kept = route(
            rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[(RTA_DST, fd00.to_vec())],
        );
        let mut all = skipped.to_vec();
        all.push(kept);
        let (_, seen, updates) = dump(&reply(4, &all), 4);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0][0], AF_INET6);
        assert_eq!(seen, 8);
    }

    #[test]
    fn skips_ambiguous_sibling_keys_including_filtered_routes() {
        // 1) IPv6 unicast routes sharing (dst, priority) across oif filter.
        let mut fd00 = [0u8; 16];
        fd00[0] = 0xfd;
        let eth0_v6 = route(
            rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[
                (RTA_DST, fd00.to_vec()),
                (RTA_PRIORITY, 256u32.to_ne_bytes().to_vec()),
                (RTA_OIF, 2u32.to_ne_bytes().to_vec()),
            ],
        );
        let br0_v6 = route(
            rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[
                (RTA_DST, fd00.to_vec()),
                (RTA_PRIORITY, 256u32.to_ne_bytes().to_vec()),
                (RTA_OIF, 5u32.to_ne_bytes().to_vec()),
            ],
        );
        // 2) Eligible IPv4 unicast route sharing its key with a filtered
        //    non-unicast (blackhole) route (missing RTA_PRIORITY -> 0 on both).
        let eligible_v4 = route(
            rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[
                (RTA_DST, vec![10, 42, 1, 0]),
                (RTA_OIF, 5u32.to_ne_bytes().to_vec()),
            ],
        );
        let blackhole_v4 = route(
            rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_BLACKHOLE, 0),
            &[(RTA_DST, vec![10, 42, 1, 0])],
        );
        // 3) Eligible IPv4 default route (missing RTA_DST -> 0.0.0.0) sharing
        //    its key with a filtered multipath default route.
        let default_v4 = route(
            rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[
                (RTA_GATEWAY, vec![10, 42, 0, 1]),
                (RTA_OIF, 5u32.to_ne_bytes().to_vec()),
            ],
        );
        let default_mp = route(
            rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[(RTA_MULTIPATH, vec![0; 8])],
        );
        // 4) Unambiguous route on br0 that SHOULD be replaced.
        let unique_v4 = route(
            rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[
                (RTA_DST, vec![10, 42, 2, 0]),
                (RTA_OIF, 5u32.to_ne_bytes().to_vec()),
            ],
        );

        let mut out = Dump::default();
        let pred = |oif: u32| oif == 5;
        parse_dump(
            &reply(
                3,
                &[
                    eth0_v6,
                    br0_v6,
                    eligible_v4,
                    blackhole_v4,
                    default_v4,
                    default_mp,
                    unique_v4,
                ],
            ),
            3,
            Some(&pred),
            &mut out,
        );
        let updates = out.into_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0][0], AF_INET);
        assert_eq!(updates[0][1], 24);
        assert!(updates[0].windows(4).any(|w| w == [10, 42, 2, 0]));
    }

    #[test]
    fn filters_by_oif_for_root_namespace() {
        let eth0 = route(
            rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[(RTA_OIF, 2u32.to_ne_bytes().to_vec())],
        );
        let cni0 = route(
            rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[
                (RTA_DST, vec![10, 42, 0, 0]),
                (RTA_OIF, 5u32.to_ne_bytes().to_vec()),
            ],
        );
        let no_oif = route(rtmsg(AF_INET, 16, RT_TABLE_MAIN, RTN_UNICAST, 0), &[]);

        let mut out = Dump::default();
        let pred = |oif: u32| oif == 5;
        parse_dump(&reply(3, &[eth0, cni0, no_oif]), 3, Some(&pred), &mut out);
        assert_eq!(out.seen, 1);
        let updates = out.into_updates();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0][1], 24);
    }

    #[test]
    fn dump_ends_and_errors() {
        let one = route(rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0), &[]);
        let mut buf = reply(5, std::slice::from_ref(&one));
        buf.extend(nlmsg(NLMSG_DONE, 2, 5, &[0; 4]));
        let (end, _, updates) = dump(&buf, 5);
        assert_eq!((end, updates.len()), (End::Done, 1));

        // Another request's messages are ignored.
        let (end, _, updates) = dump(&buf, 6);
        assert_eq!((end, updates.len()), (End::More, 0));

        // Negative status in NLMSG_DONE reports an error, not an empty dump.
        let done_err = nlmsg(NLMSG_DONE, 2, 7, &(-12i32).to_ne_bytes());
        assert_eq!(dump(&done_err, 7).0, End::Error(12));

        // NLM_F_DUMP_INTR on a dump message or NLMSG_DONE rejects the snapshot.
        let mut intr = nlmsg(RTM_NEWROUTE, 2 | NLM_F_DUMP_INTR, 9, &one);
        intr.extend(nlmsg(NLMSG_DONE, 2, 9, &[0; 4]));
        let (end, _, updates) = dump(&intr, 9);
        assert_eq!(end, End::Error(EINTR));
        assert!(updates.is_empty());

        let mut intr_done = reply(10, std::slice::from_ref(&one));
        intr_done.extend(nlmsg(NLMSG_DONE, 2 | NLM_F_DUMP_INTR, 10, &[0; 4]));
        let (end, _, updates) = dump(&intr_done, 10);
        assert_eq!(end, End::Error(EINTR));
        assert!(updates.is_empty());

        let err = nlmsg(NLMSG_ERROR, 0, 8, &(-2i32).to_ne_bytes());
        assert_eq!(parse_ack(&err, 8), End::Error(2));
        let ok = nlmsg(NLMSG_ERROR, 0, 8, &0i32.to_ne_bytes());
        assert_eq!(parse_ack(&ok, 8), End::Done);
        assert_eq!(
            parse_ack(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0),
            End::Error(EPROTO)
        );

        // A truncated attribute drops the route instead of guessing.
        let mut bad = rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0);
        bad.extend_from_slice(&[40, 0, 1, 0]);
        assert!(dump(&reply(1, &[bad]), 1).2.is_empty());
    }
}
