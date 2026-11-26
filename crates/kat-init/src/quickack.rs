//! `quickack` on container routes (issue #43). Platform independent, so it is
//! tested on the Mac; the Linux side (linux/quickack.rs) finds the container
//! network namespaces and talks rtnetlink in each.
//!
//! Request/response traffic between containers (HTTP keep-alive between pods)
//! can stall for a delayed-ACK timeout per request: a server that writes the
//! response header and body separately has the second write held back by
//! Nagle until the first is acknowledged, while the client, in interactive
//! (pingpong) mode, delays that ACK by up to 40 ms (50 ms at our HZ=100,
//! where the timer rounds up to the next tick). The `quickack` route metric
//! (`RTAX_QUICKACK`) makes TCP acknowledge at once on that route. Inside a
//! container the routes only lead over a veth to the same VM, so the extra
//! ACKs cost a few cycles and no VM exits.
//!
//! Each main-table unicast route of a namespace without the metric is
//! replaced (`NLM_F_REPLACE`, never `NLM_F_CREATE`, so a route that went away
//! meanwhile is not brought back) with the same keys and attributes plus
//! `RTAX_QUICKACK=1`. Replacing a route flushes the routing cache, so sockets
//! opened before the change pick the metric up too.

// rtnetlink (linux/netlink.h, linux/rtnetlink.h), in native byte order.

pub const NLMSG_HDR: usize = 16;
const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const RTM_NEWROUTE: u16 = 24;
const RTM_GETROUTE: u16 = 26;
const NLM_F_REQUEST: u16 = 0x1;
const NLM_F_ACK: u16 = 0x4;
const NLM_F_REPLACE: u16 = 0x100;
const NLM_F_DUMP: u16 = 0x300;
pub const AF_INET: u8 = 2;
pub const AF_INET6: u8 = 10;

/// `struct rtmsg`.
const RTMSG_LEN: usize = 12;
const RT_TABLE_MAIN: u8 = 254;
const RTN_UNICAST: u8 = 1;
const RTNH_F_ONLINK: u32 = 4;
const RTM_F_CLONED: u32 = 0x200;

const RTA_DST: u16 = 1;
const RTA_SRC: u16 = 2;
const RTA_OIF: u16 = 4;
const RTA_GATEWAY: u16 = 5;
const RTA_PRIORITY: u16 = 6;
const RTA_PREFSRC: u16 = 7;
const RTA_METRICS: u16 = 8;
const RTA_MULTIPATH: u16 = 9;
const RTA_TABLE: u16 = 15;
const RTA_VIA: u16 = 18;
const RTA_PREF: u16 = 20;
const RTA_ENCAP_TYPE: u16 = 21;
const RTA_ENCAP: u16 = 22;
const RTA_EXPIRES: u16 = 23;
const RTA_NH_ID: u16 = 30;
const NLA_TYPE_MASK: u16 = 0x3fff;

const RTAX_QUICKACK: u16 = 15;

const EPROTO: i32 = 71;

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

/// Parse one netlink datagram of a route dump, appending to `out` the body
/// (`struct rtmsg` and attributes) of every route that should get the metric.
/// Messages whose sequence number is not `expected_seq` are ignored.
pub fn parse_dump(buf: &[u8], expected_seq: u32, out: &mut Vec<Vec<u8>>) -> End {
    each_msg(buf, expected_seq, |kind, body| match kind {
        RTM_NEWROUTE => {
            if let Some(r) = with_quickack(body) {
                out.push(r);
            }
            None
        }
        _ => None,
    })
}

/// The outcome of a request sent with `NLM_F_ACK`: `Done` for success.
pub fn parse_ack(buf: &[u8], expected_seq: u32) -> End {
    each_msg(buf, expected_seq, |_, _| None)
}

/// The replace request for a route body returned by [`parse_dump`].
pub fn replace_request(route: &[u8], seq: u32) -> Vec<u8> {
    nlmsg(
        RTM_NEWROUTE,
        NLM_F_REQUEST | NLM_F_ACK | NLM_F_REPLACE,
        seq,
        route,
    )
}

fn each_msg(buf: &[u8], expected_seq: u32, mut f: impl FnMut(u16, &[u8]) -> Option<End>) -> End {
    let mut rest = buf;
    while rest.len() >= NLMSG_HDR {
        let len = u32::from_ne_bytes(rest[0..4].try_into().unwrap()) as usize;
        let kind = u16::from_ne_bytes(rest[4..6].try_into().unwrap());
        let seq = u32::from_ne_bytes(rest[8..12].try_into().unwrap());
        if len < NLMSG_HDR || len > rest.len() {
            return End::Error(EPROTO);
        }
        if seq == expected_seq {
            let body = &rest[NLMSG_HDR..len];
            match kind {
                NLMSG_DONE => return End::Done,
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
                    if let Some(end) = f(kind, body) {
                        return end;
                    }
                }
            }
        }
        rest = rest.get(align(len)..).unwrap_or(&[]);
    }
    End::More
}

/// The route `body` (from a dump) rebuilt with `RTAX_QUICKACK=1`, or `None`
/// if it already has it or is not a plain main-table unicast route.
fn with_quickack(body: &[u8]) -> Option<Vec<u8>> {
    let rtm: &[u8; RTMSG_LEN] = body.get(..RTMSG_LEN)?.try_into().ok()?;
    let family = rtm[0];
    let flags = u32::from_ne_bytes(rtm[8..12].try_into().unwrap());
    if !matches!(family, AF_INET | AF_INET6) || rtm[7] != RTN_UNICAST || flags & RTM_F_CLONED != 0 {
        return None;
    }
    let mut table = u32::from(rtm[4]);
    let mut kept: Vec<(u16, &[u8])> = Vec::new();
    let mut metrics: Vec<(u16, &[u8])> = Vec::new();
    for (kind, data) in attrs(&body[RTMSG_LEN..])? {
        match kind {
            RTA_TABLE => table = u32::from_ne_bytes(data.get(..4)?.try_into().ok()?),
            RTA_METRICS => {
                for (m, v) in attrs(data)? {
                    if m == RTAX_QUICKACK {
                        if v.get(..4).is_some_and(|v| v != [0; 4]) {
                            return None;
                        }
                    } else {
                        metrics.push((m, v));
                    }
                }
            }
            RTA_DST | RTA_SRC | RTA_OIF | RTA_GATEWAY | RTA_PRIORITY | RTA_PREFSRC | RTA_VIA
            | RTA_PREF => kept.push((kind, data)),
            // Rebuilding these is not worth it for container routes.
            RTA_MULTIPATH | RTA_ENCAP | RTA_ENCAP_TYPE | RTA_EXPIRES | RTA_NH_ID => return None,
            // Dump-only state (RTA_CACHEINFO, ...).
            _ => {}
        }
    }
    if table != u32::from(RT_TABLE_MAIN) {
        return None;
    }

    let mut out = Vec::with_capacity(body.len() + 8);
    out.extend_from_slice(&rtm[..8]);
    // Dumps also report state such as RTNH_F_LINKDOWN, which a request
    // must not carry; onlink is the only flag that is part of the route.
    out.extend_from_slice(&(flags & RTNH_F_ONLINK).to_ne_bytes());
    for (kind, data) in kept {
        put_attr(&mut out, kind, data);
    }
    let mut nested = Vec::new();
    for (m, v) in metrics {
        put_attr(&mut nested, m, v);
    }
    put_attr(&mut nested, RTAX_QUICKACK, &1u32.to_ne_bytes());
    put_attr(&mut out, RTA_METRICS, &nested);
    Some(out)
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

fn put_attr(out: &mut Vec<u8>, kind: u16, data: &[u8]) {
    out.extend_from_slice(&((4 + data.len()) as u16).to_ne_bytes());
    out.extend_from_slice(&kind.to_ne_bytes());
    out.extend_from_slice(data);
    out.resize(align(out.len()), 0);
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

    const RTA_CACHEINFO: u16 = 12;
    const RTAX_MTU: u16 = 2;
    const RTNH_F_LINKDOWN: u32 = 16;

    fn rtmsg(family: u8, dst_len: u8, table: u8, kind: u8, flags: u32) -> Vec<u8> {
        let mut b = vec![family, dst_len, 0, 0, table, 3, 0, kind];
        b.extend_from_slice(&flags.to_ne_bytes());
        b
    }

    fn route(rtm: Vec<u8>, attrs: &[(u16, Vec<u8>)]) -> Vec<u8> {
        let mut b = rtm;
        for (k, v) in attrs {
            put_attr(&mut b, *k, v);
        }
        b
    }

    fn metrics(list: &[(u16, u32)]) -> Vec<u8> {
        let mut b = Vec::new();
        for (k, v) in list {
            put_attr(&mut b, *k, &v.to_ne_bytes());
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

    fn dump(buf: &[u8], seq: u32) -> (End, Vec<Vec<u8>>) {
        let mut out = Vec::new();
        let end = parse_dump(buf, seq, &mut out);
        (end, out)
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
        // an MTU metric and dump-only cache info.
        let gw = vec![10, 42, 0, 1];
        let oif = 3u32.to_ne_bytes().to_vec();
        let r = route(
            rtmsg(
                AF_INET,
                0,
                RT_TABLE_MAIN,
                RTN_UNICAST,
                RTNH_F_LINKDOWN | RTNH_F_ONLINK,
            ),
            &[
                (RTA_TABLE, 254u32.to_ne_bytes().to_vec()),
                (RTA_GATEWAY, gw.clone()),
                (RTA_OIF, oif.clone()),
                (RTA_METRICS, metrics(&[(RTAX_MTU, 1450)])),
                (RTA_CACHEINFO, vec![0; 32]),
            ],
        );
        let (end, out) = dump(&reply(9, &[r]), 9);
        assert_eq!(end, End::More);
        let want = route(
            rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, RTNH_F_ONLINK),
            &[
                (RTA_GATEWAY, gw),
                (RTA_OIF, oif),
                (
                    RTA_METRICS,
                    metrics(&[(RTAX_MTU, 1450), (RTAX_QUICKACK, 1)]),
                ),
            ],
        );
        assert_eq!(out, vec![want.clone()]);

        let req = replace_request(&want, 10);
        assert_eq!(
            u16::from_ne_bytes(req[4..6].try_into().unwrap()),
            RTM_NEWROUTE
        );
        // REQUEST | ACK | REPLACE, and no CREATE.
        assert_eq!(u16::from_ne_bytes(req[6..8].try_into().unwrap()), 0x105);
        assert_eq!(&req[NLMSG_HDR..], &want[..]);

        // The replaced route reads back with the metric: nothing more to do.
        let again = route(
            rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
            &[(RTA_METRICS, metrics(&[(RTAX_QUICKACK, 1)]))],
        );
        assert_eq!(dump(&reply(1, &[again]), 1).1, Vec::<Vec<u8>>::new());
    }

    #[test]
    fn skips_routes_it_must_not_touch() {
        let skipped = [
            // Local table, and a local route.
            route(rtmsg(AF_INET, 32, 255, 2, 0), &[]),
            // Main table by RTA_TABLE, but another table.
            route(
                rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_TABLE, 1000u32.to_ne_bytes().to_vec())],
            ),
            // Cloned (cache) entry.
            route(
                rtmsg(AF_INET6, 128, RT_TABLE_MAIN, RTN_UNICAST, RTM_F_CLONED),
                &[],
            ),
            // Multipath and expiring routes.
            route(
                rtmsg(AF_INET, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_MULTIPATH, vec![0; 8])],
            ),
            route(
                rtmsg(AF_INET6, 0, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[(RTA_EXPIRES, 100u32.to_ne_bytes().to_vec())],
            ),
            // Not IP.
            route(rtmsg(7, 0, RT_TABLE_MAIN, RTN_UNICAST, 0), &[]),
        ];
        let kept = route(rtmsg(AF_INET6, 64, RT_TABLE_MAIN, RTN_UNICAST, 0), &[]);
        let mut all = skipped.to_vec();
        all.push(kept);
        let (_, out) = dump(&reply(4, &all), 4);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0], AF_INET6);
    }

    #[test]
    fn dump_ends_and_errors() {
        let mut buf = reply(
            5,
            &[route(
                rtmsg(AF_INET, 24, RT_TABLE_MAIN, RTN_UNICAST, 0),
                &[],
            )],
        );
        buf.extend(nlmsg(NLMSG_DONE, 2, 5, &[0; 4]));
        let (end, out) = dump(&buf, 5);
        assert_eq!((end, out.len()), (End::Done, 1));

        // Another request's messages are ignored.
        let (end, out) = dump(&buf, 6);
        assert_eq!((end, out.len()), (End::More, 0));

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
        assert_eq!(dump(&reply(1, &[bad]), 1).1, Vec::<Vec<u8>>::new());
    }
}
