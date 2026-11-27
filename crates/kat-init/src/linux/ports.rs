//! Watches the root network namespace for TCP sockets listening on every
//! address (0.0.0.0 or ::), such as docker-proxy for `docker run -p`, and
//! tells the host, which forwards the Mac's 127.0.0.1:<port> to them.
//! Loopback listeners (127.0.0.0/8, ::1) go to the auto-forwarder, which
//! forwards the ones docker-proxy holds for `docker run -p 127.0.0.1:...`
//! over vsock (linux/autofwd.rs); gvproxy cannot reach the VM's loopback.
//!
//! Each poll is one sock_diag dump of LISTEN sockets per address family,
//! which walks only the listening hash. Reading /proc/net/tcp{,6} instead
//! walks the whole established hash, sized from guest memory (131072
//! buckets at 16 GiB): about 1 ms of CPU per poll, the largest part of an
//! idle guest's CPU use. /proc is the fallback when netlink is unavailable.

use std::collections::BTreeSet;
use std::fs;
use std::net::{IpAddr, Ipv6Addr};
use std::os::fd::OwnedFd;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use kat_proto::{Msg, PortProto};

use super::autofwd::{self, AutoForward};
use super::service::Notifier;
use crate::autofwd::Listener;

const INTERVAL: Duration = Duration::from_millis(200);
const TCP_LISTEN: &str = "0A";

/// `auto` is the vsock relay (linux/autofwd.rs): it forwards ports Docker
/// published on the VM's loopback, and container ports unless auto-forwarding
/// is off. `None` only if the relay could not start.
pub fn watch(notifier: Notifier, mut auto: Option<AutoForward>) {
    let spawned = thread::Builder::new().name("ports".into()).spawn(move || {
        let mut known = BTreeSet::new();
        loop {
            let scan = root_listeners();
            let mut now = scan.wildcard;
            now.extend(super::machines::forwarded_ports());
            now.retain(|p| !super::k8s::is_internal_port(*p));
            now.extend(super::k8s::forwarded_ports());
            let (added, removed) = diff(&known, &now);
            // An auto-forward gives way before the published port is added.
            let mut auto_changes = auto
                .as_mut()
                .map(|a| a.tick(&now, &scan.loopback))
                .unwrap_or_default();
            auto_changes
                .added
                .retain(|p| !super::k8s::is_internal_port(*p));
            for m in autofwd::messages(&auto_changes.removed, false) {
                notifier.send(&m);
            }
            for port in added {
                notifier.send(&Msg::PortAdd {
                    proto: PortProto::Tcp,
                    port,
                });
            }
            for port in removed {
                notifier.send(&Msg::PortRemove {
                    proto: PortProto::Tcp,
                    port,
                });
            }
            for m in autofwd::messages(&auto_changes.added, true) {
                notifier.send(&m);
            }
            known = now;
            thread::sleep(INTERVAL);
        }
    });
    if let Err(e) = spawned {
        eprintln!("kat-init: cannot start the port watcher: {e}");
    }
}

/// A sock_diag socket in the root network namespace, shared by the callers
/// of [`listening`] (one dump at a time).
struct RootDiag {
    fd: OwnedFd,
    seq: u32,
    buf: Vec<u8>,
}

/// The root namespace's TCP listeners the forwards care about.
#[derive(Clone, Default)]
pub struct RootListeners {
    /// Ports listening on every address.
    pub wildcard: BTreeSet<u16>,
    /// Listeners on a loopback address, sorted.
    pub loopback: Vec<Listener>,
}

impl RootListeners {
    fn from_listeners(list: impl IntoIterator<Item = Listener>) -> RootListeners {
        let mut out = RootListeners::default();
        for l in list {
            if l.addr.is_unspecified() {
                out.wildcard.insert(l.port);
            } else if l.addr.is_loopback() {
                out.loopback.push(l);
            }
        }
        out.loopback.sort();
        out.loopback.dedup();
        out
    }
}

struct RootDiagState {
    diag: Option<RootDiag>,
    last: Option<RootListeners>,
}

static ROOT_DIAG: Mutex<RootDiagState> = Mutex::new(RootDiagState {
    diag: None,
    last: None,
});

/// Wildcard TCP listeners in the root network namespace. Call only from
/// threads in that namespace (the socket is made by the first caller).
pub fn listening() -> BTreeSet<u16> {
    root_listeners().wildcard
}

/// Wildcard and loopback TCP listeners in the root network namespace, as
/// [`listening`].
pub fn root_listeners() -> RootListeners {
    let mut state = ROOT_DIAG.lock().unwrap_or_else(|e| e.into_inner());
    if state.diag.is_none() {
        state.diag = autofwd::diag_socket().ok().map(|fd| RootDiag {
            fd,
            seq: 0,
            buf: vec![0; 32 << 10],
        });
    }
    match state.diag.as_mut() {
        Some(d) => match autofwd::listeners(&d.fd, &mut d.seq, &mut d.buf) {
            Ok(list) => {
                let now = RootListeners::from_listeners(list);
                state.last = Some(now.clone());
                now
            }
            Err(_) => {
                // Drop the broken netlink socket so the next poll recreates it,
                // and preserve prior state instead of flapping all forwards.
                state.diag = None;
                state.last.clone().unwrap_or_else(listening_proc)
            }
        },
        None => {
            let ports = listening_proc();
            state.last = Some(ports.clone());
            ports
        }
    }
}

fn listening_proc() -> RootListeners {
    let mut all = Vec::new();
    for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
        if let Ok(table) = fs::read_to_string(path) {
            all.extend(proc_listeners(&table));
        }
    }
    RootListeners::from_listeners(all)
}

/// Ports of LISTEN sockets bound to the wildcard address in a
/// /proc/net/tcp or /proc/net/tcp6 table. Loopback-only and specific-address
/// listeners are not reachable through the gateway, so they are skipped.
pub fn wildcard_listeners(table: &str) -> BTreeSet<u16> {
    proc_listeners(table)
        .filter(|l| l.addr.is_unspecified())
        .map(|l| l.port)
        .collect()
}

/// LISTEN sockets in a /proc/net/tcp or /proc/net/tcp6 table.
fn proc_listeners(table: &str) -> impl Iterator<Item = Listener> + '_ {
    table.lines().skip(1).filter_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.get(3) != Some(&TCP_LISTEN) {
            return None;
        }
        let (addr, port) = fields.get(1)?.split_once(':')?;
        let port = u16::from_str_radix(port, 16).ok().filter(|p| *p != 0)?;
        let inode = fields.get(9)?.parse().ok()?;
        Some(Listener {
            addr: proc_addr(addr)?,
            port,
            inode,
        })
    })
}

/// A /proc/net/tcp{,6} address: the in-memory (network order) address
/// printed as 32-bit words in host order, so each word's native bytes are
/// the address bytes.
fn proc_addr(hex: &str) -> Option<IpAddr> {
    if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let word = |i: usize| -> Option<[u8; 4]> {
        Some(
            u32::from_str_radix(hex.get(i * 8..i * 8 + 8)?, 16)
                .ok()?
                .to_ne_bytes(),
        )
    };
    match hex.len() {
        8 => Some(IpAddr::from(word(0)?)),
        32 => {
            let mut b = [0u8; 16];
            for i in 0..4 {
                b[i * 4..i * 4 + 4].copy_from_slice(&word(i)?);
            }
            Some(IpAddr::V6(Ipv6Addr::from(b)))
        }
        _ => None,
    }
}

/// Ports that appeared and ports that went away.
pub fn diff(before: &BTreeSet<u16>, now: &BTreeSet<u16>) -> (Vec<u16>, Vec<u16>) {
    (
        now.difference(before).copied().collect(),
        before.difference(now).copied().collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const TCP: &str = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000:1F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 2001 1 0000000000000000 100 0 0 10 0
   1: 0100007F:0CEA 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 2002 1 0000000000000000 100 0 0 10 0
   2: 0202A8C0:0050 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 2003 1 0000000000000000 100 0 0 10 0
   3: 00000000:01BB 0101A8C0:D431 01 00000000:00000000 00:00000000 00000000     0        0 2004 1 0000000000000000 20 4 30 10 -1
   4: 00000000:0016 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 2005 1 0000000000000000 100 0 0 10 0
";

    const TCP6: &str = "  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000000000000:1F90 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 3001 1 0000000000000000 100 0 0 10 0
   1: 00000000000000000000000001000000:1F91 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 3002 1 0000000000000000 100 0 0 10 0
   2: 00000000000000000000000000000000:2328 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 3003 1 0000000000000000 100 0 0 10 0
";

    #[test]
    fn finds_wildcard_tcp_listeners() {
        // 8080 and 22 on 0.0.0.0; not 3306 on loopback, 80 on one address,
        // or the established connection on 443.
        assert_eq!(wildcard_listeners(TCP), BTreeSet::from([22, 8080]));
    }

    #[test]
    fn finds_wildcard_tcp6_listeners() {
        // 8080 and 9000 on ::; not 8081 on ::1.
        assert_eq!(wildcard_listeners(TCP6), BTreeSet::from([8080, 9000]));
    }

    fn listener(addr: &str, port: u16, inode: u32) -> Listener {
        Listener {
            addr: addr.parse().unwrap(),
            port,
            inode,
        }
    }

    #[test]
    fn decodes_proc_addresses() {
        assert_eq!(proc_addr("0100007F"), "127.0.0.1".parse().ok());
        assert_eq!(proc_addr("0202A8C0"), "192.168.2.2".parse().ok());
        assert_eq!(
            proc_addr("00000000000000000000000001000000"),
            "::1".parse().ok()
        );
        assert_eq!(
            proc_addr("0000000000000000FFFF00000100007F"),
            "::ffff:127.0.0.1".parse().ok()
        );
        assert_eq!(proc_addr("0100007"), None);
        assert_eq!(proc_addr("+100007F"), None);
        assert_eq!(proc_addr("0100007G"), None);
    }

    #[test]
    fn splits_root_listeners_into_wildcard_and_loopback() {
        // 3306 on 127.0.0.1 and 8081 on ::1 are loopback; 80 on one address
        // is neither, and established connections are not listeners.
        let all = proc_listeners(TCP).chain(proc_listeners(TCP6));
        let root = RootListeners::from_listeners(all);
        assert_eq!(root.wildcard, BTreeSet::from([22, 8080, 9000]));
        assert_eq!(
            root.loopback,
            [
                listener("127.0.0.1", 3306, 2002),
                listener("::1", 8081, 3002)
            ]
        );
    }

    #[test]
    fn ignores_garbage() {
        assert!(wildcard_listeners("").is_empty());
        assert!(wildcard_listeners("header\n  0: junk\n  1: 00000000:ZZZZ 0 0A\n").is_empty());
        assert_eq!(
            proc_listeners("header\n  1: 0100007F:0050 0 0A\n").count(),
            0
        );
    }

    #[test]
    fn diffs_port_sets() {
        let before = BTreeSet::from([22, 80, 8080]);
        let now = BTreeSet::from([80, 8080, 9000]);
        assert_eq!(diff(&before, &now), (vec![9000], vec![22]));
        assert_eq!(diff(&now, &now), (vec![], vec![]));
        assert_eq!(diff(&BTreeSet::new(), &now), (vec![80, 8080, 9000], vec![]));
    }
}
