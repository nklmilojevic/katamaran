//! Watches the root network namespace for TCP sockets listening on every
//! address (0.0.0.0 or ::), such as docker-proxy for `docker run -p`, and
//! tells the host, which forwards the Mac's 127.0.0.1:<port> to them.
//!
//! Each poll is one sock_diag dump of LISTEN sockets per address family,
//! which walks only the listening hash. Reading /proc/net/tcp{,6} instead
//! walks the whole established hash, sized from guest memory (131072
//! buckets at 16 GiB): about 1 ms of CPU per poll, the largest part of an
//! idle guest's CPU use. /proc is the fallback when netlink is unavailable.

use std::collections::BTreeSet;
use std::fs;
use std::os::fd::OwnedFd;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use kat_proto::{Msg, PortProto};

use super::autofwd::{self, AutoForward};
use super::service::Notifier;

const INTERVAL: Duration = Duration::from_millis(200);
const TCP_LISTEN: &str = "0A";

/// With `auto`, container ports are auto-forwarded too (linux/autofwd.rs).
pub fn watch(notifier: Notifier, mut auto: Option<AutoForward>) {
    let spawned = thread::Builder::new().name("ports".into()).spawn(move || {
        let mut known = BTreeSet::new();
        loop {
            let mut now = listening();
            now.extend(super::machines::forwarded_ports());
            now.retain(|p| !super::k8s::is_internal_port(*p));
            now.extend(super::k8s::forwarded_ports());
            let (added, removed) = diff(&known, &now);
            // An auto-forward gives way before the published port is added.
            let mut auto_changes = auto.as_mut().map(|a| a.tick(&now)).unwrap_or_default();
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

struct RootDiagState {
    diag: Option<RootDiag>,
    last: Option<BTreeSet<u16>>,
}

static ROOT_DIAG: Mutex<RootDiagState> = Mutex::new(RootDiagState {
    diag: None,
    last: None,
});

/// Wildcard TCP listeners in the root network namespace. Call only from
/// threads in that namespace (the socket is made by the first caller).
pub fn listening() -> BTreeSet<u16> {
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
                let ports: BTreeSet<u16> = list
                    .into_iter()
                    .filter(|l| l.addr.is_unspecified())
                    .map(|l| l.port)
                    .collect();
                state.last = Some(ports.clone());
                ports
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

fn listening_proc() -> BTreeSet<u16> {
    let mut ports = BTreeSet::new();
    for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
        if let Ok(table) = fs::read_to_string(path) {
            ports.extend(wildcard_listeners(&table));
        }
    }
    ports
}

/// Ports of LISTEN sockets bound to the wildcard address in a
/// /proc/net/tcp or /proc/net/tcp6 table. Loopback-only and specific-address
/// listeners are not reachable through the gateway, so they are skipped.
pub fn wildcard_listeners(table: &str) -> BTreeSet<u16> {
    table
        .lines()
        .skip(1)
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let local = fields.nth(1)?;
            let state = fields.nth(1)?;
            if state != TCP_LISTEN {
                return None;
            }
            let (addr, port) = local.split_once(':')?;
            if addr.is_empty() || addr.bytes().any(|b| b != b'0') {
                return None;
            }
            u16::from_str_radix(port, 16).ok().filter(|p| *p != 0)
        })
        .collect()
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

    #[test]
    fn ignores_garbage() {
        assert!(wildcard_listeners("").is_empty());
        assert!(wildcard_listeners("header\n  0: junk\n  1: 00000000:ZZZZ 0 0A\n").is_empty());
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
