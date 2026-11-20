//! Host file changes as inotify events in the guest. The guest kernel
//! (guest/kernel/patches/0003) reports on /dev/kat_fsnotify each virtio-fs
//! inode a guest process watches with inotify; this relays those reports to
//! the host as `FsWatch`/`FsUnwatch`, and writes the host's `FsEvents` back
//! to the device, where the kernel raises the events and drops stale caches.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::thread;

use kat_proto::{FsEvent, Msg};

use super::service::Notifier;

const DEVICE: &str = "/dev/kat_fsnotify";
/// `struct kat_fsn_report` without its path.
const REPORT_HEADER: usize = 32;
/// `struct kat_fsn_event` without its name.
const EVENT_HEADER: usize = 24;
/// The most the kernel takes in one write.
const WRITE_MAX: usize = 1 << 20;
const READ_BUF: usize = 64 << 10;

const REPORT_WATCH: u16 = 1;
const REPORT_UNWATCH: u16 = 2;
const REPORT_RESET: u16 = 3;
const FLAG_DIR: u8 = 1;

pub struct FsNotify {
    dev: Arc<File>,
}

impl FsNotify {
    /// Open the device and relay watch reports to the host from a thread.
    /// `mounts` holds the `st_dev` and guest path of each virtio-fs share;
    /// with none there is nothing to watch.
    pub fn start(mounts: Vec<(u64, String)>, notifier: Notifier) -> io::Result<Option<FsNotify>> {
        if mounts.is_empty() {
            return Ok(None);
        }
        let dev = Arc::new(OpenOptions::new().read(true).write(true).open(DEVICE)?);
        let reader = dev.clone();
        thread::Builder::new()
            .name("fsnotify".into())
            .spawn(move || {
                if let Err(e) = relay_watches(&reader, &mounts, &notifier) {
                    eprintln!("kat-init: {DEVICE}: {e}");
                }
            })?;
        Ok(Some(FsNotify { dev }))
    }

    /// The open device, shared with the cache invalidation relay.
    pub fn device(&self) -> Arc<File> {
        self.dev.clone()
    }

    pub fn apply(&self, events: &[FsEvent]) {
        for chunk in encode_events(events) {
            if let Err(e) = (&*self.dev).write_all(&chunk) {
                eprintln!("kat-init: write {DEVICE}: {e}");
            }
        }
    }
}

/// The `st_dev` of each share's mount point, for mapping kernel reports to
/// guest paths.
pub fn share_mounts<'a>(paths: impl Iterator<Item = &'a str>) -> Vec<(u64, String)> {
    paths
        .filter_map(|p| Some((std::fs::metadata(p).ok()?.dev(), p.to_owned())))
        .collect()
}

fn relay_watches(dev: &File, mounts: &[(u64, String)], notifier: &Notifier) -> io::Result<()> {
    let mut buf = vec![0u8; READ_BUF];
    // Opening the device resets it; the host starts with no watches anyway.
    let mut first = true;
    loop {
        let n = match (&*dev).read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        for report in parse_reports(&buf[..n]) {
            if std::mem::take(&mut first) && report == Report::Reset {
                continue;
            }
            if let Some(msg) = to_msg(report, mounts) {
                notifier.send(&msg);
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Report<'a> {
    Watch {
        id: u64,
        dev: u64,
        dir: bool,
        path: &'a [u8],
    },
    Unwatch {
        id: u64,
    },
    Reset,
}

fn parse_reports(mut buf: &[u8]) -> Vec<Report<'_>> {
    let mut out = Vec::new();
    while buf.len() >= REPORT_HEADER {
        let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        if len < REPORT_HEADER || len > buf.len() {
            break;
        }
        let kind = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        let flags = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let dev = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let path_len = u32::from_le_bytes(buf[24..28].try_into().unwrap()) as usize;
        if REPORT_HEADER + path_len > len {
            break;
        }
        match kind {
            REPORT_WATCH => out.push(Report::Watch {
                id,
                dev,
                dir: flags & u16::from(FLAG_DIR) != 0,
                path: &buf[REPORT_HEADER..REPORT_HEADER + path_len],
            }),
            REPORT_UNWATCH => out.push(Report::Unwatch { id }),
            REPORT_RESET => out.push(Report::Reset),
            _ => {}
        }
        buf = &buf[len..];
    }
    out
}

/// The host message for a report; a watch outside the shares, or on a path
/// that is not UTF-8, is not the host's to watch.
fn to_msg(report: Report<'_>, mounts: &[(u64, String)]) -> Option<Msg> {
    Some(match report {
        Report::Watch { id, dev, dir, path } => {
            let (_, mount) = mounts.iter().find(|(d, _)| *d == dev)?;
            let rel = std::str::from_utf8(path).ok()?;
            let path = match rel.trim_end_matches('/') {
                "" => mount.clone(),
                rel => format!("{}{rel}", mount.trim_end_matches('/')),
            };
            Msg::FsWatch { id, dir, path }
        }
        Report::Unwatch { id } => Msg::FsUnwatch { id },
        Report::Reset => Msg::FsWatchReset,
    })
}

/// Kernel event records, in writes of at most [`WRITE_MAX`] bytes. A name
/// longer than Linux allows is dropped.
fn encode_events(events: &[FsEvent]) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut cur = Vec::new();
    for e in events {
        let name = e.name.as_bytes();
        if name.len() > 255 {
            continue;
        }
        let len = (EVENT_HEADER + name.len()).next_multiple_of(8);
        if cur.len() + len > WRITE_MAX {
            chunks.push(std::mem::take(&mut cur));
        }
        cur.extend_from_slice(&e.id.to_le_bytes());
        cur.extend_from_slice(&e.ino.to_le_bytes());
        cur.extend_from_slice(&e.cookie.to_le_bytes());
        cur.extend_from_slice(&u16::from(e.op as u8).to_le_bytes());
        cur.push(if e.dir { FLAG_DIR } else { 0 });
        cur.push(name.len() as u8);
        cur.extend_from_slice(name);
        cur.resize(cur.len() + len - EVENT_HEADER - name.len(), 0);
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use kat_proto::FsOp;

    use super::*;

    fn report(kind: u16, flags: u16, id: u64, dev: u64, path: &[u8]) -> Vec<u8> {
        let len = (REPORT_HEADER + path.len()).next_multiple_of(8);
        let mut r = Vec::new();
        r.extend_from_slice(&(len as u32).to_le_bytes());
        r.extend_from_slice(&kind.to_le_bytes());
        r.extend_from_slice(&flags.to_le_bytes());
        r.extend_from_slice(&id.to_le_bytes());
        r.extend_from_slice(&dev.to_le_bytes());
        r.extend_from_slice(&(path.len() as u32).to_le_bytes());
        r.extend_from_slice(&[0; 4]);
        r.extend_from_slice(path);
        r.resize(len, 0);
        r
    }

    #[test]
    fn reports_become_host_messages() {
        let mut buf = report(REPORT_RESET, 0, 0, 0, b"");
        buf.extend(report(REPORT_WATCH, 1, 5, 44, b"/me/app"));
        buf.extend(report(REPORT_WATCH, 0, 6, 44, b"/"));
        buf.extend(report(REPORT_WATCH, 0, 7, 99, b"/elsewhere"));
        buf.extend(report(REPORT_WATCH, 0, 8, 44, &[b'/', 0xff]));
        buf.extend(report(REPORT_UNWATCH, 0, 5, 0, b""));
        let mounts = [(44, "/Users".to_owned())];
        let msgs: Vec<Msg> = parse_reports(&buf)
            .into_iter()
            .filter_map(|r| to_msg(r, &mounts))
            .collect();
        assert_eq!(
            msgs,
            [
                Msg::FsWatchReset,
                Msg::FsWatch {
                    id: 5,
                    dir: true,
                    path: "/Users/me/app".into()
                },
                Msg::FsWatch {
                    id: 6,
                    dir: false,
                    path: "/Users".into()
                },
                Msg::FsUnwatch { id: 5 },
            ]
        );
    }

    #[test]
    fn a_truncated_report_stops_parsing() {
        let mut buf = report(REPORT_WATCH, 0, 5, 44, b"/abc");
        buf.truncate(buf.len() - 8);
        assert!(parse_reports(&buf).is_empty());
    }

    #[test]
    fn events_are_padded_records() {
        let ev = |name: &str| FsEvent {
            id: 3,
            ino: 77,
            cookie: 9,
            op: FsOp::MovedTo,
            dir: true,
            name: name.into(),
        };
        let chunks = encode_events(&[ev("a"), ev(""), ev(&"x".repeat(256))]);
        assert_eq!(chunks.len(), 1);
        let c = &chunks[0];
        assert_eq!(c.len(), 32 + 24);
        assert_eq!(u64::from_le_bytes(c[0..8].try_into().unwrap()), 3);
        assert_eq!(u64::from_le_bytes(c[8..16].try_into().unwrap()), 77);
        assert_eq!(u32::from_le_bytes(c[16..20].try_into().unwrap()), 9);
        assert_eq!(u16::from_le_bytes(c[20..22].try_into().unwrap()), 6);
        assert_eq!((c[22], c[23], c[24]), (FLAG_DIR, 1, b'a'));
        assert_eq!(c[32 + 23], 0);
    }

    #[test]
    fn large_batches_are_split() {
        let events: Vec<FsEvent> = (0..50_000)
            .map(|i| FsEvent {
                id: 1,
                ino: i,
                cookie: 0,
                op: FsOp::Create,
                dir: false,
                name: format!("file-{i:08}"),
            })
            .collect();
        let chunks = encode_events(&events);
        assert!(chunks.len() > 1);
        assert!(
            chunks
                .iter()
                .all(|c| c.len() <= WRITE_MAX && c.len() % 8 == 0)
        );
        assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 50_000 * 40);
    }
}
