//! Host-guest control protocol.
//!
//! Frame: `len: u32 LE | kind: u16 LE | payload`, where `len` counts kind + payload.
//! Integers are little endian, strings are `u32 len | utf-8 bytes`.
//! The guest dials the host on [`CONTROL_PORT`] and sends [`Msg::Hello`] first;
//! the host refuses any [`PROTOCOL_VERSION`] other than its own.

use std::fmt;
use std::io::{self, Read, Write};

pub mod backoff;
pub mod fscache;
pub mod k3s_config;
pub mod k8s;
pub mod machine;

pub const PROTOCOL_VERSION: u32 = 3;
/// vsock port the guest dials on the host (CID 2) for the control channel.
pub const CONTROL_PORT: u32 = 1024;
/// Guest vsock port where the agent serves the Docker API (relayed to
/// `/run/docker.sock`). The host side is a Unix socket libkrun listens on.
pub const DOCKER_PORT: u32 = 1025;
/// The VM process sends the guest agent cache invalidations for the shares
/// on this port: frames of `u32` little-endian length plus records for
/// `/dev/kat_fsnotify` (see `kat_vmm::fscache`).
pub const FSCACHE_PORT: u32 = 1031;
/// Guest vsock port where the agent relays auto-forwarded container ports.
/// The host writes the port as `u16` little endian first; the agent then
/// connects to whatever container listens on it and copies both ways. The
/// host side is a Unix socket libkrun listens on.
pub const AUTO_FORWARD_PORT: u32 = 1032;
/// Guest vsock port of the NFS server that shows guest files on the Mac. The
/// host side is a Unix socket libkrun listens on for the macOS kernel only.
pub const NFS_PORT: u32 = 2049;
/// Guest vsock port of the Linux machine service (see [`machine`]). The host
/// side is a Unix socket libkrun listens on.
pub const MACHINE_PORT: u32 = 1050;
/// Guest vsock port of the Kubernetes (k3s) management service (see [`k8s`]).
/// The host side is a Unix socket libkrun listens on.
pub const K8S_PORT: u32 = 1051;
/// Guest vsock port relayed to the k3s Kubernetes API server (`127.0.0.1:6443`).
/// The host side is a Unix socket libkrun listens on.
pub const K8S_API_PORT: u32 = 1052;
pub const MAX_FRAME: usize = 1 << 20;

pub(crate) const HEADER: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    /// Guest → host, first message on the control channel.
    Hello(Hello),
    /// Host → guest: set the guest's realtime clock.
    SetTime {
        unix_ns: u64,
    },
    Ping {
        seq: u64,
    },
    Pong {
        seq: u64,
    },
    /// Host → guest: stop everything, sync and power off.
    Shutdown,
    /// Guest → host: a supervised service is up (for example `docker`).
    ServiceReady {
        name: String,
        /// `CLOCK_BOOTTIME` when the service became ready.
        at_ns: u64,
    },
    /// Guest → host: a supervised service failed to start or exited.
    ServiceFailed {
        name: String,
        error: String,
    },
    /// Guest → host: a socket in the guest's root network namespace listens
    /// on `port` on every address; forward the Mac's `127.0.0.1:port` to it.
    PortAdd {
        proto: PortProto,
        port: u16,
    },
    /// Guest → host: nothing listens on `port` any more; drop the forward.
    PortRemove {
        proto: PortProto,
        port: u16,
    },
    /// Guest → host: a container listens on `port` in its own network
    /// namespace and nothing in the root namespace does; forward the Mac's
    /// `127.0.0.1:port` to it through [`AUTO_FORWARD_PORT`].
    AutoForwardAdd {
        proto: PortProto,
        port: u16,
    },
    /// Guest → host: drop the auto-forward of `port`.
    AutoForwardRemove {
        proto: PortProto,
        port: u16,
    },
    /// Guest → host: a guest process watches this path with inotify; watch
    /// it on the host and send changes for watch `id`.
    FsWatch {
        id: u64,
        /// Whether the watched path is a directory (its children are watched).
        dir: bool,
        /// Absolute path in the guest.
        path: String,
    },
    /// Guest → host: watch `id` is gone.
    FsUnwatch {
        id: u64,
    },
    /// Guest → host: forget every watch; `FsWatch` for each current one follows.
    FsWatchReset,
    /// Host → guest: changes the host saw under watched paths.
    FsEvents(Vec<FsEvent>),
    /// Host → guest: discard the data disk's free space now (FITRIM), so
    /// the Mac gets it back even if online discard missed some.
    Trim,
    /// Guest → host: a trim finished; `bytes` is what the file system
    /// reported as trimmed, or `error` says why it failed.
    TrimDone {
        bytes: u64,
        error: String,
    },
    /// Host → guest: report resource usage with [`Msg::Stats`].
    StatsRequest,
    /// Guest → host: resource usage, in reply to [`Msg::StatsRequest`].
    Stats(GuestStats),
}

/// The guest's memory and CPU counters, from `/proc/meminfo` and `/proc/stat`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GuestStats {
    pub mem_total_kib: u64,
    pub mem_available_kib: u64,
    /// Sum of all CPUs' non-idle ticks (everything but idle and iowait).
    pub cpu_busy_ticks: u64,
    /// Sum of all CPUs' ticks.
    pub cpu_total_ticks: u64,
    pub cpus: u32,
}

/// One change for the guest kernel (`struct kat_fsn_event` in the guest
/// kernel's `include/linux/kat_fsnotify.h`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsEvent {
    /// The watch the change is reported to.
    pub id: u64,
    /// Host inode number of the object the change is about (the removed one
    /// for delete and moved-from), 0 if unknown.
    pub ino: u64,
    /// Pairs a moved-from with its moved-to; 0 otherwise.
    pub cookie: u32,
    pub op: FsOp,
    pub dir: bool,
    /// The changed child of a watched directory, or empty for the watched
    /// path itself.
    pub name: String,
}

/// What happened, numbered as the guest kernel's `KAT_FSN_*` ops.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FsOp {
    Create = 1,
    Delete = 2,
    Modify = 3,
    Attrib = 4,
    MovedFrom = 5,
    MovedTo = 6,
}

impl FsOp {
    fn from_u8(v: u8) -> Option<FsOp> {
        Some(match v {
            1 => FsOp::Create,
            2 => FsOp::Delete,
            3 => FsOp::Modify,
            4 => FsOp::Attrib,
            5 => FsOp::MovedFrom,
            6 => FsOp::MovedTo,
            _ => return None,
        })
    }
}

/// Transport protocol of a forwarded port, encoded as its IP protocol number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PortProto {
    Tcp,
    Udp,
}

impl PortProto {
    pub fn as_str(self) -> &'static str {
        match self {
            PortProto::Tcp => "tcp",
            PortProto::Udp => "udp",
        }
    }

    fn number(self) -> u8 {
        match self {
            PortProto::Tcp => 6,
            PortProto::Udp => 17,
        }
    }

    fn from_number(n: u8) -> Result<PortProto, ProtoError> {
        match n {
            6 => Ok(PortProto::Tcp),
            17 => Ok(PortProto::Udp),
            other => Err(ProtoError::InvalidValue("port protocol", other.into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub protocol: u32,
    /// `CLOCK_BOOTTIME` when PID 1 started.
    pub init_start_ns: u64,
    /// `CLOCK_BOOTTIME` when PID 1 was ready to serve.
    pub ready_ns: u64,
    pub mem_total_kib: u64,
    pub kernel_release: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProtoError {
    Truncated,
    FrameTooLarge(usize),
    UnknownKind(u16),
    TrailingBytes(usize),
    InvalidUtf8,
    InvalidValue(&'static str, u64),
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtoError::Truncated => write!(f, "truncated frame"),
            ProtoError::FrameTooLarge(n) => write!(f, "frame of {n} bytes exceeds {MAX_FRAME}"),
            ProtoError::UnknownKind(k) => write!(f, "unknown message kind {k}"),
            ProtoError::TrailingBytes(n) => write!(f, "{n} trailing bytes after message"),
            ProtoError::InvalidUtf8 => write!(f, "string is not valid utf-8"),
            ProtoError::InvalidValue(what, v) => write!(f, "invalid {what} {v}"),
        }
    }
}

impl std::error::Error for ProtoError {}

impl From<ProtoError> for io::Error {
    fn from(e: ProtoError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}

mod kind {
    pub const HELLO: u16 = 1;
    pub const SET_TIME: u16 = 2;
    pub const PING: u16 = 3;
    pub const PONG: u16 = 4;
    pub const SHUTDOWN: u16 = 5;
    pub const SERVICE_READY: u16 = 6;
    pub const SERVICE_FAILED: u16 = 7;
    pub const PORT_ADD: u16 = 20;
    pub const PORT_REMOVE: u16 = 21;
    pub const FS_WATCH: u16 = 0x301;
    pub const FS_UNWATCH: u16 = 0x302;
    pub const FS_WATCH_RESET: u16 = 0x303;
    pub const FS_EVENTS: u16 = 0x304;
    pub const STATS_REQUEST: u16 = 0x401;
    pub const STATS: u16 = 0x402;
    pub const TRIM: u16 = 0x501;
    pub const TRIM_DONE: u16 = 0x502;
    pub const AUTO_FORWARD_ADD: u16 = 0x601;
    pub const AUTO_FORWARD_REMOVE: u16 = 0x602;
}

impl Msg {
    /// Encode as a complete frame, header included.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER];
        let k = match self {
            Msg::Hello(h) => {
                put_u32(&mut buf, h.protocol);
                put_u64(&mut buf, h.init_start_ns);
                put_u64(&mut buf, h.ready_ns);
                put_u64(&mut buf, h.mem_total_kib);
                put_str(&mut buf, &h.kernel_release);
                kind::HELLO
            }
            Msg::SetTime { unix_ns } => {
                put_u64(&mut buf, *unix_ns);
                kind::SET_TIME
            }
            Msg::Ping { seq } => {
                put_u64(&mut buf, *seq);
                kind::PING
            }
            Msg::Pong { seq } => {
                put_u64(&mut buf, *seq);
                kind::PONG
            }
            Msg::Shutdown => kind::SHUTDOWN,
            Msg::ServiceReady { name, at_ns } => {
                put_str(&mut buf, name);
                put_u64(&mut buf, *at_ns);
                kind::SERVICE_READY
            }
            Msg::ServiceFailed { name, error } => {
                put_str(&mut buf, name);
                put_str(&mut buf, error);
                kind::SERVICE_FAILED
            }
            Msg::PortAdd { proto, port } => {
                buf.push(proto.number());
                buf.extend_from_slice(&port.to_le_bytes());
                kind::PORT_ADD
            }
            Msg::PortRemove { proto, port } => {
                buf.push(proto.number());
                buf.extend_from_slice(&port.to_le_bytes());
                kind::PORT_REMOVE
            }
            Msg::AutoForwardAdd { proto, port } => {
                buf.push(proto.number());
                buf.extend_from_slice(&port.to_le_bytes());
                kind::AUTO_FORWARD_ADD
            }
            Msg::AutoForwardRemove { proto, port } => {
                buf.push(proto.number());
                buf.extend_from_slice(&port.to_le_bytes());
                kind::AUTO_FORWARD_REMOVE
            }
            Msg::FsWatch { id, dir, path } => {
                put_u64(&mut buf, *id);
                buf.push(u8::from(*dir));
                put_str(&mut buf, path);
                kind::FS_WATCH
            }
            Msg::FsUnwatch { id } => {
                put_u64(&mut buf, *id);
                kind::FS_UNWATCH
            }
            Msg::FsWatchReset => kind::FS_WATCH_RESET,
            Msg::FsEvents(events) => {
                put_u32(&mut buf, events.len() as u32);
                for e in events {
                    put_u64(&mut buf, e.id);
                    put_u64(&mut buf, e.ino);
                    put_u32(&mut buf, e.cookie);
                    buf.push(e.op as u8);
                    buf.push(u8::from(e.dir));
                    put_str(&mut buf, &e.name);
                }
                kind::FS_EVENTS
            }
            Msg::Trim => kind::TRIM,
            Msg::TrimDone { bytes, error } => {
                put_u64(&mut buf, *bytes);
                put_str(&mut buf, error);
                kind::TRIM_DONE
            }
            Msg::StatsRequest => kind::STATS_REQUEST,
            Msg::Stats(st) => {
                put_u64(&mut buf, st.mem_total_kib);
                put_u64(&mut buf, st.mem_available_kib);
                put_u64(&mut buf, st.cpu_busy_ticks);
                put_u64(&mut buf, st.cpu_total_ticks);
                put_u32(&mut buf, st.cpus);
                kind::STATS
            }
        };
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf[4..6].copy_from_slice(&k.to_le_bytes());
        buf
    }

    /// Decode the body of a frame: `kind` and the payload that follows it.
    pub fn decode(kind: u16, payload: &[u8]) -> Result<Msg, ProtoError> {
        let mut r = Cursor { buf: payload };
        let msg = match kind {
            kind::HELLO => Msg::Hello(Hello {
                protocol: r.u32()?,
                init_start_ns: r.u64()?,
                ready_ns: r.u64()?,
                mem_total_kib: r.u64()?,
                kernel_release: r.string()?,
            }),
            kind::SET_TIME => Msg::SetTime { unix_ns: r.u64()? },
            kind::PING => Msg::Ping { seq: r.u64()? },
            kind::PONG => Msg::Pong { seq: r.u64()? },
            kind::SHUTDOWN => Msg::Shutdown,
            kind::SERVICE_READY => Msg::ServiceReady {
                name: r.string()?,
                at_ns: r.u64()?,
            },
            kind::SERVICE_FAILED => Msg::ServiceFailed {
                name: r.string()?,
                error: r.string()?,
            },
            kind::PORT_ADD => Msg::PortAdd {
                proto: PortProto::from_number(r.u8()?)?,
                port: r.u16()?,
            },
            kind::PORT_REMOVE => Msg::PortRemove {
                proto: PortProto::from_number(r.u8()?)?,
                port: r.u16()?,
            },
            kind::AUTO_FORWARD_ADD => Msg::AutoForwardAdd {
                proto: PortProto::from_number(r.u8()?)?,
                port: r.u16()?,
            },
            kind::AUTO_FORWARD_REMOVE => Msg::AutoForwardRemove {
                proto: PortProto::from_number(r.u8()?)?,
                port: r.u16()?,
            },
            kind::FS_WATCH => Msg::FsWatch {
                id: r.u64()?,
                dir: r.u8()? != 0,
                path: r.string()?,
            },
            kind::FS_UNWATCH => Msg::FsUnwatch { id: r.u64()? },
            kind::FS_WATCH_RESET => Msg::FsWatchReset,
            kind::FS_EVENTS => {
                let n = r.u32()? as usize;
                // Each event takes at least 26 bytes, so a bogus count fails
                // here instead of reserving memory for it.
                if n > r.buf.len() / 26 {
                    return Err(ProtoError::Truncated);
                }
                let mut events = Vec::with_capacity(n);
                for _ in 0..n {
                    let (id, ino, cookie) = (r.u64()?, r.u64()?, r.u32()?);
                    let op = r.u8()?;
                    events.push(FsEvent {
                        id,
                        ino,
                        cookie,
                        op: FsOp::from_u8(op).ok_or(ProtoError::UnknownKind(kind))?,
                        dir: r.u8()? != 0,
                        name: r.string()?,
                    });
                }
                Msg::FsEvents(events)
            }
            kind::TRIM => Msg::Trim,
            kind::TRIM_DONE => Msg::TrimDone {
                bytes: r.u64()?,
                error: r.string()?,
            },
            kind::STATS_REQUEST => Msg::StatsRequest,
            kind::STATS => Msg::Stats(GuestStats {
                mem_total_kib: r.u64()?,
                mem_available_kib: r.u64()?,
                cpu_busy_ticks: r.u64()?,
                cpu_total_ticks: r.u64()?,
                cpus: r.u32()?,
            }),
            other => return Err(ProtoError::UnknownKind(other)),
        };
        if !r.buf.is_empty() {
            return Err(ProtoError::TrailingBytes(r.buf.len()));
        }
        Ok(msg)
    }
}

pub fn write_msg<W: Write>(w: &mut W, msg: &Msg) -> io::Result<()> {
    let frame = msg.encode();
    let len = frame.len() - 4;
    if len > MAX_FRAME {
        return Err(ProtoError::FrameTooLarge(len).into());
    }
    w.write_all(&frame)?;
    w.flush()
}

/// Read a 4-byte little-endian frame length. Returns `Ok(None)` on a clean
/// 0-byte EOF at a frame boundary, and `Err(UnexpectedEof)` if 1–3 bytes were
/// read before EOF.
pub(crate) fn read_len<R: Read>(r: &mut R) -> io::Result<Option<usize>> {
    let mut buf = [0u8; 4];
    let mut n = 0;
    while n < 4 {
        match r.read(&mut buf[n..]) {
            Ok(0) if n == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated frame length",
                ));
            }
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(Some(u32::from_le_bytes(buf) as usize))
}

/// Read one frame. Returns `Ok(None)` on a clean EOF at a frame boundary.
pub fn read_msg<R: Read>(r: &mut R) -> io::Result<Option<Msg>> {
    let Some(len) = read_len(r)? else {
        return Ok(None);
    };
    if len < 2 {
        return Err(ProtoError::Truncated.into());
    }
    if len > MAX_FRAME {
        return Err(ProtoError::FrameTooLarge(len).into());
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    let kind = u16::from_le_bytes([body[0], body[1]]);
    Ok(Some(Msg::decode(kind, &body[2..])?))
}

pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_u32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

pub(crate) struct Cursor<'a> {
    pub(crate) buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], ProtoError> {
        if self.buf.len() < n {
            return Err(ProtoError::Truncated);
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, ProtoError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, ProtoError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, ProtoError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, ProtoError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub(crate) fn string(&mut self) -> Result<String, ProtoError> {
        let n = self.u32()? as usize;
        if n > MAX_FRAME {
            return Err(ProtoError::FrameTooLarge(n));
        }
        let bytes = self.take(n)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| ProtoError::InvalidUtf8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all() -> Vec<Msg> {
        vec![
            Msg::Hello(Hello {
                protocol: PROTOCOL_VERSION,
                init_start_ns: 8_000_000,
                ready_ns: 8_400_000,
                mem_total_kib: 4 << 20,
                kernel_release: "6.18.5-katamaran".into(),
            }),
            Msg::SetTime {
                unix_ns: 1_700_000_000_000_000_000,
            },
            Msg::Ping { seq: 7 },
            Msg::Pong { seq: u64::MAX },
            Msg::Shutdown,
            Msg::ServiceReady {
                name: "docker".into(),
                at_ns: 1_234,
            },
            Msg::ServiceFailed {
                name: "docker".into(),
                error: "exited with status 1".into(),
            },
            Msg::PortAdd {
                proto: PortProto::Tcp,
                port: 8080,
            },
            Msg::PortRemove {
                proto: PortProto::Udp,
                port: u16::MAX,
            },
            Msg::AutoForwardAdd {
                proto: PortProto::Tcp,
                port: 8000,
            },
            Msg::AutoForwardRemove {
                proto: PortProto::Tcp,
                port: 1,
            },
            Msg::FsWatch {
                id: 42,
                dir: true,
                path: "/Users/me/app".into(),
            },
            Msg::FsUnwatch { id: 42 },
            Msg::FsWatchReset,
            Msg::FsEvents(vec![
                FsEvent {
                    id: 42,
                    ino: 1234,
                    cookie: 0,
                    op: FsOp::Create,
                    dir: false,
                    name: "main.rs".into(),
                },
                FsEvent {
                    id: 7,
                    ino: 0,
                    cookie: 9,
                    op: FsOp::MovedFrom,
                    dir: true,
                    name: String::new(),
                },
            ]),
            Msg::FsEvents(vec![]),
            Msg::Trim,
            Msg::TrimDone {
                bytes: 5 << 30,
                error: String::new(),
            },
            Msg::StatsRequest,
            Msg::Stats(GuestStats {
                mem_total_kib: 4 << 20,
                mem_available_kib: 3 << 20,
                cpu_busy_ticks: 12_345,
                cpu_total_ticks: u64::MAX,
                cpus: 4,
            }),
        ]
    }

    #[test]
    fn round_trip_through_stream() {
        let mut wire = Vec::new();
        for m in all() {
            write_msg(&mut wire, &m).unwrap();
        }
        let mut r = wire.as_slice();
        for m in all() {
            assert_eq!(read_msg(&mut r).unwrap(), Some(m));
        }
        assert_eq!(read_msg(&mut r).unwrap(), None);
    }

    #[test]
    fn header_counts_kind_and_payload() {
        let f = Msg::Ping { seq: 1 }.encode();
        assert_eq!(
            u32::from_le_bytes(f[0..4].try_into().unwrap()) as usize,
            f.len() - 4
        );
        assert_eq!(u16::from_le_bytes([f[4], f[5]]), kind::PING);
    }

    #[test]
    fn rejects_truncated_payload() {
        assert_eq!(
            Msg::decode(kind::PING, &[1, 2, 3]),
            Err(ProtoError::Truncated)
        );
    }

    #[test]
    fn rejects_trailing_bytes() {
        assert_eq!(
            Msg::decode(kind::SHUTDOWN, &[0]),
            Err(ProtoError::TrailingBytes(1))
        );
    }

    #[test]
    fn rejects_unknown_kind() {
        assert_eq!(Msg::decode(999, &[]), Err(ProtoError::UnknownKind(999)));
    }

    #[test]
    fn rejects_oversized_frame() {
        let mut wire = ((MAX_FRAME + 1) as u32).to_le_bytes().to_vec();
        wire.extend_from_slice(&[0, 0]);
        let err = read_msg(&mut wire.as_slice()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_frame_shorter_than_kind() {
        let wire = 1u32.to_le_bytes();
        assert!(read_msg(&mut wire.as_slice()).is_err());
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut payload = Vec::new();
        put_u32(&mut payload, 1);
        put_u64(&mut payload, 0);
        put_u64(&mut payload, 0);
        put_u64(&mut payload, 0);
        put_u32(&mut payload, 2);
        payload.extend_from_slice(&[0xff, 0xfe]);
        assert_eq!(
            Msg::decode(kind::HELLO, &payload),
            Err(ProtoError::InvalidUtf8)
        );
    }

    #[test]
    fn rejects_bogus_event_count_and_op() {
        let mut payload = Vec::new();
        put_u32(&mut payload, u32::MAX);
        assert_eq!(
            Msg::decode(kind::FS_EVENTS, &payload),
            Err(ProtoError::Truncated)
        );
        let mut frame = Msg::FsEvents(vec![FsEvent {
            id: 1,
            ino: 2,
            cookie: 0,
            op: FsOp::Modify,
            dir: false,
            name: "x".into(),
        }])
        .encode();
        frame[6 + 4 + 8 + 8 + 4] = 99;
        assert!(Msg::decode(kind::FS_EVENTS, &frame[6..]).is_err());
    }

    #[test]
    fn rejects_unknown_port_protocol() {
        assert_eq!(
            Msg::decode(kind::PORT_ADD, &[1, 0x90, 0x1f]),
            Err(ProtoError::InvalidValue("port protocol", 1))
        );
    }

    #[test]
    fn eof_inside_frame_is_an_error() {
        let f = Msg::Ping { seq: 1 }.encode();
        assert!(read_msg(&mut &f[..f.len() - 1]).is_err());
    }

    #[test]
    fn truncated_length_header_returns_unexpected_eof() {
        assert_eq!(read_msg(&mut [].as_slice()).unwrap(), None);
        for n in 1..=3 {
            let bytes = [1u8, 2, 3];
            let err = read_msg(&mut &bytes[..n]).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "len {n}");
        }
    }

    #[test]
    fn write_msg_rejects_oversized_frame() {
        let msg = Msg::ServiceFailed {
            name: "x".into(),
            error: "a".repeat(MAX_FRAME),
        };
        let mut out = Vec::new();
        let err = write_msg(&mut out, &msg).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(out.is_empty());
    }
}
