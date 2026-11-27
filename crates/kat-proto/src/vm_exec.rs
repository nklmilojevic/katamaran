//! VM root shell and exec protocol, spoken on [`crate::VM_EXEC_PORT`].
//!
//! The host opens one connection per session (same framing as [`crate::Msg`],
//! kinds 0x901-0x91f):
//!
//! 1. Optionally, host sends `Ping` and guest replies `Pong` as a
//!    side-effect-free readiness probe before submitting a command.
//! 2. Host sends `Exec(ExecSpec)` once.
//! 3. Guest validates and spawns the process in the VM's root namespaces, then
//!    answers `Started` (or `Error` if spawning failed).
//! 4. Host streams stdin as `Data`, `DataEnd`, plus `Resize` and `Signal`.
//!    `DataEnd` closes piped stdin. In a PTY session it triggers canonical EOF
//!    using the terminal's configured `VEOF` character when the line discipline
//!    is in canonical mode (`ICANON`) — flushing any pending partial line
//!    before delivering EOF — and injects no control byte in noncanonical/raw
//!    mode, where `DataEnd` cannot signal EOF to the guest process.
//!    After reading a `Signal` frame and signaling the guest process group, the
//!    guest queues `SignalAck` ahead of newly queued output frames on the
//!    guest-to-host stream so the host knows a forwarded `SIGINT` crossed any
//!    backpressured stdin queue. Because `SignalAck` still follows output frames
//!    already in flight, the host should suspend its `SignalAck` delivery
//!    timeout while blocked writing to local stdout/stderr so slow local output
//!    is not mistaken for failed signal delivery.
//! 5. Guest streams `Stdout` / `Stderr`, then `Exit { code }` (or `Error`).

use std::io::{self, Read, Write};

pub use crate::machine::Tty;
use crate::{Cursor, HEADER, MAX_FRAME, ProtoError, put_str, put_u32, read_len};

/// Payload bytes per `Data`/`Stdout`/`Stderr` frame.
pub const CHUNK: usize = 64 << 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VmExecMsg {
    Exec(ExecSpec),
    Data(Vec<u8>),
    DataEnd,
    Resize {
        rows: u16,
        cols: u16,
    },
    Signal {
        signal: i32,
    },
    Started,
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit {
        code: i32,
    },
    Error(String),
    /// Side-effect-free readiness probe sent before [`VmExecMsg::Exec`].
    Ping,
    /// Readiness reply to [`VmExecMsg::Ping`], and also used by the guest as a
    /// no-op keepalive probe during backpressured sessions (ignored by the host
    /// output relay).
    Pong,
    /// Guest acknowledgment sent after reading a [`VmExecMsg::Signal`] frame
    /// and signaling the target process group.
    SignalAck,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecSpec {
    /// Working directory inside the VM; empty defaults to `/root` (or `/`).
    pub cwd: String,
    /// Extra environment variables (`KEY`, `VALUE`).
    pub env: Vec<(String, String)>,
    /// Command and arguments; empty launches `/bin/sh`.
    pub argv: Vec<String>,
    pub tty: Option<Tty>,
}

mod kind {
    pub const EXEC: u16 = 0x901;
    pub const DATA: u16 = 0x902;
    pub const DATA_END: u16 = 0x903;
    pub const RESIZE: u16 = 0x904;
    pub const SIGNAL: u16 = 0x905;
    pub const STARTED: u16 = 0x906;
    pub const STDOUT: u16 = 0x907;
    pub const STDERR: u16 = 0x908;
    pub const EXIT: u16 = 0x909;
    pub const ERROR: u16 = 0x90a;
    pub const PING: u16 = 0x90b;
    pub const PONG: u16 = 0x90c;
    pub const SIGNAL_ACK: u16 = 0x90d;
}

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

impl VmExecMsg {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER];
        let b = &mut buf;
        let k = match self {
            VmExecMsg::Exec(e) => {
                put_str(b, &e.cwd);
                put_u32(b, e.env.len() as u32);
                for (k, v) in &e.env {
                    put_str(b, k);
                    put_str(b, v);
                }
                put_u32(b, e.argv.len() as u32);
                for a in &e.argv {
                    put_str(b, a);
                }
                match &e.tty {
                    None => b.push(0),
                    Some(t) => {
                        b.push(1);
                        put_str(b, &t.term);
                        b.extend_from_slice(&t.rows.to_le_bytes());
                        b.extend_from_slice(&t.cols.to_le_bytes());
                    }
                }
                kind::EXEC
            }
            VmExecMsg::Data(d) => {
                put_bytes(b, d);
                kind::DATA
            }
            VmExecMsg::DataEnd => kind::DATA_END,
            VmExecMsg::Resize { rows, cols } => {
                b.extend_from_slice(&rows.to_le_bytes());
                b.extend_from_slice(&cols.to_le_bytes());
                kind::RESIZE
            }
            VmExecMsg::Signal { signal } => {
                put_u32(b, *signal as u32);
                kind::SIGNAL
            }
            VmExecMsg::Started => kind::STARTED,
            VmExecMsg::Stdout(d) => {
                put_bytes(b, d);
                kind::STDOUT
            }
            VmExecMsg::Stderr(d) => {
                put_bytes(b, d);
                kind::STDERR
            }
            VmExecMsg::Exit { code } => {
                put_u32(b, *code as u32);
                kind::EXIT
            }
            VmExecMsg::Error(e) => {
                put_str(b, e);
                kind::ERROR
            }
            VmExecMsg::Ping => kind::PING,
            VmExecMsg::Pong => kind::PONG,
            VmExecMsg::SignalAck => kind::SIGNAL_ACK,
        };
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf[4..6].copy_from_slice(&k.to_le_bytes());
        buf
    }

    pub fn decode(k: u16, payload: &[u8]) -> Result<VmExecMsg, ProtoError> {
        let mut r = Cursor { buf: payload };
        let r = &mut r;
        let count = |r: &mut Cursor, min: usize| -> Result<usize, ProtoError> {
            let n = r.u32()? as usize;
            // A bogus count fails here instead of reserving memory for it.
            if n > r.buf.len() / min.max(1) {
                return Err(ProtoError::Truncated);
            }
            Ok(n)
        };
        let bytes = |r: &mut Cursor| -> Result<Vec<u8>, ProtoError> {
            let n = r.u32()? as usize;
            Ok(r.take(n)?.to_vec())
        };
        let msg = match k {
            kind::EXEC => {
                let cwd = r.string()?;
                let n = count(r, 8)?;
                let mut env = Vec::with_capacity(n);
                for _ in 0..n {
                    env.push((r.string()?, r.string()?));
                }
                let n = count(r, 4)?;
                let mut argv = Vec::with_capacity(n);
                for _ in 0..n {
                    argv.push(r.string()?);
                }
                let tty = match r.u8()? {
                    0 => None,
                    1 => Some(Tty {
                        term: r.string()?,
                        rows: r.u16()?,
                        cols: r.u16()?,
                    }),
                    v => return Err(ProtoError::InvalidValue("tty flag", v.into())),
                };
                VmExecMsg::Exec(ExecSpec {
                    cwd,
                    env,
                    argv,
                    tty,
                })
            }
            kind::DATA => VmExecMsg::Data(bytes(r)?),
            kind::DATA_END => VmExecMsg::DataEnd,
            kind::RESIZE => VmExecMsg::Resize {
                rows: r.u16()?,
                cols: r.u16()?,
            },
            kind::SIGNAL => VmExecMsg::Signal {
                signal: r.u32()? as i32,
            },
            kind::STARTED => VmExecMsg::Started,
            kind::STDOUT => VmExecMsg::Stdout(bytes(r)?),
            kind::STDERR => VmExecMsg::Stderr(bytes(r)?),
            kind::EXIT => VmExecMsg::Exit {
                code: r.u32()? as i32,
            },
            kind::ERROR => VmExecMsg::Error(r.string()?),
            kind::PING => VmExecMsg::Ping,
            kind::PONG => VmExecMsg::Pong,
            kind::SIGNAL_ACK => VmExecMsg::SignalAck,
            other => return Err(ProtoError::UnknownKind(other)),
        };
        if !r.buf.is_empty() {
            return Err(ProtoError::TrailingBytes(r.buf.len()));
        }
        Ok(msg)
    }
}

pub fn write<W: Write>(w: &mut W, msg: &VmExecMsg) -> io::Result<()> {
    let frame = msg.encode();
    let len = frame.len() - 4;
    if len > MAX_FRAME {
        return Err(ProtoError::FrameTooLarge(len).into());
    }
    w.write_all(&frame)?;
    w.flush()
}

/// Read one frame. Returns `Ok(None)` on a clean EOF at a frame boundary.
pub fn read<R: Read>(r: &mut R) -> io::Result<Option<VmExecMsg>> {
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
    let k = u16::from_le_bytes([body[0], body[1]]);
    Ok(Some(VmExecMsg::decode(k, &body[2..])?))
}

/// Whether `name` is safe to pass as an environment variable name to `Command::env`.
pub fn env_name_ok(name: &str) -> bool {
    !name.is_empty() && !name.contains(['=', '\0'])
}

/// Whether `value` is safe to pass as an argument, cwd, or environment value.
pub fn value_ok(value: &str) -> bool {
    !value.contains('\0')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all() -> Vec<VmExecMsg> {
        vec![
            VmExecMsg::Exec(ExecSpec {
                cwd: "/var/log".into(),
                env: vec![
                    ("LANG".into(), "C.UTF-8".into()),
                    ("FOO".into(), "a=b".into()),
                ],
                argv: vec!["sysctl".into(), "-n".into(), "fs.nr_open".into()],
                tty: Some(Tty {
                    term: "xterm-256color".into(),
                    rows: 40,
                    cols: 120,
                }),
            }),
            VmExecMsg::Exec(ExecSpec::default()),
            VmExecMsg::Data(vec![1, 2, 3]),
            VmExecMsg::Data(vec![]),
            VmExecMsg::DataEnd,
            VmExecMsg::Resize { rows: 24, cols: 80 },
            VmExecMsg::Signal { signal: 15 },
            VmExecMsg::Started,
            VmExecMsg::Stdout(b"out".to_vec()),
            VmExecMsg::Stderr(b"err".to_vec()),
            VmExecMsg::Exit { code: 42 },
            VmExecMsg::Exit { code: -1 },
            VmExecMsg::Error("exec failed".into()),
            VmExecMsg::Ping,
            VmExecMsg::Pong,
            VmExecMsg::SignalAck,
        ]
    }

    #[test]
    fn every_message_round_trips() {
        let mut wire = Vec::new();
        for m in all() {
            write(&mut wire, &m).unwrap();
        }
        let mut r = wire.as_slice();
        for m in all() {
            assert_eq!(read(&mut r).unwrap(), Some(m));
        }
        assert_eq!(read(&mut r).unwrap(), None);
    }

    #[test]
    fn kinds_stay_in_the_assigned_range() {
        for m in all() {
            let frame = m.encode();
            let k = u16::from_le_bytes([frame[4], frame[5]]);
            assert!((0x901..=0x91f).contains(&k), "{m:?}: {k:#x}");
        }
    }

    #[test]
    fn truncated_and_bogus_frames_fail() {
        for m in all() {
            let frame = m.encode();
            let body = &frame[6..];
            let k = u16::from_le_bytes([frame[4], frame[5]]);
            for cut in 0..body.len() {
                assert!(
                    VmExecMsg::decode(k, &body[..cut]).is_err(),
                    "{m:?} cut {cut}"
                );
            }
        }
        // Bogus env count.
        let mut bogus_env = Vec::new();
        put_str(&mut bogus_env, "");
        put_u32(&mut bogus_env, u32::MAX);
        assert_eq!(
            VmExecMsg::decode(kind::EXEC, &bogus_env),
            Err(ProtoError::Truncated)
        );
        // Bogus argv count.
        let mut bogus_argv = Vec::new();
        put_str(&mut bogus_argv, "");
        put_u32(&mut bogus_argv, 0);
        put_u32(&mut bogus_argv, u32::MAX);
        assert_eq!(
            VmExecMsg::decode(kind::EXEC, &bogus_argv),
            Err(ProtoError::Truncated)
        );
        // Invalid tty flag.
        let mut bad_tty = Vec::new();
        put_str(&mut bad_tty, "");
        put_u32(&mut bad_tty, 0);
        put_u32(&mut bad_tty, 0);
        bad_tty.push(2);
        assert_eq!(
            VmExecMsg::decode(kind::EXEC, &bad_tty),
            Err(ProtoError::InvalidValue("tty flag", 2))
        );
        assert_eq!(
            VmExecMsg::decode(0x91f, &[]),
            Err(ProtoError::UnknownKind(0x91f))
        );
        assert_eq!(
            VmExecMsg::decode(kind::STARTED, &[0]),
            Err(ProtoError::TrailingBytes(1))
        );
    }

    #[test]
    fn truncated_length_header_and_oversized_write() {
        assert_eq!(read(&mut [].as_slice()).unwrap(), None);
        for n in 1..=3 {
            let bytes = [1u8, 2, 3];
            let err = read(&mut &bytes[..n]).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "len {n}");
        }
        // Feed only the length header so EOF cannot be mistaken for size rejection.
        let header = ((MAX_FRAME + 1) as u32).to_le_bytes();
        let err = read(&mut header.as_slice()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            err.get_ref().and_then(|e| e.downcast_ref::<ProtoError>()),
            Some(&ProtoError::FrameTooLarge(MAX_FRAME + 1))
        );
        let short_header = 1u32.to_le_bytes();
        let err = read(&mut short_header.as_slice()).unwrap_err();
        assert_eq!(
            err.get_ref().and_then(|e| e.downcast_ref::<ProtoError>()),
            Some(&ProtoError::Truncated)
        );
        let mut out = Vec::new();
        let err = write(&mut out, &VmExecMsg::Error("e".repeat(MAX_FRAME))).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(out.is_empty());
    }

    #[test]
    fn validates_env_names_and_values() {
        assert!(env_name_ok("FOO") && env_name_ok("LC_ALL") && env_name_ok("_1"));
        assert!(!env_name_ok("") && !env_name_ok("A=B") && !env_name_ok("A\0B"));
        assert!(value_ok("") && value_ok("a=b") && value_ok("/var/log"));
        assert!(!value_ok("a\0b"));
    }
}
