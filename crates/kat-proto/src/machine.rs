//! Linux machine protocol, spoken on [`crate::MACHINE_PORT`].
//!
//! The host opens one connection per request and sends one request frame
//! (same framing as [`crate::Msg`], kinds 0x701–0x71f):
//!
//! | request          | then                                                         | guest answers                     |
//! |------------------|--------------------------------------------------------------|-----------------------------------|
//! | `List`, `Info`   | –                                                            | `Machines`, or `Error`            |
//! | `Create`         | the image (`root.tar.xz`) as `Data` frames, then `DataEnd`   | `Progress`…, then `Done`/`Error`  |
//! | `Start`, `Stop`, `Delete`, `SetDefault` | –                                     | `Progress`…, then `Done`/`Error`  |
//! | `Exec`           | stdin as `Data`, `DataEnd`, `Resize`, `Signal`               | `Stdout`/`Stderr`…, then `Exit` (or `Error` if it never ran) |
//! | `Connect`        | `Data`… (`DataEnd` closes the write side)                    | `Done` then `Data`…, `DataEnd`; or `Error` |
//!
//! An empty machine name means the default machine; an empty `Exec` user
//! means the machine's default user (the Mac user it was created for).

use std::io::{self, Read, Write};

use crate::{Cursor, HEADER, MAX_FRAME, ProtoError, put_str, put_u32, put_u64, read_len};

/// Payload bytes per `Data`/`Stdout`/`Stderr` frame.
pub const CHUNK: usize = 64 << 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MachineMsg {
    List,
    Info { name: String },
    Create(CreateSpec),
    Start { name: String },
    Stop { name: String },
    Delete { name: String },
    SetDefault { name: String },
    Exec(ExecSpec),
    Connect { name: String, port: u16 },
    Data(Vec<u8>),
    DataEnd,
    Resize { rows: u16, cols: u16 },
    Signal { signal: i32 },
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit { code: i32 },
    Machines(Vec<MachineInfo>),
    Done,
    Error(String),
    Progress(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateSpec {
    pub name: String,
    pub distro: String,
    pub release: String,
    /// The image's build serial (for `info`).
    pub image: String,
    /// The Mac user to create, with the same uid.
    pub user: String,
    pub uid: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecSpec {
    pub machine: String,
    pub user: String,
    /// Start here if it exists in the machine, else in the user's home.
    pub cwd: String,
    pub env: Vec<(String, String)>,
    /// Empty: the user's login shell.
    pub argv: Vec<String>,
    pub tty: Option<Tty>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tty {
    pub term: String,
    pub rows: u16,
    pub cols: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MachineInfo {
    pub name: String,
    pub distro: String,
    pub release: String,
    pub image: String,
    /// `running`, `stopped`, `starting` or `stopping`.
    pub state: String,
    /// IPv4 address on the machine bridge, empty when stopped.
    pub ip: String,
    pub user: String,
    pub uid: u32,
    pub default: bool,
    /// Whether it starts with the VM (it was running when last left).
    pub autostart: bool,
    pub created_unix: u64,
}

mod kind {
    pub const LIST: u16 = 0x701;
    pub const INFO: u16 = 0x702;
    pub const CREATE: u16 = 0x703;
    pub const START: u16 = 0x704;
    pub const STOP: u16 = 0x705;
    pub const DELETE: u16 = 0x706;
    pub const SET_DEFAULT: u16 = 0x707;
    pub const EXEC: u16 = 0x708;
    pub const CONNECT: u16 = 0x709;
    pub const DATA: u16 = 0x70a;
    pub const DATA_END: u16 = 0x70b;
    pub const RESIZE: u16 = 0x70c;
    pub const SIGNAL: u16 = 0x70d;
    pub const STDOUT: u16 = 0x70e;
    pub const STDERR: u16 = 0x70f;
    pub const EXIT: u16 = 0x710;
    pub const MACHINES: u16 = 0x711;
    pub const DONE: u16 = 0x712;
    pub const ERROR: u16 = 0x713;
    pub const PROGRESS: u16 = 0x714;
}

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

impl MachineMsg {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER];
        let b = &mut buf;
        let k = match self {
            MachineMsg::List => kind::LIST,
            MachineMsg::Info { name } => {
                put_str(b, name);
                kind::INFO
            }
            MachineMsg::Create(c) => {
                for s in [&c.name, &c.distro, &c.release, &c.image, &c.user] {
                    put_str(b, s);
                }
                put_u32(b, c.uid);
                kind::CREATE
            }
            MachineMsg::Start { name } => {
                put_str(b, name);
                kind::START
            }
            MachineMsg::Stop { name } => {
                put_str(b, name);
                kind::STOP
            }
            MachineMsg::Delete { name } => {
                put_str(b, name);
                kind::DELETE
            }
            MachineMsg::SetDefault { name } => {
                put_str(b, name);
                kind::SET_DEFAULT
            }
            MachineMsg::Exec(e) => {
                put_str(b, &e.machine);
                put_str(b, &e.user);
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
            MachineMsg::Connect { name, port } => {
                put_str(b, name);
                b.extend_from_slice(&port.to_le_bytes());
                kind::CONNECT
            }
            MachineMsg::Data(d) => {
                put_bytes(b, d);
                kind::DATA
            }
            MachineMsg::DataEnd => kind::DATA_END,
            MachineMsg::Resize { rows, cols } => {
                b.extend_from_slice(&rows.to_le_bytes());
                b.extend_from_slice(&cols.to_le_bytes());
                kind::RESIZE
            }
            MachineMsg::Signal { signal } => {
                put_u32(b, *signal as u32);
                kind::SIGNAL
            }
            MachineMsg::Stdout(d) => {
                put_bytes(b, d);
                kind::STDOUT
            }
            MachineMsg::Stderr(d) => {
                put_bytes(b, d);
                kind::STDERR
            }
            MachineMsg::Exit { code } => {
                put_u32(b, *code as u32);
                kind::EXIT
            }
            MachineMsg::Machines(list) => {
                put_u32(b, list.len() as u32);
                for m in list {
                    for s in [
                        &m.name, &m.distro, &m.release, &m.image, &m.state, &m.ip, &m.user,
                    ] {
                        put_str(b, s);
                    }
                    put_u32(b, m.uid);
                    b.push(u8::from(m.default));
                    b.push(u8::from(m.autostart));
                    put_u64(b, m.created_unix);
                }
                kind::MACHINES
            }
            MachineMsg::Done => kind::DONE,
            MachineMsg::Error(e) => {
                put_str(b, e);
                kind::ERROR
            }
            MachineMsg::Progress(p) => {
                put_str(b, p);
                kind::PROGRESS
            }
        };
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf[4..6].copy_from_slice(&k.to_le_bytes());
        buf
    }

    pub fn decode(k: u16, payload: &[u8]) -> Result<MachineMsg, ProtoError> {
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
            kind::LIST => MachineMsg::List,
            kind::INFO => MachineMsg::Info { name: r.string()? },
            kind::CREATE => MachineMsg::Create(CreateSpec {
                name: r.string()?,
                distro: r.string()?,
                release: r.string()?,
                image: r.string()?,
                user: r.string()?,
                uid: r.u32()?,
            }),
            kind::START => MachineMsg::Start { name: r.string()? },
            kind::STOP => MachineMsg::Stop { name: r.string()? },
            kind::DELETE => MachineMsg::Delete { name: r.string()? },
            kind::SET_DEFAULT => MachineMsg::SetDefault { name: r.string()? },
            kind::EXEC => {
                let (machine, user, cwd) = (r.string()?, r.string()?, r.string()?);
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
                MachineMsg::Exec(ExecSpec {
                    machine,
                    user,
                    cwd,
                    env,
                    argv,
                    tty,
                })
            }
            kind::CONNECT => MachineMsg::Connect {
                name: r.string()?,
                port: r.u16()?,
            },
            kind::DATA => MachineMsg::Data(bytes(r)?),
            kind::DATA_END => MachineMsg::DataEnd,
            kind::RESIZE => MachineMsg::Resize {
                rows: r.u16()?,
                cols: r.u16()?,
            },
            kind::SIGNAL => MachineMsg::Signal {
                signal: r.u32()? as i32,
            },
            kind::STDOUT => MachineMsg::Stdout(bytes(r)?),
            kind::STDERR => MachineMsg::Stderr(bytes(r)?),
            kind::EXIT => MachineMsg::Exit {
                code: r.u32()? as i32,
            },
            kind::MACHINES => {
                let n = count(r, 7 * 4 + 14)?;
                let mut list = Vec::with_capacity(n);
                for _ in 0..n {
                    list.push(MachineInfo {
                        name: r.string()?,
                        distro: r.string()?,
                        release: r.string()?,
                        image: r.string()?,
                        state: r.string()?,
                        ip: r.string()?,
                        user: r.string()?,
                        uid: r.u32()?,
                        default: r.u8()? != 0,
                        autostart: r.u8()? != 0,
                        created_unix: r.u64()?,
                    });
                }
                MachineMsg::Machines(list)
            }
            kind::DONE => MachineMsg::Done,
            kind::ERROR => MachineMsg::Error(r.string()?),
            kind::PROGRESS => MachineMsg::Progress(r.string()?),
            other => return Err(ProtoError::UnknownKind(other)),
        };
        if !r.buf.is_empty() {
            return Err(ProtoError::TrailingBytes(r.buf.len()));
        }
        Ok(msg)
    }
}

pub fn write<W: Write>(w: &mut W, msg: &MachineMsg) -> io::Result<()> {
    let frame = msg.encode();
    let len = frame.len() - 4;
    if len > MAX_FRAME {
        return Err(ProtoError::FrameTooLarge(len).into());
    }
    w.write_all(&frame)?;
    w.flush()
}

/// Read one frame. Returns `Ok(None)` on a clean EOF at a frame boundary.
pub fn read<R: Read>(r: &mut R) -> io::Result<Option<MachineMsg>> {
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
    Ok(Some(MachineMsg::decode(k, &body[2..])?))
}

/// A machine name: what `lxc`, the hostname and `~/Katamaran/machines/`
/// all accept. Lowercase letters, digits and inner dashes, at most 63.
pub fn name_ok(name: &str) -> bool {
    let b = name.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && b[0].is_ascii_lowercase()
        && b[b.len() - 1] != b'-'
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// A Unix user name the guest may create.
pub fn user_ok(user: &str) -> bool {
    let b = user.as_bytes();
    !b.is_empty()
        && b.len() <= 32
        && (b[0].is_ascii_lowercase() || b[0] == b'_')
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"_-.".contains(c))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all() -> Vec<MachineMsg> {
        vec![
            MachineMsg::List,
            MachineMsg::Info { name: "dev".into() },
            MachineMsg::Create(CreateSpec {
                name: "dev".into(),
                distro: "alpine".into(),
                release: "3.24".into(),
                image: "20261117_13:01".into(),
                user: "me".into(),
                uid: 501,
            }),
            MachineMsg::Start { name: "a".into() },
            MachineMsg::Stop { name: "".into() },
            MachineMsg::Delete { name: "b".into() },
            MachineMsg::SetDefault { name: "c".into() },
            MachineMsg::Exec(ExecSpec {
                machine: "dev".into(),
                user: "root".into(),
                cwd: "/Users/me".into(),
                env: vec![("LANG".into(), "C.UTF-8".into())],
                argv: vec!["sh".into(), "-c".into(), "echo hi".into()],
                tty: Some(Tty {
                    term: "xterm-256color".into(),
                    rows: 40,
                    cols: 120,
                }),
            }),
            MachineMsg::Exec(ExecSpec::default()),
            MachineMsg::Connect {
                name: "dev".into(),
                port: 22,
            },
            MachineMsg::Data(vec![1, 2, 3]),
            MachineMsg::Data(vec![]),
            MachineMsg::DataEnd,
            MachineMsg::Resize { rows: 1, cols: 2 },
            MachineMsg::Signal { signal: 15 },
            MachineMsg::Stdout(b"out".to_vec()),
            MachineMsg::Stderr(b"err".to_vec()),
            MachineMsg::Exit { code: -1 },
            MachineMsg::Machines(vec![
                MachineInfo {
                    name: "dev".into(),
                    distro: "ubuntu".into(),
                    release: "noble".into(),
                    image: "x".into(),
                    state: "running".into(),
                    ip: "198.19.249.2".into(),
                    user: "me".into(),
                    uid: 501,
                    default: true,
                    autostart: true,
                    created_unix: 1_800_000_000,
                },
                MachineInfo::default(),
            ]),
            MachineMsg::Machines(vec![]),
            MachineMsg::Done,
            MachineMsg::Error("no such machine".into()),
            MachineMsg::Progress("unpacking".into()),
        ]
    }

    #[test]
    fn every_message_round_trips() {
        for m in all() {
            let frame = m.encode();
            let back = read(&mut &frame[..]).unwrap().unwrap();
            assert_eq!(back, m);
        }
    }

    #[test]
    fn kinds_stay_in_the_assigned_range() {
        for m in all() {
            let frame = m.encode();
            let k = u16::from_le_bytes([frame[4], frame[5]]);
            assert!((0x701..=0x71f).contains(&k), "{m:?}: {k:#x}");
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
                    MachineMsg::decode(k, &body[..cut]).is_err(),
                    "{m:?} cut {cut}"
                );
            }
        }
        let mut huge = [0u8; 12];
        huge[..4].copy_from_slice(&[0, 0, 0, 0]);
        huge[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(MachineMsg::decode(kind::MACHINES, &huge[4..]).is_err());
        assert!(MachineMsg::decode(0x71f, &[]).is_err());
    }

    #[test]
    fn validates_names() {
        for ok in ["a", "dev", "ubuntu-24", "x1"] {
            assert!(name_ok(ok), "{ok}");
        }
        for bad in [
            "",
            "-a",
            "a-",
            "Dev",
            "1a",
            "a_b",
            "a.b",
            "../x",
            &"a".repeat(64),
        ] {
            assert!(!name_ok(bad), "{bad}");
        }
        assert!(user_ok("nkl") && user_ok("_svc") && user_ok("a.b-c"));
        assert!(!user_ok("") && !user_ok("Root") && !user_ok("a b") && !user_ok("1x"));
    }

    #[test]
    fn truncated_length_header_and_oversized_write() {
        assert_eq!(read(&mut [].as_slice()).unwrap(), None);
        for n in 1..=3 {
            let bytes = [1u8, 2, 3];
            let err = read(&mut &bytes[..n]).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "len {n}");
        }
        let mut out = Vec::new();
        let err = write(&mut out, &MachineMsg::Error("e".repeat(MAX_FRAME))).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(out.is_empty());
    }
}
