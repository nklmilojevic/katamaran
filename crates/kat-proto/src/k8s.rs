//! Kubernetes (k3s) management protocol, spoken on [`crate::K8S_PORT`].
//!
//! The host opens one connection per request and sends one request frame
//! (same framing as [`crate::Msg`], kinds 0x801–0x81f):
//!
//! | request          | then                                       | guest answers                               |
//! |------------------|--------------------------------------------|---------------------------------------------|
//! | `Status`         | –                                          | `StatusInfo`, or `Error`                    |
//! | `Install`        | the `k3s` binary as `Data`..., `DataEnd`   | `Progress`…, then `Done` / `Error`          |
//! | `Start`          | –                                          | `Progress`…, then `StatusInfo` / `Error`    |
//! | `Stop`           | –                                          | `Progress`…, then `Done` / `Error`          |
//! | `Reset`          | –                                          | `Progress`…, then `Done` / `Error`          |
//! | `Kubeconfig`     | –                                          | `KubeconfigData`, or `Error`                |
//! | `Connect`        | `Data`… (`DataEnd` closes the write side)  | `Done` then `Data`…, `DataEnd`; or `Error`  |
//! | `SyncImages`     | –                                          | `Done`, or `Error`                          |

use std::io::{self, Read, Write};

use crate::{Cursor, HEADER, MAX_FRAME, ProtoError, put_str, put_u32, read_len};

/// Payload bytes per `Data` frame.
pub const CHUNK: usize = 64 << 10;

/// Guest loopback port of `kubectl proxy`: the API server, authenticated,
/// over plain HTTP. The host reaches it only through `K8sMsg::Connect`.
pub const PROXY_PORT: u16 = 6445;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum K8sMsg {
    Status,
    Install {
        version: String,
    },
    Start,
    Stop,
    Reset,
    Kubeconfig,
    /// Dial an arbitrary IPv4/IPv6 address and TCP port inside the guest
    /// (used for direct Pod IP `10.42.x.x` and Service ClusterIP `10.43.x.x`
    /// connections from macOS).
    Connect {
        addr: String,
        port: u16,
    },
    SyncImages,
    Data(Vec<u8>),
    DataEnd,
    StatusInfo(K8sInfo),
    KubeconfigData(String),
    Done,
    Error(String),
    Progress(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct K8sInfo {
    /// `stopped`, `starting`, `running`, `stopping`, or `failed`.
    pub state: String,
    /// Whether the `k3s` binary is installed on the data disk.
    pub installed: bool,
    /// Installed k3s version string (empty if not installed).
    pub version: String,
    /// Whether the Kubernetes API server is accepting connections.
    pub api_ready: bool,
    /// Last error, if any.
    pub error: String,
}

mod kind {
    pub const STATUS: u16 = 0x801;
    pub const INSTALL: u16 = 0x802;
    pub const START: u16 = 0x803;
    pub const STOP: u16 = 0x804;
    pub const RESET: u16 = 0x805;
    pub const KUBECONFIG: u16 = 0x806;
    pub const CONNECT: u16 = 0x807;
    pub const SYNC_IMAGES: u16 = 0x808;
    pub const DATA: u16 = 0x80a;
    pub const DATA_END: u16 = 0x80b;
    pub const STATUS_INFO: u16 = 0x80c;
    pub const KUBECONFIG_DATA: u16 = 0x80d;
    pub const DONE: u16 = 0x80e;
    pub const ERROR: u16 = 0x80f;
    pub const PROGRESS: u16 = 0x810;
}

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

impl K8sMsg {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER];
        let b = &mut buf;
        let k = match self {
            K8sMsg::Status => kind::STATUS,
            K8sMsg::Install { version } => {
                put_str(b, version);
                kind::INSTALL
            }
            K8sMsg::Start => kind::START,
            K8sMsg::Stop => kind::STOP,
            K8sMsg::Reset => kind::RESET,
            K8sMsg::Kubeconfig => kind::KUBECONFIG,
            K8sMsg::Connect { addr, port } => {
                put_str(b, addr);
                b.extend_from_slice(&port.to_le_bytes());
                kind::CONNECT
            }
            K8sMsg::SyncImages => kind::SYNC_IMAGES,
            K8sMsg::Data(d) => {
                put_bytes(b, d);
                kind::DATA
            }
            K8sMsg::DataEnd => kind::DATA_END,
            K8sMsg::StatusInfo(info) => {
                put_str(b, &info.state);
                b.push(u8::from(info.installed));
                put_str(b, &info.version);
                b.push(u8::from(info.api_ready));
                put_str(b, &info.error);
                kind::STATUS_INFO
            }
            K8sMsg::KubeconfigData(yaml) => {
                put_str(b, yaml);
                kind::KUBECONFIG_DATA
            }
            K8sMsg::Done => kind::DONE,
            K8sMsg::Error(e) => {
                put_str(b, e);
                kind::ERROR
            }
            K8sMsg::Progress(p) => {
                put_str(b, p);
                kind::PROGRESS
            }
        };
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf[4..6].copy_from_slice(&k.to_le_bytes());
        buf
    }

    pub fn decode(kind: u16, payload: &[u8]) -> Result<K8sMsg, ProtoError> {
        let mut c = Cursor { buf: payload };
        let flag = |c: &mut Cursor<'_>, what: &'static str| -> Result<bool, ProtoError> {
            match c.u8()? {
                0 => Ok(false),
                1 => Ok(true),
                v => Err(ProtoError::InvalidValue(what, v.into())),
            }
        };
        let msg = match kind {
            kind::STATUS => K8sMsg::Status,
            kind::INSTALL => K8sMsg::Install {
                version: c.string()?,
            },
            kind::START => K8sMsg::Start,
            kind::STOP => K8sMsg::Stop,
            kind::RESET => K8sMsg::Reset,
            kind::KUBECONFIG => K8sMsg::Kubeconfig,
            kind::CONNECT => K8sMsg::Connect {
                addr: c.string()?,
                port: c.u16()?,
            },
            kind::SYNC_IMAGES => K8sMsg::SyncImages,
            kind::DATA => {
                let n = c.u32()? as usize;
                K8sMsg::Data(c.take(n)?.to_vec())
            }
            kind::DATA_END => K8sMsg::DataEnd,
            kind::STATUS_INFO => K8sMsg::StatusInfo(K8sInfo {
                state: c.string()?,
                installed: flag(&mut c, "k8s installed")?,
                version: c.string()?,
                api_ready: flag(&mut c, "k8s api_ready")?,
                error: c.string()?,
            }),
            kind::KUBECONFIG_DATA => K8sMsg::KubeconfigData(c.string()?),
            kind::DONE => K8sMsg::Done,
            kind::ERROR => K8sMsg::Error(c.string()?),
            kind::PROGRESS => K8sMsg::Progress(c.string()?),
            other => return Err(ProtoError::UnknownKind(other)),
        };
        if !c.buf.is_empty() {
            return Err(ProtoError::TrailingBytes(c.buf.len()));
        }
        Ok(msg)
    }
}

pub fn write<W: Write>(mut w: W, msg: &K8sMsg) -> io::Result<()> {
    let frame = msg.encode();
    let len = frame.len() - 4;
    if len > MAX_FRAME {
        return Err(ProtoError::FrameTooLarge(len).into());
    }
    w.write_all(&frame)?;
    w.flush()
}

pub fn read<R: Read>(mut r: R) -> io::Result<Option<K8sMsg>> {
    let Some(len) = read_len(&mut r)? else {
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
    Ok(Some(K8sMsg::decode(kind, &body[2..])?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_round_trips() {
        let msgs = [
            K8sMsg::Status,
            K8sMsg::Install {
                version: "v1.37.2+k3s1".into(),
            },
            K8sMsg::Start,
            K8sMsg::Stop,
            K8sMsg::Reset,
            K8sMsg::Kubeconfig,
            K8sMsg::Connect {
                addr: "10.43.0.1".into(),
                port: 443,
            },
            K8sMsg::SyncImages,
            K8sMsg::Data(vec![1, 2, 3, 4]),
            K8sMsg::DataEnd,
            K8sMsg::StatusInfo(K8sInfo {
                state: "running".into(),
                installed: true,
                version: "v1.37.2+k3s1".into(),
                api_ready: true,
                error: String::new(),
            }),
            K8sMsg::KubeconfigData("apiVersion: v1\n".into()),
            K8sMsg::Done,
            K8sMsg::Error("failed".into()),
            K8sMsg::Progress("starting k3s".into()),
        ];
        let mut wire = Vec::new();
        for m in &msgs {
            write(&mut wire, m).unwrap();
        }
        let mut slice = wire.as_slice();
        for want in &msgs {
            let got = read(&mut slice).unwrap().unwrap();
            assert_eq!(&got, want);
        }
        assert!(read(&mut slice).unwrap().is_none());
    }

    #[test]
    fn kinds_stay_in_the_assigned_range() {
        for m in [
            K8sMsg::Status,
            K8sMsg::Install {
                version: String::new(),
            },
            K8sMsg::Start,
            K8sMsg::Stop,
            K8sMsg::Reset,
            K8sMsg::Kubeconfig,
            K8sMsg::Connect {
                addr: String::new(),
                port: 0,
            },
            K8sMsg::SyncImages,
            K8sMsg::Data(vec![]),
            K8sMsg::DataEnd,
            K8sMsg::StatusInfo(K8sInfo::default()),
            K8sMsg::KubeconfigData(String::new()),
            K8sMsg::Done,
            K8sMsg::Error(String::new()),
            K8sMsg::Progress(String::new()),
        ] {
            let f = m.encode();
            let k = u16::from_le_bytes([f[4], f[5]]);
            assert!((0x801..=0x81f).contains(&k), "kind {k:#x} out of range");
        }
    }

    #[test]
    fn truncated_length_header_and_oversized_write() {
        assert_eq!(read([].as_slice()).unwrap(), None);
        for n in 1..=3 {
            let bytes = [1u8, 2, 3];
            let err = read(&bytes[..n]).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "len {n}");
        }
        let mut out = Vec::new();
        let err = write(&mut out, &K8sMsg::Error("e".repeat(MAX_FRAME))).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(out.is_empty());
    }
}
