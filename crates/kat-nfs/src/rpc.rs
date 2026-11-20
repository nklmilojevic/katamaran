//! ONC RPC v2 (RFC 5531) over a stream: record marking, call headers,
//! AUTH_SYS credentials and reply headers.

use std::io::{self, Read, Write};

use crate::xdr::{Garbage, Reader, Writer};

/// Largest record accepted: a 1 MiB WRITE plus headers, with room to spare.
pub const MAX_RECORD: usize = (2 << 20) + 4096;

const LAST_FRAGMENT: u32 = 1 << 31;
const CALL: u32 = 0;
const REPLY: u32 = 1;
const MSG_ACCEPTED: u32 = 0;
const MSG_DENIED: u32 = 1;
const RPC_MISMATCH: u32 = 0;
const AUTH_ERROR: u32 = 1;
const AUTH_BADCRED: u32 = 1;
pub const AUTH_NONE: u32 = 0;
pub const AUTH_SYS: u32 = 1;

pub const SUCCESS: u32 = 0;
pub const PROG_UNAVAIL: u32 = 1;
pub const PROG_MISMATCH: u32 = 2;
pub const PROC_UNAVAIL: u32 = 3;
pub const GARBAGE_ARGS: u32 = 4;

/// Read one record (all its fragments) into `buf`, reusing its space.
/// `Ok(None)` on EOF at a record boundary.
pub fn read_record<R: Read>(r: &mut R, buf: &mut Vec<u8>) -> io::Result<Option<()>> {
    let mut filled = 0;
    loop {
        let mut h = [0u8; 4];
        match r.read_exact(&mut h) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && filled == 0 => {
                buf.clear();
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        let h = u32::from_be_bytes(h);
        let len = (h & !LAST_FRAGMENT) as usize;
        if filled + len > MAX_RECORD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("RPC record over {MAX_RECORD} bytes"),
            ));
        }
        let end = filled + len;
        if buf.len() < end {
            buf.resize(end, 0);
        }
        r.read_exact(&mut buf[filled..end])?;
        filled = end;
        if h & LAST_FRAGMENT != 0 {
            buf.truncate(filled);
            return Ok(Some(()));
        }
    }
}

/// Write `body` (which starts with 4 reserved bytes) as one record.
pub fn write_record<W: Write>(w: &mut W, body: &mut [u8]) -> io::Result<()> {
    let len = (body.len() - 4) as u32 | LAST_FRAGMENT;
    body[..4].copy_from_slice(&len.to_be_bytes());
    w.write_all(body)
}

/// The caller's identity. AUTH_NONE callers are `nobody`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cred {
    pub uid: u32,
    pub gid: u32,
}

impl Cred {
    pub const NOBODY: Cred = Cred {
        uid: 65534,
        gid: 65534,
    };
}

pub struct Call<'a> {
    pub xid: u32,
    pub prog: u32,
    pub vers: u32,
    pub proc_: u32,
    pub cred: Cred,
    pub args: Reader<'a>,
}

/// Why a record is not a call we can answer normally.
#[derive(Debug, PartialEq, Eq)]
pub enum Reject {
    /// Not even an xid: drop it.
    Unparseable,
    /// Not a call (a reply, or garbage after the xid): drop it.
    NotACall,
    /// Wrong RPC version: answer RPC_MISMATCH.
    RpcMismatch(u32),
    /// Unsupported or malformed credentials: answer AUTH_ERROR.
    BadCred(u32),
}

pub fn parse_call(record: &[u8]) -> Result<Call<'_>, Reject> {
    let mut r = Reader::new(record);
    let xid = r.u32().map_err(|_| Reject::Unparseable)?;
    let hdr = |r: &mut Reader| -> Result<(u32, u32), Garbage> { Ok((r.u32()?, r.u32()?)) };
    let (mtype, rpcvers) = hdr(&mut r).map_err(|_| Reject::NotACall)?;
    if mtype != CALL {
        return Err(Reject::NotACall);
    }
    if rpcvers != 2 {
        return Err(Reject::RpcMismatch(xid));
    }
    let (prog, vers) = hdr(&mut r).map_err(|_| Reject::NotACall)?;
    let proc_ = r.u32().map_err(|_| Reject::NotACall)?;
    let flavor = r.u32().map_err(|_| Reject::BadCred(xid))?;
    let body = r.opaque(400).map_err(|_| Reject::BadCred(xid))?;
    let cred = match flavor {
        AUTH_NONE => Cred::NOBODY,
        AUTH_SYS => auth_sys(body).map_err(|_| Reject::BadCred(xid))?,
        _ => return Err(Reject::BadCred(xid)),
    };
    // The verifier is ignored, but must be there.
    r.u32().map_err(|_| Reject::BadCred(xid))?;
    r.opaque(400).map_err(|_| Reject::BadCred(xid))?;
    Ok(Call {
        xid,
        prog,
        vers,
        proc_,
        cred,
        args: r,
    })
}

fn auth_sys(body: &[u8]) -> Result<Cred, Garbage> {
    let mut r = Reader::new(body);
    r.u32()?; // stamp
    r.opaque(255)?; // machine name
    let uid = r.u32()?;
    let gid = r.u32()?;
    let n = r.u32()?;
    if n > 16 {
        return Err(Garbage);
    }
    Ok(Cred { uid, gid })
}

/// Start an accepted reply; procedure results follow.
pub fn accepted(xid: u32, stat: u32) -> Writer {
    accepted_in(Vec::with_capacity(256), xid, stat)
}

/// [`accepted`], written over `buf` to reuse its space.
pub fn accepted_in(mut buf: Vec<u8>, xid: u32, stat: u32) -> Writer {
    buf.clear();
    let mut w = Writer { buf };
    w.u32(0); // record mark, filled in by write_record
    w.u32(xid).u32(REPLY).u32(MSG_ACCEPTED);
    w.u32(AUTH_NONE).u32(0); // verifier
    w.u32(stat);
    w
}

pub fn prog_mismatch(xid: u32, low: u32, high: u32) -> Writer {
    let mut w = accepted(xid, PROG_MISMATCH);
    w.u32(low).u32(high);
    w
}

pub fn denied(reject: &Reject) -> Option<Writer> {
    let mut w = Writer::default();
    w.u32(0);
    match *reject {
        Reject::RpcMismatch(xid) => {
            w.u32(xid).u32(REPLY).u32(MSG_DENIED).u32(RPC_MISMATCH);
            w.u32(2).u32(2);
        }
        Reject::BadCred(xid) => {
            w.u32(xid).u32(REPLY).u32(MSG_DENIED).u32(AUTH_ERROR);
            w.u32(AUTH_BADCRED);
        }
        Reject::Unparseable | Reject::NotACall => return None,
    }
    Some(w)
}

/// Build a call record (for tests and tools): header, AUTH_SYS cred, args.
pub fn call(xid: u32, prog: u32, vers: u32, proc_: u32, cred: Cred, args: &[u8]) -> Vec<u8> {
    let mut auth = Writer::default();
    auth.u32(0)
        .opaque(b"test")
        .u32(cred.uid)
        .u32(cred.gid)
        .u32(0);
    let mut w = Writer::default();
    w.u32(0);
    w.u32(xid).u32(CALL).u32(2).u32(prog).u32(vers).u32(proc_);
    w.u32(AUTH_SYS).opaque(&auth.buf);
    w.u32(AUTH_NONE).u32(0);
    w.buf.extend_from_slice(args);
    w.buf
}

/// Split a reply record into its xid, accept status and results.
pub fn parse_reply(record: &[u8]) -> Result<(u32, u32, Reader<'_>), Garbage> {
    let mut r = Reader::new(record);
    let xid = r.u32()?;
    if r.u32()? != REPLY || r.u32()? != MSG_ACCEPTED {
        return Err(Garbage);
    }
    r.u32()?;
    r.opaque(400)?;
    let stat = r.u32()?;
    Ok((xid, stat, r))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_reassemble_fragments() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&3u32.to_be_bytes());
        wire.extend_from_slice(b"abc");
        wire.extend_from_slice(&(2u32 | LAST_FRAGMENT).to_be_bytes());
        wire.extend_from_slice(b"de");
        write_record(&mut wire, &mut b"\0\0\0\0xyz".to_vec()).unwrap();
        let mut r = wire.as_slice();
        let mut buf = b"left over from a longer record".to_vec();
        assert!(read_record(&mut r, &mut buf).unwrap().is_some());
        assert_eq!(buf, b"abcde");
        assert!(read_record(&mut r, &mut buf).unwrap().is_some());
        assert_eq!(buf, b"xyz");
        assert!(read_record(&mut r, &mut buf).unwrap().is_none());
    }

    #[test]
    fn oversized_and_truncated_records_fail() {
        let wire = ((MAX_RECORD as u32 + 1) | LAST_FRAGMENT).to_be_bytes();
        let mut buf = Vec::new();
        assert!(read_record(&mut wire.as_slice(), &mut buf).is_err());
        let mut wire = (10u32 | LAST_FRAGMENT).to_be_bytes().to_vec();
        wire.extend_from_slice(b"short");
        assert!(read_record(&mut wire.as_slice(), &mut buf).is_err());
    }

    #[test]
    fn parses_auth_sys_calls() {
        let cred = Cred { uid: 501, gid: 20 };
        let rec = call(9, 100003, 3, 1, cred, &[1, 2, 3, 4]);
        let c = parse_call(&rec[4..]).ok().unwrap();
        assert_eq!((c.xid, c.prog, c.vers, c.proc_), (9, 100003, 3, 1));
        assert_eq!(c.cred, cred);
        assert_eq!(c.args.remaining(), 4);
    }

    #[test]
    fn rejects_bad_calls() {
        let rec = call(5, 1, 1, 1, Cred::NOBODY, &[]);
        let mut bad_vers = rec[4..].to_vec();
        bad_vers[8..12].copy_from_slice(&3u32.to_be_bytes());
        assert_eq!(parse_call(&bad_vers).err(), Some(Reject::RpcMismatch(5)));
        let mut reply = rec[4..].to_vec();
        reply[4..8].copy_from_slice(&REPLY.to_be_bytes());
        assert_eq!(parse_call(&reply).err(), Some(Reject::NotACall));
        let mut flavor = rec[4..].to_vec();
        flavor[24..28].copy_from_slice(&6u32.to_be_bytes());
        assert_eq!(parse_call(&flavor).err(), Some(Reject::BadCred(5)));
        assert_eq!(parse_call(&[1, 2]).err(), Some(Reject::Unparseable));
        assert!(denied(&Reject::RpcMismatch(5)).is_some());
        assert!(denied(&Reject::NotACall).is_none());
    }

    #[test]
    fn replies_parse_back() {
        let mut w = accepted(77, SUCCESS);
        w.u32(42);
        let (xid, stat, mut r) = parse_reply(&w.buf[4..]).unwrap();
        assert_eq!((xid, stat, r.u32()), (77, SUCCESS, Ok(42)));
    }
}
