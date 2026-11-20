//! Cache invalidations for the virtio-fs shares, from the VM process to the
//! guest agent on [`crate::FSCACHE_PORT`], which writes each frame to
//! `/dev/kat_fsnotify` (guest/kernel/patches/0004). A frame is a `u32`
//! little-endian length and that many bytes of `struct kat_fsn_event`
//! records, each padded to 8 bytes.

use std::io::{self, Read, Write};

/// The most the guest kernel takes in one write.
pub const FRAME_MAX: usize = 1 << 20;
/// `struct kat_fsn_event` without its name.
pub const RECORD_HEADER: usize = 24;

const OP_SHARE: u16 = 16;
const OP_INVAL_INODE: u16 = 17;
const OP_INVAL_ENTRY: u16 = 18;
const OP_EXPIRE_DIR: u16 = 19;
const FLAG_DATA: u8 = 0x2;

/// What the guest must drop. `share` is the index of the share, in the
/// order the guest mounts them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Inval {
    /// A node's attributes; `data`: its page cache too.
    Inode { share: u32, node: u64, data: bool },
    /// One name in directory `dir`, and the directory's listing.
    Entry { share: u32, dir: u64, name: Vec<u8> },
    /// Every name in directory `dir`, and its attributes.
    ExpireDir { share: u32, dir: u64 },
}

fn record(out: &mut Vec<u8>, id: u64, ino: u64, cookie: u32, op: u16, flags: u8, name: &[u8]) {
    let len = (RECORD_HEADER + name.len()).next_multiple_of(8);
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&ino.to_le_bytes());
    out.extend_from_slice(&cookie.to_le_bytes());
    out.extend_from_slice(&op.to_le_bytes());
    out.push(flags);
    out.push(name.len() as u8);
    out.extend_from_slice(name);
    out.resize(out.len() + len - RECORD_HEADER - name.len(), 0);
}

/// `/dev/kat_fsnotify` records, in chunks the guest kernel takes in one
/// write. A name longer than Linux allows has nothing cached.
pub fn encode(invals: &[Inval]) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut cur = Vec::new();
    for i in invals {
        if cur.len() + RECORD_HEADER + 256 > FRAME_MAX {
            chunks.push(std::mem::take(&mut cur));
        }
        match i {
            Inval::Inode { share, node, data } => {
                let flags = if *data { FLAG_DATA } else { 0 };
                record(&mut cur, *node, 0, *share, OP_INVAL_INODE, flags, b"");
            }
            Inval::Entry { share, dir, name } if name.len() <= 255 => {
                record(&mut cur, *dir, 0, *share, OP_INVAL_ENTRY, 0, name);
            }
            Inval::Entry { .. } => {}
            Inval::ExpireDir { share, dir } => {
                record(&mut cur, *dir, 0, *share, OP_EXPIRE_DIR, 0, b"");
            }
        }
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

/// The record registering share `index`, whose mount root the agent has
/// open as `fd`. Written by the guest agent.
pub fn share_record(index: u32, fd: u64) -> Vec<u8> {
    let mut r = Vec::new();
    record(&mut r, u64::from(index), fd, 0, OP_SHARE, 0, b"");
    r
}

/// Send one chunk as a frame.
pub fn write_frame(w: &mut impl Write, chunk: &[u8]) -> io::Result<()> {
    w.write_all(&(chunk.len() as u32).to_le_bytes())?;
    w.write_all(chunk)
}

/// Read one frame (the agent's side). `None` at end of stream.
pub fn read_frame(r: &mut impl Read, buf: &mut Vec<u8>) -> io::Result<Option<()>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len) as usize;
    if len > FRAME_MAX || !len.is_multiple_of(8) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad frame length",
        ));
    }
    buf.resize(len, 0);
    r.read_exact(buf)?;
    Ok(Some(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_match_the_kernel_layout() {
        let chunks = encode(&[
            Inval::Inode {
                share: 1,
                node: 7,
                data: true,
            },
            Inval::Entry {
                share: 0,
                dir: 9,
                name: b"abc".to_vec(),
            },
            Inval::Entry {
                share: 0,
                dir: 9,
                name: vec![b'x'; 256],
            },
            Inval::ExpireDir { share: 2, dir: 4 },
        ]);
        assert_eq!(chunks.len(), 1);
        let c = &chunks[0];
        assert_eq!(c.len(), 24 + 32 + 24);
        let u64_at = |o: usize| u64::from_le_bytes(c[o..o + 8].try_into().unwrap());
        let u32_at = |o: usize| u32::from_le_bytes(c[o..o + 4].try_into().unwrap());
        let u16_at = |o: usize| u16::from_le_bytes(c[o..o + 2].try_into().unwrap());
        assert_eq!(
            (u64_at(0), u32_at(16), u16_at(20), c[22], c[23]),
            (7, 1, 17, 2, 0)
        );
        assert_eq!((u64_at(24), u32_at(40), u16_at(44), c[47]), (9, 0, 18, 3));
        assert_eq!(&c[48..51], b"abc");
        assert_eq!((u64_at(56), u32_at(72), u16_at(76)), (4, 2, 19));
        let share = share_record(3, 12);
        assert_eq!(share.len(), 24);
        assert_eq!(&share[0..8], &3u64.to_le_bytes());
        assert_eq!(&share[8..16], &12u64.to_le_bytes());
        assert_eq!(u16::from_le_bytes([share[20], share[21]]), 16);
    }

    #[test]
    fn large_batches_split_into_frames() {
        let invals: Vec<Inval> = (0..200_000)
            .map(|i| Inval::Entry {
                share: 0,
                dir: 1,
                name: format!("name-{i:010}").into_bytes(),
            })
            .collect();
        let chunks = encode(&invals);
        assert!(chunks.len() > 1);
        assert!(
            chunks
                .iter()
                .all(|c| c.len() <= FRAME_MAX && c.len() % 8 == 0)
        );
        let mut wire = Vec::new();
        for c in &chunks {
            write_frame(&mut wire, c).unwrap();
        }
        let mut r = &wire[..];
        let mut buf = Vec::new();
        let mut n = 0;
        while read_frame(&mut r, &mut buf).unwrap().is_some() {
            assert_eq!(buf, chunks[n]);
            n += 1;
        }
        assert_eq!(n, chunks.len());
    }
}
