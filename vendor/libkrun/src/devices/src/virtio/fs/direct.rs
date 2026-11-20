//! KAT_FS_DIRECT: a guest FUSE request passed in the hypercall itself.
//!
//! `x1` is the guest-physical address of a request table, `x2` its length in
//! bytes and `x3` the descriptor table address of the request queue the guest
//! would otherwise use (it names the device). The table lists the request's
//! guest buffers, the same ones its virtqueue descriptors would carry:
//!
//! ```text
//! le16 version (ABI_DIRECT)   le16 nsegs   le16 nin   le16 reserved (0)
//! nsegs x { le64 addr, le32 len, le32 reserved (0) }
//! ```
//!
//! `segs[..nin]` are read by the host and `segs[nin..]` written. The reply is
//! the number of bytes written; values from [`STATUS`] up are statuses, and on
//! every status the guest sends the request through the virtqueue instead.
//!
//! The table is copied out of guest memory once, before anything else, so the
//! guest cannot change it while it is checked or used. Every buffer must lie
//! within one guest memory region, which `Reader`/`Writer` check before they
//! touch memory.

use smallvec::{SmallVec, smallvec};
use vm_memory::{ByteValued, Bytes, GuestAddress, GuestMemoryMmap, Le16, Le32, Le64};

use super::super::descriptor_utils::{Reader, Writer};

pub const KAT_FS_DIRECT: u64 = 0xC600_4B02;
/// The table layout above; the guest asks for it in KAT_FS_PROBE's `x1`.
pub const ABI_DIRECT: u64 = 1;
/// Replies below this are reply lengths.
pub const STATUS: u64 = 1 << 32;
/// Not run here (it may block, or the device or queue is unknown).
pub const DECLINED: u64 = STATUS;
/// The table or a buffer in it is invalid; no guest buffer was read or written.
pub const INVALID: u64 = STATUS | 1;

/// Header and args in each direction plus up to 128 pages (the request size limit; a 32 KiB
/// READDIRPLUS buffer is 8 separate pages). Larger requests take the virtqueue.
pub const MAX_SEGS: usize = 4 + 128;
/// Bytes in either direction. Above what the sync policy runs on a vCPU
/// (`MAX_SYNC_IO` plus headers), so it only stops nonsense early.
pub const MAX_BYTES: u64 = 2 << 20;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RawHeader {
    version: Le16,
    nsegs: Le16,
    nin: Le16,
    reserved: Le16,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RawSeg {
    addr: Le64,
    len: Le32,
    reserved: Le32,
}

// SAFETY: plain little-endian integers without padding; any bit pattern is valid.
unsafe impl ByteValued for RawHeader {}
// SAFETY: as above.
unsafe impl ByteValued for RawSeg {}

const HDR: usize = size_of::<RawHeader>();
const SEG: usize = size_of::<RawSeg>();
const MAX_TABLE: usize = HDR + MAX_SEGS * SEG;
/// Segments kept inline: all but READDIR(PLUS) and multi-page I/O.
const INLINE_SEGS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalid {
    /// `x2` is not the size of a header and a whole number of segments.
    TableLength,
    /// The table is not in guest memory.
    TableMemory,
    Version,
    /// `nsegs` disagrees with `x2`, or there is no buffer to read or none to write.
    Counts,
    Reserved,
    EmptySegment,
    /// A direction's total exceeds [`MAX_BYTES`].
    TooLarge,
    /// A buffer is not within one guest memory region.
    BufferMemory,
}

/// The checked buffers of one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segments {
    segs: SmallVec<[(GuestAddress, u32); INLINE_SEGS]>,
    nin: usize,
}

impl Segments {
    pub fn readable(&self) -> impl Iterator<Item = (GuestAddress, u32)> + '_ {
        self.segs[..self.nin].iter().copied()
    }

    pub fn writable(&self) -> impl Iterator<Item = (GuestAddress, u32)> + '_ {
        self.segs[self.nin..].iter().copied()
    }
}

/// Copy the table at `addr` (`len` bytes) out of guest memory and check it.
pub fn parse(mem: &GuestMemoryMmap, addr: u64, len: u64) -> Result<Segments, Invalid> {
    let len = usize::try_from(len).map_err(|_| Invalid::TableLength)?;
    if !(HDR + 2 * SEG..=MAX_TABLE).contains(&len) || (len - HDR) % SEG != 0 {
        return Err(Invalid::TableLength);
    }
    let mut table: SmallVec<[u8; 256]> = smallvec![0; len];
    mem.read_slice(&mut table[..], GuestAddress(addr))
        .map_err(|_| Invalid::TableMemory)?;
    parse_table(&table)
}

/// `T` from the start of `bytes`, which need not be aligned for it.
fn copy_from<T: ByteValued + Default>(bytes: &[u8]) -> T {
    let mut v = T::default();
    let dst = v.as_mut_slice();
    dst.copy_from_slice(&bytes[..dst.len()]);
    v
}

fn parse_table(table: &[u8]) -> Result<Segments, Invalid> {
    let header: RawHeader = copy_from(&table[..HDR]);
    if u64::from(header.version.to_native()) != ABI_DIRECT {
        return Err(Invalid::Version);
    }
    if header.reserved.to_native() != 0 {
        return Err(Invalid::Reserved);
    }
    let n = header.nsegs.to_native() as usize;
    let nin = header.nin.to_native() as usize;
    if n * SEG != table.len() - HDR || nin == 0 || nin >= n {
        return Err(Invalid::Counts);
    }

    let mut out = Segments {
        segs: SmallVec::with_capacity(n),
        nin,
    };
    let (mut read, mut written) = (0u64, 0u64);
    for (i, raw) in table[HDR..].chunks_exact(SEG).enumerate() {
        let seg: RawSeg = copy_from(raw);
        if seg.reserved.to_native() != 0 {
            return Err(Invalid::Reserved);
        }
        let len = seg.len.to_native();
        if len == 0 {
            return Err(Invalid::EmptySegment);
        }
        let total = if i < nin { &mut read } else { &mut written };
        *total += u64::from(len);
        if *total > MAX_BYTES {
            return Err(Invalid::TooLarge);
        }
        out.segs.push((GuestAddress(seg.addr.to_native()), len));
    }
    Ok(out)
}

/// The request's input and reply buffers.
pub fn buffers<'a>(
    mem: &'a GuestMemoryMmap,
    segs: &Segments,
) -> Result<(Reader<'a>, Writer<'a>), Invalid> {
    let r = Reader::from_guest_buffers(mem, segs.readable()).map_err(|_| Invalid::BufferMemory)?;
    let w = Writer::from_guest_buffers(mem, segs.writable()).map_err(|_| Invalid::BufferMemory)?;
    Ok((r, w))
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAM: u64 = 0x10_0000;
    /// A second region after a hole, to catch buffers that span regions.
    const HIGH: u64 = 0x20_0000;

    fn mem() -> GuestMemoryMmap {
        GuestMemoryMmap::from_ranges(&[
            (GuestAddress(0), RAM as usize),
            (GuestAddress(HIGH), 0x1000),
        ])
        .unwrap()
    }

    fn table(version: u16, nin: u16, segs: &[(u64, u32)]) -> Vec<u8> {
        let mut t = Vec::new();
        t.extend_from_slice(&version.to_le_bytes());
        t.extend_from_slice(&(segs.len() as u16).to_le_bytes());
        t.extend_from_slice(&nin.to_le_bytes());
        t.extend_from_slice(&0u16.to_le_bytes());
        for &(addr, len) in segs {
            t.extend_from_slice(&addr.to_le_bytes());
            t.extend_from_slice(&len.to_le_bytes());
            t.extend_from_slice(&0u32.to_le_bytes());
        }
        t
    }

    /// A GETATTR-shaped request: in header + args, out header + args.
    fn getattr() -> Vec<u8> {
        table(
            1,
            2,
            &[(0x1000, 40), (0x1100, 16), (0x2000, 16), (0x2100, 104)],
        )
    }

    fn put(mem: &GuestMemoryMmap, at: u64, t: &[u8]) -> u64 {
        mem.write_slice(t, GuestAddress(at)).unwrap();
        t.len() as u64
    }

    #[test]
    fn accepts_a_well_formed_request() {
        let mem = mem();
        let len = put(&mem, 0x8000, &getattr());
        let segs = parse(&mem, 0x8000, len).unwrap();
        assert_eq!(
            segs.readable().collect::<Vec<_>>(),
            [(GuestAddress(0x1000), 40), (GuestAddress(0x1100), 16)]
        );
        assert_eq!(
            segs.writable().collect::<Vec<_>>(),
            [(GuestAddress(0x2000), 16), (GuestAddress(0x2100), 104)]
        );
        let (r, w) = buffers(&mem, &segs).unwrap();
        assert_eq!(r.available_bytes(), 56);
        assert_eq!(w.available_bytes(), 120);
    }

    #[test]
    fn rejects_table_lengths_that_are_not_whole_segments() {
        let mem = mem();
        let t = getattr();
        put(&mem, 0x8000, &t);
        for len in [
            0,
            1,
            HDR as u64,
            (HDR + SEG) as u64,
            t.len() as u64 - 1,
            t.len() as u64 + 1,
        ] {
            assert_eq!(parse(&mem, 0x8000, len), Err(Invalid::TableLength), "{len}");
        }
        let too_many = table(1, 1, &[(0x1000, 8); MAX_SEGS + 1]);
        let len = put(&mem, 0x8000, &too_many);
        assert_eq!(parse(&mem, 0x8000, len), Err(Invalid::TableLength));
        assert_eq!(parse(&mem, 0x8000, u64::MAX), Err(Invalid::TableLength));
    }

    #[test]
    fn rejects_a_table_outside_guest_memory() {
        let mem = mem();
        let len = getattr().len() as u64;
        assert_eq!(parse(&mem, RAM, len), Err(Invalid::TableMemory));
        assert_eq!(parse(&mem, RAM - 8, len), Err(Invalid::TableMemory));
        assert_eq!(parse(&mem, u64::MAX - 8, len), Err(Invalid::TableMemory));
        assert_eq!(parse(&mem, HIGH - 0x10, len), Err(Invalid::TableMemory));
    }

    #[test]
    fn parses_tables_at_any_alignment() {
        let t = getattr();
        let mut buf = vec![0u8; t.len() + 1];
        buf[1..].copy_from_slice(&t);
        assert_eq!(parse_table(&buf[1..]), parse_table(&t));
        assert!(parse_table(&buf[1..]).is_ok());
    }

    #[test]
    fn rejects_bad_headers() {
        let segs = [(0x1000, 40), (0x2000, 16)];
        let cases = [
            (table(0, 1, &segs), Invalid::Version),
            (table(2, 1, &segs), Invalid::Version),
            (table(1, 0, &segs), Invalid::Counts),
            (table(1, 2, &segs), Invalid::Counts),
            (table(1, 9, &segs), Invalid::Counts),
        ];
        for (t, want) in cases {
            assert_eq!(parse_table(&t), Err(want));
        }
        // nsegs says three, the table holds two.
        let mut t = table(1, 1, &segs);
        t[2] = 3;
        assert_eq!(parse_table(&t), Err(Invalid::Counts));
        let mut t = table(1, 1, &segs);
        t[6] = 1;
        assert_eq!(parse_table(&t), Err(Invalid::Reserved));
    }

    #[test]
    fn rejects_bad_segments() {
        let mut t = getattr();
        t[HDR + SEG + 12] = 1;
        assert_eq!(parse_table(&t), Err(Invalid::Reserved));
        assert_eq!(
            parse_table(&table(1, 1, &[(0x1000, 40), (0x2000, 0)])),
            Err(Invalid::EmptySegment)
        );
        let big = MAX_BYTES as u32 / 2 + 1;
        assert_eq!(
            parse_table(&table(1, 2, &[(0x1000, big), (0x1000, big), (0x2000, 16)])),
            Err(Invalid::TooLarge)
        );
        assert_eq!(
            parse_table(&table(1, 1, &[(0x1000, 40), (0x2000, big), (0x2000, big)])),
            Err(Invalid::TooLarge)
        );
        // Lengths near u32::MAX in every slot cannot overflow the totals.
        assert_eq!(
            parse_table(&table(1, 4, &[(0, u32::MAX); MAX_SEGS])),
            Err(Invalid::TooLarge)
        );
    }

    #[test]
    fn rejects_buffers_outside_guest_memory() {
        let mem = mem();
        let cases: [&[(u64, u32)]; 6] = [
            // Ends one byte past RAM.
            &[(RAM - 40, 41), (0x2000, 16)],
            // Starts in the hole between the regions.
            &[(0x1000, 40), (RAM + 0x1000, 16)],
            // Spans the hole from the first region into the second.
            &[(0x1000, 40), (RAM - 8, (HIGH - RAM) as u32 + 16)],
            // Runs off the end of the second region.
            &[(HIGH + 0xff0, 0x20), (0x2000, 16)],
            // Address arithmetic would wrap.
            &[(u64::MAX - 8, 40), (0x2000, 16)],
            &[(0x1000, 40), (u64::MAX, 16)],
        ];
        for segs in cases {
            let parsed = parse_table(&table(1, 1, segs)).unwrap();
            assert_eq!(
                buffers(&mem, &parsed).err(),
                Some(Invalid::BufferMemory),
                "{segs:x?}"
            );
        }
        // At the very end of each region is fine.
        let ok = parse_table(&table(1, 1, &[(RAM - 40, 40), (HIGH + 0xff0, 16)])).unwrap();
        assert!(buffers(&mem, &ok).is_ok());
    }

    #[test]
    fn the_guest_cannot_change_the_table_after_it_is_read() {
        let mem = mem();
        let len = put(&mem, 0x8000, &getattr());
        let segs = parse(&mem, 0x8000, len).unwrap();
        // Rewrite the first segment to point outside memory, as another vCPU could.
        mem.write_obj(RAM + 0x100, GuestAddress(0x8000 + HDR as u64))
            .unwrap();
        let (r, _) = buffers(&mem, &segs).unwrap();
        assert_eq!(r.available_bytes(), 56);
    }
}
