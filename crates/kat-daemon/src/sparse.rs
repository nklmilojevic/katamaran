//! A packed sparse disk image, so the app bundle carries the 64 GiB data-disk
//! template as the few MiB it really holds (codesign hashes every byte of a
//! bundled file, holes included).
//!
//! Format: `KATSPRS1`, the logical size as u64 LE, then records of
//! `offset: u64 LE | len: u64 LE | len bytes` for each data extent.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::Path;

use anyhow::{Context, bail};

const MAGIC: &[u8; 8] = b"KATSPRS1";
pub const EXTENSION: &str = "sparse";

#[derive(clap::Args)]
pub struct PackArgs {
    input: std::path::PathBuf,
    output: std::path::PathBuf,
}

pub fn pack_cmd(args: PackArgs) -> anyhow::Result<()> {
    pack(&args.input, &args.output)
}

/// The data extents of `f`, from SEEK_DATA/SEEK_HOLE.
fn extents(f: &File, size: u64) -> io::Result<Vec<(u64, u64)>> {
    let fd = f.as_raw_fd();
    let mut out = Vec::new();
    let mut pos = 0i64;
    while (pos as u64) < size {
        let data = unsafe { libc::lseek(fd, pos, libc::SEEK_DATA) };
        if data < 0 {
            let e = io::Error::last_os_error();
            // ENXIO: no data after pos.
            if e.raw_os_error() == Some(libc::ENXIO) {
                break;
            }
            return Err(e);
        }
        let hole = unsafe { libc::lseek(fd, data, libc::SEEK_HOLE) };
        if hole < 0 {
            return Err(io::Error::last_os_error());
        }
        out.push((data as u64, (hole - data) as u64));
        pos = hole;
    }
    Ok(out)
}

pub fn pack(input: &Path, output: &Path) -> anyhow::Result<()> {
    let mut f = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let size = f.metadata()?.len();
    let mut w = BufWriter::new(File::create(output)?);
    w.write_all(MAGIC)?;
    w.write_all(&size.to_le_bytes())?;
    let mut buf = vec![0u8; 1 << 20];
    for (off, len) in extents(&f, size)? {
        w.write_all(&off.to_le_bytes())?;
        w.write_all(&len.to_le_bytes())?;
        f.seek(SeekFrom::Start(off))?;
        let mut left = len;
        while left > 0 {
            let n = left.min(buf.len() as u64) as usize;
            f.read_exact(&mut buf[..n])?;
            w.write_all(&buf[..n])?;
            left -= n as u64;
        }
    }
    w.flush()?;
    Ok(())
}

/// Expand a packed image to `dest`, sparse, via a temporary file.
pub fn unpack(packed: &Path, dest: &Path) -> anyhow::Result<()> {
    let mut r =
        BufReader::new(File::open(packed).with_context(|| format!("open {}", packed.display()))?);
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        bail!("{} is not a packed katamaran image", packed.display());
    }
    let size = read_u64(&mut r)?.context("truncated header")?;
    let tmp = dest.with_extension("img.tmp");
    let mut out = File::create(&tmp)?;
    out.set_len(size)?;
    let mut buf = vec![0u8; 1 << 20];
    while let Some(off) = read_u64(&mut r)? {
        let len = read_u64(&mut r)?.context("truncated record")?;
        if off.checked_add(len).is_none_or(|end| end > size) {
            bail!("record beyond the image size");
        }
        out.seek(SeekFrom::Start(off))?;
        let mut left = len;
        while left > 0 {
            let n = left.min(buf.len() as u64) as usize;
            r.read_exact(&mut buf[..n])?;
            out.write_all(&buf[..n])?;
            left -= n as u64;
        }
    }
    out.sync_all()?;
    fs::rename(&tmp, dest)?;
    Ok(())
}

fn read_u64(r: &mut impl Read) -> io::Result<Option<u64>> {
    let mut b = [0u8; 8];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(u64::from_le_bytes(b))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn round_trips_a_sparse_file() {
        let dir = std::env::temp_dir().join(format!("kat-sparse-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let src = dir.join("src.img");
        let mut f = File::create(&src).unwrap();
        let size = 1u64 << 32;
        f.set_len(size).unwrap();
        for (off, byte) in [(0u64, 1u8), (1 << 20, 2), (size - 4096, 3)] {
            f.seek(SeekFrom::Start(off)).unwrap();
            f.write_all(&vec![byte; 4096]).unwrap();
        }
        f.sync_all().unwrap();
        drop(f);

        let packed = dir.join("src.img.sparse");
        pack(&src, &packed).unwrap();
        assert!(fs::metadata(&packed).unwrap().len() < 4 << 20);
        let out = dir.join("out.img");
        unpack(&packed, &out).unwrap();
        let m = fs::metadata(&out).unwrap();
        assert_eq!(m.len(), size);
        // Still sparse: far fewer blocks than the logical size.
        assert!(m.blocks() * 512 < 64 << 20, "{} blocks", m.blocks());
        let mut a = File::open(&src).unwrap();
        let mut b = File::open(&out).unwrap();
        for off in [0u64, 4095, 1 << 20, (1 << 20) + 100, size - 1, 1 << 30] {
            let (mut x, mut y) = ([0u8; 1], [0u8; 1]);
            a.seek(SeekFrom::Start(off)).unwrap();
            b.seek(SeekFrom::Start(off)).unwrap();
            a.read_exact(&mut x).unwrap();
            b.read_exact(&mut y).unwrap();
            assert_eq!(x, y, "byte {off}");
        }
        assert!(unpack(&src, &dir.join("bad.img")).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }
}
