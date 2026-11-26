//! Grow the btrfs data filesystem at boot when the host extended `data.img`.
//!
//! Comparing `/sys/block/<dev>/size` against `statvfs` costs two syscalls on
//! normal boots when the image has not changed. When `/dev/vdb` is larger than
//! the mounted filesystem, `BTRFS_IOC_RESIZE` with `"max"` (the same ioctl
//! `btrfs filesystem resize max` issues) expands the filesystem online in a
//! few milliseconds before containerd, dockerd, machines, or k3s start.

use std::ffi::CString;
use std::io;
use std::path::Path;

/// `_IOW(0x94, 3, struct btrfs_ioctl_vol_args)` (4096-byte argument).
const BTRFS_IOC_RESIZE: libc::c_ulong = 0x5000_9403;

#[repr(C)]
struct BtrfsIoctlVolArgs {
    fd: i64,
    name: [u8; 4088],
}

const _: () = assert!(std::mem::size_of::<BtrfsIoctlVolArgs>() == 4096);

/// Grow the btrfs filesystem mounted at `mount_point` to fill `dev_path`
/// (e.g. `/dev/vdb`) if the block device is larger than the filesystem.
/// Returns `Ok(Some((before_bytes, after_bytes)))` when a resize ran.
pub fn grow_if_needed(dev_path: &str, mount_point: &str) -> io::Result<Option<(u64, u64)>> {
    let dev_bytes = block_device_bytes(dev_path)?;
    let (fs_bytes, block_size) = fs_total_bytes(mount_point)?;
    if !needs_resize(dev_bytes, fs_bytes, block_size) {
        return Ok(None);
    }
    btrfs_resize_max(mount_point)?;
    let (after_bytes, _) = fs_total_bytes(mount_point)?;
    Ok(Some((fs_bytes, after_bytes)))
}

fn block_device_bytes(dev_path: &str) -> io::Result<u64> {
    let name = Path::new(dev_path)
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid device {dev_path}"),
            )
        })?;
    let sysfs = format!("/sys/block/{name}/size");
    let text = std::fs::read_to_string(&sysfs)
        .map_err(|e| io::Error::new(e.kind(), format!("read {sysfs}: {e}")))?;
    parse_sysfs_sectors(&text)
}

fn parse_sysfs_sectors(text: &str) -> io::Result<u64> {
    let sectors: u64 = text.trim().parse().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse sector count {text:?}: {e}"),
        )
    })?;
    sectors
        .checked_mul(512)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "block device size overflow"))
}

fn fs_total_bytes(mount_point: &str) -> io::Result<(u64, u64)> {
    let path = CString::new(mount_point)?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut st) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let unit = if st.f_frsize != 0 {
        st.f_frsize as u64
    } else {
        st.f_bsize as u64
    };
    let unit = unit.max(1);
    let total = (st.f_blocks as u64).saturating_mul(unit);
    Ok((total, unit))
}

fn needs_resize(dev_bytes: u64, fs_bytes: u64, block_size: u64) -> bool {
    let unit = block_size.max(1);
    let aligned_dev = dev_bytes - (dev_bytes % unit);
    aligned_dev > fs_bytes
}

fn btrfs_resize_max(mount_point: &str) -> io::Result<()> {
    let path = CString::new(mount_point)?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let args = resize_max_args();
    let rc = unsafe { libc::ioctl(fd, BTRFS_IOC_RESIZE as _, &args) };
    let err = io::Error::last_os_error();
    unsafe { libc::close(fd) };
    if rc < 0 {
        return Err(err);
    }
    if rc > 0 {
        return Err(io::Error::other(format!(
            "btrfs resize returned error code {rc}"
        )));
    }
    Ok(())
}

fn resize_max_args() -> BtrfsIoctlVolArgs {
    let mut args = BtrfsIoctlVolArgs {
        fd: 0,
        name: [0u8; 4088],
    };
    args.name[..3].copy_from_slice(b"max");
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sysfs_sector_counts() {
        // 64 GiB = 134_217_728 sectors of 512 bytes.
        assert_eq!(parse_sysfs_sectors("134217728\n").unwrap(), 64u64 << 30);
        assert_eq!(parse_sysfs_sectors("  2048  ").unwrap(), 1u64 << 20);
        assert!(parse_sysfs_sectors("not-a-number").is_err());
        assert!(parse_sysfs_sectors(&u64::MAX.to_string()).is_err());
    }

    #[test]
    fn detects_when_resize_is_needed() {
        let gib = 1u64 << 30;
        assert!(!needs_resize(64 * gib, 64 * gib, 4096));
        assert!(!needs_resize(64 * gib + 2048, 64 * gib, 4096));
        assert!(needs_resize(64 * gib + 4096, 64 * gib, 4096));
        assert!(needs_resize(500 * gib, 64 * gib, 4096));
        assert!(!needs_resize(32 * gib, 64 * gib, 4096));
        assert!(needs_resize(100, 99, 0));
    }

    #[test]
    fn resize_args_carry_nul_terminated_max() {
        let args = resize_max_args();
        assert_eq!(args.fd, 0);
        assert_eq!(&args.name[..4], b"max\0");
        assert!(args.name[3..].iter().all(|&b| b == 0));
    }
}
