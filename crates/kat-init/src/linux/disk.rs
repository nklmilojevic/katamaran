//! Grow the btrfs data filesystem at boot when the host extended `data.img`.
//!
//! Comparing `/sys/block/<dev>/size` against `BTRFS_IOC_DEV_INFO`'s recorded
//! `total_bytes` (aligned down to `BTRFS_IOC_FS_INFO`'s `sectorsize`, matching
//! `btrfs_ioctl_resize` in the kernel) avoids false positives on replicated
//! profiles such as DUP where `statvfs` divides total blocks by the replication
//! factor. When `/dev/vdb` is larger than the device's recorded size,
//! `BTRFS_IOC_RESIZE` expands it online in a few milliseconds before
//! containerd, dockerd, machines, or k3s start.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

/// `_IOW(0x94, 3, struct btrfs_ioctl_vol_args)` (4096-byte argument).
const BTRFS_IOC_RESIZE: libc::c_ulong = 0x5000_9403;
/// `_IOWR(0x94, 30, struct btrfs_ioctl_dev_info_args)` (4096-byte argument).
const BTRFS_IOC_DEV_INFO: libc::c_ulong = 0xD000_941E;
/// `_IOR(0x94, 31, struct btrfs_ioctl_fs_info_args)` (1024-byte argument).
const BTRFS_IOC_FS_INFO: libc::c_ulong = 0x8400_941F;

const BTRFS_UUID_SIZE: usize = 16;
const BTRFS_FSID_SIZE: usize = 16;
const BTRFS_DEVICE_PATH_NAME_MAX: usize = 1024;

#[repr(C)]
struct BtrfsIoctlVolArgs {
    fd: i64,
    name: [u8; 4088],
}

#[repr(C)]
struct BtrfsIoctlDevInfoArgs {
    devid: u64,
    uuid: [u8; BTRFS_UUID_SIZE],
    bytes_used: u64,
    total_bytes: u64,
    fsid: [u8; BTRFS_FSID_SIZE],
    unused: [u64; 377],
    path: [u8; BTRFS_DEVICE_PATH_NAME_MAX],
}

#[repr(C)]
struct BtrfsIoctlFsInfoArgs {
    max_id: u64,
    num_devices: u64,
    fsid: [u8; BTRFS_FSID_SIZE],
    nodesize: u32,
    sectorsize: u32,
    clone_alignment: u32,
    csum_type: u16,
    csum_size: u16,
    flags: u64,
    generation: u64,
    metadata_uuid: [u8; BTRFS_FSID_SIZE],
    reserved: [u8; 944],
}

const _: () = assert!(std::mem::size_of::<BtrfsIoctlVolArgs>() == 4096);
const _: () = assert!(std::mem::size_of::<BtrfsIoctlDevInfoArgs>() == 4096);
const _: () = assert!(std::mem::offset_of!(BtrfsIoctlDevInfoArgs, total_bytes) == 32);
const _: () = assert!(std::mem::offset_of!(BtrfsIoctlDevInfoArgs, path) == 3072);
const _: () = assert!(std::mem::size_of::<BtrfsIoctlFsInfoArgs>() == 1024);
const _: () = assert!(std::mem::offset_of!(BtrfsIoctlFsInfoArgs, sectorsize) == 36);

#[derive(Debug, Clone, Copy)]
struct FsInfo {
    max_id: u64,
    num_devices: u64,
    sectorsize: u64,
}

#[derive(Debug, Clone)]
struct DevInfo {
    devid: u64,
    total_bytes: u64,
    path: Option<String>,
}

/// Grow the btrfs filesystem mounted at `mount_point` to fill `dev_path`
/// (e.g. `/dev/vdb`) if the block device is larger than Btrfs's recorded
/// device size. Returns `Ok(Some((before_bytes, after_bytes)))` when a resize
/// ran.
pub fn grow_if_needed(dev_path: &str, mount_point: &str) -> io::Result<Option<(u64, u64)>> {
    let fd = open_dir(mount_point)?;
    grow_if_needed_with(
        dev_path,
        || block_device_bytes(dev_path),
        || btrfs_fs_info(fd.as_raw_fd()),
        |devid| btrfs_dev_info(fd.as_raw_fd(), devid),
        |devid| btrfs_resize_max(fd.as_raw_fd(), devid),
    )
}

fn grow_if_needed_with(
    dev_path: &str,
    mut read_dev_bytes: impl FnMut() -> io::Result<u64>,
    mut read_fs_info: impl FnMut() -> io::Result<FsInfo>,
    mut read_dev_info: impl FnMut(u64) -> io::Result<Option<DevInfo>>,
    mut resize_max: impl FnMut(u64) -> io::Result<()>,
) -> io::Result<Option<(u64, u64)>> {
    let dev_bytes = read_dev_bytes()?;
    let fs_info = read_fs_info()?;
    let target = find_target_device(dev_path, fs_info, &mut read_dev_info)?;
    if !needs_resize(dev_bytes, target.total_bytes, fs_info.sectorsize) {
        return Ok(None);
    }
    resize_max(target.devid)?;
    let after = read_dev_info(target.devid)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("btrfs devid {} disappeared after resize", target.devid),
        )
    })?;
    Ok(Some((target.total_bytes, after.total_bytes)))
}

fn find_target_device(
    dev_path: &str,
    fs_info: FsInfo,
    read_dev_info: &mut impl FnMut(u64) -> io::Result<Option<DevInfo>>,
) -> io::Result<DevInfo> {
    if let Some(info) = read_dev_info(1)?
        && (fs_info.num_devices <= 1 || info.path.as_deref() == Some(dev_path))
    {
        return Ok(info);
    }
    let max_id = fs_info.max_id.min(1024);
    for devid in 2..=max_id {
        if let Some(info) = read_dev_info(devid)?
            && (info.path.as_deref() == Some(dev_path) || fs_info.num_devices == 1)
        {
            return Ok(info);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("btrfs device {dev_path} not found"),
    ))
}

fn open_dir(mount_point: &str) -> io::Result<OwnedFd> {
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
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
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

fn btrfs_fs_info(fd: libc::c_int) -> io::Result<FsInfo> {
    let mut args: BtrfsIoctlFsInfoArgs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::ioctl(fd, BTRFS_IOC_FS_INFO as _, &mut args) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(FsInfo {
        max_id: args.max_id,
        num_devices: args.num_devices,
        sectorsize: u64::from(args.sectorsize),
    })
}

fn btrfs_dev_info(fd: libc::c_int, devid: u64) -> io::Result<Option<DevInfo>> {
    let mut args: BtrfsIoctlDevInfoArgs = unsafe { std::mem::zeroed() };
    args.devid = devid;
    let rc = unsafe { libc::ioctl(fd, BTRFS_IOC_DEV_INFO as _, &mut args) };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENODEV) {
            return Ok(None);
        }
        return Err(err);
    }
    let path_len = args
        .path
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(args.path.len());
    let path = (!args.path[..path_len].is_empty())
        .then(|| String::from_utf8_lossy(&args.path[..path_len]).into_owned());
    Ok(Some(DevInfo {
        devid: args.devid,
        total_bytes: args.total_bytes,
        path,
    }))
}

fn needs_resize(dev_bytes: u64, btrfs_dev_total_bytes: u64, sector_size: u64) -> bool {
    let unit = sector_size.max(1);
    let aligned_dev = dev_bytes - (dev_bytes % unit);
    aligned_dev > btrfs_dev_total_bytes
}

fn btrfs_resize_max(fd: libc::c_int, devid: u64) -> io::Result<()> {
    let args = resize_max_args(devid);
    let rc = unsafe { libc::ioctl(fd, BTRFS_IOC_RESIZE as _, &args) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    if rc > 0 {
        return Err(io::Error::other(format!(
            "btrfs resize returned error code {rc}"
        )));
    }
    Ok(())
}

fn resize_max_args(devid: u64) -> BtrfsIoctlVolArgs {
    let mut args = BtrfsIoctlVolArgs {
        fd: 0,
        name: [0u8; 4088],
    };
    let spec = if devid == 1 {
        "max".to_string()
    } else {
        format!("{devid}:max")
    };
    let bytes = spec.as_bytes();
    args.name[..bytes.len()].copy_from_slice(bytes);
    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

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
        assert!(!needs_resize(64 * gib + 8192, 64 * gib, 16384));
        assert!(needs_resize(64 * gib + 16384, 64 * gib, 16384));
        assert!(needs_resize(500 * gib, 64 * gib, 4096));
        assert!(!needs_resize(32 * gib, 64 * gib, 4096));
        assert!(needs_resize(100, 99, 0));
    }

    #[test]
    fn resize_args_carry_nul_terminated_spec() {
        let args = resize_max_args(1);
        assert_eq!(args.fd, 0);
        assert_eq!(&args.name[..4], b"max\0");
        assert!(args.name[3..].iter().all(|&b| b == 0));

        let dev2 = resize_max_args(2);
        assert_eq!(&dev2.name[..6], b"2:max\0");
        assert!(dev2.name[5..].iter().all(|&b| b == 0));
    }

    #[test]
    fn full_size_device_with_replicated_data_skips_resize() {
        // Regression test: on a single-device Btrfs filesystem with DUP data
        // profile, statvfs reports half the device capacity, but
        // BTRFS_IOC_DEV_INFO still reports the full 64 GiB device total_bytes.
        // We must NOT issue BTRFS_IOC_RESIZE when total_bytes already matches
        // the block device.
        let gib = 1u64 << 30;
        let resized = Cell::new(false);
        let res = grow_if_needed_with(
            "/dev/vdb",
            || Ok(64 * gib),
            || {
                Ok(FsInfo {
                    max_id: 1,
                    num_devices: 1,
                    sectorsize: 4096,
                })
            },
            |devid| {
                assert_eq!(devid, 1);
                Ok(Some(DevInfo {
                    devid: 1,
                    total_bytes: 64 * gib,
                    path: Some("/dev/vdb".into()),
                }))
            },
            |_| {
                resized.set(true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(res, None);
        assert!(!resized.get());
    }

    #[test]
    fn grows_device_and_reports_btrfs_dev_totals_before_and_after() {
        let gib = 1u64 << 30;
        let current_total = Cell::new(64 * gib);
        let resized_devid = Cell::new(None);
        let res = grow_if_needed_with(
            "/dev/vdb",
            || Ok(500 * gib),
            || {
                Ok(FsInfo {
                    max_id: 2,
                    num_devices: 1,
                    sectorsize: 4096,
                })
            },
            |devid| match devid {
                1 => Ok(None),
                2 => Ok(Some(DevInfo {
                    devid: 2,
                    total_bytes: current_total.get(),
                    path: Some("/dev/vdb".into()),
                })),
                _ => Ok(None),
            },
            |devid| {
                resized_devid.set(Some(devid));
                current_total.set(500 * gib);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(resized_devid.get(), Some(2));
        assert_eq!(res, Some((64 * gib, 500 * gib)));
    }

    #[test]
    fn propagates_resize_ioctl_errors() {
        let gib = 1u64 << 30;
        let err = grow_if_needed_with(
            "/dev/vdb",
            || Ok(500 * gib),
            || {
                Ok(FsInfo {
                    max_id: 1,
                    num_devices: 1,
                    sectorsize: 4096,
                })
            },
            |_| {
                Ok(Some(DevInfo {
                    devid: 1,
                    total_bytes: 64 * gib,
                    path: Some("/dev/vdb".into()),
                }))
            },
            |_| Err(io::Error::from_raw_os_error(libc::EROFS)),
        )
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EROFS));
    }
}
