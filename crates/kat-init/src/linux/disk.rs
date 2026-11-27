//! Grow the btrfs data filesystem at boot when the host requested a disk
//! target via `kat.disk_gib=<gib>` on the kernel command line.
//!
//! Comparing `/sys/block/<dev>/size` and the requested target against
//! `BTRFS_IOC_DEV_INFO`'s recorded `total_bytes` (aligned down to
//! `BTRFS_IOC_FS_INFO`'s `sectorsize`, matching `btrfs_ioctl_resize` in the
//! kernel) avoids false positives on replicated profiles such as DUP where
//! `statvfs` divides total blocks by the replication factor. When growth is
//! needed, `BTRFS_IOC_RESIZE` expands it online in a few milliseconds before
//! containerd, dockerd, machines, or k3s start.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

use crate::disk::{
    DevInfo, FsInfo, ResizePlan, grow_if_needed_with, parse_sysfs_sectors, resize_spec,
    sysfs_live_devids_in,
};

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
const _: () = assert!(std::mem::offset_of!(BtrfsIoctlFsInfoArgs, fsid) == 16);
const _: () = assert!(std::mem::offset_of!(BtrfsIoctlFsInfoArgs, sectorsize) == 36);

/// Grow the btrfs filesystem mounted at `mount_point` up to `requested_bytes`
/// (capped at `dev_path`'s block-device size) if `requested_bytes` is `Some`
/// and exceeds Btrfs's recorded device size. Returns
/// `Ok(Some((before_bytes, after_bytes)))` when a resize ran.
pub fn grow_if_needed(
    dev_path: &str,
    mount_point: &str,
    requested_bytes: Option<u64>,
) -> io::Result<Option<(u64, u64)>> {
    if requested_bytes.is_none() {
        return Ok(None);
    }
    let fd = open_dir(mount_point)?;
    grow_if_needed_with(
        dev_path,
        requested_bytes,
        || block_device_bytes(dev_path),
        || btrfs_fs_info(fd.as_raw_fd()),
        |fsid| sysfs_live_devids_in(Path::new("/sys/fs/btrfs"), fsid),
        |devid| btrfs_dev_info(fd.as_raw_fd(), devid),
        |devid, plan| btrfs_resize(fd.as_raw_fd(), devid, plan),
    )
}

fn open_dir(mount_point: &str) -> io::Result<OwnedFd> {
    let path = CString::new(mount_point)?;
    loop {
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if fd >= 0 {
            return Ok(unsafe { OwnedFd::from_raw_fd(fd) });
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
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

fn btrfs_fs_info(fd: libc::c_int) -> io::Result<FsInfo> {
    let mut args: BtrfsIoctlFsInfoArgs = unsafe { std::mem::zeroed() };
    loop {
        let rc = unsafe { libc::ioctl(fd, BTRFS_IOC_FS_INFO as _, &mut args) };
        if rc >= 0 {
            break;
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
    Ok(FsInfo {
        max_id: args.max_id,
        num_devices: args.num_devices,
        fsid: args.fsid,
        sectorsize: u64::from(args.sectorsize),
    })
}

fn btrfs_dev_info(fd: libc::c_int, devid: u64) -> io::Result<Option<DevInfo>> {
    let mut args: BtrfsIoctlDevInfoArgs = unsafe { std::mem::zeroed() };
    args.devid = devid;
    loop {
        let rc = unsafe { libc::ioctl(fd, BTRFS_IOC_DEV_INFO as _, &mut args) };
        if rc >= 0 {
            break;
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
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

fn btrfs_resize(fd: libc::c_int, devid: u64, plan: ResizePlan) -> io::Result<()> {
    let Some(args) = resize_args(devid, plan) else {
        return Ok(());
    };
    let rc = loop {
        let rc = unsafe { libc::ioctl(fd, BTRFS_IOC_RESIZE as _, &args) };
        if rc >= 0 {
            break rc;
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    };
    if rc > 0 {
        return Err(io::Error::other(format!(
            "btrfs resize returned error code {rc}"
        )));
    }
    Ok(())
}

fn resize_args(devid: u64, plan: ResizePlan) -> Option<BtrfsIoctlVolArgs> {
    let spec = resize_spec(devid, plan)?;
    let mut args = BtrfsIoctlVolArgs {
        fd: 0,
        name: [0u8; 4088],
    };
    let bytes = spec.as_bytes();
    args.name[..bytes.len()].copy_from_slice(bytes);
    Some(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resize_args_carry_nul_terminated_spec() {
        assert!(resize_args(1, ResizePlan::None).is_none());

        let args = resize_args(1, ResizePlan::Max).unwrap();
        assert_eq!(args.fd, 0);
        assert_eq!(&args.name[..4], b"max\0");
        assert!(args.name[3..].iter().all(|&b| b == 0));

        let dev2 = resize_args(2, ResizePlan::Max).unwrap();
        assert_eq!(&dev2.name[..6], b"2:max\0");
        assert!(dev2.name[5..].iter().all(|&b| b == 0));

        let exact = resize_args(1, ResizePlan::Bytes(100u64 << 30)).unwrap();
        assert_eq!(&exact.name[..13], b"107374182400\0");
        assert!(exact.name[12..].iter().all(|&b| b == 0));

        let exact_dev2 = resize_args(2, ResizePlan::Bytes(100u64 << 30)).unwrap();
        assert_eq!(&exact_dev2.name[..15], b"2:107374182400\0");
        assert!(exact_dev2.name[14..].iter().all(|&b| b == 0));
    }
}
