//! Grow-only Btrfs data-disk resize planning and `/proc/cmdline` parsing.
//!
//! Kept platform-independent so all planning, cmdline-gating, and error-path
//! unit tests run under `cargo test` on macOS as well as on Linux. Linux-only
//! ioctls live in `crate::linux::disk`.

use std::io;
use std::path::Path;

/// Minimum explicit `kat.disk_gib` value accepted from the host (matches
/// `kat_daemon::config::MIN_DISK_GIB`).
pub const MIN_DISK_GIB: u32 = 64;
/// Maximum explicit `kat.disk_gib` value accepted from the host (matches
/// `kat_daemon::config::MAX_DISK_GIB`).
pub const MAX_DISK_GIB: u32 = 16384;
/// Upper bound on sequential `devid` probes when sysfs
/// (`/sys/fs/btrfs/<fsid>/devinfo`) is unavailable on a multi-device image.
pub const MAX_FALLBACK_SCAN_DEVID: u64 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsInfo {
    pub max_id: u64,
    pub num_devices: u64,
    pub fsid: [u8; 16],
    pub sectorsize: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevInfo {
    pub devid: u64,
    pub total_bytes: u64,
    pub path: Option<String>,
}

/// Concrete online Btrfs resize action to perform for the data disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResizePlan {
    /// No resize needed (either no target was requested, or the filesystem
    /// already covers the requested target).
    None,
    /// Grow the device to fill the entire attached block device (`max`).
    Max,
    /// Grow the device to an exact sector-aligned byte size strictly smaller
    /// than the block device (e.g. a pre-padded custom image with a smaller
    /// explicit growth target).
    Bytes(u64),
}

/// Requested grow-only data-disk target in bytes from `kat.disk_gib=<gib>` on
/// `/proc/cmdline`, or `None` when the host did not ask to grow the filesystem.
///
/// Leaving this `None` when no disk target is configured guarantees that an
/// already-padded custom `data.img` is never unexpectedly expanded on boot.
pub fn target_bytes_from_cmdline(cmdline: &str) -> Option<u64> {
    let gib = cmdline
        .split_whitespace()
        .filter_map(|a| a.strip_prefix("kat.disk_gib="))
        .next_back()?
        .parse::<u32>()
        .ok()?;
    if !(MIN_DISK_GIB..=MAX_DISK_GIB).contains(&gib) {
        return None;
    }
    Some(u64::from(gib) << 30)
}

/// Parse `/sys/block/<dev>/size` (512-byte sectors) into bytes.
pub fn parse_sysfs_sectors(text: &str) -> io::Result<u64> {
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

/// Decide whether and how much to grow Btrfs given the optional host target,
/// the block device size, Btrfs's recorded device size, and the filesystem's
/// `sectorsize`. Never shrinks and never grows without an explicit target.
pub fn plan_resize(
    requested_bytes: Option<u64>,
    dev_bytes: u64,
    btrfs_dev_total_bytes: u64,
    sector_size: u64,
) -> ResizePlan {
    let Some(requested) = requested_bytes else {
        return ResizePlan::None;
    };
    let unit = sector_size.max(1);
    let aligned_dev = dev_bytes - (dev_bytes % unit);
    let capped = requested.min(dev_bytes);
    let aligned_target = capped - (capped % unit);
    if aligned_target <= btrfs_dev_total_bytes {
        return ResizePlan::None;
    }
    if aligned_target == aligned_dev {
        ResizePlan::Max
    } else {
        ResizePlan::Bytes(aligned_target)
    }
}

/// Format the NUL-free `btrfs_ioctl_vol_args.name` resize specification for
/// `devid` and `plan`.
pub fn resize_spec(devid: u64, plan: ResizePlan) -> Option<String> {
    let size = match plan {
        ResizePlan::None => return None,
        ResizePlan::Max => "max".to_string(),
        ResizePlan::Bytes(bytes) => bytes.to_string(),
    };
    Some(if devid == 1 {
        size
    } else {
        format!("{devid}:{size}")
    })
}

/// Format a 16-byte Btrfs `fsid` as the lowercase big-endian UUID used by
/// Linux's `/sys/fs/btrfs/<uuid>/` (`%pU`).
pub fn format_uuid(bytes: [u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

/// Enumerate live Btrfs device IDs from `<btrfs_sysfs_root>/<fsid>/devinfo`
/// (on Linux, `/sys/fs/btrfs/<uuid>/devinfo`). Returns `None` if sysfs is
/// unavailable or has no numeric entries so callers can fall back cleanly.
pub fn sysfs_live_devids_in(btrfs_sysfs_root: &Path, fsid: [u8; 16]) -> Option<Vec<u64>> {
    let dir = btrfs_sysfs_root.join(format_uuid(fsid)).join("devinfo");
    let entries = std::fs::read_dir(dir).ok()?;
    let mut devids = Vec::new();
    for entry in entries.flatten() {
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
            && id > 0
        {
            devids.push(id);
        }
    }
    devids.sort_unstable();
    devids.dedup();
    (!devids.is_empty()).then_some(devids)
}

pub fn find_target_device(
    dev_path: &str,
    fs_info: FsInfo,
    list_live_devids: &mut impl FnMut([u8; 16]) -> Option<Vec<u64>>,
    read_dev_info: &mut impl FnMut(u64) -> io::Result<Option<DevInfo>>,
) -> io::Result<DevInfo> {
    let matches =
        |info: &DevInfo| fs_info.num_devices <= 1 || info.path.as_deref() == Some(dev_path);
    // `BTRFS_IOC_FS_INFO` sets `max_id` to the highest live devid in the
    // filesystem. Checking `max_id` first handles standard devid=1 images AND
    // single-device custom images with any devid in a single ioctl.
    let mut seen = 0u64;
    if fs_info.max_id > 0
        && let Some(info) = read_dev_info(fs_info.max_id)?
    {
        if matches(&info) {
            return Ok(info);
        }
        seen += 1;
    }
    if fs_info.max_id > 1 && seen < fs_info.num_devices {
        // On multi-device images, enumerate exact live device IDs from sysfs
        // (`/sys/fs/btrfs/<fsid>/devinfo`) so high devids below `max_id` are
        // found in O(num_devices) without probing unused IDs.
        if let Some(live_ids) = list_live_devids(fs_info.fsid) {
            for devid in live_ids {
                if devid == fs_info.max_id {
                    continue;
                }
                if let Some(info) = read_dev_info(devid)? {
                    if matches(&info) {
                        return Ok(info);
                    }
                    seen += 1;
                    if seen >= fs_info.num_devices {
                        break;
                    }
                }
            }
        } else {
            // Strictly bounded fallback when sysfs is unavailable so a sparse
            // high `max_id` can never stall guest boot.
            let upper = (fs_info.max_id - 1).min(MAX_FALLBACK_SCAN_DEVID);
            for devid in 1..=upper {
                if let Some(info) = read_dev_info(devid)? {
                    if matches(&info) {
                        return Ok(info);
                    }
                    seen += 1;
                    if seen >= fs_info.num_devices {
                        break;
                    }
                }
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("btrfs device {dev_path} not found"),
    ))
}

/// Pure driver shared by Linux `grow_if_needed` and host unit tests.
///
/// Short-circuits immediately with `Ok(None)` when `requested_bytes` is `None`
/// so no block-device reads or Btrfs ioctls execute when the host did not ask
/// for a resize.
pub fn grow_if_needed_with(
    dev_path: &str,
    requested_bytes: Option<u64>,
    mut read_dev_bytes: impl FnMut() -> io::Result<u64>,
    mut read_fs_info: impl FnMut() -> io::Result<FsInfo>,
    mut list_live_devids: impl FnMut([u8; 16]) -> Option<Vec<u64>>,
    mut read_dev_info: impl FnMut(u64) -> io::Result<Option<DevInfo>>,
    mut resize: impl FnMut(u64, ResizePlan) -> io::Result<()>,
) -> io::Result<Option<(u64, u64)>> {
    let Some(requested) = requested_bytes else {
        return Ok(None);
    };
    let dev_bytes = read_dev_bytes()?;
    let fs_info = read_fs_info()?;
    let target = find_target_device(dev_path, fs_info, &mut list_live_devids, &mut read_dev_info)?;
    let plan = plan_resize(
        Some(requested),
        dev_bytes,
        target.total_bytes,
        fs_info.sectorsize,
    );
    if plan == ResizePlan::None {
        return Ok(None);
    }
    resize(target.devid, plan)?;
    let after = read_dev_info(target.devid)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("btrfs devid {} disappeared after resize", target.devid),
        )
    })?;
    Ok(Some((target.total_bytes, after.total_bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const GIB: u64 = 1 << 30;

    #[test]
    fn reads_target_bytes_from_cmdline() {
        assert_eq!(target_bytes_from_cmdline(""), None);
        assert_eq!(
            target_bytes_from_cmdline("console=hvc0 kat.nofile=1048576 kat.k8s=1"),
            None
        );
        assert_eq!(target_bytes_from_cmdline("kat.disk_gib=lots"), None);
        assert_eq!(target_bytes_from_cmdline("kat.disk_gib=0"), None);
        assert_eq!(target_bytes_from_cmdline("kat.disk_gib=63"), None);
        assert_eq!(target_bytes_from_cmdline("kat.disk_gib=16385"), None);

        assert_eq!(
            target_bytes_from_cmdline("console=hvc0 kat.disk_gib=64"),
            Some(64 * GIB)
        );
        assert_eq!(
            target_bytes_from_cmdline("kat.disk_gib=100 kat.disk_gib=500\n"),
            Some(500 * GIB)
        );
        assert_eq!(
            target_bytes_from_cmdline("kat.disk_gib=16384"),
            Some(16384 * GIB)
        );
    }

    #[test]
    fn parses_sysfs_sector_counts() {
        // 64 GiB = 134_217_728 sectors of 512 bytes.
        assert_eq!(parse_sysfs_sectors("134217728\n").unwrap(), 64 * GIB);
        assert_eq!(parse_sysfs_sectors("  2048  ").unwrap(), 1u64 << 20);
        assert!(parse_sysfs_sectors("not-a-number").is_err());
        assert!(parse_sysfs_sectors(&u64::MAX.to_string()).is_err());
    }

    #[test]
    fn plans_resize_only_when_target_is_requested_and_larger_than_filesystem() {
        // Unset target leaves even an already-padded custom image untouched.
        assert_eq!(
            plan_resize(None, 100 * GIB, 64 * GIB, 4096),
            ResizePlan::None
        );
        // Exact match skips resize.
        assert_eq!(
            plan_resize(Some(64 * GIB), 64 * GIB, 64 * GIB, 4096),
            ResizePlan::None
        );
        // Sub-sector slack is ignored; a full sector triggers growth to max.
        assert_eq!(
            plan_resize(Some(65 * GIB), 64 * GIB + 2048, 64 * GIB, 4096),
            ResizePlan::None
        );
        assert_eq!(
            plan_resize(Some(65 * GIB), 64 * GIB + 4096, 64 * GIB, 4096),
            ResizePlan::Max
        );
        assert_eq!(
            plan_resize(Some(65 * GIB), 64 * GIB + 8192, 64 * GIB, 16384),
            ResizePlan::None
        );
        assert_eq!(
            plan_resize(Some(65 * GIB), 64 * GIB + 16384, 64 * GIB, 16384),
            ResizePlan::Max
        );
        // Normal requested growth where host extended data.img to 500 GiB.
        assert_eq!(
            plan_resize(Some(500 * GIB), 500 * GIB, 64 * GIB, 4096),
            ResizePlan::Max
        );
        // Fractional 63.5 GiB image raised explicitly to 64 GiB grows to max.
        assert_eq!(
            plan_resize(Some(64 * GIB), 64 * GIB, 63 * GIB + (512 << 20), 4096),
            ResizePlan::Max
        );
        // Pre-padded 200 GiB image with an explicit 100 GiB target grows only
        // to the requested 100 GiB, not the full padded 200 GiB device.
        assert_eq!(
            plan_resize(Some(100 * GIB), 200 * GIB, 64 * GIB, 4096),
            ResizePlan::Bytes(100 * GIB)
        );
        // Never shrinks when the filesystem already exceeds the target.
        assert_eq!(
            plan_resize(Some(100 * GIB), 500 * GIB, 500 * GIB, 4096),
            ResizePlan::None
        );
        assert_eq!(
            plan_resize(Some(64 * GIB), 32 * GIB, 64 * GIB, 4096),
            ResizePlan::None
        );
        // Zero sectorsize falls back safely to byte granularity.
        assert_eq!(plan_resize(Some(100), 100, 99, 0), ResizePlan::Max);
    }

    #[test]
    fn formats_resize_specs() {
        assert_eq!(resize_spec(1, ResizePlan::None), None);
        assert_eq!(resize_spec(1, ResizePlan::Max).as_deref(), Some("max"));
        assert_eq!(resize_spec(2, ResizePlan::Max).as_deref(), Some("2:max"));
        assert_eq!(
            resize_spec(1, ResizePlan::Bytes(100 * GIB)).as_deref(),
            Some("107374182400")
        );
        assert_eq!(
            resize_spec(2, ResizePlan::Bytes(100 * GIB)).as_deref(),
            Some("2:107374182400")
        );
    }

    #[test]
    fn formats_btrfs_fsid_as_linux_sysfs_uuid_and_reads_devinfo_dir() {
        let fsid = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        assert_eq!(format_uuid(fsid), "00010203-0405-0607-0809-0a0b0c0d0e0f");

        let root = std::env::temp_dir().join(format!("kat-btrfs-sysfs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let devinfo = root.join(format_uuid(fsid)).join("devinfo");
        std::fs::create_dir_all(devinfo.join("1000000")).unwrap();
        std::fs::create_dir_all(devinfo.join("500000")).unwrap();
        std::fs::create_dir_all(devinfo.join("not-a-devid")).unwrap();
        std::fs::create_dir_all(devinfo.join("0")).unwrap();

        assert_eq!(
            sysfs_live_devids_in(&root, fsid),
            Some(vec![500_000, 1_000_000])
        );
        assert_eq!(sysfs_live_devids_in(&root, [0u8; 16]), None);

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn unset_target_leaves_already_padded_custom_image_untouched() {
        // When the host omits `kat.disk_gib=` (no growth requested), the guest
        // must not inspect or resize an already-padded custom image at boot.
        let res = grow_if_needed_with(
            "/dev/vdb",
            None,
            || unreachable!("block device size should not be read when no target is set"),
            || unreachable!("BTRFS_IOC_FS_INFO should not run when no target is set"),
            |_| unreachable!("sysfs devinfo should not be read when no target is set"),
            |_| unreachable!("BTRFS_IOC_DEV_INFO should not run when no target is set"),
            |_, _| unreachable!("BTRFS_IOC_RESIZE should not run when no target is set"),
        )
        .unwrap();
        assert_eq!(res, None);
    }

    #[test]
    fn full_size_device_with_replicated_data_skips_resize() {
        // Regression test: on a single-device Btrfs filesystem with DUP data
        // profile, statvfs reports half the device capacity, but
        // BTRFS_IOC_DEV_INFO still reports the full 64 GiB device total_bytes.
        // We must NOT issue BTRFS_IOC_RESIZE when total_bytes already matches
        // the requested target.
        let resized = Cell::new(false);
        let res = grow_if_needed_with(
            "/dev/vdb",
            Some(64 * GIB),
            || Ok(64 * GIB),
            || {
                Ok(FsInfo {
                    max_id: 1,
                    num_devices: 1,
                    fsid: [0u8; 16],
                    sectorsize: 4096,
                })
            },
            |_| unreachable!("single-device fast path should not need sysfs"),
            |devid| {
                assert_eq!(devid, 1);
                Ok(Some(DevInfo {
                    devid: 1,
                    total_bytes: 64 * GIB,
                    path: Some("/dev/vdb".into()),
                }))
            },
            |_, _| {
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
        let current_total = Cell::new(64 * GIB);
        let resize_call = Cell::new(None);
        let res = grow_if_needed_with(
            "/dev/vdb",
            Some(500 * GIB),
            || Ok(500 * GIB),
            || {
                Ok(FsInfo {
                    max_id: 2,
                    num_devices: 1,
                    fsid: [0u8; 16],
                    sectorsize: 4096,
                })
            },
            |_| unreachable!("single-device fast path should not need sysfs"),
            |devid| match devid {
                1 => Ok(None),
                2 => Ok(Some(DevInfo {
                    devid: 2,
                    total_bytes: current_total.get(),
                    path: Some("/dev/vdb".into()),
                })),
                _ => Ok(None),
            },
            |devid, plan| {
                resize_call.set(Some((devid, plan)));
                current_total.set(500 * GIB);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(resize_call.get(), Some((2, ResizePlan::Max)));
        assert_eq!(res, Some((64 * GIB, 500 * GIB)));
    }

    #[test]
    fn grows_pre_padded_image_only_up_to_requested_target() {
        let current_total = Cell::new(64 * GIB);
        let resize_call = Cell::new(None);
        let res = grow_if_needed_with(
            "/dev/vdb",
            Some(100 * GIB),
            || Ok(200 * GIB),
            || {
                Ok(FsInfo {
                    max_id: 1,
                    num_devices: 1,
                    fsid: [0u8; 16],
                    sectorsize: 4096,
                })
            },
            |_| unreachable!("single-device fast path should not need sysfs"),
            |_| {
                Ok(Some(DevInfo {
                    devid: 1,
                    total_bytes: current_total.get(),
                    path: Some("/dev/vdb".into()),
                }))
            },
            |devid, plan| {
                resize_call.set(Some((devid, plan)));
                assert_eq!(plan, ResizePlan::Bytes(100 * GIB));
                current_total.set(100 * GIB);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(resize_call.get(), Some((1, ResizePlan::Bytes(100 * GIB))));
        assert_eq!(res, Some((64 * GIB, 100 * GIB)));
    }

    #[test]
    fn propagates_resize_ioctl_errors() {
        let err = grow_if_needed_with(
            "/dev/vdb",
            Some(500 * GIB),
            || Ok(500 * GIB),
            || {
                Ok(FsInfo {
                    max_id: 1,
                    num_devices: 1,
                    fsid: [0u8; 16],
                    sectorsize: 4096,
                })
            },
            |_| unreachable!("single-device fast path should not need sysfs"),
            |_| {
                Ok(Some(DevInfo {
                    devid: 1,
                    total_bytes: 64 * GIB,
                    path: Some("/dev/vdb".into()),
                }))
            },
            |_, _| Err(io::Error::from_raw_os_error(libc::EROFS)),
        )
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EROFS));
    }

    #[test]
    fn grows_single_device_image_with_devid_above_1024_in_one_lookup() {
        let high_devid = 5000u64;
        let current_total = Cell::new(64 * GIB);
        let lookups = Cell::new(0u32);
        let resize_call = Cell::new(None);
        let res = grow_if_needed_with(
            "/dev/vdb",
            Some(500 * GIB),
            || Ok(500 * GIB),
            || {
                Ok(FsInfo {
                    max_id: high_devid,
                    num_devices: 1,
                    fsid: [0u8; 16],
                    sectorsize: 4096,
                })
            },
            |_| unreachable!("single-device fast path should not need sysfs"),
            |devid| {
                lookups.set(lookups.get() + 1);
                if devid == high_devid {
                    Ok(Some(DevInfo {
                        devid: high_devid,
                        total_bytes: current_total.get(),
                        path: Some("/dev/vdb".into()),
                    }))
                } else {
                    Ok(None)
                }
            },
            |devid, plan| {
                resize_call.set(Some((devid, plan)));
                current_total.set(500 * GIB);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(resize_call.get(), Some((high_devid, ResizePlan::Max)));
        assert_eq!(res, Some((64 * GIB, 500 * GIB)));
        // One lookup to find devid=5000, one lookup after resize to verify size.
        assert_eq!(lookups.get(), 2);
        assert_eq!(
            resize_spec(high_devid, ResizePlan::Max).as_deref(),
            Some("5000:max")
        );
    }

    #[test]
    fn finds_high_devid_in_multi_device_filesystem_via_sysfs_without_scanning_unused_ids() {
        let fsid = [7u8; 16];
        let mut probed = Vec::new();
        let found = find_target_device(
            "/dev/vdb",
            FsInfo {
                max_id: 1_000_000,
                num_devices: 2,
                fsid,
                sectorsize: 4096,
            },
            &mut |id| {
                assert_eq!(id, fsid);
                Some(vec![500_000, 1_000_000])
            },
            &mut |devid| {
                probed.push(devid);
                match devid {
                    500_000 => Ok(Some(DevInfo {
                        devid: 500_000,
                        total_bytes: 64 * GIB,
                        path: Some("/dev/vdb".into()),
                    })),
                    1_000_000 => Ok(Some(DevInfo {
                        devid: 1_000_000,
                        total_bytes: 64 * GIB,
                        path: Some("/dev/vdc".into()),
                    })),
                    _ => Ok(None),
                }
            },
        )
        .unwrap();
        assert_eq!(found.devid, 500_000);
        // Exactly two ioctls (max_id then the live sysfs devid), zero unused ID probes.
        assert_eq!(probed, vec![1_000_000, 500_000]);

        // When sysfs lists all live devices and /dev/vdb is not among them,
        // returns NotFound without running any brute-force scan.
        let mut missing_probed = Vec::new();
        let err = find_target_device(
            "/dev/vdb",
            FsInfo {
                max_id: 1_000_000,
                num_devices: 2,
                fsid,
                sectorsize: 4096,
            },
            &mut |_| Some(vec![500_000, 1_000_000]),
            &mut |devid| {
                missing_probed.push(devid);
                match devid {
                    500_000 => Ok(Some(DevInfo {
                        devid: 500_000,
                        total_bytes: 64 * GIB,
                        path: Some("/dev/vdd".into()),
                    })),
                    1_000_000 => Ok(Some(DevInfo {
                        devid: 1_000_000,
                        total_bytes: 64 * GIB,
                        path: Some("/dev/vdc".into()),
                    })),
                    _ => Ok(None),
                }
            },
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(missing_probed, vec![1_000_000, 500_000]);
    }

    #[test]
    fn bounds_fallback_scan_when_sysfs_is_unavailable() {
        // Without sysfs, scanning a multi-device filesystem with a sparse high
        // max_id is strictly bounded by MAX_FALLBACK_SCAN_DEVID so guest boot
        // can never stall.
        let mut lookups = 0u64;
        let err = find_target_device(
            "/dev/vdb",
            FsInfo {
                max_id: 1_000_000,
                num_devices: 2,
                fsid: [0u8; 16],
                sectorsize: 4096,
            },
            &mut |_| None,
            &mut |devid| {
                lookups += 1;
                if devid == 1_000_000 {
                    Ok(Some(DevInfo {
                        devid: 1_000_000,
                        total_bytes: 64 * GIB,
                        path: Some("/dev/vdc".into()),
                    }))
                } else {
                    Ok(None)
                }
            },
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(lookups, 1 + MAX_FALLBACK_SCAN_DEVID);

        // And when all live devices lie within the bounded window, stops as
        // soon as `num_devices` live devices are seen.
        let mut early_stop = 0u64;
        let err = find_target_device(
            "/dev/vdb",
            FsInfo {
                max_id: 1_000_000,
                num_devices: 2,
                fsid: [0u8; 16],
                sectorsize: 4096,
            },
            &mut |_| None,
            &mut |devid| {
                early_stop += 1;
                match devid {
                    1 => Ok(Some(DevInfo {
                        devid: 1,
                        total_bytes: 64 * GIB,
                        path: Some("/dev/vdc".into()),
                    })),
                    1_000_000 => Ok(Some(DevInfo {
                        devid: 1_000_000,
                        total_bytes: 64 * GIB,
                        path: Some("/dev/vdd".into()),
                    })),
                    _ => Ok(None),
                }
            },
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(early_stop, 2);
    }
}
