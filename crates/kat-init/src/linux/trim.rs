//! Discard the data disk's free space (FITRIM), which libkrun turns into
//! `F_PUNCHHOLE` on the sparse image so APFS gets the space back.
//!
//! btrfs already discards freed extents itself (`discard=async`); this is the
//! safety net for anything that path missed, run some minutes after boot,
//! then daily, and when the host asks with `Msg::Trim`.

use std::ffi::CString;
use std::io;
use std::thread;
use std::time::{Duration, Instant};

use kat_proto::Msg;

use super::service::Notifier;

/// Any mount of the data disk; FITRIM on btrfs covers the whole file system.
const DATA_MOUNT: &str = "/var/lib/docker";
const FIRST: Duration = Duration::from_secs(5 * 60);
const EVERY: Duration = Duration::from_secs(24 * 60 * 60);

#[repr(C)]
struct FstrimRange {
    start: u64,
    len: u64,
    minlen: u64,
}

/// `_IOWR('X', 121, struct fstrim_range)`
const FITRIM: libc::c_ulong = 0xC018_5879;

/// Trim the data disk. Returns the bytes the file system trimmed.
pub fn trim() -> io::Result<u64> {
    let path = CString::new(DATA_MOUNT)?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut range = FstrimRange {
        start: 0,
        len: u64::MAX,
        minlen: 0,
    };
    let rc = unsafe { libc::ioctl(fd, FITRIM as _, &mut range) };
    let err = io::Error::last_os_error();
    unsafe { libc::close(fd) };
    if rc < 0 {
        return Err(err);
    }
    Ok(range.len)
}

/// Trim and report the result to the host.
pub fn trim_and_report(notifier: &Notifier) {
    let t = Instant::now();
    let msg = match trim() {
        Ok(bytes) => {
            super::log(&format!(
                "trimmed {} MiB in {} ms",
                bytes >> 20,
                t.elapsed().as_millis()
            ));
            Msg::TrimDone {
                bytes,
                error: String::new(),
            }
        }
        Err(e) => Msg::TrimDone {
            bytes: 0,
            error: e.to_string(),
        },
    };
    notifier.send(&msg);
}

/// Trim on a timer, for the life of the guest.
pub fn start_periodic() {
    let r = thread::Builder::new().name("trim".into()).spawn(|| {
        thread::sleep(FIRST);
        loop {
            if let Err(e) = trim() {
                super::log(&format!("periodic trim failed: {e}"));
            }
            thread::sleep(EVERY);
        }
    });
    if let Err(e) = r {
        super::log(&format!("periodic trim not started: {e}"));
    }
}
