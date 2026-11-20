//! Cache invalidations for the shares, from the VM process (which watches
//! the Mac's files and knows what the guest caches) to the guest kernel. The
//! shares are registered with `/dev/kat_fsnotify` by index first, in the
//! order the host lists them; each frame from the host then goes to the
//! device as one write.

use std::fs::File;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::thread;

use kat_proto::FSCACHE_PORT;
use kat_proto::fscache::{read_frame, share_record};

use super::{dial_host, log};

pub fn start(dev: Arc<File>, shares: Vec<String>) {
    let spawned = thread::Builder::new()
        .name("fscache".into())
        .spawn(move || {
            if let Err(e) = relay(&dev, &shares) {
                log(&format!("no host cache invalidation: {e}"));
            }
        });
    if let Err(e) = spawned {
        log(&format!("no host cache invalidation: {e}"));
    }
}

fn relay(dev: &File, shares: &[String]) -> io::Result<()> {
    for (i, path) in shares.iter().enumerate() {
        let root = File::open(path)?;
        (&*dev).write_all(&share_record(i as u32, root.as_raw_fd() as u64))?;
    }
    let mut conn = dial_host(FSCACHE_PORT)?;
    let mut buf = Vec::new();
    while read_frame(&mut conn, &mut buf)?.is_some() {
        if let Err(e) = (&*dev).write_all(&buf) {
            log(&format!("cache invalidation: {e}"));
        }
    }
    Ok(())
}
