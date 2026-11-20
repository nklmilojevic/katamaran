// Pinned directory descriptors. A directory that keeps being used as a parent holds an O_EVTONLY
// descriptor, so names in it resolve with *at() calls on that descriptor instead of a volfs path
// lookup of the directory each time. The descriptors come from a budget; the least recently used
// ones are closed by a background thread, and the node falls back to its /.vol path.

use std::collections::HashMap;
use std::env;
use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

/// Child lookups in a directory before it is worth an open of its own.
const PIN_AFTER: u32 = 4;
const TRIM_INTERVAL: Duration = Duration::from_secs(5);

pub(super) struct Pin {
    fd: OwnedFd,
    inode: u64,
    pool: Arc<PinPool>,
}

impl Pin {
    pub fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        self.pool.count.fetch_sub(1, Ordering::AcqRel);
        let mut slots = self.pool.slots.lock().unwrap();
        // The slot may already hold a newer pin of the same inode.
        if slots
            .get(&self.inode)
            .and_then(Weak::upgrade)
            .is_none_or(|slot| slot.pin.lock().unwrap().is_none())
        {
            slots.remove(&self.inode);
        }
    }
}

/// The pin state of one node.
#[derive(Default)]
pub(super) struct PinSlot {
    pin: Mutex<Option<Arc<Pin>>>,
    last_used: AtomicU64,
    uses: AtomicU32,
}

impl PinSlot {
    fn current(&self, pool: &PinPool) -> Option<Arc<Pin>> {
        let pin = self.pin.lock().unwrap().clone()?;
        self.last_used.store(pool.now(), Ordering::Relaxed);
        Some(pin)
    }

    #[cfg(test)]
    pub fn is_pinned(&self) -> bool {
        self.pin.lock().unwrap().is_some()
    }
}

pub(super) struct PinPool {
    budget: AtomicUsize,
    count: AtomicUsize,
    slots: Mutex<HashMap<u64, Weak<PinSlot>>>,
    epoch: Instant,
}

impl PinPool {
    /// Raises the soft RLIMIT_NOFILE to the hard limit and allows pins half of it.
    /// KATAMARAN_FS_PINS=0 turns pinning off.
    pub fn new() -> Arc<Self> {
        let budget = match env::var("KATAMARAN_FS_PINS").as_deref() {
            Ok("0") => 0,
            _ => raise_nofile_limit() / 2,
        };
        let pool = Arc::new(PinPool {
            budget: AtomicUsize::new(budget),
            count: AtomicUsize::new(0),
            slots: Mutex::new(HashMap::new()),
            epoch: Instant::now(),
        });
        let weak = Arc::downgrade(&pool);
        let spawned = thread::Builder::new()
            .name("fs node trim".into())
            .spawn(move || {
                loop {
                    thread::sleep(TRIM_INTERVAL);
                    let Some(pool) = weak.upgrade() else {
                        return;
                    };
                    pool.trim();
                }
            });
        if let Err(e) = spawned {
            warn!("virtio-fs: no node trim thread: {e}");
        }
        pool
    }

    fn now(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn set_budget(&self, budget: usize) {
        self.budget.store(budget, Ordering::Release);
    }

    /// The pin of a directory node, taking one once the directory has been used as a parent
    /// often enough. `path` opens the directory itself.
    pub fn parent_pin(
        self: &Arc<Self>,
        slot: &Arc<PinSlot>,
        inode: u64,
        dev: i32,
        ino: u64,
    ) -> Option<Arc<Pin>> {
        if let Some(pin) = slot.current(self) {
            return Some(pin);
        }
        if slot.uses.fetch_add(1, Ordering::Relaxed) + 1 < PIN_AFTER {
            return None;
        }
        let path = CString::new(format!("/.vol/{dev}/{ino}")).ok()?;
        self.pin(slot, inode, &path)
    }

    /// Pins the node now if the budget allows.
    pub fn pin(self: &Arc<Self>, slot: &Arc<PinSlot>, inode: u64, path: &CStr) -> Option<Arc<Pin>> {
        let mut current = slot.pin.lock().unwrap();
        if let Some(pin) = current.as_ref() {
            return Some(pin.clone());
        }
        if self.count.fetch_add(1, Ordering::AcqRel) >= self.budget.load(Ordering::Acquire) {
            self.count.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        let fd = unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_EVTONLY | libc::O_SYMLINK | libc::O_NONBLOCK | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            self.count.fetch_sub(1, Ordering::AcqRel);
            debug!(
                "virtio-fs: pin open failed, using path anchor: {}",
                io::Error::last_os_error()
            );
            return None;
        }
        let pin = Arc::new(Pin {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
            inode,
            pool: self.clone(),
        });
        *current = Some(pin.clone());
        drop(current);
        slot.last_used.store(self.now(), Ordering::Relaxed);
        self.slots
            .lock()
            .unwrap()
            .insert(inode, Arc::downgrade(slot));
        Some(pin)
    }

    pub fn current(&self, slot: &PinSlot) -> Option<Arc<Pin>> {
        slot.current(self)
    }

    /// Unpins least recently used nodes until at most `keep` pins remain (descriptors still in
    /// use by a request close when it finishes).
    pub fn demote(&self, keep: usize) {
        let mut pinned: Vec<(u64, Arc<PinSlot>)> = self
            .slots
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
            .map(|slot| (slot.last_used.load(Ordering::Relaxed), slot))
            .collect();
        let excess = pinned.len().saturating_sub(keep);
        if excess == 0 {
            return;
        }
        pinned.sort_unstable_by_key(|(used, _)| *used);
        for (_, slot) in pinned.into_iter().take(excess) {
            let pin = slot.pin.lock().unwrap().take();
            slot.uses.store(0, Ordering::Relaxed);
            drop(pin);
        }
        debug!("virtio-fs: evicted pinned fds to path anchors, {excess} closed");
    }

    fn trim(&self) {
        let budget = self.budget.load(Ordering::Acquire);
        if self.count() > budget * 9 / 10 {
            self.demote(budget * 7 / 10);
        }
    }

    /// The process ran out of descriptors: give back half of the pins.
    pub fn relieve(&self) {
        self.demote(self.count() / 2);
    }
}

fn raise_nofile_limit() -> usize {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return 256;
    }
    let mut per_proc: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    let name = c"kern.maxfilesperproc";
    let have_max = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            (&mut per_proc as *mut libc::c_int).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    } == 0;
    let mut want = lim.rlim_max;
    if have_max && per_proc > 0 {
        want = want.min(per_proc as libc::rlim_t);
    }
    if want > lim.rlim_cur {
        let raised = libc::rlimit {
            rlim_cur: want,
            rlim_max: lim.rlim_max,
        };
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
            lim.rlim_cur = want;
        }
    }
    lim.rlim_cur.min(1 << 20) as usize
}
