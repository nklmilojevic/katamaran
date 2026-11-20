//! The calls a katamaran guest makes to have virtio-fs requests run on the
//! calling vCPU without raising an interrupt:
//!
//! - KAT_FS_DRAIN, after queueing requests: process the request queue.
//!   Returns [`DRAIN_DONE`] when done, [`DRAIN_BUSY`] when the queue is busy on
//!   another thread or unknown (the guest then notifies the queue as usual).
//! - KAT_FS_DIRECT: run one request passed in the call itself, without the
//!   queue (see [`direct`]).
//!
//! The calls are global but every virtio-fs device has its own queues, so the
//! guest names the queue by the guest address of its descriptor table, which
//! is unique to one device.

use std::sync::{Arc, RwLock};

use super::direct::{self, KAT_FS_DIRECT};
use super::worker::{DRAIN_BUSY, FsCore, note_sync};

const KAT_FS_DRAIN: u64 = 0xC600_4B01;

static CORES: RwLock<Vec<Arc<FsCore>>> = RwLock::new(Vec::new());

pub fn register(core: Arc<FsCore>) {
    CORES.write().unwrap().push(core);
    hvf::vendor_call::register(KAT_FS_DRAIN, |[queue, desc_table, _]| {
        drain(queue as usize, desc_table)
    });
    if direct_enabled() {
        hvf::vendor_call::register(KAT_FS_DIRECT, |[table, len, desc_table]| {
            run_direct(table, len, desc_table)
        });
    }
}

pub fn unregister(core: &Arc<FsCore>) {
    let mut cores = CORES.write().unwrap();
    cores.retain(|c| !Arc::ptr_eq(c, core));
    if cores.is_empty() {
        hvf::vendor_call::unregister(KAT_FS_DRAIN);
        hvf::vendor_call::unregister(KAT_FS_DIRECT);
    }
}

/// KATAMARAN_FS_DIRECT=0 turns the direct call off; the guest then keeps
/// queueing and draining.
pub fn direct_enabled() -> bool {
    std::env::var("KATAMARAN_FS_DIRECT").as_deref() != Ok("0")
}

/// KAT_FS_PROBE: the bare magic for a guest that asks for no ABI version,
/// otherwise the magic with the highest supported version in bits 32..48.
pub fn probe(magic: u64, abi: u64) -> u64 {
    if abi >= direct::ABI_DIRECT && direct_enabled() {
        magic | direct::ABI_DIRECT << 32
    } else {
        magic
    }
}

fn drain(queue: usize, desc_table: u64) -> u64 {
    let core = find(&CORES.read().unwrap(), queue, desc_table);
    match core {
        Some(core) => {
            note_sync(1);
            core.drain_sync(queue)
        }
        None => DRAIN_BUSY,
    }
}

fn run_direct(table: u64, len: u64, desc_table: u64) -> u64 {
    let found = find_request_queue(&CORES.read().unwrap(), desc_table);
    match found {
        Some((core, queue)) => core.run_direct(queue, table, len),
        None => direct::DECLINED,
    }
}

/// The device that owns the queue. A guest that passes no address (an older
/// kernel) is only served when there is a single device.
pub(super) fn find(cores: &[Arc<FsCore>], queue: usize, desc_table: u64) -> Option<Arc<FsCore>> {
    match cores {
        [only] if desc_table == 0 => Some(only.clone()),
        _ => cores
            .iter()
            .find(|c| c.owns_queue(queue, desc_table))
            .cloned(),
    }
}

/// The device and request queue whose descriptor table is at `desc_table`.
pub(super) fn find_request_queue(
    cores: &[Arc<FsCore>],
    desc_table: u64,
) -> Option<(Arc<FsCore>, usize)> {
    cores
        .iter()
        .find_map(|c| c.request_queue_at(desc_table).map(|q| (c.clone(), q)))
}
