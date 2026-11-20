#[cfg(target_os = "macos")]
use crossbeam_channel::Sender;
#[cfg(target_os = "macos")]
use utils::worker_message::WorkerMessage;

use std::io;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};
#[cfg(windows)]
use utils::windows::AsRawFd;

use crossbeam_channel::{Receiver, unbounded};
use utils::epoll::{ControlOperation, Epoll, EpollEvent, EventSet};
use utils::eventfd::EventFd;
use vm_memory::{Address, ByteValued, GuestAddress, GuestMemoryMmap};

use super::super::queue::read_small;
use super::super::{DescriptorChain, FsError, Queue};
use super::augment_fs::AugmentFs;
use super::defs::{HPQ_INDEX, REQ_INDEX};
use super::descriptor_utils::{Reader, Writer};
use super::direct;
use super::fuse::{InHeader, Opcode, ReadIn, WriteIn};
use super::inode_alloc::InodeAllocator;
#[cfg(target_os = "macos")]
use super::macos::syncopen;
use super::null_fs::NullFs;
use super::passthrough::{self, PassthroughFs};
use super::read_only::PassthroughFsRo;
use super::rosetta::Rosetta;
use super::server::Server;
use super::sync_policy::{Peek, Route, SyncPolicy};
use super::virtual_entry::VirtualDirEntry;
use crate::virtio::{InterruptTransport, VirtioShmRegion};

enum FsServer {
    ReadWrite(Server<AugmentFs<PassthroughFs>>),
    ReadOnly(Server<AugmentFs<PassthroughFsRo>>),
    Null(Server<AugmentFs<NullFs>>),
}

impl FsServer {
    fn handle_message(
        &self,
        r: Reader,
        w: Writer,
        allow_idmap: bool,
        shm_region: &Option<VirtioShmRegion>,
        exit_code: &Arc<AtomicI32>,
        #[cfg(target_os = "macos")] map_sender: &Option<Sender<WorkerMessage>>,
    ) -> super::Result<usize> {
        match self {
            FsServer::ReadWrite(s) => s.handle_message(
                r,
                w,
                allow_idmap,
                shm_region,
                exit_code,
                #[cfg(target_os = "macos")]
                map_sender,
            ),
            FsServer::ReadOnly(s) => s.handle_message(
                r,
                w,
                allow_idmap,
                shm_region,
                exit_code,
                #[cfg(target_os = "macos")]
                map_sender,
            ),
            FsServer::Null(s) => s.handle_message(
                r,
                w,
                allow_idmap,
                shm_region,
                exit_code,
                #[cfg(target_os = "macos")]
                map_sender,
            ),
        }
    }
}

/// Returned to the guest's KAT_FS_DRAIN when every request was answered.
pub const DRAIN_DONE: u64 = 0;
/// Returned when a request must run on the worker pool: the guest then
/// notifies the queue and waits for the interrupt.
pub const DRAIN_BUSY: u64 = 1;

/// Worker pool threads for requests that may block (KATAMARAN_FS_WORKERS).
const DEFAULT_POOL: usize = 4;

/// A request popped from a queue and handed to the worker pool.
struct Job {
    queue_index: usize,
    head: u16,
    desc_table: GuestAddress,
    queue_size: u16,
}

/// Request processing state, shared by the worker thread, the worker pool
/// and vCPU threads that process requests synchronously. The queues are
/// locked only to pop and complete requests and to run requests that cannot
/// block for long.
pub struct FsCore {
    /// One lock per queue, so vCPUs that submit on different request queues
    /// do not wait for each other.
    queues: Vec<Mutex<Queue>>,
    /// Guest address of each queue's descriptor table, which names the queue
    /// across devices in the guest's synchronous submission call.
    desc_tables: Vec<u64>,
    interrupt: InterruptTransport,
    mem: GuestMemoryMmap,
    allow_idmap: bool,
    shm_region: Option<VirtioShmRegion>,
    server: FsServer,
    policy: Arc<SyncPolicy>,
    pool: Mutex<Option<crossbeam_channel::Sender<Job>>>,
    exit_code: Arc<AtomicI32>,
    #[cfg(target_os = "macos")]
    map_sender: Option<Sender<WorkerMessage>>,
}

pub struct FsWorker {
    core: Arc<FsCore>,
    queue_evts: Vec<Arc<EventFd>>,
    stop_fd: EventFd,
}

/// Run the calling thread at QOS_CLASS_USER_INTERACTIVE, as the
/// virtio-fs workers do: guest file requests are on someone's critical path.
pub fn set_interactive_qos() {
    #[cfg(target_os = "macos")]
    {
        unsafe extern "C" {
            fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
        }
        const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
        unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
    }
}

#[cfg(target_os = "macos")]
fn new_policy(cfg: Option<&passthrough::Config>) -> Arc<SyncPolicy> {
    use super::macos::boundaries::MountBoundaries;
    use std::os::unix::fs::MetadataExt;

    let Some(cfg) = cfg else {
        return Arc::new(SyncPolicy::default());
    };
    let policy = SyncPolicy::new(MountBoundaries::shared());
    if let Ok(md) = std::fs::symlink_metadata(&cfg.root_dir) {
        policy.note_root(super::fuse::ROOT_ID, md.dev() as u32 as u64, md.ino());
    }
    Arc::new(policy)
}

#[cfg(not(target_os = "macos"))]
fn new_policy(_cfg: Option<&passthrough::Config>) -> Arc<SyncPolicy> {
    Arc::new(SyncPolicy::default())
}

impl FsWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        queues: Vec<Queue>,
        queue_evts: Vec<Arc<EventFd>>,
        interrupt: InterruptTransport,
        mem: GuestMemoryMmap,
        allow_idmap: bool,
        shm_region: Option<VirtioShmRegion>,
        passthrough_cfg: Option<passthrough::Config>,
        read_only: bool,
        virtual_entries: Vec<VirtualDirEntry<'static>>,
        rosetta: Option<Arc<Rosetta>>,
        stop_fd: EventFd,
        exit_code: Arc<AtomicI32>,
        #[cfg(target_os = "macos")] map_sender: Option<Sender<WorkerMessage>>,
    ) -> Result<Self, io::Error> {
        let inode_alloc = Arc::new(InodeAllocator::new());
        let policy = new_policy(passthrough_cfg.as_ref());
        let server = match passthrough_cfg {
            Some(cfg) if read_only => {
                let inner = PassthroughFsRo::new(cfg, inode_alloc.clone())?;
                FsServer::ReadOnly(Server::with_policy(
                    AugmentFs::new(inner, &inode_alloc, virtual_entries).with_rosetta(rosetta),
                    policy.clone(),
                ))
            }
            Some(cfg) => {
                let inner = PassthroughFs::new(cfg, inode_alloc.clone())?;
                FsServer::ReadWrite(Server::with_policy(
                    AugmentFs::new(inner, &inode_alloc, virtual_entries).with_rosetta(rosetta),
                    policy.clone(),
                ))
            }
            None => FsServer::Null(Server::with_policy(
                AugmentFs::new(NullFs, &inode_alloc, virtual_entries),
                policy.clone(),
            )),
        };
        let core = Arc::new(FsCore {
            desc_tables: queues.iter().map(|q| q.desc_table.0).collect(),
            queues: queues.into_iter().map(Mutex::new).collect(),
            interrupt,
            mem,
            allow_idmap,
            shm_region,
            server,
            policy,
            pool: Mutex::new(None),
            exit_code,
            #[cfg(target_os = "macos")]
            map_sender,
        });
        core.start_pool(pool_size());
        Ok(Self {
            core,
            queue_evts,
            stop_fd,
        })
    }

    pub fn core(&self) -> Arc<FsCore> {
        self.core.clone()
    }

    pub fn run(self) -> thread::JoinHandle<()> {
        thread::Builder::new()
            .name("fs worker".into())
            .spawn(|| self.work())
            .unwrap()
    }

    fn work(self) {
        set_interactive_qos();
        let stop_ev_fd = self.stop_fd.as_raw_fd();

        let mut epoll = Epoll::new().unwrap();

        for evt in &self.queue_evts {
            let fd = evt.as_raw_fd();
            let _ = epoll.ctl(
                ControlOperation::Add,
                fd,
                &EpollEvent::new(EventSet::IN, fd as u64),
            );
        }
        let _ = epoll.ctl(
            ControlOperation::Add,
            stop_ev_fd,
            &EpollEvent::new(EventSet::IN, stop_ev_fd as u64),
        );

        let mut epoll_events = vec![EpollEvent::new(EventSet::empty(), 0); 32];
        loop {
            match epoll.wait(epoll_events.len(), -1, epoll_events.as_mut_slice()) {
                Ok(ev_cnt) => {
                    for event in &epoll_events[0..ev_cnt] {
                        let source = event.fd();
                        let event_set = event.event_set();
                        let queue = self
                            .queue_evts
                            .iter()
                            .position(|evt| evt.as_raw_fd() == source);
                        match event_set {
                            EventSet::IN if queue.is_some() => {
                                self.handle_event(queue.unwrap());
                            }
                            EventSet::IN if source == stop_ev_fd => {
                                debug!("stopping worker thread");
                                let _ = self.stop_fd.read();
                                self.core.stop_pool();
                                return;
                            }
                            _ => {
                                log::warn!(
                                    "Received unknown event: {event_set:?} from fd: {source:?}"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("failed to consume muxer epoll event: {e}");
                }
            }
        }
    }

    fn handle_event(&self, queue_index: usize) {
        debug!("Fs: queue event: {queue_index}");
        if let Err(e) = self.queue_evts[queue_index].read() {
            error!("Failed to get queue event: {e:?}");
        }
        self.core.drain(queue_index);
    }
}

fn pool_size() -> usize {
    std::env::var("KATAMARAN_FS_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_POOL)
}

static STAT_REQS: AtomicU64 = AtomicU64::new(0);
static STAT_NS: AtomicU64 = AtomicU64::new(0);
static STAT_INLINE: AtomicU64 = AtomicU64::new(0);
static STAT_SYNC: AtomicU64 = AtomicU64::new(0);
static STAT_ASYNC: AtomicU64 = AtomicU64::new(0);
static STAT_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
static STAT_DIRECT: AtomicU64 = AtomicU64::new(0);
static STAT_DIRECT_DECLINED: AtomicU64 = AtomicU64::new(0);
static STAT_DIRECT_INVALID: AtomicU64 = AtomicU64::new(0);
static STATS_ON: AtomicBool = AtomicBool::new(false);
/// Requests and host nanoseconds per FUSE opcode (`OP_SLOTS - 1` collects the rest).
const OP_SLOTS: usize = 64;
static OP_REQS: [AtomicU64; OP_SLOTS] = [const { AtomicU64::new(0) }; OP_SLOTS];
static OP_NS: [AtomicU64; OP_SLOTS] = [const { AtomicU64::new(0) }; OP_SLOTS];

fn note_op(opcode: u32, ns: u64) {
    let slot = (opcode as usize).min(OP_SLOTS - 1);
    OP_REQS[slot].fetch_add(1, Ordering::Relaxed);
    OP_NS[slot].fetch_add(ns, Ordering::Relaxed);
}

/// `"getattr 120 (3.1 us)"` for the busiest opcodes since `last`, which is updated.
fn op_summary(last: &mut [(u64, u64); OP_SLOTS]) -> String {
    let mut rows: Vec<(usize, u64, u64)> = (0..OP_SLOTS)
        .map(|i| {
            let (n, ns) = (
                OP_REQS[i].load(Ordering::Relaxed),
                OP_NS[i].load(Ordering::Relaxed),
            );
            let row = (i, n - last[i].0, ns - last[i].1);
            last[i] = (n, ns);
            row
        })
        .filter(|&(_, n, _)| n > 0)
        .collect();
    rows.sort_by_key(|&(_, n, _)| std::cmp::Reverse(n));
    rows.iter()
        .take(12)
        .map(|&(op, n, ns)| format!("{op}:{n} ({:.1} us)", ns as f64 / n as f64 / 1000.0))
        .collect::<Vec<_>>()
        .join(", ")
}

/// KATAMARAN_FS_STATS=1: every 2 s, log how many requests the server handled
/// and the mean host time per request.
pub fn start_stats_reporter() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    if std::env::var("KATAMARAN_FS_STATS").as_deref() != Ok("1") {
        return;
    }
    STARTED.call_once(|| {
        STATS_ON.store(true, Ordering::Relaxed);
        thread::spawn(|| {
            let mut last = [0u64; 9];
            let mut last_ops = [(0u64, 0u64); OP_SLOTS];
            loop {
                thread::sleep(Duration::from_secs(2));
                let now = [
                    STAT_REQS.load(Ordering::Relaxed),
                    STAT_NS.load(Ordering::Relaxed),
                    STAT_INLINE.load(Ordering::Relaxed),
                    STAT_SYNC.load(Ordering::Relaxed),
                    STAT_ASYNC.load(Ordering::Relaxed),
                    STAT_TIMEOUTS.load(Ordering::Relaxed),
                    STAT_DIRECT.load(Ordering::Relaxed),
                    STAT_DIRECT_DECLINED.load(Ordering::Relaxed),
                    STAT_DIRECT_INVALID.load(Ordering::Relaxed),
                ];
                let d: Vec<u64> = now.iter().zip(last).map(|(n, l)| n - l).collect();
                if d[0] > 0 {
                    log::warn!(
                        "fs stats: {} req/s, host {:.1} us/req, drains: {} notify-inline, {} sync; {} to pool, {} open timeouts; direct: {} done, {} declined, {} invalid",
                        d[0] / 2,
                        d[1] as f64 / d[0] as f64 / 1000.0,
                        d[2],
                        d[3],
                        d[4],
                        d[5],
                        d[6],
                        d[7],
                        d[8]
                    );
                    log::warn!("fs ops (opcode:count): {}", op_summary(&mut last_ops));
                }
                last = now;
            }
        });
    });
}

pub fn note_sync(n: u64) {
    STAT_SYNC.fetch_add(n, Ordering::Relaxed);
}

pub fn note_inline(n: u64) {
    STAT_INLINE.fetch_add(n, Ordering::Relaxed);
}

/// The parts of a request that decide where it runs.
struct Peeked {
    opcode: u32,
    nodeid: u64,
    io_size: u32,
    name: Option<Vec<u8>>,
}

impl FsCore {
    fn start_pool(self: &Arc<Self>, n: usize) {
        let (tx, rx) = unbounded::<Job>();
        for i in 0..n {
            let rx: Receiver<Job> = rx.clone();
            let core: Weak<FsCore> = Arc::downgrade(self);
            let _ = thread::Builder::new()
                .name(format!("fs pool {i}"))
                .spawn(move || {
                    set_interactive_qos();
                    while let Ok(job) = rx.recv() {
                        let Some(core) = core.upgrade() else { break };
                        core.run_job(job);
                    }
                });
        }
        *self.pool.lock().unwrap() = Some(tx);
    }

    fn stop_pool(&self) {
        self.pool.lock().unwrap().take();
    }

    /// Worker thread: process everything, blocking requests on the pool.
    pub fn drain(&self, queue_index: usize) {
        let mut queue = self.queues[queue_index].lock().unwrap();
        self.drain_locked(&mut queue, queue_index, false);
    }

    /// vCPU thread after an MMIO queue notify. Returns false when another
    /// thread holds the queues; the caller then wakes the worker thread.
    pub fn try_drain_notified(&self, queue_index: usize) -> bool {
        let Some(Ok(mut queue)) = self.queues.get(queue_index).map(Mutex::try_lock) else {
            return false;
        };
        self.drain_locked(&mut queue, queue_index, false);
        true
    }

    /// vCPU thread on KATAMARAN's KAT_FS_DRAIN: process without raising an
    /// interrupt, the guest collects the replies itself on return. Stops at
    /// the first request that may block and returns [`DRAIN_BUSY`].
    /// Whether queue `queue_index` of this device has its descriptor table at
    /// `desc_table` in guest memory.
    pub fn owns_queue(&self, queue_index: usize, desc_table: u64) -> bool {
        self.desc_tables.get(queue_index) == Some(&desc_table)
    }

    pub fn drain_sync(&self, queue_index: usize) -> u64 {
        // A queue the guest does not use for requests: let it notify instead.
        if queue_index < REQ_INDEX {
            return DRAIN_BUSY;
        }
        let Some(Ok(mut queue)) = self.queues.get(queue_index).map(Mutex::try_lock) else {
            return DRAIN_BUSY;
        };
        self.drain_locked(&mut queue, queue_index, true)
    }

    /// The request queue whose descriptor table is at `desc_table`.
    pub fn request_queue_at(&self, desc_table: u64) -> Option<usize> {
        self.desc_tables
            .iter()
            .position(|&d| d == desc_table)
            .filter(|&q| q >= REQ_INDEX)
    }

    /// vCPU thread on KAT_FS_DIRECT: run the request whose table the guest
    /// passed (`len` bytes at `table`), as if it came on request queue
    /// `queue_index`, and return the reply length or a [`direct`] status. No
    /// queue is touched, so no queue lock is taken.
    pub fn run_direct(&self, queue_index: usize, table: u64, len: u64) -> u64 {
        let checked =
            direct::parse(&self.mem, table, len).and_then(|segs| direct::buffers(&self.mem, &segs));
        let (reader, writer) = match checked {
            Ok(rw) => rw,
            Err(e) => {
                if STAT_DIRECT_INVALID.fetch_add(1, Ordering::Relaxed) == 0 {
                    log::warn!("fs: invalid direct request from the guest: {e:?}");
                }
                return direct::INVALID;
            }
        };
        let peeked = self.peek_reader(reader.clone(), queue_index);
        let Some(p) = &peeked else {
            STAT_DIRECT_INVALID.fetch_add(1, Ordering::Relaxed);
            return direct::INVALID;
        };
        let (opcode, nodeid) = (p.opcode, p.nodeid);
        let route = self.route(&peeked, queue_index);
        if route == Route::Async {
            STAT_DIRECT_DECLINED.fetch_add(1, Ordering::Relaxed);
            return direct::DECLINED;
        }

        #[cfg(target_os = "macos")]
        let scope = (route == Route::BoundedOpen).then(syncopen::bounded_scope);
        let len = self.serve(reader, writer, Some(opcode));
        #[cfg(target_os = "macos")]
        if let Some(scope) = scope {
            drop(scope);
            if syncopen::take_timed_out() {
                // As on the queue path: the guest ignores what was written and
                // sends the request again, and it now goes to the pool.
                STAT_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                self.policy.mark_async(nodeid);
                STAT_DIRECT_DECLINED.fetch_add(1, Ordering::Relaxed);
                return direct::DECLINED;
            }
        }
        #[cfg(not(target_os = "macos"))]
        let _ = nodeid;
        match len {
            Some(len) if len > 0 => {
                STAT_DIRECT.fetch_add(1, Ordering::Relaxed);
                len as u64
            }
            _ => {
                STAT_DIRECT_INVALID.fetch_add(1, Ordering::Relaxed);
                direct::INVALID
            }
        }
    }

    fn drain_locked(&self, queue: &mut Queue, queue_index: usize, sync: bool) -> u64 {
        loop {
            queue.disable_notification(&self.mem).unwrap();
            let busy = self.process_queue(queue, queue_index, sync);
            let more = queue.enable_notification(&self.mem).unwrap();
            if busy {
                // The guest waits for an interrupt now, also for requests this
                // drain already answered without one.
                if queue.needs_notification(&self.mem).unwrap() {
                    self.interrupt.signal_used_queue();
                }
                return DRAIN_BUSY;
            }
            if !more {
                return DRAIN_DONE;
            }
        }
    }

    fn peek(&self, head: &DescriptorChain, queue_index: usize) -> Option<Peeked> {
        if let Some(p) = self.peek_head(head, queue_index) {
            return Some(p);
        }
        let r = Reader::new(&self.mem, head.clone()).ok()?;
        self.peek_reader(r, queue_index)
    }

    /// The parts of a request that decide where it runs, read from its input.
    fn peek_reader(&self, mut r: Reader, queue_index: usize) -> Option<Peeked> {
        let h: InHeader = r.read_obj().ok()?;
        let mut p = Peeked {
            opcode: h.opcode,
            nodeid: h.nodeid,
            io_size: 0,
            name: None,
        };
        if queue_index == HPQ_INDEX {
            return Some(p);
        }
        if h.opcode == Opcode::Read as u32 {
            p.io_size = r.read_obj::<ReadIn>().ok()?.size;
        } else if h.opcode == Opcode::Write as u32 {
            p.io_size = r.read_obj::<WriteIn>().ok()?.size;
        } else if h.opcode == Opcode::Lookup as u32 && self.policy.wants_name(h.nodeid) {
            let len = (h.len as usize).checked_sub(size_of::<InHeader>())?;
            let mut name = vec![0u8; len];
            io::Read::read_exact(&mut r, &mut name).ok()?;
            if let Some(nul) = name.iter().position(|&b| b == 0) {
                name.truncate(nul);
            }
            p.name = Some(name);
        }
        Some(p)
    }

    /// The header and READ/WRITE size straight from the first descriptor, where the guest puts
    /// them, without walking the chain. None when they are not all there or a name is needed.
    fn peek_head(&self, head: &DescriptorChain, queue_index: usize) -> Option<Peeked> {
        const HDR: usize = size_of::<InHeader>();
        if !head.is_read_only() || (head.len as usize) < HDR {
            return None;
        }
        let h: InHeader = read_small(&self.mem, head.addr)?;
        let mut p = Peeked {
            opcode: h.opcode,
            nodeid: h.nodeid,
            io_size: 0,
            name: None,
        };
        if queue_index == HPQ_INDEX {
            return Some(p);
        }
        if h.opcode == Opcode::Read as u32 {
            p.io_size = self.peek_args::<ReadIn>(head)?.size;
        } else if h.opcode == Opcode::Write as u32 {
            p.io_size = self.peek_args::<WriteIn>(head)?.size;
        } else if h.opcode == Opcode::Lookup as u32 && self.policy.wants_name(h.nodeid) {
            return None;
        }
        Some(p)
    }

    /// The arguments after the header: in the same descriptor, or (as Linux sends them) at the
    /// start of the next one.
    fn peek_args<T: ByteValued>(&self, head: &DescriptorChain) -> Option<T> {
        const HDR: usize = size_of::<InHeader>();
        let need = size_of::<T>();
        if head.len as usize >= HDR + need {
            return read_small(&self.mem, head.addr.checked_add(HDR as u64)?);
        }
        if head.len as usize != HDR {
            return None;
        }
        let next = head.next_descriptor()?;
        if !next.is_read_only() || (next.len as usize) < need {
            return None;
        }
        read_small(&self.mem, next.addr)
    }

    fn route(&self, peeked: &Option<Peeked>, queue_index: usize) -> Route {
        let Some(p) = peeked else {
            // Malformed: the server answers with an error right away.
            return Route::Sync;
        };
        if queue_index == HPQ_INDEX {
            return Route::Sync;
        }
        self.policy.route(&Peek {
            opcode: p.opcode,
            nodeid: p.nodeid,
            io_size: p.io_size,
            name: p.name.as_deref(),
        })
    }

    fn handle(&self, head: DescriptorChain) -> usize {
        let opcode = if STATS_ON.load(Ordering::Relaxed) {
            Reader::new(&self.mem, head.clone())
                .ok()
                .and_then(|mut r| r.read_obj::<InHeader>().ok())
                .map(|h| h.opcode)
        } else {
            None
        };
        let reader = Reader::new(&self.mem, head.clone())
            .map_err(FsError::QueueReader)
            .unwrap();
        let writer = Writer::new(&self.mem, head)
            .map_err(FsError::QueueWriter)
            .unwrap();
        self.serve(reader, writer, opcode).unwrap_or(0)
    }

    /// Run one request through the FUSE server; the reply length, or None when the server could
    /// not decode it. `opcode` is for the per-opcode stats.
    fn serve(&self, reader: Reader, writer: Writer, opcode: Option<u32>) -> Option<usize> {
        let started = STATS_ON.load(Ordering::Relaxed).then(Instant::now);
        let len = match self.server.handle_message(
            reader,
            writer,
            self.allow_idmap,
            &self.shm_region,
            &self.exit_code,
            #[cfg(target_os = "macos")]
            &self.map_sender,
        ) {
            Ok(len) => Some(len),
            Err(e) => {
                error!("error handling message: {e:?}");
                None
            }
        };
        if let Some(started) = started {
            let ns = started.elapsed().as_nanos() as u64;
            STAT_REQS.fetch_add(1, Ordering::Relaxed);
            STAT_NS.fetch_add(ns, Ordering::Relaxed);
            if let Some(op) = opcode {
                note_op(op, ns);
            }
        }
        len
    }

    /// Hand a popped request to the worker pool. Without a pool (stopping)
    /// the request is processed here.
    fn dispatch(&self, queue: &mut Queue, queue_index: usize, head: DescriptorChain) {
        STAT_ASYNC.fetch_add(1, Ordering::Relaxed);
        let job = Job {
            queue_index,
            head: head.index,
            desc_table: queue.desc_table,
            queue_size: queue.actual_size(),
        };
        let sent = match &*self.pool.lock().unwrap() {
            Some(tx) => tx.send(job).is_ok(),
            None => false,
        };
        if !sent {
            let index = head.index;
            let len = self.handle(head);
            self.add_used(queue, index, len, true);
        }
    }

    fn add_used(&self, queue: &mut Queue, index: u16, len: usize, signal: bool) {
        if let Err(e) = queue.add_used(&self.mem, index, len as u32) {
            error!("failed to add used elements to the queue: {e:?}");
        }
        if signal && queue.needs_notification(&self.mem).unwrap() {
            self.interrupt.signal_used_queue();
        }
    }

    fn run_job(&self, job: Job) {
        let Some(head) =
            DescriptorChain::checked_new(&self.mem, job.desc_table, job.queue_size, job.head)
        else {
            error!("fs pool: invalid descriptor {}", job.head);
            return;
        };
        let len = self.handle(head);
        let mut queue = self.queues[job.queue_index].lock().unwrap();
        self.add_used(&mut queue, job.head, len, true);
    }

    /// Returns true when the sync drain must stop and report busy.
    fn process_queue(&self, queue: &mut Queue, queue_index: usize, sync: bool) -> bool {
        while let Some(head) = queue.pop(&self.mem) {
            let peeked = self.peek(&head, queue_index);
            let route = self.route(&peeked, queue_index);
            if route == Route::Async {
                if sync {
                    queue.undo_pop();
                    return true;
                }
                self.dispatch(queue, queue_index, head);
                continue;
            }

            #[cfg(target_os = "macos")]
            let scope = (route == Route::BoundedOpen).then(syncopen::bounded_scope);
            let index = head.index;
            let len = self.handle(head.clone());
            #[cfg(target_os = "macos")]
            if let Some(scope) = scope {
                drop(scope);
                if syncopen::take_timed_out() {
                    // The reply written so far is discarded: the guest never
                    // sees a descriptor until it is in the used ring, and the
                    // request's input is read again from guest memory.
                    STAT_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                    if let Some(p) = &peeked {
                        self.policy.mark_async(p.nodeid);
                    }
                    self.dispatch(queue, queue_index, head);
                    if sync {
                        return true;
                    }
                    continue;
                }
            }
            self.add_used(queue, index, len, !sync);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::DummyIrqChip;
    use crate::virtio::descriptor_utils::{DescriptorType, create_descriptor_chain};
    use crate::virtio::queue::tests::VirtQueue;
    use std::mem::size_of;
    use utils::eventfd::EFD_NONBLOCK;
    use vm_memory::Bytes;

    use super::super::fuse::GetattrIn;

    /// A request queue at `base` holding one GETATTR of the root.
    fn queue_with_getattr(mem: &GuestMemoryMmap, base: u64) -> VirtQueue<'_> {
        let vq = VirtQueue::new(GuestAddress(base), mem, 16);
        let req = GuestAddress(base + 0x8000);
        let len = (size_of::<InHeader>() + size_of::<GetattrIn>()) as u32;
        create_descriptor_chain(
            mem,
            GuestAddress(base),
            req,
            vec![
                (DescriptorType::Readable, len),
                (DescriptorType::Writable, 0x200),
            ],
            0,
        )
        .unwrap();
        let header = InHeader {
            len,
            opcode: Opcode::Getattr as u32,
            unique: 1,
            nodeid: super::super::fuse::ROOT_ID,
            ..Default::default()
        };
        mem.write_obj(header, req).unwrap();
        vq.avail.ring[0].set(0);
        vq.avail.idx.set(1);
        vq
    }

    #[test]
    fn request_queues_are_drained_independently() {
        let mem = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x40000)]).unwrap();
        let hpq = VirtQueue::new(GuestAddress(0), &mem, 16);
        let q1 = queue_with_getattr(&mem, 0x10000);
        let q2 = queue_with_getattr(&mem, 0x20000);
        let worker = FsWorker::new(
            vec![hpq.create_queue(), q1.create_queue(), q2.create_queue()],
            (0..3)
                .map(|_| Arc::new(EventFd::new(EFD_NONBLOCK).unwrap()))
                .collect(),
            InterruptTransport::new(DummyIrqChip::new().into(), "fs test".into()).unwrap(),
            mem.clone(),
            false,
            None,
            Some(passthrough::Config {
                root_dir: "/".into(),
                ..Default::default()
            }),
            false,
            Vec::new(),
            None,
            EventFd::new(EFD_NONBLOCK).unwrap(),
            Arc::new(AtomicI32::new(0)),
            #[cfg(target_os = "macos")]
            None,
        )
        .unwrap();
        let core = worker.core();

        // Another vCPU is processing the first request queue.
        let busy = core.queues[REQ_INDEX].lock().unwrap();
        assert_eq!(core.drain_sync(REQ_INDEX), DRAIN_BUSY);
        assert_eq!(core.drain_sync(REQ_INDEX + 1), DRAIN_DONE);
        assert_eq!(q2.used.idx.get(), 1);
        assert_eq!(q1.used.idx.get(), 0);
        drop(busy);
        assert_eq!(core.drain_sync(REQ_INDEX), DRAIN_DONE);
        assert_eq!(q1.used.idx.get(), 1);

        // Not request queues: the guest notifies instead.
        assert_eq!(core.drain_sync(HPQ_INDEX), DRAIN_BUSY);
        assert_eq!(core.drain_sync(REQ_INDEX + 2), DRAIN_BUSY);
        assert!(!core.try_drain_notified(REQ_INDEX + 2));
        core.stop_pool();
    }

    fn core_with_queues(mem: &GuestMemoryMmap, bases: &[u64]) -> Arc<FsCore> {
        let queues: Vec<Queue> = bases
            .iter()
            .map(|&b| VirtQueue::new(GuestAddress(b), mem, 16).create_queue())
            .collect();
        FsWorker::new(
            queues,
            bases
                .iter()
                .map(|_| Arc::new(EventFd::new(EFD_NONBLOCK).unwrap()))
                .collect(),
            InterruptTransport::new(DummyIrqChip::new().into(), "fs test".into()).unwrap(),
            mem.clone(),
            false,
            None,
            Some(passthrough::Config {
                root_dir: "/".into(),
                ..Default::default()
            }),
            false,
            Vec::new(),
            None,
            EventFd::new(EFD_NONBLOCK).unwrap(),
            Arc::new(AtomicI32::new(0)),
            #[cfg(target_os = "macos")]
            None,
        )
        .unwrap()
        .core()
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn sync_drain_finds_the_device_that_owns_the_queue() {
        use super::super::sync_calls::find;

        let mem = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x40000)]).unwrap();
        let users = core_with_queues(&mem, &[0x1000, 0x2000]);
        let rosetta = core_with_queues(&mem, &[0x3000, 0x4000]);
        let desc = |base: u64| {
            VirtQueue::new(GuestAddress(base), &mem, 16)
                .create_queue()
                .desc_table
                .0
        };
        let cores = [users.clone(), rosetta.clone()];

        let hit = |q, d| find(&cores, q, d).map(|c| Arc::as_ptr(&c));
        assert_eq!(hit(1, desc(0x2000)), Some(Arc::as_ptr(&users)));
        assert_eq!(hit(1, desc(0x4000)), Some(Arc::as_ptr(&rosetta)));
        assert_eq!(hit(0, desc(0x4000)), None);
        assert_eq!(hit(1, 0), None);
        // An older guest passes no address; that only works with one device.
        assert_eq!(
            find(&cores[..1], 1, 0).map(|c| Arc::as_ptr(&c)),
            Some(Arc::as_ptr(&users))
        );
        users.stop_pool();
        rosetta.stop_pool();
    }

    /// A KAT_FS_DIRECT table at `at` for a GETATTR of the root whose reply goes to `reply`.
    fn direct_getattr(mem: &GuestMemoryMmap, at: u64, reply: u64, reply_len: u32) -> u64 {
        let req = at + 0x100;
        let in_len = (size_of::<InHeader>() + size_of::<GetattrIn>()) as u32;
        let header = InHeader {
            len: in_len,
            opcode: Opcode::Getattr as u32,
            unique: 7,
            nodeid: super::super::fuse::ROOT_ID,
            ..Default::default()
        };
        mem.write_obj(header, GuestAddress(req)).unwrap();
        let mut t = Vec::new();
        for v in [1u16, 2, 1, 0] {
            t.extend_from_slice(&v.to_le_bytes());
        }
        for (addr, len) in [(req, in_len), (reply, reply_len)] {
            t.extend_from_slice(&addr.to_le_bytes());
            t.extend_from_slice(&len.to_le_bytes());
            t.extend_from_slice(&0u32.to_le_bytes());
        }
        mem.write_slice(&t, GuestAddress(at)).unwrap();
        t.len() as u64
    }

    #[test]
    fn direct_requests_run_without_the_queue() {
        use super::super::fuse::{AttrOut, OutHeader};

        let mem = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x40000)]).unwrap();
        let core = core_with_queues(&mem, &[0x1000, 0x2000]);
        let q1 = VirtQueue::new(GuestAddress(0x2000), &mem, 16);
        let desc = q1.create_queue().desc_table.0;
        assert_eq!(core.request_queue_at(desc), Some(REQ_INDEX));
        let hpq = VirtQueue::new(GuestAddress(0x1000), &mem, 16);
        assert_eq!(core.request_queue_at(hpq.create_queue().desc_table.0), None);
        assert_eq!(core.request_queue_at(0xdead_0000), None);

        let reply_len = (size_of::<OutHeader>() + size_of::<AttrOut>()) as u32;
        let len = direct_getattr(&mem, 0x10000, 0x18000, reply_len);
        let ret = core.run_direct(REQ_INDEX, 0x10000, len);
        assert_eq!(ret, reply_len as u64);
        let out: OutHeader = mem.read_obj(GuestAddress(0x18000)).unwrap();
        assert_eq!((out.len, out.error, out.unique), (reply_len, 0, 7));
        // Nothing went through the queue.
        assert_eq!(q1.used.idx.get(), 0);

        // A reply buffer past the end of guest memory: refused before anything is written.
        let len = direct_getattr(&mem, 0x10000, 0x40000 - 8, reply_len);
        assert_eq!(core.run_direct(REQ_INDEX, 0x10000, len), direct::INVALID);
        // A truncated table, and one outside guest memory.
        assert_eq!(
            core.run_direct(REQ_INDEX, 0x10000, len - 1),
            direct::INVALID
        );
        assert_eq!(core.run_direct(REQ_INDEX, 0x40000, len), direct::INVALID);
        core.stop_pool();
    }
}
