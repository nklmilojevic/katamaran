#[cfg(target_os = "macos")]
use crossbeam_channel::Sender;
use std::cmp;
use std::io::Write;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use utils::eventfd::{EFD_NONBLOCK, EventFd};
#[cfg(target_os = "macos")]
use utils::worker_message::WorkerMessage;
use virtio_bindings::{virtio_config::VIRTIO_F_VERSION_1, virtio_ring::VIRTIO_RING_F_EVENT_IDX};
use vm_memory::{ByteValued, GuestMemoryMmap};

use super::super::{
    ActivateError, ActivateResult, DeviceQueue, DeviceState, FsError, QueueConfig, VirtioDevice,
    VirtioShmRegion,
};
use super::ExportTable;
use super::passthrough;
use super::rosetta::Rosetta;
use super::virtual_entry::VirtualDirEntry;
use super::worker::{FsCore, FsWorker};
use super::{CachePolicy, PermissionSemantics, defs, defs::uapi};
use crate::virtio::InterruptTransport;

#[derive(Copy, Clone)]
#[repr(C, packed)]
struct VirtioFsConfig {
    tag: [u8; 36],
    num_request_queues: u32,
}

impl Default for VirtioFsConfig {
    fn default() -> Self {
        VirtioFsConfig {
            tag: [0; 36],
            num_request_queues: 0,
        }
    }
}

unsafe impl ByteValued for VirtioFsConfig {}

pub struct Fs {
    avail_features: u64,
    acked_features: u64,
    device_state: DeviceState,
    config: VirtioFsConfig,
    allow_idmap: bool,
    shm_region: Option<VirtioShmRegion>,
    passthrough_cfg: Option<passthrough::Config>,
    read_only: bool,
    virtual_entries: Vec<VirtualDirEntry<'static>>,
    rosetta: Option<Arc<Rosetta>>,
    worker_thread: Option<JoinHandle<()>>,
    worker_stopfd: EventFd,
    /// Set when request-queue notifications are processed on the notifying
    /// vCPU thread instead of waking the worker (KATAMARAN_FS_INLINE != 0).
    inline_core: Option<Arc<FsCore>>,
    exit_code: Arc<AtomicI32>,
    #[cfg(target_os = "macos")]
    map_sender: Option<Sender<WorkerMessage>>,
}

impl Fs {
    pub fn new(
        fs_id: String,
        semantics: PermissionSemantics,
        shared_dir: Option<String>,
        exit_code: Arc<AtomicI32>,
        read_only: bool,
        virtual_entries: Vec<VirtualDirEntry<'static>>,
    ) -> super::Result<Fs> {
        let avail_features = (1u64 << VIRTIO_F_VERSION_1) | (1u64 << VIRTIO_RING_F_EVENT_IDX);

        let tag = fs_id.into_bytes();
        let mut config = VirtioFsConfig::default();
        config.tag[..tag.len()].copy_from_slice(tag.as_slice());
        config.num_request_queues = 1;

        let fs_cfg = shared_dir.map(|root_dir| passthrough::Config {
            root_dir,
            ..Default::default()
        });

        let allow_idmap = matches!(semantics, PermissionSemantics::LinuxComplete);

        // The guest probes once at driver init, before the device is activated.
        #[cfg(target_os = "macos")]
        if std::env::var("KATAMARAN_FS_INLINE").as_deref() != Ok("0") {
            hvf::vendor_call::register(KAT_FS_PROBE, |[abi, _, _]| {
                super::sync_calls::probe(KAT_FS_MAGIC, abi)
            });
        }

        let mut fs = Fs {
            avail_features,
            acked_features: 0,
            device_state: DeviceState::Inactive,
            config,
            allow_idmap,
            shm_region: None,
            passthrough_cfg: fs_cfg,
            read_only,
            virtual_entries,
            rosetta: None,
            worker_thread: None,
            worker_stopfd: EventFd::new(EFD_NONBLOCK).map_err(FsError::EventFd)?,
            inline_core: None,
            exit_code,
            #[cfg(target_os = "macos")]
            map_sender: None,
        };
        if !fs.set_permission_semantics(semantics) && fs.passthrough_cfg.is_some() {
            return Err(FsError::UnsupportedPermissionSemantics);
        }
        Ok(fs)
    }

    pub fn id(&self) -> &str {
        defs::FS_DEV_ID
    }

    /// Offer `n` request queues (at most [`defs::MAX_REQUEST_QUEUES`]) before activation. The
    /// guest gives each CPU its own queue where it can, so requests from different vCPUs are
    /// processed in parallel instead of one after another on one queue.
    pub fn set_request_queues(&mut self, n: u32) -> bool {
        if self.device_state.is_activated() || n == 0 || n as usize > defs::MAX_REQUEST_QUEUES {
            return false;
        }
        self.config.num_request_queues = n;
        true
    }

    pub fn set_shm_region(&mut self, shm_region: VirtioShmRegion) {
        self.shm_region = Some(shm_region);
    }

    pub fn set_permission_semantics(&mut self, semantics: PermissionSemantics) -> bool {
        if cfg!(target_os = "linux") && matches!(semantics, PermissionSemantics::LinuxSimplified) {
            return false;
        }
        if cfg!(not(target_os = "macos"))
            && matches!(semantics, PermissionSemantics::LinuxSimplifiedFixedOwner)
        {
            return false;
        }
        let Some(cfg) = self.passthrough_cfg.as_mut() else {
            return false;
        };
        let complete = matches!(semantics, PermissionSemantics::LinuxComplete);
        cfg.semantics = semantics;
        // Requester ownership is context-dependent, so attributes cannot be cached.
        cfg.attr_timeout = if matches!(semantics, PermissionSemantics::LinuxSimplified) {
            Duration::ZERO
        } else {
            Duration::from_secs(5)
        };
        cfg.xattr = complete;
        #[cfg(target_os = "macos")]
        {
            cfg.fixed_owner = None;
            cfg.caller_owns = false;
        }
        self.allow_idmap = complete;
        true
    }

    /// Sets the owner that `LinuxSimplifiedFixedOwner` reports; fails in any other mode.
    pub fn set_fixed_owner(&mut self, uid: u32, gid: u32) -> bool {
        #[cfg(target_os = "macos")]
        if let Some(cfg) = self.passthrough_cfg.as_mut()
            && matches!(
                cfg.semantics,
                PermissionSemantics::LinuxSimplifiedFixedOwner
            )
        {
            cfg.fixed_owner = Some((uid, gid));
            return true;
        }
        let _ = (uid, gid);
        false
    }

    /// Answer permission checks as if the caller owned every file, matching a guest that
    /// mounts the share with `owner_caller`; fails outside `LinuxSimplifiedFixedOwner` mode.
    pub fn set_caller_owns(&mut self, caller_owns: bool) -> bool {
        #[cfg(target_os = "macos")]
        if let Some(cfg) = self.passthrough_cfg.as_mut()
            && matches!(
                cfg.semantics,
                PermissionSemantics::LinuxSimplifiedFixedOwner
            )
        {
            cfg.caller_owns = caller_owns;
            return true;
        }
        let _ = caller_owns;
        false
    }

    /// Whether the device is in `LinuxSimplifiedFixedOwner` mode with no owner set.
    pub fn lacks_fixed_owner(&self) -> bool {
        #[cfg(target_os = "macos")]
        if let Some(cfg) = self.passthrough_cfg.as_ref() {
            return matches!(
                cfg.semantics,
                PermissionSemantics::LinuxSimplifiedFixedOwner
            ) && cfg.fixed_owner.is_none();
        }
        false
    }

    /// Make FSYNC and FSYNCDIR no-ops for this share (for disposable data).
    pub fn set_no_sync_io(&mut self, no_sync_io: bool) -> bool {
        #[cfg(target_os = "macos")]
        if let Some(cfg) = self.passthrough_cfg.as_mut() {
            cfg.no_sync_io = no_sync_io;
            return true;
        }
        let _ = no_sync_io;
        false
    }

    /// Offer FUSE_WRITEBACK_CACHE to the guest. Only for shares nothing else
    /// writes to while the guest runs.
    pub fn set_writeback(&mut self, writeback: bool) -> bool {
        let Some(cfg) = self.passthrough_cfg.as_mut() else {
            return false;
        };
        cfg.writeback = writeback;
        true
    }

    /// How long the guest may cache entries and attributes, and failed lookups (zero: not at
    /// all). Attributes stay uncached when ownership depends on the requester. Only for shares
    /// whose host changes are invalidated through [`Fs::guest_cache`].
    pub fn set_cache_timeouts(
        &mut self,
        entry: Duration,
        attr: Duration,
        negative: Duration,
    ) -> bool {
        let Some(cfg) = self.passthrough_cfg.as_mut() else {
            return false;
        };
        cfg.entry_timeout = entry;
        if !cfg.attr_timeout.is_zero() {
            cfg.attr_timeout = attr;
        }
        cfg.negative_timeout = negative;
        true
    }

    /// Which host files the guest holds, once the device is active.
    #[cfg(target_os = "macos")]
    pub fn guest_cache(&self) -> Option<passthrough::GuestCache> {
        self.passthrough_cfg.as_ref().map(|c| c.guest_cache.clone())
    }

    pub fn set_cache_policy(&mut self, policy: CachePolicy) -> bool {
        let Some(cfg) = self.passthrough_cfg.as_mut() else {
            return false;
        };
        cfg.cache_policy = policy;
        true
    }

    pub fn set_export_table(&mut self, export_table: ExportTable) -> u64 {
        static FS_UNIQUE_ID: AtomicU64 = AtomicU64::new(0);

        let Some(cfg) = self.passthrough_cfg.as_mut() else {
            // NullFs-backed devices have no passthrough config and don't
            // participate in cross-domain fd export. Consume (and waste) an
            // fsid so numbering stays dense, but don't store the table.
            return FS_UNIQUE_ID.fetch_add(1, Ordering::Relaxed);
        };
        cfg.export_fsid = FS_UNIQUE_ID.fetch_add(1, Ordering::Relaxed);
        cfg.export_table = Some(export_table);

        cfg.export_fsid
    }

    /// Answer Rosetta for Linux's ioctls, for a share of the RosettaLinux
    /// directory; fails when the shared directory holds no Rosetta binary.
    pub fn set_rosetta(&mut self) -> bool {
        let Some(cfg) = self.passthrough_cfg.as_ref() else {
            return false;
        };
        match Rosetta::load(Path::new(&cfg.root_dir)) {
            Ok(r) => {
                self.rosetta = Some(Arc::new(r));
                true
            }
            Err(e) => {
                error!("virtio_fs: {}: {e}", cfg.root_dir);
                false
            }
        }
    }

    pub fn add_virtual_entry(&mut self, entry: VirtualDirEntry<'static>) {
        self.virtual_entries.push(entry);
    }

    pub fn set_exit_code(&mut self, exit_code: Arc<AtomicI32>) {
        self.exit_code = exit_code;
    }

    #[cfg(target_os = "macos")]
    pub fn set_map_sender(&mut self, map_sender: Sender<WorkerMessage>) {
        self.map_sender = Some(map_sender);
    }
}

impl VirtioDevice for Fs {
    fn avail_features(&self) -> u64 {
        self.avail_features
    }

    fn acked_features(&self) -> u64 {
        self.acked_features
    }

    fn set_acked_features(&mut self, acked_features: u64) {
        self.acked_features = acked_features
    }

    fn device_type(&self) -> u32 {
        uapi::VIRTIO_ID_FS
    }

    fn device_name(&self) -> &str {
        "fs"
    }

    fn queue_config(&self) -> &[QueueConfig] {
        let n = self.config.num_request_queues;
        &defs::QUEUE_CONFIG[..defs::REQ_INDEX + n as usize]
    }

    fn config_len(&self) -> Option<u32> {
        Some(size_of::<VirtioFsConfig>() as u32)
    }

    fn read_config(&self, offset: u64, mut data: &mut [u8]) {
        let config_slice = self.config.as_slice();
        let config_len = config_slice.len() as u64;
        if offset >= config_len {
            error!("Failed to read config space");
            return;
        }
        if let Some(end) = offset.checked_add(data.len() as u64) {
            // This write can't fail, offset and end are checked against config_len.
            data.write_all(&config_slice[offset as usize..cmp::min(end, config_len) as usize])
                .unwrap();
        }
    }

    fn write_config(&mut self, offset: u64, data: &[u8]) {
        warn!(
            "fs: guest driver attempted to write device config (offset={:x}, len={:x})",
            offset,
            data.len()
        );
    }

    fn activate(
        &mut self,
        mem: GuestMemoryMmap,
        interrupt: InterruptTransport,
        queues: Vec<DeviceQueue>,
    ) -> ActivateResult {
        if self.worker_thread.is_some() {
            panic!("virtio_fs: worker thread already exists");
        }

        // Extract queues and eventfds from DeviceQueues.
        let mut worker_queues = Vec::with_capacity(queues.len());
        let mut queue_evts = Vec::with_capacity(queues.len());
        for dq in queues {
            worker_queues.push(dq.queue);
            queue_evts.push(dq.event);
        }

        let virtual_entries = self.virtual_entries.clone();
        let worker = FsWorker::new(
            worker_queues,
            queue_evts,
            interrupt.clone(),
            mem.clone(),
            self.allow_idmap,
            self.shm_region.clone(),
            self.passthrough_cfg.clone(),
            self.read_only,
            virtual_entries,
            self.rosetta.clone(),
            self.worker_stopfd.try_clone().unwrap(),
            self.exit_code.clone(),
            #[cfg(target_os = "macos")]
            self.map_sender.clone(),
        )
        .map_err(|e| {
            error!("virtio_fs: failed to create worker: {}", e);
            ActivateError::BadActivate
        })?;
        if std::env::var("KATAMARAN_FS_INLINE").as_deref() != Ok("0") {
            self.inline_core = Some(worker.core());
            #[cfg(target_os = "macos")]
            super::sync_calls::register(worker.core());
        }
        super::worker::start_stats_reporter();
        self.worker_thread = Some(worker.run());

        self.device_state = DeviceState::Activated(mem, interrupt);
        Ok(())
    }

    fn is_activated(&self) -> bool {
        self.device_state.is_activated()
    }

    fn shm_region(&self) -> Option<&VirtioShmRegion> {
        self.shm_region.as_ref()
    }

    fn handles_notify_inline(&self) -> bool {
        true
    }

    fn notify_inline(&mut self, queue_index: u32) -> bool {
        if (queue_index as usize) < defs::REQ_INDEX {
            return false;
        }
        let Some(core) = &self.inline_core else {
            return false;
        };
        let drained = core.try_drain_notified(queue_index as usize);
        if drained {
            super::worker::note_inline(1);
        }
        drained
    }

    fn reset(&mut self) -> bool {
        #[cfg(target_os = "macos")]
        if let Some(core) = self.inline_core.take() {
            super::sync_calls::unregister(&core);
        }
        self.inline_core = None;
        if let Some(worker) = self.worker_thread.take() {
            let _ = self.worker_stopfd.write(1);
            if let Err(e) = worker.join() {
                error!("error waiting for worker thread: {e:?}");
            }
        }
        self.device_state = DeviceState::Inactive;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filesystem(shared_dir: Option<String>) -> Fs {
        Fs::new(
            "shared".to_owned(),
            PermissionSemantics::LinuxComplete,
            shared_dir,
            Arc::new(AtomicI32::new(0)),
            false,
            Vec::new(),
        )
        .unwrap()
    }

    #[test]
    fn request_queues_size_the_queues_and_the_config_space() {
        let mut fs = filesystem(None);
        assert_eq!(fs.queue_config().len(), 2);
        assert!(fs.set_request_queues(14));
        assert_eq!(fs.queue_config().len(), 15);
        let mut n = [0u8; 4];
        fs.read_config(36, &mut n);
        assert_eq!(u32::from_le_bytes(n), 14);
        assert!(!fs.set_request_queues(0));
        assert!(!fs.set_request_queues(defs::MAX_REQUEST_QUEUES as u32 + 1));
        assert_eq!(fs.queue_config().len(), 15);
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn permission_changes_update_ownership_caching_and_idmap_support() {
        let mut fs = filesystem(Some("/".to_owned()));
        for semantics in [
            PermissionSemantics::LinuxSimplified,
            PermissionSemantics::LinuxComplete,
        ] {
            assert!(fs.set_permission_semantics(semantics));
            let cfg = fs.passthrough_cfg.as_ref().unwrap();
            if matches!(semantics, PermissionSemantics::LinuxComplete) {
                assert!(matches!(cfg.semantics, PermissionSemantics::LinuxComplete));
                assert_eq!(cfg.attr_timeout, Duration::from_secs(5));
                assert!(cfg.xattr);
                assert!(fs.allow_idmap);
            } else {
                assert!(matches!(
                    cfg.semantics,
                    PermissionSemantics::LinuxSimplified
                ));
                assert_eq!(cfg.attr_timeout, Duration::ZERO);
                assert!(!cfg.xattr);
                assert!(!fs.allow_idmap);
            }
        }
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn fixed_owner_semantics_keep_attributes_cacheable() {
        let mut fs = filesystem(Some("/".to_owned()));
        assert!(fs.set_permission_semantics(PermissionSemantics::LinuxSimplifiedFixedOwner));
        assert!(fs.lacks_fixed_owner());
        assert!(fs.set_fixed_owner(1234, 5678));
        assert!(!fs.lacks_fixed_owner());
        let cfg = fs.passthrough_cfg.as_ref().unwrap();
        assert_eq!(cfg.fixed_owner, Some((1234, 5678)));
        assert_eq!(cfg.attr_timeout, Duration::from_secs(5));
        assert!(!cfg.xattr);
        assert!(!fs.allow_idmap);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn fixed_owner_is_rejected_in_other_semantics() {
        let mut fs = filesystem(Some("/".to_owned()));
        for semantics in [
            PermissionSemantics::LinuxComplete,
            PermissionSemantics::LinuxSimplified,
        ] {
            assert!(fs.set_permission_semantics(semantics));
            assert!(!fs.set_fixed_owner(1234, 5678));
            assert!(!fs.lacks_fixed_owner());
            assert_eq!(fs.passthrough_cfg.as_ref().unwrap().fixed_owner, None);
        }
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn changing_semantics_clears_the_fixed_owner() {
        let mut fs = filesystem(Some("/".to_owned()));
        assert!(fs.set_permission_semantics(PermissionSemantics::LinuxSimplifiedFixedOwner));
        assert!(fs.set_fixed_owner(1234, 5678));
        assert!(fs.set_permission_semantics(PermissionSemantics::LinuxSimplified));
        assert!(fs.set_permission_semantics(PermissionSemantics::LinuxSimplifiedFixedOwner));
        assert!(fs.lacks_fixed_owner());
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn fixed_owner_semantics_are_rejected_off_macos() {
        let mut fs = filesystem(Some("/".to_owned()));
        assert!(!fs.set_permission_semantics(PermissionSemantics::LinuxSimplifiedFixedOwner));
        assert!(!fs.set_fixed_owner(1234, 5678));
        assert!(!fs.lacks_fixed_owner());
    }

    #[test]
    fn null_filesystems_reject_permission_changes() {
        let mut fs = filesystem(None);
        assert!(!fs.set_permission_semantics(PermissionSemantics::LinuxSimplified));
        assert!(fs.allow_idmap);
        assert!(fs.passthrough_cfg.is_none());
    }

    #[test]
    fn cache_policy_changes_preserve_permission_options() {
        let mut fs = filesystem(Some("/".to_owned()));
        assert_eq!(
            fs.passthrough_cfg.as_ref().unwrap().cache_policy,
            CachePolicy::Auto
        );
        let attr_timeout = Duration::from_millis(123);
        fs.passthrough_cfg.as_mut().unwrap().attr_timeout = attr_timeout;

        for policy in [CachePolicy::Never, CachePolicy::Always, CachePolicy::Auto] {
            assert!(fs.set_cache_policy(policy));
            let cfg = fs.passthrough_cfg.as_ref().unwrap();
            assert_eq!(cfg.cache_policy, policy);
            assert!(matches!(cfg.semantics, PermissionSemantics::LinuxComplete));
            assert_eq!(cfg.attr_timeout, attr_timeout);
            assert!(cfg.xattr);
            assert!(fs.allow_idmap);
        }
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn permission_changes_preserve_cache_policy() {
        let mut fs = filesystem(Some("/".to_owned()));
        assert!(fs.set_cache_policy(CachePolicy::Always));

        for semantics in [
            PermissionSemantics::LinuxSimplified,
            PermissionSemantics::LinuxComplete,
        ] {
            assert!(fs.set_permission_semantics(semantics));
            assert_eq!(
                fs.passthrough_cfg.as_ref().unwrap().cache_policy,
                CachePolicy::Always
            );
        }
    }

    #[test]
    fn null_filesystems_reject_cache_policy_changes() {
        let mut fs = filesystem(None);
        for policy in [CachePolicy::Never, CachePolicy::Auto, CachePolicy::Always] {
            assert!(!fs.set_cache_policy(policy));
        }
        assert!(fs.allow_idmap);
        assert!(fs.passthrough_cfg.is_none());
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn constructors_apply_the_selected_permission_semantics() {
        let fs = Fs::new(
            "shared".to_owned(),
            PermissionSemantics::LinuxSimplified,
            Some("/".to_owned()),
            Arc::new(AtomicI32::new(0)),
            false,
            Vec::new(),
        )
        .unwrap();
        let cfg = fs.passthrough_cfg.as_ref().unwrap();
        assert!(matches!(
            cfg.semantics,
            PermissionSemantics::LinuxSimplified
        ));
        assert_eq!(cfg.attr_timeout, Duration::ZERO);
        assert!(!cfg.xattr);
        assert!(!fs.allow_idmap);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn unsupported_permission_changes_preserve_configuration() {
        let mut fs = filesystem(Some("/".to_owned()));
        let attr_timeout = Duration::from_millis(123);
        fs.passthrough_cfg.as_mut().unwrap().attr_timeout = attr_timeout;

        assert!(!fs.set_permission_semantics(PermissionSemantics::LinuxSimplified));

        let cfg = fs.passthrough_cfg.as_ref().unwrap();
        assert!(matches!(cfg.semantics, PermissionSemantics::LinuxComplete));
        assert_eq!(cfg.attr_timeout, attr_timeout);
        assert!(cfg.xattr);
        assert!(fs.allow_idmap);
        assert!(fs.set_permission_semantics(PermissionSemantics::LinuxComplete));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn constructors_reject_unsupported_permission_semantics() {
        let result = Fs::new(
            "shared".to_owned(),
            PermissionSemantics::LinuxSimplified,
            Some("/".to_owned()),
            Arc::new(AtomicI32::new(0)),
            false,
            Vec::new(),
        );

        assert!(matches!(
            result,
            Err(FsError::UnsupportedPermissionSemantics)
        ));
    }
}

/// Guest probe: returns [`KAT_FS_MAGIC`] when synchronous submission works, with the
/// direct-request ABI version in bits 32.. when the guest asks for one.
#[cfg(target_os = "macos")]
const KAT_FS_PROBE: u64 = 0xC600_4B00;
#[cfg(target_os = "macos")]
const KAT_FS_MAGIC: u64 = 0x4B41_544D;
