//! A minimal FSEvents (CoreServices) stream with file-level events, delivered
//! on a serial dispatch queue. The FFI follows kiri's `daemon/watch.rs`
//! (Apache-2.0).

use std::ffi::{CStr, c_char, c_void};

/// Event flags, as `FSEventStreamEventFlags`.
pub mod flag {
    pub const MUST_SCAN_SUB_DIRS: u32 = 0x1;
    pub const USER_DROPPED: u32 = 0x2;
    pub const KERNEL_DROPPED: u32 = 0x4;
    pub const HISTORY_DONE: u32 = 0x10;
    pub const ROOT_CHANGED: u32 = 0x20;
    pub const ITEM_CREATED: u32 = 0x100;
    pub const ITEM_REMOVED: u32 = 0x200;
    pub const ITEM_RENAMED: u32 = 0x800;
    /// Changes under the path were coalesced or lost: look again.
    pub const RESCAN: u32 = MUST_SCAN_SUB_DIRS | USER_DROPPED | KERNEL_DROPPED | ROOT_CHANGED;
}

/// `kFSEventStreamEventIdSinceNow`.
const SINCE_NOW: u64 = u64::MAX;

/// One delivered event: path, flags.
pub type Event = (String, u32);

type FSEventStreamRef = *mut c_void;
type CFRef = *const c_void;

#[repr(C)]
struct FSEventStreamContext {
    version: isize,
    info: *mut c_void,
    retain: *const c_void,
    release: *const c_void,
    copy_description: *const c_void,
}

type Callback =
    extern "C" fn(FSEventStreamRef, *mut c_void, usize, *mut c_void, *const u32, *const u64);

#[link(name = "CoreServices", kind = "framework")]
unsafe extern "C" {
    fn FSEventStreamCreate(
        alloc: CFRef,
        cb: Callback,
        ctx: *const FSEventStreamContext,
        paths: CFRef,
        since: u64,
        latency: f64,
        flags: u32,
    ) -> FSEventStreamRef;
    fn FSEventStreamSetDispatchQueue(stream: FSEventStreamRef, queue: *mut c_void);
    fn FSEventStreamSetExclusionPaths(stream: FSEventStreamRef, paths: CFRef) -> u8;
    fn FSEventStreamStart(stream: FSEventStreamRef) -> u8;
    fn FSEventStreamFlushSync(stream: FSEventStreamRef);
    fn FSEventStreamStop(stream: FSEventStreamRef);
    fn FSEventStreamInvalidate(stream: FSEventStreamRef);
    fn FSEventStreamRelease(stream: FSEventStreamRef);
}

#[link(name = "CoreFoundation", kind = "framework")]
unsafe extern "C" {
    static kCFTypeArrayCallBacks: u8;
    fn CFStringCreateWithBytes(
        alloc: CFRef,
        bytes: *const u8,
        len: isize,
        encoding: u32,
        external: u8,
    ) -> CFRef;
    fn CFArrayCreate(
        alloc: CFRef,
        values: *const CFRef,
        count: isize,
        callbacks: *const u8,
    ) -> CFRef;
    fn CFRelease(cf: CFRef);
}

unsafe extern "C" {
    fn dispatch_queue_create(label: *const c_char, attr: *const c_void) -> *mut c_void;
    fn dispatch_sync_f(queue: *mut c_void, ctx: *mut c_void, work: extern "C" fn(*mut c_void));
}

const UTF8: u32 = 0x0800_0100;
const CREATE_NO_DEFER: u32 = 0x2;
const CREATE_FILE_EVENTS: u32 = 0x10;
/// FSEvents takes at most this many exclusion paths.
pub const MAX_EXCLUSIONS: usize = 8;

struct Info {
    sink: Box<dyn Fn(Vec<Event>) + Send + Sync>,
}

/// A running stream; dropping it delivers what it has seen and stops it,
/// after which no callback runs.
pub struct Stream {
    stream: FSEventStreamRef,
}

// SAFETY: started and torn down by its owner only; FSEvents' calls are
// thread-safe.
unsafe impl Send for Stream {}

extern "C" fn on_events(
    _: FSEventStreamRef,
    info: *mut c_void,
    n: usize,
    paths: *mut c_void,
    flags: *const u32,
    _ids: *const u64,
) {
    // SAFETY: FSEvents passes `n` C strings and flags, and `info` is the
    // Info this stream was made with, freed only by `release_info`.
    let (info, paths, flags) = unsafe {
        (
            &*(info as *const Info),
            std::slice::from_raw_parts(paths as *const *const c_char, n),
            std::slice::from_raw_parts(flags, n),
        )
    };
    let mut events = Vec::with_capacity(n);
    for (p, &f) in paths.iter().zip(flags) {
        if f & flag::HISTORY_DONE != 0 {
            continue;
        }
        // SAFETY: a NUL-terminated path from FSEvents.
        if let Ok(path) = unsafe { CStr::from_ptr(*p) }.to_str() {
            events.push((path.to_owned(), f));
        }
    }
    if !events.is_empty() {
        (info.sink)(events);
    }
}

extern "C" fn release_info(info: *const c_void) {
    // SAFETY: the Box made in Stream::start, released once by FSEvents.
    drop(unsafe { Box::from_raw(info as *mut Info) });
}

fn queue() -> *mut c_void {
    static QUEUE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    // SAFETY: a serial queue made once and kept for the process's life.
    *QUEUE.get_or_init(|| unsafe {
        dispatch_queue_create(c"dev.katamaran.fsevents".as_ptr(), std::ptr::null())
    } as usize) as *mut c_void
}

fn cf_array(items: &[String]) -> CFRef {
    // SAFETY: CoreFoundation objects made here; the array retains the strings.
    unsafe {
        let strings: Vec<CFRef> = items
            .iter()
            .map(|d| {
                CFStringCreateWithBytes(std::ptr::null(), d.as_ptr(), d.len() as isize, UTF8, 0)
            })
            .filter(|s| !s.is_null())
            .collect();
        let array = CFArrayCreate(
            std::ptr::null(),
            strings.as_ptr(),
            strings.len() as isize,
            &raw const kCFTypeArrayCallBacks,
        );
        for s in &strings {
            CFRelease(*s);
        }
        array
    }
}

impl Stream {
    /// Watch `roots` (recursively) from now on, ignoring `exclusions`, and
    /// hand each batch to `sink` on FSEvents' queue.
    pub fn start(
        roots: &[String],
        exclusions: &[String],
        latency: f64,
        sink: impl Fn(Vec<Event>) + Send + Sync + 'static,
    ) -> std::io::Result<Stream> {
        let info = Box::into_raw(Box::new(Info {
            sink: Box::new(sink),
        }));
        // SAFETY: FSEvents owns `info` from FSEventStreamCreate on and frees it
        // through `release_info`; before that it is still ours.
        unsafe {
            let paths = cf_array(roots);
            let ctx = FSEventStreamContext {
                version: 0,
                info: info.cast(),
                retain: std::ptr::null(),
                release: release_info as *const c_void,
                copy_description: std::ptr::null(),
            };
            let stream = FSEventStreamCreate(
                std::ptr::null(),
                on_events,
                &ctx,
                paths,
                SINCE_NOW,
                latency,
                CREATE_FILE_EVENTS | CREATE_NO_DEFER,
            );
            CFRelease(paths);
            if stream.is_null() {
                drop(Box::from_raw(info));
                return Err(std::io::Error::other("FSEventStreamCreate failed"));
            }
            if !exclusions.is_empty() {
                let ex = cf_array(&exclusions[..exclusions.len().min(MAX_EXCLUSIONS)]);
                FSEventStreamSetExclusionPaths(stream, ex);
                CFRelease(ex);
            }
            FSEventStreamSetDispatchQueue(stream, queue());
            if FSEventStreamStart(stream) == 0 {
                FSEventStreamInvalidate(stream);
                FSEventStreamRelease(stream);
                return Err(std::io::Error::other("FSEventStreamStart failed"));
            }
            Ok(Stream { stream })
        }
    }
}

extern "C" fn tear_down(stream: *mut c_void) {
    // SAFETY: a started stream, torn down once.
    unsafe {
        FSEventStreamStop(stream);
        FSEventStreamInvalidate(stream);
        FSEventStreamRelease(stream);
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // Deliver what the stream has seen (its callbacks run on the queue
        // meanwhile), then stop it on its own serial queue, so no callback
        // runs while it stops. Never dropped from that queue.
        // SAFETY: the queue lives for the process; the stream is ours.
        unsafe {
            FSEventStreamFlushSync(self.stream);
            dispatch_sync_f(queue(), self.stream, tear_down);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn delivers_file_events_quickly() {
        let dir = std::env::temp_dir().join(format!("kat-fsevents-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = std::fs::canonicalize(&dir).unwrap();
        let (tx, rx) = mpsc::channel();
        let tx = std::sync::Mutex::new(tx);
        let stream = Stream::start(&[dir.display().to_string()], &[], 0.01, move |events| {
            let _ = tx.lock().unwrap().send(events);
        })
        .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let file = dir.join("hello");
        let t = Instant::now();
        std::fs::write(&file, "x").unwrap();
        let want = file.display().to_string();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut seen = None;
        while seen.is_none() && Instant::now() < deadline {
            if let Ok(batch) = rx.recv_timeout(Duration::from_millis(100)) {
                seen = batch.into_iter().find(|(p, _)| *p == want);
            }
        }
        let (_, flags) = seen.expect("no event for the new file");
        assert!(flags & flag::ITEM_CREATED != 0, "flags {flags:#x}");
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "took {:?}",
            t.elapsed()
        );
        drop(stream);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
