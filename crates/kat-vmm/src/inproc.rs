//! gvproxy in the VM process: `libgvproxy.dylib` (gvproxy built with
//! `-buildmode=c-shared`, see xtask) is loaded and started with gvproxy's
//! command line. Frames then pass by function call between the virtio-net
//! device and gVisor, instead of through a datagram socket and the threads
//! that wait on it at both ends.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use anyhow::{Context, bail};
use krun::net_inproc::{InprocNet, TxFn, inproc_net_rx};

type RxFn = unsafe extern "C" fn(*mut c_void, *const libc::iovec, c_int) -> c_int;
type StartFn = unsafe extern "C" fn(
    argc: c_int,
    argv: *const *const c_char,
    rx: RxFn,
    ctx: *mut c_void,
) -> c_int;

/// gvproxy starting on its own thread, which overlaps the rest of the VM's
/// setup (about 2 ms); the VM must not run before [`Starting::wait`].
pub struct Starting(thread::JoinHandle<anyhow::Result<()>>);

impl Starting {
    pub fn wait(self) -> anyhow::Result<()> {
        self.0
            .join()
            .map_err(|_| anyhow::anyhow!("gvproxy start panicked"))?
    }
}

/// Load gvproxy from `library` and start it with `args`. The device can be
/// attached to the returned stack at once; the guest only reaches it once
/// the VM runs, after [`Starting::wait`].
pub fn start(library: &Path, args: &[String]) -> anyhow::Result<(Arc<InprocNet>, Starting)> {
    let path = CString::new(library.as_os_str().as_bytes())?;
    // SAFETY: a valid C string; the library stays loaded for the life of
    // the process, as the Go runtime inside it requires.
    let handle = unsafe { libc::dlopen(path.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
    if handle.is_null() {
        bail!("load {}: {}", library.display(), dl_error());
    }
    // SAFETY: the symbols are gvproxy's exports with these signatures
    // (cmd/gvproxy/inproc.go).
    let (start, tx) = unsafe {
        (
            symbol::<StartFn>(handle, c"kat_gvproxy_start")?,
            symbol::<TxFn>(handle, c"kat_gvproxy_tx")?,
        )
    };
    let net = Arc::new(InprocNet::new(tx));
    // gvproxy holds this for the life of the process.
    let ctx = Arc::into_raw(net.clone()) as *mut c_void;
    let args: Vec<CString> = args
        .iter()
        .map(|a| CString::new(a.as_str()))
        .collect::<Result<_, _>>()
        .context("gvproxy argument contains a NUL")?;
    let ctx = ctx as usize;
    let starting = thread::Builder::new()
        .name("gvproxy start".into())
        .spawn(move || {
            unsafe extern "C" {
                fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
            }
            const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
            unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
            let argv: Vec<*const c_char> = args.iter().map(|a| a.as_ptr()).collect();
            // SAFETY: argv holds argc valid C strings that outlive the call;
            // ctx is the InprocNet inproc_net_rx expects, kept alive above.
            let r = unsafe {
                start(
                    argv.len() as c_int,
                    argv.as_ptr(),
                    inproc_net_rx,
                    ctx as *mut c_void,
                )
            };
            if r != 0 {
                bail!(
                    "gvproxy did not start: {} (see its log)",
                    std::io::Error::from_raw_os_error(r)
                );
            }
            Ok(())
        })?;
    Ok((net, Starting(starting)))
}

/// # Safety
/// `T` must be the function pointer type of the symbol `name`.
unsafe fn symbol<T: Copy>(handle: *mut c_void, name: &CStr) -> anyhow::Result<T> {
    // SAFETY: handle is a live dlopen handle and name a C string.
    let p = unsafe { libc::dlsym(handle, name.as_ptr()) };
    if p.is_null() {
        bail!("{}: {}", name.to_string_lossy(), dl_error());
    }
    // SAFETY: T is a function pointer type, the same size as *mut c_void.
    Ok(unsafe { std::mem::transmute_copy::<*mut c_void, T>(&p) })
}

fn dl_error() -> String {
    // SAFETY: dlerror returns null or a C string valid until the next call.
    let e = unsafe { libc::dlerror() };
    if e.is_null() {
        "unknown error".into()
    } else {
        unsafe { CStr::from_ptr(e) }.to_string_lossy().into_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use super::*;

    /// Loads the real library if `cargo xtask gvproxy` built it, starts it,
    /// and asks gVisor for the gateway's MAC: the reply comes back through
    /// the receive function and, with no guest attached, is counted as a
    /// drop. One start per process, so one test.
    #[test]
    fn starts_gvproxy_and_exchanges_frames_with_it() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let library = root.join("target/release/libgvproxy.dylib");
        if !library.is_file() {
            eprintln!("skipped: no {}", library.display());
            return;
        }
        let dir = std::env::temp_dir().join(format!("kat-inproc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let api = dir.join("api.sock");
        let args: Vec<String> = [
            "-services",
            &format!("unix://{}", api.display()),
            "-ssh-port",
            "-1",
            "-mtu",
            "1500",
            "-log-file",
            &dir.join("gvproxy.log").display().to_string(),
        ]
        .map(String::from)
        .into();
        let (net, starting) = start(&library, &args).unwrap();
        starting.wait().unwrap();
        assert!(api.exists(), "no services API socket");

        let guest_mac: [u8; 6] = [0x5a, 0x94, 0xef, 0xe4, 0x0c, 0xee];
        let mut arp: Vec<u8> = Vec::new();
        arp.extend_from_slice(&[0xff; 6]);
        arp.extend_from_slice(&guest_mac);
        arp.extend_from_slice(&[0x08, 0x06, 0, 1, 0x08, 0, 6, 4, 0, 1]);
        arp.extend_from_slice(&guest_mac);
        arp.extend_from_slice(&[192, 168, 127, 2]);
        arp.extend_from_slice(&[0; 6]);
        arp.extend_from_slice(&[192, 168, 127, 1]);
        let iov = [libc::iovec {
            iov_base: arp.as_mut_ptr() as *mut c_void,
            iov_len: arp.len(),
        }];
        assert_eq!(net.transmit(&iov), 0);
        let deadline = Instant::now() + Duration::from_secs(5);
        while net.rx_dropped() == 0 {
            assert!(Instant::now() < deadline, "no reply from gVisor");
            std::thread::sleep(Duration::from_millis(1));
        }

        let again = start(&library, &args)
            .unwrap()
            .1
            .wait()
            .unwrap_err()
            .to_string();
        assert!(again.contains("did not start"), "{again}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
