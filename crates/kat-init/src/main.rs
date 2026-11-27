//! Guest PID 1. Mounts the basic filesystems, dials the host over vsock,
//! announces itself with `Hello` and serves the control channel. If the host
//! connection goes away the VM powers off, so a crashed host never leaves an
//! orphan VM.

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod autofwd;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod containerd_grpc;
#[cfg(target_os = "linux")]
mod linux;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod nofile;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod oci_rewrite;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod unix_connect;

#[cfg(target_os = "linux")]
fn main() {
    if std::process::id() != 1 {
        match std::env::args().nth(1).as_deref() {
            Some("nfsd") => linux::nfs::main(),
            Some("vm-exec-probe") => linux::vm_exec::probe_main(),
            _ => {}
        }
    }
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("kat-init only runs inside the Linux guest");
    std::process::exit(1);
}
