//! Thin wrappers over the syscalls PID 1 needs.

use std::ffi::CString;
use std::fs;
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

pub fn mount(
    src: &str,
    target: &str,
    fstype: &str,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> io::Result<()> {
    mkdir(target)?;
    let src = CString::new(src)?;
    let target_c = CString::new(target)?;
    let fstype = CString::new(fstype)?;
    let data = data.map(CString::new).transpose()?;
    let rc = unsafe {
        libc::mount(
            src.as_ptr(),
            target_c.as_ptr(),
            fstype.as_ptr(),
            flags,
            data.as_ref()
                .map_or(std::ptr::null(), |d| d.as_ptr().cast()),
        )
    };
    if rc < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(e.kind(), format!("mount {target}: {e}")));
    }
    Ok(())
}

/// Move an existing mount to `target`.
pub fn move_mount(src: &str, target: &str) -> io::Result<()> {
    mkdir(target)?;
    let src_c = CString::new(src)?;
    let target_c = CString::new(target)?;
    let rc = unsafe {
        libc::mount(
            src_c.as_ptr(),
            target_c.as_ptr(),
            std::ptr::null(),
            libc::MS_MOVE,
            std::ptr::null(),
        )
    };
    if rc < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(
            e.kind(),
            format!("move {src} -> {target}: {e}"),
        ));
    }
    Ok(())
}

pub fn umount(target: &str) -> io::Result<()> {
    let c = CString::new(target)?;
    if unsafe { libc::umount2(c.as_ptr(), 0) } < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(e.kind(), format!("umount {target}: {e}")));
    }
    Ok(())
}

/// Make `new_root` the root of the filesystem, as `switch_root` does: move it
/// over `/` and chroot into it. The initramfs stays underneath, unreachable.
/// Unlike pivot_root this works from an initramfs, and because the new root
/// is a real mount, containers can still pivot_root inside it.
pub fn switch_root(new_root: &str) -> io::Result<()> {
    let root = CString::new(new_root)?;
    let dot = CString::new(".")?;
    let slash = CString::new("/")?;
    unsafe {
        if libc::chdir(root.as_ptr()) < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::mount(
            dot.as_ptr(),
            slash.as_ptr(),
            std::ptr::null(),
            libc::MS_MOVE,
            std::ptr::null(),
        ) < 0
        {
            return Err(io::Error::last_os_error());
        }
        if libc::chroot(dot.as_ptr()) < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::chdir(slash.as_ptr()) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn mkdir(path: &str) -> io::Result<()> {
    match fs::create_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(io::Error::new(e.kind(), format!("mkdir {path}: {e}"))),
    }
}

pub fn exists(path: &str) -> bool {
    Path::new(path).exists()
}

pub fn sethostname(name: &str) -> io::Result<()> {
    if unsafe { libc::sethostname(name.as_ptr().cast(), name.len()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Bring a network interface up (`ip link set <name> up`).
pub fn link_up(name: &str) -> io::Result<()> {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = OwnedFd::from_raw_fd(fd);
        let mut req: libc::ifreq = mem::zeroed();
        for (dst, src) in req.ifr_name.iter_mut().zip(name.bytes()) {
            *dst = src as libc::c_char;
        }
        if libc::ioctl(fd.as_raw_fd(), libc::SIOCGIFFLAGS as _, &mut req) < 0 {
            return Err(io::Error::last_os_error());
        }
        req.ifr_ifru.ifru_flags |= libc::IFF_UP as libc::c_short;
        if libc::ioctl(fd.as_raw_fd(), libc::SIOCSIFFLAGS as _, &req) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn set_realtime(unix_ns: u64) -> io::Result<()> {
    let ts = libc::timespec {
        tv_sec: (unix_ns / 1_000_000_000) as _,
        tv_nsec: (unix_ns % 1_000_000_000) as _,
    };
    if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub fn boottime_ns() -> u64 {
    let mut ts: libc::timespec = unsafe { mem::zeroed() };
    unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

pub fn kernel_release() -> String {
    let mut u: libc::utsname = unsafe { mem::zeroed() };
    if unsafe { libc::uname(&mut u) } < 0 {
        return String::new();
    }
    unsafe { std::ffi::CStr::from_ptr(u.release.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

pub fn vsock_connect(cid: u32, port: u32) -> io::Result<OwnedFd> {
    let fd = vsock_socket()?;
    let addr = vsock_addr(cid, port);
    let rc = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            &addr as *const libc::sockaddr_vm as *const libc::sockaddr,
            mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

pub fn vsock_listen(port: u32) -> io::Result<OwnedFd> {
    let fd = vsock_socket()?;
    let addr = vsock_addr(libc::VMADDR_CID_ANY, port);
    unsafe {
        if libc::bind(
            fd.as_raw_fd(),
            &addr as *const libc::sockaddr_vm as *const libc::sockaddr,
            mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        ) < 0
        {
            return Err(io::Error::last_os_error());
        }
        if libc::listen(fd.as_raw_fd(), 128) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(fd)
}

pub fn accept(listener: &OwnedFd) -> io::Result<OwnedFd> {
    loop {
        let fd = unsafe {
            libc::accept4(
                listener.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_CLOEXEC,
            )
        };
        if fd >= 0 {
            return Ok(unsafe { OwnedFd::from_raw_fd(fd) });
        }
        let e = io::Error::last_os_error();
        if matches!(
            e.kind(),
            io::ErrorKind::Interrupted
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::ConnectionAborted
        ) || e.raw_os_error() == Some(libc::EPROTO)
        {
            continue;
        }
        return Err(e);
    }
}

/// Accept a vsock connection with `SOCK_CLOEXEC`, returning the socket,
/// the peer `sockaddr_vm`, and the address length populated by the kernel.
pub fn vsock_accept(
    listener: &OwnedFd,
) -> io::Result<(OwnedFd, libc::sockaddr_vm, libc::socklen_t)> {
    loop {
        let mut addr: libc::sockaddr_vm = unsafe { mem::zeroed() };
        let mut len = mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t;
        let fd = unsafe {
            libc::accept4(
                listener.as_raw_fd(),
                (&mut addr as *mut libc::sockaddr_vm).cast::<libc::sockaddr>(),
                &mut len,
                libc::SOCK_CLOEXEC,
            )
        };
        if fd >= 0 {
            return Ok((unsafe { OwnedFd::from_raw_fd(fd) }, addr, len));
        }
        let e = io::Error::last_os_error();
        if matches!(
            e.kind(),
            io::ErrorKind::Interrupted
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::ConnectionAborted
        ) || e.raw_os_error() == Some(libc::EPROTO)
        {
            continue;
        }
        return Err(e);
    }
}

pub fn pidfd_open(pid: i32) -> io::Result<OwnedFd> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0u32) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
}

pub fn pidfd_send_signal(pidfd: &OwnedFd, sig: i32) -> io::Result<()> {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            sig,
            std::ptr::null::<libc::siginfo_t>(),
            0u32,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Whether the process referred to by `pidfd` has not yet exited.
pub fn pidfd_alive(pidfd: &OwnedFd) -> bool {
    let mut pfd = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
        if rc < 0 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return false;
        }
        return rc == 0;
    }
}

fn vsock_socket() -> io::Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn vsock_addr(cid: u32, port: u32) -> libc::sockaddr_vm {
    let mut addr: libc::sockaddr_vm = unsafe { mem::zeroed() };
    addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
    addr.svm_cid = cid;
    addr.svm_port = port;
    addr
}

pub fn power_off() -> ! {
    unsafe {
        libc::sync();
        libc::reboot(libc::RB_POWER_OFF);
    }
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
