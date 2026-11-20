//! `Exec` and `Connect` on the machine port: a process in a machine (through
//! lxc-attach and a login `su`) or a TCP connection to one, relayed over the
//! vsock stream as `Data`/`Stdout`/`Stderr` frames.

use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddrV4, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use kat_proto::machine::{self, CHUNK, ExecSpec, MachineMsg, Tty};

use super::super::service::Reaper;
use super::{cmd, lxc};

type Writer = Arc<Mutex<UnixStream>>;

fn send(w: &Writer, m: &MachineMsg) -> io::Result<()> {
    machine::write(&mut *w.lock().unwrap(), m)
}

fn winsize(rows: u16, cols: u16) -> libc::winsize {
    libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
}

fn openpty(t: &Tty) -> io::Result<(OwnedFd, OwnedFd)> {
    let (mut master, mut slave) = (0, 0);
    let ws = winsize(t.rows.max(1), t.cols.max(1));
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null(),
            &ws,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    unsafe {
        libc::fcntl(master, libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(slave, libc::F_SETFD, libc::FD_CLOEXEC);
        Ok((OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)))
    }
}

/// Copy `from` into `kind` frames until EOF (or EIO: a pty whose last
/// slave closed).
fn pump(mut from: File, w: Writer, kind: fn(Vec<u8>) -> MachineMsg) {
    let mut buf = vec![0u8; CHUNK];
    loop {
        match from.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                if send(&w, &kind(buf[..n].to_vec())).is_err() {
                    return;
                }
            }
        }
    }
}

/// Run `spec` (already resolved to a machine and user) and relay it until
/// it exits.
pub fn exec(reaper: &Reaper, conn: UnixStream, spec: &ExecSpec, user: &str) -> io::Result<()> {
    let script = lxc::script(&spec.cwd, &spec.env, &spec.argv);
    let term = spec
        .tty
        .as_ref()
        .map(|t| t.term.as_str())
        .filter(|t| !t.is_empty());
    let args = lxc::attach_args(&spec.machine, user, term, &script);
    let mut c = Command::new(&args[0]);
    c.args(&args[1..]).env_clear().env("PATH", cmd::PATH);
    if let Some(t) = term {
        c.env("TERM", t);
    }
    let w: Writer = Arc::new(Mutex::new(conn.try_clone()?));
    let mut reader = conn;

    let mut outputs = Vec::new();
    let (pid, exited, stdin, master) = match &spec.tty {
        Some(t) => {
            let (master, slave) = openpty(t)?;
            c.stdin(Stdio::from(slave.try_clone()?))
                .stdout(Stdio::from(slave.try_clone()?))
                .stderr(Stdio::from(slave));
            unsafe {
                c.pre_exec(|| {
                    if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let (pid, exited) = reaper.spawn(&mut c)?;
            drop(c);
            let out = File::from(master.try_clone()?);
            let w2 = w.clone();
            outputs.push(thread::spawn(move || pump(out, w2, MachineMsg::Stdout)));
            (
                pid,
                exited,
                Some(File::from(master.try_clone()?)),
                Some(master),
            )
        }
        None => {
            let (in_r, in_w) = cmd::pipe()?;
            let (out_r, out_w) = cmd::pipe()?;
            let (err_r, err_w) = cmd::pipe()?;
            c.stdin(Stdio::from(in_r))
                .stdout(Stdio::from(out_w))
                .stderr(Stdio::from(err_w));
            let (pid, exited) = reaper.spawn(&mut c)?;
            drop(c);
            for (fd, kind) in [
                (out_r, MachineMsg::Stdout as fn(Vec<u8>) -> MachineMsg),
                (err_r, MachineMsg::Stderr),
            ] {
                let w2 = w.clone();
                outputs.push(thread::spawn(move || pump(File::from(fd), w2, kind)));
            }
            (pid, exited, Some(File::from(in_w)), None)
        }
    };

    // Input from the host until it hangs up; the process may outlive it.
    let tty = spec.tty.is_some();
    thread::spawn(move || {
        let mut stdin = stdin;
        while let Ok(Some(m)) = machine::read(&mut reader) {
            match m {
                MachineMsg::Data(d) => {
                    if let Some(s) = stdin.as_mut()
                        && s.write_all(&d).is_err()
                    {
                        stdin = None;
                    }
                }
                // A pty has no EOF of its own; the line discipline's ^D
                // is the closest.
                MachineMsg::DataEnd if tty => {
                    if let Some(s) = stdin.as_mut() {
                        let _ = s.write_all(&[4]);
                    }
                }
                MachineMsg::DataEnd => stdin = None,
                MachineMsg::Resize { rows, cols } => {
                    if let Some(m) = &master {
                        let ws = winsize(rows, cols);
                        unsafe { libc::ioctl(m.as_raw_fd(), libc::TIOCSWINSZ, &ws) };
                    }
                }
                MachineMsg::Signal { signal } => unsafe {
                    libc::kill(pid, signal);
                },
                _ => {}
            }
        }
        // The client went away: hang up the session like a closed terminal.
        unsafe { libc::kill(pid, libc::SIGHUP) };
    });

    let status = exited.recv().unwrap_or(-1);
    // A pty's reader ends when the last slave closes, which background
    // processes in the machine can delay; don't wait for it forever.
    for o in outputs {
        for _ in 0..200 {
            if o.is_finished() {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
    let code = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        128 + libc::WTERMSIG(status)
    };
    send(&w, &MachineMsg::Exit { code })
}

/// Dial `127.0.0.1:port` inside the network namespace of `pid` (the
/// machine's init), so services bound to the machine's loopback are
/// reachable too. The socket keeps the namespace it was made in; the
/// short-lived thread that switched namespaces then exits.
fn dial_in_netns(pid: i32, port: u16) -> io::Result<TcpStream> {
    let ns = File::open(format!("/proc/{pid}/ns/net"))?;
    thread::spawn(move || {
        if unsafe { libc::setns(ns.as_raw_fd(), libc::CLONE_NEWNET) } < 0 {
            return Err(io::Error::last_os_error());
        }
        TcpStream::connect_timeout(
            &SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into(),
            Duration::from_secs(5),
        )
    })
    .join()
    .unwrap_or_else(|_| Err(io::Error::other("dial thread panicked")))
}

/// Relay a TCP connection to `port` in a machine: on its loopback, or on
/// its address when nothing listens there.
pub fn connect(conn: UnixStream, pid: i32, ip: Ipv4Addr, port: u16) -> io::Result<()> {
    let tcp = match dial_in_netns(pid, port).or_else(|_| {
        TcpStream::connect_timeout(&SocketAddrV4::new(ip, port).into(), Duration::from_secs(5))
    }) {
        Ok(t) => t,
        Err(e) => {
            let mut c = conn;
            return machine::write(
                &mut c,
                &MachineMsg::Error(format!("connect {ip}:{port}: {e}")),
            );
        }
    };
    let w: Writer = Arc::new(Mutex::new(conn.try_clone()?));
    send(&w, &MachineMsg::Done)?;
    let mut tcp_w = tcp.try_clone()?;
    let mut reader = conn;
    let up = thread::spawn(move || {
        while let Ok(Some(m)) = machine::read(&mut reader) {
            match m {
                MachineMsg::Data(d) => {
                    if tcp_w.write_all(&d).is_err() {
                        break;
                    }
                }
                MachineMsg::DataEnd => break,
                _ => {}
            }
        }
        let _ = tcp_w.shutdown(Shutdown::Write);
    });
    let mut tcp_r = tcp;
    let mut buf = vec![0u8; CHUNK];
    loop {
        match tcp_r.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if send(&w, &MachineMsg::Data(buf[..n].to_vec())).is_err() {
                    break;
                }
            }
        }
    }
    let _ = send(&w, &MachineMsg::DataEnd);
    let _ = up.join();
    Ok(())
}
