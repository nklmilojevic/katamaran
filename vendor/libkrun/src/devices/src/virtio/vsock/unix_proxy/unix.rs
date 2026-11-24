use std::collections::VecDeque;
use std::fs;
use std::net::{Ipv4Addr, TcpListener};
use std::num::Wrapping;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;

use nix::errno::Errno;
use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::sys::socket::{
    AddressFamily, Backlog, MsgFlags, Shutdown, SockFlag, SockType, UnixAddr, accept, bind,
    connect, listen, send, shutdown, socket,
};

#[cfg(target_os = "macos")]
use super::super::super::linux_errno::linux_errno_raw;
use super::super::kernel_peer;
use super::super::muxer::{MuxerRx, push_packet};
use super::super::packet::{TsiConnectReq, VsockPacket};
use super::super::proxy::{NewProxyType, ProxyError, ProxyRemoval, ProxyStatus, ProxyUpdate};
use utils::epoll::EventSet;

pub type PlatformHandle = OwnedFd;

fn set_nonblock(fd: &OwnedFd, id: u64) {
    match fcntl(fd, FcntlArg::F_GETFL) {
        Ok(flags) => match OFlag::from_bits(flags) {
            Some(flags) => {
                if let Err(e) = fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)) {
                    warn!("error switching to non-blocking: id={id}, err={e}");
                }
            }
            None => error!("invalid fd flags id={id}"),
        },
        Err(e) => error!("couldn't obtain fd flags id={id}, err={e}"),
    }
}

fn set_nosigpipe(fd: &OwnedFd) {
    #[cfg(target_os = "macos")]
    {
        // nix doesn't provide an abstraction for SO_NOSIGPIPE, fall back to libc.
        let option_value: libc::c_int = 1;
        unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                &option_value as *const _ as *const libc::c_void,
                std::mem::size_of_val(&option_value) as libc::socklen_t,
            )
        };
    }
    #[cfg(not(target_os = "macos"))]
    let _ = fd;
}

pub(crate) fn create_socket(id: u64) -> Result<PlatformHandle, ProxyError> {
    let fd = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::empty(),
        None,
    )
    .map_err(|e| ProxyError::CreatingSocket(std::io::Error::from_raw_os_error(e as i32)))?;

    // macOS forces us to do this here instead of just using SockFlag::SOCK_NONBLOCK above.
    set_nonblock(&fd, id);
    set_nosigpipe(&fd);

    Ok(fd)
}

pub(crate) fn switch_to_connected(proxy: &mut super::UnixProxy) {
    proxy.status = ProxyStatus::Connected;
}

fn push_connect_rsp(proxy: &super::UnixProxy, result: i32) {
    debug!(
        "push_connect_rsp: id: {}, control_port: {}, result: {}",
        proxy.id, proxy.control_port, result
    );

    // This response goes to the control port (DGRAM).
    let rx = MuxerRx::ConnResponse {
        local_port: 1025,
        peer_port: proxy.control_port,
        result,
    };
    push_packet(proxy.cid, rx, &proxy.rxq, &proxy.queue, &proxy.mem);
}

pub(crate) fn do_connect(
    proxy: &mut super::UnixProxy,
    _pkt: &VsockPacket,
    _req: TsiConnectReq,
) -> ProxyUpdate {
    let mut update = ProxyUpdate::default();

    let addr = UnixAddr::new(&proxy.path).unwrap();

    let result = match connect(proxy.fd.as_raw_fd(), &addr) {
        Ok(()) => {
            debug!("connect: Connected");
            switch_to_connected(proxy);
            0
        }
        Err(nix::errno::Errno::EINPROGRESS) => {
            debug!("connect: Connecting");
            proxy.status = ProxyStatus::Connecting;
            0
        }
        Err(e) => {
            debug!("Error connecting: {e}");
            #[cfg(target_os = "macos")]
            let errno = -linux_errno_raw(Errno::last_raw());
            #[cfg(target_os = "linux")]
            let errno = -Errno::last_raw();
            errno
        }
    };

    if proxy.status == ProxyStatus::Connecting {
        update.polling = Some((proxy.id, proxy.fd.as_raw_fd(), proxy.poll_interest()));
    } else {
        if proxy.status == ProxyStatus::Connected {
            update.polling = Some((proxy.id, proxy.fd.as_raw_fd(), proxy.poll_interest()));
        }
        push_connect_rsp(proxy, result);
    }
    update
}

pub(crate) fn confirm_connect(
    proxy: &mut super::UnixProxy,
    pkt: &VsockPacket,
) -> Option<ProxyUpdate> {
    debug!(
        "confirm_connect: local_port={} peer_port={}, src_port={}, dst_port={}",
        pkt.dst_port(),
        pkt.src_port(),
        proxy.local_port,
        proxy.peer_port,
    );

    proxy.peer_buf_alloc = pkt.buf_alloc();
    proxy.peer_fwd_cnt = Wrapping(pkt.fwd_cnt());

    proxy.local_port = pkt.dst_port();
    proxy.peer_port = pkt.src_port();

    // This response goes to the connection.
    let rx = MuxerRx::OpResponse {
        local_port: pkt.dst_port(),
        peer_port: pkt.src_port(),
    };
    push_packet(proxy.cid, rx, &proxy.rxq, &proxy.queue, &proxy.mem);

    None
}

fn send_nonblock(fd: RawFd, data: &[u8]) -> Result<usize, Errno> {
    #[cfg(target_os = "macos")]
    let flags = MsgFlags::MSG_DONTWAIT;

    #[cfg(target_os = "linux")]
    let flags = MsgFlags::MSG_DONTWAIT | MsgFlags::MSG_NOSIGNAL;

    loop {
        match send(fd, data, flags) {
            Ok(sent) => return Ok(sent),
            Err(Errno::EINTR) => continue,
            Err(err) => return Err(err),
        }
    }
}

fn maybe_send_credit_update(
    proxy: &mut super::UnixProxy,
    force_if_changed: bool,
    update: &mut ProxyUpdate,
) {
    let delta = (proxy.tx_cnt - proxy.last_tx_cnt_sent).0;
    if delta == 0 {
        return;
    }
    let threshold = (proxy.peer_buf_alloc / 2)
        .clamp(64 * 1024, (super::defs::CONN_TX_BUF_SIZE / 4) as u32);
    if force_if_changed || delta >= threshold {
        debug!(
            "sending credit update: id={}, tx_cnt={}, last_tx_cnt={}",
            proxy.id, proxy.tx_cnt, proxy.last_tx_cnt_sent
        );
        proxy.push_credit_update();
        update.signal_queue = true;
    }
}

fn on_host_send_error(proxy: &mut super::UnixProxy, update: &mut ProxyUpdate) {
    proxy.tx_buf.clear();
    proxy.tx_buf.shrink_to_fit();
    proxy.pending_peer_shutdown_send = false;
    if !proxy.local_recv_shutdown {
        proxy.local_recv_shutdown = true;
        proxy.push_shutdown_recv();
        update.signal_queue = true;
    }
    if proxy.is_fully_shutdown() && proxy.status != ProxyStatus::Closed {
        proxy.push_reset();
        proxy.status = ProxyStatus::Closed;
        update.signal_queue = true;
        update.remove_proxy = ProxyRemoval::Deferred;
    }
}

fn flush_tx(proxy: &mut super::UnixProxy, update: &mut ProxyUpdate) {
    let mut sent_any = false;
    while !proxy.tx_buf.is_empty() {
        let (head, _) = proxy.tx_buf.as_slices();
        match send_nonblock(proxy.fd.as_raw_fd(), head) {
            Ok(0) => break,
            Ok(sent) => {
                proxy.tx_buf.drain(..sent);
                proxy.tx_cnt += Wrapping(sent as u32);
                sent_any = true;
            }
            Err(Errno::EAGAIN) => break,
            Err(err) => {
                debug!("flush_tx: host send error id={}, err={err}", proxy.id);
                on_host_send_error(proxy, update);
                return;
            }
        }
    }

    let drained = proxy.tx_buf.is_empty();
    if drained && proxy.tx_buf.capacity() > 64 * 1024 {
        proxy.tx_buf = VecDeque::new();
    }

    if sent_any {
        // When the buffer finishes draining, always report the updated `tx_cnt`
        // so a guest that hit zero credit wakes immediately.
        maybe_send_credit_update(proxy, drained, update);
    }

    if drained && proxy.pending_peer_shutdown_send {
        proxy.pending_peer_shutdown_send = false;
        if let Err(e) = shutdown(proxy.fd.as_raw_fd(), Shutdown::Write) {
            debug!("deferred shutdown(Write) failed: id={}, err={e}", proxy.id);
        }
        if proxy.is_fully_shutdown() && proxy.status != ProxyStatus::Closed {
            proxy.push_reset();
            proxy.status = ProxyStatus::Closed;
            update.signal_queue = true;
            update.remove_proxy = ProxyRemoval::Deferred;
        }
    }
}

pub(crate) fn sendmsg(proxy: &mut super::UnixProxy, pkt: &VsockPacket) -> ProxyUpdate {
    let mut update = ProxyUpdate::default();
    if proxy.local_recv_shutdown || proxy.status == ProxyStatus::Closed {
        return update;
    }

    let Some(buf) = pkt.buf() else {
        return update;
    };
    let pkt_len = (pkt.len() as usize).min(buf.len());
    if pkt_len == 0 {
        return update;
    }
    let data = &buf[..pkt_len];

    if proxy.status == ProxyStatus::Connecting {
        proxy.tx_buf.extend(data);
        return update;
    }

    let prev_interest = proxy.poll_interest();

    if proxy.tx_buf.is_empty() {
        match send_nonblock(proxy.fd.as_raw_fd(), data) {
            Ok(sent) => {
                proxy.tx_cnt += Wrapping(sent as u32);
                if sent < data.len() {
                    proxy.tx_buf.extend(&data[sent..]);
                }
                maybe_send_credit_update(proxy, false, &mut update);
            }
            Err(Errno::EAGAIN) => {
                proxy.tx_buf.extend(data);
            }
            Err(err) => {
                debug!("sendmsg: host send error id={}, err={err}", proxy.id);
                on_host_send_error(proxy, &mut update);
            }
        }
    } else {
        proxy.tx_buf.extend(data);
    }

    let next_interest = proxy.poll_interest();
    if next_interest != prev_interest {
        update.polling = Some((proxy.id, proxy.fd.as_raw_fd(), next_interest));
    }

    update
}

pub(crate) fn update_peer_credit(proxy: &mut super::UnixProxy, pkt: &VsockPacket) -> ProxyUpdate {
    debug!(
        "update_credit: buf_alloc={} rx_cnt={} fwd_cnt={}",
        pkt.buf_alloc(),
        proxy.rx_cnt,
        pkt.fwd_cnt()
    );
    proxy.peer_buf_alloc = pkt.buf_alloc();
    proxy.peer_fwd_cnt = Wrapping(pkt.fwd_cnt());

    if proxy.status != ProxyStatus::WaitingCreditUpdate || proxy.peer_avail_credit() == 0 {
        return ProxyUpdate::default();
    }

    proxy.status = ProxyStatus::Connected;

    ProxyUpdate {
        polling: Some((proxy.id, proxy.fd.as_raw_fd(), proxy.poll_interest())),
        ..Default::default()
    }
}

pub(crate) fn push_op_request(proxy: &super::UnixProxy) {
    debug!(
        "push_op_request: id={}, local_port={} peer_port={}",
        proxy.id, proxy.local_port, proxy.peer_port
    );

    // This packet goes to the connection.
    let rx = MuxerRx::OpRequest {
        local_port: proxy.local_port,
        peer_port: proxy.peer_port,
    };
    push_packet(proxy.cid, rx, &proxy.rxq, &proxy.queue, &proxy.mem);
}

pub(crate) fn process_op_response(proxy: &mut super::UnixProxy, pkt: &VsockPacket) -> ProxyUpdate {
    debug!(
        "process_op_response: id={} src_port={} dst_port={}",
        proxy.id,
        pkt.src_port(),
        pkt.dst_port()
    );

    proxy.peer_buf_alloc = pkt.buf_alloc();
    proxy.peer_fwd_cnt = Wrapping(pkt.fwd_cnt());

    switch_to_connected(proxy);

    ProxyUpdate {
        polling: Some((proxy.id, proxy.fd.as_raw_fd(), proxy.poll_interest())),
        ..Default::default()
    }
}

pub(crate) fn do_shutdown_dirs(proxy: &mut super::UnixProxy, recv_off: bool, send_off: bool) {
    let how = match (recv_off, send_off) {
        (true, true) => Shutdown::Both,
        (true, false) => Shutdown::Read,
        (false, true) => Shutdown::Write,
        (false, false) => return,
    };

    if let Err(e) = shutdown(proxy.fd.as_raw_fd(), how) {
        warn!("error sending shutdown to socket: {e}");
    }
}

pub(crate) fn release(proxy: &mut super::UnixProxy) -> ProxyUpdate {
    debug!(
        "release: id={}, tx_cnt={}, last_tx_cnt={}",
        proxy.id, proxy.tx_cnt, proxy.last_tx_cnt_sent
    );

    let prev_interest = proxy.poll_interest();
    // A connection that never reached Connected carries no data and is not
    // registered for polling yet, so there is nothing to reserve its id for.
    // Keeping it would leave the accepted host socket open, and its peer
    // blocked, until the reaper reclaims the proxy.
    let remove_proxy = match proxy.status {
        ProxyStatus::ReverseInit | ProxyStatus::Connecting => ProxyRemoval::Immediate,
        _ => ProxyRemoval::Deferred,
    };

    proxy.tx_buf.clear();
    proxy.tx_buf.shrink_to_fit();
    proxy.pending_peer_shutdown_send = false;
    proxy.status = ProxyStatus::Closed;

    ProxyUpdate {
        remove_proxy,
        polling: (!prev_interest.is_empty()).then_some((
            proxy.id,
            proxy.fd.as_raw_fd(),
            EventSet::empty(),
        )),
        ..Default::default()
    }
}

pub(crate) fn process_event(proxy: &mut super::UnixProxy, evset: EventSet) -> ProxyUpdate {
    let mut update = ProxyUpdate::default();
    let prev_interest = proxy.poll_interest();

    // A hung-up stream can still contain data, including more than the guest
    // currently has buffers or credit for. Only recv() can establish EOF.
    if evset.contains(EventSet::HANG_UP)
        && !matches!(
            proxy.status,
            ProxyStatus::Connected | ProxyStatus::WaitingCreditUpdate
        )
    {
        debug!("process_event: HANG_UP");

        if proxy.status == ProxyStatus::Connecting {
            push_connect_rsp(proxy, -libc::ECONNREFUSED);
        } else {
            proxy.push_reset();
        }

        proxy.tx_buf.clear();
        proxy.tx_buf.shrink_to_fit();
        proxy.pending_peer_shutdown_send = false;
        proxy.status = ProxyStatus::Closed;
        update.polling = Some((proxy.id, proxy.fd.as_raw_fd(), EventSet::empty()));
        update.signal_queue = true;
        update.remove_proxy = ProxyRemoval::Deferred;

        return update;
    }

    if evset.contains(EventSet::OUT) {
        debug!("process_event: OUT");
        if proxy.status == ProxyStatus::Connecting {
            switch_to_connected(proxy);
            push_connect_rsp(proxy, 0);
            update.signal_queue = true;
            if !proxy.tx_buf.is_empty() {
                flush_tx(proxy, &mut update);
            }
        } else if matches!(
            proxy.status,
            ProxyStatus::Connected | ProxyStatus::WaitingCreditUpdate
        ) {
            flush_tx(proxy, &mut update);
        } else {
            debug!("EventSet::OUT while in status {:?}", proxy.status);
        }

        if proxy.status == ProxyStatus::Closed {
            update.polling = Some((proxy.id, proxy.fd.as_raw_fd(), EventSet::empty()));
            return update;
        }
    }

    if evset.intersects(EventSet::IN | EventSet::HANG_UP) {
        debug!("process_event: IN");
        if proxy.status == ProxyStatus::Connected && !proxy.local_shutdown {
            let was_shut = proxy.local_shutdown;
            let (signal_queue, wait_credit) = proxy.recv_pkt();
            update.signal_queue |= signal_queue;

            // The host end half-closed: tell the guest it will get no more
            // data, stop reading, and keep the guest-to-host direction open
            // until the guest is done too.
            if !was_shut && proxy.local_shutdown {
                debug!("process_event: host EOF, sending shutdown: id={}", proxy.id);
                proxy.push_shutdown_send();
                update.signal_queue = true;
                if proxy.is_fully_shutdown() {
                    proxy.push_reset();
                    proxy.status = ProxyStatus::Closed;
                    update.remove_proxy = ProxyRemoval::Deferred;
                }
            } else {
                if wait_credit && proxy.status != ProxyStatus::WaitingCreditUpdate {
                    proxy.status = ProxyStatus::WaitingCreditUpdate;
                    let rx = MuxerRx::CreditRequest {
                        local_port: proxy.local_port,
                        peer_port: proxy.peer_port,
                        fwd_cnt: proxy.tx_cnt.0,
                    };
                    update.push_credit_req = Some(rx);
                }

                if proxy.status == ProxyStatus::Closed {
                    debug!(
                        "process_event: endpoint closed, sending reset: id={}",
                        proxy.id
                    );

                    proxy.push_reset();
                    update.signal_queue = true;
                    update.remove_proxy = ProxyRemoval::Deferred;
                }
            }
        } else {
            debug!("EventSet::IN while not connected: {:?}", proxy.status);
        }
    }

    let next_interest = proxy.poll_interest();
    if next_interest != prev_interest {
        update.polling = Some((proxy.id, proxy.fd.as_raw_fd(), next_interest));
    }

    update
}

pub(crate) fn as_raw_fd(proxy: &super::UnixProxy) -> RawFd {
    proxy.fd.as_raw_fd()
}

pub(crate) fn new_acceptor_proxy(
    id: u64,
    path: &Path,
    peer_port: u32,
) -> Result<super::UnixAcceptorProxy, ProxyError> {
    if kernel_peer::is_loopback_tcp(path) {
        return new_loopback_tcp_acceptor(id, path, peer_port);
    }
    let fd = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::empty(),
        None,
    )
    .map_err(|e| ProxyError::CreatingSocket(std::io::Error::from_raw_os_error(e as i32)))?;
    let addr = UnixAddr::new(path)
        .map_err(|e| ProxyError::CreatingSocket(std::io::Error::from_raw_os_error(e as i32)))?;
    let bound = bind(fd.as_raw_fd(), &addr);
    let bound = if bound == Err(Errno::EADDRINUSE) && remove_stale_socket(path) {
        bind(fd.as_raw_fd(), &addr)
    } else {
        bound
    };
    bound.map_err(|e| ProxyError::CreatingSocket(std::io::Error::from_raw_os_error(e as i32)))?;
    let metadata = fs::symlink_metadata(path).map_err(ProxyError::CreatingSocket)?;
    let proxy = super::UnixAcceptorProxy {
        id,
        fd,
        peer_port,
        path: path.to_path_buf(),
        identity: (metadata.dev(), metadata.ino()),
    };
    listen(&proxy.fd, Backlog::MAXCONN)
        .map_err(|e| ProxyError::CreatingSocket(std::io::Error::from_raw_os_error(e as i32)))?;
    Ok(proxy)
}

/// A listener on a free TCP port of 127.0.0.1, whose number is written to
/// `path` (through a rename, so a reader never sees half of it).
fn new_loopback_tcp_acceptor(
    id: u64,
    path: &Path,
    peer_port: u32,
) -> Result<super::UnixAcceptorProxy, ProxyError> {
    let listener =
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).map_err(ProxyError::CreatingSocket)?;
    let port = listener
        .local_addr()
        .map_err(ProxyError::CreatingSocket)?
        .port();
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, format!("{port}\n"))
        .and_then(|()| fs::rename(&tmp, path))
        .map_err(ProxyError::CreatingSocket)?;
    let metadata = fs::symlink_metadata(path).map_err(ProxyError::CreatingSocket)?;
    Ok(super::UnixAcceptorProxy {
        id,
        fd: OwnedFd::from(listener),
        peer_port,
        path: path.to_path_buf(),
        identity: (metadata.dev(), metadata.ino()),
    })
}

fn set_nodelay(fd: &OwnedFd) {
    let on: libc::c_int = 1;
    // SAFETY: plain setsockopt with an int value on a socket we own.
    let ret = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &on as *const _ as *const libc::c_void,
            std::mem::size_of_val(&on) as libc::socklen_t,
        )
    };
    if ret != 0 {
        warn!("setting TCP_NODELAY: {}", std::io::Error::last_os_error());
    }
}

fn remove_stale_socket(path: &Path) -> bool {
    let Ok(original) = fs::symlink_metadata(path) else {
        return false;
    };
    if !original.file_type().is_socket() {
        return false;
    }
    let Ok(probe) = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::empty(),
        None,
    ) else {
        return false;
    };
    let Ok(flags) = fcntl(&probe, FcntlArg::F_GETFL) else {
        return false;
    };
    let Some(flags) = OFlag::from_bits(flags) else {
        return false;
    };
    if fcntl(&probe, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).is_err() {
        return false;
    }
    let Ok(addr) = UnixAddr::new(path) else {
        return false;
    };
    if connect(probe.as_raw_fd(), &addr) != Err(Errno::ECONNREFUSED) {
        return false;
    }
    let Ok(current) = fs::symlink_metadata(path) else {
        return false;
    };
    original.dev() == current.dev()
        && original.ino() == current.ino()
        && fs::remove_file(path).is_ok()
}

impl Drop for super::UnixAcceptorProxy {
    fn drop(&mut self) {
        if let Ok(current) = fs::symlink_metadata(&self.path)
            && current.dev() == self.identity.0
            && current.ino() == self.identity.1
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) fn process_acceptor_event(
    proxy: &mut super::UnixAcceptorProxy,
    evset: EventSet,
) -> ProxyUpdate {
    let mut update = ProxyUpdate::default();

    if evset.contains(EventSet::HANG_UP) {
        debug!("process_event: HANG_UP");
        update.polling = Some((proxy.id, proxy.fd.as_raw_fd(), EventSet::empty()));
        update.signal_queue = true;
        update.remove_proxy = ProxyRemoval::Deferred;
        return update;
    }
    if evset.contains(EventSet::IN) {
        match accept(proxy.fd.as_raw_fd()) {
            Ok(accept_fd) if !kernel_peer::admits(&proxy.path, accept_fd) => {
                warn!(
                    "refusing connection on {}: only the kernel may connect",
                    proxy.path.display()
                );
                // Safe because we've just obtained the FD from the `accept` call above.
                drop(unsafe { OwnedFd::from_raw_fd(accept_fd) });
            }
            Ok(accept_fd) => {
                // Safe because we've just obtained the FD from the `accept` call above.
                let new_fd = unsafe { OwnedFd::from_raw_fd(accept_fd) };
                set_nonblock(&new_fd, proxy.id);
                set_nosigpipe(&new_fd);
                set_stream_buffers(&new_fd);
                if kernel_peer::is_loopback_tcp(&proxy.path) {
                    set_nodelay(&new_fd);
                }
                update.new_proxy = Some((
                    proxy.peer_port,
                    new_fd,
                    AddressFamily::Unix,
                    NewProxyType::Unix,
                ));
            }
            Err(e) => warn!("error accepting connection: id={}, err={}", proxy.id, e),
        };
        update.signal_queue = true;
    }
    update
}

/// Socket buffers for a relayed stream. macOS gives a Unix stream socket
/// 8 KiB, and a Unix sender may queue only its own send buffer's worth in
/// the peer; the guest delivers up to 64 KiB per packet, so raising the
/// buffer keeps bulk transfers on the direct non-copying send path while
/// `tx_buf` handles backpressure without blocking the event loop.
const STREAM_BUF_SIZE: libc::c_int = 4 << 20;

fn set_stream_buffers(fd: &OwnedFd) {
    for opt in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
        // SAFETY: plain setsockopt with an int value on a socket we own.
        let ret = unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                opt,
                &STREAM_BUF_SIZE as *const _ as *const libc::c_void,
                std::mem::size_of_val(&STREAM_BUF_SIZE) as libc::socklen_t,
            )
        };
        if ret != 0 {
            warn!(
                "setting socket buffer option {opt} to {STREAM_BUF_SIZE}: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

pub(crate) fn as_raw_acceptor_fd(proxy: &super::UnixAcceptorProxy) -> RawFd {
    proxy.fd.as_raw_fd()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn socket_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("krun-{label}-{}-{nonce}", std::process::id()))
    }

    #[test]
    fn acceptor_replaces_stale_socket_and_cleans_up_on_drop() {
        let path = socket_path("stale");
        drop(UnixListener::bind(&path).unwrap());
        let proxy = new_acceptor_proxy(1, &path, 1234).unwrap();
        assert!(UnixStream::connect(&path).is_ok());
        drop(proxy);
        assert!(!path.exists());
    }

    /// Clients such as `docker compose` open dozens of connections at once,
    /// faster than the muxer accepts them. macOS refuses a connection
    /// outright once the listen queue is full, so it must be deep.
    #[test]
    fn acceptor_queues_a_burst_of_connections() {
        let path = socket_path("burst");
        let proxy = new_acceptor_proxy(1, &path, 1234).unwrap();
        let clients: Vec<_> = (0..64)
            .map(|i| UnixStream::connect(&path).unwrap_or_else(|e| panic!("client {i}: {e}")))
            .collect();
        drop(clients);
        drop(proxy);
    }

    #[test]
    fn acceptor_preserves_live_listener() {
        let path = socket_path("live");
        let listener = UnixListener::bind(&path).unwrap();
        assert!(new_acceptor_proxy(1, &path, 1234).is_err());
        assert!(UnixStream::connect(&path).is_ok());
        drop(listener);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn acceptor_drop_preserves_replacement_file() {
        let path = socket_path("replaced");
        let proxy = new_acceptor_proxy(1, &path, 1234).unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"replacement").unwrap();
        drop(proxy);
        assert_eq!(fs::read(&path).unwrap(), b"replacement");
        fs::remove_file(path).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn full_live_backlog_does_not_block_acceptor_startup() {
        let path = socket_path("full-backlog");
        let listener = UnixListener::bind(&path).unwrap();
        listen(&listener, Backlog::new(1).unwrap()).unwrap();
        let addr = UnixAddr::new(&path).unwrap();
        let mut clients = Vec::new();
        let mut backlog_full = false;
        for _ in 0..16 {
            let client = socket(
                AddressFamily::Unix,
                SockType::Stream,
                SockFlag::SOCK_NONBLOCK,
                None,
            )
            .unwrap();
            match connect(client.as_raw_fd(), &addr) {
                Ok(()) => clients.push(client),
                Err(Errno::EAGAIN) => {
                    backlog_full = true;
                    break;
                }
                Err(e) => panic!("unexpected connect error: {e}"),
            }
        }
        assert!(backlog_full);

        let (tx, rx) = mpsc::channel();
        let probe_path = path.clone();
        let probe = thread::spawn(move || {
            tx.send(new_acceptor_proxy(1, &probe_path, 1234).is_err())
                .unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap());
        probe.join().unwrap();
        assert!(path.exists());
        drop(clients);
        drop(listener);
        fs::remove_file(path).unwrap();
    }

    fn test_mem() -> vm_memory::GuestMemoryMmap {
        use vm_memory::{GuestAddress, GuestMemoryMmap};
        GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 1 << 20)]).unwrap()
    }

    fn make_tx_pkt(
        mem: &vm_memory::GuestMemoryMmap,
        op: u16,
        flags: u32,
        buf_alloc: u32,
        fwd_cnt: u32,
        data: &[u8],
    ) -> VsockPacket {
        use super::super::super::super::descriptor_utils::{
            DescriptorType, create_descriptor_chain,
        };
        use super::super::super::defs::uapi;
        use super::super::super::packet::VSOCK_PKT_HDR_SIZE;
        use vm_memory::{Bytes, GuestAddress};

        let mut descs = vec![(DescriptorType::Readable, VSOCK_PKT_HDR_SIZE as u32)];
        if !data.is_empty() {
            descs.push((DescriptorType::Readable, data.len() as u32));
        }
        let chain =
            create_descriptor_chain(mem, GuestAddress(0), GuestAddress(0x1000), descs, 0).unwrap();
        let mut header = [0u8; VSOCK_PKT_HDR_SIZE];
        header[8..16].copy_from_slice(&uapi::VSOCK_HOST_CID.to_le_bytes());
        header[16..20].copy_from_slice(&1025u32.to_le_bytes());
        header[20..24].copy_from_slice(&5000u32.to_le_bytes());
        header[24..28].copy_from_slice(&(data.len() as u32).to_le_bytes());
        header[28..30].copy_from_slice(&uapi::VSOCK_TYPE_STREAM.to_le_bytes());
        header[30..32].copy_from_slice(&op.to_le_bytes());
        header[32..36].copy_from_slice(&flags.to_le_bytes());
        header[36..40].copy_from_slice(&buf_alloc.to_le_bytes());
        header[40..44].copy_from_slice(&fwd_cnt.to_le_bytes());
        mem.write_slice(&header, GuestAddress(0x1000)).unwrap();
        if !data.is_empty() {
            mem.write_slice(data, GuestAddress(0x1000 + VSOCK_PKT_HDR_SIZE as u64))
                .unwrap();
        }
        VsockPacket::from_tx_virtq_head(&chain).unwrap()
    }

    fn connected_pair() -> (
        super::super::UnixProxy,
        UnixStream,
        std::sync::Arc<std::sync::Mutex<super::super::super::muxer_rxq::MuxerRxQ>>,
        vm_memory::GuestMemoryMmap,
    ) {
        use super::super::super::super::Queue as VirtQueue;
        use super::super::super::muxer_rxq::MuxerRxQ;
        use std::sync::{Arc, Mutex};

        let (proxy_stream, peer) = UnixStream::pair().unwrap();
        let fd: OwnedFd = proxy_stream.into();
        set_nonblock(&fd, 1);
        set_nosigpipe(&fd);
        let mem = test_mem();
        // Queue with no posted descriptors causes `push_packet` to record every
        // host->guest control packet into `rxq`, so tests can inspect them directly.
        let queue = Arc::new(Mutex::new(VirtQueue::new(16)));
        let rxq = Arc::new(Mutex::new(MuxerRxQ::new()));
        let mut proxy =
            super::super::UnixProxy::new_reverse(1, 3, 5000, 1025, fd, mem.clone(), queue, rxq.clone());
        proxy.status = ProxyStatus::Connected;
        proxy.peer_buf_alloc = 256 * 1024;
        (proxy, peer, rxq, mem)
    }

    #[test]
    fn sendmsg_buffers_when_host_socket_is_full_and_flushes_on_out() {
        use super::super::super::defs::uapi;
        use super::super::super::proxy::Proxy;
        use std::io::Read;

        let (mut proxy, mut peer, rxq, mem) = connected_pair();
        peer.set_nonblocking(true).unwrap();

        // Send deterministic 4 KiB chunks until the host Unix socket buffer fills
        // and sendmsg overflows into `tx_buf` without blocking.
        let mut total_sent = Vec::new();
        let mut armed_out = false;
        for i in 0u32..256 {
            let mut chunk = vec![0u8; 4096];
            for (j, b) in chunk.iter_mut().enumerate() {
                *b = ((i as usize + j) & 0xff) as u8;
            }
            let pkt = make_tx_pkt(&mem, uapi::VSOCK_OP_RW, 0, 256 * 1024, 0, &chunk);
            let update = proxy.sendmsg(&pkt);
            total_sent.extend_from_slice(&chunk);
            if let Some((_, _, ev)) = update.polling {
                if ev.contains(EventSet::OUT) {
                    assert!(ev.contains(EventSet::IN));
                    armed_out = true;
                }
            }
            if proxy.tx_buf.len() >= 16 * 1024 {
                break;
            }
        }
        assert!(armed_out, "expected EventSet::OUT to be armed once socket filled");
        assert!(!proxy.tx_buf.is_empty());
        // tx_cnt only counts bytes accepted by the host kernel, never buffered bytes.
        assert_eq!(
            proxy.tx_cnt.0 as usize + proxy.tx_buf.len(),
            total_sent.len()
        );

        // Drain the peer socket while driving `EventSet::OUT` events.
        let mut received = Vec::new();
        let mut tmp = [0u8; 8192];
        let mut disarmed_out = false;
        for _ in 0..1024 {
            loop {
                match peer.read(&mut tmp) {
                    Ok(0) => break,
                    Ok(n) => received.extend_from_slice(&tmp[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("peer read: {e}"),
                }
            }
            if !proxy.tx_buf.is_empty() {
                let update = proxy.process_event(EventSet::OUT);
                if let Some((_, _, ev)) = update.polling {
                    if !ev.contains(EventSet::OUT) {
                        assert_eq!(ev, EventSet::IN);
                        disarmed_out = true;
                    }
                }
            } else if received.len() == total_sent.len() {
                break;
            }
        }

        assert!(proxy.tx_buf.is_empty());
        assert!(disarmed_out, "expected EventSet::OUT to disarm once drained");
        assert_eq!(received, total_sent);
        assert_eq!(proxy.tx_cnt.0 as usize, total_sent.len());
        assert_eq!(proxy.last_tx_cnt_sent, proxy.tx_cnt);

        // At least one CreditUpdate with the final tx_cnt must have been emitted.
        let mut last_fwd = None;
        let mut q = rxq.lock().unwrap();
        while let Some(item) = q.pop() {
            if let MuxerRx::CreditUpdate { fwd_cnt, .. } = item {
                last_fwd = Some(fwd_cnt);
            }
        }
        assert_eq!(last_fwd, Some(total_sent.len() as u32));
    }

    #[test]
    fn shutdown_send_is_deferred_until_tx_buf_drains() {
        use super::super::super::defs::uapi;
        use super::super::super::proxy::Proxy;
        use std::io::Read;

        let (mut proxy, mut peer, rxq, mem) = connected_pair();
        peer.set_nonblocking(true).unwrap();

        let mut expected = Vec::new();
        for i in 0u8..200 {
            let chunk = vec![i; 4096];
            let pkt = make_tx_pkt(&mem, uapi::VSOCK_OP_RW, 0, 256 * 1024, 0, &chunk);
            proxy.sendmsg(&pkt);
            expected.extend_from_slice(&chunk);
            if !proxy.tx_buf.is_empty() {
                break;
            }
        }
        assert!(!proxy.tx_buf.is_empty());

        // Guest half-closes both directions while bytes are still queued in tx_buf.
        let shut_pkt = make_tx_pkt(
            &mem,
            uapi::VSOCK_OP_SHUTDOWN,
            uapi::VSOCK_FLAGS_SHUTDOWN_SEND | uapi::VSOCK_FLAGS_SHUTDOWN_RCV,
            256 * 1024,
            0,
            &[],
        );
        let shut_upd = proxy.shutdown(&shut_pkt);
        assert_eq!(proxy.status, ProxyStatus::Connected);
        assert!(proxy.pending_peer_shutdown_send);
        // Read interest was dropped (guest shut down RCV), OUT remains armed.
        assert_eq!(shut_upd.polling.map(|p| p.2), Some(EventSet::OUT));

        // Drain peer and flush tx_buf; peer must see all bytes followed by EOF.
        let mut received = Vec::new();
        let mut buf = [0u8; 8192];
        let mut got_eof = false;
        for _ in 0..1024 {
            loop {
                match peer.read(&mut buf) {
                    Ok(0) => {
                        got_eof = true;
                        break;
                    }
                    Ok(n) => received.extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("peer read: {e}"),
                }
            }
            if proxy.status != ProxyStatus::Closed {
                proxy.process_event(EventSet::OUT);
            }
            if got_eof && proxy.status == ProxyStatus::Closed {
                break;
            }
        }

        assert!(got_eof, "peer should observe EOF after deferred drain");
        assert_eq!(received, expected);
        assert_eq!(proxy.status, ProxyStatus::Closed);
        let mut saw_reset = false;
        let mut q = rxq.lock().unwrap();
        while let Some(item) = q.pop() {
            if matches!(item, MuxerRx::Reset { .. }) {
                saw_reset = true;
            }
        }
        assert!(saw_reset, "expected Reset after deferred shutdown finished");
    }

    /// Build a `UnixProxy` backed by a real RX virtqueue with posted writable
    /// buffers, so both `batch::recv_pkts` and `push_packet` operate on real
    /// descriptors.
    fn connected_pair_with_rx_ring(
        mem: &vm_memory::GuestMemoryMmap,
    ) -> (
        super::super::UnixProxy,
        UnixStream,
        crate::virtio::queue::tests::VirtQueue<'_>,
    ) {
        use super::super::super::muxer_rxq::MuxerRxQ;
        use crate::virtio::queue::VIRTQ_DESC_F_WRITE;
        use crate::virtio::queue::tests::VirtQueue as TestVirtQueue;
        use std::sync::{Arc, Mutex};
        use vm_memory::GuestAddress;

        let (proxy_stream, peer) = UnixStream::pair().unwrap();
        let fd: OwnedFd = proxy_stream.into();
        set_nonblock(&fd, 1);
        set_nosigpipe(&fd);

        // Place the RX ring well away from `0x1000` (used by `make_tx_pkt`).
        let vq = TestVirtQueue::new(GuestAddress(0x40000), mem, 16);
        for i in 0..8usize {
            vq.dtable[i].set(0x50000 + (i as u64) * 4096, 4096, VIRTQ_DESC_F_WRITE, 0);
            vq.avail.ring[i].set(i as u16);
        }
        vq.avail.idx.set(8);

        let queue = Arc::new(Mutex::new(vq.create_queue()));
        let rxq = Arc::new(Mutex::new(MuxerRxQ::new()));
        let mut proxy = super::super::UnixProxy::new_reverse(
            1,
            3,
            5000,
            1025,
            fd,
            mem.clone(),
            queue,
            rxq,
        );
        proxy.status = ProxyStatus::Connected;
        proxy.peer_buf_alloc = 256 * 1024;
        (proxy, peer, vq)
    }

    fn read_rx_used_headers(
        mem: &vm_memory::GuestMemoryMmap,
        vq: &crate::virtio::queue::tests::VirtQueue<'_>,
    ) -> Vec<(u16, u32, u32)> {
        use super::super::super::packet::VSOCK_PKT_HDR_SIZE;
        use vm_memory::{Bytes, GuestAddress};

        let count = vq.used.idx.get() as usize;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let slot = vq.used.ring[i].get();
            let desc_idx = slot.id as usize;
            let addr = GuestAddress(vq.dtable[desc_idx].addr.get());
            let mut hdr = [0u8; VSOCK_PKT_HDR_SIZE];
            mem.read_slice(&mut hdr, addr).unwrap();
            let op = u16::from_le_bytes([hdr[30], hdr[31]]);
            let flags = u32::from_le_bytes([hdr[32], hdr[33], hdr[34], hdr[35]]);
            let fwd_cnt = u32::from_le_bytes([hdr[40], hdr[41], hdr[42], hdr[43]]);
            out.push((op, flags, fwd_cnt));
        }
        out
    }

    #[test]
    fn send_error_notifies_guest_with_shutdown_rcv_and_closes_on_eof() {
        use super::super::super::defs::uapi;
        use super::super::super::proxy::Proxy;

        let mem = test_mem();
        let (mut proxy, peer, vq) = connected_pair_with_rx_ring(&mem);
        // Close the host peer (e.g. killed client) so send() fails with EPIPE.
        drop(peer);

        let pkt = make_tx_pkt(&mem, uapi::VSOCK_OP_RW, 0, 256 * 1024, 0, b"hello");
        let update = proxy.sendmsg(&pkt);
        assert!(proxy.local_recv_shutdown);
        assert!(proxy.tx_buf.is_empty());
        assert!(update.signal_queue);
        // Host EOF hasn't been drained by recv yet, so IN remains armed (no OUT).
        assert_eq!(proxy.status, ProxyStatus::Connected);
        assert_eq!(proxy.poll_interest(), EventSet::IN);

        // Next muxer IN event observes EOF, sends SHUTDOWN_SEND + Reset, and closes.
        let eof_upd = proxy.process_event(EventSet::IN);
        assert_eq!(proxy.status, ProxyStatus::Closed);
        assert_eq!(eof_upd.polling.map(|p| p.2), Some(EventSet::empty()));
        assert!(matches!(eof_upd.remove_proxy, ProxyRemoval::Deferred));

        let hdrs = read_rx_used_headers(&mem, &vq);
        assert!(hdrs.contains(&(uapi::VSOCK_OP_SHUTDOWN, uapi::VSOCK_FLAGS_SHUTDOWN_RCV, 0)));
        assert!(hdrs.contains(&(uapi::VSOCK_OP_SHUTDOWN, uapi::VSOCK_FLAGS_SHUTDOWN_SEND, 0)));
        assert!(hdrs.iter().any(|&(op, _, _)| op == uapi::VSOCK_OP_RST));
    }

    #[test]
    fn flush_tx_epipe_with_hangup_cleans_up_buffered_proxy() {
        use super::super::super::defs::uapi;
        use super::super::super::proxy::Proxy;

        let mem = test_mem();
        let (mut proxy, peer, vq) = connected_pair_with_rx_ring(&mem);
        for i in 0u8..200 {
            let chunk = vec![i; 4096];
            let pkt = make_tx_pkt(&mem, uapi::VSOCK_OP_RW, 0, 256 * 1024, 0, &chunk);
            proxy.sendmsg(&pkt);
            if !proxy.tx_buf.is_empty() {
                break;
            }
        }
        assert!(!proxy.tx_buf.is_empty());
        assert_eq!(proxy.poll_interest(), EventSet::IN | EventSet::OUT);
        let expected_fwd = proxy.tx_cnt.0;

        // Peer closes while tx_buf still has pending bytes; macOS kqueue reports
        // EVFILT_WRITE | EV_EOF as EventSet::OUT | EventSet::HANG_UP.
        drop(peer);
        let upd = proxy.process_event(EventSet::OUT | EventSet::HANG_UP);
        assert!(proxy.local_recv_shutdown);
        assert!(proxy.local_shutdown);
        assert!(proxy.tx_buf.is_empty());
        assert_eq!(proxy.status, ProxyStatus::Closed);
        assert_eq!(upd.polling.map(|p| p.2), Some(EventSet::empty()));
        assert!(matches!(upd.remove_proxy, ProxyRemoval::Deferred));

        // Verify Shutdown packets carry the true fwd_cnt (not 0).
        let hdrs = read_rx_used_headers(&mem, &vq);
        assert!(hdrs.contains(&(
            uapi::VSOCK_OP_SHUTDOWN,
            uapi::VSOCK_FLAGS_SHUTDOWN_RCV,
            expected_fwd
        )));
        assert!(hdrs.contains(&(
            uapi::VSOCK_OP_SHUTDOWN,
            uapi::VSOCK_FLAGS_SHUTDOWN_SEND,
            expected_fwd
        )));
        assert!(hdrs.iter().any(|&(op, _, _)| op == uapi::VSOCK_OP_RST));
    }

    #[test]
    fn waiting_credit_update_preserves_out_polling_and_credit_request_replies() {
        use super::super::super::defs::uapi;
        use super::super::super::proxy::Proxy;

        let (mut proxy, _peer, rxq, mem) = connected_pair();
        proxy.tx_buf.extend(b"pending");
        proxy.tx_cnt = Wrapping(1234);

        // Exhaust guest RX credit -> WaitingCreditUpdate must still keep EventSet::OUT.
        proxy.rx_cnt = Wrapping(proxy.peer_buf_alloc);
        let wait_upd = proxy.process_event(EventSet::IN);
        assert_eq!(proxy.status, ProxyStatus::WaitingCreditUpdate);
        assert_eq!(wait_upd.polling.map(|p| p.2), Some(EventSet::OUT));
        assert!(wait_upd.push_credit_req.is_some());

        // Guest sends CreditUpdate restoring credit -> poll interest restores IN | OUT.
        let credit_pkt = make_tx_pkt(
            &mem,
            uapi::VSOCK_OP_CREDIT_UPDATE,
            0,
            256 * 1024,
            proxy.peer_buf_alloc,
            &[],
        );
        let credit_upd = proxy.update_peer_credit(&credit_pkt);
        assert_eq!(proxy.status, ProxyStatus::Connected);
        assert_eq!(
            credit_upd.polling.map(|p| p.2),
            Some(EventSet::IN | EventSet::OUT)
        );

        // Guest sends CreditRequest -> host responds with CreditUpdate(tx_cnt).
        let req_upd = proxy.send_credit_update();
        assert!(req_upd.signal_queue);
        let mut saw_update = None;
        let mut q = rxq.lock().unwrap();
        while let Some(item) = q.pop() {
            if let MuxerRx::CreditUpdate { fwd_cnt, .. } = item {
                saw_update = Some(fwd_cnt);
            }
        }
        assert_eq!(saw_update, Some(1234));
    }
}
