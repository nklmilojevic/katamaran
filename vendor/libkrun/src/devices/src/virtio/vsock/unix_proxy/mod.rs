use std::collections::{HashMap, VecDeque};
use std::num::Wrapping;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::super::Queue as VirtQueue;
use super::defs;
use super::defs::uapi;
use super::muxer::{MuxerRx, push_packet};
use super::muxer_rxq::MuxerRxQ;
use super::packet::{TsiAcceptReq, TsiConnectReq, TsiListenReq, TsiSendtoAddr, VsockPacket};

use super::proxy::{AsRawFd, OwnedFd, Proxy, ProxyError, ProxyStatus, ProxyUpdate, RawFd, RecvPkt};

use utils::epoll::EventSet;
use vm_memory::GuestMemoryMmap;

#[cfg(unix)]
mod batch;
#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix as sys;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows as sys;

// Fields are marked pub(crate) so the OS submodules can access them
pub struct UnixProxy {
    pub(crate) id: u64,
    pub(crate) cid: u64,
    pub(crate) fd: OwnedFd,
    pub status: ProxyStatus,
    pub(crate) mem: GuestMemoryMmap,
    pub(crate) queue: Arc<Mutex<VirtQueue>>,
    pub(crate) rxq: Arc<Mutex<MuxerRxQ>>,
    pub(crate) path: PathBuf,
    pub(crate) peer_port: u32,
    pub(crate) local_port: u32,
    pub(crate) control_port: u32,
    pub(crate) peer_shutdown: u32,
    /// The host end reached EOF and the guest was sent SHUTDOWN_SEND.
    pub(crate) local_shutdown: bool,
    /// The host end closed its read side (send failed) and the guest was sent SHUTDOWN_RCV.
    pub(crate) local_recv_shutdown: bool,
    /// Guest-to-host bytes waiting for the host socket to become writable.
    pub(crate) tx_buf: VecDeque<u8>,
    /// The guest asked to shut down its send side while `tx_buf` still had
    /// buffered bytes; `shutdown(SHUT_WR)` runs once `tx_buf` drains.
    pub(crate) pending_peer_shutdown_send: bool,
    pub(crate) peer_fwd_cnt: Wrapping<u32>,
    pub(crate) peer_buf_alloc: u32,
    pub(crate) tx_cnt: Wrapping<u32>,
    pub(crate) last_tx_cnt_sent: Wrapping<u32>,
    pub(crate) push_cnt: Wrapping<u32>,
    pub(crate) rx_cnt: Wrapping<u32>,
}

impl UnixProxy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: u64,
        cid: u64,
        local_port: u32,
        control_port: u32,
        mem: GuestMemoryMmap,
        queue: Arc<Mutex<VirtQueue>>,
        rxq: Arc<Mutex<MuxerRxQ>>,
        path: PathBuf,
    ) -> Result<Self, ProxyError> {
        let fd = sys::create_socket(id)?;
        Ok(UnixProxy {
            id,
            cid,
            fd,
            status: ProxyStatus::Idle,
            mem,
            queue,
            rxq,
            path,
            peer_port: 0,
            local_port,
            control_port,
            peer_shutdown: 0,
            local_shutdown: false,
            local_recv_shutdown: false,
            tx_buf: VecDeque::new(),
            pending_peer_shutdown_send: false,
            peer_fwd_cnt: Wrapping(0),
            peer_buf_alloc: 0,
            tx_cnt: Wrapping(0),
            last_tx_cnt_sent: Wrapping(0),
            push_cnt: Wrapping(0),
            rx_cnt: Wrapping(0),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_reverse(
        id: u64,
        cid: u64,
        local_port: u32,
        peer_port: u32,
        fd: OwnedFd,
        mem: GuestMemoryMmap,
        queue: Arc<Mutex<VirtQueue>>,
        rxq: Arc<Mutex<MuxerRxQ>>,
    ) -> Self {
        debug!("new_reverse: id={id} local_port={local_port} peer_port={peer_port}");
        UnixProxy {
            id,
            cid,
            local_port,
            peer_port,
            control_port: 0,
            peer_shutdown: 0,
            local_shutdown: false,
            local_recv_shutdown: false,
            tx_buf: VecDeque::new(),
            pending_peer_shutdown_send: false,
            fd,
            status: ProxyStatus::ReverseInit,
            mem,
            queue,
            rxq,
            rx_cnt: Wrapping(0),
            tx_cnt: Wrapping(0),
            last_tx_cnt_sent: Wrapping(0),
            peer_buf_alloc: 0,
            peer_fwd_cnt: Wrapping(0),
            push_cnt: Wrapping(0),
            path: Default::default(),
        }
    }

    pub(crate) fn push_reset(&self) {
        debug!(
            "push_reset: id: {}, peer_port: {}, local_port: {}",
            self.id, self.peer_port, self.local_port
        );

        let rx = MuxerRx::Reset {
            local_port: self.local_port,
            peer_port: self.peer_port,
        };

        push_packet(self.cid, rx, &self.rxq, &self.queue, &self.mem);
    }

    /// Tell the guest the host end will send no more data.
    pub(crate) fn push_shutdown_send(&self) {
        let rx = MuxerRx::Shutdown {
            local_port: self.local_port,
            peer_port: self.peer_port,
            flags: uapi::VSOCK_FLAGS_SHUTDOWN_SEND,
            fwd_cnt: self.tx_cnt.0,
        };
        push_packet(self.cid, rx, &self.rxq, &self.queue, &self.mem);
    }

    /// Tell the guest the host end will receive no more data.
    pub(crate) fn push_shutdown_recv(&self) {
        let rx = MuxerRx::Shutdown {
            local_port: self.local_port,
            peer_port: self.peer_port,
            flags: uapi::VSOCK_FLAGS_SHUTDOWN_RCV,
            fwd_cnt: self.tx_cnt.0,
        };
        push_packet(self.cid, rx, &self.rxq, &self.queue, &self.mem);
    }

    /// Report the current forwarded byte count (`tx_cnt`) to the guest.
    pub(crate) fn push_credit_update(&mut self) {
        self.last_tx_cnt_sent = self.tx_cnt;
        let rx = MuxerRx::CreditUpdate {
            local_port: self.local_port,
            peer_port: self.peer_port,
            fwd_cnt: self.tx_cnt.0,
        };
        push_packet(self.cid, rx, &self.rxq, &self.queue, &self.mem);
    }

    /// Readiness to poll the host socket for reads: nothing once EOF or guest
    /// RCV shutdown has occurred.
    pub(crate) fn read_interest(&self) -> EventSet {
        if self.status != ProxyStatus::Connected
            || self.local_shutdown
            || (self.peer_shutdown & uapi::VSOCK_FLAGS_SHUTDOWN_RCV != 0)
        {
            EventSet::empty()
        } else {
            EventSet::IN
        }
    }

    /// Combined readiness to poll the host socket for: read while connected
    /// and open, write while connecting or draining buffered guest-to-host bytes.
    pub(crate) fn poll_interest(&self) -> EventSet {
        match self.status {
            ProxyStatus::Connecting => EventSet::IN | EventSet::OUT,
            ProxyStatus::Connected => {
                let mut ev = self.read_interest();
                if !self.tx_buf.is_empty() && !self.local_recv_shutdown {
                    ev |= EventSet::OUT;
                }
                ev
            }
            ProxyStatus::WaitingCreditUpdate => {
                if !self.tx_buf.is_empty() && !self.local_recv_shutdown {
                    EventSet::OUT
                } else {
                    EventSet::empty()
                }
            }
            _ => EventSet::empty(),
        }
    }

    /// Whether both directions of the relayed stream are finished and no
    /// buffered guest-to-host data remains to be flushed.
    pub(crate) fn is_fully_shutdown(&self) -> bool {
        let host_to_guest_done =
            self.local_shutdown || (self.peer_shutdown & uapi::VSOCK_FLAGS_SHUTDOWN_RCV != 0);
        let guest_to_host_done = self.local_recv_shutdown
            || ((self.peer_shutdown & uapi::VSOCK_FLAGS_SHUTDOWN_SEND != 0)
                && self.tx_buf.is_empty());
        host_to_guest_done && guest_to_host_done
    }

    pub(crate) fn peer_avail_credit(&self) -> usize {
        (Wrapping(self.peer_buf_alloc) - (self.rx_cnt - self.peer_fwd_cnt)).0 as usize
    }

    #[cfg(unix)]
    pub fn recv_pkt(&mut self) -> (bool, bool) {
        batch::recv_pkts(self)
    }

    #[cfg(windows)]
    pub fn recv_pkt(&mut self) -> (bool, bool) {
        let mut have_used = false;
        let mut wait_credit = false;
        let mut queue = self.queue.lock().unwrap();

        while let Some(head) = queue.pop(&self.mem) {
            let len = match VsockPacket::from_rx_virtq_head(&head) {
                Ok(mut pkt) => match sys::recv_to_pkt(self, &mut pkt) {
                    RecvPkt::WaitForCredit => {
                        wait_credit = true;
                        0
                    }
                    RecvPkt::Read(cnt) => {
                        self.rx_cnt += Wrapping(cnt as u32);
                        self.init_data_pkt(&mut pkt);
                        pkt.set_len(cnt as u32);
                        pkt.hdr().len() + cnt
                    }
                    RecvPkt::Close => {
                        self.status = ProxyStatus::Closed;
                        0
                    }
                    RecvPkt::Eof => {
                        self.local_shutdown = true;
                        0
                    }
                    RecvPkt::Error => 0,
                },
                Err(e) => {
                    debug!("recv_pkt: RX queue error: {e:?}");
                    0
                }
            };

            if len == 0 {
                queue.undo_pop();
                break;
            } else {
                have_used = true;
                self.push_cnt += Wrapping(len as u32);
                debug!(
                    "recv_pkt: pushing packet with {} bytes, push_cnt={}",
                    len, self.push_cnt
                );
                if let Err(e) = queue.add_used(&self.mem, head.index, len as u32) {
                    error!("failed to add used elements to the queue: {e:?}");
                }
            }
        }

        debug!("recv_pkt: have_used={have_used}");
        (have_used, wait_credit)
    }

    pub(crate) fn init_data_pkt(&self, pkt: &mut VsockPacket) {
        debug!(
            "init_data_pkt: id={}, local_port={}, peer_port={}",
            self.id, self.local_port, self.peer_port
        );

        pkt.set_op(uapi::VSOCK_OP_RW)
            .set_src_cid(uapi::VSOCK_HOST_CID)
            .set_dst_cid(self.cid)
            .set_src_port(self.local_port)
            .set_dst_port(self.peer_port)
            .set_type(uapi::VSOCK_TYPE_STREAM)
            .set_buf_alloc(defs::CONN_TX_BUF_SIZE as u32)
            .set_fwd_cnt(self.tx_cnt.0);
    }
}

impl Proxy for UnixProxy {
    fn id(&self) -> u64 {
        self.id
    }

    fn status(&self) -> ProxyStatus {
        self.status
    }

    fn connect(&mut self, pkt: &VsockPacket, req: TsiConnectReq) -> ProxyUpdate {
        sys::do_connect(self, pkt, req)
    }

    fn confirm_connect(&mut self, pkt: &VsockPacket) -> Option<ProxyUpdate> {
        sys::confirm_connect(self, pkt)
    }

    fn getpeername(&mut self, _pkt: &VsockPacket) {
        todo!();
    }

    fn sendmsg(&mut self, pkt: &VsockPacket) -> ProxyUpdate {
        sys::sendmsg(self, pkt)
    }

    fn sendto_addr(&mut self, _req: TsiSendtoAddr) -> ProxyUpdate {
        todo!();
    }

    fn listen(
        &mut self,
        _pkt: &VsockPacket,
        _req: TsiListenReq,
        _host_port_map: &Option<HashMap<u16, u16>>,
    ) -> ProxyUpdate {
        todo!();
    }

    fn accept(&mut self, _req: TsiAcceptReq) -> ProxyUpdate {
        todo!();
    }

    fn update_peer_credit(&mut self, pkt: &VsockPacket) -> ProxyUpdate {
        sys::update_peer_credit(self, pkt)
    }

    fn send_credit_update(&mut self) -> ProxyUpdate {
        if matches!(
            self.status,
            ProxyStatus::Connected | ProxyStatus::WaitingCreditUpdate
        ) {
            self.push_credit_update();
            ProxyUpdate {
                signal_queue: true,
                ..Default::default()
            }
        } else {
            ProxyUpdate::default()
        }
    }

    fn push_op_request(&self) {
        sys::push_op_request(self)
    }

    fn process_op_response(&mut self, pkt: &VsockPacket) -> ProxyUpdate {
        sys::process_op_response(self, pkt)
    }

    fn enqueue_accept(&mut self) {
        todo!();
    }

    fn shutdown(&mut self, pkt: &VsockPacket) -> ProxyUpdate {
        const SHUTDOWN_MASK: u32 = uapi::VSOCK_FLAGS_SHUTDOWN_RCV | uapi::VSOCK_FLAGS_SHUTDOWN_SEND;

        let shutdown = pkt.flags() & SHUTDOWN_MASK;
        if shutdown == 0 || self.peer_shutdown == SHUTDOWN_MASK {
            return ProxyUpdate::default();
        }

        let prev_interest = self.poll_interest();
        let new_flags = shutdown & !self.peer_shutdown;
        self.peer_shutdown |= shutdown;

        let recv_off = new_flags & uapi::VSOCK_FLAGS_SHUTDOWN_RCV != 0;
        let send_off = new_flags & uapi::VSOCK_FLAGS_SHUTDOWN_SEND != 0;
        let defer_send = send_off && !self.tx_buf.is_empty() && !self.local_recv_shutdown;
        if defer_send {
            self.pending_peer_shutdown_send = true;
        }
        sys::do_shutdown_dirs(self, recv_off, send_off && !defer_send);

        if !self.is_fully_shutdown() {
            let next_interest = self.poll_interest();
            let mut update = ProxyUpdate::default();
            if next_interest != prev_interest {
                update.polling = Some((self.id, self.as_raw_fd(), next_interest));
            }
            return update;
        }

        self.push_reset();
        let mut update = self.release();
        self.status = ProxyStatus::Closed;
        update.polling = Some((self.id, self.as_raw_fd(), EventSet::empty()));
        update.signal_queue = true;
        update
    }

    fn release(&mut self) -> ProxyUpdate {
        sys::release(self)
    }

    fn process_event(&mut self, evset: EventSet) -> ProxyUpdate {
        sys::process_event(self, evset)
    }
}

impl AsRawFd for UnixProxy {
    fn as_raw_fd(&self) -> RawFd {
        sys::as_raw_fd(self)
    }
}

pub struct UnixAcceptorProxy {
    id: u64,
    fd: OwnedFd,
    peer_port: u32,
    #[cfg(unix)]
    path: PathBuf,
    #[cfg(unix)]
    identity: (u64, u64),
}

impl UnixAcceptorProxy {
    pub fn new(id: u64, path: &Path, peer_port: u32) -> Result<Self, ProxyError> {
        sys::new_acceptor_proxy(id, path, peer_port)
    }
}

impl Proxy for UnixAcceptorProxy {
    fn id(&self) -> u64 {
        self.id
    }
    fn status(&self) -> ProxyStatus {
        ProxyStatus::WaitingOnAccept
    }
    fn connect(&mut self, _: &VsockPacket, _: TsiConnectReq) -> ProxyUpdate {
        unreachable!()
    }
    fn getpeername(&mut self, _: &VsockPacket) {
        unreachable!()
    }
    fn sendmsg(&mut self, _: &VsockPacket) -> ProxyUpdate {
        unreachable!()
    }
    fn sendto_addr(&mut self, _: TsiSendtoAddr) -> ProxyUpdate {
        unreachable!()
    }
    fn listen(
        &mut self,
        _: &VsockPacket,
        _: TsiListenReq,
        _: &Option<HashMap<u16, u16>>,
    ) -> ProxyUpdate {
        unreachable!()
    }
    fn accept(&mut self, _: TsiAcceptReq) -> ProxyUpdate {
        unreachable!()
    }
    fn update_peer_credit(&mut self, _: &VsockPacket) -> ProxyUpdate {
        unreachable!()
    }
    fn process_op_response(&mut self, _: &VsockPacket) -> ProxyUpdate {
        unreachable!()
    }
    fn release(&mut self) -> ProxyUpdate {
        unreachable!()
    }
    fn process_event(&mut self, evset: EventSet) -> ProxyUpdate {
        sys::process_acceptor_event(self, evset)
    }
}

impl AsRawFd for UnixAcceptorProxy {
    fn as_raw_fd(&self) -> RawFd {
        sys::as_raw_acceptor_fd(self)
    }
}
