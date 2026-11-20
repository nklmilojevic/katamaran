//! The net worker for an in-process network stack ([`InprocNet`]): it sends
//! the frames vCPUs leave to it (large ones) and refills the receive queue
//! from the backlog when the guest adds buffers. Frames for the guest do not
//! pass through it.

use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::thread;

use utils::epoll::{ControlOperation, Epoll, EpollEvent, EventSet};
use utils::eventfd::EventFd;
use vm_memory::{Bytes, GuestMemoryMmap};

use super::inline_tx::{InlineTx, TxShared, TxSink, frame_iovecs, read_chain};
use crate::virtio::net::backend::WriteError;
use crate::virtio::net::inproc::InprocNet;
use crate::virtio::net::{MAX_BUFFER_SIZE, QUEUE_SIZE, VNET_HDR_LEN};
use crate::virtio::{DeviceQueue, InterruptTransport};

pub struct InprocWorker {
    net: Arc<InprocNet>,
    tx: Arc<Mutex<TxShared>>,
    tx_event: Arc<EventFd>,
    rx_event: Arc<EventFd>,
    mem: GuestMemoryMmap,
    interrupt: InterruptTransport,
    /// For a frame whose buffers are not in one mapped region.
    copy_buf: Vec<u8>,
}

impl InprocWorker {
    pub fn new(
        rx_q: DeviceQueue,
        tx_q: DeviceQueue,
        interrupt: InterruptTransport,
        mem: GuestMemoryMmap,
        net: Arc<InprocNet>,
    ) -> Self {
        let rx_event = rx_q.event.clone();
        net.attach_rx(rx_q.queue, mem.clone(), interrupt.clone());
        InprocWorker {
            net,
            tx_event: tx_q.event.clone(),
            tx: Arc::new(Mutex::new(TxShared::new(tx_q, QUEUE_SIZE as usize))),
            rx_event,
            mem,
            interrupt,
            copy_buf: Vec::with_capacity(MAX_BUFFER_SIZE),
        }
    }

    pub fn inline_tx(&self) -> InlineTx {
        InlineTx::new(
            self.tx.clone(),
            TxSink::InProcess(self.net.clone()),
            self.mem.clone(),
            self.interrupt.clone(),
        )
    }

    pub fn run(self) {
        thread::Builder::new()
            .name("virtio-net worker".into())
            .spawn(|| self.work())
            .unwrap();
    }

    fn work(mut self) {
        #[cfg(target_os = "macos")]
        {
            unsafe extern "C" {
                fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
            }
            const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
            unsafe { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0) };
        }
        let tx_fd = self.tx_event.as_raw_fd();
        let rx_fd = self.rx_event.as_raw_fd();
        let mut epoll = Epoll::new().unwrap();
        for fd in [tx_fd, rx_fd] {
            epoll
                .ctl(
                    ControlOperation::Add,
                    fd,
                    &EpollEvent::new(EventSet::IN, fd as u64),
                )
                .unwrap();
        }
        let mut events = vec![EpollEvent::new(EventSet::empty(), 0); 4];
        loop {
            let n = match epoll.wait(events.len(), -1, &mut events) {
                Ok(n) => n,
                Err(e) => {
                    log::debug!("net worker: epoll wait: {e}");
                    continue;
                }
            };
            for ev in &events[..n] {
                if ev.fd() == tx_fd {
                    let _ = self.tx_event.read();
                    self.process_tx();
                } else if ev.fd() == rx_fd {
                    let _ = self.rx_event.read();
                    self.net.refill();
                }
            }
        }
    }

    pub(crate) fn process_tx(&mut self) {
        let shared = self.tx.clone();
        let mut guard = shared.lock().unwrap();
        let tx = &mut *guard;
        loop {
            if tx.q.queue.disable_notification(&self.mem).is_err() {
                return;
            }
            let mut used = false;
            while let Some(head) = tx.q.queue.pop(&self.mem) {
                let index = head.index;
                read_chain(head, &mut tx.chain);
                let result = if frame_iovecs(&self.mem, &tx.chain, &mut tx.host_iov.0)
                    && !tx.host_iov.0.is_empty()
                {
                    self.net.send(&tx.host_iov.0)
                } else {
                    self.send_copy(&tx.chain)
                };
                if let Err(e) = result {
                    log::error!("Dropping a frame the network stack refused: {e:?}");
                }
                if let Err(e) = tx.q.queue.add_used(&self.mem, index, 0) {
                    log::error!("net: failed to add used buffer: {e:?}");
                }
                used = true;
            }
            if used && tx.q.queue.needs_notification(&self.mem).unwrap_or(true) {
                let _ = self.interrupt.try_signal_used_queue();
            }
            if !tx.q.queue.enable_notification(&self.mem).unwrap_or(false) {
                return;
            }
        }
    }

    /// Send a frame through a copy, for buffers [`frame_iovecs`] can't map.
    fn send_copy(&mut self, chain: &[(vm_memory::GuestAddress, usize)]) -> Result<(), WriteError> {
        let len: usize = chain.iter().map(|&(_, l)| l).sum();
        if len <= VNET_HDR_LEN || len > MAX_BUFFER_SIZE {
            return Err(WriteError::Internal(nix::Error::EINVAL));
        }
        self.copy_buf.resize(len, 0);
        let mut off = 0;
        for &(addr, l) in chain {
            self.mem
                .read_slice(&mut self.copy_buf[off..off + l], addr)
                .map_err(|_| WriteError::Internal(nix::Error::EFAULT))?;
            off += l;
        }
        let iov = [libc::iovec {
            iov_base: self.copy_buf[VNET_HDR_LEN..].as_mut_ptr() as *mut libc::c_void,
            iov_len: len - VNET_HDR_LEN,
        }];
        self.net.send(&iov)
    }
}
