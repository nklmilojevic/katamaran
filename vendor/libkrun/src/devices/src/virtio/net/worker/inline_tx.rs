//! Transmit state shared between the net worker and the vCPU threads, so a
//! vCPU that kicks the transmit queue can send small frames itself instead of
//! waking the worker, which on macOS costs about 7 us per kick: most of what
//! a request/response round trip spends in the device.

use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::{Arc, Mutex};

use vm_memory::{Address, GuestAddress, GuestMemoryBackend, GuestMemoryMmap};

use crate::virtio::net::VNET_HDR_LEN;
use crate::virtio::net::backend::WriteError;
use crate::virtio::net::inproc::InprocNet;
use crate::virtio::net::unixgram::send_iov;
use crate::virtio::queue::DescriptorChain;
use crate::virtio::{DeviceQueue, InterruptTransport};

/// Largest frame a vCPU sends inline. Bigger ones are left to the worker, so
/// a bulk transfer keeps the guest producing frames while the worker sends.
pub const INLINE_TX_MAX: usize = 4096;

pub struct TxShared {
    pub q: DeviceQueue,
    /// Guest buffers of the frame being sent.
    pub chain: Vec<(GuestAddress, usize)>,
    pub host_iov: HostIovecs,
}

impl TxShared {
    pub fn new(q: DeviceQueue, capacity: usize) -> Self {
        TxShared {
            q,
            chain: Vec::with_capacity(capacity),
            host_iov: HostIovecs(Vec::with_capacity(capacity)),
        }
    }
}

/// Host addresses of the frame being sent, valid only while it is sent.
pub struct HostIovecs(pub Vec<libc::iovec>);

// SAFETY: the pointers are only used under the TxShared lock, into guest
// memory that both the worker and the device keep mapped.
unsafe impl Send for HostIovecs {}

/// The readable buffers of the chain at `head`; empty if it has a writable
/// one, which a transmit chain must not.
pub fn read_chain(head: DescriptorChain, out: &mut Vec<(GuestAddress, usize)>) {
    out.clear();
    let mut next = Some(head);
    while let Some(desc) = next {
        if desc.is_write_only() {
            out.clear();
            return;
        }
        out.push((desc.addr, desc.len as usize));
        next = desc.next_descriptor();
    }
}

/// Fill `out` with the host addresses of the frame in `chain`, leaving out the
/// virtio-net header in front of it. False if a buffer is not within one
/// mapped region of guest memory.
pub fn frame_iovecs(
    mem: &GuestMemoryMmap,
    chain: &[(GuestAddress, usize)],
    out: &mut Vec<libc::iovec>,
) -> bool {
    out.clear();
    let mut skip = VNET_HDR_LEN;
    for &(addr, len) in chain {
        if len <= skip {
            skip -= len;
            continue;
        }
        let len = len - skip;
        let Ok(slice) = mem.get_slice(addr.unchecked_add(skip as u64), len) else {
            return false;
        };
        out.push(libc::iovec {
            iov_base: slice.ptr_guard().as_ptr() as *mut libc::c_void,
            iov_len: len,
        });
        skip = 0;
    }
    true
}

/// Where frames from the guest go.
pub enum TxSink {
    /// The worker's datagram socket, duplicated.
    Socket(OwnedFd),
    InProcess(Arc<InprocNet>),
}

impl TxSink {
    pub fn send(&self, iov: &[libc::iovec]) -> Result<(), WriteError> {
        match self {
            TxSink::Socket(fd) => send_iov(fd.as_raw_fd(), iov),
            TxSink::InProcess(net) => net.send(iov),
        }
    }
}

/// Sends small frames on the vCPU thread that kicked the transmit queue.
pub struct InlineTx {
    shared: Arc<Mutex<TxShared>>,
    sink: TxSink,
    mem: GuestMemoryMmap,
    interrupt: InterruptTransport,
}

enum Sent {
    All,
    /// Stopped at a frame the worker must send.
    HandOff,
}

impl InlineTx {
    pub fn new(
        shared: Arc<Mutex<TxShared>>,
        sink: TxSink,
        mem: GuestMemoryMmap,
        interrupt: InterruptTransport,
    ) -> Self {
        InlineTx {
            shared,
            sink,
            mem,
            interrupt,
        }
    }

    /// Send what the guest queued, as the worker would. False leaves the
    /// rest to the worker: the worker is sending, a frame is large or not in
    /// one mapped region, or the socket is full.
    pub fn kick(&self) -> bool {
        let Ok(mut guard) = self.shared.try_lock() else {
            return false;
        };
        let tx = &mut *guard;
        loop {
            if tx.q.queue.disable_notification(&self.mem).is_err() {
                return false;
            }
            let sent = self.send_small(tx);
            let more = tx.q.queue.enable_notification(&self.mem).unwrap_or(true);
            match sent {
                Sent::HandOff => return false,
                Sent::All if more => continue,
                Sent::All => return true,
            }
        }
    }

    fn send_small(&self, tx: &mut TxShared) -> Sent {
        let mut used = false;
        let sent = loop {
            let Some(head) = tx.q.queue.pop(&self.mem) else {
                break Sent::All;
            };
            let index = head.index;
            read_chain(head, &mut tx.chain);
            let len: usize = tx.chain.iter().map(|&(_, l)| l).sum();
            if len > INLINE_TX_MAX + VNET_HDR_LEN
                || !frame_iovecs(&self.mem, &tx.chain, &mut tx.host_iov.0)
            {
                tx.q.queue.undo_pop();
                break Sent::HandOff;
            }
            let result = if tx.host_iov.0.is_empty() {
                Err(WriteError::Internal(nix::Error::EINVAL))
            } else {
                self.sink.send(&tx.host_iov.0)
            };
            match result {
                Ok(()) => {}
                Err(WriteError::NothingWritten) => {
                    tx.q.queue.undo_pop();
                    break Sent::HandOff;
                }
                Err(e) => log::error!("Dropping a frame the backend refused: {e:?}"),
            }
            if tx.q.queue.add_used(&self.mem, index, 0).is_err() {
                break Sent::HandOff;
            }
            used = true;
        };
        if used && tx.q.queue.needs_notification(&self.mem).unwrap_or(true) {
            let _ = self.interrupt.try_signal_used_queue();
        }
        sent
    }
}
