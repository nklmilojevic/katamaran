//! A virtio-net backend for a network stack loaded into the VMM process, as
//! katamaran loads gvproxy. Frames pass by function call both ways, so no
//! socket and no thread hand-off sit between the guest and the stack:
//! - the guest's frames go to the stack's transmit function, from the
//!   vCPU that kicked the queue for small frames and from the net worker for
//!   the rest ([`InprocNet::send`]);
//! - the stack writes frames for the guest into the receive queue itself,
//!   on its own threads, through [`inproc_net_rx`].
//!
//! When the receive queue is full, frames wait in a bounded backlog that the
//! worker flushes when the guest adds buffers, as a socket's buffer did;
//! beyond it they are dropped, as a full NIC ring drops them.

use std::collections::VecDeque;
use std::ffi::{c_int, c_void};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use vm_memory::{Address, Bytes, GuestMemoryMmap};

use super::backend::WriteError;
use crate::virtio::InterruptTransport;
use crate::virtio::queue::Queue;

/// The stack's transmit function: one frame from the guest, without its
/// virtio-net header. Returns 0 or an errno.
pub type TxFn = unsafe extern "C" fn(iov: *const libc::iovec, iovcnt: c_int) -> c_int;

/// Frames kept while the guest has no receive buffers; about 2 ms at full
/// speed, as much as the datagram socket used to hold.
const BACKLOG_MAX_BYTES: usize = 8 << 20;

pub struct InprocNet {
    tx: TxFn,
    rx: Mutex<Option<RxRing>>,
    rx_dropped: AtomicU64,
}

struct RxRing {
    queue: Queue,
    mem: GuestMemoryMmap,
    interrupt: InterruptTransport,
    backlog: VecDeque<Vec<u8>>,
    backlog_bytes: usize,
}

impl InprocNet {
    pub fn new(tx: TxFn) -> Self {
        InprocNet {
            tx,
            rx: Mutex::new(None),
            rx_dropped: AtomicU64::new(0),
        }
    }

    /// Frames for the guest dropped because its receive queue and the
    /// backlog were full.
    pub fn rx_dropped(&self) -> u64 {
        self.rx_dropped.load(Ordering::Relaxed)
    }

    /// Send a frame, without its virtio-net header, to the stack. Returns
    /// the stack's 0 or errno.
    pub fn transmit(&self, iov: &[libc::iovec]) -> c_int {
        // SAFETY: the iovecs point to readable memory that stays valid for
        // the call; the stack copies what it keeps.
        unsafe { (self.tx)(iov.as_ptr(), iov.len() as c_int) }
    }

    pub(crate) fn send(&self, iov: &[libc::iovec]) -> Result<(), WriteError> {
        match self.transmit(iov) {
            0 => Ok(()),
            e => Err(WriteError::Internal(nix::Error::from_raw(e))),
        }
    }

    pub(crate) fn attach_rx(
        &self,
        queue: Queue,
        mem: GuestMemoryMmap,
        interrupt: InterruptTransport,
    ) {
        *self.rx.lock().unwrap() = Some(RxRing {
            queue,
            mem,
            interrupt,
            backlog: VecDeque::new(),
            backlog_bytes: 0,
        });
    }

    /// Write a frame from the stack (virtio-net header, then the frame) to
    /// the guest. Returns 0, or EAGAIN if it was dropped.
    pub fn deliver(&self, iov: &[libc::iovec]) -> c_int {
        let mut guard = self.rx.lock().unwrap();
        let Some(ring) = guard.as_mut() else {
            self.rx_dropped.fetch_add(1, Ordering::Relaxed);
            return libc::EAGAIN;
        };
        ring.flush();
        if ring.backlog.is_empty() && ring.write(iov) {
            ring.signal();
            return 0;
        }
        let len: usize = iov.iter().map(|v| v.iov_len).sum();
        if ring.backlog_bytes + len > BACKLOG_MAX_BYTES {
            self.rx_dropped.fetch_add(1, Ordering::Relaxed);
            return libc::EAGAIN;
        }
        let mut frame = Vec::with_capacity(len);
        for v in iov {
            // SAFETY: the stack passes readable memory for the call.
            frame.extend_from_slice(unsafe {
                std::slice::from_raw_parts(v.iov_base as *const u8, v.iov_len)
            });
        }
        ring.backlog_bytes += len;
        ring.backlog.push_back(frame);
        ring.arm();
        0
    }

    /// The guest added receive buffers: move waiting frames into them.
    pub(crate) fn refill(&self) {
        if let Some(ring) = self.rx.lock().unwrap().as_mut() {
            ring.flush();
            if !ring.backlog.is_empty() {
                ring.arm();
            }
        }
    }
}

impl RxRing {
    /// Copy one frame into the next receive buffer. False if there is none.
    fn write(&mut self, iov: &[libc::iovec]) -> bool {
        let Some(head) = self.queue.pop(&self.mem) else {
            return false;
        };
        let index = head.index;
        let mut written = 0usize;
        let mut ok = true;
        let mut src = iov.iter().map(|v| {
            // SAFETY: the stack passes readable memory for the call.
            unsafe { std::slice::from_raw_parts(v.iov_base as *const u8, v.iov_len) }
        });
        let mut chunk: &[u8] = src.next().unwrap_or_default();
        let mut desc = Some(head);
        'chain: while let Some(d) = desc {
            if !d.is_write_only() {
                ok = false;
                break;
            }
            let mut off = 0usize;
            while off < d.len as usize {
                while chunk.is_empty() {
                    match src.next() {
                        Some(c) => chunk = c,
                        None => break 'chain,
                    }
                }
                let n = chunk.len().min(d.len as usize - off);
                if self
                    .mem
                    .write_slice(&chunk[..n], d.addr.unchecked_add(off as u64))
                    .is_err()
                {
                    ok = false;
                    break 'chain;
                }
                chunk = &chunk[n..];
                off += n;
                written += n;
            }
            desc = d.next_descriptor();
        }
        if ok && (!chunk.is_empty() || src.any(|c| !c.is_empty())) {
            log::warn!("Receiving buffer is too small to hold the frame");
            ok = false;
        }
        let used = if ok { written as u32 } else { 0 };
        if let Err(e) = self.queue.add_used(&self.mem, index, used) {
            log::error!("net: failed to add used buffer: {e:?}");
        }
        true
    }

    fn flush(&mut self) {
        let mut wrote = false;
        while let Some(frame) = self.backlog.pop_front() {
            let iov = [libc::iovec {
                iov_base: frame.as_ptr() as *mut c_void,
                iov_len: frame.len(),
            }];
            if !self.write(&iov) {
                self.backlog.push_front(frame);
                break;
            }
            self.backlog_bytes -= frame.len();
            wrote = true;
        }
        if wrote {
            self.signal();
        }
    }

    /// Ask the guest to kick the queue when it adds buffers.
    fn arm(&mut self) {
        while let Ok(true) = self.queue.enable_notification(&self.mem) {
            // Buffers arrived while arming.
            let before = self.backlog.len();
            self.flush();
            if self.backlog.is_empty() || self.backlog.len() == before {
                return;
            }
        }
    }

    fn signal(&mut self) {
        if self.queue.needs_notification(&self.mem).unwrap_or(true)
            && let Err(e) = self.interrupt.try_signal_used_queue()
        {
            log::error!("net: failed to signal the guest: {e:?}");
        }
    }
}

/// The receive function for the stack: `ctx` is the [`InprocNet`], which
/// must outlive the stack.
///
/// # Safety
/// `ctx` must point to a live `InprocNet` and `iov` to `iovcnt` iovecs of
/// readable memory.
pub unsafe extern "C" fn inproc_net_rx(
    ctx: *mut c_void,
    iov: *const libc::iovec,
    iovcnt: c_int,
) -> c_int {
    // SAFETY: guaranteed by the caller.
    let (net, iov) = unsafe {
        (
            &*(ctx as *const InprocNet),
            std::slice::from_raw_parts(iov, iovcnt.max(0) as usize),
        )
    };
    net.deliver(iov)
}

#[cfg(test)]
#[path = "inproc_tests.rs"]
mod tests;
