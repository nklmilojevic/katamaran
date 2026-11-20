use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;

use utils::eventfd::{EFD_NONBLOCK, EventFd};
use vm_memory::GuestAddress;

use super::*;
use crate::virtio::device::{InterruptHandler, InterruptType};
use crate::virtio::net::VNET_HDR_LEN;
use crate::virtio::net::worker::InprocWorker;
use crate::virtio::queue::tests::VirtQueue;
use crate::virtio::queue::{VIRTQ_DESC_F_NEXT, VIRTQ_DESC_F_WRITE};
use crate::virtio::{DeviceQueue, InterruptTransport};
use std::sync::Arc;

const BUFFERS: u64 = 0x10000;
const BUF_LEN: u32 = 0x1000;

thread_local! {
    static SENT: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

/// The stack's transmit function: records each frame.
unsafe extern "C" fn record_tx(iov: *const libc::iovec, iovcnt: c_int) -> c_int {
    let iov = unsafe { std::slice::from_raw_parts(iov, iovcnt as usize) };
    let mut frame = Vec::new();
    for v in iov {
        frame.extend_from_slice(unsafe {
            std::slice::from_raw_parts(v.iov_base as *const u8, v.iov_len)
        });
    }
    SENT.with(|s| s.borrow_mut().push(frame));
    0
}

#[derive(Default)]
struct Irqs(AtomicUsize);

impl InterruptHandler for Irqs {
    fn try_signal(&self, _: InterruptType) -> Result<(), crate::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn memory() -> GuestMemoryMmap {
    GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x80000)]).unwrap()
}

/// Post receive buffer `i` in descriptor `i` and avail slot `i`.
fn post(rxq: &VirtQueue, i: u16) {
    let addr = BUFFERS + u64::from(i) * u64::from(BUF_LEN);
    rxq.dtable[i as usize].set(addr, BUF_LEN, VIRTQ_DESC_F_WRITE, 0);
    rxq.avail.ring[i as usize].set(i);
    rxq.avail.idx.set(i + 1);
}

fn iov(parts: &[&[u8]]) -> Vec<libc::iovec> {
    parts
        .iter()
        .map(|p| libc::iovec {
            iov_base: p.as_ptr() as *mut c_void,
            iov_len: p.len(),
        })
        .collect()
}

fn received(mem: &GuestMemoryMmap, rxq: &VirtQueue, n: u16) -> Vec<u8> {
    let used = rxq.used.ring[n as usize].get();
    let mut buf = vec![0u8; used.len as usize];
    mem.read_slice(
        &mut buf,
        GuestAddress(BUFFERS + u64::from(used.id) * u64::from(BUF_LEN)),
    )
    .unwrap();
    buf
}

fn attached<'a>(mem: &'a GuestMemoryMmap, posted: u16) -> (InprocNet, VirtQueue<'a>, Arc<Irqs>) {
    let rxq = VirtQueue::new(GuestAddress(0), mem, 16);
    for i in 0..posted {
        post(&rxq, i);
    }
    let irqs = Arc::new(Irqs::default());
    let net = InprocNet::new(record_tx);
    let mut queue = rxq.create_queue();
    queue.set_event_idx(true);
    net.attach_rx(
        queue,
        mem.clone(),
        InterruptTransport::from_handler(irqs.clone()),
    );
    (net, rxq, irqs)
}

#[test]
fn frames_before_activation_are_dropped() {
    let net = InprocNet::new(record_tx);
    assert_eq!(net.deliver(&iov(&[&[0; 12], &[1; 60]])), libc::EAGAIN);
    assert_eq!(net.rx_dropped(), 1);
}

#[test]
fn a_frame_lands_in_the_next_buffer_and_interrupts() {
    let mem = memory();
    let (net, rxq, irqs) = attached(&mem, 2);
    let hdr = [7u8; VNET_HDR_LEN];
    let frame: Vec<u8> = (0..1500u32).map(|i| i as u8).collect();
    assert_eq!(net.deliver(&iov(&[&hdr, &frame[..100], &frame[100..]])), 0);
    assert_eq!(rxq.used.idx.get(), 1);
    assert_eq!(received(&mem, &rxq, 0), [&hdr[..], &frame].concat());
    assert_eq!(irqs.0.load(Ordering::SeqCst), 1);
}

#[test]
fn a_full_queue_backlogs_frames_until_the_guest_refills_it() {
    let mem = memory();
    let (net, rxq, _) = attached(&mem, 1);
    for i in 1..=3u8 {
        assert_eq!(net.deliver(&iov(&[&[i; 80]])), 0);
    }
    assert_eq!(rxq.used.idx.get(), 1);
    // Armed: the guest kicks once it posts past what was consumed.
    assert_eq!(rxq.used.event.get(), 1);

    post(&rxq, 1);
    post(&rxq, 2);
    net.refill();
    assert_eq!(rxq.used.idx.get(), 3);
    for (n, i) in (1..=2u16).zip(2..=3u8) {
        assert_eq!(received(&mem, &rxq, n), [i; 80]);
    }
    assert_eq!(net.rx_dropped(), 0);
}

#[test]
fn the_backlog_is_bounded() {
    let mem = memory();
    let (net, _, _) = attached(&mem, 0);
    let frame = vec![0u8; 64 << 10];
    let fits = BACKLOG_MAX_BYTES / frame.len();
    for _ in 0..fits {
        assert_eq!(net.deliver(&iov(&[&frame])), 0);
    }
    assert_eq!(net.deliver(&iov(&[&frame])), libc::EAGAIN);
    assert_eq!(net.rx_dropped(), 1);
}

#[test]
fn the_worker_sends_scattered_frames_without_their_header() {
    let mem = memory();
    let rxq = VirtQueue::new(GuestAddress(0), &mem, 16);
    let txq = VirtQueue::new(GuestAddress(0x8000), &mem, 16);
    // Header and frame over three descriptors, the header split in two.
    let mut bytes = vec![0u8; VNET_HDR_LEN];
    let frame: Vec<u8> = (0..3000u32).map(|i| (i * 7) as u8).collect();
    bytes.extend_from_slice(&frame);
    let parts = [&bytes[..5], &bytes[5..1000], &bytes[1000..]];
    for (d, part) in parts.iter().enumerate() {
        let addr = 0x40000 + d as u64 * 0x2000;
        mem.write_slice(part, GuestAddress(addr)).unwrap();
        let flags = if d + 1 < parts.len() {
            VIRTQ_DESC_F_NEXT
        } else {
            0
        };
        txq.dtable[d].set(addr, part.len() as u32, flags, d as u16 + 1);
    }
    txq.avail.ring[0].set(0);
    txq.avail.idx.set(1);

    let net = Arc::new(InprocNet::new(record_tx));
    let irqs = Arc::new(Irqs::default());
    let event = || Arc::new(EventFd::new(EFD_NONBLOCK).unwrap());
    let mut worker = InprocWorker::new(
        DeviceQueue::new(rxq.create_queue(), event()),
        DeviceQueue::new(txq.create_queue(), event()),
        InterruptTransport::from_handler(irqs),
        mem.clone(),
        net,
    );
    SENT.with(|s| s.borrow_mut().clear());
    worker.process_tx();
    assert_eq!(SENT.with(|s| s.borrow().clone()), [frame]);
    assert_eq!(txq.used.idx.get(), 1);
}
