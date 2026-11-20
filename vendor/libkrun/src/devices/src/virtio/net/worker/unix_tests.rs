use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};
use std::sync::{Arc, Mutex};

use nix::sys::socket::{
    AddressFamily, MsgFlags, SockFlag, SockType, recv, send, setsockopt, socketpair, sockopt,
};
use utils::eventfd::{EFD_NONBLOCK, EventFd};
use vm_memory::{Bytes, GuestAddress, GuestMemoryMmap};

use super::super::inline_tx::INLINE_TX_MAX;
use super::NetWorker;
use crate::virtio::device::{InterruptHandler, InterruptType};
use crate::virtio::net::VNET_HDR_LEN;
use crate::virtio::net::device::VirtioNetBackend;
use crate::virtio::queue::tests::VirtQueue;
use crate::virtio::queue::{VIRTQ_DESC_F_NEXT, VIRTQ_DESC_F_WRITE};
use crate::virtio::{DeviceQueue, InterruptTransport};

const RX_BUFFERS: u64 = 0x10000;
const BUF_LEN: u32 = 0x1000;
const TX_BUFFERS: u64 = 0x20000;

/// Records the used index of the rx ring each time the guest is interrupted.
struct Recorder {
    mem: GuestMemoryMmap,
    used_idx: GuestAddress,
    seen: Mutex<Vec<u16>>,
}

impl InterruptHandler for Recorder {
    fn try_signal(&self, _: InterruptType) -> Result<(), crate::Error> {
        let idx: u16 = self.mem.read_obj(self.used_idx).unwrap();
        self.seen.lock().unwrap().push(idx);
        Ok(())
    }
}

struct Harness<'a> {
    rxq: VirtQueue<'a>,
    txq: VirtQueue<'a>,
    worker: NetWorker,
    peer: OwnedFd,
    irqs: Arc<Recorder>,
}

/// A worker on a datagram socketpair whose rx ring has `posted` writable
/// buffers out of `size` slots, with EVENT_IDX negotiated.
fn harness(mem: &GuestMemoryMmap, size: u16, posted: u16) -> Harness<'_> {
    let rxq = VirtQueue::new(GuestAddress(0), mem, size);
    let txq = VirtQueue::new(GuestAddress(0x8000), mem, size);
    for i in 0..posted {
        post(&rxq, i);
    }
    rxq.avail.idx.set(posted);

    let mut rx = rxq.create_queue();
    rx.set_event_idx(true);
    let mut tx = txq.create_queue();
    tx.set_event_idx(true);
    let irqs = Arc::new(Recorder {
        mem: mem.clone(),
        used_idx: rxq.used.idx.location,
        seen: Mutex::new(Vec::new()),
    });
    let (ours, peer) = socketpair(
        AddressFamily::Unix,
        SockType::Datagram,
        None,
        SockFlag::empty(),
    )
    .unwrap();
    // As Unixgram::open does: on macOS the send buffer caps a datagram.
    setsockopt(&ours, sockopt::SndBuf, &0x10000).unwrap();
    setsockopt(&peer, sockopt::RcvBuf, &0x40000).unwrap();
    let worker = NetWorker::new(
        DeviceQueue::new(rx, Arc::new(EventFd::new(EFD_NONBLOCK).unwrap())),
        DeviceQueue::new(tx, Arc::new(EventFd::new(EFD_NONBLOCK).unwrap())),
        InterruptTransport::from_handler(irqs.clone()),
        mem.clone(),
        0,
        VirtioNetBackend::UnixgramFd(ours.into_raw_fd()),
    )
    .unwrap();
    Harness {
        rxq,
        txq,
        worker,
        peer,
        irqs,
    }
}

/// Post rx buffer `i` in descriptor `i` and avail slot `i`.
fn post(rxq: &VirtQueue, i: u16) {
    let addr = RX_BUFFERS + u64::from(i) * u64::from(BUF_LEN);
    rxq.dtable[i as usize].set(addr, BUF_LEN, VIRTQ_DESC_F_WRITE, 0);
    rxq.avail.ring[i as usize].set(i);
}

fn send_frames(peer: &OwnedFd, n: usize) {
    for i in 0..n {
        let frame = [i as u8; 100];
        send(peer.as_raw_fd(), &frame, MsgFlags::empty()).unwrap();
    }
}

fn memory() -> GuestMemoryMmap {
    GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x40000)]).unwrap()
}

#[test]
fn rx_rearms_guest_kick_after_running_out_of_buffers() {
    let mem = memory();
    let mut h = harness(&mem, 8, 2);
    send_frames(&h.peer, 3);

    h.worker.process_backend_socket_readable();

    assert_eq!(h.rxq.used.idx.get(), 2);
    assert!(h.worker.rx_has_deferred_frame);
    // The guest must kick when it posts the next buffer (avail index 2).
    // Armed before the two buffers were consumed, avail_event would still
    // be 0 and the guest would never kick: with the socket drained into a
    // deferred frame, nothing would wake the worker again.
    assert_eq!(h.rxq.used.event.get(), 2);

    post(&h.rxq, 2);
    h.rxq.avail.idx.set(3);
    h.worker.rx_q.event.write(1).unwrap();
    h.worker.process_rx_queue_event();
    assert_eq!(h.rxq.used.idx.get(), 3);
    assert!(!h.worker.rx_has_deferred_frame);
    let third = mem.read_obj::<u8>(GuestAddress(
        RX_BUFFERS + 2 * u64::from(BUF_LEN) + VNET_HDR_LEN as u64,
    ));
    assert_eq!(third.unwrap(), 2);
}

#[test]
fn rx_interrupts_as_soon_as_the_guest_asks() {
    let mem = memory();
    let h = &mut harness(&mem, 8, 4);
    // used_event 0: interrupt once the first buffer is used.
    h.rxq.avail.event.set(0);
    send_frames(&h.peer, 4);
    h.worker.process_backend_socket_readable();
    assert_eq!(h.rxq.used.idx.get(), 4);
    // Woken after the first frame, not after the whole burst, so the guest
    // drains the ring while it is being filled.
    assert_eq!(*h.irqs.seen.lock().unwrap(), [1]);
}

#[test]
fn rx_does_not_interrupt_a_guest_that_is_polling() {
    let mem = memory();
    let h = &mut harness(&mem, 8, 4);
    // The guest is in NAPI and asked to be woken only past index 6.
    h.rxq.avail.event.set(6);
    send_frames(&h.peer, 3);
    h.worker.process_backend_socket_readable();
    assert_eq!(h.rxq.used.idx.get(), 3);
    assert!(h.irqs.seen.lock().unwrap().is_empty());
}

/// Queue `frame` for transmit behind a virtio-net header, split over the
/// descriptors `first..first + parts` (the header split too), in avail slot
/// `slot`.
fn queue_tx(
    mem: &GuestMemoryMmap,
    txq: &VirtQueue,
    slot: u16,
    first: u16,
    frame: &[u8],
    parts: u16,
) {
    let mut bytes = vec![0u8; VNET_HDR_LEN];
    bytes.extend_from_slice(frame);
    let chunk = bytes.len().div_ceil(parts as usize).max(1);
    for (k, part) in bytes.chunks(chunk).enumerate() {
        let d = first + k as u16;
        let addr = TX_BUFFERS + u64::from(d) * 0x2000;
        mem.write_slice(part, GuestAddress(addr)).unwrap();
        let last = (k + 1) * chunk >= bytes.len();
        let flags = if last { 0 } else { VIRTQ_DESC_F_NEXT };
        txq.dtable[d as usize].set(addr, part.len() as u32, flags, d + 1);
    }
    txq.avail.ring[slot as usize].set(first);
    txq.avail.idx.set(slot + 1);
}

fn received(peer: &OwnedFd) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 0x10000];
    while let Ok(n) = recv(peer.as_raw_fd(), &mut buf, MsgFlags::MSG_DONTWAIT) {
        out.push(buf[..n].to_vec());
    }
    out
}

fn tx_memory() -> GuestMemoryMmap {
    GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x40000)]).unwrap()
}

#[test]
fn tx_sends_a_scattered_frame_without_its_header() {
    let mem = tx_memory();
    let mut h = harness(&mem, 8, 0);
    // Five parts of 220 bytes: the 12-byte header and the frame both span
    // descriptors only at the first boundary.
    let frame: Vec<u8> = (0..1088u32).map(|i| i as u8).collect();
    queue_tx(&mem, &h.txq, 0, 0, &frame, 5);
    h.worker.process_tx_loop();
    assert_eq!(received(&h.peer), [frame]);
    assert_eq!(h.txq.used.idx.get(), 1);
}

#[test]
fn inline_kick_sends_small_frames_in_order() {
    let mem = tx_memory();
    let h = harness(&mem, 8, 0);
    let inline = h
        .worker
        .inline_tx()
        .expect("a datagram backend sends inline");
    queue_tx(&mem, &h.txq, 0, 0, &[1u8; 60], 1);
    queue_tx(&mem, &h.txq, 1, 1, &[2u8; 1500], 2);
    assert!(inline.kick());
    assert_eq!(received(&h.peer), [vec![1u8; 60], vec![2u8; 1500]]);
    assert_eq!(h.txq.used.idx.get(), 2);
}

#[test]
fn inline_kick_leaves_large_frames_to_the_worker() {
    let mem = tx_memory();
    let mut h = harness(&mem, 8, 0);
    let inline = h.worker.inline_tx().unwrap();
    let large = vec![3u8; INLINE_TX_MAX + 1];
    queue_tx(&mem, &h.txq, 0, 0, &[1u8; 60], 1);
    queue_tx(&mem, &h.txq, 1, 1, &large, 2);
    queue_tx(&mem, &h.txq, 2, 3, &[2u8; 60], 1);
    // The small frame ahead of the large one goes; the rest keeps its order
    // for the worker.
    assert!(!inline.kick());
    assert_eq!(received(&h.peer), [vec![1u8; 60]]);
    assert_eq!(h.txq.used.idx.get(), 1);
    h.worker.process_tx_loop();
    assert_eq!(received(&h.peer), [large, vec![2u8; 60]]);
    assert_eq!(h.txq.used.idx.get(), 3);
}

#[test]
fn inline_kick_does_not_wait_for_the_worker() {
    let mem = tx_memory();
    let h = harness(&mem, 8, 0);
    let inline = h.worker.inline_tx().unwrap();
    queue_tx(&mem, &h.txq, 0, 0, &[1u8; 60], 1);
    let held = h.worker.tx.lock().unwrap();
    assert!(!inline.kick());
    drop(held);
    assert!(received(&h.peer).is_empty());
    assert_eq!(h.txq.used.idx.get(), 0);
}

#[test]
fn inline_kick_hands_a_full_socket_to_the_worker() {
    let mem = tx_memory();
    let mut h = harness(&mem, 8, 0);
    let inline = h.worker.inline_tx().unwrap();
    let fd = h.worker.backend.raw_socket_fd();
    while send(fd, &[0u8; 1000], MsgFlags::MSG_DONTWAIT).is_ok() {}
    queue_tx(&mem, &h.txq, 0, 0, &[7u8; 60], 1);
    assert!(!inline.kick());
    assert_eq!(h.txq.used.idx.get(), 0);
    let backlog = received(&h.peer);
    assert!(backlog.iter().all(|f| f.len() == 1000));
    h.worker.process_tx_loop();
    assert_eq!(received(&h.peer), [vec![7u8; 60]]);
    assert_eq!(h.txq.used.idx.get(), 1);
}
