//! Host-to-guest data for a Unix proxy: one `recvmsg` fills many guest
//! receive buffers.
//!
//! Linux posts receive buffers of about 3.7 KiB, so receiving into one at a
//! time costs a system call per 3.7 KiB, and that call is most of what the
//! muxer thread does while the host streams data in. Here a batch of
//! buffers is popped first, the socket is read once straight into all of
//! them (bounded by the guest's credit), and the buffers that stayed empty
//! go back to the queue.

use std::io;
use std::num::Wrapping;

use super::super::packet::VsockPacket;
use super::{AsRawFd, ProxyStatus, RecvPkt, UnixProxy};

/// Guest buffers per `recvmsg`: about 230 KiB with Linux's buffer size.
const BATCH: usize = 64;

fn recv_into(fd: libc::c_int, iov: &mut [libc::iovec]) -> RecvPkt {
    // SAFETY: a zeroed msghdr is valid; it points at `iov`, which lives
    // across the call, and every iovec points at writable guest memory.
    let n = unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = iov.as_mut_ptr();
        msg.msg_iovlen = iov.len() as _;
        libc::recvmsg(fd, &mut msg, libc::MSG_DONTWAIT)
    };
    match n {
        n if n > 0 => RecvPkt::Read(n as usize),
        0 => RecvPkt::Eof,
        _ => match io::Error::last_os_error().raw_os_error() {
            Some(libc::EAGAIN) | Some(libc::EINTR) => RecvPkt::Error,
            _ => RecvPkt::Close,
        },
    }
}

/// How `got` bytes spread over buffers of the given sizes, in order.
pub(crate) fn spread(got: usize, sizes: &[usize]) -> Vec<usize> {
    let mut left = got;
    sizes
        .iter()
        .map(|&s| {
            let n = left.min(s);
            left -= n;
            n
        })
        .take_while(|&n| n > 0)
        .collect()
}

/// Move what the host socket has into guest receive buffers. Returns
/// whether any buffer was used and whether the guest's credit ran out,
/// like the one-buffer-at-a-time loop it replaces.
pub(crate) fn recv_pkts(proxy: &mut UnixProxy) -> (bool, bool) {
    let mut have_used = false;
    let mut wait_credit = false;
    let queue = proxy.queue.clone();
    let mut queue = queue.lock().unwrap();
    let fd = proxy.fd.as_raw_fd();

    loop {
        let credit = proxy.peer_avail_credit();
        if credit == 0 {
            wait_credit = true;
            break;
        }

        let mut room = credit;
        let mut pkts: Vec<(u16, VsockPacket)> = Vec::with_capacity(BATCH);
        let mut iov: Vec<libc::iovec> = Vec::with_capacity(BATCH);
        while pkts.len() < BATCH && room > 0 {
            let Some(head) = queue.pop(&proxy.mem) else {
                break;
            };
            let mut pkt = match VsockPacket::from_rx_virtq_head(&head) {
                Ok(pkt) => pkt,
                Err(e) => {
                    debug!("recv_pkts: RX queue error: {e:?}");
                    queue.undo_pop();
                    break;
                }
            };
            let Some(buf) = pkt.buf_mut().filter(|b| !b.is_empty()) else {
                queue.undo_pop();
                break;
            };
            let len = buf.len().min(room);
            room -= len;
            iov.push(libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: len,
            });
            pkts.push((head.index, pkt));
        }
        if pkts.is_empty() {
            break;
        }

        let capacity = credit - room;
        let got = match recv_into(fd, &mut iov) {
            RecvPkt::Read(n) => n,
            RecvPkt::Eof => {
                proxy.local_shutdown = true;
                0
            }
            RecvPkt::Close => {
                proxy.status = ProxyStatus::Closed;
                0
            }
            RecvPkt::Error | RecvPkt::WaitForCredit => 0,
        };

        let sizes: Vec<usize> = iov.iter().map(|v| v.iov_len).collect();
        let filled = spread(got, &sizes);
        for ((index, pkt), &cnt) in pkts.iter_mut().zip(&filled) {
            proxy.rx_cnt += Wrapping(cnt as u32);
            proxy.init_data_pkt(pkt);
            pkt.set_len(cnt as u32);
            let len = pkt.hdr().len() + cnt;
            proxy.push_cnt += Wrapping(len as u32);
            if let Err(e) = queue.add_used(&proxy.mem, *index, len as u32) {
                error!("failed to add used elements to the queue: {e:?}");
            }
            have_used = true;
        }
        for _ in filled.len()..pkts.len() {
            queue.undo_pop();
        }

        // A short read means the socket is drained, closed or failed.
        if got < capacity {
            break;
        }
    }

    (have_used, wait_credit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    #[test]
    fn spread_fills_buffers_in_order() {
        assert_eq!(spread(0, &[4, 4]), Vec::<usize>::new());
        assert_eq!(spread(3, &[4, 4]), vec![3]);
        assert_eq!(spread(4, &[4, 4]), vec![4]);
        assert_eq!(spread(6, &[4, 4]), vec![4, 2]);
        assert_eq!(spread(8, &[4, 4, 4]), vec![4, 4]);
    }

    #[test]
    fn one_recvmsg_scatters_over_buffers() {
        let (mut a, b) = UnixStream::pair().unwrap();
        a.write_all(b"abcdefghij").unwrap();
        let mut x = [0u8; 4];
        let mut y = [0u8; 4];
        let mut z = [0u8; 4];
        let mut iov = [&mut x[..], &mut y[..], &mut z[..]].map(|s| libc::iovec {
            iov_base: s.as_mut_ptr().cast(),
            iov_len: s.len(),
        });
        assert!(matches!(
            recv_into(b.as_raw_fd(), &mut iov),
            RecvPkt::Read(10)
        ));
        assert_eq!((&x, &y, &z[..2]), (b"abcd", b"efgh", &b"ij"[..]));
        assert!(matches!(recv_into(b.as_raw_fd(), &mut iov), RecvPkt::Error));
        drop(a);
        assert!(matches!(recv_into(b.as_raw_fd(), &mut iov), RecvPkt::Eof));
    }
}
