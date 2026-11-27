//! Guest TCP quick ACK end to end on real Kubernetes (k3s) pods, local Docker
//! bridges (`docker0`), and the `--cmdline kat.quickack=0` opt-out. Needs
//! internet access (to pull k3s and alpine) and the `kubectl` and `docker`
//! CLIs on `PATH`. Ignored by default; `cargo xtask test-vm` builds everything
//! and runs it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const BASE_IMAGE: &str = "alpine:3.24.4";
const TEST_IMAGE: &str = "kat-quickack-test:1";
const QUICKACK_PARAM: &str = "/sys/module/kat/parameters/quickack";

/// Tiny Python HTTP/1.1 keep-alive server that explicitly leaves Nagle enabled
/// (`TCP_NODELAY = 0`) and sends each response in two separate `send()` calls
/// (headers first, then a 10 KiB body — below the 65468-byte guest MSS).
/// Without quick ACK, the client's delayed-ACK timer stalls every keep-alive
/// request for ~40–50 ms.
const SPLIT_WRITE_SERVER_PY: &str = r#"
import fcntl, socket, struct, sys, termios, threading, time

BODY = b"x" * 10240
HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(BODY)).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
# 32 MiB sent in 4 x 8 MiB chunks (only 4 write/PSH boundaries) so sustained
# bulk measurements have tightly bounded drain-path ACK overhead.
BULK_CHUNK = b"b" * (8 * 1024 * 1024)
BULK_CHUNKS = 4
BULK_HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(BULK_CHUNK) * BULK_CHUNKS).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
SHORT_CHUNK = b"s" * 4096
SHORT_CHUNKS = 512
SHORT_HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(SHORT_CHUNK) * SHORT_CHUNKS).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
PACED_CHUNKS = 32
PACED_HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(SHORT_CHUNK) * PACED_CHUNKS).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
PACED_SHORT_CHUNK = b"q" * 6000
PACED_SHORT_CHUNKS = 48
PACED_SHORT_HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(PACED_SHORT_CHUNK) * PACED_SHORT_CHUNKS).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
PUSHED_CHUNK = b"p" * 60000
PUSHED_CHUNKS = 48
PUSHED_HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(PUSHED_CHUNK) * PUSHED_CHUNKS).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
THREE_MID = b"m" * 2048
THREE_TAIL = b"t" * 8192
THREE_HDR = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: " + str(len(THREE_MID) + len(THREE_TAIL)).encode() + b"\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
FULL_CHUNKS = 48
SPLIT20_FIRST_LEN = 20480
SPLIT20_SECOND = b"u" * 4096
_split20_hdr = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: XXXXX\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
SPLIT20_HDR = _split20_hdr.replace(
    b"XXXXX",
    str(SPLIT20_FIRST_LEN - len(_split20_hdr) + len(SPLIT20_SECOND)).encode(),
)
assert len(SPLIT20_HDR) == len(_split20_hdr)
SPLIT20_FIRST = SPLIT20_HDR + b"u" * (SPLIT20_FIRST_LEN - len(SPLIT20_HDR))
# 100000-byte first write spanning more than one 64 KiB MSS (it may arrive as
# one GSO/GRO skb whose short last segment carries PSH), then a 4 KiB tail
# Nagle holds behind the unacknowledged short remainder.
SPLIT100_FIRST_LEN = 100000
SPLIT100_SECOND = b"v" * 4096
SPLIT100_HDR = _split20_hdr.replace(
    b"XXXXX",
    str(SPLIT100_FIRST_LEN - len(_split20_hdr) + len(SPLIT100_SECOND) - 1).encode(),
)
assert len(SPLIT100_HDR) == len(_split20_hdr) + 1
SPLIT100_FIRST = SPLIT100_HDR + b"v" * (SPLIT100_FIRST_LEN - len(SPLIT100_HDR))
TWO_WRITE = 2048
_two_hdr = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Length: XXXX\r\n"
    b"Connection: keep-alive\r\n"
    b"\r\n"
)
# Two separate 2 KiB writes (header + body start, then the body rest): both
# far below the sender's MSS, so Nagle holds the second until the first is
# ACKed. Repeating it settles the receiver's adaptive rcv_mss at 2 KiB.
TWO_BODY = 2 * TWO_WRITE - len(_two_hdr)
TWO_HDR = _two_hdr.replace(b"XXXX", str(TWO_BODY).encode())
assert len(TWO_HDR) == len(_two_hdr)
TWO_FIRST = TWO_HDR + b"w" * (TWO_WRITE - len(TWO_HDR))
TWO_SECOND = b"w" * TWO_WRITE
# Linux SIOCOUTQ (== TIOCOUTQ): bytes not yet ACKed by the peer (unsent + in
# flight). SIOCOUTQNSD: bytes not yet sent.
SIOCOUTQ = termios.TIOCOUTQ
SIOCOUTQNSD = 0x894B

def wait_outq_empty(conn, req, what):
    deadline = time.monotonic() + 2.0
    while True:
        if struct.unpack("i", fcntl.ioctl(conn.fileno(), req, b"\0\0\0\0"))[0] <= 0:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("timed out waiting for " + what)
        time.sleep(min(0.0002, remaining))

def handle(conn):
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
        buf = b""
        while True:
            while b"\r\n\r\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
            req_hdr, buf = buf.split(b"\r\n\r\n", 1)
            if req_hdr.startswith(b"GET /bulk "):
                conn.sendall(BULK_HDR)
                for _ in range(BULK_CHUNKS):
                    conn.sendall(BULK_CHUNK)
            elif req_hdr.startswith(b"GET /short "):
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                conn.sendall(SHORT_HDR)
                for _ in range(SHORT_CHUNKS):
                    conn.sendall(SHORT_CHUNK)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
            elif req_hdr.startswith(b"GET /paced "):
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                conn.sendall(PACED_HDR)
                for _ in range(PACED_CHUNKS):
                    conn.sendall(SHORT_CHUNK)
                    time.sleep(0.001)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
            elif req_hdr.startswith(b"GET /pushed "):
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                conn.sendall(PUSHED_HDR)
                for _ in range(PUSHED_CHUNKS):
                    conn.sendall(PUSHED_CHUNK)
                    time.sleep(0.0005)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
            elif req_hdr.startswith(b"GET /three "):
                # Three sub-MSS writes with TCP_NODELAY=0. Wait (bounded) until
                # the peer has ACKed write 1 so write 2 goes out on its own
                # (hdr + 2 KiB < 4 KiB lowat), confirm write 2 left the send
                # queue, then queue write 3 (8 KiB) behind Nagle so it waits
                # on write 2's ACK: the second delayed-ACK deadline under test.
                conn.sendall(THREE_HDR)
                wait_outq_empty(conn, SIOCOUTQ, "ACK of the three-write header")
                conn.sendall(THREE_MID)
                wait_outq_empty(conn, SIOCOUTQNSD, "three-write middle chunk to be sent")
                conn.sendall(THREE_TAIL)
            elif req_hdr.startswith(b"GET /split20k "):
                # 20 KiB first write (header + body start), then a 4 KiB tail
                # Nagle holds until the first write is ACKed. Both are far
                # below the 64 KiB MSS but above any fixed small-write cutoff.
                conn.sendall(SPLIT20_FIRST)
                conn.sendall(SPLIT20_SECOND)
            elif req_hdr.startswith(b"GET /split100k "):
                conn.sendall(SPLIT100_FIRST)
                conn.sendall(SPLIT100_SECOND)
            elif req_hdr.startswith(b"GET /two2k "):
                conn.sendall(TWO_FIRST)
                conn.sendall(TWO_SECOND)
            elif req_hdr.startswith(b"GET /mss "):
                mss = str(conn.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG)).encode()
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Length: " + str(len(mss)).encode() + b"\r\n"
                    b"Connection: keep-alive\r\n"
                    b"\r\n" + mss
                )
            elif req_hdr.startswith(b"GET /pacedshort "):
                # Short, non-full-MSS PSH writes with TCP_NODELAY paced 12 ms
                # apart (between the capped timer and the stock 40 ms timer):
                # once the single per-batch capped timer is spent, stock
                # delayed-ACK batching must remain intact.
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                mss = conn.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG)
                assert len(PACED_SHORT_CHUNK) < mss, (len(PACED_SHORT_CHUNK), mss)
                conn.sendall(PACED_SHORT_HDR)
                for _ in range(PACED_SHORT_CHUNKS):
                    time.sleep(0.012)
                    conn.sendall(PACED_SHORT_CHUNK)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
            elif req_hdr.startswith(b"GET /fullmss "):
                # Full-sized PSH segments (exactly one MSS per write) paced
                # 12 ms apart: longer than the shortened delayed-ACK cap, shorter
                # than the stock 40 ms delayed-ACK timer, so stock TCP batches
                # several segments per ACK.
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                mss = conn.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG)
                chunk = b"f" * mss
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\n"
                    b"X-Kat-Mss: " + str(mss).encode() + b"\r\n"
                    b"Content-Length: " + str(mss * FULL_CHUNKS).encode() + b"\r\n"
                    b"Connection: keep-alive\r\n"
                    b"\r\n"
                )
                for _ in range(FULL_CHUNKS):
                    time.sleep(0.012)
                    conn.sendall(chunk)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
            else:
                conn.sendall(HDR)
                conn.sendall(BODY)
    finally:
        conn.close()

IPV6 = len(sys.argv) > 1 and sys.argv[1] == "--ipv6"

def serve(port, window_clamp=None):
    fam = socket.AF_INET6 if IPV6 else socket.AF_INET
    srv = socket.socket(fam, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if IPV6:
        srv.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    if window_clamp:
        # Inherited by accepted sockets before the handshake: the server
        # advertises at most this receive window, which bounds the CLIENT's
        # send MSS (tcp_bound_to_half_wnd) but not the server's responses.
        srv.setsockopt(socket.IPPROTO_TCP, socket.TCP_WINDOW_CLAMP, window_clamp)
    srv.bind(("::" if IPV6 else "0.0.0.0", port))
    srv.listen(32)
    while True:
        c, _ = srv.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()

threading.Thread(target=serve, args=(8082, 32768), daemon=True).start()
print("READY", flush=True)
serve(8080)
"#;

/// Keep-alive client that opens ONE persistent TCP connection to `host:8080`,
/// reads commands from stdin (`<count>` for split-write bursts, `bulk` for a
/// 32 MiB sustained download, `short` for an unpaced 512x4 KiB short-write
/// stream, `paced` for a 32x4 KiB paced short-write stream that lets the reader
/// drain between writes, `pushed` for a 48x60000B pushed multi-segment stream,
/// `fullmss` for 48 exactly-one-MSS PSH segments paced 12 ms apart,
/// `split20k <count>` / `split100k <count>` for 20 KiB or 100000 B first
/// writes followed by a 4 KiB tail, or
/// `rcvlowat <N>` to set `SO_RCVLOWAT` and gate reads on `select.poll()`),
/// and prints two space-separated floats flushed after each workload command.
/// Passing an optional count on argv runs a single burst and exits; passing
/// `<count> <path> <lowat> [port]` (path `two2k` or `split20k`) instead warms a
/// fresh connection with 30 of those responses and then times `<count>` of
/// them under `SO_RCVLOWAT=<lowat>`. Port 8082 of the split-write server clamps
/// its receive window to 32 KiB.
const KEEP_ALIVE_CLIENT_PY: &str = r#"
import os, select, socket, struct, sys, time

host = sys.argv[1]
req = b"GET / HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
three_req = b"GET /three HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
bulk_req = b"GET /bulk HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
short_req = b"GET /short HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
paced_req = b"GET /paced HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
paced_short_req = b"GET /pacedshort HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
pushed_req = b"GET /pushed HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
split20k_req = b"GET /split20k HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
split100k_req = b"GET /split100k HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
two2k_req = b"GET /two2k HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
fullmss_req = b"GET /fullmss HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"
mss_req = b"GET /mss HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"

def tcp_info_stats(sock):
    # Linux UAPI struct tcp_info (64-bit natural alignment):
    #   offset  16: __u32 tcpi_snd_mss
    #   offset 128: __u64 tcpi_bytes_received
    #   offset 136: __u32 tcpi_segs_out, __u32 tcpi_segs_in
    #   offset 152: __u32 tcpi_data_segs_in, __u32 tcpi_data_segs_out
    info = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 160)
    snd_mss = struct.unpack_from("=I", info, 16)[0]
    bytes_rx = struct.unpack_from("=Q", info, 128)[0]
    segs_out, _segs_in = struct.unpack_from("=II", info, 136)
    _data_segs_in, data_segs_out = struct.unpack_from("=II", info, 152)
    return snd_mss, bytes_rx, segs_out - data_segs_out

def delayed_acks():
    with open("/proc/net/netstat") as f:
        lines = f.read().splitlines()
    for i in range(0, len(lines) - 1, 2):
        keys = lines[i].split()
        vals = lines[i + 1].split()
        if keys and keys[0] == "TcpExt:" and "DelayedACKs" in keys:
            return int(vals[keys.index("DelayedACKs")])
    raise RuntimeError("DelayedACKs not found in /proc/net/netstat")

def header_value(hdr, name_lower):
    prefix = name_lower + b":"
    for line in hdr.split(b"\r\n")[1:]:
        if line.lower().startswith(prefix):
            return line.split(b":", 1)[1].strip()
    return None

def content_length(hdr):
    v = header_value(hdr, b"content-length")
    if v is None:
        raise RuntimeError("missing Content-Length")
    return int(v)

def apply_lowat(sock, n):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVLOWAT, n)
    eff = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVLOWAT)
    if eff != n:
        raise RuntimeError(
            f"SO_RCVLOWAT clamped by kernel: requested {n}, effective {eff}"
        )
    return eff

def recv_exact(sock, n, buf):
    lowered = False
    try:
        while len(buf) < n:
            rem = n - len(buf)
            if cur_lowat > 1 and rem < cur_lowat and not lowered:
                apply_lowat(sock, 1)
                lowered = True
            chunk = sock.recv(min(rem, 65536))
            if not chunk:
                raise RuntimeError("unexpected EOF")
            buf += chunk
    finally:
        if lowered:
            apply_lowat(sock, cur_lowat)
    return buf[:n], buf[n:]

def drain_exact(sock, n, buf):
    if len(buf) >= n:
        return buf[n:]
    rem = n - len(buf)
    lowered = False
    try:
        while rem > 0:
            if cur_lowat > 1 and rem < cur_lowat and not lowered:
                apply_lowat(sock, 1)
                lowered = True
            elif use_poll and rem >= cur_lowat:
                events = poller.poll(10000)
                if not events:
                    raise RuntimeError("poll timed out draining stream")
            chunk = sock.recv(min(rem, max(262144, cur_lowat)))
            if not chunk:
                raise RuntimeError("unexpected EOF in stream body")
            rem -= len(chunk)
    finally:
        if lowered:
            apply_lowat(sock, cur_lowat)
    return b""

def connect_sock(host, port, rcvbuf=None):
    infos = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    last_err = None
    for af, socktype, proto, _, sa in infos:
        s = socket.socket(af, socktype, proto)
        try:
            s.settimeout(10)
            if rcvbuf is not None:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
            s.connect(sa)
            if ":" in host:
                assert s.family == socket.AF_INET6, f"expected AF_INET6 for {host}, got {s.family}"
            return s
        except OSError as e:
            last_err = e
            s.close()
    raise last_err

is_bulkbench = len(sys.argv) > 2 and sys.argv[2] == "bulkbench"
rcvbuf = int(sys.argv[3]) if is_bulkbench else None
port = int(sys.argv[4]) if (is_bulkbench and len(sys.argv) > 4) else (
    int(sys.argv[5]) if (not is_bulkbench and len(sys.argv) > 5) else 8080
)
sock = connect_sock(host, port, rcvbuf=rcvbuf)
poller = select.poll()
poller.register(sock, select.POLLIN)
cur_lowat = 1
use_poll = False
buf = b""

def recv_one_response(req_bytes):
    global buf
    sock.sendall(req_bytes)
    if use_poll and not buf:
        events = poller.poll(10000)
        if not events:
            raise RuntimeError("poll timed out waiting for SO_RCVLOWAT")
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("unexpected EOF in headers")
        buf += chunk
    hdr, buf = buf.split(b"\r\n\r\n", 1)
    assert b"200 OK" in hdr, hdr
    body_len = content_length(hdr)
    body, buf = recv_exact(sock, body_len, buf)
    assert len(body) == body_len
    return hdr, body

def run_burst(count, req_bytes=req, report_delacks=False):
    times_ms = []
    delacks_before = delayed_acks() if report_delacks else 0
    t_all = time.monotonic()
    for _ in range(count):
        t0 = time.monotonic()
        recv_one_response(req_bytes)
        times_ms.append((time.monotonic() - t0) * 1000.0)
    total_ms = (time.monotonic() - t_all) * 1000.0
    delacks_delta = (delayed_acks() - delacks_before) if report_delacks else 0
    times_ms.sort()
    p50 = times_ms[len(times_ms) // 2]
    if report_delacks:
        print(f"{p50:.3f} {total_ms:.3f} {delacks_delta}", flush=True)
    else:
        print(f"{p50:.3f} {total_ms:.3f}", flush=True)

def measure_stream(req_bytes, expected_len, require_fullmss=False):
    global buf
    _, rx0, acks0 = tcp_info_stats(sock)
    t0 = time.monotonic()
    sock.sendall(req_bytes)
    if use_poll and not buf:
        events = poller.poll(10000)
        if not events:
            raise RuntimeError("poll timed out waiting for stream headers")
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(max(65536, cur_lowat))
        if not chunk:
            raise RuntimeError("unexpected EOF in stream headers")
        buf += chunk
    hdr, buf = buf.split(b"\r\n\r\n", 1)
    assert b"200 OK" in hdr, hdr
    cl = content_length(hdr)
    if require_fullmss:
        mss_hdr = header_value(hdr, b"x-kat-mss")
        if mss_hdr is None:
            raise RuntimeError(f"missing X-Kat-Mss header in /fullmss response: {hdr!r}")
        mss = int(mss_hdr)
        assert mss > 0, f"invalid X-Kat-Mss header: {mss}"
        assert cl == 48 * mss, f"/fullmss Content-Length {cl} != 48 * {mss}"
    else:
        assert expected_len is not None and cl == expected_len, (cl, expected_len)
    buf = drain_exact(sock, cl, buf)
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    _, rx1, acks1 = tcp_info_stats(sock)
    assert rx1 - rx0 >= cl, f"TCP_INFO bytes_received delta {rx1 - rx0} < {cl}"
    return acks1 - acks0, elapsed_ms

def run_stream(req_bytes, expected_len, require_fullmss=False):
    acks, elapsed_ms = measure_stream(req_bytes, expected_len, require_fullmss=require_fullmss)
    print(f"{acks} {elapsed_ms:.3f}", flush=True)

def set_lowat(n):
    global cur_lowat, use_poll
    cur_lowat = apply_lowat(sock, n)
    use_poll = cur_lowat > 1

if is_bulkbench:
    # Fresh connection with fixed SO_RCVBUF: enter pingpong mode and exhaust
    # initial quickack budget, warm up once with a bulk transfer, then report
    # median-of-3 per-socket pure ACK count and elapsed ms.
    for _ in range(25):
        recv_one_response(req)
    measure_stream(bulk_req, 32 * 1024 * 1024)
    samples = [measure_stream(bulk_req, 32 * 1024 * 1024) for _ in range(3)]
    samples.sort(key=lambda s: s[0])
    med_acks, med_ms = samples[1]
    print(f"{med_acks} {med_ms:.3f}", flush=True)
elif len(sys.argv) > 4:
    # Fresh connection: warm it with 30 responses of the given path at
    # SO_RCVLOWAT=1 (rcv_mss settles at the response's write size, pingpong
    # mode, quickack budget spent), then time <count> of them with the given
    # SO_RCVLOWAT so the first write alone cannot wake poll().
    mode = sys.argv[3]
    path_req = {
        "two2k": two2k_req,
        "split20k": split20k_req,
        "split20k_asym": split20k_req,
    }[mode]
    with open(os.devnull, "w") as devnull:
        stdout, sys.stdout = sys.stdout, devnull
        run_burst(30, path_req)
        sys.stdout = stdout
    if mode == "split20k_asym":
        # Verify the asymmetric condition on this exact socket: the response
        # sender has a jumbo send MSS (>= 65000) while advertising a low
        # receive MSS (<= 1460) in its SYN-ACK (seen here as tcpi_snd_mss).
        _, mss_body = recv_one_response(mss_req)
        sender_mss = int(mss_body.strip())
        client_snd_mss, _, _ = tcp_info_stats(sock)
        assert sender_mss >= 65000, f"expected jumbo sender MSS >= 65000, got {sender_mss}"
        assert client_snd_mss <= 1460, f"expected low peer advmss <= 1460, got client snd_mss={client_snd_mss}"
    set_lowat(int(sys.argv[4]))
    run_burst(int(sys.argv[2]), path_req)
elif len(sys.argv) > 2:
    run_burst(int(sys.argv[2]))
else:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        if line == "bulk":
            run_stream(bulk_req, 32 * 1024 * 1024)
        elif line == "short":
            run_stream(short_req, 512 * 4096)
        elif line == "paced":
            run_stream(paced_req, 32 * 4096)
        elif line == "pacedshort":
            run_stream(paced_short_req, 48 * 6000)
        elif line == "pushed":
            run_stream(pushed_req, 48 * 60000)
        elif line == "fullmss":
            run_stream(fullmss_req, None, require_fullmss=True)
        elif line.startswith("three_delack "):
            run_burst(int(line.split()[1]), three_req, report_delacks=True)
        elif line.startswith("delack "):
            run_burst(int(line.split()[1]), req, report_delacks=True)
        elif line.startswith("three "):
            run_burst(int(line.split()[1]), three_req)
        elif line.startswith("split20k "):
            run_burst(int(line.split()[1]), split20k_req)
        elif line.startswith("split100k "):
            run_burst(int(line.split()[1]), split100k_req)
        elif line.startswith("rcvlowat "):
            set_lowat(int(line.split()[1]))
            print(f"{cur_lowat} 0.000", flush=True)
        else:
            run_burst(int(line))
sock.close()
"#;

/// Receiver side of the accepted-connection near-MSS probe, listening on
/// port 8081. The receiving socket is the *accepted* one, so the handshake
/// has already deducted the TCP timestamp option from its `advmss`. Each
/// message is a near-MSS first write plus a 1 KiB tail; the receiver polls
/// with `SO_RCVLOWAT` above the first write, reads the whole message, and
/// replies with one byte. Each phase of messages is announced as
/// `<first> <tail> <count>\n`.
const NEAR_MSS_RECEIVER_PY: &str = r#"
import select, socket, threading

def set_lowat(conn, n):
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVLOWAT, n)
    eff = conn.getsockopt(socket.SOL_SOCKET, socket.SO_RCVLOWAT)
    if eff != n:
        raise RuntimeError(
            f"SO_RCVLOWAT clamped by kernel: requested {n}, effective {eff}"
        )

def recv_line(conn):
    line = b""
    while not line.endswith(b"\n"):
        b = conn.recv(1)
        if not b:
            return None
        line += b
    return line

def handle(conn):
    try:
        poller = select.poll()
        poller.register(conn, select.POLLIN)
        while True:
            # Each phase starts with "<first> <tail> <count>\n" at
            # SO_RCVLOWAT=1. Arm the phase watermark before sending the
            # acknowledgment that releases message 0.
            set_lowat(conn, 1)
            meta = recv_line(conn)
            if meta is None:
                return
            first, tail, count = (int(x) for x in meta.split())
            total = first + tail
            lowat = first + tail // 2
            if count > 0:
                set_lowat(conn, lowat)
            conn.sendall(b"k")
            for i in range(count):
                if not poller.poll(10000):
                    raise RuntimeError("poll timed out")
                got = 0
                lowered = False
                while got < total:
                    if total - got < lowat and not lowered:
                        set_lowat(conn, 1)
                        lowered = True
                    chunk = conn.recv(total - got)
                    if not chunk:
                        return
                    got += len(chunk)
                if i + 1 < count:
                    if lowered:
                        set_lowat(conn, lowat)
                elif not lowered:
                    set_lowat(conn, 1)
                conn.sendall(b"k")
    finally:
        conn.close()

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# A fixed large buffer keeps the window open, so poll() wakes only on
# SO_RCVLOWAT rather than on receive-window pressure, and lets the sender's
# MSS grow past the half-window bound to the full link MSS.
srv.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
srv.bind(("0.0.0.0", 8081))
srv.listen(8)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
"#;

/// Sender side of the accepted-connection near-MSS probe (Nagle enabled).
/// Connects to `argv[1]:8081`, first grows the receiver's advertised window
/// with 60000 B + 1 KiB messages until the sender's MSS (which Linux caps at
/// half the peer's largest window) reaches the full link MSS, asserts it is
/// near the 64 KiB link MSS, then sends 30 warm-up and `argv[2]` timed
/// messages of an `MSS - 8` first write (just below the full-sized payload,
/// above it if the timestamp option were deducted twice) and a 1 KiB tail that
/// Nagle holds until the first write is ACKed. Prints `<p50> <total>`.
const NEAR_MSS_SENDER_PY: &str = r#"
import socket, sys, time

sock = socket.create_connection((sys.argv[1], 8081), timeout=10)
sock.settimeout(10)
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)

def phase(first, tail, count):
    sock.sendall(f"{first} {tail} {count}\n".encode())
    if sock.recv(1) != b"k":
        raise RuntimeError("bad phase ack")
    first_buf, tail_buf = b"n" * first, b"t" * tail
    times = []
    for _ in range(count):
        t0 = time.monotonic()
        sock.sendall(first_buf)
        sock.sendall(tail_buf)
        if sock.recv(1) != b"k":
            raise RuntimeError("bad reply")
        times.append((time.monotonic() - t0) * 1000.0)
    return times

phase(60000, 1024, 20)
mss = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG)
# struct tcp_info byte 5 is tcpi_options; bit 0 is TCPI_OPT_TIMESTAMPS, which
# the case under test needs.
opts = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 104)
assert opts[5] & 1, "TCP timestamps not negotiated"
assert mss >= 65000, f"sender MSS {mss} did not reach the 64 KiB link MSS"
first = mss - 8
phase(first, 1024, 30)
t_all = time.monotonic()
times = sorted(phase(first, 1024, int(sys.argv[2])))
total = (time.monotonic() - t_all) * 1000.0
print(f"{times[len(times) // 2]:.3f} {total:.3f}", flush=True)
"#;

/// Probes whether Linux's delayed-ACK timer fires (`TcpExt: DelayedACKs` in
/// `/proc/net/netstat`) on a keep-alive TCP connection to `host:port`.
/// Warms up with 25 quick exchanges (draining initial quickacks and entering
/// pingpong mode), then issues 6 exchanges pausing 100 ms after reading each
/// response so the 40–50 ms delayed-ACK timer fires unless quick ACK is active.
/// Prints `<delta>` (change in `DelayedACKs`) to stdout.
const DELACK_PROBE_PY: &str = r#"
import socket, sys, time

host = sys.argv[1]
port = int(sys.argv[2])
req = b"GET / HTTP/1.1\r\nHost: test\r\nConnection: keep-alive\r\n\r\n"

def delayed_acks():
    with open("/proc/net/netstat") as f:
        lines = f.read().splitlines()
    for i in range(0, len(lines) - 1, 2):
        keys = lines[i].split()
        vals = lines[i + 1].split()
        if keys and keys[0] == "TcpExt:" and "DelayedACKs" in keys:
            return int(vals[keys.index("DelayedACKs")])
    raise RuntimeError("DelayedACKs not found in /proc/net/netstat")

def recv_resp(sock, buf):
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("unexpected EOF in headers")
        buf += chunk
    hdr, buf = buf.split(b"\r\n\r\n", 1)
    cl = 0
    for line in hdr.split(b"\r\n")[1:]:
        if line.lower().startswith(b"content-length:"):
            cl = int(line.split(b":", 1)[1].strip())
    while len(buf) < cl:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("unexpected EOF in body")
        buf += chunk
    return buf[cl:]

sock = socket.create_connection((host, port), timeout=10)
sock.settimeout(10)
buf = b""
for _ in range(25):
    sock.sendall(req)
    buf = recv_resp(sock, buf)

before = delayed_acks()
for _ in range(6):
    # Re-enter pingpong mode after the previous delayed-ACK timer expiry
    # exited it (TCP_PINGPONG_THRESH = 3), then pause past the 40-50 ms timer.
    for _ in range(4):
        sock.sendall(req)
        buf = recv_resp(sock, buf)
    time.sleep(0.10)
after = delayed_acks()
sock.close()
print(after - before)
"#;

/// Dumps every IPv4 and IPv6 route in every table of the current network
/// namespace with `ip -j route show table all` and inspects the structured
/// `metrics` of each one. Prints `routes=<n> eth0=<n> quickack=<n>` and then
/// one JSON line per route that carries a QUICKACK metric (locked or not).
const ROUTE_CHECK_PY: &str = r#"
import json, subprocess

def has_quickack(v):
    if isinstance(v, dict):
        return any(k == "quickack" or has_quickack(x) for k, x in v.items())
    if isinstance(v, list):
        return any(has_quickack(x) for x in v)
    return False

routes = []
for fam in ("-4", "-6"):
    out = subprocess.run(
        ["ip", "-j", fam, "route", "show", "table", "all"],
        check=True, capture_output=True, text=True,
    ).stdout
    routes += json.loads(out or "[]")
eth0 = sum(1 for r in routes if r.get("dev") == "eth0")
bad = [r for r in routes if has_quickack(r.get("metrics", []))]
print(f"routes={len(routes)} eth0={eth0} quickack={len(bad)}")
for r in bad:
    print(json.dumps(r))
"#;

/// Installs a `/32` route to `argv[1]` over `eth0` whose RTAX_QUICKACK metric
/// is 0 and locked (RTAX_LOCK bit 15). iproute2 has no `lock` keyword for
/// `quickack`, so this sends RTM_NEWROUTE over rtnetlink directly.
const LOCKED_ROUTE_PY: &str = r#"
import socket, struct, sys

RTM_NEWROUTE, NLMSG_ERROR = 24, 2
NLM_F_REQUEST, NLM_F_ACK, NLM_F_REPLACE, NLM_F_CREATE = 0x1, 0x4, 0x100, 0x400
RTA_DST, RTA_OIF, RTA_METRICS = 1, 4, 8
RTAX_LOCK, RTAX_QUICKACK = 1, 15
RT_TABLE_MAIN, RTPROT_BOOT, RT_SCOPE_LINK, RTN_UNICAST = 254, 3, 253, 1

def rta(kind, payload):
    n = 4 + len(payload)
    return struct.pack("=HH", n, kind) + payload + b"\0" * (-n % 4)

metrics = rta(RTAX_LOCK, struct.pack("=I", 1 << RTAX_QUICKACK))
metrics += rta(RTAX_QUICKACK, struct.pack("=I", 0))
body = struct.pack("=BBBBBBBBI", socket.AF_INET, 32, 0, 0, RT_TABLE_MAIN,
                   RTPROT_BOOT, RT_SCOPE_LINK, RTN_UNICAST, 0)
body += rta(RTA_DST, socket.inet_aton(sys.argv[1]))
body += rta(RTA_OIF, struct.pack("=i", socket.if_nametoindex("eth0")))
body += rta(RTA_METRICS, metrics)
flags = NLM_F_REQUEST | NLM_F_ACK | NLM_F_REPLACE | NLM_F_CREATE
msg = struct.pack("=IHHII", 16 + len(body), RTM_NEWROUTE, flags, 1, 0) + body

nl = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE)
nl.bind((0, 0))
nl.send(msg)
resp = nl.recv(65536)
kind = struct.unpack_from("=H", resp, 4)[0]
err = struct.unpack_from("=i", resp, 16)[0]
if kind != NLMSG_ERROR or err != 0:
    raise SystemExit(f"RTM_NEWROUTE failed: type={kind} error={err}")
"#;

/// Runs [`ROUTE_CHECK_PY`] output through assertions: some routes exist,
/// at least one uses `eth0`, and none carries a QUICKACK metric.
fn assert_no_quickack_routes(out: &str, where_: &str) {
    let summary = out
        .lines()
        .next()
        .unwrap_or_else(|| panic!("{where_}: empty route check output"));
    let field = |name: &str| -> u64 {
        summary
            .split_whitespace()
            .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("{where_}: bad route check summary {summary:?}"))
    };
    assert!(field("routes") > 0, "{where_}: no routes found:\n{out}");
    assert!(
        field("eth0") > 0,
        "{where_}: expected an eth0 route:\n{out}"
    );
    assert_eq!(
        field("quickack"),
        0,
        "{where_}: routes must not carry a QUICKACK metric (quick ACK lives in the kernel, not route rewrites):\n{out}"
    );
}

fn root() -> PathBuf {
    // Canonical, so Unix socket paths stay under macOS's 104-byte limit.
    std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap()
}

fn katamaran() -> PathBuf {
    root().join("target/release/katamaran")
}

fn run_child_with_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    let stdout_rx = child.stdout.take().map(|mut pipe| {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = pipe.read_to_end(&mut buf);
            let _ = tx.send(buf);
        });
        rx
    });
    let stderr_rx = child.stderr.take().map(|mut pipe| {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = pipe.read_to_end(&mut buf);
            let _ = tx.send(buf);
        });
        rx
    });

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let stdout = stdout_rx
                    .and_then(|rx| rx.recv_timeout(Duration::from_secs(5)).ok())
                    .unwrap_or_default();
                let stderr = stderr_rx
                    .and_then(|rx| rx.recv_timeout(Duration::from_secs(5)).ok())
                    .unwrap_or_default();
                return Ok(Output {
                    status,
                    stdout,
                    stderr,
                });
            }
            Ok(None) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    let _ = child.kill();
                    let _ = child.wait();
                    let stdout = stdout_rx
                        .and_then(|rx| rx.recv_timeout(Duration::from_secs(1)).ok())
                        .unwrap_or_default();
                    let stderr = stderr_rx
                        .and_then(|rx| rx.recv_timeout(Duration::from_secs(1)).ok())
                        .unwrap_or_default();
                    return Err(format!(
                        "timed out after {:.1}s\nstdout:\n{}\nstderr:\n{}",
                        timeout.as_secs_f64(),
                        String::from_utf8_lossy(&stdout),
                        String::from_utf8_lossy(&stderr),
                    ));
                }
                thread::sleep(left.min(Duration::from_millis(25)));
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("wait failed: {e}"));
            }
        }
    }
}

fn run_output_timeout(cmd: &mut Command, timeout: Duration) -> Result<Output, String> {
    let child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn failed: {e}"))?;
    run_child_with_timeout(child, timeout)
}

fn cli(home: &Path, user_home: &Path, args: &[&str]) -> Output {
    let mut cmd = Command::new(katamaran());
    cmd.args(args)
        .env("KATAMARAN_HOME", home)
        .env("HOME", user_home);
    run_output_timeout(&mut cmd, Duration::from_secs(240))
        .unwrap_or_else(|e| panic!("run katamaran {args:?} failed: {e}"))
}

fn ok(out: &Output, what: &str) -> String {
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{what} failed ({}):\nstdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

fn ok_result(res: Result<Output, String>, what: &str) -> String {
    let out = res.unwrap_or_else(|e| panic!("{what} failed: {e}"));
    ok(&out, what)
}

fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut(Duration) -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {what}");
        if f(remaining) {
            return;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "timed out waiting for {what}");
        thread::sleep(left.min(Duration::from_millis(200)));
    }
}

struct Daemon {
    child: Child,
    home: PathBuf,
}

impl Daemon {
    fn spawn(home: &Path, user_home: &Path) -> Daemon {
        std::fs::create_dir_all(home).unwrap();
        std::fs::create_dir_all(user_home).unwrap();
        let child = Command::new(katamaran())
            .arg("daemon")
            .env("KATAMARAN_HOME", home)
            .env("HOME", user_home)
            // Bind the *.k8s.kat.local responder to an ephemeral port instead of
            // the one in /etc/resolver/k8s.kat.local, which the developer's own
            // daemon may hold (ISOLATED_DNS_ENV in src/k8s.rs).
            .env("KATAMARAN_ISOLATED_DNS", "1")
            .env("KATAMARAN_GUEST_DIR", root().join("guest/out"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn katamaran daemon");
        let d = Daemon {
            child,
            home: home.to_path_buf(),
        };
        let sock = home.join("run/daemon.sock");
        wait_until("the daemon socket", Duration::from_secs(10), |_| {
            UnixStream::connect(&sock).is_ok()
        });
        d
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
            for _ in 0..600 {
                if let Ok(Some(_)) = self.child.try_wait() {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct ContainerGuard {
    docker_host: String,
    id: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = docker_timeout(
            &self.docker_host,
            &["rm", "-f", &self.id],
            Duration::from_secs(15),
        );
    }
}

struct NetworkGuard {
    docker_host: String,
    name: String,
}

impl Drop for NetworkGuard {
    fn drop(&mut self) {
        let _ = docker_timeout(
            &self.docker_host,
            &["network", "rm", &self.name],
            Duration::from_secs(15),
        );
    }
}

struct QuickackRestoreGuard {
    docker_host: String,
    restored: bool,
}

impl QuickackRestoreGuard {
    fn restore(mut self) {
        ok_result(
            docker_timeout(
                &self.docker_host,
                &[
                    "run",
                    "--rm",
                    "--privileged",
                    TEST_IMAGE,
                    "sh",
                    "-c",
                    &format!("echo 1 > {QUICKACK_PARAM}"),
                ],
                Duration::from_secs(30),
            ),
            "re-enable kat.quickack",
        );
        self.restored = true;
    }
}

impl Drop for QuickackRestoreGuard {
    fn drop(&mut self) {
        if !self.restored {
            let _ = docker_timeout(
                &self.docker_host,
                &[
                    "run",
                    "--rm",
                    "--privileged",
                    TEST_IMAGE,
                    "sh",
                    "-c",
                    &format!("echo 1 > {QUICKACK_PARAM}"),
                ],
                Duration::from_secs(15),
            );
        }
    }
}

fn kubectl_timeout(kubeconfig: &Path, args: &[&str], timeout: Duration) -> Result<Output, String> {
    let mut cmd = Command::new("kubectl");
    cmd.arg("--kubeconfig").arg(kubeconfig).args(args);
    run_output_timeout(&mut cmd, timeout)
}

fn kubectl(kubeconfig: &Path, args: &[&str]) -> Output {
    kubectl_timeout(kubeconfig, args, Duration::from_secs(60))
        .unwrap_or_else(|e| panic!("run kubectl {args:?} failed: {e}"))
}

fn docker_timeout(docker_host: &str, args: &[&str], timeout: Duration) -> Result<Output, String> {
    let mut cmd = Command::new("docker");
    cmd.env("DOCKER_HOST", docker_host)
        .env_remove("DOCKER_CONTEXT")
        .env_remove("BUILDX_BUILDER")
        .args(args);
    run_output_timeout(&mut cmd, timeout)
}

fn docker(docker_host: &str, args: &[&str]) -> Output {
    docker_timeout(docker_host, args, Duration::from_secs(60))
        .unwrap_or_else(|e| panic!("run docker {args:?} failed: {e}"))
}

fn build_test_image(docker_host: &str) {
    let mut build = Command::new("docker")
        .env("DOCKER_HOST", docker_host)
        // Keep the build (and its result) on the test VM's engine: a selected
        // context or Buildx builder could build elsewhere and never load the
        // image that `kat k8s sync-images` mirrors into k3s.
        .env_remove("DOCKER_CONTEXT")
        .env_remove("BUILDX_BUILDER")
        .args(["build", "-q", "-t", TEST_IMAGE, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn docker build");
    write!(
        build.stdin.take().unwrap(),
        "FROM {BASE_IMAGE}\nRUN apk add --no-cache python3 iproute2\n"
    )
    .unwrap();
    ok_result(
        run_child_with_timeout(build, Duration::from_secs(180)),
        "build test image",
    );
}

struct HostHttpServer {
    port: u16,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl HostHttpServer {
    /// Minimal keep-alive HTTP/1.1 server on the Mac host (`127.0.0.1`),
    /// reached by guest containers through `host.docker.internal` over `eth0`.
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind host HTTP server");
        listener
            .set_nonblocking(true)
            .expect("set nonblocking on host listener");
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = stop.clone();
        let handle = thread::spawn(move || {
            let resp =
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\npong";
            while !stop_bg.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                        let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                        let _ = stream.set_nodelay(true);
                        thread::spawn(move || {
                            let mut buf = Vec::new();
                            let mut tmp = [0u8; 1024];
                            loop {
                                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                    match stream.read(&mut tmp) {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => buf.extend_from_slice(&tmp[..n]),
                                    }
                                }
                                let pos =
                                    buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                                buf.drain(..pos);
                                if stream.write_all(resp).is_err() {
                                    return;
                                }
                            }
                        });
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            port,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for HostHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn parse_latencies(out: &str, what: &str) -> (f64, f64) {
    let line = out
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_else(|| panic!("{what}: empty output"));
    let mut parts = line.split_whitespace();
    let p50: f64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{what}: bad p50 in {line:?}"));
    let total: f64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{what}: bad total in {line:?}"));
    (p50, total)
}

fn parse_latencies_delacks(out: &str, what: &str) -> (f64, f64, u64) {
    let line = out
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_else(|| panic!("{what}: empty output"));
    let mut parts = line.split_whitespace();
    let p50: f64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{what}: bad p50 in {line:?}"));
    let total: f64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{what}: bad total in {line:?}"));
    let delacks: u64 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{what}: bad delacks in {line:?}"));
    (p50, total, delacks)
}

/// Reuse a locally cached k3s binary from the user's real `~/.katamaran/cache/k3s`
/// if present so repeated test runs avoid re-downloading ~60 MiB.
fn seed_k3s_cache(home: &Path) {
    let Some(real_home) = std::env::var_os("HOME") else {
        return;
    };
    let src_root = PathBuf::from(real_home).join(".katamaran/cache/k3s");
    let Ok(entries) = std::fs::read_dir(&src_root) else {
        return;
    };
    let dst_root = home.join("cache/k3s");
    for entry in entries.flatten() {
        let Ok(files) = std::fs::read_dir(entry.path()) else {
            continue;
        };
        for file in files.flatten() {
            let name = file.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            if name_str.starts_with("k3s-") && file.path().is_file() {
                let dst_dir = dst_root.join(entry.file_name());
                if std::fs::create_dir_all(&dst_dir).is_ok() {
                    let _ = std::fs::copy(file.path(), dst_dir.join(&name));
                }
            }
        }
    }
}

#[test]
#[ignore = "boots the system VM with k3s; run with `cargo xtask test-vm`"]
fn pod_and_bridge_split_write_keepalive_has_no_delack_stall() {
    let home = root().join(format!("target/tqa-{}", std::process::id()));
    let user_home = home.join("user-home");
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&user_home).unwrap();
    seed_k3s_cache(&home);

    ok(
        &cli(
            &home,
            &user_home,
            &[
                "config",
                "set",
                "cpus=2",
                "memory=4G",
                "mount=off",
                "auto_forward=off",
            ],
        ),
        "config set",
    );

    let _daemon = Daemon::spawn(&home, &user_home);
    ok(&cli(&home, &user_home, &["start"]), "kat start");
    ok(&cli(&home, &user_home, &["k8s", "start"]), "kat k8s start");

    let kubeconfig = home.join("run/kubeconfig.yaml");
    assert!(
        kubeconfig.is_file(),
        "missing kubeconfig at {}",
        kubeconfig.display()
    );
    let docker_host = format!("unix://{}", home.join("run/docker.sock").display());

    // Build a single alpine image containing python3 + iproute2 and sync it
    // into k3s so pods do not depend on in-cluster DNS or repeated apk fetches.
    build_test_image(&docker_host);
    ok(
        &cli(&home, &user_home, &["k8s", "sync-images"]),
        "kat k8s sync-images",
    );

    // Verify the guest kernel exposes kat.quickack enabled by default.
    let param_val = ok(
        &docker(
            &docker_host,
            &["run", "--rm", TEST_IMAGE, "cat", QUICKACK_PARAM],
        ),
        "read kat.quickack kernel parameter",
    );
    assert_eq!(
        param_val.trim(),
        "Y",
        "expected {QUICKACK_PARAM} to default to Y"
    );

    // Launch two ordinary non-host-network pods.
    for name in ["qa-peer", "qa-probe"] {
        ok(
            &kubectl(
                &kubeconfig,
                &[
                    "run",
                    name,
                    "--restart=Never",
                    &format!("--image={TEST_IMAGE}"),
                    "--image-pull-policy=Never",
                    "--",
                    // The test's daemon guard tears the whole VM down; a finite
                    // sleep could make the pod terminal mid-test.
                    "sleep",
                    "infinity",
                ],
            ),
            &format!("create pod {name}"),
        );
    }
    for name in ["qa-peer", "qa-probe"] {
        ok_result(
            kubectl_timeout(
                &kubeconfig,
                &[
                    "wait",
                    "--for=condition=Ready",
                    "--timeout=120s",
                    &format!("pod/{name}"),
                ],
                Duration::from_secs(150),
            ),
            &format!("wait for pod {name}"),
        );
    }

    let peer_ip = ok(
        &kubectl(
            &kubeconfig,
            &["get", "pod", "qa-peer", "-o", "jsonpath={.status.podIP}"],
        ),
        "get qa-peer IP",
    );
    let peer_ip = peer_ip.trim().to_string();
    assert!(!peer_ip.is_empty(), "qa-peer has empty podIP");

    // Start the split-write keep-alive server in qa-peer.
    let _peer_srv = ChildGuard(
        Command::new("kubectl")
            .arg("--kubeconfig")
            .arg(&kubeconfig)
            .args([
                "exec",
                "qa-peer",
                "--",
                "python3",
                "-c",
                SPLIT_WRITE_SERVER_PY,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start split-write server in qa-peer"),
    );

    wait_until("qa-peer server ready", Duration::from_secs(15), |left| {
        kubectl_timeout(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &peer_ip,
                "1",
            ],
            left.min(Duration::from_secs(5)),
        )
        .is_ok_and(|o| o.status.success())
    });

    // Receiver for the accepted-connection near-MSS probe (qa-peer:8081).
    let _peer_recv = ChildGuard(
        Command::new("kubectl")
            .arg("--kubeconfig")
            .arg(&kubeconfig)
            .args([
                "exec",
                "qa-peer",
                "--",
                "python3",
                "-c",
                NEAR_MSS_RECEIVER_PY,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start near-MSS receiver in qa-peer"),
    );
    let near_mss = |count: &str, timeout: Duration| -> Result<Output, String> {
        kubectl_timeout(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                NEAR_MSS_SENDER_PY,
                &peer_ip,
                count,
            ],
            timeout,
        )
    };
    wait_until(
        "qa-peer near-MSS receiver ready",
        Duration::from_secs(30),
        |left| {
            kubectl_timeout(
                &kubeconfig,
                &[
                    "exec",
                    "qa-probe",
                    "--",
                    "python3",
                    "-c",
                    "import socket, sys; socket.create_connection((sys.argv[1], 8081), timeout=2).close()",
                    &peer_ip,
                ],
                left.min(Duration::from_secs(5)),
            )
            .is_ok_and(|o| o.status.success())
        },
    );
    // Accepted socket receiving a write just below the full-sized payload
    // (MSS - 8) under SO_RCVLOWAT: the handshake already deducted the TCP
    // timestamp option from its advmss, so deducting it again would misjudge
    // that write as full-sized and leave the Nagle-held tail waiting for the
    // stock delayed-ACK timer.
    let run_near_mss = |what: &str| -> (f64, f64) {
        let out = ok_result(near_mss("15", Duration::from_secs(60)), what);
        parse_latencies(&out, what)
    };
    let (near_on_p50, near_on_total) =
        run_near_mss("accepted-socket near-MSS split write (quickack=Y)");
    eprintln!(
        "pod-to-pod accepted-socket near-MSS split write (quickack=Y): p50={near_on_p50:.2}ms total={near_on_total:.2}ms"
    );
    assert!(
        near_on_p50 < 15.0,
        "accepted-socket near-MSS split write stalled with quickack=Y (p50={near_on_p50:.2}ms total={near_on_total:.2}ms)"
    );

    // 1) Open ONE persistent keep-alive TCP connection from qa-probe to qa-peer
    //    and exercise three bursts on that exact established socket:
    //    - quickack=Y (default): fast (< 1 ms/req, no delayed-ACK stall)
    //    - toggle kat.quickack=0 at runtime: ALREADY-ESTABLISHED socket
    //      immediately hits the ~40–50 ms delayed-ACK stall
    //    - toggle kat.quickack=1 at runtime: SAME established socket
    //      immediately returns to < 1 ms/req
    let mut client = ChildGuard(
        Command::new("kubectl")
            .arg("--kubeconfig")
            .arg(&kubeconfig)
            .args([
                "exec",
                "-i",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &peer_ip,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start persistent keep-alive client in qa-probe"),
    );
    let mut client_in = client.0.stdin.take().unwrap();
    let client_stdout = client.0.stdout.take().unwrap();
    let (line_tx, line_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(client_stdout).lines() {
            match line {
                Ok(l) => {
                    if line_tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    let mut send_raw = |cmd: &str, what: &str| -> String {
        writeln!(client_in, "{cmd}").expect("write client command");
        client_in.flush().expect("flush client command");
        line_rx
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|e| panic!("{what}: timed out or closed waiting for result: {e}"))
    };
    fn send_cmd(
        send_raw: &mut impl FnMut(&str, &str) -> String,
        cmd: &str,
        what: &str,
    ) -> (f64, f64) {
        let line = send_raw(cmd, what);
        parse_latencies(&line, what)
    }
    fn send_cmd_delacks(
        send_raw: &mut impl FnMut(&str, &str) -> String,
        cmd: &str,
        what: &str,
    ) -> (f64, f64, u64) {
        let line = send_raw(cmd, what);
        parse_latencies_delacks(&line, what)
    }
    fn median_of_3(
        send_raw: &mut impl FnMut(&str, &str) -> String,
        cmd: &str,
        what: &str,
    ) -> (f64, f64) {
        let mut samples = [
            send_cmd(send_raw, cmd, what),
            send_cmd(send_raw, cmd, what),
            send_cmd(send_raw, cmd, what),
        ];
        samples.sort_by(|a, b| a.0.total_cmp(&b.0));
        samples[1]
    }
    let bulk_bench = |what: &str| -> (f64, f64) {
        let out = ok(
            &kubectl(
                &kubeconfig,
                &[
                    "exec",
                    "qa-probe",
                    "--",
                    "python3",
                    "-c",
                    KEEP_ALIVE_CLIENT_PY,
                    &peer_ip,
                    "bulkbench",
                    "262144",
                ],
            ),
            what,
        );
        parse_latencies(&out, what)
    };

    let (pod_p50, pod_total) = send_cmd(&mut send_raw, "25", "pod-to-pod keep-alive (quickack=Y)");
    eprintln!("pod-to-pod keep-alive (quickack=Y): p50={pod_p50:.2}ms total={pod_total:.2}ms");
    assert!(
        pod_p50 < 15.0 && pod_total < 400.0,
        "pod-to-pod keep-alive stalled (p50={pod_p50:.2}ms total={pod_total:.2}ms)"
    );

    // Measure sustained 32 MiB bulk on a fresh connection with fixed SO_RCVBUF,
    // matched pingpong+bulk warm-up, and per-socket TCP_INFO pure ACK counts,
    // plus warm up the persistent connection before short-write streams.
    let (bulk_acks_on, bulk_ms_on) = bulk_bench("sustained 32MiB bulk (quickack=Y)");
    eprintln!(
        "pod-to-pod sustained 32MiB bulk median (quickack=Y): pure_acks={bulk_acks_on:.0} elapsed={bulk_ms_on:.2}ms"
    );
    send_cmd(&mut send_raw, "bulk", "warmup persistent connection");
    let (short_acks_on, short_ms_on) = median_of_3(
        &mut send_raw,
        "short",
        "short-write 512x4KiB stream (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod short-write 512x4KiB stream median (quickack=Y): pure_acks={short_acks_on:.0} elapsed={short_ms_on:.2}ms"
    );
    let (paced_acks_on, paced_ms_on) = send_cmd(
        &mut send_raw,
        "paced",
        "paced short-write 32x4KiB stream (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod paced short-write 32x4KiB stream (quickack=Y): pure_acks={paced_acks_on:.0} elapsed={paced_ms_on:.2}ms"
    );

    // Poll-driven split-write keep-alive with SO_RCVLOWAT=4096 (above ~80B
    // headers, below 10 KiB body): poll() cannot wake on the short header, so
    // __tcp_ack_snd_check() must ACK the pushed header below SO_RCVLOWAT.
    // Also record DelayedACKs delta so we prove draining the 10 KiB tail does
    // not fire delayed ACKs (establishing a clean baseline for `three_delack`).
    send_cmd(&mut send_raw, "rcvlowat 4096", "set SO_RCVLOWAT=4096");
    let (lowat_on_p50, lowat_on_total, lowat_on_delacks) = send_cmd_delacks(
        &mut send_raw,
        "delack 15",
        "SO_RCVLOWAT=4096 poll keep-alive (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=4096 poll keep-alive (quickack=Y): p50={lowat_on_p50:.2}ms total={lowat_on_total:.2}ms delacks={lowat_on_delacks}"
    );
    assert!(
        lowat_on_p50 < 15.0 && lowat_on_total < 300.0,
        "SO_RCVLOWAT=4096 poll keep-alive stalled with quickack=Y (p50={lowat_on_p50:.2}ms total={lowat_on_total:.2}ms)"
    );
    assert!(
        lowat_on_delacks <= 1,
        "two-write SO_RCVLOWAT=4096 burst unexpectedly triggered DelayedACKs (delacks={lowat_on_delacks})"
    );

    // Three-write response with SO_RCVLOWAT=4096 (70B hdr + 2 KiB mid < 4 KiB,
    // followed by 8 KiB tail behind Nagle): write 1 is ACKed immediately and
    // write 2's delayed ACK is capped to a short deadline rather than 40-50 ms.
    // Assert DelayedACKs increments (middle write was ACKed by the capped timer,
    // not an immediate ACK) while staying well under the stock 40-50 ms stall.
    let (three_on_p50, three_on_total, three_on_delacks) = send_cmd_delacks(
        &mut send_raw,
        "three_delack 10",
        "SO_RCVLOWAT=4096 three-write keep-alive (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=4096 three-write keep-alive (quickack=Y): p50={three_on_p50:.2}ms total={three_on_total:.2}ms delacks={three_on_delacks}"
    );
    assert!(
        three_on_p50 < 25.0,
        "SO_RCVLOWAT=4096 three-write hit stock delayed-ACK stall with quickack=Y (p50={three_on_p50:.2}ms total={three_on_total:.2}ms)"
    );
    assert!(
        three_on_delacks >= 8 && three_on_delacks > lowat_on_delacks + 5,
        "SO_RCVLOWAT=4096 three-write did not ACK the middle write via delayed-ACK timer (three_delacks={three_on_delacks}, two_write_delacks={lowat_on_delacks})"
    );

    // 20 KiB + 4 KiB split write with SO_RCVLOWAT=22528 on the warmed
    // connection (rcv_mss ~64 KiB): the 20 KiB first write is short relative
    // to the peer's MSS, so it is ACKed early and Nagle releases the tail.
    send_cmd(&mut send_raw, "rcvlowat 22528", "set SO_RCVLOWAT=22528");
    let (split20_on_p50, split20_on_total) = send_cmd(
        &mut send_raw,
        "split20k 10",
        "SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=Y): p50={split20_on_p50:.2}ms total={split20_on_total:.2}ms"
    );
    assert!(
        split20_on_p50 < 15.0,
        "SO_RCVLOWAT=22528 20KiB+4KiB split write stalled with quickack=Y (p50={split20_on_p50:.2}ms total={split20_on_total:.2}ms)"
    );

    // 100000 B first write spanning one MSS: the receiver classifies the
    // short, PSH-carrying last segment (not the GSO skb's gso_size), so it
    // never holds back the ACK Nagle's 4 KiB tail waits for. Stock Linux also
    // ACKs immediately here once more than one full frame is unacknowledged
    // and the window advances, so there is no quickack=N stall to contrast;
    // this guards against the policy delaying it.
    send_cmd(&mut send_raw, "rcvlowat 102400", "set SO_RCVLOWAT=102400");
    let (split100_on_p50, split100_on_total) = send_cmd(
        &mut send_raw,
        "split100k 10",
        "SO_RCVLOWAT=102400 100000B+4KiB split write (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=102400 100000B+4KiB split write (quickack=Y): p50={split100_on_p50:.2}ms total={split100_on_total:.2}ms"
    );
    assert!(
        split100_on_p50 < 15.0,
        "SO_RCVLOWAT=102400 100000B+4KiB split write stalled with quickack=Y (p50={split100_on_p50:.2}ms total={split100_on_total:.2}ms)"
    );

    // Repeated two-write 2 KiB responses on a fresh connection whose adaptive
    // rcv_mss settles at 2 KiB, polled with SO_RCVLOWAT=3072: the first write
    // is still a short push (judged against the advertised MSS, not rcv_mss),
    // so it is ACKed early and Nagle releases the second write.
    let one_shot = |path: &str, lowat: &str, port: &str, what: &str| -> (f64, f64) {
        let out = ok(
            &kubectl(
                &kubeconfig,
                &[
                    "exec",
                    "qa-probe",
                    "--",
                    "python3",
                    "-c",
                    KEEP_ALIVE_CLIENT_PY,
                    &peer_ip,
                    "15",
                    path,
                    lowat,
                    port,
                ],
            ),
            what,
        );
        parse_latencies(&out, what)
    };
    let two2k = |what: &str| one_shot("two2k", "3072", "8080", what);
    // The response sender (server) clamps its own receive window to 32 KiB,
    // which caps the reader's send MSS at 16 KiB. That limit only governs the
    // reader's sends, so the server's 20 KiB first write must still count as
    // short and be ACKed early.
    let split20k_clamped = |what: &str| one_shot("split20k", "22528", "8082", what);
    let (clamp_on_p50, clamp_on_total) =
        split20k_clamped("window-clamped server 20KiB+4KiB split write (quickack=Y)");
    eprintln!(
        "pod-to-pod window-clamped server SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=Y): p50={clamp_on_p50:.2}ms total={clamp_on_total:.2}ms"
    );
    assert!(
        clamp_on_p50 < 15.0,
        "window-clamped server 20KiB+4KiB split write stalled with quickack=Y (p50={clamp_on_p50:.2}ms total={clamp_on_total:.2}ms)"
    );
    let (two_on_p50, two_on_total) = two2k("SO_RCVLOWAT=3072 two-write 2KiB (quickack=Y)");
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=3072 two-write 2KiB keep-alive (quickack=Y): p50={two_on_p50:.2}ms total={two_on_total:.2}ms"
    );
    assert!(
        two_on_p50 < 15.0,
        "SO_RCVLOWAT=3072 two-write 2KiB stalled with quickack=Y (p50={two_on_p50:.2}ms total={two_on_total:.2}ms)"
    );

    // Multi-segment pushed stream with SO_RCVLOWAT=262144 (>4 segments):
    // 60000B pushes are short relative to the ~64 KiB MSS, but
    // !after(rcv_wup, copied_seq) limits immediate ACKs to one per unread
    // batch and the capped timer batches the rest (compared with quickack=N).
    send_cmd(&mut send_raw, "rcvlowat 262144", "set SO_RCVLOWAT=262144");
    let (pushed_acks_on, pushed_ms_on) = median_of_3(
        &mut send_raw,
        "pushed",
        "SO_RCVLOWAT=262144 pushed 48x60000B stream (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=262144 pushed 48x60000B median (quickack=Y): pure_acks={pushed_acks_on:.0} elapsed={pushed_ms_on:.2}ms"
    );
    // Full-sized PSH segments paced 12 ms apart below SO_RCVLOWAT: neither the
    // immediate ACK nor the shortened delayed-ACK cap may apply, so ACK
    // batching must match stock (compared against quickack=N below).
    let (fullmss_acks_on, fullmss_ms_on) = median_of_3(
        &mut send_raw,
        "fullmss",
        "SO_RCVLOWAT=262144 paced full-MSS stream (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=262144 paced 48xMSS median (quickack=Y): pure_acks={fullmss_acks_on:.0} elapsed={fullmss_ms_on:.2}ms"
    );
    // Short, non-full-MSS TCP_NODELAY stream (48 x 6000 B) paced 12 ms apart
    // below SO_RCVLOWAT=262144: once the bounded per-batch capped timer is
    // spent, stock delayed-ACK batching must remain intact instead of ACKing
    // every push.
    let (paced_short_acks_on, paced_short_ms_on) = median_of_3(
        &mut send_raw,
        "pacedshort",
        "SO_RCVLOWAT=262144 paced short 48x6000B stream (quickack=Y)",
    );
    eprintln!(
        "pod-to-pod SO_RCVLOWAT=262144 paced short 48x6000B median (quickack=Y): pure_acks={paced_short_acks_on:.0} elapsed={paced_short_ms_on:.2}ms"
    );
    send_cmd(&mut send_raw, "rcvlowat 1", "reset SO_RCVLOWAT=1");

    {
        let restore = QuickackRestoreGuard {
            docker_host: docker_host.clone(),
            restored: false,
        };
        ok(
            &docker(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--privileged",
                    TEST_IMAGE,
                    "sh",
                    "-c",
                    &format!("echo 0 > {QUICKACK_PARAM}"),
                ],
            ),
            "disable kat.quickack",
        );
        // Same already-established socket immediately observes quickack=0.
        let (off_p50, off_total) = send_cmd(
            &mut send_raw,
            "8",
            "established pod keep-alive (quickack=N)",
        );
        eprintln!(
            "pod-to-pod established keep-alive (quickack=N): p50={off_p50:.2}ms total={off_total:.2}ms"
        );
        assert!(
            off_p50 >= 25.0,
            "expected delayed-ACK stall on established socket with quickack=0 (got p50={off_p50:.2}ms total={off_total:.2}ms)"
        );

        send_cmd(
            &mut send_raw,
            "rcvlowat 4096",
            "set SO_RCVLOWAT=4096 (quickack=N)",
        );
        let (lowat_off_p50, lowat_off_total) = send_cmd(
            &mut send_raw,
            "6",
            "SO_RCVLOWAT=4096 poll keep-alive (quickack=N)",
        );
        eprintln!(
            "pod-to-pod SO_RCVLOWAT=4096 poll keep-alive (quickack=N): p50={lowat_off_p50:.2}ms total={lowat_off_total:.2}ms"
        );
        assert!(
            lowat_off_p50 >= 25.0,
            "expected SO_RCVLOWAT=4096 poll stall with quickack=0 (got p50={lowat_off_p50:.2}ms total={lowat_off_total:.2}ms)"
        );
        let (three_off_p50, three_off_total) = send_cmd(
            &mut send_raw,
            "three 6",
            "SO_RCVLOWAT=4096 three-write keep-alive (quickack=N)",
        );
        eprintln!(
            "pod-to-pod SO_RCVLOWAT=4096 three-write keep-alive (quickack=N): p50={three_off_p50:.2}ms total={three_off_total:.2}ms"
        );
        assert!(
            three_off_p50 >= 35.0,
            "expected SO_RCVLOWAT=4096 three-write stall with quickack=0 (got p50={three_off_p50:.2}ms total={three_off_total:.2}ms)"
        );

        send_cmd(
            &mut send_raw,
            "rcvlowat 22528",
            "set SO_RCVLOWAT=22528 (quickack=N)",
        );
        let (split20_off_p50, split20_off_total) = send_cmd(
            &mut send_raw,
            "split20k 6",
            "SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=N)",
        );
        eprintln!(
            "pod-to-pod SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=N): p50={split20_off_p50:.2}ms total={split20_off_total:.2}ms"
        );
        assert!(
            split20_off_p50 >= 25.0,
            "expected SO_RCVLOWAT=22528 20KiB+4KiB split-write stall with quickack=0 (got p50={split20_off_p50:.2}ms total={split20_off_total:.2}ms)"
        );
        let (near_off_p50, near_off_total) =
            run_near_mss("accepted-socket near-MSS split write (quickack=N)");
        eprintln!(
            "pod-to-pod accepted-socket near-MSS split write (quickack=N): p50={near_off_p50:.2}ms total={near_off_total:.2}ms"
        );
        assert!(
            near_off_p50 >= 25.0,
            "expected accepted-socket near-MSS split-write stall with quickack=0 (got p50={near_off_p50:.2}ms total={near_off_total:.2}ms)"
        );

        let (clamp_off_p50, clamp_off_total) =
            split20k_clamped("window-clamped server 20KiB+4KiB split write (quickack=N)");
        eprintln!(
            "pod-to-pod window-clamped server SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=N): p50={clamp_off_p50:.2}ms total={clamp_off_total:.2}ms"
        );
        assert!(
            clamp_off_p50 >= 25.0,
            "expected window-clamped server 20KiB+4KiB split-write stall with quickack=0 (got p50={clamp_off_p50:.2}ms total={clamp_off_total:.2}ms)"
        );
        let (two_off_p50, two_off_total) = two2k("SO_RCVLOWAT=3072 two-write 2KiB (quickack=N)");
        eprintln!(
            "pod-to-pod SO_RCVLOWAT=3072 two-write 2KiB keep-alive (quickack=N): p50={two_off_p50:.2}ms total={two_off_total:.2}ms"
        );
        assert!(
            two_off_p50 >= 25.0,
            "expected SO_RCVLOWAT=3072 two-write 2KiB stall with quickack=0 (got p50={two_off_p50:.2}ms total={two_off_total:.2}ms)"
        );

        send_cmd(
            &mut send_raw,
            "rcvlowat 262144",
            "set SO_RCVLOWAT=262144 (quickack=N)",
        );
        let (pushed_acks_off, pushed_ms_off) = median_of_3(
            &mut send_raw,
            "pushed",
            "SO_RCVLOWAT=262144 pushed 48x60000B stream (quickack=N)",
        );
        eprintln!(
            "pod-to-pod SO_RCVLOWAT=262144 pushed 48x60000B median (quickack=N): pure_acks={pushed_acks_off:.0} elapsed={pushed_ms_off:.2}ms"
        );
        // 48 pushed segments: ACKing each one below 262144 would give ~48+
        // pure ACKs; stock pacing must handle subsequent segments in each batch.
        assert!(
            pushed_acks_on < 40.0 && pushed_acks_on <= pushed_acks_off * 1.4 + 6.0,
            "SO_RCVLOWAT=262144 pushed stream inflated ACKs (on={pushed_acks_on:.0}, off={pushed_acks_off:.0})"
        );
        let (fullmss_acks_off, fullmss_ms_off) = median_of_3(
            &mut send_raw,
            "fullmss",
            "SO_RCVLOWAT=262144 paced full-MSS stream (quickack=N)",
        );
        eprintln!(
            "pod-to-pod SO_RCVLOWAT=262144 paced 48xMSS median (quickack=N): pure_acks={fullmss_acks_off:.0} elapsed={fullmss_ms_off:.2}ms"
        );
        // ACKing every full-sized segment (immediately or via the capped
        // timer) would give ~48 pure ACKs, while stock batches the 12 ms-paced
        // segments; the small slack covers the final sub-SO_RCVLOWAT drain.
        assert!(
            fullmss_acks_on <= fullmss_acks_off * 1.25 + 6.0,
            "SO_RCVLOWAT=262144 paced full-MSS stream lost ACK batching (on={fullmss_acks_on:.0}, off={fullmss_acks_off:.0})"
        );
        let (paced_short_acks_off, paced_short_ms_off) = median_of_3(
            &mut send_raw,
            "pacedshort",
            "SO_RCVLOWAT=262144 paced short 48x6000B stream (quickack=N)",
        );
        eprintln!(
            "pod-to-pod SO_RCVLOWAT=262144 paced short 48x6000B median (quickack=N): pure_acks={paced_short_acks_off:.0} elapsed={paced_short_ms_off:.2}ms"
        );
        // 48 short TCP_NODELAY writes paced 12 ms apart below SO_RCVLOWAT:
        // ACKing every push via repeated capped timers would give ~48 pure ACKs;
        // bounded capping keeps batching close to stock Linux.
        assert!(
            paced_short_acks_on < 38.0 && paced_short_acks_on <= paced_short_acks_off * 1.25 + 6.0,
            "SO_RCVLOWAT=262144 paced short 48x6000B stream lost delayed-ACK batching (on={paced_short_acks_on:.0}, off={paced_short_acks_off:.0})"
        );
        send_cmd(
            &mut send_raw,
            "rcvlowat 1",
            "reset SO_RCVLOWAT=1 (quickack=N)",
        );

        let (bulk_acks_off, bulk_ms_off) = bulk_bench("sustained 32MiB bulk (quickack=N)");
        eprintln!(
            "pod-to-pod sustained 32MiB bulk median (quickack=N): pure_acks={bulk_acks_off:.0} elapsed={bulk_ms_off:.2}ms"
        );
        let (short_acks_off, short_ms_off) = median_of_3(
            &mut send_raw,
            "short",
            "short-write 512x4KiB stream (quickack=N)",
        );
        eprintln!(
            "pod-to-pod short-write 512x4KiB stream median (quickack=N): pure_acks={short_acks_off:.0} elapsed={short_ms_off:.2}ms"
        );
        // 2 MiB fits inside the warmed receive buffer so stock Linux itself
        // varies with vCPU scheduling; ensure quickack=Y stays far below
        // 1-ACK-per-write (512 writes).
        assert!(
            short_acks_on < 128.0,
            "quickack=Y inflated short-write stream ACK traffic (on={short_acks_on:.0}, off={short_acks_off:.0})"
        );
        let (paced_acks_off, paced_ms_off) = send_cmd(
            &mut send_raw,
            "paced",
            "paced short-write 32x4KiB stream (quickack=N)",
        );
        eprintln!(
            "pod-to-pod paced short-write 32x4KiB stream (quickack=N): pure_acks={paced_acks_off:.0} elapsed={paced_ms_off:.2}ms"
        );
        assert!(
            paced_acks_on <= 40.0 && paced_ms_on < 500.0,
            "paced short-write stream misbehaved (on={paced_acks_on:.0} acks in {paced_ms_on:.2}ms, off={paced_acks_off:.0} acks in {paced_ms_off:.2}ms)"
        );

        restore.restore();
        // Same already-established socket immediately observes quickack=1 again.
        let (on_p50, on_total) = send_cmd(
            &mut send_raw,
            "15",
            "established pod keep-alive restored (quickack=Y)",
        );
        eprintln!(
            "pod-to-pod established keep-alive restored (quickack=Y): p50={on_p50:.2}ms total={on_total:.2}ms"
        );
        assert!(
            on_p50 < 15.0 && on_total < 300.0,
            "established socket stayed stalled after restoring quickack=1 (p50={on_p50:.2}ms total={on_total:.2}ms)"
        );

        // Matched fresh-connection bulk benchmark: fixed SO_RCVBUF (256 KiB),
        // identical pingpong + warm-up sequence, 4 x 8 MiB server writes (at
        // most 4 drain-path PSH boundaries), and per-connection TCP_INFO pure
        // ACK counts. Sample quickack=Y on both sides of quickack=N and check
        // that enabled pure ACKs stay tightly bounded and strictly reject a
        // doubled stock baseline (< 1.8x stock and <= 1.35x stock + 8).
        let (bulk_acks_on2, bulk_ms_on2) = bulk_bench("sustained 32MiB bulk restored (quickack=Y)");
        eprintln!(
            "pod-to-pod sustained 32MiB bulk median restored (quickack=Y): pure_acks={bulk_acks_on2:.0} elapsed={bulk_ms_on2:.2}ms"
        );
        // Coarse throughput guard for the per-packet receive-path hooks: a
        // 32 MiB transfer takes a few ms either way, so only a gross slowdown
        // (well beyond vCPU scheduling noise) trips this.
        let bulk_ms_on_best = bulk_ms_on.min(bulk_ms_on2);
        assert!(
            bulk_ms_on_best <= bulk_ms_off * 2.0 + 5.0,
            "quickack=Y slowed sustained bulk transfers (on={bulk_ms_on:.2}/{bulk_ms_on2:.2}ms, off={bulk_ms_off:.2}ms)"
        );
        let bulk_on = bulk_acks_on.min(bulk_acks_on2);
        assert!(
            bulk_acks_off >= 40.0,
            "unexpectedly small stock 32MiB bulk pure-ACK baseline ({bulk_acks_off:.0}); cannot calibrate doubling guard"
        );
        assert!(
            bulk_on < bulk_acks_off * 1.8 && bulk_on <= bulk_acks_off * 1.35 + 8.0,
            "quickack=Y inflated sustained bulk pure-ACK traffic (on={bulk_acks_on:.0}/{bulk_acks_on2:.0}, off={bulk_acks_off:.0})"
        );
    }
    drop(client_in);
    drop(client);

    // 2) Kubernetes ClusterIP Service path (qa-probe -> qa-svc ClusterIP ->
    //    qa-peer via kube-proxy DNAT): routed via pod's default gateway, stays
    //    in-VM (never crosses virtio-net), so quick ACK still applies.
    ok(
        &kubectl(
            &kubeconfig,
            &[
                "expose",
                "pod",
                "qa-peer",
                "--name=qa-svc",
                "--port=8080",
                "--target-port=8080",
            ],
        ),
        "expose qa-peer as ClusterIP service",
    );
    let svc_ip = ok(
        &kubectl(
            &kubeconfig,
            &["get", "svc", "qa-svc", "-o", "jsonpath={.spec.clusterIP}"],
        ),
        "get qa-svc ClusterIP",
    )
    .trim()
    .to_string();
    assert!(!svc_ip.is_empty(), "qa-svc has empty clusterIP");
    wait_until("qa-svc endpoints ready", Duration::from_secs(30), |left| {
        kubectl_timeout(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &svc_ip,
                "1",
            ],
            left.min(Duration::from_secs(5)),
        )
        .is_ok_and(|o| o.status.success())
    });
    let svc_out = ok(
        &kubectl(
            &kubeconfig,
            &[
                "exec",
                "qa-probe",
                "--",
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &svc_ip,
                "15",
            ],
        ),
        "pod-to-ClusterIP split-write keep-alive",
    );
    let (svc_p50, svc_total) = parse_latencies(&svc_out, "pod-to-ClusterIP keep-alive");
    eprintln!(
        "pod-to-ClusterIP keep-alive (quickack=Y): p50={svc_p50:.2}ms total={svc_total:.2}ms"
    );
    assert!(
        svc_p50 < 15.0 && svc_total < 300.0,
        "pod-to-ClusterIP keep-alive stalled (p50={svc_p50:.2}ms total={svc_total:.2}ms)"
    );

    // 3) Root namespace (`--network host`) to bridged Docker container over
    //    `docker0`: verifies root bridge dsts get quickack without mutating
    //    live routing tables or touching the `eth0` uplink.
    let srv_id = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "-d",
                "--rm",
                TEST_IMAGE,
                "python3",
                "-c",
                SPLIT_WRITE_SERVER_PY,
            ],
        ),
        "start bridged docker server",
    )
    .trim()
    .to_string();
    let _srv_guard = ContainerGuard {
        docker_host: docker_host.clone(),
        id: srv_id.clone(),
    };

    let srv_ip = ok(
        &docker(
            &docker_host,
            &[
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                &srv_id,
            ],
        ),
        "inspect bridged docker server IP",
    )
    .trim()
    .to_string();
    assert!(!srv_ip.is_empty(), "empty bridged container IP");

    wait_until(
        "bridged docker server ready",
        Duration::from_secs(15),
        |left| {
            docker_timeout(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    TEST_IMAGE,
                    "python3",
                    "-c",
                    KEEP_ALIVE_CLIENT_PY,
                    &srv_ip,
                    "1",
                ],
                left.min(Duration::from_secs(10)),
            )
            .is_ok_and(|o| o.status.success())
        },
    );

    let bridge_out = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                "--network",
                "host",
                TEST_IMAGE,
                "python3",
                "-c",
                KEEP_ALIVE_CLIENT_PY,
                &srv_ip,
                "25",
            ],
        ),
        "root-to-bridge split-write keep-alive",
    );
    let (br_p50, br_total) = parse_latencies(&bridge_out, "root-to-bridge keep-alive");
    eprintln!("root-to-bridge keep-alive (quickack=Y): p50={br_p50:.2}ms total={br_total:.2}ms");
    assert!(
        br_p50 < 15.0 && br_total < 400.0,
        "root-to-bridge keep-alive stalled (p50={br_p50:.2}ms total={br_total:.2}ms)"
    );

    // 3b) IPv6 Docker bridge functional probe (`AF_INET6` listener and client
    //     on a custom IPv6-enabled bridge) plus asymmetric low-`advmss` jumbo
    //     path probe (server advertises `advmss 1460` only on its route back to
    //     the root client while keeping the 65520 MTU path, sending 20 KiB +
    //     4 KiB below SO_RCVLOWAT=22528). Both are contrasted under
    //     quickack=Y vs quickack=N.
    let v6_net = format!("kat-qa-v6-{}", std::process::id());
    ok(
        &docker(
            &docker_host,
            &[
                "network",
                "create",
                "--ipv6",
                "--subnet",
                "fd00:db8:6a::/64",
                &v6_net,
            ],
        ),
        "create IPv6 docker bridge network",
    );
    let _v6_net_guard = NetworkGuard {
        docker_host: docker_host.clone(),
        name: v6_net.clone(),
    };
    let srv_v6_id = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "-d",
                "--rm",
                "--network",
                &v6_net,
                TEST_IMAGE,
                "python3",
                "-c",
                SPLIT_WRITE_SERVER_PY,
                "--ipv6",
            ],
        ),
        "start IPv6 bridged docker server",
    )
    .trim()
    .to_string();
    let _srv_v6_guard = ContainerGuard {
        docker_host: docker_host.clone(),
        id: srv_v6_id.clone(),
    };
    let srv_v6_ip = ok(
        &docker(
            &docker_host,
            &[
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.GlobalIPv6Address}}{{end}}",
                &srv_v6_id,
            ],
        ),
        "inspect IPv6 bridged docker server address",
    )
    .trim()
    .to_string();
    assert!(
        srv_v6_ip.contains(':'),
        "expected IPv6 address for bridged container, got {srv_v6_ip:?}"
    );
    wait_until(
        "IPv6 bridged docker server ready",
        Duration::from_secs(15),
        |left| {
            docker_timeout(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    TEST_IMAGE,
                    "python3",
                    "-c",
                    "import socket, sys; socket.create_connection((sys.argv[1], 8080), timeout=2).close()",
                    &srv_v6_ip,
                ],
                left.min(Duration::from_secs(5)),
            )
            .is_ok_and(|o| o.status.success())
        },
    );

    // Server with a low `advmss 1460` metric only on its route toward the root
    // `docker0` gateway, without lowering interface or route MTU (still 65520):
    // it advertises MSS=1460 in SYN-ACK while sending jumbo segments.
    let asym_srv_id = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "-d",
                "--rm",
                "--cap-add",
                "NET_ADMIN",
                "-e",
                &format!("SRV_PY={SPLIT_WRITE_SERVER_PY}"),
                TEST_IMAGE,
                "sh",
                "-euc",
                r#"gw=$(ip route show default | awk '{print $3; exit}')
ip route replace "$gw/32" dev eth0 advmss 1460
ip route show "$gw/32" >&2
exec python3 -c "$SRV_PY""#,
            ],
        ),
        "start asymmetric-advmss bridged docker server",
    )
    .trim()
    .to_string();
    let _asym_srv_guard = ContainerGuard {
        docker_host: docker_host.clone(),
        id: asym_srv_id.clone(),
    };
    let asym_srv_ip = ok(
        &docker(
            &docker_host,
            &[
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                &asym_srv_id,
            ],
        ),
        "inspect asymmetric-advmss server IP",
    )
    .trim()
    .to_string();
    assert!(!asym_srv_ip.is_empty(), "empty asymmetric-advmss server IP");
    wait_until(
        "asymmetric-advmss server ready",
        Duration::from_secs(15),
        |left| {
            docker_timeout(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    TEST_IMAGE,
                    "python3",
                    "-c",
                    "import socket, sys; socket.create_connection((sys.argv[1], 8080), timeout=2).close()",
                    &asym_srv_ip,
                ],
                left.min(Duration::from_secs(5)),
            )
            .is_ok_and(|o| o.status.success())
        },
    );

    let run_v6_bridge = |count: &str, what: &str| -> (f64, f64) {
        let out = ok(
            &docker(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    TEST_IMAGE,
                    "python3",
                    "-c",
                    KEEP_ALIVE_CLIENT_PY,
                    &srv_v6_ip,
                    count,
                ],
            ),
            what,
        );
        parse_latencies(&out, what)
    };
    let run_asym_split20k = |count: &str, what: &str| -> (f64, f64) {
        let out = ok(
            &docker(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    TEST_IMAGE,
                    "python3",
                    "-c",
                    KEEP_ALIVE_CLIENT_PY,
                    &asym_srv_ip,
                    count,
                    "split20k_asym",
                    "22528",
                    "8080",
                ],
            ),
            what,
        );
        parse_latencies(&out, what)
    };

    let (v6_on_p50, v6_on_total) =
        run_v6_bridge("25", "IPv6 root-to-bridge keep-alive (quickack=Y)");
    eprintln!(
        "IPv6 root-to-bridge keep-alive (quickack=Y): p50={v6_on_p50:.2}ms total={v6_on_total:.2}ms"
    );
    assert!(
        v6_on_p50 < 15.0 && v6_on_total < 400.0,
        "IPv6 root-to-bridge keep-alive stalled (p50={v6_on_p50:.2}ms total={v6_on_total:.2}ms)"
    );

    let (asym_on_p50, asym_on_total) = run_asym_split20k(
        "15",
        "asymmetric-advmss 20KiB+4KiB split write (quickack=Y)",
    );
    eprintln!(
        "asymmetric-advmss jumbo-path SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=Y): p50={asym_on_p50:.2}ms total={asym_on_total:.2}ms"
    );
    assert!(
        asym_on_p50 < 25.0,
        "asymmetric-advmss 20KiB+4KiB split write stalled with quickack=Y (p50={asym_on_p50:.2}ms total={asym_on_total:.2}ms)"
    );

    {
        let restore = QuickackRestoreGuard {
            docker_host: docker_host.clone(),
            restored: false,
        };
        ok(
            &docker(
                &docker_host,
                &[
                    "run",
                    "--rm",
                    "--privileged",
                    TEST_IMAGE,
                    "sh",
                    "-c",
                    &format!("echo 0 > {QUICKACK_PARAM}"),
                ],
            ),
            "disable kat.quickack for IPv6 and asymmetric-advmss contrast",
        );
        let (v6_off_p50, v6_off_total) =
            run_v6_bridge("8", "IPv6 root-to-bridge keep-alive (quickack=N)");
        eprintln!(
            "IPv6 root-to-bridge keep-alive (quickack=N): p50={v6_off_p50:.2}ms total={v6_off_total:.2}ms"
        );
        assert!(
            v6_off_p50 >= 25.0,
            "expected IPv6 root-to-bridge delayed-ACK stall with quickack=0 (p50={v6_off_p50:.2}ms total={v6_off_total:.2}ms)"
        );

        let (asym_off_p50, asym_off_total) =
            run_asym_split20k("6", "asymmetric-advmss 20KiB+4KiB split write (quickack=N)");
        eprintln!(
            "asymmetric-advmss jumbo-path SO_RCVLOWAT=22528 20KiB+4KiB split write (quickack=N): p50={asym_off_p50:.2}ms total={asym_off_total:.2}ms"
        );
        assert!(
            asym_off_p50 >= 25.0,
            "expected asymmetric-advmss 20KiB+4KiB delayed-ACK stall with quickack=0 (p50={asym_off_p50:.2}ms total={asym_off_total:.2}ms)"
        );

        restore.restore();
    }

    // A route that locks RTAX_QUICKACK (here locked at 0) overrides
    // Katamaran's default: a bridged container client on such a route sees the
    // stock delayed-ACK stall even with kat.quickack=1. Deleting the route and
    // reconnecting over the ordinary bridge subnet route restores quick ACKs.
    let locked_out = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                "--cap-add",
                "NET_ADMIN",
                "-e",
                &format!("LOCK_PY={LOCKED_ROUTE_PY}"),
                "-e",
                &format!("CLIENT_PY={KEEP_ALIVE_CLIENT_PY}"),
                TEST_IMAGE,
                "sh",
                "-euc",
                r#"python3 -c "$LOCK_PY" "$1"
ip route show "$1/32" >&2
python3 -c "$CLIENT_PY" "$1" 21
ip route del "$1/32"
python3 -c "$CLIENT_PY" "$1" 21"#,
                "sh",
                &srv_ip,
            ],
        ),
        "container keep-alive over a route with RTAX_QUICKACK locked",
    );
    let mut locked_lines = locked_out.lines().filter(|l| !l.trim().is_empty());
    let (locked_p50, locked_total) = parse_latencies(
        locked_lines.next().unwrap_or(""),
        "container keep-alive (RTAX_QUICKACK locked at 0)",
    );
    let (unlocked_p50, unlocked_total) = parse_latencies(
        locked_lines.next().unwrap_or(""),
        "container keep-alive (route lock removed)",
    );
    eprintln!(
        "container-to-bridge keep-alive: locked quickack=0 p50={locked_p50:.2}ms total={locked_total:.2}ms, unlocked p50={unlocked_p50:.2}ms total={unlocked_total:.2}ms"
    );
    assert!(
        locked_p50 >= 25.0,
        "a locked RTAX_QUICKACK=0 route must keep stock delayed ACKs (p50={locked_p50:.2}ms total={locked_total:.2}ms)"
    );
    assert!(
        unlocked_p50 < 15.0 && unlocked_total < 300.0,
        "keep-alive stalled after removing the locked route (p50={unlocked_p50:.2}ms total={unlocked_total:.2}ms)"
    );

    // 4) Container-to-uplink (`eth0` / virtio-net -> host.docker.internal)
    //    vs container-to-container (`docker0`, in-VM): verifies packets
    //    arriving over virtio-net (`eth0`) do NOT force quick ACKs (delayed
    //    ACKs fire normally), while in-VM container traffic sends immediate
    //    quick ACKs (zero delayed ACKs).
    let host_srv = HostHttpServer::start();
    let uplink_delacks: u64 = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                "--add-host=host.docker.internal:host-gateway",
                TEST_IMAGE,
                "python3",
                "-c",
                DELACK_PROBE_PY,
                "host.docker.internal",
                &host_srv.port.to_string(),
            ],
        ),
        "container-to-uplink DelayedACKs probe",
    )
    .trim()
    .parse()
    .expect("parse uplink DelayedACKs delta");
    let local_delacks: u64 = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                TEST_IMAGE,
                "python3",
                "-c",
                DELACK_PROBE_PY,
                &srv_ip,
                "8080",
            ],
        ),
        "container-to-container DelayedACKs probe",
    )
    .trim()
    .parse()
    .expect("parse local DelayedACKs delta");
    eprintln!(
        "DelayedACKs delta (6 paused responses): uplink={uplink_delacks}, local={local_delacks}"
    );
    assert!(
        uplink_delacks >= 4,
        "expected normal delayed ACKs on container->eth0 uplink connection (delta={uplink_delacks})"
    );
    assert_eq!(
        local_delacks, 0,
        "expected immediate quick ACKs (0 delayed ACKs) on in-VM container connection"
    );

    // 5) No route in any table of the guest root namespace or a pod's
    //    namespace (where a userspace workaround used to rewrite routes)
    //    carries a QUICKACK metric, locked or not.
    let root_routes = ok(
        &docker(
            &docker_host,
            &[
                "run",
                "--rm",
                "--network",
                "host",
                TEST_IMAGE,
                "python3",
                "-c",
                ROUTE_CHECK_PY,
            ],
        ),
        "inspect root namespace routes",
    );
    assert_no_quickack_routes(&root_routes, "guest root namespace");
    let pod_routes = ok(
        &kubectl(
            &kubeconfig,
            &["exec", "qa-probe", "--", "python3", "-c", ROUTE_CHECK_PY],
        ),
        "inspect qa-probe pod routes",
    );
    assert_no_quickack_routes(&pod_routes, "qa-probe pod namespace");

    ok(&cli(&home, &user_home, &["stop"]), "kat stop");
}

#[test]
#[ignore = "boots a foreground VM with --cmdline kat.quickack=0; run with `cargo xtask test-vm`"]
fn cmdline_opt_out_disables_quickack() {
    let root = root();
    let home = root.join(format!("target/tqa-off-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();

    struct Cleanup(Child, PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(None) = self.0.try_wait() {
                unsafe { libc::kill(self.0.id() as i32, libc::SIGTERM) };
                // `katamaran run` gives its katamaran-vm child up to 45 s
                // (lifecycle::SHUTDOWN_TIMEOUT) before killing and reaping it
                // itself; wait past that so SIGKILL never skips that cleanup.
                for _ in 0..1300 {
                    if let Ok(Some(_)) = self.0.try_wait() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
            let _ = std::fs::remove_dir_all(&self.1);
        }
    }

    let mut child = Command::new(katamaran())
        .current_dir(&root)
        .env("KATAMARAN_HOME", &home)
        .args([
            "run",
            "--no-mount",
            "--no-share",
            "--cpus",
            "2",
            "--memory-mib",
            "1024",
            "--cmdline",
            "kat.quickack=0",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn katamaran run");

    let (tx, lines) = mpsc::channel();
    for stream in [
        Box::new(child.stdout.take().unwrap()) as Box<dyn Read + Send>,
        Box::new(child.stderr.take().unwrap()),
    ] {
        let tx = tx.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                let _ = tx.send(line);
            }
        });
    }
    let _vm = Cleanup(child, home.clone());

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut output = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match lines.recv_timeout(left) {
            Ok(line) => {
                let ready = line.contains("docker ready");
                output.push(line);
                if ready {
                    break;
                }
            }
            Err(_) => panic!(
                "timed out waiting for foreground VM docker ready:\n{}",
                output.join("\n")
            ),
        }
    }

    let docker_host = format!("unix://{}", home.join("run/docker.sock").display());
    let val = ok_result(
        docker_timeout(
            &docker_host,
            &["run", "--rm", BASE_IMAGE, "cat", QUICKACK_PARAM],
            Duration::from_secs(120),
        ),
        "read kat.quickack under --cmdline kat.quickack=0",
    );
    assert_eq!(
        val.trim(),
        "N",
        "expected {QUICKACK_PARAM} to be N with --cmdline kat.quickack=0"
    );
}
