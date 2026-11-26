//! Minimal HTTP/2 (h2c) unary gRPC client over containerd's Unix domain
//! socket for operations that `ctr` does not expose directly.
//!
//! Specifically, `ctr images rm` only deletes by image name, which can race
//! with a concurrent kubelet registry pull that replaces a Katamaran-rewritten
//! tag with a native image. Containerd v2.4.2's `Images.Delete` RPC accepts
//! `DeleteImageRequest.target` and performs an atomic compare-and-delete inside
//! its BoltDB write transaction, returning gRPC `NOT_FOUND` (code 5) without
//! touching the image if its target digest has changed.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

const FRAME_DATA: u8 = 0x00;
const FRAME_HEADERS: u8 = 0x01;
const FRAME_RST_STREAM: u8 = 0x03;
const FRAME_SETTINGS: u8 = 0x04;
const FRAME_PING: u8 = 0x06;
const FRAME_GOAWAY: u8 = 0x07;
const FRAME_WINDOW_UPDATE: u8 = 0x08;
const FRAME_CONTINUATION: u8 = 0x09;

const FLAG_END_STREAM: u8 = 0x01;
const FLAG_ACK: u8 = 0x01;
const FLAG_END_HEADERS: u8 = 0x04;
const FLAG_PADDED: u8 = 0x08;
const FLAG_PRIORITY: u8 = 0x20;

const MAX_DATA_CHUNK: usize = 16_384;
const MAX_SERVER_FRAME: usize = 1 << 20;
const MAX_HEADER_BLOCK: usize = 256 << 10;
const DEFAULT_DYNAMIC_TABLE_SIZE: usize = 4096;

/// Outcome of a conditional [`delete_image_if_target_matches`] RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    /// Image existed with the expected target digest and was deleted.
    Deleted,
    /// Image was already absent, or its target digest changed (for example,
    /// replaced by a concurrent native kubelet pull) and was left untouched.
    NotFoundOrChanged,
}

/// Call containerd's `/containerd.services.images.v1.Images/Delete` RPC in
/// `namespace` with `DeleteImageRequest.target.digest` set to `target_digest`.
///
/// Containerd atomically checks that the stored image still points to
/// `target_digest` before deleting it, preserving any replacement image if the
/// tag changed after it was inspected.
pub fn delete_image_if_target_matches(
    socket_path: &Path,
    namespace: &str,
    name: &str,
    target_digest: &str,
    timeout: Duration,
) -> Result<DeleteOutcome, String> {
    if name.is_empty() {
        return Err("refusing to delete image with empty name".into());
    }
    if target_digest.is_empty() {
        return Err(format!(
            "refusing to delete {name} without an expected target digest"
        ));
    }

    let mut stream = UnixStream::connect(socket_path)
        .map_err(|e| format!("connect {}: {e}", socket_path.display()))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| format!("set read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| format!("set write timeout: {e}"))?;

    // 1. Client preface + initial SETTINGS (request header table size 0).
    stream
        .write_all(CLIENT_PREFACE)
        .map_err(|e| format!("write h2 preface: {e}"))?;
    let settings = [0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
    write_frame(&mut stream, FRAME_SETTINGS, 0, 0, &settings)
        .map_err(|e| format!("write h2 settings: {e}"))?;

    // 2. Request HEADERS on stream 1.
    let timeout_hdr = format!("{}S", timeout.as_secs().max(1));
    let req_headers = encode_request_headers(&[
        (":method", "POST"),
        (":scheme", "http"),
        (":path", "/containerd.services.images.v1.Images/Delete"),
        (":authority", "localhost"),
        ("content-type", "application/grpc"),
        ("te", "trailers"),
        ("containerd-namespace", namespace),
        ("grpc-timeout", &timeout_hdr),
    ]);
    write_frame(
        &mut stream,
        FRAME_HEADERS,
        FLAG_END_HEADERS,
        1,
        &req_headers,
    )
    .map_err(|e| format!("write h2 headers: {e}"))?;

    // 3. Request DATA on stream 1 with gRPC length-prefixed DeleteImageRequest.
    let proto = encode_delete_image_request(name, target_digest);
    let grpc_body = frame_grpc_message(&proto)?;
    for (idx, chunk) in grpc_body.chunks(MAX_DATA_CHUNK).enumerate() {
        let is_last = (idx + 1) * MAX_DATA_CHUNK >= grpc_body.len();
        let flags = if is_last { FLAG_END_STREAM } else { 0 };
        write_frame(&mut stream, FRAME_DATA, flags, 1, chunk)
            .map_err(|e| format!("write h2 data: {e}"))?;
    }
    stream
        .flush()
        .map_err(|e| format!("flush h2 request: {e}"))?;

    // 4. Read server frames until stream 1 terminates (bounded by overall deadline).
    let deadline = Instant::now() + timeout;
    let mut decoder = HpackDecoder::new(DEFAULT_DYNAMIC_TABLE_SIZE);
    let mut http_status: Option<String> = None;
    let mut grpc_status: Option<String> = None;
    let mut grpc_message = String::new();

    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err("containerd Images.Delete timed out".into());
        }
        let _ = stream.set_read_timeout(Some(left));
        let (frame_type, flags, stream_id, payload) = read_frame(&mut stream)?;
        match frame_type {
            FRAME_SETTINGS => {
                if flags & FLAG_ACK == 0 {
                    // Best-effort ACK: a peer that already sent the full
                    // response and closed its socket may return EPIPE while
                    // response frames remain buffered for us to read.
                    let _ = write_frame(&mut stream, FRAME_SETTINGS, FLAG_ACK, 0, &[]);
                    let _ = stream.flush();
                }
            }
            FRAME_PING => {
                if flags & FLAG_ACK == 0 && payload.len() == 8 {
                    let _ = write_frame(&mut stream, FRAME_PING, FLAG_ACK, 0, &payload);
                    let _ = stream.flush();
                }
            }
            FRAME_WINDOW_UPDATE => {}
            FRAME_GOAWAY => {
                let last_stream_id = if payload.len() >= 4 {
                    u32::from_be_bytes([payload[0] & 0x7f, payload[1], payload[2], payload[3]])
                } else {
                    0
                };
                if last_stream_id < 1 {
                    let code = if payload.len() >= 8 {
                        u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]])
                    } else {
                        0
                    };
                    let debug = if payload.len() > 8 {
                        String::from_utf8_lossy(&payload[8..]).into_owned()
                    } else {
                        String::new()
                    };
                    return Err(format!(
                        "containerd closed HTTP/2 connection with GOAWAY (code {code}): {debug}"
                    ));
                }
            }
            FRAME_RST_STREAM if stream_id == 1 => {
                let code = if payload.len() >= 4 {
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                } else {
                    0
                };
                return Err(format!("containerd reset HTTP/2 stream 1 (code {code})"));
            }
            FRAME_DATA if stream_id == 1 => {
                if flags & FLAG_END_STREAM != 0 {
                    break;
                }
            }
            FRAME_HEADERS if stream_id == 1 => {
                let end_stream = flags & FLAG_END_STREAM != 0;
                let mut block = strip_headers_framing(&payload, flags)?;
                let mut cur_flags = flags;
                while cur_flags & FLAG_END_HEADERS == 0 {
                    let (ct, cf, cs, cp) = read_frame(&mut stream)?;
                    if ct != FRAME_CONTINUATION || cs != 1 {
                        return Err(format!(
                            "expected CONTINUATION on stream 1, got type {ct} on stream {cs}"
                        ));
                    }
                    if block.len() + cp.len() > MAX_HEADER_BLOCK {
                        return Err("HTTP/2 header block exceeded limit".into());
                    }
                    block.extend_from_slice(&cp);
                    cur_flags = cf;
                }
                for (k, v) in decoder.decode(&block)? {
                    match k.as_str() {
                        ":status" => http_status = Some(v),
                        "grpc-status" => grpc_status = Some(v),
                        "grpc-message" => grpc_message = percent_decode(&v),
                        _ => {}
                    }
                }
                if end_stream {
                    break;
                }
            }
            _ => {}
        }
    }

    if let Some(status) = &http_status
        && status != "200"
    {
        return Err(format!(
            "containerd Images.Delete returned HTTP {status} ({grpc_message})"
        ));
    }
    match grpc_status.as_deref() {
        Some("0") => Ok(DeleteOutcome::Deleted),
        // gRPC status 5 = NOT_FOUND: returned both when the image does not
        // exist and when DeleteImageRequest.target.digest does not match.
        Some("5") => Ok(DeleteOutcome::NotFoundOrChanged),
        Some(code) => Err(format!(
            "containerd Images.Delete failed (grpc-status {code}): {grpc_message}"
        )),
        None => Err("containerd Images.Delete response missing grpc-status trailer".into()),
    }
}

/// Encode `containerd.services.images.v1.DeleteImageRequest`:
/// - field 1 (string): `name`
/// - field 3 (message `containerd.types.Descriptor`): `target`
///   - field 2 (string): `digest`
fn encode_delete_image_request(name: &str, target_digest: &str) -> Vec<u8> {
    let mut target = Vec::with_capacity(target_digest.len() + 4);
    encode_proto_bytes(&mut target, 2, target_digest.as_bytes());

    let mut req = Vec::with_capacity(name.len() + target.len() + 8);
    encode_proto_bytes(&mut req, 1, name.as_bytes());
    encode_proto_bytes(&mut req, 3, &target);
    req
}

fn encode_proto_bytes(out: &mut Vec<u8>, field_num: u32, data: &[u8]) {
    encode_varint(out, ((field_num as u64) << 3) | 2);
    encode_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

fn encode_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn frame_grpc_message(payload: &[u8]) -> Result<Vec<u8>, String> {
    let len = u32::try_from(payload.len()).map_err(|_| "gRPC message too large".to_string())?;
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0); // uncompressed
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

fn write_frame(
    w: &mut impl Write,
    frame_type: u8,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> io::Result<()> {
    let len = payload.len();
    let hdr = [
        ((len >> 16) & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        (len & 0xff) as u8,
        frame_type,
        flags,
        ((stream_id >> 24) & 0x7f) as u8,
        ((stream_id >> 16) & 0xff) as u8,
        ((stream_id >> 8) & 0xff) as u8,
        (stream_id & 0xff) as u8,
    ];
    w.write_all(&hdr)?;
    if !payload.is_empty() {
        w.write_all(payload)?;
    }
    Ok(())
}

fn read_frame(r: &mut impl Read) -> Result<(u8, u8, u32, Vec<u8>), String> {
    let mut hdr = [0u8; 9];
    r.read_exact(&mut hdr)
        .map_err(|e| format!("read h2 frame header: {e}"))?;
    let len = ((hdr[0] as usize) << 16) | ((hdr[1] as usize) << 8) | (hdr[2] as usize);
    if len > MAX_SERVER_FRAME {
        return Err(format!("h2 frame length {len} exceeds limit"));
    }
    let frame_type = hdr[3];
    let flags = hdr[4];
    let stream_id = u32::from_be_bytes([hdr[5] & 0x7f, hdr[6], hdr[7], hdr[8]]);
    let mut payload = vec![0u8; len];
    if len > 0 {
        r.read_exact(&mut payload)
            .map_err(|e| format!("read h2 frame payload: {e}"))?;
    }
    Ok((frame_type, flags, stream_id, payload))
}

fn strip_headers_framing(payload: &[u8], flags: u8) -> Result<Vec<u8>, String> {
    let mut slice = payload;
    let pad_len = if flags & FLAG_PADDED != 0 {
        let Some((&p, rest)) = slice.split_first() else {
            return Err("truncated padded HEADERS frame".into());
        };
        slice = rest;
        p as usize
    } else {
        0
    };
    if flags & FLAG_PRIORITY != 0 {
        if slice.len() < 5 {
            return Err("truncated priority HEADERS frame".into());
        }
        slice = &slice[5..];
    }
    if pad_len > slice.len() {
        return Err("invalid HEADERS padding length".into());
    }
    Ok(slice[..slice.len() - pad_len].to_vec())
}

/// Encode headers using RFC 7541 §6.2.2 literal header fields without indexing.
fn encode_request_headers(headers: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    for &(name, value) in headers {
        out.push(0x00);
        encode_hpack_str(&mut out, name.as_bytes());
        encode_hpack_str(&mut out, value.as_bytes());
    }
    out
}

fn encode_hpack_str(out: &mut Vec<u8>, s: &[u8]) {
    encode_hpack_int(out, 0x00, 7, s.len());
    out.extend_from_slice(s);
}

fn encode_hpack_int(out: &mut Vec<u8>, prefix_bits: u8, n: u8, mut val: usize) {
    let max_prefix = (1usize << n) - 1;
    if val < max_prefix {
        out.push(prefix_bits | (val as u8));
        return;
    }
    out.push(prefix_bits | (max_prefix as u8));
    val -= max_prefix;
    while val >= 128 {
        out.push(((val & 0x7f) as u8) | 0x80);
        val >>= 7;
    }
    out.push(val as u8);
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// RFC 7541 HPACK decoder supporting static table, dynamic table with eviction,
/// table size updates, and Appendix B Huffman decoding.
struct HpackDecoder {
    dynamic: VecDeque<(String, String)>,
    dynamic_bytes: usize,
    max_size: usize,
}

impl HpackDecoder {
    fn new(max_size: usize) -> Self {
        Self {
            dynamic: VecDeque::new(),
            dynamic_bytes: 0,
            max_size,
        }
    }

    fn decode(&mut self, mut input: &[u8]) -> Result<Vec<(String, String)>, String> {
        let mut out = Vec::new();
        while !input.is_empty() {
            let b = input[0];
            if b & 0x80 != 0 {
                // Indexed Header Field (§6.1)
                let (idx, rest) = decode_hpack_int(input, 7)?;
                input = rest;
                out.push(self.lookup(idx)?);
            } else if b & 0x40 != 0 {
                // Literal Header Field with Incremental Indexing (§6.2.1)
                let (name, val, rest) = self.decode_literal(input, 6)?;
                input = rest;
                self.insert(name.clone(), val.clone());
                out.push((name, val));
            } else if b & 0x20 != 0 {
                // Dynamic Table Size Update (§6.3)
                let (new_max, rest) = decode_hpack_int(input, 5)?;
                input = rest;
                self.max_size = new_max;
                self.evict();
            } else {
                // Literal without Indexing (§6.2.2) or Never Indexed (§6.2.3)
                let (name, val, rest) = self.decode_literal(input, 4)?;
                input = rest;
                out.push((name, val));
            }
        }
        Ok(out)
    }

    fn decode_literal<'a>(
        &self,
        input: &'a [u8],
        prefix_bits: u8,
    ) -> Result<(String, String, &'a [u8]), String> {
        let (name_idx, rest) = decode_hpack_int(input, prefix_bits)?;
        let (name, rest) = if name_idx == 0 {
            decode_hpack_str(rest)?
        } else {
            (self.lookup(name_idx)?.0, rest)
        };
        let (val, rest) = decode_hpack_str(rest)?;
        Ok((name, val, rest))
    }

    fn lookup(&self, idx: usize) -> Result<(String, String), String> {
        if idx == 0 {
            return Err("invalid HPACK index 0".into());
        }
        if idx <= STATIC_TABLE.len() {
            let (k, v) = STATIC_TABLE[idx - 1];
            return Ok((k.to_string(), v.to_string()));
        }
        let dyn_idx = idx - STATIC_TABLE.len() - 1;
        self.dynamic
            .get(dyn_idx)
            .cloned()
            .ok_or_else(|| format!("HPACK dynamic table index {idx} out of bounds"))
    }

    fn insert(&mut self, name: String, value: String) {
        let entry_size = name.len() + value.len() + 32;
        if entry_size > self.max_size {
            self.dynamic.clear();
            self.dynamic_bytes = 0;
            return;
        }
        self.dynamic.push_front((name, value));
        self.dynamic_bytes += entry_size;
        self.evict();
    }

    fn evict(&mut self) {
        while self.dynamic_bytes > self.max_size {
            if let Some((k, v)) = self.dynamic.pop_back() {
                self.dynamic_bytes = self.dynamic_bytes.saturating_sub(k.len() + v.len() + 32);
            } else {
                self.dynamic_bytes = 0;
                break;
            }
        }
    }
}

fn decode_hpack_int(input: &[u8], n: u8) -> Result<(usize, &[u8]), String> {
    let Some((&first, mut rest)) = input.split_first() else {
        return Err("truncated HPACK integer".into());
    };
    let mask = ((1u16 << n) - 1) as u8;
    let mut val = (first & mask) as usize;
    if val < mask as usize {
        return Ok((val, rest));
    }
    let mut shift = 0u32;
    loop {
        let Some((&b, next)) = rest.split_first() else {
            return Err("truncated HPACK multi-byte integer".into());
        };
        rest = next;
        let add = ((b & 0x7f) as usize)
            .checked_shl(shift)
            .ok_or("HPACK integer overflow")?;
        val = val.checked_add(add).ok_or("HPACK integer overflow")?;
        if b & 0x80 == 0 {
            return Ok((val, rest));
        }
        shift += 7;
        if shift > 28 {
            return Err("HPACK integer overflow".into());
        }
    }
}

fn decode_hpack_str(input: &[u8]) -> Result<(String, &[u8]), String> {
    let Some(&first) = input.first() else {
        return Err("truncated HPACK string".into());
    };
    let huffman = first & 0x80 != 0;
    let (len, rest) = decode_hpack_int(input, 7)?;
    if len > rest.len() {
        return Err("truncated HPACK string payload".into());
    }
    let (raw, tail) = rest.split_at(len);
    let bytes = if huffman {
        huffman_decode(raw)?
    } else {
        raw.to_vec()
    };
    Ok((String::from_utf8_lossy(&bytes).into_owned(), tail))
}

fn huffman_decode(src: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(src.len() * 2);
    let mut cur: u64 = 0;
    let mut nbits: u8 = 0;

    for &byte in src {
        cur = (cur << 8) | (byte as u64);
        nbits += 8;
        while nbits >= 5 {
            let mut matched = None;
            for (sym, (&code, &len)) in HUFFMAN_CODES
                .iter()
                .zip(HUFFMAN_CODE_LEN.iter())
                .enumerate()
            {
                if len <= nbits && ((cur >> (nbits - len)) as u32) == code {
                    matched = Some((sym as u8, len));
                    break;
                }
            }
            if let Some((sym, len)) = matched {
                out.push(sym);
                nbits -= len;
                cur &= (1u64 << nbits) - 1;
            } else {
                break;
            }
        }
        if nbits > 30 {
            return Err("invalid HPACK Huffman code".into());
        }
    }

    if nbits > 7 {
        return Err("invalid HPACK Huffman trailing bits".into());
    }
    if nbits > 0 {
        let pad_mask = (1u64 << nbits) - 1;
        if cur != pad_mask {
            return Err("invalid HPACK Huffman padding".into());
        }
    }
    Ok(out)
}

/// RFC 7541 Appendix A static table (1-based indices 1..=61).
const STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

/// RFC 7541 Appendix B Huffman codes for bytes 0..=255.
#[rustfmt::skip]
const HUFFMAN_CODES: [u32; 256] = [
    0x1ff8, 0x7fffd8, 0xfffffe2, 0xfffffe3, 0xfffffe4, 0xfffffe5, 0xfffffe6, 0xfffffe7,
    0xfffffe8, 0xffffea, 0x3ffffffc, 0xfffffe9, 0xfffffea, 0x3ffffffd, 0xfffffeb, 0xfffffec,
    0xfffffed, 0xfffffee, 0xfffffef, 0xffffff0, 0xffffff1, 0xffffff2, 0x3ffffffe, 0xffffff3,
    0xffffff4, 0xffffff5, 0xffffff6, 0xffffff7, 0xffffff8, 0xffffff9, 0xffffffa, 0xffffffb,
    0x14, 0x3f8, 0x3f9, 0xffa, 0x1ff9, 0x15, 0xf8, 0x7fa,
    0x3fa, 0x3fb, 0xf9, 0x7fb, 0xfa, 0x16, 0x17, 0x18,
    0x0, 0x1, 0x2, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
    0x1e, 0x1f, 0x5c, 0xfb, 0x7ffc, 0x20, 0xffb, 0x3fc,
    0x1ffa, 0x21, 0x5d, 0x5e, 0x5f, 0x60, 0x61, 0x62,
    0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a,
    0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72,
    0xfc, 0x73, 0xfd, 0x1ffb, 0x7fff0, 0x1ffc, 0x3ffc, 0x22,
    0x7ffd, 0x3, 0x23, 0x4, 0x24, 0x5, 0x25, 0x26,
    0x27, 0x6, 0x74, 0x75, 0x28, 0x29, 0x2a, 0x7,
    0x2b, 0x76, 0x2c, 0x8, 0x9, 0x2d, 0x77, 0x78,
    0x79, 0x7a, 0x7b, 0x7ffe, 0x7fc, 0x3ffd, 0x1ffd, 0xffffffc,
    0xfffe6, 0x3fffd2, 0xfffe7, 0xfffe8, 0x3fffd3, 0x3fffd4, 0x3fffd5, 0x7fffd9,
    0x3fffd6, 0x7fffda, 0x7fffdb, 0x7fffdc, 0x7fffdd, 0x7fffde, 0xffffeb, 0x7fffdf,
    0xffffec, 0xffffed, 0x3fffd7, 0x7fffe0, 0xffffee, 0x7fffe1, 0x7fffe2, 0x7fffe3,
    0x7fffe4, 0x1fffdc, 0x3fffd8, 0x7fffe5, 0x3fffd9, 0x7fffe6, 0x7fffe7, 0xffffef,
    0x3fffda, 0x1fffdd, 0xfffe9, 0x3fffdb, 0x3fffdc, 0x7fffe8, 0x7fffe9, 0x1fffde,
    0x7fffea, 0x3fffdd, 0x3fffde, 0xfffff0, 0x1fffdf, 0x3fffdf, 0x7fffeb, 0x7fffec,
    0x1fffe0, 0x1fffe1, 0x3fffe0, 0x1fffe2, 0x7fffed, 0x3fffe1, 0x7fffee, 0x7fffef,
    0xfffea, 0x3fffe2, 0x3fffe3, 0x3fffe4, 0x7ffff0, 0x3fffe5, 0x3fffe6, 0x7ffff1,
    0x3ffffe0, 0x3ffffe1, 0xfffeb, 0x7fff1, 0x3fffe7, 0x7ffff2, 0x3fffe8, 0x1ffffec,
    0x3ffffe2, 0x3ffffe3, 0x3ffffe4, 0x7ffffde, 0x7ffffdf, 0x3ffffe5, 0xfffff1, 0x1ffffed,
    0x7fff2, 0x1fffe3, 0x3ffffe6, 0x7ffffe0, 0x7ffffe1, 0x3ffffe7, 0x7ffffe2, 0xfffff2,
    0x1fffe4, 0x1fffe5, 0x3ffffe8, 0x3ffffe9, 0xffffffd, 0x7ffffe3, 0x7ffffe4, 0x7ffffe5,
    0xfffec, 0xfffff3, 0xfffed, 0x1fffe6, 0x3fffe9, 0x1fffe7, 0x1fffe8, 0x7ffff3,
    0x3fffea, 0x3fffeb, 0x1ffffee, 0x1ffffef, 0xfffff4, 0xfffff5, 0x3ffffea, 0x7ffff4,
    0x3ffffeb, 0x7ffffe6, 0x3ffffec, 0x3ffffed, 0x7ffffe7, 0x7ffffe8, 0x7ffffe9, 0x7ffffea,
    0x7ffffeb, 0xffffffe, 0x7ffffec, 0x7ffffed, 0x7ffffee, 0x7ffffef, 0x7fffff0, 0x3ffffee,
];

/// RFC 7541 Appendix B Huffman code lengths (in bits) for bytes 0..=255.
#[rustfmt::skip]
const HUFFMAN_CODE_LEN: [u8; 256] = [
    13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28,
    28, 28, 28, 28, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 28,
     6, 10, 10, 12, 13,  6,  8, 11, 10, 10,  8, 11,  8,  6,  6,  6,
     5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  7,  8, 15,  6, 12, 10,
    13,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
     7,  7,  7,  7,  7,  7,  7,  7,  8,  7,  8, 13, 19, 13, 14,  6,
    15,  5,  6,  5,  6,  5,  6,  6,  6,  5,  7,  7,  6,  6,  6,  5,
     6,  7,  6,  5,  5,  6,  7,  7,  7,  7,  7, 15, 11, 14, 13, 28,
    20, 22, 20, 20, 22, 22, 22, 23, 22, 23, 23, 23, 23, 23, 24, 23,
    24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24,
    22, 21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23,
    21, 21, 22, 21, 23, 22, 23, 23, 20, 22, 22, 22, 23, 22, 22, 23,
    26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25,
    19, 21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27,
    20, 24, 20, 21, 22, 21, 21, 23, 22, 22, 25, 25, 24, 24, 26, 23,
    26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26,
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::net::UnixListener;
    use std::sync::{Arc, Mutex};
    use std::thread;

    fn huffman_encode(s: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut cur: u64 = 0;
        let mut nbits: u8 = 0;
        for &b in s {
            let code = HUFFMAN_CODES[b as usize] as u64;
            let len = HUFFMAN_CODE_LEN[b as usize];
            cur = (cur << len) | code;
            nbits += len;
            while nbits >= 8 {
                nbits -= 8;
                out.push((cur >> nbits) as u8);
                cur &= (1u64 << nbits) - 1;
            }
        }
        if nbits > 0 {
            let pad = 8 - nbits;
            out.push(((cur << pad) | ((1u64 << pad) - 1)) as u8);
        }
        out
    }

    fn encode_hpack_huffman_header(out: &mut Vec<u8>, name: &str, value: &str, index: bool) {
        out.push(if index { 0x40 } else { 0x00 });
        let n = huffman_encode(name.as_bytes());
        encode_hpack_int(out, 0x80, 7, n.len());
        out.extend_from_slice(&n);
        let v = huffman_encode(value.as_bytes());
        encode_hpack_int(out, 0x80, 7, v.len());
        out.extend_from_slice(&v);
    }

    fn decode_varint(mut data: &[u8]) -> (u64, &[u8]) {
        let mut val = 0u64;
        let mut shift = 0u32;
        loop {
            let b = data[0];
            data = &data[1..];
            val |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return (val, data);
            }
            shift += 7;
        }
    }

    fn decode_delete_request(mut data: &[u8]) -> (String, Option<String>) {
        let mut name = String::new();
        let mut target_digest = None;
        while !data.is_empty() {
            let (tag, rest) = decode_varint(data);
            let field = tag >> 3;
            let wt = tag & 0x7;
            assert_eq!(wt, 2);
            let (len, rest) = decode_varint(rest);
            let (val, next) = rest.split_at(len as usize);
            data = next;
            match field {
                1 => name = String::from_utf8(val.to_vec()).unwrap(),
                3 => {
                    let mut inner = val;
                    while !inner.is_empty() {
                        let (itag, irest) = decode_varint(inner);
                        let ifield = itag >> 3;
                        let (ilen, irest) = decode_varint(irest);
                        let (ival, inext) = irest.split_at(ilen as usize);
                        inner = inext;
                        if ifield == 2 {
                            target_digest = Some(String::from_utf8(ival.to_vec()).unwrap());
                        }
                    }
                }
                _ => {}
            }
        }
        (name, target_digest)
    }

    #[test]
    fn encodes_delete_image_request_with_target_digest() {
        let bytes = encode_delete_image_request("docker.io/library/app:dev", "sha256:abc123");
        let (name, digest) = decode_delete_request(&bytes);
        assert_eq!(name, "docker.io/library/app:dev");
        assert_eq!(digest.as_deref(), Some("sha256:abc123"));
    }

    #[test]
    fn hpack_decodes_rfc7541_huffman_and_dynamic_table() {
        // RFC 7541 Appendix C.4.1 vector for "www.example.com"
        let rfc_vec = [
            0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
        ];
        assert_eq!(huffman_decode(&rfc_vec).unwrap(), b"www.example.com");

        let mut block = Vec::new();
        block.push(0x88); // static index 8 -> :status: 200
        encode_hpack_huffman_header(&mut block, "grpc-status", "5", true);
        encode_hpack_huffman_header(&mut block, "grpc-message", "image%20not%20found", false);
        block.push(0xbe); // dynamic index 62 -> grpc-status: 5

        let mut dec = HpackDecoder::new(4096);
        let hdrs = dec.decode(&block).unwrap();
        assert_eq!(hdrs[0], (":status".into(), "200".into()));
        assert_eq!(hdrs[1], ("grpc-status".into(), "5".into()));
        assert_eq!(
            hdrs[2],
            ("grpc-message".into(), "image%20not%20found".into())
        );
        assert_eq!(hdrs[3], ("grpc-status".into(), "5".into()));
    }

    /// Regression test: if kubelet replaces a rewritten tag with a native pull
    /// between `list_ctr_images` and `retire_rewritten_image`, containerd's
    /// conditional `Images.Delete` RPC (`DeleteImageRequest.target`) returns
    /// NOT_FOUND and leaves the newly pulled native image intact.
    #[test]
    fn conditional_delete_preserves_native_replacement_on_digest_mismatch() {
        let dir = std::env::temp_dir().join(format!("kat-grpc-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("containerd.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::from([
            (
                "docker.io/library/retire-me:dev".into(),
                "sha256:rewritten1".into(),
            ),
            // Simulates a tag whose rewritten copy (`sha256:rewritten2`) was
            // replaced by a native kubelet pull (`sha256:native2`) before delete.
            (
                "docker.io/library/replaced:dev".into(),
                "sha256:native2".into(),
            ),
        ])));

        let server_store = Arc::clone(&store);
        let server = thread::spawn(move || {
            for _ in 0..3 {
                let (mut conn, _) = listener.accept().unwrap();
                let mut preface = [0u8; 24];
                conn.read_exact(&mut preface).unwrap();
                assert_eq!(&preface, CLIENT_PREFACE);

                // Send server SETTINGS to exercise ACK handling.
                write_frame(&mut conn, FRAME_SETTINGS, 0, 0, &[]).unwrap();
                conn.flush().unwrap();

                let mut req_headers_map = HashMap::new();
                let mut req_body = Vec::new();
                let mut dec = HpackDecoder::new(4096);
                loop {
                    let (ft, flags, sid, payload) = read_frame(&mut conn).unwrap();
                    match ft {
                        FRAME_SETTINGS => {
                            if flags & FLAG_ACK == 0 {
                                write_frame(&mut conn, FRAME_SETTINGS, FLAG_ACK, 0, &[]).unwrap();
                                conn.flush().unwrap();
                            }
                        }
                        FRAME_HEADERS if sid == 1 => {
                            for (k, v) in dec.decode(&payload).unwrap() {
                                req_headers_map.insert(k, v);
                            }
                        }
                        FRAME_DATA if sid == 1 => {
                            req_body.extend_from_slice(&payload);
                            if flags & FLAG_END_STREAM != 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                assert_eq!(
                    req_headers_map.get(":path").map(String::as_str),
                    Some("/containerd.services.images.v1.Images/Delete")
                );
                assert_eq!(
                    req_headers_map
                        .get("containerd-namespace")
                        .map(String::as_str),
                    Some("k8s.io")
                );
                let msg_len =
                    u32::from_be_bytes([req_body[1], req_body[2], req_body[3], req_body[4]])
                        as usize;
                let (name, target_digest) = decode_delete_request(&req_body[5..5 + msg_len]);

                if name == "docker.io/library/fail-me:dev" {
                    let mut trailers = vec![0x88]; // :status 200
                    encode_hpack_huffman_header(&mut trailers, "grpc-status", "13", false);
                    encode_hpack_huffman_header(&mut trailers, "grpc-message", "bolt%20err", false);
                    write_frame(
                        &mut conn,
                        FRAME_HEADERS,
                        FLAG_END_HEADERS | FLAG_END_STREAM,
                        1,
                        &trailers,
                    )
                    .unwrap();
                    conn.flush().unwrap();
                    continue;
                }

                let mut map = server_store.lock().unwrap();
                let matched = target_digest
                    .as_deref()
                    .is_some_and(|want| map.get(&name).is_some_and(|cur| cur == want));
                if matched {
                    map.remove(&name);
                    let mut hdr = vec![0x88]; // :status 200
                    encode_hpack_huffman_header(
                        &mut hdr,
                        "content-type",
                        "application/grpc",
                        false,
                    );
                    write_frame(&mut conn, FRAME_HEADERS, FLAG_END_HEADERS, 1, &hdr).unwrap();
                    write_frame(&mut conn, FRAME_DATA, 0, 1, &[0, 0, 0, 0, 0]).unwrap();
                    let mut trl = Vec::new();
                    encode_hpack_huffman_header(&mut trl, "grpc-status", "0", false);
                    write_frame(
                        &mut conn,
                        FRAME_HEADERS,
                        FLAG_END_HEADERS | FLAG_END_STREAM,
                        1,
                        &trl,
                    )
                    .unwrap();
                } else {
                    // Trailers-Only gRPC NOT_FOUND (status 5)
                    let mut trl = vec![0x88]; // :status 200
                    encode_hpack_huffman_header(
                        &mut trl,
                        "content-type",
                        "application/grpc",
                        false,
                    );
                    encode_hpack_huffman_header(&mut trl, "grpc-status", "5", false);
                    encode_hpack_huffman_header(
                        &mut trl,
                        "grpc-message",
                        "target%20digest%20mismatch",
                        false,
                    );
                    write_frame(
                        &mut conn,
                        FRAME_HEADERS,
                        FLAG_END_HEADERS | FLAG_END_STREAM,
                        1,
                        &trl,
                    )
                    .unwrap();
                }
                conn.flush().unwrap();
            }
        });

        // 1. Matching rewritten target -> deleted.
        let r1 = delete_image_if_target_matches(
            &sock,
            "k8s.io",
            "docker.io/library/retire-me:dev",
            "sha256:rewritten1",
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(r1, DeleteOutcome::Deleted);
        assert!(
            !store
                .lock()
                .unwrap()
                .contains_key("docker.io/library/retire-me:dev")
        );

        // 2. Replaced by a native kubelet pull -> NOT_FOUND, successor preserved!
        let r2 = delete_image_if_target_matches(
            &sock,
            "k8s.io",
            "docker.io/library/replaced:dev",
            "sha256:rewritten2",
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(r2, DeleteOutcome::NotFoundOrChanged);
        assert_eq!(
            store
                .lock()
                .unwrap()
                .get("docker.io/library/replaced:dev")
                .map(String::as_str),
            Some("sha256:native2"),
            "native replacement must not be deleted"
        );

        // 3. Non-NOT_FOUND server error surfaces decoded grpc-message.
        let err = delete_image_if_target_matches(
            &sock,
            "k8s.io",
            "docker.io/library/fail-me:dev",
            "sha256:any",
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(err.contains("grpc-status 13"), "{err}");
        assert!(err.contains("bolt err"), "{err}");

        server.join().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }
}
