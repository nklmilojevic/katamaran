import Foundation

enum SocketError: Error, LocalizedError {
    case path(String)
    case connect(String, Int32)
    case io(Int32)
    case closed
    case protocolError(String)
    case rpc(Int, String)

    var errorDescription: String? {
        switch self {
        case .path(let p): "socket path too long: \(p)"
        case .connect(let p, let e): "connect \(p): \(String(cString: strerror(e)))"
        case .io(let e): String(cString: strerror(e))
        case .closed: "connection closed"
        case .protocolError(let m): m
        case .rpc(_, let m): m
        }
    }
}

/// A blocking stream socket to a Unix-domain path. Call only off the main thread.
final class UnixSocket {
    let fd: Int32

    init(path: String, timeout: TimeInterval = 10) throws {
        fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw SocketError.io(errno) }
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let bytes = Array(path.utf8)
        let cap = MemoryLayout.size(ofValue: addr.sun_path)
        guard bytes.count < cap else { close(fd); throw SocketError.path(path) }
        withUnsafeMutableBytes(of: &addr.sun_path) { buf in
            for (i, b) in bytes.enumerated() { buf[i] = b }
        }
        setTimeout(timeout)
        var one: Int32 = 1
        setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
        let rc = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        if rc != 0 {
            let e = errno
            close(fd)
            throw SocketError.connect(path, e)
        }
    }

    deinit { close(fd) }

    /// Converts a timeout in seconds into `timeval`, populating `tv_usec` from the
    /// fractional part and clamping any `timeout > 0` to at least 1 µs so sub-second
    /// timeouts never collapse to infinite (`0`).
    static func makeTimeval(_ timeout: TimeInterval) -> timeval {
        guard timeout.isFinite, timeout > 0 else { return timeval(tv_sec: 0, tv_usec: 0) }
        let clamped = min(timeout, Double(Int32.max))
        let totalUsec = max(Int64(1), Int64((clamped * 1_000_000).rounded()))
        return timeval(tv_sec: Int(totalUsec / 1_000_000), tv_usec: Int32(totalUsec % 1_000_000))
    }

    func setTimeout(_ timeout: TimeInterval) {
        var tv = Self.makeTimeval(timeout)
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))
    }

    func write(_ data: Data) throws {
        try data.withUnsafeBytes { (buf: UnsafeRawBufferPointer) in
            var off = 0
            while off < buf.count {
                let n = Darwin.write(fd, buf.baseAddress! + off, buf.count - off)
                if n < 0 {
                    if errno == EINTR { continue }
                    throw SocketError.io(errno)
                }
                if n == 0 { throw SocketError.closed }
                off += n
            }
        }
    }

    func read(max: Int = 65536) throws -> Data {
        var buf = [UInt8](repeating: 0, count: max)
        while true {
            let n = Darwin.read(fd, &buf, max)
            if n < 0 {
                if errno == EINTR { continue }
                throw SocketError.io(errno)
            }
            return Data(buf[0..<n])
        }
    }

    func shutdownWrite() { Darwin.shutdown(fd, SHUT_WR) }
}

/// JSON-RPC 2.0 over the daemon's socket, one JSON object per line
/// (`crates/kat-daemon/src/api.rs`). One connection per call keeps it simple.
struct DaemonClient {
    let socketPath: String

    static func defaultSocket() -> String {
        let env = ProcessInfo.processInfo.environment
        let home = env["KATAMARAN_HOME"].flatMap { $0.isEmpty ? nil : $0 }
            ?? (NSHomeDirectory() as NSString).appendingPathComponent(".katamaran")
        return (home as NSString).appendingPathComponent("run/daemon.sock")
    }

    func call(_ method: String, _ params: [String: Any]? = nil, timeout: TimeInterval = 10) throws -> Data {
        let sock = try UnixSocket(path: socketPath, timeout: timeout)
        var req: [String: Any] = ["jsonrpc": "2.0", "id": 1, "method": method]
        if let params { req["params"] = params }
        var line = try JSONSerialization.data(withJSONObject: req)
        line.append(0x0A)
        try sock.write(line)
        var buf = Data()
        while !buf.contains(0x0A) {
            let chunk = try sock.read()
            if chunk.isEmpty { throw SocketError.closed }
            buf.append(chunk)
        }
        let lineData = buf.prefix(upTo: buf.firstIndex(of: 0x0A)!)
        guard let obj = try JSONSerialization.jsonObject(with: lineData) as? [String: Any] else {
            throw SocketError.protocolError("bad response")
        }
        if let err = obj["error"] as? [String: Any] {
            throw SocketError.rpc(err["code"] as? Int ?? 0, err["message"] as? String ?? "error")
        }
        let result = obj["result"] ?? NSNull()
        if result is NSNull { return Data("null".utf8) }
        return try JSONSerialization.data(withJSONObject: result, options: [.fragmentsAllowed])
    }

    func decode<T: Decodable>(_ method: String, _ params: [String: Any]? = nil, timeout: TimeInterval = 10) throws -> T {
        try JSONDecoder().decode(T.self, from: call(method, params, timeout: timeout))
    }
}
