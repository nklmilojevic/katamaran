import Foundation

// MARK: - Engine API models
//
// Only the fields the app shows. Docker adds fields freely and leaves some
// out (or null), so nearly everything is optional.

/// One container from `GET /containers/json?all=1`.
struct ContainerSummary: Decodable, Identifiable, Equatable {
    var Id: String
    var Names: [String]
    var Image: String
    var ImageID: String?
    var Command: String?
    var Created: Int64?
    var State: String
    var Status: String
    var Ports: [ContainerPort]?
    var Labels: [String: String]?
    var NetworkSettings: SummaryNetworks?
    var Mounts: [ContainerMount]?

    var id: String { Id }
    var name: String { Names.first.map { $0.hasPrefix("/") ? String($0.dropFirst()) : $0 } ?? shortID }
    var shortID: String { String(Id.prefix(12)) }
    var running: Bool { State == "running" }
    var project: String? { Labels?[ComposeLabel.project].flatMap { $0.isEmpty ? nil : $0 } }
    var service: String? { Labels?[ComposeLabel.service] }
    var createdDate: Date? { Created.map { Date(timeIntervalSince1970: TimeInterval($0)) } }

    /// Published TCP ports, once each (Docker lists IPv4 and IPv6 separately).
    var publishedPorts: [UInt16] {
        var seen = Set<UInt16>()
        return (Ports ?? []).compactMap { p in
            guard let pub = p.PublicPort, p.type == "tcp", seen.insert(pub).inserted else { return nil }
            return pub
        }.sorted()
    }

    var networkNames: [String] { (NetworkSettings?.Networks ?? [:]).keys.sorted() }
    var volumeNames: [String] { (Mounts ?? []).filter { $0.type == "volume" }.compactMap(\.Name) }

    /// How the row's dot looks: running, paused, failed or stopped.
    var health: ContainerHealth {
        switch State {
        case "running": Status.contains("(unhealthy)") ? .failed : .running
        case "paused", "restarting", "created", "removing": .transitional
        case "dead": .failed
        default: ContainerSummary.exitCode(Status).map { $0 != 0 && $0 != 137 && $0 != 143 } == true ? .failed : .stopped
        }
    }

    /// The code in "Exited (1) 5 minutes ago".
    static func exitCode(_ status: String) -> Int? {
        guard status.hasPrefix("Exited ("), let close = status.firstIndex(of: ")") else { return nil }
        return Int(status[status.index(status.startIndex, offsetBy: 8)..<close])
    }
}

enum ContainerHealth { case running, transitional, failed, stopped }

enum ComposeLabel {
    static let project = "com.docker.compose.project"
    static let service = "com.docker.compose.service"
    static let workingDir = "com.docker.compose.project.working_dir"
    static let configFiles = "com.docker.compose.project.config_files"
}

struct ContainerPort: Decodable, Equatable {
    var IP: String?
    var PrivatePort: UInt16
    var PublicPort: UInt16?
    var type: String

    enum CodingKeys: String, CodingKey {
        case IP, PrivatePort, PublicPort
        case type = "Type"
    }
}

struct SummaryNetworks: Decodable, Equatable {
    var Networks: [String: EndpointSettings]?
}

struct EndpointSettings: Decodable, Equatable {
    var NetworkID: String?
    var IPAddress: String?
    var Gateway: String?
    var MacAddress: String?
}

struct ContainerMount: Decodable, Equatable {
    var type: String?
    var Name: String?
    var Source: String?
    var Destination: String?
    var RW: Bool?

    enum CodingKeys: String, CodingKey {
        case type = "Type"
        case Name, Source, Destination, RW
    }
}

/// `GET /containers/{id}/json`.
struct ContainerInspect: Decodable, Equatable {
    var Id: String
    var Name: String
    var Created: String?
    var Path: String?
    var Args: [String]?
    var State: InspectState
    var Image: String?
    var Config: InspectConfig
    var HostConfig: InspectHostConfig?
    var Mounts: [ContainerMount]?
    var NetworkSettings: InspectNetworkSettings?

    var name: String { Name.hasPrefix("/") ? String(Name.dropFirst()) : Name }
    var tty: Bool { Config.Tty ?? false }

    /// The command line, as `docker ps --no-trunc` would show it.
    var commandLine: String {
        ([Path ?? ""] + (Args ?? [])).filter { !$0.isEmpty }.map(shellQuote).joined(separator: " ")
    }

    /// `KEY=value` pairs split once, in Docker's order.
    var environment: [(String, String)] {
        (Config.Env ?? []).map { line in
            guard let eq = line.firstIndex(of: "=") else { return (line, "") }
            return (String(line[..<eq]), String(line[line.index(after: eq)...]))
        }
    }

    /// Published ports as "host → container/proto", sorted by host port.
    var portBindings: [PortBinding] {
        var out: [PortBinding] = []
        var seen = Set<String>()
        for (key, bindings) in NetworkSettings?.Ports ?? [:] {
            let parts = key.split(separator: "/")
            guard let container = parts.first.flatMap({ UInt16($0) }) else { continue }
            let proto = parts.count > 1 ? String(parts[1]) : "tcp"
            for b in bindings ?? [] {
                guard let host = b.HostPort.flatMap(UInt16.init) else { continue }
                if seen.insert("\(host)/\(container)/\(proto)").inserted {
                    out.append(PortBinding(host: host, container: container, proto: proto))
                }
            }
        }
        return out.sorted { ($0.host, $0.container) < ($1.host, $1.container) }
    }
}

struct PortBinding: Equatable {
    var host: UInt16
    var container: UInt16
    var proto: String
}

struct InspectState: Decodable, Equatable {
    var Status: String
    var Running: Bool?
    var ExitCode: Int?
    var StartedAt: String?
    var FinishedAt: String?
    var Health: InspectHealth?
}

struct InspectHealth: Decodable, Equatable {
    var Status: String?
}

struct InspectConfig: Decodable, Equatable {
    var Image: String?
    var Env: [String]?
    var Labels: [String: String]?
    var Tty: Bool?
    var WorkingDir: String?
    var User: String?
    var Hostname: String?
}

struct InspectHostConfig: Decodable, Equatable {
    var RestartPolicy: RestartPolicy?
    var NetworkMode: String?
}

struct RestartPolicy: Decodable, Equatable {
    var Name: String?
}

struct InspectNetworkSettings: Decodable, Equatable {
    var Networks: [String: EndpointSettings]?
    var Ports: [String: [HostBinding]?]?
}

struct HostBinding: Decodable, Equatable {
    var HostIp: String?
    var HostPort: String?
}

/// A volume from `GET /system/df?type=volume`, which also carries sizes.
struct VolumeInfo: Decodable, Identifiable, Equatable {
    var Name: String
    var Driver: String?
    var Mountpoint: String?
    var CreatedAt: String?
    var Labels: [String: String]?
    var UsageData: VolumeUsage?

    var id: String { Name }
    var size: Int64? { UsageData.flatMap { $0.Size >= 0 ? $0.Size : nil } }
    var refCount: Int64? { UsageData.flatMap { $0.RefCount >= 0 ? $0.RefCount : nil } }
    var project: String? { Labels?[ComposeLabel.project] }
}

struct VolumeUsage: Decodable, Equatable {
    var Size: Int64
    var RefCount: Int64
}

struct SystemDF: Decodable {
    var Volumes: [VolumeInfo]?
}

/// `GET /images/json`.
struct ImageInfo: Decodable, Identifiable, Equatable {
    var Id: String
    var RepoTags: [String]?
    var RepoDigests: [String]?
    var Created: Int64?
    var Size: Int64?
    var Containers: Int?
    var Labels: [String: String]?

    var id: String { Id }
    var shortID: String { String(Id.replacingOccurrences(of: "sha256:", with: "").prefix(12)) }
    var tags: [String] { (RepoTags ?? []).filter { $0 != "<none>:<none>" } }
    var name: String {
        if let t = tags.first { return t }
        if let d = RepoDigests?.first, let at = d.firstIndex(of: "@") { return String(d[..<at]) + " (untagged)" }
        return shortID
    }
    var createdDate: Date? { Created.map { Date(timeIntervalSince1970: TimeInterval($0)) } }
}

/// `GET /networks`.
struct NetworkInfo: Decodable, Identifiable, Equatable {
    var Name: String
    var Id: String
    var Created: String?
    var Scope: String?
    var Driver: String?
    var Internal: Bool?
    var IPAM: NetworkIPAM?
    var Labels: [String: String]?

    var id: String { Id }
    var shortID: String { String(Id.prefix(12)) }
    /// Docker's own networks, which can't be removed.
    var builtIn: Bool { ["bridge", "host", "none"].contains(Name) }
    var subnets: [String] { (IPAM?.Config ?? []).compactMap(\.Subnet) }
    var gateways: [String] { (IPAM?.Config ?? []).compactMap(\.Gateway) }
    var project: String? { Labels?[ComposeLabel.project] }
}

struct NetworkIPAM: Decodable, Equatable {
    var Driver: String?
    var Config: [IPAMConfig]?
}

struct IPAMConfig: Decodable, Equatable {
    var Subnet: String?
    var Gateway: String?
}

/// One line of `GET /events`.
struct DockerEvent: Decodable, Equatable {
    var `Type`: String?
    var Action: String?
    var Actor: EventActor?

    struct EventActor: Decodable, Equatable {
        var ID: String?
    }

    /// Events that change what the lists show; exec and attach chatter doesn't.
    var isRelevant: Bool {
        guard let a = Action else { return false }
        return !(a.hasPrefix("exec_") || ["attach", "detach", "resize", "top", "archive-path", "copy", "export"].contains(a))
    }
}

/// One reading of `GET /containers/{id}/stats`: cumulative counters, so rates
/// need two of them.
struct StatsSample: Equatable {
    var read: Date
    var cpuTotal: UInt64
    var systemTotal: UInt64
    var onlineCPUs: Int
    var memoryUsed: UInt64
    var memoryLimit: UInt64
    var netRx: UInt64
    var netTx: UInt64
    var blockRead: UInt64
    var blockWrite: UInt64
    var pids: UInt64

    /// Percent of one CPU, as `docker stats` shows it (400% = four busy CPUs).
    func cpuPercent(since prev: StatsSample) -> Double {
        guard cpuTotal >= prev.cpuTotal, systemTotal > prev.systemTotal else { return 0 }
        let cpu = Double(cpuTotal - prev.cpuTotal)
        let system = Double(systemTotal - prev.systemTotal)
        return cpu / system * Double(max(onlineCPUs, 1)) * 100
    }

    /// Bytes per second of each counter since `prev`.
    func rates(since prev: StatsSample) -> (rx: Double, tx: Double, read: Double, write: Double) {
        let dt = max(read.timeIntervalSince(prev.read), 0.001)
        func r(_ a: UInt64, _ b: UInt64) -> Double { a >= b ? Double(a - b) / dt : 0 }
        return (r(netRx, prev.netRx), r(netTx, prev.netTx), r(blockRead, prev.blockRead), r(blockWrite, prev.blockWrite))
    }

    /// The reading and, when Docker included one, the reading before it.
    static func parse(_ data: Data) throws -> (current: StatsSample, previous: StatsSample?) {
        let raw = try JSONDecoder().decode(RawStats.self, from: data)
        let mem = raw.memory_stats
        // Page cache doesn't count, as in `docker stats` (cgroup v2, then v1).
        let cache = mem?.stats?["inactive_file"] ?? mem?.stats?["total_inactive_file"] ?? 0
        let used = (mem?.usage ?? 0) >= cache ? (mem?.usage ?? 0) - cache : 0
        var rx: UInt64 = 0, tx: UInt64 = 0
        for n in (raw.networks ?? [:]).values { rx += n.rx_bytes ?? 0; tx += n.tx_bytes ?? 0 }
        var rd: UInt64 = 0, wr: UInt64 = 0
        for e in raw.blkio_stats?.io_service_bytes_recursive ?? [] {
            switch e.op?.lowercased() {
            case "read": rd += e.value ?? 0
            case "write": wr += e.value ?? 0
            default: break
            }
        }
        func sample(_ cpu: RawStats.CPU?, read: Date) -> StatsSample {
            StatsSample(
                read: read,
                cpuTotal: cpu?.cpu_usage?.total_usage ?? 0,
                systemTotal: cpu?.system_cpu_usage ?? 0,
                onlineCPUs: cpu?.online_cpus ?? cpu?.cpu_usage?.percpu_usage?.count ?? 1,
                memoryUsed: used, memoryLimit: mem?.limit ?? 0,
                netRx: rx, netTx: tx, blockRead: rd, blockWrite: wr,
                pids: raw.pids_stats?.current ?? 0)
        }
        let now = raw.read.flatMap(parseDockerDate) ?? Date()
        let current = sample(raw.cpu_stats, read: now)
        var previous: StatsSample? = nil
        if let pre = raw.precpu_stats, (pre.system_cpu_usage ?? 0) > 0 {
            previous = sample(pre, read: raw.preread.flatMap(parseDockerDate) ?? now.addingTimeInterval(-1))
            // Only the CPU counters are in the previous reading.
            previous?.netRx = rx; previous?.netTx = tx
            previous?.blockRead = rd; previous?.blockWrite = wr
        }
        return (current, previous)
    }

    private struct RawStats: Decodable {
        var read: String?
        var preread: String?
        var cpu_stats: CPU?
        var precpu_stats: CPU?
        var memory_stats: Memory?
        var networks: [String: Net]?
        var blkio_stats: Blkio?
        var pids_stats: Pids?

        struct CPU: Decodable {
            var cpu_usage: Usage?
            var system_cpu_usage: UInt64?
            var online_cpus: Int?
        }
        struct Usage: Decodable {
            var total_usage: UInt64?
            var percpu_usage: [UInt64]?
        }
        struct Memory: Decodable {
            var usage: UInt64?
            var limit: UInt64?
            var stats: [String: UInt64]?
        }
        struct Net: Decodable {
            var rx_bytes: UInt64?
            var tx_bytes: UInt64?
        }
        struct Blkio: Decodable {
            var io_service_bytes_recursive: [Entry]?
            struct Entry: Decodable {
                var op: String?
                var value: UInt64?
            }
        }
        struct Pids: Decodable {
            var current: UInt64?
        }
    }
}

/// Docker's RFC 3339 timestamps, with or without nanoseconds.
func parseDockerDate(_ s: String) -> Date? {
    guard !s.hasPrefix("0001-") else { return nil }
    let f = ISO8601DateFormatter()
    f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    // Foundation reads at most milliseconds; drop the rest of the fraction.
    var t = s
    if let dot = t.firstIndex(of: "."), let end = t[dot...].firstIndex(where: { $0 == "Z" || $0 == "+" || $0 == "-" }) {
        let frac = t[t.index(after: dot)..<end]
        t.replaceSubrange(dot..<end, with: "." + frac.prefix(3).padding(toLength: 3, withPad: "0", startingAt: 0))
        return f.date(from: t)
    }
    f.formatOptions = [.withInternetDateTime]
    return f.date(from: t)
}

func shellQuote(_ s: String) -> String {
    let safe = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_./:=@%+,"))
    if !s.isEmpty && s.unicodeScalars.allSatisfy(safe.contains) { return s }
    return "'" + s.replacingOccurrences(of: "'", with: "'\\''") + "'"
}

// MARK: - Streaming decoders

/// Decodes `Transfer-Encoding: chunked` as bytes arrive.
struct ChunkedDecoder {
    private enum State { case size, data(Int), crlf }
    private var state = State.size
    private var buffer: [UInt8] = []
    private(set) var finished = false

    mutating func feed(_ data: Data) throws -> Data {
        buffer.append(contentsOf: data)
        var out = Data()
        var i = 0
        loop: while !finished {
            switch state {
            case .size:
                guard let nl = buffer[i...].firstIndex(of: 0x0A) else { break loop }
                var line = String(decoding: buffer[i..<nl], as: UTF8.self)
                if line.hasSuffix("\r") { line.removeLast() }
                let hex = line.split(separator: ";", omittingEmptySubsequences: false).first
                    .map { $0.trimmingCharacters(in: .whitespaces) } ?? ""
                let isASCIIHex = !hex.isEmpty && hex.utf8.allSatisfy { b in
                    (0x30...0x39).contains(b) || (0x41...0x46).contains(b) || (0x61...0x66).contains(b)
                }
                guard isASCIIHex, let size = Int(hex, radix: 16), size >= 0 else {
                    throw SocketError.protocolError("bad chunk size")
                }
                i = nl + 1
                if size == 0 { finished = true } else { state = .data(size) }
            case .data(let left):
                let n = min(left, buffer.count - i)
                if n == 0 { break loop }
                out.append(contentsOf: buffer[i..<i + n])
                i += n
                state = left == n ? .crlf : .data(left - n)
            case .crlf:
                guard buffer.count - i >= 2 else { break loop }
                guard buffer[i] == 0x0D && buffer[i + 1] == 0x0A else {
                    throw SocketError.protocolError("bad chunk terminator")
                }
                i += 2
                state = .size
            }
        }
        buffer.removeFirst(i)
        return out
    }
}

/// Splits a byte stream into lines, keeping a partial last line for later.
struct LineSplitter {
    private var pending = Data()

    mutating func feed(_ data: Data) -> [Data] {
        pending.append(data)
        var lines: [Data] = []
        var start = pending.startIndex
        while let nl = pending[start...].firstIndex(of: 0x0A) {
            var end = nl
            if end > start && pending[pending.index(before: end)] == 0x0D {
                end = pending.index(before: end)
            }
            lines.append(Data(pending[start..<end]))
            start = pending.index(after: nl)
        }
        if start > pending.startIndex {
            pending = start < pending.endIndex ? Data(pending[start...]) : Data()
        }
        return lines
    }

    /// What's left after the stream ended.
    mutating func flush() -> Data? {
        defer { pending = Data() }
        return pending.isEmpty ? nil : pending
    }
}

/// Splits the logs of a container without a TTY into its stdout and stderr
/// frames: an 8-byte header (stream, 0, 0, 0, big-endian length), then the
/// payload. With a TTY, the stream is the raw output.
struct LogDemuxer {
    enum Stream: UInt8 { case stdin = 0, stdout = 1, stderr = 2 }
    let multiplexed: Bool
    private var buffer: [UInt8] = []

    init(multiplexed: Bool) { self.multiplexed = multiplexed }

    mutating func feed(_ data: Data) -> [(Stream, Data)] {
        guard multiplexed else { return data.isEmpty ? [] : [(.stdout, data)] }
        buffer.append(contentsOf: data)
        var out: [(Stream, Data)] = []
        var i = 0
        while buffer.count - i >= 8 {
            let size = Int(buffer[i + 4]) << 24 | Int(buffer[i + 5]) << 16 | Int(buffer[i + 6]) << 8 | Int(buffer[i + 7])
            guard buffer.count - i - 8 >= size else { break }
            let stream = Stream(rawValue: buffer[i]) ?? .stdout
            out.append((stream, Data(buffer[(i + 8)..<(i + 8 + size)])))
            i += 8 + size
        }
        buffer.removeFirst(i)
        return out
    }
}

// MARK: - Client

/// Parsed HTTP/1.1 response status line and framing headers.
struct HTTPResponseHead: Equatable {
    let code: Int
    let chunked: Bool
    let contentLength: Int?
    let connectionClose: Bool
}

/// Just enough HTTP/1.1 for the Docker Engine API on a Unix socket. Blocking:
/// call it off the main thread.
struct DockerClient: Sendable {
    let socketPath: String

    func request(_ method: String, _ path: String, timeout: TimeInterval = 15) throws -> (Int, Data) {
        try DockerClient.exchange(socketPath, host: "docker", method, path, timeout: timeout)
    }

    /// One HTTP request over a Unix socket, read to the end.
    static func exchange(_ socketPath: String, host: String, _ method: String, _ path: String,
                         timeout: TimeInterval) throws -> (Int, Data) {
        let sock = try UnixSocket(path: socketPath, timeout: timeout)
        try sock.write(Data(head(method, path, host: host).utf8))
        var raw = Data()
        while true {
            let chunk = try sock.read()
            if chunk.isEmpty { break }
            raw.append(chunk)
        }
        return try parse(raw)
    }

    static func head(_ method: String, _ path: String, close: Bool = true, host: String = "docker") -> String {
        "\(method) \(path) HTTP/1.1\r\nHost: \(host)\r\nUser-Agent: katamaran-app\r\nContent-Length: 0\r\n"
            + (close ? "Connection: close\r\n" : "") + "\r\n"
    }

    /// Status code and body of a complete response (chunked or not).
    static func parse(_ raw: Data) throws -> (Int, Data) {
        guard let sep = raw.range(of: Data("\r\n\r\n".utf8)) else {
            throw SocketError.protocolError("incomplete HTTP response")
        }
        let head = try parseResponseHead(raw[raw.startIndex..<sep.lowerBound])
        var body = Data(raw[sep.upperBound...])
        if head.chunked {
            var d = ChunkedDecoder()
            body = try d.feed(body)
            guard d.finished else { throw SocketError.protocolError("truncated chunked response") }
        }
        return (head.code, body)
    }

    /// Parses the status code and framing headers from an HTTP response head.
    static func parseResponseHead(_ head: Data) throws -> HTTPResponseHead {
        let lines = String(decoding: head, as: UTF8.self).components(separatedBy: "\r\n")
        let parts = lines.first?.split(separator: " ") ?? []
        guard parts.count >= 2, let code = Int(parts[1]) else {
            throw SocketError.protocolError("bad status line")
        }
        var chunked = false
        var contentLength: Int? = nil
        var connectionClose = false
        for line in lines.dropFirst() {
            guard let colon = line.firstIndex(of: ":") else { continue }
            let name = line[..<colon].trimmingCharacters(in: .whitespaces).lowercased()
            let val = line[line.index(after: colon)...].trimmingCharacters(in: .whitespaces).lowercased()
            if name == "transfer-encoding" && val.contains("chunked") {
                chunked = true
            } else if name == "content-length", let n = Int(val), n >= 0 {
                contentLength = n
            } else if name == "connection" && val.split(separator: ",").map({ $0.trimmingCharacters(in: .whitespaces) }).contains("close") {
                connectionClose = true
            }
        }
        return HTTPResponseHead(code: code, chunked: chunked, contentLength: contentLength, connectionClose: connectionClose)
    }

    /// The status code, and whether the body is chunked.
    static func parseHead(_ head: Data) throws -> (Int, Bool) {
        let parsed = try parseResponseHead(head)
        return (parsed.code, parsed.chunked)
    }

    /// Reads an HTTP error body without blocking indefinitely on keep-alive sockets.
    static func readErrorBody(
        from sock: UnixSocket,
        head: HTTPResponseHead,
        initial: Data,
        maxBytes: Int = 65_536,
        timeout: TimeInterval = 2
    ) -> Data {
        if head.code == 204 || head.code == 304 { return Data() }
        sock.setTimeout(timeout)
        if head.chunked {
            var decoder = ChunkedDecoder()
            var out = Data()
            func append(_ piece: Data) {
                let remaining = maxBytes - out.count
                if remaining > 0 { out.append(piece.prefix(remaining)) }
            }
            if let piece = try? decoder.feed(initial) {
                append(piece)
            } else {
                return Data(initial.prefix(maxBytes))
            }
            while !decoder.finished && out.count < maxBytes {
                guard let more = try? sock.read(), !more.isEmpty else { break }
                guard let piece = try? decoder.feed(more) else { break }
                append(piece)
            }
            return out
        }
        if let length = head.contentLength {
            let target = min(length, maxBytes)
            var rest = Data(initial.prefix(maxBytes))
            while rest.count < target {
                guard let more = try? sock.read(max: min(65_536, maxBytes - rest.count)), !more.isEmpty else { break }
                rest.append(more.prefix(maxBytes - rest.count))
            }
            return rest
        }
        var rest = Data(initial.prefix(maxBytes))
        while rest.count < maxBytes {
            guard let more = try? sock.read(max: min(65_536, maxBytes - rest.count)), !more.isEmpty else { break }
            rest.append(more.prefix(maxBytes - rest.count))
        }
        return rest
    }

    /// Docker's `{"message": …}`, or the status code.
    static func failure(_ code: Int, _ body: Data) -> SocketError {
        let msg = (try? JSONSerialization.jsonObject(with: body) as? [String: Any])?["message"] as? String
        return SocketError.protocolError(msg ?? "Docker answered HTTP \(code)")
    }

    func get<T: Decodable>(_ path: String, as: T.Type = T.self, timeout: TimeInterval = 15) throws -> T {
        let (code, body) = try request("GET", path, timeout: timeout)
        guard code == 200 else { throw DockerClient.failure(code, body) }
        return try JSONDecoder().decode(T.self, from: body)
    }

    @discardableResult
    func send(_ method: String, _ path: String, timeout: TimeInterval = 60) throws -> Data {
        let (code, body) = try request(method, path, timeout: timeout)
        guard (200..<300).contains(code) || code == 304 else { throw DockerClient.failure(code, body) }
        return body
    }

    func containers() throws -> [ContainerSummary] { try get("/containers/json?all=1") }
    func inspect(_ id: String) throws -> ContainerInspect { try get("/containers/\(id)/json") }
    func images() throws -> [ImageInfo] { try get("/images/json") }
    func networks() throws -> [NetworkInfo] { try get("/networks") }
    func volumes() throws -> [VolumeInfo] {
        try get("/system/df?type=volume", as: SystemDF.self, timeout: 60).Volumes ?? []
    }

    /// One reading, without the second Docker would wait for to fill in `precpu`.
    func stats(_ id: String) throws -> StatsSample {
        let (code, body) = try request("GET", "/containers/\(id)/stats?stream=false&one-shot=true", timeout: 10)
        guard code == 200 else { throw DockerClient.failure(code, body) }
        return try StatsSample.parse(body).current
    }

    func act(_ id: String, _ action: ContainerAction) throws {
        switch action {
        case .start: try send("POST", "/containers/\(id)/start")
        case .stop: try send("POST", "/containers/\(id)/stop?t=5")
        case .restart: try send("POST", "/containers/\(id)/restart?t=5")
        case .remove: try send("DELETE", "/containers/\(id)?force=1")
        }
    }

    func removeVolume(_ name: String) throws { try send("DELETE", "/volumes/\(percent(name))") }
    func removeImage(_ id: String, force: Bool = true) throws {
        try send("DELETE", "/images/\(percent(id))\(force ? "?force=1" : "")")
    }
    func removeNetwork(_ id: String) throws { try send("DELETE", "/networks/\(percent(id))") }

    /// Remove everything of `kind` that nothing uses; returns the bytes freed.
    @discardableResult
    func prune(_ kind: DockerKind) throws -> Int64 {
        let path: String = switch kind {
        // Named volumes too, as `docker volume prune --all` does.
        case .volumes: "/volumes/prune?filters=" + percent(#"{"all":["true"]}"#)
        // Every image no container uses, as `docker image prune --all`.
        case .images: "/images/prune?filters=" + percent(#"{"dangling":["false"]}"#)
        case .networks: "/networks/prune"
        case .containers: "/containers/prune"
        }
        let body = try send("POST", path, timeout: 300)
        let obj = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
        return (obj?["SpaceReclaimed"] as? NSNumber)?.int64Value ?? 0
    }

    private func percent(_ s: String) -> String {
        s.addingPercentEncoding(withAllowedCharacters: .alphanumerics.union(CharacterSet(charactersIn: "-._~:"))) ?? s
    }
}

enum ContainerAction: Sendable { case start, stop, restart, remove }

enum DockerKind: String, CaseIterable, Sendable { case containers, volumes, images, networks }

/// A long-lived response (events, logs, stats), read on its own thread. The
/// body arrives on the main queue in pieces, de-chunked; `onEnd` is called
/// once, with the error unless the stream was cancelled.
final class DockerStream: @unchecked Sendable {
    private let socketPath: String
    private let path: String
    private let host: String
    private let lock = NSLock()
    private var socket: UnixSocket?
    private var cancelled = false
    var onData: (Data) -> Void = { _ in }
    var onEnd: (Error?) -> Void = { _ in }

    init(socketPath: String, path: String, host: String = "docker") {
        self.socketPath = socketPath
        self.path = path
        self.host = host
    }

    func start() {
        let t = Thread { [self] in run() }
        t.name = "docker-stream"
        t.start()
    }

    func cancel() {
        lock.lock()
        cancelled = true
        if let s = socket { Darwin.shutdown(s.fd, SHUT_RDWR) }
        lock.unlock()
    }

    private var isCancelled: Bool {
        lock.lock(); defer { lock.unlock() }
        return cancelled
    }

    private func deliver(_ data: Data) {
        guard !data.isEmpty else { return }
        DispatchQueue.main.async { [self] in if !isCancelled { onData(data) } }
    }

    private func run() {
        var failure: Error? = nil
        do {
            // No read timeout: a quiet container can log nothing for hours.
            let sock = try UnixSocket(path: socketPath, timeout: 0)
            lock.lock()
            if cancelled { lock.unlock(); return }
            socket = sock
            lock.unlock()
            defer { lock.lock(); socket = nil; lock.unlock() }
            try sock.write(Data(DockerClient.head("GET", path, close: false, host: host).utf8))

            var head = Data()
            var body = Data()
            while true {
                let chunk = try sock.read()
                if chunk.isEmpty { throw SocketError.closed }
                head.append(chunk)
                if let sep = head.range(of: Data("\r\n\r\n".utf8)) {
                    body = Data(head[sep.upperBound...])
                    head = Data(head[head.startIndex..<sep.lowerBound])
                    break
                }
            }
            let parsed = try DockerClient.parseResponseHead(head)
            guard parsed.code == 200 else {
                let rest = DockerClient.readErrorBody(from: sock, head: parsed, initial: body)
                throw DockerClient.failure(parsed.code, rest)
            }
            let chunked = parsed.chunked
            var decoder = ChunkedDecoder()
            func pass(_ d: Data) throws { deliver(chunked ? try decoder.feed(d) : d) }
            try pass(body)
            while !decoder.finished {
                let chunk = try sock.read()
                if chunk.isEmpty { break }
                try pass(chunk)
            }
        } catch {
            failure = error
        }
        let err = failure
        DispatchQueue.main.async { [self] in onEnd(isCancelled ? nil : err) }
    }
}
