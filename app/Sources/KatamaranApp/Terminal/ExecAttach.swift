import Foundation

/// A shell in a container through the Docker Engine API: `POST /containers/{id}/exec` with a
/// TTY, then `POST /exec/{id}/start` on a connection the daemon hijacks (HTTP 101 UPGRADED,
/// then raw bytes both ways; with a TTY there is no stdout/stderr framing), resized with
/// `POST /exec/{id}/resize`, and its exit code from `GET /exec/{id}/json`.
enum ExecAPI {
    /// The command: bash when the image has it, else sh.
    static let command = ["sh", "-c", ExecCommand.shell]

    /// The variable that marks a session's processes, for `hangUpBody`.
    static let sessionVariable = "KATAMARAN_SESSION"

    static func createBody(cols: Int, rows: Int, session: String) -> Data {
        let body: [String: Any] = [
            "AttachStdin": true,
            "AttachStdout": true,
            "AttachStderr": true,
            "Tty": true,
            "Cmd": command,
            "Env": ["TERM=xterm-256color", "COLORTERM=truecolor", "\(sessionVariable)=\(session)"],
            "ConsoleSize": [max(1, rows), max(1, cols)],
        ]
        return (try? JSONSerialization.data(withJSONObject: body, options: [.sortedKeys])) ?? Data()
    }

    /// Docker keeps an exec's processes running after its client hangs up. This exec sends
    /// SIGHUP to the session's processes (found by their environment), as closing a terminal
    /// would.
    static func hangUpBody(session: String) -> Data {
        let script = "for p in /proc/[0-9]*; do tr '\\0' '\\n' < $p/environ 2>/dev/null"
            + " | grep -qx '\(sessionVariable)=\(session)' && kill -HUP ${p#/proc/}; done"
        let body: [String: Any] = ["AttachStdout": false, "AttachStderr": false, "Tty": false, "Cmd": ["sh", "-c", script]]
        return (try? JSONSerialization.data(withJSONObject: body, options: [.sortedKeys])) ?? Data()
    }

    static func startBody(cols: Int, rows: Int) -> Data {
        let body: [String: Any] = ["Detach": false, "Tty": true, "ConsoleSize": [max(1, rows), max(1, cols)]]
        return (try? JSONSerialization.data(withJSONObject: body, options: [.sortedKeys])) ?? Data()
    }

    /// An HTTP/1.1 request with a JSON body. `upgrade` asks the daemon to hijack the
    /// connection for the exec's stream.
    static func request(_ method: String, _ path: String, body: Data? = nil, upgrade: Bool = false) -> Data {
        var head = "\(method) \(path) HTTP/1.1\r\nHost: docker\r\nUser-Agent: katamaran-app\r\n"
        if let body {
            head += "Content-Type: application/json\r\nContent-Length: \(body.count)\r\n"
        } else {
            head += "Content-Length: 0\r\n"
        }
        head += upgrade ? "Connection: Upgrade\r\nUpgrade: tcp\r\n" : "Connection: close\r\n"
        var data = Data((head + "\r\n").utf8)
        if let body { data.append(body) }
        return data
    }

    static func execID(fromCreate body: Data) throws -> String {
        guard let obj = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let id = obj["Id"] as? String, !id.isEmpty
        else { throw SocketError.protocolError("Docker returned no exec id") }
        return id
    }

    static func exitCode(fromInspect body: Data) -> Int? {
        let obj = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
        return (obj?["ExitCode"] as? NSNumber)?.intValue
    }

    /// Splits the response to `start`: the parsed head and whatever bytes of the stream came
    /// in with it. Nil while the head is incomplete.
    static func splitUpgradeResponse(_ raw: Data) throws -> (head: HTTPResponseHead, rest: Data)? {
        guard let sep = raw.range(of: Data("\r\n\r\n".utf8)) else { return nil }
        let head = try DockerClient.parseResponseHead(raw[raw.startIndex..<sep.lowerBound])
        return (head, Data(raw[sep.upperBound...]))
    }

    /// Splits the response to `start`: the status code, and whatever bytes of the stream came
    /// in with the head. Nil while the head is incomplete.
    static func splitUpgrade(_ raw: Data) throws -> (code: Int, rest: Data)? {
        guard let split = try splitUpgradeResponse(raw) else { return nil }
        return (split.head.code, split.rest)
    }
}

/// One exec session: create, attach, stream, resize. Output arrives on the reader thread;
/// `onEnd` on the main queue, once.
final class ExecAttach: @unchecked Sendable {
    enum End: Equatable {
        /// The shell exited (code when Docker knows it).
        case exited(Int?)
        case failed(String)
    }

    private let socketPath: String
    private let container: String
    private let lock = NSLock()
    private var socket: UnixSocket?
    private var execID: String?
    private var closed = false
    private var size: (cols: Int, rows: Int)
    /// Writes go out in order, off the main thread.
    private let writer = DispatchQueue(label: "katamaran.exec.write")
    private var resizeWork: DispatchWorkItem?
    /// Marks this session's processes in the container.
    let session = UUID().uuidString

    /// Called on the reader thread with each piece of output.
    var onOutput: (Data) -> Void = { _ in }
    /// Called on the main queue once the shell runs.
    var onStart: () -> Void = {}
    var onEnd: (End) -> Void = { _ in }

    init(socketPath: String, container: String, cols: Int, rows: Int) {
        self.socketPath = socketPath
        self.container = container
        size = (cols, rows)
    }

    func start() {
        let t = Thread { [self] in run() }
        t.name = "docker-exec"
        t.start()
    }

    private var isClosed: Bool {
        lock.lock(); defer { lock.unlock() }
        return closed
    }

    /// Bytes for the shell (keys, paste).
    func send(_ data: Data) {
        guard !data.isEmpty else { return }
        writer.async { [self] in
            lock.lock()
            let s = socket
            lock.unlock()
            try? s?.write(data)
        }
    }

    /// The terminal's new size (debounced: a live resize sends one request at the end).
    func resize(cols: Int, rows: Int) {
        lock.lock()
        guard (cols, rows) != size else { lock.unlock(); return }
        size = (cols, rows)
        lock.unlock()
        resizeWork?.cancel()
        let work = DispatchWorkItem { [self] in sendResize() }
        resizeWork = work
        DispatchQueue.global().asyncAfter(deadline: .now() + 0.08, execute: work)
    }

    private func sendResize() {
        lock.lock()
        let (id, s) = (execID, size)
        lock.unlock()
        guard let id else { return }
        let client = DockerClient(socketPath: socketPath)
        _ = try? client.request("POST", "/exec/\(id)/resize?h=\(s.rows)&w=\(s.cols)", timeout: 5)
    }

    /// Hangs up: closes the stream and sends the shell (and what it runs) SIGHUP. `wait`
    /// blocks until the signal went out; `group` runs asynchronously tracked by that group.
    func close(wait: Bool = false, group: DispatchGroup? = nil) {
        lock.lock()
        let wasOpen = !closed && execID != nil
        closed = true
        if let s = socket { Darwin.shutdown(s.fd, SHUT_RDWR) }
        lock.unlock()
        guard wasOpen else { return }
        let (path, container, session) = (socketPath, self.container, self.session)
        let work = { ExecAttach.hangUp(socketPath: path, container: container, session: session) }
        if wait {
            work()
        } else if let group {
            group.enter()
            BlockingIO.docker.async {
                work()
                group.leave()
            }
        } else {
            BlockingIO.docker.async(execute: work)
        }
    }

    private static func hangUp(socketPath: String, container: String, session: String) {
        guard let sock = try? UnixSocket(path: socketPath, timeout: 3) else { return }
        try? sock.write(ExecAPI.request("POST", "/containers/\(container)/exec", body: ExecAPI.hangUpBody(session: session)))
        var raw = Data()
        while let chunk = try? sock.read(), !chunk.isEmpty { raw.append(chunk) }
        guard let (code, body) = try? DockerClient.parse(raw), code == 201 || code == 200,
              let id = try? ExecAPI.execID(fromCreate: body),
              let start = try? UnixSocket(path: socketPath, timeout: 3)
        else { return }
        let detach = (try? JSONSerialization.data(withJSONObject: ["Detach": true, "Tty": false])) ?? Data()
        try? start.write(ExecAPI.request("POST", "/exec/\(id)/start", body: detach))
        _ = try? start.read()
    }

    private func run() {
        var end = End.exited(nil)
        do {
            let (cols, rows) = { lock.lock(); defer { lock.unlock() }; return size }()
            // Create.
            let create = try exchange(ExecAPI.request("POST", "/containers/\(container)/exec",
                                                      body: ExecAPI.createBody(cols: cols, rows: rows, session: session)))
            guard create.0 == 201 || create.0 == 200 else { throw DockerClient.failure(create.0, create.1) }
            let id = try ExecAPI.execID(fromCreate: create.1)
            lock.lock()
            execID = id
            lock.unlock()

            // Start, on a connection that stays open as the stream.
            let sock = try UnixSocket(path: socketPath, timeout: 0)
            lock.lock()
            if closed { lock.unlock(); return }
            socket = sock
            lock.unlock()
            defer { lock.lock(); socket = nil; lock.unlock() }
            try sock.write(ExecAPI.request("POST", "/exec/\(id)/start",
                                           body: ExecAPI.startBody(cols: cols, rows: rows), upgrade: true))
            var raw = Data()
            var upgraded: (head: HTTPResponseHead, rest: Data)?
            while upgraded == nil {
                let chunk = try sock.read()
                if chunk.isEmpty { throw SocketError.closed }
                raw.append(chunk)
                upgraded = try ExecAPI.splitUpgradeResponse(raw)
            }
            guard let upgraded, upgraded.head.code == 101 || upgraded.head.code == 200 else {
                let head = upgraded?.head
                let rest = head.map {
                    DockerClient.readErrorBody(from: sock, head: $0, initial: upgraded?.rest ?? Data())
                } ?? (upgraded?.rest ?? Data())
                throw DockerClient.failure(head?.code ?? 0, rest)
            }
            DispatchQueue.main.async { [self] in if !isClosed { onStart() } }
            // The size may have changed while starting.
            sendResize()
            if !upgraded.rest.isEmpty { onOutput(upgraded.rest) }
            while true {
                let chunk = try sock.read()
                if chunk.isEmpty { break }
                onOutput(chunk)
            }
            end = .exited(exitCode(id))
        } catch {
            end = isClosed ? .exited(nil) : .failed(error.localizedDescription)
        }
        let result = end
        DispatchQueue.main.async { [self] in onEnd(result) }
    }

    /// A request on its own connection, read to the end.
    private func exchange(_ request: Data) throws -> (Int, Data) {
        let sock = try UnixSocket(path: socketPath, timeout: 15)
        try sock.write(request)
        var raw = Data()
        while true {
            let chunk = try sock.read()
            if chunk.isEmpty { break }
            raw.append(chunk)
        }
        return try DockerClient.parse(raw)
    }

    private func exitCode(_ id: String) -> Int? {
        // Docker records the code a moment after the stream ends.
        for _ in 0..<10 {
            if let (code, body) = try? DockerClient(socketPath: socketPath).request("GET", "/exec/\(id)/json", timeout: 5),
               code == 200 {
                let obj = try? JSONSerialization.jsonObject(with: body) as? [String: Any]
                if obj?["Running"] as? Bool != true { return ExecAPI.exitCode(fromInspect: body) }
            }
            Thread.sleep(forTimeInterval: 0.05)
        }
        return nil
    }
}
