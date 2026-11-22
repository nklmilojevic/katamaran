import AppKit
import CKatTerm
import Foundation
import Testing
@testable import KatamaranApp

// MARK: Exec API framing

@Test func execRequestsCarryTheirBody() throws {
    let body = ExecAPI.createBody(cols: 120, rows: 40, session: "s1")
    let raw = String(decoding: ExecAPI.request("POST", "/containers/web/exec", body: body), as: UTF8.self)
    let (head, rest) = try #require(raw.range(of: "\r\n\r\n").map { (raw[..<$0.lowerBound], raw[$0.upperBound...]) })
    #expect(head.hasPrefix("POST /containers/web/exec HTTP/1.1\r\n"))
    #expect(head.contains("Content-Type: application/json"))
    #expect(head.contains("Content-Length: \(body.count)"))
    #expect(head.contains("Connection: close"))
    #expect(!head.contains("Upgrade"))
    #expect(Data(rest.utf8) == body)

    let obj = try #require(try JSONSerialization.jsonObject(with: body) as? [String: Any])
    #expect(obj["Tty"] as? Bool == true)
    #expect(obj["AttachStdin"] as? Bool == true)
    #expect(obj["Cmd"] as? [String] == ["sh", "-c", "command -v bash >/dev/null 2>&1 && exec bash || exec sh"])
    // Docker wants [height, width].
    #expect(obj["ConsoleSize"] as? [Int] == [40, 120])
    let env = obj["Env"] as? [String] ?? []
    #expect(env.contains("TERM=xterm-256color"))
    #expect(env.contains("KATAMARAN_SESSION=s1"))
}

@Test func startAsksForTheConnection() {
    let raw = String(decoding: ExecAPI.request("POST", "/exec/abc/start", body: ExecAPI.startBody(cols: 80, rows: 24),
                                               upgrade: true), as: UTF8.self)
    #expect(raw.contains("Connection: Upgrade\r\nUpgrade: tcp\r\n"))
    #expect(!raw.contains("Connection: close"))
    #expect(raw.hasSuffix(#"{"ConsoleSize":[24,80],"Detach":false,"Tty":true}"#))
}

@Test func upgradeResponseSplitsHeadFromStream() throws {
    #expect(try ExecAPI.splitUpgrade(Data("HTTP/1.1 101 UPGRADED\r\nContent-Type: application/vnd".utf8)) == nil)
    let raw = Data("HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n\u{1B}[?2004hroot@c:/# ".utf8)
    let split = try #require(try ExecAPI.splitUpgrade(raw))
    #expect(split.code == 101)
    #expect(String(decoding: split.rest, as: UTF8.self) == "\u{1B}[?2004hroot@c:/# ")
    let notFound = try #require(try ExecAPI.splitUpgrade(Data("HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\n\r\n{}".utf8)))
    #expect(notFound.code == 404)
}

@Test func execIDsAndExitCodes() throws {
    #expect(try ExecAPI.execID(fromCreate: Data(#"{"Id":"f00d"}"#.utf8)) == "f00d")
    #expect(throws: SocketError.self) { try ExecAPI.execID(fromCreate: Data("{}".utf8)) }
    #expect(ExecAPI.exitCode(fromInspect: Data(#"{"Running":false,"ExitCode":130}"#.utf8)) == 130)
    #expect(ExecAPI.exitCode(fromInspect: Data("nope".utf8)) == nil)
    let hangUp = try #require(try JSONSerialization.jsonObject(with: ExecAPI.hangUpBody(session: "abc")) as? [String: Any])
    let script = (hangUp["Cmd"] as? [String])?.last ?? ""
    #expect(script.contains("grep -qx 'KATAMARAN_SESSION=abc'"))
    #expect(script.contains("tr '\\0' '\\n'"))
    #expect(script.contains("kill -HUP"))
}

// MARK: A session against a fake Docker

/// Answers exec create, start (echoing the stream once upgraded) and inspect on a Unix socket.
private final class FakeDocker: @unchecked Sendable {
    let path: String
    private let fd: Int32
    private let lock = NSLock()
    private(set) var requests: [String] = []

    init() throws {
        path = "/tmp/kat-test-\(getpid())-\(UInt32.random(in: 0...UInt32.max)).sock"
        unlink(path)
        fd = socket(AF_UNIX, SOCK_STREAM, 0)
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &addr.sun_path) { buf in
            for (i, b) in path.utf8.enumerated() { buf[i] = b }
        }
        let rc = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
        }
        guard rc == 0, listen(fd, 8) == 0 else { throw SocketError.io(errno) }
        Thread { [self] in serve() }.start()
    }

    deinit {
        close(fd)
        unlink(path)
    }

    private func serve() {
        while true {
            let c = accept(fd, nil, nil)
            if c < 0 { return }
            Thread { [self] in handle(c) }.start()
        }
    }

    private func readRequest(_ c: Int32) -> String {
        var buf = [UInt8](repeating: 0, count: 65536)
        var raw = Data()
        while true {
            let n = read(c, &buf, buf.count)
            if n <= 0 { break }
            raw.append(contentsOf: buf[0..<n])
            guard let sep = raw.range(of: Data("\r\n\r\n".utf8)) else { continue }
            let head = String(decoding: raw[..<sep.lowerBound], as: UTF8.self)
            let length = head.components(separatedBy: "\r\n")
                .first { $0.lowercased().hasPrefix("content-length:") }
                .flatMap { Int($0.split(separator: ":")[1].trimmingCharacters(in: .whitespaces)) } ?? 0
            if raw.count - sep.upperBound >= length { break }
        }
        return String(decoding: raw, as: UTF8.self)
    }

    private func reply(_ c: Int32, _ s: String) {
        let d = Array(s.utf8)
        _ = d.withUnsafeBytes { write(c, $0.baseAddress, d.count) }
    }

    private func handle(_ c: Int32) {
        defer { close(c) }
        let req = readRequest(c)
        lock.lock(); requests.append(req); lock.unlock()
        let line = req.components(separatedBy: "\r\n").first ?? ""
        if line.hasPrefix("POST /containers/") {
            reply(c, "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 13\r\n\r\n{\"Id\":\"e123\"}")
        } else if line.hasPrefix("POST /exec/e123/start") {
            reply(c, "HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n$ ")
            // Echo until the client sends "exit\r".
            var buf = [UInt8](repeating: 0, count: 1024)
            var typed = ""
            while true {
                let n = read(c, &buf, buf.count)
                if n <= 0 { return }
                _ = buf.withUnsafeBytes { write(c, $0.baseAddress, n) }
                typed += String(decoding: buf[0..<n], as: UTF8.self)
                if typed.hasSuffix("exit\r") { return }
            }
        } else if line.hasPrefix("GET /exec/e123/json") {
            let body = #"{"Running":false,"ExitCode":3}"#
            reply(c, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \(body.count)\r\n\r\n\(body)")
        } else {
            reply(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        }
    }

    var seen: [String] { lock.lock(); defer { lock.unlock() }; return requests }
}

private final class Collector: @unchecked Sendable {
    let lock = NSLock()
    var data = Data()
    func add(_ d: Data) { lock.lock(); data.append(d); lock.unlock() }
    var text: String { lock.lock(); defer { lock.unlock() }; return String(decoding: data, as: UTF8.self) }
}

@MainActor
@Test func sessionStreamsBothWaysAndReportsTheExitCode() async throws {
    let docker = try FakeDocker()
    let attach = ExecAttach(socketPath: docker.path, container: "web", cols: 100, rows: 30)
    let output = Collector()
    attach.onOutput = { output.add($0) }
    let ended = AsyncStream<ExecAttach.End> { cont in
        attach.onEnd = { cont.yield($0); cont.finish() }
        attach.onStart = { attach.send(Data("echo hi\r".utf8)) }
    }
    attach.start()
    for _ in 0..<100 where !output.text.contains("echo hi") { try await Task.sleep(for: .milliseconds(20)) }
    #expect(output.text == "$ echo hi\r")
    attach.send(Data("exit\r".utf8))
    var end: ExecAttach.End?
    for await e in ended { end = e }
    #expect(end == .exited(3))
    let create = try #require(docker.seen.first { $0.hasPrefix("POST /containers/web/exec") })
    #expect(create.contains(#""ConsoleSize":[30,100]"#))
    #expect(docker.seen.contains { $0.hasPrefix("POST /exec/e123/start") && $0.contains("Upgrade: tcp") })
}

@MainActor
@Test func failuresAreReported() async throws {
    let attach = ExecAttach(socketPath: "/tmp/kat-test-no-such.sock", container: "web", cols: 80, rows: 24)
    let ended = AsyncStream<ExecAttach.End> { cont in
        attach.onEnd = { cont.yield($0); cont.finish() }
    }
    attach.start()
    var end: ExecAttach.End?
    for await e in ended { end = e }
    guard case .failed(let message) = end else {
        Issue.record("expected a failure, got \(String(describing: end))")
        return
    }
    #expect(message.contains("kat-test-no-such.sock"))
}

@MainActor
@Test func execAttachDoesNotDeadlockOnKeepAliveStartError() async throws {
    let path = "/tmp/kat-exec-ka-\(getpid())-\(UInt32.random(in: 0...UInt32.max)).sock"
    unlink(path)
    let fd = socket(AF_UNIX, SOCK_STREAM, 0)
    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    withUnsafeMutableBytes(of: &addr.sun_path) { buf in for (i, b) in path.utf8.enumerated() { buf[i] = b } }
    let rc = withUnsafePointer(to: &addr) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
    }
    guard rc == 0, listen(fd, 8) == 0 else { throw SocketError.io(errno) }
    let release = DispatchSemaphore(value: 0)
    defer {
        release.signal()
        close(fd)
        unlink(path)
    }
    Thread {
        for _ in 0..<2 {
            let c = accept(fd, nil, nil)
            if c < 0 { break }
            Thread {
                var buf = [UInt8](repeating: 0, count: 4096)
                let n = read(c, &buf, buf.count)
                let req = n > 0 ? String(decoding: buf[0..<n], as: UTF8.self) : ""
                if req.hasPrefix("POST /containers/") {
                    let resp = "HTTP/1.1 201 Created\r\nContent-Length: 13\r\n\r\n{\"Id\":\"e999\"}"
                    _ = resp.withCString { write(c, $0, strlen($0)) }
                    close(c)
                } else {
                    let body = #"{"message":"container is not running"}"#
                    let resp = "HTTP/1.1 409 Conflict\r\nContent-Length: \(body.utf8.count)\r\n\r\n\(body)"
                    _ = resp.withCString { write(c, $0, strlen($0)) }
                    // Hold connection open (keep-alive) to verify ExecAttach stops at Content-Length.
                    _ = release.wait(timeout: .now() + 5)
                    close(c)
                }
            }.start()
        }
    }.start()

    let t0 = Date()
    let attach = ExecAttach(socketPath: path, container: "web", cols: 80, rows: 24)
    let ended = AsyncStream<ExecAttach.End> { cont in
        attach.onEnd = { cont.yield($0); cont.finish() }
    }
    attach.start()
    var end: ExecAttach.End?
    for await e in ended { end = e }
    #expect(end == .failed("container is not running"))
    #expect(Date().timeIntervalSince(t0) < 1.5)
}

// MARK: The engine through Swift

@Test func gridSnapshotReadsTheEngine() {
    let term = KatTerminal(cols: 12, rows: 3)
    term.resize(cols: 12, rows: 3, cellWidth: 8, cellHeight: 16)
    term.setTheme(.light)
    term.feed(Data("ab\u{1B}[31mc\u{1B}[0m\r\ne\u{301}🚀".utf8))
    let snap = GridSnapshot()
    #expect(snap.update(term.handle))
    #expect(!snap.update(term.handle))
    #expect((snap.cols, snap.rows) == (12, 3))
    #expect(snap.info.background == TerminalTheme.light.background)
    #expect(snap.cell(col: 0, row: 0)?.ch == UInt32(Character("a").unicodeScalars.first!.value))
    #expect(snap.cell(col: 2, row: 0)?.fg == TerminalTheme.light.palette[1])
    #expect(snap.rowClusters[1][0] == "e\u{301}")
    #expect(snap.cell(col: 1, row: 1).map { $0.flags & CellFlag.wide != 0 } == true)
    #expect((snap.info.cursor_col, snap.info.cursor_row) == (3, 1))

    // A resize to a bigger grid reallocates and refills.
    term.resize(cols: 20, rows: 5, cellWidth: 8, cellHeight: 16)
    #expect(snap.update(term.handle))
    #expect(snap.cells.count == 100)
    #expect(snap.cell(col: 1, row: 0)?.ch == UInt32(Character("b").unicodeScalars.first!.value))
}

@Test func keysAndPasteComeBackEncoded() {
    let term = KatTerminal(cols: 10, rows: 2)
    #expect(term.key(kind: KeyKind.named, code: UInt32(KAT_KEY_UP), mods: 0, action: KeyActionCode.press,
                     text: nil, baseLayout: 0) == Data("\u{1B}[A".utf8))
    #expect(term.key(kind: KeyKind.text, code: 0x63, mods: KeyMods.ctrl, action: KeyActionCode.press,
                     text: nil, baseLayout: 0) == Data([3]))
    term.feed(Data("\u{1B}[?2004h".utf8))
    #expect(term.paste("x\ny") == Data("\u{1B}[200~x\ny\u{1B}[201~".utf8))
    term.feed(Data("\u{1B}[5n".utf8))
    #expect(term.takeReplies() == Data("\u{1B}[0n".utf8))
}

@Test func themeFollowsAppearance() {
    #expect(TerminalTheme.forAppearance(NSAppearance(named: .darkAqua)!) == .dark)
    #expect(TerminalTheme.forAppearance(NSAppearance(named: .aqua)!) == .light)
    var raw = TerminalTheme.dark.raw
    let p = withUnsafeBytes(of: &raw.palette) { Array($0.bindMemory(to: UInt32.self)) }
    #expect(p == TerminalTheme.dark.palette)
}
