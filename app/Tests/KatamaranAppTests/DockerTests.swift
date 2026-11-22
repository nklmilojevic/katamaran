import AppKit
import Testing
@testable import KatamaranApp

// MARK: Fixtures

private func container(_ name: String, image: String = "nginx:alpine", state: String = "running",
                       status: String = "Up 5 minutes", project: String? = nil, service: String? = nil,
                       id: String? = nil, ports: [(UInt16, UInt16?)] = [], volumes: [String] = []) -> ContainerSummary {
    var labels: [String: String] = [:]
    if let project { labels[ComposeLabel.project] = project }
    if let service { labels[ComposeLabel.service] = service }
    return ContainerSummary(
        Id: id ?? String(repeating: name.first.map(String.init) ?? "a", count: 64), Names: ["/\(name)"], Image: image,
        ImageID: "sha256:\(image)", Command: nil, Created: 1_700_000_000, State: state, Status: status,
        Ports: ports.map { ContainerPort(IP: "0.0.0.0", PrivatePort: $0.0, PublicPort: $0.1, type: "tcp") },
        Labels: labels, NetworkSettings: nil,
        Mounts: volumes.map { ContainerMount(type: "volume", Name: $0, Source: nil, Destination: "/data", RW: true) })
}

/// What `GET /containers/json` returned for a Compose service, trimmed.
private let containersJSON = """
[{"Id":"0569da2af910ce4c2f251015da1719cf03f227bdccfa099c55a5ef3828fa4fe3","Names":["/shop-web-1"],
  "Image":"nginx:alpine","ImageID":"sha256:0563c36f","Command":"/docker-entrypoint.sh nginx -g 'daemon off;'",
  "Created":1795040977,
  "Ports":[{"IP":"0.0.0.0","PrivatePort":80,"PublicPort":18080,"Type":"tcp"},
           {"IP":"::","PrivatePort":80,"PublicPort":18080,"Type":"tcp"},
           {"PrivatePort":443,"Type":"tcp"}],
  "Labels":{"com.docker.compose.project":"shop","com.docker.compose.service":"web"},
  "State":"running","Status":"Up 11 seconds","HostConfig":{"NetworkMode":"shop_default"},
  "Health":{"Status":"none","FailingStreak":0},
  "NetworkSettings":{"Networks":{"shop_default":{"IPAMConfig":null,"NetworkID":"36d2a3","IPAddress":"172.18.0.4"}}},
  "Mounts":[{"Type":"volume","Name":"pgdata","Source":"/var/lib/docker/volumes/pgdata/_data",
             "Destination":"/data","Driver":"local","Mode":"z","RW":true,"Propagation":""}],
  "SomethingNew":{"x":1}}]
"""

// MARK: HTTP and streams

@Test func chunkedBodiesDecodeAcrossArbitrarySplits() throws {
    let body = "4\r\nWiki\r\n6;ext=1\r\npedia \r\nE\r\nin \r\n\r\nchunks.\r\n0\r\n\r\n"
    let bytes = Array(body.utf8)
    for split in 1..<bytes.count {
        var d = ChunkedDecoder()
        var out = try d.feed(Data(bytes[..<split]))
        out += try d.feed(Data(bytes[split...]))
        #expect(String(decoding: out, as: UTF8.self) == "Wikipedia in \r\n\r\nchunks.", "split at \(split)")
        #expect(d.finished)
    }
    // One byte at a time.
    var d = ChunkedDecoder()
    var out = Data()
    for b in bytes { out += try d.feed(Data([b])) }
    #expect(String(decoding: out, as: UTF8.self) == "Wikipedia in \r\n\r\nchunks.")
}

@Test func badChunkSizesThrow() {
    for bad in ["zz\r\nhello\r\n", "-1\r\n", "+4\r\nWiki\r\n", ";abc\r\n", "\r\n", "4\r\nWikiXX0\r\n\r\n"] {
        var d = ChunkedDecoder()
        #expect(throws: SocketError.self, "input: \(bad.debugDescription)") {
            _ = try d.feed(Data(bad.utf8))
        }
    }
    let truncated = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n"
    #expect(throws: SocketError.self) {
        _ = try DockerClient.parse(Data(truncated.utf8))
    }
}

@Test func responsesParseWithAndWithoutChunking() throws {
    let plain = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n[]"
    let (code, body) = try DockerClient.parse(Data(plain.utf8))
    #expect(code == 200)
    #expect(body == Data("[]".utf8))
    let chunked = "HTTP/1.1 404 Not Found\r\nTransfer-Encoding: Chunked\r\n\r\n12\r\n{\"message\":\"gone\"}\r\n0\r\n\r\n"
    let (c2, b2) = try DockerClient.parse(Data(chunked.utf8))
    #expect(c2 == 404)
    #expect(DockerClient.failure(c2, b2).localizedDescription == "gone")
    #expect(DockerClient.failure(500, Data()).localizedDescription == "Docker answered HTTP 500")
    #expect(throws: SocketError.self) { try DockerClient.parse(Data("HTTP/1.1 200 OK\r\n".utf8)) }
}

@Test func linesSplitAcrossReads() {
    var s = LineSplitter()
    #expect(s.feed(Data("{\"a\":1}\n{\"b\"".utf8)) == [Data("{\"a\":1}".utf8)])
    #expect(s.feed(Data(":2}\r\n\n".utf8)) == [Data("{\"b\":2}".utf8), Data()])
    #expect(s.feed(Data("tail".utf8)).isEmpty)
    #expect(s.flush() == Data("tail".utf8))
    #expect(s.flush() == nil)

    // Many lines in one chunk should stay linear and preserve order & CRLF stripping.
    var big = LineSplitter()
    let count = 5_000
    let payload = (0..<count).map { "line-\($0)\r\n" }.joined() + "partial"
    let out = big.feed(Data(payload.utf8))
    #expect(out.count == count)
    #expect(out.first == Data("line-0".utf8))
    #expect(out.last == Data("line-\(count - 1)".utf8))
    #expect(big.flush() == Data("partial".utf8))
}

@Test func logFramesSplitIntoStdoutAndStderr() throws {
    func frame(_ stream: UInt8, _ text: String) -> [UInt8] {
        let p = Array(text.utf8)
        return [stream, 0, 0, 0, UInt8(p.count >> 24 & 0xFF), UInt8(p.count >> 16 & 0xFF), UInt8(p.count >> 8 & 0xFF),
                UInt8(p.count & 0xFF)] + p
    }
    let bytes = frame(1, "out line\n") + frame(2, "err line\n") + frame(1, String(repeating: "x", count: 300))
    var d = LogDemuxer(multiplexed: true)
    // A frame header split across reads.
    var got = d.feed(Data(bytes[..<5]))
    #expect(got.isEmpty)
    got = d.feed(Data(bytes[5..<36]))
    try #require(got.count == 2)
    #expect(got[0].0 == .stdout && got[0].1 == Data("out line\n".utf8))
    #expect(got[1].0 == .stderr && got[1].1 == Data("err line\n".utf8))
    got = d.feed(Data(bytes[36...]))
    #expect(got.count == 1 && got[0].1.count == 300)

    var tty = LogDemuxer(multiplexed: false)
    #expect(tty.feed(Data("raw\n".utf8)).map(\.1) == [Data("raw\n".utf8)])
}

/// A one-shot HTTP server on a Unix socket: answers the first request with `response`.
private func serve(_ response: [String], delay: TimeInterval = 0.05) throws -> (String, Thread) {
    let path = FileManager.default.temporaryDirectory.appendingPathComponent("kat-\(UUID().uuidString.prefix(8)).sock").path
    let fd = socket(AF_UNIX, SOCK_STREAM, 0)
    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    withUnsafeMutableBytes(of: &addr.sun_path) { buf in for (i, b) in path.utf8.enumerated() { buf[i] = b } }
    _ = withUnsafePointer(to: &addr) { $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
        bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
    } }
    listen(fd, 1)
    let t = Thread {
        let c = accept(fd, nil, nil)
        var buf = [UInt8](repeating: 0, count: 4096)
        _ = read(c, &buf, buf.count)
        for part in response {
            _ = part.withCString { write(c, $0, strlen($0)) }
            Thread.sleep(forTimeInterval: delay)
        }
        close(c)
        close(fd)
        unlink(path)
    }
    t.start()
    return (path, t)
}

@Test func requestsReadWholeResponses() throws {
    let json = containersJSON.replacingOccurrences(of: "\n", with: "")
    let (path, _) = try serve(["HTTP/1.1 200 OK\r\nContent-Length: \(json.utf8.count)\r\n\r\n", json])
    let list = try DockerClient(socketPath: path).containers()
    #expect(list.map(\.name) == ["shop-web-1"])
}

@Test @MainActor func streamsDeliverChunksAsTheyArrive() async throws {
    func chunk(_ s: String) -> String { String(s.utf8.count, radix: 16) + "\r\n" + s + "\r\n" }
    let (path, _) = try serve([
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
        chunk(#"{"Type":"container","Action":"#),
        chunk("\"start\"}\n{\"Typ"),
        chunk("e\":\"image\",\"Action\":\"pull\"}\n"),
        "0\r\n\r\n",
    ])
    let stream = DockerStream(socketPath: path, path: "/events")
    var lines = LineSplitter()
    var events: [DockerEvent] = []
    let ended: Bool = await withCheckedContinuation { cont in
        stream.onData = { d in
            events += lines.feed(d).compactMap { try? JSONDecoder().decode(DockerEvent.self, from: $0) }
        }
        stream.onEnd = { err in cont.resume(returning: err == nil) }
        stream.start()
    }
    #expect(ended)
    #expect(events.map(\.Action) == ["start", "pull"])
    #expect(events.map(\.Type) == ["container", "image"])
}

@Test @MainActor func streamsReportDockersError() async throws {
    let (path, _) = try serve(["HTTP/1.1 404 Not Found\r\nContent-Length: 30\r\n\r\n", "{\"message\":\"No such container\"}"])
    let stream = DockerStream(socketPath: path, path: "/containers/x/logs?follow=1")
    let message: String? = await withCheckedContinuation { cont in
        stream.onEnd = { err in cont.resume(returning: err?.localizedDescription) }
        stream.start()
    }
    #expect(message == "No such container")
}

// MARK: Models

@Test func containersDecodeAndIgnoreUnknownFields() throws {
    let list = try JSONDecoder().decode([ContainerSummary].self, from: Data(containersJSON.utf8))
    let c = try #require(list.first)
    #expect(c.name == "shop-web-1")
    #expect(c.shortID == "0569da2af910")
    #expect(c.project == "shop")
    #expect(c.service == "web")
    // IPv4 and IPv6 bindings are one port; unpublished ports aren't listed.
    #expect(c.publishedPorts == [18080])
    #expect(c.networkNames == ["shop_default"])
    #expect(c.volumeNames == ["pgdata"])
    #expect(c.health == .running)
}

@Test func healthFollowsStateAndExitCode() {
    #expect(container("a", state: "exited", status: "Exited (0) 2 minutes ago").health == .stopped)
    #expect(container("a", state: "exited", status: "Exited (1) 2 minutes ago").health == .failed)
    // Stopped by `docker stop`: SIGKILL or SIGTERM, not a failure.
    #expect(container("a", state: "exited", status: "Exited (137) 1 hour ago").health == .stopped)
    #expect(container("a", state: "running", status: "Up 3 minutes (unhealthy)").health == .failed)
    #expect(container("a", state: "restarting", status: "Restarting (1) 4 seconds ago").health == .transitional)
    #expect(container("a", state: "created", status: "Created").health == .transitional)
    #expect(ContainerSummary.exitCode("Exited (255) 1 second ago") == 255)
    #expect(ContainerSummary.exitCode("Up 1 second") == nil)
}

@Test func inspectGivesEnvironmentPortsAndCommand() throws {
    let json = """
    {"Id":"abc","Name":"/web","Created":"2026-11-18T22:29:37.123456789Z","Path":"/docker-entrypoint.sh",
     "Args":["nginx","-g","daemon off;"],
     "State":{"Status":"running","Running":true,"ExitCode":0,"StartedAt":"2026-11-18T22:29:37.5Z","FinishedAt":"0001-01-01T00:00:00Z"},
     "Config":{"Env":["PATH=/usr/bin:/bin","EMPTY=","EQ=a=b","NOVALUE"],"Tty":false,"Labels":{}},
     "NetworkSettings":{"Ports":{"80/tcp":[{"HostIp":"0.0.0.0","HostPort":"18080"},{"HostIp":"::","HostPort":"18080"}],
                                 "443/tcp":null,"53/udp":[{"HostIp":"0.0.0.0","HostPort":"5353"}]}}}
    """
    let i = try JSONDecoder().decode(ContainerInspect.self, from: Data(json.utf8))
    #expect(i.name == "web")
    #expect(!i.tty)
    #expect(i.environment.map(\.0) == ["PATH", "EMPTY", "EQ", "NOVALUE"])
    #expect(i.environment[2].1 == "a=b")
    #expect(i.portBindings == [PortBinding(host: 5353, container: 53, proto: "udp"),
                               PortBinding(host: 18080, container: 80, proto: "tcp")])
    #expect(i.commandLine == "/docker-entrypoint.sh nginx -g 'daemon off;'")
    #expect(parseDockerDate(i.Created!) != nil)
    #expect(parseDockerDate(i.State.FinishedAt!) == nil)
}

@Test func dockerDatesParseWithAnyFraction() throws {
    let a = try #require(parseDockerDate("2026-11-18T22:29:37Z"))
    let b = try #require(parseDockerDate("2026-11-18T22:29:37.999999999Z"))
    let c = try #require(parseDockerDate("2026-11-18T23:29:37.5+01:00"))
    #expect(b.timeIntervalSince(a) > 0.99 && b.timeIntervalSince(a) < 1)
    #expect(abs(c.timeIntervalSince(a) - 0.5) < 0.001)
}

@Test func statsGiveCPUPercentMemoryAndCounters() throws {
    let json = """
    {"read":"2026-11-18T22:30:00.000Z","preread":"2026-11-18T22:29:59.000Z",
     "cpu_stats":{"cpu_usage":{"total_usage":3000000000},"system_cpu_usage":20000000000,"online_cpus":4},
     "precpu_stats":{"cpu_usage":{"total_usage":2000000000},"system_cpu_usage":16000000000,"online_cpus":4},
     "memory_stats":{"usage":104857600,"limit":4294967296,"stats":{"inactive_file":4857600}},
     "networks":{"eth0":{"rx_bytes":1000,"tx_bytes":200},"eth1":{"rx_bytes":24,"tx_bytes":0}},
     "blkio_stats":{"io_service_bytes_recursive":[{"op":"read","value":4096},{"op":"write","value":8192},{"op":"Read","value":4096}]},
     "pids_stats":{"current":7}}
    """
    let (cur, pre) = try StatsSample.parse(Data(json.utf8))
    let prev = try #require(pre)
    // 1 s of CPU over 4 s of system time on 4 CPUs: one CPU busy.
    #expect(abs(cur.cpuPercent(since: prev) - 100) < 0.001)
    #expect(cur.memoryUsed == 100_000_000)
    #expect(cur.memoryLimit == 4_294_967_296)
    #expect(cur.netRx == 1024 && cur.netTx == 200)
    #expect(cur.blockRead == 8192 && cur.blockWrite == 8192)
    #expect(cur.pids == 7)

    var later = cur
    later.read = cur.read.addingTimeInterval(2)
    later.netRx += 2048
    later.blockWrite += 1000
    let r = later.rates(since: cur)
    #expect(r.rx == 1024 && r.tx == 0 && r.write == 500)
    // Counters that went backwards (a restart) are not negative rates.
    later.netTx = 0
    #expect(later.rates(since: cur).tx == 0)
    #expect(prev.cpuPercent(since: cur) == 0)
}

@Test func oneShotStatsHaveNoPreviousReading() throws {
    let json = #"{"cpu_stats":{"cpu_usage":{"total_usage":5}},"precpu_stats":{"cpu_usage":{"total_usage":0}},"memory_stats":{}}"#
    let (cur, pre) = try StatsSample.parse(Data(json.utf8))
    #expect(pre == nil)
    #expect(cur.cpuTotal == 5 && cur.memoryUsed == 0 && cur.onlineCPUs == 1)
}

@Test func eventsSkipExecAndAttachChatter() throws {
    func event(_ action: String) throws -> DockerEvent {
        try JSONDecoder().decode(DockerEvent.self, from: Data(#"{"Type":"container","Action":"\#(action)","Actor":{"ID":"x"}}"#.utf8))
    }
    #expect(try event("start").isRelevant)
    #expect(try event("health_status: healthy").isRelevant)
    #expect(try !event("exec_start: sh").isRelevant)
    #expect(try !event("attach").isRelevant)
}

@Test func volumesImagesAndNetworksDecode() throws {
    let df = #"{"Volumes":[{"Name":"pg","Driver":"local","UsageData":{"Size":47842488,"RefCount":1},"Labels":null},{"Name":"x","UsageData":{"Size":-1,"RefCount":-1}}]}"#
    let v = try JSONDecoder().decode(SystemDF.self, from: Data(df.utf8)).Volumes ?? []
    #expect(v[0].size == 47_842_488 && v[0].refCount == 1)
    #expect(v[1].size == nil && v[1].refCount == nil)
    let images = #"[{"Id":"sha256:0563c36f94a6d3ec","RepoTags":["<none>:<none>"],"RepoDigests":["nginx@sha256:1"],"Created":1,"Size":5,"Containers":-1},{"Id":"sha256:abcdef0123456789","RepoTags":null}]"#
    let i = try JSONDecoder().decode([ImageInfo].self, from: Data(images.utf8))
    #expect(i[0].name == "nginx (untagged)")
    #expect(i[1].name == "abcdef012345")
    let nets = #"[{"Name":"bridge","Id":"1","Driver":"bridge","IPAM":{"Config":[{"Subnet":"172.17.0.0/16","Gateway":"172.17.0.1"}]}},{"Name":"shop_default","Id":"2","IPAM":{"Config":null}}]"#
    let n = try JSONDecoder().decode([NetworkInfo].self, from: Data(nets.utf8))
    #expect(n[0].builtIn && n[0].subnets == ["172.17.0.0/16"])
    #expect(!n[1].builtIn && n[1].subnets.isEmpty)
}

// MARK: Grouping

@Test func composeProjectsGroupAndSortByService() {
    let all = [
        container("shop-web-1", project: "shop", service: "web"),
        container("cache", image: "redis"),
        container("shop-db-1", image: "postgres", state: "exited", status: "Exited (0)", project: "shop", service: "db"),
        container("Alpha", state: "exited", status: "Exited (0)"),
        container("migrate", state: "exited", status: "Exited (0)"),
        container("old-api-1", state: "exited", status: "Exited (0)", project: "old", service: "api"),
    ]
    let sections = ContainerList.sections(all)
    #expect(sections.map(\.kind) == [.running, .stopped])
    // A project with one running container is running, and keeps its stopped ones.
    #expect(sections[0].items.map(\.key) == ["container:" + all[1].Id, "project:shop"])
    guard case .project(let shop) = sections[0].items[1] else { Issue.record("no project"); return }
    #expect(shop.containers.map(\.service) == ["db", "web"])
    #expect(shop.runningCount == 1 && shop.anyRunning)
    // Case-insensitive names.
    #expect(sections[1].items.map(\.sortName) == ["Alpha", "migrate", "old"])
}

@Test func searchMatchesNameImageProjectAndID() {
    let all = [
        container("shop-web-1", project: "shop", service: "web"),
        container("shop-db-1", image: "postgres:17", project: "shop", service: "db"),
        container("cache", image: "redis:7", id: "deadbeef" + String(repeating: "0", count: 56)),
    ]
    func keys(_ q: String) -> [String] { ContainerList.sections(all, query: q).flatMap(\.items).map(\.key) }
    func names(_ q: String) -> [String] {
        ContainerList.sections(all, query: q).flatMap(\.items).flatMap { item -> [String] in
            switch item {
            case .project(let p): p.containers.map(\.name)
            case .container(let c): [c.name]
            }
        }
    }
    // A matching container keeps its project, without the others in it.
    #expect(names("POSTGRES") == ["shop-db-1"])
    #expect(keys("postgres") == ["project:shop"])
    // A matching project keeps all its containers.
    #expect(names("shop") == ["shop-db-1", "shop-web-1"])
    #expect(names("deadbeef") == ["cache"])
    #expect(names("  redis ") == ["cache"])
    #expect(keys("nothing").isEmpty)
    #expect(names("") .count == 3)
}

@Test func emptyListHasNoSections() {
    #expect(ContainerList.sections([]).isEmpty)
    #expect(ContainerList.summary([]) == "No containers")
}

@Test func summariesCountRunningContainers() {
    let up = container("a"), down = container("b", state: "exited", status: "Exited (0)")
    #expect(ContainerList.summary([up, up]) == "2 running")
    #expect(ContainerList.summary([down]) == "None running")
    #expect(ContainerList.summary([up, down]) == "1 of 2 running")
}

@Test func manyContainersGroupQuickly() {
    // The list regroups on every Docker event; keep it cheap at a few hundred.
    let all = (0..<500).map { i in
        container("svc-\(i)", state: i % 3 == 0 ? "exited" : "running", status: "x",
                  project: i % 2 == 0 ? "p\(i % 10)" : nil, service: "s\(i)", id: String(format: "%064d", i))
    }
    let start = Date()
    let sections = ContainerList.sections(all, query: "svc")
    #expect(Date().timeIntervalSince(start) < 0.5)
    let count = sections.flatMap(\.items).reduce(0) { n, item in
        if case .project(let p) = item { return n + p.containers.count }
        return n + 1
    }
    #expect(count == 500)
}

@Test func usageSectionsSplitAndFilter() {
    let s = UsageSections(["b", "A", "c", "d"], query: "", name: { $0 }, search: { [$0] }, inUse: { $0 != "c" })
    #expect(s.inUse == ["A", "b", "d"])
    #expect(s.unused == ["c"])
    let f = UsageSections(["alpha", "beta"], query: "ALP", name: { $0 }, search: { [$0] }, inUse: { _ in false })
    #expect(f.inUse.isEmpty && f.unused == ["alpha"])
}

// MARK: Small helpers

@Test func imageIconsGoByRepository() {
    #expect(ImageIcon.baseName("docker.io/library/postgres:17-alpine") == "postgres")
    #expect(ImageIcon.baseName("ghcr.io/org/app@sha256:abc") == "app")
    #expect(ImageIcon.baseName("localhost:5000/team/redis:7") == "redis")
    #expect(ImageIcon.symbol(for: "postgres:17") == "cylinder.split.1x2.fill")
    #expect(ImageIcon.symbol(for: "traefik/whoami") == "globe")
    #expect(ImageIcon.symbol(for: "something-custom") == "shippingbox.fill")
    #expect(ImageIcon.tint(for: "nginx:alpine") == ImageIcon.tint(for: "nginx:1.27"))
    for s in ["cylinder.split.1x2.fill", "globe", "arrow.left.arrow.right", "chart.xyaxis.line", "externaldrive.fill",
              "chevron.left.forwardslash.chevron.right", "envelope.fill", "terminal.fill", "shippingbox.fill",
              "square.stack.3d.up.fill", "point.3.connected.trianglepath.dotted", "apple.terminal", "sailboat",
              "ellipsis.circle", "memorychip", "internaldrive", "laptopcomputer"] {
        #expect(NSImage(systemSymbolName: s, accessibilityDescription: nil) != nil, "\(s)")
    }
    for s in MainSection.allCases {
        #expect(NSImage(systemSymbolName: s.symbol, accessibilityDescription: nil) != nil, "\(s)")
    }
}

@Test func shellQuotingAndTheExecScript() {
    #expect(shellQuote("plain-name_1.2") == "plain-name_1.2")
    #expect(shellQuote("it's") == #"'it'\''s'"#)
    #expect(shellQuote("") == "''")
    let script = ExecCommand.script(container: "my app", socket: "/Users/me/.katamaran/run/docker.sock", docker: "/opt/homebrew/bin/docker")
    #expect(script.hasPrefix("#!/bin/sh\n"))
    #expect(script.contains("export DOCKER_HOST=unix:///Users/me/.katamaran/run/docker.sock\n"))
    #expect(script.contains("exec /opt/homebrew/bin/docker exec -it 'my app' sh -c "))
    #expect(ExecCommand.display("web") == "docker exec -it web sh -c 'command -v bash >/dev/null 2>&1 && exec bash || exec sh'")
}

@Test func logLinesLoseColorCodes() {
    #expect(stripANSI("\u{1B}[32mINFO\u{1B}[0m ready") == "INFO ready")
    #expect(stripANSI("plain") == "plain")
}

@Test func bytesNeverSayZero() {
    #expect(bytes(0).contains("0"))
    #expect(rate(0).hasSuffix("/s"))
    #expect(rate(.nan).hasSuffix("/s"))
    #expect(rate(.infinity).hasSuffix("/s"))
    #expect(rate(-.infinity).hasSuffix("/s"))
    #expect(rate(-50).hasSuffix("/s"))
    #expect(rate(Double(Int64.max)).hasSuffix("/s"))
    #expect(rate(1e30).hasSuffix("/s"))
}

@Test func socketTimevalPopulatesMicrosecondsAndClampsSmallPositives() {
    let zero = UnixSocket.makeTimeval(0)
    #expect(zero.tv_sec == 0 && zero.tv_usec == 0)
    let neg = UnixSocket.makeTimeval(-1)
    #expect(neg.tv_sec == 0 && neg.tv_usec == 0)
    let tiny = UnixSocket.makeTimeval(1e-9)
    #expect(tiny.tv_sec == 0 && tiny.tv_usec == 1)
    let quarter = UnixSocket.makeTimeval(0.25)
    #expect(quarter.tv_sec == 0 && quarter.tv_usec == 250_000)
    let oneHalf = UnixSocket.makeTimeval(1.5)
    #expect(oneHalf.tv_sec == 1 && oneHalf.tv_usec == 500_000)
    let carry = UnixSocket.makeTimeval(0.9999999)
    #expect(carry.tv_sec == 1 && carry.tv_usec == 0)
}

@Test func activitySortIsStrictWeakOrdering() {
    let rows: [ContainerUsage] = [
        .init(id: "c", name: "Beta", image: "a", cpu: 0.0, memory: 100, netRate: .nan, diskRate: 0.0, pids: 2),
        .init(id: "a", name: "beta", image: "a", cpu: 0.0, memory: 100, netRate: 0.0, diskRate: .infinity, pids: 2),
        .init(id: "b", name: "alpha", image: "a", cpu: .nan, memory: 200, netRate: .nan, diskRate: -.infinity, pids: 1),
        .init(id: "d", name: "gamma", image: "a", cpu: 12.5, memory: 0, netRate: -0.0, diskRate: 0.0, pids: 5),
        .init(id: "e", name: "delta", image: "a", cpu: .nan, memory: 100, netRate: 5.0, diskRate: .nan, pids: 2),
    ]
    for key in ["name", "cpu", "memory", "net", "disk", "pids"] {
        for asc in [true, false] {
            let cmp: (ContainerUsage, ContainerUsage) -> Bool = {
                ContainerUsage.comesBefore($0, $1, key: key, ascending: asc)
            }
            for x in rows {
                #expect(!cmp(x, x), "irreflexivity failed for \(key) asc=\(asc)")
                for y in rows {
                    if cmp(x, y) {
                        #expect(!cmp(y, x), "antisymmetry failed for \(key) asc=\(asc)")
                    }
                    for z in rows where cmp(x, y) && cmp(y, z) {
                        #expect(cmp(x, z), "transitivity failed for \(key) asc=\(asc)")
                    }
                }
            }
            let sorted = rows.sorted(by: cmp)
            #expect(sorted.count == rows.count)
            // Equal CPU (0.0) tie-breaks deterministically by id ("a" before "c"), and NaNs sort after finite values.
            if key == "cpu" {
                let ids = sorted.map(\.id)
                if asc {
                    #expect(ids == ["a", "c", "d", "b", "e"])
                } else {
                    #expect(ids == ["d", "a", "c", "b", "e"])
                }
            }
        }
    }
}

@Test @MainActor func streamsDoNotDeadlockOnKeepAliveErrors() async throws {
    func serveKeepAlive(_ response: String) throws -> (String, DispatchSemaphore) {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent("kat-ka-\(UUID().uuidString.prefix(8)).sock").path
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &addr.sun_path) { buf in for (i, b) in path.utf8.enumerated() { buf[i] = b } }
        _ = withUnsafePointer(to: &addr) { $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
        } }
        listen(fd, 1)
        let release = DispatchSemaphore(value: 0)
        Thread {
            let c = accept(fd, nil, nil)
            var buf = [UInt8](repeating: 0, count: 4096)
            _ = read(c, &buf, buf.count)
            _ = response.withCString { write(c, $0, strlen($0)) }
            // Keep the socket open without closing (HTTP/1.1 keep-alive)!
            _ = release.wait(timeout: .now() + 5)
            close(c)
            close(fd)
            unlink(path)
        }.start()
        return (path, release)
    }

    // Content-Length error on keep-alive connection.
    let body = #"{"message":"keep-alive error"}"#
    let (p1, rel1) = try serveKeepAlive("HTTP/1.1 404 Not Found\r\nContent-Length: \(body.utf8.count)\r\n\r\n\(body)")
    defer { rel1.signal() }
    let t0 = Date()
    let stream1 = DockerStream(socketPath: p1, path: "/events")
    let msg1: String? = await withCheckedContinuation { cont in
        stream1.onEnd = { err in cont.resume(returning: err?.localizedDescription) }
        stream1.start()
    }
    #expect(msg1 == "keep-alive error")
    #expect(Date().timeIntervalSince(t0) < 1.5)

    // Chunked error on keep-alive connection.
    let chunkedBody = #"{"message":"chunked keep-alive"}"#
    let hexLen = String(chunkedBody.utf8.count, radix: 16)
    let (p2, rel2) = try serveKeepAlive("HTTP/1.1 500 Internal Server Error\r\nTransfer-Encoding: chunked\r\n\r\n\(hexLen)\r\n\(chunkedBody)\r\n0\r\n\r\n")
    defer { rel2.signal() }
    let t1 = Date()
    let stream2 = DockerStream(socketPath: p2, path: "/events")
    let msg2: String? = await withCheckedContinuation { cont in
        stream2.onEnd = { err in cont.resume(returning: err?.localizedDescription) }
        stream2.start()
    }
    #expect(msg2 == "chunked keep-alive")
    #expect(Date().timeIntervalSince(t1) < 1.5)
}

@Test func removeImagePercentEncodesWithAndWithoutForce() throws {
    func captureRequest(force: Bool, id: String) throws -> String {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent("kat-rm-\(UUID().uuidString.prefix(8)).sock").path
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &addr.sun_path) { buf in for (i, b) in path.utf8.enumerated() { buf[i] = b } }
        _ = withUnsafePointer(to: &addr) { $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
        } }
        listen(fd, 1)
        let lock = NSLock()
        var reqLine = ""
        let done = DispatchSemaphore(value: 0)
        Thread {
            let c = accept(fd, nil, nil)
            var buf = [UInt8](repeating: 0, count: 4096)
            let n = read(c, &buf, buf.count)
            if n > 0 {
                let s = String(decoding: buf[0..<n], as: UTF8.self)
                lock.lock()
                reqLine = s.components(separatedBy: "\r\n").first ?? ""
                lock.unlock()
            }
            let reply = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]"
            _ = reply.withCString { write(c, $0, strlen($0)) }
            close(c)
            close(fd)
            unlink(path)
            done.signal()
        }.start()
        try DockerClient(socketPath: path).removeImage(id, force: force)
        _ = done.wait(timeout: .now() + 2)
        lock.lock(); defer { lock.unlock() }
        return reqLine
    }

    let forced = try captureRequest(force: true, id: "org/img:1?x")
    #expect(forced == "DELETE /images/org%2Fimg:1%3Fx?force=1 HTTP/1.1")
    let unforced = try captureRequest(force: false, id: "org/img:1?x")
    #expect(unforced == "DELETE /images/org%2Fimg:1%3Fx HTTP/1.1")
}

@Test @MainActor func dockerStoreRejectsStaleRefreshAcrossSamePathReconnect() async throws {
    let path = FileManager.default.temporaryDirectory
        .appendingPathComponent("kat-reconn-\(UUID().uuidString.prefix(8)).sock").path
    unlink(path)
    let fd = socket(AF_UNIX, SOCK_STREAM, 0)
    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    withUnsafeMutableBytes(of: &addr.sun_path) { buf in for (i, b) in path.utf8.enumerated() { buf[i] = b } }
    let rc = withUnsafePointer(to: &addr) {
        $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
    }
    guard rc == 0, listen(fd, 8) == 0 else { throw SocketError.io(errno) }

    final class State: @unchecked Sendable {
        private let lock = NSLock()
        private var count = 0
        private var arrived = false
        func nextCall() -> Int {
            lock.lock(); defer { lock.unlock() }
            count += 1
            return count
        }
        func markArrived() {
            lock.lock(); arrived = true; lock.unlock()
        }
        var didArrive: Bool {
            lock.lock(); defer { lock.unlock() }
            return arrived
        }
    }
    let state = State()
    let releaseFirstList = DispatchSemaphore(value: 0)
    let releaseSecondList = DispatchSemaphore(value: 0)

    defer {
        releaseFirstList.signal()
        releaseSecondList.signal()
        close(fd)
        unlink(path)
    }

    Thread {
        while true {
            let c = accept(fd, nil, nil)
            if c < 0 { break }
            Thread {
                defer { close(c) }
                var buf = [UInt8](repeating: 0, count: 4096)
                let n = read(c, &buf, buf.count)
                let req = n > 0 ? String(decoding: buf[0..<n], as: UTF8.self) : ""
                if req.hasPrefix("GET /containers/json") {
                    let call = state.nextCall()
                    if call == 1 {
                        state.markArrived()
                        _ = releaseFirstList.wait(timeout: .now() + 5)
                        let body = #"[{"Id":"old-id","Names":["/old"],"Image":"alpine","State":"running","Status":"Up"}]"#
                        let resp = "HTTP/1.1 200 OK\r\nContent-Length: \(body.utf8.count)\r\n\r\n\(body)"
                        _ = resp.withCString { write(c, $0, strlen($0)) }
                    } else {
                        _ = releaseSecondList.wait(timeout: .now() + 5)
                        let body = #"[{"Id":"new-id","Names":["/new"],"Image":"alpine","State":"running","Status":"Up"}]"#
                        let resp = "HTTP/1.1 200 OK\r\nContent-Length: \(body.utf8.count)\r\n\r\n\(body)"
                        _ = resp.withCString { write(c, $0, strlen($0)) }
                    }
                } else {
                    // /events stream: return 200 chunked EOF immediately.
                    let resp = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
                    _ = resp.withCString { write(c, $0, strlen($0)) }
                }
            }.start()
        }
    }.start()

    func readyStatus() throws -> DaemonStatus {
        let json = """
        {"state":"running","since_ms":0,"daemon":{"pid":1,"home":"/tmp/k"},
         "versions":{"katamaran":"0.1.0","protocol":1},
         "config":{"cpus":4,"memory_mib":4096,"mount":false,"launch_at_login":false},
         "docker":{"state":"ready","socket":"\(path)"},
         "mount":{"enabled":false,"mounted":false},"ports":[]}
        """
        return try JSONDecoder().decode(DaemonStatus.self, from: Data(json.utf8))
    }

    let store = DockerStore()
    store.follow(try readyStatus())
    let epoch1 = store.connectionEpoch
    for _ in 0..<100 where !state.didArrive {
        try await Task.sleep(for: .milliseconds(20))
    }
    #expect(state.didArrive)

    // Disconnect and immediately reconnect to the exact same socket path while fetch #1 is in flight.
    store.follow(nil)
    #expect(store.loading.isEmpty)
    store.follow(try readyStatus())
    #expect(store.connectionEpoch > epoch1)
    #expect(store.loading.contains(.containers))

    // Let fetch #1 finish first: it must be rejected so stale "old" data and loading clear do not apply.
    releaseFirstList.signal()
    try await Task.sleep(for: .milliseconds(150))
    #expect(store.containers.isEmpty)
    #expect(store.loading.contains(.containers))

    // Now let fetch #2 finish: new connection data applies cleanly.
    releaseSecondList.signal()
    for _ in 0..<50 where !store.loaded.contains(.containers) {
        try await Task.sleep(for: .milliseconds(20))
    }
    #expect(store.containers.map(\.name) == ["new"])
    #expect(store.loading.isEmpty)
    store.follow(nil)
}
