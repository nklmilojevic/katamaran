import Foundation

/// The Docker CLI with Buildx, pointed at this VM's engine. BuildKit's history
/// lives behind its gRPC control API; Buildx already speaks it (HTTP/2,
/// protobuf, the trace store), so the app asks Buildx instead of carrying its
/// own HTTP/2 client. Blocking calls: run them off the main thread.
struct BuildxCLI: Sendable {
    let docker: String
    let socket: String

    /// The Docker CLI on the user's PATH as a login shell would have it, or in a usual place.
    static func find() -> String? {
        let home = NSHomeDirectory()
        var dirs = (ProcessInfo.processInfo.environment["PATH"] ?? "").split(separator: ":").map(String.init)
        dirs += ["/opt/homebrew/bin", "/usr/local/bin", "\(home)/.nix-profile/bin", "/run/current-system/sw/bin",
                 "/nix/var/nix/profiles/default/bin", "\(home)/.docker/bin",
                 "/Applications/Docker.app/Contents/Resources/bin"]
        return dirs.map { ($0 as NSString).appendingPathComponent("docker") }
            .first { FileManager.default.isExecutableFile(atPath: $0) }
    }

    /// The default builder of the engine at DOCKER_HOST, whatever builder the user picked elsewhere.
    func process(_ args: [String]) -> Process {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: docker)
        p.arguments = ["buildx"] + args.prefix(2) + ["--builder", "default"] + args.dropFirst(2)
        var env = ProcessInfo.processInfo.environment
        env["DOCKER_HOST"] = "unix://" + socket
        env["DOCKER_CONTEXT"] = nil
        env["BUILDX_BUILDER"] = nil
        env["NO_COLOR"] = "1"
        // Apps start with a bare PATH; credential helpers and plugins look there.
        let dir = (docker as NSString).deletingLastPathComponent
        env["PATH"] = ([dir, "/opt/homebrew/bin", "/usr/local/bin"] + (env["PATH"] ?? "/usr/bin:/bin").split(separator: ":").map(String.init))
            .joined(separator: ":")
        p.environment = env
        p.standardInput = FileHandle.nullDevice
        return p
    }

    /// Run `docker buildx <args>` to the end; stdout (and stderr, where progress
    /// printers write, with `progress`), or the CLI's error.
    @discardableResult
    func run(_ args: [String], timeout: TimeInterval = 30, progress: Bool = false) throws -> Data {
        let p = process(args)
        let out = Pipe(), err = Pipe()
        p.standardOutput = out
        p.standardError = err
        try p.run()
        let timer = DispatchWorkItem { if p.isRunning { p.terminate() } }
        DispatchQueue.global().asyncAfter(deadline: .now() + timeout, execute: timer)
        // Read both before waiting: a full pipe would block the CLI forever.
        var stderr = Data()
        let group = DispatchGroup()
        group.enter()
        DispatchQueue.global().async {
            stderr = err.fileHandleForReading.readDataToEndOfFile()
            group.leave()
        }
        let stdout = out.fileHandleForReading.readDataToEndOfFile()
        group.wait()
        p.waitUntilExit()
        timer.cancel()
        guard p.terminationStatus == 0 else {
            throw SocketError.protocolError(BuildxCLI.message(stderr) ?? "docker buildx \(args.prefix(2).joined(separator: " ")) failed")
        }
        return progress ? stdout + stderr : stdout
    }

    /// The CLI's last "ERROR: …" line, without the prefix.
    static func message(_ stderr: Data) -> String? {
        let all = String(decoding: stderr, as: UTF8.self)
        if all.contains("is not a docker command") || all.contains("unknown command: docker buildx") {
            return "The Docker CLI has no Buildx plugin."
        }
        let lines = all.split(separator: "\n").map {
            $0.trimmingCharacters(in: .whitespaces)
        }.filter { !$0.isEmpty }
        guard var last = lines.last(where: { $0.hasPrefix("ERROR:") }) ?? lines.last else { return nil }
        if last.hasPrefix("ERROR:") { last = last.dropFirst(6).trimmingCharacters(in: .whitespaces) }
        return last
    }
}

/// A build's progress text, read whole for a finished build or as it comes
/// for a running one.
@MainActor
final class BuildLog {
    private(set) var parser = BuildProgressParser()
    /// Every line, as `docker build --progress plain` prints them.
    private(set) var lines: [String] = []
    private(set) var finished = false
    private(set) var error: String?
    fileprivate var process: Process?
    private var splitter = LineSplitter()

    fileprivate func append(_ data: Data) {
        for l in splitter.feed(data) { add(String(decoding: l, as: UTF8.self)) }
    }

    fileprivate func add(_ line: String) {
        let clean = stripANSI(line)
        lines.append(clean)
        parser.feed(line: clean)
    }

    fileprivate func finish(_ error: String?) {
        if let rest = splitter.flush() { add(String(decoding: rest, as: UTF8.self)) }
        finished = true
        self.error = error
        process = nil
    }

    fileprivate func set(_ text: String) {
        lines = []
        parser = BuildProgressParser()
        for l in text.split(separator: "\n", omittingEmptySubsequences: false) { add(String(l)) }
        while lines.last?.isEmpty == true { lines.removeLast() }
        finished = true
    }
}

/// Build history from BuildKit, kept current while something shows it.
///
/// Nothing announces that a build started (the engine's events only mention
/// the image it exports at the end), so the list is polled: every 2 s while
/// the Builds section is on screen, every 10 s while the window is open for
/// the sidebar's badge, and not at all otherwise. Image events refresh it at
/// once. Running builds on screen stream their progress; finished builds'
/// details and logs are fetched once and kept.
@MainActor
final class BuildStore {
    static let shared = BuildStore()

    private(set) var socket: String?
    private(set) var records: [BuildRecord] = []
    private(set) var loaded = false
    /// Why the list couldn't be read: no Docker CLI, no Buildx, Docker's answer.
    private(set) var loadError: String?
    private(set) var infos: [String: BuildInfo] = [:]
    private var logs: [String: BuildLog] = [:]
    private var fetchingInfo: Set<String> = []
    private var fetchingList = false
    private var timer: Timer?
    private var ticker: Timer?
    private var observers: [(BuildStore) -> Void] = []
    private var cli: BuildxCLI?
    private var refreshSoon: DispatchWorkItem?

    /// The Builds section is on screen.
    var visible = false { didSet { if visible != oldValue { activityChanged() } } }
    /// The main window is open (the sidebar shows a badge for running builds).
    var windowOpen = false { didSet { if windowOpen != oldValue { activityChanged() } } }

    func observe(_ f: @escaping (BuildStore) -> Void) { observers.append(f) }
    private func changed() { observers.forEach { $0(self) } }

    /// Status from `inspect` when known: `ls` calls a canceled build an error.
    func status(_ r: BuildRecord) -> BuildStatus {
        if r.isRunning { return .running }
        return infos[r.id]?.status ?? r.status
    }

    var running: [BuildRecord] { records.filter(\.isRunning) }

    func record(_ id: String) -> BuildRecord? { records.first { $0.id == id } }

    func follow(_ socket: String?) {
        guard socket != self.socket else { return }
        self.socket = socket
        logs.values.forEach { $0.process?.terminate() }
        logs = [:]
        infos = [:]
        records = []
        loaded = false
        loadError = nil
        cli = nil
        if let socket {
            if let docker = BuildxCLI.find() { cli = BuildxCLI(docker: docker, socket: socket) }
            else { loadError = "Builds are read with the Docker CLI, which isn’t installed." }
        }
        activityChanged()
        changed()
    }

    /// An engine event: an image appearing usually means a build just finished.
    func engineEvent(_ e: DockerEvent) {
        guard e.Type == "image", ["create", "tag", "untag", "delete"].contains(e.Action ?? "") else { return }
        guard windowOpen else { return }
        refreshSoon?.cancel()
        let w = DispatchWorkItem { [weak self] in self?.refresh() }
        refreshSoon = w
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3, execute: w)
    }

    private func activityChanged() {
        timer?.invalidate()
        timer = nil
        let interval: TimeInterval? = cli == nil ? nil : visible ? (running.isEmpty ? 2 : 1.5) : windowOpen ? 10 : nil
        if let interval {
            if !loaded || visible { refresh() }
            let t = Timer(timeInterval: interval, repeats: true) { [weak self] _ in
                MainActor.assumeIsolated { self?.refresh() }
            }
            t.tolerance = interval * 0.2
            RunLoop.main.add(t, forMode: .common)
            timer = t
        }
        // Running builds' clocks tick while on screen.
        let tick = visible && !running.isEmpty
        if tick, ticker == nil {
            let t = Timer(timeInterval: 1, repeats: true) { [weak self] _ in
                MainActor.assumeIsolated { self.map { $0.changed() } }
            }
            RunLoop.main.add(t, forMode: .common)
            ticker = t
        } else if !tick {
            ticker?.invalidate()
            ticker = nil
        }
        if !visible {
            // Live logs only stream while someone watches.
            for (id, log) in logs where !log.finished {
                log.process?.terminate()
                logs[id] = nil
            }
        } else {
            for r in running { _ = log(r.id) }
        }
    }

    func refresh() {
        guard let cli, !fetchingList else { return }
        fetchingList = true
        let sock = socket
        Task.detached {
            var list: [BuildRecord]? = nil
            var problem: String? = nil
            do { list = BuildRecord.parseList(try cli.run(["history", "ls", "--format", "json"])) }
            catch { problem = error.localizedDescription }
            let (l, p) = (list, problem)
            await MainActor.run {
                self.fetchingList = false
                guard self.socket == sock else { return }
                self.apply(l, error: p)
            }
        }
    }

    private func apply(_ list: [BuildRecord]?, error: String?) {
        let wasRunning = Set(running.map(\.id))
        var dirty = error != loadError || !loaded
        loadError = error
        loaded = true
        if let list, list != records {
            records = list
            dirty = true
        }
        let ids = Set(records.map(\.id))
        // Builds that just finished: their details and logs are final now.
        for id in wasRunning where !(record(id)?.isRunning ?? true) {
            infos[id] = nil
            if logs[id]?.finished == true || logs[id]?.process == nil { logs[id] = nil }
        }
        for id in infos.keys where !ids.contains(id) { infos[id] = nil }
        for id in logs.keys where !ids.contains(id) { logs[id]?.process?.terminate(); logs[id] = nil }
        if wasRunning != Set(running.map(\.id)) { activityChanged() }
        if visible { fetchInfos() }
        if dirty { changed() }
    }

    /// Details for the latest builds, one at a time, newest first; finished ones are kept.
    private func fetchInfos() {
        guard let cli, fetchingInfo.isEmpty else { return }
        let want = records.prefix(60).first { infos[$0.id] == nil || $0.isRunning && infos[$0.id]?.status != .running }
        guard let r = want, !(r.isRunning && infos[r.id] != nil) else { return }
        fetchingInfo.insert(r.id)
        let sock = socket
        Task.detached {
            let info = try? BuildInfo.parse(cli.run(["history", "inspect", r.id, "--format", "json"]))
            await MainActor.run {
                self.fetchingInfo.remove(r.id)
                guard self.socket == sock, self.record(r.id) != nil else { return }
                if let info { self.infos[r.id] = info; self.changed() }
                if info != nil { self.fetchInfos() }
            }
        }
    }

    /// Details now, for the selected build.
    func info(_ id: String) -> BuildInfo? {
        if infos[id] == nil, fetchingInfo.isEmpty { fetchInfos() }
        return infos[id]
    }

    /// The build's progress: streamed while it runs, read once when it's done.
    func log(_ id: String) -> BuildLog? {
        if let l = logs[id] { return l }
        guard let cli, let r = record(id) else { return nil }
        let log = BuildLog()
        logs[id] = log
        if r.isRunning {
            guard visible else { logs[id] = nil; return nil }
            let p = cli.process(["history", "logs", id, "--progress", "plain"])
            let out = Pipe()
            p.standardOutput = out
            p.standardError = out
            out.fileHandleForReading.readabilityHandler = { [weak log] h in
                let d = h.availableData
                guard !d.isEmpty else { h.readabilityHandler = nil; return }
                DispatchQueue.main.async { log?.append(d); self.changed() }
            }
            p.terminationHandler = { [weak log] p in
                let code = p.terminationStatus
                DispatchQueue.main.async {
                    guard let log else { return }
                    log.finish(code == 0 ? nil : "The build’s progress stopped.")
                    self.changed()
                    self.refresh()
                }
            }
            log.process = p
            do { try p.run() } catch { log.finish(error.localizedDescription) }
        } else {
            let sock = socket
            Task.detached {
                var text = "", problem: String? = nil
                do { text = String(decoding: try cli.run(["history", "logs", id, "--progress", "plain"], timeout: 60, progress: true), as: UTF8.self) }
                catch { problem = error.localizedDescription }
                let (t, p) = (text, problem)
                await MainActor.run {
                    guard self.socket == sock, self.logs[id] === log else { return }
                    log.set(t)
                    if let p { log.finish(p) }
                    self.trimLogs(keeping: id)
                    self.changed()
                }
            }
        }
        return log
    }

    /// Finished logs can be large: keep the last few looked at.
    private var recent: [String] = []
    private func trimLogs(keeping id: String) {
        recent.removeAll { $0 == id }
        recent.append(id)
        while recent.count > 12 {
            let old = recent.removeFirst()
            if logs[old]?.finished == true { logs[old] = nil }
        }
    }

    // MARK: Actions

    var onError: (String, String) -> Void = { _, _ in }

    func remove(_ ids: [String]) {
        guard let cli, !ids.isEmpty else { return }
        perform(ids.count == 1 ? "Couldn’t delete the build record" : "Couldn’t delete the build records") {
            try cli.run(["history", "rm"] + ids)
        }
    }

    func removeAll() {
        guard let cli else { return }
        perform("Couldn’t clear the build history") { try cli.run(["history", "rm", "--all"]) }
    }

    private func perform(_ what: String, _ body: @escaping @Sendable () throws -> Void) {
        Task.detached {
            var failure: String? = nil
            do { try body() } catch { failure = error.localizedDescription }
            let err = failure
            await MainActor.run {
                if let err { self.onError(what, err) }
                self.refresh()
            }
        }
    }
}
