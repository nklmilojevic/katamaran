import AppKit
import Foundation

/// Polls the daemon and runs its actions; Docker itself is `DockerStore`.
/// Views register with `observe` and are told after every change.
@MainActor
final class AppModel {
    private(set) var status: DaemonStatus?
    private(set) var samples: [Sample] = []
    private(set) var lastError: String?
    private var busyCount = 0
    var busy: Bool { busyCount > 0 }
    private(set) var restartRequired = false
    private var observers: [() -> Void] = []
    private var refreshing = false
    private var refreshPending = false
    private var refreshSeq: UInt64 = 0

    func observe(_ f: @escaping () -> Void) { observers.append(f) }
    private func changed() { observers.forEach { $0() } }

    let daemon = DaemonClient(socketPath: DaemonClient.defaultSocket())
    var phase: Phase { Phase(status) }
    private var timer: Timer?
    /// Called once, after the first successful status (for the smoke test).
    var onFirstStatus: ((DaemonStatus) -> Void)? {
        didSet {
            if let status, let f = onFirstStatus { onFirstStatus = nil; f(status) }
        }
    }

    static let shared = AppModel()

    init() {
        refresh()
        // .common, so it keeps firing while the menu is tracking.
        let t = Timer(timeInterval: 2, repeats: true) { [weak self] _ in
            Task { @MainActor in self?.refresh() }
        }
        RunLoop.main.add(t, forMode: .common)
        timer = t
    }

    /// For `--steps` snapshots: a made-up daemon status instead of the daemon's.
    private var faking = false

    func fake(_ status: DaemonStatus?) {
        faking = true
        refreshSeq &+= 1
        apply(status: status, stats: nil, view: nil)
    }

    func refresh(force: Bool = false) {
        guard !faking else { return }
        if refreshing {
            if force {
                refreshSeq &+= 1
                refreshPending = true
            }
            return
        }
        refreshing = true
        refreshPending = false
        refreshSeq &+= 1
        let seq = refreshSeq
        let daemon = daemon
        BlockingIO.run(on: BlockingIO.daemon) {
            let status: DaemonStatus? = try? daemon.decode("status", timeout: 3)
            let stats: StatsResult? = status == nil ? nil : try? daemon.decode("stats", timeout: 3)
            let view: ConfigView? = status == nil ? nil : try? daemon.decode("config.get", timeout: 3)
            return (status, stats, view)
        } completion: { [weak self] status, stats, view in
            guard let self else { return }
            self.refreshing = false
            guard !self.faking else { return }
            if seq == self.refreshSeq {
                self.apply(status: status, stats: stats, view: view)
            }
            if self.refreshPending {
                self.refreshPending = false
                self.refresh()
            }
        }
    }

    private func apply(status: DaemonStatus?, stats: StatsResult?, view: ConfigView?) {
        self.status = status
        if let stats { samples = stats.samples }
        if status == nil { samples = [] }
        restartRequired = view?.restart_required ?? false
        changed()
        if let status, let f = onFirstStatus {
            onFirstStatus = nil
            f(status)
        }
    }

    /// Run a daemon call off the main thread, then refresh.
    /// `done` is told whether the call succeeded, before the refresh.
    private func perform(_ what: String, queue: DispatchQueue = BlockingIO.daemon,
                         done: ((Bool) -> Void)? = nil, _ body: @escaping @Sendable () throws -> Void) {
        busyCount += 1
        lastError = nil
        changed()
        BlockingIO.run(on: queue) {
            var failure: String? = nil
            do { try body() } catch { failure = "\(what): \(error.localizedDescription)" }
            return failure
        } completion: { [weak self] err in
            guard let self else { return }
            self.busyCount = max(0, self.busyCount - 1)
            self.lastError = err
            done?(err == nil)
            self.changed()
            self.refresh(force: true)
        }
    }

    func start() {
        if status == nil {
            // No daemon: the CLI starts one (through launchd when installed).
            perform("start") { try AppModel.runCLI(["start"]) }
        } else {
            let d = daemon
            perform("start") { _ = try d.call("start") }
        }
    }

    func stop() {
        let d = daemon
        perform("stop") { _ = try d.call("stop", ["wait": true], timeout: 90) }
    }

    func restart() {
        let d = daemon
        perform("restart") { _ = try d.call("restart", timeout: 120) }
    }

    private struct SendableDict: @unchecked Sendable {
        let value: [String: Any]
    }

    func saveConfig(_ values: [String: Any], done: ((Bool) -> Void)? = nil) {
        let d = daemon
        let box = SendableDict(value: values)
        perform("settings", queue: BlockingIO.config, done: done) {
            _ = try d.call("config.set", ["values": box.value])
        }
    }

    func k8sReset() {
        let d = daemon
        perform("kubernetes") { _ = try d.call("k8s.reset", timeout: 300) }
    }

    func k8sSyncImages() {
        let d = daemon
        perform("kubernetes") { _ = try d.call("k8s.sync_images", timeout: 120) }
    }

    /// The `katamaran` CLI next to this app's executable (in the bundle), or $KATAMARAN_BIN.
    nonisolated static func cliPath() -> String {
        if let p = ProcessInfo.processInfo.environment["KATAMARAN_BIN"], !p.isEmpty { return p }
        let dir = Bundle.main.executableURL?.deletingLastPathComponent()
        return dir?.appendingPathComponent("katamaran").path ?? "katamaran"
    }

    nonisolated static func runCLI(_ args: [String], executable: String? = nil, timeout: TimeInterval = 60) throws {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: executable ?? cliPath())
        p.arguments = args
        p.standardInput = FileHandle.nullDevice
        let err = Pipe()
        p.standardError = err
        p.standardOutput = FileHandle.nullDevice
        try p.run()
        let timeoutLock = NSLock()
        var timedOut = false
        let timer = DispatchWorkItem {
            guard p.isRunning else { return }
            timeoutLock.lock()
            timedOut = true
            timeoutLock.unlock()
            p.terminate()
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 2) {
                if p.isRunning { kill(p.processIdentifier, SIGKILL) }
            }
        }
        if timeout > 0 {
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeout, execute: timer)
        }
        // Drain stderr before waiting for exit so a verbose CLI never fills the pipe and deadlocks.
        let errData = err.fileHandleForReading.readDataToEndOfFile()
        p.waitUntilExit()
        timer.cancel()
        timeoutLock.lock()
        let didTimeout = timedOut
        timeoutLock.unlock()
        if didTimeout {
            throw SocketError.protocolError("command timed out")
        }
        if p.terminationStatus != 0 {
            let msg = String(decoding: errData, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
            throw SocketError.protocolError(msg.isEmpty ? "exit status \(p.terminationStatus)" : msg)
        }
    }
}
