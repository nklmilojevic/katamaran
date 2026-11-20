import AppKit
import Foundation

/// Polls the daemon and runs its actions; Docker itself is `DockerStore`.
/// Views register with `observe` and are told after every change.
@MainActor
final class AppModel {
    private(set) var status: DaemonStatus?
    private(set) var samples: [Sample] = []
    private(set) var lastError: String?
    private(set) var busy = false
    private(set) var restartRequired = false
    private var observers: [() -> Void] = []

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
        apply(status: status, stats: nil, view: nil)
    }

    func refresh() {
        guard !faking else { return }
        let daemon = daemon
        Task.detached {
            let status: DaemonStatus? = try? daemon.decode("status", timeout: 3)
            let stats: StatsResult? = status == nil ? nil : try? daemon.decode("stats", timeout: 3)
            let view: ConfigView? = status == nil ? nil : try? daemon.decode("config.get", timeout: 3)
            await MainActor.run {
                guard !self.faking else { return }
                self.apply(status: status, stats: stats, view: view)
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
    private func perform(_ what: String, done: ((Bool) -> Void)? = nil, _ body: @escaping @Sendable () throws -> Void) {
        busy = true
        lastError = nil
        changed()
        Task.detached {
            var failure: String? = nil
            do { try body() } catch { failure = "\(what): \(error.localizedDescription)" }
            let err = failure
            await MainActor.run {
                self.busy = false
                self.lastError = err
                done?(err == nil)
                self.refresh()
            }
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

    func saveConfig(_ values: [String: Any], done: ((Bool) -> Void)? = nil) {
        let d = daemon
        perform("settings", done: done) { _ = try d.call("config.set", ["values": values]) }
    }

    /// The `katamaran` CLI next to this app's executable (in the bundle), or $KATAMARAN_BIN.
    nonisolated static func cliPath() -> String {
        if let p = ProcessInfo.processInfo.environment["KATAMARAN_BIN"], !p.isEmpty { return p }
        let dir = Bundle.main.executableURL?.deletingLastPathComponent()
        return dir?.appendingPathComponent("katamaran").path ?? "katamaran"
    }

    nonisolated static func runCLI(_ args: [String]) throws {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: cliPath())
        p.arguments = args
        let err = Pipe()
        p.standardError = err
        p.standardOutput = FileHandle.nullDevice
        try p.run()
        p.waitUntilExit()
        if p.terminationStatus != 0 {
            let msg = String(decoding: err.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
            throw SocketError.protocolError(msg.trimmingCharacters(in: .whitespacesAndNewlines))
        }
    }
}
