import Foundation

/// What the Resources pane offers on this Mac. Mirrors the daemon's checks in
/// `crates/kat-daemon/src/config.rs`: 1 to all CPUs, 1 GiB to the Mac's memory
/// minus 2 GiB. Memory is a ceiling, not a reservation: what the VM does not
/// use goes back to the Mac, so the default is half of it, up to 16 GiB.
struct ResourceLimits: Equatable {
    let cpus: ClosedRange<Int>
    let memoryGiB: ClosedRange<Int>
    let defaultMemoryGiB: Int
    let diskGiB: ClosedRange<Int> = 64...16384
    let defaultDiskGiB: Int = 64

    init(hostCPUs: Int, hostMemoryBytes: UInt64) {
        cpus = 1...max(1, hostCPUs)
        let hostMiB = Int(hostMemoryBytes >> 20)
        memoryGiB = 1...max(1, (hostMiB - 2048) / 1024)
        let half = min(max(hostMiB / 2, 2048), 16384)
        defaultMemoryGiB = min(max(half / 1024, memoryGiB.lowerBound), memoryGiB.upperBound)
    }

    static let host = ResourceLimits(hostCPUs: ProcessInfo.processInfo.processorCount,
                                     hostMemoryBytes: ProcessInfo.processInfo.physicalMemory)

    func clampCPUs(_ n: Int) -> Int { min(max(n, cpus.lowerBound), cpus.upperBound) }

    func clampDiskGiB(_ n: Int) -> Int { min(max(n, diskGiB.lowerBound), diskGiB.upperBound) }

    /// The slider position for a saved size, in whole GiB.
    func memoryGiB(fromMiB mib: Int) -> Int {
        let gib = Int((Double(mib) / 1024).rounded())
        return min(max(gib, memoryGiB.lowerBound), memoryGiB.upperBound)
    }

    /// One tick per GiB while they stay apart, none on Macs with more memory.
    var memoryTickMarks: Int { memoryGiB.count <= 16 ? memoryGiB.count : 0 }
}

/// Memory the way About This Mac writes it ("8 GB").
func memoryLabel(gib: Int) -> String {
    ByteCountFormatter.string(fromByteCount: Int64(gib) << 30, countStyle: .memory)
}

extension Config {
    /// The `config.set` values for the keys that differ from `old`, so an
    /// untouched odd size (1536 MiB from the CLI) is never rounded and saved.
    func values(changedFrom old: Config) -> [String: Any] {
        var v: [String: Any] = [:]
        if cpus != old.cpus { v["cpus"] = cpus }
        if memory_mib != old.memory_mib { v["memory_mib"] = memory_mib }
        if disk_gib != old.disk_gib, let disk_gib { v["disk_gib"] = disk_gib }
        if mount != old.mount { v["mount"] = mount }
        if launch_at_login != old.launch_at_login { v["launch_at_login"] = launch_at_login }
        if kubernetes != old.kubernetes, let kubernetes { v["kubernetes"] = kubernetes }
        return v
    }
}

/// The note Settings shows above its groups, if any.
enum SettingsNotice: Equatable {
    case daemonNotRunning
    case saveFailed(String)
    case kubernetesFailed(String)
    case restartRequired

    static let errorPrefix = "settings: "
    static let kubernetesErrorPrefix = "kubernetes: "

    init?(status: DaemonStatus?, restartRequired: Bool, lastError: String?) {
        guard let status else { self = .daemonNotRunning; return }
        if let e = lastError, e.hasPrefix(Self.errorPrefix) {
            self = .saveFailed(String(e.dropFirst(Self.errorPrefix.count)))
        } else if let e = lastError, e.hasPrefix(Self.kubernetesErrorPrefix) {
            self = .kubernetesFailed(String(e.dropFirst(Self.kubernetesErrorPrefix.count)))
        } else if restartRequired && Phase(status).isUp {
            self = .restartRequired
        } else {
            return nil
        }
    }

    var message: String {
        switch self {
        case .daemonNotRunning: "Katamaran isn’t running. Start it to change settings."
        case .saveFailed(let why): "Couldn’t save the change: \(why)"
        case .kubernetesFailed(let why): "Kubernetes error: \(why)"
        case .restartRequired: "Restart the VM to apply your changes."
        }
    }

    var symbol: String {
        switch self {
        case .daemonNotRunning: "power.circle.fill"
        case .saveFailed, .kubernetesFailed: "exclamationmark.triangle.fill"
        case .restartRequired: "arrow.clockwise.circle.fill"
        }
    }

    /// The button title, when the note offers an action.
    var action: String? {
        switch self {
        case .daemonNotRunning: "Start"
        case .saveFailed, .kubernetesFailed: nil
        case .restartRequired: "Restart"
        }
    }
}

/// The Settings panes, in toolbar order.
enum SettingsPane: String, CaseIterable {
    case general, resources, kubernetes

    var title: String {
        switch self {
        case .general: "General"
        case .resources: "Resources"
        case .kubernetes: "Kubernetes"
        }
    }

    var symbol: String {
        switch self {
        case .general: "gearshape"
        case .resources: "cpu"
        case .kubernetes: "hexagon"
        }
    }
}
