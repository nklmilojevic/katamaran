import AppKit
import Testing
@testable import KatamaranApp

private let gib: UInt64 = 1 << 30

private func status(state: String = "running", docker: String = "ready") throws -> DaemonStatus {
    let json = """
    {"state": "\(state)", "since_ms": 0,
     "daemon": {"pid": 1, "home": "/tmp/k"},
     "versions": {"katamaran": "0.1.0", "protocol": 1},
     "config": {"cpus": 4, "memory_mib": 4096, "mount": true, "launch_at_login": true},
     "docker": {"state": "\(docker)", "socket": "/tmp/k/run/docker.sock"},
     "mount": {"enabled": true, "mounted": true},
     "ports": []}
    """
    return try JSONDecoder().decode(DaemonStatus.self, from: Data(json.utf8))
}

private let base = Config(cpus: 4, memory_mib: 4096, mount: true, mount_path: nil, launch_at_login: true)

@Test func limitsFollowTheDaemonsChecks() {
    let l = ResourceLimits(hostCPUs: 10, hostMemoryBytes: 32 * gib)
    #expect(l.cpus == 1...10)
    // config.rs: up to the Mac's memory minus 2 GiB.
    #expect(l.memoryGiB == 1...30)
    #expect(l.memoryTickMarks == 0)
    #expect(l.clampCPUs(0) == 1)
    #expect(l.clampCPUs(64) == 10)
}

@Test func smallMacsGetOneTickPerGiB() {
    let l = ResourceLimits(hostCPUs: 8, hostMemoryBytes: 8 * gib)
    #expect(l.memoryGiB == 1...6)
    #expect(l.memoryTickMarks == 6)
    // Never an empty range, even on a Mac the daemon would refuse anyway.
    #expect(ResourceLimits(hostCPUs: 0, hostMemoryBytes: 2 * gib).memoryGiB == 1...1)
    #expect(ResourceLimits(hostCPUs: 0, hostMemoryBytes: 2 * gib).cpus == 1...1)
}

@Test func savedMemoryRoundsToTheSlider() {
    let l = ResourceLimits(hostCPUs: 8, hostMemoryBytes: 16 * gib)
    #expect(l.memoryGiB(fromMiB: 4096) == 4)
    #expect(l.memoryGiB(fromMiB: 1536) == 2)
    #expect(l.memoryGiB(fromMiB: 512) == 1)
    #expect(l.memoryGiB(fromMiB: 64 * 1024) == 14)
}

@Test func memoryReadsLikeAboutThisMac() {
    #expect(memoryLabel(gib: 8).contains("8"))
    #expect(memoryLabel(gib: 8).contains("GB"))
}

@Test func onlyChangedKeysAreSent() {
    #expect(base.values(changedFrom: base).isEmpty)
    var next = base
    next.cpus = 2
    #expect(NSDictionary(dictionary: next.values(changedFrom: base)) == ["cpus": 2])
    next = base
    next.launch_at_login = false
    next.mount = false
    #expect(NSDictionary(dictionary: next.values(changedFrom: base)) == ["launch_at_login": false, "mount": false])
    // An odd size from the CLI stays as it is unless memory itself changes.
    var odd = base
    odd.memory_mib = 1536
    next = odd
    next.cpus = 6
    #expect(next.values(changedFrom: odd)["memory_mib"] == nil)
}

@Test func noticeWithoutADaemon() {
    let n = SettingsNotice(status: nil, restartRequired: false, lastError: nil)
    #expect(n == .daemonNotRunning)
    #expect(n?.action == "Start")
}

@Test func noticeAsksForARestartOnlyWhileTheVMIsUp() throws {
    #expect(SettingsNotice(status: try status(), restartRequired: true, lastError: nil) == .restartRequired)
    #expect(SettingsNotice(status: try status(state: "starting"), restartRequired: true, lastError: nil) == .restartRequired)
    #expect(SettingsNotice(status: try status(state: "stopped"), restartRequired: true, lastError: nil) == nil)
    #expect(SettingsNotice(status: try status(), restartRequired: false, lastError: nil) == nil)
}

@Test func noticeShowsFailedSavesButNotOtherErrors() throws {
    let failed = SettingsNotice(status: try status(), restartRequired: true, lastError: "settings: cpus must be between 1 and 10")
    #expect(failed == .saveFailed("cpus must be between 1 and 10"))
    #expect(failed?.action == nil)
    #expect(SettingsNotice(status: try status(), restartRequired: false, lastError: "stop: timed out") == nil)
}

@Test func symbolsExist() {
    let notices: [SettingsNotice] = [.daemonNotRunning, .saveFailed("x"), .restartRequired]
    for name in SettingsPane.allCases.map(\.symbol) + notices.map(\.symbol) {
        #expect(NSImage(systemSymbolName: name, accessibilityDescription: nil) != nil, "\(name)")
    }
    #expect(SettingsPane(rawValue: "resources") == .resources)
}

@Test func defaultMemoryMatchesTheDaemon() {
    let gib: UInt64 = 1 << 30
    // config.rs default_memory_mib: half of the Mac, 2 to 16 GiB.
    #expect(ResourceLimits(hostCPUs: 8, hostMemoryBytes: 8 * gib).defaultMemoryGiB == 4)
    #expect(ResourceLimits(hostCPUs: 8, hostMemoryBytes: 18 * gib).defaultMemoryGiB == 9)
    #expect(ResourceLimits(hostCPUs: 8, hostMemoryBytes: 36 * gib).defaultMemoryGiB == 16)
    #expect(ResourceLimits(hostCPUs: 8, hostMemoryBytes: 128 * gib).defaultMemoryGiB == 16)
    // Always a slider position.
    #expect(ResourceLimits(hostCPUs: 8, hostMemoryBytes: 3 * gib).defaultMemoryGiB == 1)
}

@MainActor
@Test func machineNamesAreValidated() {
    #expect(NewMachineSheet.nameProblem("dev", taken: []) == nil)
    #expect(NewMachineSheet.nameProblem("dev-2", taken: []) == nil)
    #expect(NewMachineSheet.nameProblem("", taken: []) != nil)
    #expect(NewMachineSheet.nameProblem("Dev", taken: []) != nil)
    #expect(NewMachineSheet.nameProblem("-dev", taken: []) != nil)
    #expect(NewMachineSheet.nameProblem("dev", taken: ["dev"]) != nil)
}

@Test func firstRunSetupOnlyForInstalledCopies() {
    #expect(FirstRun.shouldRun(bundlePath: "/Applications/Katamaran.app", env: [:]))
    #expect(!FirstRun.shouldRun(bundlePath: "/Users/me/src/katamaran/target/Katamaran.app", env: [:]))
    #expect(!FirstRun.shouldRun(bundlePath: "/Applications/Katamaran.app", env: ["KATAMARAN_HOME": "/tmp/x"]))
}
