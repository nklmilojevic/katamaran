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
    next.kubernetes = true
    #expect(NSDictionary(dictionary: next.values(changedFrom: base)) == ["launch_at_login": false, "mount": false, "kubernetes": true])
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

@Test func noticeShowsFailedSavesAndKubernetesErrorsButNotOtherErrors() throws {
    let failed = SettingsNotice(status: try status(), restartRequired: true, lastError: "settings: cpus must be between 1 and 10")
    #expect(failed == .saveFailed("cpus must be between 1 and 10"))
    #expect(failed?.action == nil)
    let k8sFailed = SettingsNotice(status: try status(), restartRequired: true, lastError: "kubernetes: cluster reset timed out")
    #expect(k8sFailed == .kubernetesFailed("cluster reset timed out"))
    #expect(k8sFailed?.action == nil)
    #expect(k8sFailed?.message.contains("cluster reset timed out") == true)
    #expect(SettingsNotice(status: try status(), restartRequired: false, lastError: "stop: timed out") == nil)
}

@Test func symbolsExist() {
    let notices: [SettingsNotice] = [.daemonNotRunning, .saveFailed("x"), .kubernetesFailed("y"), .restartRequired]
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

@Test func firstRunRerunsSetupOnUpgradeAndVersionMismatch() {
    #expect(FirstRun.bundleStamp(info: [
        "CFBundleShortVersionString": "0.6.0",
        "CFBundleVersion": "250",
    ]) == "0.6.0 (250)")
    #expect(FirstRun.bundleStamp(info: ["CFBundleShortVersionString": "0.6.0"]) == "0.6.0")
    #expect(FirstRun.bundleStamp(info: ["CFBundleVersion": "250"]) == "250")
    #expect(FirstRun.bundleStamp(info: ["CFBundleShortVersionString": "  "]) == nil)
    #expect(FirstRun.bundleStamp(info: nil) == nil)

    // Initial install runs setup.
    #expect(FirstRun.launchAction(setupDone: false, lastStamp: nil, currentStamp: "0.6.0 (250)") == .setup)
    // Upgrade from an earlier release that had no version stamp runs setup.
    #expect(FirstRun.launchAction(setupDone: true, lastStamp: nil, currentStamp: "0.6.0 (250)") == .setup)
    // Upgrade to a newer version or build runs setup.
    #expect(FirstRun.launchAction(setupDone: true, lastStamp: "0.5.0 (210)", currentStamp: "0.6.0 (250)") == .setup)
    #expect(FirstRun.launchAction(setupDone: true, lastStamp: "0.6.0 (250)", currentStamp: "0.6.0 (251)") == .setup)
    // Same version only ensures the Docker context.
    #expect(FirstRun.launchAction(setupDone: true, lastStamp: "0.6.0 (250)", currentStamp: "0.6.0 (250)") == .useContext)
    // Missing bundle version falls back to setupDone.
    #expect(FirstRun.launchAction(setupDone: true, lastStamp: nil, currentStamp: nil) == .useContext)

    // Live daemon version mismatch triggers a one-shot upgrade.
    #expect(FirstRun.shouldUpgradeDaemon(daemonVersion: "0.1.0", appVersion: "0.6.0", alreadyTried: false, setupInFlight: false))
    #expect(!FirstRun.shouldUpgradeDaemon(daemonVersion: "0.6.0", appVersion: "0.6.0", alreadyTried: false, setupInFlight: false))
    #expect(!FirstRun.shouldUpgradeDaemon(daemonVersion: "0.1.0", appVersion: "0.6.0", alreadyTried: true, setupInFlight: false))
    #expect(!FirstRun.shouldUpgradeDaemon(daemonVersion: "0.1.0", appVersion: "0.6.0", alreadyTried: false, setupInFlight: true))
    #expect(!FirstRun.shouldUpgradeDaemon(daemonVersion: nil, appVersion: "0.6.0", alreadyTried: false, setupInFlight: false))
    #expect(!FirstRun.shouldUpgradeDaemon(daemonVersion: "0.1.0", appVersion: "", alreadyTried: false, setupInFlight: false))
}

@Test func machineShellScriptQuotesSingleQuotesAndSpecialChars() {
    let script = MachineShellCommand.script(
        name: "dev'$(id);rm -rf /",
        cliPath: "/Applications/My's App/katamaran",
        katamaranHome: "/tmp/it's home/$(whoami)"
    )
    #expect(script.hasPrefix("#!/bin/sh\n"))
    #expect(script.contains(#"export KATAMARAN_HOME='/tmp/it'\''s home/$(whoami)'"#))
    #expect(script.contains(#"exec '/Applications/My'\''s App/katamaran' shell 'dev'\''$(id);rm -rf /'"#))

    let noHome = MachineShellCommand.script(name: "ubuntu", cliPath: "/usr/local/bin/katamaran", katamaranHome: nil)
    #expect(noHome == "#!/bin/sh\nexec /usr/local/bin/katamaran shell ubuntu\n")
}

@Test func machineShellFilenameSanitizesPathSeparatorsAndControlChars() {
    let f1 = MachineShellCommand.filename(for: "../../etc/passwd")
    #expect(!f1.contains("/"))
    #expect(!f1.contains(".."))
    #expect(f1.hasPrefix("katamaran-shell-") && f1.hasSuffix(".command"))

    let f2 = MachineShellCommand.filename(for: "foo/bar\0baz'qux")
    #expect(!f2.contains("/"))
    #expect(!f2.contains("\0"))
    #expect(!f2.contains("'"))
    #expect(f2.hasPrefix("katamaran-shell-foo_bar_baz_qux-") && f2.hasSuffix(".command"))

    let f3 = MachineShellCommand.filename(for: "")
    #expect(f3.hasPrefix("katamaran-shell-machine-") && f3.hasSuffix(".command"))

    // Distinct names that sanitize to the same stem must not collide.
    #expect(MachineShellCommand.filename(for: "a/b") != MachineShellCommand.filename(for: "a?b"))
    let longPrefix = String(repeating: "a", count: 70)
    #expect(MachineShellCommand.filename(for: longPrefix + "1") != MachineShellCommand.filename(for: longPrefix + "2"))
}

@Test func runCLIDrainsLargeStderrAndEnforcesTimeout() throws {
    // Large stderr (> 64 KiB macOS pipe buffer) must not deadlock.
    do {
        try AppModel.runCLI(
            ["-c", "head -c 131072 /dev/zero | tr '\\0' 'x' >&2; exit 2"],
            executable: "/bin/sh",
            timeout: 5
        )
        Issue.record("expected non-zero exit error")
    } catch let err as SocketError {
        #expect(err.localizedDescription.count == 131072)
    }

    // Standard input is nullDevice so reading stdin returns EOF immediately.
    try AppModel.runCLI(["-c", "cat >/dev/null"], executable: "/bin/sh", timeout: 5)

    // Timeout terminates a hanging child process.
    let start = Date()
    do {
        try AppModel.runCLI(["-c", "sleep 30"], executable: "/bin/sh", timeout: 0.2)
        Issue.record("expected timeout error")
    } catch {
        #expect(error.localizedDescription.contains("timed out"))
        #expect(Date().timeIntervalSince(start) < 3.0)
    }

    // Timeout must not hang on stderr EOF if a grandchild inherits the stderr pipe.
    let grandchildStart = Date()
    do {
        try AppModel.runCLI(["-c", "(sleep 5) & sleep 5"], executable: "/bin/sh", timeout: 0.2)
        Issue.record("expected timeout error with lingering grandchild")
    } catch {
        #expect(error.localizedDescription.contains("timed out"))
        #expect(Date().timeIntervalSince(grandchildStart) < 3.0)
    }
}

@MainActor
@Test func updaterConfigurationAndPresentationHelpers() {
    #expect(Updater.isConfigured(info: [
        "SUFeedURL": "https://katamaran.rs/appcast.xml",
        "SUPublicEDKey": "abc123",
    ]))
    #expect(!Updater.isConfigured(info: ["SUFeedURL": "https://katamaran.rs/appcast.xml"]))
    #expect(!Updater.isConfigured(info: ["SUFeedURL": "  ", "SUPublicEDKey": "abc123"]))
    #expect(!Updater.isConfigured(info: nil))

    #expect(Updater.menuItemTitle(pendingVersion: nil) == "Check for Updates…")
    #expect(Updater.menuItemTitle(pendingVersion: "  ") == "Check for Updates…")
    #expect(Updater.menuItemTitle(pendingVersion: "0.7.0") == "Update Available (0.7.0)…")

    #expect(Updater.versionSubtitle(info: ["CFBundleShortVersionString": "0.6.0"]) == "Version 0.6.0")
    #expect(Updater.versionSubtitle(info: ["CFBundleShortVersionString": "0.6.0"], pendingVersion: "0.7.0") == "Version 0.6.0 · 0.7.0 available")
    #expect(Updater.versionSubtitle(info: nil, pendingVersion: "0.7.0") == "Version 0.7.0 available")
    #expect(Updater.versionSubtitle(info: nil, pendingVersion: nil) == nil)
    #expect(NSImage(systemSymbolName: "arrow.down.circle.fill", accessibilityDescription: nil) != nil)
}

@Test func daemonStatusDecodesPortErrors() throws {
    let clean = try status()
    #expect(clean.port_errors == nil)
    #expect(clean.portError(port: 80) == nil)
    #expect(clean.portErrorSummary == nil)
    #expect(clean.portErrorDetails == nil)

    let json = """
    {"state": "running", "since_ms": 0,
     "daemon": {"pid": 1, "home": "/tmp/k"},
     "versions": {"katamaran": "0.8.0", "protocol": 3},
     "config": {"cpus": 4, "memory_mib": 4096, "mount": true, "launch_at_login": true},
     "docker": {"state": "ready", "socket": "/tmp/k/run/docker.sock"},
     "mount": {"enabled": true, "mounted": true},
     "ports": [],
     "port_errors": [
       {"proto": "tcp", "port": 80, "error": "address already in use"},
       {"proto": "tcp", "port": 443, "error": "permission denied"}
     ]}
    """
    let decoded = try JSONDecoder().decode(DaemonStatus.self, from: Data(json.utf8))
    #expect(decoded.portError(proto: "TCP", port: 80) == "address already in use")
    #expect(decoded.portError(proto: "udp", port: 80) == nil)
    #expect(decoded.portError(port: 443) == "permission denied")
    #expect(decoded.portErrorSummary == "Ports not forwarded: 80/tcp, 443/tcp")
    #expect(decoded.portErrorDetails == "80/tcp: address already in use\n443/tcp: permission denied")

    let singleJSON = json.replacingOccurrences(
        of: #",\s*\{"proto": "tcp", "port": 443, "error": "permission denied"\}"#,
        with: "",
        options: .regularExpression
    )
    let single = try JSONDecoder().decode(DaemonStatus.self, from: Data(singleJSON.utf8))
    #expect(single.portErrorSummary == "Port 80/tcp not forwarded")

    // Service reveal skips failed ports and returns nil when every port failed.
    #expect(K8sGrouping.firstReachableLocalPort([80, 443], status: decoded) == nil)
    #expect(K8sGrouping.firstReachableLocalPort([80, 8080], status: decoded) == 8080)
    #expect(K8sGrouping.firstReachableLocalPort([80, 443], status: clean) == 80)
}

@MainActor
@Test func containerBindingsDeduplicateByHostAndProtocol() {
    let bindings = [
        PortBinding(host: 53, container: 53, proto: "udp"),
        PortBinding(host: 53, container: 53, proto: "UDP"),
        PortBinding(host: 53, container: 53, proto: "tcp"),
        PortBinding(host: 53, container: 53, proto: "tcp"),
    ]
    let unique = InfoTabView.uniqueHostBindings(bindings)
    #expect(unique.count == 2)
    #expect(unique.map(\.proto) == ["udp", "tcp"])
}

@Test func portLinksPreferLocalhostWhenBoundAndUseAliasOtherwise() throws {
    let json = """
    {"state": "running", "since_ms": 0,
     "daemon": {"pid": 1, "home": "/tmp/k"},
     "versions": {"katamaran": "0.8.0", "protocol": 3},
     "config": {"cpus": 4, "memory_mib": 4096, "mount": true, "launch_at_login": true},
     "docker": {"state": "ready", "socket": "/tmp/k/run/docker.sock"},
     "mount": {"enabled": true, "mounted": true},
     "ports": [
       {"proto": "tcp", "port": 8080, "addresses": ["172.16.1.200"]},
       {"proto": "tcp", "port": 8443, "addresses": ["172.16.1.200", "127.0.0.1"]},
       {"proto": "tcp", "port": 9000}
     ]}
    """
    let decoded = try JSONDecoder().decode(DaemonStatus.self, from: Data(json.utf8))
    // Alias-only port uses the bound alias.
    #expect(decoded.portHost(port: 8080) == "172.16.1.200")
    #expect(decoded.portURL(port: 8080)?.absoluteString == "http://172.16.1.200:8080")
    // Multi-address port prefers localhost whenever 127.0.0.1 is bound.
    #expect(decoded.portHost(port: 8443) == "localhost")
    #expect(decoded.portURL(port: 8443)?.absoluteString == "http://localhost:8443")
    // Port without addresses (older daemon) or not yet listed falls back to localhost.
    #expect(decoded.portHost(port: 9000) == "localhost")
    #expect(decoded.portURL(port: 9000)?.absoluteString == "http://localhost:9000")
    #expect(decoded.portHost(port: 9999) == "localhost")
}
