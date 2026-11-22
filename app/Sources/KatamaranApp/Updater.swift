import AppKit
import Sparkle

/// Automatic updates via Sparkle, driven by `SUFeedURL` and `SUPublicEDKey` in
/// Info.plist (injected only on release builds; local dev builds never update).
/// Release builds set `SUEnableAutomaticChecks = true`,
/// `SUScheduledCheckInterval = 86400` (daily), and
/// `SUAllowsAutomaticUpdates = false` so Sparkle always offers a dialog instead
/// of silently deferring installation until quit.
@MainActor
final class Updater: NSObject, NSMenuItemValidation {
    static let shared = Updater()

    private var controller: SPUStandardUpdaterController?
    private var observations: [NSKeyValueObservation] = []
    private var observers: [() -> Void] = []

    /// Version string of an update found by a scheduled check that has not yet
    /// received user attention.
    private(set) var pendingUpdateVersion: String?

    var isConfigured: Bool { controller != nil }

    var canCheckForUpdates: Bool {
        controller?.updater.canCheckForUpdates ?? false
    }

    var automaticallyChecksForUpdates: Bool {
        get { controller?.updater.automaticallyChecksForUpdates ?? false }
        set {
            controller?.updater.automaticallyChecksForUpdates = newValue
            changed()
        }
    }

    func observe(_ f: @escaping () -> Void) { observers.append(f) }
    private func changed() { observers.forEach { $0() } }

    func start() {
        guard Self.isConfigured(info: Bundle.main.infoDictionary) else { return }
        let ctrl = SPUStandardUpdaterController(
            startingUpdater: true,
            updaterDelegate: nil,
            userDriverDelegate: self
        )
        controller = ctrl
        observations = [
            ctrl.updater.observe(\.canCheckForUpdates, options: [.new]) { [weak self] _, _ in
                Task { @MainActor in self?.changed() }
            },
            ctrl.updater.observe(\.automaticallyChecksForUpdates, options: [.new]) { [weak self] _, _ in
                Task { @MainActor in self?.changed() }
            },
        ]
        changed()
    }

    @objc func checkForUpdates(_ sender: Any?) {
        NSApp.activate()
        controller?.checkForUpdates(sender)
    }

    nonisolated func validateMenuItem(_ item: NSMenuItem) -> Bool {
        if item.action == #selector(checkForUpdates(_:)) {
            return MainActor.assumeIsolated {
                item.title = Self.menuItemTitle(pendingVersion: pendingUpdateVersion)
                return canCheckForUpdates
            }
        }
        return true
    }

    static func isConfigured(info: [String: Any]?) -> Bool {
        guard let feed = (info?["SUFeedURL"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines),
              !feed.isEmpty,
              let key = (info?["SUPublicEDKey"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines),
              !key.isEmpty
        else { return false }
        return true
    }

    static func menuItemTitle(pendingVersion: String?) -> String {
        if let version = pendingVersion?.trimmingCharacters(in: .whitespacesAndNewlines), !version.isEmpty {
            return "Update Available (\(version))…"
        }
        return "Check for Updates…"
    }

    static func versionSubtitle(info: [String: Any]?, pendingVersion: String? = nil) -> String? {
        let current = FirstRun.appShortVersion(info: info)
        let pending = pendingVersion?.trimmingCharacters(in: .whitespacesAndNewlines)
        let nonEmptyPending = (pending?.isEmpty == false) ? pending : nil
        switch (current, nonEmptyPending) {
        case let (cur?, next?): return "Version \(cur) · \(next) available"
        case let (cur?, nil): return "Version \(cur)"
        case let (nil, next?): return "Version \(next) available"
        case (nil, nil): return nil
        }
    }
}

extension Updater: SPUStandardUserDriverDelegate {
    nonisolated var supportsGentleScheduledUpdateReminders: Bool { true }

    nonisolated func standardUserDriverShouldHandleShowingScheduledUpdate(
        _ update: SUAppcastItem,
        andInImmediateFocus immediateFocus: Bool
    ) -> Bool {
        true
    }

    nonisolated func standardUserDriverWillHandleShowingUpdate(
        _ handleShowingUpdate: Bool,
        forUpdate update: SUAppcastItem,
        state: SPUUserUpdateState
    ) {
        let version = update.displayVersionString
        let userInitiated = state.userInitiated
        MainActor.assumeIsolated {
            if !userInitiated {
                pendingUpdateVersion = version
                changed()
            }
            if handleShowingUpdate {
                NSApp.activate()
            }
        }
    }

    nonisolated func standardUserDriverWillShowModalAlert() {
        MainActor.assumeIsolated {
            NSApp.activate()
        }
    }

    nonisolated func standardUserDriverDidReceiveUserAttention(forUpdate update: SUAppcastItem) {
        MainActor.assumeIsolated {
            if pendingUpdateVersion != nil {
                pendingUpdateVersion = nil
                changed()
            }
        }
    }

    nonisolated func standardUserDriverWillFinishUpdateSession() {
        MainActor.assumeIsolated {
            if pendingUpdateVersion != nil {
                pendingUpdateVersion = nil
                changed()
            }
        }
    }
}
