import AppKit
import Sparkle

/// Automatic updates via Sparkle, driven by `SUFeedURL` and `SUPublicEDKey` in
/// Info.plist (injected only on release builds; local dev builds never update).
@MainActor
final class Updater: NSObject, NSMenuItemValidation {
    static let shared = Updater()

    private var controller: SPUStandardUpdaterController?

    var isConfigured: Bool { controller != nil }

    var canCheckForUpdates: Bool {
        controller?.updater.canCheckForUpdates ?? false
    }

    func start() {
        let info = Bundle.main.infoDictionary ?? [:]
        guard let feed = info["SUFeedURL"] as? String, !feed.isEmpty,
              let key = info["SUPublicEDKey"] as? String, !key.isEmpty
        else {
            return
        }
        controller = SPUStandardUpdaterController(
            startingUpdater: true,
            updaterDelegate: nil,
            userDriverDelegate: nil
        )
    }

    @objc func checkForUpdates(_ sender: Any?) {
        controller?.checkForUpdates(sender)
    }

    nonisolated func validateMenuItem(_ item: NSMenuItem) -> Bool {
        if item.action == #selector(checkForUpdates(_:)) {
            return MainActor.assumeIsolated { canCheckForUpdates }
        }
        return true
    }
}
