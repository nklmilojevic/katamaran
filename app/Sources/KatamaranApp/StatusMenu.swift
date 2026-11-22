import AppKit

/// The menu-bar item and its short menu: the VM's state, start and stop, and
/// the way into the window. Containers live in the window.
@MainActor
final class StatusMenu: NSObject, NSMenuDelegate {
    let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
    let menu = NSMenu()
    private let model: AppModel
    /// What the menu was last built from.
    private var signature = ""
    var openWindow: () -> Void = {}
    var openSettings: () -> Void = {}

    init(model: AppModel) {
        self.model = model
        super.init()
        menu.delegate = self
        menu.autoenablesItems = false
        item.menu = menu
        item.button?.toolTip = "Katamaran"
        model.observe { [weak self] in self?.update() }
        update()
    }

    func menuWillOpen(_ menu: NSMenu) { model.refresh() }

    func update() {
        let phase = model.phase
        let img = NSImage(systemSymbolName: phase.symbol, accessibilityDescription: "Katamaran: \(phase.label)")
        img?.isTemplate = true
        item.button?.image = img
        item.button?.appearsDisabled = phase == .noDaemon

        let sig = [
            "\(phase)", "\(model.busy)", "\(model.restartRequired)", model.lastError ?? "", model.status?.error ?? "",
            "\(model.status?.vm != nil)", statusSummary(model.status) ?? "",
        ].joined(separator: "|")
        if sig != signature {
            signature = sig
            rebuild()
        }
    }

    private func rebuild() {
        menu.removeAllItems()
        let phase = model.phase

        let state = NSMenuItem(title: "Katamaran - \(phase.label)", action: nil, keyEquivalent: "")
        state.image = dotImage(phase.color, size: 9)
        state.subtitle = statusSummary(model.status) ?? "The daemon isn’t running"
        state.isEnabled = false
        menu.addItem(state)
        if let message = model.lastError ?? model.status?.error {
            let i = NSMenuItem(title: message.count > 90 ? String(message.prefix(90)) + "…" : message, action: nil, keyEquivalent: "")
            i.toolTip = message
            i.image = NSImage(systemSymbolName: "exclamationmark.triangle.fill", accessibilityDescription: nil)?
                .withSymbolConfiguration(.init(paletteColors: [.systemRed]))
            i.isEnabled = false
            menu.addItem(i)
        }
        menu.addItem(.separator())

        let vmUp = model.status?.vm != nil
        let idle = !model.busy && phase != .stopping
        if model.restartRequired && phase.isUp {
            let i = action("Restart to Apply Settings", #selector(restart), symbol: "arrow.clockwise.circle.fill")
            i.isEnabled = idle
            menu.addItem(i)
        }
        if vmUp {
            let stop = action("Stop", #selector(stopVM), symbol: "stop.fill")
            stop.isEnabled = idle
            menu.addItem(stop)
            let restart = action("Restart", #selector(restart), symbol: "arrow.clockwise")
            restart.isEnabled = idle
            menu.addItem(restart)
        } else {
            let start = action(model.busy ? "Starting…" : "Start", #selector(startVM), symbol: "play.fill")
            start.isEnabled = idle
            menu.addItem(start)
        }
        menu.addItem(.separator())
        menu.addItem(action("Open Katamaran", #selector(open), symbol: "macwindow", key: "o"))
        if Updater.shared.isConfigured {
            let check = NSMenuItem(
                title: "Check for Updates…",
                action: #selector(Updater.checkForUpdates(_:)),
                keyEquivalent: ""
            )
            check.target = Updater.shared
            check.image = NSImage(systemSymbolName: "arrow.down.circle", accessibilityDescription: nil)
            check.isEnabled = Updater.shared.canCheckForUpdates
            menu.addItem(check)
        }
        menu.addItem(action("Settings…", #selector(settings), symbol: "gearshape", key: ","))
        menu.addItem(.separator())
        menu.addItem(action("Quit Katamaran", #selector(quit), key: "q"))
    }

    private func action(_ title: String, _ sel: Selector, symbol: String? = nil, key: String = "") -> NSMenuItem {
        let i = NSMenuItem(title: title, action: sel, keyEquivalent: key)
        i.target = self
        if let symbol { i.image = NSImage(systemSymbolName: symbol, accessibilityDescription: nil) }
        return i
    }

    @objc private func startVM() { model.start() }
    @objc private func stopVM() { model.stop() }
    @objc private func restart() { model.restart() }
    @objc private func open() { openWindow() }
    @objc private func settings() { openSettings() }
    @objc private func quit() { NSApp.terminate(nil) }
}
