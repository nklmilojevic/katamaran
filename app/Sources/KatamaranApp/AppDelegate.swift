import AppKit

/// Owns the status menu and the windows. The app is a menu-bar agent; while
/// the main window is open it also has a Dock icon and a menu bar.
///
/// Headless modes for automation:
/// - `--smoke-test`: exit 0 once the daemon answers `status` (1 after 20 s);
/// - `--snapshot menu|settings|main <png>`: open that UI, capture it with
///   `screencapture -l <window>` and exit. `--appearance light|dark` overrides
///   the system's; for settings `--pane general|resources` picks the pane; for
///   main `--section <name>`, `--select <container or project>` and
///   `--tab info|stats|logs|terminal|files` pick what it shows.
/// - `--steps "section:images,docker:up,list:420,shot:a.png,docker:down,shot:b.png"`
///   with `--snapshot main <png>`: fake the daemon (Docker served from the
///   `--fake-docker <socket>`), run the steps a second apart and capture the
///   window at each `shot:`. Docker states: up, waiting, starting, stopping,
///   stopped, none. Other steps: select:<name>, list:<width>, sidebar, size:<w>x<h>.
/// - `--background`: don't open the main window at launch (as at login).
@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    private let model = AppModel.shared
    private let store = DockerStore.shared
    private var statusMenu: StatusMenu!
    private lazy var settings = SettingsWindowController(model: model)
    private lazy var main: MainWindowController = {
        let m = MainWindowController(model: model, store: store)
        m.onClose = { [weak self] in self?.mainClosed() }
        return m
    }()
    private var mainCreated = false
    private let args = CommandLine.arguments

    func applicationDidFinishLaunching(_ notification: Notification) {
        let headless = args.contains("--smoke-test") || args.contains("--snapshot")
        if !headless {
            Updater.shared.start()
        }
        NSApp.mainMenu = mainMenu()
        statusMenu = StatusMenu(model: model)
        statusMenu.openWindow = { [weak self] in self?.showMain() }
        statusMenu.openSettings = { [weak self] in self?.settings.present() }
        model.observe { [weak self] in
            self?.store.follow(self?.model.status)
            K8sStore.shared.follow(self?.model.status)
        }
        store.onError = { [weak self] what, why in self?.showError(what, why) }
        K8sStore.shared.onError = { [weak self] what, why in self?.showError(what, why) }

        let steps = option("--steps").map { $0.split(separator: ",").map(String.init) } ?? []
        if !steps.isEmpty { model.fake(fakeStatus("stopped")) }
        if headless {
            DispatchQueue.main.asyncAfter(deadline: .now() + 25 + Double(steps.count) * 1.2) {
                FileHandle.standardError.write(Data("katamaran-app: timed out\n".utf8))
                exit(1)
            }
        }
        if args.contains("--smoke-test") { smokeTest() }
        if let i = args.firstIndex(of: "--snapshot"), i + 2 < args.count {
            let (what, path) = (args[i + 1], args[i + 2])
            model.onFirstStatus = { [weak self] _ in
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) { self?.snapshot(what, to: path) }
            }
        }
        if !headless {
            runFirstSetupIfNeeded()
        }
        if !headless && !args.contains("--background") && !launchedAtLogin(notification) { showMain() }
    }

    private static let setupKey = "firstRunSetupDone"

    /// The first launch of an installed app runs `katamaran setup --default` once:
    /// the LaunchAgent that keeps the daemon running, the `katamaran` Docker context
    /// (set as the default) and the `kat` link. Subsequent launches make sure the
    /// `katamaran` context is active. Development builds under target/ are left alone.
    private func runFirstSetupIfNeeded() {
        guard FirstRun.shouldRun(bundlePath: Bundle.main.bundlePath,
                                 env: ProcessInfo.processInfo.environment) else { return }
        if UserDefaults.standard.bool(forKey: Self.setupKey) {
            DispatchQueue.global().async {
                _ = try? AppModel.runCLI(["use-context"])
            }
            return
        }
        DispatchQueue.global().async { [weak self] in
            let result = Result { try AppModel.runCLI(["setup", "--default"]) }
            DispatchQueue.main.async {
                guard let self else { return }
                switch result {
                case .success:
                    UserDefaults.standard.set(true, forKey: Self.setupKey)
                    self.model.refresh()
                case .failure(let e):
                    self.showError("Katamaran couldn’t finish setting up", e.localizedDescription)
                }
            }
        }
    }

    /// Opening the app again from the Dock or Finder brings the window back.
    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        showMain()
        return false
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { false }

    private func launchedAtLogin(_ n: Notification) -> Bool {
        let event = NSAppleEventManager.shared().currentAppleEvent
        return event?.eventID == kAEOpenApplication
            && event?.paramDescriptor(forKeyword: keyAEPropData)?.enumCodeValue == keyAELaunchedAsLogInItem
    }

    private func showMain(section: MainSection? = nil) {
        mainCreated = true
        if NSApp.activationPolicy() != .regular { NSApp.setActivationPolicy(.regular) }
        main.present(section: section)
    }

    /// Back to a menu-bar agent: no Dock icon without the window.
    private func mainClosed() {
        DispatchQueue.main.async {
            if NSApp.activationPolicy() != .accessory { NSApp.setActivationPolicy(.accessory) }
        }
    }

    private func showError(_ what: String, _ why: String) {
        let alert = NSAlert()
        alert.messageText = what
        alert.informativeText = why
        alert.alertStyle = .warning
        if mainCreated, let w = main.window, w.isVisible { alert.beginSheetModal(for: w) } else { alert.runModal() }
    }

    // MARK: Menus

    private func mainMenu() -> NSMenu {
        let main = NSMenu()
        func submenu(_ title: String, _ items: [NSMenuItem]) -> NSMenu {
            let item = NSMenuItem()
            let menu = NSMenu(title: title)
            items.forEach(menu.addItem)
            item.submenu = menu
            main.addItem(item)
            return menu
        }
        func item(_ title: String, _ sel: Selector?, _ key: String = "", mods: NSEvent.ModifierFlags = .command,
                  target: AnyObject? = nil) -> NSMenuItem {
            let i = NSMenuItem(title: title, action: sel, keyEquivalent: key)
            i.keyEquivalentModifierMask = mods
            i.target = target
            return i
        }

        _ = submenu("Katamaran", [
            item("About Katamaran", #selector(NSApplication.orderFrontStandardAboutPanel(_:))),
            item("Check for Updates…", #selector(Updater.checkForUpdates(_:)), target: Updater.shared),
            .separator(),
            item("Settings…", #selector(openSettings), ",", target: self),
            .separator(),
            item("Hide Katamaran", #selector(NSApplication.hide(_:)), "h"),
            item("Hide Others", #selector(NSApplication.hideOtherApplications(_:)), "h", mods: [.command, .option]),
            item("Show All", #selector(NSApplication.unhideAllApplications(_:))),
            .separator(),
            item("Quit Katamaran", #selector(NSApplication.terminate(_:)), "q"),
        ])
        _ = submenu("Edit", [
            item("Undo", Selector(("undo:")), "z"),
            item("Redo", Selector(("redo:")), "z", mods: [.command, .shift]),
            .separator(),
            item("Cut", #selector(NSText.cut(_:)), "x"),
            item("Copy", #selector(NSText.copy(_:)), "c"),
            item("Paste", #selector(NSText.paste(_:)), "v"),
            item("Select All", #selector(NSText.selectAll(_:)), "a"),
            .separator(),
            {
                let i = item("Find…", #selector(NSTextView.performFindPanelAction(_:)), "f")
                i.tag = Int(NSFindPanelAction.showFindPanel.rawValue)
                return i
            }(),
        ])
        var view: [NSMenuItem] = MainSection.allCases.enumerated().map { i, s in
            let m = item(s.title, #selector(showSection(_:)), i < 9 ? "\(i + 1)" : "", target: self)
            m.tag = i
            return m
        }
        view += [.separator(), item("Toggle Sidebar", #selector(NSSplitViewController.toggleSidebar(_:)), "s",
                                    mods: [.command, .control])]
        _ = submenu("View", view)
        let window = submenu("Window", [
            item("Minimize", #selector(NSWindow.performMiniaturize(_:)), "m"),
            item("Zoom", #selector(NSWindow.performZoom(_:))),
            item("Close", #selector(NSWindow.performClose(_:)), "w"),
            .separator(),
            item("Katamaran", #selector(showMainWindow), "0", target: self),
        ])
        NSApp.windowsMenu = window
        return main
    }

    @objc private func openSettings() { settings.present() }
    @objc private func showMainWindow() { showMain() }
    @objc private func showSection(_ sender: NSMenuItem) { showMain(section: MainSection.allCases[safe: sender.tag]) }

    // MARK: Automation

    private func smokeTest() {
        let symbols = [Phase.running, .starting, .error, .stopped].map(\.symbol)
        let missing = symbols.filter { NSImage(systemSymbolName: $0, accessibilityDescription: nil) == nil }
        model.onFirstStatus = { [weak self] status in
            guard let self else { return }
            let shown = self.statusMenu.item.button?.image != nil
            let containerItems = self.statusMenu.menu.items.filter { $0.title.localizedCaseInsensitiveContains("container") }
            // Build the window without showing it: every section must load.
            let window = self.main
            var sections: [String] = []
            for s in MainSection.allCases {
                window.show(s, remember: false)
                if window.currentPane?.list.view.superview == nil { sections.append(s.rawValue) }
            }
            print("katamaran-app: connected to daemon pid \(status.daemon.pid), state \(status.state)")
            if !missing.isEmpty { print("katamaran-app: missing symbols \(missing)") }
            if !shown { print("katamaran-app: status item has no image") }
            if !containerItems.isEmpty { print("katamaran-app: the menu lists containers") }
            let docker = status.docker.state == "ready"
            // Without Docker the Docker sections show its state instead of their list.
            let broken = sections.filter { s in docker || s == MainSection.logs.rawValue }
            if !broken.isEmpty { print("katamaran-app: sections not shown \(broken)") }
            fflush(stdout)
            exit(missing.isEmpty && shown && containerItems.isEmpty && broken.isEmpty ? 0 : 2)
        }
    }

    private func option(_ name: String) -> String? {
        args.firstIndex(of: name).flatMap { args.indices.contains($0 + 1) ? args[$0 + 1] : nil }
    }

    private func snapshot(_ what: String, to path: String) {
        let appearance: NSAppearance? = switch option("--appearance") {
        case "light": NSAppearance(named: .aqua)
        case "dark": NSAppearance(named: .darkAqua)
        default: nil
        }
        if let appearance { NSApp.appearance = appearance }
        switch what {
        case "settings":
            settings.present(pane: option("--pane").flatMap(SettingsPane.init(rawValue:)))
            capture(after: 1.5, window: settings.window, to: path)
        case "main", "logs":
            let section = what == "logs" ? .logs : option("--section").flatMap(MainSection.init(rawValue:)) ?? .containers
            showMain(section: section)
            main.window?.setContentSize(NSSize(width: 1180, height: 740))
            main.window?.center()
            if let steps = option("--steps") {
                run(steps: steps.split(separator: ",").map(String.init), then: path)
                return
            }
            // Wait for Docker's lists, then pick what to show.
            var tries = 0
            let t = Timer(timeInterval: 0.25, repeats: true) { [weak self] timer in
                MainActor.assumeIsolated {
                    guard let self else { return }
                    tries += 1
                    let ready = !section.needsDockerData || self.store.loaded.contains(.containers) || tries > 24
                    guard ready else { return }
                    timer.invalidate()
                    if let name = self.option("--select") { self.main.select(name: name) }
                    if section == .containers, let tab = self.option("--tab").flatMap({ t in ContainerTab.allCases.first { $0.title.lowercased() == t } }) {
                        self.main.select(tab: tab)
                    } else if let t = self.option("--tab"), let pane = self.main.currentPane,
                              let i = pane.tabs.firstIndex(where: { $0.lowercased() == t }) {
                        pane.selectedTab = i
                    }
                    let delay = Double(self.option("--delay") ?? "") ?? 2.5
                    self.capture(after: delay, window: self.main.window, to: path)
                }
            }
            RunLoop.main.add(t, forMode: .common)
        default:
            // Menu tracking blocks the main thread; a .common-mode timer
            // still fires inside it. Poll until the menu window is on screen.
            var tries = 0
            let t = Timer(timeInterval: 0.5, repeats: true) { [weak self] timer in
                MainActor.assumeIsolated {
                    tries += 1
                    guard let id = AppDelegate.menuWindowID() else {
                        if tries < 12 { return }
                        print("katamaran-app: no menu window to capture")
                        exit(3)
                    }
                    guard tries >= 3 else { return }
                    timer.invalidate()
                    let ok = AppDelegate.screencapture(window: id, to: path)
                    print(ok ? "snapshot \(path)" : "katamaran-app: capture failed")
                    fflush(stdout)
                    self?.statusMenu.menu.cancelTracking()
                    exit(ok ? 0 : 3)
                }
            }
            RunLoop.main.add(t, forMode: .common)
            statusMenu.item.button?.performClick(nil)
        }
    }

    /// The `--steps` driver; see the top of the file.
    private func run(steps: [String], then path: String) {
        guard let step = steps.first else {
            capture(after: 1.0, window: main.window, to: path)
            return
        }
        let (verb, arg) = step.firstIndex(of: ":").map { (String(step[..<$0]), String(step[step.index(after: $0)...])) } ?? (step, "")
        switch verb {
        case "section": if let s = MainSection(rawValue: arg) { main.present(section: s) }
        case "docker": model.fake(fakeStatus(arg))
        case "select": main.select(name: arg)
        case "list": main.setListWidth(CGFloat(Double(arg) ?? 380))
        case "sidebar": main.toggleSidebar()
        case "size":
            let wh = arg.split(separator: "x").compactMap { Double($0) }
            if wh.count == 2 { main.window?.setContentSize(NSSize(width: wh[0], height: wh[1])) }
        case "shot":
            let ok = main.window.map { AppDelegate.screencapture(window: CGWindowID($0.windowNumber), to: arg) } ?? false
            print(ok ? "snapshot \(arg)" : "katamaran-app: capture failed \(arg)")
            fflush(stdout)
        default: print("katamaran-app: unknown step \(step)")
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + (verb == "shot" ? 0.1 : 1.2)) { [weak self] in
            self?.run(steps: Array(steps.dropFirst()), then: path)
        }
    }

    private func fakeStatus(_ docker: String) -> DaemonStatus? {
        if docker == "none" { return nil }
        let state = switch docker {
        case "up", "waiting": "running"
        case "starting", "stopping": docker
        default: "stopped"
        }
        let ready = docker == "up"
        let vm = state == "stopped" ? "null" : #"{"cpus":4,"memory_mib":8192}"#
        let json = """
        {"state":"\(state)","since_ms":0,"uptime_s":\(state == "running" ? "3600" : "null"),
         "daemon":{"pid":1,"home":"/fake"},"versions":{"katamaran":"0.0.0","protocol":1},
         "config":{"cpus":4,"memory_mib":8192,"mount":false,"launch_at_login":false},"vm":\(vm),
         "docker":{"state":"\(ready ? "ready" : "starting")","socket":"\(option("--fake-docker") ?? "/nonexistent")"},
         "mount":{"enabled":false,"mounted":false},"ports":[]}
        """
        return try? JSONDecoder().decode(DaemonStatus.self, from: Data(json.utf8))
    }

    private func capture(after delay: Double, window: NSWindow?, to path: String) {
        DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
            let ok = window.map { AppDelegate.screencapture(window: CGWindowID($0.windowNumber), to: path) } ?? false
            print(ok ? "snapshot \(path)" : "katamaran-app: capture failed")
            fflush(stdout)
            exit(ok ? 0 : 3)
        }
    }

    /// The largest on-screen window of this process above the normal level: the open menu.
    private static func menuWindowID() -> CGWindowID? {
        let list = CGWindowListCopyWindowInfo([.optionOnScreenOnly], kCGNullWindowID) as? [[String: Any]] ?? []
        let pid = Int(getpid())
        return list
            .filter { ($0[kCGWindowOwnerPID as String] as? Int) == pid && ($0[kCGWindowLayer as String] as? Int ?? 0) > 0 }
            .max { area($0) < area($1) }
            .flatMap { $0[kCGWindowNumber as String] as? CGWindowID }
    }

    private static func area(_ w: [String: Any]) -> CGFloat {
        guard let b = w[kCGWindowBounds as String] as? [String: CGFloat] else { return 0 }
        return (b["Width"] ?? 0) * (b["Height"] ?? 0)
    }

    private static func screencapture(window id: CGWindowID, to path: String) -> Bool {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: "/usr/sbin/screencapture")
        p.arguments = ["-x", "-o", "-l\(id)", path]
        guard (try? p.run()) != nil else { return false }
        p.waitUntilExit()
        return p.terminationStatus == 0 && FileManager.default.fileExists(atPath: path)
    }
}

extension MainSection {
    var needsDockerData: Bool { self != .logs }
}

enum FirstRun {
    /// Only installed copies set up launchd and Docker; a build in a source
    /// tree (or one pointed at another home) would replace the real ones.
    static func shouldRun(bundlePath: String, env: [String: String]) -> Bool {
        if env["KATAMARAN_HOME"].map({ !$0.isEmpty }) == true { return false }
        return !bundlePath.contains("/target/") && !bundlePath.contains("/.build/")
    }
}
