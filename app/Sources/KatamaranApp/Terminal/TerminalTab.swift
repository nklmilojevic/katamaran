import AppKit

/// A shell in one container: the terminal, its exec session, and a bar for when it ended.
@MainActor
final class ContainerTerminal: NSView {
    enum State: Equatable {
        case connecting
        case running
        case ended(Int?)
        case failed(String)
    }

    let containerID: String
    let term = KatTerminal(cols: 80, rows: 24)
    let view: TerminalView
    private(set) var state = State.connecting
    private var attach: ExecAttach?
    private let socket: () -> String?
    private let banner = NSView()
    private let bannerLabel = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize))
    private let reconnect = NSButton(title: "Reconnect", target: nil, action: nil)
    /// Called when the shell is up (automation hooks).
    var onRunning: () -> Void = {}

    init(containerID: String, socket: @escaping () -> String?) {
        self.containerID = containerID
        self.socket = socket
        view = TerminalView(term: term)
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        view.translatesAutoresizingMaskIntoConstraints = false
        view.send = { [weak self] data in self?.attach?.send(data) }
        view.onResize = { [weak self] cols, rows in self?.attach?.resize(cols: cols, rows: rows) }
        addSubview(view)

        banner.translatesAutoresizingMaskIntoConstraints = false
        banner.wantsLayer = true
        banner.layer?.cornerRadius = 8
        banner.layer?.borderWidth = 1
        bannerLabel.textColor = .secondaryLabelColor
        reconnect.bezelStyle = .push
        reconnect.controlSize = .small
        reconnect.target = self
        reconnect.action = #selector(reconnectClicked)
        reconnect.keyEquivalent = "\r"
        let row = NSStackView(views: [bannerLabel, reconnect])
        row.spacing = 10
        row.translatesAutoresizingMaskIntoConstraints = false
        banner.addSubview(row)
        addSubview(banner)
        NSLayoutConstraint.activate([
            view.topAnchor.constraint(equalTo: topAnchor),
            view.leadingAnchor.constraint(equalTo: leadingAnchor),
            view.trailingAnchor.constraint(equalTo: trailingAnchor),
            view.bottomAnchor.constraint(equalTo: bottomAnchor),
            row.topAnchor.constraint(equalTo: banner.topAnchor, constant: 6),
            row.bottomAnchor.constraint(equalTo: banner.bottomAnchor, constant: -6),
            row.leadingAnchor.constraint(equalTo: banner.leadingAnchor, constant: 12),
            row.trailingAnchor.constraint(equalTo: banner.trailingAnchor, constant: -8),
            banner.centerXAnchor.constraint(equalTo: centerXAnchor),
            banner.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -14),
            banner.leadingAnchor.constraint(greaterThanOrEqualTo: leadingAnchor, constant: 14),
        ])
        banner.isHidden = true
    }

    required init?(coder: NSCoder) { fatalError() }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        styleBanner()
    }

    private func styleBanner() {
        effectiveAppearance.performAsCurrentDrawingAppearance {
            banner.layer?.backgroundColor = NSColor.windowBackgroundColor.cgColor
            banner.layer?.borderColor = NSColor.separatorColor.cgColor
        }
    }

    /// Starts (or restarts) the shell.
    func start() {
        attach?.close()
        attach = nil
        guard let socket = socket() else {
            setState(.failed("Docker is not running."))
            return
        }
        if state != .connecting {
            // Leave whatever full-screen program was running, and start below the old output.
            term.feed(Data("\u{1B}[?1049l\u{1B}[!p\r\n".utf8))
        }
        setState(.connecting)
        let grid = view.fittingGrid()
        let a = ExecAttach(socketPath: socket, container: containerID, cols: grid.cols, rows: grid.rows)
        let term = self.term
        let signal = view.outputSignal
        a.onOutput = { [weak a] data in
            term.feed(data)
            let replies = term.takeReplies()
            if !replies.isEmpty { a?.send(replies) }
            signal()
        }
        a.onStart = { [weak self, weak a] in
            guard let self, let a, self.attach === a else { return }
            self.setState(.running)
            self.onRunning()
        }
        a.onEnd = { [weak self, weak a] end in
            guard let self, let a, self.attach === a else { return }
            switch end {
            case .exited(let code): self.setState(.ended(code))
            case .failed(let message): self.setState(.failed(message))
            }
        }
        attach = a
        a.start()
    }

    /// Hangs up (the window closed, or the app quits: `wait` / `group`).
    func close(wait: Bool = false, group: DispatchGroup? = nil) {
        attach?.close(wait: wait, group: group)
        attach = nil
    }

    private func setState(_ s: State) {
        state = s
        view.inputEnabled = s == .running || s == .connecting
        switch s {
        case .connecting, .running:
            banner.isHidden = true
        case .ended(let code):
            bannerLabel.stringValue = code.map { "Session ended (exit code \($0))." } ?? "Session ended."
            banner.isHidden = false
        case .failed(let message):
            bannerLabel.stringValue = "Could not open a shell: \(message)"
            banner.isHidden = false
        }
        styleBanner()
        view.renderNow(force: true)
    }

    @objc private func reconnectClicked() {
        start()
        window?.makeFirstResponder(view)
    }
}

/// The Terminal tab: a shell in the selected container, embedded. Each container keeps its
/// session while the window is open (switching containers or tabs leaves it running).
@MainActor
final class EmbeddedTerminalTabView: NSView, ContainerTabView {
    private let store: DockerStore
    private let empty = EmptyStateView()
    private let bar = NSView()
    private let title = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let openExternal = NSButton(title: "Open in Terminal App", target: nil, action: nil)
    private let host = NSView()
    private var terminals: [String: ContainerTerminal] = [:]
    private var shown: ContainerTerminal?
    private var container: ContainerSummary?
    private var active = false
    private weak var observedWindow: NSWindow?

    init(store: DockerStore) {
        self.store = store
        super.init(frame: .zero)
        openExternal.bezelStyle = .accessoryBarAction
        openExternal.controlSize = .small
        openExternal.image = NSImage(systemSymbolName: "arrow.up.forward.app", accessibilityDescription: nil)
        openExternal.imagePosition = .imageLeading
        openExternal.target = self
        openExternal.action = #selector(openInTerminalApp)
        openExternal.toolTip = "Open a shell in this container in your terminal app"
        title.lineBreakMode = .byTruncatingMiddle
        for v in [bar, host, title, openExternal] as [NSView] { v.translatesAutoresizingMaskIntoConstraints = false }
        bar.addSubview(title)
        bar.addSubview(openExternal)
        let line = NSBox()
        line.boxType = .separator
        line.translatesAutoresizingMaskIntoConstraints = false
        for v in [bar, line, host] { addSubview(v) }
        addSubview(empty)
        pin(empty, to: self)
        NSLayoutConstraint.activate([
            bar.topAnchor.constraint(equalTo: topAnchor),
            bar.leadingAnchor.constraint(equalTo: leadingAnchor),
            bar.trailingAnchor.constraint(equalTo: trailingAnchor),
            bar.heightAnchor.constraint(equalToConstant: 30),
            title.leadingAnchor.constraint(equalTo: bar.leadingAnchor, constant: 12),
            title.centerYAnchor.constraint(equalTo: bar.centerYAnchor),
            title.trailingAnchor.constraint(lessThanOrEqualTo: openExternal.leadingAnchor, constant: -8),
            openExternal.trailingAnchor.constraint(equalTo: bar.trailingAnchor, constant: -8),
            openExternal.centerYAnchor.constraint(equalTo: bar.centerYAnchor),
            line.topAnchor.constraint(equalTo: bar.bottomAnchor),
            line.leadingAnchor.constraint(equalTo: leadingAnchor),
            line.trailingAnchor.constraint(equalTo: trailingAnchor),
            // Without a height the separator could stand up as a tall vertical
            // line, and its hugging would squeeze the whole detail column.
            line.heightAnchor.constraint(equalToConstant: 1),
            host.topAnchor.constraint(equalTo: line.bottomAnchor),
            host.leadingAnchor.constraint(equalTo: leadingAnchor),
            host.trailingAnchor.constraint(equalTo: trailingAnchor),
            host.bottomAnchor.constraint(equalTo: bottomAnchor),
        ])
        empty.onAction = { [weak self] in self?.openInTerminalApp() }
        store.observe { [weak self] s in self?.pruneTerminals(s) }
        NotificationCenter.default.addObserver(self, selector: #selector(appWillTerminate),
                                               name: NSApplication.willTerminateNotification, object: nil)
    }

    @objc private func appWillTerminate() {
        let active = Array(terminals.values)
        terminals.removeAll()
        shown = nil
        guard !active.isEmpty else { return }
        let group = DispatchGroup()
        for t in active { t.close(group: group) }
        _ = group.wait(timeout: .now() + 1.5)
    }

    private func pruneTerminals(_ s: DockerStore) {
        if s.socket != nil && !s.loaded.contains(.containers) { return }
        let keep: Set<String> = s.socket == nil ? [] : Set(s.containers.map(\.Id))
        let stale = terminals.keys.filter { !keep.contains($0) }
        guard !stale.isEmpty else { return }
        for id in stale {
            guard let t = terminals.removeValue(forKey: id) else { continue }
            t.close()
            t.removeFromSuperview()
            if shown === t { shown = nil }
        }
        if let c = container, !keep.contains(c.Id) {
            container = nil
            update(focus: false)
        } else if shown == nil {
            update(focus: false)
        }
    }

    required init?(coder: NSCoder) { fatalError() }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        let nc = NotificationCenter.default
        if let old = observedWindow { nc.removeObserver(self, name: NSWindow.willCloseNotification, object: old) }
        observedWindow = window
        if let window {
            nc.addObserver(self, selector: #selector(windowWillClose), name: NSWindow.willCloseNotification, object: window)
        }
    }

    /// Sessions live as long as the window: closing it hangs them up.
    @objc private func windowWillClose() {
        for t in terminals.values {
            t.close()
            t.removeFromSuperview()
        }
        terminals.removeAll()
        shown = nil
        update(focus: false)
    }

    func setActive(_ a: Bool) {
        let became = a && !active
        active = a
        update(focus: became)
    }

    func show(_ c: ContainerSummary?, _ inspect: ContainerInspect?) {
        let changed = c?.Id != container?.Id
        container = c
        update(focus: changed && active)
    }

    private func update(focus: Bool) {
        guard let c = container else {
            showTerminal(nil)
            empty.show(symbol: nil, title: "No Container")
            return
        }
        var t = terminals[c.Id]
        if t == nil, c.running, active, window != nil {
            let new = ContainerTerminal(containerID: c.Id) { [store] in store.socket }
            new.onRunning = { [weak self, weak new] in if let new { self?.automate(new) } }
            terminals[c.Id] = new
            t = new
            host.addSubview(new)
            pin(new, to: host)
            layoutSubtreeIfNeeded()
            new.start()
        }
        showTerminal(t)
        title.stringValue = "Shell in \(c.name)"
        if t == nil {
            if c.running {
                empty.show(symbol: "apple.terminal", title: "Shell in \(c.name)", busy: true)
            } else {
                empty.show(symbol: "apple.terminal", title: "Not Running",
                           message: "Start the container to open a shell in it.")
            }
        }
        if focus, let t, let window {
            window.makeFirstResponder(t.view)
        }
    }

    private func showTerminal(_ t: ContainerTerminal?) {
        if shown !== t {
            shown?.isHidden = true
            shown = t
            t?.isHidden = false
        }
        let on = t != nil
        empty.isHidden = on
        bar.isHidden = !on
        host.isHidden = !on
        for v in subviews where v is NSBox { v.isHidden = !on }
        openExternal.isEnabled = container?.running == true
    }

    @objc private func openInTerminalApp() {
        guard let c = container, let socket = store.socket else { return }
        do {
            try ExecCommand.open(container: c.name, socket: socket)
        } catch {
            let alert = NSAlert(error: error)
            if let w = window { alert.beginSheetModal(for: w) } else { alert.runModal() }
        }
    }

    // MARK: Automation

    /// `--terminal-input <text>` types into the first shell once it runs (`\r`, `\n`, `\t`
    /// and `\e` escapes are understood); `--terminal-latency <n>` types n keys and prints how
    /// long each took to reach the screen.
    private func automate(_ t: ContainerTerminal) {
        let args = CommandLine.arguments
        func option(_ name: String) -> String? {
            args.firstIndex(of: name).flatMap { args.indices.contains($0 + 1) ? args[$0 + 1] : nil }
        }
        if let text = option("--terminal-input") {
            let unescaped = text.replacingOccurrences(of: "\\r", with: "\r")
                .replacingOccurrences(of: "\\n", with: "\n")
                .replacingOccurrences(of: "\\t", with: "\t")
                .replacingOccurrences(of: "\\e", with: "\u{1B}")
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.8) { [weak t] in
                t?.view.send(Data(unescaped.utf8))
            }
        }
        if let n = option("--terminal-latency").flatMap(Int.init), n > 0 {
            LatencyProbe(view: t.view, count: n).run(after: 1.5)
        }
    }
}

/// Types keys through the view's own key handling and reports keystroke-to-screen times.
@MainActor
final class LatencyProbe {
    private let view: TerminalView
    private let count: Int
    private var samples: [Double] = []
    private var sent = 0

    init(view: TerminalView, count: Int) {
        self.view = view
        self.count = count
    }

    func run(after delay: TimeInterval) {
        view.onLatency = { [self] ms in samples.append(ms) }
        DispatchQueue.main.asyncAfter(deadline: .now() + delay) { [self] in next() }
    }

    private func next() {
        guard sent < count else {
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) { [self] in report() }
            return
        }
        sent += 1
        let letters = Array("abcdefghijklmnopqrstuvwxyz")
        let ch = String(letters[(sent - 1) % letters.count])
        if let ev = NSEvent.keyEvent(with: .keyDown, location: .zero, modifierFlags: [], timestamp: ProcessInfo.processInfo.systemUptime,
                                     windowNumber: view.window?.windowNumber ?? 0, context: nil, characters: ch,
                                     charactersIgnoringModifiers: ch, isARepeat: false, keyCode: 0) {
            view.keyDown(with: ev)
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.12) { [self] in next() }
    }

    private func report() {
        view.onLatency = nil
        // Clear the typed line.
        view.send(Data("\u{15}".utf8))
        let s = samples.sorted()
        guard !s.isEmpty else {
            print("katamaran-app: terminal latency: no samples")
            fflush(stdout)
            return
        }
        func pct(_ p: Double) -> Double { s[min(s.count - 1, Int(Double(s.count - 1) * p))] }
        print(String(format: "katamaran-app: terminal latency over %d keys: min %.1f ms, median %.1f ms, p95 %.1f ms, max %.1f ms",
                     s.count, s[0], pct(0.5), pct(0.95), s[s.count - 1]))
        fflush(stdout)
    }
}
