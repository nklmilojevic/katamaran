import AppKit
import CryptoKit

/// A machine as the daemon's `machine.list` reports it
/// (`crates/kat-daemon/src/machines.rs`).
struct MachineInfo: Decodable, Equatable {
    let name: String
    let distro: String
    let release: String
    let state: String
    let ip: String?
    let user: String
    let `default`: Bool
}

/// Linux machines: name, distro, state, with Start/Stop and a shell in
/// Terminal. Polls the daemon while shown.
@MainActor
final class MachinesPane: SectionPane {
    private let vc: MachinesViewController
    var onChange: () -> Void = {}

    init(model: AppModel) {
        vc = MachinesViewController(model: model)
        vc.onSubtitle = { [weak self] in self?.onChange() }
    }

    var list: NSViewController { vc }
    var detail: NSViewController? { nil }
    var needsDocker: Bool { false }
    var subtitle: String { vc.subtitle }
    func setActive(_ active: Bool) { vc.setActive(active) }
}

@MainActor
final class MachinesViewController: NSViewController {
    private let model: AppModel
    private let stack = NSStackView()
    private let empty = NSTextField(wrappingLabelWithString: "")
    private var machines: [MachineInfo] = []
    private var timer: Timer?
    private var busy: Set<String> = []
    /// Machines being created: name → distro, shown as rows with a spinner.
    private var creating: [String: String] = [:]
    private lazy var newButton: NSButton = {
        let b = NSButton(title: "New Machine", image: NSImage(systemSymbolName: "plus", accessibilityDescription: nil)!,
                         target: self, action: #selector(newMachine))
        b.imagePosition = .imageLeading
        b.bezelStyle = .push
        b.controlSize = .large
        b.keyEquivalent = "n"
        b.keyEquivalentModifierMask = .command
        return b
    }()
    private(set) var subtitle = ""
    var onSubtitle: () -> Void = {}

    init(model: AppModel) {
        self.model = model
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 8
        stack.edgeInsets = NSEdgeInsets(top: 16, left: 16, bottom: 16, right: 16)
        empty.textColor = .secondaryLabelColor
        let scroll = NSScrollView()
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        let doc = FlippedView()
        doc.translatesAutoresizingMaskIntoConstraints = false
        stack.translatesAutoresizingMaskIntoConstraints = false
        doc.addSubview(stack)
        scroll.documentView = doc
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: doc.leadingAnchor),
            stack.trailingAnchor.constraint(equalTo: doc.trailingAnchor),
            stack.topAnchor.constraint(equalTo: doc.topAnchor),
            stack.bottomAnchor.constraint(equalTo: doc.bottomAnchor),
            doc.widthAnchor.constraint(equalTo: scroll.contentView.widthAnchor),
        ])
        view = scroll
        render()
    }

    func setActive(_ active: Bool) {
        timer?.invalidate()
        timer = nil
        guard active else { return }
        refresh()
        timer = Timer.scheduledTimer(withTimeInterval: 3, repeats: true) { [weak self] _ in
            MainActor.assumeIsolated { self?.refresh() }
        }
    }

    private func refresh() {
        let d = model.daemon
        DispatchQueue.global().async { [weak self] in
            let list: [MachineInfo]? = try? d.decode("machine.list", timeout: 10)
            DispatchQueue.main.async {
                guard let self else { return }
                let next = list ?? []
                let text = list == nil ? "Machines need the VM running"
                    : "\(next.count) machine\(next.count == 1 ? "" : "s")"
                guard next != self.machines || text != self.subtitle else { return }
                self.machines = next
                let changed = text != self.subtitle
                self.subtitle = text
                self.render()
                if changed { self.onSubtitle() }
            }
        }
    }

    private func render() {
        stack.arrangedSubviews.forEach { $0.removeFromSuperview() }
        stack.addArrangedSubview(newButton)
        stack.setCustomSpacing(16, after: newButton)
        let pending = creating.keys.sorted().filter { n in !machines.contains { $0.name == n } }
        if machines.isEmpty && pending.isEmpty {
            empty.stringValue = "No Linux machines yet. A machine is a full Linux system with your Mac user, sudo and /Users at the same path."
            stack.addArrangedSubview(empty)
            return
        }
        for m in machines { stack.addArrangedSubview(row(m)) }
        for n in pending { stack.addArrangedSubview(creatingRow(n, creating[n] ?? "")) }
    }

    private func creatingRow(_ name: String, _ distro: String) -> NSView {
        let spin = NSProgressIndicator()
        spin.style = .spinning
        spin.controlSize = .small
        spin.startAnimation(nil)
        let title = NSTextField(labelWithString: name)
        title.font = .systemFont(ofSize: 13, weight: .semibold)
        let sub = NSTextField(labelWithString: "Creating \(distro)… downloading and booting")
        sub.textColor = .secondaryLabelColor
        sub.font = .systemFont(ofSize: 11)
        let text = NSStackView(views: [title, sub])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 2
        let h = NSStackView(views: [spin, text])
        h.spacing = 10
        return h
    }

    @objc private func newMachine() {
        guard let window = view.window else { return }
        let sheet = NewMachineSheet(taken: Set(machines.map(\.name)).union(creating.keys))
        sheet.onCreate = { [weak self] name, distro, release in self?.create(name, distro, release) }
        window.beginSheet(sheet.window)
        sheet.retainUntilClosed()
    }

    private func create(_ name: String, _ distro: String, _ release: String?) {
        creating[name] = distro
        render()
        let d = model.daemon
        var params: [String: Any] = ["name": name, "distro": distro]
        if let release { params["release"] = release }
        let p = params
        DispatchQueue.global().async { [weak self] in
            let err: Error? = Result { try d.call("machine.create", p, timeout: 600) }.failure
            DispatchQueue.main.async {
                guard let self else { return }
                self.creating[name] = nil
                if let err {
                    let a = NSAlert(error: err)
                    a.messageText = "Couldn’t create \(name)"
                    a.informativeText = err.localizedDescription
                    a.runModal()
                }
                self.refresh()
                self.render()
            }
        }
    }

    @objc private func deleteMachine(_ sender: NSButton) {
        guard let name = sender.identifier?.rawValue, let window = view.window else { return }
        let a = NSAlert()
        a.messageText = "Delete \(name)?"
        a.informativeText = "The machine and all its files are deleted. Your Mac files in /Users are not touched."
        a.alertStyle = .warning
        a.addButton(withTitle: "Delete").hasDestructiveAction = true
        a.addButton(withTitle: "Cancel")
        a.beginSheetModal(for: window) { [weak self] r in
            guard r == .alertFirstButtonReturn, let self else { return }
            self.busy.insert(name)
            self.render()
            let d = self.model.daemon
            DispatchQueue.global().async { [weak self] in
                let err: Error? = Result { try d.call("machine.delete", ["name": name], timeout: 120) }.failure
                DispatchQueue.main.async {
                    guard let self else { return }
                    self.busy.remove(name)
                    self.render()
                    if let err { NSAlert(error: err).runModal() }
                    self.refresh()
                }
            }
        }
    }

    private func row(_ m: MachineInfo) -> NSView {
        let running = m.state == "running"
        let dot = NSImageView(image: NSImage(systemSymbolName: "circle.fill", accessibilityDescription: m.state)!)
        dot.contentTintColor = running ? .systemGreen : .secondaryLabelColor
        dot.symbolConfiguration = .init(pointSize: 8, weight: .regular)
        let title = NSTextField(labelWithString: m.default ? "\(m.name) (default)" : m.name)
        title.font = .systemFont(ofSize: 13, weight: .semibold)
        let sub = NSTextField(labelWithString: [m.distro + " " + m.release, m.state, m.ip ?? ""]
            .filter { !$0.isEmpty }.joined(separator: " · "))
        sub.textColor = .secondaryLabelColor
        sub.font = .systemFont(ofSize: 11)
        let text = NSStackView(views: [title, sub])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 2
        let toggle = NSButton(title: running ? "Stop" : "Start", target: self, action: #selector(toggle(_:)))
        toggle.identifier = NSUserInterfaceItemIdentifier(m.name)
        toggle.isEnabled = !busy.contains(m.name)
        let shell = NSButton(title: "Shell", target: self, action: #selector(openShell(_:)))
        shell.identifier = NSUserInterfaceItemIdentifier(m.name)
        let trash = NSButton(image: NSImage(systemSymbolName: "trash", accessibilityDescription: "Delete")!,
                             target: self, action: #selector(deleteMachine(_:)))
        trash.bezelStyle = .push
        trash.toolTip = "Delete…"
        trash.identifier = NSUserInterfaceItemIdentifier(m.name)
        trash.isEnabled = !busy.contains(m.name)
        let spacer = NSView()
        spacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let h = NSStackView(views: [dot, text, spacer, toggle, shell, trash])
        h.orientation = .horizontal
        h.spacing = 10
        h.translatesAutoresizingMaskIntoConstraints = false
        h.widthAnchor.constraint(greaterThanOrEqualToConstant: 360).isActive = true
        return h
    }

    @objc private func toggle(_ sender: NSButton) {
        guard let name = sender.identifier?.rawValue,
              let m = machines.first(where: { $0.name == name }) else { return }
        let method = m.state == "running" ? "machine.stop" : "machine.start"
        busy.insert(name)
        render()
        let d = model.daemon
        DispatchQueue.global().async { [weak self] in
            let err: Error? = Result { try d.call(method, ["name": name], timeout: 60) }.failure
            DispatchQueue.main.async {
                guard let self else { return }
                self.busy.remove(name)
                self.render()
                if let err { NSAlert(error: err).runModal() }
                self.refresh()
            }
        }
    }

    /// `katamaran shell <name>` in Terminal, through a .command file.
    @objc private func openShell(_ sender: NSButton) {
        guard let name = sender.identifier?.rawValue else { return }
        let script = MachineShellCommand.script(
            name: name,
            cliPath: AppModel.cliPath(),
            katamaranHome: ProcessInfo.processInfo.environment["KATAMARAN_HOME"]
        )
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent(MachineShellCommand.filename(for: name))
        do {
            try script.write(to: url, atomically: true, encoding: .utf8)
            try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: url.path)
            NSWorkspace.shared.open(url)
        } catch {
            NSAlert(error: error).runModal()
        }
    }
}

/// Builds the `.command` script and safe filename for opening a machine shell in Terminal.
enum MachineShellCommand {
    static func script(name: String, cliPath: String, katamaranHome: String?) -> String {
        let env = katamaranHome.map { "export KATAMARAN_HOME=\(shellQuote($0))\n" } ?? ""
        return "#!/bin/sh\n\(env)exec \(shellQuote(cliPath)) shell \(shellQuote(name))\n"
    }

    static func filename(for name: String) -> String {
        let sanitized = String(name.unicodeScalars.map { s -> Character in
            let isSafe = ("a"..."z").contains(s)
                || ("A"..."Z").contains(s)
                || ("0"..."9").contains(s)
                || s == "-" || s == "_"
            return isSafe ? Character(s) : "_"
        })
        let stem = sanitized.isEmpty ? "machine" : String(sanitized.prefix(48))
        let digest = SHA256.hash(data: Data(name.utf8))
            .prefix(8)
            .map { String(format: "%02x", $0) }
            .joined()
        return "katamaran-shell-\(stem)-\(digest).command"
    }
}

private extension Result {
    var failure: Failure? {
        if case .failure(let e) = self { return e }
        return nil
    }
}

/// The "New Machine" sheet: name, distro and an optional release.
@MainActor
final class NewMachineSheet: NSObject, NSTextFieldDelegate {
    static let distros: [(id: String, title: String, releases: String)] = [
        ("ubuntu", "Ubuntu", "e.g. noble, jammy"),
        ("debian", "Debian", "e.g. trixie, bookworm"),
        ("fedora", "Fedora", "e.g. 42"),
        ("arch", "Arch Linux", "current"),
        ("alpine", "Alpine", "e.g. 3.22"),
    ]

    let window: NSWindow
    private let name = NSTextField()
    private let distro = NSPopUpButton()
    private let release = NSTextField()
    private let hint = NSTextField(labelWithString: "")
    private let create = NSButton(title: "Create", target: nil, action: nil)
    private let taken: Set<String>
    private static var open: [NewMachineSheet] = []
    var onCreate: (String, String, String?) -> Void = { _, _, _ in }

    init(taken: Set<String>) {
        self.taken = taken
        window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 420, height: 220),
                          styleMask: [.titled], backing: .buffered, defer: false)
        super.init()
        build()
    }

    func retainUntilClosed() { Self.open.append(self) }

    private func build() {
        let title = NSTextField(labelWithString: "New Linux Machine")
        title.font = .systemFont(ofSize: 15, weight: .semibold)
        name.placeholderString = "dev"
        name.delegate = self
        var n = "ubuntu"
        var i = 1
        while taken.contains(n) { i += 1; n = "ubuntu-\(i)" }
        name.stringValue = n
        for d in Self.distros { distro.addItem(withTitle: d.title) }
        distro.target = self
        distro.action = #selector(distroChanged)
        release.placeholderString = "Latest"
        hint.font = .systemFont(ofSize: NSFont.smallSystemFontSize)
        hint.textColor = .secondaryLabelColor
        func labeled(_ t: String, _ v: NSView) -> [NSView] {
            let l = NSTextField(labelWithString: t)
            l.alignment = .right
            return [l, v]
        }
        let grid = NSGridView(views: [labeled("Name", name), labeled("Distribution", distro),
                                      labeled("Release", release), [NSGridCell.emptyContentView, hint]])
        grid.column(at: 0).xPlacement = .trailing
        grid.rowSpacing = 10
        grid.columnSpacing = 10
        name.widthAnchor.constraint(equalToConstant: 240).isActive = true
        release.widthAnchor.constraint(equalToConstant: 240).isActive = true
        let cancel = NSButton(title: "Cancel", target: self, action: #selector(cancelSheet))
        cancel.keyEquivalent = "\u{1b}"
        create.target = self
        create.action = #selector(confirm)
        create.keyEquivalent = "\r"
        let buttons = NSStackView(views: [NSView(), cancel, create])
        let v = NSStackView(views: [title, grid, buttons])
        v.orientation = .vertical
        v.alignment = .leading
        v.spacing = 16
        v.edgeInsets = NSEdgeInsets(top: 20, left: 20, bottom: 20, right: 20)
        buttons.translatesAutoresizingMaskIntoConstraints = false
        window.contentView = v
        buttons.widthAnchor.constraint(equalTo: v.widthAnchor, constant: -40).isActive = true
        distroChanged()
        validate()
    }

    @objc private func distroChanged() {
        let d = Self.distros[max(0, distro.indexOfSelectedItem)]
        release.placeholderString = "Latest (\(d.releases))"
        validate()
    }

    func controlTextDidChange(_ obj: Notification) { validate() }

    static func nameProblem(_ n: String, taken: Set<String>) -> String? {
        if n.isEmpty { return "Give the machine a name." }
        if n.count > 40 { return "Use at most 40 characters." }
        let ok = n.unicodeScalars.allSatisfy { ("a"..."z").contains($0) || ("0"..."9").contains($0) || $0 == "-" }
        if !ok || n.hasPrefix("-") || n.hasSuffix("-") { return "Use lowercase letters, digits and dashes." }
        if taken.contains(n) { return "A machine named \(n) already exists." }
        return nil
    }

    private func validate() {
        let problem = Self.nameProblem(name.stringValue, taken: taken)
        hint.stringValue = problem ?? "Your Mac user, passwordless sudo and /Users are set up inside."
        hint.textColor = problem == nil ? .secondaryLabelColor : .systemRed
        create.isEnabled = problem == nil
    }

    @objc private func confirm() {
        guard Self.nameProblem(name.stringValue, taken: taken) == nil else { return }
        let r = release.stringValue.trimmingCharacters(in: .whitespaces)
        onCreate(name.stringValue, Self.distros[max(0, distro.indexOfSelectedItem)].id, r.isEmpty ? nil : r)
        close()
    }

    @objc private func cancelSheet() { close() }

    private func close() {
        window.sheetParent?.endSheet(window)
        Self.open.removeAll { $0 === self }
    }
}
