import AppKit

enum ContainerTab: Int, CaseIterable {
    case info, stats, logs, terminal, files

    var title: String {
        switch self {
        case .info: "Info"
        case .stats: "Stats"
        case .logs: "Logs"
        case .terminal: "Terminal"
        case .files: "Files"
        }
    }
}

/// One tab of the detail pane.
@MainActor
protocol ContainerTabView: NSView {
    /// The container (and its inspect data, once fetched) to show; nil for none.
    func show(_ c: ContainerSummary?, _ inspect: ContainerInspect?)
    /// Streams run only while the tab is on screen.
    func setActive(_ active: Bool)
}

/// The selected container or project: a header with its actions, then the tab
/// the toolbar picked.
@MainActor
final class ContainerDetailViewController: NSViewController {
    private let store: DockerStore
    private let model: AppModel
    private let header = DetailHeader()
    private let empty = EmptyStateView()
    private let body = NSView()
    private var views: [ContainerTab: ContainerTabView] = [:]
    private let projectView = ProjectInfoView()
    private(set) var tab: ContainerTab
    private var key: String?
    private var inspect: ContainerInspect?
    private var inspecting: String?
    private var active = false
    var onTabsChanged: () -> Void = {}

    private static let tabKey = "containerTab"

    init(store: DockerStore, model: AppModel) {
        self.store = store
        self.model = model
        tab = ContainerTab(rawValue: UserDefaults.standard.integer(forKey: Self.tabKey)) ?? .info
        super.init(nibName: nil, bundle: nil)
        store.observe { [weak self] s in self?.storeChanged(s) }
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        let root = NSView()
        views = [
            .info: InfoTabView(model: model),
            .stats: StatsTabView(store: store),
            .logs: LogsTabView(store: store),
            .terminal: EmbeddedTerminalTabView(store: store),
            .files: FilesTabView(model: model),
        ]
        header.onAction = { [weak self] a in self?.act(a) }
        body.translatesAutoresizingMaskIntoConstraints = false
        let line = NSBox()
        line.boxType = .separator
        line.translatesAutoresizingMaskIntoConstraints = false
        for v in [header, line, body, empty] as [NSView] { root.addSubview(v) }
        for v in Array(views.values) as [NSView] + [projectView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            v.isHidden = true
            body.addSubview(v)
            NSLayoutConstraint.activate([
                v.topAnchor.constraint(equalTo: body.topAnchor),
                v.leadingAnchor.constraint(equalTo: body.leadingAnchor),
                v.trailingAnchor.constraint(equalTo: body.trailingAnchor),
                v.bottomAnchor.constraint(equalTo: body.bottomAnchor),
            ])
        }
        projectView.onSelect = { [weak self] name in self?.selectInList?(name) }
        NSLayoutConstraint.activate([
            header.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor),
            header.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            header.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            line.topAnchor.constraint(equalTo: header.bottomAnchor),
            line.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            line.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            line.heightAnchor.constraint(equalToConstant: 1),
            body.topAnchor.constraint(equalTo: line.bottomAnchor),
            body.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            body.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            body.bottomAnchor.constraint(equalTo: root.bottomAnchor),
            empty.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor),
            empty.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            empty.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            empty.bottomAnchor.constraint(equalTo: root.bottomAnchor),
        ])
        empty.show(symbol: nil, title: "No Selection")
        view = root
        refresh()
    }

    var selectInList: ((String) -> Void)?

    /// What the list selected: "container:<id>", "project:<name>" or nil.
    func show(key k: String?) {
        if k != key {
            key = k
            inspect = nil
            inspecting = nil
        }
        refresh()
    }

    func select(_ t: ContainerTab) {
        guard t != tab else { return }
        views[tab]?.setActive(false)
        tab = t
        UserDefaults.standard.set(t.rawValue, forKey: Self.tabKey)
        refresh()
        onTabsChanged()
    }

    func setActive(_ a: Bool) {
        active = a
        refresh()
    }

    private var container: ContainerSummary? {
        guard let key, key.hasPrefix("container:") else { return nil }
        return store.container(String(key.dropFirst("container:".count)))
    }

    private var project: ComposeProject? {
        guard let key, key.hasPrefix("project:") else { return nil }
        let name = String(key.dropFirst("project:".count))
        let members = store.containers.filter { $0.project == name }
        return members.isEmpty ? nil : ContainerList.sections(members).flatMap(\.items).compactMap {
            if case .project(let p) = $0 { p } else { nil }
        }.first
    }

    var enabledTabs: Set<Int> {
        guard container != nil else { return project != nil ? [ContainerTab.info.rawValue] : [] }
        return Set(ContainerTab.allCases.map(\.rawValue))
    }

    private func storeChanged(_ s: DockerStore) {
        guard let c = container else { refresh(); return }
        if s.changedContainers.contains(c.Id) || inspect?.State.Status != c.State { inspect = nil; inspecting = nil }
        refresh()
    }

    private func refresh() {
        guard isViewLoaded else { return }
        let c = container
        let p = c == nil ? project : nil
        empty.isHidden = c != nil || p != nil
        header.isHidden = !empty.isHidden
        body.isHidden = !empty.isHidden
        if let c {
            header.show(container: c)
            projectView.isHidden = true
            if inspect?.Id != c.Id { fetchInspect(c.Id) }
            for (t, v) in views {
                v.isHidden = t != tab
                if t == tab { v.show(c, inspect?.Id == c.Id ? inspect : nil) }
                v.setActive(active && t == tab)
            }
        } else {
            views.values.forEach { $0.isHidden = true; $0.setActive(false) }
            if let p {
                header.show(project: p)
                projectView.isHidden = false
                projectView.show(p)
            }
        }
    }

    private func fetchInspect(_ id: String) {
        guard inspecting != id, let client = store.client else { return }
        let epoch = store.connectionEpoch
        inspecting = id
        BlockingIO.run(on: BlockingIO.docker) {
            try? client.inspect(id)
        } completion: { [weak self] result in
            guard let self, self.store.connectionEpoch == epoch, self.inspecting == id else { return }
            self.inspect = result
            self.refresh()
        }
    }

    private func act(_ action: ContainerAction) {
        let targets: [ContainerSummary]
        let title: String
        if let c = container { targets = [c]; title = c.name }
        else if let p = project { targets = p.containers; title = p.name }
        else { return }
        if action == .remove {
            confirm(view.window, title: "Delete \(title)?",
                    message: targets.count > 1 ? "All \(targets.count) containers are stopped and deleted. Volumes stay."
                        : "The container and its files are deleted. Volumes stay.",
                    button: "Delete") { [store] in store.act(.remove, on: targets) }
        } else {
            let filtered = switch action {
            case .start: targets.filter { !$0.running }
            case .stop, .restart: targets.filter(\.running)
            case .remove: targets
            }
            store.act(action, on: filtered.isEmpty ? targets : filtered)
        }
    }
}

/// Icon, name and state of the selection, with its buttons.
@MainActor
final class DetailHeader: NSView {
    private let tile = IconTile(size: 36)
    private let name = label(font: .systemFont(ofSize: 15, weight: .semibold), color: .labelColor)
    private let state = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let start = NSButton()
    private let stop = NSButton()
    private let restart = NSButton()
    private let delete = NSButton()
    var onAction: (ContainerAction) -> Void = { _ in }

    init() {
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        for (b, symbol, tip, sel) in [
            (start, "play.fill", "Start", #selector(doStart)), (stop, "stop.fill", "Stop", #selector(doStop)),
            (restart, "arrow.clockwise", "Restart", #selector(doRestart)), (delete, "trash", "Delete", #selector(doDelete)),
        ] {
            b.image = NSImage(systemSymbolName: symbol, accessibilityDescription: tip)
            b.bezelStyle = .push
            b.controlSize = .regular
            b.toolTip = tip
            b.target = self
            b.action = sel
        }
        name.isSelectable = true
        let text = NSStackView(views: [name, state])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 2
        let buttons = NSStackView(views: [start, stop, restart, delete])
        buttons.spacing = 6
        for v in [tile, text, buttons] as [NSView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            addSubview(v)
        }
        name.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        state.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        NSLayoutConstraint.activate([
            heightAnchor.constraint(equalToConstant: 64),
            tile.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 20),
            tile.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.leadingAnchor.constraint(equalTo: tile.trailingAnchor, constant: 12),
            text.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.trailingAnchor.constraint(lessThanOrEqualTo: buttons.leadingAnchor, constant: -12),
            buttons.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),
            buttons.centerYAnchor.constraint(equalTo: centerYAnchor),
        ])
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(container c: ContainerSummary) {
        tile.symbol = ImageIcon.symbol(for: c.Image)
        tile.tint = ImageIcon.tint(for: c.Image)
        tile.dimmed = !c.running
        tile.dot = c.health.color
        name.stringValue = c.name
        state.stringValue = [c.Status, c.shortID].joined(separator: " · ")
        start.isHidden = c.running
        stop.isHidden = !c.running
        restart.isHidden = !c.running
    }

    func show(project p: ComposeProject) {
        tile.symbol = "square.stack.3d.up.fill"
        tile.tint = .systemIndigo
        tile.dimmed = !p.anyRunning
        tile.dot = nil
        name.stringValue = p.name
        state.stringValue = "Compose project · \(p.runningCount) of \(p.containers.count) running"
        start.isHidden = p.runningCount == p.containers.count
        stop.isHidden = !p.anyRunning
        restart.isHidden = !p.anyRunning
    }

    @objc private func doStart() { onAction(.start) }
    @objc private func doStop() { onAction(.stop) }
    @objc private func doRestart() { onAction(.restart) }
    @objc private func doDelete() { onAction(.remove) }
}

// MARK: - Info

@MainActor
final class InfoTabView: NSView, ContainerTabView {
    private let model: AppModel
    private let content = ScrollingStack()
    private var shown: (ContainerSummary, ContainerInspect?)?

    init(model: AppModel) {
        self.model = model
        super.init(frame: .zero)
        addSubview(content.scroll)
        pin(content.scroll, to: self)
    }

    required init?(coder: NSCoder) { fatalError() }

    func setActive(_ active: Bool) {}

    func show(_ c: ContainerSummary?, _ inspect: ContainerInspect?) {
        guard let c else { return }
        if let s = shown, s.0 == c, s.1 == inspect { return }
        shown = (c, inspect)
        var groups: [NSView] = []

        var general: [NSView] = [
            InfoRow("Image", c.Image),
            InfoRow("Status", inspect.map { stateText($0, c) } ?? c.Status),
            InfoRow("ID", c.Id, mono: true),
        ]
        if let created = c.createdDate { general.append(InfoRow("Created", "\(absolute(created)) (\(relative(created)))")) }
        if let cmd = inspect?.commandLine ?? c.Command, !cmd.isEmpty { general.append(InfoRow("Command", cmd, mono: true)) }
        if let r = inspect?.HostConfig?.RestartPolicy?.Name, !r.isEmpty, r != "no" { general.append(InfoRow("Restart", r)) }
        if let p = c.project {
            general.append(InfoRow("Compose", "\(p) · \(c.service ?? "")"))
        }
        groups.append(FormGroup(header: "General", rows: general))

        let bindings = inspect?.portBindings ?? (c.Ports ?? []).compactMap { p in
            p.PublicPort.map { PortBinding(host: $0, container: p.PrivatePort, proto: p.type) }
        }
        var network: [NSView] = []
        if !bindings.isEmpty {
            var seen = Set<UInt16>()
            let links = bindings.filter { seen.insert($0.host).inserted }.map { b -> NSView in
                let link = linkButton("localhost:\(b.host)", target: self, action: #selector(openPort(_:)), tag: Int(b.host))
                link.isEnabled = b.proto == "tcp"
                let arrow = label("→ \(b.container)/\(b.proto)", font: .systemFont(ofSize: NSFont.systemFontSize), color: .secondaryLabelColor)
                let row = NSStackView(views: [link, arrow])
                row.spacing = 6
                return row
            }
            let stack = NSStackView(views: links)
            stack.orientation = .vertical
            stack.alignment = .leading
            stack.spacing = 4
            network.append(InfoRow("Ports", view: stack))
        }
        for (name, ep) in (inspect?.NetworkSettings?.Networks ?? c.NetworkSettings?.Networks ?? [:]).sorted(by: { $0.key < $1.key }) {
            let ip = ep.IPAddress.flatMap { $0.isEmpty ? nil : $0 }
            network.append(InfoRow(name, ip ?? "No address", mono: ip != nil))
        }
        if !network.isEmpty { groups.append(FormGroup(header: "Network", rows: network)) }

        let mounts = inspect?.Mounts ?? c.Mounts ?? []
        if !mounts.isEmpty {
            groups.append(FormGroup(header: "Mounts", rows: mounts.map { m in
                let source = m.type == "volume" ? (m.Name ?? m.Source ?? "") : (m.Source ?? "")
                let text = source + (m.RW == false ? " (read-only)" : "")
                if m.type == "volume", let name = m.Name, let dir = volumeDirectory(name) {
                    let link = linkButton(name, target: self, action: #selector(openPath(_:)))
                    link.identifier = .init(dir.path)
                    link.toolTip = "Show in Finder"
                    return InfoRow(m.Destination ?? "", view: link)
                }
                return InfoRow(m.Destination ?? "", text, mono: true)
            }))
        }

        if let env = inspect?.environment, !env.isEmpty {
            groups.append(FormGroup(header: "Environment", rows: env.map { InfoRow($0.0, $0.1, mono: true) }))
        }

        let labels = (inspect?.Config.Labels ?? c.Labels ?? [:]).sorted { $0.key < $1.key }
        if !labels.isEmpty {
            let rows = labels.map { kv -> NSView in
                let text = NSTextField(wrappingLabelWithString: "\(kv.key)\n\(kv.value)")
                text.isSelectable = true
                text.font = .monospacedSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular)
                let attr = NSMutableAttributedString(string: kv.key + "\n", attributes: [
                    .font: NSFont.systemFont(ofSize: NSFont.smallSystemFontSize, weight: .medium),
                    .foregroundColor: NSColor.secondaryLabelColor,
                ])
                attr.append(NSAttributedString(string: kv.value.isEmpty ? "-" : kv.value, attributes: [
                    .font: NSFont.monospacedSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                    .foregroundColor: NSColor.labelColor,
                ]))
                text.attributedStringValue = attr
                return PaddedRow(text)
            }
            groups.append(FormGroup(header: "Labels", rows: rows))
        }
        content.set(groups)
    }

    private func stateText(_ i: ContainerInspect, _ c: ContainerSummary) -> String {
        var s = c.Status
        if !c.running, let code = i.State.ExitCode { s += code == 0 ? "" : " · exit code \(code)" }
        if let h = i.State.Health?.Status, !h.isEmpty, h != "none" { s += " · \(h)" }
        return s
    }

    private func volumeDirectory(_ name: String) -> URL? {
        guard let root = model.status?.mount.path, model.status?.mount.mounted == true else { return nil }
        return URL(fileURLWithPath: root).appendingPathComponent("docker/volumes/\(name)")
    }

    @objc private func openPort(_ sender: NSButton) { openLocalhost(UInt16(sender.tag)) }

    @objc private func openPath(_ sender: NSButton) {
        guard let path = sender.identifier?.rawValue else { return }
        NSWorkspace.shared.open(URL(fileURLWithPath: path))
    }
}

/// Any view as a group row, with the row's insets.
@MainActor
final class PaddedRow: NSView {
    init(_ content: NSView) {
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        content.translatesAutoresizingMaskIntoConstraints = false
        addSubview(content)
        content.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        NSLayoutConstraint.activate([
            content.leadingAnchor.constraint(equalTo: leadingAnchor, constant: FormLayout.rowInset),
            content.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -FormLayout.rowInset),
            content.topAnchor.constraint(equalTo: topAnchor, constant: 8),
            content.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -8),
        ])
    }

    required init?(coder: NSCoder) { fatalError() }
}

@MainActor
func pin(_ v: NSView, to parent: NSView, top: NSLayoutYAxisAnchor? = nil) {
    v.translatesAutoresizingMaskIntoConstraints = false
    NSLayoutConstraint.activate([
        v.topAnchor.constraint(equalTo: top ?? parent.topAnchor),
        v.leadingAnchor.constraint(equalTo: parent.leadingAnchor),
        v.trailingAnchor.constraint(equalTo: parent.trailingAnchor),
        v.bottomAnchor.constraint(equalTo: parent.bottomAnchor),
    ])
}

/// A Compose project: its services, where it lives.
@MainActor
final class ProjectInfoView: NSView {
    private let content = ScrollingStack()
    private var shown: ComposeProject?
    var onSelect: (String) -> Void = { _ in }

    init() {
        super.init(frame: .zero)
        addSubview(content.scroll)
        pin(content.scroll, to: self)
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(_ p: ComposeProject) {
        guard p != shown else { return }
        shown = p
        let services = p.containers.map { c -> NSView in
            let tile = IconTile(size: 22)
            tile.symbol = ImageIcon.symbol(for: c.Image)
            tile.tint = ImageIcon.tint(for: c.Image)
            tile.dimmed = !c.running
            tile.dot = c.health.color
            let link = linkButton(c.name, target: self, action: #selector(select(_:)))
            link.identifier = .init(c.name)
            let status = label(c.Status, font: .systemFont(ofSize: NSFont.systemFontSize), color: .secondaryLabelColor)
            let row = NSStackView(views: [tile, link, status])
            row.spacing = 8
            return InfoRow(c.service ?? c.name, view: row)
        }
        var groups: [NSView] = [FormGroup(header: "Services", rows: services)]
        var place: [NSView] = []
        if let dir = p.workingDir {
            let link = linkButton((dir as NSString).abbreviatingWithTildeInPath, target: self, action: #selector(openDir(_:)))
            link.identifier = .init(dir)
            link.toolTip = "Show in Finder"
            place.append(InfoRow("Directory", view: link))
        }
        if let files = p.configFiles {
            place.append(InfoRow("Compose file", files.split(separator: ",").map {
                (String($0) as NSString).abbreviatingWithTildeInPath
            }.joined(separator: "\n"), mono: true))
        }
        if !place.isEmpty { groups.append(FormGroup(header: "Project", rows: place)) }
        content.set(groups)
    }

    @objc private func select(_ sender: NSButton) { sender.identifier.map { onSelect($0.rawValue) } }

    @objc private func openDir(_ sender: NSButton) {
        guard let path = sender.identifier?.rawValue else { return }
        NSWorkspace.shared.open(URL(fileURLWithPath: path))
    }
}

// MARK: - Stats

@MainActor
final class StatsTabView: NSView, ContainerTabView {
    private let store: DockerStore
    private let cpu = MetricCard("CPU", symbol: "cpu", color: .systemBlue)
    private let mem = MetricCard("Memory", symbol: "memorychip", color: .systemPurple)
    private let net = MetricCard("Network", symbol: "network", color: .systemGreen)
    private let disk = MetricCard("Disk", symbol: "internaldrive", color: .systemOrange)
    private let empty = EmptyStateView()
    private let grid = NSGridView()
    private var stream: DockerStream?
    private var streamID: String?
    private var lines = LineSplitter()
    private var prev: StatsSample?
    private var history: (cpu: [Double], mem: [Double], net: [Double], disk: [Double]) = ([], [], [], [])
    private var container: ContainerSummary?
    private var active = false

    init(store: DockerStore) {
        self.store = store
        super.init(frame: .zero)
        let row1 = NSStackView(views: [cpu, mem])
        let row2 = NSStackView(views: [net, disk])
        let stack = NSStackView(views: [row1, row2])
        for r in [row1, row2] {
            r.distribution = .fillEqually
            r.spacing = 14
        }
        stack.orientation = .vertical
        stack.spacing = 14
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        addSubview(empty)
        pin(empty, to: self)
        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(equalTo: topAnchor, constant: 20),
            stack.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 20),
            stack.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),
            row1.widthAnchor.constraint(equalTo: stack.widthAnchor),
            row2.widthAnchor.constraint(equalTo: stack.widthAnchor),
        ])
        for card in [cpu, mem, net, disk] {
            card.valueLabel.stringValue = "-"
            card.spark.capacity = 90
        }
        empty.show(symbol: "chart.xyaxis.line", title: "Not Running", message: "Start the container to see its usage.")
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(_ c: ContainerSummary?, _ inspect: ContainerInspect?) {
        container = c
        empty.isHidden = c?.running ?? false
        update()
    }

    func setActive(_ a: Bool) {
        active = a
        update()
    }

    private func update() {
        let want = active && (container?.running ?? false) ? container?.Id : nil
        guard want != streamID else { return }
        stream?.cancel()
        stream = nil
        streamID = want
        if want != nil && want != lastID {
            history = ([], [], [], [])
            prev = nil
            for card in [cpu, mem, net, disk] { card.valueLabel.stringValue = "-"; card.detailLabel.stringValue = " "; card.spark.values = [] }
        }
        guard let id = want, let socket = store.socket else { return }
        lastID = id
        lines = LineSplitter()
        let s = DockerStream(socketPath: socket, path: "/containers/\(id)/stats?stream=true")
        s.onData = { [weak self] d in self?.received(d) }
        s.onEnd = { [weak self, weak s] _ in
            guard let self, self.stream === s else { return }
            self.stream = nil
            self.streamID = nil
        }
        stream = s
        s.start()
    }

    private var lastID: String?

    private func received(_ data: Data) {
        for line in lines.feed(data) {
            guard let (cur, pre) = try? StatsSample.parse(line) else { continue }
            let cpuPct = pre.map { cur.cpuPercent(since: $0) } ?? 0
            let r = prev.map { cur.rates(since: $0) } ?? (rx: 0, tx: 0, read: 0, write: 0)
            prev = cur
            func push(_ a: inout [Double], _ v: Double) { a.append(v); if a.count > 90 { a.removeFirst(a.count - 90) } }
            push(&history.cpu, cpuPct)
            push(&history.mem, Double(cur.memoryUsed))
            push(&history.net, r.rx + r.tx)
            push(&history.disk, r.read + r.write)

            cpu.valueLabel.stringValue = String(format: "%.1f%%", cpuPct)
            cpu.detailLabel.stringValue = "\(cur.onlineCPUs) CPUs · \(cur.pids) processes"
            cpu.spark.maxValue = 100
            cpu.spark.values = history.cpu
            mem.valueLabel.stringValue = bytes(Int64(clamping: cur.memoryUsed))
            mem.detailLabel.stringValue = cur.memoryLimit > 0 ? "of \(bytes(Int64(clamping: cur.memoryLimit)))" : " "
            mem.spark.maxValue = (history.mem.max() ?? 0) * 1.25
            mem.spark.values = history.mem
            net.valueLabel.stringValue = "↓ \(rate(r.rx))   ↑ \(rate(r.tx))"
            net.detailLabel.stringValue = "\(bytes(Int64(clamping: cur.netRx))) in · \(bytes(Int64(clamping: cur.netTx))) out"
            net.spark.maxValue = 1024
            net.spark.values = history.net
            disk.valueLabel.stringValue = "↓ \(rate(r.read))   ↑ \(rate(r.write))"
            disk.detailLabel.stringValue = "\(bytes(Int64(clamping: cur.blockRead))) read · \(bytes(Int64(clamping: cur.blockWrite))) written"
            disk.spark.maxValue = 1024
            disk.spark.values = history.disk
        }
    }
}

// MARK: - Logs

@MainActor
final class LogsTabView: NSView, ContainerTabView, NSSearchFieldDelegate {
    private let store: DockerStore
    private let text = LogTextView()
    private let filter = NSSearchField()
    private let follow = NSButton(checkboxWithTitle: "Follow", target: nil, action: nil)
    private var stream: DockerStream?
    private var streamID: String?
    private var demux = LogDemuxer(multiplexed: true)
    private var outLines = LineSplitter()
    private var errLines = LineSplitter()
    private var container: ContainerSummary?
    private var tty: Bool?
    private var active = false
    private var wasRunning = false

    init(store: DockerStore) {
        self.store = store
        super.init(frame: .zero)
        filter.placeholderString = "Filter"
        filter.delegate = self
        filter.sendsWholeSearchString = false
        filter.controlSize = .regular
        follow.state = .on
        follow.target = self
        follow.action = #selector(followChanged)
        let clear = NSButton(image: NSImage(systemSymbolName: "trash", accessibilityDescription: "Clear") ?? NSImage(),
                             target: self, action: #selector(clearText))
        clear.bezelStyle = .push
        clear.toolTip = "Clear"
        let bar = NSStackView(views: [filter, follow, clear])
        bar.spacing = 10
        bar.edgeInsets = NSEdgeInsets(top: 10, left: 20, bottom: 10, right: 20)
        bar.translatesAutoresizingMaskIntoConstraints = false
        filter.widthAnchor.constraint(greaterThanOrEqualToConstant: 160).isActive = true
        filter.setContentHuggingPriority(.defaultLow, for: .horizontal)
        addSubview(bar)
        addSubview(text.scroll)
        NSLayoutConstraint.activate([
            bar.topAnchor.constraint(equalTo: topAnchor),
            bar.leadingAnchor.constraint(equalTo: leadingAnchor),
            bar.trailingAnchor.constraint(equalTo: trailingAnchor),
        ])
        pin(text.scroll, to: self, top: bar.bottomAnchor)
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(_ c: ContainerSummary?, _ inspect: ContainerInspect?) {
        let changed = c?.Id != container?.Id
        container = c
        if let inspect, inspect.Id == c?.Id { tty = inspect.tty }
        if changed { tty = inspect?.Id == c?.Id ? inspect?.tty : nil }
        // A container that starts again logs on; reconnect to follow it.
        let running = c?.running ?? false
        if running && !wasRunning && !changed && stream == nil { streamID = nil }
        wasRunning = running
        update(reset: changed)
    }

    func setActive(_ a: Bool) {
        active = a
        update(reset: false)
    }

    private func update(reset: Bool) {
        // TTY or not decides how the stream is framed: wait for inspect.
        let want = active && tty != nil ? container?.Id : nil
        if reset { text.clear() }
        guard want != streamID || reset else { return }
        stream?.cancel()
        stream = nil
        streamID = want
        guard let id = want, let socket = store.socket, let tty else { return }
        text.clear()
        demux = LogDemuxer(multiplexed: !tty)
        outLines = LineSplitter()
        errLines = LineSplitter()
        let s = DockerStream(socketPath: socket, path: "/containers/\(id)/logs?follow=1&stdout=1&stderr=1&tail=2000")
        s.onData = { [weak self] d in self?.received(d) }
        s.onEnd = { [weak self, weak s] err in
            guard let self, self.stream === s else { return }
            if let rest = self.outLines.flush() {
                self.text.append([String(decoding: rest, as: UTF8.self)], error: false)
            }
            if let rest = self.errLines.flush() {
                self.text.append([String(decoding: rest, as: UTF8.self)], error: true)
            }
            if let err { self.text.append(["- \(err.localizedDescription) -"], error: true) }
            if self.follow.state == .on { self.text.scrollToEnd() }
            self.stream = nil
        }
        stream = s
        s.start()
    }

    private func received(_ data: Data) {
        var out: [String] = [], err: [String] = []
        for (kind, payload) in demux.feed(data) {
            if kind == .stderr { err += errLines.feed(payload).map { String(decoding: $0, as: UTF8.self) } }
            else { out += outLines.feed(payload).map { String(decoding: $0, as: UTF8.self) } }
        }
        if !out.isEmpty { text.append(out, error: false) }
        if !err.isEmpty { text.append(err, error: true) }
        if follow.state == .on { text.scrollToEnd() }
    }

    func controlTextDidChange(_ obj: Notification) { text.filter = filter.stringValue }

    @objc private func followChanged() { if follow.state == .on { text.scrollToEnd() } }
    @objc private func clearText() { text.clear() }
}

/// A read-only log view that keeps the last lines and can filter them.
@MainActor
final class LogTextView {
    let scroll = NSTextView.scrollableTextView()
    private var textView: NSTextView { scroll.documentView as! NSTextView }
    private var lines: [(String, Bool)] = []
    private let maxLines = 10_000
    var filter = "" { didSet { if filter != oldValue { redraw() } } }

    init() {
        let t = textView
        t.isEditable = false
        t.isSelectable = true
        t.isRichText = false
        t.usesFindBar = true
        t.isIncrementalSearchingEnabled = true
        t.textContainerInset = NSSize(width: 16, height: 10)
        t.drawsBackground = true
        t.backgroundColor = .textBackgroundColor
        t.font = .mono(11.5)
        scroll.hasVerticalScroller = true
        scroll.borderType = .noBorder
        scroll.drawsBackground = true
        scroll.backgroundColor = .textBackgroundColor
    }

    private func attributed(_ batch: [(String, Bool)]) -> NSAttributedString {
        let out = NSMutableAttributedString()
        for (line, err) in batch {
            out.append(NSAttributedString(string: line + "\n", attributes: [
                .font: NSFont.mono(11.5), .foregroundColor: err ? NSColor.secondaryLabelColor : NSColor.labelColor,
            ]))
        }
        return out
    }

    private func matches(_ s: String) -> Bool { filter.isEmpty || s.localizedCaseInsensitiveContains(filter) }

    func append(_ new: [String], error: Bool) {
        let batch = new.map { (stripANSI($0), error) }
        lines += batch
        guard let storage = textView.textStorage else { return }
        if lines.count > maxLines + 1000 {
            lines.removeFirst(lines.count - maxLines)
            redraw()
            return
        }
        storage.append(attributed(batch.filter { matches($0.0) }))
    }

    func clear() {
        lines = []
        textView.string = ""
    }

    func scrollToEnd() { textView.scrollToEndOfDocument(nil) }

    private func redraw() {
        textView.textStorage?.setAttributedString(attributed(lines.filter { matches($0.0) }))
        scrollToEnd()
    }
}

/// Drops terminal color codes, which the log view doesn't render.
func stripANSI(_ s: String) -> String {
    guard s.contains("\u{1B}") else { return s }
    return s.replacingOccurrences(of: "\u{1B}\\[[0-9;?]*[ -/]*[@-~]", with: "", options: .regularExpression)
}

// MARK: - Terminal

/// A shell in the container, in the user's terminal app: an embedded terminal
/// emulator would need a third-party package.
@MainActor
final class TerminalTabView: NSView, ContainerTabView {
    private let store: DockerStore
    private let empty = EmptyStateView()
    private let command = NSTextField(wrappingLabelWithString: "")
    private let copy = NSButton(title: "Copy Command", target: nil, action: nil)
    private var container: ContainerSummary?

    init(store: DockerStore) {
        self.store = store
        super.init(frame: .zero)
        addSubview(empty)
        pin(empty, to: self)
        command.font = .mono(11)
        command.isSelectable = true
        command.preferredMaxLayoutWidth = 360
        command.alignment = .center
        command.textColor = .secondaryLabelColor
        copy.bezelStyle = .push
        copy.target = self
        copy.action = #selector(copyCommand)
        let extra = NSStackView(views: [command, copy])
        extra.orientation = .vertical
        extra.spacing = 10
        extra.translatesAutoresizingMaskIntoConstraints = false
        addSubview(extra)
        NSLayoutConstraint.activate([
            extra.topAnchor.constraint(equalTo: empty.button.bottomAnchor, constant: 22),
            extra.centerXAnchor.constraint(equalTo: centerXAnchor),
            command.widthAnchor.constraint(lessThanOrEqualToConstant: 360),
        ])
        empty.onAction = { [weak self] in self?.open() }
    }

    required init?(coder: NSCoder) { fatalError() }

    func setActive(_ active: Bool) {}

    func show(_ c: ContainerSummary?, _ inspect: ContainerInspect?) {
        container = c
        guard let c else { return }
        if c.running {
            empty.show(symbol: "apple.terminal", title: "Shell in \(c.name)",
                       message: "Opens your terminal app with a shell inside the container (bash if it has one, else sh).",
                       action: "Open in Terminal")
        } else {
            empty.show(symbol: "apple.terminal", title: "Not Running", message: "Start the container to open a shell in it.")
        }
        command.stringValue = c.running ? ExecCommand.display(c.name) : ""
        command.isHidden = !c.running
        copy.isHidden = !c.running
    }

    private func open() {
        guard let c = container, let socket = store.socket else { return }
        do {
            try ExecCommand.open(container: c.name, socket: socket)
        } catch {
            let alert = NSAlert(error: error)
            if let w = window { alert.beginSheetModal(for: w) } else { alert.runModal() }
        }
    }

    @objc private func copyCommand() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(command.stringValue, forType: .string)
    }
}

/// `docker exec -it` against this VM's Docker, in a `.command` file the
/// system opens with the user's terminal app.
enum ExecCommand {
    static let shell = "command -v bash >/dev/null 2>&1 && exec bash || exec sh"

    static func display(_ container: String) -> String {
        "docker exec -it \(shellQuote(container)) sh -c \(shellQuote(shell))"
    }

    /// The script for `container`, with the Docker CLI's path when one was found.
    static func script(container: String, socket: String, docker: String?) -> String {
        """
        #!/bin/sh
        clear
        export DOCKER_HOST=\(shellQuote("unix://" + socket))
        exec \(shellQuote(docker ?? "docker")) exec -it \(shellQuote(container)) sh -c \(shellQuote(shell))

        """
    }

    /// The Docker CLI: on the user's PATH as a login shell would have it, or in a usual place.
    static func dockerPath() -> String? {
        let home = NSHomeDirectory()
        var dirs = (ProcessInfo.processInfo.environment["PATH"] ?? "").split(separator: ":").map(String.init)
        dirs += ["/opt/homebrew/bin", "/usr/local/bin", "\(home)/.nix-profile/bin", "/run/current-system/sw/bin",
                 "/nix/var/nix/profiles/default/bin", "\(home)/.docker/bin",
                 "/Applications/Docker.app/Contents/Resources/bin"]
        return dirs.map { ($0 as NSString).appendingPathComponent("docker") }
            .first { FileManager.default.isExecutableFile(atPath: $0) }
    }

    static func open(container: String, socket: String) throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("katamaran-exec", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let safe = container.replacingOccurrences(of: "/", with: "_")
        let file = dir.appendingPathComponent("\(safe).command")
        try script(container: container, socket: socket, docker: dockerPath()).write(to: file, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: file.path)
        NSWorkspace.shared.open(file)
    }
}

// MARK: - Files

/// The container's root filesystem as the Mac sees it, under the VM's file share.
@MainActor
final class FilesTabView: NSView, ContainerTabView, NSOutlineViewDataSource, NSOutlineViewDelegate {
    private let model: AppModel
    private let outline = NSOutlineView()
    private let scroll = NSScrollView()
    private let path = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let reveal = NSButton(title: "Open in Finder", target: nil, action: nil)
    private let empty = EmptyStateView()
    private var root: FileNode?
    private var name: String?

    init(model: AppModel) {
        self.model = model
        super.init(frame: .zero)
        let nameCol = NSTableColumn(identifier: .init("name"))
        nameCol.title = "Name"
        nameCol.width = 160
        nameCol.minWidth = 120
        let sizeCol = NSTableColumn(identifier: .init("size"))
        sizeCol.title = "Size"
        sizeCol.width = 70
        sizeCol.headerCell.alignment = .right
        let dateCol = NSTableColumn(identifier: .init("date"))
        dateCol.title = "Modified"
        dateCol.width = 130
        for c in [nameCol, sizeCol, dateCol] { outline.addTableColumn(c) }
        outline.outlineTableColumn = nameCol
        outline.columnAutoresizingStyle = .firstColumnOnlyAutoresizingStyle
        outline.style = .inset
        outline.usesAlternatingRowBackgroundColors = false
        outline.dataSource = self
        outline.delegate = self
        outline.target = self
        outline.doubleAction = #selector(openItem)
        scroll.documentView = outline
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        reveal.bezelStyle = .push
        reveal.image = NSImage(systemSymbolName: "folder", accessibilityDescription: nil)
        reveal.imagePosition = .imageLeading
        reveal.target = self
        reveal.action = #selector(openInFinder)
        path.lineBreakMode = .byTruncatingHead
        path.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let bar = NSStackView(views: [path, reveal])
        bar.spacing = 10
        bar.edgeInsets = NSEdgeInsets(top: 10, left: 20, bottom: 10, right: 20)
        bar.translatesAutoresizingMaskIntoConstraints = false
        addSubview(bar)
        addSubview(scroll)
        addSubview(empty)
        NSLayoutConstraint.activate([
            bar.topAnchor.constraint(equalTo: topAnchor),
            bar.leadingAnchor.constraint(equalTo: leadingAnchor),
            bar.trailingAnchor.constraint(equalTo: trailingAnchor),
        ])
        pin(scroll, to: self, top: bar.bottomAnchor)
        pin(empty, to: self)
    }

    required init?(coder: NSCoder) { fatalError() }

    func setActive(_ active: Bool) {
        if active, let name { load(name, force: false) }
    }

    func show(_ c: ContainerSummary?, _ inspect: ContainerInspect?) {
        guard let c else { return }
        if c.name != name || !(c.running) != (root == nil) {
            name = c.name
            load(c.name, force: true)
        }
    }

    private func directory(_ name: String) -> URL? {
        guard let root = model.status?.mount.path, model.status?.mount.mounted == true else { return nil }
        return URL(fileURLWithPath: root).appendingPathComponent("docker/containers/\(name)", isDirectory: true)
    }

    private func load(_ name: String, force: Bool) {
        guard let dir = directory(name) else {
            root = nil
            showEmpty("Files Aren’t Shared", "Turn on “Show VM files in Finder” in Settings to browse container files.")
            return
        }
        guard FileManager.default.fileExists(atPath: dir.path) else {
            root = nil
            showEmpty("Not Running", "A container’s files are here while it runs.")
            return
        }
        empty.isHidden = true
        scroll.isHidden = false
        reveal.isHidden = false
        path.isHidden = false
        path.stringValue = (dir.path as NSString).abbreviatingWithTildeInPath
        if force || root?.url != dir {
            root = FileNode(url: dir, isDirectory: true)
            outline.reloadData()
        }
    }

    private func showEmpty(_ title: String, _ message: String) {
        empty.isHidden = false
        empty.show(symbol: "folder", title: title, message: message)
        scroll.isHidden = true
        reveal.isHidden = true
        path.isHidden = true
        outline.reloadData()
    }

    @objc private func openInFinder() {
        guard let name, let dir = directory(name) else { return }
        let picked = outline.selectedRowIndexes.compactMap { (outline.item(atRow: $0) as? FileNode)?.url }
        if picked.isEmpty { NSWorkspace.shared.open(dir) } else { NSWorkspace.shared.activateFileViewerSelecting(picked) }
    }

    @objc private func openItem() {
        guard let node = outline.item(atRow: outline.clickedRow) as? FileNode else { return }
        if node.isDirectory {
            if outline.isItemExpanded(node) { outline.collapseItem(node) } else { outline.expandItem(node) }
        } else {
            NSWorkspace.shared.activateFileViewerSelecting([node.url])
        }
    }

    func outlineView(_ outlineView: NSOutlineView, numberOfChildrenOfItem item: Any?) -> Int {
        ((item as? FileNode) ?? root)?.children.count ?? 0
    }

    func outlineView(_ outlineView: NSOutlineView, child index: Int, ofItem item: Any?) -> Any {
        ((item as? FileNode) ?? root)!.children[index]
    }

    func outlineView(_ outlineView: NSOutlineView, isItemExpandable item: Any) -> Bool {
        (item as? FileNode)?.isDirectory ?? false
    }

    func outlineView(_ outlineView: NSOutlineView, viewFor tableColumn: NSTableColumn?, item: Any) -> NSView? {
        guard let node = item as? FileNode, let col = tableColumn?.identifier.rawValue else { return nil }
        let cell = NSTableCellView()
        let text = label(font: .systemFont(ofSize: NSFont.systemFontSize), color: .labelColor)
        cell.addSubview(text)
        cell.textField = text
        switch col {
        case "name":
            let icon = NSImageView(image: node.icon)
            icon.translatesAutoresizingMaskIntoConstraints = false
            cell.addSubview(icon)
            text.stringValue = node.url.lastPathComponent
            NSLayoutConstraint.activate([
                icon.leadingAnchor.constraint(equalTo: cell.leadingAnchor),
                icon.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
                icon.widthAnchor.constraint(equalToConstant: 16),
                icon.heightAnchor.constraint(equalToConstant: 16),
                text.leadingAnchor.constraint(equalTo: icon.trailingAnchor, constant: 6),
                text.trailingAnchor.constraint(lessThanOrEqualTo: cell.trailingAnchor),
                text.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
            ])
            return cell
        case "size":
            text.stringValue = node.isDirectory ? "-" : bytes(node.size)
            text.textColor = .secondaryLabelColor
            text.alignment = .right
        default:
            text.stringValue = node.modified.map { DateFormatter.localizedString(from: $0, dateStyle: .short, timeStyle: .short) } ?? ""
            text.textColor = .secondaryLabelColor
        }
        NSLayoutConstraint.activate([
            text.leadingAnchor.constraint(equalTo: cell.leadingAnchor),
            text.trailingAnchor.constraint(equalTo: cell.trailingAnchor),
            text.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
        ])
        return cell
    }
}

/// A file or directory; a directory lists its entries when first expanded.
final class FileNode: NSObject {
    let url: URL
    let isDirectory: Bool
    let size: Int64
    let modified: Date?
    private var loaded: [FileNode]?

    private static let keys: [URLResourceKey] = [.isDirectoryKey, .fileSizeKey, .contentModificationDateKey, .isSymbolicLinkKey]

    init(url: URL, isDirectory: Bool, size: Int64 = 0, modified: Date? = nil) {
        self.url = url
        self.isDirectory = isDirectory
        self.size = size
        self.modified = modified
    }

    var icon: NSImage {
        NSImage(systemSymbolName: isDirectory ? "folder.fill" : "doc", accessibilityDescription: nil) ?? NSImage()
    }

    var children: [FileNode] {
        if let loaded { return loaded }
        let urls = (try? FileManager.default.contentsOfDirectory(at: url, includingPropertiesForKeys: Self.keys)) ?? []
        let nodes = urls.map { u -> FileNode in
            let v = try? u.resourceValues(forKeys: Set(Self.keys))
            let dir = (v?.isDirectory ?? false) && !(v?.isSymbolicLink ?? false)
            return FileNode(url: u, isDirectory: dir, size: Int64(v?.fileSize ?? 0), modified: v?.contentModificationDate)
        }.sorted {
            $0.isDirectory != $1.isDirectory ? $0.isDirectory
                : $0.url.lastPathComponent.localizedStandardCompare($1.url.lastPathComponent) == .orderedAscending
        }
        loaded = nodes
        return nodes
    }
}
