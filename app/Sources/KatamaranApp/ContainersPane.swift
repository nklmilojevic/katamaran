import AppKit

/// Containers: the grouped list, and the selected container's tabs.
@MainActor
final class ContainersPane: SectionPane {
    private let store: DockerStore
    private let listVC: ContainerListViewController
    private let detailVC: ContainerDetailViewController
    var onChange: () -> Void = {}

    init(store: DockerStore, model: AppModel) {
        self.store = store
        listVC = ContainerListViewController(store: store)
        detailVC = ContainerDetailViewController(store: store, model: model)
        listVC.onSelect = { [weak self] key in
            self?.detailVC.show(key: key)
            self?.onChange()
        }
        detailVC.onTabsChanged = { [weak self] in self?.onChange() }
        detailVC.selectInList = { [weak listVC] name in listVC?.select(name: name) }
        store.observe { [weak self] _ in self?.onChange() }
    }

    var list: NSViewController { listVC }
    var detail: NSViewController? { detailVC }
    var needsDocker: Bool { true }
    var subtitle: String { store.loaded.contains(.containers) ? ContainerList.summary(store.containers) : "Loading…" }
    var tabs: [String] { ContainerTab.allCases.map(\.title) }
    var enabledTabs: Set<Int> { detailVC.enabledTabs }
    var selectedTab: Int {
        get { detailVC.tab.rawValue }
        set { detailVC.select(ContainerTab(rawValue: newValue) ?? .info) }
    }
    var pruneTitle: String? { "Delete Stopped Containers…" }

    func prune() {
        let stopped = store.containers.filter { !$0.running }
        guard !stopped.isEmpty else { return }
        confirm(listVC.view.window, title: "Delete \(stopped.count) stopped container\(stopped.count == 1 ? "" : "s")?",
                message: "Their files are deleted too. Volumes stay.", button: "Delete") { [store] in
            store.perform("Couldn’t delete stopped containers") { try $0.prune(.containers) }
        }
    }

    func search(_ text: String) { listVC.query = text }
    func setActive(_ active: Bool) { detailVC.setActive(active) }

    /// For snapshots: select a container or project by name.
    func select(name: String) { listVC.select(name: name) }
    func select(tab: ContainerTab) { detailVC.select(tab) }
}

// MARK: - List

/// A row of the outline: a section header, a project or a container.
final class ContainerNode: NSObject {
    enum Kind {
        case section(ContainerSection.Kind)
        case project(ComposeProject)
        case container(ContainerSummary)
    }
    let key: String
    var kind: Kind
    var children: [ContainerNode] = []

    init(key: String, kind: Kind) {
        self.key = key
        self.kind = kind
    }

    var isSection: Bool { if case .section = kind { true } else { false } }
}

@MainActor
final class ContainerListViewController: NSViewController, NSOutlineViewDataSource, NSOutlineViewDelegate, NSMenuDelegate {
    private let store: DockerStore
    private let outline = NSOutlineView()
    private let empty = EmptyStateView()
    private var roots: [ContainerNode] = []
    /// Nodes by key, reused so the outline keeps expansion and selection.
    private var nodes: [String: ContainerNode] = [:]
    private var collapsed: Set<String>
    private var reloading = false
    private var shown: [ContainerSection]?
    private(set) var selectedKey: String?
    var onSelect: (String?) -> Void = { _ in }
    var query = "" { didSet { if query != oldValue { reload() } } }

    private static let collapsedKey = "collapsedProjects"

    init(store: DockerStore) {
        self.store = store
        collapsed = Set(UserDefaults.standard.stringArray(forKey: Self.collapsedKey) ?? [])
        super.init(nibName: nil, bundle: nil)
        store.observe { [weak self] _ in self?.reload() }
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        let column = NSTableColumn(identifier: .init("main"))
        outline.addTableColumn(column)
        outline.outlineTableColumn = column
        outline.headerView = nil
        outline.style = .inset
        outline.rowSizeStyle = .custom
        outline.floatsGroupRows = false
        outline.indentationPerLevel = 12
        outline.autoresizesOutlineColumn = false
        outline.dataSource = self
        outline.delegate = self
        outline.target = self
        outline.doubleAction = #selector(doubleClicked)
        let menu = NSMenu()
        menu.delegate = self
        outline.menu = menu
        let scroll = NSScrollView()
        scroll.documentView = outline
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        scroll.translatesAutoresizingMaskIntoConstraints = false
        let root = NSView()
        root.addSubview(scroll)
        root.addSubview(empty)
        NSLayoutConstraint.activate([
            scroll.topAnchor.constraint(equalTo: root.topAnchor),
            scroll.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            scroll.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            scroll.bottomAnchor.constraint(equalTo: root.bottomAnchor),
            empty.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor),
            empty.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            empty.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            empty.bottomAnchor.constraint(equalTo: root.bottomAnchor),
        ])
        view = root
        reload()
    }

    // MARK: Data

    func reload() {
        guard isViewLoaded else { return }
        let sections = ContainerList.sections(store.containers, query: query)
        updateEmpty(sections)
        guard sections != shown else { refreshBusy(); return }
        shown = sections
        var seen: [String: ContainerNode] = [:]
        func node(_ key: String, _ kind: ContainerNode.Kind) -> ContainerNode {
            let n = nodes[key] ?? ContainerNode(key: key, kind: kind)
            n.kind = kind
            seen[key] = n
            return n
        }
        roots = sections.map { section in
            let s = node("section:\(section.kind.rawValue)", .section(section.kind))
            s.children = section.items.map { item in
                switch item {
                case .project(let p):
                    let n = node(item.key, .project(p))
                    n.children = p.containers.map { node("container:\($0.Id)", .container($0)) }
                    return n
                case .container(let c):
                    let n = node(item.key, .container(c))
                    n.children = []
                    return n
                }
            }
            return s
        }
        nodes = seen
        reloading = true
        outline.reloadData()
        for r in roots {
            outline.expandItem(r)
            for n in r.children where !n.children.isEmpty {
                if collapsed.contains(n.key) && query.isEmpty { outline.collapseItem(n) } else { outline.expandItem(n) }
            }
        }
        reloading = false
        restoreSelection()
    }

    private func busyAction(_ node: ContainerNode) -> ContainerAction? {
        switch node.kind {
        case .section: nil
        case .project(let p): p.containers.lazy.compactMap { self.store.busy[$0.Id] }.first
        case .container(let c): store.busy[c.Id]
        }
    }

    /// Spinners only: the rows themselves haven't changed.
    private func refreshBusy() {
        let visible = outline.rows(in: outline.visibleRect)
        for row in visible.location..<(visible.location + visible.length) {
            guard let node = outline.item(atRow: row) as? ContainerNode,
                  let cell = outline.view(atColumn: 0, row: row, makeIfNecessary: false) as? ContainerCell else { continue }
            cell.setBusy(busyAction(node))
        }
    }

    private func updateEmpty(_ sections: [ContainerSection]) {
        let loaded = store.loaded.contains(.containers)
        empty.isHidden = !sections.isEmpty || !loaded
        if store.containers.isEmpty {
            empty.show(symbol: "shippingbox", title: "No Containers",
                       message: "Containers you run with docker run or docker compose show up here.")
        } else {
            empty.show(symbol: "magnifyingglass", title: "No Results", message: "No container matches “\(query)”.")
        }
    }

    private func restoreSelection() {
        guard let key = selectedKey else { return }
        if let n = nodes[key] {
            let row = outline.row(forItem: n)
            if row >= 0 {
                reloading = true
                outline.selectRowIndexes([row], byExtendingSelection: false)
                reloading = false
            }
            onSelect(key)
        } else if query.isEmpty {
            // Deleted: nothing is selected any more.
            selectedKey = nil
            onSelect(nil)
        }
    }

    func select(name: String) {
        _ = view
        guard let n = nodes.values.first(where: {
            switch $0.kind {
            case .project(let p): p.name == name
            case .container(let c): c.name == name
            case .section: false
            }
        }) else { return }
        let row = outline.row(forItem: n)
        if row >= 0 {
            outline.selectRowIndexes([row], byExtendingSelection: false)
            outline.scrollRowToVisible(row)
        }
    }

    // MARK: Outline

    func outlineView(_ outlineView: NSOutlineView, numberOfChildrenOfItem item: Any?) -> Int {
        (item as? ContainerNode)?.children.count ?? roots.count
    }

    func outlineView(_ outlineView: NSOutlineView, child index: Int, ofItem item: Any?) -> Any {
        (item as? ContainerNode)?.children[index] ?? roots[index]
    }

    func outlineView(_ outlineView: NSOutlineView, isItemExpandable item: Any) -> Bool {
        !((item as? ContainerNode)?.children.isEmpty ?? true)
    }

    func outlineView(_ outlineView: NSOutlineView, isGroupItem item: Any) -> Bool {
        (item as? ContainerNode)?.isSection ?? false
    }

    func outlineView(_ outlineView: NSOutlineView, shouldShowOutlineCellForItem item: Any) -> Bool {
        !((item as? ContainerNode)?.isSection ?? false)
    }

    func outlineView(_ outlineView: NSOutlineView, shouldSelectItem item: Any) -> Bool {
        !((item as? ContainerNode)?.isSection ?? true)
    }

    func outlineView(_ outlineView: NSOutlineView, heightOfRowByItem item: Any) -> CGFloat {
        (item as? ContainerNode)?.isSection == true ? 30 : 46
    }

    func outlineView(_ outlineView: NSOutlineView, viewFor tableColumn: NSTableColumn?, item: Any) -> NSView? {
        guard let node = item as? ContainerNode else { return nil }
        switch node.kind {
        case .section(let kind):
            let cell = outlineView.makeView(withIdentifier: .init("section"), owner: nil) as? NSTableCellView
                ?? sectionHeader("")
            cell.identifier = .init("section")
            cell.textField?.stringValue = kind.rawValue
            return cell
        case .project(let p):
            let cell = reuse(outlineView)
            cell.show(project: p)
            cell.setBusy(busyAction(node))
            cell.onAction = { [weak self] a in self?.perform(a, on: p.containers, title: p.name, project: true) }
            return cell
        case .container(let c):
            let cell = reuse(outlineView)
            cell.show(container: c, inProject: outlineView.level(forItem: node) > 1)
            cell.setBusy(busyAction(node))
            cell.onAction = { [weak self] a in self?.perform(a, on: [c], title: c.name, project: false) }
            return cell
        }
    }

    private func reuse(_ outlineView: NSOutlineView) -> ContainerCell {
        if let c = outlineView.makeView(withIdentifier: ContainerCell.id, owner: nil) as? ContainerCell { return c }
        let c = ContainerCell()
        c.identifier = ContainerCell.id
        return c
    }

    func outlineViewSelectionDidChange(_ notification: Notification) {
        guard !reloading else { return }
        let key = (outline.item(atRow: outline.selectedRow) as? ContainerNode)?.key
        selectedKey = key
        onSelect(key)
    }

    func outlineViewItemDidCollapse(_ notification: Notification) { expansionChanged(notification, collapsed: true) }
    func outlineViewItemDidExpand(_ notification: Notification) { expansionChanged(notification, collapsed: false) }

    private func expansionChanged(_ n: Notification, collapsed c: Bool) {
        guard !reloading, query.isEmpty, let node = n.userInfo?["NSObject"] as? ContainerNode,
              case .project = node.kind else { return }
        if c { collapsed.insert(node.key) } else { collapsed.remove(node.key) }
        UserDefaults.standard.set(Array(collapsed).sorted(), forKey: Self.collapsedKey)
    }

    @objc private func doubleClicked() {
        guard let node = outline.item(atRow: outline.clickedRow) as? ContainerNode, case .project = node.kind else { return }
        if outline.isItemExpanded(node) { outline.animator().collapseItem(node) } else { outline.animator().expandItem(node) }
    }

    // MARK: Actions

    private func perform(_ action: ContainerAction, on containers: [ContainerSummary], title: String, project: Bool) {
        guard action == .remove else {
            let targets = switch action {
            case .start: containers.filter { !$0.running }
            case .stop: containers.filter(\.running)
            default: containers
            }
            store.act(action, on: targets.isEmpty ? containers : targets)
            return
        }
        let message = project
            ? "Its \(containers.count) container\(containers.count == 1 ? "" : "s") are stopped and deleted. Volumes stay."
            : (containers.first?.running == true ? "It’s running; it will be stopped and deleted." : "Its files are deleted. Volumes stay.")
        confirm(view.window, title: "Delete \(title)?", message: message, button: "Delete") { [store] in
            store.act(.remove, on: containers)
        }
    }

    func menuNeedsUpdate(_ menu: NSMenu) {
        menu.removeAllItems()
        guard let node = outline.item(atRow: outline.clickedRow) as? ContainerNode else { return }
        let containers: [ContainerSummary]
        let title: String
        switch node.kind {
        case .section: return
        case .project(let p): containers = p.containers; title = p.name
        case .container(let c): containers = [c]; title = c.name
        }
        let project = containers.count > 1 || { if case .project = node.kind { true } else { false } }()
        func add(_ t: String, _ symbol: String, _ f: @escaping () -> Void) {
            let i = ClosureMenuItem(title: t, action: f)
            i.image = NSImage(systemSymbolName: symbol, accessibilityDescription: nil)
            menu.addItem(i)
        }
        if containers.contains(where: { !$0.running }) {
            add("Start", "play.fill") { [weak self] in self?.perform(.start, on: containers, title: title, project: project) }
        }
        if containers.contains(where: \.running) {
            add("Stop", "stop.fill") { [weak self] in self?.perform(.stop, on: containers, title: title, project: project) }
            add("Restart", "arrow.clockwise") { [weak self] in self?.perform(.restart, on: containers, title: title, project: project) }
        }
        if !project, let c = containers.first {
            let ports = c.publishedPorts
            if !ports.isEmpty { menu.addItem(.separator()) }
            for port in ports {
                add("Open localhost:\(port)", "safari") { openLocalhost(port) }
            }
            menu.addItem(.separator())
            add("Copy ID", "doc.on.doc") {
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(c.Id, forType: .string)
            }
        }
        menu.addItem(.separator())
        add("Delete…", "trash") { [weak self] in self?.perform(.remove, on: containers, title: title, project: project) }
    }
}

/// A menu item that runs a closure.
final class ClosureMenuItem: NSMenuItem {
    private let run: () -> Void

    init(title: String, key: String = "", action: @escaping () -> Void) {
        run = action
        super.init(title: title, action: #selector(fire), keyEquivalent: key)
        target = self
    }

    required init(coder: NSCoder) { fatalError() }

    @objc private func fire() { run() }
}

@MainActor
func openLocalhost(_ port: UInt16) {
    if let url = URL(string: "http://localhost:\(port)") { NSWorkspace.shared.open(url) }
}

/// A project or container row: icon, name, a detail line and inline actions.
@MainActor
final class ContainerCell: NSTableCellView {
    static let id = NSUserInterfaceItemIdentifier("container")
    private let tile = IconTile(size: 28)
    private let name = label(font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium), color: .labelColor)
    private let detail = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private lazy var startButton = rowButton("play.fill", "Start", target: self, action: #selector(start))
    private lazy var stopButton = rowButton("stop.fill", "Stop", target: self, action: #selector(stop))
    private lazy var restartButton = rowButton("arrow.clockwise", "Restart", target: self, action: #selector(restart))
    private lazy var deleteButton = rowButton("trash", "Delete", target: self, action: #selector(remove))
    private let pill = StatusPill()
    private let spinner = NSProgressIndicator()
    private var buttons: NSStackView!
    private var restDetail = ""
    private var busyAction: ContainerAction?
    private var hovering = false { didSet { updateTrailing() } }
    var onAction: (ContainerAction) -> Void = { _ in }

    override var backgroundStyle: NSView.BackgroundStyle { didSet { updateTrailing() } }

    init() {
        super.init(frame: .zero)
        let text = NSStackView(views: [name, detail])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 1
        text.translatesAutoresizingMaskIntoConstraints = false
        self.buttons = NSStackView(views: [startButton, stopButton, restartButton, deleteButton])
        self.buttons.spacing = 2
        spinner.style = .spinning
        spinner.controlSize = .small
        spinner.isDisplayedWhenStopped = false
        let trailing = NSStackView(views: [pill, spinner, self.buttons!])
        trailing.spacing = 6
        trailing.translatesAutoresizingMaskIntoConstraints = false
        let buttons = trailing
        for v in [tile, text, buttons] as [NSView] { addSubview(v) }
        addTrackingArea(NSTrackingArea(rect: .zero, options: [.mouseEnteredAndExited, .activeInActiveApp, .inVisibleRect],
                                       owner: self, userInfo: nil))
        tile.wantsLayer = true
        name.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        detail.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        NSLayoutConstraint.activate([
            tile.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 2),
            tile.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.leadingAnchor.constraint(equalTo: tile.trailingAnchor, constant: 10),
            text.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.trailingAnchor.constraint(lessThanOrEqualTo: buttons.leadingAnchor, constant: -6),
            buttons.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -4),
            buttons.centerYAnchor.constraint(equalTo: centerYAnchor),
        ])
        textField = name
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(project p: ComposeProject) {
        tile.symbol = "square.stack.3d.up.fill"
        tile.tint = .systemIndigo
        tile.dimmed = !p.anyRunning
        tile.dot = nil
        name.stringValue = p.name
        name.textColor = p.anyRunning ? .labelColor : .secondaryLabelColor
        let n = p.containers.count
        detail.stringValue = "\(n) service\(n == 1 ? "" : "s")" + (p.anyRunning ? " · \(p.runningCount) running" : "")
        setButtons(running: p.anyRunning, allRunning: p.runningCount == p.containers.count)
        restDetail = detail.stringValue
        let n2 = p.containers.count
        if p.runningCount == n2 { pill.show("Running", .systemGreen) }
        else if p.anyRunning { pill.show("\(p.runningCount) of \(n2)", .systemOrange) }
        else { pill.show("Stopped", .secondaryLabelColor) }
        toolTip = p.workingDir
    }

    func show(container c: ContainerSummary, inProject: Bool = false) {
        tile.symbol = ImageIcon.symbol(for: c.Image)
        tile.tint = ImageIcon.tint(for: c.Image)
        tile.dimmed = !c.running
        tile.dot = nil
        name.stringValue = inProject ? (c.service ?? c.name) : c.name
        name.textColor = c.running ? .labelColor : .secondaryLabelColor
        var parts = [c.Image.hasPrefix("sha256:") ? String(c.Image.dropFirst(7).prefix(12)) : c.Image]
        if let p = c.publishedPorts.first { parts.append(c.publishedPorts.count > 1 ? ":\(p) +\(c.publishedPorts.count - 1)" : ":\(p)") }
        if !c.running { parts.append(c.Status) }
        detail.stringValue = parts.joined(separator: " · ")
        setButtons(running: c.running, allRunning: c.running)
        restDetail = detail.stringValue
        switch c.health {
        case .running: pill.show(inProject ? "" : "Running", .systemGreen)
        case .transitional: pill.show(c.State.capitalized, .systemYellow)
        case .failed: pill.show(c.running ? "Unhealthy" : "Failed", .systemRed)
        case .stopped: pill.show("Exited", .secondaryLabelColor)
        }
        toolTip = "\(c.name) · \(c.shortID)\n\(c.Status)"
    }

    private func setButtons(running: Bool, allRunning: Bool) {
        startButton.isHidden = allRunning
        stopButton.isHidden = !running
        restartButton.isHidden = !running
    }

    func setBusy(_ action: ContainerAction?) {
        guard action != busyAction else { return }
        busyAction = action
        if let action {
            detail.stringValue = switch action {
            case .start: "Starting…"
            case .stop: "Stopping…"
            case .restart: "Restarting…"
            case .remove: "Deleting…"
            }
            spinner.startAnimation(nil)
            let pulse = CABasicAnimation(keyPath: "opacity")
            pulse.fromValue = 1
            pulse.toValue = 0.35
            pulse.duration = 0.7
            pulse.autoreverses = true
            pulse.repeatCount = .infinity
            pulse.timingFunction = CAMediaTimingFunction(name: .easeInEaseOut)
            tile.layer?.add(pulse, forKey: "busy")
        } else {
            detail.stringValue = restDetail
            spinner.stopAnimation(nil)
            tile.layer?.removeAnimation(forKey: "busy")
            // Settle into the new state instead of snapping.
            NSAnimationContext.runAnimationGroup { ctx in
                ctx.duration = 0.25
                pill.animator().alphaValue = 1
            }
        }
        updateTrailing()
    }

    override func mouseEntered(with event: NSEvent) { hovering = true }
    override func mouseExited(with event: NSEvent) { hovering = false }

    override func prepareForReuse() {
        super.prepareForReuse()
        hovering = false
        busyAction = nil
        spinner.stopAnimation(nil)
        tile.layer?.removeAnimation(forKey: "busy")
    }

    /// Status pill at rest, actions on hover or selection, a spinner while busy.
    private func updateTrailing() {
        let busy = busyAction != nil
        let showActions = !busy && (hovering || backgroundStyle == .emphasized)
        buttons.isHidden = !showActions
        pill.isHidden = busy || showActions || pill.isEmpty
    }

    @objc private func start() { onAction(.start) }
    @objc private func stop() { onAction(.stop) }
    @objc private func restart() { onAction(.restart) }
    @objc private func remove() { onAction(.remove) }
}

/// A small tinted capsule with a status word.
@MainActor
final class StatusPill: NSView {
    private let text = NSTextField(labelWithString: "")

    init() {
        super.init(frame: .zero)
        wantsLayer = true
        layer?.cornerCurve = .continuous
        text.font = .systemFont(ofSize: NSFont.smallSystemFontSize - 1, weight: .semibold)
        text.translatesAutoresizingMaskIntoConstraints = false
        addSubview(text)
        NSLayoutConstraint.activate([
            text.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 7),
            text.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -7),
            text.topAnchor.constraint(equalTo: topAnchor, constant: 2),
            text.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -2),
        ])
        setContentHuggingPriority(.required, for: .horizontal)
        setContentCompressionResistancePriority(.required, for: .horizontal)
    }

    required init?(coder: NSCoder) { fatalError() }

    private var tint: NSColor = .secondaryLabelColor
    var isEmpty: Bool { text.stringValue.isEmpty }

    func show(_ title: String, _ color: NSColor) {
        isHidden = title.isEmpty
        text.stringValue = title
        tint = color
        text.textColor = color
        needsDisplay = true
        updateLayer()
    }

    override var wantsUpdateLayer: Bool { true }

    override func updateLayer() {
        effectiveAppearance.performAsCurrentDrawingAppearance {
            layer?.backgroundColor = tint.withAlphaComponent(0.16).cgColor
        }
        layer?.cornerRadius = bounds.height / 2
    }

    override func layout() {
        super.layout()
        layer?.cornerRadius = bounds.height / 2
    }
}
