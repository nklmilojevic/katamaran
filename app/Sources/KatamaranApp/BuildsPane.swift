import AppKit

/// What a row of the builds list shows.
struct BuildRow: Equatable {
    var id: String
    var title: String
    var detail: String
    var status: BuildStatus
    /// 0…1 while running, when the steps tell how far along it is.
    var progress: Double?
}

/// Builds: BuildKit's history, running builds on top with their progress, and
/// the selection's steps, logs and details.
@MainActor
final class BuildsPane: SectionPane {
    let store: DockerStore
    let builds: BuildStore
    private(set) lazy var listVC = BuildListViewController(pane: self)
    private(set) lazy var detailVC = BuildDetailViewController(pane: self)
    var onChange: () -> Void = {}
    var query = ""

    init(store: DockerStore, builds: BuildStore) {
        self.store = store
        self.builds = builds
        builds.follow(store.socket)
        store.observe { [weak builds] s in builds?.follow(s.socket) }
        store.onEvent = { [weak builds] e in builds?.engineEvent(e) }
        builds.onError = { [weak store] what, why in store?.onError(what, why) }
        builds.observe { [weak self] _ in
            guard let self else { return }
            self.listVC.reload()
            if let name = self.pendingSelection, self.builds.loaded { self.select(name: name) }
            self.detailVC.reload()
            self.onChange()
        }
    }

    var list: NSViewController { listVC }
    var detail: NSViewController? { detailVC }
    var needsDocker: Bool { true }

    var subtitle: String {
        guard builds.loaded else { return "Loading…" }
        if builds.loadError != nil && builds.records.isEmpty { return "Unavailable" }
        return BuildList.summary(builds.records, status: builds.status)
    }

    var tabs: [String] { ["Info", "Logs"] }
    var enabledTabs: Set<Int> { detailVC.id == nil ? [] : [0, 1] }
    var selectedTab: Int {
        get { detailVC.tab }
        set { detailVC.tab = newValue }
    }

    var pruneTitle: String? { "Clear Build History…" }

    func prune() {
        let done = builds.records.filter { !$0.isRunning }
        guard !done.isEmpty else { return }
        confirm(listVC.view.window, title: "Clear the history of \(done.count) build\(done.count == 1 ? "" : "s")?",
                message: "Their logs and details are deleted. Images and the build cache stay.", button: "Clear") { [builds] in
            builds.removeAll()
        }
    }

    func search(_ text: String) {
        query = text
        listVC.reload()
    }

    func setActive(_ active: Bool) { builds.visible = active }

    var sections: [BuildSection] { BuildList.sections(builds.records, query: query, status: builds.status) }

    func row(_ r: BuildRecord) -> BuildRow {
        let status = builds.status(r)
        let info = builds.infos[r.id]
        let target = r.target ?? info?.Target.flatMap { $0.isEmpty || $0 == "default" ? nil : $0 }
        if status == .running {
            var parts: [String] = []
            var progress: Double? = nil
            if let log = builds.log(r.id) {
                let p = log.parser
                if let step = p.current {
                    let pos = step.parts.position.map { "\($0) " } ?? ""
                    parts.append(step.isInternal ? "Preparing" : pos + step.parts.instruction)
                }
                if let pr = p.progress, pr.total > 0 { progress = Double(pr.done) / Double(pr.total) }
            }
            if parts.isEmpty { parts.append("Starting") }
            if let d = r.duration { parts.insert(elapsedClock(d), at: 0) }
            return BuildRow(id: r.id, title: r.title, detail: parts.joined(separator: " · "), status: status,
                            progress: progress ?? 0)
        }
        var parts: [String] = []
        if let target { parts.append(target) }
        if let d = r.duration { parts.append(buildDuration(d)) }
        if r.cachedSteps > 0 { parts.append("\(r.cachedSteps)/\(r.totalSteps) cached") }
        parts.append(shortRelative(r.createdAt))
        return BuildRow(id: r.id, title: r.title, detail: parts.joined(separator: " · "), status: status, progress: nil)
    }

    func confirmDelete(_ id: String) {
        guard let r = builds.record(id), !r.isRunning else { return }
        confirm(listVC.view.window, title: "Delete the record of \(r.title)?",
                message: "Its logs and details are deleted. The image and the build cache stay.", button: "Delete") { [builds] in
            builds.remove([id])
        }
    }

    func copyID(_ id: String) {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(id, forType: .string)
    }

    /// Compose projects' directories, for builds whose record has no context.
    var projectDirs: [String: String] {
        var out: [String: String] = [:]
        for c in store.containers {
            if let p = c.project, let d = c.Labels?[ComposeLabel.workingDir] { out[p] = d }
        }
        return out
    }

    /// For snapshots: select a build by name or ID.
    func select(name: String) {
        let all = sections.flatMap(\.builds)
        if let r = all.first(where: { $0.id == name || $0.title == name || $0.name == name }) {
            pendingSelection = nil
            listVC.select(id: r.id)
        } else if !builds.loaded {
            pendingSelection = name
        }
    }

    /// A snapshot's selection, waiting for the history to load.
    private var pendingSelection: String?
}

extension BuildStatus {
    var color: NSColor {
        switch self {
        case .running: .systemBlue
        case .completed: .systemGreen
        case .failed: .systemRed
        case .canceled: .secondaryLabelColor
        }
    }

    var symbol: String {
        switch self {
        case .running: "hammer.fill"
        case .completed: "checkmark"
        case .failed: "xmark"
        case .canceled: "minus"
        }
    }
}

// MARK: - List

@MainActor
final class BuildListViewController: NSViewController, NSTableViewDataSource, NSTableViewDelegate, NSMenuDelegate {
    private unowned let pane: BuildsPane
    private let table = NSTableView()
    private let empty = EmptyStateView()
    private enum Row: Equatable { case header(String), item(BuildRow) }
    private var rows: [Row] = []
    private(set) var selectedID: String?
    var onSelect: (String?) -> Void = { _ in }
    private var reloading = false

    init(pane: BuildsPane) {
        self.pane = pane
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        let column = NSTableColumn(identifier: .init("main"))
        table.addTableColumn(column)
        table.headerView = nil
        table.style = .inset
        table.rowSizeStyle = .custom
        table.dataSource = self
        table.delegate = self
        let menu = NSMenu()
        menu.delegate = self
        table.menu = menu
        let scroll = NSScrollView()
        scroll.documentView = table
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        let root = NSView()
        root.addSubview(scroll)
        root.addSubview(empty)
        pin(scroll, to: root)
        pin(empty, to: root, top: root.safeAreaLayoutGuide.topAnchor)
        view = root
        reload()
    }

    func reload() {
        guard isViewLoaded else { return }
        let b = pane.builds
        var next: [Row] = []
        for s in pane.sections {
            next.append(.header(s.kind.rawValue))
            next += s.builds.map { .item(pane.row($0)) }
        }
        empty.isHidden = !next.isEmpty || !b.loaded
        if let err = b.loadError, b.records.isEmpty {
            empty.show(symbol: "exclamationmark.triangle", title: "Can’t Read Builds", message: err)
        } else if b.records.isEmpty {
            empty.show(symbol: "hammer", title: "No Builds",
                       message: "Images you build with docker build or Compose show up here, step by step while they run.")
        } else {
            empty.show(symbol: "magnifyingglass", title: "No Results", message: "Nothing matches “\(pane.query)”.")
        }
        guard next != rows else { return }
        reloading = true
        // Same rows in the same places: refresh them in place, keeping hover and selection.
        if next.count == rows.count, zip(next, rows).allSatisfy({ a, b in key(a) == key(b) }) {
            for (i, (n, o)) in zip(next, rows).enumerated() where n != o {
                if case .item(let r) = n, let cell = table.view(atColumn: 0, row: i, makeIfNecessary: false) as? BuildCell {
                    cell.show(r)
                }
            }
            rows = next
        } else {
            rows = next
            table.reloadData()
            if let id = selectedID, let i = index(of: id) {
                table.selectRowIndexes([i], byExtendingSelection: false)
            } else if selectedID != nil, pane.query.isEmpty {
                selectedID = nil
                onSelect(nil)
            }
        }
        reloading = false
    }

    private func key(_ r: Row) -> String {
        switch r {
        case .header(let t): "h:" + t
        case .item(let b): "b:" + b.id
        }
    }

    private func index(of id: String) -> Int? {
        rows.firstIndex { if case .item(let r) = $0 { r.id == id } else { false } }
    }

    func select(id: String) {
        _ = view
        guard let i = index(of: id) else { return }
        table.selectRowIndexes([i], byExtendingSelection: false)
        table.scrollRowToVisible(i)
    }

    func numberOfRows(in tableView: NSTableView) -> Int { rows.count }

    func tableView(_ tableView: NSTableView, isGroupRow row: Int) -> Bool {
        if case .header = rows[row] { true } else { false }
    }

    func tableView(_ tableView: NSTableView, shouldSelectRow row: Int) -> Bool {
        if case .item = rows[row] { true } else { false }
    }

    func tableView(_ tableView: NSTableView, heightOfRow row: Int) -> CGFloat {
        if case .header = rows[row] { 30 } else { 54 }
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        switch rows[row] {
        case .header(let title):
            let cell = tableView.makeView(withIdentifier: .init("section"), owner: nil) as? NSTableCellView ?? sectionHeader("")
            cell.identifier = .init("section")
            cell.textField?.stringValue = title
            return cell
        case .item(let r):
            let cell = tableView.makeView(withIdentifier: BuildCell.id, owner: nil) as? BuildCell ?? BuildCell()
            cell.identifier = BuildCell.id
            cell.show(r)
            cell.onDelete = { [weak self] in self?.pane.confirmDelete(r.id) }
            cell.onCopy = { [weak self] in self?.pane.copyID(r.id) }
            return cell
        }
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard !reloading else { return }
        if case .item(let item) = rows[safe: table.selectedRow] { selectedID = item.id } else { selectedID = nil }
        onSelect(selectedID)
    }

    func menuNeedsUpdate(_ menu: NSMenu) {
        menu.removeAllItems()
        guard case .item(let r) = rows[safe: table.clickedRow] else { return }
        func add(_ t: String, _ symbol: String, _ f: @escaping () -> Void) {
            let i = ClosureMenuItem(title: t, action: f)
            i.image = NSImage(systemSymbolName: symbol, accessibilityDescription: nil)
            menu.addItem(i)
        }
        add("Copy Build ID", "doc.on.doc") { [weak self] in self?.pane.copyID(r.id) }
        if let socket = pane.builds.socket {
            add("Copy Logs Command", "terminal") {
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(BuildCommands.logs(r.id, socket: socket), forType: .string)
            }
        }
        if r.status != .running {
            menu.addItem(.separator())
            add("Delete Record…", "trash") { [weak self] in self?.pane.confirmDelete(r.id) }
        }
    }
}

/// A build in the list: icon, name, a detail line, and a status pill that
/// gives way to actions on hover. Running builds pulse and show their progress.
@MainActor
final class BuildCell: NSTableCellView {
    static let id = NSUserInterfaceItemIdentifier("build")
    private let tile = IconTile(size: 28)
    private let name = label(font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium), color: .labelColor)
    private let detail = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let bar = ProgressLine()
    private let pill = StatusPill()
    private lazy var copyButton = rowButton("doc.on.doc", "Copy Build ID", target: self, action: #selector(doCopy))
    private lazy var deleteButton = rowButton("trash", "Delete Record", target: self, action: #selector(doDelete))
    private var buttons: NSStackView!
    private var hovering = false { didSet { updateTrailing() } }
    private var shown: BuildRow?
    var onDelete: () -> Void = {}
    var onCopy: () -> Void = {}

    override var backgroundStyle: NSView.BackgroundStyle { didSet { updateTrailing() } }

    init() {
        super.init(frame: .zero)
        let text = NSStackView(views: [name, detail, bar])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 2
        text.setCustomSpacing(5, after: detail)
        text.translatesAutoresizingMaskIntoConstraints = false
        buttons = NSStackView(views: [copyButton, deleteButton])
        buttons.spacing = 2
        let trailing = NSStackView(views: [pill, buttons])
        trailing.spacing = 6
        trailing.translatesAutoresizingMaskIntoConstraints = false
        for v in [tile, text, trailing] as [NSView] { addSubview(v) }
        addTrackingArea(NSTrackingArea(rect: .zero, options: [.mouseEnteredAndExited, .activeInActiveApp, .inVisibleRect],
                                       owner: self, userInfo: nil))
        tile.wantsLayer = true
        name.lineBreakMode = .byTruncatingMiddle
        name.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        detail.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        NSLayoutConstraint.activate([
            tile.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 2),
            tile.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.leadingAnchor.constraint(equalTo: tile.trailingAnchor, constant: 10),
            text.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.trailingAnchor.constraint(lessThanOrEqualTo: trailing.leadingAnchor, constant: -8),
            bar.widthAnchor.constraint(equalTo: text.widthAnchor),
            bar.heightAnchor.constraint(equalToConstant: 3),
            trailing.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -4),
            trailing.centerYAnchor.constraint(equalTo: centerYAnchor),
        ])
        // The bar spans the text column, not just the longest label.
        let wide = text.widthAnchor.constraint(equalToConstant: 2000)
        wide.priority = .defaultLow
        wide.isActive = true
        textField = name
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(_ r: BuildRow) {
        let was = shown
        shown = r
        let running = r.status == .running
        tile.symbol = "hammer.fill"
        tile.tint = running ? .systemBlue : ImageIcon.tint(for: r.title)
        tile.dimmed = r.status == .canceled
        tile.dot = running ? nil : r.status == .completed ? nil : r.status.color
        name.stringValue = r.title
        detail.stringValue = r.detail
        bar.isHidden = !running
        bar.value = r.progress ?? 0
        pill.show(r.status.title, r.status.color)
        deleteButton.isHidden = running
        toolTip = "\(r.title)\n\(r.id)"
        if running != (was?.status == .running) || was == nil { pulse(running) }
        updateTrailing()
    }

    private func pulse(_ on: Bool) {
        tile.layer?.removeAnimation(forKey: "running")
        guard on else { return }
        let a = CABasicAnimation(keyPath: "opacity")
        a.fromValue = 1
        a.toValue = 0.45
        a.duration = 0.9
        a.autoreverses = true
        a.repeatCount = .infinity
        a.timingFunction = CAMediaTimingFunction(name: .easeInEaseOut)
        tile.layer?.add(a, forKey: "running")
    }

    override func mouseEntered(with event: NSEvent) { hovering = true }
    override func mouseExited(with event: NSEvent) { hovering = false }

    override func prepareForReuse() {
        super.prepareForReuse()
        hovering = false
        shown = nil
        tile.layer?.removeAnimation(forKey: "running")
    }

    private func updateTrailing() {
        let actions = hovering || backgroundStyle == .emphasized
        buttons.isHidden = !actions
        pill.isHidden = actions
    }

    @objc private func doCopy() { onCopy() }
    @objc private func doDelete() { onDelete() }
}

/// A thin rounded progress line in the accent color.
final class ProgressLine: NSView {
    var value: Double = 0 { didSet { if value != oldValue { needsDisplay = true } } }
    var color: NSColor = .systemBlue { didSet { needsDisplay = true } }

    override init(frame: NSRect) {
        super.init(frame: frame)
        translatesAutoresizingMaskIntoConstraints = false
    }

    required init?(coder: NSCoder) { fatalError() }

    override func draw(_ dirtyRect: NSRect) {
        let r = bounds
        NSColor.quaternaryLabelColor.setFill()
        NSBezierPath(roundedRect: r, xRadius: r.height / 2, yRadius: r.height / 2).fill()
        let w = max(r.height, r.width * CGFloat(min(max(value, 0), 1)))
        guard value > 0 else { return }
        color.setFill()
        NSBezierPath(roundedRect: NSRect(x: 0, y: 0, width: w, height: r.height), xRadius: r.height / 2, yRadius: r.height / 2).fill()
    }

    override func viewDidChangeEffectiveAppearance() { needsDisplay = true }
}

// MARK: - Detail

@MainActor
final class BuildDetailViewController: NSViewController, NSSearchFieldDelegate {
    private unowned let pane: BuildsPane
    private(set) var id: String?
    private let header = NSView()
    private let separator = NSBox()
    private let tile = IconTile(size: 36)
    private let name = label(font: .systemFont(ofSize: 15, weight: .semibold), color: .labelColor)
    private let sub = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let copyButton = NSButton()
    private let revealButton = NSButton()
    private let deleteButton = NSButton()
    private let content = ScrollingStack()
    private let empty = EmptyStateView()
    private let logsView = NSView()
    private let logText = LogTextView()
    private let filter = NSSearchField()
    private let follow = NSButton(checkboxWithTitle: "Follow", target: nil, action: nil)
    private var shownLines = 0
    private var shownLog: BuildLog?
    private var signature = ""
    private var liveCard: RunningCard?
    private var dockerfile: URL?
    var tab = 0 { didSet { if tab != oldValue { showTab() } } }

    init(pane: BuildsPane) {
        self.pane = pane
        super.init(nibName: nil, bundle: nil)
        pane.listVC.onSelect = { [weak self] id in
            guard let self else { return }
            self.id = id
            self.signature = ""
            self.shownLog = nil
            self.filter.stringValue = ""
            self.logText.filter = ""
            self.reload()
            self.pane.onChange()
        }
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        let root = NSView()
        for (b, symbol, tip, sel) in [
            (copyButton, "doc.on.doc", "Copy Build ID", #selector(doCopy)),
            (revealButton, "folder", "Show Dockerfile in Finder", #selector(doReveal)),
            (deleteButton, "trash", "Delete Record", #selector(doDelete)),
        ] {
            b.image = NSImage(systemSymbolName: symbol, accessibilityDescription: tip)
            b.bezelStyle = .push
            b.toolTip = tip
            b.target = self
            b.action = sel
        }
        name.isSelectable = true
        name.lineBreakMode = .byTruncatingMiddle
        name.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        sub.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let text = NSStackView(views: [name, sub])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 2
        let buttons = NSStackView(views: [copyButton, revealButton, deleteButton])
        buttons.spacing = 6
        header.translatesAutoresizingMaskIntoConstraints = false
        for v in [tile, text, buttons] as [NSView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            header.addSubview(v)
        }
        separator.boxType = .separator
        separator.translatesAutoresizingMaskIntoConstraints = false

        filter.placeholderString = "Filter"
        filter.delegate = self
        filter.sendsWholeSearchString = false
        follow.state = .on
        follow.target = self
        follow.action = #selector(followChanged)
        let bar = NSStackView(views: [filter, follow])
        bar.spacing = 10
        bar.edgeInsets = NSEdgeInsets(top: 10, left: 20, bottom: 10, right: 20)
        bar.translatesAutoresizingMaskIntoConstraints = false
        filter.widthAnchor.constraint(greaterThanOrEqualToConstant: 160).isActive = true
        filter.setContentHuggingPriority(.defaultLow, for: .horizontal)
        logsView.translatesAutoresizingMaskIntoConstraints = false
        logsView.addSubview(bar)
        logsView.addSubview(logText.scroll)
        NSLayoutConstraint.activate([
            bar.topAnchor.constraint(equalTo: logsView.topAnchor),
            bar.leadingAnchor.constraint(equalTo: logsView.leadingAnchor),
            bar.trailingAnchor.constraint(equalTo: logsView.trailingAnchor),
        ])
        pin(logText.scroll, to: logsView, top: bar.bottomAnchor)

        for v in [header, separator, content.scroll, logsView, empty] as [NSView] { root.addSubview(v) }
        NSLayoutConstraint.activate([
            header.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor),
            header.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            header.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            header.heightAnchor.constraint(equalToConstant: 64),
            tile.leadingAnchor.constraint(equalTo: header.leadingAnchor, constant: 20),
            tile.centerYAnchor.constraint(equalTo: header.centerYAnchor),
            text.leadingAnchor.constraint(equalTo: tile.trailingAnchor, constant: 12),
            text.centerYAnchor.constraint(equalTo: header.centerYAnchor),
            text.trailingAnchor.constraint(lessThanOrEqualTo: buttons.leadingAnchor, constant: -12),
            buttons.trailingAnchor.constraint(equalTo: header.trailingAnchor, constant: -20),
            buttons.centerYAnchor.constraint(equalTo: header.centerYAnchor),
            separator.topAnchor.constraint(equalTo: header.bottomAnchor),
            separator.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            separator.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            separator.heightAnchor.constraint(equalToConstant: 1),
        ])
        pin(content.scroll, to: root, top: separator.bottomAnchor)
        pin(logsView, to: root, top: separator.bottomAnchor)
        pin(empty, to: root, top: root.safeAreaLayoutGuide.topAnchor)
        empty.show(symbol: nil, title: "No Selection")
        view = root
        reload()
    }

    private func showTab() {
        guard isViewLoaded else { return }
        let has = id.flatMap(pane.builds.record) != nil
        content.scroll.isHidden = !has || tab != 0
        logsView.isHidden = !has || tab != 1
        if tab == 1 { reloadLogs() }
        pane.onChange()
    }

    func reload() {
        guard isViewLoaded else { return }
        let builds = pane.builds
        let record = id.flatMap(builds.record)
        empty.isHidden = record != nil
        header.isHidden = record == nil
        separator.isHidden = record == nil
        content.scroll.isHidden = record == nil || tab != 0
        logsView.isHidden = record == nil || tab != 1
        guard let record else { return }
        let status = builds.status(record)
        let info = builds.info(record.id)
        let log = builds.log(record.id)
        let running = status == .running

        tile.symbol = "hammer.fill"
        tile.tint = running ? .systemBlue : ImageIcon.tint(for: record.title)
        tile.dimmed = status == .canceled
        tile.dot = status == .completed || running ? nil : status.color
        name.stringValue = record.title
        var subParts = [status.title]
        if let d = record.duration { subParts.append(running ? elapsedClock(d) : buildDuration(d)) }
        if let c = record.createdAt { subParts.append(absolute(c)) }
        sub.stringValue = subParts.joined(separator: " · ")
        dockerfile = info?.dockerfileURL(projectDirs: pane.projectDirs)
        revealButton.isHidden = dockerfile == nil
        deleteButton.isEnabled = !running

        // Rebuilding the groups would reset the scroll position for nothing.
        let steps = log?.parser.steps ?? []
        let sig = [status.rawValue, info == nil ? "-" : "i", log?.finished == true ? "f" : "",
                   steps.map { "\($0.number)\($0.state)\($0.duration ?? -1)" }.joined(separator: ","),
                   "\(record.cachedSteps)/\(record.totalSteps)", dockerfile?.path ?? ""].joined(separator: "|")
        if sig != signature {
            signature = sig
            content.set(groups(record, status: status, info: info, log: log))
        }
        liveCard?.update(record: record, log: log)
        if tab == 1 { reloadLogs() }
    }

    // MARK: Info

    private func groups(_ r: BuildRecord, status: BuildStatus, info: BuildInfo?, log: BuildLog?) -> [NSView] {
        var out: [NSView] = []
        let parser = log?.parser
        liveCard = nil
        if status == .running {
            let card = RunningCard()
            card.update(record: r, log: log)
            liveCard = card
            out.append(card)
        }
        if status == .failed || (status == .canceled && info?.Error?.Message != nil && info?.Error?.Message != "context canceled") {
            out.append(ErrorCard(info?.Error, step: parser?.failed, status: status))
        }

        var rows: [NSView] = []
        if let f = info?.Dockerfile {
            if let url = dockerfile {
                rows.append(InfoRow("Dockerfile", view: pathLink((url.path as NSString).abbreviatingWithTildeInPath)))
            } else {
                rows.append(InfoRow("Dockerfile", f, mono: true))
            }
        }
        if let c = info?.contextDisplay { rows.append(InfoRow("Context", c, mono: true)) }
        if let t = r.target ?? info?.Target, !t.isEmpty { rows.append(InfoRow("Target", t)) }
        if let p = info?.platforms, !p.isEmpty { rows.append(InfoRow("Platforms", p.joined(separator: ", "))) }
        let output = BuildOutput(steps: parser?.steps ?? [])
        if !output.tags.isEmpty { rows.append(InfoRow("Tags", output.tags.joined(separator: "\n"), mono: true)) }
        if let k = output.kind { rows.append(InfoRow("Output", k)) }
        if let base = info?.baseImages, !base.isEmpty { rows.append(InfoRow("Base Images", base.joined(separator: "\n"), mono: true)) }
        if status != .running {
            let total = info?.NumTotalSteps ?? r.totalSteps
            let cached = info?.NumCachedSteps ?? r.cachedSteps
            if total > 0 {
                let pct = Int((Double(cached) / Double(total) * 100).rounded())
                rows.append(InfoRow("Cache", "\(cached) of \(total) steps cached (\(pct)%)"))
            }
            if let d = info?.seconds ?? r.duration { rows.append(InfoRow("Duration", buildDuration(d))) }
        }
        if let c = r.createdAt { rows.append(InfoRow("Started", "\(absolute(c)) (\(relative(c)))")) }
        if let repo = info?.VCSRepository {
            let rev = info?.VCSRevision.map { " @ " + String($0.prefix(12)) } ?? ""
            rows.append(InfoRow("Source", repo + rev, mono: true))
        }
        if let p = info?.composeProject {
            rows.append(InfoRow("Compose", [p, info?.composeService].compactMap { $0 }.joined(separator: " · ")))
        }
        rows.append(InfoRow("Build ID", r.id, mono: true))
        out.append(FormGroup(header: "Build", rows: rows))

        if let steps = parser?.steps, !steps.isEmpty {
            out.append(StepTimeline(steps: steps, running: status == .running) { [weak self] n in self?.showLogs(step: n) })
        } else if log?.finished == false || log == nil {
            out.append(FormGroup(header: "Steps", rows: [InfoRow("", view: spinnerLabel("Reading steps…"), keyWidth: 0)]))
        } else if let e = log?.error {
            out.append(FormGroup(header: "Steps", rows: [InfoRow("", e)]))
        }

        if let args = info?.BuildArgs, !args.isEmpty {
            out.append(FormGroup(header: "Build Arguments",
                                 footer: args.contains { BuildSecrets.isSecret($0.Name) } ? "Values that look like credentials are hidden." : nil,
                                 rows: args.map { InfoRow($0.Name, BuildSecrets.display($0), mono: true) }))
        }
        if let labels = info?.Labels, !labels.isEmpty {
            out.append(FormGroup(header: "Labels", rows: labels.map { InfoRow($0.Name, $0.Value ?? "", mono: true) }))
        }
        if let socket = pane.builds.socket {
            var cmds: [NSView] = [
                CommandRow("Logs", BuildCommands.logs(r.id, socket: socket)),
                CommandRow("Inspect", BuildCommands.inspect(r.id, socket: socket)),
            ]
            if status != .running, let info, let cmd = BuildCommands.rebuild(info, tags: output.tags, socket: socket) {
                cmds.append(CommandRow("Rebuild", cmd))
            }
            out.append(FormGroup(header: "Commands",
                                 footer: status == .running ? nil : "Run the same build again from Terminal; credentials come from your shell’s environment.",
                                 rows: cmds))
        }
        return out
    }

    private func spinnerLabel(_ text: String) -> NSView {
        let s = NSProgressIndicator()
        s.style = .spinning
        s.controlSize = .small
        s.startAnimation(nil)
        let l = label(text, font: .systemFont(ofSize: NSFont.systemFontSize), color: .secondaryLabelColor)
        let v = NSStackView(views: [s, l])
        v.spacing = 8
        return v
    }

    private func pathLink(_ path: String) -> NSView {
        let b = linkButton(path, target: self, action: #selector(doReveal))
        b.lineBreakMode = .byTruncatingMiddle
        b.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        return b
    }

    // MARK: Logs

    private func showLogs(step: Int) {
        filter.stringValue = "#\(step) "
        logText.filter = filter.stringValue
        tab = 1
        pane.onChange()
    }

    private func reloadLogs() {
        guard let id, let log = pane.builds.log(id) else { return }
        if log !== shownLog || log.lines.count < shownLines {
            logText.clear()
            shownLog = log
            shownLines = 0
        }
        guard log.lines.count > shownLines else { return }
        let new = Array(log.lines[shownLines...])
        shownLines = log.lines.count
        logText.append(new, error: false)
        if follow.state == .on { logText.scrollToEnd() }
    }

    func controlTextDidChange(_ obj: Notification) { logText.filter = filter.stringValue }

    @objc private func followChanged() { if follow.state == .on { logText.scrollToEnd() } }

    // MARK: Actions

    @objc private func doCopy() { if let id { pane.copyID(id) } }
    @objc private func doDelete() { if let id { pane.confirmDelete(id) } }
    @objc private func doReveal() {
        if let url = dockerfile { NSWorkspace.shared.activateFileViewerSelecting([url]) }
    }
}

/// A running build's progress at the top of its details.
@MainActor
final class RunningCard: NSBox {
    private let heading = label(font: .systemFont(ofSize: NSFont.systemFontSize, weight: .semibold), color: .labelColor)
    private let step = label(font: .mono(NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let clock = label(font: .monospacedDigitSystemFont(ofSize: 20, weight: .semibold), color: .labelColor)
    private let bar = ProgressLine()

    init() {
        super.init(frame: .zero)
        boxType = .custom
        borderWidth = 1
        borderColor = NSColor.systemBlue.withAlphaComponent(0.35)
        fillColor = NSColor.systemBlue.withAlphaComponent(0.07)
        cornerRadius = FormLayout.cornerRadius
        contentViewMargins = .zero
        titlePosition = .noTitle
        translatesAutoresizingMaskIntoConstraints = false
        let spinner = NSProgressIndicator()
        spinner.style = .spinning
        spinner.controlSize = .small
        spinner.startAnimation(nil)
        let head = NSStackView(views: [spinner, heading])
        head.spacing = 8
        step.lineBreakMode = .byTruncatingMiddle
        step.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let c = NSView()
        for v in [head, step, clock, bar] as [NSView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            c.addSubview(v)
        }
        NSLayoutConstraint.activate([
            head.leadingAnchor.constraint(equalTo: c.leadingAnchor, constant: 14),
            head.topAnchor.constraint(equalTo: c.topAnchor, constant: 12),
            clock.trailingAnchor.constraint(equalTo: c.trailingAnchor, constant: -14),
            clock.centerYAnchor.constraint(equalTo: head.centerYAnchor, constant: 6),
            step.leadingAnchor.constraint(equalTo: head.leadingAnchor),
            step.topAnchor.constraint(equalTo: head.bottomAnchor, constant: 4),
            step.trailingAnchor.constraint(lessThanOrEqualTo: clock.leadingAnchor, constant: -12),
            bar.leadingAnchor.constraint(equalTo: c.leadingAnchor, constant: 14),
            bar.trailingAnchor.constraint(equalTo: c.trailingAnchor, constant: -14),
            bar.topAnchor.constraint(equalTo: step.bottomAnchor, constant: 10),
            bar.heightAnchor.constraint(equalToConstant: 4),
            bar.bottomAnchor.constraint(equalTo: c.bottomAnchor, constant: -14),
        ])
        contentView = c
    }

    required init?(coder: NSCoder) { fatalError() }

    func update(record: BuildRecord, log: BuildLog?) {
        clock.stringValue = record.duration.map(elapsedClock) ?? ""
        let p = log?.parser
        if let pr = p?.progress {
            heading.stringValue = "Step \(min(pr.done + 1, pr.total)) of \(pr.total)"
            bar.value = Double(pr.done) / Double(max(pr.total, 1))
        } else {
            heading.stringValue = "Building…"
            bar.value = 0
        }
        if let s = p?.current {
            let last = s.logs.last { !$0.isEmpty }.map { " — " + $0 } ?? ""
            step.stringValue = (s.isInternal ? s.name : s.parts.instruction) + last
        } else {
            step.stringValue = "Waiting for BuildKit"
        }
    }
}

/// Why a build failed: the step, BuildKit's message, the step's last output
/// and the Dockerfile lines around it.
@MainActor
final class ErrorCard: NSBox {
    init(_ error: BuildInfo.BuildError?, step: BuildStep?, status: BuildStatus) {
        super.init(frame: .zero)
        boxType = .custom
        borderWidth = 1
        borderColor = NSColor.systemRed.withAlphaComponent(0.4)
        fillColor = NSColor.systemRed.withAlphaComponent(0.07)
        cornerRadius = FormLayout.cornerRadius
        contentViewMargins = .zero
        titlePosition = .noTitle
        translatesAutoresizingMaskIntoConstraints = false

        let icon = NSImageView(image: NSImage(systemSymbolName: "xmark.octagon.fill", accessibilityDescription: nil) ?? NSImage())
        icon.contentTintColor = .systemRed
        icon.symbolConfiguration = .init(pointSize: 14, weight: .semibold)
        let where_ = error?.Name ?? step?.name
        let title = label(where_.map { "Failed at \($0)" } ?? (status == .canceled ? "Canceled" : "Build failed"),
                          font: .systemFont(ofSize: NSFont.systemFontSize, weight: .semibold), color: .labelColor)
        title.lineBreakMode = .byTruncatingMiddle
        title.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let head = NSStackView(views: [icon, title])
        head.spacing = 8

        var views: [NSView] = [head]
        if let m = error?.Message ?? step?.error {
            let msg = NSTextField(wrappingLabelWithString: m)
            msg.isSelectable = true
            msg.font = .systemFont(ofSize: NSFont.systemFontSize)
            msg.textColor = .labelColor
            views.append(msg)
        }
        let logs = (error?.Logs?.isEmpty == false ? error?.Logs : nil) ?? step.map { Array($0.logs.suffix(12)) } ?? []
        if !logs.isEmpty { views.append(Self.block(logs.suffix(12).joined(separator: "\n"), title: "Output")) }
        if let src = error?.source { views.append(Self.block(src, title: "Dockerfile")) }
        let stack = NSStackView(views: views)
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 8
        stack.translatesAutoresizingMaskIntoConstraints = false
        let c = NSView()
        c.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: c.leadingAnchor, constant: 14),
            stack.trailingAnchor.constraint(equalTo: c.trailingAnchor, constant: -14),
            stack.topAnchor.constraint(equalTo: c.topAnchor, constant: 12),
            stack.bottomAnchor.constraint(equalTo: c.bottomAnchor, constant: -12),
        ])
        for v in views.dropFirst() { v.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true }
        contentView = c
    }

    required init?(coder: NSCoder) { fatalError() }

    /// Monospaced lines on the window's text background.
    private static func block(_ text: String, title: String) -> NSView {
        let t = NSTextField(wrappingLabelWithString: text)
        t.isSelectable = true
        t.font = .mono(11)
        t.textColor = .labelColor
        t.translatesAutoresizingMaskIntoConstraints = false
        let box = NSBox()
        box.boxType = .custom
        box.borderWidth = 0
        box.fillColor = NSColor(name: nil) { a in
            a.bestMatch(from: [.darkAqua, .aqua]) == .darkAqua ? NSColor(white: 0, alpha: 0.35) : NSColor(white: 1, alpha: 0.75)
        }
        box.cornerRadius = 6
        box.titlePosition = .noTitle
        box.contentViewMargins = NSSize(width: 10, height: 8)
        box.translatesAutoresizingMaskIntoConstraints = false
        let inner = NSView()
        inner.addSubview(t)
        pin(t, to: inner)
        box.contentView = inner
        let cap = label(title.uppercased(), font: .systemFont(ofSize: 10, weight: .semibold), color: .secondaryLabelColor)
        let v = NSStackView(views: [cap, box])
        v.orientation = .vertical
        v.alignment = .leading
        v.spacing = 4
        box.widthAnchor.constraint(equalTo: v.widthAnchor).isActive = true
        return v
    }
}

/// A copyable command line in a details group.
@MainActor
final class CommandRow: NSView {
    private let command: String

    init(_ title: String, _ command: String) {
        self.command = command
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        let k = label(title, font: .systemFont(ofSize: NSFont.systemFontSize), color: .secondaryLabelColor)
        let v = NSTextField(wrappingLabelWithString: command)
        v.isSelectable = true
        v.font = .mono(NSFont.smallSystemFontSize)
        v.textColor = .labelColor
        v.preferredMaxLayoutWidth = 0
        v.translatesAutoresizingMaskIntoConstraints = false
        v.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let copy = rowButton("doc.on.doc", "Copy", target: self, action: #selector(doCopy))
        for x in [k, v, copy] as [NSView] { addSubview(x) }
        let inset = FormLayout.rowInset
        NSLayoutConstraint.activate([
            k.leadingAnchor.constraint(equalTo: leadingAnchor, constant: inset),
            k.widthAnchor.constraint(equalToConstant: InfoRow.keyWidth),
            k.firstBaselineAnchor.constraint(equalTo: v.firstBaselineAnchor),
            v.leadingAnchor.constraint(equalTo: k.trailingAnchor, constant: 12),
            v.trailingAnchor.constraint(lessThanOrEqualTo: copy.leadingAnchor, constant: -8),
            v.topAnchor.constraint(equalTo: topAnchor, constant: 9),
            v.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -9),
            copy.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -inset + 4),
            copy.topAnchor.constraint(equalTo: topAnchor, constant: 5),
            heightAnchor.constraint(greaterThanOrEqualToConstant: 34),
        ])
    }

    required init?(coder: NSCoder) { fatalError() }

    @objc private func doCopy() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(command, forType: .string)
    }
}

// MARK: - Steps

/// The build's steps in order: state, instruction, stage, and a bar for how
/// long each took relative to the slowest. BuildKit's own setup steps fold
/// into one row.
@MainActor
final class StepTimeline: NSStackView {
    init(steps all: [BuildStep], running: Bool, onSelect: @escaping (Int) -> Void) {
        super.init(frame: .zero)
        orientation = .vertical
        alignment = .leading
        spacing = 6
        translatesAutoresizingMaskIntoConstraints = false

        let setup = all.filter(\.isInternal)
        let steps = all.filter { !$0.isInternal }
        let slowest = max(all.compactMap(\.duration).max() ?? 0, 0.1)
        var rows: [NSView] = []
        if !setup.isEmpty {
            let state: BuildStep.State = setup.contains { $0.state == .failed } ? .failed
                : setup.contains { $0.state == .running } ? .running : .done
            let total = setup.compactMap(\.duration).reduce(0, +)
            rows.append(StepRow(number: setup[0].number, title: "Load definition, metadata and context",
                                caption: "\(setup.count) setup step\(setup.count == 1 ? "" : "s")", state: state,
                                duration: total > 0 || state == .done ? total : nil, slowest: slowest, dim: true, onSelect: onSelect))
        }
        for s in steps {
            let p = s.parts
            let caption = [p.stage, p.position].compactMap { $0 }.joined(separator: " · ")
            rows.append(StepRow(number: s.number, title: p.instruction, caption: caption.isEmpty ? nil : caption,
                                state: s.state, duration: s.duration, slowest: slowest, dim: false, onSelect: onSelect))
        }

        let done = steps.filter { $0.state == .done || $0.state == .cached }.count
        let cached = steps.filter { $0.state == .cached }.count
        let h = NSTextField(labelWithString: "Steps")
        h.font = .boldSystemFont(ofSize: NSFont.systemFontSize)
        let note = label(running ? "\(done) of \(steps.count) done" : "\(steps.count) steps · \(cached) cached",
                         font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
        let head = NSStackView(views: [h, note])
        head.spacing = 8
        head.alignment = .firstBaseline
        addArrangedSubview(head)
        setCustomSpacing(8, after: head)
        let group = FormGroup(rows: rows)
        addArrangedSubview(group)
        group.widthAnchor.constraint(equalTo: widthAnchor).isActive = true
        let foot = label("Click a step to see its output.", font: .systemFont(ofSize: NSFont.smallSystemFontSize),
                         color: .secondaryLabelColor)
        addArrangedSubview(foot)
        foot.leadingAnchor.constraint(equalTo: leadingAnchor, constant: FormLayout.rowInset).isActive = true
    }

    required init(coder: NSCoder) { fatalError() }
}

@MainActor
final class StepRow: NSView {
    private let number: Int
    private let onSelect: (Int) -> Void
    private var hovering = false { didSet { needsDisplay = true } }

    init(number: Int, title: String, caption: String?, state: BuildStep.State, duration: Double?, slowest: Double,
         dim: Bool, onSelect: @escaping (Int) -> Void) {
        self.number = number
        self.onSelect = onSelect
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false

        let icon: NSView
        if state == .running {
            let s = NSProgressIndicator()
            s.style = .spinning
            s.controlSize = .small
            s.startAnimation(nil)
            icon = s
        } else {
            let (symbol, color): (String, NSColor) = switch state {
            case .done: ("checkmark.circle.fill", .systemGreen)
            case .cached: ("bolt.circle.fill", .systemTeal)
            case .failed: ("xmark.circle.fill", .systemRed)
            case .canceled, .running: ("minus.circle.fill", .tertiaryLabelColor)
            }
            let v = NSImageView(image: NSImage(systemSymbolName: symbol, accessibilityDescription: nil) ?? NSImage())
            v.symbolConfiguration = .init(pointSize: 14, weight: .regular)
            v.contentTintColor = dim && state == .done ? .tertiaryLabelColor : color
            icon = v
        }
        let t = label(title, font: .mono(11.5, state == .failed ? .semibold : .regular),
                      color: dim || state == .canceled ? .secondaryLabelColor : .labelColor)
        t.lineBreakMode = .byTruncatingMiddle
        t.toolTip = title
        t.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let c = label(caption ?? "", font: .systemFont(ofSize: NSFont.smallSystemFontSize - 1), color: .secondaryLabelColor)
        c.isHidden = caption == nil
        let text = NSStackView(views: [t, c])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 1

        let trailing: NSView
        if state == .cached {
            let pill = StatusPill()
            pill.show("Cached", .systemTeal)
            trailing = pill
        } else {
            let bar = ProgressLine()
            bar.color = state == .failed ? .systemRed : state == .running ? .systemBlue : dim ? .tertiaryLabelColor : .systemGreen
            bar.value = duration.map { max($0 / slowest, 0.02) } ?? (state == .running ? 0.02 : 0)
            let d = label(duration.map(buildDuration) ?? (state == .running ? "…" : "—"),
                          font: .monospacedDigitSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                          color: .secondaryLabelColor)
            d.alignment = .right
            let s = NSStackView(views: [bar, d])
            s.spacing = 8
            NSLayoutConstraint.activate([
                bar.widthAnchor.constraint(equalToConstant: 72),
                bar.heightAnchor.constraint(equalToConstant: 4),
                d.widthAnchor.constraint(equalToConstant: 48),
            ])
            trailing = s
        }
        for v in [icon, text, trailing] as [NSView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            addSubview(v)
        }
        let inset = FormLayout.rowInset
        NSLayoutConstraint.activate([
            icon.leadingAnchor.constraint(equalTo: leadingAnchor, constant: inset),
            icon.centerYAnchor.constraint(equalTo: centerYAnchor),
            icon.widthAnchor.constraint(equalToConstant: 18),
            text.leadingAnchor.constraint(equalTo: icon.trailingAnchor, constant: 10),
            text.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.topAnchor.constraint(greaterThanOrEqualTo: topAnchor, constant: 7),
            text.bottomAnchor.constraint(lessThanOrEqualTo: bottomAnchor, constant: -7),
            text.trailingAnchor.constraint(lessThanOrEqualTo: trailing.leadingAnchor, constant: -12),
            trailing.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -inset),
            trailing.centerYAnchor.constraint(equalTo: centerYAnchor),
            heightAnchor.constraint(greaterThanOrEqualToConstant: caption == nil ? 34 : 42),
        ])
        addTrackingArea(NSTrackingArea(rect: .zero, options: [.mouseEnteredAndExited, .activeInActiveApp, .inVisibleRect],
                                       owner: self, userInfo: nil))
    }

    required init?(coder: NSCoder) { fatalError() }

    override func draw(_ dirtyRect: NSRect) {
        guard hovering else { return }
        NSColor.quaternarySystemFill.setFill()
        NSBezierPath(roundedRect: bounds.insetBy(dx: 4, dy: 1), xRadius: 6, yRadius: 6).fill()
    }

    override func mouseEntered(with event: NSEvent) { hovering = true }
    override func mouseExited(with event: NSEvent) { hovering = false }
    override func mouseUp(with event: NSEvent) {
        if bounds.contains(convert(event.locationInWindow, from: nil)) { onSelect(number) }
    }
    override func resetCursorRects() { addCursorRect(bounds, cursor: .pointingHand) }
}

/// "5 min. ago", for list rows where the full form doesn't fit.
func shortRelative(_ date: Date?) -> String {
    guard let date else { return "—" }
    let f = RelativeDateTimeFormatter()
    f.unitsStyle = .short
    return f.localizedString(for: date, relativeTo: Date())
}
