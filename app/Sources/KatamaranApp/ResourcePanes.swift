import AppKit

/// A row of the volumes, images or networks list.
struct ResourceRow: Equatable {
    var key: String
    var title: String
    var subtitle: String
    var symbol: String
    var tint: NSColor
    var inUse: Bool
    var deletable: Bool
    /// A status dot on the tile (pods).
    var dot: NSColor? = nil
}

/// What the shared resource list and detail views show: grouped rows, and
/// the selection's details with Delete and an optional open action.
@MainActor
protocol ResourceSource: AnyObject {
    var listVC: ResourceListViewController { get }
    var query: String { get }
    var noun: String { get }
    var emptySymbol: String { get }
    var emptyMessage: String { get }
    /// Whether the rows have been fetched at least once.
    var isLoaded: Bool { get }
    /// The list's sections, in order; empty ones are left out.
    var groups: [(title: String, rows: [ResourceRow])] { get }
    func rows() -> [ResourceRow]
    func row(_ key: String) -> ResourceRow?
    func headerSubtitle(_ key: String) -> String
    func details(_ key: String) -> [NSView]
    /// Where the detail's open button goes (Finder, a browser), if anywhere.
    func reveal(_ key: String) -> URL?
    var revealTitle: String { get }
    var revealSymbol: String { get }
    func confirmDelete(_ key: String)
}

/// Volumes, images and networks share one layout: a list in "In Use" and
/// "Unused" sections, and the selection's details with Delete. Subclasses
/// say what's in them.
@MainActor
class ResourcePane: SectionPane, ResourceSource {
    let store: DockerStore
    let kind: DockerKind
    private(set) lazy var listVC = ResourceListViewController(pane: self)
    private(set) lazy var detailVC = ResourceDetailViewController(pane: self)
    var onChange: () -> Void = {}
    var query = ""

    init(store: DockerStore, kind: DockerKind) {
        self.store = store
        self.kind = kind
        store.observe { [weak self] _ in
            guard let self else { return }
            self.listVC.reload()
            self.detailVC.reload()
            self.onChange()
        }
    }

    var list: NSViewController { listVC }
    var detail: NSViewController? { detailVC }
    var needsDocker: Bool { true }
    var noun: String { "" }
    var emptySymbol: String { "" }
    var emptyMessage: String { "" }

    var subtitle: String {
        guard store.loaded.contains(kind) else { return "Loading…" }
        let rows = rows()
        let used = rows.filter(\.inUse).count
        if rows.isEmpty { return "None" }
        return "\(rows.count) \(noun)\(rows.count == 1 ? "" : "s") · \(used) in use"
    }

    func search(_ text: String) {
        query = text
        listVC.reload()
    }

    func setActive(_ active: Bool) {
        if active { store.interest.insert(kind) } else { store.interest.remove(kind) }
    }

    // Subclasses fill these in.
    func rows() -> [ResourceRow] { [] }
    func searchable(_ row: ResourceRow) -> [String] { [row.title, row.subtitle] }
    func details(_ key: String) -> [NSView] { [] }
    func headerSubtitle(_ key: String) -> String { "" }
    func delete(_ key: String) {}
    func deleteMessage(_ key: String) -> String { "" }
    func reveal(_ key: String) -> URL? { nil }
    var revealTitle: String { "Open in Finder" }
    var revealSymbol: String { "folder" }
    var pruneTitle: String? { nil }
    func prune() {}

    var sections: UsageSections<ResourceRow> {
        UsageSections(rows(), query: query, name: \.title, search: searchable, inUse: \.inUse)
    }

    var isLoaded: Bool { store.loaded.contains(kind) }

    var groups: [(title: String, rows: [ResourceRow])] {
        let s = sections
        return [("In Use", s.inUse), ("Unused", s.unused)]
    }

    func row(_ key: String) -> ResourceRow? { rows().first { $0.key == key } }

    func confirmDelete(_ key: String) {
        guard let r = row(key) else { return }
        confirm(listVC.view.window, title: "Delete \(r.title)?", message: deleteMessage(key), button: "Delete") { [weak self] in
            self?.delete(key)
        }
    }

    func confirmPrune(title: String, message: String, _ body: @escaping @Sendable (DockerClient) throws -> Void) {
        confirm(listVC.view.window, title: title, message: message, button: "Delete") { [store] in
            store.perform("Couldn’t clean up", body)
        }
    }
}

@MainActor
final class ResourceListViewController: NSViewController, NSTableViewDataSource, NSTableViewDelegate {
    private unowned let pane: ResourceSource
    private let table = NSTableView()
    private let empty = EmptyStateView()
    private enum Row: Equatable { case header(String), item(ResourceRow) }
    private var rows: [Row] = []
    private(set) var selectedKey: String?
    var onSelect: (String?) -> Void = { _ in }
    private var reloading = false

    init(pane: ResourceSource) {
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
        var next: [Row] = []
        for g in pane.groups where !g.rows.isEmpty {
            next.append(.header(g.title))
            next += g.rows.map(Row.item)
        }
        let loaded = pane.isLoaded
        empty.isHidden = !next.isEmpty || !loaded
        if pane.rows().isEmpty {
            empty.show(symbol: pane.emptySymbol, title: "No \(pane.noun.capitalized)s", message: pane.emptyMessage)
        } else {
            empty.show(symbol: "magnifyingglass", title: "No Results", message: "Nothing matches “\(pane.query)”.")
        }
        guard next != rows else { return }
        rows = next
        reloading = true
        table.reloadData()
        if let key = selectedKey, let i = rows.firstIndex(where: { if case .item(let r) = $0 { r.key == key } else { false } }) {
            table.selectRowIndexes([i], byExtendingSelection: false)
        } else if selectedKey != nil, pane.query.isEmpty {
            selectedKey = nil
            onSelect(nil)
        }
        reloading = false
    }

    func select(key: String) {
        _ = view
        guard let i = rows.firstIndex(where: { if case .item(let r) = $0 { r.key == key } else { false } }) else { return }
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
        if case .header = rows[row] { 30 } else { 46 }
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        switch rows[row] {
        case .header(let title):
            let cell = tableView.makeView(withIdentifier: .init("section"), owner: nil) as? NSTableCellView ?? sectionHeader("")
            cell.identifier = .init("section")
            cell.textField?.stringValue = title
            return cell
        case .item(let r):
            let cell = tableView.makeView(withIdentifier: ResourceCell.id, owner: nil) as? ResourceCell ?? ResourceCell()
            cell.identifier = ResourceCell.id
            cell.show(r)
            cell.onDelete = { [weak self] in self?.pane.confirmDelete(r.key) }
            return cell
        }
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard !reloading else { return }
        let r = rows[safe: table.selectedRow]
        if case .item(let item) = r { selectedKey = item.key } else { selectedKey = nil }
        onSelect(selectedKey)
    }
}

@MainActor
final class ResourceCell: NSTableCellView {
    static let id = NSUserInterfaceItemIdentifier("resource")
    private let tile = IconTile(size: 28)
    private let name = label(font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium), color: .labelColor)
    private let detail = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private lazy var delete = rowButton("trash", "Delete", target: self, action: #selector(doDelete))
    var onDelete: () -> Void = {}

    init() {
        super.init(frame: .zero)
        let text = NSStackView(views: [name, detail])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 1
        for v in [tile, text, delete] as [NSView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            addSubview(v)
        }
        name.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        detail.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        name.lineBreakMode = .byTruncatingMiddle
        NSLayoutConstraint.activate([
            tile.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 4),
            tile.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.leadingAnchor.constraint(equalTo: tile.trailingAnchor, constant: 10),
            text.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.trailingAnchor.constraint(lessThanOrEqualTo: delete.leadingAnchor, constant: -6),
            delete.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -4),
            delete.centerYAnchor.constraint(equalTo: centerYAnchor),
        ])
        textField = name
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(_ r: ResourceRow) {
        tile.symbol = r.symbol
        tile.tint = r.tint
        tile.dimmed = !r.inUse
        tile.dot = r.dot
        name.stringValue = r.title
        detail.stringValue = r.subtitle
        delete.isHidden = !r.deletable
        toolTip = r.title
    }

    @objc private func doDelete() { onDelete() }
}

@MainActor
final class ResourceDetailViewController: NSViewController {
    private var separator: NSBox?
    private unowned let pane: ResourceSource
    private let tile = IconTile(size: 36)
    private let name = label(font: .systemFont(ofSize: 15, weight: .semibold), color: .labelColor)
    private let sub = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let reveal = NSButton(title: "Open in Finder", target: nil, action: nil)
    private let delete = NSButton(title: "Delete", target: nil, action: nil)
    private let content = ScrollingStack()
    private let empty = EmptyStateView()
    private let header = NSView()
    private(set) var key: String?
    private var shown: ResourceRow?
    /// Shown instead of the details (a pod's logs), below the header.
    private var body: NSView?
    private var bodyTop: NSLayoutYAxisAnchor?
    /// Told after the selection changes, before the reload.
    var onSelect: (String?) -> Void = { _ in }

    init(pane: ResourceSource) {
        self.pane = pane
        super.init(nibName: nil, bundle: nil)
        pane.listVC.onSelect = { [weak self] k in
            self?.key = k
            self?.shown = nil
            self?.onSelect(k)
            self?.reload()
        }
    }

    /// Show `view` in place of the details, or the details again with nil.
    func setBody(_ view: NSView?) {
        _ = self.view
        guard view !== body else { return }
        body?.removeFromSuperview()
        body = view
        if let view, let top = bodyTop {
            self.view.addSubview(view)
            pin(view, to: self.view, top: top)
        }
        reload()
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        let root = NSView()
        reveal.image = NSImage(systemSymbolName: "folder", accessibilityDescription: nil)
        delete.image = NSImage(systemSymbolName: "trash", accessibilityDescription: nil)
        for b in [reveal, delete] {
            b.bezelStyle = .push
            b.imagePosition = .imageLeading
            b.target = self
        }
        reveal.action = #selector(doReveal)
        delete.action = #selector(doDelete)
        name.isSelectable = true
        name.lineBreakMode = .byTruncatingMiddle
        let text = NSStackView(views: [name, sub])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 2
        let buttons = NSStackView(views: [reveal, delete])
        buttons.spacing = 6
        header.translatesAutoresizingMaskIntoConstraints = false
        for v in [tile, text, buttons] as [NSView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            header.addSubview(v)
        }
        name.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let line = NSBox()
        line.boxType = .separator
        line.translatesAutoresizingMaskIntoConstraints = false
        for v in [header, line, content.scroll, empty] as [NSView] { root.addSubview(v) }
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
            line.topAnchor.constraint(equalTo: header.bottomAnchor),
            line.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            line.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            line.heightAnchor.constraint(equalToConstant: 1),
        ])
        separator = line
        bodyTop = line.bottomAnchor
        pin(content.scroll, to: root, top: line.bottomAnchor)
        pin(empty, to: root, top: root.safeAreaLayoutGuide.topAnchor)
        empty.show(symbol: nil, title: "No Selection")
        view = root
        reload()
    }

    func reload() {
        guard isViewLoaded else { return }
        let row = key.flatMap(pane.row)
        empty.isHidden = row != nil
        header.isHidden = row == nil
        separator?.isHidden = row == nil
        content.scroll.isHidden = row == nil || body != nil
        body?.isHidden = row == nil
        guard let row, let key else { return }
        tile.symbol = row.symbol
        tile.tint = row.tint
        tile.dimmed = !row.inUse
        tile.dot = row.dot
        name.stringValue = row.title
        sub.stringValue = pane.headerSubtitle(key)
        delete.isEnabled = row.deletable
        reveal.isHidden = pane.reveal(key) == nil
        reveal.title = pane.revealTitle
        reveal.image = NSImage(systemSymbolName: pane.revealSymbol, accessibilityDescription: nil)
        // Rebuilding the groups would reset the scroll position for nothing.
        if row != shown {
            shown = row
            content.set(pane.details(key))
        }
    }

    @objc private func doReveal() {
        if let key, let url = pane.reveal(key) { NSWorkspace.shared.open(url) }
    }

    @objc private func doDelete() {
        if let key { pane.confirmDelete(key) }
    }
}

/// Containers as links in a details row.
@MainActor
final class ContainerLinks: NSStackView {
    var onSelect: (String) -> Void = { _ in }

    init(_ containers: [ContainerSummary]) {
        super.init(frame: .zero)
        orientation = .vertical
        alignment = .leading
        spacing = 4
        for c in containers.sorted(by: { $0.name < $1.name }) {
            let tile = IconTile(size: 18)
            tile.symbol = ImageIcon.symbol(for: c.Image)
            tile.tint = ImageIcon.tint(for: c.Image)
            tile.dimmed = !c.running
            let l = label(c.name, font: .systemFont(ofSize: NSFont.systemFontSize), color: .labelColor)
            let s = label(c.running ? "running" : "stopped", font: .systemFont(ofSize: NSFont.smallSystemFontSize),
                          color: .secondaryLabelColor)
            let row = NSStackView(views: [tile, l, s])
            row.spacing = 6
            addArrangedSubview(row)
        }
    }

    required init(coder: NSCoder) { fatalError() }
}

// MARK: - Volumes

@MainActor
final class VolumesPane: ResourcePane {
    private let model: AppModel

    init(store: DockerStore, model: AppModel) {
        self.model = model
        super.init(store: store, kind: .volumes)
    }

    override var noun: String { "volume" }
    override var emptySymbol: String { "externaldrive" }
    override var emptyMessage: String { "Named volumes keep data when containers are deleted." }

    override func rows() -> [ResourceRow] {
        store.volumes.map { v in
            let users = store.containers(usingVolume: v)
            var sub = [v.size.map(bytes) ?? "-"]
            if let p = v.project { sub.append(p) }
            if !users.isEmpty { sub.append(users.map(\.name).sorted().joined(separator: ", ")) }
            return ResourceRow(key: v.Name, title: v.Name, subtitle: sub.joined(separator: " · "),
                               symbol: "externaldrive.fill", tint: .systemTeal,
                               inUse: store.volumeInUse(v), deletable: true)
        }
    }

    private func volume(_ key: String) -> VolumeInfo? { store.volumes.first { $0.Name == key } }

    override func headerSubtitle(_ key: String) -> String {
        guard let v = volume(key) else { return "" }
        return [v.size.map(bytes), v.Driver].compactMap { $0 }.joined(separator: " · ")
    }

    override func details(_ key: String) -> [NSView] {
        guard let v = volume(key) else { return [] }
        var rows: [NSView] = [InfoRow("Size", v.size.map(bytes) ?? "Unknown")]
        if let created = v.CreatedAt.flatMap(parseDockerDate) {
            rows.append(InfoRow("Created", "\(absolute(created)) (\(relative(created)))"))
        }
        rows.append(InfoRow("Driver", v.Driver ?? "local"))
        if let p = v.project { rows.append(InfoRow("Compose", p)) }
        if let m = v.Mountpoint { rows.append(InfoRow("In the VM", m, mono: true)) }
        if let url = reveal(key) { rows.append(InfoRow("On this Mac", (url.path as NSString).abbreviatingWithTildeInPath, mono: true)) }
        var groups: [NSView] = [FormGroup(header: "Volume", rows: rows)]
        let users = store.containers(usingVolume: v)
        groups.append(FormGroup(header: "Used By", rows: users.isEmpty
            ? [InfoRow("Containers", "None")] : [InfoRow("Containers", view: ContainerLinks(users))]))
        if let labels = v.Labels, !labels.isEmpty {
            groups.append(FormGroup(header: "Labels", rows: labels.sorted { $0.key < $1.key }.map { InfoRow($0.key, $0.value, mono: true) }))
        }
        return groups
    }

    override func reveal(_ key: String) -> URL? {
        guard let root = model.status?.mount.path, model.status?.mount.mounted == true else { return nil }
        return URL(fileURLWithPath: root).appendingPathComponent("docker/volumes/\(key)")
    }

    override func deleteMessage(_ key: String) -> String {
        guard let v = volume(key) else { return "" }
        let users = store.containers(usingVolume: v)
        return users.isEmpty ? "Its data is deleted for good."
            : "It’s used by \(users.map(\.name).joined(separator: ", ")); delete those containers first."
    }

    override func delete(_ key: String) {
        store.perform("Couldn’t delete \(key)") { try $0.removeVolume(key) }
    }

    override var pruneTitle: String? { "Delete Unused Volumes…" }

    override func prune() {
        let unused = store.volumes.filter { !store.volumeInUse($0) }
        guard !unused.isEmpty else { return }
        let size = unused.compactMap(\.size).reduce(0, +)
        confirmPrune(title: "Delete \(unused.count) unused volume\(unused.count == 1 ? "" : "s")?",
                     message: "Their data (\(bytes(size))) is deleted for good.") { try $0.prune(.volumes) }
    }
}

// MARK: - Images

@MainActor
final class ImagesPane: ResourcePane {
    init(store: DockerStore) { super.init(store: store, kind: .images) }

    override var noun: String { "image" }
    override var emptySymbol: String { "square.stack.3d.up" }
    override var emptyMessage: String { "Images you pull or build show up here." }

    override func rows() -> [ResourceRow] {
        store.images.map { i in
            let users = store.containers(usingImage: i).count
            var sub = [i.Size.map(bytes) ?? "-", relative(i.createdDate)]
            if users > 0 { sub.append("\(users) container\(users == 1 ? "" : "s")") }
            return ResourceRow(key: i.Id, title: i.name, subtitle: sub.joined(separator: " · "),
                               symbol: ImageIcon.symbol(for: i.name), tint: ImageIcon.tint(for: i.name),
                               inUse: users > 0, deletable: true)
        }
    }

    override func searchable(_ row: ResourceRow) -> [String] { [row.title, row.key] }

    private func image(_ key: String) -> ImageInfo? { store.images.first { $0.Id == key } }

    override func headerSubtitle(_ key: String) -> String {
        guard let i = image(key) else { return "" }
        return [i.Size.map(bytes) ?? "", i.shortID].joined(separator: " · ")
    }

    override func details(_ key: String) -> [NSView] {
        guard let i = image(key) else { return [] }
        var rows: [NSView] = [
            InfoRow("Size", i.Size.map(bytes) ?? "Unknown"),
            InfoRow("Created", "\(absolute(i.createdDate)) (\(relative(i.createdDate)))"),
            InfoRow("ID", i.Id, mono: true),
        ]
        if !i.tags.isEmpty { rows.append(InfoRow("Tags", i.tags.joined(separator: "\n"))) }
        if let d = i.RepoDigests, !d.isEmpty { rows.append(InfoRow("Digests", d.joined(separator: "\n"), mono: true)) }
        var groups: [NSView] = [FormGroup(header: "Image", rows: rows)]
        let users = store.containers(usingImage: i)
        groups.append(FormGroup(header: "Used By", rows: users.isEmpty
            ? [InfoRow("Containers", "None")] : [InfoRow("Containers", view: ContainerLinks(users))]))
        if let labels = i.Labels, !labels.isEmpty {
            groups.append(FormGroup(header: "Labels", rows: labels.sorted { $0.key < $1.key }.map { InfoRow($0.key, $0.value, mono: true) }))
        }
        return groups
    }

    override func deleteMessage(_ key: String) -> String {
        guard let i = image(key) else { return "" }
        let users = store.containers(usingImage: i)
        return users.isEmpty ? "You can pull it again later."
            : "It’s used by \(users.map(\.name).joined(separator: ", ")); delete those containers first."
    }

    override func delete(_ key: String) {
        let name = image(key)?.name ?? key
        let inUse = image(key).map { !store.containers(usingImage: $0).isEmpty } ?? false
        store.perform("Couldn’t delete \(name)") { client in
            // Forcing an image that a container uses only untags it; let Docker say no instead.
            try client.removeImage(key, force: !inUse)
        }
    }

    override var pruneTitle: String? { "Delete Unused Images…" }

    override func prune() {
        let unused = store.images.filter { !store.imageInUse($0) }
        guard !unused.isEmpty else { return }
        let size = unused.compactMap(\.Size).reduce(0, +)
        confirmPrune(title: "Delete \(unused.count) unused image\(unused.count == 1 ? "" : "s")?",
                     message: "That frees about \(bytes(size)). You can pull them again later.") { try $0.prune(.images) }
    }
}

// MARK: - Networks

@MainActor
final class NetworksPane: ResourcePane {
    init(store: DockerStore) { super.init(store: store, kind: .networks) }

    override var noun: String { "network" }
    override var emptySymbol: String { "point.3.connected.trianglepath.dotted" }

    override func rows() -> [ResourceRow] {
        store.networks.map { n in
            let users = store.containers(onNetwork: n).count
            var sub = [n.Driver ?? "", n.subnets.first].compactMap { $0 }.filter { !$0.isEmpty }
            if users > 0 { sub.append("\(users) container\(users == 1 ? "" : "s")") }
            if n.builtIn { sub.append("built-in") }
            return ResourceRow(key: n.Id, title: n.Name, subtitle: sub.joined(separator: " · "),
                               symbol: "point.3.connected.trianglepath.dotted", tint: n.builtIn ? .systemGray : .systemBlue,
                               inUse: store.networkInUse(n), deletable: !n.builtIn)
        }
    }

    private func network(_ key: String) -> NetworkInfo? { store.networks.first { $0.Id == key } }

    override func headerSubtitle(_ key: String) -> String {
        guard let n = network(key) else { return "" }
        return [n.Driver ?? "", n.Scope ?? "", n.shortID].filter { !$0.isEmpty }.joined(separator: " · ")
    }

    override func details(_ key: String) -> [NSView] {
        guard let n = network(key) else { return [] }
        var rows: [NSView] = [InfoRow("Driver", n.Driver ?? "-")]
        if !n.subnets.isEmpty { rows.append(InfoRow("Subnet", n.subnets.joined(separator: "\n"), mono: true)) }
        if !n.gateways.isEmpty { rows.append(InfoRow("Gateway", n.gateways.joined(separator: "\n"), mono: true)) }
        if n.Internal == true { rows.append(InfoRow("Internal", "Yes: no access outside")) }
        if let created = n.Created.flatMap(parseDockerDate) { rows.append(InfoRow("Created", absolute(created))) }
        if let p = n.project { rows.append(InfoRow("Compose", p)) }
        rows.append(InfoRow("ID", n.Id, mono: true))
        var groups: [NSView] = [FormGroup(header: "Network", rows: rows)]
        let users = store.containers(onNetwork: n)
        groups.append(FormGroup(header: "Connected", rows: users.isEmpty
            ? [InfoRow("Containers", "None")] : [InfoRow("Containers", view: ContainerLinks(users))]))
        return groups
    }

    override func deleteMessage(_ key: String) -> String {
        guard let n = network(key) else { return "" }
        let users = store.containers(onNetwork: n)
        return users.isEmpty ? "Containers can no longer join it." : "Disconnect \(users.map(\.name).joined(separator: ", ")) first."
    }

    override func delete(_ key: String) {
        let name = network(key)?.Name ?? key
        store.perform("Couldn’t delete \(name)") { try $0.removeNetwork(key) }
    }

    override var pruneTitle: String? { "Delete Unused Networks…" }

    override func prune() {
        let unused = store.networks.filter { !store.networkInUse($0) }
        guard !unused.isEmpty else { return }
        confirmPrune(title: "Delete \(unused.count) unused network\(unused.count == 1 ? "" : "s")?",
                     message: unused.map(\.Name).joined(separator: ", ")) { try $0.prune(.networks) }
    }
}
