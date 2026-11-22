import AppKit

/// The sidebar's sections, in order.
enum MainSection: String, CaseIterable {
    case containers, volumes, images, builds, networks, pods, services, machines, activity, logs

    var title: String {
        switch self {
        case .containers: "Containers"
        case .volumes: "Volumes"
        case .images: "Images"
        case .builds: "Builds"
        case .networks: "Networks"
        case .pods: "Pods"
        case .services: "Services"
        case .machines: "Machines"
        case .activity: "Activity"
        case .logs: "Logs"
        }
    }

    var symbol: String {
        switch self {
        case .containers: "shippingbox"
        case .volumes: "externaldrive"
        case .images: "square.stack.3d.up"
        case .builds: "hammer"
        case .networks: "point.3.connected.trianglepath.dotted"
        case .pods: "cube"
        case .services: "network"
        case .machines: "server.rack"
        case .activity: "chart.xyaxis.line"
        case .logs: "text.alignleft"
        }
    }

    var group: String {
        switch self {
        case .containers, .volumes, .images, .builds, .networks: "Docker"
        case .pods, .services: "Kubernetes"
        case .machines: "Linux"
        case .activity, .logs: "General"
        }
    }
}

/// What a section puts in the window: a list column, maybe a detail column,
/// the subtitle and the toolbar's search and tabs.
@MainActor
protocol SectionPane: AnyObject {
    var list: NSViewController { get }
    var detail: NSViewController? { get }
    /// Shows Docker's state instead of the list while Docker isn't ready.
    var needsDocker: Bool { get }
    var subtitle: String { get }
    /// The detail's tabs for the toolbar, and which are usable now.
    var tabs: [String] { get }
    var enabledTabs: Set<Int> { get }
    var selectedTab: Int { get set }
    /// What "Remove Unused…" removes, if the section has such a thing.
    var pruneTitle: String? { get }
    func prune()
    func search(_ text: String)
    func setActive(_ active: Bool)
    /// The section tells the window its subtitle or tabs changed.
    var onChange: () -> Void { get set }
    /// Shown instead of the list while the section can't work (Kubernetes off).
    var blockedState: BlockedState? { get }
    func blockedAction()
    /// Extra items for the toolbar's More menu.
    var menuItems: [NSMenuItem] { get }
}

extension SectionPane {
    var blockedState: BlockedState? { nil }
    func blockedAction() {}
    var menuItems: [NSMenuItem] { [] }
    var tabs: [String] { [] }
    var enabledTabs: Set<Int> { [] }
    var selectedTab: Int { get { 0 } set {} }
    var pruneTitle: String? { nil }
    func prune() {}
    func search(_ text: String) {}
}

/// The main window: sidebar, list and detail, with the toolbar on glass.
@MainActor
final class MainWindowController: NSWindowController, NSWindowDelegate, NSToolbarDelegate, NSSearchFieldDelegate, NSMenuDelegate {
    private let model: AppModel
    private let store: DockerStore
    private let split = NSSplitViewController()
    private let sidebar: SidebarViewController
    private let listHost = HostViewController()
    private let detailHost = HostViewController()
    private var sidebarItem: NSSplitViewItem!
    private var listItem: NSSplitViewItem!
    private var detailItem: NSSplitViewItem!
    private var panes: [MainSection: SectionPane] = [:]
    private(set) var section: MainSection = .containers
    private let dockerState = EmptyStateView()
    private let sectionState = EmptyStateView()
    private let noSelection: EmptyStateView = {
        let v = EmptyStateView()
        v.show(symbol: nil, title: "No Selection")
        return v
    }()
    private let tabControl = NSSegmentedControl()
    private var tabItem: NSToolbarItem?
    private var searchItem: NSSearchToolbarItem?
    private var pruneItem: NSToolbarItem?
    private struct ShownDockerState: Equatable {
        let phase: Phase
        let busy: Bool
        let errorMessage: String?
    }
    private var shownState: ShownDockerState?
    private var shownSectionBlock: BlockedState?
    private var listWidth: CGFloat = {
        let w = UserDefaults.standard.double(forKey: MainWindowController.listWidthKey)
        return w > 0 ? w : 380
    }()
    private var listWidthConstraint: NSLayoutConstraint?
    private var draggingDivider = false
    private var mouseMonitor: Any?
    var onClose: () -> Void = {}
    var openSettings: () -> Void = {}

    private static let sectionKey = "mainSection"
    private static let listWidthKey = "mainListWidth"
    private static let tabSymbols = [
        "Info": "info.circle", "Stats": "chart.xyaxis.line", "Logs": "text.alignleft",
        "Terminal": "apple.terminal", "Files": "folder",
    ]

    private enum ID {
        static let search = NSToolbarItem.Identifier("search")
        static let listSeparator = NSToolbarItem.Identifier("listSeparator")
        static let tabs = NSToolbarItem.Identifier("tabs")
        static let prune = NSToolbarItem.Identifier("prune")
    }

    init(model: AppModel, store: DockerStore) {
        self.model = model
        self.store = store
        sidebar = SidebarViewController(model: model)
        let w = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1180, height: 740),
                         styleMask: [.titled, .closable, .miniaturizable, .resizable, .fullSizeContentView],
                         backing: .buffered, defer: false)
        w.title = "Katamaran"
        w.minSize = NSSize(width: 860, height: 480)
        w.isReleasedWhenClosed = false
        w.titlebarSeparatorStyle = .automatic
        w.toolbarStyle = .unified
        w.tabbingMode = .disallowed
        super.init(window: w)
        w.delegate = self
        build()
        model.observe { [weak self] in self?.statusChanged() }
        store.observe { [weak self] _ in self?.refreshChrome() }
    }

    required init?(coder: NSCoder) { fatalError() }

    private func build() {
        panes = [
            .containers: ContainersPane(store: store, model: model),
            .volumes: VolumesPane(store: store, model: model),
            .images: ImagesPane(store: store),
            .builds: BuildsPane(store: store, builds: .shared),
            .networks: NetworksPane(store: store),
            .pods: PodsPane(store: .shared, model: model),
            .services: ServicesPane(store: .shared, model: model),
            .machines: MachinesPane(model: model),
            .activity: ActivityPane(store: store, model: model),
            .logs: LogsPane(model: model),
        ]
        for pane in panes.values { pane.onChange = { [weak self, weak pane] in
            guard let self, let pane, pane === self.panes[self.section] else { return }
            self.refreshChrome()
        } }

        sidebarItem = NSSplitViewItem(sidebarWithViewController: sidebar)
        sidebarItem.minimumThickness = 190
        sidebarItem.maximumThickness = 280
        sidebarItem.canCollapse = true
        listItem = NSSplitViewItem(contentListWithViewController: listHost)
        listItem.minimumThickness = 300
        listItem.maximumThickness = 560
        listItem.holdingPriority = NSLayoutConstraint.Priority(NSLayoutConstraint.Priority.defaultLow.rawValue + 1)
        detailItem = NSSplitViewItem(viewController: detailHost)
        detailItem.minimumThickness = 400
        detailItem.canCollapse = true
        // Activity and Logs collapse it: the list widens, the window stays put.
        detailItem.collapseBehavior = .preferResizingSiblingsWithFixedSplitView
        split.splitViewItems = [sidebarItem, listItem, detailItem]
        let width = listHost.view.widthAnchor.constraint(equalToConstant: listWidth)
        width.priority = NSLayoutConstraint.Priority(499)
        listWidthConstraint = width
        mouseMonitor = NSEvent.addLocalMonitorForEvents(matching: [.leftMouseDown, .leftMouseUp]) { [weak self] e in
            MainActor.assumeIsolated {
                if e.type == .leftMouseDown { self?.dividerMouseDown(e) } else { self?.dividerMouseUp() }
            }
            return e
        }
        window?.contentViewController = split
        window?.setContentSize(NSSize(width: 1180, height: 740))

        dockerState.onAction = { [weak self] in self?.model.start() }

        let toolbar = NSToolbar(identifier: "KatamaranMain")
        toolbar.delegate = self
        toolbar.displayMode = .iconOnly
        toolbar.allowsUserCustomization = false
        window?.toolbar = toolbar

        window?.setFrameAutosaveName("KatamaranMain")
        sidebar.onSelect = { [weak self] s in self?.show(s) }
        BuildStore.shared.observe { [weak self] b in
            let n = b.running.count
            self?.sidebar.setBadge(.builds, n > 0 ? "\(n)" : nil)
        }
        let saved = UserDefaults.standard.string(forKey: Self.sectionKey).flatMap(MainSection.init(rawValue:))
        show(saved ?? .containers, remember: false)
    }

    // MARK: Showing

    func present(section s: MainSection? = nil) {
        if let s { show(s) }
        NSApp.activate()
        if !(window?.isVisible ?? false) && !(window?.setFrameUsingName("KatamaranMain") ?? false) { window?.center() }
        showWindow(nil)
        window?.makeKeyAndOrderFront(nil)
        BuildStore.shared.windowOpen = true
        panes[section]?.setActive(true)
    }

    var currentPane: SectionPane? { panes[section] }

    func show(_ s: MainSection, remember: Bool = true) {
        let old = panes[section]
        if old !== panes[s] || listHost.children.isEmpty {
            old?.setActive(false)
            searchItem?.searchField.stringValue = ""
            old?.search("")
        }
        section = s
        if remember { UserDefaults.standard.set(s.rawValue, forKey: Self.sectionKey) }
        sidebar.select(s)
        shownState = nil
        shownSectionBlock = nil
        refreshChrome()
        if window?.isVisible ?? false { panes[s]?.setActive(true) }
    }

    /// For snapshots: select a container, project, volume, image or network by name.
    func select(name: String) {
        if let p = panes[section] as? ContainersPane { p.select(name: name) }
        if let p = panes[section] as? BuildsPane { p.select(name: name) }
        if let p = panes[section] as? ResourceSource, let r = p.rows().first(where: { $0.title == name }) {
            p.listVC.select(key: r.key)
        }
    }

    /// For snapshots: give the list a width, as dragging its divider would.
    func setListWidth(_ w: CGFloat) {
        listWidth = w
        UserDefaults.standard.set(Double(w), forKey: Self.listWidthKey)
        applyListWidth()
    }

    func toggleSidebar() { split.toggleSidebar(nil) }

    /// The list keeps the width the user dragged it to in every section, and
    /// across launches: what a pane shows never moves its divider. Below the
    /// window's own size (500), so it never makes the window grow.
    private func applyListWidth() {
        guard let c = listWidthConstraint else { return }
        c.constant = min(max(listWidth, listItem.minimumThickness), 560)
        c.isActive = !detailItem.isCollapsed && !draggingDivider
    }

    /// A drag of the list's divider lets go of the width until the mouse is up.
    private func dividerMouseDown(_ e: NSEvent) {
        guard e.window === window, !detailItem.isCollapsed else { return }
        let sv = split.splitView
        let p = sv.convert(e.locationInWindow, from: nil)
        let edge = listHost.view.convert(listHost.view.bounds, to: sv).maxX
        guard sv.bounds.contains(p), abs(p.x - edge) <= 6 else { return }
        draggingDivider = true
        applyListWidth()
    }

    private func dividerMouseUp() {
        guard draggingDivider else { return }
        draggingDivider = false
        listWidth = listHost.view.frame.width
        UserDefaults.standard.set(Double(listWidth), forKey: Self.listWidthKey)
        applyListWidth()
    }

    func select(tab: ContainerTab) {
        (panes[.containers] as? ContainersPane)?.select(tab: tab)
        refreshChrome()
    }

    private func statusChanged() {
        sidebar.update()
        refreshChrome()
    }

    /// Lay out the columns for the section and Docker's state, and refresh the toolbar.
    private func refreshChrome() {
        guard let pane = panes[section], let window else { return }
        let dockerUp = store.socket != nil
        let sectionBlock = pane.blockedState
        let blocked = (pane.needsDocker && !dockerUp) || sectionBlock != nil

        if let b = sectionBlock {
            if b != shownSectionBlock {
                shownSectionBlock = b
                sectionState.show(symbol: b.symbol, title: b.title, message: b.message, action: b.action, busy: b.busy)
            }
            sectionState.onAction = { [weak pane] in pane?.blockedAction() }
            listHost.embed(nil, overlay: sectionState)
        } else if blocked {
            shownSectionBlock = nil
            showDockerState()
            listHost.embed(nil, overlay: dockerState)
        } else {
            shownSectionBlock = nil
            listHost.embed(pane.list, overlay: nil)
        }
        // Docker's state stays in the list column, as a list's own empty state
        // would: the columns, and the title above the list, don't move.
        detailHost.embed(blocked ? nil : pane.detail, overlay: blocked && pane.detail != nil ? noSelection : nil)
        let collapse = pane.detail == nil
        // Alone, the list takes the whole width.
        if detailItem.isCollapsed != collapse {
            listItem.maximumThickness = collapse ? NSSplitViewItem.unspecifiedDimension : 560
            detailItem.isCollapsed = collapse
        }
        applyListWidth()

        window.title = section.title
        window.subtitle = sectionBlock != nil ? "" : blocked ? model.phase.label : pane.subtitle

        let tabs = blocked ? [] : pane.tabs
        if tabControl.segmentCount != tabs.count || (0..<tabs.count).contains(where: { (tabControl.toolTip(forSegment: $0) ?? tabControl.label(forSegment: $0)) != tabs[$0] }) {
            tabControl.segmentCount = tabs.count
            for (i, t) in tabs.enumerated() {
                tabControl.setLabel(t, forSegment: i)
                tabControl.setWidth(0, forSegment: i)
                if let symbol = Self.tabSymbols[t],
                   let image = NSImage(systemSymbolName: symbol, accessibilityDescription: t) {
                    tabControl.setImage(image, forSegment: i)
                    tabControl.setLabel("", forSegment: i)
                    tabControl.setWidth(34, forSegment: i)
                    tabControl.setToolTip(t, forSegment: i)
                }
            }
        }
        for i in 0..<tabs.count { tabControl.setEnabled(pane.enabledTabs.contains(i), forSegment: i) }
        if !tabs.isEmpty { tabControl.selectedSegment = pane.selectedTab }
        tabItem?.isHidden = tabs.isEmpty
        searchItem?.isHidden = blocked || section == .activity || section == .logs
        pruneItem?.isHidden = blocked || (pane.pruneTitle == nil && pane.menuItems.isEmpty)
        pruneItem?.toolTip = "More"
    }

    private func showDockerState() {
        let phase = model.phase
        let errorMessage = model.status?.error ?? model.lastError
        let next = ShownDockerState(phase: phase, busy: model.busy, errorMessage: errorMessage)
        guard next != shownState else { return }
        shownState = next
        switch phase {
        case .noDaemon, .stopped:
            let startError = model.lastError?.hasPrefix("start: ") == true ? model.lastError : nil
            dockerState.show(symbol: "sailboat", title: "Katamaran is stopped",
                             message: startError ?? "Start the VM to use Docker.", action: "Start", busy: model.busy)
        case .starting:
            dockerState.show(symbol: "sailboat", title: "Starting…", message: "Docker will be ready in a moment.", busy: true)
        case .stopping:
            dockerState.show(symbol: "sailboat", title: "Stopping…", busy: true)
        case .running:
            dockerState.show(symbol: "sailboat", title: "Waiting for Docker…", busy: true)
        case .error:
            dockerState.show(symbol: "exclamationmark.triangle", title: "Katamaran ran into a problem",
                             message: errorMessage ?? "Docker isn’t answering.",
                             action: "Restart", busy: model.busy)
            dockerState.onAction = { [weak self] in self?.model.restart() }
            return
        }
        dockerState.onAction = { [weak self] in self?.model.start() }
    }

    // MARK: Toolbar

    func toolbarDefaultItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        [.toggleSidebar, .sidebarTrackingSeparator, .flexibleSpace, ID.prune, ID.search, ID.listSeparator,
         .flexibleSpace, ID.tabs]
    }

    func toolbarAllowedItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        toolbarDefaultItemIdentifiers(toolbar)
    }

    func toolbar(_ toolbar: NSToolbar, itemForItemIdentifier id: NSToolbarItem.Identifier,
                 willBeInsertedIntoToolbar flag: Bool) -> NSToolbarItem? {
        switch id {
        case ID.search:
            let item = NSSearchToolbarItem(itemIdentifier: id)
            item.searchField.delegate = self
            item.searchField.placeholderString = "Search"
            item.preferredWidthForSearchField = 180
            searchItem = item
            return item
        case ID.listSeparator:
            return NSTrackingSeparatorToolbarItem(identifier: id, splitView: split.splitView, dividerIndex: 1)
        case ID.tabs:
            tabControl.segmentStyle = .automatic
            tabControl.trackingMode = .selectOne
            tabControl.target = self
            tabControl.action = #selector(tabChanged)
            refreshChrome()
            let item = NSToolbarItem(itemIdentifier: id)
            item.view = tabControl
            item.label = "View"
            tabItem = item
            refreshChrome()
            return item
        case ID.prune:
            let item = NSMenuToolbarItem(itemIdentifier: id)
            item.image = NSImage(systemSymbolName: "ellipsis.circle", accessibilityDescription: "More")
            item.showsIndicator = false
            item.menu = pruneMenu
            pruneMenu.delegate = self
            pruneItem = item
            refreshChrome()
            return item
        default:
            return nil
        }
    }

    @objc private func tabChanged() {
        panes[section]?.selectedTab = tabControl.selectedSegment
    }

    @objc private func prune() { panes[section]?.prune() }

    private let pruneMenu = NSMenu()

    func menuNeedsUpdate(_ menu: NSMenu) {
        menu.removeAllItems()
        for item in panes[section]?.menuItems ?? [] { menu.addItem(item) }
        guard let title = panes[section]?.pruneTitle else { return }
        let i = NSMenuItem(title: title, action: #selector(prune), keyEquivalent: "")
        i.target = self
        i.image = NSImage(systemSymbolName: "trash", accessibilityDescription: nil)
        menu.addItem(i)
    }

    func controlTextDidChange(_ obj: Notification) {
        guard let field = obj.object as? NSSearchField else { return }
        panes[section]?.search(field.stringValue)
    }

    /// ⌘F focuses the toolbar's search field.
    @objc func performFindPanelAction(_ sender: Any?) {
        searchItem?.beginSearchInteraction()
    }

    @objc func selectSection(_ sender: NSMenuItem) {
        if let s = MainSection.allCases[safe: sender.tag] { present(section: s) }
    }

    // MARK: Window

    func windowWillClose(_ notification: Notification) {
        panes[section]?.setActive(false)
        BuildStore.shared.windowOpen = false
        onClose()
    }

    func windowDidBecomeKey(_ notification: Notification) {
        panes[section]?.setActive(true)
    }
}

extension Array {
    subscript(safe i: Int) -> Element? { indices.contains(i) ? self[i] : nil }
}

/// A column that shows one child controller, or an overlay view instead.
@MainActor
final class HostViewController: NSViewController {
    private var overlayView: NSView?

    override func loadView() {
        view = NSView()
    }

    func embed(_ child: NSViewController?, overlay: NSView?) {
        if let child, children.first === child, overlayView == nil { return }
        if child == nil, overlay != nil, overlayView === overlay, children.isEmpty { return }
        for c in children {
            c.view.removeFromSuperview()
            c.removeFromParent()
        }
        overlayView?.removeFromSuperview()
        overlayView = nil
        let v: NSView
        if let child {
            addChild(child)
            v = child.view
        } else if let overlay {
            overlayView = overlay
            v = overlay
        } else {
            return
        }
        v.translatesAutoresizingMaskIntoConstraints = false
        view.addSubview(v)
        // Lists scroll under the toolbar; overlays sit below it.
        let top = child == nil ? view.safeAreaLayoutGuide.topAnchor : view.topAnchor
        NSLayoutConstraint.activate([
            v.topAnchor.constraint(equalTo: top),
            v.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            v.trailingAnchor.constraint(equalTo: view.trailingAnchor),
            v.bottomAnchor.constraint(equalTo: view.bottomAnchor),
        ])
    }
}

/// The sidebar: sections in groups, and the VM's state at the bottom.
@MainActor
final class SidebarViewController: NSViewController, NSOutlineViewDataSource, NSOutlineViewDelegate {
    private let model: AppModel
    private let outline = NSOutlineView()
    private let groups: [(String, [MainSection])]
    private let footer = VMStatusView()
    var onSelect: (MainSection) -> Void = { _ in }
    private var selecting = false

    init(model: AppModel) {
        self.model = model
        var g: [(String, [MainSection])] = []
        for s in MainSection.allCases {
            if let i = g.firstIndex(where: { $0.0 == s.group }) { g[i].1.append(s) } else { g.append((s.group, [s])) }
        }
        groups = g
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        let column = NSTableColumn(identifier: .init("main"))
        outline.addTableColumn(column)
        outline.outlineTableColumn = column
        outline.headerView = nil
        outline.style = .sourceList
        outline.rowSizeStyle = .default
        outline.floatsGroupRows = false
        outline.dataSource = self
        outline.delegate = self
        outline.indentationPerLevel = 0
        let scroll = NSScrollView()
        scroll.documentView = outline
        scroll.drawsBackground = false
        scroll.hasVerticalScroller = true
        scroll.translatesAutoresizingMaskIntoConstraints = false
        footer.onToggle = { [weak self] in
            guard let self else { return }
            if self.model.status?.vm != nil { self.model.stop() } else { self.model.start() }
        }
        let root = NSView()
        root.addSubview(scroll)
        root.addSubview(footer)
        NSLayoutConstraint.activate([
            footer.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor),
            footer.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            footer.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            scroll.topAnchor.constraint(equalTo: footer.bottomAnchor),
            scroll.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            scroll.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            scroll.bottomAnchor.constraint(equalTo: root.bottomAnchor),
        ])
        view = root
        outline.reloadData()
        groups.forEach { outline.expandItem($0.0) }
        update()
    }

    func select(_ s: MainSection) {
        _ = view
        let row = outline.row(forItem: s.rawValue)
        guard row >= 0, outline.selectedRow != row else { return }
        selecting = true
        outline.selectRowIndexes([row], byExtendingSelection: false)
        selecting = false
    }

    func update() { footer.update(model) }

    /// A count with a spinner after a section's name (running builds), or nothing.
    private var badges: [MainSection: String] = [:]

    func setBadge(_ s: MainSection, _ text: String?) {
        guard badges[s] != text else { return }
        badges[s] = text
        guard isViewLoaded else { return }
        let row = outline.row(forItem: s.rawValue)
        if row >= 0 { outline.reloadData(forRowIndexes: [row], columnIndexes: [0]) }
    }

    func outlineView(_ outlineView: NSOutlineView, numberOfChildrenOfItem item: Any?) -> Int {
        guard let item else { return groups.count }
        return groups.first { $0.0 == item as? String }?.1.count ?? 0
    }

    func outlineView(_ outlineView: NSOutlineView, child index: Int, ofItem item: Any?) -> Any {
        guard let item else { return groups[index].0 }
        return groups.first { $0.0 == item as? String }!.1[index].rawValue
    }

    func outlineView(_ outlineView: NSOutlineView, isItemExpandable item: Any) -> Bool { isGroup(item) }
    func outlineView(_ outlineView: NSOutlineView, isGroupItem item: Any) -> Bool { isGroup(item) }
    func outlineView(_ outlineView: NSOutlineView, shouldSelectItem item: Any) -> Bool { !isGroup(item) }
    func outlineView(_ outlineView: NSOutlineView, shouldShowOutlineCellForItem item: Any) -> Bool { false }

    private func isGroup(_ item: Any) -> Bool { groups.contains { $0.0 == item as? String } }

    func outlineView(_ outlineView: NSOutlineView, viewFor tableColumn: NSTableColumn?, item: Any) -> NSView? {
        let cell = NSTableCellView()
        let text = NSTextField(labelWithString: "")
        text.translatesAutoresizingMaskIntoConstraints = false
        text.lineBreakMode = .byTruncatingTail
        cell.addSubview(text)
        cell.textField = text
        if isGroup(item) {
            text.stringValue = item as? String ?? ""
            NSLayoutConstraint.activate([
                text.leadingAnchor.constraint(equalTo: cell.leadingAnchor, constant: 2),
                text.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
            ])
            return cell
        }
        guard let s = MainSection(rawValue: item as? String ?? "") else { return nil }
        text.stringValue = s.title
        let icon = NSImageView(image: NSImage(systemSymbolName: s.symbol, accessibilityDescription: s.title) ?? NSImage())
        icon.translatesAutoresizingMaskIntoConstraints = false
        cell.addSubview(icon)
        cell.imageView = icon
        NSLayoutConstraint.activate([
            icon.leadingAnchor.constraint(equalTo: cell.leadingAnchor, constant: 4),
            icon.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
            icon.widthAnchor.constraint(equalToConstant: 20),
            text.leadingAnchor.constraint(equalTo: icon.trailingAnchor, constant: 6),
            text.trailingAnchor.constraint(lessThanOrEqualTo: cell.trailingAnchor, constant: -4),
            text.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
        ])
        if let badge = badges[s] { addBadge(badge, to: cell, after: text) }
        return cell
    }

    private func addBadge(_ text: String, to cell: NSView, after title: NSView) {
        let spinner = NSProgressIndicator()
        spinner.style = .spinning
        spinner.controlSize = .mini
        spinner.startAnimation(nil)
        let count = NSTextField(labelWithString: text)
        count.font = .monospacedDigitSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .medium)
        count.textColor = .secondaryLabelColor
        let badge = NSStackView(views: [spinner, count])
        badge.spacing = 4
        badge.translatesAutoresizingMaskIntoConstraints = false
        badge.toolTip = "\(text) running"
        cell.addSubview(badge)
        NSLayoutConstraint.activate([
            badge.trailingAnchor.constraint(equalTo: cell.trailingAnchor, constant: -6),
            badge.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
            badge.leadingAnchor.constraint(greaterThanOrEqualTo: title.trailingAnchor, constant: 6),
        ])
    }

    func outlineViewSelectionDidChange(_ notification: Notification) {
        guard !selecting, let item = outline.item(atRow: outline.selectedRow) as? String,
              let s = MainSection(rawValue: item) else { return }
        onSelect(s)
    }
}

/// The VM's state at the foot of the sidebar, with a start/stop button.
@MainActor
final class VMStatusView: NSView {
    private let icon = NSImageView()
    private let title = label(font: .systemFont(ofSize: NSFont.systemFontSize, weight: .medium), color: .labelColor)
    private let detail = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize), color: .secondaryLabelColor)
    private let power = NSButton()
    var onToggle: () -> Void = {}

    init() {
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        icon.symbolConfiguration = .init(pointSize: 15, weight: .medium)
        icon.translatesAutoresizingMaskIntoConstraints = false
        power.isBordered = false
        power.bezelStyle = .accessoryBarAction
        power.symbolConfiguration = .init(pointSize: 13, weight: .medium)
        power.contentTintColor = .secondaryLabelColor
        power.target = self
        power.action = #selector(toggle)
        power.translatesAutoresizingMaskIntoConstraints = false
        // Truncate rather than widen the sidebar when the summary gets longer.
        for l in [title, detail] {
            l.lineBreakMode = .byTruncatingTail
            l.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        }
        let text = NSStackView(views: [title, detail])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 1
        text.translatesAutoresizingMaskIntoConstraints = false
        for v in [icon, text, power] as [NSView] { addSubview(v) }
        NSLayoutConstraint.activate([
            heightAnchor.constraint(equalToConstant: 52),
            icon.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 18),
            icon.centerYAnchor.constraint(equalTo: centerYAnchor),
            icon.widthAnchor.constraint(equalToConstant: 22),
            text.leadingAnchor.constraint(equalTo: icon.trailingAnchor, constant: 8),
            text.centerYAnchor.constraint(equalTo: centerYAnchor),
            text.trailingAnchor.constraint(lessThanOrEqualTo: power.leadingAnchor, constant: -6),
            power.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -14),
            power.centerYAnchor.constraint(equalTo: centerYAnchor),
            power.widthAnchor.constraint(equalToConstant: 26),
            power.heightAnchor.constraint(equalToConstant: 26),
        ])
    }

    required init?(coder: NSCoder) { fatalError() }

    func update(_ model: AppModel) {
        let phase = model.phase
        icon.image = NSImage(systemSymbolName: phase.symbol, accessibilityDescription: phase.label)
        icon.contentTintColor = phase.color
        title.stringValue = phase.label
        detail.stringValue = statusSummary(model.status) ?? "Daemon not running"
        let up = model.status?.vm != nil
        power.image = NSImage(systemSymbolName: up ? "stop.circle" : "play.circle",
                              accessibilityDescription: up ? "Stop" : "Start")
        power.toolTip = up ? "Stop the VM" : "Start the VM"
        power.isEnabled = !model.busy && phase != .stopping && phase != .starting
    }

    @objc private func toggle() { onToggle() }
}

extension Phase {
    var color: NSColor {
        switch self {
        case .running: .systemGreen
        case .starting, .stopping: .systemOrange
        case .error: .systemRed
        case .stopped, .noDaemon: .secondaryLabelColor
        }
    }
}

/// "up 2h 5m · 4 CPUs · 4 GiB", or nil without a daemon.
func statusSummary(_ s: DaemonStatus?) -> String? {
    guard let s else { return nil }
    var parts: [String] = []
    if let up = s.uptime_s, Phase(s) == .running { parts.append("up \(duration(up))") }
    let cpus = s.vm?.cpus ?? s.config.cpus
    parts.append("\(cpus) CPU\(cpus == 1 ? "" : "s")")
    parts.append(gib(s.vm?.memory_mib ?? s.config.memory_mib))
    return parts.joined(separator: " · ")
}
