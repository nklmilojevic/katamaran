import AppKit

/// Settings in the System Settings style: toolbar panes of grouped rows in
/// system colors, following the system appearance. A change is saved to
/// `<home>/config.toml` through the daemon as soon as it is made; when the
/// running VM needs a restart for it, a note with a Restart button appears at
/// the top of the pane.
@MainActor
final class SettingsWindowController: NSWindowController {
    private let model: AppModel
    private let limits = ResourceLimits.host
    private let tabs = SettingsTabViewController()
    private var notices: [NoticeGroup] = []

    private let login = NSSwitch()
    private let mount = NSSwitch()
    private let autoUpdates = NSSwitch()
    private let checkUpdatesBtn = NSButton(title: "Check Now", target: nil, action: nil)
    private var updatesCheckRow: FormRow?
    private let k8sSwitch = NSSwitch()
    private let k8sSyncBtn = NSButton(title: "Sync Images", target: nil, action: nil)
    private let k8sResetBtn = NSButton(title: "Reset Cluster", target: nil, action: nil)
    private let k8sStatusLabel = NSTextField(labelWithString: "Disabled")
    private var k8sRow: FormRow!
    private var k8sStatusRow: FormRow!
    private let cpuField = NSTextField()
    private let cpuStepper = NSStepper()
    private let memSlider = NSSlider()
    private let memValue = NSTextField(labelWithString: "")
    private var mountRow: FormRow!

    /// What the controls show, and the daemon's config when last looked at.
    private var shown: Config?
    private var seen: Config?

    private static let paneKey = "settingsPane"

    init(model: AppModel) {
        self.model = model
        let win = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 520, height: 200),
                         styleMask: [.titled, .closable, .fullSizeContentView],
                         backing: .buffered, defer: false)
        win.toolbarStyle = .preference
        win.titlebarSeparatorStyle = .none
        win.isReleasedWhenClosed = false
        super.init(window: win)
        build()
        model.observe { [weak self] in self?.sync() }
        Updater.shared.observe { [weak self] in self?.syncUpdates() }
    }

    required init?(coder: NSCoder) { fatalError() }

    // MARK: Building

    private func build() {
        tabs.tabStyle = .toolbar
        tabs.transitionOptions = .crossfade
        var generalViews: [NSView] = [generalGroup()]
        if Updater.shared.isConfigured {
            generalViews.append(updatesGroup())
        }
        let panes: [SettingsPane: [NSView]] = [
            .general: generalViews,
            .resources: [resourcesGroup()],
            .kubernetes: [kubernetesGroup()],
        ]
        for pane in SettingsPane.allCases {
            let notice = NoticeGroup()
            notice.onAction = { [weak self] in self?.noticeAction() }
            notices.append(notice)
            let vc = PaneViewController(title: pane.title, views: [notice] + (panes[pane] ?? []))
            let item = NSTabViewItem(viewController: vc)
            item.label = pane.title
            item.image = NSImage(systemSymbolName: pane.symbol, accessibilityDescription: pane.title)
            item.identifier = pane.rawValue
            tabs.addTabViewItem(item)
        }
        let saved = UserDefaults.standard.string(forKey: Self.paneKey).flatMap(SettingsPane.init(rawValue:))
        tabs.selectedTabViewItemIndex = SettingsPane.allCases.firstIndex(of: saved ?? .general) ?? 0
        tabs.onSelect = { [weak self] pane in
            UserDefaults.standard.set(pane.rawValue, forKey: Self.paneKey)
            self?.fitWindow(animated: true)
        }
        window?.contentViewController = tabs
    }

    private func generalGroup() -> NSView {
        for toggle in [login, mount] {
            toggle.controlSize = .mini
            toggle.target = self
        }
        login.action = #selector(loginChanged)
        mount.action = #selector(mountChanged)
        mountRow = FormRow("Show VM files in Finder", control: mount)
        return FormGroup(footer: "Turning file sharing on or off takes effect the next time the VM starts.",
                         rows: [FormRow("Start Katamaran at login", control: login), mountRow])
    }

    private func updatesGroup() -> NSView {
        autoUpdates.controlSize = .mini
        autoUpdates.target = self
        autoUpdates.action = #selector(autoUpdatesChanged)
        checkUpdatesBtn.target = self
        checkUpdatesBtn.action = #selector(checkUpdatesClicked)
        checkUpdatesBtn.bezelStyle = .rounded
        checkUpdatesBtn.controlSize = .small
        let checkRow = FormRow(
            "Software updates",
            subtitle: Updater.versionSubtitle(info: Bundle.main.infoDictionary),
            control: checkUpdatesBtn
        )
        updatesCheckRow = checkRow
        return FormGroup(
            header: "Updates",
            footer: "Checks once a day and shows an update dialog when a new version is available.",
            rows: [
                FormRow("Check for updates automatically", control: autoUpdates),
                checkRow,
            ]
        )
    }

    private func resourcesGroup() -> NSView {
        let formatter = NumberFormatter()
        formatter.allowsFloats = false
        formatter.minimum = NSNumber(value: limits.cpus.lowerBound)
        formatter.maximum = NSNumber(value: limits.cpus.upperBound)
        cpuField.formatter = formatter
        cpuField.alignment = .right
        cpuField.bezelStyle = .roundedBezel
        cpuField.font = .monospacedDigitSystemFont(ofSize: NSFont.systemFontSize, weight: .regular)
        cpuField.target = self
        cpuField.action = #selector(cpuTyped)
        cpuField.translatesAutoresizingMaskIntoConstraints = false
        cpuField.widthAnchor.constraint(equalToConstant: 44).isActive = true
        cpuStepper.minValue = Double(limits.cpus.lowerBound)
        cpuStepper.maxValue = Double(limits.cpus.upperBound)
        cpuStepper.increment = 1
        cpuStepper.valueWraps = false
        cpuStepper.target = self
        cpuStepper.action = #selector(cpuStepped)
        let cpu = NSStackView(views: [cpuField, cpuStepper])
        cpu.spacing = 4

        memSlider.minValue = Double(limits.memoryGiB.lowerBound)
        memSlider.maxValue = Double(limits.memoryGiB.upperBound)
        if limits.memoryTickMarks > 0 {
            memSlider.numberOfTickMarks = limits.memoryTickMarks
            memSlider.allowsTickMarkValuesOnly = true
        }
        memSlider.isContinuous = true
        memSlider.target = self
        memSlider.action = #selector(memoryMoved)
        memSlider.translatesAutoresizingMaskIntoConstraints = false
        memSlider.widthAnchor.constraint(equalToConstant: 200).isActive = true
        memValue.font = .monospacedDigitSystemFont(ofSize: NSFont.systemFontSize, weight: .regular)
        memValue.textColor = .secondaryLabelColor
        memValue.alignment = .right
        memValue.translatesAutoresizingMaskIntoConstraints = false
        memValue.widthAnchor.constraint(equalToConstant: 48).isActive = true
        let memory = NSStackView(views: [memSlider, memValue])
        memory.spacing = 8

        let mac = memoryLabel(gib: Int(ProcessInfo.processInfo.physicalMemory >> 30))
        return FormGroup(
            footer: "The VM gets these the next time it starts.",
            rows: [
                FormRow("CPUs", subtitle: "This Mac has \(limits.cpus.upperBound).", control: cpu),
                FormRow("Memory",
                        subtitle: "A limit: what the VM doesn’t use goes back to this Mac (\(mac)). "
                            + "Default \(memoryLabel(gib: limits.defaultMemoryGiB)).",
                        control: memory),
            ])
    }

    private func kubernetesGroup() -> NSView {
        k8sSwitch.controlSize = .mini
        k8sSwitch.target = self
        k8sSwitch.action = #selector(k8sChanged)
        k8sSyncBtn.target = self
        k8sSyncBtn.action = #selector(k8sSyncClicked)
        k8sSyncBtn.bezelStyle = .rounded
        k8sSyncBtn.controlSize = .small
        k8sResetBtn.target = self
        k8sResetBtn.action = #selector(k8sResetClicked)
        k8sResetBtn.bezelStyle = .rounded
        k8sResetBtn.controlSize = .small
        k8sStatusLabel.textColor = .secondaryLabelColor
        k8sStatusLabel.font = .systemFont(ofSize: NSFont.smallSystemFontSize)
        let actions = NSStackView(views: [k8sSyncBtn, k8sResetBtn])
        actions.spacing = 8
        k8sRow = FormRow(
            "Enable Kubernetes",
            subtitle: "Single-node k3s cluster sharing Docker’s containerd. Off uses zero resources.",
            control: k8sSwitch
        )
        k8sStatusRow = FormRow("Cluster status", control: k8sStatusLabel)
        return FormGroup(
            footer: "Updates ~/.kube/config automatically. Run `kat k8s dns install` for *.k8s.kat.local.",
            rows: [
                k8sRow,
                k8sStatusRow,
                FormRow("Cluster actions", control: actions),
            ]
        )
    }

    // MARK: Showing

    /// Open on `pane`, or on the pane last used.
    func present(pane: SettingsPane? = nil) {
        if let pane, let idx = SettingsPane.allCases.firstIndex(of: pane) { tabs.selectedTabViewItemIndex = idx }
        seen = nil
        sync()
        NSApp.activate()
        if !(window?.isVisible ?? false) {
            fitWindow(animated: false)
            window?.center()
        }
        showWindow(nil)
        window?.makeKeyAndOrderFront(nil)
        // No focus ring on the first control, as in System Settings.
        window?.makeFirstResponder(nil)
    }

    /// Follow the daemon: the note, which controls work, and the saved values
    /// when they changed elsewhere (the CLI, or a failed save).
    private func sync() {
        let status = model.status
        let config = status?.config
        if config != seen && !model.busy {
            seen = config
            if config != shown {
                shown = config
                showValues()
            }
        }
        for control in [login, mount, cpuField, cpuStepper, memSlider] as [NSControl] { control.isEnabled = status != nil }
        let k8s = status?.k8s
        let k8sTransitioning = k8s?.state == "starting" || k8s?.state == "stopping"
        k8sSwitch.isEnabled = status != nil && !model.busy
        let k8sRunning = k8s?.state == "running"
        k8sSyncBtn.isEnabled = status != nil && k8sRunning && !model.busy
        k8sResetBtn.isEnabled = status != nil && !model.busy && !k8sTransitioning
        if let k8s, k8s.enabled || k8s.state != "stopped" {
            var desc = k8s.state.capitalized
            if let version = k8s.version, !version.isEmpty { desc += " (\(version))" }
            if let port = k8s.apiPort { desc += " · 127.0.0.1:\(port)" }
            if let err = k8s.error, !err.isEmpty { desc = "Error: \(err)" }
            k8sStatusLabel.stringValue = desc
            k8sStatusRow.subtitle = k8s.context.map { "Context: \($0)" }
        } else {
            k8sStatusLabel.stringValue = "Disabled"
            k8sStatusRow.subtitle = nil
        }
        let path = status?.mount.path ?? config?.mount_path
        mountRow.subtitle = path.map { "At \(($0 as NSString).abbreviatingWithTildeInPath)" }

        let notice = SettingsNotice(status: status, restartRequired: model.restartRequired, lastError: model.lastError)
        var changed = false
        for noticeView in notices { changed = noticeView.show(notice, busy: model.busy) || changed }
        if changed { fitWindow(animated: window?.isVisible ?? false) }
        syncUpdates()
    }

    private func syncUpdates() {
        guard Updater.shared.isConfigured else { return }
        autoUpdates.state = Updater.shared.automaticallyChecksForUpdates ? .on : .off
        checkUpdatesBtn.isEnabled = Updater.shared.canCheckForUpdates
        let pending = Updater.shared.pendingUpdateVersion
        checkUpdatesBtn.title = pending != nil ? "View Update…" : "Check Now"
        updatesCheckRow?.subtitle = Updater.versionSubtitle(
            info: Bundle.main.infoDictionary,
            pendingVersion: pending
        )
    }

    private func showValues() {
        guard let cfg = shown else { return }
        login.state = cfg.launch_at_login ? .on : .off
        mount.state = cfg.mount ? .on : .off
        k8sSwitch.state = (cfg.kubernetes ?? false) ? .on : .off
        let cpus = limits.clampCPUs(cfg.cpus)
        cpuField.integerValue = cpus
        cpuStepper.integerValue = cpus
        let gib = limits.memoryGiB(fromMiB: cfg.memory_mib)
        memSlider.integerValue = gib
        memValue.stringValue = memoryLabel(gib: gib)
    }

    /// Size the window to the selected pane, keeping its top edge in place.
    /// The pane runs under the toolbar, so its glass has content behind it.
    private func fitWindow(animated: Bool) {
        guard let window, let pane = tabs.selectedPane as? PaneViewController else { return }
        let bars = window.frame.height - window.contentLayoutRect.height
        let size = NSSize(width: FormLayout.width + 2 * FormLayout.margin, height: pane.fittingHeight + bars)
        var frame = window.frameRect(forContentRect: NSRect(origin: .zero, size: size))
        frame.origin = NSPoint(x: window.frame.minX, y: window.frame.maxY - frame.height)
        guard frame != window.frame else { return }
        window.setFrame(frame, display: true, animate: animated)
    }

    // MARK: Changes

    /// Save `change` right away, sending only the keys it touched.
    private func apply(_ change: (inout Config) -> Void) {
        guard let before = shown else { return }
        var next = before
        change(&next)
        let values = next.values(changedFrom: before)
        guard !values.isEmpty else { return }
        shown = next
        model.saveConfig(values) { [weak self] ok in
            guard let self, !ok else { return }
            self.seen = nil
            if !self.model.busy {
                // Show what is actually saved.
                self.shown = self.model.status?.config
                self.showValues()
            }
        }
    }

    @objc private func loginChanged() { apply { $0.launch_at_login = login.state == .on } }

    @objc private func mountChanged() { apply { $0.mount = mount.state == .on } }

    @objc private func autoUpdatesChanged() {
        Updater.shared.automaticallyChecksForUpdates = autoUpdates.state == .on
    }

    @objc private func checkUpdatesClicked() {
        Updater.shared.checkForUpdates(checkUpdatesBtn)
    }

    @objc private func k8sChanged() { apply { $0.kubernetes = k8sSwitch.state == .on } }

    @objc private func k8sSyncClicked() { model.k8sSyncImages() }

    @objc private func k8sResetClicked() {
        let alert = NSAlert()
        alert.messageText = "Reset Kubernetes cluster?"
        alert.informativeText = "This stops Kubernetes and deletes all cluster state, workloads, and persistent volumes. Cached images in containerd’s k8s.io namespace are kept by default; Docker images are untouched."
        alert.alertStyle = .warning
        let removeImages = NSButton(
            checkboxWithTitle: "Also remove cached Kubernetes images (k8s.io)",
            target: nil,
            action: nil
        )
        removeImages.state = .off
        removeImages.sizeToFit()
        alert.accessoryView = removeImages
        alert.addButton(withTitle: "Reset")
        alert.addButton(withTitle: "Cancel")
        if alert.runModal() == .alertFirstButtonReturn {
            model.k8sReset(images: removeImages.state == .on)
        }
    }

    @objc private func cpuStepped() {
        cpuField.integerValue = cpuStepper.integerValue
        apply { $0.cpus = cpuStepper.integerValue }
    }

    @objc private func cpuTyped() {
        let count = limits.clampCPUs(cpuField.integerValue)
        cpuField.integerValue = count
        cpuStepper.integerValue = count
        apply { $0.cpus = count }
    }

    @objc private func memoryMoved() {
        let gib = Int(memSlider.doubleValue.rounded())
        memValue.stringValue = memoryLabel(gib: gib)
        // Save once the knob is let go, not at every step of a drag.
        if let type = NSApp.currentEvent?.type, type == .leftMouseDown || type == .leftMouseDragged { return }
        apply { $0.memory_mib = gib * 1024 }
    }

    private func noticeAction() {
        switch SettingsNotice(status: model.status, restartRequired: model.restartRequired, lastError: model.lastError) {
        case .daemonNotRunning: model.start()
        case .restartRequired: model.restart()
        default: break
        }
    }
}

/// The tab view controller, telling which pane got selected.
@MainActor
private final class SettingsTabViewController: NSTabViewController {
    var onSelect: (SettingsPane) -> Void = { _ in }

    var selectedPane: NSViewController? {
        let idx = selectedTabViewItemIndex
        return tabViewItems.indices.contains(idx) ? tabViewItems[idx].viewController : nil
    }

    override func tabView(_ tabView: NSTabView, didSelect item: NSTabViewItem?) {
        super.tabView(tabView, didSelect: item)
        if let id = item?.identifier as? String, let pane = SettingsPane(rawValue: id) { onSelect(pane) }
    }
}

/// One pane: its groups stacked at the form's width.
@MainActor
private final class PaneViewController: NSViewController {
    private let views: [NSView]

    init(title: String, views: [NSView]) {
        self.views = views
        super.init(nibName: nil, bundle: nil)
        self.title = title
    }

    required init?(coder: NSCoder) { fatalError() }

    private let stack = NSStackView()

    /// The height the pane needs below the toolbar.
    var fittingHeight: CGFloat {
        _ = view
        stack.layoutSubtreeIfNeeded()
        return stack.fittingSize.height + 2 * FormLayout.margin
    }

    override func loadView() {
        views.forEach(stack.addArrangedSubview)
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 20
        stack.translatesAutoresizingMaskIntoConstraints = false
        for item in views { item.widthAnchor.constraint(equalToConstant: FormLayout.width).isActive = true }
        let root = NSView()
        root.addSubview(stack)
        let margin = FormLayout.margin
        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor, constant: margin),
            stack.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: margin),
        ])
        view = root
    }
}

/// The note above a pane's groups: a colored symbol, what happened, and the
/// button that deals with it. Hidden when there is nothing to say.
@MainActor
private final class NoticeGroup: NSView {
    var onAction: () -> Void = {}
    private let icon = symbolView("circle", color: .systemGray)
    private let button = NSButton(title: "", target: nil, action: nil)
    private var row: FormRow!
    private var current: SettingsNotice?

    init() {
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        button.bezelStyle = .push
        if #available(macOS 26.0, *) { button.tintProminence = .primary }
        button.target = self
        button.action = #selector(act)
        button.setContentHuggingPriority(.required, for: .horizontal)
        row = FormRow("", icon: icon, control: button)
        row.titleLabel.lineBreakMode = .byWordWrapping
        row.titleLabel.maximumNumberOfLines = 0
        row.titleLabel.preferredMaxLayoutWidth = FormLayout.width - 160
        let group = FormGroup(rows: [row])
        addSubview(group)
        NSLayoutConstraint.activate([
            group.leadingAnchor.constraint(equalTo: leadingAnchor),
            group.trailingAnchor.constraint(equalTo: trailingAnchor),
            group.topAnchor.constraint(equalTo: topAnchor),
            group.bottomAnchor.constraint(equalTo: bottomAnchor),
        ])
        isHidden = true
    }

    required init?(coder: NSCoder) { fatalError() }

    /// Returns whether the note appeared, went away or changed size.
    func show(_ notice: SettingsNotice?, busy: Bool) -> Bool {
        button.isEnabled = !busy
        guard notice != current else { return false }
        current = notice
        isHidden = notice == nil
        guard let notice else { return true }
        let color: NSColor = switch notice {
        case .daemonNotRunning: .systemGray
        case .saveFailed, .kubernetesFailed: .systemRed
        case .restartRequired: .systemOrange
        }
        icon.image = symbolView(notice.symbol, color: color).image
        row.titleLabel.stringValue = notice.message
        button.title = notice.action ?? ""
        button.isHidden = notice.action == nil
        return true
    }

    @objc private func act() { onAction() }
}
