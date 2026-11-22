import AppKit

/// What a section shows instead of its list while it can't work yet.
struct BlockedState: Equatable {
    var symbol: String
    var title: String
    var message: String?
    var action: String?
    var busy: Bool = false
}

/// Pods and services share their empty states and namespace grouping.
@MainActor
class KubernetesPane: SectionPane, ResourceSource {
    let store: K8sStore
    let model: AppModel
    let kind: K8sStore.Kind
    private(set) lazy var listVC = ResourceListViewController(pane: self)
    private(set) lazy var detailVC = ResourceDetailViewController(pane: self)
    var onChange: () -> Void = {}
    var query = ""

    init(store: K8sStore, model: AppModel, kind: K8sStore.Kind) {
        self.store = store
        self.model = model
        self.kind = kind
        store.observe { [weak self] _ in self?.reloadAll() }
        model.observe { [weak self] in self?.onChange() }
    }

    func reloadAll() {
        listVC.reload()
        detailVC.reload()
        onChange()
    }

    var list: NSViewController { listVC }
    var detail: NSViewController? { detailVC }
    var needsDocker: Bool { false }
    var noun: String { "" }
    var emptySymbol: String { "" }
    var emptyMessage: String { "" }
    var isLoaded: Bool { store.loaded.contains(kind) }

    var subtitle: String {
        guard blockedState == nil else { return "" }
        guard isLoaded else { return "Loading…" }
        let n = rows().count
        return n == 0 ? "None" : "\(n) \(noun)\(n == 1 ? "" : "s")"
    }

    func search(_ text: String) {
        query = text
        listVC.reload()
    }

    func setActive(_ active: Bool) {
        if active { store.interest.insert(kind) } else { store.interest.remove(kind) }
    }

    // Overridden by panes with detail tabs.
    var tabs: [String] { [] }
    var enabledTabs: Set<Int> { [] }
    var selectedTab: Int {
        get { 0 }
        set {}
    }

    /// Rows of the namespaces shown, before the search.
    func rows() -> [ResourceRow] { [] }
    /// Each row's namespace, for grouping.
    func namespace(_ key: String) -> String { String(key.split(separator: "/", maxSplits: 1).first ?? "") }
    func searchable(_ row: ResourceRow) -> [String] { [row.title, row.subtitle, namespace(row.key)] }
    func row(_ key: String) -> ResourceRow? { rows().first { $0.key == key } }
    func headerSubtitle(_ key: String) -> String { "" }
    func details(_ key: String) -> [NSView] { [] }
    func detailVersion(_ key: String) -> AnyHashable? { nil }
    func reveal(_ key: String) -> URL? { nil }
    var revealTitle: String { "Open" }
    var revealSymbol: String { "safari" }
    func deleteMessage(_ key: String) -> String { "" }
    func delete(_ key: String) {}

    var groups: [(title: String, rows: [ResourceRow])] {
        K8sGrouping.byNamespace(rows(), query: query, namespace: namespace, search: searchable)
    }

    func confirmDelete(_ key: String) {
        guard let r = row(key) else { return }
        confirm(listVC.view.window, title: "Delete \(r.title)?", message: deleteMessage(key), button: "Delete") { [weak self] in
            self?.delete(key)
        }
    }

    var menuItems: [NSMenuItem] {
        let i = NSMenuItem(title: "Show System Namespaces", action: #selector(toggleSystem), keyEquivalent: "")
        i.target = self
        i.state = store.showSystem ? .on : .off
        return [i]
    }

    @objc private func toggleSystem() { store.showSystem.toggle() }

    var blockedState: BlockedState? {
        K8sGrouping.blocked(phase: model.phase, busy: model.busy, k8s: store.status,
                            connected: store.socket != nil, loaded: isLoaded, loadError: store.loadError(for: kind))
    }

    func blockedAction() {
        switch model.phase {
        case .running:
            if store.status?.state == "running", store.socket != nil {
                store.retry(kind)
            } else if store.status?.enabled == true || store.status?.state == "failed" {
                model.k8sStart()
            } else {
                model.saveConfig(["kubernetes": true])
            }
        case .error: model.restart()
        default: model.start()
        }
    }
}

/// The pure parts of the Kubernetes panes, for tests.
enum K8sGrouping {
    /// Rows by namespace (sorted, `default` first), filtered by `query`.
    static func byNamespace(_ rows: [ResourceRow], query: String, namespace: (String) -> String,
                            search: (ResourceRow) -> [String]) -> [(title: String, rows: [ResourceRow])] {
        let q = query.trimmingCharacters(in: .whitespaces).lowercased()
        let kept = rows.filter { q.isEmpty || search($0).contains { $0.lowercased().contains(q) } }
        let byNS = Dictionary(grouping: kept) { namespace($0.key) }
        let names = byNS.keys.sorted { a, b in
            if (a == "default") != (b == "default") { return a == "default" }
            return a < b
        }
        return names.map { ns in (ns, byNS[ns]!.sorted { $0.title.lowercased() < $1.title.lowercased() }) }
    }

    static func blocked(phase: Phase, busy: Bool, k8s: K8sStatus?, connected: Bool,
                        loaded: Bool, loadError: String?) -> BlockedState? {
        switch phase {
        case .noDaemon, .stopped:
            return BlockedState(symbol: "sailboat", title: "Katamaran is stopped",
                                message: "Start the VM to use Kubernetes.", action: "Start", busy: busy)
        case .starting:
            return BlockedState(symbol: "sailboat", title: "Starting…", busy: true)
        case .stopping:
            return BlockedState(symbol: "sailboat", title: "Stopping…", busy: true)
        case .error:
            return BlockedState(symbol: "exclamationmark.triangle", title: "Katamaran ran into a problem",
                                action: "Restart", busy: busy)
        case .running:
            break
        }
        guard let k8s else { return BlockedState(symbol: "helm", title: "Waiting for Kubernetes…", busy: true) }
        switch k8s.state {
        case "starting":
            return BlockedState(symbol: "helm", title: "Starting Kubernetes…",
                                message: "The cluster will be ready in a moment.", busy: true)
        case "stopping":
            return BlockedState(symbol: "helm", title: "Stopping Kubernetes…", busy: true)
        case "running":
            if !loaded, let loadError {
                return BlockedState(symbol: "exclamationmark.triangle", title: "Couldn’t connect to Kubernetes",
                                    message: loadError, action: "Try Again", busy: busy)
            }
            if !connected || !loaded {
                return BlockedState(symbol: "helm", title: "Connecting to Kubernetes…", busy: true)
            }
            return nil
        case "failed":
            let err = k8s.error.flatMap { $0.isEmpty ? nil : $0 }
            return BlockedState(symbol: "exclamationmark.triangle", title: "Kubernetes ran into a problem",
                                message: err, action: "Try Again", busy: busy)
        default:
            if let err = k8s.error, !err.isEmpty, k8s.enabled {
                return BlockedState(symbol: "exclamationmark.triangle", title: "Kubernetes ran into a problem",
                                    message: err, action: "Try Again", busy: busy)
            }
            if k8s.enabled {
                return BlockedState(symbol: "helm", title: "Starting Kubernetes…", busy: true)
            }
            return BlockedState(symbol: "helm", title: "Kubernetes is off",
                                message: "Turn it on to run a local k3s cluster. Your kubeconfig gets a katamaran context.",
                                action: "Turn On", busy: busy)
        }
    }

    /// "Running · 1/1 · 3 restarts · 2h"
    static func podSubtitle(_ p: K8sPod, now: Date = Date()) -> String {
        var parts = [p.displayStatus, "\(p.readyCount)/\(p.containerCount)"]
        let r = p.restarts
        if r > 0 { parts.append("\(r) restart\(r == 1 ? "" : "s")") }
        parts.append(compactAge(p.metadata.created, now: now))
        return parts.joined(separator: " · ")
    }

    /// "LoadBalancer · 80:30080/TCP"
    static func serviceSubtitle(_ s: K8sService) -> String {
        [s.type, s.portsLabel].filter { !$0.isEmpty }.joined(separator: " · ")
    }
}

// MARK: - Pods

@MainActor
final class PodsPane: KubernetesPane {
    private lazy var logs = PodLogsView(store: store)
    private var tab = 0
    private var isActive = false

    init(store: K8sStore, model: AppModel) {
        super.init(store: store, model: model, kind: .pods)
        detailVC.onSelect = { [weak self] _ in self?.showTab() }
        store.observeTick { [weak self] in self?.refreshAges() }
    }

    /// Re-render visible pod rows on timer ticks so `compactAge` advances even
    /// when the API response is unchanged. `ResourceListViewController.reload()`
    /// diffs rows and skips work when no displayed age unit changed.
    func refreshAges() {
        guard isActive, !store.pods.isEmpty else { return }
        listVC.reload()
    }

    private var selectedPod: K8sPod? {
        detailVC.key.flatMap(store.pod).flatMap { store.visible($0.namespace) ? $0 : nil }
    }

    override var noun: String { "pod" }
    override var emptySymbol: String { "cube" }
    override var emptyMessage: String { "Pods appear here when you deploy to the cluster, for example with kubectl apply." }

    override func reloadAll() {
        super.reloadAll()
        let p = selectedPod
        logs.setActive(isActive && tab == 1 && p != nil)
        logs.show(p)
    }

    override func detailVersion(_ key: String) -> AnyHashable? { store.pod(key) }

    override func rows() -> [ResourceRow] {
        store.pods.filter { store.visible($0.namespace) }.map { p in
            let h = p.health
            return ResourceRow(key: p.key, title: p.name, subtitle: K8sGrouping.podSubtitle(p),
                               symbol: "cube.fill", tint: .systemBlue,
                               inUse: h != .stopped, deletable: true, dot: h.color)
        }
    }

    override func headerSubtitle(_ key: String) -> String {
        guard let p = store.pod(key) else { return "" }
        return [p.namespace, p.displayStatus].joined(separator: " · ")
    }

    override func details(_ key: String) -> [NSView] {
        guard let p = store.pod(key) else { return [] }
        var rows: [NSView] = [
            InfoRow("Status", p.displayStatus),
            InfoRow("Ready", "\(p.readyCount)/\(p.containerCount)"),
            InfoRow("Restarts", "\(p.restarts)"),
        ]
        if let created = p.metadata.created { rows.append(InfoRow("Created", "\(absolute(created)) (\(relative(created)))")) }
        rows.append(InfoRow("Namespace", p.namespace))
        if let o = p.owner { rows.append(InfoRow("Controlled By", "\(o.kind) \(o.name)")) }
        if let n = p.spec?.nodeName, !n.isEmpty { rows.append(InfoRow("Node", n)) }
        if let ip = p.status?.podIP, !ip.isEmpty { rows.append(InfoRow("Pod IP", ip, mono: true)) }
        if let m = p.status?.message, !m.isEmpty { rows.append(InfoRow("Message", m)) }
        var groups: [NSView] = [FormGroup(header: "Pod", rows: rows)]
        let statuses = Dictionary(p.statuses.map { ($0.name, $0) }, uniquingKeysWith: { a, _ in a })
        let containers = (p.spec?.containers ?? []).map { c -> NSView in
            let s = statuses[c.name]
            var value = [c.image ?? s?.image ?? "-"]
            if let st = s?.state { value.append(st.label) }
            if let r = s?.restartCount, r > 0 { value.append("\(r) restart\(r == 1 ? "" : "s")") }
            return InfoRow(c.name, value.joined(separator: " · "), mono: true)
        }
        if !containers.isEmpty { groups.append(FormGroup(header: "Containers", rows: containers)) }
        if let labels = p.metadata.labels, !labels.isEmpty {
            groups.append(FormGroup(header: "Labels", rows: labels.sorted { $0.key < $1.key }.map { InfoRow($0.key, $0.value, mono: true) }))
        }
        return groups
    }

    override func deleteMessage(_ key: String) -> String {
        guard let p = store.pod(key) else { return "" }
        if let o = p.owner { return "Its \(o.kind) \(o.name) may start a new one in its place." }
        return "It isn’t managed by a controller, so it won’t come back."
    }

    override func delete(_ key: String) {
        guard let p = store.pod(key) else { return }
        let (ns, name) = (p.namespace, p.name)
        store.perform("Couldn’t delete \(name)") { try $0.deletePod(namespace: ns, name: name) }
    }

    // Tabs: Info and Logs.
    override var tabs: [String] { ["Info", "Logs"] }
    override var enabledTabs: Set<Int> { selectedPod == nil ? [] : [0, 1] }
    override var selectedTab: Int {
        get { tab }
        set { tab = newValue; showTab() }
    }

    private func showTab() {
        let p = selectedPod
        let showLogs = tab == 1 && p != nil
        detailVC.setBody(showLogs ? logs : nil)
        logs.setActive(isActive && showLogs)
        logs.show(p)
        onChange()
    }

    override func setActive(_ active: Bool) {
        isActive = active
        super.setActive(active)
        logs.setActive(active && tab == 1 && selectedPod != nil)
    }
}

/// A pod's log, followed, with a container picker for multi-container pods.
@MainActor
final class PodLogsView: NSView, NSSearchFieldDelegate {
    private let store: K8sStore
    private let text = LogTextView()
    private let filter = NSSearchField()
    private let picker = NSPopUpButton()
    private let follow = NSButton(checkboxWithTitle: "Follow", target: nil, action: nil)
    private var stream: DockerStream?
    /// What the stream follows: pod key and container.
    private var streaming: String?
    private var lines = LineSplitter()
    private var pod: K8sPod?
    private var active = false

    init(store: K8sStore) {
        self.store = store
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        filter.placeholderString = "Filter"
        filter.delegate = self
        filter.sendsWholeSearchString = false
        picker.target = self
        picker.action = #selector(containerChanged)
        follow.state = .on
        follow.target = self
        follow.action = #selector(followChanged)
        let clear = NSButton(image: NSImage(systemSymbolName: "trash", accessibilityDescription: "Clear") ?? NSImage(),
                             target: self, action: #selector(clearText))
        clear.bezelStyle = .push
        clear.toolTip = "Clear"
        let bar = NSStackView(views: [picker, filter, follow, clear])
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

    func show(_ p: K8sPod?) {
        let names = p?.containerNames ?? []
        if names != picker.itemTitles {
            let keep = picker.titleOfSelectedItem
            picker.removeAllItems()
            picker.addItems(withTitles: names)
            if let keep, names.contains(keep) { picker.selectItem(withTitle: keep) }
        }
        picker.isHidden = names.count < 2
        // Restart following when the pod is replaced (UID changes) or its
        // selected container restarts, cancelling any previous stream via
        // update(). Also retry a finished/failed stream when the container's
        // state transitions (e.g. ContainerCreating -> Running).
        if let p, let old = pod, old.key == p.key {
            let selected = picker.titleOfSelectedItem
            let oldStatus = old.statuses.first { $0.name == selected }
            let newStatus = p.statuses.first { $0.name == selected }
            let restarted = selected != nil
                ? (newStatus?.restartCount ?? 0) != (oldStatus?.restartCount ?? 0)
                : p.restarts != old.restarts
            let stateChanged = stream == nil
                && (p.displayStatus != old.displayStatus || newStatus?.state != oldStatus?.state)
            if p.metadata.uid != old.metadata.uid || restarted || stateChanged {
                streaming = nil
            }
        }
        pod = p
        update()
    }

    func setActive(_ a: Bool) {
        active = a
        update()
    }

    private func update() {
        let container = picker.titleOfSelectedItem
        let socket = store.socket
        let want: String? = if active, let pod, let socket {
            "\(socket)|\(pod.key)|\(pod.metadata.uid ?? "")|\(container ?? "")"
        } else {
            nil
        }
        guard want != streaming else { return }
        stream?.cancel()
        stream = nil
        streaming = want
        text.clear()
        guard want != nil, let pod, let socket else { return }
        lines = LineSplitter()
        let path = K8sClient.logPath(namespace: pod.namespace, pod: pod.name, container: container)
        let s = DockerStream(socketPath: socket, path: path, host: K8sClient.host, service: "Kubernetes")
        s.onData = { [weak self] d in self?.received(d) }
        s.onEnd = { [weak self, weak s] err in
            guard let self, self.stream === s else { return }
            if let rest = self.lines.flush() { self.text.append([String(decoding: rest, as: UTF8.self)], error: false) }
            if let err { self.text.append(["- \(err.localizedDescription) -"], error: true) }
            if self.follow.state == .on { self.text.scrollToEnd() }
            self.stream = nil
        }
        stream = s
        s.start()
    }

    private func received(_ data: Data) {
        let out = lines.feed(data).map { String(decoding: $0, as: UTF8.self) }
        if !out.isEmpty { text.append(out, error: false) }
        if follow.state == .on { text.scrollToEnd() }
    }

    func controlTextDidChange(_ obj: Notification) { text.filter = filter.stringValue }

    @objc private func containerChanged() { update() }
    @objc private func followChanged() { if follow.state == .on { text.scrollToEnd() } }
    @objc private func clearText() { text.clear() }
}

// MARK: - Services

@MainActor
final class ServicesPane: KubernetesPane {
    init(store: K8sStore, model: AppModel) {
        super.init(store: store, model: model, kind: .services)
    }

    override var noun: String { "service" }
    override var emptySymbol: String { "network" }
    override var emptyMessage: String { "Services expose pods. LoadBalancer and NodePort services open on localhost." }

    /// A service's details list the pods it selects.
    override func setActive(_ active: Bool) {
        if active { store.interest.formUnion([.services, .pods]) } else { store.interest.subtract([.services, .pods]) }
    }

    override func rows() -> [ResourceRow] {
        store.services.filter { store.visible($0.namespace) }.map { s in
            ResourceRow(key: s.key, title: s.name, subtitle: K8sGrouping.serviceSubtitle(s),
                        symbol: "network", tint: .systemPurple, inUse: true,
                        deletable: !(s.namespace == "default" && s.name == "kubernetes"))
        }
    }

    override func headerSubtitle(_ key: String) -> String {
        guard let s = store.service(key) else { return "" }
        return [s.namespace, s.type].joined(separator: " · ")
    }

    override func reveal(_ key: String) -> URL? {
        store.service(key)?.localPorts.first.flatMap { URL(string: "http://localhost:\($0)") }
    }

    private struct ServiceDetailState: Hashable {
        var service: K8sService
        var pods: [String]
    }

    override func detailVersion(_ key: String) -> AnyHashable? {
        guard let s = store.service(key) else { return nil }
        return ServiceDetailState(service: s, pods: store.pods(for: s).map(\.name).sorted())
    }

    override func details(_ key: String) -> [NSView] {
        guard let s = store.service(key) else { return [] }
        var rows: [NSView] = [InfoRow("Type", s.type)]
        if let ip = s.spec?.clusterIP, !ip.isEmpty { rows.append(InfoRow("Cluster IP", ip, mono: true)) }
        if let created = s.metadata.created { rows.append(InfoRow("Created", "\(absolute(created)) (\(relative(created)))")) }
        rows.append(InfoRow("Namespace", s.namespace))
        rows.append(InfoRow("In the Cluster", "\(s.name).\(s.namespace).svc.cluster.local", mono: true))
        var groups: [NSView] = [FormGroup(header: "Service", rows: rows)]
        let ports = s.ports.map { p -> NSView in
            var v = "\(p.port)"
            if let t = p.targetPort { v += " → \(t)" }
            v += "/\(p.protocol ?? "TCP")"
            if let n = p.nodePort { v += " · node port \(n)" }
            return InfoRow(p.name ?? "port", v, mono: true)
        }
        if !ports.isEmpty { groups.append(FormGroup(header: "Ports", rows: ports)) }
        let local = s.localPorts.compactMap { URL(string: "http://localhost:\($0)") }
        if !local.isEmpty {
            groups.append(FormGroup(header: "On this Mac", rows: [InfoRow("Open", view: URLLinks(local))]))
        }
        let pods = store.pods(for: s)
        if let sel = s.spec?.selector, !sel.isEmpty {
            groups.append(FormGroup(header: "Selector", rows: [
                InfoRow("Labels", sel.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }.joined(separator: ", "), mono: true),
                InfoRow("Pods", pods.isEmpty ? "None" : pods.map(\.name).sorted().joined(separator: "\n")),
            ]))
        }
        if let labels = s.metadata.labels, !labels.isEmpty {
            groups.append(FormGroup(header: "Labels", rows: labels.sorted { $0.key < $1.key }.map { InfoRow($0.key, $0.value, mono: true) }))
        }
        return groups
    }

    override func deleteMessage(_ key: String) -> String {
        "Its pods keep running, but the service’s address and ports go away."
    }

    override func delete(_ key: String) {
        guard let s = store.service(key) else { return }
        let (ns, name) = (s.namespace, s.name)
        store.perform("Couldn’t delete \(name)") { try $0.deleteService(namespace: ns, name: name) }
    }
}

/// URLs as links in a details row.
@MainActor
final class URLLinks: NSStackView {
    private let urls: [URL]

    init(_ urls: [URL]) {
        self.urls = urls
        super.init(frame: .zero)
        orientation = .vertical
        alignment = .leading
        spacing = 4
        for (i, u) in urls.enumerated() {
            addArrangedSubview(linkButton(u.absoluteString, target: self, action: #selector(open(_:)), tag: i))
        }
    }

    required init(coder: NSCoder) { fatalError() }

    @objc private func open(_ sender: NSButton) {
        if let u = urls[safe: sender.tag] { NSWorkspace.shared.open(u) }
    }
}
