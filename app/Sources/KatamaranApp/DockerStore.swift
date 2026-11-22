import AppKit

/// What Docker has, kept current from its event stream: a change refetches
/// the affected list once, however many events arrive together. Volume sizes
/// cost Docker a walk of every volume, so volumes, images and networks are
/// only fetched while something shows them.
@MainActor
final class DockerStore {
    static let shared = DockerStore()

    private(set) var socket: String?
    /// Monotonic generation incremented on every socket transition so completions from an
    /// older connection to the same socket path are rejected.
    private(set) var connectionEpoch: UInt64 = 0
    private(set) var containers: [ContainerSummary] = []
    private(set) var volumes: [VolumeInfo] = []
    private(set) var images: [ImageInfo] = []
    private(set) var networks: [NetworkInfo] = []
    /// The lists fetched at least once since Docker came up.
    private(set) var loaded: Set<DockerKind> = []
    private(set) var loadError: String?
    /// Kinds with a fetch running, for spinners.
    private(set) var loading: Set<DockerKind> = []

    /// Kinds a visible view shows; containers always.
    var interest: Set<DockerKind> = [.containers] {
        didSet {
            let fresh = interest.subtracting(oldValue).subtracting(loading)
            if !fresh.isEmpty { refresh(fresh) }
        }
    }

    /// Shown to the user when an action fails.
    var onError: (String, String) -> Void = { _, _ in }

    private var observers: [(DockerStore) -> Void] = []
    private var events: DockerStream?
    private var eventLines = LineSplitter()
    private var pending: Set<DockerKind> = []
    private var flush: DispatchWorkItem?
    /// Containers an event named since the last flush, for open detail views.
    private var touched: Set<String> = []
    private var retry: DispatchWorkItem?

    var client: DockerClient? { socket.map(DockerClient.init) }

    func observe(_ f: @escaping (DockerStore) -> Void) { observers.append(f) }
    /// Every relevant engine event, as it arrives.
    var onEvent: (DockerEvent) -> Void = { _ in }
    private func changed() { observers.forEach { $0(self) } }

    /// Containers whose details may have changed; observers read it during the callback.
    private(set) var changedContainers: Set<String> = []

    /// Follow the daemon: connect when Docker is ready, forget everything when it isn't.
    func follow(_ status: DaemonStatus?) {
        let ready = status?.docker.state == "ready" && status?.state == "running"
        let sock = ready ? status?.docker.socket : nil
        guard sock != socket else { return }
        disconnect()
        socket = sock
        containers = []; volumes = []; images = []; networks = []
        loaded = []
        loadError = nil
        if sock != nil {
            connectEvents()
            refresh(interest)
        }
        changed()
    }

    private func disconnect() {
        connectionEpoch &+= 1
        events?.cancel()
        events = nil
        retry?.cancel()
        retry = nil
        flush?.cancel()
        flush = nil
        pending = []
        touched = []
        loading = []
        busy = [:]
    }

    private func connectEvents() {
        guard let socket else { return }
        let filters = #"{"type":["container","image","volume","network"]}"#
            .addingPercentEncoding(withAllowedCharacters: .alphanumerics) ?? ""
        let s = DockerStream(socketPath: socket, path: "/events?filters=\(filters)")
        eventLines = LineSplitter()
        s.onData = { [weak self] data in self?.received(data) }
        s.onEnd = { [weak self, weak s] _ in
            guard let self, let s, self.events === s else { return }
            // Docker restarted or the VM is going away; try again while it's up.
            let w = DispatchWorkItem { [weak self] in
                guard let self, self.events === s, self.socket != nil else { return }
                self.connectEvents()
                self.refresh(self.interest)
            }
            self.retry = w
            DispatchQueue.main.asyncAfter(deadline: .now() + 2, execute: w)
        }
        events = s
        s.start()
    }

    private func received(_ data: Data) {
        for line in eventLines.feed(data) {
            guard let e = try? JSONDecoder().decode(DockerEvent.self, from: line), e.isRelevant else { continue }
            onEvent(e)
            switch e.Type {
            case "container":
                // Creating or removing a container changes what uses volumes, images and networks.
                pending.formUnion([.containers, .volumes, .images, .networks])
                if let id = e.Actor?.ID { touched.insert(id) }
            case "image": pending.insert(.images)
            case "volume": pending.insert(.volumes)
            case "network": pending.formUnion([.networks, .containers])
            default: break
            }
        }
        scheduleFlush()
    }

    private func scheduleFlush() {
        guard flush == nil, !pending.isEmpty else { return }
        let w = DispatchWorkItem { [weak self] in
            guard let self else { return }
            self.flush = nil
            let kinds = self.pending
            self.pending = []
            // The rest is fetched when it's shown again.
            let wanted = kinds.intersection(self.interest)
            self.loaded.subtract(kinds.subtracting(self.interest))
            self.refresh(wanted)
        }
        flush = w
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25, execute: w)
    }

    /// Refetch `kinds` (all four by default) off the main thread.
    func refresh(_ kinds: Set<DockerKind> = Set(DockerKind.allCases)) {
        guard let client, !kinds.isEmpty else { return }
        let ids = touched
        touched = []
        loading.formUnion(kinds)
        let sock = socket
        let epoch = connectionEpoch
        BlockingIO.run(on: BlockingIO.docker) {
            var c: [ContainerSummary]? = nil, v: [VolumeInfo]? = nil, i: [ImageInfo]? = nil, n: [NetworkInfo]? = nil
            var problem: String? = nil
            do {
                if kinds.contains(.containers) { c = try client.containers() }
                if kinds.contains(.images) { i = try client.images() }
                if kinds.contains(.networks) { n = try client.networks() }
                if kinds.contains(.volumes) { v = try client.volumes() }
            } catch {
                problem = error.localizedDescription
            }
            return (c, v, i, n, problem)
        } completion: { [weak self] cs, vs, ims, ns, err in
            guard let self, self.connectionEpoch == epoch, self.socket == sock else { return }
            self.loading.subtract(kinds)
            var dirty = !ids.isEmpty || self.loadError != err
            self.loadError = err
            if let cs { dirty = dirty || cs != self.containers || !self.loaded.contains(.containers); self.containers = cs; self.loaded.insert(.containers) }
            if let vs { dirty = dirty || vs != self.volumes || !self.loaded.contains(.volumes); self.volumes = vs; self.loaded.insert(.volumes) }
            if let ims { dirty = dirty || ims != self.images || !self.loaded.contains(.images); self.images = ims; self.loaded.insert(.images) }
            if let ns { dirty = dirty || ns != self.networks || !self.loaded.contains(.networks); self.networks = ns; self.loaded.insert(.networks) }
            self.changedContainers = ids
            if dirty { self.changed() }
            self.changedContainers = []
        }
    }

    // MARK: Questions the views ask

    func container(_ id: String) -> ContainerSummary? { containers.first { $0.Id == id } }

    func volumeInUse(_ v: VolumeInfo) -> Bool {
        if let r = v.refCount, r > 0 { return true }
        return containers.contains { $0.volumeNames.contains(v.Name) }
    }

    func imageInUse(_ i: ImageInfo) -> Bool {
        containers.contains { $0.ImageID == i.Id }
    }

    func containers(usingImage i: ImageInfo) -> [ContainerSummary] { containers.filter { $0.ImageID == i.Id } }
    func containers(usingVolume v: VolumeInfo) -> [ContainerSummary] { containers.filter { $0.volumeNames.contains(v.Name) } }

    func containers(onNetwork n: NetworkInfo) -> [ContainerSummary] {
        containers.filter { c in
            (c.NetworkSettings?.Networks ?? [:]).contains { $0.key == n.Name || $0.value.NetworkID == n.Id }
        }
    }

    func networkInUse(_ n: NetworkInfo) -> Bool { n.builtIn || !containers(onNetwork: n).isEmpty }

    // MARK: Actions

    /// Run `body` against Docker off the main thread; a failure is reported as `what`.
    func perform(_ what: String, _ body: @escaping @Sendable (DockerClient) throws -> Void) {
        guard let client else { return }
        let sock = socket
        let epoch = connectionEpoch
        BlockingIO.run(on: BlockingIO.docker) {
            var failure: String? = nil
            do { try body(client) } catch { failure = error.localizedDescription }
            return failure
        } completion: { [weak self] err in
            guard let self, self.connectionEpoch == epoch, self.socket == sock else { return }
            if let err { self.onError(what, err) }
            // Events cover most changes; prunes of unused things send few.
            self.refresh(self.interest)
        }
    }

    /// Containers with an action in flight, so rows can show it until Docker is done.
    private(set) var busy: [String: ContainerAction] = [:]

    private func finished(_ id: String, epoch: UInt64, socket sock: String?) {
        guard connectionEpoch == epoch, socket == sock else { return }
        busy[id] = nil
        changed()
    }

    func act(_ action: ContainerAction, on containers: [ContainerSummary]) {
        let ids = containers.map(\.Id)
        let sock = socket
        let epoch = connectionEpoch
        for id in ids { busy[id] = action }
        changed()
        let verb = switch action {
        case .start: "start"
        case .stop: "stop"
        case .restart: "restart"
        case .remove: "delete"
        }
        let what = containers.count == 1 ? "Couldn’t \(verb) \(containers[0].name)" : "Couldn’t \(verb) all containers"
        perform(what) { client in
            // Compose projects: one at a time keeps dependency order roughly right
            // and Docker's per-container errors readable.
            var errors: [String] = []
            for id in ids {
                do { try client.act(id, action) } catch { errors.append(error.localizedDescription) }
                DispatchQueue.main.async {
                    MainActor.assumeIsolated { self.finished(id, epoch: epoch, socket: sock) }
                }
            }
            if !errors.isEmpty { throw SocketError.protocolError(errors.joined(separator: "\n")) }
        }
    }
}
