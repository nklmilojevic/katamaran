import AppKit

/// What the Kubernetes cluster has: pods and services, polled every two
/// seconds while a view shows them.
@MainActor
final class K8sStore {
    static let shared = K8sStore()

    enum Kind: CaseIterable { case pods, services }

    private(set) var socket: String?
    private var epoch: UInt64 = 0
    private(set) var pods: [K8sPod] = []
    private(set) var services: [K8sService] = []
    private(set) var loaded: Set<Kind> = []
    private(set) var loadError: String?
    /// The cluster as the daemon reports it, for the panes' empty states.
    private(set) var status: K8sStatus?

    /// Kinds a visible view shows.
    var interest: Set<Kind> = [] {
        didSet {
            if !interest.subtracting(oldValue).isEmpty { refresh() }
            schedule()
        }
    }

    /// Whether system namespaces (`kube-system`…) are listed.
    var showSystem: Bool = UserDefaults.standard.bool(forKey: K8sStore.showSystemKey) {
        didSet {
            UserDefaults.standard.set(showSystem, forKey: K8sStore.showSystemKey)
            changed()
        }
    }
    private static let showSystemKey = "k8sShowSystemNamespaces"

    var onError: (String, String) -> Void = { _, _ in }

    private var observers: [(K8sStore) -> Void] = []
    private var timer: Timer?
    private var fetching = false

    var client: K8sClient? { socket.map(K8sClient.init) }

    func observe(_ f: @escaping (K8sStore) -> Void) { observers.append(f) }
    private func changed() { observers.forEach { $0(self) } }

    /// Follow the daemon: connect while the cluster runs, forget it otherwise.
    func follow(_ daemon: DaemonStatus?) {
        let k8s = daemon?.state == "running" ? daemon?.k8s : nil
        let sock = k8s?.state == "running" ? k8s?.proxySocket : nil
        let statusChanged = k8s != status
        status = k8s
        guard sock != socket else {
            if statusChanged { changed() }
            return
        }
        epoch &+= 1
        socket = sock
        pods = []; services = []
        loaded = []
        loadError = nil
        fetching = false
        refresh()
        schedule()
        changed()
    }

    private func schedule() {
        let want = socket != nil && !interest.isEmpty
        if want, timer == nil {
            let t = Timer(timeInterval: 2, repeats: true) { [weak self] _ in
                MainActor.assumeIsolated { self?.refresh() }
            }
            RunLoop.main.add(t, forMode: .common)
            timer = t
        } else if !want {
            timer?.invalidate()
            timer = nil
        }
    }

    func refresh() {
        guard let client, !interest.isEmpty, !fetching else { return }
        let kinds = interest
        let current = epoch
        fetching = true
        BlockingIO.run(on: BlockingIO.docker) {
            var p: [K8sPod]? = nil, s: [K8sService]? = nil
            var problem: String? = nil
            do {
                if kinds.contains(.pods) { p = try client.pods() }
                if kinds.contains(.services) { s = try client.services() }
            } catch {
                problem = error.localizedDescription
            }
            return (p, s, problem)
        } completion: { [weak self] p, s, err in
            guard let self, self.epoch == current else { return }
            self.fetching = false
            var dirty = self.loadError != err
            self.loadError = err
            if let p { dirty = dirty || p != self.pods || !self.loaded.contains(.pods); self.pods = p; self.loaded.insert(.pods) }
            if let s { dirty = dirty || s != self.services || !self.loaded.contains(.services); self.services = s; self.loaded.insert(.services) }
            if dirty { self.changed() }
        }
    }

    func visible(_ namespace: String) -> Bool { showSystem || !K8sNamespaces.isSystem(namespace) }

    func pod(_ key: String) -> K8sPod? { pods.first { $0.key == key } }
    func service(_ key: String) -> K8sService? { services.first { $0.key == key } }

    /// Pods a service selects.
    func pods(for svc: K8sService) -> [K8sPod] {
        guard let sel = svc.spec?.selector, !sel.isEmpty else { return [] }
        return pods.filter { p in
            p.namespace == svc.namespace && sel.allSatisfy { p.metadata.labels?[$0.key] == $0.value }
        }
    }

    /// Run `body` against the cluster off the main thread; a failure is reported as `what`.
    func perform(_ what: String, _ body: @escaping @Sendable (K8sClient) throws -> Void) {
        guard let client else { return }
        let current = epoch
        BlockingIO.run(on: BlockingIO.docker) {
            var failure: String? = nil
            do { try body(client) } catch { failure = error.localizedDescription }
            return failure
        } completion: { [weak self] err in
            guard let self, self.epoch == current else { return }
            if let err { self.onError(what, err) }
            self.refresh()
        }
    }
}
