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
    private(set) var loadErrors: [Kind: String] = [:]
    /// The cluster as the daemon reports it, for the panes' empty states.
    private(set) var status: K8sStatus?

    func loadError(for kind: Kind) -> String? { loadErrors[kind] }

    /// Kinds a visible view shows.
    var interest: Set<Kind> = [] {
        didSet {
            if !interest.subtracting(oldValue).isEmpty { refresh(force: true) }
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
    private var tickObservers: [() -> Void] = []
    private var timer: Timer?
    private var fetching = false
    private var refreshPending = false

    var client: K8sClient? { socket.map(K8sClient.init) }

    func observe(_ f: @escaping (K8sStore) -> Void) { observers.append(f) }
    /// Fired on each periodic poll tick so time-derived displays (pod ages) can
    /// repaint even when Kubernetes objects are unchanged.
    func observeTick(_ f: @escaping () -> Void) { tickObservers.append(f) }
    private func changed() { observers.forEach { $0(self) } }
    private func tick() { tickObservers.forEach { $0() } }

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
        loadErrors = [:]
        fetching = false
        refreshPending = false
        refresh()
        schedule()
        changed()
    }

    /// Clear a failed initial load for `kind` and retry immediately.
    func retry(_ kind: Kind) {
        loadErrors[kind] = nil
        changed()
        refresh(force: true)
    }

    private func schedule() {
        let want = socket != nil && !interest.isEmpty
        if want, timer == nil {
            let t = Timer(timeInterval: 2, repeats: true) { [weak self] _ in
                MainActor.assumeIsolated {
                    self?.refresh()
                    self?.tick()
                }
            }
            RunLoop.main.add(t, forMode: .common)
            timer = t
        } else if !want {
            timer?.invalidate()
            timer = nil
        }
    }

    func refresh(force: Bool = false) {
        guard let client, !interest.isEmpty else { return }
        if fetching {
            if force { refreshPending = true }
            return
        }
        let kinds = interest
        let current = epoch
        fetching = true
        refreshPending = false
        BlockingIO.run(on: BlockingIO.docker) {
            var p: [K8sPod]? = nil, s: [K8sService]? = nil
            var podsErr: String? = nil, servicesErr: String? = nil
            if kinds.contains(.pods) {
                do { p = try client.pods() } catch { podsErr = error.localizedDescription }
            }
            if kinds.contains(.services) {
                do { s = try client.services() } catch { servicesErr = error.localizedDescription }
            }
            return (p, podsErr, s, servicesErr)
        } completion: { [weak self] p, podsErr, s, servicesErr in
            guard let self, self.epoch == current else { return }
            self.fetching = false
            var dirty = false
            if kinds.contains(.pods) {
                if self.loadErrors[.pods] != podsErr { dirty = true; self.loadErrors[.pods] = podsErr }
                if let p {
                    if p != self.pods || !self.loaded.contains(.pods) { dirty = true }
                    self.pods = p
                    self.loaded.insert(.pods)
                }
            }
            if kinds.contains(.services) {
                if self.loadErrors[.services] != servicesErr { dirty = true; self.loadErrors[.services] = servicesErr }
                if let s {
                    if s != self.services || !self.loaded.contains(.services) { dirty = true }
                    self.services = s
                    self.loaded.insert(.services)
                }
            }
            if dirty { self.changed() }
            if self.refreshPending {
                self.refreshPending = false
                self.refresh()
            }
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
            self.refresh(force: true)
        }
    }
}
