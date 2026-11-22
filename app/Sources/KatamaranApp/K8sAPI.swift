import AppKit

// MARK: - Kubernetes API models
//
// Only the fields the app shows, from `GET /api/v1/pods` and `/services`.
// The API leaves out empty fields, so nearly everything is optional.

struct K8sList<T: Decodable>: Decodable {
    var items: [T]
}

struct K8sMeta: Decodable, Equatable {
    var name: String
    var namespace: String?
    var uid: String?
    var creationTimestamp: String?
    var deletionTimestamp: String?
    var labels: [String: String]?
    var ownerReferences: [OwnerRef]?

    struct OwnerRef: Decodable, Equatable {
        var kind: String
        var name: String
    }

    var created: Date? { creationTimestamp.flatMap(parseDockerDate) }
}

struct K8sPod: Decodable, Equatable {
    var metadata: K8sMeta
    var spec: Spec?
    var status: Status?

    struct Spec: Decodable, Equatable {
        var nodeName: String?
        var containers: [Container]?
    }

    struct Container: Decodable, Equatable {
        var name: String
        var image: String?
    }

    struct Status: Decodable, Equatable {
        var phase: String?
        var reason: String?
        var message: String?
        var podIP: String?
        var containerStatuses: [ContainerStatus]?
    }

    struct ContainerStatus: Decodable, Equatable {
        var name: String
        var ready: Bool?
        var restartCount: Int?
        var image: String?
        var state: State?
    }

    struct State: Decodable, Equatable {
        var waiting: Detail?
        var running: Running?
        var terminated: Detail?

        struct Detail: Decodable, Equatable {
            var reason: String?
            var exitCode: Int?
        }

        struct Running: Decodable, Equatable {
            var startedAt: String?
        }

        var label: String {
            if let w = waiting { return w.reason ?? "Waiting" }
            if let t = terminated { return t.reason ?? "Terminated" }
            if running != nil { return "Running" }
            return "Unknown"
        }
    }

    var name: String { metadata.name }
    var namespace: String { metadata.namespace ?? "default" }
    /// Unique across namespaces.
    var key: String { "\(namespace)/\(name)" }
    var containerNames: [String] { (spec?.containers ?? []).map(\.name) }
    var statuses: [ContainerStatus] { status?.containerStatuses ?? [] }
    var readyCount: Int { statuses.filter { $0.ready == true }.count }
    var containerCount: Int { max(spec?.containers?.count ?? 0, statuses.count) }
    var restarts: Int { statuses.reduce(0) { $0 + ($1.restartCount ?? 0) } }
    var owner: K8sMeta.OwnerRef? { metadata.ownerReferences?.first }

    /// What `kubectl get pods` puts in STATUS: a container's waiting or
    /// failure reason when there is one, otherwise the phase.
    var displayStatus: String {
        if metadata.deletionTimestamp != nil { return "Terminating" }
        if let r = statuses.lazy.compactMap({ $0.state?.waiting?.reason }).first { return r }
        if let r = statuses.lazy.compactMap({ s -> String? in
            guard let t = s.state?.terminated, t.exitCode != 0 else { return nil }
            return t.reason ?? "Error"
        }).first { return r }
        if let r = status?.reason { return r }
        return status?.phase ?? "Unknown"
    }

    var health: ContainerHealth {
        if metadata.deletionTimestamp != nil { return .transitional }
        switch status?.phase {
        case "Succeeded": return .stopped
        case "Failed": return .failed
        case "Running":
            let s = displayStatus
            if s.contains("BackOff") || s.hasPrefix("Err") || s == "Error" || s == "OOMKilled" { return .failed }
            return readyCount == containerCount ? .running : .transitional
        default:
            let s = displayStatus
            if s.contains("BackOff") || s.hasPrefix("Err") || s.hasPrefix("Invalid") { return .failed }
            return .transitional
        }
    }
}

struct K8sService: Decodable, Equatable {
    var metadata: K8sMeta
    var spec: Spec?
    var status: Status?

    struct Spec: Decodable, Equatable {
        var type: String?
        var clusterIP: String?
        var ports: [ServicePort]?
        var selector: [String: String]?
    }

    struct ServicePort: Decodable, Equatable {
        var name: String?
        var port: Int
        var targetPort: IntOrString?
        var nodePort: Int?
        var `protocol`: String?
    }

    struct Status: Decodable, Equatable {
        var loadBalancer: LoadBalancer?
        struct LoadBalancer: Decodable, Equatable {
            var ingress: [Ingress]?
        }
        struct Ingress: Decodable, Equatable {
            var ip: String?
            var hostname: String?
        }
    }

    var name: String { metadata.name }
    var namespace: String { metadata.namespace ?? "default" }
    var key: String { "\(namespace)/\(name)" }
    var type: String { spec?.type ?? "ClusterIP" }
    var ports: [ServicePort] { spec?.ports ?? [] }

    /// `80:30080/TCP, 443/TCP`, as `kubectl get svc` shows them.
    var portsLabel: String {
        ports.map { p in
            var s = "\(p.port)"
            if let n = p.nodePort, type == "NodePort" || type == "LoadBalancer" { s += ":\(n)" }
            return s + "/\(p.protocol ?? "TCP")"
        }.joined(separator: ", ")
    }

    /// Ports reachable on the Mac's `localhost`: a LoadBalancer's own ports and
    /// NodePorts, which the VM forwards (TCP only).
    var localPorts: [Int] {
        var out: [Int] = []
        for p in ports where (p.protocol ?? "TCP") == "TCP" {
            if type == "LoadBalancer" { out.append(p.port) }
            if let n = p.nodePort, type == "NodePort" || type == "LoadBalancer" { out.append(n) }
        }
        var seen = Set<Int>()
        return out.filter { seen.insert($0).inserted }
    }
}

/// A port as a number or a named container port.
enum IntOrString: Decodable, Equatable, CustomStringConvertible {
    case int(Int), string(String)

    init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if let i = try? c.decode(Int.self) { self = .int(i) } else { self = .string(try c.decode(String.self)) }
    }

    var description: String {
        switch self {
        case .int(let i): "\(i)"
        case .string(let s): s
        }
    }
}

/// Namespaces k3s keeps for itself, hidden unless asked for.
enum K8sNamespaces {
    static let system: Set<String> = ["kube-system", "kube-public", "kube-node-lease"]
    static func isSystem(_ ns: String) -> Bool { system.contains(ns) }
}

/// "45s", "12m", "3h", "2d": the age `kubectl` shows.
func compactAge(_ date: Date?, now: Date = Date()) -> String {
    guard let date else { return "-" }
    let s = max(0, Int(now.timeIntervalSince(date)))
    if s < 60 { return "\(s)s" }
    if s < 3600 { return "\(s / 60)m" }
    if s < 86400 { return "\(s / 3600)h" }
    return "\(s / 86400)d"
}

// MARK: - Client

/// The cluster's REST API, authenticated by `kubectl proxy` in the VM and
/// reached over the daemon's `k8s-proxy.sock`.
struct K8sClient: Sendable {
    let socketPath: String
    static let host = "localhost"

    func request(_ method: String, _ path: String, timeout: TimeInterval = 15) throws -> (Int, Data) {
        try DockerClient.exchange(socketPath, host: K8sClient.host, method, path, timeout: timeout)
    }

    /// The API's `Status.message`, or the status code.
    static func failure(_ code: Int, _ body: Data) -> SocketError {
        let msg = (try? JSONSerialization.jsonObject(with: body) as? [String: Any])?["message"] as? String
        return SocketError.protocolError(msg ?? "Kubernetes answered HTTP \(code)")
    }

    func get<T: Decodable>(_ path: String, as: T.Type = T.self) throws -> T {
        let (code, body) = try request("GET", path)
        guard code == 200 else { throw K8sClient.failure(code, body) }
        return try JSONDecoder().decode(T.self, from: body)
    }

    func pods() throws -> [K8sPod] { try get("/api/v1/pods", as: K8sList<K8sPod>.self).items }
    func services() throws -> [K8sService] { try get("/api/v1/services", as: K8sList<K8sService>.self).items }

    func deletePod(namespace: String, name: String) throws {
        try delete("/api/v1/namespaces/\(K8sClient.segment(namespace))/pods/\(K8sClient.segment(name))")
    }

    func deleteService(namespace: String, name: String) throws {
        try delete("/api/v1/namespaces/\(K8sClient.segment(namespace))/services/\(K8sClient.segment(name))")
    }

    private func delete(_ path: String) throws {
        let (code, body) = try request("DELETE", path, timeout: 30)
        guard (200..<300).contains(code) else { throw K8sClient.failure(code, body) }
    }

    /// Following a container's log: the last `tail` lines, then new ones.
    static func logPath(namespace: String, pod: String, container: String?, tail: Int = 2000) -> String {
        var p = "/api/v1/namespaces/\(segment(namespace))/pods/\(segment(pod))/log?follow=true&tailLines=\(tail)"
        if let container { p += "&container=\(segment(container))" }
        return p
    }

    /// Names are DNS labels, but encode anyway: they come from the cluster.
    static func segment(_ s: String) -> String {
        s.addingPercentEncoding(withAllowedCharacters: .alphanumerics.union(CharacterSet(charactersIn: "-._~"))) ?? s
    }
}
