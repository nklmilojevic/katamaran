import Foundation

/// Mirrors `crates/kat-daemon/src/api.rs`. Unknown fields are ignored and
/// optional ones may be missing, so the app tolerates a newer daemon.
struct DaemonStatus: Decodable, Equatable {
    var state: String
    var error: String?
    var since_ms: UInt64
    var uptime_s: Double?
    var daemon: DaemonInfo
    var versions: Versions
    var config: Config
    var vm: VmInfo?
    var usage: Sample?
    var docker: DockerInfo
    var k8s: K8sStatus?
    var mount: MountInfo
    var ports: [Port]
    var port_errors: [PortError]?
}

struct DaemonInfo: Decodable, Equatable {
    var pid: UInt32
    var home: String
    var launchd_label: String?
}

struct Versions: Decodable, Equatable {
    var katamaran: String
    var `protocol`: UInt32
    var kernel: String?
}

struct Config: Codable, Equatable {
    var cpus: Int
    var memory_mib: Int
    var mount: Bool
    var mount_path: String?
    var launch_at_login: Bool
    var kubernetes: Bool? = nil
}

struct ConfigView: Decodable {
    var config: Config
    var restart_required: Bool
}

struct VmInfo: Decodable, Equatable {
    var cpus: Int
    var memory_mib: Int
    var pid: UInt32?
    var boot_ms: Double?
    var docker_ready_ms: Double?
}

struct Sample: Decodable, Equatable {
    var t_ms: UInt64
    var guest_cpu_percent: Double
    var guest_mem_used_mib: UInt64
    var guest_mem_total_mib: UInt64
    var host_cpu_percent: Double
    var host_footprint_mib: UInt64
}

struct StatsResult: Decodable {
    var interval_ms: UInt64
    var samples: [Sample]
}

struct DockerInfo: Decodable, Equatable {
    var state: String
    var socket: String
}

struct K8sStatus: Decodable, Equatable {
    var enabled: Bool
    var state: String
    var installed: Bool
    var version: String?
    var apiPort: UInt16?
    var proxySocket: String?
    var context: String?
    var kubeconfig: String?
    var dnsDomain: String?
    var dnsPort: UInt16?
    var dnsInstalled: Bool?
    var error: String?

    enum CodingKeys: String, CodingKey {
        case enabled, state, installed, version, context, kubeconfig, error
        case apiPort = "api_port"
        case proxySocket = "proxy_socket"
        case dnsDomain = "dns_domain"
        case dnsPort = "dns_port"
        case dnsInstalled = "dns_installed"
    }
}

struct MountInfo: Decodable, Equatable {
    var enabled: Bool
    var path: String?
    var mounted: Bool
    var error: String?
}

struct Port: Decodable, Equatable, Hashable {
    var proto: String
    var port: UInt16
    var addresses: [String]? = nil
}

struct PortError: Decodable, Equatable, Hashable {
    var proto: String
    var port: UInt16
    var error: String
}

extension DaemonStatus {
    /// Preferred Mac host for opening a forwarded port in the browser:
    /// `"localhost"` when `127.0.0.1` is bound (or when no address list is
    /// reported), otherwise the port's first bound alias address.
    func portHost(proto: String = "tcp", port: UInt16) -> String {
        guard let addrs = ports.first(where: {
            $0.port == port && $0.proto.caseInsensitiveCompare(proto) == .orderedSame
        })?.addresses, !addrs.isEmpty else {
            return "localhost"
        }
        if addrs.contains("127.0.0.1") {
            return "localhost"
        }
        return addrs[0]
    }

    func portURL(proto: String = "tcp", port: UInt16) -> URL? {
        URL(string: "http://\(portHost(proto: proto, port: port)):\(port)")
    }

    func portError(proto: String = "tcp", port: UInt16) -> String? {
        port_errors?.first {
            $0.port == port && $0.proto.caseInsensitiveCompare(proto) == .orderedSame
        }?.error
    }

    var portErrorSummary: String? {
        guard let errs = port_errors, !errs.isEmpty else { return nil }
        if errs.count == 1, let first = errs.first {
            return "Port \(first.port)/\(first.proto) not forwarded"
        }
        let list = errs.map { "\($0.port)/\($0.proto)" }.joined(separator: ", ")
        return "Ports not forwarded: \(list)"
    }

    var portErrorDetails: String? {
        guard let errs = port_errors, !errs.isEmpty else { return nil }
        return errs.map { "\($0.port)/\($0.proto): \($0.error)" }.joined(separator: "\n")
    }
}

struct LogChunk: Decodable {
    var data: String
    var offset: UInt64
}

/// What the menu-bar icon shows.
enum Phase: Equatable {
    case noDaemon, stopped, starting, running, stopping, error

    init(_ s: DaemonStatus?) {
        guard let s else { self = .noDaemon; return }
        switch s.state {
        case "running": self = s.docker.state == "failed" ? .error : .running
        case "starting": self = .starting
        case "stopping": self = .stopping
        case "error": self = .error
        default: self = .stopped
        }
    }

    var label: String {
        switch self {
        case .noDaemon: "Not running"
        case .stopped: "Stopped"
        case .starting: "Starting…"
        case .running: "Running"
        case .stopping: "Stopping…"
        case .error: "Error"
        }
    }

    var symbol: String {
        switch self {
        case .running: "sailboat.fill"
        case .starting, .stopping: "sailboat.circle"
        case .error: "exclamationmark.triangle"
        case .stopped, .noDaemon: "sailboat"
        }
    }

    var isUp: Bool { self == .running || self == .starting }
}
