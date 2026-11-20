import Foundation

/// A Compose project: the containers sharing a `com.docker.compose.project` label.
struct ComposeProject: Equatable {
    var name: String
    var containers: [ContainerSummary]

    var runningCount: Int { containers.filter(\.running).count }
    var anyRunning: Bool { containers.contains(where: \.running) }
    var workingDir: String? { containers.lazy.compactMap { $0.Labels?[ComposeLabel.workingDir] }.first }
    var configFiles: String? { containers.lazy.compactMap { $0.Labels?[ComposeLabel.configFiles] }.first }
}

/// A top-level row of the containers list.
enum ContainerListItem: Equatable {
    case project(ComposeProject)
    case container(ContainerSummary)

    /// Stable across refreshes, for selection and expansion.
    var key: String {
        switch self {
        case .project(let p): "project:\(p.name)"
        case .container(let c): "container:\(c.Id)"
        }
    }

    var sortName: String {
        switch self {
        case .project(let p): p.name
        case .container(let c): c.name
        }
    }

    var isRunning: Bool {
        switch self {
        case .project(let p): p.anyRunning
        case .container(let c): c.running
        }
    }
}

struct ContainerSection: Equatable {
    enum Kind: String { case running = "Running", stopped = "Stopped" }
    var kind: Kind
    var items: [ContainerListItem]
}

enum ContainerList {
    /// Running first, then stopped. Compose projects are one item each, in the
    /// running section when any of their containers runs; their containers are
    /// sorted by service. Items sort by name, ignoring case.
    ///
    /// A non-empty `query` keeps containers whose name, image, ID, project or
    /// service contains it; a project whose name matches keeps all its containers.
    static func sections(_ all: [ContainerSummary], query: String = "") -> [ContainerSection] {
        let q = query.trimmingCharacters(in: .whitespaces).lowercased()
        var projects: [String: [ContainerSummary]] = [:]
        var loose: [ContainerSummary] = []
        for c in all {
            if let p = c.project { projects[p, default: []].append(c) } else { loose.append(c) }
        }
        var items: [ContainerListItem] = []
        for (name, members) in projects {
            let kept = q.isEmpty || name.lowercased().contains(q) ? members : members.filter { matches($0, q) }
            guard !kept.isEmpty else { continue }
            let sorted = kept.sorted {
                let a = ($0.service ?? $0.name).lowercased(), b = ($1.service ?? $1.name).lowercased()
                return a == b ? $0.name.lowercased() < $1.name.lowercased() : a < b
            }
            items.append(.project(ComposeProject(name: name, containers: sorted)))
        }
        items += loose.filter { q.isEmpty || matches($0, q) }.map(ContainerListItem.container)
        items.sort {
            let a = $0.sortName.lowercased(), b = $1.sortName.lowercased()
            return a == b ? $0.key < $1.key : a < b
        }
        return [ContainerSection.Kind.running, .stopped].compactMap { kind in
            let these = items.filter { $0.isRunning == (kind == .running) }
            return these.isEmpty ? nil : ContainerSection(kind: kind, items: these)
        }
    }

    static func matches(_ c: ContainerSummary, _ q: String) -> Bool {
        [c.name, c.Image, c.Id, c.project ?? "", c.service ?? ""].contains { $0.lowercased().contains(q) }
    }

    /// "3 running", "None running" or "2 of 5 running", for the window subtitle.
    static func summary(_ all: [ContainerSummary]) -> String {
        let running = all.filter(\.running).count
        if all.isEmpty { return "No containers" }
        if running == 0 { return "None running" }
        return running == all.count ? "\(running) running" : "\(running) of \(all.count) running"
    }
}

/// Splits volumes, images or networks into the ones in use and the rest.
struct UsageSections<T> {
    var inUse: [T]
    var unused: [T]

    init(_ items: [T], query: String, name: (T) -> String, search: (T) -> [String], inUse used: (T) -> Bool) {
        let q = query.trimmingCharacters(in: .whitespaces).lowercased()
        let kept = items
            .filter { q.isEmpty || search($0).contains { $0.lowercased().contains(q) } }
            .sorted { name($0).lowercased() < name($1).lowercased() }
        inUse = kept.filter(used)
        unused = kept.filter { !used($0) }
    }
}
