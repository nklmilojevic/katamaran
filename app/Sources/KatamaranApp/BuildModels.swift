import Foundation

// MARK: - Build history models
//
// From `docker buildx history ls --format json` (one object per line) and
// `docker buildx history inspect <id> --format json`. Buildx adds and drops
// fields between versions, so nearly everything is optional.

enum BuildStatus: String, Equatable {
    case running, completed, failed, canceled

    /// `ls` says Running, Completed or Error (a canceled build is an Error
    /// there); `inspect` says running, completed, failed or canceled.
    init(_ raw: String) {
        switch raw.lowercased() {
        case "running": self = .running
        case "completed", "complete": self = .completed
        case "canceled", "cancelled": self = .canceled
        default: self = .failed
        }
    }

    var title: String {
        switch self {
        case .running: "Running"
        case .completed: "Completed"
        case .failed: "Failed"
        case .canceled: "Canceled"
        }
    }
}

/// One line of `history ls`.
struct BuildRecord: Equatable, Decodable {
    var ref: String
    var name: String
    var rawStatus: String
    var createdAt: Date?
    var completedAt: Date?
    var totalSteps: Int
    var completedSteps: Int
    var cachedSteps: Int

    enum CodingKeys: String, CodingKey {
        case ref, name
        case rawStatus = "status"
        case createdAt = "created_at"
        case completedAt = "completed_at"
        case totalSteps = "total_steps"
        case completedSteps = "completed_steps"
        case cachedSteps = "cached_steps"
    }

    init(ref: String, name: String, status: String, createdAt: Date?, completedAt: Date? = nil,
         totalSteps: Int = 0, completedSteps: Int = 0, cachedSteps: Int = 0) {
        self.ref = ref
        self.name = name
        rawStatus = status
        self.createdAt = createdAt
        self.completedAt = completedAt
        self.totalSteps = totalSteps
        self.completedSteps = completedSteps
        self.cachedSteps = cachedSteps
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        ref = try c.decode(String.self, forKey: .ref)
        name = try c.decodeIfPresent(String.self, forKey: .name) ?? ""
        rawStatus = try c.decodeIfPresent(String.self, forKey: .rawStatus) ?? ""
        createdAt = try c.decodeIfPresent(String.self, forKey: .createdAt).flatMap(parseDockerDate)
        completedAt = try c.decodeIfPresent(String.self, forKey: .completedAt).flatMap(parseDockerDate)
        totalSteps = try c.decodeIfPresent(Int.self, forKey: .totalSteps) ?? 0
        completedSteps = try c.decodeIfPresent(Int.self, forKey: .completedSteps) ?? 0
        cachedSteps = try c.decodeIfPresent(Int.self, forKey: .cachedSteps) ?? 0
    }

    /// "default/default/m5hfid…" → "m5hfid…", what `history inspect` and `rm` take.
    var id: String { ref.split(separator: "/").last.map(String.init) ?? ref }
    var status: BuildStatus { BuildStatus(rawStatus) }
    var isRunning: Bool { status == .running }

    /// "docker/sso (local)" → ("docker/sso", "local"): Compose and Bake put the target in parentheses.
    var title: String { BuildRecord.split(name).title }
    var target: String? { BuildRecord.split(name).target }

    static func split(_ name: String) -> (title: String, target: String?) {
        guard name.hasSuffix(")"), let open = name.lastIndex(of: "("), open > name.startIndex,
              name[name.index(before: open)] == " " else { return (name.isEmpty ? "Untitled build" : name, nil) }
        let target = String(name[name.index(after: open)..<name.index(before: name.endIndex)])
        return (String(name[..<name.index(before: open)]), target.isEmpty ? nil : target)
    }

    var duration: Double? {
        guard let start = createdAt else { return nil }
        return (completedAt ?? Date()).timeIntervalSince(start)
    }

    /// Every line of `history ls --format json`; lines that don't parse are skipped.
    static func parseList(_ data: Data) -> [BuildRecord] {
        let d = JSONDecoder()
        return data.split(separator: 0x0A).compactMap { try? d.decode(BuildRecord.self, from: Data($0)) }
    }
}

/// `history inspect --format json`.
struct BuildInfo: Equatable, Decodable {
    var Name: String?
    var Ref: String?
    var Context: String?
    var Dockerfile: String?
    var VCSRepository: String?
    var VCSRevision: String?
    var Target: String?
    var Platform: [String]?
    var StartedAt: String?
    var CompletedAt: String?
    /// Nanoseconds.
    var Duration: Int64?
    var Status: String?
    var Error: BuildError?
    var NumCompletedSteps: Int?
    var NumTotalSteps: Int?
    var NumCachedSteps: Int?
    var BuildArgs: [NameValue]?
    var Labels: [NameValue]?
    var Materials: [Material]?
    var Attachments: [Attachment]?

    struct NameValue: Equatable, Decodable {
        var Name: String
        var Value: String?
    }

    struct Material: Equatable, Decodable {
        var URI: String?
        var Digests: [String]?
    }

    struct Attachment: Equatable, Decodable {
        var Digest: String?
        var Platform: String?
        var `Type`: String?
    }

    struct BuildError: Equatable, Decodable {
        var Code: Int?
        var Message: String?
        /// The failing step's name.
        var Name: String?
        /// The failing step's last lines.
        var Logs: [String]?
        /// Base64 of the Dockerfile excerpt around the failing line.
        var Sources: String?

        /// The Dockerfile excerpt, decoded.
        var source: String? {
            guard let s = Sources, let d = Data(base64Encoded: s) else { return nil }
            let text = String(decoding: d, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
            return text.isEmpty ? nil : text
        }
    }

    enum CodingKeys: String, CodingKey {
        case Name, Ref, Context, Dockerfile, VCSRepository, VCSRevision, Target, Platform, StartedAt, CompletedAt
        case Duration, Status, Error, NumCompletedSteps, NumTotalSteps, NumCachedSteps, BuildArgs, Labels
        case Materials, Attachments
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        Name = try? c.decodeIfPresent(String.self, forKey: .Name)
        Ref = try? c.decodeIfPresent(String.self, forKey: .Ref)
        Context = try? c.decodeIfPresent(String.self, forKey: .Context)
        Dockerfile = try? c.decodeIfPresent(String.self, forKey: .Dockerfile)
        VCSRepository = try? c.decodeIfPresent(String.self, forKey: .VCSRepository)
        VCSRevision = try? c.decodeIfPresent(String.self, forKey: .VCSRevision)
        Target = try? c.decodeIfPresent(String.self, forKey: .Target)
        // A list in current buildx, a string in some older ones.
        Platform = (try? c.decodeIfPresent([String].self, forKey: .Platform))
            ?? (try? c.decodeIfPresent(String.self, forKey: .Platform)).flatMap { $0.map { [$0] } }
        StartedAt = try? c.decodeIfPresent(String.self, forKey: .StartedAt)
        CompletedAt = try? c.decodeIfPresent(String.self, forKey: .CompletedAt)
        Duration = try? c.decodeIfPresent(Int64.self, forKey: .Duration)
        Status = try? c.decodeIfPresent(String.self, forKey: .Status)
        Error = try? c.decodeIfPresent(BuildError.self, forKey: .Error)
        NumCompletedSteps = try? c.decodeIfPresent(Int.self, forKey: .NumCompletedSteps)
        NumTotalSteps = try? c.decodeIfPresent(Int.self, forKey: .NumTotalSteps)
        NumCachedSteps = try? c.decodeIfPresent(Int.self, forKey: .NumCachedSteps)
        BuildArgs = try? c.decodeIfPresent([NameValue].self, forKey: .BuildArgs)
        Labels = try? c.decodeIfPresent([NameValue].self, forKey: .Labels)
        Materials = try? c.decodeIfPresent([Material].self, forKey: .Materials)
        Attachments = try? c.decodeIfPresent([Attachment].self, forKey: .Attachments)
    }

    static func parse(_ data: Data) throws -> BuildInfo { try JSONDecoder().decode(BuildInfo.self, from: data) }

    var status: BuildStatus? { Status.map(BuildStatus.init) }
    var startedAt: Date? { StartedAt.flatMap(parseDockerDate) }
    var completedAt: Date? { CompletedAt.flatMap(parseDockerDate) }
    var seconds: Double? { Duration.map { Double($0) / 1e9 } }

    /// What it was built for: as asked, else what the image manifests or base images say.
    var platforms: [String] {
        if let p = Platform, !p.isEmpty { return p }
        var out: [String] = []
        for a in Attachments ?? [] { if let p = a.Platform, !out.contains(p) { out.append(p) } }
        if !out.isEmpty { return out }
        for m in Materials ?? [] {
            guard let uri = m.URI, let q = URLComponents(string: uri)?.queryItems,
                  let p = q.first(where: { $0.name == "platform" })?.value, !out.contains(p) else { continue }
            out.append(p)
        }
        return out
    }

    /// Base images and other inputs, without the `pkg:docker/` noise: "alpine@3.20".
    var baseImages: [String] {
        (Materials ?? []).compactMap { m in
            guard var s = m.URI, s.hasPrefix("pkg:docker/") else { return nil }
            s.removeFirst("pkg:docker/".count)
            if let q = s.firstIndex(of: "?") { s = String(s[..<q]) }
            return s.removingPercentEncoding ?? s
        }
    }

    var composeProject: String? { Labels?.first { $0.Name == ComposeLabel.project }?.Value }
    var composeService: String? { Labels?.first { $0.Name == ComposeLabel.service }?.Value }

    /// The Dockerfile on this Mac, when it's a local build and the file is still there.
    /// Compose builds may leave out the context; their project's directory stands in.
    func dockerfileURL(projectDirs: [String: String] = [:], exists: (String) -> Bool = FileManager.default.fileExists) -> URL? {
        let file = Dockerfile ?? "Dockerfile"
        guard !file.contains("://"), file != "-" else { return nil }
        var candidates: [String] = []
        if file.hasPrefix("/") { candidates.append(file) }
        if let ctx = Context, ctx.hasPrefix("/") { candidates.append((ctx as NSString).appendingPathComponent(file)) }
        if let p = composeProject, let dir = projectDirs[p] { candidates.append((dir as NSString).appendingPathComponent(file)) }
        return candidates.map { ($0 as NSString).standardizingPath }.first(where: exists).map { URL(fileURLWithPath: $0) }
    }

    /// The context, shortened for display: a local path with `~`, or the URL as is.
    var contextDisplay: String? {
        guard let c = Context, !c.isEmpty else { return nil }
        return c.hasPrefix("/") ? (c as NSString).abbreviatingWithTildeInPath : c
    }
}

/// Build arguments that probably hold credentials are masked in the UI and in copied commands.
enum BuildSecrets {
    private static let words = ["TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASS", "PWD", "CREDENTIAL", "AUTH",
                                "PRIVATE", "APIKEY", "API_KEY", "ACCESS_KEY", "_KEY", "KEY_", "CERT", "SIGNATURE"]

    static func isSecret(_ name: String) -> Bool {
        let n = name.uppercased()
        if n == "KEY" { return true }
        return words.contains { n.contains($0) }
    }

    static func display(_ arg: BuildInfo.NameValue) -> String {
        guard let v = arg.Value else { return "" }
        return isSecret(arg.Name) && !v.isEmpty ? String(repeating: "•", count: 8) : v
    }
}

/// What a build produced, read from its export step's progress lines.
struct BuildOutput: Equatable {
    var tags: [String] = []
    /// "Docker image", "Pushed to registry", "Local directory", "Tar archive", "OCI archive".
    var kind: String?

    init(tags: [String] = [], kind: String? = nil) {
        self.tags = tags
        self.kind = kind
    }

    init(steps: [BuildStep]) {
        for s in steps {
            let n = s.name.lowercased()
            if n.hasPrefix("exporting to image") || n.hasPrefix("exporting to docker image") { kind = kind ?? "Docker image" }
            else if n.hasPrefix("exporting to client directory") { kind = "Local directory" }
            else if n.hasPrefix("exporting to client tarball") { kind = "Tar archive" }
            else if n.hasPrefix("exporting to oci image format") || n.hasPrefix("exporting to docker image format") { kind = "Image archive" }
            for line in s.logs {
                if line.hasPrefix("naming to ") {
                    let name = Self.short(String(line.dropFirst("naming to ".count).split(separator: " ").first ?? ""))
                    if !name.isEmpty, !tags.contains(name) { tags.append(name) }
                } else if line.hasPrefix("pushing ") {
                    kind = "Pushed to registry"
                }
            }
        }
        if kind == nil, steps.contains(where: { $0.name.lowercased().hasPrefix("exporting") }) { kind = "Exported" }
    }

    /// "docker.io/library/nginx:latest" → "nginx:latest", "docker.io/demo/ok:latest" → "demo/ok:latest".
    static func short(_ ref: String) -> String {
        var s = ref
        if s.hasPrefix("docker.io/") { s.removeFirst("docker.io/".count) }
        if s.hasPrefix("library/") { s.removeFirst("library/".count) }
        return s
    }
}

// MARK: - List

struct BuildSection: Equatable {
    enum Kind: String { case running = "Running", today = "Today", yesterday = "Yesterday", earlier = "Earlier" }
    var kind: Kind
    var builds: [BuildRecord]
}

enum BuildList {
    /// Running builds first, then the rest by day; newest first in each.
    /// A non-empty `query` keeps builds whose name, target, ID or status contains it.
    static func sections(_ all: [BuildRecord], query: String = "", now: Date = Date(),
                         calendar: Calendar = .current, status: (BuildRecord) -> BuildStatus = \.status) -> [BuildSection] {
        let q = query.trimmingCharacters(in: .whitespaces).lowercased()
        let kept = all.filter { b in
            q.isEmpty || [b.name, b.id, status(b).title].contains { $0.lowercased().contains(q) }
        }.sorted {
            let a = $0.createdAt ?? .distantPast, b = $1.createdAt ?? .distantPast
            return a == b ? $0.ref < $1.ref : a > b
        }
        var buckets: [BuildSection.Kind: [BuildRecord]] = [:]
        for b in kept {
            let kind: BuildSection.Kind
            if status(b) == .running { kind = .running }
            else if let d = b.createdAt, calendar.isDate(d, inSameDayAs: now) { kind = .today }
            else if let d = b.createdAt, let y = calendar.date(byAdding: .day, value: -1, to: now),
                    calendar.isDate(d, inSameDayAs: y) { kind = .yesterday }
            else { kind = .earlier }
            buckets[kind, default: []].append(b)
        }
        return [BuildSection.Kind.running, .today, .yesterday, .earlier].compactMap { k in
            buckets[k].map { BuildSection(kind: k, builds: $0) }
        }
    }

    /// "2 running · 14 builds", "14 builds · 3 failed" or "No builds", for the window subtitle.
    static func summary(_ all: [BuildRecord], status: (BuildRecord) -> BuildStatus = \.status) -> String {
        if all.isEmpty { return "No builds" }
        let running = all.filter { status($0) == .running }.count
        let failed = all.filter { status($0) == .failed }.count
        var parts: [String] = []
        if running > 0 { parts.append("\(running) running") }
        parts.append("\(all.count) build\(all.count == 1 ? "" : "s")")
        if failed > 0 { parts.append("\(failed) failed") }
        return parts.joined(separator: " · ")
    }

    /// "4 of 9 cached", or nil before any step finished.
    static func cacheSummary(cached: Int, total: Int) -> String? {
        guard total > 0 else { return nil }
        if cached == 0 { return "none cached" }
        return cached == total ? "all \(total) cached" : "\(cached) of \(total) cached"
    }
}

/// A build's length as BuildKit prints it: "0.4s", "7.8s", "1m 12s", "1h 3m".
func buildDuration(_ secs: Double) -> String {
    if secs < 10 { return String(format: "%.1fs", max(secs, 0)) }
    let s = Int(secs.rounded())
    if s < 60 { return "\(s)s" }
    if s < 3600 { return "\(s / 60)m \(s % 60)s" }
    return "\(s / 3600)h \((s % 3600) / 60)m"
}

/// A running build's clock: "0:07", "12:30", "1:02:03".
func elapsedClock(_ secs: Double) -> String {
    let s = max(Int(secs), 0)
    return s < 3600 ? String(format: "%d:%02d", s / 60, s % 60) : String(format: "%d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60)
}

// MARK: - Commands to copy

/// `docker` command lines aimed at this VM's engine, whatever the user's current context.
enum BuildCommands {
    static func base(_ socket: String) -> String { "docker -H \(shellQuote("unix://" + socket))" }

    static func logs(_ id: String, socket: String) -> String {
        "\(base(socket)) buildx history logs --builder default \(shellQuote(id))"
    }
    static func inspect(_ id: String, socket: String) -> String {
        "\(base(socket)) buildx history inspect --builder default \(shellQuote(id))"
    }

    /// The same build again, when its context is a local directory or a URL.
    /// Secret-looking build args are passed by name, so the shell supplies them.
    static func rebuild(_ info: BuildInfo, tags: [String], socket: String) -> String? {
        guard let ctx = info.Context, !ctx.isEmpty else { return nil }
        var parts = ["\(base(socket)) build"]
        if let f = info.Dockerfile, f != "Dockerfile" {
            let path = f.hasPrefix("/") || !ctx.hasPrefix("/") ? f : (ctx as NSString).appendingPathComponent(f)
            parts.append("-f \(shellQuote(path))")
        }
        if let t = info.Target, !t.isEmpty, info.composeProject == nil { parts.append("--target \(shellQuote(t))") }
        if let p = info.Platform, !p.isEmpty { parts.append("--platform \(shellQuote(p.joined(separator: ",")))") }
        for a in (info.BuildArgs ?? []).filter({ $0.Name != "SOURCE_DATE_EPOCH" && !$0.Name.hasPrefix("BUILDKIT_") }) {
            if BuildSecrets.isSecret(a.Name) || a.Value == nil { parts.append("--build-arg \(shellQuote(a.Name))") }
            else { parts.append("--build-arg \(shellQuote("\(a.Name)=\(a.Value ?? "")"))") }
        }
        for t in tags { parts.append("-t \(shellQuote(t))") }
        parts.append(shellQuote(ctx))
        return parts.joined(separator: " ")
    }
}
