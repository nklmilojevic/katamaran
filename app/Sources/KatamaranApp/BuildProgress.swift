import Foundation

/// One step (BuildKit vertex) of a build, from its plain progress output.
struct BuildStep: Equatable {
    enum State: Equatable { case running, done, cached, failed, canceled }

    var number: Int
    var name: String
    var state: State = .running
    /// What BuildKit reported on DONE or ERROR; nil while running or when cached.
    var duration: Double?
    /// The step's own output, without the `#N 0.123` prefix.
    var logs: [String] = []
    var error: String?

    /// `[internal] load …` and friends: BuildKit's housekeeping, not the user's Dockerfile.
    var isInternal: Bool { name.hasPrefix("[internal]") || name.hasPrefix("[auth]") }

    /// "[builder 2/5] RUN make" → ("builder", "2/5", "RUN make").
    var parts: (stage: String?, position: String?, instruction: String) {
        guard name.hasPrefix("["), let close = name.firstIndex(of: "]") else { return (nil, nil, name) }
        let inside = name[name.index(after: name.startIndex)..<close].split(separator: " ")
        let rest = name[name.index(after: close)...].trimmingCharacters(in: .whitespaces)
        guard let last = inside.last, last.contains("/"), last.split(separator: "/").allSatisfy({ Int($0) != nil }) else {
            return (inside.joined(separator: " "), nil, rest)
        }
        let stage = inside.dropLast().joined(separator: " ")
        return (stage.isEmpty ? nil : stage, String(last), rest)
    }
}

/// Reads `docker buildx history logs --progress plain` (the same text as
/// `docker build --progress plain`) into steps, as lines arrive:
///
///     #5 [2/3] RUN make
///     #5 0.412 compiling…
///     #5 DONE 12.3s
///     #6 CACHED
///     #7 ERROR: process "/bin/sh -c exit 1" did not complete successfully: exit code: 1
struct BuildProgressParser {
    private(set) var steps: [BuildStep] = []
    private var index: [Int: Int] = [:]
    /// The summary after the steps: BuildKit's "ERROR: failed to solve: …" and its context.
    private(set) var trailer: [String] = []
    private var inErrorBlock = false

    mutating func feed(_ text: String) {
        for line in text.split(separator: "\n", omittingEmptySubsequences: false) { feed(line: String(line)) }
    }

    mutating func feed(line raw: String) {
        let line = stripANSI(raw.hasSuffix("\r") ? String(raw.dropLast()) : raw)
        guard line.hasPrefix("#"), let space = line.firstIndex(of: " "),
              let n = Int(line[line.index(after: line.startIndex)..<space]) else {
            // "------" blocks after a failure repeat the failing step's last lines; keep the rest.
            if line == "------" { inErrorBlock.toggle(); return }
            if !inErrorBlock, !line.trimmingCharacters(in: .whitespaces).isEmpty, !steps.isEmpty { trailer.append(line) }
            return
        }
        // #0 is buildx's own "building with … instance" line.
        guard n > 0 else { return }
        let rest = String(line[line.index(after: space)...])
        guard let i = index[n] else {
            index[n] = steps.count
            steps.append(BuildStep(number: n, name: rest))
            return
        }
        if rest == "CACHED" {
            steps[i].state = .cached
        } else if rest.hasPrefix("DONE ") {
            steps[i].state = .done
            steps[i].duration = Self.seconds(rest.dropFirst(5))
        } else if rest.hasPrefix("ERROR") {
            steps[i].state = .failed
            let msg = rest.dropFirst(5).drop { $0 == ":" || $0 == " " }
            // "ERROR 1.2s: message" in some versions.
            if let d = Self.seconds(msg.prefix { $0 != ":" }) {
                steps[i].duration = d
                steps[i].error = String(msg.drop { $0 != ":" }.dropFirst().drop { $0 == " " })
            } else {
                steps[i].error = String(msg)
            }
        } else if rest.hasPrefix("CANCELED") {
            steps[i].state = .canceled
        } else if rest == steps[i].name {
            // The printer repeats the header when it switches back to a step.
        } else if let log = Self.logLine(rest) {
            steps[i].logs.append(log)
        } else {
            // Progress chatter: "transferring context: 2B done", "sha256:… 1.2MB / 3MB 0.4s".
            steps[i].logs.append(rest)
        }
    }

    /// "0.412 compiling…" → "compiling…"; nil when it isn't a timestamped log line.
    static func logLine(_ s: String) -> String? {
        guard let sp = s.firstIndex(of: " ") else { return Double(s) != nil ? "" : nil }
        let head = s[..<sp]
        guard head.contains("."), Double(head) != nil else { return nil }
        return String(s[s.index(after: sp)...])
    }

    /// "12.3s" → 12.3.
    static func seconds<S: StringProtocol>(_ s: S) -> Double? {
        let t = s.trimmingCharacters(in: .whitespaces)
        guard t.hasSuffix("s") else { return nil }
        return Double(t.dropLast())
    }

    /// The step that's running now, or the last one that started.
    var current: BuildStep? {
        steps.last { $0.state == .running && !$0.isInternal } ?? steps.last { $0.state == .running } ?? steps.last
    }

    var failed: BuildStep? { steps.first { $0.state == .failed } }

    /// Dockerfile steps finished, of the total their "[stage k/n]" positions announce so far.
    var progress: (done: Int, total: Int)? {
        var totals: [String: Int] = [:]
        var done = 0
        for s in steps {
            let p = s.parts
            guard let pos = p.position, let n = pos.split(separator: "/").last.flatMap({ Int($0) }) else { continue }
            totals[p.stage ?? "", default: 0] = max(totals[p.stage ?? ""] ?? 0, n)
            if s.state == .done || s.state == .cached { done += 1 }
        }
        guard !totals.isEmpty else { return nil }
        let total = totals.values.reduce(0, +)
        return (min(done, total), total)
    }
}
