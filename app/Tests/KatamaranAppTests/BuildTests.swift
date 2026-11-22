import Foundation
import Testing
@testable import KatamaranApp

/// Captured from `docker buildx history` (Buildx 0.37.2, Docker 29.10) against
/// a Katamaran VM, with local paths shortened.
private func fixture(_ name: String) throws -> Data {
    let url = try #require(Bundle.module.url(forResource: name, withExtension: nil, subdirectory: "Fixtures/buildx"))
    return try Data(contentsOf: url)
}

private func text(_ name: String) throws -> String { String(decoding: try fixture(name), as: UTF8.self) }

private func parse(_ log: String) -> BuildProgressParser {
    var p = BuildProgressParser()
    p.feed(log)
    return p
}

@Suite struct BuildHistoryParsing {
    @Test func listLines() throws {
        let list = BuildRecord.parseList(try fixture("ls.json"))
        #expect(list.count == 4)
        let canceled = list[0], ok = list[3]
        #expect(ok.id == "m5hfid023iprqxkinunv1w5z8")
        #expect(ok.ref == "default/default/m5hfid023iprqxkinunv1w5z8")
        #expect(ok.status == .completed)
        #expect(ok.totalSteps == 9 && ok.completedSteps == 9 && ok.cachedSteps == 0)
        let d = try #require(ok.duration)
        #expect(abs(d - 7.828) < 0.01)
        // `ls` reports a canceled build as an error; `inspect` knows better.
        #expect(canceled.status == .failed)
        #expect(list[2].status == .failed)
    }

    @Test func runningLine() throws {
        let list = BuildRecord.parseList(try fixture("ls-running.json"))
        let r = try #require(list.first)
        #expect(r.isRunning)
        #expect(r.completedAt == nil)
        #expect(r.totalSteps == 0)
        #expect(r.name == "b-slow")
    }

    @Test func badLinesAreSkipped() {
        let data = Data("{\"ref\":\"a/b/c\",\"status\":\"Completed\"}\nnot json\n\n".utf8)
        let list = BuildRecord.parseList(data)
        #expect(list.map(\.id) == ["c"])
        #expect(list[0].title == "Untitled build")
    }

    @Test func nameAndTarget() {
        #expect(BuildRecord.split("docker/sso (local)") == ("docker/sso", "local"))
        #expect(BuildRecord.split("b-ok") == ("b-ok", nil))
        #expect(BuildRecord.split("(weird)") == ("(weird)", nil))
        #expect(BuildRecord.split("web ()") == ("web", nil))
    }

    @Test func inspectCompleted() throws {
        let i = try BuildInfo.parse(try fixture("inspect-ok.json"))
        #expect(i.status == .completed)
        #expect(i.Context == "/Users/dev/src/b-ok")
        #expect(i.Dockerfile == "Dockerfile")
        #expect(i.platforms == ["linux/arm64"])
        #expect(i.baseImages == ["alpine@3.20"])
        #expect(i.BuildArgs?.map(\.Name) == ["API_TOKEN", "GREETING"])
        #expect(abs((i.seconds ?? 0) - 7.828) < 0.01)
        #expect(i.Error == nil)
    }

    @Test func inspectFailed() throws {
        let i = try BuildInfo.parse(try fixture("inspect-fail.json"))
        #expect(i.status == .failed)
        let e = try #require(i.Error)
        #expect(e.Name == #"[3/3] RUN echo "about to fail" && ls /nope"#)
        #expect(e.Message?.hasSuffix("exit code: 1") == true)
        #expect(e.Logs == ["about to fail", "ls: /nope: No such file or directory"])
        let src = try #require(e.source)
        #expect(src.hasPrefix("Dockerfile:3"))
        #expect(src.contains(">>> RUN echo \"about to fail\""))
    }

    @Test func inspectCanceledAndRunning() throws {
        #expect(try BuildInfo.parse(try fixture("inspect-canceled.json")).status == .canceled)
        let r = try BuildInfo.parse(try fixture("inspect-running.json"))
        #expect(r.status == .running)
        #expect(r.CompletedAt == nil)
        #expect(r.platforms.isEmpty)
    }

    @Test func platformAsString() throws {
        let i = try BuildInfo.parse(Data(#"{"Platform":"linux/amd64","Status":"completed"}"#.utf8))
        #expect(i.platforms == ["linux/amd64"])
        let j = try BuildInfo.parse(Data(#"{"Platform":["linux/amd64","linux/arm64"]}"#.utf8))
        #expect(j.platforms == ["linux/amd64", "linux/arm64"])
    }

    @Test func dockerfileLocation() throws {
        let i = try BuildInfo.parse(try fixture("inspect-ok.json"))
        #expect(i.dockerfileURL(exists: { $0 == "/Users/dev/src/b-ok/Dockerfile" })?.path == "/Users/dev/src/b-ok/Dockerfile")
        #expect(i.dockerfileURL(exists: { _ in false }) == nil)
        // Compose builds without a context fall back to the project's directory.
        let compose = try BuildInfo.parse(Data(#"""
            {"Dockerfile":"docker/sso/Dockerfile","Labels":[{"Name":"com.docker.compose.project","Value":"shop"}]}
            """#.utf8))
        #expect(compose.dockerfileURL(projectDirs: ["shop": "/src/shop"], exists: { _ in true })?.path == "/src/shop/docker/sso/Dockerfile")
        let remote = try BuildInfo.parse(Data(#"{"Context":"https://github.com/a/b.git","Dockerfile":"Dockerfile"}"#.utf8))
        #expect(remote.dockerfileURL(exists: { _ in true }) == nil)
    }
}

@Suite struct BuildProgressParsing {
    @Test func completedSteps() throws {
        let p = parse(try text("logs-ok.txt"))
        #expect(p.steps.map(\.number) == Array(1...9))
        #expect(p.steps.filter(\.isInternal).count == 4)
        let run = try #require(p.steps.first { $0.number == 7 })
        #expect(run.name == #"[stage-1 1/2] RUN echo "hey" && sleep 1"#)
        #expect(run.state == .done)
        #expect(run.duration == 1.2)
        #expect(run.logs == ["hey"])
        #expect(run.parts.stage == "stage-1")
        #expect(run.parts.position == "1/2")
        #expect(run.parts.instruction == #"RUN echo "hey" && sleep 1"#)
        #expect(p.steps[1].duration == 5.8)
        #expect(p.progress?.done == 4)
        #expect(p.progress?.total == 4)
        #expect(p.failed == nil)
    }

    @Test func outputTags() throws {
        let out = BuildOutput(steps: parse(try text("logs-ok.txt")).steps)
        #expect(out.tags == ["demo/ok:latest"])
        #expect(out.kind == "Docker image")
        #expect(BuildOutput.short("docker.io/library/nginx:1") == "nginx:1")
        #expect(BuildOutput.short("ghcr.io/a/b:c") == "ghcr.io/a/b:c")
    }

    @Test func failedStep() throws {
        let p = parse(try text("logs-fail.txt"))
        #expect(p.steps.first { $0.number == 4 }?.state == .cached)
        let f = try #require(p.failed)
        #expect(f.number == 6)
        #expect(f.error?.hasPrefix("process \"/bin/sh -c echo") == true)
        #expect(f.logs == ["about to fail", "ls: /nope: No such file or directory"])
        // The "------" recap repeats the step's output; it isn't a second copy of anything.
        #expect(p.trailer.isEmpty)
    }

    @Test func runningStep() throws {
        let p = parse(try text("logs-running.txt"))
        let cur = try #require(p.current)
        #expect(cur.number == 5)
        #expect(cur.state == .running)
        #expect(cur.logs.last == "tick 11")
        #expect(cur.parts.position == "2/3")
        #expect(p.progress?.done == 1)
        #expect(p.progress?.total == 3)
    }

    @Test func canceledAndOtherForms() {
        var p = BuildProgressParser()
        p.feed(line: "#0 building with \"default\" instance using docker driver")
        p.feed(line: "#3 [2/2] RUN sleep 30")
        p.feed(line: "#3 CANCELED")
        p.feed(line: "#4 [build 1/1] RUN make")
        p.feed(line: "#4 ERROR 2.5s: exit code: 2")
        p.feed(line: "\u{1B}[31m#5 exporting to image\u{1B}[0m")
        #expect(p.steps.map(\.number) == [3, 4, 5])
        #expect(p.steps[0].state == .canceled)
        #expect(p.steps[1].state == .failed)
        #expect(p.steps[1].duration == 2.5)
        #expect(p.steps[1].error == "exit code: 2")
        #expect(p.steps[2].name == "exporting to image")
        #expect(BuildStep(number: 1, name: "[internal] load .dockerignore").parts.instruction == "load .dockerignore")
    }

    @Test func linesArriveInPieces() throws {
        let whole = parse(try text("logs-ok.txt"))
        var p = BuildProgressParser()
        for line in try text("logs-ok.txt").split(separator: "\n", omittingEmptySubsequences: false) { p.feed(line: String(line)) }
        #expect(p.steps == whole.steps)
    }
}

@Suite struct BuildListGrouping {
    private let now = ISO8601DateFormatter().date(from: "2026-11-19T12:00:00Z")!
    private var utc: Calendar {
        var c = Calendar(identifier: .gregorian)
        c.timeZone = TimeZone(identifier: "UTC")!
        return c
    }

    private func rec(_ id: String, _ status: String, hoursAgo: Double, name: String? = nil) -> BuildRecord {
        BuildRecord(ref: "default/default/\(id)", name: name ?? id, status: status,
                    createdAt: now.addingTimeInterval(-hoursAgo * 3600))
    }

    @Test func runningFirstThenByDay() {
        let all = [
            rec("old", "Completed", hoursAgo: 72),
            rec("today1", "Completed", hoursAgo: 1),
            rec("run", "Running", hoursAgo: 30),
            rec("yest", "Error", hoursAgo: 20),
            rec("today2", "Error", hoursAgo: 0.5),
        ]
        let s = BuildList.sections(all, now: now, calendar: utc)
        #expect(s.map(\.kind) == [.running, .today, .yesterday, .earlier])
        #expect(s[0].builds.map(\.id) == ["run"])
        #expect(s[1].builds.map(\.id) == ["today2", "today1"])
        #expect(s[2].builds.map(\.id) == ["yest"])
        #expect(s[3].builds.map(\.id) == ["old"])
    }

    @Test func search() {
        let all = [rec("a1", "Completed", hoursAgo: 1, name: "docker/sso (local)"),
                   rec("b2", "Error", hoursAgo: 2, name: "web"),
                   rec("c3", "Running", hoursAgo: 0, name: "api")]
        #expect(BuildList.sections(all, query: "SSO", now: now, calendar: utc).flatMap(\.builds).map(\.id) == ["a1"])
        #expect(BuildList.sections(all, query: "failed", now: now, calendar: utc).flatMap(\.builds).map(\.id) == ["b2"])
        #expect(BuildList.sections(all, query: "c3", now: now, calendar: utc).flatMap(\.builds).map(\.id) == ["c3"])
        #expect(BuildList.sections(all, query: "nothing", now: now, calendar: utc).isEmpty)
    }

    @Test func statusOverride() {
        let all = [rec("x", "Error", hoursAgo: 1)]
        let s = BuildList.sections(all, query: "canceled", now: now, calendar: utc, status: { _ in .canceled })
        #expect(s.flatMap(\.builds).count == 1)
        #expect(BuildList.summary(all, status: { _ in .canceled }) == "1 build")
    }

    @Test func summaries() {
        #expect(BuildList.summary([]) == "No builds")
        let all = [rec("a", "Running", hoursAgo: 0), rec("b", "Error", hoursAgo: 1), rec("c", "Completed", hoursAgo: 2)]
        #expect(BuildList.summary(all) == "1 running · 3 builds · 1 failed")
        #expect(BuildList.cacheSummary(cached: 4, total: 9) == "4 of 9 cached")
        #expect(BuildList.cacheSummary(cached: 9, total: 9) == "all 9 cached")
        #expect(BuildList.cacheSummary(cached: 0, total: 9) == "none cached")
        #expect(BuildList.cacheSummary(cached: 0, total: 0) == nil)
    }

    @Test func durations() {
        #expect(buildDuration(0.43) == "0.4s")
        #expect(buildDuration(7.83) == "7.8s")
        #expect(buildDuration(42) == "42s")
        #expect(buildDuration(72) == "1m 12s")
        #expect(buildDuration(3720) == "1h 2m")
        #expect(elapsedClock(7) == "0:07")
        #expect(elapsedClock(750) == "12:30")
        #expect(elapsedClock(3723) == "1:02:03")
    }
}

@Suite struct BuildCommandLines {
    @Test func secretsAreMasked() {
        for n in ["API_TOKEN", "GITHUB_TOKEN", "SATIS_PASSWORD", "aws_secret_access_key", "NPM_AUTH", "SSH_PRIVATE_KEY"] {
            #expect(BuildSecrets.isSecret(n), "\(n)")
        }
        for n in ["GREETING", "NODE_VERSION", "SOURCE_DATE_EPOCH", "KEYBOARD_LAYOUT"] {
            #expect(!BuildSecrets.isSecret(n), "\(n)")
        }
        #expect(BuildSecrets.display(.init(Name: "API_TOKEN", Value: "supersecret")) == "••••••••")
        #expect(BuildSecrets.display(.init(Name: "GREETING", Value: "hey")) == "hey")
    }

    @Test func rebuild() throws {
        let i = try BuildInfo.parse(try fixture("inspect-ok.json"))
        let cmd = try #require(BuildCommands.rebuild(i, tags: ["demo/ok:latest"], socket: "/Users/me/.katamaran/run/docker.sock"))
        #expect(cmd == "docker -H unix:///Users/me/.katamaran/run/docker.sock build --build-arg API_TOKEN --build-arg GREETING=hey -t demo/ok:latest /Users/dev/src/b-ok")
        #expect(!cmd.contains("supersecret"))
        // No context in the record (Compose, Bake): nothing to rebuild from.
        let compose = try BuildInfo.parse(Data(#"{"Dockerfile":"Dockerfile"}"#.utf8))
        #expect(BuildCommands.rebuild(compose, tags: [], socket: "/s") == nil)
        #expect(BuildCommands.logs("abc", socket: "/s") == "docker -H unix:///s buildx history logs --builder default abc")
        #expect(BuildCommands.inspect("abc", socket: "/s") == "docker -H unix:///s buildx history inspect --builder default abc")
        #expect(BuildCommands.logs("a b'c", socket: "/s") == #"docker -H unix:///s buildx history logs --builder default 'a b'\''c'"#)
        #expect(BuildCommands.inspect("a b'c", socket: "/s") == #"docker -H unix:///s buildx history inspect --builder default 'a b'\''c'"#)
    }

    @Test func nextInfoToFetchSkipsFailedInFlightAndFetchedBuilds() throws {
        let now = Date()
        let records = [
            BuildRecord(ref: "default/default/run1", name: "run", status: "Running", createdAt: now),
            BuildRecord(ref: "default/default/fail1", name: "fail", status: "Error", createdAt: now),
            BuildRecord(ref: "default/default/ok1", name: "ok", status: "Completed", createdAt: now),
            BuildRecord(ref: "default/default/ok2", name: "ok2", status: "Completed", createdAt: now),
        ]
        let runningInfo = try BuildInfo.parse(Data(#"{"Status":"running"}"#.utf8))
        let cooldown = ["fail1": now.addingTimeInterval(30)]
        // Even if run1 is running and already has info, and fail1 is in cooldown, ok1 is selected next.
        let next = BuildStore.nextInfoToFetch(
            records: records,
            infos: ["run1": runningInfo],
            fetching: [],
            failedRetryAfter: cooldown,
            now: now
        )
        #expect(next == "ok1")
        // If ok1 is in-flight, ok2 is selected.
        let next2 = BuildStore.nextInfoToFetch(
            records: records,
            infos: ["run1": runningInfo],
            fetching: ["ok1"],
            failedRetryAfter: cooldown,
            now: now
        )
        #expect(next2 == "ok2")
        // Once the retry cooldown expires, transient failures on finished builds become eligible again.
        let retry = BuildStore.nextInfoToFetch(
            records: records,
            infos: ["run1": runningInfo],
            fetching: [],
            failedRetryAfter: cooldown,
            now: now.addingTimeInterval(31)
        )
        #expect(retry == "fail1")
    }

    @MainActor
    @Test func buildStoreFollowAdvancesConnectionEpochOnReconnectToSameSocket() {
        let store = BuildStore()
        let e0 = store.connectionEpoch
        store.follow("/tmp/kat-build-test.sock")
        let e1 = store.connectionEpoch
        #expect(e1 > e0)
        store.follow(nil)
        let e2 = store.connectionEpoch
        #expect(e2 > e1)
        store.follow("/tmp/kat-build-test.sock")
        let e3 = store.connectionEpoch
        #expect(e3 > e2)
        store.follow(nil)
    }

    @Test func cliErrors() {
        #expect(BuildxCLI.message(Data("ERROR: no such build record\n".utf8)) == "no such build record")
        #expect(BuildxCLI.message(Data("docker: 'buildx' is not a docker command.\nSee 'docker --help'\n".utf8)) == "The Docker CLI has no Buildx plugin.")
        #expect(BuildxCLI.message(Data()) == nil)
    }
}
