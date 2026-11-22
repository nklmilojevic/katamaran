import AppKit
import Testing
@testable import KatamaranApp

// MARK: Fixtures

/// What `GET /api/v1/pods` returned from k3s, trimmed: a ready nginx pod, a
/// crash-looping one, and coredns in kube-system.
private let podsJSON = """
{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"912"},"items":[
 {"metadata":{"name":"web-7d4b9c8f5-abcde","namespace":"default","uid":"u1",
   "creationTimestamp":"2026-11-22T10:00:00Z","labels":{"app":"web","pod-template-hash":"7d4b9c8f5"},
   "ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"web-7d4b9c8f5","uid":"r1","controller":true}]},
  "spec":{"nodeName":"katamaran","containers":[{"name":"nginx","image":"nginx:alpine","ports":[{"containerPort":80,"protocol":"TCP"}]}]},
  "status":{"phase":"Running","podIP":"10.42.0.9","startTime":"2026-11-22T10:00:00Z",
   "containerStatuses":[{"name":"nginx","ready":true,"restartCount":0,"image":"docker.io/library/nginx:alpine",
     "state":{"running":{"startedAt":"2026-11-22T10:00:02Z"}}}]}},
 {"metadata":{"name":"crashy","namespace":"default","uid":"u2","creationTimestamp":"2026-11-22T11:00:00Z"},
  "spec":{"containers":[{"name":"app","image":"busybox"},{"name":"sidecar","image":"busybox"}]},
  "status":{"phase":"Running","containerStatuses":[
     {"name":"app","ready":false,"restartCount":4,"state":{"waiting":{"reason":"CrashLoopBackOff","message":"back-off"}},
      "lastState":{"terminated":{"exitCode":1,"reason":"Error"}}},
     {"name":"sidecar","ready":true,"restartCount":1,"state":{"running":{}}}]}},
 {"metadata":{"name":"coredns-5d78c9869d-x","namespace":"kube-system","uid":"u3","creationTimestamp":"2026-11-22T09:00:00Z"},
  "spec":{"containers":[{"name":"coredns","image":"rancher/mirrored-coredns-coredns:1.12.1"}]},
  "status":{"phase":"Running","containerStatuses":[{"name":"coredns","ready":true,"restartCount":0,"state":{"running":{}}}]}}
]}
"""

private let servicesJSON = """
{"kind":"ServiceList","apiVersion":"v1","items":[
 {"metadata":{"name":"kubernetes","namespace":"default","creationTimestamp":"2026-11-22T09:00:00Z"},
  "spec":{"type":"ClusterIP","clusterIP":"10.43.0.1","ports":[{"name":"https","protocol":"TCP","port":443,"targetPort":6443}]}},
 {"metadata":{"name":"web","namespace":"default","creationTimestamp":"2026-11-22T10:00:05Z"},
  "spec":{"type":"LoadBalancer","clusterIP":"10.43.12.7","selector":{"app":"web"},
   "ports":[{"protocol":"TCP","port":8080,"targetPort":80,"nodePort":31234},
            {"name":"metrics","protocol":"UDP","port":9000,"targetPort":"metrics","nodePort":31000}]},
  "status":{"loadBalancer":{"ingress":[{"ip":"192.168.5.15","ipMode":"VIP"}]}}},
 {"metadata":{"name":"api","namespace":"shop"},
  "spec":{"type":"NodePort","ports":[{"protocol":"TCP","port":80,"targetPort":"http","nodePort":30080}]}}
]}
"""

private func decodePods() throws -> [K8sPod] {
    try JSONDecoder().decode(K8sList<K8sPod>.self, from: Data(podsJSON.utf8)).items
}

private func decodeServices() throws -> [K8sService] {
    try JSONDecoder().decode(K8sList<K8sService>.self, from: Data(servicesJSON.utf8)).items
}

private func row(_ key: String, _ title: String) -> ResourceRow {
    ResourceRow(key: key, title: title, subtitle: "", symbol: "cube.fill", tint: .systemBlue, inUse: true, deletable: true)
}

private func ns(_ key: String) -> String { String(key.split(separator: "/").first ?? "") }

// MARK: Pods

@Test func podsDecodeAndSummarizeLikeKubectl() throws {
    let pods = try decodePods()
    #expect(pods.count == 3)
    let web = pods[0]
    #expect(web.key == "default/web-7d4b9c8f5-abcde")
    #expect(web.displayStatus == "Running")
    #expect(web.readyCount == 1 && web.containerCount == 1)
    #expect(web.health == .running)
    #expect(web.owner?.kind == "ReplicaSet")
    #expect(web.status?.podIP == "10.42.0.9")

    let crashy = pods[1]
    #expect(crashy.displayStatus == "CrashLoopBackOff")
    #expect(crashy.restarts == 5)
    #expect(crashy.readyCount == 1 && crashy.containerCount == 2)
    #expect(crashy.health == .failed)
    #expect(crashy.containerNames == ["app", "sidecar"])
    #expect(crashy.owner == nil)
}

@Test func podStatusCoversTerminatingAndPending() throws {
    var pod = try decodePods()[0]
    pod.metadata.deletionTimestamp = "2026-11-22T12:00:00Z"
    #expect(pod.displayStatus == "Terminating")
    #expect(pod.health == .transitional)

    var pending = try decodePods()[0]
    pending.status = K8sPod.Status(phase: "Pending", containerStatuses: [
        K8sPod.ContainerStatus(name: "nginx", ready: false, restartCount: 0,
                               state: K8sPod.State(waiting: .init(reason: "ContainerCreating"))),
    ])
    #expect(pending.displayStatus == "ContainerCreating")
    #expect(pending.health == .transitional)

    var done = try decodePods()[0]
    done.status = K8sPod.Status(phase: "Succeeded", containerStatuses: [
        K8sPod.ContainerStatus(name: "nginx", ready: false, restartCount: 0,
                               state: K8sPod.State(terminated: .init(reason: "Completed", exitCode: 0))),
    ])
    #expect(done.displayStatus == "Succeeded")
    #expect(done.health == .stopped)
}

@Test func podSubtitleShowsStatusReadinessRestartsAndAge() throws {
    let pods = try decodePods()
    let now = parseDockerDate("2026-11-22T12:00:00Z")!
    #expect(K8sGrouping.podSubtitle(pods[0], now: now) == "Running · 1/1 · 2h")
    #expect(K8sGrouping.podSubtitle(pods[1], now: now) == "CrashLoopBackOff · 1/2 · 5 restarts · 1h")
}

@Test func compactAgeMatchesKubectlUnits() {
    let now = Date(timeIntervalSince1970: 1_000_000)
    #expect(compactAge(now.addingTimeInterval(-45), now: now) == "45s")
    #expect(compactAge(now.addingTimeInterval(-600), now: now) == "10m")
    #expect(compactAge(now.addingTimeInterval(-7200), now: now) == "2h")
    #expect(compactAge(now.addingTimeInterval(-3 * 86400), now: now) == "3d")
    #expect(compactAge(now.addingTimeInterval(30), now: now) == "0s")
    #expect(compactAge(nil) == "-")
}

// MARK: Services

@Test func servicesDecodeIntOrStringPortsAndLocalPorts() throws {
    let svcs = try decodeServices()
    #expect(svcs.count == 3)
    #expect(svcs[0].type == "ClusterIP")
    #expect(svcs[0].localPorts.isEmpty)
    #expect(svcs[0].portsLabel == "443/TCP")

    let web = svcs[1]
    #expect(web.ports[0].targetPort == .int(80))
    #expect(web.ports[1].targetPort == .string("metrics"))
    #expect(web.portsLabel == "8080:31234/TCP, 9000:31000/UDP")
    // The LoadBalancer's port and its node port, TCP only: UDP isn't forwarded.
    #expect(web.localPorts == [8080, 31234])
    #expect(K8sGrouping.serviceSubtitle(web) == "LoadBalancer · 8080:31234/TCP, 9000:31000/UDP")

    let api = svcs[2]
    #expect(api.localPorts == [30080])
    #expect(api.spec?.clusterIP == nil)
}

// MARK: Grouping and filtering

@Test func rowsGroupByNamespaceWithDefaultFirst() {
    let rows = [row("shop/api", "api"), row("default/web", "web"), row("apps/b", "B"), row("apps/a", "a")]
    let groups = K8sGrouping.byNamespace(rows, query: "", namespace: ns, search: { [$0.title] })
    #expect(groups.map(\.title) == ["default", "apps", "shop"])
    #expect(groups[1].rows.map(\.title) == ["a", "B"])
}

@Test func searchFiltersRowsAndDropsEmptyNamespaces() {
    let rows = [row("shop/api", "api"), row("default/web", "web")]
    let groups = K8sGrouping.byNamespace(rows, query: " SHOP ", namespace: ns, search: { [$0.title, ns($0.key)] })
    #expect(groups.map(\.title) == ["shop"])
}

@Test func systemNamespacesAreRecognized() {
    #expect(K8sNamespaces.isSystem("kube-system"))
    #expect(K8sNamespaces.isSystem("kube-node-lease"))
    #expect(!K8sNamespaces.isSystem("default"))
    #expect(!K8sNamespaces.isSystem("kube-flannel-custom"))
}

// MARK: Empty states

private func status(_ state: String, enabled: Bool = true, error: String? = nil) -> K8sStatus {
    K8sStatus(enabled: enabled, state: state, installed: true, error: error)
}

@Test func blockedStatesFollowTheVMAndCluster() {
    #expect(K8sGrouping.blocked(phase: .stopped, busy: false, k8s: nil, connected: false, loaded: false, loadError: nil)?.action == "Start")
    #expect(K8sGrouping.blocked(phase: .running, busy: false, k8s: status("stopped", enabled: false),
                                connected: false, loaded: false, loadError: nil)?.action == "Turn On")
    let starting = K8sGrouping.blocked(phase: .running, busy: false, k8s: status("starting"),
                                       connected: false, loaded: false, loadError: nil)
    #expect(starting?.busy == true && starting?.action == nil)
    let failed = K8sGrouping.blocked(phase: .running, busy: false, k8s: status("failed", error: "k3s exited"),
                                     connected: false, loaded: false, loadError: nil)
    #expect(failed?.action == "Try Again" && failed?.message == "k3s exited")
    // Running and fetched: the list shows.
    #expect(K8sGrouping.blocked(phase: .running, busy: false, k8s: status("running"),
                                connected: true, loaded: true, loadError: nil) == nil)
    // Running but the proxy isn't answering yet.
    let connecting = K8sGrouping.blocked(phase: .running, busy: false, k8s: status("running"),
                                         connected: true, loaded: false, loadError: "connection refused")
    #expect(connecting?.busy == true && connecting?.message == "connection refused")
    // A later failed poll keeps the last list rather than blanking it.
    #expect(K8sGrouping.blocked(phase: .running, busy: false, k8s: status("running"),
                                connected: true, loaded: true, loadError: "timeout") == nil)
}

@Test func daemonStatusDecodesProxySocket() throws {
    let json = #"{"enabled":true,"state":"running","installed":true,"api_port":6443,"proxy_socket":"/Users/x/.katamaran/run/k8s-proxy.sock","context":"katamaran"}"#
    let s = try JSONDecoder().decode(K8sStatus.self, from: Data(json.utf8))
    #expect(s.proxySocket == "/Users/x/.katamaran/run/k8s-proxy.sock")
    #expect(s.apiPort == 6443)
}

// MARK: HTTP

@Test func kubernetesRequestsUseLocalhostHostHeader() {
    let head = DockerClient.head("DELETE", "/api/v1/namespaces/default/pods/web", host: K8sClient.host)
    #expect(head.hasPrefix("DELETE /api/v1/namespaces/default/pods/web HTTP/1.1\r\nHost: localhost\r\n"))
    #expect(DockerClient.head("GET", "/_ping").contains("Host: docker\r\n"))
}

@Test func logPathEncodesNamesAndContainer() {
    #expect(K8sClient.logPath(namespace: "default", pod: "web-1", container: nil, tail: 10)
        == "/api/v1/namespaces/default/pods/web-1/log?follow=true&tailLines=10")
    #expect(K8sClient.logPath(namespace: "a b", pod: "p", container: "c/d", tail: 5)
        == "/api/v1/namespaces/a%20b/pods/p/log?follow=true&tailLines=5&container=c%2Fd")
}

@Test func apiErrorsUseTheStatusMessage() {
    let body = Data(#"{"kind":"Status","status":"Failure","message":"pods \"x\" not found","code":404}"#.utf8)
    if case .protocolError(let m) = K8sClient.failure(404, body) { #expect(m == "pods \"x\" not found") } else { Issue.record("wrong error") }
    if case .protocolError(let m) = K8sClient.failure(502, Data()) { #expect(m == "Kubernetes answered HTTP 502") } else { Issue.record("wrong error") }
}
