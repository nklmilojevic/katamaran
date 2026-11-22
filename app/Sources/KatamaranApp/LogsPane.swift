import AppKit

/// The daemon's, guest console's, VM's and network stack's logs, polled by
/// byte offset. Works while the VM is stopped, which is when they matter most.
@MainActor
final class LogsPane: SectionPane {
    private let vc: LogsViewController
    var onChange: () -> Void = {}

    init(model: AppModel) { vc = LogsViewController(model: model) }

    var list: NSViewController { vc }
    var detail: NSViewController? { nil }
    var needsDocker: Bool { false }
    var subtitle: String { "Katamaran’s own logs" }
    func setActive(_ active: Bool) { vc.setActive(active) }
}

@MainActor
final class LogsViewController: NSViewController, NSSearchFieldDelegate {
    private let model: AppModel
    private let sources = [("Daemon", "daemon"), ("Console", "console"), ("VM", "vm"), ("Network", "gvproxy")]
    private let picker = NSSegmentedControl()
    private let filter = NSSearchField()
    private let follow = NSButton(checkboxWithTitle: "Follow", target: nil, action: nil)
    private let text = LogTextView()
    private var offset: UInt64 = 0
    private var timer: Timer?
    private var generation = 0
    private var requestSeq: UInt64 = 0
    private var inFlight = false
    private var partial = ""
    private var showingError = false

    private static let sourceKey = "logsSource"

    init(model: AppModel) {
        self.model = model
        super.init(nibName: nil, bundle: nil)
    }

    required init?(coder: NSCoder) { fatalError() }

    override func loadView() {
        picker.segmentCount = sources.count
        for (i, s) in sources.enumerated() { picker.setLabel(s.0, forSegment: i) }
        picker.selectedSegment = min(UserDefaults.standard.integer(forKey: Self.sourceKey), sources.count - 1)
        picker.target = self
        picker.action = #selector(sourceChanged)
        filter.placeholderString = "Filter"
        filter.delegate = self
        follow.state = .on
        follow.target = self
        follow.action = #selector(followChanged)
        let spacer = NSView()
        spacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let bar = NSStackView(views: [picker, spacer, filter, follow])
        bar.spacing = 10
        bar.edgeInsets = NSEdgeInsets(top: 10, left: 20, bottom: 10, right: 20)
        bar.translatesAutoresizingMaskIntoConstraints = false
        filter.widthAnchor.constraint(equalToConstant: 200).isActive = true
        let line = NSBox()
        line.boxType = .separator
        line.translatesAutoresizingMaskIntoConstraints = false
        let root = NSView()
        for v in [bar, line, text.scroll] as [NSView] { root.addSubview(v) }
        NSLayoutConstraint.activate([
            bar.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor),
            bar.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            bar.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            line.topAnchor.constraint(equalTo: bar.bottomAnchor),
            line.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            line.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            line.heightAnchor.constraint(equalToConstant: 1),
        ])
        pin(text.scroll, to: root, top: line.bottomAnchor)
        view = root
    }

    func setActive(_ active: Bool) {
        timer?.invalidate()
        timer = nil
        guard active else {
            generation += 1
            inFlight = false
            return
        }
        _ = view
        reload()
        let t = Timer(timeInterval: 1, repeats: true) { [weak self] _ in
            MainActor.assumeIsolated { self?.poll() }
        }
        RunLoop.main.add(t, forMode: .common)
        timer = t
    }

    private var source: String { sources[max(0, picker.selectedSegment)].1 }

    @objc private func sourceChanged() {
        UserDefaults.standard.set(picker.selectedSegment, forKey: Self.sourceKey)
        reload()
    }

    @objc private func followChanged() { if follow.state == .on { text.scrollToEnd() } }

    func controlTextDidChange(_ obj: Notification) { text.filter = filter.stringValue }

    private func reload() {
        generation += 1
        inFlight = false
        text.clear()
        offset = 0
        partial = ""
        showingError = false
        fetch(["source": source, "lines": 1000])
    }

    private func poll() {
        guard !inFlight else { return }
        fetch(["source": source, "offset": offset])
    }

    private func fetch(_ params: [String: Any]) {
        inFlight = true
        requestSeq &+= 1
        let d = model.daemon
        let gen = generation
        let seq = requestSeq
        let p = params as NSDictionary
        BlockingIO.run(on: BlockingIO.logs) {
            Result<LogChunk, Error> { try d.decode("logs", p as? [String: Any], timeout: 3) }
        } completion: { [weak self] result in
            self?.apply(result, generation: gen, seq: seq)
        }
    }

    private func apply(_ result: Result<LogChunk, Error>, generation gen: Int, seq: UInt64) {
        guard gen == generation, seq == requestSeq else { return }
        inFlight = false
        switch result {
        case .failure(let e):
            if offset == 0 && partial.isEmpty {
                text.clear()
                text.append(["No log: \(e.localizedDescription)"], error: false)
                showingError = true
            }
        case .success(let chunk):
            if showingError {
                text.clear()
                showingError = false
            }
            if chunk.offset < offset { text.clear(); partial = "" }
            offset = chunk.offset
            guard !chunk.data.isEmpty else { return }
            var lines = (partial + chunk.data).components(separatedBy: "\n")
            partial = lines.removeLast()
            text.append(lines, error: false)
            if follow.state == .on { text.scrollToEnd() }
        }
    }
}
