import AppKit

/// The VM's CPU and memory over time, and what each running container uses.
@MainActor
final class ActivityPane: SectionPane {
    private let vc: ActivityViewController
    var onChange: () -> Void = {}

    init(store: DockerStore, model: AppModel) {
        vc = ActivityViewController(store: store, model: model)
        model.observe { [weak self] in self?.onChange() }
    }

    var list: NSViewController { vc }
    var detail: NSViewController? { nil }
    var needsDocker: Bool { true }
    var subtitle: String { vc.subtitle }
    func setActive(_ active: Bool) { vc.setActive(active) }
}

/// One container's usage in the table.
struct ContainerUsage: Equatable {
    var id: String
    var name: String
    var image: String
    var cpu: Double
    var memory: UInt64
    var netRate: Double
    var diskRate: Double
    var pids: UInt64

    /// Strict weak ordering comparator for the Activity table.
    /// Finite floats sort before non-finite (`NaN`/`±Inf`), and equal values tie-break on `id`.
    static func comesBefore(_ a: ContainerUsage, _ b: ContainerUsage, key: String, ascending: Bool) -> Bool {
        func compare<T: Comparable>(_ lhs: T, _ rhs: T) -> Bool {
            if lhs != rhs { return ascending ? lhs < rhs : lhs > rhs }
            return a.id < b.id
        }
        func compareDouble(_ lhs: Double, _ rhs: Double) -> Bool {
            switch (lhs.isFinite, rhs.isFinite) {
            case (true, true):
                if lhs != rhs { return ascending ? lhs < rhs : lhs > rhs }
                return a.id < b.id
            case (true, false):
                return true
            case (false, true):
                return false
            case (false, false):
                return a.id < b.id
            }
        }
        switch key {
        case "name": return compare(a.name.lowercased(), b.name.lowercased())
        case "memory": return compare(a.memory, b.memory)
        case "net": return compareDouble(a.netRate, b.netRate)
        case "disk": return compareDouble(a.diskRate, b.diskRate)
        case "pids": return compare(a.pids, b.pids)
        default: return compareDouble(a.cpu, b.cpu)
        }
    }
}

@MainActor
final class ActivityViewController: NSViewController, NSTableViewDataSource, NSTableViewDelegate {
    private let store: DockerStore
    private let model: AppModel
    private let cpu = MetricCard("VM CPU", symbol: "cpu", color: .systemBlue)
    private let mem = MetricCard("VM Memory", symbol: "memorychip", color: .systemPurple)
    private let mac = MetricCard("On This Mac", symbol: "laptopcomputer", color: .systemPink)
    private let table = NSTableView()
    private var rows: [ContainerUsage] = []
    private var previous: [String: StatsSample] = [:]
    private var timer: Timer?
    private var polling = false

    init(store: DockerStore, model: AppModel) {
        self.store = store
        self.model = model
        super.init(nibName: nil, bundle: nil)
        model.observe { [weak self] in self?.updateVM() }
    }

    required init?(coder: NSCoder) { fatalError() }

    var subtitle: String {
        guard let s = model.status?.usage ?? model.samples.last else { return "" }
        return String(format: "CPU %.0f%% · Memory %@", s.guest_cpu_percent, mib(s.guest_mem_used_mib))
    }

    override func loadView() {
        let cards = NSStackView(views: [cpu, mem, mac])
        cards.distribution = .fillEqually
        cards.spacing = 14
        cards.translatesAutoresizingMaskIntoConstraints = false

        let columns: [(String, String, CGFloat, Bool)] = [
            ("name", "Container", 160, false), ("cpu", "CPU", 80, true), ("memory", "Memory", 100, true),
            ("net", "Network", 110, true), ("disk", "Disk", 110, true), ("pids", "Processes", 80, true),
        ]
        for (id, title, width, right) in columns {
            let c = NSTableColumn(identifier: .init(id))
            c.title = title
            c.width = width
            c.minWidth = 60
            c.headerCell.alignment = right ? .right : .left
            c.sortDescriptorPrototype = NSSortDescriptor(key: id, ascending: id == "name")
            table.addTableColumn(c)
        }
        table.style = .inset
        table.rowSizeStyle = .custom
        table.rowHeight = 30
        table.usesAlternatingRowBackgroundColors = true
        table.columnAutoresizingStyle = .uniformColumnAutoresizingStyle
        table.dataSource = self
        table.delegate = self
        table.sortDescriptors = [NSSortDescriptor(key: "cpu", ascending: false)]
        let scroll = NSScrollView()
        scroll.documentView = table
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        scroll.translatesAutoresizingMaskIntoConstraints = false

        let heading = label("Containers", font: .boldSystemFont(ofSize: NSFont.systemFontSize), color: .labelColor)
        let root = NSView()
        for v in [cards, heading, scroll] as [NSView] { root.addSubview(v) }
        NSLayoutConstraint.activate([
            cards.topAnchor.constraint(equalTo: root.safeAreaLayoutGuide.topAnchor, constant: 20),
            cards.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: 20),
            cards.trailingAnchor.constraint(equalTo: root.trailingAnchor, constant: -20),
            heading.topAnchor.constraint(equalTo: cards.bottomAnchor, constant: 24),
            heading.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: 20),
            scroll.topAnchor.constraint(equalTo: heading.bottomAnchor, constant: 6),
            scroll.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: 8),
            scroll.trailingAnchor.constraint(equalTo: root.trailingAnchor, constant: -8),
            scroll.bottomAnchor.constraint(equalTo: root.bottomAnchor),
        ])
        view = root
        updateVM()
    }

    func setActive(_ active: Bool) {
        timer?.invalidate()
        timer = nil
        guard active else { return }
        poll()
        let t = Timer(timeInterval: 3, repeats: true) { [weak self] _ in
            MainActor.assumeIsolated { self?.poll() }
        }
        RunLoop.main.add(t, forMode: .common)
        timer = t
    }

    private func updateVM() {
        guard isViewLoaded else { return }
        let samples = model.samples
        let last = samples.last ?? model.status?.usage
        cpu.valueLabel.stringValue = String(format: "%.0f%%", last?.guest_cpu_percent ?? 0)
        let cpus = model.status?.vm?.cpus ?? model.status?.config.cpus ?? 0
        cpu.detailLabel.stringValue = "\(cpus) CPUs"
        cpu.spark.maxValue = 100
        cpu.spark.values = samples.map(\.guest_cpu_percent)
        mem.valueLabel.stringValue = mib(last?.guest_mem_used_mib ?? 0)
        mem.detailLabel.stringValue = "of \(mib(last?.guest_mem_total_mib ?? 0))"
        mem.spark.maxValue = Double(last?.guest_mem_total_mib ?? 1)
        mem.spark.values = samples.map { Double($0.guest_mem_used_mib) }
        mac.valueLabel.stringValue = mib(last?.host_footprint_mib ?? 0)
        mac.detailLabel.stringValue = String(format: "memory footprint · CPU %.0f%%", last?.host_cpu_percent ?? 0)
        mac.spark.maxValue = 100
        mac.spark.values = samples.map(\.host_cpu_percent)
    }

    /// One reading of every running container, eight at a time. CPU needs two
    /// readings, so the first round shows 0%.
    private func poll() {
        guard !polling, let client = store.client else { return }
        let running = store.containers.filter(\.running)
        if running.isEmpty {
            apply([:], [])
            return
        }
        let sock = store.socket
        polling = true
        BlockingIO.run(on: BlockingIO.docker) {
            let lock = NSLock()
            let sem = DispatchSemaphore(value: 8)
            let group = DispatchGroup()
            var readings: [String: StatsSample] = [:]
            for c in running {
                sem.wait()
                group.enter()
                BlockingIO.docker.async {
                    defer { sem.signal(); group.leave() }
                    if let s = try? client.stats(c.Id) {
                        lock.lock()
                        readings[c.Id] = s
                        lock.unlock()
                    }
                }
            }
            group.wait()
            return readings
        } completion: { [weak self] result in
            guard let self else { return }
            guard self.store.socket == sock else {
                self.polling = false
                return
            }
            self.apply(result, running)
        }
    }

    private func apply(_ readings: [String: StatsSample], _ running: [ContainerSummary]) {
        polling = false
        rows = running.compactMap { c in
            guard let s = readings[c.Id] else { return nil }
            let prev = previous[c.Id]
            let r = prev.map { s.rates(since: $0) }
            return ContainerUsage(id: c.Id, name: c.name, image: c.Image,
                                  cpu: prev.map { s.cpuPercent(since: $0) } ?? 0, memory: s.memoryUsed,
                                  netRate: (r?.rx ?? 0) + (r?.tx ?? 0), diskRate: (r?.read ?? 0) + (r?.write ?? 0), pids: s.pids)
        }
        previous = readings
        sortRows()
        table.reloadData()
    }

    private func sortRows() {
        guard let d = table.sortDescriptors.first, let key = d.key else { return }
        let asc = d.ascending
        rows.sort { ContainerUsage.comesBefore($0, $1, key: key, ascending: asc) }
    }

    func tableView(_ tableView: NSTableView, sortDescriptorsDidChange oldDescriptors: [NSSortDescriptor]) {
        sortRows()
        table.reloadData()
    }

    func numberOfRows(in tableView: NSTableView) -> Int { rows.count }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard let id = tableColumn?.identifier.rawValue else { return nil }
        let u = rows[row]
        let cell = NSTableCellView()
        let text = label(font: .monospacedDigitSystemFont(ofSize: NSFont.systemFontSize, weight: .regular), color: .labelColor)
        cell.addSubview(text)
        cell.textField = text
        if id == "name" {
            let tile = IconTile(size: 18)
            tile.symbol = ImageIcon.symbol(for: u.image)
            tile.tint = ImageIcon.tint(for: u.image)
            cell.addSubview(tile)
            text.font = .systemFont(ofSize: NSFont.systemFontSize)
            text.stringValue = u.name
            NSLayoutConstraint.activate([
                tile.leadingAnchor.constraint(equalTo: cell.leadingAnchor, constant: 2),
                tile.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
                text.leadingAnchor.constraint(equalTo: tile.trailingAnchor, constant: 8),
                text.trailingAnchor.constraint(lessThanOrEqualTo: cell.trailingAnchor),
                text.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
            ])
            return cell
        }
        text.alignment = .right
        text.stringValue = switch id {
        case "cpu": String(format: "%.1f%%", u.cpu)
        case "memory": bytes(Int64(clamping: u.memory))
        case "net": rate(u.netRate)
        case "disk": rate(u.diskRate)
        default: "\(u.pids)"
        }
        NSLayoutConstraint.activate([
            text.leadingAnchor.constraint(equalTo: cell.leadingAnchor),
            text.trailingAnchor.constraint(equalTo: cell.trailingAnchor, constant: -4),
            text.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
        ])
        return cell
    }
}
