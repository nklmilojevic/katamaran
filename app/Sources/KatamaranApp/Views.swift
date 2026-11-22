import AppKit

// Building blocks of the main window, all in system colors so they follow
// light and dark mode and the accent color.

final class FlippedView: NSView {
    override var isFlipped: Bool { true }
}

/// A vertical stack in a scroll view that tracks the scroll view's width.
@MainActor
final class ScrollingStack {
    let scroll = NSScrollView()
    let stack = NSStackView()

    init(insets: NSEdgeInsets = NSEdgeInsets(top: 16, left: 20, bottom: 20, right: 20), spacing: CGFloat = 18) {
        let doc = FlippedView()
        doc.translatesAutoresizingMaskIntoConstraints = false
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = spacing
        stack.translatesAutoresizingMaskIntoConstraints = false
        doc.addSubview(stack)
        scroll.documentView = doc
        scroll.hasVerticalScroller = true
        scroll.drawsBackground = false
        scroll.borderType = .noBorder
        scroll.translatesAutoresizingMaskIntoConstraints = false
        let clip = scroll.contentView
        NSLayoutConstraint.activate([
            doc.leadingAnchor.constraint(equalTo: clip.leadingAnchor),
            doc.trailingAnchor.constraint(equalTo: clip.trailingAnchor),
            doc.topAnchor.constraint(equalTo: clip.topAnchor),
            stack.leadingAnchor.constraint(equalTo: doc.leadingAnchor, constant: insets.left),
            stack.trailingAnchor.constraint(equalTo: doc.trailingAnchor, constant: -insets.right),
            stack.topAnchor.constraint(equalTo: doc.topAnchor, constant: insets.top),
            stack.bottomAnchor.constraint(equalTo: doc.bottomAnchor, constant: -insets.bottom),
        ])
    }

    /// Replace the stack's views; each spans the stack's width.
    func set(_ views: [NSView]) {
        stack.arrangedSubviews.forEach { $0.removeFromSuperview() }
        for v in views {
            stack.addArrangedSubview(v)
            v.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        }
    }
}

/// A rounded, tinted square with a white symbol, and an optional status dot:
/// the "app icon" of a container, project, volume, image or network.
final class IconTile: NSView {
    var symbol = "shippingbox.fill" { didSet { needsDisplay = true } }
    var tint: NSColor = .systemBlue { didSet { needsDisplay = true } }
    var dot: NSColor? { didSet { needsDisplay = true } }
    var dimmed = false { didSet { needsDisplay = true } }
    private let side: CGFloat

    init(size: CGFloat = 28) {
        side = size
        super.init(frame: NSRect(x: 0, y: 0, width: size, height: size))
        translatesAutoresizingMaskIntoConstraints = false
        widthAnchor.constraint(equalToConstant: size).isActive = true
        heightAnchor.constraint(equalToConstant: size).isActive = true
    }

    required init?(coder: NSCoder) { fatalError() }

    override var intrinsicContentSize: NSSize { NSSize(width: side, height: side) }

    override func draw(_ dirtyRect: NSRect) {
        let r = bounds.insetBy(dx: 0.5, dy: 0.5)
        let path = NSBezierPath(roundedRect: r, xRadius: side * 0.24, yRadius: side * 0.24)
        let base = dimmed ? tint.blended(withFraction: 0.55, of: .systemGray) ?? tint : tint
        NSGradient(starting: base.highlight(withLevel: 0.18) ?? base, ending: base)?.draw(in: path, angle: -90)
        let config = NSImage.SymbolConfiguration(pointSize: side * 0.46, weight: .semibold)
            .applying(.init(paletteColors: [.white]))
        if let img = NSImage(systemSymbolName: symbol, accessibilityDescription: nil)?.withSymbolConfiguration(config) {
            let s = img.size
            img.draw(in: NSRect(x: (bounds.width - s.width) / 2, y: (bounds.height - s.height) / 2,
                                width: s.width, height: s.height))
        }
        if let dot {
            let d = max(8, side * 0.32)
            let rect = NSRect(x: bounds.maxX - d + 2, y: -2, width: d, height: d)
            NSColor.windowBackgroundColor.setFill()
            NSBezierPath(ovalIn: rect.insetBy(dx: -1.5, dy: -1.5)).fill()
            dot.setFill()
            NSBezierPath(ovalIn: rect).fill()
        }
    }

    override func viewDidChangeEffectiveAppearance() { needsDisplay = true }
}

/// Icons for what an image probably is, by name. Unknown images get a box.
enum ImageIcon {
    private static let kinds: [(symbol: String, names: [String])] = [
        ("cylinder.split.1x2.fill", ["postgres", "mysql", "mariadb", "mongo", "redis", "valkey", "clickhouse",
                                      "cockroach", "cassandra", "couchdb", "elasticsearch", "opensearch",
                                      "memcached", "influxdb", "timescale", "keydb", "dragonfly", "sqlite", "neo4j"]),
        ("globe", ["nginx", "traefik", "caddy", "httpd", "haproxy", "envoy", "apache", "whoami", "kong"]),
        ("arrow.left.arrow.right", ["kafka", "zookeeper", "rabbitmq", "nats", "redpanda", "pulsar", "mosquitto", "emqx"]),
        ("chart.xyaxis.line", ["grafana", "prometheus", "loki", "tempo", "jaeger", "victoria", "otel", "zipkin"]),
        ("externaldrive.fill", ["minio", "localstack", "azurite", "registry", "seaweedfs"]),
        ("chevron.left.forwardslash.chevron.right", ["node", "python", "golang", "ruby", "php", "openjdk", "rust",
                                                     "eclipse-temurin", "bun", "deno", "dotnet", "gradle", "maven"]),
        ("envelope.fill", ["mailhog", "mailpit", "maildev", "smtp"]),
        ("terminal.fill", ["alpine", "ubuntu", "debian", "busybox", "fedora", "centos", "archlinux", "amazonlinux"]),
    ]

    private static let tints: [NSColor] = [
        .systemBlue, .systemPurple, .systemPink, .systemOrange, .systemTeal, .systemIndigo,
        .systemGreen, .systemRed, .systemCyan, .systemBrown, .systemMint, .systemYellow,
    ]

    /// The repository's last path component, without tag or digest: "library/nginx:alpine" → "nginx".
    static func baseName(_ image: String) -> String {
        var s = image
        if let at = s.firstIndex(of: "@") { s = String(s[..<at]) }
        if let slash = s.lastIndex(of: "/") { s = String(s[s.index(after: slash)...]) }
        if let colon = s.firstIndex(of: ":") { s = String(s[..<colon]) }
        return s.lowercased()
    }

    static func symbol(for image: String) -> String {
        let name = baseName(image)
        return kinds.first { $0.names.contains { name.contains($0) } }?.symbol ?? "shippingbox.fill"
    }

    /// The same color for the same name, every launch.
    static func tint(for key: String) -> NSColor {
        var h: UInt32 = 2166136261
        for b in baseName(key).utf8 { h = (h ^ UInt32(b)) &* 16777619 }
        return tints[Int(h % UInt32(tints.count))]
    }
}

extension ContainerHealth {
    var color: NSColor {
        switch self {
        case .running: .systemGreen
        case .transitional: .systemYellow
        case .failed: .systemRed
        case .stopped: .tertiaryLabelColor
        }
    }
}

/// A centered symbol, title, message and optional button: "No containers",
/// "Katamaran is stopped".
@MainActor
final class EmptyStateView: NSView {
    let icon = NSImageView()
    let title = NSTextField(labelWithString: "")
    let message = NSTextField(wrappingLabelWithString: "")
    let button = NSButton(title: "", target: nil, action: nil)
    let spinner = NSProgressIndicator()
    var onAction: () -> Void = {}

    init() {
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        icon.symbolConfiguration = .init(pointSize: 40, weight: .light)
        icon.contentTintColor = .tertiaryLabelColor
        title.font = .systemFont(ofSize: 17, weight: .semibold)
        title.textColor = .secondaryLabelColor
        title.alignment = .center
        message.font = .systemFont(ofSize: NSFont.systemFontSize)
        message.textColor = .secondaryLabelColor
        message.alignment = .center
        message.isSelectable = false
        message.preferredMaxLayoutWidth = 320
        button.bezelStyle = .push
        button.controlSize = .large
        if #available(macOS 26.0, *) { button.tintProminence = .primary }
        button.target = self
        button.action = #selector(act)
        spinner.style = .spinning
        spinner.controlSize = .small
        spinner.isDisplayedWhenStopped = false
        let stack = NSStackView(views: [icon, title, message, spinner, button])
        stack.orientation = .vertical
        stack.alignment = .centerX
        stack.spacing = 8
        stack.setCustomSpacing(14, after: icon)
        stack.setCustomSpacing(16, after: message)
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        NSLayoutConstraint.activate([
            stack.centerXAnchor.constraint(equalTo: centerXAnchor),
            stack.centerYAnchor.constraint(equalTo: centerYAnchor, constant: -16),
            stack.leadingAnchor.constraint(greaterThanOrEqualTo: leadingAnchor, constant: 24),
            stack.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -24),
            message.widthAnchor.constraint(lessThanOrEqualToConstant: 320),
        ])
    }

    required init?(coder: NSCoder) { fatalError() }

    func show(symbol: String?, title: String, message: String? = nil, action: String? = nil, busy: Bool = false) {
        icon.image = symbol.flatMap { NSImage(systemSymbolName: $0, accessibilityDescription: nil) }
        icon.isHidden = symbol == nil
        self.title.stringValue = title
        self.message.stringValue = message ?? ""
        self.message.isHidden = message == nil
        button.title = action ?? ""
        button.isHidden = action == nil || busy
        if busy { spinner.startAnimation(nil) } else { spinner.stopAnimation(nil) }
    }

    @objc private func act() { onAction() }
}

/// A filled line over the last samples.
final class SparklineView: NSView {
    var values: [Double] = [] { didSet { needsDisplay = true } }
    var maxValue: Double = 100 { didSet { needsDisplay = true } }
    var color: NSColor = .systemBlue { didSet { needsDisplay = true } }
    var capacity = 60

    override func draw(_ dirtyRect: NSRect) {
        let vals = Array(values.suffix(capacity))
        guard vals.count > 1 else { return }
        let top = max(maxValue, vals.max() ?? 0, 0.0001)
        let step = bounds.width / CGFloat(capacity - 1)
        let start = bounds.width - step * CGFloat(vals.count - 1)
        let pts = vals.enumerated().map { i, v in
            NSPoint(x: start + CGFloat(i) * step, y: 1 + CGFloat(v / top) * (bounds.height - 2))
        }
        let area = NSBezierPath()
        area.move(to: NSPoint(x: pts[0].x, y: 0))
        pts.forEach { area.line(to: $0) }
        area.line(to: NSPoint(x: pts.last!.x, y: 0))
        area.close()
        NSGradient(starting: color.withAlphaComponent(0.3), ending: color.withAlphaComponent(0.02))?
            .draw(in: area, angle: -90)
        let line = NSBezierPath()
        line.lineWidth = 1.5
        line.lineJoinStyle = .round
        line.lineCapStyle = .round
        line.move(to: pts[0])
        pts.dropFirst().forEach { line.line(to: $0) }
        color.setStroke()
        line.stroke()
    }

    override func viewDidChangeEffectiveAppearance() { needsDisplay = true }
}

/// A rounded card: title, a big value, a detail line and a sparkline.
@MainActor
final class MetricCard: NSBox {
    let titleLabel = label(font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .medium), color: .secondaryLabelColor)
    let valueLabel = label(font: .monospacedDigitSystemFont(ofSize: 20, weight: .semibold), color: .labelColor)
    let detailLabel = label(font: .monospacedDigitSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular),
                            color: .secondaryLabelColor)
    let spark = SparklineView()

    init(_ title: String, symbol: String, color: NSColor) {
        super.init(frame: .zero)
        boxType = .custom
        borderWidth = 1
        borderColor = .separatorColor
        fillColor = .quinarySystemFill
        cornerRadius = FormLayout.cornerRadius
        contentViewMargins = .zero
        titlePosition = .noTitle
        translatesAutoresizingMaskIntoConstraints = false
        titleLabel.stringValue = title
        spark.color = color
        spark.translatesAutoresizingMaskIntoConstraints = false
        let icon = NSImageView(image: NSImage(systemSymbolName: symbol, accessibilityDescription: nil) ?? NSImage())
        icon.contentTintColor = color
        icon.symbolConfiguration = .init(pointSize: 11, weight: .semibold)
        let head = NSStackView(views: [icon, titleLabel])
        head.spacing = 5
        let content = NSView()
        for v in [head, valueLabel, detailLabel, spark] as [NSView] {
            v.translatesAutoresizingMaskIntoConstraints = false
            content.addSubview(v)
        }
        NSLayoutConstraint.activate([
            head.leadingAnchor.constraint(equalTo: content.leadingAnchor, constant: 14),
            head.topAnchor.constraint(equalTo: content.topAnchor, constant: 12),
            valueLabel.leadingAnchor.constraint(equalTo: head.leadingAnchor),
            valueLabel.topAnchor.constraint(equalTo: head.bottomAnchor, constant: 6),
            valueLabel.trailingAnchor.constraint(lessThanOrEqualTo: content.trailingAnchor, constant: -14),
            detailLabel.leadingAnchor.constraint(equalTo: head.leadingAnchor),
            detailLabel.topAnchor.constraint(equalTo: valueLabel.bottomAnchor, constant: 2),
            detailLabel.trailingAnchor.constraint(lessThanOrEqualTo: content.trailingAnchor, constant: -14),
            spark.leadingAnchor.constraint(equalTo: content.leadingAnchor, constant: 1),
            spark.trailingAnchor.constraint(equalTo: content.trailingAnchor, constant: -1),
            spark.topAnchor.constraint(equalTo: detailLabel.bottomAnchor, constant: 10),
            spark.heightAnchor.constraint(equalToConstant: 44),
            spark.bottomAnchor.constraint(equalTo: content.bottomAnchor, constant: -1),
        ])
        contentView = content
        spark.wantsLayer = true
        spark.layer?.cornerRadius = FormLayout.cornerRadius - 1
        spark.layer?.maskedCorners = [.layerMinXMinYCorner, .layerMaxXMinYCorner]
    }

    required init?(coder: NSCoder) { fatalError() }
}

/// A row of a details group: a fixed-width key and a wrapping, selectable value
/// (or any view).
@MainActor
final class InfoRow: NSView {
    static let keyWidth: CGFloat = 112

    convenience init(_ key: String, _ value: String, mono: Bool = false) {
        let v = NSTextField(wrappingLabelWithString: value)
        v.isSelectable = true
        v.font = mono ? .monospacedSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular)
            : .systemFont(ofSize: NSFont.systemFontSize)
        v.textColor = .labelColor
        self.init(key, view: v)
    }

    init(_ key: String, view: NSView, keyWidth: CGFloat = 112) {
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        let k = NSTextField(labelWithString: key)
        k.textColor = .secondaryLabelColor
        k.font = .systemFont(ofSize: NSFont.systemFontSize)
        k.lineBreakMode = .byTruncatingMiddle
        k.toolTip = key
        for v in [k, view] {
            v.translatesAutoresizingMaskIntoConstraints = false
            addSubview(v)
        }
        view.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        if let t = view as? NSTextField { t.preferredMaxLayoutWidth = 0 }
        let inset = FormLayout.rowInset
        NSLayoutConstraint.activate([
            k.leadingAnchor.constraint(equalTo: leadingAnchor, constant: inset),
            k.widthAnchor.constraint(equalToConstant: keyWidth),
            k.firstBaselineAnchor.constraint(equalTo: view.firstBaselineAnchor),
            view.leadingAnchor.constraint(equalTo: k.trailingAnchor, constant: 12),
            view.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -inset),
            view.topAnchor.constraint(equalTo: topAnchor, constant: 9),
            view.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -9),
            heightAnchor.constraint(greaterThanOrEqualToConstant: 34),
        ])
    }

    required init?(coder: NSCoder) { fatalError() }
}

/// A link-styled button: ports open in the browser, paths in Finder.
@MainActor
func linkButton(_ title: String, target: AnyObject?, action: Selector, tag: Int = 0) -> NSButton {
    let b = NSButton(title: title, target: target, action: action)
    b.isBordered = false
    b.tag = tag
    b.attributedTitle = NSAttributedString(string: title, attributes: [
        .foregroundColor: NSColor.linkColor, .font: NSFont.systemFont(ofSize: NSFont.systemFontSize),
    ])
    b.setContentHuggingPriority(.required, for: .horizontal)
    return b
}

/// A small borderless symbol button for list rows.
@MainActor
func rowButton(_ symbol: String, _ tip: String, target: AnyObject?, action: Selector) -> NSButton {
    let b = NSButton(image: NSImage(systemSymbolName: symbol, accessibilityDescription: tip) ?? NSImage(),
                     target: target, action: action)
    b.isBordered = false
    b.bezelStyle = .accessoryBarAction
    b.symbolConfiguration = .init(pointSize: 12, weight: .medium)
    b.contentTintColor = .secondaryLabelColor
    b.toolTip = tip
    b.setButtonType(.momentaryChange)
    b.translatesAutoresizingMaskIntoConstraints = false
    b.widthAnchor.constraint(equalToConstant: 24).isActive = true
    b.heightAnchor.constraint(equalToConstant: 24).isActive = true
    return b
}

/// A group header for list sections ("Running", "In Use").
@MainActor
func sectionHeader(_ title: String) -> NSTableCellView {
    let cell = NSTableCellView()
    let t = label(title, font: .systemFont(ofSize: NSFont.smallSystemFontSize, weight: .semibold), color: .secondaryLabelColor)
    cell.addSubview(t)
    cell.textField = t
    NSLayoutConstraint.activate([
        t.leadingAnchor.constraint(equalTo: cell.leadingAnchor, constant: 6),
        t.trailingAnchor.constraint(lessThanOrEqualTo: cell.trailingAnchor),
        t.bottomAnchor.constraint(equalTo: cell.bottomAnchor, constant: -4),
    ])
    return cell
}

func bytes(_ n: Int64) -> String {
    let f = ByteCountFormatter()
    f.countStyle = .file
    f.allowsNonnumericFormatting = false
    return f.string(fromByteCount: n)
}

func rate(_ bytesPerSecond: Double) -> String {
    guard bytesPerSecond.isFinite, bytesPerSecond > 0 else { return bytes(0) + "/s" }
    // Double(Int64.max) rounds up to 2^63, which traps Int64(...), so clamp strictly below 2^63.
    let clamped = min(bytesPerSecond, Double(Int64.max - 1024))
    return bytes(Int64(clamped)) + "/s"
}

func relative(_ date: Date?) -> String {
    guard let date else { return "-" }
    let f = RelativeDateTimeFormatter()
    f.unitsStyle = .full
    return f.localizedString(for: date, relativeTo: Date())
}

func absolute(_ date: Date?) -> String {
    guard let date else { return "-" }
    return DateFormatter.localizedString(from: date, dateStyle: .medium, timeStyle: .short)
}

/// Ask before something that can't be undone, as a sheet on `window`.
@MainActor
func confirm(_ window: NSWindow?, title: String, message: String, button: String, then: @escaping () -> Void) {
    let alert = NSAlert()
    alert.messageText = title
    alert.informativeText = message
    alert.alertStyle = .warning
    alert.addButton(withTitle: button).hasDestructiveAction = true
    alert.addButton(withTitle: "Cancel")
    if let window {
        alert.beginSheetModal(for: window) { if $0 == .alertFirstButtonReturn { then() } }
    } else if alert.runModal() == .alertFirstButtonReturn {
        then()
    }
}
