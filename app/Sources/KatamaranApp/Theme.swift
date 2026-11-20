import AppKit

extension NSFont {
    /// Numbers and logs use the system monospaced font.
    static func mono(_ size: CGFloat, _ weight: NSFont.Weight = .regular) -> NSFont {
        .monospacedSystemFont(ofSize: size, weight: weight)
    }
}

func label(_ text: String = "", font: NSFont = .systemFont(ofSize: 12), color: NSColor? = nil) -> NSTextField {
    let l = NSTextField(labelWithString: text)
    l.font = font
    if let color { l.textColor = color }
    l.lineBreakMode = .byTruncatingTail
    l.translatesAutoresizingMaskIntoConstraints = false
    return l
}

/// A filled dot for menu items (container state).
func dotImage(_ color: NSColor, size: CGFloat = 8) -> NSImage {
    let img = NSImage(size: NSSize(width: size, height: size), flipped: false) { r in
        color.setFill()
        NSBezierPath(ovalIn: r.insetBy(dx: 0.5, dy: 0.5)).fill()
        return true
    }
    img.isTemplate = false
    return img
}

func mib(_ v: UInt64) -> String {
    v >= 10240 ? String(format: "%.1f GiB", Double(v) / 1024) : "\(v) MiB"
}

func gib(_ mib: Int) -> String {
    mib % 1024 == 0 ? "\(mib / 1024) GiB" : String(format: "%.1f GiB", Double(mib) / 1024)
}

func duration(_ secs: Double) -> String {
    let s = Int(secs)
    if s < 60 { return "\(s)s" }
    if s < 3600 { return "\(s / 60)m" }
    return "\(s / 3600)h \((s % 3600) / 60)m"
}
