import AppKit

/// Grouped rows in the style of System Settings: a rounded group on a faint
/// system fill, each row with its title on the left and its control on the
/// right, an optional header above and footnote below.
@MainActor
enum FormLayout {
    /// The pane's content width; the window adds `margin` on both sides.
    static let width: CGFloat = 480
    static let margin: CGFloat = 20
    static let rowInset: CGFloat = 12
    static let rowHeight: CGFloat = 40
    static let cornerRadius: CGFloat = 12
}

@MainActor
final class FormRow: NSView {
    let titleLabel: NSTextField
    let subtitleLabel: NSTextField

    /// `control` sits on the trailing edge; `icon` before the title.
    init(_ title: String, subtitle: String? = nil, icon: NSImageView? = nil, control: NSView) {
        titleLabel = NSTextField(labelWithString: title)
        subtitleLabel = NSTextField(wrappingLabelWithString: subtitle ?? "")
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = false
        titleLabel.font = .systemFont(ofSize: NSFont.systemFontSize)
        titleLabel.textColor = .labelColor
        titleLabel.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        subtitleLabel.font = .systemFont(ofSize: NSFont.smallSystemFontSize)
        subtitleLabel.textColor = .secondaryLabelColor
        subtitleLabel.isHidden = subtitle == nil
        subtitleLabel.isSelectable = false

        let text = NSStackView(views: [titleLabel, subtitleLabel])
        text.orientation = .vertical
        text.alignment = .leading
        text.spacing = 2

        let lead = NSStackView(views: [icon, text].compactMap { $0 })
        lead.orientation = .horizontal
        lead.alignment = .centerY
        lead.spacing = 10
        for v in [lead, control] {
            v.translatesAutoresizingMaskIntoConstraints = false
            addSubview(v)
        }
        control.setContentHuggingPriority(.required, for: .horizontal)
        control.setContentCompressionResistancePriority(.required, for: .horizontal)
        let inset = FormLayout.rowInset
        NSLayoutConstraint.activate([
            lead.leadingAnchor.constraint(equalTo: leadingAnchor, constant: inset),
            lead.trailingAnchor.constraint(lessThanOrEqualTo: control.leadingAnchor, constant: -16),
            control.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -inset),
            lead.centerYAnchor.constraint(equalTo: centerYAnchor),
            control.centerYAnchor.constraint(equalTo: centerYAnchor),
            lead.topAnchor.constraint(greaterThanOrEqualTo: topAnchor, constant: 8),
            lead.bottomAnchor.constraint(lessThanOrEqualTo: bottomAnchor, constant: -8),
            control.topAnchor.constraint(greaterThanOrEqualTo: topAnchor, constant: 6),
            control.bottomAnchor.constraint(lessThanOrEqualTo: bottomAnchor, constant: -6),
            heightAnchor.constraint(greaterThanOrEqualToConstant: FormLayout.rowHeight),
        ])
        // Wrap long subtitles instead of widening the window.
        subtitleLabel.preferredMaxLayoutWidth = FormLayout.width - 2 * inset - 200
    }

    required init?(coder: NSCoder) { fatalError() }

    var subtitle: String? {
        get { subtitleLabel.isHidden ? nil : subtitleLabel.stringValue }
        set {
            subtitleLabel.stringValue = newValue ?? ""
            subtitleLabel.isHidden = newValue == nil
        }
    }
}

/// One rounded group of rows, with separators between them.
@MainActor
final class FormGroup: NSStackView {
    private let box = NSBox()
    private let rowStack = NSStackView()
    private let footerLabel = NSTextField(wrappingLabelWithString: "")

    init(header: String? = nil, footer: String? = nil, rows: [NSView]) {
        super.init(frame: .zero)
        orientation = .vertical
        alignment = .leading
        spacing = 6
        translatesAutoresizingMaskIntoConstraints = false

        if let header {
            let h = NSTextField(labelWithString: header)
            h.font = .boldSystemFont(ofSize: NSFont.systemFontSize)
            h.textColor = .labelColor
            addArrangedSubview(h)
            setCustomSpacing(8, after: h)
        }

        box.boxType = .custom
        box.borderWidth = 1
        box.borderColor = .separatorColor
        box.fillColor = .quinarySystemFill
        box.cornerRadius = FormLayout.cornerRadius
        box.contentViewMargins = .zero
        box.titlePosition = .noTitle
        box.translatesAutoresizingMaskIntoConstraints = false

        rowStack.orientation = .vertical
        rowStack.alignment = .leading
        rowStack.spacing = 0
        rowStack.translatesAutoresizingMaskIntoConstraints = false
        for (i, row) in rows.enumerated() {
            for v in i > 0 ? [separator(), row] : [row] {
                rowStack.addArrangedSubview(v)
                v.widthAnchor.constraint(equalTo: rowStack.widthAnchor).isActive = true
            }
        }
        box.contentView = rowStack
        addArrangedSubview(box)
        NSLayoutConstraint.activate([
            box.widthAnchor.constraint(equalTo: widthAnchor),
            rowStack.leadingAnchor.constraint(equalTo: box.leadingAnchor),
            rowStack.trailingAnchor.constraint(equalTo: box.trailingAnchor),
            rowStack.topAnchor.constraint(equalTo: box.topAnchor),
            rowStack.bottomAnchor.constraint(equalTo: box.bottomAnchor),
        ])

        footerLabel.font = .systemFont(ofSize: NSFont.smallSystemFontSize)
        footerLabel.textColor = .secondaryLabelColor
        footerLabel.isSelectable = false
        footerLabel.preferredMaxLayoutWidth = FormLayout.width - 2 * FormLayout.rowInset
        footerLabel.stringValue = footer ?? ""
        footerLabel.isHidden = footer == nil
        addArrangedSubview(footerLabel)
        // Footnotes line up with the row titles, as in System Settings.
        footerLabel.leadingAnchor.constraint(equalTo: leadingAnchor, constant: FormLayout.rowInset).isActive = true
    }

    required init(coder: NSCoder) { fatalError() }

    private func separator() -> NSView {
        let line = NSBox()
        line.boxType = .separator
        line.translatesAutoresizingMaskIntoConstraints = false
        let holder = NSView()
        holder.translatesAutoresizingMaskIntoConstraints = false
        holder.addSubview(line)
        NSLayoutConstraint.activate([
            line.leadingAnchor.constraint(equalTo: holder.leadingAnchor, constant: FormLayout.rowInset),
            line.trailingAnchor.constraint(equalTo: holder.trailingAnchor, constant: -FormLayout.rowInset),
            line.centerYAnchor.constraint(equalTo: holder.centerYAnchor),
            holder.heightAnchor.constraint(equalToConstant: 1),
        ])
        return holder
    }
}

/// A SF Symbol at row size, in a system color.
@MainActor
func symbolView(_ name: String, color: NSColor, size: CGFloat = 20) -> NSImageView {
    let config = NSImage.SymbolConfiguration(pointSize: size, weight: .regular)
        .applying(.init(paletteColors: [.white, color]))
    let image = NSImage(systemSymbolName: name, accessibilityDescription: nil)?.withSymbolConfiguration(config)
    let view = NSImageView(image: image ?? NSImage())
    view.translatesAutoresizingMaskIntoConstraints = false
    view.setContentHuggingPriority(.required, for: .horizontal)
    return view
}
