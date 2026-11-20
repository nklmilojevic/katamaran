import AppKit
import CoreText
import Metal
import CKatTerm

/// One positioned glyph of a shaped row.
struct ShapedGlyph {
    /// Grid column of the glyph's cluster.
    var col: Int
    /// Pixel offset from the column's left edge (non zero for ligature pieces and marks).
    var x: Float
    /// Pixel offset from the baseline, positive upwards.
    var y: Float
    var font: Int
    var glyph: CGGlyph
    var color: Bool
}

/// Cache key for shaped rows: per cell scalar + style bits, plus grapheme clusters.
struct ShapeKey: Hashable {
    var cells: [UInt32]
    var clusters: [Int: String]
}

/// The terminal font: a family by name, or nil for the system's monospaced font.
struct TerminalFont: Equatable {
    var family: String?
    var size: CGFloat

    /// Points; the app's monospaced text uses the same.
    static var current = TerminalFont(
        family: UserDefaults.standard.string(forKey: "terminalFontFamily"),
        size: CGFloat(UserDefaults.standard.double(forKey: "terminalFontSize")).nonZero ?? 12)
}

private extension CGFloat {
    var nonZero: CGFloat? { self > 0 ? self : nil }
}

/// Fonts, cell metrics, shaping (CoreText, with ligatures and fallback) and the glyph atlas for
/// one backing scale factor. All metrics are in device pixels. Adapted from Thurm.
final class FontShaper {
    // MARK: Shared instances (one per font and backing scale)

    private struct InstanceKey: Hashable {
        var family: String?
        var size: CGFloat
        var scale: CGFloat
    }

    private static var instances: [InstanceKey: FontShaper] = [:]

    static func shared(scale: CGFloat, font: TerminalFont = .current) -> FontShaper? {
        let key = InstanceKey(family: font.family, size: font.size, scale: max(1, scale))
        if let existing = instances[key] { return existing }
        guard let ctx = MetalContext.shared,
              let shaper = FontShaper(font: font, scale: key.scale, device: ctx.device)
        else { return nil }
        instances[key] = shaper
        return shaper
    }

    // MARK: Properties

    let scale: CGFloat
    /// regular, bold, italic, bold-italic.
    let styleFonts: [CTFont]
    let cellWidth: Int
    let cellHeight: Int
    /// Distance from the cell top to the baseline.
    let baseline: Int
    /// Underline offset below the baseline and thickness.
    let underlineOffset: Int
    let lineThickness: Int
    /// Strikeout offset above the baseline.
    let strikeOffset: Int
    let glyphs: GlyphCache

    private var fontList: [CTFont] = []
    private var fontIndex: [String: Int] = [:]
    /// Shaped rows, two generations: when the current one fills up it becomes the old one,
    /// and rows still in use move back on their next hit (an LRU without the bookkeeping).
    private var rowCache: [ShapeKey: [ShapedGlyph]] = [:]
    private var oldRowCache: [ShapeKey: [ShapedGlyph]] = [:]
    private static let rowCacheGeneration = 4096
    private var boxCache: [UInt32: [BoxRect]] = [:]
    private let fontAttributeKey = NSAttributedString.Key(rawValue: kCTFontAttributeName as String)

    init?(font: TerminalFont, scale: CGFloat, device: MTLDevice) {
        guard let cache = GlyphCache(device: device, thicken: nil) else { return nil }
        self.glyphs = cache
        self.scale = scale
        let pixelSize = max(4, font.size) * scale

        let regular: CTFont
        let bold: CTFont
        if let family = font.family, let named = NSFont(name: family, size: pixelSize)
            ?? NSFontManager.shared.font(withFamily: family, traits: [], weight: 5, size: pixelSize) {
            regular = named as CTFont
            bold = FontShaper.variant(of: regular, traits: .traitBold, size: pixelSize)
        } else {
            regular = NSFont.monospacedSystemFont(ofSize: pixelSize, weight: .regular) as CTFont
            bold = NSFont.monospacedSystemFont(ofSize: pixelSize, weight: .bold) as CTFont
        }
        let italic = FontShaper.variant(of: regular, traits: .traitItalic, size: pixelSize)
        let boldItalic = FontShaper.variant(of: bold, traits: .traitItalic, size: pixelSize)
        styleFonts = [regular, bold, italic, boldItalic]

        // Cell metrics from the primary font.
        let ascent = CTFontGetAscent(regular)
        let descent = CTFontGetDescent(regular)
        let leading = CTFontGetLeading(regular)
        var chars: [UniChar] = [0x4D] // "M"
        var mGlyph: [CGGlyph] = [0]
        var advance: CGFloat = pixelSize * 0.6
        if CTFontGetGlyphsForCharacters(regular, &chars, &mGlyph, 1) {
            var size = CGSize.zero
            advance = CGFloat(CTFontGetAdvancesForGlyphs(regular, .horizontal, &mGlyph, &size, 1))
        }
        cellWidth = max(1, Int(advance.rounded()))
        let natural = ascent + descent + leading
        // A little more air between lines than the font's own, like Terminal.app.
        cellHeight = max(1, Int(ceil(ceil(natural) * 1.1)))
        let topPad = (CGFloat(cellHeight) - natural) / 2
        baseline = max(1, min(cellHeight - 1, Int((topPad + leading / 2 + ascent).rounded())))

        let thickness = max(1, Int(CTFontGetUnderlineThickness(regular).rounded()))
        lineThickness = thickness
        var ulOffset = max(1, Int((-CTFontGetUnderlinePosition(regular)).rounded()))
        if baseline + ulOffset + thickness > cellHeight {
            ulOffset = max(0, cellHeight - baseline - thickness)
        }
        underlineOffset = ulOffset
        strikeOffset = max(1, Int((CTFontGetXHeight(regular) / 2).rounded()))
    }

    /// The traits that pick a style face: italic alone must not come out bold.
    private static let styleTraits: CTFontSymbolicTraits = [.traitBold, .traitItalic]

    private static func variant(of base: CTFont, traits: CTFontSymbolicTraits, size: CGFloat) -> CTFont {
        let descriptor = CTFontCopyFontDescriptor(base)
        let wanted = CTFontGetSymbolicTraits(base).intersection(styleTraits).union(traits)
        if let desc = CTFontDescriptorCreateCopyWithSymbolicTraits(descriptor, wanted, styleTraits) {
            let font = CTFontCreateWithFontDescriptor(desc, size, nil)
            if CTFontGetSymbolicTraits(font).contains(traits) { return font }
        }
        if let font = CTFontCreateCopyWithSymbolicTraits(base, size, nil, wanted, styleTraits) {
            return font
        }
        return base
    }

    // MARK: Font registry

    func fontId(_ font: CTFont) -> Int {
        let name = CTFontCopyPostScriptName(font) as String
        let key = "\(name)|\(CTFontGetSize(font))"
        if let id = fontIndex[key] { return id }
        fontList.append(font)
        let id = fontList.count - 1
        fontIndex[key] = id
        return id
    }

    func font(_ id: Int) -> CTFont {
        (id >= 0 && id < fontList.count) ? fontList[id] : styleFonts[0]
    }

    // MARK: Box drawing

    func boxRects(for ch: UInt32) -> [BoxRect] {
        if let cached = boxCache[ch] { return cached }
        let rects = BoxDrawing.rects(for: ch, width: cellWidth, height: cellHeight, light: lineThickness)
        boxCache[ch] = rects
        return rects
    }

    // MARK: Shaping

    /// The scalar that takes part in shaping for `cell`; 32 for anything drawn as blank
    /// (empty, hidden, control characters, procedural box drawing).
    static func shapedScalar(_ cell: kat_cell) -> UInt32 {
        let ch = cell.ch
        if ch <= 32 || ch == 0x7F || cell.flags & CellFlag.hidden != 0 { return 32 }
        if ch > 0x10FFFF || (ch >= 0xD800 && ch <= 0xDFFF) { return 32 }
        if BoxDrawing.isProcedural(ch) { return 32 }
        return ch
    }

    /// Shapes one grid row. Cached by content, so unchanged rows are not reshaped.
    func shapeRow(_ cells: UnsafeBufferPointer<kat_cell>, clusters: [Int: String]) -> [ShapedGlyph] {
        let cols = cells.count
        var key: [UInt32] = []
        key.reserveCapacity(cols)
        var anyText = !clusters.isEmpty
        for c in 0..<cols {
            let cell = cells[c]
            if cell.flags & CellFlag.wideSpacer != 0 {
                key.append(0xFFFF_FFFF)
                continue
            }
            let scalar = FontShaper.shapedScalar(cell)
            if scalar != 32 { anyText = true }
            let style = UInt32(cell.flags & 3)
            let wide: UInt32 = (cell.flags & CellFlag.wide) != 0 ? (1 << 23) : 0
            key.append(scalar | (style << 21) | wide)
        }
        if !anyText { return [] }
        let shapeKey = ShapeKey(cells: key, clusters: clusters)
        if let hit = rowCache[shapeKey] { return hit }
        if let hit = oldRowCache.removeValue(forKey: shapeKey) {
            storeRow(shapeKey, hit)
            return hit
        }

        var out: [ShapedGlyph] = []
        var units: [UniChar] = []
        var map: [Int] = []
        var runStyle = -1
        units.reserveCapacity(cols + 8)
        map.reserveCapacity(cols + 8)

        for c in 0..<cols {
            let cell = cells[c]
            if cell.flags & CellFlag.wideSpacer != 0 { continue }
            let scalar = FontShaper.shapedScalar(cell)
            let cluster = clusters[c]
            if scalar == 32 && cluster == nil {
                // Spaces never break a style run.
                units.append(32)
                map.append(c)
                continue
            }
            let style = Int(cell.flags & 3)
            if style != runStyle {
                if runStyle >= 0 { shapeRun(units, map, style: runStyle, into: &out) }
                units.removeAll(keepingCapacity: true)
                map.removeAll(keepingCapacity: true)
                runStyle = style
            }
            if let cluster = cluster {
                for u in cluster.utf16 {
                    units.append(u)
                    map.append(c)
                }
            } else if let us = Unicode.Scalar(scalar) {
                for u in String(Character(us)).utf16 {
                    units.append(u)
                    map.append(c)
                }
            }
        }
        if runStyle >= 0 { shapeRun(units, map, style: runStyle, into: &out) }

        storeRow(shapeKey, out)
        return out
    }

    private func storeRow(_ key: ShapeKey, _ glyphs: [ShapedGlyph]) {
        if rowCache.count >= FontShaper.rowCacheGeneration {
            oldRowCache = rowCache
            rowCache = [:]
        }
        rowCache[key] = glyphs
    }

    /// Shapes one run of identically styled text. `map[i]` is the column of UTF-16 unit `i`.
    private func shapeRun(_ units: [UniChar], _ map: [Int], style: Int, into out: inout [ShapedGlyph]) {
        guard !units.isEmpty, units.contains(where: { $0 != 32 }) else { return }
        let baseFont = styleFonts[max(0, min(3, style))]
        let string = String(utf16CodeUnits: units, count: units.count)
        let attributed = NSAttributedString(string: string, attributes: [fontAttributeKey: baseFont])
        let line = CTLineCreateWithAttributedString(attributed as CFAttributedString)
        let runs = CTLineGetGlyphRuns(line) as! [CTRun]

        // Pen x of the first glyph of each column's cluster, so that every glyph is positioned
        // relative to its own cell and the grid never drifts.
        var clusterStart: [Int: CGFloat] = [:]
        for run in runs {
            let count = CTRunGetGlyphCount(run)
            if count <= 0 { continue }
            var runFont = baseFont
            let attrs = CTRunGetAttributes(run) as NSDictionary
            if let value = attrs[kCTFontAttributeName as String] {
                runFont = value as! CTFont
            }
            let fid = fontId(runFont)
            let isColor = CTFontGetSymbolicTraits(runFont).contains(.traitColorGlyphs)

            var glyphIds = [CGGlyph](repeating: 0, count: count)
            var positions = [CGPoint](repeating: .zero, count: count)
            var indices = [CFIndex](repeating: 0, count: count)
            let all = CFRange(location: 0, length: 0)
            CTRunGetGlyphs(run, all, &glyphIds)
            CTRunGetPositions(run, all, &positions)
            CTRunGetStringIndices(run, all, &indices)

            for i in 0..<count {
                let si = indices[i]
                guard si >= 0, si < map.count else { continue }
                if units[si] == 32 { continue }
                let col = map[si]
                let p = positions[i]
                let startX: CGFloat
                if let s = clusterStart[col] {
                    startX = s
                } else {
                    clusterStart[col] = p.x
                    startX = p.x
                }
                out.append(ShapedGlyph(col: col, x: Float(p.x - startX), y: Float(p.y),
                                       font: fid, glyph: glyphIds[i], color: isColor))
            }
        }
    }
}
