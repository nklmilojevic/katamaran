import AppKit
import CKatTerm

/// The terminal engine (crates/kat-term) for one shell. Thread-safe: output is fed from the
/// session's reader thread, everything else happens on the main thread.
final class KatTerminal: @unchecked Sendable {
    let handle: OpaquePointer

    init(cols: Int, rows: Int, scrollback: Int = 10_000) {
        handle = kat_term_new(UInt16(clamping: cols), UInt16(clamping: rows), UInt32(scrollback))
    }

    deinit { kat_term_free(handle) }

    /// Takes ownership of bytes the library returned.
    static func take(_ b: kat_bytes) -> Data {
        guard let p = b.ptr, b.len > 0 else { return Data() }
        let d = Data(bytes: p, count: b.len)
        kat_bytes_free(b)
        return d
    }

    func feed(_ data: Data) {
        data.withUnsafeBytes { raw in
            kat_term_feed(handle, raw.bindMemory(to: UInt8.self).baseAddress, raw.count)
        }
    }

    func takeReplies() -> Data { Self.take(kat_term_take_replies(handle)) }
    func takeBell() -> Bool { kat_term_take_bell(handle) }

    /// The title the program set, when it changed.
    func titleChange() -> String? {
        var changed = false
        let t = String(decoding: Self.take(kat_term_title(handle, &changed)), as: UTF8.self)
        return changed ? t : nil
    }

    func resize(cols: Int, rows: Int, cellWidth: Int, cellHeight: Int) {
        kat_term_resize(handle, UInt16(clamping: cols), UInt16(clamping: rows),
                        UInt32(clamping: cellWidth), UInt32(clamping: cellHeight))
    }

    func setTheme(_ theme: TerminalTheme) {
        var t = theme.raw
        kat_term_set_theme(handle, &t)
    }

    func key(kind: UInt32, code: UInt32, mods: UInt8, action: UInt8, text: String?, baseLayout: UInt32) -> Data {
        func encode(_ ptr: UnsafePointer<CChar>?) -> Data {
            var ev = kat_key_event(kind: kind, code: code, mods: mods, action: action, text: ptr,
                                   base_layout: baseLayout)
            return Self.take(kat_term_key(handle, &ev))
        }
        if let text { return text.withCString { encode($0) } }
        return encode(nil)
    }

    func paste(_ text: String) -> Data { text.withCString { Self.take(kat_term_paste(handle, $0)) } }
    func focus(_ focused: Bool) -> Data { Self.take(kat_term_focus(handle, focused)) }

    func mouse(_ ev: kat_mouse_event) -> (outcome: UInt8, bytes: Data) {
        var e = ev
        var out = kat_bytes()
        let r = kat_term_mouse(handle, &e, &out)
        return (r, Self.take(out))
    }

    func wheel(lines: Int32, col: UInt16, row: UInt16, mods: UInt8) -> Data {
        Self.take(kat_term_wheel(handle, lines, col, row, mods))
    }

    func scroll(_ kind: Int32, _ n: Int32 = 0) { kat_term_scroll(handle, UInt8(kind), n) }
    func scrollToBottom() { scroll(Int32(KAT_SCROLL_BOTTOM)) }

    var selectionText: String { String(decoding: Self.take(kat_term_selection_text(handle)), as: UTF8.self) }
    var hasSelection: Bool { kat_term_has_selection(handle) }
    func selectAll() { kat_term_select_all(handle) }
    @discardableResult func clearSelection() -> Bool { kat_term_clear_selection(handle) }
    var screenText: String { String(decoding: Self.take(kat_term_screen_text(handle)), as: UTF8.self) }
    func reset() { kat_term_reset(handle) }
}

/// Terminal colors for the app's light and dark appearance.
struct TerminalTheme: Equatable {
    var foreground: UInt32
    var background: UInt32
    var cursor: UInt32
    var cursorText: UInt32
    var selectionFg: UInt32
    var selectionBg: UInt32
    var palette: [UInt32]
    var dark: Bool

    static let dark = TerminalTheme(
        foreground: 0xDCDFE4, background: 0x1F2023, cursor: 0xDCDFE4, cursorText: 0x1F2023,
        selectionFg: 0xFFFFFF, selectionBg: 0x3E5A85,
        palette: [0x2C2E33, 0xE06C75, 0x98C379, 0xE5C07B, 0x61AFEF, 0xC678DD, 0x56B6C2, 0xDCDFE4,
                  0x5C6370, 0xF2777A, 0xB5E08F, 0xF4D58D, 0x82C4FF, 0xDA9AEB, 0x7FD3DD, 0xFFFFFF],
        dark: true)

    static let light = TerminalTheme(
        foreground: 0x24292F, background: 0xFFFFFF, cursor: 0x24292F, cursorText: 0xFFFFFF,
        selectionFg: 0x24292F, selectionBg: 0xB6D7FF,
        palette: [0x24292F, 0xCF222E, 0x116329, 0x7D4E00, 0x0550AE, 0x8250DF, 0x1B7C83, 0x6E7781,
                  0x57606A, 0xA40E26, 0x1A7F37, 0x633C01, 0x218BFF, 0xA475F9, 0x3192AA, 0x8C959F],
        dark: false)

    static func forAppearance(_ appearance: NSAppearance) -> TerminalTheme {
        appearance.bestMatch(from: [.darkAqua, .aqua]) == .darkAqua ? .dark : .light
    }

    var raw: kat_theme {
        var t = kat_theme()
        t.foreground = foreground
        t.background = background
        t.cursor = cursor
        t.cursor_text = cursorText
        t.selection_fg = selectionFg
        t.selection_bg = selectionBg
        withUnsafeMutableBytes(of: &t.palette) { raw in
            let p = raw.bindMemory(to: UInt32.self)
            for (i, c) in palette.prefix(16).enumerated() { p[i] = c }
        }
        t.dark = dark
        return t
    }
}
