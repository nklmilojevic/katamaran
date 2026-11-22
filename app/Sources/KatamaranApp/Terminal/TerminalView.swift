import AppKit
import CKatTerm
import QuartzCore

/// A terminal grid on a Metal layer, with keyboard (input methods included), mouse, selection,
/// scrollback and the clipboard. Adapted from Thurm's TerminalView: the engine is local
/// (`KatTerminal`) and its input goes to `send`.
final class TerminalView: NSView, NSTextInputClient, NSMenuItemValidation {
    let term: KatTerminal
    /// Bytes for the program.
    var send: (Data) -> Void = { _ in }
    /// The grid got a new size (cols, rows).
    var onResize: (Int, Int) -> Void = { _, _ in }
    /// Keystroke-to-screen times (ms), for measuring input latency.
    var onLatency: ((Double) -> Void)?
    /// No program attached: keys go nowhere.
    var inputEnabled = true

    private let renderer = TerminalRenderer()
    private var shaper: FontShaper?
    private(set) var cols = 80
    private(set) var rows = 24
    private var lastCellSize: (width: Int, height: Int) = (0, 0)
    private var theme = TerminalTheme.dark
    private let renderSignal = RenderSignal()

    private var markedText = NSMutableAttributedString()
    private var markedLabel: NSTextField?
    /// Text input methods committed during the current `keyDown` (nil outside it).
    private var keyTextAccumulator: [String]?
    private var keyCommandIssued = false
    private var pressedButton: UInt8?
    private var rightPressReported = false
    private var scrollAccumulator: CGFloat = 0
    /// When the oldest key not yet answered on screen was pressed.
    private var keyTime: CFTimeInterval?

    static let padding = CGSize(width: 8, height: 6)

    init(term: KatTerminal) {
        self.term = term
        super.init(frame: NSRect(x: 0, y: 0, width: 640, height: 400))
        wantsLayer = true
        layerContentsRedrawPolicy = .never
        applyTheme()
        renderSignal.fire = { [weak self] in self?.renderNow() }
    }

    required init?(coder: NSCoder) { fatalError() }

    // MARK: Layer

    override func makeBackingLayer() -> CALayer {
        let layer = CAMetalLayer()
        layer.device = MetalContext.shared?.device
        layer.pixelFormat = .bgra8Unorm
        layer.framebufferOnly = true
        layer.isOpaque = true
        layer.colorspace = CGColorSpace(name: CGColorSpace.sRGB)
        layer.contentsScale = NSScreen.main?.backingScaleFactor ?? 2
        // While resizing, keep the last frame unscaled at the top left until the next one.
        layer.contentsGravity = .topLeft
        layer.needsDisplayOnBoundsChange = false
        return layer
    }

    private var metalLayer: CAMetalLayer? { layer as? CAMetalLayer }

    override var isFlipped: Bool { true }
    override var isOpaque: Bool { true }
    override var acceptsFirstResponder: Bool { true }

    private var backingScale: CGFloat {
        window?.backingScaleFactor ?? NSScreen.main?.backingScaleFactor ?? 2
    }

    private var padXPixels: Int { Int((Self.padding.width * backingScale).rounded()) }
    private var padYPixels: Int { Int((Self.padding.height * backingScale).rounded()) }

    private var cellSizePoints: CGSize {
        guard let s = shaper else { return CGSize(width: 7, height: 15) }
        return CGSize(width: CGFloat(s.cellWidth) / backingScale, height: CGFloat(s.cellHeight) / backingScale)
    }

    private var isKeyFocused: Bool {
        guard let window else { return false }
        return NSApp.isActive && window.isKeyWindow && window.firstResponder === self
    }

    private var modes: UInt32 { renderer.snapshot.info.modes }

    // MARK: Window attachment, size, appearance

    override func viewWillMove(toWindow newWindow: NSWindow?) {
        super.viewWillMove(toWindow: newWindow)
        let nc = NotificationCenter.default
        if let old = window {
            nc.removeObserver(self, name: NSWindow.didBecomeKeyNotification, object: old)
            nc.removeObserver(self, name: NSWindow.didResignKeyNotification, object: old)
        }
        if let newWindow {
            nc.addObserver(self, selector: #selector(windowKeyChanged), name: NSWindow.didBecomeKeyNotification,
                           object: newWindow)
            nc.addObserver(self, selector: #selector(windowKeyChanged), name: NSWindow.didResignKeyNotification,
                           object: newWindow)
        }
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        updateScale()
    }

    override func viewDidChangeBackingProperties() {
        super.viewDidChangeBackingProperties()
        updateScale()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        applyTheme()
        renderNow()
    }

    override func viewDidHide() {
        super.viewDidHide()
        markedLabel?.removeFromSuperview()
        markedLabel = nil
    }

    override func viewDidUnhide() {
        super.viewDidUnhide()
        renderNow(force: true)
    }

    @objc private func windowKeyChanged() { renderNow(force: true) }

    private func applyTheme() {
        let t = TerminalTheme.forAppearance(effectiveAppearance)
        theme = t
        term.setTheme(t)
        metalLayer?.backgroundColor = NSColor(rgb: t.background).cgColor
    }

    /// The terminal's background, for chrome drawn around it.
    var backgroundColor: NSColor { NSColor(rgb: theme.background) }

    override func setFrameSize(_ newSize: NSSize) {
        super.setFrameSize(newSize)
        updateDrawableSize()
        updateGridSize()
        renderNow(force: true)
    }

    private func updateScale() {
        guard window != nil else { return }
        metalLayer?.contentsScale = backingScale
        shaper = FontShaper.shared(scale: backingScale)
        updateDrawableSize()
        updateGridSize()
        renderNow(force: true)
    }

    /// The font changed: new metrics, new grid size.
    func fontChanged() {
        shaper = FontShaper.shared(scale: backingScale)
        updateGridSize()
        renderNow(force: true)
    }

    private func updateDrawableSize() {
        let s = backingScale
        metalLayer?.drawableSize = CGSize(width: max(1, (bounds.width * s).rounded()),
                                          height: max(1, (bounds.height * s).rounded()))
    }

    /// The grid that fits the view (also before it is in a window, from the main screen's scale).
    func fittingGrid() -> (cols: Int, rows: Int) {
        guard let s = shaper ?? FontShaper.shared(scale: backingScale) else { return (80, 24) }
        let w = bounds.width * backingScale - CGFloat(2 * padXPixels)
        let h = bounds.height * backingScale - CGFloat(2 * padYPixels)
        return (max(2, Int(w) / s.cellWidth), max(1, Int(h) / s.cellHeight))
    }

    private func updateGridSize() {
        guard let s = shaper, bounds.width > 0, bounds.height > 0 else { return }
        let (c, r) = fittingGrid()
        let cellSize = (width: s.cellWidth, height: s.cellHeight)
        guard c != cols || r != rows || renderer.snapshot.cols != c || renderer.snapshot.rows != r || cellSize != lastCellSize else { return }
        cols = c
        rows = r
        lastCellSize = cellSize
        term.resize(cols: c, rows: r, cellWidth: s.cellWidth, cellHeight: s.cellHeight)
        onResize(c, r)
    }

    // MARK: Rendering

    /// Output arrived (any thread): draw soon, once.
    func outputArrived() { renderSignal.signal() }

    /// `outputArrived` for the reader thread.
    var outputSignal: @Sendable () -> Void {
        let s = renderSignal
        return { s.signal() }
    }

    /// Draws when the grid changed (or always with `force`).
    func renderNow(force: Bool = false) {
        guard window != nil, !isHiddenOrHasHiddenAncestor, let layer = metalLayer,
              let shaper = shaper ?? FontShaper.shared(scale: backingScale)
        else { return }
        self.shaper = shaper
        let changed = renderer.snapshot.update(term.handle)
        guard changed || force else { return }
        let params = RenderParams(
            shaper: shaper, padX: padXPixels, padY: padYPixels,
            viewportWidth: Int(layer.drawableSize.width), viewportHeight: Int(layer.drawableSize.height),
            focused: isKeyFocused, cursorOn: true, themeBackground: theme.background)
        var presented: ((CFTimeInterval) -> Void)?
        if changed, let start = keyTime, let report = onLatency {
            keyTime = nil
            presented = { t in
                let ms = (t - start) * 1000
                DispatchQueue.main.async { report(ms) }
            }
        }
        renderer.draw(layer: layer, params: params, presented: presented)
        if markedText.length > 0 { updateMarkedTextOverlay() }
    }

    // MARK: Focus

    override func becomeFirstResponder() -> Bool {
        let ok = super.becomeFirstResponder()
        if ok {
            sendInput(term.focus(true))
            renderNow(force: true)
        }
        return ok
    }

    override func resignFirstResponder() -> Bool {
        let ok = super.resignFirstResponder()
        if ok {
            sendInput(term.focus(false))
            renderNow(force: true)
        }
        return ok
    }

    private func sendInput(_ data: Data) {
        guard inputEnabled, !data.isEmpty else { return }
        send(data)
    }

    // MARK: Keyboard

    private var optionIsAlt: Bool { UserDefaults.standard.bool(forKey: "terminalOptionAsAlt") }

    override func performKeyEquivalent(with event: NSEvent) -> Bool {
        guard event.type == .keyDown, window?.firstResponder === self else { return false }
        let flags = event.modifierFlags
        // Command shortcuts belong to the menu.
        if flags.contains(.command) { return false }
        // Control combinations (Ctrl+Tab, Ctrl+Return, ...) would otherwise be eaten by AppKit.
        if flags.contains(.control) {
            keyDown(with: event)
            return true
        }
        return false
    }

    override func keyDown(with event: NSEvent) {
        guard inputEnabled else { return }
        NSCursor.setHiddenUntilMouseMoves(true)
        if keyTime == nil { keyTime = CACurrentMediaTime() }
        let flags = event.modifierFlags
        let action = event.isARepeat ? KeyActionCode.repeating : KeyActionCode.press

        if flags.contains(.command) {
            // Not a menu shortcut: line editing as in other macOS terminals, else forward.
            if handleCommandFallback(event) { return }
            sendKey(event, action: action, text: nil, optionIsAlt: false)
            return
        }

        let optionAlt = KeyMapping.optionActsAsAlt(event, enabled: optionIsAlt)
        let hadMarked = hasMarkedText()

        if optionAlt && !hadMarked {
            // Option is Alt: send the unmodified character with the Alt modifier and skip
            // macOS composition (so Option+e does not start a dead key).
            var text: String? = nil
            if !flags.contains(.control),
               let t = event.characters(byApplyingModifiers: flags.intersection([.shift, .capsLock])),
               KeyMapping.isPrintable(t) {
                text = t
            }
            sendKey(event, action: action, text: text, optionIsAlt: true)
            return
        }

        keyTextAccumulator = []
        keyCommandIssued = false
        interpretKeyEvents([event])
        let committed = keyTextAccumulator ?? []
        keyTextAccumulator = nil

        if !committed.isEmpty {
            let text = committed.joined()
            if hadMarked {
                // Input method / dead key commit: plain text, not a key press.
                typeText(text)
                // Some input methods (Korean) commit and pass the key on too.
                if keyCommandIssued {
                    sendKey(event, action: action, text: nil, optionIsAlt: false)
                }
            } else {
                sendKey(event, action: action, text: KeyMapping.isPrintable(text) ? text : nil, optionIsAlt: false)
            }
            return
        }
        if hadMarked || hasMarkedText() {
            // Composition ended and the input method passed the key on.
            if keyCommandIssued && !hasMarkedText() {
                sendKey(event, action: action, text: nil, optionIsAlt: false)
            }
            // Otherwise the input method consumed the key (preedit update / cancel).
            return
        }
        sendKey(event, action: action, text: nil, optionIsAlt: false)
    }

    override func keyUp(with event: NSEvent) {
        guard inputEnabled else { return }
        if event.modifierFlags.contains(.command) || hasMarkedText() { return }
        let alt = KeyMapping.optionActsAsAlt(event, enabled: optionIsAlt)
        sendKey(event, action: KeyActionCode.release, text: nil, optionIsAlt: alt)
    }

    override func flagsChanged(with event: NSEvent) {
        guard inputEnabled else { return }
        let flags = event.modifierFlags
        let mods = KeyMapping.mods(flags, optionIsAlt: optionIsAlt)
        guard let m = KeyMapping.modifierKeys[event.keyCode] else { return }
        let down = (flags.rawValue & m.mask) != 0
        sendInput(term.key(kind: KeyKind.named, code: m.key, mods: mods,
                           action: down ? KeyActionCode.press : KeyActionCode.release, text: nil, baseLayout: 0))
    }

    /// Encodes a key event for the program.
    private func sendKey(_ event: NSEvent, action: UInt8, text: String?, optionIsAlt: Bool) {
        let keyCode = event.keyCode
        let named = KeyMapping.named[keyCode]
        // Option never composes text on named keys (arrows, F-keys...), so it is always Alt there.
        let mods = KeyMapping.mods(event.modifierFlags, optionIsAlt: optionIsAlt || named != nil)
        if let named {
            // Keypad keys keep their text (digits, operators) for the legacy encoding.
            let keyText = KeyMapping.isKeypad(keyCode) ? text : nil
            sendInput(term.key(kind: KeyKind.named, code: named, mods: mods, action: action, text: keyText,
                               baseLayout: 0))
            return
        }
        let base = KeyMapping.baseLayoutScalar(keyCode)
        let code = KeyMapping.lowercasedScalar(event.characters(byApplyingModifiers: []))
            ?? KeyMapping.lowercasedScalar(event.charactersIgnoringModifiers)
            ?? base
        guard code != 0 else {
            if let t = text, action != KeyActionCode.release { typeText(t) }
            return
        }
        sendInput(term.key(kind: KeyKind.text, code: code, mods: mods, action: action,
                           text: action == KeyActionCode.release ? nil : text,
                           baseLayout: base == code ? 0 : base))
    }

    /// Text typed outside a key press (input methods, the character palette, dictation).
    private func typeText(_ text: String) {
        guard inputEnabled, !text.isEmpty else { return }
        term.scrollToBottom()
        send(Data(text.utf8))
    }

    /// Command shortcuts the menu has no item for: line editing as in other macOS terminals,
    /// Cmd+Backspace deletes to the line's start (Ctrl+U), Cmd+Left/Right go to its start/end
    /// (Ctrl+A/Ctrl+E); Cmd+K clears the screen.
    private func handleCommandFallback(_ event: NSEvent) -> Bool {
        let flags = event.modifierFlags
        guard flags.isDisjoint(with: [.shift, .control, .option]) else { return false }
        let text: String
        switch event.keyCode {
        case 0x33: text = "\u{15}"
        case 0x7B: text = "\u{01}"
        case 0x7C: text = "\u{05}"
        case 0x28: text = "\u{0C}" // K
        default: return false
        }
        typeText(text)
        return true
    }

    // MARK: NSTextInputClient

    func insertText(_ string: Any, replacementRange: NSRange) {
        let text: String
        if let s = string as? NSAttributedString {
            text = s.string
        } else if let s = string as? String {
            text = s
        } else {
            return
        }
        markedText = NSMutableAttributedString()
        updateMarkedTextOverlay()
        if keyTextAccumulator != nil {
            keyTextAccumulator?.append(text)
            return
        }
        typeText(text)
    }

    override func doCommand(by selector: Selector) {
        // Return, arrows and Ctrl combinations are sent as key events after interpretKeyEvents
        // returns (never beep); note that the input method passed it on.
        if keyTextAccumulator != nil { keyCommandIssued = true }
    }

    func setMarkedText(_ string: Any, selectedRange: NSRange, replacementRange: NSRange) {
        if let s = string as? NSAttributedString {
            markedText = NSMutableAttributedString(attributedString: s)
        } else if let s = string as? String {
            markedText = NSMutableAttributedString(string: s)
        }
        updateMarkedTextOverlay()
    }

    func unmarkText() {
        markedText = NSMutableAttributedString()
        updateMarkedTextOverlay()
    }

    func selectedRange() -> NSRange { NSRange(location: NSNotFound, length: 0) }

    func markedRange() -> NSRange {
        markedText.length > 0 ? NSRange(location: 0, length: markedText.length) : NSRange(location: NSNotFound, length: 0)
    }

    func hasMarkedText() -> Bool { markedText.length > 0 }

    func attributedSubstring(forProposedRange range: NSRange, actualRange: NSRangePointer?) -> NSAttributedString? {
        nil
    }

    func validAttributesForMarkedText() -> [NSAttributedString.Key] {
        [.underlineStyle, .backgroundColor, .foregroundColor]
    }

    func firstRect(forCharacterRange range: NSRange, actualRange: NSRangePointer?) -> NSRect {
        guard let window else { return .zero }
        return window.convertToScreen(convert(cursorRect(), to: nil))
    }

    func characterIndex(for point: NSPoint) -> Int { NSNotFound }

    private func cursorRect() -> NSRect {
        let info = renderer.snapshot.info
        let cell = cellSizePoints
        let x = Self.padding.width + CGFloat(info.cursor_col) * cell.width
        let y = Self.padding.height + CGFloat(info.cursor_row) * cell.height
        return NSRect(x: x, y: y, width: cell.width, height: cell.height)
    }

    /// The text an input method is composing, over the cursor.
    private func updateMarkedTextOverlay() {
        guard markedText.length > 0 else {
            markedLabel?.removeFromSuperview()
            markedLabel = nil
            return
        }
        let label: NSTextField
        if let existing = markedLabel {
            label = existing
        } else {
            label = NSTextField(labelWithString: "")
            label.drawsBackground = true
            label.isBordered = false
            addSubview(label)
            markedLabel = label
        }
        let font = TerminalFont.current
        let attrs: [NSAttributedString.Key: Any] = [
            .font: font.family.flatMap { NSFont(name: $0, size: font.size) } ?? NSFont.monospacedSystemFont(ofSize: font.size, weight: .regular),
            .foregroundColor: NSColor(rgb: theme.foreground),
            .underlineStyle: NSUnderlineStyle.single.rawValue,
        ]
        label.attributedStringValue = NSAttributedString(string: markedText.string, attributes: attrs)
        label.backgroundColor = NSColor(rgb: theme.background)
        label.sizeToFit()
        let rect = cursorRect()
        label.frame = NSRect(x: rect.minX, y: rect.minY, width: max(label.frame.width, rect.width),
                             height: max(label.frame.height, rect.height))
    }

    // MARK: Edit actions

    @objc func copy(_ sender: Any?) {
        let text = term.selectionText
        guard !text.isEmpty else { return }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(text, forType: .string)
    }

    @objc func paste(_ sender: Any?) {
        guard let text = NSPasteboard.general.string(forType: .string), !text.isEmpty else { return }
        sendInput(term.paste(text))
        renderNow()
    }

    @objc override func selectAll(_ sender: Any?) {
        term.selectAll()
        renderNow()
    }

    @objc func clearScrollback(_ sender: Any?) {
        typeText("\u{0C}")
    }

    func validateMenuItem(_ item: NSMenuItem) -> Bool {
        switch item.action {
        case #selector(copy(_:)): return term.hasSelection
        case #selector(paste(_:)): return inputEnabled && NSPasteboard.general.string(forType: .string) != nil
        default: return true
        }
    }

    // MARK: Mouse

    private struct CellPosition {
        var col: Int
        var row: Int
        var rightHalf: Bool
        var x: Int
        var y: Int
    }

    private func cellPosition(_ point: NSPoint) -> CellPosition {
        guard let s = shaper else { return CellPosition(col: 0, row: 0, rightHalf: false, x: 0, y: 0) }
        let scale = backingScale
        let x = point.x * scale - CGFloat(padXPixels)
        let y = point.y * scale - CGFloat(padYPixels)
        let fcol = x / CGFloat(s.cellWidth)
        let frow = y / CGFloat(s.cellHeight)
        let col = min(max(0, cols - 1), max(0, Int(floor(fcol))))
        let row = min(max(0, rows - 1), max(0, Int(floor(frow))))
        return CellPosition(col: col, row: row, rightHalf: fcol - floor(fcol) >= 0.5, x: max(0, Int(x)), y: max(0, Int(y)))
    }

    private func mouseMods(_ flags: NSEvent.ModifierFlags) -> UInt8 {
        var m: UInt8 = 0
        if flags.contains(.shift) { m |= KeyMods.shift }
        if flags.contains(.control) { m |= KeyMods.ctrl }
        if flags.contains(.option) { m |= KeyMods.alt }
        if flags.contains(.command) { m |= KeyMods.superKey }
        return m
    }

    @discardableResult
    private func sendMouse(_ event: NSEvent, kind: UInt8, button: UInt8) -> UInt8 {
        let p = cellPosition(convert(event.locationInWindow, from: nil))
        let clicks = event.type == .mouseMoved ? 1 : max(1, event.clickCount)
        let ev = kat_mouse_event(kind: kind, button: button, mods: mouseMods(event.modifierFlags),
                                 clicks: UInt8(clamping: clicks), col: UInt16(clamping: p.col),
                                 row: UInt16(clamping: p.row), right_half: p.rightHalf,
                                 x: UInt32(clamping: p.x), y: UInt32(clamping: p.y))
        let (outcome, bytes) = term.mouse(ev)
        if outcome == MouseOutcome.reported { sendInput(bytes) }
        if outcome != MouseOutcome.none { renderNow() }
        return outcome
    }

    override func mouseDown(with event: NSEvent) {
        if window?.firstResponder !== self { window?.makeFirstResponder(self) }
        pressedButton = MouseButtonCode.left
        sendMouse(event, kind: MouseKindCode.press, button: MouseButtonCode.left)
    }

    override func mouseDragged(with event: NSEvent) {
        guard pressedButton != nil else { return }
        sendMouse(event, kind: MouseKindCode.move, button: MouseButtonCode.left)
    }

    override func mouseUp(with event: NSEvent) {
        guard pressedButton != nil else { return }
        pressedButton = nil
        sendMouse(event, kind: MouseKindCode.release, button: MouseButtonCode.left)
    }

    override func rightMouseDown(with event: NSEvent) {
        if (modes & TermMode.mouseAny) != 0 && !event.modifierFlags.contains(.shift) {
            rightPressReported = true
            sendMouse(event, kind: MouseKindCode.press, button: MouseButtonCode.right)
            return
        }
        if window?.firstResponder !== self { window?.makeFirstResponder(self) }
        let menu = NSMenu()
        menu.addItem(withTitle: "Copy", action: #selector(copy(_:)), keyEquivalent: "")
        menu.addItem(withTitle: "Paste", action: #selector(paste(_:)), keyEquivalent: "")
        menu.addItem(.separator())
        menu.addItem(withTitle: "Select All", action: #selector(selectAll(_:)), keyEquivalent: "")
        menu.addItem(withTitle: "Clear Screen", action: #selector(clearScrollback(_:)), keyEquivalent: "")
        for item in menu.items { item.target = self }
        NSMenu.popUpContextMenu(menu, with: event, for: self)
    }

    override func rightMouseUp(with event: NSEvent) {
        if rightPressReported {
            rightPressReported = false
            sendMouse(event, kind: MouseKindCode.release, button: MouseButtonCode.right)
        }
    }

    override func rightMouseDragged(with event: NSEvent) {
        if rightPressReported { sendMouse(event, kind: MouseKindCode.move, button: MouseButtonCode.right) }
    }

    private func otherButton(_ event: NSEvent) -> UInt8 {
        switch event.buttonNumber {
        case 3: return MouseButtonCode.back
        case 4: return MouseButtonCode.forward
        default: return MouseButtonCode.middle
        }
    }

    override func otherMouseDown(with event: NSEvent) {
        sendMouse(event, kind: MouseKindCode.press, button: otherButton(event))
    }

    override func otherMouseUp(with event: NSEvent) {
        sendMouse(event, kind: MouseKindCode.release, button: otherButton(event))
    }

    override func otherMouseDragged(with event: NSEvent) {
        sendMouse(event, kind: MouseKindCode.move, button: otherButton(event))
    }

    override func updateTrackingAreas() {
        super.updateTrackingAreas()
        for area in trackingAreas { removeTrackingArea(area) }
        addTrackingArea(NSTrackingArea(rect: .zero, options: [.mouseMoved, .activeInKeyWindow, .inVisibleRect, .cursorUpdate],
                                       owner: self, userInfo: nil))
    }

    override func cursorUpdate(with event: NSEvent) { NSCursor.iBeam.set() }

    override func mouseMoved(with event: NSEvent) {
        if (modes & TermMode.mouseMotion) != 0 {
            sendMouse(event, kind: MouseKindCode.move, button: MouseButtonCode.none)
        }
    }

    override func scrollWheel(with event: NSEvent) {
        guard let s = shaper else { return }
        if event.phase == .began || event.phase == .mayBegin { scrollAccumulator = 0 }
        var delta = event.scrollingDeltaY
        if event.hasPreciseScrollingDeltas {
            // Trackpad: points → lines.
            delta /= max(1, CGFloat(s.cellHeight) / backingScale)
        } else if delta != 0 {
            // Mouse wheel: whole notches, three lines each.
            delta = (delta < 0 ? min(-1, delta.rounded()) : max(1, delta.rounded())) * 3
        }
        scrollAccumulator += delta
        let lines = Int(scrollAccumulator.rounded(.towardZero))
        guard lines != 0 else { return }
        scrollAccumulator -= CGFloat(lines)
        let p = cellPosition(convert(event.locationInWindow, from: nil))
        let bytes = term.wheel(lines: Int32(clamping: lines), col: UInt16(clamping: p.col),
                               row: UInt16(clamping: p.row), mods: mouseMods(event.modifierFlags))
        sendInput(bytes)
        renderNow()
    }

    // MARK: Accessibility

    override func isAccessibilityElement() -> Bool { true }
    override func accessibilityRole() -> NSAccessibility.Role? { .textArea }
    override func accessibilityLabel() -> String? { "Terminal" }
    override func accessibilityValue() -> Any? { term.screenText }
}

/// Coalesces "output arrived" from the reader thread into one render on the main queue.
final class RenderSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var pending = false
    @MainActor var fire: () -> Void = {}

    func signal() {
        lock.lock()
        let schedule = !pending
        pending = true
        lock.unlock()
        guard schedule else { return }
        DispatchQueue.main.async { [self] in
            lock.lock()
            pending = false
            lock.unlock()
            MainActor.assumeIsolated { fire() }
        }
    }
}

extension NSColor {
    /// 0xRRGGBB, sRGB.
    convenience init(rgb: UInt32) {
        self.init(srgbRed: CGFloat((rgb >> 16) & 0xFF) / 255, green: CGFloat((rgb >> 8) & 0xFF) / 255,
                  blue: CGFloat(rgb & 0xFF) / 255, alpha: 1)
    }
}
