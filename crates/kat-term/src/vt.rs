//! Thin safe layer over libghostty-vt's C API: the parts the container terminal uses. Trimmed
//! from Thurm's `thurm-term` (vt.rs).

use std::ffi::c_void;
use std::ptr;

use ghostty_vt_rs_sys as ffi;

pub use ffi::{ColorRgb, GridRef, Style, StyleColor};

/// Things libghostty-vt reports while it processes output (its "effects").
#[derive(Default)]
pub struct Effects {
    /// Replies for the program (device attributes, status reports...).
    pub pty: Vec<u8>,
    pub bell: bool,
    pub title_changed: bool,
    /// Answer for color scheme queries (CSI ? 996 n): dark or light.
    pub dark: bool,
    /// Answer for XTWINOPS size queries.
    pub size: ffi::SizeReportSize,
}

/// A libghostty-vt terminal.
pub struct Vt {
    raw: ffi::Terminal,
    /// Heap-allocated so the userdata pointer libghostty-vt keeps stays valid when `Vt` moves.
    effects: Box<Effects>,
    /// Mouse selection state (click count, anchor, dragging), and an event of each kind.
    gesture: ffi::SelectionGesture,
    press: ffi::SelectionGestureEvent,
    drag: ffi::SelectionGestureEvent,
    release: ffi::SelectionGestureEvent,
}

// libghostty-vt terminals have no thread affinity; `Vt` is only used behind a mutex.
unsafe impl Send for Vt {}

/// A selection: both ends as (column, row from the top of the scrollback), in the order the
/// user made them, and whether it is a rectangle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sel {
    pub start: (u16, u32),
    pub end: (u16, u32),
    pub rectangle: bool,
}

pub use ffi::SelectionGestureBehavior as SelectBy;
pub use ffi::SelectionGestureGeometry as SelectGeometry;

/// Where the pointer is for a selection gesture: its cell (column, row from the top of the
/// scrollback) and pixel position in the grid.
#[derive(Clone, Copy, Debug)]
pub struct Pointer {
    pub cell: (u16, u32),
    pub px: (f64, f64),
}

/// DEC private mode number (or ANSI with `ansi`), as libghostty-vt encodes it.
pub const fn mode(value: u16, ansi: bool) -> ffi::Mode {
    (value & 0x7fff) | ((ansi as u16) << 15)
}

pub fn default_style() -> Style {
    let mut s: Style = unsafe { std::mem::zeroed() };
    unsafe { ffi::ghostty_style_default(&mut s) };
    s
}

unsafe extern "C" fn on_write_pty(_: ffi::Terminal, ud: *mut c_void, data: *const u8, len: usize) {
    let fx = unsafe { &mut *(ud as *mut Effects) };
    if !data.is_null() && len > 0 {
        fx.pty
            .extend_from_slice(unsafe { std::slice::from_raw_parts(data, len) });
    }
}

unsafe extern "C" fn on_bell(_: ffi::Terminal, ud: *mut c_void) {
    unsafe { (*(ud as *mut Effects)).bell = true };
}

unsafe extern "C" fn on_title(_: ffi::Terminal, ud: *mut c_void) {
    unsafe { (*(ud as *mut Effects)).title_changed = true };
}

unsafe extern "C" fn on_color_scheme(
    _: ffi::Terminal,
    ud: *mut c_void,
    out: *mut ffi::ColorScheme::Type,
) -> bool {
    let dark = unsafe { (*(ud as *mut Effects)).dark };
    unsafe {
        *out = if dark {
            ffi::ColorScheme::DARK
        } else {
            ffi::ColorScheme::LIGHT
        }
    };
    true
}

unsafe extern "C" fn on_size(
    _: ffi::Terminal,
    ud: *mut c_void,
    out: *mut ffi::SizeReportSize,
) -> bool {
    unsafe { *out = (*(ud as *mut Effects)).size };
    true
}

unsafe extern "C" fn on_device_attributes(
    _: ffi::Terminal,
    _: *mut c_void,
    out: *mut ffi::DeviceAttributes,
) -> bool {
    // A VT102 (`CSI ? 6 c`), and `CSI > 0 ; version ; 1 c`.
    let out = unsafe { &mut *out };
    out.primary.conformance_level = 6;
    out.primary.num_features = 0;
    out.secondary.device_type = 0;
    out.secondary.firmware_version = 100;
    out.secondary.rom_cartridge = 1;
    out.tertiary.unit_id = 0;
    true
}

unsafe extern "C" fn on_xtversion(_: ffi::Terminal, _: *mut c_void) -> ffi::String {
    const VERSION: &str = concat!("Katamaran ", env!("CARGO_PKG_VERSION"));
    ffi::String {
        ptr: VERSION.as_ptr(),
        len: VERSION.len(),
    }
}

/// The bytes of a libghostty-vt string (borrowed for the call).
unsafe fn ghostty_str(s: &ffi::String) -> &[u8] {
    if s.ptr.is_null() || s.len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(s.ptr, s.len) }
    }
}

impl Vt {
    /// A terminal keeping at least `scrollback` lines of history.
    pub fn new(cols: u16, rows: u16, scrollback: usize) -> Self {
        let mut raw: ffi::Terminal = ptr::null_mut();
        let r =
            unsafe { ffi::ghostty_terminal_new(ptr::null(), &mut raw, cols.max(1), rows.max(1)) };
        assert!(
            r == ffi::Result::SUCCESS && !raw.is_null(),
            "ghostty_terminal_new failed ({r})"
        );
        set_scrollback(raw, scrollback);
        unsafe {
            // No byte limit on top: the line limit alone decides.
            let unlimited = usize::MAX;
            ffi::ghostty_terminal_set(
                raw,
                ffi::TerminalOption::SCROLLBACK_MAX_BYTES,
                (&unlimited as *const usize).cast(),
            );
        }
        let mut gesture: ffi::SelectionGesture = ptr::null_mut();
        let events = [
            ffi::SelectionGestureEventType::PRESS,
            ffi::SelectionGestureEventType::DRAG,
            ffi::SelectionGestureEventType::RELEASE,
        ]
        .map(|ty| {
            let mut ev: ffi::SelectionGestureEvent = ptr::null_mut();
            unsafe { ffi::ghostty_selection_gesture_event_new(ptr::null(), &mut ev, ty) };
            ev
        });
        unsafe { ffi::ghostty_selection_gesture_new(ptr::null(), &mut gesture) };
        let mut vt = Self {
            raw,
            effects: Box::default(),
            gesture,
            press: events[0],
            drag: events[1],
            release: events[2],
        };
        let ud = &mut *vt.effects as *mut Effects as *const c_void;
        let write_pty: ffi::TerminalWritePtyFn = Some(on_write_pty);
        let bell: ffi::TerminalBellFn = Some(on_bell);
        let title: ffi::TerminalTitleChangedFn = Some(on_title);
        let size: ffi::TerminalSizeFn = Some(on_size);
        let da: ffi::TerminalDeviceAttributesFn = Some(on_device_attributes);
        let xtversion: ffi::TerminalXtversionFn = Some(on_xtversion);
        let scheme: ffi::TerminalColorSchemeFn = Some(on_color_scheme);
        unsafe {
            use ffi::TerminalOption as O;
            ffi::ghostty_terminal_set(raw, O::USERDATA, ud);
            ffi::ghostty_terminal_set(raw, O::WRITE_PTY, fn_ptr(write_pty));
            ffi::ghostty_terminal_set(raw, O::BELL, fn_ptr(bell));
            ffi::ghostty_terminal_set(raw, O::TITLE_CHANGED, fn_ptr(title));
            ffi::ghostty_terminal_set(raw, O::SIZE, fn_ptr(size));
            ffi::ghostty_terminal_set(raw, O::DEVICE_ATTRIBUTES, fn_ptr(da));
            ffi::ghostty_terminal_set(raw, O::XTVERSION, fn_ptr(xtversion));
            ffi::ghostty_terminal_set(raw, O::COLOR_SCHEME, fn_ptr(scheme));
            // No images: nothing for kitty graphics to keep, and no files for it to read.
            let zero: u64 = 0;
            ffi::ghostty_terminal_set(
                raw,
                O::KITTY_IMAGE_STORAGE_LIMIT,
                (&zero as *const u64).cast(),
            );
            let off = false;
            for o in [O::KITTY_IMAGE_MEDIUM_FILE, O::KITTY_IMAGE_MEDIUM_SHARED_MEM] {
                ffi::ghostty_terminal_set(raw, o, (&off as *const bool).cast());
            }
            ffi::ghostty_terminal_set(raw, O::KITTY_IMAGE_MEDIUM_TEMP_FILE, ptr::null());
            ffi::ghostty_terminal_set(raw, O::GLYPH_PROTOCOL, (&off as *const bool).cast());
        }
        vt
    }

    pub(crate) fn raw(&self) -> ffi::Terminal {
        self.raw
    }

    pub fn effects(&mut self) -> &mut Effects {
        &mut self.effects
    }

    pub fn write(&mut self, bytes: &[u8]) {
        if !bytes.is_empty() {
            unsafe { ffi::ghostty_terminal_vt_write(self.raw, bytes.as_ptr(), bytes.len()) }
        }
    }

    pub fn resize(&mut self, cols: u16, rows: u16, cell_width: u32, cell_height: u32) {
        self.effects.size = ffi::SizeReportSize {
            rows,
            columns: cols,
            cell_width,
            cell_height,
        };
        unsafe {
            ffi::ghostty_terminal_resize(
                self.raw,
                cols.max(1),
                rows.max(1),
                cell_width,
                cell_height,
            );
        }
    }

    pub fn reset(&mut self) {
        unsafe { ffi::ghostty_terminal_reset(self.raw) }
    }

    fn get<T: Copy>(&self, data: ffi::TerminalData::Type) -> Option<T> {
        let mut out = std::mem::MaybeUninit::<T>::zeroed();
        let r = unsafe { ffi::ghostty_terminal_get(self.raw, data, out.as_mut_ptr().cast()) };
        (r == ffi::Result::SUCCESS).then(|| unsafe { out.assume_init() })
    }

    pub fn cols(&self) -> u16 {
        self.get(ffi::TerminalData::COLS).unwrap_or(1)
    }

    pub fn rows(&self) -> u16 {
        self.get(ffi::TerminalData::ROWS).unwrap_or(1)
    }

    pub fn is_alt_screen(&self) -> bool {
        self.get::<ffi::TerminalScreen::Type>(ffi::TerminalData::ACTIVE_SCREEN)
            == Some(ffi::TerminalScreen::ALTERNATE)
    }

    pub fn kitty_flags(&self) -> u8 {
        self.get(ffi::TerminalData::KITTY_KEYBOARD_FLAGS)
            .unwrap_or(0)
    }

    pub fn title(&self) -> String {
        self.get::<ffi::String>(ffi::TerminalData::TITLE)
            .map(|s| String::from_utf8_lossy(unsafe { ghostty_str(&s) }).into_owned())
            .unwrap_or_default()
    }

    pub fn scrollbar(&self) -> ffi::TerminalScrollbar {
        self.get(ffi::TerminalData::SCROLLBAR).unwrap_or_default()
    }

    /// Rows of scrollback above the active area.
    pub fn history(&self) -> usize {
        self.get(ffi::TerminalData::SCROLLBACK_ROWS).unwrap_or(0)
    }

    pub fn mode(&self, mode: ffi::Mode) -> bool {
        let mut m = ffi::TerminalModeConfig { mode, value: false };
        unsafe {
            ffi::ghostty_terminal_get(
                self.raw,
                ffi::TerminalData::MODE,
                (&mut m as *mut ffi::TerminalModeConfig).cast(),
            )
        };
        m.value
    }

    pub fn scroll_delta(&mut self, delta: isize) {
        self.scroll(
            ffi::TerminalScrollViewportTag::DELTA,
            ffi::TerminalScrollViewportValue { delta },
        );
    }

    pub fn scroll_top(&mut self) {
        self.scroll(
            ffi::TerminalScrollViewportTag::TOP,
            ffi::TerminalScrollViewportValue { row: 0 },
        );
    }

    pub fn scroll_bottom(&mut self) {
        self.scroll(
            ffi::TerminalScrollViewportTag::BOTTOM,
            ffi::TerminalScrollViewportValue { row: 0 },
        );
    }

    fn scroll(
        &mut self,
        tag: ffi::TerminalScrollViewportTag::Type,
        value: ffi::TerminalScrollViewportValue,
    ) {
        unsafe {
            ffi::ghostty_terminal_scroll_viewport(
                self.raw,
                ffi::TerminalScrollViewport { tag, value },
            );
        }
    }

    fn set_color(&mut self, which: ffi::TerminalOption::Type, rgb: u32) {
        let c = to_rgb(rgb);
        unsafe { ffi::ghostty_terminal_set(self.raw, which, (&c as *const ColorRgb).cast()) };
    }

    pub fn set_default_colors(&mut self, fg: u32, bg: u32, cursor: u32, palette: &[u32; 256]) {
        use ffi::TerminalOption as O;
        self.set_color(O::COLOR_FOREGROUND, fg);
        self.set_color(O::COLOR_BACKGROUND, bg);
        self.set_color(O::COLOR_CURSOR, cursor);
        let p: [ColorRgb; 256] = std::array::from_fn(|i| to_rgb(palette[i]));
        unsafe { ffi::ghostty_terminal_set(self.raw, O::COLOR_PALETTE, p.as_ptr().cast()) };
    }

    pub fn default_palette(&self) -> [u32; 256] {
        let mut p = [ColorRgb::default(); 256];
        unsafe {
            ffi::ghostty_terminal_get(
                self.raw,
                ffi::TerminalData::COLOR_PALETTE_DEFAULT,
                p.as_mut_ptr().cast(),
            )
        };
        p.map(from_rgb)
    }

    /// Grid reference for (column, row) with rows counted from the top of the scrollback.
    pub fn screen_ref(&self, x: u16, y: u32) -> Option<GridRef> {
        let point = ffi::Point {
            tag: ffi::PointTag::SCREEN,
            value: ffi::PointValue {
                coordinate: ffi::PointCoordinate { x, y },
            },
        };
        let mut out = empty_ref();
        let r = unsafe { ffi::ghostty_terminal_grid_ref(self.raw, point, &mut out) };
        (r == ffi::Result::SUCCESS && !out.node.is_null()).then_some(out)
    }
}

impl Vt {
    /// The active selection.
    pub fn selection(&self) -> Option<Sel> {
        let mut sel = empty_selection();
        let r = unsafe {
            ffi::ghostty_terminal_get(
                self.raw,
                ffi::TerminalData::SELECTION,
                (&mut sel as *mut ffi::Selection).cast(),
            )
        };
        if r != ffi::Result::SUCCESS {
            return None;
        }
        self.sel_from(&sel)
    }

    fn sel_from(&self, sel: &ffi::Selection) -> Option<Sel> {
        Some(Sel {
            start: self.screen_point_of(&sel.start)?,
            end: self.screen_point_of(&sel.end)?,
            rectangle: sel.rectangle,
        })
    }

    /// (column, row from the top of the scrollback) of a grid reference.
    pub fn screen_point_of(&self, r: &GridRef) -> Option<(u16, u32)> {
        let mut out = ffi::PointCoordinate { x: 0, y: 0 };
        let res = unsafe {
            ffi::ghostty_terminal_point_from_grid_ref(self.raw, r, ffi::PointTag::SCREEN, &mut out)
        };
        (res == ffi::Result::SUCCESS).then_some((out.x, out.y))
    }

    /// Install (or with `None`, clear) the active selection; libghostty-vt keeps it on the
    /// text as it scrolls and reflows.
    pub fn set_selection(&mut self, sel: Option<Sel>) {
        let raw = sel.and_then(|s| {
            Some(ffi::Selection {
                start: self.screen_ref(s.start.0, s.start.1)?,
                end: self.screen_ref(s.end.0, s.end.1)?,
                rectangle: s.rectangle,
            })
        });
        let value = raw
            .as_ref()
            .map_or(ptr::null(), |r| (r as *const ffi::Selection).cast());
        unsafe { ffi::ghostty_terminal_set(self.raw, ffi::TerminalOption::SELECTION, value) };
    }

    /// Select everything (scrollback and screen).
    pub fn select_all(&mut self) {
        let mut sel = empty_selection();
        if unsafe { ffi::ghostty_terminal_select_all(self.raw, &mut sel) } == ffi::Result::SUCCESS {
            let s = self.sel_from(&sel);
            self.set_selection(s);
        }
    }

    /// The selected text as it is copied: soft wraps joined, trailing blanks trimmed.
    pub fn selection_text(&self) -> String {
        let mut sel = empty_selection();
        let r = unsafe {
            ffi::ghostty_terminal_get(
                self.raw,
                ffi::TerminalData::SELECTION,
                (&mut sel as *mut ffi::Selection).cast(),
            )
        };
        if r != ffi::Result::SUCCESS {
            return String::new();
        }
        String::from_utf8_lossy(&self.format_plain(Some(&sel))).into_owned()
    }

    /// The active screen as plain text (soft wraps joined, trailing blanks trimmed).
    pub fn screen_text(&self) -> String {
        String::from_utf8_lossy(&self.format_plain(None)).into_owned()
    }

    /// A mouse press starting a selection (`by`: what a click selects), with word boundaries.
    /// Returns the selection it makes, if any (a single click selects nothing yet).
    pub fn gesture_press(&mut self, at: Pointer, by: SelectBy::Type, words: &[u32]) -> Option<Sel> {
        let ev = self.press;
        let behaviors = ffi::SelectionGestureBehaviors {
            single_click: by,
            double_click: by,
            triple_click: by,
        };
        unsafe {
            use ffi::SelectionGestureEventOption as O;
            // Untimed: the app counts clicks, and `by` says what this one selects.
            ffi::ghostty_selection_gesture_reset(self.gesture, self.raw);
            ffi::ghostty_selection_gesture_event_set(
                ev,
                O::BEHAVIORS,
                (&behaviors as *const ffi::SelectionGestureBehaviors).cast(),
            );
        }
        self.gesture_event(ev, at, None, false, words)
    }

    /// Dragging with the button held.
    pub fn gesture_drag(
        &mut self,
        at: Pointer,
        geometry: ffi::SelectionGestureGeometry,
        rectangle: bool,
        words: &[u32],
    ) -> Option<Sel> {
        let ev = self.drag;
        self.gesture_event(ev, at, Some(geometry), rectangle, words)
    }

    pub fn gesture_release(&mut self, at: Pointer) {
        let ev = self.release;
        if let Some(r) = self.screen_ref(at.cell.0, at.cell.1) {
            unsafe {
                ffi::ghostty_selection_gesture_event_set(
                    ev,
                    ffi::SelectionGestureEventOption::REF,
                    (&r as *const GridRef).cast(),
                );
                ffi::ghostty_selection_gesture_event(self.gesture, self.raw, ev, ptr::null_mut());
            }
        }
    }

    fn gesture_event(
        &mut self,
        ev: ffi::SelectionGestureEvent,
        at: Pointer,
        geometry: Option<ffi::SelectionGestureGeometry>,
        rectangle: bool,
        words: &[u32],
    ) -> Option<Sel> {
        let r = self.screen_ref(at.cell.0, at.cell.1)?;
        let pos = ffi::SurfacePosition {
            x: at.px.0,
            y: at.px.1,
        };
        let cps = ffi::Codepoints {
            ptr: words.as_ptr(),
            len: words.len(),
        };
        let mut sel = empty_selection();
        let res = unsafe {
            use ffi::SelectionGestureEventOption as O;
            let set = |o, v: *const c_void| {
                ffi::ghostty_selection_gesture_event_set(ev, o, v);
            };
            set(O::REF, (&r as *const GridRef).cast());
            set(O::POSITION, (&pos as *const ffi::SurfacePosition).cast());
            if !words.is_empty() {
                set(
                    O::WORD_BOUNDARY_CODEPOINTS,
                    (&cps as *const ffi::Codepoints).cast(),
                );
            }
            if let Some(g) = &geometry {
                set(
                    O::GEOMETRY,
                    (g as *const ffi::SelectionGestureGeometry).cast(),
                );
                set(O::RECTANGLE, (&rectangle as *const bool).cast());
            }
            ffi::ghostty_selection_gesture_event(self.gesture, self.raw, ev, &mut sel)
        };
        (res == ffi::Result::SUCCESS)
            .then(|| self.sel_from(&sel))
            .flatten()
    }

    /// The active screen (or `sel`) as plain text.
    fn format_plain(&self, sel: Option<&ffi::Selection>) -> Vec<u8> {
        let mut f: ffi::Formatter = ptr::null_mut();
        if unsafe { ffi::ghostty_formatter_terminal_new(ptr::null(), &mut f, self.raw) }
            != ffi::Result::SUCCESS
        {
            return Vec::new();
        }
        fn set<T>(f: ffi::Formatter, opt: ffi::FormatterOption::Type, value: &T) -> bool {
            let value = (value as *const T).cast();
            unsafe { ffi::ghostty_formatter_set(f, opt, value) == ffi::Result::SUCCESS }
        }
        use ffi::FormatterOption as O;
        let applied = set(f, O::EMIT, &ffi::FormatterFormat::PLAIN)
            && set(f, O::UNWRAP, &true)
            && set(f, O::TRIM, &true)
            && set(f, O::EXTRAS, &ffi::FORMATTER_EXTRAS_NONE)
            && sel.is_none_or(|s| set(f, O::SELECTION, s));
        if !applied {
            unsafe { ffi::ghostty_formatter_free(f) };
            return Vec::new();
        }
        let (mut out, mut len): (*mut u8, usize) = (ptr::null_mut(), 0);
        let r = unsafe { ffi::ghostty_formatter_format_alloc(f, ptr::null(), &mut out, &mut len) };
        let bytes = if r == ffi::Result::SUCCESS && !out.is_null() {
            let v = unsafe { std::slice::from_raw_parts(out, len) }.to_vec();
            unsafe { ffi::ghostty_free(ptr::null(), out, len) };
            v
        } else {
            Vec::new()
        };
        unsafe { ffi::ghostty_formatter_free(f) };
        bytes
    }
}

fn empty_ref() -> GridRef {
    GridRef {
        node: ptr::null_mut(),
        x: 0,
        y: 0,
    }
}

fn empty_selection() -> ffi::Selection {
    ffi::Selection {
        start: empty_ref(),
        end: empty_ref(),
        rectangle: false,
    }
}

impl Drop for Vt {
    fn drop(&mut self) {
        unsafe {
            ffi::ghostty_selection_gesture_free(self.gesture, self.raw);
            for ev in [self.press, self.drag, self.release] {
                ffi::ghostty_selection_gesture_event_free(ev);
            }
            ffi::ghostty_terminal_free(self.raw)
        }
    }
}

fn fn_ptr<F>(f: Option<F>) -> *const c_void {
    // Callback options take the function pointer itself as the value.
    match f {
        Some(f) => {
            let p: *const c_void = unsafe { std::mem::transmute_copy(&f) };
            p
        }
        None => ptr::null(),
    }
}

pub fn to_rgb(c: u32) -> ColorRgb {
    ColorRgb {
        r: (c >> 16) as u8,
        g: (c >> 8) as u8,
        b: c as u8,
    }
}

pub fn from_rgb(c: ColorRgb) -> u32 {
    (c.r as u32) << 16 | (c.g as u32) << 8 | c.b as u32
}

fn set_scrollback(raw: ffi::Terminal, lines: usize) {
    // libghostty-vt prunes whole pages (a few hundred rows), so the limit gets a page of
    // margin to keep at least `lines` lines.
    let max_lines = if lines == 0 { 0 } else { lines + 512 };
    unsafe {
        ffi::ghostty_terminal_set(
            raw,
            ffi::TerminalOption::SCROLLBACK_MAX_LINES,
            (&max_lines as *const usize).cast(),
        );
    }
}

/// One cell of the viewport as the render state has it, colors resolved.
#[derive(Clone, Copy, Default)]
pub struct RenderCell {
    pub codepoint: u32,
    pub wide: ffi::CellWide::Type,
    pub style: Option<Style>,
    /// Resolved foreground / background (palette applied), `None` for the default.
    pub fg: Option<u32>,
    pub bg: Option<u32>,
    pub selected: bool,
    /// The cell holds a grapheme cluster: base and combining characters (see `graphemes`).
    pub cluster: bool,
}

pub const WIDE: ffi::CellWide::Type = ffi::CellWide::WIDE;
pub const SPACER_TAIL: ffi::CellWide::Type = ffi::CellWide::SPACER_TAIL;
pub const SPACER_HEAD: ffi::CellWide::Type = ffi::CellWide::SPACER_HEAD;

/// libghostty-vt's render state of a terminal's viewport: rows it rebuilds only where they
/// changed, and marks dirty until read.
pub struct Render {
    state: ffi::RenderState,
    rows: ffi::RenderStateRowIterator,
    cells: ffi::RenderStateRowCells,
}

unsafe impl Send for Render {}

/// Render-state colors: default foreground / background, explicit cursor color, palette.
pub struct RenderColors {
    pub fg: u32,
    pub bg: u32,
    pub cursor: Option<u32>,
    pub palette: [u32; 256],
}

/// The cursor as the render state has it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenderCursor {
    /// Viewport position, `None` when it is not on screen (scrolled back).
    pub at: Option<(u16, u16)>,
    pub wide_tail: bool,
    pub visible: bool,
    pub blinking: bool,
    pub style: ffi::CursorStyle::Type,
}

pub use ffi::CursorStyle;

impl Default for Render {
    fn default() -> Self {
        Self::new()
    }
}

impl Render {
    pub fn new() -> Self {
        let mut r = Self {
            state: ptr::null_mut(),
            rows: ptr::null_mut(),
            cells: ptr::null_mut(),
        };
        unsafe {
            ffi::ghostty_render_state_new(ptr::null(), &mut r.state);
            ffi::ghostty_render_state_row_iterator_new(ptr::null(), &mut r.rows);
            ffi::ghostty_render_state_row_cells_new(ptr::null(), &mut r.cells);
        }
        r
    }

    /// Bring it up to date with `vt`; whether anything changed since rows were last read.
    pub fn update(&mut self, vt: &Vt) -> ffi::RenderStateDirty::Type {
        let mut dirty = ffi::RenderStateDirty::FULL;
        unsafe {
            ffi::ghostty_render_state_update(self.state, vt.raw());
            ffi::ghostty_render_state_get(
                self.state,
                ffi::RenderStateData::DIRTY,
                (&mut dirty as *mut ffi::RenderStateDirty::Type).cast(),
            );
        }
        dirty
    }

    pub fn colors(&self) -> RenderColors {
        let mut c = ffi::RenderStateColors {
            size: std::mem::size_of::<ffi::RenderStateColors>(),
            ..Default::default()
        };
        unsafe {
            ffi::ghostty_render_state_get(
                self.state,
                ffi::RenderStateData::COLORS,
                (&mut c as *mut ffi::RenderStateColors).cast(),
            )
        };
        RenderColors {
            fg: from_rgb(c.foreground),
            bg: from_rgb(c.background),
            cursor: c.cursor_has_value.then(|| from_rgb(c.cursor)),
            palette: c.palette.map(from_rgb),
        }
    }

    pub fn cursor(&self) -> RenderCursor {
        let mut c = ffi::RenderStateCursor {
            size: std::mem::size_of::<ffi::RenderStateCursor>(),
            ..Default::default()
        };
        unsafe {
            ffi::ghostty_render_state_get(
                self.state,
                ffi::RenderStateData::CURSOR,
                (&mut c as *mut ffi::RenderStateCursor).cast(),
            )
        };
        RenderCursor {
            at: c.viewport_has_value.then_some((c.viewport_x, c.viewport_y)),
            wide_tail: c.viewport_has_value && c.wide_tail,
            visible: c.visible,
            blinking: c.blinking,
            style: c.visual_style,
        }
    }

    /// For each viewport row (top to bottom), `f(row, cells)` when `want(row, dirty)`; then
    /// marks everything clean. Cells with a grapheme cluster get it in the second argument.
    pub fn rows(
        &mut self,
        mut want: impl FnMut(usize, bool) -> bool,
        mut f: impl FnMut(usize, Vec<RenderCell>, Vec<(u16, Vec<u32>)>),
    ) {
        use ffi::RenderStateRowCellsData as C;
        use ffi::RenderStateRowData as R;
        unsafe {
            let mut it = self.rows;
            if ffi::ghostty_render_state_get(
                self.state,
                ffi::RenderStateData::ROW_ITERATOR,
                (&mut it as *mut ffi::RenderStateRowIterator).cast(),
            ) != ffi::Result::SUCCESS
            {
                return;
            }
            let mut y = 0;
            while ffi::ghostty_render_state_row_iterator_next(it) {
                let mut dirty = true;
                ffi::ghostty_render_state_row_get(it, R::DIRTY, (&mut dirty as *mut bool).cast());
                if want(y, dirty) {
                    let mut cells = Vec::new();
                    let mut clusters = Vec::new();
                    let mut c = self.cells;
                    ffi::ghostty_render_state_row_get(
                        it,
                        R::CELLS,
                        (&mut c as *mut ffi::RenderStateRowCells).cast(),
                    );
                    let mut x: u16 = 0;
                    while ffi::ghostty_render_state_row_cells_next(c) {
                        let mut raw: ffi::Cell = 0;
                        ffi::ghostty_render_state_row_cells_get(
                            c,
                            C::RAW,
                            (&mut raw as *mut ffi::Cell).cast(),
                        );
                        let mut cell = RenderCell::default();
                        let mut has_styling = false;
                        let mut tag: ffi::CellContentTag::Type = 0;
                        ffi::ghostty_cell_get(
                            raw,
                            ffi::CellData::CODEPOINT,
                            (&mut cell.codepoint as *mut u32).cast(),
                        );
                        ffi::ghostty_cell_get(
                            raw,
                            ffi::CellData::WIDE,
                            (&mut cell.wide as *mut ffi::CellWide::Type).cast(),
                        );
                        ffi::ghostty_cell_get(
                            raw,
                            ffi::CellData::CONTENT_TAG,
                            (&mut tag as *mut ffi::CellContentTag::Type).cast(),
                        );
                        ffi::ghostty_cell_get(
                            raw,
                            ffi::CellData::HAS_STYLING,
                            (&mut has_styling as *mut bool).cast(),
                        );
                        if has_styling {
                            let mut st = default_style();
                            ffi::ghostty_render_state_row_cells_get(
                                c,
                                C::STYLE,
                                (&mut st as *mut Style).cast(),
                            );
                            cell.style = Some(st);
                        }
                        let mut rgb = ColorRgb::default();
                        if ffi::ghostty_render_state_row_cells_get(
                            c,
                            C::FG_COLOR,
                            (&mut rgb as *mut ColorRgb).cast(),
                        ) == ffi::Result::SUCCESS
                        {
                            cell.fg = Some(from_rgb(rgb));
                        }
                        if ffi::ghostty_render_state_row_cells_get(
                            c,
                            C::BG_COLOR,
                            (&mut rgb as *mut ColorRgb).cast(),
                        ) == ffi::Result::SUCCESS
                        {
                            cell.bg = Some(from_rgb(rgb));
                        }
                        ffi::ghostty_render_state_row_cells_get(
                            c,
                            C::SELECTED,
                            (&mut cell.selected as *mut bool).cast(),
                        );
                        if tag == ffi::CellContentTag::CODEPOINT_GRAPHEME {
                            let mut len = 0u32;
                            ffi::ghostty_render_state_row_cells_get(
                                c,
                                C::GRAPHEMES_LEN,
                                (&mut len as *mut u32).cast(),
                            );
                            let mut buf = vec![0u32; len as usize];
                            let mut out = ffi::CodepointBuffer {
                                ptr: buf.as_mut_ptr(),
                                cap: buf.len(),
                                len: 0,
                            };
                            ffi::ghostty_render_state_row_cells_get(
                                c,
                                C::GRAPHEMES_BUF,
                                (&mut out as *mut ffi::CodepointBuffer).cast(),
                            );
                            buf.truncate(out.len.min(buf.len()));
                            if buf.len() > 1 {
                                cell.cluster = true;
                                clusters.push((x, buf));
                            }
                        }
                        cells.push(cell);
                        x = x.saturating_add(1);
                    }
                    f(y, cells, clusters);
                }
                let clean = false;
                ffi::ghostty_render_state_row_set(
                    it,
                    ffi::RenderStateRowOption::DIRTY,
                    (&clean as *const bool).cast(),
                );
                y += 1;
            }
            let clean = ffi::RenderStateDirty::FALSE;
            ffi::ghostty_render_state_set(
                self.state,
                ffi::RenderStateOption::DIRTY,
                (&clean as *const ffi::RenderStateDirty::Type).cast(),
            );
        }
    }
}

impl Drop for Render {
    fn drop(&mut self) {
        unsafe {
            ffi::ghostty_render_state_row_cells_free(self.cells);
            ffi::ghostty_render_state_row_iterator_free(self.rows);
            ffi::ghostty_render_state_free(self.state);
        }
    }
}
