//! A terminal for one container shell: libghostty-vt's state, its render state read into a
//! grid of resolved cells (the layout Thurm's renderer draws), input encoding and selection.

use ghostty_vt_rs_sys as ffi;

use crate::keys::{self, Encoders, KeyEvent, MouseButton, MouseEvent, MouseKind, mods};
use crate::vt::{self, Render, Vt};

/// Cell flags (`KAT_CELL_*`).
pub mod cf {
    pub const BOLD: u16 = 1 << 0;
    pub const ITALIC: u16 = 1 << 1;
    pub const UNDERLINE: u16 = 1 << 2;
    pub const DOUBLE_UNDERLINE: u16 = 1 << 3;
    pub const UNDERCURL: u16 = 1 << 4;
    pub const DOTTED_UNDERLINE: u16 = 1 << 5;
    pub const DASHED_UNDERLINE: u16 = 1 << 6;
    pub const STRIKEOUT: u16 = 1 << 7;
    pub const WIDE: u16 = 1 << 8;
    pub const WIDE_SPACER: u16 = 1 << 9;
    pub const HIDDEN: u16 = 1 << 10;
    pub const SELECTED: u16 = 1 << 11;
    pub const DEFAULT_BG: u16 = 1 << 13;
    pub const DIM: u16 = 1 << 14;
}

/// Mode bits (`KAT_MODE_*`).
pub mod tm {
    pub const SHOW_CURSOR: u32 = 1;
    pub const APP_CURSOR: u32 = 1 << 1;
    pub const MOUSE_REPORT_CLICK: u32 = 1 << 3;
    pub const BRACKETED_PASTE: u32 = 1 << 4;
    pub const MOUSE_MOTION: u32 = 1 << 6;
    pub const FOCUS_IN_OUT: u32 = 1 << 11;
    pub const ALT_SCREEN: u32 = 1 << 12;
    pub const MOUSE_DRAG: u32 = 1 << 13;
    pub const MOUSE_ANY: u32 = MOUSE_REPORT_CLICK | MOUSE_MOTION | MOUSE_DRAG;
}

/// Cursor shapes (`KAT_CURSOR_*`).
pub mod cursor {
    pub const BLOCK: u8 = 0;
    pub const UNDERLINE: u8 = 1;
    pub const BEAM: u8 = 2;
    pub const HOLLOW_BLOCK: u8 = 3;
    pub const HIDDEN: u8 = 4;
}

/// Underline color "use the text color".
pub const NO_COLOR: u32 = 0xFF00_0000;

/// One grid cell with its colors resolved (`kat_cell`; the same layout as Thurm's
/// `thurm_cell`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cell {
    /// Unicode scalar; 0 or ' ' = blank.
    pub ch: u32,
    /// 0x00RRGGBB, inverse and faint applied.
    pub fg: u32,
    pub bg: u32,
    /// Underline color, `NO_COLOR` = the text color.
    pub ul: u32,
    pub flags: u16,
    pub link: u16,
}

/// What the grid shows besides its cells (`kat_grid_info`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GridInfo {
    pub cols: u16,
    pub rows: u16,
    pub cursor_col: u16,
    pub cursor_row: u16,
    pub cursor_shape: u8,
    pub cursor_blinking: bool,
    pub cursor_wide: bool,
    /// Rows scrolled back (0 at the bottom).
    pub display_offset: u32,
    pub history_size: u32,
    pub modes: u32,
    pub foreground: u32,
    pub background: u32,
    pub cursor_color: u32,
    pub cursor_text_color: u32,
    pub selection_fg: u32,
    pub selection_bg: u32,
    /// Changes whenever anything shown changes.
    pub generation: u64,
}

/// Colors the app picks (light or dark appearance) (`kat_theme`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Theme {
    pub foreground: u32,
    pub background: u32,
    pub cursor: u32,
    pub cursor_text: u32,
    pub selection_fg: u32,
    pub selection_bg: u32,
    /// The 16 ANSI colors; the rest of the 256-color palette stays xterm's.
    pub palette: [u32; 16],
    pub dark: bool,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            foreground: 0xD8D8D8,
            background: 0x1E1E1E,
            cursor: 0xD8D8D8,
            cursor_text: 0x1E1E1E,
            selection_fg: 0xFFFFFF,
            selection_bg: 0x3F638B,
            palette: [
                0x000000, 0xCD3131, 0x0DBC79, 0xE5E510, 0x2472C8, 0xBC3FBC, 0x11A8CD, 0xE5E5E5,
                0x666666, 0xF14C4C, 0x23D18B, 0xF5F543, 0x3B8EEA, 0xD670D6, 0x29B8DB, 0xFFFFFF,
            ],
            dark: true,
        }
    }
}

/// What a mouse event did (`KAT_MOUSE_*` outcomes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MouseOutcome {
    None = 0,
    /// Reported to the program: send the bytes.
    Reported = 1,
    SelectionChanged = 2,
    /// The button was released over a selection (copy-on-select would copy now).
    SelectionDone = 3,
}

/// Scrolling the viewport (`KAT_SCROLL_*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scroll {
    /// Lines towards older output (negative: newer).
    Lines(i32),
    /// Pages towards older output.
    Pages(i32),
    Top,
    Bottom,
}

#[derive(Clone, Copy, Debug)]
enum Drag {
    /// Shift-click extended an existing selection.
    Extend,
    Gesture {
        rect: bool,
    },
}

const M_APP_CURSOR: u16 = vt::mode(1, false);
const M_X10_MOUSE: u16 = vt::mode(9, false);
const M_SHOW_CURSOR: u16 = vt::mode(25, false);
const M_MOUSE_CLICK: u16 = vt::mode(1000, false);
const M_MOUSE_DRAG: u16 = vt::mode(1002, false);
const M_MOUSE_MOTION: u16 = vt::mode(1003, false);
const M_FOCUS: u16 = vt::mode(1004, false);
const M_BRACKETED_PASTE: u16 = vt::mode(2004, false);

/// Characters a double click stops at.
const WORD_SEPARATORS: &str = " \t,│`|:\"'()[]{}<>";

pub struct Term {
    vt: Vt,
    render: Render,
    enc: Encoders,
    cols: u16,
    rows: u16,
    cell_width: u32,
    cell_height: u32,
    theme: Theme,
    selecting: Option<Drag>,
    /// Re-read every row on the next snapshot (colors or size changed).
    stale: bool,
    cells: Vec<Cell>,
    /// Grapheme clusters by row: (column, text).
    clusters: Vec<Vec<(u16, String)>>,
    info: GridInfo,
}

impl Term {
    pub fn new(cols: u16, rows: u16, scrollback: usize) -> Option<Self> {
        let (cols, rows) = (cols.max(1), rows.max(1));
        let mut t = Self {
            vt: Vt::new(cols, rows, scrollback)?,
            render: Render::new()?,
            enc: Encoders::new()?,
            cols,
            rows,
            cell_width: 1,
            cell_height: 1,
            theme: Theme::default(),
            selecting: None,
            stale: true,
            cells: Vec::new(),
            clusters: Vec::new(),
            info: GridInfo::default(),
        };
        t.vt.resize(cols, rows, 1, 1);
        t.apply_theme();
        Some(t)
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        self.vt.write(bytes);
    }

    /// Replies the terminal has for the program (device attributes, cursor reports...).
    pub fn take_replies(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.vt.effects().pty)
    }

    pub fn take_bell(&mut self) -> bool {
        std::mem::take(&mut self.vt.effects().bell)
    }

    /// The window title the program set, and whether it changed since the last call.
    pub fn title(&mut self) -> (String, bool) {
        let changed = std::mem::take(&mut self.vt.effects().title_changed);
        (self.vt.title(), changed)
    }

    pub fn resize(&mut self, cols: u16, rows: u16, cell_width: u32, cell_height: u32) {
        let (cols, rows) = (cols.max(1), rows.max(1));
        self.cols = cols;
        self.rows = rows;
        self.cell_width = cell_width.max(1);
        self.cell_height = cell_height.max(1);
        self.vt
            .resize(cols, rows, self.cell_width, self.cell_height);
        self.stale = true;
    }

    pub fn size(&self) -> (u16, u16) {
        (self.cols, self.rows)
    }

    pub fn set_theme(&mut self, theme: Theme) {
        if theme != self.theme {
            self.theme = theme;
            self.apply_theme();
        }
    }

    fn apply_theme(&mut self) {
        let t = self.theme;
        let mut palette = self.vt.default_palette();
        palette[..16].copy_from_slice(&t.palette);
        self.vt
            .set_default_colors(t.foreground, t.background, t.cursor, &palette);
        self.vt.effects().dark = t.dark;
        self.stale = true;
    }

    pub fn modes(&self) -> u32 {
        let v = &self.vt;
        let mut m = 0;
        for (on, bit) in [
            (v.mode(M_SHOW_CURSOR), tm::SHOW_CURSOR),
            (v.mode(M_APP_CURSOR), tm::APP_CURSOR),
            (
                v.mode(M_MOUSE_CLICK) || v.mode(M_X10_MOUSE),
                tm::MOUSE_REPORT_CLICK,
            ),
            (v.mode(M_BRACKETED_PASTE), tm::BRACKETED_PASTE),
            (v.mode(M_MOUSE_MOTION), tm::MOUSE_MOTION),
            (v.mode(M_FOCUS), tm::FOCUS_IN_OUT),
            (v.is_alt_screen(), tm::ALT_SCREEN),
            (v.mode(M_MOUSE_DRAG), tm::MOUSE_DRAG),
        ] {
            if on {
                m |= bit;
            }
        }
        m
    }

    fn display_offset(&self) -> u32 {
        let sb = self.vt.scrollbar();
        sb.total.saturating_sub(sb.len).saturating_sub(sb.offset) as u32
    }

    // -----------------------------------------------------------------------------------------
    // Frames
    // -----------------------------------------------------------------------------------------

    /// Brings the grid up to date. Returns whether anything shown changed.
    pub fn snapshot(&mut self) -> bool {
        let dirty = self.render.update(&self.vt);
        let (cols, rows) = (self.cols as usize, self.rows as usize);
        let full =
            self.stale || dirty == ffi::RenderStateDirty::FULL || self.cells.len() != cols * rows;
        if full {
            self.cells = vec![Cell::default(); cols * rows];
            self.clusters = vec![Vec::new(); rows];
        }
        let colors = self.render.colors();
        let mut changed = full;
        {
            let (cells, clusters) = (&mut self.cells, &mut self.clusters);
            self.render.rows(
                |y, d| y < rows && (full || d),
                |y, row, cl| {
                    changed = true;
                    let base = y * cols;
                    for (x, c) in row.iter().take(cols).enumerate() {
                        cells[base + x] = convert(c, &colors);
                    }
                    for x in row.len()..cols {
                        cells[base + x] = Cell {
                            ch: ' ' as u32,
                            fg: colors.fg,
                            bg: colors.bg,
                            ul: NO_COLOR,
                            flags: cf::DEFAULT_BG,
                            link: 0,
                        };
                    }
                    clusters[y] = cl
                        .into_iter()
                        .filter(|(x, _)| (*x as usize) < cols)
                        .map(|(x, cps)| (x, cps.into_iter().filter_map(char::from_u32).collect()))
                        .collect();
                },
            );
        }
        self.stale = false;

        let cur = self.render.cursor();
        let shape = if !cur.visible {
            cursor::HIDDEN
        } else {
            match cur.style {
                vt::CursorStyle::BAR => cursor::BEAM,
                vt::CursorStyle::UNDERLINE => cursor::UNDERLINE,
                vt::CursorStyle::BLOCK_HOLLOW => cursor::HOLLOW_BLOCK,
                _ => cursor::BLOCK,
            }
        };
        let (cx, cy) = cur.at.unwrap_or((0, 0));
        // On the right half of a wide character: draw it over the whole character.
        let cx = if cur.wide_tail {
            cx.saturating_sub(1)
        } else {
            cx
        };
        let wide = cur.at.is_some()
            && self
                .cells
                .get(cy as usize * cols + cx as usize)
                .is_some_and(|c| c.flags & cf::WIDE != 0);
        let t = self.theme;
        let mut info = GridInfo {
            cols: self.cols,
            rows: self.rows,
            cursor_col: cx,
            cursor_row: cy,
            cursor_shape: if cur.at.is_some() {
                shape
            } else {
                cursor::HIDDEN
            },
            cursor_blinking: cur.blinking,
            cursor_wide: wide,
            display_offset: self.display_offset(),
            history_size: self.vt.history() as u32,
            modes: self.modes(),
            foreground: colors.fg,
            background: colors.bg,
            cursor_color: colors.cursor.unwrap_or(t.cursor),
            cursor_text_color: t.cursor_text,
            selection_fg: t.selection_fg,
            selection_bg: t.selection_bg,
            generation: self.info.generation,
        };
        if changed || info != self.info {
            info.generation = self.info.generation + 1;
            self.info = info;
            return true;
        }
        false
    }

    pub fn info(&self) -> &GridInfo {
        &self.info
    }

    pub fn cells(&self) -> &[Cell] {
        &self.cells
    }

    /// Grapheme clusters as of the last snapshot: (row, column, text).
    pub fn clusters(&self) -> impl Iterator<Item = (u16, u16, &str)> {
        self.clusters
            .iter()
            .enumerate()
            .flat_map(|(y, row)| row.iter().map(move |(x, s)| (y as u16, *x, s.as_str())))
    }

    pub fn screen_text(&self) -> String {
        self.vt.screen_text()
    }

    // -----------------------------------------------------------------------------------------
    // Input
    // -----------------------------------------------------------------------------------------

    /// Encode a key for the program. Typing scrolls back to the bottom.
    pub fn key(&mut self, ev: &KeyEvent) -> Vec<u8> {
        let bytes = self.enc.key(&self.vt, ev);
        if !bytes.is_empty() && keys::types_input(ev) {
            self.vt.scroll_bottom();
        }
        bytes
    }

    pub fn paste(&mut self, text: &str) -> Vec<u8> {
        self.vt.scroll_bottom();
        keys::paste(text, self.vt.mode(M_BRACKETED_PASTE))
    }

    /// Focus report, when the program asked for them.
    pub fn focus(&mut self, focused: bool) -> Vec<u8> {
        if self.vt.mode(M_FOCUS) {
            keys::focus(focused)
        } else {
            Vec::new()
        }
    }

    fn grid(&self) -> keys::Grid {
        keys::Grid {
            cols: self.cols,
            rows: self.rows,
            cell_width: self.cell_width,
            cell_height: self.cell_height,
        }
    }

    /// Mouse press/release/motion: reported to the program when it grabbed the mouse (unless
    /// Shift is held), otherwise used for selection.
    pub fn mouse(&mut self, ev: &MouseEvent) -> (MouseOutcome, Vec<u8>) {
        if self.modes() & tm::MOUSE_ANY != 0 && ev.mods & mods::SHIFT == 0 {
            if (ev.kind, ev.button) == (MouseKind::Press, MouseButton::Left) {
                self.clear_selection();
            }
            return match self.enc.mouse(&self.vt, ev, self.grid()) {
                Some(b) => (MouseOutcome::Reported, b),
                None => (MouseOutcome::None, Vec::new()),
            };
        }
        let Some(at) = self.pointer(ev.col, ev.row, ev.right_half, Some((ev.x, ev.y))) else {
            return (MouseOutcome::None, Vec::new());
        };
        match (ev.kind, ev.button) {
            (MouseKind::Press, MouseButton::Left) => {
                if ev.mods & mods::SHIFT != 0
                    && let Some(sel) = self.vt.selection()
                {
                    self.vt.set_selection(Some(vt::Sel {
                        end: at.cell,
                        ..sel
                    }));
                    self.selecting = Some(Drag::Extend);
                } else {
                    let by = match ev.clicks {
                        2 => vt::SelectBy::WORD,
                        3.. => vt::SelectBy::LINE,
                        _ => vt::SelectBy::CELL,
                    };
                    let rect = ev.clicks < 2 && ev.mods & mods::ALT != 0;
                    let words = word_boundaries();
                    let sel = self.vt.gesture_press(at, by, &words);
                    self.vt.set_selection(sel);
                    self.selecting = Some(Drag::Gesture { rect });
                }
                (MouseOutcome::SelectionChanged, Vec::new())
            }
            (MouseKind::Move, _) if self.selecting.is_some() => {
                self.drag_selection(at);
                (MouseOutcome::SelectionChanged, Vec::new())
            }
            (MouseKind::Release, MouseButton::Left) if self.selecting.is_some() => {
                self.selecting = None;
                self.vt.gesture_release(at);
                if self.vt.selection().is_none() {
                    return (MouseOutcome::SelectionChanged, Vec::new());
                }
                (MouseOutcome::SelectionDone, Vec::new())
            }
            _ => (MouseOutcome::None, Vec::new()),
        }
    }

    /// Wheel scrolling (`lines` > 0: towards older output). Returns bytes for the program
    /// when it takes the wheel (mouse reporting, or arrows on the alternate screen);
    /// otherwise scrolls the scrollback.
    pub fn wheel(&mut self, lines: i32, col: u16, row: u16, m: u8) -> Vec<u8> {
        let grabbed = self.modes() & tm::MOUSE_ANY != 0 && m & mods::SHIFT == 0;
        if (grabbed || self.vt.is_alt_screen())
            && let Some(b) = self.enc.wheel(&self.vt, lines, (col, row), m, self.grid())
        {
            return b;
        }
        self.scroll(Scroll::Lines(lines));
        Vec::new()
    }

    pub fn scroll(&mut self, s: Scroll) {
        match s {
            Scroll::Lines(n) => self.vt.scroll_delta(-(n as isize)),
            Scroll::Pages(n) => self
                .vt
                .scroll_delta((-(n as isize)).saturating_mul(self.rows as isize)),
            Scroll::Top => self.vt.scroll_top(),
            Scroll::Bottom => self.vt.scroll_bottom(),
        }
    }

    /// The pointer for a selection gesture at a viewport cell: its pixel position is `px`
    /// when that is inside the cell, else a point on the half `right_half` says.
    fn pointer(
        &self,
        col: u16,
        row: u16,
        right_half: bool,
        px: Option<(u32, u32)>,
    ) -> Option<vt::Pointer> {
        let row = row.min(self.rows - 1);
        let col = col.min(self.cols - 1);
        let line = row as i64 - self.display_offset() as i64;
        let y = self.vt.history() as i64 + line;
        if y < 0 {
            return None;
        }
        let (cw, ch) = (self.cell_width, self.cell_height);
        let (x0, y0) = (
            (col as u32).saturating_mul(cw),
            (row as u32).saturating_mul(ch),
        );
        let x = match px {
            Some((x, _)) if (x0..x0.saturating_add(cw)).contains(&x) => x,
            _ => x0.saturating_add(if right_half {
                (u64::from(cw) * 3 / 4) as u32
            } else {
                cw / 4
            }),
        };
        let py = match px {
            Some((_, y)) if (y0..y0.saturating_add(ch)).contains(&y) => y,
            _ => y0.saturating_add(ch / 2),
        };
        Some(vt::Pointer {
            cell: (col, y as u32),
            px: (x as f64, py as f64),
        })
    }

    fn drag_selection(&mut self, at: vt::Pointer) {
        match self.selecting {
            Some(Drag::Extend) => {
                if let Some(sel) = self.vt.selection() {
                    self.vt.set_selection(Some(vt::Sel {
                        end: at.cell,
                        ..sel
                    }));
                }
            }
            Some(Drag::Gesture { rect }) => {
                let geometry = vt::SelectGeometry {
                    columns: self.cols as u32,
                    cell_width: self.cell_width,
                    padding_left: 0,
                    screen_height: (self.rows as u32).saturating_mul(self.cell_height),
                };
                let words = word_boundaries();
                if let Some(sel) = self.vt.gesture_drag(at, geometry, rect, &words) {
                    self.vt.set_selection(Some(sel));
                }
            }
            None => {}
        }
    }

    pub fn has_selection(&self) -> bool {
        self.vt.selection().is_some()
    }

    pub fn clear_selection(&mut self) -> bool {
        self.selecting = None;
        if !self.has_selection() {
            return false;
        }
        self.vt.set_selection(None);
        true
    }

    pub fn select_all(&mut self) {
        self.vt.select_all();
    }

    pub fn selection_text(&self) -> String {
        self.vt.selection_text()
    }

    /// Forget everything shown (a new session in the same view).
    pub fn reset(&mut self) {
        self.vt.reset();
        self.vt.scroll_bottom();
        self.selecting = None;
        self.stale = true;
    }
}

fn word_boundaries() -> Vec<u32> {
    WORD_SEPARATORS.chars().map(u32::from).collect()
}

fn dim(c: u32) -> u32 {
    let f = |v: u32| (v as f32 * 0.66) as u32;
    f((c >> 16) & 0xff) << 16 | f((c >> 8) & 0xff) << 8 | f(c & 0xff)
}

/// A render-state cell as drawn: colors resolved, inverse and faint applied.
fn convert(c: &vt::RenderCell, colors: &vt::RenderColors) -> Cell {
    let style = c.style.unwrap_or_else(vt::default_style);
    let f = style.flags;
    let has = |bit: u16| f & bit != 0;
    let mut fg = c.fg.unwrap_or(colors.fg);
    let mut bg = c.bg.unwrap_or(colors.bg);
    let mut flags = 0u16;
    if has(ffi::STYLE_FLAG_INVERSE) {
        std::mem::swap(&mut fg, &mut bg);
    } else if c.bg.is_none() {
        flags |= cf::DEFAULT_BG;
    }
    if has(ffi::STYLE_FLAG_FAINT) {
        fg = dim(fg);
        flags |= cf::DIM;
    }
    for (bit, flag) in [
        (ffi::STYLE_FLAG_BOLD, cf::BOLD),
        (ffi::STYLE_FLAG_ITALIC, cf::ITALIC),
        (ffi::STYLE_FLAG_STRIKETHROUGH, cf::STRIKEOUT),
        (ffi::STYLE_FLAG_INVISIBLE, cf::HIDDEN),
    ] {
        if has(bit) {
            flags |= flag;
        }
    }
    flags |= match (f & ffi::STYLE_UNDERLINE_MASK) >> ffi::STYLE_UNDERLINE_SHIFT {
        1 => cf::UNDERLINE,
        2 => cf::DOUBLE_UNDERLINE,
        3 => cf::UNDERCURL,
        4 => cf::DOTTED_UNDERLINE,
        5 => cf::DASHED_UNDERLINE,
        _ => 0,
    };
    flags |= match c.wide {
        vt::WIDE => cf::WIDE,
        vt::SPACER_TAIL | vt::SPACER_HEAD => cf::WIDE_SPACER,
        _ => 0,
    };
    if c.selected {
        flags |= cf::SELECTED;
    }
    let ul = match style.underline_color.tag {
        t if t == ffi::StyleColorTag::PALETTE as u8 => {
            colors.palette[unsafe { style.underline_color.value.palette } as usize]
        }
        t if t == ffi::StyleColorTag::RGB as u8 => {
            vt::from_rgb(unsafe { style.underline_color.value.rgb })
        }
        _ => NO_COLOR,
    };
    Cell {
        ch: if c.codepoint == 0 {
            ' ' as u32
        } else {
            c.codepoint
        },
        fg,
        bg,
        ul,
        flags,
        link: 0,
    }
}

/// Wraps a key description from the C side.
pub fn key_event(
    kind: u32,
    code: u32,
    m: u8,
    action: u8,
    text: &str,
    base_layout: u32,
) -> Option<KeyEvent> {
    let key = match kind {
        0 => keys::Key::Char(char::from_u32(code)?),
        _ => keys::Key::Named(code),
    };
    Some(KeyEvent {
        key,
        mods: m,
        action: match action {
            1 => keys::KeyAction::Repeat,
            2 => keys::KeyAction::Release,
            _ => keys::KeyAction::Press,
        },
        text: text.to_owned(),
        base_layout: char::from_u32(base_layout).filter(|_| base_layout != 0),
    })
}
