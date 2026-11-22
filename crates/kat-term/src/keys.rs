//! Key, mouse, focus and paste encoding, by libghostty-vt's encoders (legacy xterm sequences
//! and the kitty keyboard protocol, configured from the terminal's modes). Trimmed from
//! Thurm's `thurm-term` (keys.rs).

use std::ptr;

use ghostty_vt_rs_sys as ffi;

use crate::vt::{self, Vt};

/// Modifier bits (the kitty protocol's: shift 1, alt 2, ctrl 4, super 8, caps lock 64, num
/// lock 128); `KAT_MOD_*` in kat_term.h.
pub mod mods {
    pub const SHIFT: u8 = 1;
    pub const ALT: u8 = 2;
    pub const CTRL: u8 = 4;
    pub const SUPER: u8 = 8;
    pub const CAPS_LOCK: u8 = 64;
    pub const NUM_LOCK: u8 = 128;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyAction {
    Press,
    Repeat,
    Release,
}

/// A key: a character (its unshifted lowercase codepoint: 'a' for Shift+A) or a named key
/// (`KAT_KEY_*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key {
    Char(char),
    Named(u32),
}

#[derive(Clone, Debug)]
pub struct KeyEvent {
    pub key: Key,
    pub mods: u8,
    pub action: KeyAction,
    /// Text the key produces with the current modifiers and layout ("" for none).
    pub text: String,
    /// Base-layout (US) key for non-Latin layouts.
    pub base_layout: Option<char>,
}

/// Named keys (`KAT_KEY_*`).
pub mod named {
    pub const ESCAPE: u32 = 1;
    pub const ENTER: u32 = 2;
    pub const TAB: u32 = 3;
    pub const BACKSPACE: u32 = 4;
    pub const INSERT: u32 = 5;
    pub const DELETE: u32 = 6;
    pub const LEFT: u32 = 7;
    pub const RIGHT: u32 = 8;
    pub const UP: u32 = 9;
    pub const DOWN: u32 = 10;
    pub const PAGE_UP: u32 = 11;
    pub const PAGE_DOWN: u32 = 12;
    pub const HOME: u32 = 13;
    pub const END: u32 = 14;
    pub const CAPS_LOCK: u32 = 15;
    pub const SCROLL_LOCK: u32 = 16;
    pub const NUM_LOCK: u32 = 17;
    pub const PRINT_SCREEN: u32 = 18;
    pub const PAUSE: u32 = 19;
    pub const MENU: u32 = 20;
    /// KP_0 ... KP_9 = 21 ... 30.
    pub const KP_0: u32 = 21;
    pub const KP_DECIMAL: u32 = 31;
    pub const KP_DIVIDE: u32 = 32;
    pub const KP_MULTIPLY: u32 = 33;
    pub const KP_SUBTRACT: u32 = 34;
    pub const KP_ADD: u32 = 35;
    pub const KP_ENTER: u32 = 36;
    pub const KP_EQUAL: u32 = 37;
    pub const LEFT_SHIFT: u32 = 38;
    pub const LEFT_CONTROL: u32 = 39;
    pub const LEFT_ALT: u32 = 40;
    pub const LEFT_SUPER: u32 = 41;
    pub const RIGHT_SHIFT: u32 = 42;
    pub const RIGHT_CONTROL: u32 = 43;
    pub const RIGHT_ALT: u32 = 44;
    pub const RIGHT_SUPER: u32 = 45;
    /// F1 ... F25 = 100 ... 124.
    pub const F1: u32 = 100;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MouseKind {
    Press,
    Release,
    Move,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MouseButton {
    Left,
    Middle,
    Right,
    Back,
    Forward,
    None,
}

#[derive(Clone, Copy, Debug)]
pub struct MouseEvent {
    pub kind: MouseKind,
    pub button: MouseButton,
    /// Viewport cell.
    pub col: u16,
    pub row: u16,
    /// Pointer is on the right half of the cell (selection side).
    pub right_half: bool,
    pub mods: u8,
    /// 1 = single, 2 = double (word), 3 = triple (line) click.
    pub clicks: u8,
    /// Pixel position inside the grid.
    pub x: u32,
    pub y: u32,
}

/// libghostty-vt's key and mouse encoders, with an event of each kind to fill.
pub struct Encoders {
    key: ffi::KeyEncoder,
    key_event: ffi::KeyEvent,
    mouse: ffi::MouseEncoder,
    mouse_event: ffi::MouseEvent,
}

// Only used behind the terminal's mutex.
unsafe impl Send for Encoders {}

impl Drop for Encoders {
    fn drop(&mut self) {
        unsafe {
            if !self.key_event.is_null() {
                ffi::ghostty_key_event_free(self.key_event);
            }
            if !self.key.is_null() {
                ffi::ghostty_key_encoder_free(self.key);
            }
            if !self.mouse_event.is_null() {
                ffi::ghostty_mouse_event_free(self.mouse_event);
            }
            if !self.mouse.is_null() {
                ffi::ghostty_mouse_encoder_free(self.mouse);
            }
        }
    }
}

/// Where the grid is, for mouse positions: its size in cells and a cell's size in pixels.
#[derive(Clone, Copy, Debug)]
pub struct Grid {
    pub cols: u16,
    pub rows: u16,
    pub cell_width: u32,
    pub cell_height: u32,
}

impl Encoders {
    pub fn new() -> Option<Self> {
        let mut e = Self {
            key: ptr::null_mut(),
            key_event: ptr::null_mut(),
            mouse: ptr::null_mut(),
            mouse_event: ptr::null_mut(),
        };
        unsafe {
            if ffi::ghostty_key_encoder_new(ptr::null(), &mut e.key) != ffi::Result::SUCCESS
                || e.key.is_null()
            {
                return None;
            }
            if ffi::ghostty_key_event_new(ptr::null(), &mut e.key_event) != ffi::Result::SUCCESS
                || e.key_event.is_null()
            {
                return None;
            }
            if ffi::ghostty_mouse_encoder_new(ptr::null(), &mut e.mouse) != ffi::Result::SUCCESS
                || e.mouse.is_null()
            {
                return None;
            }
            if ffi::ghostty_mouse_event_new(ptr::null(), &mut e.mouse_event) != ffi::Result::SUCCESS
                || e.mouse_event.is_null()
            {
                return None;
            }
        }
        Some(e)
    }

    /// Bytes for the program for a key, in the terminal's current modes (legacy or the kitty
    /// keyboard protocol, as the program asked).
    pub fn key(&mut self, vt: &Vt, ev: &KeyEvent) -> Vec<u8> {
        let (key, unshifted) = match ev.key {
            Key::Char(c) => (physical_key(ev.base_layout.unwrap_or(c)), c as u32),
            Key::Named(k) => (named_key(k), 0),
        };
        let m = ghostty_mods(ev.mods);
        // Shift is used up producing a shifted character ("A", "!"); the encoder reports it
        // only where the protocol wants it.
        let consumed = match ev.key {
            Key::Char(c)
                if ev.mods & mods::SHIFT != 0
                    && !ev.text.is_empty()
                    && ev.text != c.to_string() =>
            {
                ffi::MODS_SHIFT
            }
            _ => 0,
        };
        // The key's text as the encoder wants it: the character itself where the app sends
        // none or a control character (Ctrl combinations).
        let text = match ev.key {
            Key::Char(c) if ev.text.is_empty() || ev.text.chars().all(char::is_control) => {
                c.to_string()
            }
            _ => ev.text.clone(),
        };
        let action = match ev.action {
            KeyAction::Press => ffi::KeyAction::PRESS,
            KeyAction::Repeat => ffi::KeyAction::REPEAT,
            KeyAction::Release => ffi::KeyAction::RELEASE,
        };
        let mut out = unsafe {
            ffi::ghostty_key_encoder_set_from_terminal(self.key, vt.raw());
            // Option-as-Alt is decided by the app: ALT only arrives when it applies.
            let alt = ffi::OptionAsAlt::TRUE;
            ffi::ghostty_key_encoder_set(
                self.key,
                ffi::KeyEncoderOption::MACOS_OPTION_AS_ALT,
                (&alt as *const ffi::OptionAsAlt::Type).cast(),
            );
            let ev_ = self.key_event;
            ffi::ghostty_key_event_set_action(ev_, action);
            ffi::ghostty_key_event_set_key(ev_, key);
            ffi::ghostty_key_event_set_mods(ev_, m);
            ffi::ghostty_key_event_set_consumed_mods(ev_, consumed);
            ffi::ghostty_key_event_set_unshifted_codepoint(ev_, unshifted);
            ffi::ghostty_key_event_set_utf8(
                ev_,
                ffi::String {
                    ptr: text.as_ptr(),
                    len: text.len(),
                },
            );
            encode(|buf, len, out| ffi::ghostty_key_encoder_encode(self.key, ev_, buf, len, out))
        };
        // Line feed/new line mode (LNM): Return sends CR LF.
        if out == b"\r" && vt.mode(vt::mode(20, true)) {
            out.push(b'\n');
        }
        out
    }

    /// Mouse press/release/motion for an application that grabbed the mouse, if its mode
    /// reports this event.
    pub fn mouse(&mut self, vt: &Vt, ev: &MouseEvent, grid: Grid) -> Option<Vec<u8>> {
        let action = match ev.kind {
            MouseKind::Press => ffi::MouseAction::PRESS,
            MouseKind::Release => ffi::MouseAction::RELEASE,
            MouseKind::Move => ffi::MouseAction::MOTION,
        };
        let button = match ev.button {
            MouseButton::Left => Some(ffi::MouseButton::LEFT),
            MouseButton::Middle => Some(ffi::MouseButton::MIDDLE),
            MouseButton::Right => Some(ffi::MouseButton::RIGHT),
            MouseButton::Back => Some(ffi::MouseButton::EIGHT),
            MouseButton::Forward => Some(ffi::MouseButton::NINE),
            MouseButton::None => None,
        };
        // Within the cell the app put the pointer in (the pixel position may be anywhere).
        let (cw, ch) = (grid.cell_width.max(1), grid.cell_height.max(1));
        let x0 = (ev.col as u32).saturating_mul(cw);
        let y0 = (ev.row as u32).saturating_mul(ch);
        let x = x0.saturating_add(ev.x.saturating_sub(x0).min(cw - 1));
        let y = y0.saturating_add(ev.y.saturating_sub(y0).min(ch - 1));
        self.encode_mouse(
            vt,
            action,
            button,
            ev.mods,
            (x, y),
            grid,
            ev.button != MouseButton::None,
        )
    }

    /// Wheel movement (`lines` > 0: up) at a cell. For an application that grabbed the mouse
    /// this is buttons 4 / 5; on the alternate screen with alternate scroll mode (1007), arrow
    /// keys. `None` when neither applies.
    pub fn wheel(
        &mut self,
        vt: &Vt,
        lines: i32,
        (col, row): (u16, u16),
        m: u8,
        grid: Grid,
    ) -> Option<Vec<u8>> {
        let lines = lines.clamp(-1024, 1024);
        if lines == 0 {
            return None;
        }
        let tracking = [9, 1000, 1002, 1003]
            .iter()
            .any(|&n| vt.mode(vt::mode(n, false)));
        let mut out = Vec::new();
        if tracking {
            let button = if lines > 0 {
                ffi::MouseButton::FOUR
            } else {
                ffi::MouseButton::FIVE
            };
            let pos = (
                (col as u32).saturating_mul(grid.cell_width.max(1)),
                (row as u32).saturating_mul(grid.cell_height.max(1)),
            );
            for _ in 0..lines.unsigned_abs() {
                out.extend(self.encode_mouse(
                    vt,
                    ffi::MouseAction::PRESS,
                    Some(button),
                    m,
                    pos,
                    grid,
                    false,
                )?);
            }
            return Some(out);
        }
        let alternate_scroll = vt.is_alt_screen() && vt.mode(vt::mode(1007, false));
        if !alternate_scroll {
            return None;
        }
        let arrow = KeyEvent {
            key: Key::Named(if lines > 0 { named::UP } else { named::DOWN }),
            mods: 0,
            action: KeyAction::Press,
            text: String::new(),
            base_layout: None,
        };
        for _ in 0..lines.unsigned_abs() {
            out.extend(self.key(vt, &arrow));
        }
        Some(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_mouse(
        &mut self,
        vt: &Vt,
        action: ffi::MouseAction::Type,
        button: Option<ffi::MouseButton::Type>,
        m: u8,
        (x, y): (u32, u32),
        grid: Grid,
        pressed: bool,
    ) -> Option<Vec<u8>> {
        let size = ffi::MouseEncoderSize {
            size: std::mem::size_of::<ffi::MouseEncoderSize>(),
            screen_width: (grid.cols as u32).saturating_mul(grid.cell_width.max(1)),
            screen_height: (grid.rows as u32).saturating_mul(grid.cell_height.max(1)),
            cell_width: grid.cell_width.max(1),
            cell_height: grid.cell_height.max(1),
            ..Default::default()
        };
        let out = unsafe {
            ffi::ghostty_mouse_encoder_set_from_terminal(self.mouse, vt.raw());
            ffi::ghostty_mouse_encoder_set(
                self.mouse,
                ffi::MouseEncoderOption::SIZE,
                (&size as *const ffi::MouseEncoderSize).cast(),
            );
            let any = pressed && action == ffi::MouseAction::MOTION;
            ffi::ghostty_mouse_encoder_set(
                self.mouse,
                ffi::MouseEncoderOption::ANY_BUTTON_PRESSED,
                (&any as *const bool).cast(),
            );
            let ev = self.mouse_event;
            ffi::ghostty_mouse_event_set_action(ev, action);
            match button {
                Some(b) => ffi::ghostty_mouse_event_set_button(ev, b),
                None => ffi::ghostty_mouse_event_clear_button(ev),
            };
            ffi::ghostty_mouse_event_set_mods(ev, ghostty_mods(m));
            ffi::ghostty_mouse_event_set_position(
                ev,
                ffi::MousePosition {
                    x: x as f32,
                    y: y as f32,
                },
            );
            encode(|buf, len, out| ffi::ghostty_mouse_encoder_encode(self.mouse, ev, buf, len, out))
        };
        (!out.is_empty()).then_some(out)
    }
}

/// Focus in / out report (for mode 1004, which the caller checks).
pub fn focus(focused: bool) -> Vec<u8> {
    let ev = if focused {
        ffi::FocusEvent::GAINED
    } else {
        ffi::FocusEvent::LOST
    };
    unsafe { encode(|buf, len, out| ffi::ghostty_focus_encode(ev, buf, len, out)) }
}

/// Pasted text for the program: unsafe control bytes replaced, bracketed when the mode is on
/// (otherwise newlines become carriage returns).
pub fn paste(text: &str, bracketed: bool) -> Vec<u8> {
    let mut data = text.as_bytes().to_vec();
    unsafe {
        encode(|buf, len, out| {
            ffi::ghostty_paste_encode(
                data.as_mut_ptr().cast(),
                data.len(),
                bracketed,
                buf,
                len,
                out,
            )
        })
    }
}

/// Runs a libghostty-vt encode call, growing the buffer when it's too small.
unsafe fn encode(mut f: impl FnMut(*mut u8, usize, *mut usize) -> ffi::Result::Type) -> Vec<u8> {
    let mut buf = vec![0u8; 64];
    let mut len = 0usize;
    let mut r = f(buf.as_mut_ptr(), buf.len(), &mut len);
    if r == ffi::Result::OUT_OF_SPACE {
        buf.resize(len, 0);
        r = f(buf.as_mut_ptr(), buf.len(), &mut len);
    }
    if r != ffi::Result::SUCCESS {
        return Vec::new();
    }
    buf.truncate(len);
    buf
}

fn ghostty_mods(m: u8) -> ffi::Mods {
    let mut g = 0;
    for (t, f) in [
        (mods::SHIFT, ffi::MODS_SHIFT),
        (mods::CTRL, ffi::MODS_CTRL),
        (mods::ALT, ffi::MODS_ALT),
        (mods::SUPER, ffi::MODS_SUPER),
        (mods::CAPS_LOCK, ffi::MODS_CAPS_LOCK),
        (mods::NUM_LOCK, ffi::MODS_NUM_LOCK),
    ] {
        if m & t != 0 {
            g |= f;
        }
    }
    g
}

/// The US-layout physical key producing `c` unshifted.
fn physical_key(c: char) -> ffi::Key::Type {
    use ffi::Key as K;
    match c {
        'a'..='z' => K::A + (c as u32 - 'a' as u32) as ffi::Key::Type,
        '0'..='9' => K::DIGIT_0 + (c as u32 - '0' as u32) as ffi::Key::Type,
        '`' => K::BACKQUOTE,
        '\\' => K::BACKSLASH,
        '[' => K::BRACKET_LEFT,
        ']' => K::BRACKET_RIGHT,
        ',' => K::COMMA,
        '=' => K::EQUAL,
        '-' => K::MINUS,
        '.' => K::PERIOD,
        '\'' => K::QUOTE,
        ';' => K::SEMICOLON,
        '/' => K::SLASH,
        ' ' => K::SPACE,
        _ => K::UNIDENTIFIED,
    }
}

fn named_key(k: u32) -> ffi::Key::Type {
    use ffi::Key as K;
    use named as N;
    match k {
        N::ESCAPE => K::ESCAPE,
        N::ENTER => K::ENTER,
        N::TAB => K::TAB,
        N::BACKSPACE => K::BACKSPACE,
        N::INSERT => K::INSERT,
        N::DELETE => K::DELETE,
        N::LEFT => K::ARROW_LEFT,
        N::RIGHT => K::ARROW_RIGHT,
        N::UP => K::ARROW_UP,
        N::DOWN => K::ARROW_DOWN,
        N::PAGE_UP => K::PAGE_UP,
        N::PAGE_DOWN => K::PAGE_DOWN,
        N::HOME => K::HOME,
        N::END => K::END,
        N::CAPS_LOCK => K::CAPS_LOCK,
        N::SCROLL_LOCK => K::SCROLL_LOCK,
        N::NUM_LOCK => K::NUM_LOCK,
        N::PRINT_SCREEN => K::PRINT_SCREEN,
        N::PAUSE => K::PAUSE,
        N::MENU => K::CONTEXT_MENU,
        n @ N::KP_0..=30 => K::NUMPAD_0 + (n - N::KP_0) as ffi::Key::Type,
        N::KP_DECIMAL => K::NUMPAD_DECIMAL,
        N::KP_DIVIDE => K::NUMPAD_DIVIDE,
        N::KP_MULTIPLY => K::NUMPAD_MULTIPLY,
        N::KP_SUBTRACT => K::NUMPAD_SUBTRACT,
        N::KP_ADD => K::NUMPAD_ADD,
        N::KP_ENTER => K::NUMPAD_ENTER,
        N::KP_EQUAL => K::NUMPAD_EQUAL,
        N::LEFT_SHIFT => K::SHIFT_LEFT,
        N::LEFT_CONTROL => K::CONTROL_LEFT,
        N::LEFT_ALT => K::ALT_LEFT,
        N::LEFT_SUPER => K::META_LEFT,
        N::RIGHT_SHIFT => K::SHIFT_RIGHT,
        N::RIGHT_CONTROL => K::CONTROL_RIGHT,
        N::RIGHT_ALT => K::ALT_RIGHT,
        N::RIGHT_SUPER => K::META_RIGHT,
        n @ N::F1..=124 => K::F1 + (n - N::F1) as ffi::Key::Type,
        _ => K::UNIDENTIFIED,
    }
}

/// Whether a key event types input (as opposed to a lone modifier or a release).
pub fn types_input(ev: &KeyEvent) -> bool {
    ev.action != KeyAction::Release
        && !matches!(
            ev.key,
            Key::Named(
                named::LEFT_SHIFT
                    | named::LEFT_CONTROL
                    | named::LEFT_ALT
                    | named::LEFT_SUPER
                    | named::RIGHT_SHIFT
                    | named::RIGHT_CONTROL
                    | named::RIGHT_ALT
                    | named::RIGHT_SUPER
                    | named::CAPS_LOCK
                    | named::NUM_LOCK
            )
        )
}
