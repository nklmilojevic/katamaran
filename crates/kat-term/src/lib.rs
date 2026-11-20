//! C ABI of the Mac app's terminal engine (`include/kat_term.h`): a libghostty-vt terminal
//! per container shell. Every call locks the terminal, so output can be fed from a reader
//! thread while the view takes snapshots on the main thread.

pub mod keys;
pub mod term;
pub mod vt;

use std::ffi::{CStr, c_char};
use std::sync::{Mutex, MutexGuard};

pub use term::{Cell, GridInfo, MouseOutcome, Scroll, Term, Theme};

/// The handle the app holds.
pub struct KatTerm(Mutex<Term>);

impl KatTerm {
    fn lock(&self) -> MutexGuard<'_, Term> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Bytes owned by the library; free with `kat_bytes_free`.
#[repr(C)]
pub struct KatBytes {
    pub ptr: *mut u8,
    pub len: usize,
}

impl KatBytes {
    fn from(v: Vec<u8>) -> Self {
        if v.is_empty() {
            return Self {
                ptr: std::ptr::null_mut(),
                len: 0,
            };
        }
        let b = v.into_boxed_slice();
        let len = b.len();
        Self {
            ptr: Box::into_raw(b).cast(),
            len,
        }
    }
}

#[repr(C)]
pub struct KatKeyEvent {
    /// 0 = text key (`code`: the unshifted Unicode scalar, 'a' for Shift+A), 1 = named key
    /// (`code`: KAT_KEY_*).
    pub kind: u32,
    pub code: u32,
    pub mods: u8,
    /// 0 press, 1 repeat, 2 release.
    pub action: u8,
    /// UTF-8 text the key produces (after Option / input methods), may be NULL.
    pub text: *const c_char,
    /// The key in the US layout (for non-Latin layouts), 0 when none.
    pub base_layout: u32,
}

#[repr(C)]
pub struct KatMouseEvent {
    /// 0 press, 1 release, 2 move.
    pub kind: u8,
    /// 0 left, 1 middle, 2 right, 3 back, 4 forward, 5 none.
    pub button: u8,
    pub mods: u8,
    pub clicks: u8,
    pub col: u16,
    pub row: u16,
    pub right_half: bool,
    pub x: u32,
    pub y: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KatCluster {
    pub row: u16,
    pub col: u16,
    /// UTF-8, `len` bytes (clusters longer than the buffer are cut at a character).
    pub len: u8,
    pub text: [u8; 59],
}

unsafe fn term<'a>(t: *const KatTerm) -> Option<&'a KatTerm> {
    unsafe { t.as_ref() }
}

/// # Safety
/// `ptr` must be valid for `len` bytes (or `len` zero).
unsafe fn slice<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

unsafe fn str_arg<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        ""
    } else {
        unsafe { CStr::from_ptr(s) }.to_str().unwrap_or("")
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn kat_term_new(cols: u16, rows: u16, scrollback: u32) -> *mut KatTerm {
    Box::into_raw(Box::new(KatTerm(Mutex::new(Term::new(
        cols,
        rows,
        scrollback as usize,
    )))))
}

/// # Safety
/// `t` must come from `kat_term_new` and not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_free(t: *mut KatTerm) {
    if !t.is_null() {
        drop(unsafe { Box::from_raw(t) });
    }
}

/// # Safety
/// `b` must come from this library, once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_bytes_free(b: KatBytes) {
    if !b.ptr.is_null() {
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(b.ptr, b.len)) });
    }
}

/// # Safety
/// `t` from `kat_term_new`; `bytes` valid for `len`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_feed(t: *const KatTerm, bytes: *const u8, len: usize) {
    if let Some(t) = unsafe { term(t) } {
        t.lock().feed(unsafe { slice(bytes, len) });
    }
}

/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_take_replies(t: *const KatTerm) -> KatBytes {
    KatBytes::from(
        unsafe { term(t) }
            .map(|t| t.lock().take_replies())
            .unwrap_or_default(),
    )
}

/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_take_bell(t: *const KatTerm) -> bool {
    unsafe { term(t) }.is_some_and(|t| t.lock().take_bell())
}

/// The title the program set (UTF-8, empty for none); `changed` (may be NULL) tells whether
/// it changed since the last call.
///
/// # Safety
/// `t` from `kat_term_new`; `changed` NULL or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_title(t: *const KatTerm, changed: *mut bool) -> KatBytes {
    let Some(t) = (unsafe { term(t) }) else {
        return KatBytes::from(Vec::new());
    };
    let (title, did) = t.lock().title();
    if let Some(c) = unsafe { changed.as_mut() } {
        *c = did;
    }
    KatBytes::from(title.into_bytes())
}

/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_resize(
    t: *const KatTerm,
    cols: u16,
    rows: u16,
    cell_width: u32,
    cell_height: u32,
) {
    if let Some(t) = unsafe { term(t) } {
        t.lock().resize(cols, rows, cell_width, cell_height);
    }
}

/// # Safety
/// `t` from `kat_term_new`; `theme` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_set_theme(t: *const KatTerm, theme: *const Theme) {
    if let (Some(t), Some(theme)) = (unsafe { term(t) }, unsafe { theme.as_ref() }) {
        t.lock().set_theme(*theme);
    }
}

/// Brings the grid up to date. Returns false when nothing changed since `info->generation`
/// (`info` untouched). Otherwise fills `info` and, when `cap` >= cols * rows, `cells` (row
/// major); with a smaller `cap` only `info`, so the caller can size the buffer and call again.
///
/// # Safety
/// `t` from `kat_term_new`; `info` valid; `cells` valid for `cap` cells.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_snapshot(
    t: *const KatTerm,
    info: *mut GridInfo,
    cells: *mut Cell,
    cap: usize,
) -> bool {
    let (Some(t), Some(info)) = (unsafe { term(t) }, unsafe { info.as_mut() }) else {
        return false;
    };
    let mut term = t.lock();
    term.snapshot();
    let current = *term.info();
    if current.generation == info.generation
        && current.cols == info.cols
        && current.rows == info.rows
    {
        return false;
    }
    *info = current;
    let src = term.cells();
    if !cells.is_null() && cap >= src.len() {
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), cells, src.len()) };
    }
    true
}

/// Grapheme clusters (base character plus combining ones) of the last snapshot. Copies up to
/// `max` into `out` and returns the total.
///
/// # Safety
/// `t` from `kat_term_new`; `out` valid for `max` (or NULL with `max` 0).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_clusters(
    t: *const KatTerm,
    out: *mut KatCluster,
    max: usize,
) -> usize {
    let Some(t) = (unsafe { term(t) }) else {
        return 0;
    };
    let term = t.lock();
    let mut n = 0;
    for (row, col, text) in term.clusters() {
        if n < max && !out.is_null() {
            let mut c = KatCluster {
                row,
                col,
                len: 0,
                text: [0; 59],
            };
            let mut len = 0;
            for ch in text.chars() {
                let w = ch.len_utf8();
                if len + w > c.text.len() {
                    break;
                }
                ch.encode_utf8(&mut c.text[len..]);
                len += w;
            }
            c.len = len as u8;
            unsafe { out.add(n).write(c) };
        }
        n += 1;
    }
    n
}

/// Bytes for the program for a key, in the terminal's current modes.
///
/// # Safety
/// `t` from `kat_term_new`; `ev` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_key(t: *const KatTerm, ev: *const KatKeyEvent) -> KatBytes {
    let (Some(t), Some(ev)) = (unsafe { term(t) }, unsafe { ev.as_ref() }) else {
        return KatBytes::from(Vec::new());
    };
    let text = unsafe { str_arg(ev.text) };
    match term::key_event(ev.kind, ev.code, ev.mods, ev.action, text, ev.base_layout) {
        Some(k) => KatBytes::from(t.lock().key(&k)),
        None => KatBytes::from(Vec::new()),
    }
}

/// Pasted text as the program should get it (bracketed when it asked).
///
/// # Safety
/// `t` from `kat_term_new`; `text` NUL-terminated UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_paste(t: *const KatTerm, text: *const c_char) -> KatBytes {
    let Some(t) = (unsafe { term(t) }) else {
        return KatBytes::from(Vec::new());
    };
    KatBytes::from(t.lock().paste(unsafe { str_arg(text) }))
}

/// Focus in / out report, empty unless the program asked for them.
///
/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_focus(t: *const KatTerm, focused: bool) -> KatBytes {
    KatBytes::from(
        unsafe { term(t) }
            .map(|t| t.lock().focus(focused))
            .unwrap_or_default(),
    )
}

/// A mouse event: reported to the program (`KAT_MOUSE_REPORTED`, bytes in `out`) or used for
/// selection.
///
/// # Safety
/// `t` from `kat_term_new`; `ev` valid; `out` valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_mouse(
    t: *const KatTerm,
    ev: *const KatMouseEvent,
    out: *mut KatBytes,
) -> u8 {
    let (Some(t), Some(e)) = (unsafe { term(t) }, unsafe { ev.as_ref() }) else {
        return MouseOutcome::None as u8;
    };
    use keys::{MouseButton as B, MouseKind as K};
    let ev = keys::MouseEvent {
        kind: match e.kind {
            0 => K::Press,
            1 => K::Release,
            _ => K::Move,
        },
        button: match e.button {
            0 => B::Left,
            1 => B::Middle,
            2 => B::Right,
            3 => B::Back,
            4 => B::Forward,
            _ => B::None,
        },
        col: e.col,
        row: e.row,
        right_half: e.right_half,
        mods: e.mods,
        clicks: e.clicks,
        x: e.x,
        y: e.y,
    };
    let (outcome, bytes) = t.lock().mouse(&ev);
    if let Some(o) = unsafe { out.as_mut() } {
        *o = KatBytes::from(bytes);
    }
    outcome as u8
}

/// The wheel, in whole lines (positive: towards older output). Returns bytes when the program
/// takes it; otherwise the scrollback scrolls.
///
/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_wheel(
    t: *const KatTerm,
    lines: i32,
    col: u16,
    row: u16,
    mods: u8,
) -> KatBytes {
    KatBytes::from(
        unsafe { term(t) }
            .map(|t| t.lock().wheel(lines, col, row, mods))
            .unwrap_or_default(),
    )
}

/// Scroll the viewport: `KAT_SCROLL_LINES` / `KAT_SCROLL_PAGES` by `n` (positive: older),
/// `KAT_SCROLL_TOP`, `KAT_SCROLL_BOTTOM`.
///
/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_scroll(t: *const KatTerm, kind: u8, n: i32) {
    if let Some(t) = unsafe { term(t) } {
        t.lock().scroll(match kind {
            0 => Scroll::Lines(n),
            1 => Scroll::Pages(n),
            2 => Scroll::Top,
            _ => Scroll::Bottom,
        });
    }
}

/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_selection_text(t: *const KatTerm) -> KatBytes {
    KatBytes::from(
        unsafe { term(t) }
            .map(|t| t.lock().selection_text().into_bytes())
            .unwrap_or_default(),
    )
}

/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_has_selection(t: *const KatTerm) -> bool {
    unsafe { term(t) }.is_some_and(|t| t.lock().has_selection())
}

/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_select_all(t: *const KatTerm) {
    if let Some(t) = unsafe { term(t) } {
        t.lock().select_all();
    }
}

/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_clear_selection(t: *const KatTerm) -> bool {
    unsafe { term(t) }.is_some_and(|t| t.lock().clear_selection())
}

/// The active screen as plain text.
///
/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_screen_text(t: *const KatTerm) -> KatBytes {
    KatBytes::from(
        unsafe { term(t) }
            .map(|t| t.lock().screen_text().into_bytes())
            .unwrap_or_default(),
    )
}

/// Clears screen, scrollback and modes (a new session in the same view).
///
/// # Safety
/// `t` from `kat_term_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kat_term_reset(t: *const KatTerm) {
    if let Some(t) = unsafe { term(t) } {
        t.lock().reset();
    }
}

#[cfg(test)]
mod tests;
