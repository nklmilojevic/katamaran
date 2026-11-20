//! The C ABI as the app uses it: output in, cells out; keys, paste, mouse and selection.

use std::ffi::CString;

use super::*;
use crate::term::{cf, cursor, tm};

struct T(*mut KatTerm);

impl Drop for T {
    fn drop(&mut self) {
        unsafe { kat_term_free(self.0) }
    }
}

impl T {
    fn new(cols: u16, rows: u16) -> Self {
        let t = T(kat_term_new(cols, rows, 1000));
        unsafe { kat_term_resize(t.0, cols, rows, 10, 20) };
        t
    }

    fn feed(&self, s: &str) {
        unsafe { kat_term_feed(self.0, s.as_ptr(), s.len()) }
    }

    fn snapshot(&self) -> (GridInfo, Vec<Cell>) {
        let mut info = GridInfo::default();
        assert!(unsafe { kat_term_snapshot(self.0, &mut info, std::ptr::null_mut(), 0) });
        let mut cells = vec![Cell::default(); info.cols as usize * info.rows as usize];
        info.generation = 0;
        assert!(unsafe { kat_term_snapshot(self.0, &mut info, cells.as_mut_ptr(), cells.len()) });
        (info, cells)
    }

    fn row(&self, row: usize) -> String {
        let (info, cells) = self.snapshot();
        let cols = info.cols as usize;
        cells[row * cols..(row + 1) * cols]
            .iter()
            .filter(|c| c.flags & cf::WIDE_SPACER == 0)
            .map(|c| char::from_u32(c.ch).unwrap_or('?'))
            .collect::<String>()
            .trim_end()
            .to_owned()
    }

    fn key(&self, kind: u32, code: u32, mods: u8, text: &str) -> Vec<u8> {
        let text = CString::new(text).unwrap();
        let ev = KatKeyEvent {
            kind,
            code,
            mods,
            action: 0,
            text: text.as_ptr(),
            base_layout: 0,
        };
        take(unsafe { kat_term_key(self.0, &ev) })
    }

    fn mouse(
        &self,
        kind: u8,
        button: u8,
        col: u16,
        row: u16,
        mods: u8,
        clicks: u8,
    ) -> (u8, Vec<u8>) {
        let ev = KatMouseEvent {
            kind,
            button,
            mods,
            clicks,
            col,
            row,
            right_half: false,
            x: col as u32 * 10 + 2,
            y: row as u32 * 20 + 10,
        };
        let mut out = KatBytes {
            ptr: std::ptr::null_mut(),
            len: 0,
        };
        let r = unsafe { kat_term_mouse(self.0, &ev, &mut out) };
        (r, take(out))
    }
}

fn take(b: KatBytes) -> Vec<u8> {
    let v = if b.ptr.is_null() {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(b.ptr, b.len) }.to_vec()
    };
    unsafe { kat_bytes_free(b) };
    v
}

const DARK: Theme = Theme {
    foreground: 0xEEEEEE,
    background: 0x101010,
    cursor: 0xFF8800,
    cursor_text: 0x000000,
    selection_fg: 0x111111,
    selection_bg: 0x99CCFF,
    palette: [
        0x000000, 0xAA0000, 0x00AA00, 0xAAAA00, 0x0000AA, 0xAA00AA, 0x00AAAA, 0xAAAAAA, 0x555555,
        0xFF5555, 0x55FF55, 0xFFFF55, 0x5555FF, 0xFF55FF, 0x55FFFF, 0xFFFFFF,
    ],
    dark: true,
};

#[test]
fn text_lands_in_cells() {
    let t = T::new(20, 4);
    t.feed("hello\r\nworld");
    assert_eq!(t.row(0), "hello");
    assert_eq!(t.row(1), "world");
    let (info, _) = t.snapshot();
    assert_eq!((info.cols, info.rows), (20, 4));
    assert_eq!((info.cursor_col, info.cursor_row), (5, 1));
    assert_eq!(info.cursor_shape, cursor::BLOCK);
    assert_ne!(info.modes & tm::SHOW_CURSOR, 0);
}

#[test]
fn snapshot_reports_changes_only() {
    let t = T::new(10, 2);
    t.feed("a");
    let mut info = GridInfo::default();
    let mut cells = vec![Cell::default(); 20];
    assert!(unsafe { kat_term_snapshot(t.0, &mut info, cells.as_mut_ptr(), 20) });
    assert!(!unsafe { kat_term_snapshot(t.0, &mut info, cells.as_mut_ptr(), 20) });
    t.feed("b");
    assert!(unsafe { kat_term_snapshot(t.0, &mut info, cells.as_mut_ptr(), 20) });
    assert_eq!(cells[1].ch, 'b' as u32);
}

#[test]
fn colors_are_resolved() {
    let t = T::new(20, 2);
    unsafe { kat_term_set_theme(t.0, &DARK) };
    // Red on default, then 24-bit on blue, then inverse, bold and underline.
    t.feed("\x1b[31mR\x1b[0m\x1b[38;2;1;2;3;44mT\x1b[0m\x1b[7mI\x1b[0m\x1b[1;4mB\x1b[0m\x1b[2mD");
    let (info, cells) = t.snapshot();
    assert_eq!(info.foreground, 0xEEEEEE);
    assert_eq!(info.background, 0x101010);
    assert_eq!(info.cursor_color, 0xFF8800);
    assert_eq!((cells[0].ch, cells[0].fg), ('R' as u32, 0xAA0000));
    assert_ne!(cells[0].flags & cf::DEFAULT_BG, 0);
    assert_eq!((cells[1].fg, cells[1].bg), (0x010203, 0x0000AA));
    assert_eq!(cells[1].flags & cf::DEFAULT_BG, 0);
    assert_eq!((cells[2].fg, cells[2].bg), (0x101010, 0xEEEEEE));
    assert_eq!(cells[2].flags & cf::DEFAULT_BG, 0);
    assert_ne!(cells[3].flags & cf::BOLD, 0);
    assert_ne!(cells[3].flags & cf::UNDERLINE, 0);
    assert_eq!(cells[3].ul, term::NO_COLOR);
    assert_ne!(cells[4].flags & cf::DIM, 0);
    assert!(cells[4].fg < 0xEEEEEE);
    // A new theme recolors what is already on screen.
    let light = Theme {
        foreground: 0x222222,
        background: 0xFAFAFA,
        dark: false,
        ..DARK
    };
    unsafe { kat_term_set_theme(t.0, &light) };
    let (info, cells) = t.snapshot();
    assert_eq!((info.foreground, info.background), (0x222222, 0xFAFAFA));
    assert_eq!(cells[5].bg, 0xFAFAFA);
}

#[test]
fn wide_characters_and_clusters() {
    let t = T::new(10, 2);
    // A wide CJK character, then e + combining acute accent.
    t.feed("漢e\u{301}x");
    let (_, cells) = t.snapshot();
    assert_eq!(cells[0].ch, '漢' as u32);
    assert_ne!(cells[0].flags & cf::WIDE, 0);
    assert_ne!(cells[1].flags & cf::WIDE_SPACER, 0);
    assert_eq!(cells[2].ch, 'e' as u32);
    assert_eq!(cells[3].ch, 'x' as u32);
    let n = unsafe { kat_term_clusters(t.0, std::ptr::null_mut(), 0) };
    assert_eq!(n, 1);
    let mut out = vec![
        KatCluster {
            row: 0,
            col: 0,
            len: 0,
            text: [0; 59]
        };
        n
    ];
    unsafe { kat_term_clusters(t.0, out.as_mut_ptr(), n) };
    assert_eq!((out[0].row, out[0].col), (0, 2));
    assert_eq!(&out[0].text[..out[0].len as usize], "e\u{301}".as_bytes());
}

#[test]
fn keys_follow_modes() {
    let t = T::new(10, 2);
    assert_eq!(t.key(0, 'a' as u32, 0, "a"), b"a");
    assert_eq!(t.key(0, 'a' as u32, keys::mods::SHIFT, "A"), b"A");
    assert_eq!(t.key(0, 'c' as u32, keys::mods::CTRL, ""), b"\x03");
    assert_eq!(t.key(1, keys::named::ENTER, 0, ""), b"\r");
    assert_eq!(t.key(1, keys::named::BACKSPACE, 0, ""), b"\x7f");
    assert_eq!(t.key(1, keys::named::UP, 0, ""), b"\x1b[A");
    assert_eq!(t.key(0, 'b' as u32, keys::mods::ALT, "b"), b"\x1bb");
    assert_eq!(t.key(1, keys::named::F1, 0, ""), b"\x1bOP");
    // Application cursor keys (vim, less).
    t.feed("\x1b[?1h");
    assert_eq!(t.key(1, keys::named::UP, 0, ""), b"\x1bOA");
    // The kitty keyboard protocol, when the program pushes it.
    t.feed("\x1b[>1u");
    assert_eq!(t.key(1, keys::named::ESCAPE, 0, ""), b"\x1b[27u");
}

#[test]
fn replies_go_back_to_the_program() {
    let t = T::new(10, 4);
    t.feed("ab\x1b[6n");
    assert_eq!(take(unsafe { kat_term_take_replies(t.0) }), b"\x1b[1;3R");
    assert!(take(unsafe { kat_term_take_replies(t.0) }).is_empty());
    t.feed("\x07");
    assert!(unsafe { kat_term_take_bell(t.0) });
    assert!(!unsafe { kat_term_take_bell(t.0) });
    t.feed("\x1b]2;my title\x07");
    let mut changed = false;
    assert_eq!(
        take(unsafe { kat_term_title(t.0, &mut changed) }),
        b"my title"
    );
    assert!(changed);
}

#[test]
fn paste_is_bracketed_when_asked() {
    let t = T::new(10, 2);
    let text = CString::new("ls\nps").unwrap();
    assert_eq!(
        take(unsafe { kat_term_paste(t.0, text.as_ptr()) }),
        b"ls\rps"
    );
    t.feed("\x1b[?2004h");
    assert_eq!(
        take(unsafe { kat_term_paste(t.0, text.as_ptr()) }),
        b"\x1b[200~ls\nps\x1b[201~"
    );
    assert!(take(unsafe { kat_term_focus(t.0, true) }).is_empty());
    t.feed("\x1b[?1004h");
    assert_eq!(take(unsafe { kat_term_focus(t.0, true) }), b"\x1b[I");
}

#[test]
fn mouse_selects_or_reports() {
    let t = T::new(20, 3);
    t.feed("hello world\r\nsecond line");
    // Double click selects a word.
    let (r, _) = t.mouse(0, 0, 7, 0, 0, 2);
    assert_eq!(r, MouseOutcome::SelectionChanged as u8);
    let (r, _) = t.mouse(1, 0, 7, 0, 0, 2);
    assert_eq!(r, MouseOutcome::SelectionDone as u8);
    assert_eq!(take(unsafe { kat_term_selection_text(t.0) }), b"world");
    let (_, cells) = t.snapshot();
    assert_ne!(cells[7].flags & cf::SELECTED, 0);
    assert_eq!(cells[0].flags & cf::SELECTED, 0);
    // Dragging across lines.
    t.mouse(0, 0, 6, 0, 0, 1);
    // Released on the left half of column 6: up to and including column 5.
    t.mouse(2, 0, 6, 1, 0, 1);
    t.mouse(1, 0, 6, 1, 0, 1);
    assert_eq!(
        take(unsafe { kat_term_selection_text(t.0) }),
        b"world\nsecond"
    );
    assert!(unsafe { kat_term_clear_selection(t.0) });
    assert!(!unsafe { kat_term_has_selection(t.0) });
    // With SGR mouse reporting on, clicks go to the program.
    t.feed("\x1b[?1000h\x1b[?1006h");
    let (r, bytes) = t.mouse(0, 0, 2, 1, 0, 1);
    assert_eq!(r, MouseOutcome::Reported as u8);
    assert_eq!(bytes, b"\x1b[<0;3;2M");
    // Shift still selects.
    let (r, _) = t.mouse(0, 0, 2, 1, keys::mods::SHIFT, 2);
    assert_eq!(r, MouseOutcome::SelectionChanged as u8);
}

#[test]
fn wheel_scrolls_back_or_goes_to_the_program() {
    let t = T::new(10, 3);
    for i in 0..10 {
        t.feed(&format!("line{i}\r\n"));
    }
    let (info, _) = t.snapshot();
    assert_eq!(info.display_offset, 0);
    assert!(info.history_size >= 7);
    assert!(take(unsafe { kat_term_wheel(t.0, 2, 0, 0, 0) }).is_empty());
    let (info, _) = t.snapshot();
    assert_eq!(info.display_offset, 2);
    assert_eq!(
        info.cursor_shape,
        cursor::HIDDEN,
        "cursor is below the viewport"
    );
    // Typing goes back to the bottom.
    t.key(0, 'x' as u32, 0, "x");
    let (info, _) = t.snapshot();
    assert_eq!(info.display_offset, 0);
    unsafe { kat_term_scroll(t.0, 2, 0) };
    let (info, _) = t.snapshot();
    assert_eq!(info.display_offset, info.history_size);
    unsafe { kat_term_scroll(t.0, 3, 0) };
    // On the alternate screen with alternate scroll, the wheel is arrow keys.
    t.feed("\x1b[?1049h\x1b[?1007h");
    assert_eq!(take(unsafe { kat_term_wheel(t.0, 1, 0, 0, 0) }), b"\x1b[A");
    let (info, _) = t.snapshot();
    assert_ne!(info.modes & tm::ALT_SCREEN, 0);
}

#[test]
fn resize_reflows() {
    let t = T::new(10, 3);
    t.feed("0123456789abcdef");
    assert_eq!(t.row(0), "0123456789");
    assert_eq!(t.row(1), "abcdef");
    unsafe { kat_term_resize(t.0, 20, 3, 10, 20) };
    assert_eq!(t.row(0), "0123456789abcdef");
    let (info, _) = t.snapshot();
    assert_eq!(info.cols, 20);
    // Size reports answer with the new size.
    t.feed("\x1b[18t");
    assert_eq!(take(unsafe { kat_term_take_replies(t.0) }), b"\x1b[8;3;20t");
}

#[test]
fn cursor_styles() {
    let t = T::new(10, 2);
    t.feed("\x1b[6 q");
    assert_eq!(t.snapshot().0.cursor_shape, cursor::BEAM);
    t.feed("\x1b[4 q");
    assert_eq!(t.snapshot().0.cursor_shape, cursor::UNDERLINE);
    t.feed("\x1b[?25l");
    assert_eq!(t.snapshot().0.cursor_shape, cursor::HIDDEN);
}

#[test]
fn reset_and_screen_text() {
    let t = T::new(10, 2);
    t.feed("abc");
    assert_eq!(take(unsafe { kat_term_screen_text(t.0) }), b"abc");
    unsafe { kat_term_select_all(t.0) };
    assert!(unsafe { kat_term_has_selection(t.0) });
    unsafe { kat_term_reset(t.0) };
    assert_eq!(t.row(0), "");
}
