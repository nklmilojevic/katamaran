/* The Mac app's terminal engine (crates/kat-term): a libghostty-vt terminal per container
 * shell. Every call locks the terminal: output can be fed from a reader thread while the view
 * takes snapshots on the main thread. */

#ifndef KAT_TERM_H
#define KAT_TERM_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kat_term kat_term;

/* Bytes owned by the library; free with kat_bytes_free. ptr is NULL when len is 0. */
typedef struct {
    uint8_t *ptr;
    size_t len;
} kat_bytes;

void kat_bytes_free(kat_bytes bytes);

kat_term *kat_term_new(uint16_t cols, uint16_t rows, uint32_t scrollback);
void kat_term_free(kat_term *term);

/* Output of the program. */
void kat_term_feed(const kat_term *term, const uint8_t *bytes, size_t len);
/* What the terminal answers the program (device attributes, cursor reports...): send it. */
kat_bytes kat_term_take_replies(const kat_term *term);
bool kat_term_take_bell(const kat_term *term);
/* The title the program set (UTF-8); `changed` (may be NULL): changed since the last call. */
kat_bytes kat_term_title(const kat_term *term, bool *changed);
/* Clears screen, scrollback and modes. */
void kat_term_reset(const kat_term *term);

void kat_term_resize(const kat_term *term, uint16_t cols, uint16_t rows, uint32_t cell_width,
                     uint32_t cell_height);

/* ---------------------------------------------------------------------------------------
 * Colors
 * ------------------------------------------------------------------------------------- */

typedef struct {
    uint32_t foreground; /* 0x00RRGGBB */
    uint32_t background;
    uint32_t cursor;
    uint32_t cursor_text;
    uint32_t selection_fg;
    uint32_t selection_bg;
    uint32_t palette[16]; /* the ANSI colors; 16-255 stay xterm's */
    bool dark;            /* the answer to color scheme queries */
} kat_theme;

void kat_term_set_theme(const kat_term *term, const kat_theme *theme);

/* ---------------------------------------------------------------------------------------
 * Screen contents (the layout of Thurm's thurm_cell / thurm_grid_info)
 * ------------------------------------------------------------------------------------- */

enum {
    KAT_CELL_BOLD = 1 << 0,
    KAT_CELL_ITALIC = 1 << 1,
    KAT_CELL_UNDERLINE = 1 << 2,
    KAT_CELL_DOUBLE_UNDERLINE = 1 << 3,
    KAT_CELL_UNDERCURL = 1 << 4,
    KAT_CELL_DOTTED_UNDERLINE = 1 << 5,
    KAT_CELL_DASHED_UNDERLINE = 1 << 6,
    KAT_CELL_STRIKEOUT = 1 << 7,
    KAT_CELL_WIDE = 1 << 8,        /* first half of a double-width character */
    KAT_CELL_WIDE_SPACER = 1 << 9, /* second half: draw nothing */
    KAT_CELL_HIDDEN = 1 << 10,
    KAT_CELL_SELECTED = 1 << 11,
    KAT_CELL_DEFAULT_BG = 1 << 13, /* the background is the default one */
    KAT_CELL_DIM = 1 << 14,
};

#define KAT_NO_COLOR 0xFF000000u

typedef struct {
    uint32_t ch; /* Unicode scalar; 0 or ' ' = blank */
    uint32_t fg; /* 0x00RRGGBB, inverse and faint applied */
    uint32_t bg;
    uint32_t ul; /* underline color, KAT_NO_COLOR = fg */
    uint16_t flags;
    uint16_t link;
} kat_cell;

enum {
    KAT_CURSOR_BLOCK = 0,
    KAT_CURSOR_UNDERLINE = 1,
    KAT_CURSOR_BEAM = 2,
    KAT_CURSOR_HOLLOW_BLOCK = 3,
    KAT_CURSOR_HIDDEN = 4,
};

enum {
    KAT_MODE_SHOW_CURSOR = 1 << 0,
    KAT_MODE_APP_CURSOR = 1 << 1,
    KAT_MODE_MOUSE_REPORT_CLICK = 1 << 3,
    KAT_MODE_BRACKETED_PASTE = 1 << 4,
    KAT_MODE_MOUSE_MOTION = 1 << 6,
    KAT_MODE_FOCUS_IN_OUT = 1 << 11,
    KAT_MODE_ALT_SCREEN = 1 << 12,
    KAT_MODE_MOUSE_DRAG = 1 << 13,
    KAT_MODE_MOUSE_ANY = (1 << 3) | (1 << 6) | (1 << 13),
};

typedef struct {
    uint16_t cols;
    uint16_t rows;
    uint16_t cursor_col;
    uint16_t cursor_row;
    uint8_t cursor_shape;
    bool cursor_blinking;
    bool cursor_wide;
    uint32_t display_offset; /* rows scrolled back, 0 at the bottom */
    uint32_t history_size;
    uint32_t modes; /* KAT_MODE_* */
    uint32_t foreground;
    uint32_t background;
    uint32_t cursor_color;
    uint32_t cursor_text_color;
    uint32_t selection_fg;
    uint32_t selection_bg;
    uint64_t generation; /* changes whenever anything shown changes */
} kat_grid_info;

/* Brings the grid up to date. Returns false when nothing changed since info->generation
 * (info untouched). Otherwise fills info and, when cap >= cols * rows, cells (row major);
 * with a smaller cap only info, so the caller can size the buffer and call again. */
bool kat_term_snapshot(const kat_term *term, kat_grid_info *info, kat_cell *cells, size_t cap);

typedef struct {
    uint16_t row;
    uint16_t col;
    uint8_t len;      /* bytes of text */
    char text[59];    /* UTF-8, not NUL-terminated */
} kat_cluster;

/* Grapheme clusters (emoji sequences, combining marks) of the last snapshot. Copies up to
 * max into out and returns the total (max = 0 to count). */
size_t kat_term_clusters(const kat_term *term, kat_cluster *out, size_t max);

/* The active screen as plain text. */
kat_bytes kat_term_screen_text(const kat_term *term);

/* ---------------------------------------------------------------------------------------
 * Input: every call returns the bytes to send to the program (maybe none).
 * ------------------------------------------------------------------------------------- */

enum {
    KAT_MOD_SHIFT = 1,
    KAT_MOD_ALT = 2,
    KAT_MOD_CTRL = 4,
    KAT_MOD_SUPER = 8,
    KAT_MOD_CAPS_LOCK = 64,
};

enum {
    KAT_KEY_ESCAPE = 1,
    KAT_KEY_ENTER = 2,
    KAT_KEY_TAB = 3,
    KAT_KEY_BACKSPACE = 4,
    KAT_KEY_INSERT = 5,
    KAT_KEY_DELETE = 6,
    KAT_KEY_LEFT = 7,
    KAT_KEY_RIGHT = 8,
    KAT_KEY_UP = 9,
    KAT_KEY_DOWN = 10,
    KAT_KEY_PAGE_UP = 11,
    KAT_KEY_PAGE_DOWN = 12,
    KAT_KEY_HOME = 13,
    KAT_KEY_END = 14,
    KAT_KEY_CAPS_LOCK = 15,
    KAT_KEY_SCROLL_LOCK = 16,
    KAT_KEY_NUM_LOCK = 17,
    KAT_KEY_PRINT_SCREEN = 18,
    KAT_KEY_PAUSE = 19,
    KAT_KEY_MENU = 20,
    KAT_KEY_KP_0 = 21, /* ... KAT_KEY_KP_9 = 30 */
    KAT_KEY_KP_DECIMAL = 31,
    KAT_KEY_KP_DIVIDE = 32,
    KAT_KEY_KP_MULTIPLY = 33,
    KAT_KEY_KP_SUBTRACT = 34,
    KAT_KEY_KP_ADD = 35,
    KAT_KEY_KP_ENTER = 36,
    KAT_KEY_KP_EQUAL = 37,
    KAT_KEY_LEFT_SHIFT = 38,
    KAT_KEY_LEFT_CONTROL = 39,
    KAT_KEY_LEFT_ALT = 40,
    KAT_KEY_LEFT_SUPER = 41,
    KAT_KEY_RIGHT_SHIFT = 42,
    KAT_KEY_RIGHT_CONTROL = 43,
    KAT_KEY_RIGHT_ALT = 44,
    KAT_KEY_RIGHT_SUPER = 45,
    KAT_KEY_F1 = 100, /* ... KAT_KEY_F25 = 124 */
};

typedef struct {
    /* 0 = text key (code: the unshifted Unicode scalar, 'a' for Shift+A),
     * 1 = named key (code: KAT_KEY_*). */
    uint32_t kind;
    uint32_t code;
    uint8_t mods;   /* KAT_MOD_* */
    uint8_t action; /* 0 press, 1 repeat, 2 release */
    const char *text; /* UTF-8 the key types (after Option / input methods), may be NULL */
    uint32_t base_layout; /* the key in the US layout, 0 when none */
} kat_key_event;

/* Typing scrolls back to the bottom. */
kat_bytes kat_term_key(const kat_term *term, const kat_key_event *event);
/* Bracketed when the program asked for it; control characters made safe. */
kat_bytes kat_term_paste(const kat_term *term, const char *text);
/* Empty unless the program asked for focus reports. */
kat_bytes kat_term_focus(const kat_term *term, bool focused);

enum { KAT_MOUSE_PRESS = 0, KAT_MOUSE_RELEASE = 1, KAT_MOUSE_MOVE = 2 };
enum {
    KAT_BUTTON_LEFT = 0,
    KAT_BUTTON_MIDDLE = 1,
    KAT_BUTTON_RIGHT = 2,
    KAT_BUTTON_BACK = 3,
    KAT_BUTTON_FORWARD = 4,
    KAT_BUTTON_NONE = 5,
};

typedef struct {
    uint8_t kind;
    uint8_t button;
    uint8_t mods;
    uint8_t clicks; /* 1, 2 (word) or 3 (line) */
    uint16_t col;   /* viewport cell */
    uint16_t row;
    bool right_half; /* pointer on the right half of the cell */
    uint32_t x;      /* pixels inside the grid */
    uint32_t y;
} kat_mouse_event;

/* What a mouse event did. */
enum {
    KAT_MOUSE_NONE = 0,
    KAT_MOUSE_REPORTED = 1,          /* reported to the program: send *out */
    KAT_MOUSE_SELECTION_CHANGED = 2,
    KAT_MOUSE_SELECTION_DONE = 3,    /* released over a selection */
};

/* Reported to a program that grabbed the mouse (unless Shift is held), else selection. */
uint8_t kat_term_mouse(const kat_term *term, const kat_mouse_event *event, kat_bytes *out);
/* Whole lines, positive towards older output. Bytes when the program takes the wheel
 * (mouse reporting, arrows on the alternate screen); otherwise the scrollback scrolls. */
kat_bytes kat_term_wheel(const kat_term *term, int32_t lines, uint16_t col, uint16_t row,
                         uint8_t mods);

enum { KAT_SCROLL_LINES = 0, KAT_SCROLL_PAGES = 1, KAT_SCROLL_TOP = 2, KAT_SCROLL_BOTTOM = 3 };
void kat_term_scroll(const kat_term *term, uint8_t kind, int32_t n);

/* ---------------------------------------------------------------------------------------
 * Selection
 * ------------------------------------------------------------------------------------- */

kat_bytes kat_term_selection_text(const kat_term *term);
bool kat_term_has_selection(const kat_term *term);
void kat_term_select_all(const kat_term *term);
bool kat_term_clear_selection(const kat_term *term);

#ifdef __cplusplus
}
#endif

#endif
