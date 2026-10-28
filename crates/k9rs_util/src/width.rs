//! Fitting text into a number of terminal columns.
//!
//! Display columns, not bytes and not chars: a CJK glyph occupies two cells
//! and a combining accent none, so every function here measures with
//! `unicode_width` and cuts on grapheme boundaries. These were spread across
//! two ends of `lib.rs` with 250 unrelated lines between them.
//!
//! A fourth, `truncate_to_width`, went with `widgets::tabs` — it was that
//! breadcrumb widget's helper, and the widget had been superseded by the one
//! in `views::resource` without either being removed. Narrowing
//! `k9rs_ui::widgets` to `pub(crate)` is what let `dead_code` finally see the
//! widget, and the helper fell out behind it.

/// The slice of `s` visible in `width` columns with the caret kept on screen,
/// and the caret's column within that slice.
///
/// A one-line viewport. The caret is the ANCHOR, because a caret you cannot
/// see is a caret you cannot trust: five render sites each wrote `if cursor_x
/// < right_edge { set_cursor(..) }`, so the moment what you typed outgrew the
/// bar the text clipped AND the caret stopped being drawn at all. The guard
/// looked defensive and WAS the bug.
///
/// Grapheme-granular, for the reason `TextInput`'s cursor is: a byte slice
/// between a letter and its combining accent panics. Scrolls only as far as
/// it must, so short input does not jitter — and a value that exactly fills
/// the width gives up its first column, because the caret sitting after its
/// last character needs a cell of its own.
///
/// Lives here rather than only on the text-input type because the filter
/// bar is handed a `&str` and a column, not the input. (Named in prose, not
/// linked: `util` is the bottom of the tree and a link upward from it could
/// not resolve once these are separate crates.)
pub fn window_around(s: &str, caret_col: usize, width: usize) -> (&str, usize) {
    use unicode_segmentation::UnicodeSegmentation;
    use unicode_width::UnicodeWidthStr;
    if width == 0 {
        return ("", 0);
    }
    let fits = |run: &str| -> usize {
        let mut w = 0usize;
        for (i, g) in run.grapheme_indices(true) {
            let gw = UnicodeWidthStr::width(g);
            if w + gw > width {
                return i;
            }
            w += gw;
        }
        run.len()
    };
    // The caret may sit one past the last column, so the last usable column
    // is `width - 1`.
    let first_col = caret_col.saturating_sub(width - 1);
    if first_col == 0 {
        let end = fits(s);
        return (&s[..end], caret_col);
    }
    let mut start = s.len();
    let mut skipped = 0usize;
    for (i, g) in s.grapheme_indices(true) {
        if skipped >= first_col {
            start = i;
            break;
        }
        skipped += UnicodeWidthStr::width(g);
    }
    let tail = &s[start..];
    let end = fits(tail);
    (&tail[..end], caret_col.saturating_sub(skipped))
}

/// Keep the END of `s`, marking what was dropped with a leading ellipsis.
///
/// The mirror of [`truncate`], for the surfaces where the TAIL is the answer.
/// A nav breadcrumb is one: `pods > /api > describe(web-7f9)` cut on the
/// right loses `describe(web-7f9)`, which is the only part of it you did not
/// already know — you are looking at the thing it names.
pub fn truncate_left(s: &str, max_width: usize) -> std::borrow::Cow<'_, str> {
    use unicode_segmentation::UnicodeSegmentation;
    use unicode_width::UnicodeWidthStr;
    if UnicodeWidthStr::width(s) <= max_width {
        return std::borrow::Cow::Borrowed(s);
    }
    if max_width <= 1 {
        return std::borrow::Cow::Borrowed(if max_width == 0 { "" } else { "\u{2026}" });
    }
    // One column for the ellipsis; fill the rest from the right, in whole
    // grapheme clusters (a byte slice would split one and panic the caller).
    let budget = max_width - 1;
    let mut width = 0usize;
    let mut start = s.len();
    for (i, g) in s.grapheme_indices(true).rev() {
        let w = UnicodeWidthStr::width(g);
        if width + w > budget {
            break;
        }
        width += w;
        start = i;
    }
    std::borrow::Cow::Owned(format!("\u{2026}{}", &s[start..]))
}

/// Truncates `s` to at most `max` display COLUMNS, marking the cut with `…`.
///
/// Columns, not chars — the unit this module's header promises and the unit
/// every caller passes it: a flash line's `area.width`, a completion ghost's
/// remaining columns, a describe value's 80-column budget. It counted
/// `s.chars()`, so a CJK or emoji string measured as much as half under its
/// real width, came back UNCHANGED, and was then clipped by `set_line` with
/// no `…` to say a cut had happened — which is precisely the silent tail
/// loss the flash widget's own comment claims to have fixed, and flash text
/// carries daemon and apiserver strings.
pub fn truncate(s: &str, max: usize) -> String {
    use unicode_segmentation::UnicodeSegmentation;
    use unicode_width::UnicodeWidthStr;
    if max == 0 {
        return String::new();
    }
    if UnicodeWidthStr::width(s) <= max {
        return s.to_string();
    }
    if max == 1 {
        return "\u{2026}".to_string();
    }
    // Cut on a grapheme boundary for the reason `window_around` does: a byte
    // slice between a letter and its combining accent panics. A wide glyph
    // that would straddle the budget is dropped whole rather than halved.
    let budget = max - 1; // the ellipsis owns the last column
    let mut out = String::new();
    let mut w = 0usize;
    for g in s.graphemes(true) {
        let gw = UnicodeWidthStr::width(g);
        if w + gw > budget {
            break;
        }
        out.push_str(g);
        w += gw;
    }
    out.push('\u{2026}');
    out
}

/// Pads `s` on the right until it occupies exactly `cells` display columns.
///
/// `format!("{:<width$}")` pads to a CHAR count, which is a different number:
/// a string built by measuring columns and then padded by char count
/// overflows its budget by the difference. Measured —
/// `format!(" {:<10} ", "日本語")` is 12 chars and **15 columns** against a
/// 12-column cell, so a CJK or emoji value spilled into the next column;
/// a value of combining marks under-filled and left the tail unwritten.
///
/// Wider than `cells` is returned unchanged: this pads, it does not cut —
/// [`truncate`] does that, and a caller that needs both says so.
pub fn pad_to_cells(s: &str, cells: usize) -> String {
    use unicode_width::UnicodeWidthStr;
    let w = UnicodeWidthStr::width(s);
    let mut out = String::with_capacity(s.len() + cells.saturating_sub(w));
    out.push_str(s);
    out.extend(std::iter::repeat_n(' ', cells.saturating_sub(w)));
    out
}

#[cfg(test)]
#[path = "tests/width.rs"]
mod tests;
