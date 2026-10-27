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

/// Truncates a string to the given maximum number of characters.
/// If truncated, appends an ellipsis character.
pub fn truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else if max <= 1 {
        "\u{2026}".to_string()
    } else {
        let mut result: String = chars[..max - 1].iter().collect();
        result.push('\u{2026}');
        result
    }
}

#[cfg(test)]
#[path = "tests/width.rs"]
mod tests;
