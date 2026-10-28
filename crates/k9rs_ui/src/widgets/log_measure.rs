//! Measuring and wrapping a log line, before anything is painted.
//!
//! Split out of `log_view.rs` on a measured seam: nothing here names the
//! renderer in code (only in doc comments, where the point is that the two
//! must agree), and the renderer reaches in at four call sites. Wrapping and
//! measuring is where the subtle work is — grapheme clusters, zero-width
//! joiners, tabs, the wide-glyph straddle — and it is now readable without
//! the painting around it.
//!
//! The agreement with the renderer is NOT structural and must not be assumed
//! to be: `prepare_line` builds the spans and these functions measure what
//! it will build. `tests/ansi.rs`'s `visible_text_matches_the_spans` is what
//! holds the two together.

use ratatui::text::{Line, Span};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use k9rs_app::StoredLine;

/// Display width of one grapheme cluster, measured the way ratatui measures
/// it at the moment it writes it into a cell.
///
/// This is the ONLY width authority in this file, and it exists because there
/// used to be four. They agreed with each other and disagreed with the
/// renderer: `Buffer::set_stringn` segments into GRAPHEME CLUSTERS and takes
/// `UnicodeWidthStr::width` of each, while everything here summed
/// `UnicodeWidthChar::width` per `char`. For `⚠️` (`U+26A0 U+FE0F`) that is 1
/// against 2, so wrap packed one column too much into every row containing
/// one — and `set_stringn` dropped the overflowing cluster, which was on no
/// other row and reachable at no scroll offset. ZWJ sequences diverge the
/// other way (`👨‍👩‍👧` counts 6, draws 2), splitting one glyph across rows and
/// inflating the scrollbar.
///
/// Measuring the same unit the renderer places is the only way these can
/// agree; counting the same unit AS EACH OTHER is what kept the invariant
/// test green while the text was being eaten.
pub(crate) fn cluster_width(g: &str) -> usize {
    // ratatui drops any grapheme containing a control char — TAB INCLUDED
    // (`Buffer::set_stringn` filters on `char::is_control`). `unicode-width`
    // gives a tab width 1, so a tab-indented line (every Go panic, every
    // Java stack trace) was counted one column wider than it draws and broke
    // one column early per level of indentation.
    if g.chars().any(char::is_control) {
        return 0;
    }
    UnicodeWidthStr::width(g)
}

/// The grapheme clusters of `text`, skipping ANSI escapes when `ansi` and
/// dropping the non-tab control characters `k9rs_util::text::sanitize_terminal`
/// removes on the render path — so the measurement sees exactly the clusters
/// that will be drawn.
/// THE one enumeration of a text run into drawable units.
///
/// Everything that turns text into columns goes through this: the height
/// measurement, the wrapper, the left-clip and the width. They used to walk
/// the string themselves — three independent copies of "skip the escapes,
/// drop the controls, segment, take the width" — and every bug this file has
/// had was two of those copies disagreeing.
///
/// A lending cursor rather than an `Iterator`, because the general case has
/// to own a cleaned copy of the run (removing a control character can rejoin
/// the clusters either side of it, so cleaning must happen BEFORE
/// segmenting) and the units borrow from it.
///
/// There is no "already parsed" mode. The invariant is "what ratatui will
/// draw", and it drops every grapheme containing a control — tab included —
/// whether or not the run has been through the ANSI parser. Making that
/// conditional is what let the measurement and the wrapper disagree about a
/// tab-indented line: `sanitize_terminal` keeps tabs for the shell and flash
/// surfaces, so a parsed span still carries one.
///
/// The ASCII case is not a second algorithm — it is the same walk over a
/// representation that happens to make every unit one byte wide. That is a
/// property of the data, established once here, not a branch each caller
/// re-derives.
struct Units<'a> {
    src: std::borrow::Cow<'a, str>,
    at: usize,
    /// Every unit is one byte and one column: no cleaning was needed and no
    /// segmentation can group anything.
    plain: bool,
}

impl<'a> Units<'a> {
    fn new(text: &'a str) -> Self {
        // Printable ASCII is handed straight back: nothing to strip, and no
        // two such bytes can form one cluster, so the common case allocates
        // nothing and segments nothing. ONE predicate, because "ASCII" and
        // "no controls" were two scans asking one question.
        let plain = text.bytes().all(|b| (0x20..0x7f).contains(&b));
        if plain {
            return Self { src: std::borrow::Cow::Borrowed(text), at: 0, plain };
        }
        let src = clean(text);
        let plain = src.is_ascii();
        Self { src, at: 0, plain }
    }

    /// The next drawable unit and the columns it occupies.
    fn next(&mut self) -> Option<(&str, usize)> {
        if self.at >= self.src.len() {
            return None;
        }
        if self.plain {
            let g = &self.src[self.at..self.at + 1];
            self.at += 1;
            return Some((g, 1));
        }
        let g = self.src[self.at..].graphemes(true).next()?;
        self.at += g.len();
        Some((g, cluster_width(g)))
    }
}

/// Strip ANSI escapes and the control characters the renderer drops.
///
/// Borrowed when there is nothing to remove. Done BEFORE segmentation
/// because `push_ansi_text` removes controls before the text reaches a Span,
/// so the renderer segments the CLEANED string — `⚠<0x01>\u{FE0F}` is two
/// clusters raw and one cluster cleaned, and counting the raw form reported
/// one column where two are drawn.
fn clean(text: &str) -> std::borrow::Cow<'_, str> {
    let bytes = text.as_bytes();
    let dirty = {
        let mut i = 0usize;
        loop {
            if i >= bytes.len() {
                break false;
            }
            if k9rs_util::text::skip_ansi_escape(bytes, i).is_some() {
                break true;
            }
            let c = text[i..].chars().next().unwrap();
            if c.is_control() {
                break true;
            }
            i += c.len_utf8();
        }
    };
    if !dirty {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = k9rs_util::text::skip_ansi_escape(bytes, i) {
            i = next;
            continue;
        }
        let c = text[i..].chars().next().unwrap();
        i += c.len_utf8();
        if !c.is_control() {
            out.push(c);
        }
    }
    std::borrow::Cow::Owned(out)
}

/// Feed a text run into a running `(col, rows)` wrap accumulator at width
/// `w`. When `ansi`, escapes are skipped via [`k9rs_util::text::skip_ansi_escape`]
/// — the SAME tokenizer `parse_ansi_line` (the renderer) uses — so the counted
/// width cannot drift from the rendered width. Raw runs (container prefix,
/// timestamp) pass `ansi = false` and count every byte, matching how the
/// renderer emits those as raw (un-parsed) spans. Non-tab control chars are
/// width-0 (`unwrap_or(0)`), which equals the renderer dropping them.
pub(crate) fn wrap_feed(text: &str, w: usize, col: &mut usize, rows: &mut usize) {
    let mut units = Units::new(text);
    while let Some((_, cw)) = units.next() {
        if *col + cw > w {
            *rows += 1;
            *col = cw;
        } else {
            *col += cw;
        }
    }
}

/// Physical (char-wrapped) row count of a log line at width `w`. MUST agree
/// with `wrap_line` exactly so the Viewport's physical extent is truthful and
/// `end()` reaches the real last row. Mirrors `prepare_line`'s visible text:
/// source prefix + optional timestamp + body — INCLUDING which of those the
/// toggles are currently showing, since a hidden prefix is a shorter line and
/// a wrong height here is a scrollbar that lies.
pub(crate) fn line_phys_rows(line: &StoredLine, shed: usize, w: usize) -> usize {
    if w == 0 {
        return 1;
    }
    let mut rows = 1usize;
    let mut col = 0usize;
    // ONE walk over the visible roles, in order. The measurer and the
    // renderer below consume the SAME projection, so neither can disagree
    // with the other about which parts are on screen.
    for (role, text, sep) in line.segments() {
        if !role.shown(shed) {
            continue;
        }
        wrap_feed(text, w, &mut col, &mut rows);
        wrap_feed(sep, w, &mut col, &mut rows);
    }
    rows
}

/// Total display width of an already-styled line. Same per-char accounting as
/// [`wrap_line`], so the pan extent and the rendering agree about where a line
/// ends.
pub(crate) fn line_cols(line: &Line<'static>) -> usize {
    line.spans
        .iter()
        .map(|s| {
            // `ansi = false`: these spans came OUT of the parser, so their
            // escapes are already gone. Same walk, same widths.
            let mut units = Units::new(s.content.as_ref());
            let mut cols = 0usize;
            while let Some((_, cw)) = units.next() {
                cols += cw;
            }
            cols
        })
        .sum()
}

/// Drop the first `n` display columns off a styled line, preserving span
/// styles — the horizontal analogue of windowing rows, and the only thing the
/// pan does to the content.
///
/// A double-width glyph straddling the cut is replaced by spaces for the part
/// that survives, not half-drawn: there is no half of a `本`, and emitting the
/// whole char would push every following column one to the right of where this
/// function says they are.
pub(crate) fn clip_left(line: &Line<'static>, n: usize) -> Line<'static> {
    if n == 0 {
        return line.clone();
    }
    let mut out: Vec<Span<'static>> = Vec::new();
    let mut col = 0usize;
    for span in &line.spans {
        let mut kept = String::new();
        let mut units = Units::new(span.content.as_ref());
        while let Some((c, cw)) = units.next() {
            if col >= n {
                kept.push_str(c);
            } else if col + cw > n {
                // Straddles the cut: emit spaces for the columns that
                // survive it, so the rest of the line still lands where
                // the measurement above says it does.
                kept.push_str(&" ".repeat(col + cw - n));
            }
            col += cw;
        }
        if !kept.is_empty() {
            out.push(Span::styled(kept, span.style));
        }
    }
    Line::from(out)
}

/// Char-wrap an already-styled line into physical rows of at most `w` display
/// columns, preserving span styles across the break. Same column logic as
/// [`line_phys_rows`], so heights and rendering agree exactly.
pub(crate) fn wrap_line(line: &Line<'static>, w: usize) -> Vec<Line<'static>> {
    if w == 0 {
        return vec![line.clone()];
    }
    let mut rows: Vec<Vec<Span<'static>>> = vec![Vec::new()];
    let mut col = 0usize;
    for span in &line.spans {
        let style = span.style;
        let mut cur = String::new();
        let mut units = Units::new(span.content.as_ref());
        while let Some((c, cw)) = units.next() {
            if col + cw > w {
                if !cur.is_empty() {
                    rows.last_mut().unwrap().push(Span::styled(std::mem::take(&mut cur), style));
                }
                rows.push(Vec::new());
                col = 0;
            }
            cur.push_str(c);
            col += cw;
        }
        if !cur.is_empty() {
            rows.last_mut().unwrap().push(Span::styled(cur, style));
        }
    }
    rows.into_iter().map(Line::from).collect()
}
