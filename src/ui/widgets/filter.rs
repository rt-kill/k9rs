use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Widget},
};

use crate::ui::theme::Theme;

/// Filter input widget: bordered box with `/` prefix,
/// visible block cursor, and match count.
pub struct FilterBar<'a> {
    active: bool,
    text: &'a str,
    match_count: usize,
    total_count: usize,
    /// Why the pattern isn't running as a regex, if it isn't
    /// ([`crate::app::element::Element::grep_fallback`]). Rendered because
    /// the alternative is a `[0/47]` the user has no way to explain.
    fallback: Option<&'a str>,
    theme: &'a Theme,
}

impl<'a> FilterBar<'a> {
    pub fn new(
        active: bool,
        text: &'a str,
        match_count: usize,
        total_count: usize,
        theme: &'a Theme,
    ) -> Self {
        Self {
            active,
            text,
            match_count,
            total_count,
            fallback: None,
            theme,
        }
    }

    pub fn fallback(mut self, why: Option<&'a str>) -> Self {
        self.fallback = why;
        self
    }

    /// The trailing `[n/total]`, or the reason the pattern degraded — the
    /// count is meaningless while the pattern isn't the one you wrote.
    fn counter(&self) -> (String, ratatui::style::Style) {
        match self.fallback {
            Some(why) => (format!("  [literal: {why}]"), self.theme.filter_degraded),
            None => (
                format!("  [{}/{}]", self.match_count, self.total_count),
                self.theme.title_counter,
            ),
        }
    }
}

/// The log analogue of [`FilterBar`]. Logs STACK greps — every committed
/// `/pattern` pushes a `LogFilter` element — so this shows the whole chain
/// plus the resulting visible-line count, where the table's bar shows one
/// pattern and a match/total ratio.
///
/// It lives HERE, beside `FilterBar`, as a widget the view places in its own
/// layout row — deliberately not something the log content widget paints for
/// itself. It used to be drawn inside `LogViewer::render`, which the view only
/// instantiates when there are lines to draw; a grep that matched nothing took
/// the "No matching lines." branch instead, so the pattern disappeared from the
/// screen at the one moment the user needed to read it in order to fix it.
/// Chrome that explains why the content is empty cannot be owned by the
/// content. (It also stopped covering the last log row, which it used to paint
/// over while the Viewport still counted that row as visible.)
pub struct LogFilterBar<'a> {
    /// Whether the draft input is open — `/` pressed, not yet committed.
    active: bool,
    /// Text being typed. Empty unless `active`.
    draft: &'a str,
    /// Caret position within `draft`, in COLUMNS. The block is drawn THERE,
    /// not appended, so editing mid-pattern shows where the next character
    /// lands — which is the whole point of the input having a caret.
    caret_col: usize,
    /// Committed greps, oldest first: each as the user typed it (`!` and
    /// all) with how many lines survive it.
    committed: &'a [(String, usize)],
    /// Lines visible through the whole chain, draft included.
    visible_count: usize,
    /// Why a grep in the chain isn't running as a regex, if one isn't.
    fallback: Option<&'a str>,
    theme: &'a Theme,
}

impl<'a> LogFilterBar<'a> {
    pub fn new(
        active: bool,
        draft: &'a str,
        caret_col: usize,
        committed: &'a [(String, usize)],
        visible_count: usize,
        theme: &'a Theme,
    ) -> Self {
        Self { active, draft, caret_col, committed, visible_count, fallback: None, theme }
    }

    pub fn fallback(mut self, why: Option<&'a str>) -> Self {
        self.fallback = why;
        self
    }

    /// Whether this bar has anything to say. The view asks BEFORE laying out,
    /// so an idle bar costs no row — and the row it does claim is never blank.
    pub fn wants_row(&self) -> bool {
        self.active || !self.committed.is_empty()
    }
}

impl Widget for LogFilterBar<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height == 0 || area.width == 0 || !self.wants_row() {
            return;
        }
        let y = area.y;
        for x in area.x..area.x + area.width {
            buf.set_string(x, y, " ", self.theme.status_bar);
        }

        let mut spans = vec![Span::styled(" /", self.theme.status_bar_key)];
        // The committed chain shows even while a new grep is being typed:
        // refining a filter you can't see is the same defect one level down.
        for (i, (pattern, kept)) in self.committed.iter().enumerate() {
            if i > 0 {
                spans.push(Span::styled(" | ", self.theme.status_bar));
            }
            // An EXCLUDING step reads differently from a narrowing one, so
            // it is coloured differently — the `!` alone is one glyph to
            // miss in a chain of four.
            let style = if pattern.starts_with('!') {
                self.theme.filter_degraded
            } else {
                self.theme.filter
            };
            spans.push(Span::styled(pattern.clone(), style));
            // PER-STEP counts, because the aggregate could only say the
            // chain was empty, never which step emptied it.
            spans.push(Span::styled(format!("[{kept}]"), self.theme.title_counter));
        }
        if self.active {
            if !self.committed.is_empty() {
                spans.push(Span::styled(" | ", self.theme.status_bar));
            }
            // Split at the caret and draw the block between the halves. The
            // split is by COLUMNS, walked as graphemes, so it lands on a
            // character boundary whatever the pattern contains.
            //
            // WINDOWED first: a long committed chain pushes the draft right,
            // and the line is clipped at the bar's edge — so what you were
            // typing, caret and all, could be off screen while you typed it.
            // The chain is context; the draft is the thing in your hands.
            let used: usize = spans
                .iter()
                .map(|sp| unicode_width::UnicodeWidthStr::width(sp.content.as_ref()))
                .sum();
            // `- 1` for the caret block itself.
            let avail = (area.width as usize).saturating_sub(used).saturating_sub(1);
            let (draft, caret_col) =
                crate::util::window_around(self.draft, self.caret_col, avail);
            let mut col = 0usize;
            let mut at = draft.len();
            for (i, g) in unicode_segmentation::UnicodeSegmentation::grapheme_indices(draft, true) {
                if col >= caret_col {
                    at = i;
                    break;
                }
                col += unicode_width::UnicodeWidthStr::width(g);
            }
            spans.push(Span::styled(draft[..at].to_string(), self.theme.filter));
            spans.push(Span::styled("\u{2588}", self.theme.filter));
            spans.push(Span::styled(draft[at..].to_string(), self.theme.filter));
        }
        // A visible-line count is meaningless while the pattern isn't the one
        // you wrote, so the reason takes its place rather than sitting beside
        // it.
        if let Some(why) = self.fallback {
            spans.push(Span::styled(
                format!("  [literal: {why}]"),
                self.theme.filter_degraded,
            ));
        } else if self.active {
            spans.push(Span::styled(
                format!("  [{} visible]", self.visible_count),
                self.theme.title_counter,
            ));
        } else {
            let n = self.committed.len();
            spans.push(Span::styled(
                format!(
                    "  [{} filter{}, {} visible]",
                    n,
                    if n == 1 { "" } else { "s" },
                    self.visible_count
                ),
                self.theme.title_counter,
            ));
        }
        buf.set_line(area.x, y, &Line::from(spans), area.width);
    }
}

impl Widget for FilterBar<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height == 0 || area.width == 0 {
            return;
        }

        if !self.active && self.text.is_empty() {
            return;
        }

        // When we have 3 lines, draw a bordered box
        if area.height >= 3 {
            let border_style = if self.active {
                self.theme.border_focused
            } else {
                self.theme.border
            };
            let block = Block::bordered()
                .border_style(border_style);
            let inner = block.inner(area);
            block.render(area, buf);

            if inner.height == 0 || inner.width == 0 {
                return;
            }

            let mut spans = Vec::new();
            // "/" prefix
            spans.push(Span::styled("/", self.theme.filter));
            // Filter text
            spans.push(Span::styled(self.text, self.theme.filter));
            // Cursor indicator when active
            if self.active {
                spans.push(Span::styled(
                    "\u{2588}",
                    self.theme.filter.add_modifier(Modifier::BOLD),
                ));
            }
            // Match count
            if !self.text.is_empty() {
                let (text, style) = self.counter();
                spans.push(Span::styled(text, style));
            }
            let line = Line::from(spans);
            buf.set_line(inner.x, inner.y, &line, inner.width);
        } else {
            // Single-line fallback
            let mut spans = Vec::new();
            spans.push(Span::styled("/", self.theme.filter));
            spans.push(Span::styled(self.text, self.theme.filter));
            if self.active {
                spans.push(Span::styled("\u{2588}", self.theme.filter));
            }
            if !self.text.is_empty() {
                let (text, style) = self.counter();
                spans.push(Span::styled(text, style));
            }
            let line = Line::from(spans);
            buf.set_line(area.x, area.y, &line, area.width);
        }
    }
}
