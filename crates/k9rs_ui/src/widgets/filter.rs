use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Widget},
};

use k9rs_theme::Theme;

/// The spans for a text input and its caret: WINDOWED around the caret so
/// it is always on screen, and split on a grapheme boundary so the block
/// never lands inside a glyph.
///
/// One helper for both bars in this file. `LogFilterBar` had it inline and
/// `FilterBar` had nothing at all — it appended the block after the whole
/// text and handed the line to `set_line`, so the table's `/` and `~`
/// filters clipped at the right edge with the caret outside it (you typed
/// blind), and after `Left`/`Home` the block was drawn at the END while the
/// keystrokes landed mid-pattern. That made them the sixth text input here
/// to lose its caret past the edge, in a class that was supposed to be
/// closed — while `FilterInputState::caret_col`, whose doc reads "Caret
/// position for the renderer, in columns", had no callers at all.
fn caret_spans<'t>(
    text: &'t str,
    caret_col: usize,
    avail: usize,
    style: ratatui::style::Style,
    caret_style: ratatui::style::Style,
) -> Vec<Span<'t>> {
    use unicode_segmentation::UnicodeSegmentation;
    let (shown, caret_col) = k9rs_util::width::window_around(text, caret_col, avail);
    let mut col = 0usize;
    let mut at = shown.len();
    for (i, g) in shown.grapheme_indices(true) {
        if col >= caret_col {
            at = i;
            break;
        }
        col += unicode_width::UnicodeWidthStr::width(g);
    }
    vec![
        Span::styled(&shown[..at], style),
        Span::styled("\u{2588}", caret_style),
        Span::styled(&shown[at..], style),
    ]
}

/// Filter input widget: bordered box with `/` prefix,
/// visible block cursor, and match count.
pub(crate) struct FilterBar<'a> {
    active: bool,
    text: &'a str,
    /// Caret position within `text`, in COLUMNS. The block is drawn THERE,
    /// not appended — see [`caret_spans`].
    caret_col: usize,
    match_count: usize,
    total_count: usize,
    /// Why the pattern isn't running as a regex, if it isn't
    /// ([`k9rs_app::Element::grep_fallback`]). Rendered because
    /// the alternative is a `[0/47]` the user has no way to explain.
    fallback: Option<&'a str>,
    theme: &'a Theme,
}

impl<'a> FilterBar<'a> {
    pub(crate) fn new(
        active: bool,
        text: &'a str,
        caret_col: usize,
        match_count: usize,
        total_count: usize,
        theme: &'a Theme,
    ) -> Self {
        Self {
            active,
            text,
            caret_col,
            match_count,
            total_count,
            fallback: None,
            theme,
        }
    }

    pub(crate) fn fallback(mut self, why: Option<&'a str>) -> Self {
        self.fallback = why;
        self
    }

    /// The trailing `[n/total]`, or the reason the pattern degraded — the
    /// count is meaningless while the pattern isn't the one you wrote.
    /// The whole bar as spans, fitted to `width`.
    ///
    /// The counter is the FIXED TAIL and its width is reserved before the
    /// text is windowed, so a long pattern can never push the `[3/47]` off
    /// the end — that ratio is the number telling you whether the pattern is
    /// doing anything, and a degraded pattern's `[literal: why]` even more
    /// so.
    fn spans(&self, width: u16) -> Vec<Span<'_>> {
        let mut spans = vec![Span::styled("/", self.theme.filter)];
        let counter = (!self.text.is_empty()).then(|| self.counter());
        let tail_w = counter
            .as_ref()
            .map(|(t, _)| unicode_width::UnicodeWidthStr::width(t.as_str()))
            .unwrap_or(0);
        if self.active {
            let avail = (width as usize)
                .saturating_sub(1) // the "/" prefix
                .saturating_sub(tail_w)
                .saturating_sub(1); // the caret block itself
            spans.extend(caret_spans(
                self.text,
                self.caret_col,
                avail,
                self.theme.filter,
                self.theme.filter.add_modifier(Modifier::BOLD),
            ));
        } else {
            // Committed and inactive: no caret to keep on screen, and the
            // title carries the pattern too.
            spans.push(Span::styled(self.text, self.theme.filter));
        }
        if let Some((text, style)) = counter {
            spans.push(Span::styled(text, style));
        }
        spans
    }

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
pub(crate) struct LogFilterBar<'a> {
    /// Whether the draft input is open — `/` pressed, not yet committed.
    active: bool,
    /// Text being typed. Empty unless `active`.
    draft: &'a str,
    /// Caret position within `draft`, in COLUMNS. The block is drawn THERE,
    /// not appended, so editing mid-pattern shows where the next character
    /// lands — which is the whole point of the input having a caret.
    caret_col: usize,
    /// Committed greps, oldest first — each as the user typed it (`!` and
    /// all), how many lines survive it, and whether it NARROWS or EXCLUDES.
    ///
    /// The sense is carried because it cannot be recovered from the label:
    /// `\!foo` is a narrowing filter whose label is `!foo`, the same string
    /// an excluding filter for `foo` produces. This used to read that
    /// leading `!` to pick the colour, and so painted an escaped literal in
    /// the excluding colour.
    committed: &'a [k9rs_app::ChainStep],
    /// Lines visible through the whole chain, draft included.
    visible_count: usize,
    /// Why a grep in the chain isn't running as a regex, if one isn't.
    fallback: Option<&'a str>,
    theme: &'a Theme,
}

impl<'a> LogFilterBar<'a> {
    pub(crate) fn new(
        active: bool,
        draft: &'a str,
        caret_col: usize,
        committed: &'a [k9rs_app::ChainStep],
        visible_count: usize,
        theme: &'a Theme,
    ) -> Self {
        Self { active, draft, caret_col, committed, visible_count, fallback: None, theme }
    }

    pub(crate) fn fallback(mut self, why: Option<&'a str>) -> Self {
        self.fallback = why;
        self
    }

    /// Whether this bar has anything to say. The view asks BEFORE laying out,
    /// so an idle bar costs no row — and the row it does claim is never blank.
    pub(crate) fn wants_row(&self) -> bool {
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
        for (i, step) in self.committed.iter().enumerate() {
            if i > 0 {
                spans.push(Span::styled(" | ", self.theme.status_bar));
            }
            // An EXCLUDING step reads differently from a narrowing one, so
            // it is coloured differently — the `!` alone is one glyph to
            // miss in a chain of four. From the step's OWN sense, not from
            // its label's first character.
            let style = match step.sense {
                k9rs_app::Sense::Drop => self.theme.filter_degraded,
                k9rs_app::Sense::Keep => self.theme.filter,
            };
            spans.push(Span::styled(step.label.clone(), style));
            // PER-STEP counts, because the aggregate could only say the
            // chain was empty, never which step emptied it.
            spans.push(Span::styled(format!("[{}]", step.kept), self.theme.title_counter));
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
            spans.extend(caret_spans(
                self.draft,
                self.caret_col,
                avail,
                self.theme.filter,
                self.theme.filter,
            ));
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

            let spans = self.spans(inner.width);
            let line = Line::from(spans);
            buf.set_line(inner.x, inner.y, &line, inner.width);
        } else {
            // Single-line fallback — the same spans in a smaller space, so
            // the windowing is the same decision rather than a second one.
            let line = Line::from(self.spans(area.width));
            buf.set_line(area.x, area.y, &line, area.width);
        }
    }
}
