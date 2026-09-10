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
            theme,
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
    /// Committed greps, oldest first.
    committed: &'a [String],
    /// Lines visible through the whole chain, draft included.
    visible_count: usize,
    theme: &'a Theme,
}

impl<'a> LogFilterBar<'a> {
    pub fn new(
        active: bool,
        draft: &'a str,
        committed: &'a [String],
        visible_count: usize,
        theme: &'a Theme,
    ) -> Self {
        Self { active, draft, committed, visible_count, theme }
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
        for (i, pattern) in self.committed.iter().enumerate() {
            if i > 0 {
                spans.push(Span::styled(" | ", self.theme.status_bar));
            }
            spans.push(Span::styled(pattern.clone(), self.theme.filter));
        }
        if self.active {
            if !self.committed.is_empty() {
                spans.push(Span::styled(" | ", self.theme.status_bar));
            }
            spans.push(Span::styled(self.draft.to_string(), self.theme.filter));
            spans.push(Span::styled("\u{2588}", self.theme.filter));
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
                spans.push(Span::styled(
                    format!("  [{}/{}]", self.match_count, self.total_count),
                    self.theme.title_counter,
                ));
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
                spans.push(Span::styled(
                    format!("  [{}/{}]", self.match_count, self.total_count),
                    self.theme.title_counter,
                ));
            }
            let line = Line::from(spans);
            buf.set_line(area.x, area.y, &line, area.width);
        }
    }
}
