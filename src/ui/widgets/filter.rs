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
