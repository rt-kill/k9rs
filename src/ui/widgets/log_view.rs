use ratatui::{
    buffer::Buffer,
    layout::Rect,
    text::{Line, Span},
    widgets::{Block, Paragraph, StatefulWidget, Widget, Wrap},
};

use crate::ui::theme::Theme;
use crate::util::truncate_to_width;

/// State for the log viewer widget.
pub struct LogViewState {
    pub scroll: usize,
    pub follow: bool,
    pub wrap: bool,
    pub show_timestamps: bool,
    pub total_lines: usize,
    /// When the caller pre-windows the line data, this field holds the real
    /// scroll offset for accurate scrollbar rendering. If `None`, the widget
    /// uses `scroll` for the scrollbar.
    pub scroll_display: Option<usize>,
}

impl LogViewState {
    pub fn new() -> Self {
        Self {
            scroll: 0,
            follow: true,
            wrap: false,
            show_timestamps: true,
            total_lines: 0,
            scroll_display: None,
        }
    }

    pub fn scroll_up(&mut self, amount: usize) {
        self.follow = false;
        self.scroll = self.scroll.saturating_sub(amount);
    }

    pub fn scroll_down(&mut self, amount: usize, visible: usize) {
        let max_scroll = self.total_lines.saturating_sub(visible);
        self.scroll = (self.scroll + amount).min(max_scroll);
        // Re-enable follow if at the bottom
        if self.scroll >= max_scroll {
            self.follow = true;
        }
    }

    pub fn scroll_to_top(&mut self) {
        self.follow = false;
        self.scroll = 0;
    }

    pub fn scroll_to_bottom(&mut self, visible: usize) {
        self.scroll = self.total_lines.saturating_sub(visible);
        self.follow = true;
    }

    pub fn toggle_follow(&mut self, visible: usize) {
        self.follow = !self.follow;
        if self.follow {
            self.scroll = self.total_lines.saturating_sub(visible);
        }
    }

    pub fn toggle_wrap(&mut self) {
        self.wrap = !self.wrap;
    }

    pub fn toggle_timestamps(&mut self) {
        self.show_timestamps = !self.show_timestamps;
    }
}

impl Default for LogViewState {
    fn default() -> Self {
        Self::new()
    }
}

/// Log viewer widget.
///
/// Displays scrollable log output with follow mode, line wrapping,
/// and timestamp display toggle. Renders from a ring buffer of log lines.
///
/// Accepts `&[&str]` so it works with both `Vec<String>` and `VecDeque<String>`
/// (the caller converts to a slice of borrowed strings).
pub struct LogViewer<'a> {
    lines: &'a [&'a str],
    pod_name: &'a str,
    container_name: &'a str,
    since_label: &'a str,
    theme: &'a Theme,
}

impl<'a> LogViewer<'a> {
    pub fn new(
        lines: &'a [&'a str],
        pod_name: &'a str,
        container_name: &'a str,
        since_label: &'a str,
        theme: &'a Theme,
    ) -> Self {
        Self {
            lines,
            pod_name,
            container_name,
            since_label,
            theme,
        }
    }

    /// Parse a log line to separate timestamp from content.
    /// Kubernetes log timestamps are typically in RFC3339 format at the start.
    fn parse_timestamp(line: &str) -> Option<(&str, &str)> {
        // Typical format: "2024-01-15T10:30:00.123456789Z message..."
        // Timestamps are always ASCII, so check bytes directly to avoid
        // panicking on lines that start with multi-byte UTF-8 characters.
        let bytes = line.as_bytes();
        if bytes.len() > 30
            && bytes[0].is_ascii_digit()
            && bytes[1].is_ascii_digit()
            && bytes[2].is_ascii_digit()
            && bytes[3].is_ascii_digit()
            && bytes[4] == b'-'
        {
            // Find the end of the timestamp (space after the Z or +offset)
            if let Some(space_pos) = line.find(' ') {
                if space_pos <= 35 {
                    return Some((&line[..space_pos], &line[space_pos + 1..]));
                }
            }
        }
        None
    }
}

impl StatefulWidget for LogViewer<'_> {
    type State = LogViewState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        // Only update total_lines from the slice length if the caller hasn't
        // already set a higher value (e.g. when the caller pre-windows the data).
        if state.total_lines < self.lines.len() {
            state.total_lines = self.lines.len();
        }

        // Build title with follow/wrap/since indicators
        let follow_indicator = if state.follow { " \u{25cf}" } else { " \u{25cb}" }; // ● / ○
        let wrap_indicator = if state.wrap { " [WRAP]" } else { "" };
        let since_indicator = format!(" [{}]", self.since_label);
        let title = format!(
            " Logs: {}/{}{}{}{} ",
            self.pod_name, self.container_name, follow_indicator, wrap_indicator, since_indicator
        );

        let block = Block::bordered()
            .title(title)
            .title_style(self.theme.title)
            .border_style(self.theme.border);

        let inner = block.inner(area);
        block.render(area, buf);

        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let visible_height = inner.height as usize;

        // In follow mode, always show the latest lines
        if state.follow && self.lines.len() > visible_height {
            state.scroll = self.lines.len() - visible_height;
        }

        // Clamp scroll
        let max_scroll = self.lines.len().saturating_sub(visible_height);
        if state.scroll > max_scroll {
            state.scroll = max_scroll;
        }

        // Render visible lines
        if state.wrap {
            // In wrap mode, window the content starting at scroll offset to
            // avoid u16 overflow in Paragraph::scroll for large log buffers.
            let start_line = state.scroll.min(self.lines.len().saturating_sub(1));
            let mut text_lines: Vec<Line<'_>> = Vec::new();
            for line_idx in start_line..self.lines.len() {
                let line = self.lines[line_idx];
                if state.show_timestamps {
                    if let Some((ts, content)) = Self::parse_timestamp(line) {
                        text_lines.push(Line::from(vec![
                            Span::styled(ts.to_string(), self.theme.log_timestamp),
                            Span::styled(" ", self.theme.log_text),
                            Span::styled(content.to_string(), self.theme.log_text),
                        ]));
                    } else {
                        text_lines.push(Line::from(Span::styled(line.to_string(), self.theme.log_text)));
                    }
                } else {
                    let content = if let Some((_, content)) = Self::parse_timestamp(line) {
                        content
                    } else {
                        line
                    };
                    text_lines.push(Line::from(Span::styled(content.to_string(), self.theme.log_text)));
                }
            }
            // Windowing already handled — render from offset 0
            let paragraph = Paragraph::new(text_lines)
                .wrap(Wrap { trim: false })
                .scroll((0, 0));
            paragraph.render(inner, buf);
        } else {
            // No wrap: render each line individually, truncating to width
            let end = (state.scroll + visible_height).min(self.lines.len());
            for (vi, line_idx) in (state.scroll..end).enumerate() {
                let y = inner.y + vi as u16;
                let line = self.lines[line_idx];

                if state.show_timestamps {
                    if let Some((ts, content)) = Self::parse_timestamp(line) {
                        let spans = vec![
                            Span::styled(ts.to_string(), self.theme.log_timestamp),
                            Span::styled(" ", self.theme.log_text),
                            Span::styled(content.to_string(), self.theme.log_text),
                        ];
                        let styled_line = Line::from(spans);
                        buf.set_line(inner.x, y, &styled_line, inner.width);
                    } else {
                        buf.set_string(inner.x, y, line, self.theme.log_text);
                    }
                } else {
                    let content = if let Some((_, content)) = Self::parse_timestamp(line) {
                        content
                    } else {
                        line
                    };
                    let max_w = inner.width as usize;
                    let display = truncate_to_width(content, max_w);
                    buf.set_string(inner.x, y, display, self.theme.log_text);
                }
            }
        }

        // Scrollbar indicator (simple right-edge marks).
        // Use total_lines (the full log size) rather than self.lines.len()
        // so the scrollbar is correct even when the caller pre-windows the data.
        let total_for_scrollbar = state.total_lines;
        let max_scroll_total = total_for_scrollbar.saturating_sub(visible_height);
        if total_for_scrollbar > visible_height {
            let scrollbar_height = visible_height;
            let thumb_size = ((visible_height as f64 / total_for_scrollbar as f64)
                * scrollbar_height as f64)
                .max(1.0) as usize;
            // Use scroll_display (the real scroll offset before windowing) if
            // the caller pre-windowed the data, otherwise fall back to scroll.
            let scroll_for_bar = state
                .scroll_display
                .unwrap_or(state.scroll)
                .min(max_scroll_total);
            let thumb_pos = if max_scroll_total > 0 {
                ((scroll_for_bar as f64 / max_scroll_total as f64)
                    * (scrollbar_height - thumb_size) as f64) as usize
            } else {
                0
            };

            let scrollbar_x = inner.x + inner.width - 1;
            for i in 0..scrollbar_height {
                let y = inner.y + i as u16;
                if i >= thumb_pos && i < thumb_pos + thumb_size {
                    buf.set_string(scrollbar_x, y, "\u{2588}", self.theme.border);
                } else {
                    buf.set_string(scrollbar_x, y, "\u{2591}", self.theme.border);
                }
            }
        }
    }
}
