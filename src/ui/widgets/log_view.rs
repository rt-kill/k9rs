use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Paragraph, StatefulWidget, Widget, Wrap},
};

use crate::ui::theme::Theme;
use crate::util::truncate_to_width;

/// Parsed timestamp and content from a Kubernetes log line.
struct LogTimestamp<'a> {
    timestamp: &'a str,
    content: &'a str,
}

/// Stable color for a container name. Uses a simple hash-to-palette so the
/// same container always gets the same color across log lines.
fn container_color(name: &str) -> Color {
    const PALETTE: [Color; 8] = [
        Color::Cyan,
        Color::Green,
        Color::Yellow,
        Color::Blue,
        Color::Magenta,
        Color::Red,
        Color::LightCyan,
        Color::LightGreen,
    ];
    let hash: usize = name.bytes().fold(0usize, |acc, b| acc.wrapping_mul(31).wrapping_add(b as usize));
    PALETTE[hash % PALETTE.len()]
}

/// Try to parse a container-name prefix from a multi-container log line.
/// kubectl `--all-containers` prefixes each line with the container name
/// followed by a space. Returns `(prefix, rest)` if found.
fn parse_container_prefix(line: &str) -> Option<(&str, &str)> {
    // Container prefix is the first word, must not contain '=' or ':' (those
    // are timestamp or key=value patterns, not prefixes).
    let space_pos = line.find(' ')?;
    let prefix = &line[..space_pos];
    if prefix.is_empty() || prefix.contains('=') || prefix.contains(':') || prefix.contains('/') {
        return None;
    }
    Some((prefix, &line[space_pos + 1..]))
}

/// Split a text line into spans, highlighting matches for all active filter
/// patterns using SearchPattern (smartcase regex).
fn highlight_filters<'a>(text: &'a str, patterns: &[String], normal: Style, highlight: Style) -> Line<'a> {
    if patterns.is_empty() {
        return Line::from(Span::styled(text, normal));
    }
    // Collect all match ranges from all patterns.
    let mut all_matches: Vec<(usize, usize)> = Vec::new();
    for term in patterns {
        if term.is_empty() { continue; }
        let pat = crate::util::SearchPattern::new(term);
        all_matches.extend(pat.find_all(text));
    }
    if all_matches.is_empty() {
        return Line::from(Span::styled(text, normal));
    }
    // Sort by start position, then merge overlapping ranges.
    all_matches.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (s, e) in all_matches {
        if let Some(last) = merged.last_mut() {
            if s <= last.1 {
                last.1 = last.1.max(e);
                continue;
            }
        }
        merged.push((s, e));
    }
    let mut spans = Vec::new();
    let mut last = 0;
    for (start, end) in merged {
        if start > last {
            spans.push(Span::styled(&text[last..start], normal));
        }
        spans.push(Span::styled(&text[start..end], highlight));
        last = end;
    }
    if last < text.len() {
        spans.push(Span::styled(&text[last..], normal));
    }
    Line::from(spans)
}

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
    /// All active filter patterns (committed + draft) for highlighting.
    pub active_patterns: Vec<String>,
    /// Whether the filter input bar is active (draft being typed).
    pub filter_input_active: bool,
    /// Text being typed in the filter input bar.
    pub filter_input: String,
    /// Total number of visible lines after filtering.
    pub visible_count: usize,
    /// Number of committed (stacked) filters.
    pub committed_filter_count: usize,
}

// `LogViewState` is pure data — the authoritative state lives in
// [`crate::app::LogState`] (inside `Route::Logs`). The view function
// snapshots into a `LogViewState` via struct literal each draw, so impl
// methods on this type would never be called.

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
    /// User-facing label for the header bar — derived from the typed
    /// [`crate::kube::protocol::LogContainer`] via `ContainerRef::container_label`.
    container_label: &'a str,
    /// Whether this view is streaming all containers (vs a single one).
    /// Drives the per-line container-prefix parsing & coloring. Replaces
    /// the previous `container_name == "all"` magic-string check.
    is_all_containers: bool,
    since_label: &'a str,
    theme: &'a Theme,
}

impl<'a> LogViewer<'a> {
    pub fn new(
        lines: &'a [&'a str],
        pod_name: &'a str,
        container_label: &'a str,
        is_all_containers: bool,
        since_label: &'a str,
        theme: &'a Theme,
    ) -> Self {
        Self {
            lines,
            pod_name,
            container_label,
            is_all_containers,
            since_label,
            theme,
        }
    }

    /// Parse a log line to separate timestamp from content.
    /// Kubernetes log timestamps are typically in RFC3339 format at the start.
    fn parse_timestamp(line: &str) -> Option<LogTimestamp<'_>> {
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
                    return Some(LogTimestamp {
                        timestamp: &line[..space_pos],
                        content: &line[space_pos + 1..],
                    });
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
            self.pod_name, self.container_label, follow_indicator, wrap_indicator, since_indicator
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
            let mut text_lines: Vec<Line<'_>> = Vec::with_capacity(visible_height);
            for line_idx in start_line..self.lines.len() {
                let line = self.lines[line_idx];
                if state.show_timestamps {
                    if let Some(LogTimestamp { timestamp: ts, content }) = Self::parse_timestamp(line) {
                        text_lines.push(Line::from(vec![
                            Span::styled(ts, self.theme.log_timestamp),
                            Span::styled(" ", self.theme.log_text),
                            Span::styled(content, self.theme.log_text),
                        ]));
                    } else {
                        text_lines.push(Line::from(Span::styled(line, self.theme.log_text)));
                    }
                } else {
                    let content = if let Some(LogTimestamp { content, .. }) = Self::parse_timestamp(line) {
                        content
                    } else {
                        line
                    };
                    text_lines.push(Line::from(Span::styled(content, self.theme.log_text)));
                }
            }
            // Windowing already handled — render from offset 0
            let paragraph = Paragraph::new(text_lines)
                .wrap(Wrap { trim: false })
                .scroll((0, 0));
            paragraph.render(inner, buf);
        } else {
            // No wrap: render each line individually, truncating to width.
            let patterns = &state.active_patterns;
            let end = (state.scroll + visible_height).min(self.lines.len());
            for (vi, line_idx) in (state.scroll..end).enumerate() {
                let y = inner.y + vi as u16;
                let line = self.lines[line_idx];

                let content = if state.show_timestamps {
                    // Render timestamp prefix + content with filter highlighting.
                    if let Some(LogTimestamp { timestamp: ts, content }) = Self::parse_timestamp(line) {
                        let ts_span = Span::styled(format!("{} ", ts), self.theme.log_timestamp);
                        let content_line = highlight_filters(content, patterns, self.theme.log_text, self.theme.search_match);
                        let mut spans = vec![ts_span];
                        spans.extend(content_line.spans);
                        let styled_line = Line::from(spans);
                        buf.set_line(inner.x, y, &styled_line, inner.width);
                        continue;
                    }
                    line
                } else {
                    if let Some(LogTimestamp { content, .. }) = Self::parse_timestamp(line) {
                        content
                    } else {
                        line
                    }
                };
                // Per-container colored prefix for multi-container logs.
                let (prefix_span, display_content) = if self.is_all_containers {
                    if let Some((prefix, rest)) = parse_container_prefix(content) {
                        let color = container_color(prefix);
                        (Some(Span::styled(format!("{} ", prefix), Style::default().fg(color))), rest)
                    } else {
                        (None, content)
                    }
                } else {
                    (None, content)
                };
                let max_w = inner.width as usize;
                let display = truncate_to_width(display_content, max_w);
                if patterns.is_empty() && prefix_span.is_none() {
                    buf.set_string(inner.x, y, display, self.theme.log_text);
                } else {
                    let mut spans = Vec::new();
                    if let Some(ps) = prefix_span {
                        spans.push(ps);
                    }
                    let highlighted = highlight_filters(display, patterns, self.theme.log_text, self.theme.search_match);
                    spans.extend(highlighted.spans);
                    buf.set_line(inner.x, y, &Line::from(spans), inner.width);
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

        // Filter bar at the bottom of the log area.
        if state.filter_input_active || state.committed_filter_count > 0 {
            let bar_y = inner.y + inner.height.saturating_sub(1);
            // Clear the line.
            for x in inner.x..inner.x + inner.width {
                buf.set_string(x, bar_y, " ", self.theme.status_bar);
            }
            let mut spans = vec![Span::styled(" /", self.theme.status_bar_key)];
            if state.filter_input_active {
                spans.push(Span::styled(&state.filter_input, self.theme.filter));
                spans.push(Span::styled("\u{2588}", self.theme.filter));
                // Show live visible count while typing
                spans.push(Span::styled(
                    format!("  [{} visible]", state.visible_count),
                    self.theme.title_counter,
                ));
            } else {
                // Show committed filter stack summary
                let label = state.active_patterns.join(" | ");
                spans.push(Span::styled(label, self.theme.filter));
                spans.push(Span::styled(
                    format!("  [{} filter{}, {} visible]",
                        state.committed_filter_count,
                        if state.committed_filter_count == 1 { "" } else { "s" },
                        state.visible_count),
                    self.theme.title_counter,
                ));
            }
            buf.set_line(inner.x, bar_y, &Line::from(spans), inner.width);
        }
    }
}
