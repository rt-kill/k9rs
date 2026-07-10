use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Paragraph, StatefulWidget, Widget, Wrap},
};

use crate::kube::protocol::LogLine;
use crate::ui::theme::Theme;

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

/// State for the log viewer widget.
pub struct LogViewState {
    pub scroll: usize,
    pub follow: bool,
    /// True during initial tail fetch — suppresses follow-mode auto-scroll
    /// so the view doesn't jump as lines stream in.
    pub initial_load: bool,
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
    /// Visible window of typed log lines. Each carries `content` plus an
    /// optional source `container` (daemon-tagged for `--all-containers`
    /// streams); the renderer colors that container as a prefix directly,
    /// with no per-line string parsing.
    lines: &'a [&'a LogLine],
    pod_name: &'a str,
    /// User-facing label for the header bar — derived from the typed
    /// [`crate::kube::protocol::LogContainer`] via `ContainerRef::container_label`.
    container_label: &'a str,
    since_label: &'a str,
    theme: &'a Theme,
}

impl<'a> LogViewer<'a> {
    pub fn new(
        lines: &'a [&'a LogLine],
        pod_name: &'a str,
        container_label: &'a str,
        since_label: &'a str,
        theme: &'a Theme,
    ) -> Self {
        Self {
            lines,
            pod_name,
            container_label,
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

        // In follow mode, snap to the latest lines — but skip during
        // initial_load to prevent the view from jumping as the initial
        // tail batch streams in. The snap happens once when initial_load
        // transitions to false.
        if state.follow && !state.initial_load && self.lines.len() > visible_height {
            state.scroll = self.lines.len() - visible_height;
        }

        // Clamp scroll
        let max_scroll = self.lines.len().saturating_sub(visible_height);
        if state.scroll > max_scroll {
            state.scroll = max_scroll;
        }

        // -- Content preparation: single pipeline for both wrap/non-wrap --
        // Rendering is agnostic — wrap flows lines into a Paragraph,
        // non-wrap renders line-by-line. Same content logic for both.
        // Precompile filter patterns ONCE per frame (not per line).
        let compiled_patterns: Vec<crate::util::SearchPattern> = state.active_patterns.iter()
            .filter(|s| !s.is_empty())
            .map(|s| crate::util::SearchPattern::new(s))
            .collect();

        let prepare_line = |line: &LogLine, theme: &Theme, show_ts: bool,
                            compiled: &[crate::util::SearchPattern]| -> Line<'static> {
            let mut spans: Vec<Span<'static>> = Vec::new();

            // 1. Container prefix — the daemon-tagged source container. Present
            //    only for --all-containers streams (where `container` is `Some`);
            //    single-container streams skip it. Normally drawn in the
            //    container's stable color, but when an active filter matches the
            //    container name it's drawn highlighted — otherwise a line shown
            //    *because* its container matched the grep would have no visible
            //    reason for being there.
            if let Some(container) = &line.container {
                let style = if compiled.iter().any(|p| p.is_match(container)) {
                    theme.search_match
                } else {
                    Style::default().fg(container_color(container))
                };
                spans.push(Span::styled(format!("{} ", container), style));
            }

            // 2. Timestamp (optional), parsed from the line content.
            let body = if show_ts {
                if let Some(LogTimestamp { timestamp: ts, content }) = Self::parse_timestamp(&line.content) {
                    spans.push(Span::styled(ts.to_string(), theme.log_timestamp));
                    spans.push(Span::styled(" ".to_string(), theme.log_text));
                    content
                } else {
                    &line.content
                }
            } else if let Some(LogTimestamp { content, .. }) = Self::parse_timestamp(&line.content) {
                content
            } else {
                &line.content
            };

            // 3. Body: ANSI colors preserved, filter highlights overlaid.
            //    Always parse ANSI first. If filters active, find match
            //    ranges on stripped text and apply highlight to matching spans.
            let ansi_spans = crate::util::parse_ansi_line(body, theme.log_text);
            if !compiled.is_empty() {
                let stripped = crate::util::strip_ansi(body);
                let match_ranges = {
                    let mut ranges = Vec::new();
                    for pat in compiled {
                        ranges.extend(pat.find_all(&stripped));
                    }
                    ranges.sort_unstable();
                    // Merge overlapping ranges from multiple patterns.
                    let mut merged: Vec<(usize, usize)> = Vec::new();
                    for (ms, me) in ranges {
                        if let Some(last) = merged.last_mut() {
                            if ms <= last.1 {
                                last.1 = last.1.max(me);
                                continue;
                            }
                        }
                        merged.push((ms, me));
                    }
                    merged
                };
                if match_ranges.is_empty() {
                    spans.extend(ansi_spans.into_iter().map(|s| {
                        Span::styled(s.content.to_string(), s.style)
                    }));
                } else {
                    let mut pos: usize = 0;
                    for s in &ansi_spans {
                        let text = s.content.as_ref();
                        let span_start = pos;
                        let span_end = pos + text.len();

                        let mut cursor = 0usize;
                        for &(ms, me) in &match_ranges {
                            if ms >= span_end || me <= span_start { continue; }
                            let local_start = ms.saturating_sub(span_start);
                            let local_end = (me - span_start).min(text.len());
                            if local_start > cursor {
                                spans.push(Span::styled(text[cursor..local_start].to_string(), s.style));
                            }
                            spans.push(Span::styled(text[local_start..local_end].to_string(), theme.search_match));
                            cursor = local_end;
                        }
                        if cursor < text.len() {
                            spans.push(Span::styled(text[cursor..].to_string(), s.style));
                        } else if cursor == 0 {
                            spans.push(Span::styled(text.to_string(), s.style));
                        }
                        pos = span_end;
                    }
                }
            } else {
                spans.extend(ansi_spans.into_iter().map(|s| {
                    Span::styled(s.content.to_string(), s.style)
                }));
            }

            Line::from(spans)
        };

        // -- Rendering: wrap vs non-wrap are purely layout concerns --
        if state.wrap {
            // Window from scroll position to avoid O(n) preparation of all lines.
            // In wrap mode, a single logical line may occupy multiple visual rows,
            // so we prepare a generous window and let Paragraph handle wrapping.
            let start = state.scroll.min(self.lines.len().saturating_sub(1));
            let text_lines: Vec<Line<'static>> = self.lines[start..]
                .iter()
                .map(|&line| prepare_line(line, self.theme, state.show_timestamps, &compiled_patterns))
                .collect();
            let paragraph = Paragraph::new(text_lines)
                .wrap(Wrap { trim: false })
                .scroll((0, 0));
            paragraph.render(inner, buf);
        } else {
            let end = (state.scroll + visible_height).min(self.lines.len());
            for (vi, line_idx) in (state.scroll..end).enumerate() {
                let y = inner.y + vi as u16;
                let styled = prepare_line(self.lines[line_idx], self.theme, state.show_timestamps, &compiled_patterns);
                buf.set_line(inner.x, y, &styled, inner.width);
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
