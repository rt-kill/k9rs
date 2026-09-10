use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::StatefulWidget,
};
use unicode_width::UnicodeWidthChar;

use crate::kube::protocol::LogLine;
use crate::ui::theme::Theme;

/// Feed a text run into a running `(col, rows)` char-wrap accumulator at width
/// `w`. When `ansi`, escapes are skipped via [`crate::util::skip_ansi_escape`]
/// — the SAME tokenizer `parse_ansi_line` (the renderer) uses — so the counted
/// width cannot drift from the rendered width. Raw runs (container prefix,
/// timestamp) pass `ansi = false` and count every byte, matching how the
/// renderer emits those as raw (un-parsed) spans. Non-tab control chars are
/// width-0 (`unwrap_or(0)`), which equals the renderer dropping them.
fn wrap_feed(text: &str, ansi: bool, w: usize, col: &mut usize, rows: &mut usize) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if ansi {
            if let Some(next) = crate::util::skip_ansi_escape(bytes, i) {
                i = next;
                continue;
            }
        }
        let ch = text[i..].chars().next().unwrap();
        i += ch.len_utf8();
        let cw = UnicodeWidthChar::width(ch).unwrap_or(0);
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
/// container prefix + optional timestamp + body.
fn line_phys_rows(line: &LogLine, show_ts: bool, w: usize) -> usize {
    if w == 0 {
        return 1;
    }
    let mut rows = 1usize;
    let mut col = 0usize;
    if let Some(container) = &line.container {
        wrap_feed(&format!("{} ", container), false, w, &mut col, &mut rows);
    }
    match LogViewer::parse_timestamp(&line.content) {
        Some(LogTimestamp { timestamp, content }) if show_ts => {
            wrap_feed(timestamp, false, w, &mut col, &mut rows);
            wrap_feed(" ", false, w, &mut col, &mut rows);
            wrap_feed(content, true, w, &mut col, &mut rows);
        }
        Some(LogTimestamp { content, .. }) => wrap_feed(content, true, w, &mut col, &mut rows),
        None => wrap_feed(&line.content, true, w, &mut col, &mut rows),
    }
    rows
}

/// Char-wrap an already-styled line into physical rows of at most `w` display
/// columns, preserving span styles across the break. Same column logic as
/// [`line_phys_rows`], so heights and rendering agree exactly.
fn wrap_line(line: &Line<'static>, w: usize) -> Vec<Line<'static>> {
    if w == 0 {
        return vec![line.clone()];
    }
    let mut rows: Vec<Vec<Span<'static>>> = vec![Vec::new()];
    let mut col = 0usize;
    for span in &line.spans {
        let style = span.style;
        let mut cur = String::new();
        for c in span.content.chars() {
            let cw = UnicodeWidthChar::width(c).unwrap_or(0);
            if col + cw > w {
                if !cur.is_empty() {
                    rows.last_mut().unwrap().push(Span::styled(std::mem::take(&mut cur), style));
                }
                rows.push(Vec::new());
                col = 0;
            }
            cur.push(c);
            col += cw;
        }
        if !cur.is_empty() {
            rows.last_mut().unwrap().push(Span::styled(cur, style));
        }
    }
    rows.into_iter().map(Line::from).collect()
}

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

/// State for the log viewer widget. Pure data, snapshotted each draw.
///
/// It carries only what CONTENT rendering needs. Chrome — the bordered block,
/// the title indicators, the grep bar — belongs to the view, which draws it in
/// every state including the ones where there is no content at all.
pub struct LogViewState {
    /// Physical (wrap-expanded) row offset — from the element's Viewport.
    pub offset: usize,
    pub wrap: bool,
    pub show_timestamps: bool,
    /// WRITTEN BACK by the widget: total physical (wrap-expanded) row count,
    /// which the caller publishes to the Viewport via `set_metrics`.
    pub content_rows: usize,
    /// All active filter patterns (committed + draft) for highlighting.
    pub active_patterns: Vec<String>,
}

// `LogViewState` is pure data — the authoritative state lives in
// [`crate::app::LogState`] (inside `Route::Logs`). The view function
// snapshots into a `LogViewState` via struct literal each draw, so impl
// methods on this type would never be called.

/// Log CONTENT widget: the scrollable, wrappable, filter-highlighted line
/// body and its scrollbar, drawn into the area the view hands it.
///
/// It owns no chrome. The view draws the bordered block and the grep bar
/// around it, because those must also appear in the states this widget is
/// never constructed for — connecting, no logs yet, nothing matched the grep.
pub struct LogViewer<'a> {
    /// Visible window of typed log lines. Each carries `content` plus an
    /// optional source `container` (daemon-tagged for `--all-containers`
    /// streams); the renderer colors that container as a prefix directly,
    /// with no per-line string parsing.
    lines: &'a [&'a LogLine],
    theme: &'a Theme,
}

impl<'a> LogViewer<'a> {
    pub fn new(lines: &'a [&'a LogLine], theme: &'a Theme) -> Self {
        Self { lines, theme }
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
        let inner = area;
        if inner.height == 0 || inner.width == 0 {
            return;
        }

        let visible_height = inner.height as usize;
        let w = inner.width as usize;
        // Autoscroll (follow) and the offset clamp now live in the element's
        // Viewport (published back by the caller via `set_metrics`); the widget
        // just windows PHYSICAL rows from the offset it is handed.

        // -- Content preparation (shared by wrap and non-wrap) --
        // Both modes style each line with `prepare_line`; wrap mode then
        // CHAR-wraps the styled spans into physical rows (`wrap_line`, breaking
        // mid-word at the column edge — deliberately char-wrap, unlike the
        // word-wrap the pre-Viewport `Paragraph` render used), non-wrap windows
        // logical lines directly.
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
                // Match on the ACTUAL rendered text — the concatenated span
                // content — so the offsets align with the spans indexed below.
                // `strip_ansi` would be a THIRD, differently-tokenized view of
                // the line (different control-char/OSC/CSI rules than
                // `parse_ansi_line`) whose offsets don't map onto these spans,
                // causing mis-highlights and a mid-char / reversed-range slice
                // panic under multibyte content.
                let visible: String = ansi_spans.iter().map(|s| s.content.as_ref()).collect();
                let match_ranges = {
                    let mut ranges = Vec::new();
                    for pat in compiled {
                        ranges.extend(pat.find_all(&visible));
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

        // -- Rendering: window PHYSICAL (wrap-expanded) rows from the offset. --
        let total_physical = if state.wrap {
            // Per-line physical heights (cheap: width measurement, no styling).
            let heights: Vec<usize> = self.lines.iter()
                .map(|l| line_phys_rows(l, state.show_timestamps, w))
                .collect();
            let total: usize = heights.iter().sum();
            let offset = state.offset.min(total.saturating_sub(visible_height));
            // Map the physical offset to (start logical line, intra-line skip).
            let mut acc = 0usize;
            let mut start = 0usize;
            while start < heights.len() && acc + heights[start] <= offset {
                acc += heights[start];
                start += 1;
            }
            let skip = offset - acc;
            // Style + char-wrap forward only until the window fills — O(height).
            let mut phys: Vec<Line<'static>> = Vec::with_capacity(visible_height + 4);
            let mut li = start;
            while li < self.lines.len() && phys.len() < skip + visible_height {
                let styled = prepare_line(self.lines[li], self.theme, state.show_timestamps, &compiled_patterns);
                phys.extend(wrap_line(&styled, w));
                li += 1;
            }
            for (vi, row) in phys.iter().skip(skip).take(visible_height).enumerate() {
                buf.set_line(inner.x, inner.y + vi as u16, row, inner.width);
            }
            total
        } else {
            let total = self.lines.len();
            let offset = state.offset.min(total.saturating_sub(visible_height));
            let end = (offset + visible_height).min(total);
            for (vi, idx) in (offset..end).enumerate() {
                let styled = prepare_line(self.lines[idx], self.theme, state.show_timestamps, &compiled_patterns);
                buf.set_line(inner.x, inner.y + vi as u16, &styled, inner.width);
            }
            total
        };
        // Publish the physical extent for the caller to feed back to the Viewport.
        state.content_rows = total_physical;

        // Scrollbar — physical units throughout.
        let max_scroll_total = total_physical.saturating_sub(visible_height);
        if total_physical > visible_height {
            let scrollbar_height = visible_height;
            let thumb_size = ((visible_height as f64 / total_physical as f64)
                * scrollbar_height as f64)
                .max(1.0) as usize;
            let scroll_for_bar = state.offset.min(max_scroll_total);
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

#[cfg(test)]
#[path = "../../tests/ui/widgets/log_view.rs"]
mod tests;
