use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::StatefulWidget,
};
use unicode_segmentation::UnicodeSegmentation;

use k9rs_app::StoredLine;

use super::log_measure::{clip_left, line_cols, line_phys_rows, wrap_line};
use k9rs_proto::protocol::Role;
use k9rs_theme::Theme;

/// The colour for a source: its palette slot, ASSIGNED by first-seen order.
///
/// Not a hash. The previous one was `acc * 31 + byte`, and `31 mod 8 == -1`,
/// which makes the bucket an alternating sum of the bytes — replica names
/// sharing a long prefix therefore collapsed onto the low bits of a short
/// suffix, and four pods routinely drew three of one colour. Strengthening
/// the hash would not have fixed it either: four items in eight buckets
/// collide 59% of the time no matter how good the mixing is.
///
/// Hashing is for when you cannot know the set. We can: the daemon plans it
/// and every line carries its tag, so `sources` is the stream's own list and
/// the index into it is distinct by construction up to the palette size.
/// Past that the palette wraps, which is honest — eight is how many colours
/// are worth telling apart in a terminal.
fn source_color(name: &str, sources: &[String]) -> Color {
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
    // A source not in the list can only be one the store has not seen, which
    // cannot happen for a line the store is holding — total anyway.
    let slot = sources.iter().position(|s| s == name).unwrap_or(sources.len());
    PALETTE[slot % PALETTE.len()]
}

/// State for the log viewer widget. Pure data, snapshotted each draw.
///
/// It carries only what CONTENT rendering needs. Chrome — the bordered block,
/// the title indicators, the grep bar — belongs to the view, which draws it in
/// every state including the ones where there is no content at all.
pub(crate) struct LogViewState {
    /// Physical (wrap-expanded) row offset — from the element's Viewport on
    /// the way in, and WRITTEN BACK on the way out with the offset actually
    /// used (see `follow`).
    ///
    /// Only consulted when `anchor` is `None`; see there.
    pub offset: usize,
    /// Where to park, as a line in the VISIBLE SLICE plus how many wrapped
    /// rows into it — used INSTEAD of `offset` when present.
    ///
    /// The caller persists a store-absolute line sequence and resolves it to a
    /// slice index here, because it owns the store and the visible set; this
    /// widget turns the pair into a physical row, because it owns the wrap
    /// layout. Neither reaches into the other's arithmetic. `None` means the
    /// position was moved by a key since the last paint, so the raw `offset`
    /// is the truth for this frame.
    pub anchor: Option<(usize, usize)>,
    /// WRITTEN BACK: the (visible-slice line, intra-line row) the window
    /// actually started on, whatever decided it — anchor, raw offset, follow
    /// or the clamp. The caller turns it back into a line sequence and
    /// persists it as the next frame's anchor.
    ///
    /// `None` until the widget has actually drawn something. It bails on a
    /// zero-size rect WITHOUT measuring anything, and the caller could not
    /// tell that apart from a genuine `(0, 0)` — so one frame at a terminal
    /// height of four published a fabricated zero extent, which clamped the
    /// offset to the top of the buffer and re-anchored on the first line.
    /// An `Option` is the difference between "measured nothing" and
    /// "measured zero".
    pub top: Option<(usize, usize)>,
    /// Stick to the bottom. Resolved HERE rather than by the caller, because
    /// the bottom is `content_rows - visible_height` and this widget is the
    /// only thing that knows `content_rows` — the wrap-expanded physical row
    /// count. The caller used to publish the extent AFTER the render, so a
    /// following view rendered against the PREVIOUS frame's extent and sat one
    /// screen behind the tail; when the stream went quiet there was no next
    /// frame to catch up on, so it just stayed there saying "AutoScroll: On".
    pub follow: bool,
    pub wrap: bool,
    /// How many chrome roles are hidden — see [`Role::SHED_ORDER`].
    pub shed: usize,
    /// Display columns to skip off the left of every line. Ignored under
    /// `wrap`, where there is nothing to the right to pan to.
    pub pan: usize,
    /// WRITTEN BACK by the widget: total physical (wrap-expanded) row count,
    /// which the caller publishes via `Viewport::apply_render_anchored` —
    /// NOT `set_metrics`, which would re-resolve follow after the frame was
    /// already drawn (see `ui::views::log`'s write-back site).
    pub content_rows: usize,
    /// WRITTEN BACK: the widest line the widget drew, in display columns —
    /// what bounds the next pan keypress. Zero under `wrap`, which is the
    /// honest measurement: wrapped content has no off-screen right edge.
    pub widest: usize,
    /// All active filter patterns (committed + draft) for highlighting.
    pub active_patterns: Vec<String>,
    /// The stream's distinct source tags in first-seen order — a source's
    /// colour is its index here. From the SESSION's store, so a grep over a
    /// fan-out keeps the colours its parent assigned.
    pub sources: Vec<String>,
}

// `LogViewState` is pure data — the authoritative state lives in the nav
// element ([`k9rs_app::LogInteraction`], reached via
// `Element::log_view`). The view function snapshots into this struct by
// literal each draw, so impl methods on this type would never be called.

/// Log CONTENT widget: the scrollable, wrappable, filter-highlighted line
/// body and its scrollbar, drawn into the area the view hands it.
///
/// It owns no chrome. The view draws the bordered block and the grep bar
/// around it, because those must also appear in the states this widget is
/// never constructed for — connecting, no logs yet, nothing matched the grep.
pub(crate) struct LogViewer<'a> {
    /// Visible window of typed log lines. Each carries `content` plus an
    /// optional `source` — daemon-tagged whenever the stream has more than
    /// one source (`--all-containers`, or a multi-pod fan-out). The renderer
    /// colors it as a prefix directly, with no per-line string parsing.
    lines: &'a [&'a StoredLine],
    theme: &'a Theme,
}

impl<'a> LogViewer<'a> {
    pub(crate) fn new(lines: &'a [&'a StoredLine], theme: &'a Theme) -> Self {
        Self { lines, theme }
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
        // The scrollbar column is RESERVED, not shared. It is painted at
        // `inner.width - 1` after the text, so anything the layout put there
        // was silently overwritten: the last character of every line
        // disappeared the moment a log grew past one screen, and the pan
        // extent promised a final column that could never be seen. Reserving
        // it unconditionally also breaks the circularity — whether a
        // scrollbar is NEEDED depends on the wrapped height, which depends
        // on the width, which would depend on the scrollbar.
        let w = (inner.width as usize).saturating_sub(1);
        if w == 0 {
            return;
        }
        let content_width = w as u16;
        // The offset CLAMP lives in the element's Viewport; follow is
        // resolved here, in `window_from`, because the bottom is
        // `content_rows - visible_height` and only this widget knows the
        // wrap-expanded `content_rows` (see the `follow` field).

        // -- Content preparation (shared by wrap and non-wrap) --
        // Both modes style each line with `prepare_line`; wrap mode then
        // CHAR-wraps the styled spans into physical rows (`wrap_line`, breaking
        // mid-word at the column edge — deliberately char-wrap, unlike the
        // word-wrap the pre-Viewport `Paragraph` render used), non-wrap windows
        // logical lines directly.
        // Precompile filter patterns ONCE per frame (not per line).
        let compiled_patterns: Vec<k9rs_util::SearchPattern> = state.active_patterns.iter()
            .filter(|s| !s.is_empty())
            .map(|s| k9rs_util::SearchPattern::new(s))
            .collect();

        let sources: &[String] = &state.sources;
        let prepare_line = |line: &StoredLine, theme: &Theme, shed: usize,
                            compiled: &[k9rs_util::SearchPattern]| -> Line<'static> {
            let mut spans: Vec<Span<'static>> = Vec::new();

            // 1. Source prefix — the daemon's tag for where this line came
            //    from (a container, a pod, or `pod/container`). Present only
            //    for multi-source streams; single-source streams skip it.
            //    Normally drawn in the source's stable color, but when an
            //    active filter matches the source it's drawn highlighted —
            //    otherwise a line shown *because* its source matched would
            //    have no visible reason for being there.
            if let Some(source) = Role::Source
                .shown(shed)
                .then_some(line.source.as_deref())
                .flatten()
            {
                let style = if compiled.iter().any(|p| p.is_match(source)) {
                    theme.search_match
                } else {
                    Style::default().fg(source_color(source, sources))
                };
                spans.push(Span::styled(format!("{} ", source), style));
            }

            // 2. Timestamp — a field the DAEMON filled, not something sniffed
            //    out of the body. Toggling it off hides chrome; it can no
            //    longer remove text the workload wrote.
            if Role::Timestamp.shown(shed) {
                if let Some(ts) = &line.timestamp {
                    spans.push(Span::styled(ts.clone(), theme.log_timestamp));
                    spans.push(Span::styled(" ".to_string(), theme.log_text));
                }
            }
            let body = &line.content;

            // 3. Body: ANSI colors preserved, filter highlights overlaid.
            //    Always parse ANSI first. If filters active, find match
            //    ranges on stripped text and apply highlight to matching spans.
            let ansi_spans = crate::ansi::parse_ansi_line(body, theme.log_text);
            if !compiled.is_empty() {
                // Match in the ONE visible-text space, which is where the
                // filter decided this line was a hit — and which equals the
                // concatenated span content by construction
                // (`visible_text_matches_the_spans`), so these offsets index
                // the spans below. It used to BE that concatenation, built
                // per line per frame; the shared definition is the same
                // bytes and borrows when the line has no escapes at all.
                // Indexing spans with offsets from a differently-tokenized
                // view has already produced mis-highlights and a mid-char
                // slice panic here, which is why there is only one view.
                let visible = line.visible();
                let match_ranges = {
                    let mut ranges = Vec::new();
                    for pat in compiled {
                        ranges.extend(pat.find_all(visible));
                    }
                    ranges.sort_unstable();
                    // Snap every boundary OUT to a grapheme-cluster edge.
                    //
                    // A match can legitimately end inside a cluster — `/👩`
                    // against a ZWJ family, or any pattern whose match stops
                    // between a base char and its `U+FE0F`. Slicing the spans
                    // there splits one cluster into two, and the renderer
                    // then lays out MORE columns than `line_phys_rows`
                    // measured over whole clusters: the extent under-reports
                    // and a following view loses its tail while still saying
                    // AutoScroll is on. Snapping also stops half a glyph
                    // being highlighted, which was the same bug's visible
                    // half.
                    let snap = |byte: usize, out_forward: bool| -> usize {
                        let mut at = 0usize;
                        for g in visible.graphemes(true) {
                            let end = at + g.len();
                            if byte > at && byte < end {
                                return if out_forward { end } else { at };
                            }
                            if byte <= at {
                                break;
                            }
                            at = end;
                        }
                        byte
                    };
                    let ranges: Vec<(usize, usize)> = ranges
                        .into_iter()
                        .map(|(ms, me)| (snap(ms, false), snap(me, true)))
                        .collect();
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
        // Window from the bottom when following — `total` is known inside each
        // branch below, which is the earliest this can be decided.
        let window_from = |total: usize, parked: usize| -> usize {
            if state.follow {
                total.saturating_sub(visible_height)
            } else {
                parked.min(total.saturating_sub(visible_height))
            }
        };

        let total_physical = if state.wrap {
            // Per-line physical heights (cheap: width measurement, no styling).
            let heights: Vec<usize> = self.lines.iter()
                .map(|l| line_phys_rows(l, state.shed, w))
                .collect();
            let total: usize = heights.iter().sum();
            // The anchored line's physical row, AT THIS WIDTH. This is the whole
            // reason the anchor is resolved here and not by the caller: the row
            // a line starts on depends on the wrap heights of every line above
            // it, which only exist inside this frame. An anchor past the end
            // (the grep dropped everything below it) parks at the end, and a
            // skip past the line's height clamps into it — both total, so a
            // stale anchor degrades to a sensible position instead of a panic.
            let parked = match state.anchor {
                Some((li, skip)) => {
                    let li = li.min(heights.len());
                    let base: usize = heights[..li].iter().sum();
                    base + skip.min(heights.get(li).map_or(0, |h| h.saturating_sub(1)))
                }
                None => state.offset,
            };
            let offset = window_from(total, parked);
            state.offset = offset;
            // Map the physical offset to (start logical line, intra-line skip).
            let mut acc = 0usize;
            let mut start = 0usize;
            while start < heights.len() && acc + heights[start] <= offset {
                acc += heights[start];
                start += 1;
            }
            let skip = offset - acc;
            state.top = Some((start, skip));
            // Style + char-wrap forward only until the window fills — O(height).
            let mut phys: Vec<Line<'static>> = Vec::with_capacity(visible_height + 4);
            let mut li = start;
            while li < self.lines.len() && phys.len() < skip + visible_height {
                let styled = prepare_line(
                    self.lines[li], self.theme, state.shed, &compiled_patterns,
                );
                phys.extend(wrap_line(&styled, w));
                li += 1;
            }
            for (vi, row) in phys.iter().skip(skip).take(visible_height).enumerate() {
                buf.set_line(inner.x, inner.y + vi as u16, row, content_width);
            }
            // Wrapped: every column of every line is already on screen, so
            // there is nowhere to pan. Publishing 0 is what pins the pan at
            // the left margin — no special case in the action layer.
            state.widest = 0;
            total
        } else {
            let total = self.lines.len();
            // Unwrapped: one line IS one row, so the anchor's line index is
            // already the physical row. The skip is discarded rather than
            // honoured — there are no intra-line rows to skip into — which is
            // what makes toggling `w` off land on the anchored line's first row.
            let parked = match state.anchor {
                Some((li, _skip)) => li,
                None => state.offset,
            };
            let offset = window_from(total, parked);
            state.offset = offset;
            state.top = Some((offset, 0));
            let end = (offset + visible_height).min(total);
            // The widest line IN THE WINDOW, not in the store: it bounds the
            // next keypress, and measuring 50,000 lines to decide how far one
            // `l` may travel would cost more than the whole frame.
            let mut widest = 0usize;
            for (vi, idx) in (offset..end).enumerate() {
                let styled = prepare_line(
                    self.lines[idx], self.theme, state.shed, &compiled_patterns,
                );
                widest = widest.max(line_cols(&styled));
                let shifted = clip_left(&styled, state.pan);
                buf.set_line(inner.x, inner.y + vi as u16, &shifted, content_width);
            }
            state.widest = widest;
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
#[path = "../tests/widgets/log_view.rs"]
mod tests;
