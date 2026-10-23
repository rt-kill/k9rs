use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::Span,
    Frame,
};

use k9rs_app::App;
use k9rs_app::store::StoredLine;
use crate::widgets::{LogFilterBar, LogViewer};

/// Draw the log streaming view for the TOP element (LogSession or a
/// LogFilter over one). Everything rendered is element-owned: the header
/// identity, the visible-line derivation (memoized), the display toggles.
///
/// Layout — the same shape as [`crate::views::resource::draw_resources`]:
/// chrome is a SIBLING of the content, not something the content widget paints
/// for itself, so it survives every state in which there is nothing to draw.
///
/// ```text
/// +--log content (fills remaining)------------------------------+
/// | Logs: pod/container ● [WRAP] [tail]                         |
/// +--grep bar (1 line, optional)--------------------------------+
/// | /error | timeout█  [0 visible]                              |
/// +--indicator bar (1 line)-------------------------------------+
/// +--keybinding bar (1 line)------------------------------------+
/// ```
pub fn draw_logs(f: &mut Frame, app: &mut App, area: Rect) {
    // Same authority as every other data view. `of_link` rather than `of`
    // because a log stream has no cluster-side stream state to consult: the
    // store can't self-report either — its `live` flag stays true across a
    // daemon gap (the bridge's `Ended` died in the closed channel, which is
    // why `revive_if_dead` exists at all) — so the link IS the whole answer,
    // and without it the view keeps advertising AutoScroll over a tail that
    // stopped arriving.
    let liveness = k9rs_app::Liveness::of_link(&app.conn);
    let link_connecting = !liveness.shows_data();

    // Materialize the element's visible-line set (the ephemeral log view),
    // then copy out the small display fields so the store read below can't
    // conflict with anything.
    let Some(indices) = app.nav.top_mut().log_visible() else { return };
    let element = app.nav.top();
    // `target` is whatever the stream is over: a pod, "3 pods", or
    // "deploy/api" — see `LogTarget::display`.
    let Some((target, container_label, since_label)) = element.log_header() else { return };
    let active_patterns = element.log_patterns();
    let grep_fallback = element.grep_fallback();
    // A pattern that couldn't compile and a history we couldn't collect are
    // both "what you see is not what you asked for"; the bar has one slot and
    // the broken pattern is the more urgent of the two.
    // ONE slot, three things that can make the result not what you asked
    // for, most urgent first: a pattern that could not compile, matches that
    // were dropped before this filter saw them, and smartcase having flipped
    // the search to case-sensitive. The last is the quietest — nothing else
    // on screen distinguishes "no such line" from "no such line WITH THAT
    // CAPITALISATION".
    let case_sensitive = active_patterns
        .iter()
        .any(|p| !p.is_empty() && k9rs_util::SearchPattern::new(p).is_case_sensitive());
    let grep_note = grep_fallback
        .clone()
        .or_else(|| {
            element
                .log_lost_history()
                .then(|| "older matches were dropped before this filter saw them".to_string())
        })
        .or_else(|| {
            case_sensitive.then(|| {
                "case-sensitive: a capital in the pattern turns smartcase off".to_string()
            })
        });
    // The chain WITH per-step counts, walked off the nav stack — the
    // element only knows its accumulated patterns, not what each step kept.
    let committed = app.nav.log_chain();
    let Some(view) = element.log_view() else { return };
    let (offset, anchor, follow, wrap, shed, pan, initial_load) = (
        view.viewport.offset(),
        view.viewport.anchor(),
        view.viewport.following(),
        view.wrap,
        view.shed,
        view.pan.offset(),
        view.initial_load,
    );
    let (filter_input_active, filter_input) = (
        view.is_filtering(),
        view.draft.clone().unwrap_or_default(),
    );
    let Some(store) = element.log_store().cloned() else { return };
    // Colours come from the SESSION's source list, not this element's. A grep
    // over a fan-out retains a SUBSET of the lines, so building the ordering
    // from its own store would renumber the pods that survived the filter and
    // recolour them the moment you typed a pattern.
    let sources = element.log_root_store().map(|s| s.sources()).unwrap_or_default();
    let theme = &app.ui.theme;
    let anim = &app.ui.anim;

    // The grep bar is built BEFORE the layout, because whether it claims a row
    // is its own question to answer — and it claims that row in every branch
    // below, including the ones with no content at all.
    let visible_count = indices.len();
    let grep_bar =
        LogFilterBar::new(filter_input_active, filter_input.value(), filter_input.cursor_col(), &committed, visible_count, theme)
            .fallback(grep_note.as_deref());
    let grep_height: u16 = if grep_bar.wants_row() { 1 } else { 0 };

    let chunks = Layout::vertical([
        Constraint::Fill(1),             // log content
        Constraint::Length(grep_height), // grep bar
        Constraint::Length(1),           // indicator bar
        Constraint::Length(1),           // keybinding bar
    ])
    .split(area);

    let log_area = chunks[0];
    let grep_area = chunks[1];
    let indicator_area = chunks[2];
    let bar_area = chunks[3];

    // ONE block for every state (connecting / waiting / empty / no match /
    // lines), so a reconnect or a grep that empties the view doesn't shift the
    // layout — and the toggle indicators keep answering `w`/`s` when there is
    // nothing to apply them to.
    let follow_indicator = if follow { " \u{25cf}" } else { " \u{25cb}" }; // ● / ○
    let wrap_indicator = if wrap { " [WRAP]" } else { "" };
    let block = ratatui::widgets::Block::bordered()
        .title(format!(
            " Logs: {}/{}{}{} [{}] ",
            target, container_label, follow_indicator, wrap_indicator, since_label
        ))
        .title_style(theme.title)
        .border_style(theme.border);
    // The drawable interior, MEASURED — never `height - 2` reconstructed from a
    // chrome constant the action layer had to keep in sync.
    let content_area = block.inner(log_area);
    f.render_widget(block, log_area);
    let inner_height = content_area.height as usize;

    // What this frame MEASURED: the offset the widget actually windowed from
    // and the extent it measured, or `None` if no content was drawn at all.
    //
    // An Option, not a pair of zeroed locals, because only a real measurement
    // may be published. Every status screen — connecting, waiting, no logs, no
    // matching lines — measures nothing, and publishing its zero extent
    // clamps the persisted offset to 0: type one character of a grep that
    // matches nothing, backspace it, and you are at the top of a 5000-line
    // log. The connecting path was guarded against exactly this by hand; the
    // other three were not, and a bool would have let the next one be missed
    // too.
    let mut measured: Option<(usize, usize, usize, k9rs_app::viewport::LineAnchor)> = None;
    if link_connecting {
        crate::draw_centered_loading(f, content_area, "Connecting...", theme.status_pending, anim);
    } else {
        store.with_read(|inner| {
            if visible_count > 0 {
                // Pass ALL visible lines; the widget windows PHYSICAL (wrap-
                // expanded) rows from the offset and reports the extent back.
                let all_lines: Vec<&StoredLine> =
                    indices.iter().filter_map(|&i| inner.lines.get(i)).collect();
                let log_viewer = LogViewer::new(&all_lines, theme);
                let mut view_state = crate::widgets::LogViewState {
                    offset,
                    // Store-absolute line sequence -> index into the VISIBLE
                    // slice. `indices` is ascending, so the partition point is
                    // "the first visible line at or after the anchor" — total
                    // for every way the anchor can go stale: the line was
                    // evicted (lands on the new front), a grep dropped it
                    // (lands on the next match), a grep dropped everything
                    // below it (lands past the end, which the widget parks at
                    // the bottom).
                    anchor: anchor.map(|a| {
                        let li = indices
                            .partition_point(|&i| inner.evicted + (i as u64) < a.seq);
                        (li, a.skip)
                    }),
                    top: None,
                    // Suppressed during the initial tail fetch so the view
                    // doesn't race while the batch streams in; the event loop
                    // clears `initial_load` once delivery gaps.
                    follow: follow && !initial_load,
                    wrap,
                    shed,
                    pan,
                    content_rows: 0,
                    widest: 0,
                    active_patterns: active_patterns.clone(),
                    sources: sources.clone(),
                };
                f.render_stateful_widget(log_viewer, content_area, &mut view_state);
                // ...and back: the slice index the widget actually drew from,
                // re-expressed as the line's own sequence so it survives
                // everything that renumbers the slice.
                let Some((top_line, top_skip)) = view_state.top else { return };
                if let Some(&i) = indices.get(top_line) {
                    measured = Some((
                        view_state.offset,
                        view_state.content_rows,
                        view_state.widest,
                        k9rs_app::viewport::LineAnchor {
                            seq: inner.evicted + i as u64,
                            skip: top_skip,
                        },
                    ));
                }
            } else if inner.live && inner.lines.is_empty() && committed.is_empty() {
                // "Waiting" only where waiting is the truth. A FILTER with an
                // empty store has drained its parent and found nothing —
                // which is "no matching lines", not "no data yet". Before the
                // filter owned a store it read the parent's, so an empty
                // result still saw the parent's lines and fell through
                // correctly; now the emptiness is its own.
                crate::draw_centered_loading(f, content_area, "Waiting for logs...", theme.status_pending, anim);
            } else if content_area.height > 0 && content_area.width > 0 {
                let msg = if inner.lines.is_empty() && committed.is_empty() {
                    "No logs."
                } else {
                    "No matching lines."
                };
                let line = ratatui::text::Line::from(Span::styled(msg, theme.status_pending));
                let cx = content_area.x + content_area.width.saturating_sub(msg.len() as u16) / 2;
                let cy = content_area.y + content_area.height / 2;
                f.render_widget(line, ratatui::layout::Rect::new(cx, cy, content_area.width, 1));
            }
        });
    }

    // The grep bar, in EVERY state — a pattern you can't see is a pattern you
    // can't correct, and "no lines matched" is exactly when you need to.
    f.render_widget(grep_bar, grep_area);

    // Publish what the render measured — the single write-back site, and it
    // runs only when there WAS a measurement (see `measured`).
    //
    // `apply_render_anchored`, not `set_metrics`: the widget resolved the
    // follow-window itself (it is the only thing that knows the wrap-expanded
    // extent), so this persists the offset it used alongside the extent it
    // measured. `set_metrics` would have snapped the offset AFTER the frame was
    // already drawn, leaving a following view one screen behind the tail — and
    // with no further data there is no next frame to catch up on.
    //
    // The `_anchored` half is what makes the position survive its own content:
    // the offset is only good for the extent it was measured against, and the
    // ring evicts under the window between frames.
    if let Some((rendered_offset, content_rows, widest, anchor)) = measured {
        if let Some(view) = app.nav.top_mut().log_view_mut() {
            view.viewport.apply_render_anchored(
                rendered_offset,
                anchor,
                content_rows,
                inner_height,
            );
            // Same contract on the other axis: the render measured it, the
            // action layer never guesses it.
            // `width - 1`: the widget reserves the last column for the
            // scrollbar and draws into the rest, so publishing the full
            // width put `max` one short and made the final column of the
            // widest line unreachable at every pan offset.
            view.pan.set_extent(widest, (content_area.width as usize).saturating_sub(1));
        }
    }

    // Indicator bar: element-owned toggle states.
    let follow_state = if follow { "On" } else { "Off" };
    let wrap_state = if wrap { "On" } else { "Off" };
    // ONE indicator for the whole cycle, naming what is showing rather than
    // one On/Off per role — which is the same collapse the key went through.
    let fields: Vec<&str> = k9rs_proto::protocol::Role::SHED_ORDER
        .iter()
        .filter(|r| r.shown(shed))
        .map(|r| match r {
            k9rs_proto::protocol::Role::Timestamp => "time",
            k9rs_proto::protocol::Role::Source => "source",
            k9rs_proto::protocol::Role::Body => "message",
        })
        .collect();
    let fields_state =
        if fields.is_empty() { "message".to_string() } else { format!("{}+message", fields.join("+")) };

    // GROUPS, not a flat span list: a bar that does not fit drops whole
    // indicators from the right and says so (`crate::header::fit_bar`),
    // and half of `Since:24h` would be worse than none of it. This used to
    // be one `Line::from(spans)` with no width at all — the same silent
    // right-edge truncation the hint bar below it had, at 76 columns with
    // every field shown.
    let pair = |label: &'static str, value: String, first: bool| -> Vec<Span<'static>> {
        let mut g = Vec::new();
        g.push(Span::styled(
            if first { " " } else { " \u{2502} " },
            theme.status_bar,
        ));
        g.push(Span::styled(label, theme.status_bar_key));
        g.push(Span::styled(value, theme.status_bar));
        g
    };
    let mut indicator_groups = vec![
        pair("AutoScroll:", follow_state.to_string(), true),
        pair("Wrap:", wrap_state.to_string(), false),
        pair("Fields:", fields_state.clone(), false),
        pair("Since:", since_label.to_string(), false),
    ];
    // A horizontally panned view is missing its left edge, which looks like
    // corruption rather than a setting unless the bar says so. Shown only
    // when it is non-zero — it is the only one of these that is a position
    // rather than a mode, and a permanent "Pan:0" is noise. And not under
    // wrap, where the widget ignores the pan entirely: advertising a shift
    // the content did not take is worse than saying nothing.

    if pan > 0 && !wrap {
        indicator_groups.push(pair("Pan:", format!("+{pan}"), false));
    }

    crate::fill_line_bg(f, indicator_area, theme.status_bar);
    let indicator_line =
        crate::header::fit_bar(indicator_groups, indicator_area.width, theme);
    f.render_widget(indicator_line, indicator_area);

    // Keybinding bar
    // Projected from the ONE keymap declaration — see `event::keymap`.
    let hints = k9rs_app::keymap::bar_hints(k9rs_app::keymap::ViewClass::Log);

    crate::fill_line_bg(f, bar_area, theme.status_bar);

    let line = crate::header::render_keybinding_bar(&hints, bar_area.width, theme);
    f.render_widget(line, bar_area);
}
