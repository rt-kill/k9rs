use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    Frame,
};

use crate::app::App;
use crate::kube::protocol::LogLine;
use crate::ui::widgets::{LogFilterBar, LogViewer};

/// Draw the log streaming view for the TOP element (LogSession or a
/// LogFilter over one). Everything rendered is element-owned: the header
/// identity, the visible-line derivation (memoized), the display toggles.
///
/// Layout — the same shape as [`crate::ui::views::resource::draw_resources`]:
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
    let liveness = crate::app::Liveness::of_link(&app.conn);
    let link_connecting = !liveness.shows_data();

    // Materialize the element's visible-line set (the ephemeral log view),
    // then copy out the small display fields so the store read below can't
    // conflict with anything.
    let Some(indices) = app.nav.top_mut().log_visible() else { return };
    let element = app.nav.top();
    let Some((pod_name, container_label, since_label)) = element.log_header() else { return };
    let active_patterns = element.log_patterns();
    let committed = element.log_committed_patterns();
    let Some(view) = element.log_view() else { return };
    let (offset, follow, wrap, show_timestamps, initial_load) = (
        view.viewport.offset(),
        view.viewport.following(),
        view.wrap,
        view.show_timestamps,
        view.initial_load,
    );
    let (filter_input_active, filter_input) = (
        view.is_filtering(),
        view.draft.clone().unwrap_or_default(),
    );
    let Some(store) = element.log_store().cloned() else { return };
    let theme = &app.ui.theme;
    let anim = &app.ui.anim;

    // The grep bar is built BEFORE the layout, because whether it claims a row
    // is its own question to answer — and it claims that row in every branch
    // below, including the ones with no content at all.
    let visible_count = indices.len();
    let grep_bar =
        LogFilterBar::new(filter_input_active, &filter_input, &committed, visible_count, theme);
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
            pod_name, container_label, follow_indicator, wrap_indicator, since_label
        ))
        .title_style(theme.title)
        .border_style(theme.border);
    // The drawable interior, MEASURED — never `height - 2` reconstructed from a
    // chrome constant the action layer had to keep in sync.
    let content_area = block.inner(log_area);
    f.render_widget(block, log_area);
    let inner_height = content_area.height as usize;

    let mut content_rows = 0usize;
    if link_connecting {
        crate::ui::draw_centered_loading(f, content_area, "Connecting...", theme.status_pending, anim);
    } else {
        store.with_read(|inner| {
            if visible_count > 0 {
                // Pass ALL visible lines; the widget windows PHYSICAL (wrap-
                // expanded) rows from the offset and reports the extent back.
                let all_lines: Vec<&LogLine> =
                    indices.iter().filter_map(|&i| inner.lines.get(i)).collect();
                let log_viewer = LogViewer::new(&all_lines, theme);
                let mut view_state = crate::ui::widgets::LogViewState {
                    offset,
                    wrap,
                    show_timestamps,
                    content_rows: 0,
                    active_patterns: active_patterns.clone(),
                };
                f.render_stateful_widget(log_viewer, content_area, &mut view_state);
                content_rows = view_state.content_rows;
            } else if inner.live && inner.lines.is_empty() {
                crate::ui::draw_centered_loading(f, content_area, "Waiting for logs...", theme.status_pending, anim);
            } else if content_area.height > 0 && content_area.width > 0 {
                let msg = if inner.lines.is_empty() { "No logs." } else { "No matching lines." };
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

    // Publish the physical extent the render measured back into the Viewport —
    // the single write-back site; autoscroll is gated on the initial tail load.
    // Skipped while the connecting screen is up: that frame measured no
    // content, and publishing a zero extent would clamp the offset to 0 and
    // lose the user's scroll position across the gap.
    if !link_connecting {
        if let Some(view) = app.nav.top_mut().log_view_mut() {
            view.viewport.set_metrics(content_rows, inner_height, follow && !initial_load);
        }
    }

    // Indicator bar: element-owned toggle states.
    let follow_state = if follow { "On" } else { "Off" };
    let wrap_state = if wrap { "On" } else { "Off" };
    let ts_state = if show_timestamps { "On" } else { "Off" };

    let indicator_spans = vec![
        Span::styled(" AutoScroll:", theme.status_bar_key),
        Span::styled(follow_state, theme.status_bar),
        Span::styled(" \u{2502} ", theme.status_bar),
        Span::styled("Wrap:", theme.status_bar_key),
        Span::styled(wrap_state, theme.status_bar),
        Span::styled(" \u{2502} ", theme.status_bar),
        Span::styled("Timestamps:", theme.status_bar_key),
        Span::styled(ts_state, theme.status_bar),
        Span::styled(" \u{2502} ", theme.status_bar),
        Span::styled("Since:", theme.status_bar_key),
        Span::styled(since_label.as_str(), theme.status_bar),
    ];

    crate::ui::fill_line_bg(f, indicator_area, theme.status_bar);
    let indicator_line = Line::from(indicator_spans);
    f.render_widget(indicator_line, indicator_area);

    // Keybinding bar
    let hints = vec![
        ("s", "follow"),
        ("w", "wrap"),
        ("t", "timestamps"),
        ("0", "tail"),
        ("1", "1m"),
        ("2", "5m"),
        ("3", "15m"),
        ("4", "30m"),
        ("5", "1h"),
        ("6", "24h"),
        ("q", "back"),
    ];

    crate::ui::fill_line_bg(f, bar_area, theme.status_bar);

    let line = crate::ui::header::render_keybinding_bar(&hints, theme);
    f.render_widget(line, bar_area);
}
