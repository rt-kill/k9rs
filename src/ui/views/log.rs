use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    Frame,
};

use crate::app::App;
use crate::kube::protocol::LogLine;
use crate::ui::widgets::LogViewer;

/// Draw the log streaming view for the TOP element (LogSession or a
/// LogFilter over one). Everything rendered is element-owned: the header
/// identity, the visible-line derivation (memoized), the display toggles.
///
/// Layout:
/// - Log content area (most of the space)
/// - Indicator bar showing toggle states
/// - Bottom bar showing keybindings
pub fn draw_logs(f: &mut Frame, app: &mut App, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Fill(1),   // log content
        Constraint::Length(1), // indicator bar
        Constraint::Length(1), // keybinding bar
    ])
    .split(area);

    let log_area = chunks[0];
    let indicator_area = chunks[1];
    let bar_area = chunks[2];

    // Materialize the element's visible-line set (the ephemeral log view),
    // then copy out the small display fields so the store read below can't
    // conflict with anything.
    let Some(indices) = app.nav.top_mut().log_visible() else { return };
    let element = app.nav.top();
    let Some((pod_name, container_label, since_label)) = element.log_header() else { return };
    let active_patterns = element.log_patterns();
    let committed_filter_count = element.log_committed_count();
    let Some(view) = element.log_view() else { return };
    let (follow, wrap, show_timestamps, initial_load, scroll_pos) =
        (view.follow, view.wrap, view.show_timestamps, view.initial_load, view.scroll);
    let (filter_input_active, filter_input) = (
        view.is_filtering(),
        view.draft.clone().unwrap_or_default(),
    );
    let Some(store) = element.log_store().cloned() else { return };
    let theme = &app.ui.theme;

    store.with_read(|inner| {
        let total = indices.len();
        if total > 0 {
            let inner_height = log_area.height.saturating_sub(2) as usize; // border
            let height = if inner_height == 0 { 1 } else { inner_height };

            if wrap {
                // Wrap mode: pass all visible lines; the widget handles
                // wrapping and scrolling internally.
                let all_lines: Vec<&LogLine> =
                    indices.iter().filter_map(|&i| inner.lines.get(i)).collect();

                let log_viewer = LogViewer::new(
                    &all_lines,
                    &pod_name,
                    &container_label,
                    &since_label,
                    theme,
                );
                let scroll = if follow { total.saturating_sub(1) } else { scroll_pos };
                let mut view_state = crate::ui::widgets::LogViewState {
                    scroll,
                    follow,
                    initial_load,
                    wrap,
                    show_timestamps,
                    total_lines: total,
                    scroll_display: None,
                    active_patterns: active_patterns.clone(),
                    filter_input_active,
                    filter_input: filter_input.clone(),
                    visible_count: total,
                    committed_filter_count,
                };
                f.render_stateful_widget(log_viewer, log_area, &mut view_state);
            } else {
                // No wrap: only collect the visible window.
                let scroll = if follow {
                    total.saturating_sub(height)
                } else {
                    scroll_pos.min(total.saturating_sub(height))
                };
                let start = scroll;
                let end = (start + height).min(total);
                let visible_lines: Vec<&LogLine> = indices[start..end]
                    .iter()
                    .filter_map(|&i| inner.lines.get(i))
                    .collect();

                let log_viewer = LogViewer::new(
                    &visible_lines,
                    &pod_name,
                    &container_label,
                    &since_label,
                    theme,
                );
                let mut view_state = crate::ui::widgets::LogViewState {
                    scroll: 0,
                    follow,
                    initial_load,
                    wrap,
                    show_timestamps,
                    total_lines: total,
                    scroll_display: Some(scroll),
                    active_patterns: active_patterns.clone(),
                    filter_input_active,
                    filter_input: filter_input.clone(),
                    visible_count: total,
                    committed_filter_count,
                };
                f.render_stateful_widget(log_viewer, log_area, &mut view_state);
            }
        } else {
            // No visible lines — show a status by streaming state.
            let since_title = if since_label == "tail" {
                String::new()
            } else {
                format!(" [{}]", since_label)
            };
            let block = ratatui::widgets::Block::bordered()
                .title(format!(" Logs: {}/{}{} ", pod_name, container_label, since_title))
                .title_style(theme.title)
                .border_style(theme.border);
            let block_inner = block.inner(log_area);
            f.render_widget(block, log_area);
            if inner.live && inner.lines.is_empty() {
                crate::ui::draw_centered_loading(f, block_inner, "Waiting for logs...", theme.status_pending);
            } else if block_inner.height > 0 && block_inner.width > 0 {
                let msg = if inner.lines.is_empty() { "No logs." } else { "No matching lines." };
                let line = ratatui::text::Line::from(Span::styled(msg, theme.status_pending));
                let cx = block_inner.x + block_inner.width.saturating_sub(msg.len() as u16) / 2;
                let cy = block_inner.y + block_inner.height / 2;
                f.render_widget(line, ratatui::layout::Rect::new(cx, cy, block_inner.width, 1));
            }
        }
    });

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
