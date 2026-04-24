use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    Frame,
};

use crate::app::{App, Route};
use crate::ui::widgets::LogViewer;

/// Draw the log streaming view.
///
/// Layout:
/// - Log content area (most of the space)
/// - Indicator bar showing toggle states
/// - Bottom bar showing keybindings
pub fn draw_logs(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.ui.theme;

    let chunks = Layout::vertical([
        Constraint::Fill(1),   // log content
        Constraint::Length(1), // indicator bar
        Constraint::Length(1), // keybinding bar
    ])
    .split(area);

    let log_area = chunks[0];
    let indicator_area = chunks[1];
    let bar_area = chunks[2];

    use crate::kube::protocol::LogContainer;
    // Extract pod/container and state from route. The container is the
    // typed `LogContainer`; the widget needs both a display label and a
    // boolean for "should I parse per-line container prefixes".
    let (pod_name, container_label, is_all_containers, logs) = match &app.route {
        Route::Logs { ref target, ref state, .. } => (
            target.pod.as_str(),
            target.container_label(),
            matches!(target.container, LogContainer::All),
            state.as_ref(),
        ),
        _ => return, // Not a log view — nothing to draw
    };
    let since_label = logs.since.as_deref().unwrap_or("tail");

    // Only collect the visible window from the VecDeque to avoid allocating
    // a Vec for all 50k lines every frame.
    //
    // When filters are active, iterate filtered_indices instead of raw lines.
    let filtered = &logs.filtered_indices;
    let is_filtered = !logs.filters.is_empty() || logs.draft_filter.is_some();
    let effective_total = if is_filtered { filtered.len() } else { logs.lines.len() };

    if effective_total > 0 {
        let total = effective_total;
        let inner_height = log_area.height.saturating_sub(2) as usize; // account for border
        let height = if inner_height == 0 { 1 } else { inner_height };

        if logs.wrap {
            // When wrap is enabled, pass all visible lines to the widget and let
            // ratatui's Paragraph handle wrapping and scrolling internally.
            let all_lines: Vec<&str> = if is_filtered {
                filtered.iter().map(|&i| logs.lines[i].as_str()).collect()
            } else {
                logs.lines.iter().map(|s| s.as_str()).collect()
            };

            let log_viewer = LogViewer::new(
                &all_lines,
                pod_name,
                container_label,
                is_all_containers,
                since_label,
                theme,
            );

            // For wrap mode, use the real scroll offset directly since
            // the widget receives all lines (not a pre-windowed slice).
            let scroll = if logs.follow {
                // Use a large value; the widget will clamp as needed.
                total.saturating_sub(1)
            } else {
                logs.scroll
            };

            let mut view_state = crate::ui::widgets::LogViewState {
                scroll,
                follow: logs.follow,
                wrap: logs.wrap,
                show_timestamps: logs.show_timestamps,
                total_lines: total,
                scroll_display: None,
                active_patterns: logs.active_patterns(),
                filter_input_active: logs.is_filtering(),
                filter_input: logs.draft_filter.clone().unwrap_or_default(),
                visible_count: logs.visible_count(),
                committed_filter_count: logs.filters.len(),
            };

            f.render_stateful_widget(log_viewer, log_area, &mut view_state);
        } else {
            // No wrap: only collect the visible window for efficiency.
            let scroll = if logs.follow {
                total.saturating_sub(height)
            } else {
                logs.scroll.min(total.saturating_sub(height))
            };
            let start = scroll;
            let end = (start + height).min(total);

            let visible_lines: Vec<&str> = if is_filtered {
                filtered[start..end].iter().map(|&i| logs.lines[i].as_str()).collect()
            } else {
                logs.lines.range(start..end).map(|s| s.as_str()).collect()
            };

            let log_viewer = LogViewer::new(
                &visible_lines,
                pod_name,
                container_label,
                is_all_containers,
                since_label,
                theme,
            );

            let mut view_state = crate::ui::widgets::LogViewState {
                scroll: 0,
                follow: logs.follow,
                wrap: logs.wrap,
                show_timestamps: logs.show_timestamps,
                total_lines: total,
                scroll_display: Some(scroll),
                active_patterns: logs.active_patterns(),
                filter_input_active: logs.is_filtering(),
                filter_input: logs.draft_filter.clone().unwrap_or_default(),
                visible_count: logs.visible_count(),
                committed_filter_count: logs.filters.len(),
            };

            f.render_stateful_widget(log_viewer, log_area, &mut view_state);
        }
    } else {
        // No log lines -- show appropriate message based on streaming state
        let since_title = if since_label == "tail" {
            String::new()
        } else {
            format!(" [{}]", since_label)
        };
        let block = ratatui::widgets::Block::bordered()
            .title(format!(" Logs: {}/{}{} ", pod_name, container_label, since_title))
            .title_style(theme.title)
            .border_style(theme.border);
        let inner = block.inner(log_area);
        f.render_widget(block, log_area);
        if logs.streaming {
            crate::ui::draw_centered_loading(f, inner, "Waiting for logs...", theme.status_pending);
        } else if inner.height > 0 && inner.width > 0 {
            let line = ratatui::text::Line::from(Span::styled("No logs.", theme.status_pending));
            let cx = inner.x + inner.width.saturating_sub(8) / 2;
            let cy = inner.y + inner.height / 2;
            f.render_widget(line, ratatui::layout::Rect::new(cx, cy, inner.width, 1));
        }
    }

    // Indicator bar: show toggle states
    let follow_state = if logs.follow { "On" } else { "Off" };
    let wrap_state = if logs.wrap { "On" } else { "Off" };
    let ts_state = if logs.show_timestamps { "On" } else { "Off" };

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
        Span::styled(since_label, theme.status_bar),
    ];

    // Fill indicator bar background
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

    // Fill background
    crate::ui::fill_line_bg(f, bar_area, theme.status_bar);

    let line = crate::ui::header::render_keybinding_bar(&hints, theme);
    f.render_widget(line, bar_area);
}
