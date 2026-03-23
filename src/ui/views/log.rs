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
    let theme = &app.theme;

    let chunks = Layout::vertical([
        Constraint::Fill(1),   // log content
        Constraint::Length(1), // indicator bar
        Constraint::Length(1), // keybinding bar
    ])
    .split(area);

    let log_area = chunks[0];
    let indicator_area = chunks[1];
    let bar_area = chunks[2];

    // Extract pod/container from route
    let (pod_name, container_name) = match &app.route {
        Route::Logs { pod, container, .. } => (pod.as_str(), container.as_str()),
        _ => ("unknown", "unknown"),
    };

    let logs = &app.logs;
    let since_label = logs.since.as_deref().unwrap_or("tail");

    // Only collect the visible window from the VecDeque to avoid allocating
    // a Vec for all 50k lines every frame.
    if !logs.lines.is_empty() {
        let total = logs.lines.len();
        let inner_height = log_area.height.saturating_sub(2) as usize; // account for border
        let height = if inner_height == 0 { 1 } else { inner_height };

        if logs.wrap {
            // When wrap is enabled, pass all lines to the widget and let
            // ratatui's Paragraph handle wrapping and scrolling internally.
            let all_lines: Vec<&str> = logs.lines.iter().map(|s| s.as_str()).collect();

            let log_viewer = LogViewer::new(
                &all_lines,
                pod_name,
                container_name,
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

            let visible_lines: Vec<&str> = logs.lines.range(start..end).map(|s| s.as_str()).collect();

            let log_viewer = LogViewer::new(
                &visible_lines,
                pod_name,
                container_name,
                since_label,
                theme,
            );

            let mut view_state = crate::ui::widgets::LogViewState {
                scroll: 0, // lines are already windowed, widget renders from index 0
                follow: logs.follow,
                wrap: logs.wrap,
                show_timestamps: logs.show_timestamps,
                total_lines: total,
                scroll_display: Some(scroll), // real scroll offset for scrollbar
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
            .title(format!(" Logs: {}/{}{} ", pod_name, container_name, since_title))
            .title_style(theme.title)
            .border_style(theme.border);
        let inner = block.inner(log_area);
        f.render_widget(block, log_area);
        if inner.height > 0 && inner.width > 0 {
            let message = if logs.streaming {
                let spinner_frames = [
                    "\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}",
                    "\u{283c}", "\u{2834}", "\u{2826}", "\u{2827}",
                    "\u{2807}", "\u{280f}",
                ];
                let spinner = spinner_frames[(app.tick_count / 2) % spinner_frames.len()];
                format!("{} Waiting for logs...", spinner)
            } else {
                "No logs.".to_string()
            };
            let text_len = message.len() as u16;
            let waiting = ratatui::text::Line::from(Span::styled(
                message,
                theme.status_pending,
            ));
            let center_y = inner.y + inner.height / 2;
            let center_x = inner.x + inner.width.saturating_sub(text_len) / 2;
            f.render_widget(
                waiting,
                ratatui::layout::Rect::new(center_x, center_y, inner.width, 1),
            );
        }
    }

    // Indicator bar: show toggle states (k9s-style)
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
    let ind_bg = " ".repeat(indicator_area.width as usize);
    let ind_bg_line = Line::styled(ind_bg, theme.status_bar);
    f.render_widget(ind_bg_line, indicator_area);
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

    let mut spans = Vec::new();
    spans.push(Span::styled(" ", theme.status_bar));
    for (i, (key, desc)) in hints.iter().enumerate() {
        spans.push(Span::styled(format!("<{}>", key), theme.status_bar_key));
        spans.push(Span::styled(format!(" {} ", desc), theme.status_bar));
        if i < hints.len() - 1 {
            spans.push(Span::styled("\u{2502}", theme.status_bar));
        }
    }

    // Fill background
    let bg = " ".repeat(bar_area.width as usize);
    let bg_line = Line::styled(bg, theme.status_bar);
    f.render_widget(bg_line, bar_area);

    let line = Line::from(spans);
    f.render_widget(line, bar_area);
}
