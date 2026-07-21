use ratatui::{
    layout::{Alignment, Constraint, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Borders, Padding, Paragraph},
    Frame,
};

use crate::app::{App, InputMode};
use crate::ui::header;
use crate::ui::theme::Theme;

/// Draw the cluster overview landing page.
///
/// Uses the same layout as the resource view (header, content, tab bar, flash)
/// so it feels like a natural part of the app. No heavy resource subscriptions.
pub fn draw_overview(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.ui.theme;

    let header_height: u16 = if app.ui.show_header { crate::ui::HEADER_HEIGHT } else { 0 };
    let command_height: u16 = if matches!(app.ui.input_mode, InputMode::Command { .. }) { 3 } else { 0 };

    let chunks = Layout::vertical([
        Constraint::Length(header_height),      // header
        Constraint::Length(command_height),     // command prompt
        Constraint::Fill(1),                   // content
        Constraint::Length(1),                 // tab bar
        Constraint::Length(1),                 // flash
    ])
    .split(area);

    let header_area = chunks[0];
    let command_area = chunks[1];
    let content_area = chunks[2];
    let tab_bar_area = chunks[3];
    let _flash_area = chunks[4];

    // 1. Header (same as resource view; key hints live in ? help)
    if app.ui.show_header {
        header::draw_header(f, app, header_area, theme);
    }

    // 2. Command prompt (same as resource view)
    if matches!(app.ui.input_mode, InputMode::Command { .. }) {
        super::resource::draw_command_prompt(f, app, command_area, theme);
    }

    // 3. Main content — cluster overview
    draw_content(f, app, content_area, theme);

    // 4. Tab bar — show "overview" label, not the nav stack's resource
    // (which defaults to pods but has no active subscription here).
    let status_line = Line::from(vec![
        Span::styled(" overview ", theme.breadcrumb_active),
        Span::styled(" ", theme.status_bar),
        Span::styled(format!(" {} ", app.kube.selected_ns.display()), theme.breadcrumb_inactive),
    ]);
    f.render_widget(status_line, tab_bar_area);
}

fn draw_content(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    // Stats computed by App — view just renders them.
    let core_stats = app.core_resource_stats();

    // Big centered title
    let mut lines: Vec<Line> = vec![
        Line::from(""),
        Line::from(""),
        Line::from(
            Span::styled("k9rs", theme.title.add_modifier(Modifier::BOLD))
        ).alignment(Alignment::Center),
        Line::from(
            Span::styled("Kubernetes TUI", theme.info_label)
        ).alignment(Alignment::Center),
        Line::from(""),
        Line::from(""),
    ];

    // Cluster info — centered, one field per line
    let info_fields: &[(&str, &str)] = &[
        ("Context: ", if app.kube.context.is_empty() { "connecting..." } else { app.kube.context.as_str() }),
        ("Cluster: ", if app.kube.identity.cluster.is_empty() { "n/a" } else { &app.kube.identity.cluster }),
        ("User: ", if app.kube.identity.user.is_empty() { "n/a" } else { &app.kube.identity.user }),
        ("K8s: ", if app.kube.identity.k8s_version.is_empty() { "n/a" } else { &app.kube.identity.k8s_version }),
    ];
    for (label, value) in info_fields {
        lines.push(Line::from(vec![
            Span::styled(*label, theme.info_label),
            Span::styled(*value, theme.info_value),
        ]).alignment(Alignment::Center));
    }
    lines.push(Line::from(""));

    // Stats — built dynamically from core resources. `has_unhealthy` is a
    // pure predicate over the same data, computed independently of the
    // per-label formatting rather than flagged as a side effect inside it.
    let has_unhealthy = core_stats.iter().any(|(_, total, healthy)| total - healthy > 0);
    let stats = if core_stats.is_empty() {
        "Loading...".to_string()
    } else {
        core_stats.iter().map(|(label, total, healthy)| {
            let unhealthy = total - healthy;
            if unhealthy > 0 {
                format!("{}: {} ({} healthy, {} unhealthy)", label, total, healthy, unhealthy)
            } else if *total > 0 {
                format!("{}: {} (all healthy)", label, total)
            } else {
                format!("{}: loading...", label)
            }
        }).collect::<Vec<_>>().join("  |  ")
    };
    let stats_style = if has_unhealthy { theme.status_pending } else { theme.status_running };
    lines.push(Line::from(
        Span::styled(stats, stats_style)
    ).alignment(Alignment::Center));

    lines.push(Line::from(""));
    lines.push(Line::from(""));

    // Hint
    lines.push(Line::from(vec![
        Span::styled("Press ", theme.info_label),
        Span::styled(":", theme.title.add_modifier(Modifier::BOLD)),
        Span::styled(" to enter a command  |  ", theme.info_label),
        Span::styled("Tab", theme.title.add_modifier(Modifier::BOLD)),
        Span::styled(" to browse resources  |  ", theme.info_label),
        Span::styled("?", theme.title.add_modifier(Modifier::BOLD)),
        Span::styled(" help", theme.info_label),
    ]).alignment(Alignment::Center));

    let block = Block::default()
        .borders(Borders::NONE)
        .padding(Padding::new(0, 0, 0, 0));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}
