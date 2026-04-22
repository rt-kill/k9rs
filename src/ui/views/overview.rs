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
use crate::ui::widgets::TabBar;

/// Draw the cluster overview landing page.
///
/// Uses the same layout as the resource view (header, content, tab bar, flash)
/// so it feels like a natural part of the app. No heavy resource subscriptions.
pub fn draw_overview(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.theme;

    let header_height: u16 = if app.show_header { crate::ui::HEADER_HEIGHT } else { 0 };
    let command_height: u16 = if matches!(app.input_mode, InputMode::Command { .. }) { 3 } else { 0 };

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

    // 1. Header (same as resource view)
    if app.show_header {
        header::draw_header(f, app, header_area, theme, |f, area, theme| {
            use crate::ui::header::KeyHint;
            let hints = vec![
                KeyHint { key: ":", description: "command" },
                KeyHint { key: "Tab", description: "resources" },
                KeyHint { key: "?", description: "help" },
                KeyHint { key: "q", description: "quit" },
            ];
            header::draw_key_hint_grid(f, area, &hints, theme);
        });
    }

    // 2. Command prompt (same as resource view)
    if matches!(app.input_mode, InputMode::Command { .. }) {
        super::resource::draw_command_prompt(f, app, command_area, theme);
    }

    // 3. Main content — cluster overview
    draw_content(f, app, content_area, theme);

    // 4. Tab bar (same as resource view — shows available resources)
    let tab_bar = TabBar::new(app.nav.resource_id(), theme)
        .namespace(app.selected_ns.display());
    f.render_widget(tab_bar, tab_bar_area);
}

fn draw_content(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    use crate::kube::resources::row::RowHealth;

    // Gather stats for all core resources dynamically from the registry.
    let mut core_stats: Vec<(&str, usize, usize)> = Vec::new();
    for def in crate::kube::resource_defs::REGISTRY.all() {
        if !def.is_core() { continue; }
        let rid = def.resource_id();
        let label = def.short_label();
        if let Some(table) = app.data.unified.get(&rid) {
            let total = table.items.len();
            let healthy = table.items.iter()
                .filter(|r| matches!(r.health, RowHealth::Normal))
                .count();
            core_stats.push((label, total, healthy));
        }
    }

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

    // Cluster info — centered block
    let ctx_display = if app.context.is_empty() { "connecting..." } else { app.context.as_str() };
    let cluster_display = if app.identity.cluster.is_empty() { "n/a" } else { &app.identity.cluster };
    let user_display = if app.identity.user.is_empty() { "n/a" } else { &app.identity.user };
    let ctx_line = format!("Context: {}  |  Cluster: {}  |  User: {}", ctx_display, cluster_display, user_display);
    lines.push(Line::from(
        Span::styled(ctx_line, theme.info_value)
    ).alignment(Alignment::Center));
    lines.push(Line::from(""));

    // Stats — built dynamically from core resources
    let mut has_unhealthy = false;
    let stats = if core_stats.is_empty() {
        "Loading...".to_string()
    } else {
        let parts: Vec<String> = core_stats.iter().map(|(label, total, healthy)| {
            let unhealthy = total - healthy;
            if unhealthy > 0 {
                has_unhealthy = true;
                format!("{}: {} ({} healthy, {} unhealthy)", label, total, healthy, unhealthy)
            } else if *total > 0 {
                format!("{}: {} (all healthy)", label, total)
            } else {
                format!("{}: loading...", label)
            }
        }).collect();
        parts.join("  |  ")
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
