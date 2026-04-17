use ratatui::{
    layout::{Alignment, Constraint, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::{Block, Borders, Padding, Paragraph},
    Frame,
};

use crate::app::{App, InputMode};
use crate::app::nav::rid;
use crate::kube::resource_def::BuiltInKind;
use crate::ui::header;
use crate::ui::theme::Theme;
use crate::ui::widgets::TabBar;

/// Draw the cluster overview landing page.
///
/// Uses the same layout as the resource view (header, content, tab bar, flash)
/// so it feels like a natural part of the app. No heavy resource subscriptions.
pub fn draw_overview(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.theme;

    let header_height: u16 = if app.show_header { 7 } else { 0 };
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
    let nodes_table = app.data.unified.get(&rid(BuiltInKind::Node));
    let node_count = nodes_table.map(|t| t.items.len()).unwrap_or(0);
    // Use the typed `row.health` field instead of substring-matching on
    // the STATUS cell — the converter already classified the node, and
    // coupling the overview to the cell column order would silently
    // break if the node row schema changed.
    let node_ready = nodes_table.map(|t| t.items.iter()
        .filter(|row| matches!(row.health, RowHealth::Normal))
        .count()).unwrap_or(0);
    let node_not_ready = node_count - node_ready;
    let ns_count = app.data.unified.get(&rid(BuiltInKind::Namespace))
        .map(|t| t.items.len()).unwrap_or(0);

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
    let ctx_display = if app.context.is_empty() { "connecting…" } else { app.context.as_str() };
    let cluster_display = if app.identity.cluster.is_empty() { "n/a" } else { &app.identity.cluster };
    let user_display = if app.identity.user.is_empty() { "n/a" } else { &app.identity.user };
    let ctx_line = format!("Context: {}  |  Cluster: {}  |  User: {}", ctx_display, cluster_display, user_display);
    lines.push(Line::from(
        Span::styled(ctx_line, theme.info_value)
    ).alignment(Alignment::Center));
    lines.push(Line::from(""));

    // Stats
    let node_status = if node_not_ready > 0 {
        format!("Nodes: {} ({} Ready, {} NotReady)", node_count, node_ready, node_not_ready)
    } else if node_count > 0 {
        format!("Nodes: {} (all Ready)", node_count)
    } else {
        "Nodes: loading...".to_string()
    };
    let ns_status = if ns_count > 0 {
        format!("Namespaces: {}", ns_count)
    } else {
        "Namespaces: loading...".to_string()
    };
    let stats = format!("{}  |  {}", node_status, ns_status);
    let stats_style = if node_not_ready > 0 { theme.status_pending } else { theme.status_running };
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
