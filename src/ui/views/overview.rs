use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Padding, Paragraph},
    Frame,
};

use crate::app::App;
use crate::ui::header;
use crate::ui::theme::Theme;

/// Draw the cluster overview landing page.
///
/// Shows cluster-level information from the always-on watchers (namespaces + nodes)
/// without subscribing to any heavy resource types (pods, deployments, etc.).
pub fn draw_overview(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.theme;

    let header_height: u16 = if app.show_header { 7 } else { 0 };
    let command_height: u16 = if app.command_mode { 3 } else { 0 };

    let chunks = Layout::vertical([
        Constraint::Length(header_height),
        Constraint::Length(command_height),
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(area);

    let header_area = chunks[0];
    let command_area = chunks[1];
    let content_area = chunks[2];
    let hints_area = chunks[3];
    let _flash_area = chunks[4];

    // Header
    if app.show_header {
        header::draw_header(f, app, header_area, theme, |f, area, theme| {
            draw_overview_key_hints(f, area, theme);
        });
    }

    // Command prompt
    if app.command_mode {
        crate::ui::views::resource::draw_command_prompt(f, app, command_area, theme);
    }

    // Main content
    draw_overview_content(f, app, content_area, theme);

    // Bottom hints
    let hints = Line::from(vec![
        Span::styled(" :", Style::default().fg(Color::Yellow)),
        Span::styled("command ", Style::default().fg(Color::DarkGray)),
        Span::styled(" ?", Style::default().fg(Color::Yellow)),
        Span::styled("help ", Style::default().fg(Color::DarkGray)),
        Span::styled(" q", Style::default().fg(Color::Yellow)),
        Span::styled("quit", Style::default().fg(Color::DarkGray)),
    ]);
    f.render_widget(hints, hints_area);
}

fn draw_overview_content(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    // Compute stats from always-on watchers
    let node_count = app.data.nodes.items.len();
    let node_ready = app.data.nodes.items.iter()
        .filter(|n| n.status.contains("Ready") && !n.status.contains("NotReady"))
        .count();
    let node_not_ready = node_count - node_ready;
    let ns_count = app.data.namespaces.items.len();

    let mut lines: Vec<Line> = Vec::new();

    // Title
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  k9rs", Style::default().fg(theme.logo.fg.unwrap_or(Color::Cyan)).add_modifier(Modifier::BOLD)),
        Span::styled(" — Kubernetes TUI", Style::default().fg(Color::DarkGray)),
    ]));
    lines.push(Line::from(""));

    // Context info
    lines.push(Line::from(vec![
        Span::styled("  Context:   ", Style::default().fg(Color::DarkGray)),
        Span::styled(&app.context, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Cluster:   ", Style::default().fg(Color::DarkGray)),
        Span::styled(&app.cluster, Style::default().fg(Color::White)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  User:      ", Style::default().fg(Color::DarkGray)),
        Span::styled(&app.user, Style::default().fg(Color::White)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Namespace: ", Style::default().fg(Color::DarkGray)),
        Span::styled(&app.selected_ns, Style::default().fg(Color::White)),
    ]));

    lines.push(Line::from(""));

    // Cluster stats
    let node_status = if node_not_ready > 0 {
        format!("{} ({} Ready, {} NotReady)", node_count, node_ready, node_not_ready)
    } else if node_count > 0 {
        format!("{} (all Ready)", node_count)
    } else {
        "loading...".to_string()
    };

    lines.push(Line::from(vec![
        Span::styled("  Nodes:      ", Style::default().fg(Color::DarkGray)),
        Span::styled(node_status, Style::default().fg(if node_not_ready > 0 { Color::Yellow } else { Color::Green })),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Namespaces: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            if ns_count > 0 { ns_count.to_string() } else { "loading...".to_string() },
            Style::default().fg(Color::Green),
        ),
    ]));

    lines.push(Line::from(""));
    lines.push(Line::from(""));

    // Quick navigation
    lines.push(Line::from(vec![
        Span::styled("  Quick Navigation", Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
    ]));
    lines.push(Line::from(""));

    let nav_rows: &[&[(&str, &str)]] = &[
        &[(":pods", "po"), (":deploy", "dp"), (":svc", "svc"), (":nodes", "no")],
        &[(":ns", "ns"), (":sts", "sts"), (":ds", "ds"), (":jobs", "job")],
        &[(":secrets", "sec"), (":cm", "cm"), (":ing", "ing"), (":crd", "crd")],
        &[(":ctx", "contexts"), (":pv", "pv"), (":pvc", "pvc"), (":sa", "sa")],
    ];

    for row in nav_rows {
        let mut spans = vec![Span::raw("  ")];
        for (cmd, label) in *row {
            spans.push(Span::styled(format!("{:<14}", cmd), Style::default().fg(Color::Cyan)));
            let _ = label; // label reserved for future tooltip
        }
        lines.push(Line::from(spans));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  Press ", Style::default().fg(Color::DarkGray)),
        Span::styled(":", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::styled(" to enter a command, or type a resource name directly", Style::default().fg(Color::DarkGray)),
    ]));

    let block = Block::default()
        .borders(Borders::NONE)
        .padding(Padding::new(1, 1, 0, 0));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_overview_key_hints(f: &mut Frame, area: Rect, theme: &Theme) {
    let hints = vec![
        (":", "command"),
        ("?", "help"),
        ("q", "quit"),
    ];
    header::draw_key_hint_grid(f, area, &hints, theme);
}
