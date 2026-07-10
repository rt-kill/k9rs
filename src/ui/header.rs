use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::app::App;
use crate::ui::theme::Theme;

/// A keyboard shortcut hint for the UI.
#[derive(Debug, Clone)]
pub struct KeyHint {
    pub key: &'static str,
    pub description: &'static str,
}

// ---------------------------------------------------------------------------
// k9rs ASCII art logo (rendered in orange)
// ---------------------------------------------------------------------------

pub const LOGO: &[&str] = &[
    r" _     ___            ",
    r"| | __/ _ \ _ __ ___  ",
    r"| |/ / (_) | '__/ __| ",
    r"|   < \__, | |  \__ \ ",
    r"|_|\_\  /_/|_|  |___/ ",
];

// ---------------------------------------------------------------------------
// Header: cluster info (left), key hints (center), logo (right)
// ---------------------------------------------------------------------------

/// Compact header: context/cluster/user stacked vertically on the left,
/// k9rs logo on the right. No key hints — those live in the ? help dialog.
pub fn draw_header(
    f: &mut Frame,
    app: &App,
    area: Rect,
    theme: &Theme,
    _draw_center_fn: impl FnOnce(&mut Frame, Rect, &Theme),
) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let ctx = if app.kube.context.is_empty() { "connecting..." } else { app.kube.context.as_str() };
    let cluster = if app.kube.identity.cluster.is_empty() { "n/a" } else { &app.kube.identity.cluster };
    let user = if app.kube.identity.user.is_empty() { "n/a" } else { &app.kube.identity.user };
    let k8s_ver = if app.kube.identity.k8s_version.is_empty() { "n/a" } else { &app.kube.identity.k8s_version };

    let logo_width = LOGO.iter().map(|l| l.len()).max().unwrap_or(0) as u16 + 2;
    let cols = Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(logo_width),
    ]).split(area);

    // Left: context / cluster / user / k8s version stacked vertically.
    let info = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(" Context: ", theme.info_label),
            Span::styled(ctx, theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" Cluster: ", theme.info_label),
            Span::styled(cluster, theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" User:    ", theme.info_label),
            Span::styled(user, theme.info_value),
        ]),
        Line::from(vec![
            Span::styled(" K8s:     ", theme.info_label),
            Span::styled(k8s_ver, theme.info_value),
        ]),
    ]);
    f.render_widget(info, cols[0]);

    // Right: k9rs logo.
    let logo_lines: Vec<Line> = LOGO.iter()
        .map(|l| Line::from(Span::styled(*l, theme.logo)))
        .collect();
    let logo = Paragraph::new(logo_lines)
        .alignment(ratatui::layout::Alignment::Right);
    f.render_widget(logo, cols[1]);
}

/// Draw a key-hint grid in a two-column layout. Reusable by any view that
/// wants compact `<key> desc` pairs in its center header panel.
pub fn draw_key_hint_grid(
    f: &mut Frame,
    area: Rect,
    hints: &[KeyHint],
    theme: &Theme,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let col_width = area.width / 2;
    let entries_per_col = hints.len().div_ceil(2);

    for row in 0..entries_per_col {
        if row as u16 >= area.height {
            break;
        }
        let y = area.y + row as u16;

        // Left column
        if row < hints.len() {
            let hint = &hints[row];
            let line = Line::from(vec![
                Span::styled(format!(" <{}>", hint.key), theme.help_key),
                Span::styled(format!(" {}", hint.description), theme.info_value),
            ]);
            f.render_widget(line, Rect::new(area.x, y, col_width, 1));
        }

        // Right column
        let right_idx = row + entries_per_col;
        if right_idx < hints.len() {
            let hint = &hints[right_idx];
            let line = Line::from(vec![
                Span::styled(format!(" <{}>", hint.key), theme.help_key),
                Span::styled(format!(" {}", hint.description), theme.info_value),
            ]);
            f.render_widget(line, Rect::new(area.x + col_width, y, col_width, 1));
        }
    }
}

/// Build a keybinding bar `Line` from a list of `(key, description)` pairs.
/// Used by describe, yaml, log, and context views for their bottom status bars.
pub fn render_keybinding_bar(hints: &[(&str, &str)], theme: &Theme) -> Line<'static> {
    let mut spans = Vec::new();
    spans.push(Span::styled(" ", theme.status_bar));
    for (i, (key, desc)) in hints.iter().enumerate() {
        spans.push(Span::styled(format!("<{}>", key), theme.status_bar_key));
        spans.push(Span::styled(format!(" {} ", desc), theme.status_bar));
        if i < hints.len() - 1 {
            spans.push(Span::styled("\u{2502}", theme.status_bar));
        }
    }
    Line::from(spans)
}
