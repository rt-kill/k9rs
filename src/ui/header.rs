use ratatui::{
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
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

/// Draw the standard header with a caller-supplied center-panel function.
///
/// `draw_center_fn` receives the center area and can render whatever key hints
/// are appropriate for the current view.
pub fn draw_header(
    f: &mut Frame,
    app: &App,
    area: Rect,
    theme: &Theme,
    draw_center_fn: impl FnOnce(&mut Frame, Rect, &Theme),
) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    // Split into 3 columns: 40% / 40% / 20%
    let cols = Layout::horizontal([
        Constraint::Percentage(40),
        Constraint::Percentage(40),
        Constraint::Percentage(20),
    ])
    .split(area);

    // Left: cluster info
    draw_cluster_info(f, app, cols[0], theme);
    // Center: caller-provided key hints
    draw_center_fn(f, cols[1], theme);
    // Right: logo
    draw_logo(f, cols[2], theme);
}

/// Left panel: Context, Cluster, User, K9rs Rev, CPU, MEM
pub fn draw_cluster_info(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    let lines: Vec<Line> = vec![
        Line::from(vec![
            Span::styled(" Context:   ", theme.info_label),
            if app.context.is_empty() {
                Span::styled("connecting…", theme.info_na)
            } else {
                Span::styled(&app.context, theme.info_value)
            },
        ]),
        Line::from(vec![
            Span::styled(" Cluster:   ", theme.info_label),
            if app.cluster.is_empty() {
                Span::styled("n/a", theme.info_na)
            } else {
                Span::styled(&app.cluster, theme.info_value)
            },
        ]),
        Line::from(vec![
            Span::styled(" User:      ", theme.info_label),
            if app.user.is_empty() {
                Span::styled("n/a", theme.info_na)
            } else {
                Span::styled(&app.user, theme.info_value)
            },
        ]),
        Line::from(vec![
            Span::styled(" K9rs Rev:  ", theme.info_label),
            Span::styled(env!("CARGO_PKG_VERSION"), theme.info_value),
        ]),
    ];

    for (i, line) in lines.iter().enumerate() {
        if i as u16 >= area.height {
            break;
        }
        f.render_widget(line.clone(), Rect::new(area.x, area.y + i as u16, area.width, 1));
    }
}

/// Right panel: k9rs ASCII art logo in orange
pub fn draw_logo(f: &mut Frame, area: Rect, theme: &Theme) {
    if area.height == 0 || area.width == 0 {
        return;
    }

    // Center logo vertically within the 7-line header
    let logo_height = LOGO.len() as u16;
    let start_y = area.y + (area.height.saturating_sub(logo_height)) / 2;

    for (i, line_text) in LOGO.iter().enumerate() {
        let y = start_y + i as u16;
        if y >= area.y + area.height {
            break;
        }
        // Truncate logo if wider than area
        let display: String = line_text.chars().take(area.width as usize).collect();
        let line = Line::styled(display, theme.logo);
        f.render_widget(line, Rect::new(area.x, y, area.width, 1));
    }
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
    let entries_per_col = (hints.len() + 1) / 2;

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
