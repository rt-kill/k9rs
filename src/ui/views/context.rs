use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::Block,
    Frame,
};

use crate::app::App;
use crate::ui::header;
use crate::ui::theme::Theme;
use crate::util::truncate_to_width;


/// Draw the context switcher view.
///
/// Uses the same full k9s layout as the resource view: header, table area,
/// breadcrumb bar, and flash line.  Enter to switch, Esc/q to go back.
pub fn draw_contexts(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.theme;

    let header_height: u16 = if app.show_header { 7 } else { 0 };

    let chunks = Layout::vertical([
        Constraint::Length(header_height), // header
        Constraint::Fill(1),  // context table
        Constraint::Length(1), // breadcrumb / key-hints bar
        Constraint::Length(1), // flash (reserved, drawn by overlay)
    ])
    .split(area);

    let header_area = chunks[0];
    let table_area = chunks[1];
    let breadcrumb_area = chunks[2];
    let flash_area = chunks[3];

    // 1. Header (shared with resource view, context-specific key hints)
    if app.show_header {
        header::draw_header(f, app, header_area, theme, |f, area, theme| {
            draw_context_key_hints(f, area, theme);
        });
    }

    // 2. Context table
    draw_context_table(f, app, table_area, theme);

    // 3. Breadcrumb / key-hints bar
    draw_context_breadcrumbs(f, app, breadcrumb_area, theme);

    // 4. Flash area (reserved — the flash overlay in ui/mod.rs draws on top)
    if flash_area.width > 0 && flash_area.height > 0 {
        let empty = Line::raw("");
        f.render_widget(empty, flash_area);
    }
}

// ---------------------------------------------------------------------------
// Context-specific key hints (center panel of header)
// ---------------------------------------------------------------------------

fn draw_context_key_hints(f: &mut Frame, area: Rect, theme: &Theme) {
    let hints: Vec<(&str, &str)> = vec![
        ("j/k", "navigate"),
        ("Enter", "switch"),
        ("q/Esc", "back"),
        ("?", "help"),
    ];
    header::draw_key_hint_grid(f, area, &hints, theme);
}

// ---------------------------------------------------------------------------
// Context table (bordered, with columns: CURRENT, NAME, CLUSTER)
// ---------------------------------------------------------------------------

fn draw_context_table(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    let table = &app.data.contexts;
    let selected = table.selected;
    let total = table.len();

    let title = format!(" Contexts [{}/{}] ", selected.saturating_add(1).min(total), total);

    let block = Block::bordered()
        .title(title)
        .title_style(theme.title)
        .border_style(theme.border);

    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    // Column widths: CURRENT(3), NAME(flexible), CLUSTER(flexible)
    let name_width = (inner.width as usize).saturating_sub(3) / 2;
    let cluster_width = (inner.width as usize).saturating_sub(3).saturating_sub(name_width);

    // Header row
    let header_text = format!(
        " {} {:<nw$} {:<cw$}",
        " ",
        "NAME",
        "CLUSTER",
        nw = name_width,
        cw = cluster_width,
    );
    let header_style = theme.header;
    let header_display = truncate_to_width(&header_text, inner.width as usize);
    f.buffer_mut().set_string(inner.x, inner.y, header_display, header_style);

    let rows_area_y = inner.y + 1;
    let visible_height = (inner.height as usize).saturating_sub(1); // minus header row

    if visible_height == 0 {
        return;
    }

    let visible = table.visible_items();
    let offset = table.offset;

    for (vi, ctx) in visible.iter().enumerate() {
        if vi >= visible_height {
            break;
        }
        let y = rows_area_y + vi as u16;
        let is_selected = vi + offset == selected;

        let current_marker = if ctx.is_current { "\u{2713}" } else { " " };
        let cluster_display = if ctx.cluster.is_empty() {
            ctx.name.clone()
        } else {
            ctx.cluster.clone()
        };
        let line_text = format!(
            " {} {:<nw$} {:<cw$}",
            current_marker,
            ctx.name,
            cluster_display,
            nw = name_width,
            cw = cluster_width,
        );

        let style = if is_selected {
            theme.selected
        } else if ctx.is_current {
            theme.status_running
        } else {
            theme.row_normal
        };

        // Fill background for selected row
        if is_selected {
            for dx in 0..inner.width {
                f.buffer_mut()
                    .set_string(inner.x + dx, y, " ", theme.selected);
            }
        }

        // Truncate to available width using character-aware measurement
        let max_w = inner.width as usize;
        let truncated = truncate_to_width(&line_text, max_w);
        let display = if truncated.len() < line_text.len() {
            // Text was truncated — append ellipsis (need room for it)
            let truncated_for_ellipsis = truncate_to_width(&line_text, max_w.saturating_sub(1));
            format!("{}\u{2026}", truncated_for_ellipsis)
        } else {
            line_text
        };

        f.buffer_mut().set_string(inner.x, y, &display, style);
    }
}

// ---------------------------------------------------------------------------
// Breadcrumb bar with context-view key hints
// ---------------------------------------------------------------------------

fn draw_context_breadcrumbs(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let ctx_label = format!(" ctx: {} ", app.context);

    let hints = vec![
        ("j/k", "navigate"),
        ("Enter", "switch context"),
        ("q/Esc", "back"),
    ];

    let mut spans = Vec::new();
    spans.push(Span::styled(
        ctx_label,
        theme.status_bar.add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::styled(" \u{2502} ", theme.status_bar));
    for (i, (key, desc)) in hints.iter().enumerate() {
        spans.push(Span::styled(format!("<{}>", key), theme.status_bar_key));
        spans.push(Span::styled(format!(" {} ", desc), theme.status_bar));
        if i < hints.len() - 1 {
            spans.push(Span::styled("\u{2502}", theme.status_bar));
        }
    }

    // Fill background
    let bg = " ".repeat(area.width as usize);
    let bg_line = Line::styled(bg, theme.status_bar);
    f.render_widget(bg_line, area);

    let line = Line::from(spans);
    f.render_widget(line, area);
}
