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
/// Uses the same layout as the resource view: header, table area,
/// breadcrumb bar, and flash line.  Enter to switch, Esc/q to go back.
pub fn draw_contexts(f: &mut Frame, app: &App, area: Rect) {
    let theme = &app.ui.theme;

    let header_height: u16 = if app.ui.show_header { crate::ui::HEADER_HEIGHT } else { 0 };

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

    // 1. Header (shared with resource view; key hints live in ? help)
    if app.ui.show_header {
        header::draw_header(f, app, header_area, theme);
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
// Context table (bordered, with columns: CURRENT, NAME, CLUSTER)
// ---------------------------------------------------------------------------

fn draw_context_table(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    // The top element IS the picker: its own table copy, seeded at push.
    let crate::app::element::Element::ContextList(picker) = app.nav.top() else { return };
    let table = &picker.table;
    let selected = table.selected();
    let total = table.len();

    let title = format!(" Contexts [{}] ", total);

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
    let header_text = format_context_row(" ", "NAME", "CLUSTER", name_width, cluster_width);
    let header_style = theme.header;
    let header_display = truncate_to_width(&header_text, inner.width as usize);
    f.buffer_mut().set_string(inner.x, inner.y, header_display, header_style);

    let rows_area_y = inner.y + 1;
    let visible_height = (inner.height as usize).saturating_sub(1); // minus header row

    if visible_height == 0 {
        return;
    }

    let visible = table.visible_items();
    let offset = table.offset();

    for (vi, ctx) in visible.iter().enumerate() {
        if vi >= visible_height {
            break;
        }
        let y = rows_area_y + vi as u16;
        let is_selected = vi + offset == selected;

        // Derive "current" from the LIVE active context, not the stored
        // `is_current` flag — a context switch updates `kube.context` but
        // the flags only re-sync on a later KubeconfigLoaded, so the `✓`
        // would otherwise lag the switch.
        let is_current = !app.kube.context.is_empty() && ctx.name == app.kube.context;
        let current_marker = if is_current { "\u{2713}" } else { " " };
        let cluster_display: &str = if ctx.identity.cluster.is_empty() {
            ctx.name.as_str()
        } else {
            &ctx.identity.cluster
        };
        let line_text = format_context_row(
            current_marker, ctx.name.as_str(), cluster_display, name_width, cluster_width,
        );

        let style = if is_selected {
            theme.selected
        } else if is_current {
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

/// Format one context row into the fixed three-column layout (current-marker,
/// NAME, CLUSTER). Shared by the header and the data rows so their columns
/// stay aligned by construction — the two used to carry duplicate format
/// strings that could drift apart.
fn format_context_row(
    marker: &str,
    name: &str,
    cluster: &str,
    name_width: usize,
    cluster_width: usize,
) -> String {
    format!(" {} {:<nw$} {:<cw$}", marker, name, cluster, nw = name_width, cw = cluster_width)
}

// ---------------------------------------------------------------------------
// Breadcrumb bar with context-view key hints
// ---------------------------------------------------------------------------

fn draw_context_breadcrumbs(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    let ctx_label = format!(" ctx: {} ", app.kube.context);

    let hints = vec![
        ("j/k", "navigate"),
        ("Enter", "switch context"),
        ("c", "copy"),
        ("q/Esc", "back"),
    ];

    // Fill background
    crate::ui::fill_line_bg(f, area, theme.status_bar);

    // Build the bar: prefix with context label, then shared keybinding hints
    let keybinding_line = header::render_keybinding_bar(&hints, theme);
    let mut spans = vec![
        Span::styled(
            ctx_label,
            theme.status_bar.add_modifier(Modifier::BOLD),
        ),
        Span::styled(" \u{2502}", theme.status_bar),
    ];
    spans.extend(keybinding_line.spans);

    let line = Line::from(spans);
    f.render_widget(line, area);
}
