use std::collections::HashSet;

use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::Block,
    Frame,
};

use unicode_width::UnicodeWidthStr;

use crate::app::{App, InputMode, StatefulTable};
use crate::kube::protocol::ResourceId;

use crate::ui::header;
use crate::ui::theme::Theme;
use crate::ui::widgets::{FilterBar, ResourceTable, ResourceTableState};

// ---------------------------------------------------------------------------
// Key hints (displayed in header center panel)
// ---------------------------------------------------------------------------

fn key_hints_for_resource(rid: &ResourceId) -> Vec<crate::ui::header::KeyHint> {
    use crate::ui::header::KeyHint;
    let mut hints = vec![
        KeyHint { key: ":", description: "cmd" },
        KeyHint { key: "/", description: "filter" },
        KeyHint { key: "d", description: "desc" },
        KeyHint { key: "y", description: "yaml" },
        KeyHint { key: "Ctrl-d", description: "del" },
        KeyHint { key: "?", description: "help" },
    ];
    let mut pos = 4;
    if rid.supports_logs() {
        hints.insert(pos, KeyHint { key: "l", description: "logs" });
        pos += 1;
    }
    if rid.supports_shell() {
        hints.insert(pos, KeyHint { key: "s", description: "shell" });
        pos += 1;
        hints.insert(pos, KeyHint { key: "f", description: "pf" });
        pos += 1;
        hints.insert(pos, KeyHint { key: "p", description: "prev-logs" });
        pos += 1;
        hints.insert(pos, KeyHint { key: "o", description: "node" });
        pos += 1;
    }
    if rid.supports_restart() {
        hints.insert(pos, KeyHint { key: "r", description: "restart" });
        pos += 1;
    }
    if rid.supports_scale() {
        hints.insert(pos, KeyHint { key: "s", description: "scale" });
        let _ = pos;
    }
    hints.push(KeyHint { key: "Space", description: "mark" });
    hints.push(KeyHint { key: "q", description: "quit" });
    hints
}

// ---------------------------------------------------------------------------
// Render a generic resource table given headers and row data.
// ---------------------------------------------------------------------------

fn draw_resource_table<T: Clone>(
    f: &mut Frame,
    area: Rect,
    title: &str,
    headers: Vec<&str>,
    rows: &[Vec<String>],
    table: &mut StatefulTable<T>,
    namespace: &str,
    filter_text: &str,
    theme: &Theme,
    marked_filtered: &HashSet<usize>,
    display_sort_col: Option<usize>,
    changed_rows: &std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
) {
    // The widget uses Block::bordered(), so inner height = area.height - 2 (borders).
    // Minus 1 more for the header row.
    let visible_height = (area.height as usize).saturating_sub(3);

    let rt = ResourceTable::new(headers, rows, title, theme)
        .sort(display_sort_col, table.sort_ascending)
        .namespace(namespace)
        .filter_text(filter_text)
        .marked(marked_filtered)
        .changed_rows(changed_rows);

    let mut state = ResourceTableState {
        selected: table.selected,
        offset: table.offset,
        filtered_count: 0,
    };

    f.render_stateful_widget(rt, area, &mut state);

    // Write back corrected offset and actual page_size to the StatefulTable.
    table.offset = state.offset;
    table.page_size = visible_height;
}


/// Draw function for unified ResourceRow tables. Uses runtime column headers
/// from the TableDescriptor instead of a static `T::headers()`.
///
/// Column visibility is controlled by `column_level`: only columns whose
/// `ColumnLevel` is <= `column_level` are displayed. The NAMESPACE column
/// is also hidden when viewing a single namespace. Rows always contain all
/// cells; this function extracts only the visible subset for display.
fn draw_unified_table(
    f: &mut Frame,
    area: Rect,
    title: &str,
    table: &mut StatefulTable<crate::kube::resources::row::ResourceRow>,
    namespace: &str,
    filter_text: &str,
    theme: &Theme,
    descriptor: Option<&crate::app::TableDescriptor>,
    column_level: crate::app::ColumnLevel,
    changed_rows: &std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
) {
    let display_title = if table.loading {
        format!("{} (loading...)", title)
    } else {
        title.to_string()
    };
    let title = &display_title;

    let skip_ns = namespace != "all"
        && !namespace.is_empty()
        && descriptor.map_or(false, |d| d.headers.first().map_or(false, |h| h.eq_ignore_ascii_case("NAMESPACE")));

    // Compute which columns (by data index) are visible at the current level.
    let visible: Vec<(usize, &str)> = if let Some(desc) = descriptor {
        desc.visible_columns(column_level, skip_ns)
    } else {
        vec![(0, "NAME")]
    };
    let visible_indices: Vec<usize> = visible.iter().map(|&(i, _)| i).collect();
    let headers: Vec<&str> = visible.iter().map(|&(_, name)| name).collect();

    if table.items.is_empty() {
        let loading_text = if let Some(ref err) = table.error {
            format!("Error: {}", err)
        } else if table.has_data {
            format!("No {} found.", title.to_lowercase())
        } else {
            crate::util::loading_bar("Loading...")
        };
        let loading_line = Line::from(Span::styled(loading_text, theme.info_value));
        let title_ns = if !namespace.is_empty() {
            format!(" {}({})[0]", title.to_lowercase(), namespace)
        } else {
            format!(" {}[0]", title.to_lowercase())
        };
        let block = Block::bordered()
            .title(Span::styled(title_ns, theme.title))
            .border_style(theme.border);
        let inner = block.inner(area);
        f.render_widget(block, area);
        if inner.height > 0 && inner.width > 0 {
            let center_y = inner.y + inner.height / 2;
            let center_x = inner.x
                + inner.width.saturating_sub(loading_line.width() as u16) / 2;
            f.render_widget(
                loading_line,
                Rect::new(center_x, center_y, inner.width, 1),
            );
        }
        return;
    }

    // Extract only the visible cells from each row.
    let rows: Vec<Vec<String>> = table
        .filtered_indices
        .iter()
        .filter_map(|&i| table.items.get(i))
        .map(|r| {
            visible_indices.iter()
                .map(|&ci| r.cells.get(ci).cloned().unwrap_or_default())
                .collect()
        })
        .collect();

    let marked_filtered: HashSet<usize> = table.filtered_indices
        .iter()
        .enumerate()
        .filter(|(_pos, &real_idx)| table.marked.contains(&real_idx))
        .map(|(pos, _)| pos)
        .collect();

    // Map the data sort column to a display column index.
    let display_sort_col = visible_indices.iter().position(|&i| i == table.sort_column);

    draw_resource_table(f, area, title, headers, &rows, table, namespace, filter_text, theme, &marked_filtered, display_sort_col, changed_rows);
}

// ---------------------------------------------------------------------------
// Main entry point: draw_resources
// ---------------------------------------------------------------------------

/// Draw the main resource browser view:
///
/// ```text
/// +--header (7 lines)------------------------------------------+
/// | cluster info  |  key hints  |  k9rs logo                   |
/// +--command prompt (3 lines, optional)-------------------------+
/// | :pods                                                       |
/// +--filter (3 lines, optional)---------------------------------+
/// | /filter_text                                                |
/// +--table (fills remaining)------------------------------------+
/// | pods(default)[42]  </:filter>                               |
/// | NAMESPACE  NAME  ...                                        |
/// +--breadcrumbs (1 line)--------------------------------------+
/// +--flash (1 line)--------------------------------------------+
/// ```
pub fn draw_resources(f: &mut Frame, app: &mut App, area: Rect) {
    let theme = &app.theme;

    // Determine dynamic section heights
    let header_height: u16 = if app.show_header { 7 } else { 0 };
    let command_height: u16 = if app.input_mode.is_active() { 3 } else { 0 };
    // Only show the filter bar box while actively typing; when committed
    // (inactive but text non-empty), the table title shows `</:filter_text>`.
    let filter_visible = app.nav.filter_input().active;
    let filter_height: u16 = if filter_visible { 3 } else { 0 };

    let chunks = Layout::vertical([
        Constraint::Length(header_height),      // header
        Constraint::Length(command_height),     // command prompt
        Constraint::Length(filter_height),      // filter
        Constraint::Fill(1),                   // table
        Constraint::Length(1),                 // breadcrumbs
        Constraint::Length(1),                 // flash
    ])
    .split(area);

    let header_area = chunks[0];
    let command_area = chunks[1];
    let filter_area = chunks[2];
    let table_area = chunks[3];
    let breadcrumb_area = chunks[4];
    let flash_area = chunks[5];

    // 1. Header section: 3 columns (only when visible)
    if app.show_header {
        let current_rid = app.nav.resource_id().clone();
        header::draw_header(f, app, header_area, theme, |f, area, theme| {
            draw_key_hints(f, &current_rid, area, theme);
        });
    }

    // 2. Command prompt (only when command mode active)
    if app.input_mode.is_active() {
        draw_command_prompt(f, app, command_area, theme);
    }

    // 3. Filter prompt
    if filter_visible {
        let counts = app.active_table_items_count();
        let match_count = if app.nav.filter_input().text.is_empty() && !app.nav.is_drilled() { counts.total } else { counts.filtered };
        let filter_bar = FilterBar::new(
            app.nav.filter_input().active,
            &app.nav.filter_input().text,
            match_count,
            counts.total,
            theme,
        );
        f.render_widget(filter_bar, filter_area);
    }

    // 4. Resource table
    let ns = app.selected_ns.display();
    let ft = "";
    let cr = &app.changed_rows;
    let cl = app.column_level;
    let current_rid = app.nav.resource_id().clone();
    let title = current_rid.short_label().to_string();
    if let Some(table) = app.data.unified.get_mut(&current_rid) {
        let desc = app.data.descriptors.get(&current_rid);
        draw_unified_table(f, table_area, &title, table, ns, ft, theme, desc, cl, cr);
    } else {
        // Table doesn't exist yet (e.g., CRD not yet discovered). Show loading bar.
        let loading_text = crate::util::loading_bar("Loading...");
        let loading_line = Line::from(Span::styled(loading_text, theme.info_value));
        let title_ns = if !ns.is_empty() {
            format!(" {}({})[0]", title.to_lowercase(), ns)
        } else {
            format!(" {}[0]", title.to_lowercase())
        };
        let block = ratatui::widgets::Block::bordered()
            .title(Span::styled(title_ns, theme.title))
            .border_style(theme.border);
        let inner = block.inner(table_area);
        f.render_widget(block, table_area);
        if inner.height > 0 && inner.width > 0 {
            let center_y = inner.y + inner.height / 2;
            let center_x = inner.x + inner.width.saturating_sub(loading_line.width() as u16) / 2;
            f.render_widget(loading_line, ratatui::layout::Rect::new(center_x, center_y, inner.width, 1));
        }
    }

    // 5. Breadcrumb bar — always shows the navigation path
    let bc = app.nav.breadcrumb();
    let mut spans = Vec::new();
    spans.push(Span::styled(format!(" {} ", bc), theme.title));
    if app.nav.is_drilled() {
        spans.push(Span::styled(" | ", theme.info_label));
        spans.push(Span::styled("Esc", theme.title_filter_indicator));
        spans.push(Span::styled(" back", theme.info_label));
    }
    let line = Line::from(spans);
    f.render_widget(line, breadcrumb_area);

    // 6. Flash message area (handled by ui/mod.rs overlay, but we reserve the line)
    // Draw a subtle flash area background
    if flash_area.width > 0 && flash_area.height > 0 {
        let empty = Line::raw("");
        f.render_widget(empty, flash_area);
    }
}

// ---------------------------------------------------------------------------
// Header: key hints center panel (delegates to shared header module)
// ---------------------------------------------------------------------------

/// Center panel: compact key hint grid for the resource view.
fn draw_key_hints(f: &mut Frame, rid: &ResourceId, area: Rect, theme: &Theme) {
    let hints = key_hints_for_resource(rid);
    header::draw_key_hint_grid(f, area, &hints, theme);
}

// ---------------------------------------------------------------------------
// Command prompt
// ---------------------------------------------------------------------------

pub fn draw_command_prompt(f: &mut Frame, app: &App, area: Rect, theme: &Theme) {
    if area.height < 3 || area.width == 0 {
        return;
    }

    let block = Block::bordered()
        .border_style(theme.border_focused);
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    let input = app.input_mode.input().unwrap_or("");
    let is_scale = matches!(app.input_mode, InputMode::Scale { .. });
    let ghost: String = app.best_completion()
        .and_then(|c| {
            if c.starts_with(input) {
                Some(c[input.len()..].to_string())
            } else {
                None
            }
        })
        .unwrap_or_default();

    // Fish-style rendering: typed text (bright) followed immediately by ghost
    // text (dim/italic) with no block cursor in between. The terminal cursor
    // is placed right after the typed text via set_cursor_position.
    let prefix = app.input_mode.prompt();
    let prefix_len: u16 = prefix.width() as u16;
    let typed_len = input.width() as u16;

    let mut spans = vec![
        Span::styled(prefix, theme.command.add_modifier(Modifier::BOLD)),
        Span::styled(input, theme.command),
    ];

    if !ghost.is_empty() && !is_scale {
        spans.push(Span::styled(
            ghost,
            theme.command_suggestion.add_modifier(Modifier::DIM | Modifier::ITALIC),
        ));
    }

    let line = Line::from(spans);
    f.render_widget(line, inner);

    // Place the real terminal cursor right after the typed text (thin line /
    // blinking bar depending on the user's terminal emulator settings).
    let cursor_x = inner.x + prefix_len + typed_len;
    let cursor_y = inner.y;
    if cursor_x < inner.x + inner.width {
        f.set_cursor_position((cursor_x, cursor_y));
    }
}

