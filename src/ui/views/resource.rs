
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::Block,
    Frame,
};

use unicode_width::UnicodeWidthStr;

use crate::app::{App, StatefulTable};
use crate::kube::protocol::{OperationKind, ResourceCapabilities};

use crate::ui::header;
use crate::ui::theme::Theme;
use crate::ui::widgets::{FilterBar, ResourceTable, ResourceTableState};

// ---------------------------------------------------------------------------
// Key hints (displayed in header center panel)
// ---------------------------------------------------------------------------

/// Build key hints based on the server-declared capabilities for the
/// current resource type. Driven entirely by `ResourceCapabilities` —
/// no static type knowledge needed on the client side.
fn key_hints_for_resource(caps: &ResourceCapabilities) -> Vec<crate::ui::header::KeyHint> {
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
    if caps.supports(OperationKind::StreamLogs) {
        hints.insert(pos, KeyHint { key: "l", description: "logs" });
        pos += 1;
    }
    if caps.supports(OperationKind::Shell) {
        hints.insert(pos, KeyHint { key: "s", description: "shell" });
        pos += 1;
    }
    if caps.supports(OperationKind::PortForward) {
        hints.insert(pos, KeyHint { key: "f", description: "pf" });
        pos += 1;
    }
    if caps.supports(OperationKind::PreviousLogs) {
        hints.insert(pos, KeyHint { key: "p", description: "prev-logs" });
        pos += 1;
    }
    if caps.supports(OperationKind::ShowNode) {
        hints.insert(pos, KeyHint { key: "o", description: "node" });
        pos += 1;
    }
    if caps.supports(OperationKind::Restart) {
        hints.insert(pos, KeyHint { key: "r", description: "restart" });
        pos += 1;
    }
    if caps.supports(OperationKind::Scale) {
        hints.insert(pos, KeyHint { key: "s", description: "scale" });
    }
    hints.push(KeyHint { key: "Space", description: "mark" });
    hints.push(KeyHint { key: "q", description: "quit" });
    hints
}

// ---------------------------------------------------------------------------
// Render a generic resource table given headers and row data.
// ---------------------------------------------------------------------------

/// Render a resource table. Returns the new `(offset, page_size)` so the
/// caller can write them back to the `StatefulTable` after the immutable
/// borrow is released — avoids the `&mut table` + `&table.marked`
/// borrow conflict that would otherwise force a per-frame allocation.
fn draw_resource_table(
    f: &mut Frame,
    area: Rect,
    title: &str,
    headers: Vec<&str>,
    rows: &[Vec<String>],
    selected: usize,
    initial_offset: usize,
    selected_col: usize,
    col_offset: u16,
    sort_ascending: bool,
    namespace: &str,
    theme: &Theme,
    marked: &std::collections::HashSet<crate::kube::protocol::ObjectKey>,
    display_sort_col: Option<usize>,
    changed_rows: &std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
    row_keys: &[crate::kube::protocol::ObjectKey],
    row_health: &[crate::kube::resources::row::RowHealth],
) -> (usize, usize, u16) {
    let visible_height = (area.height as usize).saturating_sub(3);

    let rt = ResourceTable::new(headers, rows, title, theme)
        .sort(display_sort_col, sort_ascending)
        .namespace(namespace)
        .marked(marked)
        .changed_rows(changed_rows)
        .row_keys(row_keys)
        .row_health(row_health);

    let mut state = ResourceTableState {
        selected,
        offset: initial_offset,
        selected_col,
        col_offset,
        filtered_count: 0,
    };

    f.render_stateful_widget(rt, area, &mut state);

    (state.offset, visible_height, state.col_offset)
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
    namespace: &crate::kube::protocol::Namespace,
    theme: &Theme,
    descriptor: Option<&crate::app::TableDescriptor>,
    column_level: crate::app::ColumnLevel,
    changed_rows: &std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
    rid: &crate::kube::protocol::ResourceId,
) {
    let display_title = if table.loading {
        format!("{} (loading...)", title)
    } else {
        title.to_string()
    };
    let title = &display_title;

    // Hide the NAMESPACE column when viewing a single namespace —
    // `visible_columns` walks columns by name, so the first-column gate the
    // old code added was redundant (and would have masked any future
    // descriptor that put NAMESPACE at a non-zero index).
    let skip_ns = !namespace.is_all();

    // Compute which columns (by data index) are visible at the current level.
    let visible: Vec<(usize, &str)> = if let Some(desc) = descriptor {
        desc.visible_columns(rid, column_level, skip_ns)
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
        let title_ns = if namespace.is_all() {
            format!(" {}[0]", title.to_lowercase())
        } else {
            format!(" {}({})[0]", title.to_lowercase(), namespace.display())
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

    // Build the parallel arrays the widget needs in one pass inside
    // StatefulTable — no manual index arithmetic in the render path.
    let view = table.prepare_view(&visible_indices);

    let selected = table.selected;
    let initial_offset = table.offset;
    let selected_col = table.selected_col;
    let col_offset = table.col_offset;
    let sort_ascending = table.sort_ascending;
    let display_sort_col = visible_indices.iter().position(|&i| i == table.sort_column);
    let marked: &std::collections::HashSet<crate::kube::protocol::ObjectKey> = &table.marked;

    let ns_label = if namespace.is_all() { "" } else { namespace.display() };
    let (new_offset, new_page_size, new_col_offset) = draw_resource_table(
        f, area, title, headers, &view.rows,
        selected, initial_offset, selected_col, col_offset,
        sort_ascending, ns_label, theme,
        marked, display_sort_col, changed_rows, &view.keys, &view.health,
    );

    table.offset = new_offset;
    table.page_size = new_page_size;
    table.col_offset = new_col_offset;
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
    let header_height: u16 = if app.show_header { crate::ui::HEADER_HEIGHT } else { 0 };
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
        let caps = app.current_capabilities();
        header::draw_header(f, app, header_area, theme, |f, area, theme| {
            draw_key_hints(f, &caps, area, theme);
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

    // 4. Resource table. Collect the borrows we need into locals *before*
    // taking the mutable reference to the table so the borrow checker stays
    // happy.
    let ns_display = app.selected_ns.display();
    let ns = app.selected_ns.clone();
    let cl = app.column_level;
    let current_rid = app.nav.resource_id().clone();
    let title = current_rid.short_label().to_string();
    let desc = app.data.descriptors.get(&current_rid).cloned();
    // Split-borrow `App` at the field level: we need `&mut app.data.unified`
    // and `app.deltas` simultaneously, and they're disjoint fields.
    let changed_rows = app.deltas.changed_rows();
    if let Some(table) = app.data.unified.get_mut(&current_rid) {
        draw_unified_table(f, table_area, &title, table, &ns, theme, desc.as_ref(), cl, changed_rows, &current_rid);
    } else {
        // Table doesn't exist yet (e.g., CRD not yet discovered). Show loading bar.
        let loading_text = crate::util::loading_bar("Loading...");
        let loading_line = Line::from(Span::styled(loading_text, theme.info_value));
        let title_ns = if !ns_display.is_empty() {
            format!(" {}({})[0]", title.to_lowercase(), ns_display)
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
fn draw_key_hints(f: &mut Frame, caps: &ResourceCapabilities, area: Rect, theme: &Theme) {
    let hints = key_hints_for_resource(caps);
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
    let ghost: String = app.best_completion()
        .and_then(|c| c.strip_prefix(input).map(str::to_string))
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

    if !ghost.is_empty() {
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

