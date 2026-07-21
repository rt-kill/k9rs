
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::Modifier,
    text::{Line, Span},
    widgets::Block,
    Frame,
};

use unicode_width::UnicodeWidthStr;

use crate::app::App;

use crate::ui::header;
use crate::ui::theme::Theme;
use crate::ui::widgets::{FilterBar, ResourceTable, ResourceTableState};

// ---------------------------------------------------------------------------
// Key hints (displayed in header center panel)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Render a generic resource table given headers and row data.
// ---------------------------------------------------------------------------

/// Snapshot of table state extracted before the mutable borrow for rendering.
/// Groups the 15+ parameters that `draw_unified_table` passes to the widget,
/// avoiding the 17-arg function that was here before.
struct TableSnapshot<'a> {
    headers: Vec<&'a str>,
    rows: &'a [Vec<String>],
    selected: usize,
    offset: usize,
    selected_col: usize,
    col_offset: u16,
    sort_ascending: bool,
    display_sort_col: Option<usize>,
    namespace: &'a str,
    marked: &'a std::collections::HashSet<crate::kube::protocol::ObjectKey>,
    changed_rows: &'a std::collections::HashMap<crate::kube::protocol::ObjectKey, std::time::Instant>,
    row_keys: &'a [crate::kube::protocol::ObjectKey],
    row_health: &'a [crate::kube::resources::row::RowHealth],
    cell_style: &'a [Vec<Option<crate::kube::resources::row::RowHealth>>],
    /// Pre-computed per-column display widths, carried from the memoized
    /// [`PreparedView`] so the widget doesn't re-scan rows each frame.
    col_widths: &'a [u16],
    search_patterns: &'a [crate::util::SearchPattern],
}

/// Render a resource table from a pre-built snapshot. Returns the new
/// `(offset, page_size, col_offset)` for the caller to write back.
fn draw_resource_table(
    f: &mut Frame,
    area: Rect,
    title: &str,
    snap: &TableSnapshot<'_>,
    theme: &Theme,
) -> (usize, usize, u16) {
    let visible_height = (area.height as usize).saturating_sub(3);

    let rt = ResourceTable::new(snap.headers.clone(), snap.rows, title, theme)
        .sort(snap.display_sort_col, snap.sort_ascending)
        .namespace(snap.namespace)
        .marked(snap.marked)
        .changed_rows(snap.changed_rows)
        .row_keys(snap.row_keys)
        .row_health(snap.row_health)
        .cell_style(snap.cell_style)
        .col_widths(snap.col_widths)
        .search_patterns(snap.search_patterns);

    let mut state = ResourceTableState {
        selected: snap.selected,
        offset: snap.offset,
        selected_col: snap.selected_col,
        col_offset: snap.col_offset,
        filtered_count: 0,
    };

    f.render_stateful_widget(rt, area, &mut state);

    (state.offset, visible_height, state.col_offset)
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
    let theme = &app.ui.theme;

    // Determine dynamic section heights
    let header_height: u16 = if app.ui.show_header { crate::ui::HEADER_HEIGHT } else { 0 };
    let command_height: u16 = if app.ui.input_mode.is_active() { 3 } else { 0 };
    // Only show the filter bar box while actively typing; when committed
    // (inactive but text non-empty), the table title shows `</:filter_text>`.
    let filter_visible = app.nav.top().filter_input().active();
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

    // 1. Header section (cluster info + logo; key hints live in ? help)
    if app.ui.show_header {
        header::draw_header(f, app, header_area, theme);
    }

    // 2. Command prompt (only when command mode active)
    if app.ui.input_mode.is_active() {
        draw_command_prompt(f, app, command_area, theme);
    }

    // Materialize the TOP element's view ONCE up front. The filter bar's
    // match count must reflect THIS frame's filter — reading the memo
    // (`counts()`) before deriving showed the PREVIOUS frame's count, so
    // on quiet data the bar lagged a keystroke behind the table forever.
    // The Arc is owned, so the mut borrow ends here and the blocks below
    // re-borrow freely.
    let level = app.ui.column_level;
    let max_col_width = app.config.ui.max_column_width;
    let view = {
        let element = app.nav.top_mut();
        let v = element.view(level, max_col_width);
        // Record what this frame SHOWS the user (select mode or not) on
        // the element — the select-mode gate refuses batch keys that
        // arrive after an async prune flipped the mode out from under the
        // painted frame.
        let select_mode = element.has_marks();
        if let Some(it) = element.table_interaction_mut() {
            it.rendered_select_mode = select_mode;
        }
        v
    };

    // 3. Filter prompt — counts from THIS frame's view.
    if filter_visible {
        let filtered = view.keys.len();
        let total = view.total_rows;
        let fi = app.nav.top().filter_input();
        let match_count = if fi.text().is_empty() && !app.nav.is_drilled() {
            total
        } else {
            filtered
        };
        let filter_bar = FilterBar::new(fi.active(), fi.text(), match_count, total, theme);
        f.render_widget(filter_bar, filter_area);
    }

    // 4. Resource table — element-owned: query + predicates, columns
    // (NAMESPACE membership fixed at construction), title, scope label,
    // cursor. Nothing reads ambient state; nothing reaches below the top.
    let element = app.nav.top_mut();
    if view.total_rows == 0 {
        // No data at all (loading / failed / genuinely empty): bordered
        // block with a centered status line.
        let text = match element.data_state() {
            crate::app::table::TableDataState::Failed(err) => format!("Error: {}", err),
            crate::app::table::TableDataState::Ready => format!("No {} found.", element.title()),
            crate::app::table::TableDataState::Initializing => app.ui.anim.bar("Loading..."),
        };
        let status_line = Line::from(Span::styled(text, theme.info_value));
        let scope = element.scope_label();
        // The selected-count indicator renders HERE too: this branch is
        // exactly what shows during the Ctrl-R window, where marks
        // survive the cleared rows — select mode must not visually
        // vanish at the one moment its honesty matters most.
        let marked_count = element.marked_count();
        let sel_suffix = if marked_count > 0 {
            format!("[{} selected]", marked_count)
        } else {
            String::new()
        };
        let title_str = if scope.is_empty() {
            format!(" {}[0]{}", element.title(), sel_suffix)
        } else {
            format!(" {}({})[0]{}", element.title(), scope, sel_suffix)
        };
        let block = Block::bordered()
            .title(Span::styled(title_str, theme.title))
            .border_style(theme.border);
        let inner = block.inner(table_area);
        f.render_widget(block, table_area);
        if inner.height > 0 && inner.width > 0 {
            let center_y = inner.y + inner.height / 2;
            let center_x = inner.x + inner.width.saturating_sub(status_line.width() as u16) / 2;
            f.render_widget(status_line, Rect::new(center_x, center_y, inner.width, 1));
        }
    } else {
        let marked = element.marked_snapshot();
        let changed = element.changed_snapshot();
        let patterns = element.grep_patterns();
        let title = element.title().to_string();
        let Some(it) = element.table_interaction() else { return };
        let headers: Vec<&str> = view.headers.iter().map(String::as_str).collect();
        let display_sort_col = view.visible_cols.iter().position(|&i| i == it.sort.col);
        let snap = TableSnapshot {
            headers,
            rows: &view.rows,
            selected: it.selected.min(view.keys.len().saturating_sub(1)),
            offset: it.offset,
            selected_col: it.selected_col.min(view.visible_cols.len().saturating_sub(1)),
            col_offset: it.col_offset,
            sort_ascending: it.sort.ascending,
            display_sort_col,
            namespace: element.scope_label(),
            marked: &marked,
            changed_rows: &changed,
            row_keys: &view.keys,
            row_health: &view.health,
            cell_style: &view.cell_style,
            col_widths: &view.col_widths,
            search_patterns: &patterns,
        };
        let (new_offset, new_page_size, new_col_offset) =
            draw_resource_table(f, table_area, &title, &snap, theme);
        if let Some(it) = element.table_interaction_mut() {
            it.offset = new_offset;
            it.page_size = new_page_size;
            it.col_offset = new_col_offset;
        }
    }

    // 5. Breadcrumb bar — nav path on left, ? help hint on right
    let bc = app.nav.breadcrumb();
    let mut spans = Vec::new();
    spans.push(Span::styled(format!(" {} ", bc), theme.title));
    if app.nav.is_drilled() {
        spans.push(Span::styled(" | ", theme.info_label));
        spans.push(Span::styled("Esc", theme.title_filter_indicator));
        spans.push(Span::styled(" back", theme.info_label));
    }
    let left = Line::from(spans);
    f.render_widget(left, breadcrumb_area);
    // Right-aligned help hint
    let hint = " ? help ";
    let hint_x = breadcrumb_area.x + breadcrumb_area.width.saturating_sub(hint.len() as u16);
    f.buffer_mut().set_string(hint_x, breadcrumb_area.y, hint, theme.info_label);

    // 6. Flash message area (handled by ui/mod.rs overlay, but we reserve the line)
    // Draw a subtle flash area background
    if flash_area.width > 0 && flash_area.height > 0 {
        let empty = Line::raw("");
        f.render_widget(empty, flash_area);
    }
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

    let input = app.ui.input_mode.input().unwrap_or("");
    let ghost: String = app.best_completion()
        .and_then(|c| c.strip_prefix(input).map(str::to_string))
        .unwrap_or_default();

    // Fish-style rendering: typed text (bright) followed immediately by ghost
    // text (dim/italic) with no block cursor in between. The terminal cursor
    // is placed right after the typed text via set_cursor_position.
    let prefix = app.ui.input_mode.prompt();
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

